use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use capulus::managed::{
    BuildArtifacts, JobId, RedeployCoordinator, SystemInstallation, SystemUninstallation,
    UnixAccount,
};
use serde::{Deserialize, Serialize};

use crate::product::{ACCESS_GROUP, managed_product, wait_until_healthy};
use crate::vault::Vault;

const ACCESS_POLICY_PATH: &str = "/var/lib/auc/access-policy.json";
const ACCESS_POLICY_SCHEMA: u16 = 1;
const COMMAND_OUTPUT_LIMIT: usize = 64 * 1024;

pub async fn install(operator_uid: u32) -> Result<()> {
    require_root()?;
    validate_audit_login(operator_uid)?;
    let operator = UnixAccount::by_uid(operator_uid)?;
    operator.validate_interactive()?;
    ensure_uhid_device()?;
    let product = managed_product()?;
    let artifacts = BuildArtifacts::from_installed_program(&product)?;
    Vault::open().context("failed to initialize or validate the auc vault")?;
    let mut access = AccessGroupSetup::prepare(operator)?;
    let mut installation =
        match SystemInstallation::prepare(&product, JobId::random(), &artifacts).await {
            Ok(installation) => installation,
            Err(error) => {
                access.rollback()?;
                return Err(error.context("failed to prepare the auc system installation"));
            }
        };
    if let Err(error) = installation.commit_files() {
        return Err(rollback_installation(&mut installation, &mut access, error).await);
    }
    if let Err(error) = installation.activate().await {
        return Err(rollback_installation(&mut installation, &mut access, error).await);
    }
    let version = product.version().clone();
    let health =
        tokio::task::spawn_blocking(move || wait_until_healthy(&version, Duration::from_secs(60)))
            .await
            .context("auc installation health task panicked")?;
    if let Err(error) = health {
        return Err(rollback_installation(&mut installation, &mut access, error).await);
    }
    if let Err(error) = access.commit() {
        return Err(rollback_installation(&mut installation, &mut access, error).await);
    }
    if let Err(error) = installation.finalize() {
        if installation.acceptance_committed() {
            access.finish();
            return Err(error.context(
                "auc is installed and healthy, but committed installation cleanup failed",
            ));
        }
        return Err(rollback_installation(&mut installation, &mut access, error).await);
    }
    access.finish();
    Ok(())
}

pub async fn uninstall(operator_uid: u32, purge_vault: bool) -> Result<()> {
    require_root()?;
    validate_audit_login(operator_uid)?;
    let operator = UnixAccount::by_uid(operator_uid)?;
    operator.validate_interactive()?;
    let policy = read_access_policy()?.ok_or_else(|| anyhow!("auc access policy is missing"))?;
    if !policy.operator_uids.contains(&operator_uid) || !user_has_group(&operator)? {
        bail!("the invoking user is not an authorized auc operator");
    }
    let product = Arc::new(managed_product()?);
    if RedeployCoordinator::new(Arc::clone(&product))?
        .reconciled_active()
        .await?
        .is_some_and(|job| !job.phase.is_terminal())
    {
        bail!("auc cannot be uninstalled while a redeploy is active");
    }
    ensure_no_pending_presence()?;
    let mut uninstallation = SystemUninstallation::prepare(&product, JobId::random()).await?;
    if let Err(error) = uninstallation.deactivate().await {
        return Err(rollback_uninstallation(&mut uninstallation, error).await);
    }
    if let Err(error) = uninstallation.remove_files() {
        return Err(rollback_uninstallation(&mut uninstallation, error).await);
    }
    if let Err(error) = uninstallation.finalize().await {
        if uninstallation.removal_committed() {
            return Err(error.context(
                "auc system files were removed, but committed uninstall cleanup is incomplete",
            ));
        }
        return Err(rollback_uninstallation(&mut uninstallation, error).await);
    }
    if purge_vault {
        Vault::purge().context("auc system files were removed, but vault destruction failed")?;
        checked_command(
            "/usr/sbin/groupdel",
            &[ACCESS_GROUP],
            "remove the auc access group",
        )
        .context("auc and its vault were removed, but the access group remains")?;
    }
    Ok(())
}

async fn rollback_uninstallation(
    uninstallation: &mut SystemUninstallation,
    cause: anyhow::Error,
) -> anyhow::Error {
    match uninstallation.rollback().await {
        Ok(()) => cause.context("auc uninstall failed and the installation was restored"),
        Err(rollback) => anyhow!(
            "auc uninstall failed: {cause:#}; restoring the installation also failed: {rollback:#}"
        ),
    }
}

fn ensure_no_pending_presence() -> Result<()> {
    use crate::application::{ApplicationClient, ApplicationRequest, ApplicationResponse};
    use crate::product::APPLICATION_SOCKET_PATH;

    match ApplicationClient::new(APPLICATION_SOCKET_PATH).request(ApplicationRequest::Status)? {
        ApplicationResponse::Status(status) if status.pending_touch => {
            bail!("auc cannot be uninstalled while a presence request is pending")
        }
        ApplicationResponse::Status(_) => Ok(()),
        _ => bail!("auc-agent returned the wrong status response before uninstall"),
    }
}

async fn rollback_installation(
    installation: &mut SystemInstallation,
    access: &mut AccessGroupSetup,
    cause: anyhow::Error,
) -> anyhow::Error {
    let installation_rollback = installation.rollback().await;
    let access_rollback = access.rollback();
    match (installation_rollback, access_rollback) {
        (Ok(()), Ok(())) => cause.context("auc installation failed and was rolled back"),
        (system, group) => anyhow!(
            "auc installation failed: {cause:#}; system rollback: {}; access-group rollback: {}",
            outcome(&system),
            outcome(&group),
        ),
    }
}

fn outcome(result: &Result<()>) -> String {
    match result {
        Ok(()) => "succeeded".to_string(),
        Err(error) => format!("failed: {error:#}"),
    }
}

fn validate_audit_login(operator_uid: u32) -> Result<()> {
    let value = fs::read_to_string("/proc/self/loginuid")
        .context("failed to read the kernel audit login UID")?;
    let login_uid = value
        .trim()
        .parse::<u32>()
        .context("kernel audit login UID is malformed")?;
    if login_uid == u32::MAX {
        bail!("kernel audit login UID is unset; run auc system install from a real login session");
    }
    if login_uid != operator_uid {
        bail!("requested auc operator does not match the kernel audit login UID");
    }
    Ok(())
}

fn ensure_uhid_device() -> Result<()> {
    let path = Path::new("/dev/uhid");
    if fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound) {
        checked_command(
            "/usr/sbin/modprobe",
            &["uhid"],
            "load the Linux UHID kernel module",
        )?;
    }
    let metadata = fs::symlink_metadata(path).context("Linux did not expose /dev/uhid")?;
    if !metadata.file_type().is_char_device() {
        bail!("/dev/uhid is not a real character device");
    }
    Ok(())
}

struct AccessGroupSetup {
    operator: UnixAccount,
    policy: AccessPolicy,
    group_created: bool,
    member_added: bool,
    policy_written: bool,
    finished: bool,
}

impl AccessGroupSetup {
    fn prepare(operator: UnixAccount) -> Result<Self> {
        let existing_policy = read_access_policy()?;
        let group_exists = command_success("/usr/bin/getent", &["group", ACCESS_GROUP])?;
        let group_created = match (existing_policy.is_some(), group_exists) {
            (true, true) => false,
            (true, false) => bail!("auc access policy exists but its Unix group is missing"),
            (false, true) => bail!("refusing to take ownership of a pre-existing auc Unix group"),
            (false, false) => {
                checked_command(
                    "/usr/sbin/groupadd",
                    &["--system", ACCESS_GROUP],
                    "create the auc access group",
                )?;
                true
            }
        };
        let policy = existing_policy.unwrap_or(AccessPolicy {
            schema: ACCESS_POLICY_SCHEMA,
            operator_uids: Vec::new(),
        });
        policy.validate()?;
        Ok(Self {
            operator,
            policy,
            group_created,
            member_added: false,
            policy_written: false,
            finished: false,
        })
    }

    fn commit(&mut self) -> Result<()> {
        if !self.policy.operator_uids.contains(&self.operator.uid) {
            checked_command(
                "/usr/sbin/usermod",
                &["--append", "--groups", ACCESS_GROUP, &self.operator.name],
                "add the selected user to the auc access group",
            )?;
            self.member_added = true;
            self.policy.operator_uids.push(self.operator.uid);
            self.policy.operator_uids.sort_unstable();
        }
        if !user_has_group(&self.operator)? {
            bail!("selected auc operator did not acquire access-group membership");
        }
        capulus::store::atomic_write(
            Path::new(ACCESS_POLICY_PATH),
            &serde_json::to_vec(&self.policy)?,
            Some(0o600),
            None,
        )?;
        self.policy_written = true;
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        if self.policy_written {
            if self.group_created {
                if let Err(error) = fs::remove_file(ACCESS_POLICY_PATH)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    errors.push(anyhow!(error).context("remove new auc access policy"));
                }
            } else if self.member_added {
                self.policy
                    .operator_uids
                    .retain(|uid| *uid != self.operator.uid);
                if let Err(error) = capulus::store::atomic_write(
                    Path::new(ACCESS_POLICY_PATH),
                    &serde_json::to_vec(&self.policy)?,
                    Some(0o600),
                    None,
                ) {
                    errors.push(error.context("restore auc access policy"));
                }
            }
            self.policy_written = false;
        }
        if self.group_created {
            if let Err(error) = checked_command(
                "/usr/sbin/groupdel",
                &[ACCESS_GROUP],
                "remove the new auc access group",
            ) {
                errors.push(error);
            }
            self.group_created = false;
            self.member_added = false;
        } else if self.member_added {
            if let Err(error) = checked_command(
                "/usr/bin/gpasswd",
                &["--delete", &self.operator.name, ACCESS_GROUP],
                "restore auc access-group membership",
            ) {
                errors.push(error);
            }
            self.member_added = false;
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!(
                "{}",
                errors
                    .into_iter()
                    .map(|error| format!("{error:#}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        }
    }

    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for AccessGroupSetup {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.rollback();
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccessPolicy {
    schema: u16,
    operator_uids: Vec<u32>,
}

impl AccessPolicy {
    fn validate(&self) -> Result<()> {
        if self.schema != ACCESS_POLICY_SCHEMA
            || self.operator_uids.len() > 64
            || self.operator_uids.contains(&0)
            || self
                .operator_uids
                .windows(2)
                .any(|window| window[0] >= window[1])
        {
            bail!("auc access policy is invalid");
        }
        Ok(())
    }
}

fn read_access_policy() -> Result<Option<AccessPolicy>> {
    let path = Path::new(ACCESS_POLICY_PATH);
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file()
        || metadata.uid() != 0
        || metadata.gid() != 0
        || metadata.mode() & 0o7777 != 0o600
        || metadata.len() > 16 * 1024
    {
        bail!("auc access policy failed ownership, type, mode, or size validation");
    }
    let policy: AccessPolicy = serde_json::from_slice(&fs::read(path)?)?;
    policy.validate()?;
    Ok(Some(policy))
}

pub(crate) fn operator_is_authorized(uid: u32) -> Result<bool> {
    Ok(
        read_access_policy()?
            .is_some_and(|policy| policy.operator_uids.binary_search(&uid).is_ok()),
    )
}

fn user_has_group(account: &UnixAccount) -> Result<bool> {
    let output = checked_output(
        "/usr/bin/id",
        &["--name", "--groups", &account.name],
        "inspect auc access-group membership",
    )?;
    Ok(String::from_utf8(output.stdout)?
        .split_whitespace()
        .any(|group| group == ACCESS_GROUP))
}

fn command_success(program: &str, arguments: &[&str]) -> Result<bool> {
    let output = bounded_command(program, arguments).output()?;
    ensure_bounded_output(&output)?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(2) => Ok(false),
        _ => bail!("{} failed: {}", program, output_detail(&output)),
    }
}

fn checked_command(program: &str, arguments: &[&str], action: &str) -> Result<()> {
    checked_output(program, arguments, action).map(|_| ())
}

fn checked_output(program: &str, arguments: &[&str], action: &str) -> Result<Output> {
    let output = bounded_command(program, arguments)
        .output()
        .with_context(|| format!("failed to {action}"))?;
    ensure_bounded_output(&output)?;
    if !output.status.success() {
        bail!("failed to {action}: {}", output_detail(&output));
    }
    Ok(output)
}

fn bounded_command(program: &str, arguments: &[&str]) -> Command {
    let mut command = Command::new("/usr/bin/timeout");
    command
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .env("LANG", "C.UTF-8")
        .args(["--signal=TERM", "--kill-after=2s", "15s", "--", program])
        .args(arguments);
    command
}

fn ensure_bounded_output(output: &Output) -> Result<()> {
    if output.stdout.len() > COMMAND_OUTPUT_LIMIT || output.stderr.len() > COMMAND_OUTPUT_LIMIT {
        bail!("system account command output exceeded its safety limit");
    }
    Ok(())
}

fn output_detail(output: &Output) -> String {
    String::from_utf8_lossy(if output.stderr.is_empty() {
        &output.stdout
    } else {
        &output.stderr
    })
    .trim()
    .to_string()
}

fn require_root() -> Result<()> {
    if rustix::process::geteuid().is_root() {
        Ok(())
    } else {
        bail!("auc system installation requires root")
    }
}
