use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use auc_tool::application::{
    ApplicationClient, ApplicationError, ApplicationRequest, ApplicationResponse,
    ErrorCode as ApplicationErrorCode,
};
use auc_tool::product::{APPLICATION_SOCKET_PATH, MANAGEMENT_SOCKET_PATH, managed_product};
use capulus::managed::{
    ErrorCode as ManagementErrorCode, JobId, JobPhase, ManagementClient, ManagementClientOptions,
    ManagementError, ManagementRequest, ManagementResponse, RedeployJob, VersionTarget,
};
use capulus::ui::{TaskOptions, TaskVisibility, Ui};
use clap::{Parser, Subcommand};

#[path = "auc/ui.rs"]
mod ui;

use ui::{
    phase_name, redeploy_recorded_state, render_credential, render_job, render_status,
    terminal_text,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const JOB_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);
const REDEPLOY_WAIT_GRACE: Duration = Duration::from_secs(2 * 60);

#[derive(Parser)]
#[command(
    name = "auc",
    version,
    about = "Machine-local software passkey authenticator",
    infer_subcommands = true
)]
struct Cli {
    #[command(flatten)]
    ui: ui::UiArgs,

    #[command(subcommand)]
    command: CommandLine,
}

#[derive(Subcommand)]
enum CommandLine {
    /// Show the agent, virtual authenticator, presence, and credential status.
    Status,

    /// Approve the passkey operation currently waiting for user presence.
    Touch,

    /// Inspect or permanently delete resident credentials.
    Credentials {
        #[command(subcommand)]
        command: CredentialCommand,
    },

    /// Install, repair, redeploy, or remove the managed system service.
    System {
        #[command(subcommand)]
        command: SystemCommand,
    },
}

#[derive(Subcommand)]
enum CredentialCommand {
    /// List every resident credential in the encrypted local vault.
    List,

    /// Permanently tombstone one credential.
    Delete {
        #[arg(value_name = "CREDENTIAL_ID")]
        credential_id: String,
        #[arg(long, help = "Delete without an interactive confirmation")]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum SystemCommand {
    /// Bootstrap the managed auc system service through sudo.
    Install,

    /// Restore managed files and refresh the installed systemd units.
    Repair,

    /// Redeploy auc from its configured Cargo registry release.
    Redeploy {
        #[arg(
            long,
            value_name = "VERSION",
            help = "Require this exact release version"
        )]
        version: Option<String>,
        #[arg(long, help = "Return after scheduling instead of following the job")]
        no_wait: bool,
    },

    /// Show the durable state of one redeploy job.
    Job {
        #[arg(value_name = "JOB_ID")]
        job: String,
    },

    /// Remove the managed service while preserving the encrypted vault by default.
    Uninstall {
        #[arg(
            long,
            help = "Also destroy every credential, PIN, tombstone, and vault key"
        )]
        purge_vault: bool,
        #[arg(long, help = "Uninstall without interactive confirmations")]
        yes: bool,
    },
}

fn main() -> capulus::CliTermination {
    let cli = Cli::parse();
    let ui = match Ui::from_options(cli.ui.options()) {
        Ok(ui) => ui,
        Err(error) => return capulus::CliTermination::without_ui(Err(error)),
    };
    capulus::CliTermination::with_ui(&ui, run(cli.command, &ui).map(|()| 0))
}

fn run(command: CommandLine, ui: &Ui) -> Result<()> {
    match command {
        CommandLine::Status => status(ui),
        CommandLine::Touch => touch(ui),
        CommandLine::Credentials { command } => credentials(command, ui),
        CommandLine::System { command } => system(command, ui),
    }
}

fn application_client() -> ApplicationClient {
    ApplicationClient::new(APPLICATION_SOCKET_PATH)
}

fn management_client() -> ManagementClient {
    ManagementClient::new(ManagementClientOptions::new(MANAGEMENT_SOCKET_PATH))
}

fn job_status_client() -> ManagementClient {
    let mut options = ManagementClientOptions::new(MANAGEMENT_SOCKET_PATH);
    options.timeout = JOB_STATUS_TIMEOUT;
    ManagementClient::new(options)
}

fn application_request(ui: &Ui, request: ApplicationRequest) -> Result<ApplicationResponse> {
    match application_client().request(request) {
        Ok(response) => Ok(response),
        Err(error) => {
            ui.check_cancelled()?;
            Err(error.into())
        }
    }
}

fn management_request(ui: &Ui, request: ManagementRequest) -> Result<ManagementResponse> {
    match management_client().request(request) {
        Ok(response) => Ok(response),
        Err(error) => {
            ui.check_cancelled()?;
            Err(error.into())
        }
    }
}

fn application_rejection_is_definitive(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<ApplicationError>(),
        Some(ApplicationError::Remote {
            code: ApplicationErrorCode::BadRequest
                | ApplicationErrorCode::Unauthorized
                | ApplicationErrorCode::UnsupportedProtocol
                | ApplicationErrorCode::NotFound
                | ApplicationErrorCode::Conflict,
            ..
        })
    )
}

fn management_rejection_is_definitive(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<ManagementError>(),
        Some(ManagementError::Remote {
            code: ManagementErrorCode::BadRequest
                | ManagementErrorCode::Unauthorized
                | ManagementErrorCode::UnsupportedProtocol
                | ManagementErrorCode::NotFound
                | ManagementErrorCode::Unavailable,
            ..
        })
    )
}

fn management_query_error_is_definitive(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<ManagementError>(),
        Some(ManagementError::Remote {
            code: ManagementErrorCode::BadRequest
                | ManagementErrorCode::Unauthorized
                | ManagementErrorCode::UnsupportedProtocol
                | ManagementErrorCode::NotFound,
            ..
        })
    )
}

fn request_task(ui: &Ui, label: impl Into<String>) -> Result<capulus::ui::Task> {
    ui.task(TaskOptions {
        label: label.into(),
        deadline: Some(REQUEST_TIMEOUT),
        ..TaskOptions::default()
    })
}

fn status(ui: &Ui) -> Result<()> {
    let task = request_task(ui, "Checking auc status")?;
    let status = match application_request(ui, ApplicationRequest::Status)? {
        ApplicationResponse::Status(status) => status,
        _ => bail!("auc-agent returned the wrong status response"),
    };
    task.finish_and_clear();
    println!("{}", render_status(&status, &ui.stdout_render_target()));
    Ok(())
}

fn touch(ui: &Ui) -> Result<()> {
    let task = request_task(ui, "Approving passkey user presence")?;
    let receipt = match application_request(ui, ApplicationRequest::Touch) {
        Ok(ApplicationResponse::Touch(receipt)) => receipt,
        Ok(_) => {
            task.abandon("Approval state unavailable; check the browser before retrying");
            bail!("auc-agent returned the wrong touch response");
        }
        Err(error) if application_rejection_is_definitive(&error) => {
            task.finish_and_clear();
            return Err(error);
        }
        Err(error) => {
            task.abandon(if capulus::error_is_cancelled(&error) {
                "Approval interrupted; the browser may already have consumed the gesture"
            } else {
                "Approval result unavailable; check the browser before retrying"
            });
            let context = format!(
                "passkey approval response was not received; the gesture may have been consumed: {error}"
            );
            return Err(error).context(context);
        }
    };
    task.finish_and_clear();
    ui.success(format!(
        "Approved {} for {}.",
        terminal_text(&receipt.operation),
        terminal_text(&receipt.rp_id)
    ));
    Ok(())
}

fn credentials(command: CredentialCommand, ui: &Ui) -> Result<()> {
    match command {
        CredentialCommand::List => list_credentials(ui),
        CredentialCommand::Delete { credential_id, yes } => {
            delete_credential(ui, credential_id, yes)
        }
    }
}

fn list_credentials(ui: &Ui) -> Result<()> {
    let task = request_task(ui, "Loading auc credentials")?;
    let credentials = match application_request(ui, ApplicationRequest::ListCredentials)? {
        ApplicationResponse::Credentials { credentials } => credentials,
        _ => bail!("auc-agent returned the wrong credential-list response"),
    };
    task.finish_and_clear();
    if credentials.is_empty() {
        println!("No credentials.");
    } else {
        let target = ui.stdout_render_target();
        for credential in &credentials {
            for line in render_credential(credential, &target) {
                println!("{line}");
            }
        }
    }
    Ok(())
}

fn delete_credential(ui: &Ui, credential_id: String, yes: bool) -> Result<()> {
    if !yes
        && !ui.suspend(|| {
            capulus::ui::prompt_confirm_with_message(
                &format!(
                    "Permanently delete credential {}?",
                    terminal_text(&credential_id)
                ),
                false,
                "Credential deletion requires an interactive terminal or --yes.",
            )
        })?
    {
        ui.info("Credential retained.");
        return Ok(());
    }
    let task = request_task(ui, "Deleting auc credential")?;
    let deleted = match application_request(
        ui,
        ApplicationRequest::DeleteCredential { credential_id },
    ) {
        Ok(ApplicationResponse::Deleted { credential_id }) => credential_id,
        Ok(_) => {
            task.abandon("Deletion state unavailable; list credentials before retrying");
            bail!("auc-agent returned the wrong credential-delete response");
        }
        Err(error) if application_rejection_is_definitive(&error) => {
            task.finish_and_clear();
            return Err(error);
        }
        Err(error) => {
            task.abandon(if capulus::error_is_cancelled(&error) {
                "Deletion interrupted; the credential outcome is unknown"
            } else {
                "Deletion result unavailable; list credentials before retrying"
            });
            let context = format!(
                "credential deletion response was not received; list credentials before retrying: {error}"
            );
            return Err(error).context(context);
        }
    };
    task.finish_and_clear();
    ui.success(format!("Deleted credential {}.", terminal_text(&deleted)));
    Ok(())
}

fn system(command: SystemCommand, ui: &Ui) -> Result<()> {
    match command {
        SystemCommand::Install => install(ui),
        SystemCommand::Repair => repair(ui),
        SystemCommand::Redeploy { version, no_wait } => redeploy(ui, version, no_wait),
        SystemCommand::Job { job } => {
            let job_id = JobId::parse(&job)?;
            let task = ui.task(TaskOptions {
                label: format!("Loading auc redeploy job {job_id}"),
                deadline: Some(JOB_STATUS_TIMEOUT),
                ..TaskOptions::default()
            })?;
            let job = query_job(ui, job_id)?;
            task.finish_and_clear();
            print_job(&job, &ui.stdout_render_target());
            if job.phase == JobPhase::Failed {
                bail!("auc redeploy job {job_id} failed")
            } else {
                Ok(())
            }
        }
        SystemCommand::Uninstall { purge_vault, yes } => uninstall(ui, purge_vault, yes),
    }
}

fn repair(ui: &Ui) -> Result<()> {
    let task = ui.task(TaskOptions {
        label: "Repairing the managed auc installation".to_string(),
        deadline: Some(REQUEST_TIMEOUT),
        visibility: TaskVisibility::Immediate,
        ..TaskOptions::default()
    })?;
    task.set_phase("checking managed files and systemd units");
    let outcome = match management_request(ui, ManagementRequest::Repair) {
        Ok(ManagementResponse::Repair(outcome)) => outcome,
        Ok(_) => {
            task.abandon("Repair state unavailable; rerun repair to reconcile managed state");
            bail!("auc-agent returned the wrong repair response");
        }
        Err(error) if management_rejection_is_definitive(&error) => {
            task.finish_and_clear();
            return Err(error);
        }
        Err(error) => {
            task.abandon(if capulus::error_is_cancelled(&error) {
                "Repair interrupted; managed state may have changed"
            } else {
                "Repair result unavailable; rerun repair to reconcile managed state"
            });
            let context = format!(
                "auc repair response was not received; rerun repair to reconcile managed state: {error}"
            );
            return Err(error).context(context);
        }
    };
    task.finish(terminal_text(&outcome.detail));
    Ok(())
}

fn redeploy(ui: &Ui, version: Option<String>, no_wait: bool) -> Result<()> {
    let wait_timeout = if no_wait {
        None
    } else {
        Some(managed_product()?.redeploy_runtime_max() + REDEPLOY_WAIT_GRACE)
    };
    let schedule = ui.task(TaskOptions {
        label: "Scheduling an auc redeploy".to_string(),
        deadline: Some(REQUEST_TIMEOUT),
        visibility: TaskVisibility::Immediate,
        ..TaskOptions::default()
    })?;
    let target = match version {
        Some(version) => {
            schedule.set_phase(format!(
                "requesting exact release v{}",
                terminal_text(&version)
            ));
            VersionTarget::Exact(version)
        }
        None => {
            schedule.set_phase("resolving the latest published release");
            VersionTarget::Latest
        }
    };
    let outcome = match management_request(
        ui,
        ManagementRequest::Redeploy {
            target,
            reinstall_requesting_user: true,
        },
    ) {
        Ok(ManagementResponse::Redeploy(outcome)) => outcome,
        Ok(_) => {
            schedule
                .abandon("Redeploy scheduling state unavailable; a matching job may have started");
            ui.detail("Rerun the same redeploy command to join any matching active job.");
            bail!("auc-agent returned the wrong redeploy response");
        }
        Err(error) if management_rejection_is_definitive(&error) => {
            schedule.finish_and_clear();
            return Err(error);
        }
        Err(error) => {
            schedule.abandon(if capulus::error_is_cancelled(&error) {
                "Redeploy scheduling interrupted; a matching job may have started"
            } else {
                "Redeploy scheduling result unavailable; a matching job may have started"
            });
            ui.detail("Rerun the same redeploy command to join any matching active job.");
            let context = format!(
                "auc redeploy scheduling response was not received; a matching job may be active: {error}"
            );
            return Err(error).context(context);
        }
    };
    schedule.finish(format!(
        "{} auc v{} redeploy · job {}",
        if outcome.started { "Started" } else { "Joined" },
        terminal_text(&outcome.version),
        outcome.job
    ));
    match wait_timeout {
        None => {
            ui.detail(format!(
                "Follow with `auc system job {}`; the redeploy continues independently.",
                outcome.job
            ));
            Ok(())
        }
        Some(wait_timeout) => wait_for_job(ui, outcome.job, wait_timeout),
    }
}

fn install(ui: &Ui) -> Result<()> {
    let uid = rustix::process::geteuid().as_raw();
    if uid == 0 {
        bail!("run auc system install as the local user who should operate auc, not as root");
    }
    let agent = sibling_agent()?;
    verify_sibling_version(&agent)?;
    ui.info("Installing auc through sudo; authentication may be required.");
    let status = ui
        .suspend(|| {
            Command::new("/usr/bin/sudo")
                .arg("--")
                .arg(agent)
                .arg("install")
                .arg("--operator-uid")
                .arg(uid.to_string())
                .status()
        })
        .context("failed to invoke sudo for auc system installation")?;
    if !status.success() {
        if let Err(error) = ui.check_cancelled() {
            ui.warn("Installation interrupted; its final state is unknown.");
            ui.detail("Check `auc status`; if installed, run `auc system repair`.");
            return Err(error.into());
        }
        bail!("auc system installation failed with {status}");
    }
    ui.success("auc was installed and its virtual authenticator is healthy.");
    ui.detail("Next: start a new login session so membership in the auc group is active.");
    Ok(())
}

fn uninstall(ui: &Ui, purge_vault: bool, yes: bool) -> Result<()> {
    let uid = rustix::process::geteuid().as_raw();
    if uid == 0 {
        bail!("run auc system uninstall as an authorized local auc operator, not as root");
    }
    if !yes
        && !ui.suspend(|| {
            capulus::ui::prompt_confirm_with_message(
                "Remove the auc system service and binaries?",
                false,
                "auc uninstall requires an interactive terminal or --yes.",
            )
        })?
    {
        ui.info("auc was retained.");
        return Ok(());
    }
    if purge_vault {
        ui.warn(
            "Vault purge is irreversible: every credential, PIN, tombstone, and key will be destroyed.",
        );
        if !yes
            && !ui.suspend(|| {
                capulus::ui::prompt_confirm_with_message(
                    "Permanently destroy the encrypted auc vault?",
                    false,
                    "Vault purge requires an interactive terminal or --yes.",
                )
            })?
        {
            ui.info("auc was retained; the encrypted vault was not changed.");
            return Ok(());
        }
    }
    let agent = Path::new("/usr/local/bin/auc-agent");
    let metadata = fs::symlink_metadata(agent).context("system auc-agent is not installed")?;
    if !metadata.file_type().is_file()
        || metadata.uid() != 0
        || metadata.gid() != 0
        || metadata.permissions().mode() & 0o111 == 0
    {
        bail!("system auc-agent failed ownership, type, or mode validation");
    }
    let mut command = Command::new("/usr/bin/sudo");
    command
        .arg("--")
        .arg(agent)
        .arg("uninstall")
        .arg("--operator-uid")
        .arg(uid.to_string());
    if purge_vault {
        command.arg("--purge-vault");
    }
    ui.info("Uninstalling auc through sudo; authentication may be required.");
    let status = ui
        .suspend(|| command.status())
        .context("failed to invoke sudo for auc system uninstall")?;
    if !status.success() {
        if let Err(error) = ui.check_cancelled() {
            ui.warn(if purge_vault {
                "Uninstall interrupted; the system-removal and vault-purge outcomes are unknown."
            } else {
                "Uninstall interrupted; the system-removal outcome is unknown, but no vault purge was requested."
            });
            ui.detail("Check `auc status` and the installed files before retrying.");
            return Err(error.into());
        }
        bail!("auc system uninstall failed with {status}");
    }
    if purge_vault {
        ui.success("auc, its access group, and its encrypted vault were removed.");
        ui.detail("All credentials, PIN state, tombstones, and vault keys were destroyed.");
    } else {
        ui.success("auc system files were removed.");
        ui.detail("The encrypted vault and auc access policy were preserved.");
    }
    Ok(())
}

fn sibling_agent() -> Result<PathBuf> {
    let executable = fs::canonicalize("/proc/self/exe")
        .context("failed to resolve the running auc executable")?;
    let agent = executable
        .parent()
        .ok_or_else(|| anyhow!("auc executable has no parent directory"))?
        .join("auc-agent");
    let metadata = fs::symlink_metadata(&agent).with_context(|| {
        format!(
            "auc-agent is not installed beside auc at {}",
            agent.display()
        )
    })?;
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o111 == 0
    {
        bail!("sibling auc-agent is not an executable regular file owned by the invoking user");
    }
    Ok(agent)
}

fn verify_sibling_version(agent: &Path) -> Result<()> {
    let output = Command::new(agent)
        .arg("--version")
        .output()
        .context("failed to query sibling auc-agent version")?;
    if !output.status.success()
        || output.stdout.len() > 4096
        || String::from_utf8(output.stdout)?.split_whitespace().last()
            != Some(env!("CARGO_PKG_VERSION"))
    {
        bail!(
            "sibling auc-agent does not match auc {}",
            env!("CARGO_PKG_VERSION")
        );
    }
    Ok(())
}

fn wait_for_job(ui: &Ui, job: JobId, wait_timeout: Duration) -> Result<()> {
    let task = ui.task(TaskOptions {
        label: format!("Following auc redeploy job {job}"),
        deadline: Some(wait_timeout),
        visibility: TaskVisibility::Immediate,
        ..TaskOptions::default()
    })?;
    let started = Instant::now();
    let mut rendered_phase = None;
    let mut last_status = None;
    loop {
        let last_observation;
        match query_job(ui, job) {
            Ok(status) => {
                let phase = phase_name(&status.phase);
                if rendered_phase.as_deref() != Some(phase) {
                    task.set_phase(phase);
                    rendered_phase = Some(phase.to_string());
                }
                last_observation = terminal_text(&status.detail);
                task.set_detail(last_observation.clone());
                match &status.phase {
                    JobPhase::Complete => {
                        task.finish(format!(
                            "auc v{} redeployed · job {job}",
                            terminal_text(&status.version)
                        ));
                        if status.required_user_reinstalled == Some(false) {
                            ui.warn(
                                "The system redeploy completed, but the requesting user's auc CLI was not reinstalled.",
                            );
                        }
                        return Ok(());
                    }
                    JobPhase::Failed => {
                        task.fail(format!(
                            "auc v{} redeploy failed · {}",
                            terminal_text(&status.version),
                            last_observation
                        ));
                        report_observed_job_state(ui, job, Some(&status));
                        bail!("auc redeploy job {job} failed");
                    }
                    _ => {}
                }
                last_status = Some(status);
            }
            Err(error) if capulus::error_is_cancelled(&error) => {
                task.abandon(format!(
                    "Wait interrupted; redeploy job {job} continues independently"
                ));
                report_observed_job_state(ui, job, last_status.as_ref());
                return Err(error).with_context(|| {
                    format!("redeploy wait interrupted; job {job} continues independently")
                });
            }
            Err(error) if management_query_error_is_definitive(&error) => {
                task.fail(format!("Unable to follow redeploy job {job} · {error}"));
                report_observed_job_state(ui, job, last_status.as_ref());
                return Err(error);
            }
            Err(error) => {
                let phase = "waiting for auc-agent";
                if rendered_phase.as_deref() != Some(phase) {
                    task.set_phase(phase);
                    rendered_phase = Some(phase.to_string());
                }
                last_observation = format!("job status unavailable: {error}");
                task.set_detail(terminal_text(&last_observation));
            }
        }
        if started.elapsed() >= wait_timeout {
            task.abandon(format!(
                "Wait timed out; redeploy job {job} may still be running"
            ));
            report_observed_job_state(ui, job, last_status.as_ref());
            bail!(
                "timed out after {} waiting for redeploy job {job}; last observed state: {}",
                capulus::ui::format_duration(wait_timeout),
                terminal_text(&last_observation)
            );
        }
        if let Err(error) = ui.sleep(JOB_POLL_INTERVAL) {
            task.abandon(format!(
                "Wait interrupted; redeploy job {job} continues independently"
            ));
            report_observed_job_state(ui, job, last_status.as_ref());
            return Err(error).with_context(|| {
                format!("redeploy wait interrupted; job {job} continues independently")
            });
        }
    }
}

fn query_job(ui: &Ui, job: JobId) -> Result<RedeployJob> {
    let response = match job_status_client().request(ManagementRequest::JobStatus { job }) {
        Ok(response) => response,
        Err(error) => {
            ui.check_cancelled()?;
            return Err(error.into());
        }
    };
    match response {
        ManagementResponse::Job(status) => Ok(status),
        _ => bail!("auc-agent returned the wrong redeploy-job response"),
    }
}

fn report_observed_job_state(ui: &Ui, job: JobId, status: Option<&RedeployJob>) {
    if let Some(status) = status {
        ui.detail(format!(
            "job {job} · last recorded {} · {}",
            phase_name(&status.phase),
            redeploy_recorded_state(status)
        ));
    } else {
        ui.detail(format!(
            "job {job} · no durable state was observed; inspect it with `auc system job {job}`."
        ));
    }
}

fn print_job(job: &RedeployJob, target: &impl capulus::ui::RenderTarget) {
    for line in render_job(job, target) {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capulus::ui::{ColorMode, ProgressMode, UiOptions};

    #[test]
    fn mutation_reporting_distinguishes_rejections_from_unknown_outcomes() {
        let rejected: anyhow::Error = ApplicationError::Remote {
            code: ApplicationErrorCode::Conflict,
            message: "nothing is waiting".to_string(),
        }
        .into();
        assert!(application_rejection_is_definitive(&rejected));

        let unresolved: anyhow::Error = ManagementError::Remote {
            code: ManagementErrorCode::Unavailable,
            message: "release unavailable".to_string(),
        }
        .into();
        assert!(management_rejection_is_definitive(&unresolved));

        let scheduling_unknown: anyhow::Error = ManagementError::Remote {
            code: ManagementErrorCode::Conflict,
            message: "scheduling failed".to_string(),
        }
        .into();
        assert!(!management_rejection_is_definitive(&scheduling_unknown));

        let missing_job: anyhow::Error = ManagementError::Remote {
            code: ManagementErrorCode::NotFound,
            message: "job missing".to_string(),
        }
        .into();
        assert!(management_query_error_is_definitive(&missing_job));

        let temporarily_unavailable: anyhow::Error = ManagementError::Remote {
            code: ManagementErrorCode::Unavailable,
            message: "try again".to_string(),
        }
        .into();
        assert!(!management_query_error_is_definitive(
            &temporarily_unavailable
        ));
    }

    #[test]
    fn global_presentation_options_are_available_after_nested_commands() {
        let cli = Cli::try_parse_from([
            "auc",
            "system",
            "job",
            "deadbeefdeadbeefdeadbeefdeadbeef",
            "--progress",
            "plain",
            "--color",
            "never",
        ])
        .unwrap();
        assert!(matches!(cli.command, CommandLine::System { .. }));
        let UiOptions {
            progress, color, ..
        } = cli.ui.options();
        assert_eq!(progress, ProgressMode::Plain);
        assert_eq!(color, ColorMode::Never);
    }
}
