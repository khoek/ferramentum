use std::env;
use std::fmt;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};

use crate::codex::AccountRotation;

mod auth;
mod auth_lock;
mod enroll;
mod isolated_home;
mod paths;
mod quota;
mod rollout;
mod store;
mod ui;

use self::auth::{Credential, CredentialFacts, validate_email};
use self::paths::RuntimePaths;
use self::store::{Profile, Store, ensure_codex_uses_file_credentials};
use self::ui::{AccountStatus, AccountView, ListView, QuotaStatus};

#[derive(Debug, Args)]
#[command(after_help = concat!(
    "Examples:\n",
    "  kai cred add personal@example.com\n",
    "  kai cred add work@example.com --device-auth\n",
    "  kai cred add personal@example.com --force\n",
    "  kai cred fix\n",
    "  kai cred list\n",
    "  kai cred tickle\n",
    "  kai next\n",
    "  kai cred activate personal@example.com\n",
    "  kai cred remove work@example.com",
))]
pub struct CredArgs {
    #[command(subcommand)]
    pub command: CredCommand,
}

#[derive(Debug, Subcommand)]
pub enum CredCommand {
    #[command(
        visible_alias = "ls",
        about = "List enrolled accounts with their live quota and active state."
    )]
    List(ListArgs),

    #[command(
        about = "Start any untouched seven-day quota countdowns.",
        long_about = concat!(
            "Find enrolled credentials whose quota reset time is still exactly seven days, ",
            "temporarily activate each one, and ask Codex for the current system GCC version ",
            "from the user's home directory. The original active credential is restored ",
            "afterward, including when a request fails.",
        )
    )]
    Tickle,

    #[command(about = "Activate the next usable enrolled account.")]
    Next,

    #[command(about = "Activate an enrolled account.")]
    Activate(AccountArgs),

    #[command(
        about = "Enroll an account through an isolated Codex login.",
        long_about = concat!(
            "Enroll an account through an isolated Codex login. If Codex is already using this ",
            "email, Kai imports the current credential without opening a browser. Kai ",
            "automatically uses device-code authentication in SSH, CI, and headless Linux ",
            "sessions; use --browser-auth or --device-auth to override detection. The new ",
            "account is activated when no account is active or the managed active account has ",
            "no remaining quota. For an already-enrolled account, --force runs a fresh isolated ",
            "login and safely replaces its credential.",
        )
    )]
    Add(AddArgs),

    #[command(
        about = "Find and reauthenticate broken enrolled credentials.",
        long_about = concat!(
            "Check every enrolled account and reauthenticate credentials that are invalid or ",
            "rejected by the Codex quota service. Each replacement is imported through an isolated ",
            "Codex login and must match the enrolled email and account/workspace ID. Repairs run ",
            "one at a time and wait for Enter before opening each account's sign-in.",
        )
    )]
    Fix(FixArgs),

    #[command(about = "Remove an enrolled account.")]
    Remove(RemoveArgs),
}

#[derive(Debug, Args)]
pub struct ListArgs {
    /// Emit stable machine-readable output without terminal styling.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct AccountArgs {
    /// Email address of the enrolled Codex account.
    #[arg(value_name = "EMAIL")]
    pub email: String,
}

#[derive(Debug, Args)]
pub struct AuthFlowArgs {
    /// Force Codex's device-code authentication flow.
    #[arg(long, conflicts_with = "browser_auth")]
    pub device_auth: bool,

    /// Use Codex's browser authentication flow, overriding environment detection.
    #[arg(long, conflicts_with = "device_auth")]
    pub browser_auth: bool,
}

#[derive(Debug, Args)]
pub struct AddArgs {
    /// Email address expected from the completed Codex login.
    #[arg(value_name = "EMAIL")]
    pub email: String,

    #[command(flatten)]
    pub auth: AuthFlowArgs,

    /// Activate after enrollment even if the current account has remaining quota.
    #[arg(long)]
    pub activate: bool,

    /// Reauthenticate and replace the credential if the account is already enrolled.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct FixArgs {
    #[command(flatten)]
    pub auth: AuthFlowArgs,
}

#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Email address of the enrolled Codex account.
    #[arg(value_name = "EMAIL")]
    pub email: String,

    /// Skip the destructive confirmation prompt.
    #[arg(short = 'y', long)]
    pub yes: bool,
}

enum LiveAuth {
    Absent,
    Present(Credential),
    Invalid(anyhow::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuotaAvailability {
    Remaining,
    Resettable,
    Exhausted,
    Unknown,
    Unusable,
}

#[derive(Debug)]
struct NoUsableQuota(String);

impl fmt::Display for NoUsableQuota {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NoUsableQuota {}

/// The account selected by one quota rotation, together with whether its quota was
/// positively confirmed.  Supervised recovery uses this to decide whether the selected
/// credential should also become the systemwide credential.
#[derive(Debug)]
struct NextSelection {
    target: Profile,
    changed: bool,
    target_has_remaining_quota: bool,
}

fn resolve_sqlite_home(cwd: &Path, codex_home: &Path, configured: Option<&str>) -> PathBuf {
    configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            }
        })
        .unwrap_or_else(|| codex_home.to_owned())
}

#[derive(Default)]
pub(crate) struct QuotaRecovery {
    source_paths: Option<RuntimePaths>,
    active_auth_file: Option<PathBuf>,
    sqlite_home: Option<PathBuf>,
}

impl QuotaRecovery {
    pub(crate) fn prepare(&mut self, cwd: &Path) -> Result<()> {
        if self.source_paths.is_some() {
            return Ok(());
        }
        let _invocation_lock = capulus::acquire("kai-cred", true)?;
        let store = open_store()?;
        let source_paths = store.paths().clone();
        let configured_sqlite_home = env::var("CODEX_SQLITE_HOME").ok();
        let sqlite_home = resolve_sqlite_home(
            cwd,
            &source_paths.codex_home,
            configured_sqlite_home.as_deref(),
        );
        rollout::normalize_rollout_paths(
            &sqlite_home,
            &source_paths.credentials_home,
            &source_paths.codex_home,
        )
        .context("could not repair stale supervised Codex rollout paths")?;
        let active_auth_file = load_live_strict(&store)?
            .and_then(|credential| managed_profile(&store, &credential))
            .map(|profile| store.profile_auth_path(profile));
        self.source_paths = Some(source_paths);
        self.active_auth_file = active_auth_file;
        self.sqlite_home = Some(sqlite_home);
        Ok(())
    }

    pub(crate) fn codex_home(&self) -> Result<&Path> {
        Ok(&self
            .source_paths
            .as_ref()
            .context("quota recovery was not prepared")?
            .codex_home)
    }

    /// The canonical enrolled auth file used by a supervised downstream child, if there is a
    /// managed active credential. All non-auth Codex state remains under codex_home.
    pub(crate) fn auth_file(&self) -> Option<PathBuf> {
        self.active_auth_file.clone()
    }

    pub(crate) fn sqlite_home(&self) -> Result<&Path> {
        self.sqlite_home
            .as_deref()
            .context("quota recovery was not prepared")
    }

    pub(crate) fn rotate(&mut self) -> Result<AccountRotation> {
        classify_account_rotation(credential_runtime()?.block_on(self.rotate_async()))
    }

    async fn rotate_async(&mut self) -> Result<PathBuf> {
        let _invocation_lock = capulus::acquire("kai-cred", true)?;
        let mut store = self.open_store()?;
        let current_profile = self.active_auth_file.as_ref().and_then(|auth_file| {
            store
                .profiles()
                .iter()
                .find(|profile| store.profile_auth_path(profile) == *auth_file)
                .cloned()
        });
        let selection = choose_next_selection_from(&mut store, current_profile.as_ref()).await?;
        // Keep the systemwide promotion in this same critical section as the canonical path switch.
        self.promote_systemwide_if_exhausted(&selection).await?;
        let auth_file = store.profile_auth_path(&selection.target);
        self.active_auth_file = Some(auth_file.clone());
        Ok(auth_file)
    }

    /// A supervised run normally changes only the selected canonical auth file. When recovery
    /// found a confirmed-capacity account, also move the user's systemwide account if that account is
    /// currently exhausted.  This runs from [`Self::rotate_async`], while the `kai-cred`
    /// invocation lock is held, so the live credential cannot be switched by another Kai
    /// credential operation between the quota check and the install.
    async fn promote_systemwide_if_exhausted(&self, selection: &NextSelection) -> Result<()> {
        if !selection.changed || !selection.target_has_remaining_quota {
            return Ok(());
        }

        let mut primary = self.open_primary_store()?;
        let live = match load_live(&primary) {
            LiveAuth::Present(credential) => credential,
            LiveAuth::Absent | LiveAuth::Invalid(_) => return Ok(()),
        };
        let active = match managed_profile(&primary, &live).cloned() {
            Some(profile) => profile,
            None => return Ok(()),
        };
        if active.account_id == selection.target.account_id {
            return Ok(());
        }

        let active_quota = match fetch_credential_quota(&primary, &live).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                capulus::ui::warn(&format!(
                    "Could not check whether {} has remaining quota; leaving it active: {err:#}",
                    active.email
                ));
                return Ok(());
            }
        };
        if active_quota.remaining_percent > 0.0 {
            return Ok(());
        }

        // Re-read the live credential after the quota worker finishes.  A non-Kai Codex process
        // may have changed it while the isolated request was in flight; never overwrite that
        // newer account based on the stale account's quota result.
        match load_live(&primary) {
            LiveAuth::Present(credential) if credential.facts.account_id == active.account_id => {}
            LiveAuth::Absent | LiveAuth::Invalid(_) | LiveAuth::Present(_) => return Ok(()),
        }

        let target = match primary
            .find_profile_by_account(&selection.target.account_id)
            .cloned()
        {
            Some(profile) => profile,
            None => return Ok(()),
        };
        activate(&mut primary, &target)?;
        Ok(())
    }

    fn open_store(&self) -> Result<Store> {
        let source_paths = self
            .source_paths
            .as_ref()
            .context("quota recovery was not prepared")?;
        ensure_codex_uses_file_credentials(source_paths)?;
        Store::open(source_paths.clone())
    }

    fn open_primary_store(&self) -> Result<Store> {
        let source_paths = self
            .source_paths
            .as_ref()
            .context("quota recovery was not prepared")?;
        ensure_codex_uses_file_credentials(source_paths)?;
        Store::open(source_paths.clone())
    }
}

fn classify_account_rotation(result: Result<PathBuf>) -> Result<AccountRotation> {
    match result {
        Ok(auth_file) => Ok(AccountRotation::Rotated { auth_file }),
        Err(error) if error.downcast_ref::<NoUsableQuota>().is_some() => {
            Ok(AccountRotation::NoQuota(format!("{error:#}")))
        }
        Err(error) => Err(error),
    }
}

pub fn run(command: CredCommand) -> Result<()> {
    credential_runtime()?.block_on(run_async(command))
}

async fn run_async(command: CredCommand) -> Result<()> {
    let _invocation_lock = capulus::acquire("kai-cred", true)?;
    let mut store = open_store()?;
    match command {
        CredCommand::List(args) => cmd_list(&store, args).await,
        CredCommand::Tickle => cmd_tickle(&mut store).await,
        CredCommand::Next => cmd_next(&mut store).await,
        CredCommand::Activate(args) => cmd_activate(&mut store, &args.email),
        CredCommand::Add(args) => cmd_add(&mut store, args).await,
        CredCommand::Fix(args) => cmd_fix(&mut store, args).await,
        CredCommand::Remove(args) => cmd_remove(&mut store, args),
    }
}

fn credential_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("could not initialize the credential runtime")
}

fn open_store() -> Result<Store> {
    let paths = RuntimePaths::from_env()?;
    ensure_codex_uses_file_credentials(&paths)?;
    let store = Store::open(paths)?;
    store.ensure_canonical_active()?;
    Ok(store)
}

async fn cmd_list(store: &Store, args: ListArgs) -> Result<()> {
    let live = load_live(store);
    let active_profile = match &live {
        LiveAuth::Present(credential) => managed_profile(store, credential),
        _ => None,
    };
    let active_id = active_profile.map(|profile| profile.id.as_str());
    let active_index = active_profile.and_then(|active| {
        store
            .profiles()
            .iter()
            .position(|profile| profile.id == active.id)
    });
    let can_select_next = matches!(&live, LiveAuth::Absent) || active_profile.is_some();
    let quota_client = quota::Client::new(store.paths()).map_err(|err| format!("{err:#}"));
    let mut quota_tasks = tokio::task::JoinSet::new();
    let mut accounts = Vec::with_capacity(store.profiles().len());
    for profile in store.profiles() {
        let active = active_id == Some(profile.id.as_str());
        let credential = if active {
            match &live {
                LiveAuth::Present(credential) => {
                    Credential::from_bytes(credential.as_bytes().to_vec())
                }
                _ => unreachable!(),
            }
        } else {
            store.credential(profile)
        };
        let (mut account, credential) = match credential {
            Ok(credential) => {
                let facts = credential.facts.clone();
                (
                    ready_account_view(profile, active, facts, QuotaStatus::Loading),
                    Some(credential),
                )
            }
            Err(err) => (
                AccountView {
                    email: profile.email.clone(),
                    active,
                    plan: None,
                    last_refresh: None,
                    status: AccountStatus::Invalid {
                        error: format!("{err:#}"),
                    },
                    quota: QuotaStatus::Unavailable {
                        error: "credential is invalid".to_owned(),
                        authentication_required: true,
                    },
                },
                None,
            ),
        };
        let index = accounts.len();
        if let Some(credential) = credential {
            match &quota_client {
                Ok(client) => {
                    let client = client.clone();
                    let auth_file = store.profile_auth_path(profile);
                    quota_tasks
                        .spawn(async move { (index, client.fetch(credential, auth_file).await) });
                }
                Err(error) => {
                    account.quota = QuotaStatus::Unavailable {
                        error: error.clone(),
                        authentication_required: false,
                    };
                }
            }
        }
        accounts.push(account);
    }
    let active = active_profile.map(|profile| profile.email.clone());
    let mut view = ListView {
        active,
        next: None,
        accounts,
    };
    let live_list = if args.json {
        None
    } else {
        ui::LiveList::start(&view)
    };
    while let Some(result) = quota_tasks.join_next().await {
        let (index, outcome) = result.context("a quota lookup task stopped unexpectedly")?;
        persist_quota_credential(store, &store.profiles()[index], &outcome)?;
        view.accounts[index].last_refresh = outcome.credential.facts.last_refresh.clone();
        view.accounts[index].set_quota(outcome.snapshot);
        if let Some(live_list) = &live_list {
            live_list.update(index, &view.accounts[index]);
        }
    }
    if can_select_next {
        let order = rotation_order(store.profiles().len(), active_index);
        view.next = preferred_rotation_index(order.iter().copied().map(|index| {
            (
                index,
                quota_status_availability(&view.accounts[index].quota),
            )
        }))
        .map(|index| store.profiles()[index].email.clone());
    }
    if let Some(live_list) = live_list {
        live_list.finish(&view)?;
    } else {
        ui::print_list(&view, args.json)?;
    }

    if !args.json {
        match live {
            LiveAuth::Present(credential) if active_profile.is_none() => {
                capulus::ui::warn(&format!(
                    concat!(
                        "Codex is signed in as {}, but that account is not enrolled. ",
                        "Run `kai cred add {}` to preserve it before switching.",
                    ),
                    credential.facts.email, credential.facts.email
                ))
            }
            LiveAuth::Invalid(err) => capulus::ui::warn(&format!(
                "The active Codex credential could not be read: {err:#}"
            )),
            _ => {}
        }
    }
    Ok(())
}

async fn cmd_tickle(store: &mut Store) -> Result<()> {
    if store.profiles().is_empty() {
        bail!("no accounts are enrolled; run `kai cred add <email>` first");
    }

    let live = load_live_strict(store)?;
    let original_active = match &live {
        Some(credential) => {
            let profile = require_managed_profile(store, credential)?.clone();
            Some(profile)
        }
        None => None,
    };
    let profile_indices = (0..store.profiles().len()).collect::<Vec<_>>();
    capulus::ui::stage("Checking enrolled account quotas");
    let checks = fetch_profile_quotas(store, &profile_indices, live.as_ref()).await?;
    let now = chrono::Utc::now().timestamp();
    let mut targets = Vec::new();
    for (index, result) in checks {
        match result {
            Ok(snapshot) if quota::countdown_has_not_started(&snapshot, now) => {
                targets.push(store.profiles()[index].clone());
            }
            Ok(_) => {}
            Err(err) => capulus::ui::warn(&format!(
                "Could not retrieve quota for {}: {err:#}",
                store.profiles()[index].email
            )),
        }
    }

    if targets.is_empty() {
        capulus::ui::success("No enrolled credentials have an untouched seven-day countdown.");
        return Ok(());
    }

    let codex = which::which("codex").context("could not find `codex` on PATH")?;
    let home = capulus::paths::home_dir()
        .context("could not determine the current user's home directory")?;
    let noun = if targets.len() == 1 {
        "credential"
    } else {
        "credentials"
    };
    capulus::ui::stage(&format!(
        "Starting {} untouched seven-day quota {noun}",
        targets.len()
    ));

    let tickle_result = tickle_profiles(store, &targets, &codex, &home);
    let restore_result = restore_original_active(store, original_active.as_ref());
    match (tickle_result, restore_result) {
        (Ok(()), Ok(())) => {
            capulus::ui::success(&format!(
                "Tickled {} {noun} and restored the original Codex credential.",
                targets.len()
            ));
            Ok(())
        }
        (Err(err), Ok(())) => Err(err),
        (Ok(()), Err(restore_err)) => Err(restore_err)
            .context("Codex requests completed, but the original credential was not restored"),
        (Err(tickle_err), Err(restore_err)) => bail!(
            "{tickle_err:#}; additionally, could not restore the original credential: \
             {restore_err:#}"
        ),
    }
}

fn tickle_profiles(
    store: &mut Store,
    targets: &[Profile],
    codex: &Path,
    home: &Path,
) -> Result<()> {
    let mut failures = Vec::new();
    for target in targets {
        activate(store, target)
            .with_context(|| format!("could not temporarily activate {}", target.email))?;
        capulus::ui::detail(&format!("Tickling {}...", target.email));
        if let Err(err) = run_codex_tickle(codex, home) {
            capulus::ui::warn(&format!("Could not tickle {}: {err:#}", target.email));
            failures.push(target.email.clone());
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("Codex request failed for {}", failures.join(", "))
    }
}

fn run_codex_tickle(codex: &Path, home: &Path) -> Result<()> {
    let output = Command::new(codex)
        .args([
            "exec",
            "--skip-git-repo-check",
            "--ephemeral",
            "What is the current system `gcc` version? (Reply with only the version number.)",
        ])
        .env_remove("CODEX_AUTH_FILE")
        .current_dir(home)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("could not start {}", codex.display()))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        bail!("Codex exited with {}", output.status);
    }
    bail!("Codex exited with {}: {stderr}", output.status)
}

fn ready_account_view(
    profile: &Profile,
    active: bool,
    facts: CredentialFacts,
    quota: QuotaStatus,
) -> AccountView {
    AccountView {
        email: profile.email.clone(),
        active,
        plan: facts.plan,
        last_refresh: facts.last_refresh,
        status: AccountStatus::Ready,
        quota,
    }
}

async fn cmd_next(store: &mut Store) -> Result<()> {
    let selection = choose_next_selection_from(store, None).await?;
    let changed = activate(store, &selection.target)?;
    if changed {
        capulus::ui::success(&format!("Codex is now using {}.", selection.target.email));
        if store.uses_primary_codex_home() {
            warn_running_codex();
        }
    }
    Ok(())
}

async fn choose_next_selection_from(
    store: &mut Store,
    selected_active: Option<&Profile>,
) -> Result<NextSelection> {
    if store.profiles().is_empty() {
        return Err(NoUsableQuota(
            "no accounts are enrolled; run `kai cred add <email>` first".to_owned(),
        )
        .into());
    }
    let (live, active_index) = match selected_active {
        Some(active) => {
            let active_index = store
                .profiles()
                .iter()
                .position(|profile| profile.id == active.id)
                .context("active profile disappeared while selecting the next account")?;
            let live = store.credential(active)?;
            (Some(live), Some(active_index))
        }
        None => {
            let live = load_live_strict(store)?;
            let active_index = match &live {
                None => None,
                Some(credential) => {
                    let active = require_managed_profile(store, credential)?;
                    Some(
                        store
                            .profiles()
                            .iter()
                            .position(|profile| profile.id == active.id)
                            .context(
                                "active profile disappeared while selecting the next account",
                            )?,
                    )
                }
            };
            (live, active_index)
        }
    };
    let order = rotation_order(store.profiles().len(), active_index);
    capulus::ui::stage("Checking enrolled account quotas");
    let checks = fetch_profile_quotas(store, &order, live.as_ref()).await?;
    let candidates = checks
        .iter()
        .map(|(index, result)| (*index, quota_result_availability(result)));
    let target_index = preferred_rotation_index(candidates);
    let Some(target_index) = target_index else {
        let scope = if active_index.is_some() && store.profiles().len() > 1 {
            "no other enrolled account has remaining Codex quota or usable reset credits"
        } else {
            "no enrolled account has remaining Codex quota or usable reset credits"
        };
        return Err(NoUsableQuota(format!(
            "{scope}; checked: {}",
            quota_check_summary(store, &checks)
        ))
        .into());
    };
    let target = store.profiles()[target_index].clone();
    let quota = checks
        .into_iter()
        .find_map(|(index, result)| (index == target_index).then_some(result))
        .context("selected account quota result disappeared")?;
    let changed = active_index.is_none_or(|index| store.profiles()[index].id != target.id);
    let target_has_remaining_quota = quota
        .as_ref()
        .is_ok_and(|snapshot| snapshot.remaining_percent > 0.0);
    match &quota {
        Ok(snapshot) => {
            ui::print_quota(snapshot);
            if snapshot.remaining_percent <= 0.0
                && let Some(reset_credits) = &snapshot.rate_limit_reset_credits
            {
                ui::print_reset_credit_notice(&target.email, reset_credits);
            }
        }
        Err(err) => capulus::ui::warn(&format!(
            "Could not retrieve quota for {}: {err:#}",
            target.email
        )),
    }
    Ok(NextSelection {
        target,
        changed,
        target_has_remaining_quota,
    })
}

async fn fetch_profile_quotas(
    store: &Store,
    profile_indices: &[usize],
    live: Option<&Credential>,
) -> Result<Vec<(usize, Result<quota::Snapshot>)>> {
    let client = match quota::Client::new(store.paths()) {
        Ok(client) => client,
        Err(err) => {
            let message = format!("{err:#}");
            return Ok(profile_indices
                .iter()
                .map(|index| (*index, Err(anyhow!(message.clone()))))
                .collect());
        }
    };
    let mut results = std::iter::repeat_with(|| None)
        .take(profile_indices.len())
        .collect::<Vec<Option<Result<quota::Snapshot>>>>();
    let mut tasks = tokio::task::JoinSet::new();
    for (slot, index) in profile_indices.iter().copied().enumerate() {
        let profile = &store.profiles()[index];
        let credential = match live {
            Some(credential) if credential.facts.account_id == profile.account_id => {
                Credential::from_bytes(credential.as_bytes().to_vec())?
            }
            _ => store.credential(profile)?,
        };
        let auth_file = store.profile_auth_path(profile);
        let client = client.clone();
        tasks.spawn(async move { (slot, client.fetch(credential, auth_file).await) });
    }
    while let Some(result) = tasks.join_next().await {
        let (slot, outcome) = result.context("a quota lookup task stopped unexpectedly")?;
        let profile_index = profile_indices[slot];
        persist_quota_credential(store, &store.profiles()[profile_index], &outcome)?;
        results[slot] = Some(outcome.snapshot);
    }
    Ok(profile_indices
        .iter()
        .copied()
        .zip(results.into_iter().map(|result| {
            result.expect("every quota result is set directly or by a completed task")
        }))
        .collect())
}

/// The quota worker already writes the canonical profile through the downstream auth manager.
/// Keep this hook as a consistency check, but never copy an outcome back over a newer canonical
/// credential after the app-server exits.
fn persist_quota_credential(
    store: &Store,
    profile: &Profile,
    outcome: &quota::Outcome,
) -> Result<()> {
    let current = store.credential(profile)?;
    if outcome.credential_changed() && outcome.source_matches(&current) {
        bail!(
            "Codex quota worker changed {} credentials without persisting the canonical profile",
            profile.email
        );
    }
    Ok(())
}

fn rotation_order(profile_count: usize, active_index: Option<usize>) -> Vec<usize> {
    if profile_count == 0 {
        return Vec::new();
    }
    match active_index {
        None => (0..profile_count).collect(),
        Some(active_index) if profile_count == 1 => vec![active_index],
        Some(active_index) => (1..profile_count)
            .map(|offset| (active_index + offset) % profile_count)
            .collect(),
    }
}

fn preferred_rotation_index(
    candidates: impl IntoIterator<Item = (usize, QuotaAvailability)>,
) -> Option<usize> {
    let mut first_resettable = None;
    let mut first_unknown = None;
    for (index, availability) in candidates {
        match availability {
            QuotaAvailability::Remaining => return Some(index),
            QuotaAvailability::Resettable => {
                first_resettable.get_or_insert(index);
            }
            QuotaAvailability::Unknown => {
                first_unknown.get_or_insert(index);
            }
            QuotaAvailability::Exhausted | QuotaAvailability::Unusable => {}
        }
    }
    first_resettable.or(first_unknown)
}

fn quota_result_availability(result: &Result<quota::Snapshot>) -> QuotaAvailability {
    match result {
        Ok(snapshot) if snapshot.remaining_percent > 0.0 => QuotaAvailability::Remaining,
        Ok(snapshot) if snapshot.rate_limit_reset_credits.is_some() => {
            QuotaAvailability::Resettable
        }
        Ok(_) => QuotaAvailability::Exhausted,
        Err(err) if quota::requires_authentication(err) => QuotaAvailability::Unusable,
        Err(_) => QuotaAvailability::Unknown,
    }
}

fn quota_check_summary(store: &Store, checks: &[(usize, Result<quota::Snapshot>)]) -> String {
    checks
        .iter()
        .map(|(index, result)| {
            let status = match result {
                Ok(snapshot) if snapshot.remaining_percent > 0.0 => "quota remaining".to_owned(),
                Ok(snapshot) if snapshot.rate_limit_reset_credits.is_some() => {
                    "reset credit available".to_owned()
                }
                Ok(_) => "quota exhausted".to_owned(),
                Err(err) if quota::requires_authentication(err) => {
                    format!("authentication failed: {err:#}")
                }
                Err(err) => format!("quota unavailable: {err:#}"),
            };
            format!("{} ({status})", store.profiles()[*index].email)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn quota_status_availability(status: &QuotaStatus) -> QuotaAvailability {
    match status {
        QuotaStatus::Available { snapshot } if snapshot.remaining_percent > 0.0 => {
            QuotaAvailability::Remaining
        }
        QuotaStatus::Available { snapshot } if snapshot.rate_limit_reset_credits.is_some() => {
            QuotaAvailability::Resettable
        }
        QuotaStatus::Available { .. } => QuotaAvailability::Exhausted,
        QuotaStatus::Unavailable {
            authentication_required: true,
            ..
        } => QuotaAvailability::Unusable,
        QuotaStatus::Loading | QuotaStatus::Unavailable { .. } => QuotaAvailability::Unknown,
    }
}

fn cmd_activate(store: &mut Store, email: &str) -> Result<()> {
    validate_email(email)?;
    let target = store
        .find_profile(email)
        .with_context(|| format!("{email} is not enrolled; run `kai cred add {email}` first"))?
        .clone();
    let changed = activate(store, &target)?;
    if changed {
        capulus::ui::success(&format!("Codex is now using {}.", target.email));
        warn_running_codex();
    } else {
        capulus::ui::success(&format!("{} is already active.", target.email));
    }
    Ok(())
}

async fn cmd_add(store: &mut Store, args: AddArgs) -> Result<()> {
    let expected = args.email.trim();
    validate_email(expected)?;
    if let Some(target) = store.find_profile(expected).cloned() {
        if !args.force {
            bail!("{expected} is already enrolled; rerun with `--force` to reauthenticate it");
        }
        return repair_profile(store, &target, args.auth.auth_preference(), args.activate);
    }

    let live = load_live(store);
    if let LiveAuth::Present(credential) = &live
        && credential.matches_email(expected)
    {
        let profile = store.insert_profile(credential)?;
        store.activate_profile(&profile)?;
        capulus::ui::success(&format!(
            "Imported the active Codex account {}.",
            profile.email
        ));
        return Ok(());
    }

    // Only a currently managed account may be replaced automatically. Capture this before
    // inserting the new profile so enrollment cannot make an unrelated live credential appear
    // managed merely because it happens to share an account ID.
    let managed_account_before_add = match &live {
        LiveAuth::Present(credential) if managed_profile(store, credential).is_some() => {
            Some(credential.facts.account_id.clone())
        }
        _ => None,
    };

    if args.activate {
        ensure_live_can_be_replaced(store, &live)?;
    }
    let credential = enroll::run(store.paths(), expected, args.auth.auth_preference())?;
    let profile = store.insert_profile(&credential)?;
    let active_for_quota = if !args.activate {
        match (managed_account_before_add.as_deref(), load_live(store)) {
            (Some(expected_account_id), LiveAuth::Present(active))
                if active.facts.account_id == expected_account_id =>
            {
                Some(active)
            }
            _ => None,
        }
    } else {
        None
    };
    let exhausted_active = if let Some(active) = active_for_quota {
        match fetch_credential_quota(store, &active).await {
            Ok(snapshot) if snapshot.remaining_percent <= 0.0 => Some(active.facts.email.clone()),
            Ok(_) => None,
            Err(err) => {
                capulus::ui::warn(&format!(
                    "Could not check whether {} has remaining quota; leaving it active: {err:#}",
                    active.facts.email
                ));
                None
            }
        }
    } else {
        None
    };
    let activate_after_add =
        args.activate || matches!(&live, LiveAuth::Absent) || exhausted_active.is_some();
    if activate_after_add {
        activate(store, &profile)?;
        if let Some(active_email) = exhausted_active {
            capulus::ui::success(&format!(
                "Enrolled and activated {} because {} has no remaining quota.",
                profile.email, active_email
            ));
        } else {
            capulus::ui::success(&format!("Enrolled and activated {}.", profile.email));
        }
        warn_running_codex();
    } else {
        capulus::ui::success(&format!("Enrolled {}.", profile.email));
        match live {
            LiveAuth::Present(active) if managed_profile(store, &active).is_some() => {
                capulus::ui::detail(&format!(
                    "{} remains active. Run `kai cred activate {}` when ready.",
                    active.facts.email, profile.email
                ))
            }
            LiveAuth::Present(active) => capulus::ui::detail(&format!(
                concat!(
                    "{} remains active but is not enrolled. Run `kai cred add {}` before switching ",
                    "so its latest refresh token is preserved.",
                ),
                active.facts.email, active.facts.email
            )),
            LiveAuth::Invalid(_) => capulus::ui::detail(&format!(
                "Run `kai cred activate {}` after resolving the unreadable active Codex credential.",
                profile.email
            )),
            LiveAuth::Absent => unreachable!(),
        }
    }
    Ok(())
}

async fn cmd_fix(store: &mut Store, args: FixArgs) -> Result<()> {
    if store.profiles().is_empty() {
        bail!("no accounts are enrolled; run `kai cred add <email>` first");
    }

    capulus::ui::stage("Checking enrolled account credentials");
    let live = load_live(store);
    let unreadable_active = if let LiveAuth::Invalid(err) = &live {
        capulus::ui::warn(&format!(
            "The active Codex credential is unreadable and cannot be matched to an enrolled account: {err:#}"
        ));
        Some(format!("{err:#}"))
    } else {
        None
    };
    let mut needs_repair = vec![false; store.profiles().len()];
    let mut indeterminate = 0;
    let mut check_indices = Vec::new();
    for (index, profile) in store.profiles().iter().enumerate() {
        let structurally_valid = matches!(
            &live,
            LiveAuth::Present(credential) if credential.facts.account_id == profile.account_id
        ) || store.credential(profile).is_ok();
        if structurally_valid {
            check_indices.push(index);
        } else {
            needs_repair[index] = true;
        }
    }
    let live_credential = match &live {
        LiveAuth::Present(credential) => Some(credential),
        LiveAuth::Absent | LiveAuth::Invalid(_) => None,
    };
    let checks = fetch_profile_quotas(store, &check_indices, live_credential).await?;
    for (index, result) in checks {
        if let Err(err) = result {
            if quota::requires_authentication(&err) {
                needs_repair[index] = true;
            } else {
                indeterminate += 1;
                capulus::ui::warn(&format!(
                    "Could not determine whether {} needs authentication repair: {err:#}",
                    store.profiles()[index].email
                ));
            }
        }
    }

    let targets = store
        .profiles()
        .iter()
        .zip(needs_repair)
        .filter(|(_, needs_repair)| *needs_repair)
        .map(|(profile, _)| profile.clone())
        .collect::<Vec<_>>();
    if targets.is_empty() {
        if indeterminate == 0 && unreadable_active.is_none() {
            capulus::ui::success("All enrolled account credentials appear usable.");
        } else {
            capulus::ui::detail("No credentials were repaired.");
        }
    } else {
        let noun = if targets.len() == 1 {
            "credential"
        } else {
            "credentials"
        };
        capulus::ui::detail(&format!("Repairing {} broken {noun}.", targets.len()));
        let preference = args.auth.auth_preference();
        for target in targets {
            confirm_fix_account(&target.email)?;
            repair_profile(store, &target, preference, false)?;
        }
    }
    if let Some(err) = unreadable_active {
        bail!(
            concat!(
                "the active Codex credential remains unreadable and cannot be repaired ",
                "automatically ({}); run `kai cred add <email> --force --activate` for the ",
                "intended active account",
            ),
            err
        );
    }
    Ok(())
}

fn confirm_fix_account(email: &str) -> Result<()> {
    eprint!(
        "Press Enter to open sign-in for {email}; select this account in the browser (Ctrl-C to stop): "
    );
    io::stderr()
        .flush()
        .context("could not display the credential repair confirmation")?;

    let mut confirmation = String::new();
    let bytes_read = io::stdin()
        .read_line(&mut confirmation)
        .context("could not read the credential repair confirmation")?;
    if bytes_read == 0 {
        bail!("confirmation ended before sign-in started for {email}");
    }
    if !io::stdin().is_terminal() {
        eprintln!();
    }
    Ok(())
}

fn repair_profile(
    store: &mut Store,
    target: &Profile,
    auth_preference: enroll::AuthPreference,
    activate_target: bool,
) -> Result<()> {
    let live = load_live(store);
    let target_is_active = matches!(
        &live,
        LiveAuth::Present(credential) if credential.facts.account_id == target.account_id
    );
    if activate_target && !target_is_active {
        ensure_live_can_be_replaced(store, &live)?;
    }
    let credential = enroll::run(store.paths(), &target.email, auth_preference)?;
    if credential.facts.account_id != target.account_id {
        bail!(
            concat!(
                "signed in as {}, but its account/workspace ID does not match the enrolled profile; ",
                "the new credential was discarded and no credentials were changed",
            ),
            credential.facts.email
        );
    }

    store.sync_profile(target, &credential)?;
    if target_is_active {
        capulus::ui::success(&format!(
            "Updated credentials for {} and refreshed the active Codex credential.",
            target.email
        ));
        warn_running_codex();
    } else if activate_target {
        activate(store, target)?;
        capulus::ui::success(&format!(
            "Updated credentials for {} and activated it.",
            target.email
        ));
        warn_running_codex();
    } else {
        capulus::ui::success(&format!("Updated credentials for {}.", target.email));
    }
    Ok(())
}

impl AuthFlowArgs {
    fn auth_preference(&self) -> enroll::AuthPreference {
        if self.device_auth {
            enroll::AuthPreference::Device
        } else if self.browser_auth {
            enroll::AuthPreference::Browser
        } else {
            enroll::AuthPreference::Auto
        }
    }
}

async fn fetch_credential_quota(store: &Store, credential: &Credential) -> Result<quota::Snapshot> {
    let index = store
        .profiles()
        .iter()
        .position(|profile| profile.account_id == credential.facts.account_id)
        .context("active credential is not enrolled and cannot be checked safely")?;
    fetch_profile_quotas(store, &[index], Some(credential))
        .await?
        .pop()
        .context("quota result disappeared")?
        .1
}

fn cmd_remove(store: &mut Store, args: RemoveArgs) -> Result<()> {
    validate_email(&args.email)?;
    let target = store
        .find_profile(&args.email)
        .with_context(|| format!("{} is not enrolled", args.email))?
        .clone();
    if !args.yes
        && !capulus::ui::prompt_confirm(
            &format!("Remove {} from Kai's credential vault?", target.email),
            false,
        )?
    {
        capulus::ui::detail("No changes made.");
        return Ok(());
    }

    let live = load_live(store);
    let active = match &live {
        LiveAuth::Present(credential) => managed_profile(store, credential),
        LiveAuth::Invalid(err) => {
            bail!(
                "cannot safely remove an account while the active Codex credential is unreadable: {err:#}"
            )
        }
        LiveAuth::Absent => None,
    };
    let target_is_active = active.is_some_and(|profile| profile.id == target.id);

    if target_is_active {
        if let Some(successor) = next_profile_excluding(store, &target) {
            let successor = successor.clone();
            activate(store, &successor)?;
            store.remove_profile(&target.id)?;
            capulus::ui::success(&format!(
                "Removed {}. Codex is now using {}.",
                target.email, successor.email
            ));
            warn_running_codex();
            return Ok(());
        }
        store.remove_active()?;
    }

    store.remove_profile(&target.id)?;
    capulus::ui::success(&format!("Removed {}.", target.email));
    if target_is_active {
        capulus::ui::detail("No accounts remain; Codex is locally signed out.");
    }
    Ok(())
}

fn activate(store: &mut Store, target: &Profile) -> Result<bool> {
    if let Some(live) = load_live_strict(store)? {
        let active = require_managed_profile(store, &live)?;
        if active.id == target.id {
            return Ok(false);
        }
    }
    install_profile(store, target)?;
    Ok(true)
}

fn install_profile(store: &Store, target: &Profile) -> Result<()> {
    store.activate_profile(target)?;
    let installed = store.credential(target)?;
    if installed.facts.account_id != target.account_id {
        bail!(
            "credential activation verification failed for {}",
            target.email
        );
    }
    Ok(())
}

fn restore_original_active(store: &Store, original: Option<&Profile>) -> Result<()> {
    match original {
        Some(profile) => install_profile(store, profile),
        None => store.remove_active(),
    }
}

fn load_live(store: &Store) -> LiveAuth {
    match fs::symlink_metadata(store.paths().active_auth()) {
        Ok(_) => match store.active_profile_path().and_then(|path| {
            let _lock = auth_lock::acquire(&path)?;
            Credential::read(&path)
        }) {
            Ok(credential) => LiveAuth::Present(credential),
            Err(err) => LiveAuth::Invalid(err),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => LiveAuth::Absent,
        Err(err) => LiveAuth::Invalid(err.into()),
    }
}

fn load_live_strict(store: &Store) -> Result<Option<Credential>> {
    match load_live(store) {
        LiveAuth::Absent => Ok(None),
        LiveAuth::Present(credential) => Ok(Some(credential)),
        LiveAuth::Invalid(err) => {
            Err(err).context("cannot safely replace the active Codex credential")
        }
    }
}

fn managed_profile<'a>(store: &'a Store, credential: &Credential) -> Option<&'a Profile> {
    store.find_profile_by_account(&credential.facts.account_id)
}

fn require_managed_profile<'a>(store: &'a Store, credential: &Credential) -> Result<&'a Profile> {
    if let Some(profile) = managed_profile(store, credential) {
        return Ok(profile);
    }
    if let Some(profile) = store.find_profile(&credential.facts.email) {
        bail!(
            concat!(
                "Codex is using {} with a different account/workspace ID than the enrolled ",
                "profile; remove and re-enroll it before switching",
            ),
            profile.email
        );
    }
    bail!(
        concat!(
            "Codex is using {}, which is not enrolled. Run `kai cred add {}` before switching ",
            "so its latest refresh token is preserved",
        ),
        credential.facts.email,
        credential.facts.email
    )
}

fn ensure_live_can_be_replaced(store: &Store, live: &LiveAuth) -> Result<()> {
    match live {
        LiveAuth::Absent => Ok(()),
        LiveAuth::Present(credential) => {
            require_managed_profile(store, credential)?;
            Ok(())
        }
        LiveAuth::Invalid(err) => bail!(
            "cannot activate a new account while the active Codex credential is unreadable: {err:#}"
        ),
    }
}

fn next_profile_excluding<'a>(store: &'a Store, removed: &Profile) -> Option<&'a Profile> {
    if store.profiles().len() <= 1 {
        return None;
    }
    let index = store
        .profiles()
        .iter()
        .position(|profile| profile.id == removed.id)?;
    store.profiles().get((index + 1) % store.profiles().len())
}

fn warn_running_codex() {
    if let Some(count) = running_codex_process_count()
        && count > 0
    {
        let noun = if count == 1 { "process" } else { "processes" };
        capulus::ui::warn(&format!(
            concat!(
                "{} running Codex {} may still hold the previous credential in memory; ",
                "restart them before continuing work.",
            ),
            count, noun,
        ));
    }
}

#[cfg(target_os = "linux")]
fn running_codex_process_count() -> Option<usize> {
    let current = std::process::id();
    let entries = fs::read_dir("/proc").ok()?;
    Some(
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u32>().ok())
            .filter(|pid| *pid != current)
            .filter(|pid| {
                fs::read_to_string(format!("/proc/{pid}/comm"))
                    .is_ok_and(|name| name.trim() == "codex")
            })
            .count(),
    )
}

#[cfg(not(target_os = "linux"))]
fn running_codex_process_count() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::auth::tests::auth_json;
    use super::*;

    fn setup() -> (tempfile::TempDir, Store) {
        let root = tempdir().unwrap();
        let paths =
            RuntimePaths::new(root.path().join("credentials"), root.path().join("codex")).unwrap();
        let store = Store::open(paths).unwrap();
        (root, store)
    }

    fn credential(email: &str, account: &str, refresh: &str) -> Credential {
        Credential::from_bytes(auth_json(email, account, "pro", 2_000_000_000, refresh)).unwrap()
    }

    #[test]
    fn supervised_sqlite_home_matches_codex_environment_resolution() {
        let root = tempdir().unwrap();
        let cwd = root.path().join("workspace");
        let codex_home = root.path().join("codex");
        let shared_home = root.path().join("shared-state");

        assert_eq!(
            resolve_sqlite_home(&cwd, &codex_home, Some(" state ")),
            cwd.join("state")
        );
        assert_eq!(
            resolve_sqlite_home(
                &cwd,
                &codex_home,
                Some(&format!(" {} ", shared_home.display())),
            ),
            shared_home
        );
        assert_eq!(
            resolve_sqlite_home(&cwd, &codex_home, Some("  ")),
            codex_home
        );
        assert_eq!(resolve_sqlite_home(&cwd, &codex_home, None), codex_home);
    }

    #[test]
    fn switching_reuses_the_canonical_profile_without_reconciliation() {
        let (_root, mut store) = setup();
        let alice = store
            .insert_profile(&credential("alice@example.com", "alice-id", "alice-old"))
            .unwrap();
        let bob = store
            .insert_profile(&credential("bob@example.com", "bob-id", "bob-token"))
            .unwrap();
        store.activate_profile(&alice).unwrap();
        store
            .write_active(&credential("alice@example.com", "alice-id", "alice-new"))
            .unwrap();

        assert!(activate(&mut store, &bob).unwrap());
        assert_eq!(
            store.credential(&alice).unwrap().as_bytes(),
            auth_json(
                "alice@example.com",
                "alice-id",
                "pro",
                2_000_000_000,
                "alice-new"
            )
        );
        assert!(activate(&mut store, &alice).unwrap());
        assert_eq!(
            Credential::read(&store.active_profile_path().unwrap())
                .unwrap()
                .facts
                .account_id,
            "alice-id"
        );
    }

    #[test]
    fn supervised_recovery_uses_the_profile_as_its_canonical_auth_file() {
        let (_root, mut store) = setup();
        let alice = store
            .insert_profile(&credential("alice@example.com", "alice-id", "alice-source"))
            .unwrap();
        assert!(activate(&mut store, &alice).unwrap());

        assert_eq!(
            store.active_profile_path().unwrap(),
            store.profile_auth_path(&alice)
        );
        let refreshed = credential("alice@example.com", "alice-id", "alice-refreshed");
        store.sync_profile(&alice, &refreshed).unwrap();
        assert_eq!(
            Credential::read(&store.active_profile_path().unwrap())
                .unwrap()
                .as_bytes(),
            refreshed.as_bytes()
        );
    }

    #[test]
    fn switching_refuses_to_overwrite_an_unenrolled_live_account() {
        let (_root, mut store) = setup();
        let bob = store
            .insert_profile(&credential("bob@example.com", "bob-id", "bob-token"))
            .unwrap();
        store
            .write_active(&credential(
                "outside@example.com",
                "outside-id",
                "outside-token",
            ))
            .unwrap();

        let error = activate(&mut store, &bob).unwrap_err();

        assert!(format!("{error:#}").contains("not enrolled"));
        assert_eq!(
            Credential::read(&store.active_profile_path().unwrap())
                .unwrap()
                .facts
                .account_id,
            "outside-id"
        );
    }

    #[test]
    fn rotation_order_starts_after_the_active_account_and_wraps() {
        assert_eq!(rotation_order(0, None), Vec::<usize>::new());
        assert_eq!(rotation_order(3, None), vec![0, 1, 2]);
        assert_eq!(rotation_order(3, Some(1)), vec![2, 0]);
        assert_eq!(rotation_order(1, Some(0)), vec![0]);
    }

    #[test]
    fn supervised_rotation_classifies_only_no_usable_quota() {
        assert_eq!(
            classify_account_rotation(Err(NoUsableQuota("none available".to_owned()).into()))
                .unwrap(),
            AccountRotation::NoQuota("none available".to_owned())
        );

        let error =
            classify_account_rotation(Err(anyhow!("could not read auth.json"))).unwrap_err();
        assert_eq!(error.to_string(), "could not read auth.json");
    }

    #[test]
    fn rotation_skips_exhausted_accounts_and_prefers_confirmed_capacity() {
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Exhausted),
                (2, QuotaAvailability::Remaining),
            ]),
            Some(2)
        );
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Unknown),
                (2, QuotaAvailability::Remaining),
            ]),
            Some(2)
        );
    }

    #[test]
    fn rotation_uses_reset_credits_after_remaining_quota_but_before_unknown_quota() {
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Resettable),
                (2, QuotaAvailability::Remaining),
            ]),
            Some(2)
        );
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Unknown),
                (2, QuotaAvailability::Resettable),
            ]),
            Some(2)
        );
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Unusable),
                (2, QuotaAvailability::Exhausted),
            ]),
            None
        );
    }

    #[test]
    fn rotation_falls_back_to_unknown_quota_but_never_to_known_exhaustion() {
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Exhausted),
                (2, QuotaAvailability::Unknown),
                (3, QuotaAvailability::Unknown),
            ]),
            Some(2)
        );
        assert_eq!(
            preferred_rotation_index([
                (1, QuotaAvailability::Exhausted),
                (2, QuotaAvailability::Exhausted),
            ]),
            None
        );
    }

    #[test]
    fn removing_the_active_profile_activates_its_successor() {
        let (_root, mut store) = setup();
        let alice = store
            .insert_profile(&credential("alice@example.com", "alice-id", "alice-token"))
            .unwrap();
        store
            .insert_profile(&credential("bob@example.com", "bob-id", "bob-token"))
            .unwrap();
        store
            .write_active(&credential("alice@example.com", "alice-id", "alice-live"))
            .unwrap();

        cmd_remove(
            &mut store,
            RemoveArgs {
                email: alice.email,
                yes: true,
            },
        )
        .unwrap();

        assert_eq!(store.profiles().len(), 1);
        assert_eq!(store.profiles()[0].email, "bob@example.com");
        assert_eq!(
            Credential::read(&store.active_profile_path().unwrap())
                .unwrap()
                .facts
                .account_id,
            "bob-id"
        );
    }
}
