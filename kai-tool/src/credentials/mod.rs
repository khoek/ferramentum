use std::fs;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

mod auth;
mod enroll;
mod paths;
mod store;
mod ui;

use self::auth::{Credential, validate_email};
use self::paths::RuntimePaths;
use self::store::{Profile, Store, ensure_codex_uses_file_credentials};
use self::ui::{AccountStatus, AccountView, ListView};

#[derive(Debug, Args)]
#[command(after_help = concat!(
    "Examples:\n",
    "  kai cred add personal@example.com\n",
    "  kai cred add work@example.com --device-auth\n",
    "  kai cred list\n",
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
        about = "List enrolled accounts and show which one Codex is using."
    )]
    List(ListArgs),

    #[command(about = "Activate the next enrolled account, wrapping at the end.")]
    Next,

    #[command(about = "Activate an enrolled account.")]
    Activate(AccountArgs),

    #[command(
        about = "Enroll an account through an isolated Codex login.",
        long_about = concat!(
            "Enroll an account through an isolated Codex login. If Codex is already using this ",
            "email, Kai imports the current credential without opening a browser.",
        )
    )]
    Add(AddArgs),

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
pub struct AddArgs {
    /// Email address expected from the completed Codex login.
    #[arg(value_name = "EMAIL")]
    pub email: String,

    /// Use Codex's device-code authentication flow.
    #[arg(long)]
    pub device_auth: bool,

    /// Activate the account after enrollment even if another account is active.
    #[arg(long)]
    pub activate: bool,
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

pub fn run(command: CredCommand) -> Result<()> {
    let _invocation_lock = capulus::acquire("kai-cred", false)?;
    let paths = RuntimePaths::from_env()?;
    ensure_codex_uses_file_credentials(&paths)?;
    let mut store = Store::open(paths)?;
    match command {
        CredCommand::List(args) => cmd_list(&store, args),
        CredCommand::Next => cmd_next(&mut store),
        CredCommand::Activate(args) => cmd_activate(&mut store, &args.email),
        CredCommand::Add(args) => cmd_add(&mut store, args),
        CredCommand::Remove(args) => cmd_remove(&mut store, args),
    }
}

fn cmd_list(store: &Store, args: ListArgs) -> Result<()> {
    let live = load_live(store);
    let active_profile = match &live {
        LiveAuth::Present(credential) => managed_profile(store, credential),
        _ => None,
    };
    let active_id = active_profile.map(|profile| profile.id.as_str());
    let accounts = store
        .profiles()
        .iter()
        .map(|profile| {
            let active = active_id == Some(profile.id.as_str());
            let facts = if active {
                match &live {
                    LiveAuth::Present(credential) => Ok(credential.facts.clone()),
                    _ => unreachable!(),
                }
            } else {
                store.credential(profile).map(|credential| credential.facts)
            };
            match facts {
                Ok(facts) => AccountView {
                    email: profile.email.clone(),
                    active,
                    plan: facts.plan,
                    access_expires_at: facts.access_expires_at,
                    last_refresh: facts.last_refresh,
                    status: AccountStatus::Ready,
                },
                Err(err) => AccountView {
                    email: profile.email.clone(),
                    active,
                    plan: None,
                    access_expires_at: None,
                    last_refresh: None,
                    status: AccountStatus::Invalid {
                        error: format!("{err:#}"),
                    },
                },
            }
        })
        .collect::<Vec<_>>();
    let next = match (&live, active_profile) {
        (LiveAuth::Absent, _) => store.profiles().first(),
        (LiveAuth::Present(_), Some(active)) => next_profile(store, Some(active)),
        _ => None,
    }
    .map(|profile| profile.email.clone());
    let active = active_profile.map(|profile| profile.email.clone());

    ui::print_list(
        &ListView {
            active,
            next,
            accounts,
        },
        args.json,
    )?;

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

fn cmd_next(store: &mut Store) -> Result<()> {
    if store.profiles().is_empty() {
        bail!("no accounts are enrolled; run `kai cred add <email>` first");
    }
    let live = load_live_strict(store)?;
    let target = match &live {
        None => store.profiles()[0].clone(),
        Some(credential) => {
            let active = require_managed_profile(store, credential)?;
            next_profile(store, Some(active))
                .context("no enrolled account is available")?
                .clone()
        }
    };
    if activate(store, &target)? {
        capulus::ui::success(&format!("Codex is now using {}.", target.email));
        warn_running_codex();
    } else {
        capulus::ui::success(&format!("{} is the only enrolled account.", target.email));
    }
    Ok(())
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

fn cmd_add(store: &mut Store, args: AddArgs) -> Result<()> {
    let expected = args.email.trim();
    validate_email(expected)?;
    if store.find_profile(expected).is_some() {
        bail!("{expected} is already enrolled");
    }

    let live = load_live(store);
    if let LiveAuth::Present(credential) = &live
        && credential.matches_email(expected)
    {
        let profile = store.insert_profile(credential)?;
        capulus::ui::success(&format!(
            "Imported the active Codex account {}.",
            profile.email
        ));
        return Ok(());
    }

    if args.activate {
        ensure_live_can_be_replaced(store, &live)?;
    }
    let credential = enroll::run(store.paths(), expected, args.device_auth)?;
    let profile = store.insert_profile(&credential)?;
    let activate_after_add = args.activate || matches!(live, LiveAuth::Absent);
    if activate_after_add {
        activate(store, &profile)?;
        capulus::ui::success(&format!("Enrolled and activated {}.", profile.email));
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
        let credential = match &live {
            LiveAuth::Present(credential) => credential,
            _ => unreachable!(),
        };
        store.sync_profile(&target, credential)?;
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
    } else if let Some(active) = active {
        let credential = match &live {
            LiveAuth::Present(credential) => credential,
            _ => unreachable!(),
        };
        store.sync_profile(active, credential)?;
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
        store.sync_profile(active, &live)?;
        if active.id == target.id {
            return Ok(false);
        }
    }
    let credential = store.credential(target)?;
    store.write_active(&credential)?;
    let installed = Credential::read(&store.paths().active_auth())?;
    if installed.facts.account_id != target.account_id {
        bail!(
            "credential activation verification failed for {}",
            target.email
        );
    }
    Ok(true)
}

fn load_live(store: &Store) -> LiveAuth {
    match fs::symlink_metadata(store.paths().active_auth()) {
        Ok(_) => match Credential::read(&store.paths().active_auth()) {
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

fn next_profile<'a>(store: &'a Store, active: Option<&Profile>) -> Option<&'a Profile> {
    if store.profiles().is_empty() {
        return None;
    }
    let Some(active) = active else {
        return store.profiles().first();
    };
    let index = store
        .profiles()
        .iter()
        .position(|profile| profile.id == active.id)?;
    store.profiles().get((index + 1) % store.profiles().len())
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
    fn switching_saves_the_live_rotated_token_before_installing_next() {
        let (_root, mut store) = setup();
        let alice = store
            .insert_profile(&credential("alice@example.com", "alice-id", "alice-old"))
            .unwrap();
        let bob = store
            .insert_profile(&credential("bob@example.com", "bob-id", "bob-token"))
            .unwrap();
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
            Credential::read(&store.paths().active_auth())
                .unwrap()
                .facts
                .account_id,
            "alice-id"
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
            Credential::read(&store.paths().active_auth())
                .unwrap()
                .facts
                .account_id,
            "outside-id"
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
            Credential::read(&store.paths().active_auth())
                .unwrap()
                .facts
                .account_id,
            "bob-id"
        );
    }
}
