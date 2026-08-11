use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use auc_tool::application::{
    ApplicationClient, ApplicationRequest, ApplicationResponse, CredentialSummary,
};
use auc_tool::product::{APPLICATION_SOCKET_PATH, MANAGEMENT_SOCKET_PATH};
use capulus::managed::{
    JobId, JobPhase, ManagementClient, ManagementClientOptions, ManagementRequest,
    ManagementResponse, RedeployJob, VersionTarget,
};
use clap::{Parser, Subcommand};
use dialoguer::Confirm;

#[derive(Parser)]
#[command(
    name = "auc",
    version,
    about = "Machine-local software passkey authenticator"
)]
struct Cli {
    #[command(subcommand)]
    command: CommandLine,
}

#[derive(Subcommand)]
enum CommandLine {
    Status,
    Touch,
    Credentials {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    System {
        #[command(subcommand)]
        command: SystemCommand,
    },
}

#[derive(Subcommand)]
enum CredentialCommand {
    List,
    Delete {
        credential_id: String,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum SystemCommand {
    Install,
    Repair,
    Redeploy {
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        no_wait: bool,
    },
    Job {
        job: String,
    },
    Uninstall {
        #[arg(long)]
        purge_vault: bool,
        #[arg(long)]
        yes: bool,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        CommandLine::Status => status(),
        CommandLine::Touch => touch(),
        CommandLine::Credentials { command } => credentials(command),
        CommandLine::System { command } => system(command),
    }
}

fn application_client() -> ApplicationClient {
    ApplicationClient::new(APPLICATION_SOCKET_PATH)
}

fn management_client() -> ManagementClient {
    ManagementClient::new(ManagementClientOptions::new(MANAGEMENT_SOCKET_PATH))
}

fn status() -> Result<()> {
    match application_client().request(ApplicationRequest::Status)? {
        ApplicationResponse::Status(status) => {
            println!("auc-agent {}", status.version);
            println!(
                "virtual authenticator: {}",
                if status.device_present {
                    "present"
                } else {
                    "absent"
                }
            );
            println!(
                "presence request: {}",
                if status.pending_touch {
                    "waiting"
                } else {
                    "none"
                }
            );
            println!("credentials: {}", status.credential_count);
            Ok(())
        }
        _ => bail!("auc-agent returned the wrong status response"),
    }
}

fn touch() -> Result<()> {
    match application_client().request(ApplicationRequest::Touch)? {
        ApplicationResponse::Touch(receipt) => {
            println!(
                "accepted {} for {}",
                terminal_text(&receipt.operation),
                terminal_text(&receipt.rp_id)
            );
            Ok(())
        }
        _ => bail!("auc-agent returned the wrong touch response"),
    }
}

fn credentials(command: CredentialCommand) -> Result<()> {
    match command {
        CredentialCommand::List => {
            match application_client().request(ApplicationRequest::ListCredentials)? {
                ApplicationResponse::Credentials { credentials } => {
                    if credentials.is_empty() {
                        println!("No credentials.");
                    } else {
                        for credential in credentials {
                            print_credential(&credential);
                        }
                    }
                    Ok(())
                }
                _ => bail!("auc-agent returned the wrong credential-list response"),
            }
        }
        CredentialCommand::Delete { credential_id, yes } => {
            if !yes
                && !Confirm::new()
                    .with_prompt(format!("Permanently delete credential {credential_id}?"))
                    .default(false)
                    .interact()?
            {
                println!("Credential was not deleted.");
                return Ok(());
            }
            match application_client()
                .request(ApplicationRequest::DeleteCredential { credential_id })?
            {
                ApplicationResponse::Deleted { credential_id } => {
                    println!("Deleted credential {credential_id}.");
                    Ok(())
                }
                _ => bail!("auc-agent returned the wrong credential-delete response"),
            }
        }
    }
}

fn print_credential(credential: &CredentialSummary) {
    println!("{}", credential.credential_id);
    println!("  relying party: {}", terminal_text(&credential.rp_id));
    if let Some(user_name) = &credential.user_name {
        println!("  account: {}", terminal_text(user_name));
    }
    println!(
        "  discoverable: {}; backup eligible: {}; backed up: {}",
        credential.discoverable, credential.backup_eligible, credential.backed_up
    );
}

fn terminal_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control()
            || matches!(
                character,
                '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
            )
        {
            escaped.extend(character.escape_unicode());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn system(command: SystemCommand) -> Result<()> {
    match command {
        SystemCommand::Install => install(),
        SystemCommand::Repair => match management_client().request(ManagementRequest::Repair)? {
            ManagementResponse::Repair(outcome) => {
                println!("{}", outcome.detail);
                Ok(())
            }
            _ => bail!("auc-agent returned the wrong repair response"),
        },
        SystemCommand::Redeploy { version, no_wait } => {
            let target = version.map_or(VersionTarget::Latest, VersionTarget::Exact);
            match management_client().request(ManagementRequest::Redeploy {
                target,
                reinstall_requesting_user: true,
            })? {
                ManagementResponse::Redeploy(outcome) => {
                    println!(
                        "{} redeploy {} for auc {}",
                        if outcome.started { "Started" } else { "Joined" },
                        outcome.job,
                        outcome.version
                    );
                    if no_wait {
                        Ok(())
                    } else {
                        wait_for_job(outcome.job)
                    }
                }
                _ => bail!("auc-agent returned the wrong redeploy response"),
            }
        }
        SystemCommand::Job { job } => {
            let job = query_job(JobId::parse(&job)?)?;
            print_job(&job);
            if job.phase == JobPhase::Failed {
                bail!("auc redeploy failed")
            } else {
                Ok(())
            }
        }
        SystemCommand::Uninstall { purge_vault, yes } => uninstall(purge_vault, yes),
    }
}

fn install() -> Result<()> {
    let uid = rustix::process::geteuid().as_raw();
    if uid == 0 {
        bail!("run auc system install as the local user who should operate auc, not as root");
    }
    let agent = sibling_agent()?;
    verify_sibling_version(&agent)?;
    let status = Command::new("/usr/bin/sudo")
        .arg("--")
        .arg(agent)
        .arg("install")
        .arg("--operator-uid")
        .arg(uid.to_string())
        .status()
        .context("failed to invoke sudo for auc system installation")?;
    if !status.success() {
        bail!("auc system installation failed with {status}");
    }
    println!("auc was installed and its virtual authenticator is healthy.");
    println!("Start a new login session before using auc so the auc group is active.");
    Ok(())
}

fn uninstall(purge_vault: bool, yes: bool) -> Result<()> {
    let uid = rustix::process::geteuid().as_raw();
    if uid == 0 {
        bail!("run auc system uninstall as an authorized local auc operator, not as root");
    }
    if !yes
        && !Confirm::new()
            .with_prompt("Remove the auc system service and binaries?")
            .default(false)
            .interact()?
    {
        println!("auc was not uninstalled.");
        return Ok(());
    }
    if purge_vault
        && !yes
        && !Confirm::new()
            .with_prompt("Permanently destroy every auc credential, PIN, and vault key?")
            .default(false)
            .interact()?
    {
        println!("auc was not uninstalled.");
        return Ok(());
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
    let status = command
        .status()
        .context("failed to invoke sudo for auc system uninstall")?;
    if !status.success() {
        bail!("auc system uninstall failed with {status}");
    }
    if purge_vault {
        println!("auc, its access group, and its encrypted vault were removed.");
    } else {
        println!("auc system files were removed; the encrypted vault was preserved.");
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

fn wait_for_job(job: JobId) -> Result<()> {
    let mut previous = None;
    loop {
        let status = query_job(job)?;
        if previous.as_ref() != Some(&status.phase) {
            println!("{}: {}", phase_name(&status.phase), status.detail);
            previous = Some(status.phase.clone());
        }
        if status.phase.is_terminal() {
            if status.phase == JobPhase::Complete {
                return Ok(());
            }
            print_job(&status);
            bail!("auc redeploy failed");
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn query_job(job: JobId) -> Result<RedeployJob> {
    match management_client().request(ManagementRequest::JobStatus { job })? {
        ManagementResponse::Job(status) => Ok(status),
        _ => bail!("auc-agent returned the wrong redeploy-job response"),
    }
}

fn print_job(job: &RedeployJob) {
    println!("job: {}", job.job);
    println!("version: {}", job.version);
    println!("phase: {}", phase_name(&job.phase));
    println!("detail: {}", job.detail);
    println!("system committed: {}", job.system_committed);
    if let Some(rollback) = job.rollback_succeeded {
        println!("rollback succeeded: {rollback}");
    }
    if let Some(reinstalled) = job.required_user_reinstalled {
        println!("requesting user reinstalled: {reinstalled}");
    }
}

fn phase_name(phase: &JobPhase) -> &'static str {
    match phase {
        JobPhase::Queued => "queued",
        JobPhase::Preparing => "preparing",
        JobPhase::Toolchain => "toolchain",
        JobPhase::Resolving => "resolving",
        JobPhase::Building => "building",
        JobPhase::Validating => "validating",
        JobPhase::Staging => "staging",
        JobPhase::CommittingSystem => "committing-system",
        JobPhase::RestartingAgent => "restarting-agent",
        JobPhase::ReinstallingUser => "reinstalling-user",
        JobPhase::Complete => "complete",
        JobPhase::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_text_escapes_control_and_bidirectional_formatting_characters() {
        let escaped = terminal_text("alice\n\u{1b}[31m\u{202e}");
        assert_eq!(escaped, "alice\\u{a}\\u{1b}[31m\\u{202e}");
    }
}
