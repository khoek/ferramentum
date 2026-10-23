use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::json;

use super::{Instance, Provider, cleanup_details, client::Client};
use crate::cli::{PullArgs, PushArgs, ShellArgs};
use crate::model::{Cloud, IceConfig};
use crate::providers::{CloudInstance, CloudProvider};
use crate::ssh_probe::{Failure, ProbeFailure, run_probe};
use crate::support::{run_command_status, shell_quote_single};

pub(super) fn identity(config: &IceConfig) -> Result<Option<PathBuf>> {
    crate::support::ensure_command_available("ssh")?;
    if let Some(path) = config.default.verda.ssh_key_path.as_deref() {
        let path = PathBuf::from(path);
        if !path.is_file() {
            bail!("Configured Verda SSH private key path does not exist");
        }
        return Ok(Some(path));
    }
    Ok(crate::remote::discover_local_ssh_keypair()?.map(|(path, _)| path))
}

fn host(instance: &Instance) -> Result<IpAddr> {
    instance
        .ip
        .as_deref()
        .context("Verda instance has no public IP")?
        .parse()
        .context("Verda returned an invalid public IP")
}

fn options(identity: Option<&Path>) -> Vec<String> {
    let mut args = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ServerAliveInterval=5".into(),
        "-o".into(),
        "ServerAliveCountMax=2".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
    ];
    if let Some(path) = identity {
        args.extend([
            "-i".into(),
            path.display().to_string(),
            "-o".into(),
            "IdentitiesOnly=yes".into(),
        ]);
    }
    args
}

pub(super) fn connection_command(instance: &Instance, identity: Option<&Path>) -> Result<String> {
    let mut parts = vec!["ssh".to_owned()];
    parts.extend(options(identity));
    parts.push(format!("root@{}", host(instance)?));
    Ok(parts
        .iter()
        .map(|s| shell_quote_single(s))
        .collect::<Vec<_>>()
        .join(" "))
}

pub(super) fn wait_ready(
    instance: &Instance,
    identity: Option<&Path>,
    deadline: Instant,
) -> Result<()> {
    let host = host(instance)?.to_string();
    wait_ready_with(instance, deadline, Duration::from_secs(2), |timeout| {
        let mut command = Command::new("ssh");
        command
            .args([
                "-v",
                "-N",
                "-T",
                "-S",
                "none",
                "-o",
                "ControlMaster=no",
                "-o",
                "ClearAllForwardings=yes",
                "-o",
                "PermitLocalCommand=no",
                "-o",
                "RemoteCommand=none",
                "-o",
                "ForkAfterAuthentication=no",
                "-o",
                "ConnectionAttempts=1",
            ])
            .args(options(identity))
            .arg(format!("root@{host}"));
        run_probe(command, timeout, &host)
    })
}

fn wait_ready_with(
    instance: &Instance,
    deadline: Instant,
    retry_delay: Duration,
    mut probe: impl FnMut(Duration) -> std::result::Result<(), ProbeFailure>,
) -> Result<()> {
    let cancellation = capulus::Cancellation::install()?;
    let mut attempts = 0;
    let mut last_failure = None;
    while Instant::now() < deadline {
        cancellation.check()?;
        attempts += 1;
        match probe(Duration::from_secs(10).min(deadline.saturating_duration_since(Instant::now())))
        {
            Ok(()) => return Ok(()),
            Err(failure) if failure.kind == Failure::Interrupted => {
                return Err(capulus::Cancelled.into());
            }
            Err(failure) if failure.kind == Failure::Transport => last_failure = Some(failure),
            Err(failure) => {
                return Err(readiness_error(
                    instance,
                    attempts,
                    failure.kind,
                    Some(&failure),
                ));
            }
        }
        cancellation.sleep(retry_delay.min(deadline.saturating_duration_since(Instant::now())))?;
    }
    Err(readiness_error(
        instance,
        attempts,
        Failure::Timeout,
        last_failure.as_ref(),
    ))
}

fn readiness_error(
    instance: &Instance,
    attempts: usize,
    kind: Failure,
    last: Option<&ProbeFailure>,
) -> anyhow::Error {
    let mut details = cleanup_details(&instance.id, Some(instance));
    details["ssh"] = json!({"attempts":attempts, "last_failure":last});
    let message = match kind {
        Failure::Authentication => {
            "Verda SSH rejected the local key. Check that it matches the registered instance key and is available to ssh-agent."
        }
        Failure::HostKey => {
            "Verda SSH host key verification failed. Verify the server identity before correcting known_hosts."
        }
        Failure::LocalSetup => {
            "Verda SSH could not use the local SSH setup. Check the key path, permissions, format and agent access."
        }
        Failure::Timeout => {
            "Verda SSH startup deadline expired. Inspect the last SSH failure and the instance before retrying; it may remain billable."
        }
        _ => "Verda SSH failed; inspect the structured SSH failure before retrying.",
    };
    crate::automation::error(kind.code(), message, details)
}

fn resolve(config: &IceConfig, identifier: &str) -> Result<(Client, Instance)> {
    let client = Client::from_config(config)?;
    let instance = Provider::resolve_instance(&client, identifier)?;
    Ok((client, instance))
}

pub(super) fn shell(config: &IceConfig, args: &ShellArgs) -> Result<()> {
    if args.preserve_ephemeral {
        bail!("--preserve-ephemeral is unsupported on Verda");
    }
    let (client, instance) = resolve(config, &args.instance)?;
    if !args.no_probe && !instance.is_running() {
        return Err(crate::automation::error(
            "instance_stopped",
            "Start the Verda instance explicitly before connecting.",
            json!({"instance_id":instance.id,"next_command":format!("ice start --cloud verda {}",instance.id)}),
        ));
    }
    let identity = if args.no_probe {
        config
            .default
            .verda
            .ssh_key_path
            .as_ref()
            .map(PathBuf::from)
    } else {
        super::keys::instance_identity(&client, config, &instance)?
    };
    if !args.no_probe {
        wait_ready(
            &instance,
            identity.as_deref(),
            Instant::now() + crate::automation::startup_timeout(),
        )?;
    }
    if args.print_creds {
        let command = connection_command(&instance, identity.as_deref())?;
        if crate::output::is_json() {
            return crate::output::emit(
                "shell",
                Cloud::Verda,
                json!({"instance":instance.json_summary(), "connect_command":command,
                "readiness":if args.no_probe {"unchecked"} else {"ssh_verified"}}),
            );
        }
        println!("{command}");
        return Ok(());
    }
    run_command_status(
        Command::new("ssh")
            .args(options(identity.as_deref()))
            .arg(format!("root@{}", host(&instance)?)),
        "Open Verda shell",
    )
}

fn transfer(
    client: &Client,
    config: &IceConfig,
    instance: &Instance,
    local: &Path,
    remote: &str,
    upload: bool,
) -> Result<()> {
    if !instance.is_running() {
        bail!("Verda instance is not running; start it explicitly");
    }
    let identity = super::keys::instance_identity(client, config, instance)?;
    let host = host(instance)?;
    let host = match host {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    let transport = std::iter::once("ssh".to_owned())
        .chain(options(identity.as_deref()))
        .map(|s| shell_quote_single(&s))
        .collect::<Vec<_>>()
        .join(" ");
    let mut command = Command::new("rsync");
    command.args(["-az", "--protect-args", "-e", &transport, "--"]);
    let remote = format!("root@{host}:{remote}");
    if upload {
        command.arg(local).arg(remote);
    } else {
        command.arg(remote).arg(local);
    }
    if crate::output::is_json() {
        command.stdout(Stdio::null());
    }
    run_command_status(&mut command, "Transfer Verda files")
}

pub(super) fn push(config: &IceConfig, args: &PushArgs) -> Result<()> {
    if !args.local_path.exists() {
        bail!("Upload path does not exist");
    }
    let (client, instance) = resolve(config, &args.instance)?;
    transfer(
        &client,
        config,
        &instance,
        &args.local_path,
        args.remote_path.as_deref().unwrap_or("."),
        true,
    )
}

pub(super) fn pull(config: &IceConfig, args: &PullArgs) -> Result<()> {
    let (client, instance) = resolve(config, &args.instance)?;
    transfer(
        &client,
        config,
        &instance,
        args.local_path.as_deref().unwrap_or(Path::new(".")),
        &args.remote_path,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance() -> Instance {
        serde_json::from_value(json!({"id":"11111111-1111-4111-8111-111111111111","hostname":"ice-test","status":"running","ip":"192.0.2.1"})).unwrap()
    }

    #[test]
    fn readiness_retries_transport_but_stops_on_permanent_failures() {
        for kind in [
            Failure::Authentication,
            Failure::HostKey,
            Failure::LocalSetup,
            Failure::Unknown,
        ] {
            let mut calls = 0;
            let error = wait_ready_with(
                &instance(),
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
                |_| {
                    calls += 1;
                    Err(ProbeFailure::new(kind, "fixture failure"))
                },
            )
            .unwrap_err();
            assert_eq!(calls, 1);
            let error = error
                .downcast_ref::<crate::automation::AgentError>()
                .unwrap();
            assert_eq!(error.code, kind.code());
            assert_eq!(
                error.details["ssh"]["last_failure"]["reason"],
                "fixture failure"
            );
            assert!(
                error.details["cleanup_command"]
                    .as_str()
                    .unwrap()
                    .contains("ice delete")
            );
        }
        let mut calls = 0;
        wait_ready_with(
            &instance(),
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
            |_| {
                calls += 1;
                if calls == 1 {
                    Err(ProbeFailure::new(Failure::Transport, "connection refused"))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(calls, 2);
    }

    #[test]
    fn expired_deadline_starts_no_probe_and_timeout_keeps_last_failure() {
        let error = wait_ready_with(&instance(), Instant::now(), Duration::ZERO, |_| {
            panic!("deadline expired")
        })
        .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::automation::AgentError>()
                .unwrap()
                .details["ssh"]["attempts"],
            0
        );
        let start = Instant::now();
        let error = wait_ready_with(
            &instance(),
            start + Duration::from_millis(30),
            Duration::from_secs(2),
            |timeout| {
                assert!(timeout <= Duration::from_millis(30));
                Err(ProbeFailure::new(Failure::Transport, "connection refused"))
            },
        )
        .unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(1));
        let error = error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap();
        assert_eq!(error.code, "ssh_recovery_timeout");
        assert_eq!(
            error.details["ssh"]["last_failure"]["reason"],
            "connection refused"
        );
    }

    #[test]
    fn interrupted_probe_preserves_cancellation_and_does_not_retry() {
        let mut calls = 0;
        let error = wait_ready_with(
            &instance(),
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
            |_| {
                calls += 1;
                Err(ProbeFailure::new(Failure::Interrupted, "interrupted"))
            },
        )
        .unwrap_err();
        assert_eq!(calls, 1);
        assert!(capulus::error_is_cancelled(&error));
    }
}
