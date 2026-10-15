//! OpenSSH diagnostics and bounded authentication probes shared by cloud adapters.
use std::io::{Read, Seek, SeekFrom};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Failure {
    Transport,
    Authentication,
    ServerPermissions,
    HostKey,
    RemoteCommand,
    LocalSetup,
    Unknown,
    Timeout,
    Interrupted,
}

impl Failure {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Transport => "ssh_transport_failed",
            Self::Authentication => "ssh_authentication_failed",
            Self::ServerPermissions => "ssh_server_permissions",
            Self::HostKey => "ssh_host_key_failed",
            Self::RemoteCommand => "ssh_remote_command_failed",
            Self::LocalSetup => "ssh_local_setup_failed",
            Self::Unknown => "ssh_connection_failed",
            Self::Timeout => "ssh_recovery_timeout",
            Self::Interrupted => "ssh_probe_interrupted",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProbeFailure {
    pub(crate) kind: Failure,
    pub(crate) exit_code: Option<i32>,
    pub(crate) reason: String,
}

impl ProbeFailure {
    pub(crate) fn new(kind: Failure, reason: &str) -> Self {
        Self {
            kind,
            exit_code: None,
            reason: reason.to_owned(),
        }
    }
}

pub(crate) fn authenticated(log: &str, host: &str) -> bool {
    // A ProxyJump helper can write its own authentication message to stderr.
    // Require confirmation for the requested endpoint, not an intermediate host.
    let prefix = format!("Authenticated to {host} ");
    log.lines().any(|line| line.starts_with(&prefix))
}

/// Inspect OpenSSH's own diagnostics from an authentication-only (-N) probe.
/// A remote command's output is never input to automatic key recovery.
pub(crate) fn classify(code: Option<i32>, log: &str) -> ProbeFailure {
    let lower = log.to_ascii_lowercase();
    let (kind, reason) = if code.is_none() {
        (Failure::Interrupted, "SSH probe ended without an exit code")
    } else if lower.contains("host key verification failed")
        || lower.contains("remote host identification has changed")
    {
        (Failure::HostKey, "SSH host key verification failed")
    } else if lower.contains("signing failed")
        || lower.contains("unprotected private key file")
        || lower.lines().any(|line| {
            line.starts_with("load key ")
                && [
                    "permission denied",
                    "bad permissions",
                    "invalid format",
                    "incorrect passphrase",
                    "error in libcrypto",
                ]
                .iter()
                .any(|text| line.contains(text))
        })
    {
        (
            Failure::LocalSetup,
            "SSH could not load or use a local private key; inspect key permissions, format, and agent access",
        )
    } else if lower.contains("permission denied (publickey")
        || lower.contains("too many authentication failures")
    {
        (
            Failure::Authentication,
            "SSH server rejected public-key authentication",
        )
    } else if let Some(reason) = [
        "connection refused",
        "connection timed out",
        "operation timed out",
        "no route to host",
        "network is unreachable",
        "could not resolve hostname",
        "connection reset",
        "connection closed",
    ]
    .into_iter()
    .find(|text| lower.contains(text))
    {
        (Failure::Transport, reason)
    } else {
        // In particular, 255 alone says nothing about whether authentication
        // happened. Do not infer a missing key from the exit status.
        (
            Failure::Unknown,
            "SSH probe failed without a recognized authentication or transport diagnostic",
        )
    };
    ProbeFailure {
        kind,
        exit_code: code,
        reason: reason.to_owned(),
    }
}

pub(crate) struct ProbeChild(pub(crate) Child);

impl Drop for ProbeChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_some() {
            return;
        }
        #[cfg(unix)]
        // SAFETY: this probe has its own process group. It has not been reaped,
        // so its PID cannot have been reused for an unrelated process group.
        unsafe {
            libc::kill(-(self.0.id() as libc::pid_t), libc::SIGKILL);
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub(crate) fn run_probe(
    mut command: Command,
    timeout: Duration,
    host: &str,
) -> std::result::Result<(), ProbeFailure> {
    let result = (|| -> Result<_> {
        let cancellation = capulus::Cancellation::install()?;
        cancellation.check()?;
        // Independent descriptors allow polling without moving SSH's write
        // offset. The private file prevents pipe deadlocks and is removed on
        // drop; only a bounded diagnostic tail is ever read into memory.
        let mut log = tempfile::NamedTempFile::new()?;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log.reopen()?))
            .env("LC_ALL", "C")
            .env("SSH_ASKPASS_REQUIRE", "never");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = ProbeChild(command.spawn().context("Failed to start SSH probe")?);
        let start = Instant::now();
        loop {
            cancellation.check()?;
            let status = child.0.try_wait()?;
            let size = log.as_file().metadata()?.len();
            log.as_file_mut()
                .seek(SeekFrom::Start(size.saturating_sub(64 * 1024)))?;
            let mut bytes = Vec::new();
            log.as_file_mut().take(64 * 1024).read_to_end(&mut bytes)?;
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if authenticated(&text, host) {
                // No command/session was requested. Close the authenticated
                // probe before running the user's operation on one connection.
                return Ok(Ok(()));
            }
            if let Some(status) = status {
                return Ok(Err(classify(status.code(), &text)));
            }
            if start.elapsed() >= timeout {
                return Ok(Err(ProbeFailure::new(
                    Failure::Transport,
                    "SSH probe exceeded its connection timeout",
                )));
            }
            thread::sleep(Duration::from_millis(10).min(timeout.saturating_sub(start.elapsed())));
        }
    })();
    match result {
        Ok(result) => result,
        Err(err) if capulus::error_is_cancelled(&err) => Err(ProbeFailure::new(
            Failure::Interrupted,
            "SSH probe was interrupted",
        )),
        Err(_) => Err(ProbeFailure::new(
            Failure::LocalSetup,
            "Could not run the local SSH probe; check the SSH executable and temporary-directory access",
        )),
    }
}
