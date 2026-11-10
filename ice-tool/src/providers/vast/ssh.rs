//! Bounded connection recovery, separate from the user's shell/transfer/workload.
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::{Duration, Instant};
#[cfg(test)]
use std::{process::Stdio, thread};

use anyhow::{Result, bail};
use serde::Serialize;
use serde_json::json;

use super::{InstanceSshKeyAttachStatus, VastClient, VastInstance};
use crate::remote::{RemoteAccess, discover_local_ssh_keypair};
use crate::ssh_probe::{Failure, ProbeFailure, run_probe};
#[cfg(test)]
use crate::ssh_probe::{authenticated, classify};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Endpoint {
    pub kind: &'static str,
    pub host: String,
    pub port: u16,
}

/// Only use endpoints reported in this invocation's provider response. Never
/// guess a mapped port or retain it across instance/container replacements.
pub(super) fn endpoints(instance: &VastInstance) -> Vec<Endpoint> {
    let direct = instance.public_ipaddr.as_deref().and_then(|host| {
        host.parse::<IpAddr>().ok()?;
        let port = instance
            .ports
            .get("22/tcp")?
            .as_array()?
            .iter()
            .find_map(|p| {
                let raw = &p["HostPort"];
                raw.as_str()
                    .and_then(|s| s.parse::<u16>().ok())
                    .or_else(|| raw.as_u64().and_then(|n| u16::try_from(n).ok()))
                    .filter(|p| *p > 0)
            })?;
        Some(Endpoint {
            kind: "direct",
            host: host.to_owned(),
            port,
        })
    });
    let mut result = Vec::new();
    if let (Some(host), Some(port)) = (instance.ssh_host.as_deref(), instance.ssh_port) {
        let host = host.trim();
        if !host.is_empty() && port > 0 && !host.chars().any(char::is_whitespace) {
            let kind = if direct
                .as_ref()
                .is_some_and(|d| d.host == host && d.port == port)
            {
                "direct"
            } else if host.ends_with(".vast.ai") {
                "relay"
            } else {
                "reported"
            };
            result.push(Endpoint {
                kind,
                host: host.to_owned(),
                port,
            });
        }
    }
    if let Some(direct) = direct
        && !result
            .iter()
            .any(|e| e.host == direct.host && e.port == direct.port)
    {
        result.push(direct);
    }
    result
}

#[derive(Clone, Debug, Serialize)]
struct Attempt {
    endpoint: Endpoint,
    identity: &'static str,
    failure: Option<ProbeFailure>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Diagnostics {
    instance_id: String,
    elapsed_seconds: f64,
    budget_seconds: f64,
    probe_limit: usize,
    attempts: Vec<Attempt>,
    key_attachment: &'static str,
    provider_logs: &'static str,
    next_command: String,
}

#[derive(Debug)]
pub(super) struct Connection {
    pub endpoint: Endpoint,
    pub identity: Option<PathBuf>,
    pub diagnostics: Diagnostics,
}

impl Connection {
    pub fn access(&self) -> RemoteAccess<'_> {
        RemoteAccess {
            user: "root",
            host: &self.endpoint.host,
            port: Some(self.endpoint.port),
            identity_file: self.identity.as_deref(),
        }
    }

    pub fn command(&self, remote: Option<&str>) -> String {
        connection_command(&self.endpoint, self.identity.as_deref(), remote)
    }
}

pub(super) fn connection_command(
    endpoint: &Endpoint,
    identity: Option<&Path>,
    remote: Option<&str>,
) -> String {
    let mut args = super::ssh_args(&endpoint.host, endpoint.port, identity);
    if let Some(remote) = remote {
        args.push("-t".to_owned());
        args.push(remote.to_owned());
    }
    crate::support::render_command_line("ssh", args)
}

#[derive(Clone, Copy)]
struct Policy {
    timeout: Duration,
    probe_timeout: Duration,
    max_probes: usize,
    settle_delay: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(90),
            probe_timeout: Duration::from_secs(10),
            max_probes: 7,
            settle_delay: Duration::from_secs(1),
        }
    }
}

trait RecoveryIo {
    fn probe(
        &mut self,
        endpoint: &Endpoint,
        identity: Option<&Path>,
        timeout: Duration,
    ) -> std::result::Result<(), ProbeFailure>;
    fn keypair(&mut self) -> Result<Option<(PathBuf, String)>>;
    fn permissions_rejected(&mut self, id: u64, deadline: Instant) -> Result<bool>;
    fn attach(
        &mut self,
        id: u64,
        key: &str,
        deadline: Instant,
    ) -> Result<InstanceSshKeyAttachStatus>;
}

struct LiveIo<'a>(&'a VastClient);

impl RecoveryIo for LiveIo<'_> {
    fn probe(
        &mut self,
        endpoint: &Endpoint,
        identity: Option<&Path>,
        timeout: Duration,
    ) -> std::result::Result<(), ProbeFailure> {
        let mut command = Command::new("ssh");
        // These checks never ask for a password, passphrase, host confirmation,
        // or a TTY, including when the eventual shell will be interactive.
        command.args([
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
            "BatchMode=yes",
            "-o",
            "ConnectionAttempts=1",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=1",
        ]);
        command.args(super::ssh_args(&endpoint.host, endpoint.port, identity));
        run_probe(command, timeout, &endpoint.host)
    }

    fn keypair(&mut self) -> Result<Option<(PathBuf, String)>> {
        discover_local_ssh_keypair()
    }

    fn permissions_rejected(&mut self, id: u64, deadline: Instant) -> Result<bool> {
        self.0.ssh_permissions_rejected(id, deadline)
    }

    fn attach(
        &mut self,
        id: u64,
        key: &str,
        deadline: Instant,
    ) -> Result<InstanceSshKeyAttachStatus> {
        self.0
            .attach_instance_ssh_key(id, key, remaining(deadline)?.min(Duration::from_secs(10)))
    }
}

pub(super) fn command_status(status: ExitStatus) -> Result<()> {
    if status.success() {
        return Ok(());
    }
    if let Some(code) = status.code().filter(|code| *code != 255) {
        return Err(crate::automation::error(
            Failure::RemoteCommand.code(),
            format!("SSH remote command exited with status {code}"),
            json!({"exit_code": code}),
        ));
    }
    bail!("SSH operation exited with status {status}");
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        bail!("SSH recovery budget exhausted");
    }
    Ok(left)
}

struct Recovery<'a, I> {
    io: &'a mut I,
    start: Instant,
    deadline: Instant,
    policy: Policy,
    diagnostics: Diagnostics,
}

impl<I: RecoveryIo> Recovery<'_, I> {
    fn check_cancellation(&mut self) -> Result<()> {
        if capulus::Cancellation::passive().is_requested() {
            return Err(cancelled_error(
                self.fail(Failure::Interrupted, "SSH recovery interrupted"),
            ));
        }
        Ok(())
    }

    fn probe(
        &mut self,
        endpoint: &Endpoint,
        identity: Option<&Path>,
    ) -> Result<std::result::Result<(), Failure>> {
        self.check_cancellation()?;
        if self.diagnostics.attempts.len() >= self.policy.max_probes
            || Instant::now() >= self.deadline
        {
            return Err(self.fail(
                Failure::Timeout,
                "SSH recovery reached its time or attempt limit",
            ));
        }
        let timeout = self
            .policy
            .probe_timeout
            .min(self.deadline.saturating_duration_since(Instant::now()));
        crate::ui::print_stage(&format!(
            "Checking SSH via {} {}:{}",
            endpoint.kind, endpoint.host, endpoint.port
        ));
        let result = self.io.probe(endpoint, identity, timeout);
        let kind = result.as_ref().err().map(|f| f.kind);
        self.diagnostics.attempts.push(Attempt {
            endpoint: endpoint.clone(),
            identity: if identity.is_some() {
                "local_key"
            } else {
                "ssh_defaults"
            },
            failure: result.err(),
        });
        if let Some(kind) = kind {
            if matches!(
                kind,
                Failure::HostKey
                    | Failure::RemoteCommand
                    | Failure::LocalSetup
                    | Failure::Interrupted
            ) {
                let error = self.fail(
                    kind,
                    "SSH readiness check failed; key recovery cannot resolve this failure",
                );
                return Err(if capulus::Cancellation::passive().is_requested() {
                    cancelled_error(error)
                } else {
                    error
                });
            }
            return Ok(Err(kind));
        }
        Ok(Ok(()))
    }

    fn finish(&mut self, endpoint: Endpoint, identity: Option<PathBuf>) -> Connection {
        self.diagnostics.elapsed_seconds = self.start.elapsed().as_secs_f64();
        Connection {
            endpoint,
            identity,
            diagnostics: self.diagnostics.clone(),
        }
    }

    fn fail(&mut self, kind: Failure, message: &str) -> anyhow::Error {
        self.diagnostics.elapsed_seconds = self.start.elapsed().as_secs_f64();
        if matches!(
            kind,
            Failure::Transport | Failure::Timeout | Failure::Unknown
        ) {
            self.diagnostics.next_command = format!(
                "ice shell --cloud vast.ai {} --print-creds --no-probe --json",
                self.diagnostics.instance_id
            );
        } else if kind == Failure::LocalSetup {
            self.diagnostics.next_command = "ssh -V".to_owned();
        } else if kind == Failure::HostKey
            && let Some(attempt) = self.diagnostics.attempts.last()
        {
            self.diagnostics.next_command = crate::support::render_command_line(
                "ssh-keygen",
                [
                    "-F".to_owned(),
                    format!("[{}]:{}", attempt.endpoint.host, attempt.endpoint.port),
                ],
            );
        }
        let failures = self
            .diagnostics
            .attempts
            .iter()
            .filter_map(|a| {
                a.failure.as_ref().map(|f| {
                    format!(
                        "{}:{} ({}) — {}",
                        a.endpoint.host, a.endpoint.port, a.endpoint.kind, f.reason
                    )
                })
            })
            .collect::<Vec<_>>();
        let first = failures
            .first()
            .map(String::as_str)
            .unwrap_or("no probe completed");
        let last = failures
            .last()
            .map(String::as_str)
            .unwrap_or("no probe completed");
        crate::automation::error(
            kind.code(),
            format!(
                "{message}. First failure: {first}. Last failure: {last}. {} probes in {:.1}s. Key attachment: {}. Next diagnostic: {}",
                self.diagnostics.attempts.len(),
                self.diagnostics.elapsed_seconds,
                self.diagnostics.key_attachment,
                self.diagnostics.next_command
            ),
            json!({"ssh": self.diagnostics}),
        )
    }
}

pub(super) fn connect(client: &VastClient, instance: &VastInstance) -> Result<Connection> {
    recover(&mut LiveIo(client), instance, Policy::default())
}

fn cancelled_error(diagnostic: anyhow::Error) -> anyhow::Error {
    // Keep Cancelled as the source for exit-status handling, while allowing the
    // output layer to downcast the structured diagnostic used as context.
    let diagnostic = diagnostic
        .downcast::<crate::automation::AgentError>()
        .expect("recovery diagnostics are typed");
    anyhow::Error::new(capulus::Cancelled).context(diagnostic)
}

fn recover<I: RecoveryIo>(
    io: &mut I,
    instance: &VastInstance,
    policy: Policy,
) -> Result<Connection> {
    let start = Instant::now();
    let mut state = Recovery {
        io,
        start,
        deadline: start + policy.timeout,
        policy,
        diagnostics: Diagnostics {
            instance_id: instance.id.to_string(),
            elapsed_seconds: 0.0,
            budget_seconds: policy.timeout.as_secs_f64(),
            probe_limit: policy.max_probes,
            attempts: Vec::new(),
            key_attachment: "not_attempted",
            provider_logs: "not_requested",
            next_command: format!(
                "ice logs --cloud vast.ai {} --provider-logs --tail 200",
                instance.id
            ),
        },
    };
    let candidates = endpoints(instance);
    if candidates.is_empty() {
        return Err(state.fail(
            Failure::Transport,
            "Vast has not reported a usable SSH endpoint; inspect instance readiness",
        ));
    }
    let mut rejected = Vec::new();
    let mut last = Failure::Unknown;
    for endpoint in &candidates {
        match state.probe(endpoint, None)? {
            Ok(()) => return Ok(state.finish(endpoint.clone(), None)),
            Err(kind) => {
                last = kind;
                if kind == Failure::Authentication {
                    rejected.push(endpoint.clone());
                }
            }
        }
    }
    if rejected.is_empty() {
        return Err(state.fail(
            last,
            "Reported SSH endpoints failed; no key changes were attempted",
        ));
    }
    let keypair = state
        .io
        .keypair()
        .map_err(|_| state.fail(Failure::LocalSetup, "Could not read a local SSH keypair"))?;
    if let Some((identity, _)) = &keypair {
        let mut still_rejected = Vec::new();
        for endpoint in rejected.iter().rev() {
            match state.probe(endpoint, Some(identity))? {
                Ok(()) => return Ok(state.finish(endpoint.clone(), Some(identity.clone()))),
                Err(kind) => {
                    last = kind;
                    if kind == Failure::Authentication {
                        still_rejected.push(endpoint.clone());
                    }
                }
            }
        }
        rejected = still_rejected;
        if rejected.is_empty() {
            return Err(state.fail(
                last,
                "Explicit-key probes failed before authentication; no key changes were attempted",
            ));
        }
    }
    if Instant::now() >= state.deadline {
        return Err(state.fail(
            Failure::Timeout,
            "SSH recovery time limit reached before provider diagnostics",
        ));
    }
    let log_deadline = state.deadline.min(Instant::now() + Duration::from_secs(15));
    state.check_cancellation()?;
    let diagnostic = state.io.permissions_rejected(instance.id, log_deadline);
    state.check_cancellation()?;
    match diagnostic {
        Ok(true) => {
            state.diagnostics.provider_logs = "sshd_permissions_rejected";
            return Err(state.fail(Failure::ServerPermissions, "Provider logs report sshd refusing authorized_keys because of ownership or modes; repair the selected instance's ownership/permissions before retrying"));
        }
        Ok(false) => state.diagnostics.provider_logs = "no_permission_evidence",
        Err(_) => state.diagnostics.provider_logs = "unavailable",
    }
    let Some((identity, public_key)) = keypair else {
        return Err(state.fail(
            Failure::Authentication,
            "Authentication failed and no local SSH keypair is available to attach",
        ));
    };
    if Instant::now() >= state.deadline || state.diagnostics.attempts.len() >= policy.max_probes {
        return Err(state.fail(
            Failure::Timeout,
            "SSH recovery budget exhausted before key attachment",
        ));
    }
    state.diagnostics.key_attachment = "outcome_unknown";
    let attached = state.io.attach(instance.id, &public_key, state.deadline)
        .map_err(|_| state.fail(Failure::Authentication, "Instance-key attachment failed; its outcome may be unknown, inspect the instance before retrying"))?;
    state.diagnostics.key_attachment = match attached {
        InstanceSshKeyAttachStatus::Attached => "attached",
        InstanceSshKeyAttachStatus::AlreadyAssociated => "already_associated",
    };
    state.check_cancellation()?;
    if attached == InstanceSshKeyAttachStatus::AlreadyAssociated {
        return Err(state.fail(Failure::Authentication, "Vast reports the local key is already associated, but SSH rejected it; repeating key attachment will not help"));
    }
    crate::ui::print_stage("Attached local SSH key to this instance; checking authentication");
    // Prefer the direct endpoint when both endpoints rejected authentication.
    let endpoint = rejected
        .iter()
        .find(|e| e.kind == "direct")
        .unwrap_or(&rejected[0]);
    for attempt in 0..3 {
        if attempt != 0 {
            let _ = capulus::Cancellation::passive().sleep(
                policy
                    .settle_delay
                    .min(state.deadline.saturating_duration_since(Instant::now())),
            );
            state.check_cancellation()?;
        }
        match state.probe(endpoint, Some(&identity))? {
            Ok(()) => return Ok(state.finish(endpoint.clone(), Some(identity))),
            Err(Failure::Authentication) => (),
            Err(kind) => {
                return Err(state.fail(
                    kind,
                    "SSH failed after instance-key attachment; stopping authentication retries",
                ));
            }
        }
    }
    Err(state.fail(Failure::Authentication, "SSH rejected the attached key after bounded retries; inspect provider logs and instance SSH configuration"))
}

/// Summarize only recognized sshd diagnostics, never return arbitrary workload
/// logs or key contents inside structured connection errors.
pub(super) fn logs_reject_permissions(logs: &str) -> bool {
    let lower = logs.to_ascii_lowercase();
    lower.contains("authentication refused: bad ownership or modes")
        || lower.lines().any(|line| {
            line.contains("could not open")
                && line.contains("authorized keys")
                && line.contains("permission denied")
        })
}

pub(super) fn operation<T>(
    connection: &Connection,
    action: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    // Never replay a user's operation. An rsync/remote-shell error after the
    // successful probe can be a transport change OR a workload error, so retain
    // the operation failure without claiming it is an authentication failure.
    action(connection).map_err(|err| {
        if capulus::error_is_cancelled(&err) { return err; }
        if capulus::Cancellation::passive().is_requested() { return anyhow::Error::new(capulus::Cancelled).context(format!("{err:#}")); }
        let typed = err.downcast_ref::<crate::automation::AgentError>();
        let code = typed.map_or("ssh_operation_failed", |e| e.code);
        let mut details = typed.map_or_else(|| json!({}), |e| e.details.clone());
        details["endpoint"] = json!(connection.endpoint);
        details["ssh"] = json!(connection.diagnostics);
        crate::automation::error(code, format!("SSH operation failed after a successful connection probe; it was not retried: {err:#}"), details)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::VecDeque;

    fn instance(direct: bool) -> VastInstance {
        serde_json::from_value(json!({
            "id": 42, "label": "ice-test", "cur_state": "running", "image_runtype": "ssh",
            "ssh_host": "ssh1.vast.ai", "ssh_port": 1234,
            "public_ipaddr": if direct { Some("203.0.113.42") } else { None },
            "ports": {"22/tcp": [{"HostPort": "4321"}]}
        }))
        .unwrap()
    }

    struct MockIo {
        outcomes: VecDeque<Option<Failure>>,
        probes: Vec<(Endpoint, bool)>,
        attachments: usize,
        keys_read: usize,
        log_reads: usize,
        permissions: Option<bool>,
        attach_status: InstanceSshKeyAttachStatus,
        key_present: bool,
    }

    impl MockIo {
        fn new(outcomes: &[Option<Failure>]) -> Self {
            Self {
                outcomes: outcomes.iter().copied().collect(),
                probes: Vec::new(),
                attachments: 0,
                keys_read: 0,
                log_reads: 0,
                permissions: Some(false),
                attach_status: InstanceSshKeyAttachStatus::Attached,
                key_present: true,
            }
        }
    }

    impl RecoveryIo for MockIo {
        fn probe(
            &mut self,
            endpoint: &Endpoint,
            identity: Option<&Path>,
            timeout: Duration,
        ) -> std::result::Result<(), ProbeFailure> {
            assert!(timeout > Duration::ZERO && timeout <= Duration::from_secs(10));
            self.probes.push((endpoint.clone(), identity.is_some()));
            match self
                .outcomes
                .pop_front()
                .expect("unexpected extra SSH probe")
            {
                None => Ok(()),
                Some(kind) => Err(ProbeFailure::new(kind, &format!("mock {kind:?}"))),
            }
        }

        fn keypair(&mut self) -> Result<Option<(PathBuf, String)>> {
            self.keys_read += 1;
            Ok(self.key_present.then(|| {
                (
                    PathBuf::from("/tmp/test identity"),
                    "ssh-ed25519 test-public-key".to_owned(),
                )
            }))
        }

        fn permissions_rejected(&mut self, id: u64, deadline: Instant) -> Result<bool> {
            assert_eq!(id, 42);
            assert!(deadline > Instant::now());
            self.log_reads += 1;
            self.permissions
                .ok_or_else(|| anyhow::anyhow!("log provider unavailable"))
        }

        fn attach(
            &mut self,
            id: u64,
            key: &str,
            deadline: Instant,
        ) -> Result<InstanceSshKeyAttachStatus> {
            assert_eq!(id, 42);
            assert_eq!(key, "ssh-ed25519 test-public-key");
            assert!(deadline > Instant::now());
            self.attachments += 1;
            Ok(self.attach_status)
        }
    }

    fn policy() -> Policy {
        Policy {
            settle_delay: Duration::ZERO,
            ..Policy::default()
        }
    }

    fn details(error: &anyhow::Error) -> &crate::automation::AgentError {
        error.downcast_ref().expect("structured error")
    }

    #[test]
    fn transport_refusal_does_not_read_or_change_keys() {
        let mut io = MockIo::new(&[Some(Failure::Transport)]);
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(details(&err).code, "ssh_transport_failed");
        assert_eq!((io.keys_read, io.attachments, io.log_reads), (0, 0, 0));
        assert_eq!(
            details(&err).details["ssh"]["attempts"][0]["endpoint"]["kind"],
            "relay"
        );
    }

    #[test]
    fn relay_failure_uses_reported_direct_endpoint_with_the_same_keys() {
        for kind in [
            Failure::Transport,
            Failure::Authentication,
            Failure::Unknown,
        ] {
            let mut io = MockIo::new(&[Some(kind), None]);
            let connection = recover(&mut io, &instance(true), policy()).unwrap();
            assert_eq!(connection.endpoint.kind, "direct");
            assert_eq!(connection.endpoint.port, 4321);
            assert_eq!(connection.endpoint.host, "203.0.113.42");
            assert!(connection.command(None).contains("4321"));
            assert!(connection.identity.is_none());
            assert_eq!((io.attachments, io.keys_read, io.log_reads), (0, 0, 0));
            assert_eq!(io.probes.len(), 2);
        }
    }

    #[test]
    fn failed_direct_probe_keeps_its_own_cause_and_the_first_failure() {
        let mut io = MockIo::new(&[Some(Failure::Transport), Some(Failure::Unknown)]);
        let err = recover(&mut io, &instance(true), policy()).unwrap_err();
        let attempts = &details(&err).details["ssh"]["attempts"];
        assert_eq!(attempts[0]["failure"]["kind"], "transport");
        assert_eq!(attempts[1]["failure"]["kind"], "unknown");
        assert_eq!(attempts[1]["endpoint"]["kind"], "direct");
        assert!(err.to_string().contains("ssh1.vast.ai:1234"));
    }

    #[test]
    fn permission_evidence_stops_recovery_before_any_key_mutation() {
        let mut io = MockIo::new(&[Some(Failure::Authentication), Some(Failure::Authentication)]);
        io.permissions = Some(true);
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(details(&err).code, "ssh_server_permissions");
        assert_eq!(
            details(&err).details["ssh"]["provider_logs"],
            "sshd_permissions_rejected"
        );
        assert_eq!(io.attachments, 0);
        assert_eq!(io.log_reads, 1);
        assert_eq!(io.probes.len(), 2);
        assert!(err.to_string().contains("ownership/permissions"));
    }

    #[test]
    fn selecting_existing_identity_can_succeed_without_key_attachment() {
        let mut io = MockIo::new(&[Some(Failure::Authentication), None]);
        let connection = recover(&mut io, &instance(false), policy()).unwrap();
        assert_eq!(
            connection.identity.as_deref(),
            Some(Path::new("/tmp/test identity"))
        );
        assert_eq!(io.attachments, 0);
        assert_eq!(io.log_reads, 0);
    }

    #[test]
    fn a_genuinely_missing_key_is_attached_only_to_the_selected_instance() {
        let mut io = MockIo::new(&[
            Some(Failure::Authentication),
            Some(Failure::Authentication),
            None,
        ]);
        let connection = recover(&mut io, &instance(false), policy()).unwrap();
        assert_eq!(io.attachments, 1);
        assert_eq!(connection.diagnostics.key_attachment, "attached");
        assert_eq!(
            io.probes
                .iter()
                .map(|(_, explicit)| *explicit)
                .collect::<Vec<_>>(),
            [false, true, true]
        );
        assert_eq!(
            connection.access().identity_file,
            connection.identity.as_deref()
        );
    }

    #[test]
    fn already_associated_key_does_not_trigger_repeated_attachment_or_retries() {
        let mut io = MockIo::new(&[Some(Failure::Authentication), Some(Failure::Authentication)]);
        io.attach_status = InstanceSshKeyAttachStatus::AlreadyAssociated;
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(io.attachments, 1);
        assert_eq!(io.probes.len(), 2);
        assert_eq!(
            details(&err).details["ssh"]["key_attachment"],
            "already_associated"
        );
    }

    #[test]
    fn failed_log_lookup_retains_authentication_cause_and_next_diagnostic() {
        let mut io = MockIo::new(&[Some(Failure::Authentication)]);
        io.permissions = None;
        io.key_present = false;
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(details(&err).code, "ssh_authentication_failed");
        assert_eq!(details(&err).details["ssh"]["provider_logs"], "unavailable");
        assert_eq!(
            details(&err).details["ssh"]["next_command"],
            "ice logs --cloud vast.ai 42 --provider-logs --tail 200"
        );
        assert_eq!(io.attachments, 0);
    }

    #[test]
    fn newly_attached_key_has_only_three_propagation_probes() {
        let mut io = MockIo::new(&[Some(Failure::Authentication); 5]);
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(details(&err).code, "ssh_authentication_failed");
        assert_eq!(io.probes.len(), 5);
        assert_eq!(io.attachments, 1);
    }

    #[test]
    fn changed_failure_after_attachment_stops_authentication_retries() {
        let mut io = MockIo::new(&[
            Some(Failure::Authentication),
            Some(Failure::Authentication),
            Some(Failure::Transport),
        ]);
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert_eq!(details(&err).code, "ssh_transport_failed");
        assert_eq!(io.probes.len(), 3);
        assert_eq!(io.attachments, 1);
    }

    #[test]
    fn budgets_are_checked_before_probes_and_before_key_mutation() {
        let mut io = MockIo::new(&[]);
        let err = recover(
            &mut io,
            &instance(false),
            Policy {
                timeout: Duration::ZERO,
                ..policy()
            },
        )
        .unwrap_err();
        assert_eq!(details(&err).code, "ssh_recovery_timeout");
        assert!(io.probes.is_empty());
        let mut io = MockIo::new(&[Some(Failure::Authentication); 2]);
        let err = recover(
            &mut io,
            &instance(false),
            Policy {
                max_probes: 2,
                ..policy()
            },
        )
        .unwrap_err();
        assert_eq!(details(&err).code, "ssh_recovery_timeout");
        assert_eq!(io.attachments, 0);
    }

    #[test]
    fn remote_operation_failure_is_not_replayed_or_reclassified_as_a_key_error() {
        let mut io = MockIo::new(&[None]);
        let connection = recover(&mut io, &instance(false), policy()).unwrap();
        let mut actions = 0;
        let err = operation(&connection, |_| -> Result<()> {
            actions += 1;
            bail!("tmux failed with status 255: Permission denied (publickey)");
        })
        .unwrap_err();
        assert_eq!(actions, 1);
        assert_eq!(io.probes.len(), 1);
        assert_eq!(io.attachments, 0);
        assert_eq!(details(&err).code, "ssh_operation_failed");
        assert!(err.to_string().contains("tmux failed"));
    }

    #[test]
    fn ssh_diagnostics_distinguish_authentication_transport_and_remote_failure() {
        let cases = [
            (
                "sign_and_send_pubkey: signing failed for ED25519 from agent: agent refused operation\nPermission denied (publickey)",
                Failure::LocalSetup,
            ),
            (
                "WARNING: UNPROTECTED PRIVATE KEY FILE!\nPermission denied (publickey)",
                Failure::LocalSetup,
            ),
            (
                "Load key \"/tmp/id\": Permission denied\nPermission denied (publickey)",
                Failure::LocalSetup,
            ),
            (
                "root@host: Permission denied (publickey).",
                Failure::Authentication,
            ),
            (
                "Received disconnect: Too many authentication failures",
                Failure::Authentication,
            ),
            (
                "ssh: connect to host example port 22: Connection refused",
                Failure::Transport,
            ),
            ("Connection timed out", Failure::Transport),
            ("Connection reset by peer", Failure::Transport),
            ("Could not resolve hostname example", Failure::Transport),
            ("Host key verification failed.", Failure::HostKey),
            (
                "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!",
                Failure::HostKey,
            ),
            ("unrecognized SSH error", Failure::Unknown),
            ("", Failure::Unknown),
        ];
        for (log, kind) in cases {
            assert_eq!(classify(Some(255), log).kind, kind, "{log}");
        }
        assert_eq!(classify(Some(0), "").kind, Failure::Unknown);
        assert_eq!(classify(None, "").kind, Failure::Interrupted);
        assert!(authenticated(
            "Authenticated to example using publickey.",
            "example"
        ));
        assert!(authenticated(
            "Authenticated to example ([203.0.113.42]:4321) using publickey.",
            "example"
        ));
        assert!(!authenticated(
            "Authenticated to bastion using publickey.",
            "example"
        ));
        assert!(!authenticated(
            "Authenticated to example.attacker using publickey.",
            "example"
        ));
    }

    #[test]
    fn host_key_remote_command_and_local_failures_never_change_keys() {
        for failure in [
            Failure::HostKey,
            Failure::RemoteCommand,
            Failure::LocalSetup,
            Failure::Interrupted,
            Failure::Unknown,
        ] {
            let mut io = MockIo::new(&[Some(failure)]);
            let err = recover(&mut io, &instance(false), policy()).unwrap_err();
            assert_eq!(details(&err).code, failure.code());
            assert_eq!((io.attachments, io.keys_read, io.log_reads), (0, 0, 0));
        }
    }

    #[test]
    fn direct_ports_use_only_current_provider_mapping_and_are_not_guessed() {
        let mut instance = instance(true);
        assert_eq!(endpoints(&instance)[1].port, 4321);
        instance.ports = json!({"22/tcp": [{"HostPort": 7654}]});
        assert_eq!(endpoints(&instance)[1].port, 7654);
        for value in [
            Value::Null,
            json!({"22/tcp": [{"HostPort": "invalid"}]}),
            json!({"22/tcp": [{"HostPort": 65536}]}),
            json!({"22/tcp": [{"HostPort": 0}]}),
        ] {
            instance.ports = value;
            assert_eq!(endpoints(&instance).len(), 1);
        }
        instance.ports = json!({"22/tcp": [{"HostPort": "4321"}]});
        instance.ssh_host = instance.public_ipaddr.clone();
        instance.ssh_port = Some(4321);
        assert_eq!(endpoints(&instance).len(), 1);
        assert_eq!(endpoints(&instance)[0].kind, "direct");
        instance.ssh_host = None;
        instance.ssh_port = None;
        assert_eq!(endpoints(&instance).len(), 1);
    }

    #[test]
    fn only_explicit_sshd_permission_diagnostics_are_recognized() {
        assert!(logs_reject_permissions(
            "sshd: Authentication refused: bad ownership or modes for file /root/.ssh/authorized_keys"
        ));
        assert!(logs_reject_permissions(
            "sshd: Could not open user 'root' authorized keys '/root/.ssh/authorized_keys': Permission denied"
        ));
        assert!(!logs_reject_permissions(
            "root@host: Permission denied (publickey)"
        ));
        assert!(!logs_reject_permissions(
            "workload failed: permission denied opening /data/results"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn subprocess_probe_captures_diagnostics_and_closes_stdin() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "if read answer; then exit 9; fi; printf 'root@host: Permission denied (publickey).\\n' >&2; exit 255"]);
        let err = run_probe(command, Duration::from_secs(2), "example").unwrap_err();
        assert_eq!(err.kind, Failure::Authentication);
        assert_eq!(err.exit_code, Some(255));
    }

    #[cfg(unix)]
    #[test]
    fn a_hung_probe_is_terminated_within_its_budget() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "/bin/sleep 30 & wait"]);
        let start = Instant::now();
        let err = run_probe(command, Duration::from_millis(50), "example").unwrap_err();
        assert_eq!(err.kind, Failure::Transport);
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn authenticated_probe_finishes_without_running_a_remote_command() {
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "printf 'Authenticated to example using publickey.\\n' >&2; /bin/sleep 30 & wait",
        ]);
        let start = Instant::now();
        run_probe(command, Duration::from_secs(10), "example").unwrap();
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn remote_exit_status_is_preserved_without_recovery_or_replay() {
        use std::os::unix::process::ExitStatusExt;
        let mut io = MockIo::new(&[None]);
        let connection = recover(&mut io, &instance(false), policy()).unwrap();
        let err = operation(&connection, |_| {
            command_status(ExitStatus::from_raw(42 << 8))
        })
        .unwrap_err();
        assert_eq!(details(&err).code, "ssh_remote_command_failed");
        assert_eq!(details(&err).details["exit_code"], 42);
        assert_eq!(details(&err).details["endpoint"]["kind"], "relay");
        assert_eq!(io.probes.len(), 1);
        assert_eq!(io.attachments, 0);
        let err = operation(&connection, |_| {
            command_status(ExitStatus::from_raw(255 << 8))
        })
        .unwrap_err();
        assert_eq!(details(&err).code, "ssh_operation_failed");
    }

    #[cfg(unix)]
    #[test]
    fn probe_cancellation_child() {
        let Some(ready) = std::env::var_os("ICE_SSH_CANCEL_READY") else {
            return;
        };
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "printf ready > \"$1\"; /bin/sleep 30 & wait", "probe"])
            .arg(ready);
        let err = run_probe(command, Duration::from_secs(30), "example").unwrap_err();
        assert_eq!(err.kind, Failure::Interrupted);
        // Preserve cancellation typing through the structured error so the CLI
        // returns 130, and do not start another probe or perform a key mutation.
        let mut io = MockIo::new(&[]);
        let err = recover(&mut io, &instance(false), policy()).unwrap_err();
        assert!(capulus::error_is_cancelled(&err));
        assert_eq!(details(&err).code, "ssh_probe_interrupted");
    }

    #[cfg(unix)]
    #[test]
    fn interrupting_a_probe_stops_recovery_and_reaps_the_probe() {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "providers::vast::ssh::tests::probe_cancellation_child",
            ])
            .env("ICE_SSH_CANCEL_READY", &ready)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while !ready.exists() {
            if start.elapsed() > Duration::from_secs(5) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("probe did not start");
            }
            thread::sleep(Duration::from_millis(10));
        }
        // SAFETY: signal only the isolated test subprocess we just spawned.
        assert_eq!(
            unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) },
            0
        );
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "isolated cancellation test failed: {status}"
                );
                break;
            }
            if start.elapsed() > Duration::from_secs(5) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("cancellation did not stop the probe");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}
