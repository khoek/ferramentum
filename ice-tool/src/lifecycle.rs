//! Bounded, observable rental operations. Workload execution is outside this contract.
use std::cell::Cell;
use std::io::{Read, Seek, SeekFrom};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};

use crate::automation::{error, recovery};
use crate::providers::{CloudInstance, CloudProvider};

thread_local! { static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) }; }

pub(crate) struct Budget(Option<Instant>);
impl Budget {
    pub(crate) fn enter(timeout: Duration) -> Result<Self> {
        let end = Instant::now()
            .checked_add(timeout)
            .context("Lifecycle timeout is out of range")?;
        Ok(Self(
            DEADLINE.replace(Some(deadline().map_or(end, |old| old.min(end)))),
        ))
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        DEADLINE.set(self.0);
    }
}
pub(crate) fn deadline() -> Option<Instant> {
    DEADLINE.get()
}
pub(crate) fn remaining_timeout(limit: Duration) -> Result<Duration> {
    let remaining = deadline().map_or(limit, |d| {
        d.saturating_duration_since(Instant::now()).min(limit)
    });
    if remaining.is_zero() {
        return Err(error(
            "operation_timeout",
            "Lifecycle deadline elapsed; the provider operation may still complete.",
            json!({"mutation_retried":false}),
        ));
    }
    Ok(remaining)
}

/// Bound provider CLI execution too, not just the polling interval. Temporary
/// files avoid pipe deadlocks; errors never echo credentials or raw CLI output.
pub(crate) fn run_output(command: &mut Command, context: &str) -> Result<Output> {
    let cancellation = capulus::Cancellation::install()?;
    cancellation.check()?;
    remaining_timeout(Duration::from_secs(30))?;
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command
        .stdin(Stdio::null())
        .stdout(stdout.try_clone()?)
        .stderr(stderr.try_clone()?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = crate::ssh_probe::ProbeChild(
        command
            .spawn()
            .with_context(|| format!("Unable to {context}"))?,
    );
    let status = loop {
        cancellation.check()?;
        let remaining = remaining_timeout(Duration::from_millis(25))?;
        if let Some(status) = child.0.try_wait()? {
            break status;
        }
        cancellation.sleep(remaining)?;
    };
    if !status.success() {
        return Err(error(
            "provider_command_failed",
            format!("Unable to {context}; inspect provider state before repeating an operation."),
            json!({"exit_code":status.code(),"mutation_retried":false}),
        ));
    }
    fn read(file: &mut std::fs::File) -> Result<Vec<u8>> {
        anyhow::ensure!(
            file.metadata()?.len() <= 16 * 1024 * 1024,
            "Provider CLI response is too large"
        );
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    Ok(Output {
        status,
        stdout: read(&mut stdout)?,
        stderr: read(&mut stderr)?,
    })
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Start,
    Stop,
    Delete,
}
impl Action {
    pub(crate) fn command(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Delete => "delete",
        }
    }
    fn state(self) -> &'static str {
        match self {
            Self::Start => "running",
            Self::Stop => "stopped",
            Self::Delete => "deleted",
        }
    }
    fn reached<I: CloudInstance>(self, cloud: crate::model::Cloud, instance: Option<&I>) -> bool {
        match (self, instance) {
            (Self::Start, Some(i)) => i.is_running(),
            (Self::Stop, Some(i)) => i.is_stopped(),
            (Self::Delete, None) => true,
            (Self::Delete, Some(i)) => {
                (cloud == crate::model::Cloud::Aws
                    && i.state_value().eq_ignore_ascii_case("terminated"))
                    || (cloud == crate::model::Cloud::Verda
                        && i.state_value().eq_ignore_ascii_case("discontinued"))
            }
            _ => false,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct Operation {
    action: Action,
    pub(crate) instance_id: String,
    outcome: &'static str,
    state: &'static str,
    request: &'static str,
    verification: &'static str,
    elapsed_ms: u128,
    mutation_retried: bool,
}
pub(crate) fn verified(action: Action, id: String, request: &'static str, start: Instant) -> Value {
    serde_json::to_value(Operation {
        action,
        instance_id: id,
        outcome: "verified",
        state: action.state(),
        request,
        verification: "read_back",
        elapsed_ms: start.elapsed().as_millis(),
        mutation_retried: false,
    })
    .expect("operation serializes")
}

fn completed<I: CloudInstance>(
    cloud: crate::model::Cloud,
    action: Action,
    id: String,
    request: &'static str,
    start: Instant,
    instance: Option<&I>,
) -> Value {
    let mut result = verified(action, id, request, start);
    result["instance"] = instance.map_or(Value::Null, |i| i.json_summary());
    result["storage"] =
        json!({"verification":"unknown","deleted_volume_ids":null,"retained_volume_ids":null});
    if action == Action::Stop {
        result["billing_note"] = json!(if cloud == crate::model::Cloud::Verda {
            "Verda shutdown retains compute and storage billing."
        } else {
            "Stopped state does not verify storage cleanup or the end of all charges."
        });
    }
    if action == Action::Delete {
        result["billing_note"] = json!(
            "Instance removal is verified; storage cleanup and the end of all charges are not verified."
        );
    }
    result
}

/// `instance` must come from this invocation's fresh resolve_instance lookup.
pub(crate) fn transition<P: CloudProvider>(
    context: &P::ProviderContext<'_>,
    instance: &P::Instance,
    action: Action,
) -> Result<Value> {
    let start = Instant::now();
    let id = instance.json_summary()["id"]
        .as_str()
        .context("Instance summary is missing its ID")?
        .to_owned();
    let mut details = json!({"action":action,"instance_id":id,"instance":instance.json_summary(),
        "outcome":"unverified","mutation_retried":false,"reconcile_before_retry":true,
        "next_command":format!("ice list --cloud {} --json",P::CLOUD)});
    recovery("changing_state", details.clone());
    let cancellation = capulus::Cancellation::install()?;
    cancellation.check()?;
    remaining_timeout(Duration::from_secs(30))?;
    if action.reached(P::CLOUD, Some(instance)) {
        return Ok(completed(
            P::CLOUD,
            action,
            id,
            "not_needed",
            start,
            Some(instance),
        ));
    }
    // Mark the request as uncertain before issuing it, including cancellation.
    details["request"] = json!("unconfirmed");
    recovery("changing_state", details.clone());
    let result = match action {
        Action::Start => P::set_running(context, instance, true),
        Action::Stop => P::set_running(context, instance, false),
        Action::Delete => P::delete_instance(context, instance),
    };
    let request = if result.is_ok() {
        "acknowledged"
    } else {
        "unconfirmed"
    };
    if let Err(err) = result {
        if capulus::error_is_cancelled(&err) {
            return Err(err);
        }
        details["request_error_code"] = json!(
            err.downcast_ref::<crate::automation::AgentError>()
                .map(|e| e.code)
                .unwrap_or("provider_error")
        );
    }
    details["request"] = json!(request);
    recovery("verifying_state", details.clone());
    loop {
        cancellation.check()?;
        let observed = remaining_timeout(Duration::from_secs(30))
            .and_then(|_| P::observe_instance(context, instance));
        match observed {
            Ok(observed) if action.reached(P::CLOUD, observed.as_ref()) => {
                return Ok(completed(
                    P::CLOUD,
                    action,
                    id,
                    request,
                    start,
                    observed.as_ref(),
                ));
            }
            Ok(observed) => {
                details["observed_state"] = json!(observed.as_ref().map(|i| i.state_value()));
            }
            Err(err) if capulus::error_is_cancelled(&err) => return Err(err),
            Err(err) => {
                details["verification_error_code"] = json!(
                    err.downcast_ref::<crate::automation::AgentError>()
                        .map(|e| e.code)
                        .unwrap_or("provider_error")
                );
                details["elapsed_ms"] = json!(start.elapsed().as_millis());
                return Err(error(
                    "operation_unverified",
                    "The requested state was not verified. Inspect the resource before retrying; billing may continue.",
                    details,
                ));
            }
        }
        // Return through the common error path on the next iteration at deadline.
        if let Ok(wait) = remaining_timeout(Duration::from_secs(2)) {
            cancellation.sleep(wait)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_budgets_cannot_extend_deadlines_and_restore_the_parent() {
        assert!(deadline().is_none());
        let outer = Budget::enter(Duration::from_secs(1)).unwrap();
        let end = deadline();
        {
            let _inner = Budget::enter(Duration::from_secs(20)).unwrap();
            assert_eq!(deadline(), end);
        }
        assert_eq!(deadline(), end);
        drop(outer);
        assert!(deadline().is_none());
    }

    #[test]
    fn terminated_means_deleted_only_for_aws() {
        let vm = crate::providers::gcp::GcpInstance {
            status: "TERMINATED".into(),
            name: "ice-test".into(),
            zone: "test-a".into(),
            machine_type: "test".into(),
            creation_timestamp: None,
            last_start_timestamp: None,
            workload: None,
        };
        assert!(!Action::Delete.reached(crate::model::Cloud::Gcp, Some(&vm)));
        assert!(Action::Stop.reached(crate::model::Cloud::Gcp, Some(&vm)));
        assert!(Action::Delete.reached(crate::model::Cloud::Aws, Some(&vm)));
    }

    #[cfg(unix)]
    #[test]
    fn deadline_kills_and_reaps_a_hung_provider_process() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("pid");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "echo $$ > \"$1\"; exec /bin/sleep 30", "test"])
            .arg(&pid_path);
        let _budget = Budget::enter(Duration::from_millis(200)).unwrap();
        let started = Instant::now();
        let error = run_output(&mut command, "test hung CLI").unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::automation::AgentError>()
                .unwrap()
                .code,
            "operation_timeout"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        let pid: libc::pid_t = std::fs::read_to_string(pid_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: signal zero only checks process existence.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}
