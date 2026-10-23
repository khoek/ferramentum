//! Explicit, non-secret projections for the CLI's machine-readable output.
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{OnceLock, mpsc};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::model::{Cloud, CloudMachineCandidate, IceConfig, RuntimeCostEstimate};
use crate::providers::vast::VastOffer;
use crate::workload::InstanceWorkload;

pub(crate) const SCHEMA_VERSION: u64 = 2;

// A CLI process handles one command. Output policy is initialized once, before dispatch.
static JSON: OnceLock<bool> = OnceLock::new();
static LOG_CONTEXT: OnceLock<(Cloud, String)> = OnceLock::new();

pub(crate) fn initialize(json: bool) {
    let _ = JSON.set(json);
}

pub(crate) fn is_json() -> bool {
    JSON.get().copied().unwrap_or(false)
}

pub(crate) fn document(command: &str, cloud: impl Into<Option<Cloud>>, mut result: Value) -> Value {
    if command == "create"
        && let Some(selection) = crate::selection::snapshot()
    {
        result["selection"] = selection;
    }
    json!({"schema_version": SCHEMA_VERSION, "command": command, "cloud": cloud.into(), "result": result})
}

pub(crate) fn write_document(mut writer: impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut writer, value).context("Failed to write JSON output")?;
    writeln!(writer).context("Failed to finish JSON output")?;
    writer.flush().context("Failed to flush JSON output")
}

pub(crate) fn emit(command: &str, cloud: impl Into<Option<Cloud>>, result: Value) -> Result<()> {
    write_document(io::stdout().lock(), &document(command, cloud, result))
}

pub(crate) fn error(
    command: Option<&str>,
    cloud: Option<Cloud>,
    code: &str,
    message: &str,
) -> Result<()> {
    write_document(
        io::stdout().lock(),
        &json!({
            "schema_version": SCHEMA_VERSION, "command": command, "cloud": cloud,
            "error": {"code": code, "message": message},
        }),
    )
}

pub(crate) fn config_values(config: &IceConfig) -> Result<Value> {
    let raw = serde_json::to_value(config)?;
    let mut values = serde_json::Map::new();
    for &key in crate::config_store::supported_config_keys() {
        let value = key.split('.').fold(&raw, |value, part| &value[part]);
        let value = if matches!(
            key,
            "auth.vast_ai.api_key"
                | "auth.aws.access_key_id"
                | "auth.aws.secret_access_key"
                | "auth.verda.client_id"
                | "auth.verda.client_secret"
        ) && !value.is_null()
        {
            json!("<redacted>")
        } else {
            value.clone()
        };
        values.insert(key.to_owned(), value);
    }
    Ok(Value::Object(values))
}

pub(crate) fn begin_logs(cloud: Cloud, instance: &str) {
    if is_json() {
        let _ = LOG_CONTEXT.set((cloud, instance.to_owned()));
    }
}

pub(crate) fn streaming_logs() -> bool {
    LOG_CONTEXT.get().is_some()
}

pub(crate) fn log_event(event: Value) -> Result<()> {
    let (cloud, instance) = LOG_CONTEXT
        .get()
        .context("Log output was not initialized")?;
    let mut result = event;
    result["instance"] = json!(instance);
    emit("logs", *cloud, result)
}

pub(crate) fn log_text(stream: &str, text: &str, reset: bool) -> Result<()> {
    if text.is_empty() && !reset {
        return Ok(());
    }
    log_event(json!({"event": "data", "stream": stream, "text": text, "reset": reset}))
}

// Decode incrementally so multibyte UTF-8 split across reads is preserved. Invalid
// bytes are replaced, like provider text APIs; records are chunks, not log lines.
#[derive(Default)]
pub(crate) struct LogDecoder {
    pending: Vec<u8>,
}

impl LogDecoder {
    pub(crate) fn feed(
        &mut self,
        bytes: &[u8],
        finished: bool,
        mut emit: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        self.pending.extend_from_slice(bytes);
        let mut offset = 0;
        while offset < self.pending.len() {
            match std::str::from_utf8(&self.pending[offset..]) {
                Ok(text) => {
                    emit(text)?;
                    offset = self.pending.len();
                }
                Err(error) => {
                    let end = offset + error.valid_up_to();
                    if end > offset {
                        emit(std::str::from_utf8(&self.pending[offset..end])?)?;
                    }
                    offset = end;
                    if let Some(length) = error.error_len() {
                        emit("\u{fffd}")?;
                        offset += length;
                    } else {
                        break;
                    }
                }
            }
        }
        self.pending.drain(..offset);
        if finished && !self.pending.is_empty() {
            emit(&String::from_utf8_lossy(&self.pending))?;
            self.pending.clear();
        }
        Ok(())
    }
}

fn read_log_chunks(mut reader: impl Read, mut emit: impl FnMut(&str) -> Result<()>) -> Result<()> {
    let mut buffer = [0u8; 8192];
    let mut decoder = LogDecoder::default();
    loop {
        let count = match reader.read(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        decoder.feed(&buffer[..count], count == 0, &mut emit)?;
        if count == 0 {
            return Ok(());
        }
    }
}

pub(crate) fn run_log_command(command: &mut Command, context: &str) -> Result<()> {
    crate::automation::prepare_command(command);
    let cancellation =
        capulus::Cancellation::install().context("Failed to install log cancellation handler")?;
    // Provider CLIs may launch SSH as a child. Keep the entire log transport in
    // its own process group so a closed consumer pipe can stop both processes.
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(command, 0);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to {context}"))?;
    let stdout = child.stdout.take().context("Missing log stdout")?;
    let stderr = child.stderr.take().context("Missing log stderr")?;
    // Bounded buffering and concurrent readers keep either pipe from blocking
    // the other. A broken output pipe stops the subprocess and reaps it.
    let status = std::thread::scope(|scope| -> Result<_> {
        let (sender, receiver) = mpsc::sync_channel::<Result<(&str, String)>>(16);
        for (reader, stream) in [
            (Box::new(stdout) as Box<dyn Read + Send>, "stdout"),
            (Box::new(stderr) as Box<dyn Read + Send>, "stderr"),
        ] {
            let sender = sender.clone();
            scope.spawn(move || {
                let result = read_log_chunks(reader, |text| {
                    sender
                        .send(Ok((stream, text.to_owned())))
                        .map_err(|_| anyhow::anyhow!("Log output closed"))
                });
                if let Err(error) = result {
                    let _ = sender.send(Err(error));
                }
            });
        }
        drop(sender);
        let result = loop {
            if let Err(error) = cancellation.check() {
                break Err(error.into());
            }
            match receiver.recv_timeout(std::time::Duration::from_millis(50)) {
                Ok(message) => {
                    if let Err(error) =
                        message.and_then(|(stream, text)| log_text(stream, &text, false))
                    {
                        break Err(error);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
            }
        };
        drop(receiver);
        if result.is_err() {
            #[cfg(unix)]
            // SAFETY: this group was created by the spawn above. The child has
            // not been reaped, so its PID cannot have been reused for another group.
            unsafe {
                libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = child.kill();
        }
        let status = child
            .wait()
            .with_context(|| format!("Failed to wait for {context}"));
        result?;
        status
    })?;
    if !status.success() {
        bail!("Failed to {context}: command exited with status {status}");
    }
    Ok(())
}

pub(crate) fn cost(cost: &RuntimeCostEstimate) -> Value {
    json!({
        "currency": "USD", "hourly_usd": cost.hourly_usd,
        "requested_hours": cost.requested_hours, "scheduled_hours": cost.billed_hours,
        "estimated_total_usd": cost.total_usd,
    })
}

pub(crate) fn workload(workload: Option<&InstanceWorkload>) -> Value {
    match workload {
        Some(InstanceWorkload::Shell) => json!({"kind": "shell"}),
        Some(InstanceWorkload::Container(image)) => {
            json!({"kind": "container", "image": image.container_ref()})
        }
        Some(InstanceWorkload::Unpack(source)) => json!({"kind": "unpack", "source": source}),
        None => Value::Null,
    }
}

pub(crate) fn vast_offer(offer: &VastOffer) -> Value {
    json!({
        "offer_id": offer.id, "gpu_model": offer.gpu_name,
        "machine_id": offer.machine_id, "host_id": offer.host_id,
        "driver_version": offer.driver_version, "vms_enabled": offer.vms_enabled,
        "gpu_memory_gb": offer.gpu_ram.map(|v| v / 1000.0),
        "available_disk_gb": offer.disk_space,
        "download_mbps": offer.inet_down, "upload_mbps": offer.inet_up,
        "download_usd_per_gb": offer.inet_down_cost, "upload_usd_per_gb": offer.inet_up_cost,
        "num_gpus": offer.num_gpus, "cpu_cores": offer.cpu_cores_effective,
        "ram_mb": offer.cpu_ram, "hourly_usd": offer.hourly_price(),
        "provider_hourly_usd": offer.dph_total,
        "quoted_total_hourly_usd": offer.quoted_total_hourly_price(),
        "available_seconds": offer.duration, "location": offer.geolocation,
        "verification": offer.verification, "reliability": offer.reliability,
    })
}

pub(crate) fn machine(candidate: &CloudMachineCandidate) -> Value {
    json!({
        "machine_type": candidate.machine, "cpu_cores": candidate.vcpus,
        "ram_mb": candidate.ram_mb, "gpus": candidate.gpus,
        "hourly_usd": candidate.hourly_usd, "region": candidate.region,
        "zone": candidate.zone,
    })
}

pub(crate) fn allocated_disk_gb(config: &IceConfig, cloud: Cloud) -> Option<u32> {
    match cloud {
        Cloud::Gcp => Some(config.default.gcp.boot_disk_gb.unwrap_or(50)),
        // Without an override, AWS uses the chosen AMI's root volume size.
        Cloud::Aws => config.default.aws.root_disk_gb,
        Cloud::VastAi => Some(
            config
                .default
                .vast_ai
                .disk_gb
                .unwrap_or(crate::support::VAST_DEFAULT_DISK_GB as u32),
        ),
        Cloud::Verda => Some(config.default.verda.disk_gb.unwrap_or(100)),
        Cloud::Local => None,
    }
}

pub(crate) fn connection(cloud: Cloud, instance: Value, command: &str) -> Result<()> {
    emit(
        "shell",
        cloud,
        json!({"instance": instance, "connect_command": command}),
    )
}

pub(crate) fn command_error(
    command: Option<&str>,
    cloud: Option<Cloud>,
    error: &anyhow::Error,
    message: &str,
) -> Result<()> {
    let typed = error.downcast_ref::<crate::automation::AgentError>();
    let mut details = typed
        .map(|err| err.details.clone())
        .unwrap_or_else(|| json!({}));
    let recovery = crate::automation::recovery_details();
    if !recovery.is_null() {
        details["recovery"] = recovery;
    }
    if let Some(selection) = crate::selection::snapshot() {
        details["selection"] = selection;
    }
    write_document(
        io::stdout().lock(),
        &json!({"schema_version": SCHEMA_VERSION, "command": command, "cloud": cloud,
        "error": {"code": typed.map_or("command_failed", |err| err.code), "message": message, "details": details}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_decoder_preserves_split_unicode_and_replaces_invalid_bytes() {
        let mut decoder = LogDecoder::default();
        let mut output = String::new();
        for byte in "hello 🦀\n終".as_bytes() {
            decoder
                .feed(&[*byte], false, |text| {
                    output.push_str(text);
                    Ok(())
                })
                .unwrap();
        }
        decoder
            .feed(&[0xff, 0xe2], true, |text| {
                output.push_str(text);
                Ok(())
            })
            .unwrap();
        assert_eq!(output, "hello 🦀\n終\u{fffd}\u{fffd}");
    }

    #[test]
    fn output_is_one_json_document_with_typed_values_and_newline() {
        let expected = document(
            "list",
            Cloud::VastAi,
            json!({"instances": [], "cost": null}),
        );
        let mut bytes = Vec::new();
        write_document(&mut bytes, &expected).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
        assert_eq!(expected["schema_version"], 2);
        assert!(
            expected["result"]["instances"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(expected["result"]["cost"].is_null());
    }

    #[test]
    fn output_reports_write_failures() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("broken pipe"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert!(write_document(Broken, &json!({})).is_err());
    }

    #[test]
    fn unknown_aws_disk_size_is_not_reported_as_an_invented_default() {
        let mut config = IceConfig::default();
        assert_eq!(allocated_disk_gb(&config, Cloud::Aws), None);
        config.default.aws.root_disk_gb = Some(80);
        assert_eq!(allocated_disk_gb(&config, Cloud::Aws), Some(80));
        assert_eq!(allocated_disk_gb(&config, Cloud::Gcp), Some(50));
    }

    #[test]
    fn vast_quote_and_output_do_not_expose_unrelated_provider_fields() {
        let offer: VastOffer = serde_json::from_value(json!({
            "id": 123, "gpu_name": "RTX 5060 Ti", "num_gpus": 1,
            "machine_id": 456, "host_id": 789, "driver_version": "595.71.05",
            "vms_enabled": false,
            "dph_total": 0.12, "search": {"totalHour": 0.15},
            "image_login": "secret-registry-token", "api_key": "secret-api-token"
        }))
        .unwrap();
        let value = vast_offer(&offer);
        assert_eq!(value["offer_id"], 123);
        assert_eq!(value["num_gpus"], 1);
        assert_eq!(value["machine_id"], 456);
        assert_eq!(value["host_id"], 789);
        assert_eq!(value["driver_version"], "595.71.05");
        assert_eq!(value["vms_enabled"], false);
        assert!(value.get("profiling_access").is_none());
        assert_eq!(value["hourly_usd"], 0.15);
        assert_eq!(value["provider_hourly_usd"], 0.12);
        assert_eq!(value["quoted_total_hourly_usd"], 0.15);
        assert_eq!(offer.quoted_total_hourly_price(), Some(0.15));
        assert!(!value.to_string().contains("secret-"));
        let incomplete: VastOffer = serde_json::from_value(json!({"id": 456})).unwrap();
        assert_eq!(incomplete.quoted_total_hourly_price(), None);
        let incomplete = vast_offer(&incomplete);
        for field in ["machine_id", "host_id", "driver_version", "vms_enabled"] {
            assert_eq!(incomplete[field], Value::Null);
        }
    }
}
