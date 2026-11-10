//! Match existing local/agent keys to provider public keys; never register or edit keys.
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};

use super::{Instance, SshKey, client::Client, resource_path};
use crate::model::IceConfig;
use crate::ssh_probe::ProbeChild;

/// Reject missing local setup before authentication or catalog discovery. Dry-run
/// callers deliberately skip this: a quote does not require an SSH identity.
pub(super) fn creation_key_id(config: &IceConfig) -> Result<&str> {
    let id = config.default.verda.ssh_key_id.as_deref().filter(|id| !id.trim().is_empty()).ok_or_else(|| {
        crate::automation::error("ssh_key_required", "Select an existing Verda SSH key with --ssh-key-id or default.verda.ssh_key_id before creating a VM.",
            json!({"flag":"--ssh-key-id", "config_key":"default.verda.ssh_key_id", "resource_created":false}))
    })?;
    if !super::valid_id(id) {
        return Err(crate::automation::error(
            "invalid_ssh_key_id",
            "The Verda SSH key ID must be a full UUID from the provider console.",
            json!({"flag":"--ssh-key-id", "config_key":"default.verda.ssh_key_id", "resource_created":false}),
        ));
    }
    if let Some(path) = config.default.verda.ssh_key_path.as_deref()
        && !Path::new(path).is_file()
    {
        return Err(crate::automation::error(
            "ssh_identity_unavailable",
            "Configured Verda SSH private key path is not an accessible file. Update default.verda.ssh_key_path before creating a VM.",
            json!({"config_key":"default.verda.ssh_key_path", "resource_created":false}),
        ));
    }
    Ok(id)
}

fn public_key(value: &str) -> Option<(&str, &str)> {
    let mut fields = value.split_whitespace();
    let kind = fields.next()?;
    if !(kind.starts_with("ssh-") || kind.starts_with("ecdsa-") || kind.starts_with("sk-")) {
        return None;
    }
    Some((kind, fields.next()?))
}

fn candidate_paths(configured: Option<&Path>, directory: Option<&Path>) -> Result<Vec<PathBuf>> {
    if let Some(path) = configured {
        anyhow::ensure!(
            path.is_file(),
            "Configured Verda SSH private key path does not exist"
        );
        return Ok(vec![path.to_owned()]);
    }
    let mut paths = Vec::new();
    if let Some(directory) = directory.filter(|dir| dir.is_dir()) {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "pub") {
                let private = path.with_extension("");
                if private.is_file() {
                    paths.push(private);
                }
            }
        }
        for name in [
            "id_ed25519",
            "id_rsa",
            "id_ecdsa",
            "id_ed25519_sk",
            "id_ecdsa_sk",
        ] {
            let path = directory.join(name);
            if path.is_file() && !paths.contains(&path) {
                paths.push(path);
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn local_output(command: &mut Command, deadline: Instant) -> Result<Option<String>> {
    let cancellation = capulus::Cancellation::install()?;
    cancellation.check()?;
    if Instant::now() >= deadline {
        return Ok(None);
    }
    let mut output = tempfile::NamedTempFile::new()?;
    command
        .stdin(Stdio::null())
        .stdout(output.reopen()?)
        .stderr(Stdio::null())
        .env("SSH_ASKPASS_REQUIRE", "never")
        .env("LC_ALL", "C");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = match command.spawn() {
        Ok(child) => ProbeChild(child),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    loop {
        cancellation.check()?;
        if let Some(status) = child.0.try_wait()? {
            if !status.success() || output.as_file().metadata()?.len() > 64 * 1024 {
                return Ok(None);
            }
            let mut text = String::new();
            output
                .as_file_mut()
                .take(64 * 1024)
                .read_to_string(&mut text)?;
            return Ok(Some(text));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        cancellation.sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
        )?;
    }
}

fn choose_identity(
    registered: &[SshKey],
    paths: &[PathBuf],
    configured: bool,
    mut derive_public: impl FnMut(&Path) -> Result<Option<String>>,
    agent_keys: impl FnOnce() -> Result<Option<String>>,
) -> Result<Option<PathBuf>> {
    let keys = registered
        .iter()
        .filter_map(|key| key.key.as_deref().and_then(public_key))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !keys.is_empty(),
        "Verda SSH key metadata omitted usable public keys; inspect the selected key IDs"
    );
    let matches = |text: &str| {
        text.lines()
            .filter_map(public_key)
            .any(|key| keys.contains(&key))
    };
    let mut encrypted_candidate = None;
    for path in paths {
        if let Some(public) = derive_public(path)? {
            if matches(&public) {
                return Ok(Some(path.clone()));
            }
        } else {
            // An encrypted/hardware key can still be used through ssh-agent.
            // In that case require its public counterpart as well as an agent match.
            let mut public_path = path.as_os_str().to_os_string();
            public_path.push(".pub");
            if let Ok(public) = fs::read_to_string(PathBuf::from(public_path))
                && matches(&public)
            {
                encrypted_candidate = Some((path.clone(), public));
            }
        }
    }
    if let Some(agent) = agent_keys()? {
        if let Some((path, public)) = encrypted_candidate
            && agent
                .lines()
                .filter_map(public_key)
                .any(|key| Some(key) == public_key(&public))
        {
            return Ok(Some(path));
        }
        if !configured && matches(&agent) {
            return Ok(None);
        }
    }
    Err(crate::automation::error(
        "ssh_key_mismatch",
        "No usable local SSH identity matches the selected Verda public key. Set default.verda.ssh_key_path to its private key; load encrypted keys with ssh-add. This check did not change any instance or key.",
        json!({"ssh_key_ids":registered.iter().map(|key| &key.id).collect::<Vec<_>>(), "configured_identity":configured, "key_changed":false}),
    ))
}

pub(super) fn identity(config: &IceConfig, registered: &[SshKey]) -> Result<Option<PathBuf>> {
    crate::support::ensure_command_available("ssh")?;
    let configured = config.default.verda.ssh_key_path.as_deref().map(Path::new);
    let directory = dirs::home_dir().map(|home| home.join(".ssh"));
    let paths = candidate_paths(configured, directory.as_deref())?;
    let deadline = Instant::now() + Duration::from_secs(10);
    choose_identity(
        registered,
        &paths,
        configured.is_some(),
        |path| {
            local_output(
                Command::new("ssh-keygen")
                    .args(["-y", "-P", "", "-f"])
                    .arg(path),
                deadline,
            )
        },
        || local_output(Command::new("ssh-add").arg("-L"), deadline),
    )
}

pub(super) fn registered_key(client: &Client, id: &str) -> Result<SshKey> {
    let endpoint = resource_path("ssh-keys", id)?;
    let response: Value = client.get(&endpoint, Duration::from_secs(30))?;
    let invalid = |reason: &str| {
        crate::automation::error(
            "verda_ssh_key_response_invalid",
            "Verda returned invalid registered SSH key details. Expected one usable key with the requested ID; inspect the selected key in the provider console.",
            json!({"stage":"ssh_key_lookup", "method":"GET", "endpoint":endpoint,
                "ssh_key_id":id, "reason":reason, "key_changed":false}),
        )
    };
    // The live detail endpoint returns a singleton array. Also accept the bare
    // object used by earlier responses, without treating this as a key listing.
    let record = match response {
        Value::Object(_) => response,
        Value::Array(mut records) if records.len() == 1 => records.remove(0),
        Value::Array(_) => return Err(invalid("unexpected_key_count")),
        _ => return Err(invalid("invalid_schema")),
    };
    if !record.is_object() {
        return Err(invalid("invalid_schema"));
    }
    // Do not include deserialization errors or response values: a malformed
    // field can contain public-key material or other provider metadata.
    let key: SshKey = serde_json::from_value(record).map_err(|_| invalid("invalid_schema"))?;
    if key.id != id {
        return Err(invalid("id_mismatch"));
    }
    if key.key.as_deref().and_then(public_key).is_none() {
        return Err(invalid("unusable_public_key"));
    }
    Ok(key)
}

pub(super) fn creation_identity(
    client: &Client,
    config: &IceConfig,
    id: &str,
) -> Result<Option<PathBuf>> {
    let result = registered_key(client, id).and_then(|key| identity(config, &[key]));
    result.map_err(|err| {
        if capulus::error_is_cancelled(&err) {
            return err;
        }
        let cause = err.downcast_ref::<crate::automation::AgentError>();
        let mut details = cause.map_or_else(|| json!({}), |cause| cause.details.clone());
        details["stage"] = json!("ssh_key_preflight");
        details["method"] = json!("GET");
        details["endpoint"] = json!(resource_path("ssh-keys", id).ok());
        details["ssh_key_id"] = json!(id);
        details["resource_created"] = json!(false);
        details["instance_request_sent"] = json!(false);
        details["key_changed"] = json!(false);
        crate::automation::error(
            cause.map_or("ssh_key_preflight_failed", |cause| cause.code),
            format!("Verda SSH key preflight failed before VM creation: {err}"),
            details,
        )
    })
}

pub(super) fn instance_identity(
    client: &Client,
    config: &IceConfig,
    instance: &Instance,
) -> Result<Option<PathBuf>> {
    let ids = if instance.ssh_key_ids.is_empty() {
        config
            .default
            .verda
            .ssh_key_id
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    } else {
        instance.ssh_key_ids.clone()
    };
    if ids.is_empty() {
        return super::ssh::identity(config);
    }
    let registered = ids
        .iter()
        .map(|id| registered_key(client, id))
        .collect::<Result<Vec<_>>>()?;
    identity(config, &registered)
}

#[cfg(test)]
mod tests;
