use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use crate::support::{run_command_status, shell_quote_single};

pub(crate) struct RemoteAccess<'a> {
    pub(crate) user: &'a str,
    pub(crate) host: &'a str,
    pub(crate) port: Option<u16>,
    pub(crate) identity_file: Option<&'a Path>,
}

pub(crate) fn discover_local_ssh_keypair() -> Result<Option<(PathBuf, String)>> {
    let Some(home) = dirs::home_dir() else {
        return Ok(None);
    };
    let ssh_dir = home.join(".ssh");
    if !ssh_dir.is_dir() {
        return Ok(None);
    }

    let mut candidate_privates = Vec::new();
    for name in [
        "id_ed25519",
        "id_rsa",
        "id_ecdsa",
        "id_ed25519_sk",
        "id_ecdsa_sk",
        "id_dsa",
    ] {
        let path = ssh_dir.join(name);
        if path.is_file() {
            candidate_privates.push(path);
        }
    }

    for entry in
        fs::read_dir(&ssh_dir).with_context(|| format!("Failed to read {}", ssh_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("Failed to read entry in {}", ssh_dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("pub") {
            continue;
        }
        let private = path.with_extension("");
        if private.is_file()
            && !candidate_privates
                .iter()
                .any(|candidate| candidate == &private)
        {
            candidate_privates.push(private);
        }
    }

    for private in candidate_privates {
        let public = private.with_extension("pub");
        if !public.is_file() {
            continue;
        }
        if let Some(public_key) = read_first_ssh_public_key_line(&public)? {
            return Ok(Some((private, public_key)));
        }
    }

    Ok(None)
}

pub(crate) fn run_rsync_download(
    access: RemoteAccess<'_>,
    remote_path: &str,
    local_path: Option<&Path>,
    context: &str,
) -> Result<()> {
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let remote_spec = format!("{}@{}:{remote_path}", access.user, access.host);

    let mut command = Command::new("rsync");
    command
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport(&access))
        .arg(remote_spec)
        .arg(destination);
    run_command_status(&mut command, context)
}

pub(crate) fn run_rsync_upload(
    access: RemoteAccess<'_>,
    local_path: &Path,
    remote_path: &str,
    context: &str,
) -> Result<()> {
    let remote_spec = format!("{}@{}:{remote_path}/", access.user, access.host);

    let mut command = Command::new("rsync");
    command
        .arg("-az")
        .arg("--delete")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport(&access))
        .arg(format!("{}/", local_path.display()))
        .arg(remote_spec);
    run_command_status(&mut command, context)
}

pub(crate) fn run_rsync_upload_path(
    access: RemoteAccess<'_>,
    local_path: &Path,
    remote_path: Option<&str>,
    context: &str,
) -> Result<()> {
    let destination = remote_path.unwrap_or(".");
    let remote_spec = format!("{}@{}:{destination}", access.user, access.host);

    let mut command = Command::new("rsync");
    command
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport(&access))
        .arg(local_path)
        .arg(remote_spec);
    run_command_status(&mut command, context)
}

fn read_first_ssh_public_key_line(path: &Path) -> Result<Option<String>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if looks_like_ssh_public_key(trimmed) {
            return Ok(Some(trimmed.to_owned()));
        }
    }
    Ok(None)
}

fn looks_like_ssh_public_key(line: &str) -> bool {
    line.starts_with("ssh-")
        || line.starts_with("ecdsa-")
        || line.starts_with("sk-ssh-")
        || line.starts_with("sk-ecdsa-")
}

fn ssh_transport(access: &RemoteAccess<'_>) -> String {
    let mut parts = vec!["ssh".to_owned()];
    if let Some(identity) = access.identity_file {
        parts.push("-i".to_owned());
        parts.push(shell_quote_single(&identity.display().to_string()));
        parts.push("-o".to_owned());
        parts.push("IdentitiesOnly=yes".to_owned());
    }
    if let Some(port) = access.port {
        parts.push("-p".to_owned());
        parts.push(port.to_string());
    }
    parts.push("-o".to_owned());
    parts.push("StrictHostKeyChecking=accept-new".to_owned());
    parts.join(" ")
}
