use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use crate::arca::resolve_local_arca_artifact;
use crate::local::{detect_local_container_runtime, local_registry_login};
use crate::model::IceConfig;
use crate::providers::{aws::AwsInstance, gcp::GcpInstance, vast::VastInstance};
use crate::support::{
    ICE_UNPACK_ENV_SCRIPT, ICE_UNPACK_EXIT_CODE_FILE, ICE_UNPACK_LOG_FILE, ICE_UNPACK_PID_FILE,
    ICE_UNPACK_ROOT_DIR, ICE_UNPACK_ROOTFS_DIR, ICE_UNPACK_RUN_SCRIPT, ICE_UNPACK_SHELL_SCRIPT,
    now_unix_secs, run_command_status, shell_quote_single, spinner,
};
use crate::workload::{
    ContainerImageReference, normalize_workload_source, wrap_remote_shell_script,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedUnpackSource {
    Archive(PathBuf),
    LocalImage(String),
    RemoteContainer(ContainerImageReference),
}

#[derive(Debug, Deserialize)]
struct SavedImageManifestRow {
    #[serde(rename = "Config")]
    config: String,
    #[serde(rename = "Layers")]
    layers: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SavedImageConfigFile {
    #[serde(default)]
    config: SavedImageRuntimeConfig,
}

#[derive(Debug, Default, Deserialize)]
struct SavedImageRuntimeConfig {
    #[serde(default, rename = "Entrypoint")]
    entrypoint: Option<Vec<String>>,
    #[serde(default, rename = "Cmd")]
    cmd: Option<Vec<String>>,
    #[serde(default, rename = "Env")]
    env: Option<Vec<String>>,
    #[serde(default, rename = "WorkingDir")]
    working_dir: Option<String>,
}

#[derive(Debug)]
pub(crate) struct SavedImageBundle {
    pub(crate) command: Vec<String>,
    pub(crate) working_dir: Option<String>,
    pub(crate) env: Vec<String>,
    pub(crate) layers: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct MaterializedUnpackBundle {
    pub(crate) root: PathBuf,
    pub(crate) working_dir: Option<String>,
}

fn resolve_unpack_source_inner(source: &str) -> Result<ResolvedUnpackSource> {
    if let Some(selector) = crate::arca::parse_arca_source(source) {
        let artifact = resolve_local_arca_artifact(selector)?;
        return Ok(ResolvedUnpackSource::LocalImage(artifact.local_tag));
    }
    let source = normalize_workload_source(source)?;
    let path = Path::new(&source);
    if path.exists() {
        return Ok(ResolvedUnpackSource::Archive(path.to_path_buf()));
    }
    if let Ok(container) = ContainerImageReference::from_container_ref(&source) {
        return Ok(ResolvedUnpackSource::RemoteContainer(container));
    }
    Ok(ResolvedUnpackSource::LocalImage(source))
}

pub(crate) fn materialize_unpack_bundle(
    config: &IceConfig,
    source: &str,
) -> Result<MaterializedUnpackBundle> {
    let root = create_temp_dir("ice-unpack")?;
    let bundle = materialize_unpack_bundle_in(config, source, &root);
    if bundle.is_err() {
        let _ = fs::remove_dir_all(&root);
    }
    bundle
}

pub(crate) fn materialize_unpack_bundle_in(
    config: &IceConfig,
    source: &str,
    root: &Path,
) -> Result<MaterializedUnpackBundle> {
    let source = resolve_unpack_source_inner(source)?;
    let archive_path = match source {
        ResolvedUnpackSource::Archive(path) => path,
        ResolvedUnpackSource::LocalImage(image) => {
            let archive_path = root.join("image.tar");
            save_local_image_archive(&image, &archive_path)?;
            archive_path
        }
        ResolvedUnpackSource::RemoteContainer(container) => {
            let archive_path = root.join("image.tar");
            save_remote_image_archive(config, &container, &archive_path)?;
            archive_path
        }
    };

    let bundle = load_saved_image_bundle(&archive_path)?;
    let rootfs_dir = root.join(ICE_UNPACK_ROOTFS_DIR);
    fs::create_dir_all(&rootfs_dir).with_context(|| {
        format!(
            "Failed to create unpack rootfs dir: {}",
            rootfs_dir.display()
        )
    })?;
    extract_saved_image_layers(&archive_path, &bundle.layers, &rootfs_dir)?;
    if archive_path.starts_with(root) {
        fs::remove_file(&archive_path).with_context(|| {
            format!("Failed to remove staged archive {}", archive_path.display())
        })?;
    }

    let env_script = render_unpack_env_script(&bundle.env, bundle.working_dir.as_deref())?;
    fs::write(root.join(ICE_UNPACK_ENV_SCRIPT), env_script).with_context(|| {
        format!(
            "Failed to write {}",
            root.join(ICE_UNPACK_ENV_SCRIPT).display()
        )
    })?;
    fs::write(
        root.join(ICE_UNPACK_RUN_SCRIPT),
        render_unpack_run_script(&bundle.command),
    )
    .with_context(|| {
        format!(
            "Failed to write {}",
            root.join(ICE_UNPACK_RUN_SCRIPT).display()
        )
    })?;
    fs::write(
        root.join(ICE_UNPACK_SHELL_SCRIPT),
        render_unpack_shell_script(),
    )
    .with_context(|| {
        format!(
            "Failed to write {}",
            root.join(ICE_UNPACK_SHELL_SCRIPT).display()
        )
    })?;

    Ok(MaterializedUnpackBundle {
        root: root.to_path_buf(),
        working_dir: bundle.working_dir,
    })
}

pub(crate) fn create_temp_dir(prefix: &str) -> Result<PathBuf> {
    let base = std::env::temp_dir();
    for attempt in 0..1024u32 {
        let path = base.join(format!(
            "{prefix}-{}-{}-{attempt}",
            now_unix_secs(),
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("Failed to create temporary dir: {}", path.display())
                });
            }
        }
    }
    bail!("Failed to allocate a unique temporary dir for unpack workload staging.")
}

fn save_local_image_archive(image: &str, output_path: &Path) -> Result<()> {
    let runtime = detect_local_container_runtime()?;
    let spinner = spinner(&format!("Saving local image {image}..."));
    let mut command = runtime.command();
    command.arg("save").arg("-o").arg(output_path).arg(image);
    run_command_status(&mut command, "save local container image")?;
    spinner.finish_with_message(format!("Saved {image}."));
    Ok(())
}

fn save_remote_image_archive(
    config: &IceConfig,
    container: &ContainerImageReference,
    output_path: &Path,
) -> Result<()> {
    let runtime = detect_local_container_runtime()?;
    let registry_auth = crate::providers::gcp::registry_login(config)?;
    let login_spinner = spinner("Logging into container registry for unpack...");
    local_registry_login(&runtime, container, &registry_auth)?;
    login_spinner.finish_with_message("Registry login succeeded.");

    let pull_spinner = spinner(&format!(
        "Pulling {} locally for unpack...",
        container.container_ref()
    ));
    let mut pull = runtime.command();
    pull.arg("pull").arg(container.container_ref());
    run_command_status(&mut pull, "pull unpack workload container")?;
    pull_spinner.finish_with_message(format!("Pulled {}.", container.container_ref()));

    let save_spinner = spinner(&format!("Saving {}...", container.container_ref()));
    let mut save = runtime.command();
    save.arg("save")
        .arg("-o")
        .arg(output_path)
        .arg(container.container_ref());
    run_command_status(&mut save, "save unpack workload container")?;
    save_spinner.finish_with_message(format!("Saved {}.", container.container_ref()));
    Ok(())
}

pub(crate) fn load_saved_image_bundle(archive_path: &Path) -> Result<SavedImageBundle> {
    let manifest_bytes = read_saved_archive_entry_bytes(archive_path, "manifest.json")?;
    let manifest_rows = serde_json::from_slice::<Vec<SavedImageManifestRow>>(&manifest_bytes)
        .with_context(|| {
            format!(
                "Failed to parse manifest.json from saved image archive {}",
                archive_path.display()
            )
        })?;
    let manifest = manifest_rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Saved image archive manifest.json was empty."))?;
    let config_bytes = read_saved_archive_entry_bytes(archive_path, &manifest.config)?;
    let config =
        serde_json::from_slice::<SavedImageConfigFile>(&config_bytes).with_context(|| {
            format!(
                "Failed to parse image config {} from {}",
                manifest.config,
                archive_path.display()
            )
        })?;
    let command = build_saved_image_command(&config.config)?;
    let executable_path =
        resolve_saved_image_executable_path(&command[0], config.config.working_dir.as_deref())?;
    let start_layer =
        find_entrypoint_layer_index(archive_path, &manifest.layers, &executable_path)?;
    Ok(SavedImageBundle {
        command,
        working_dir: config
            .config
            .working_dir
            .filter(|value| !value.trim().is_empty()),
        env: config.config.env.unwrap_or_default(),
        layers: manifest.layers[start_layer..].to_vec(),
    })
}

fn read_saved_archive_entry_bytes(archive_path: &Path, entry_name: &str) -> Result<Vec<u8>> {
    let target = clean_tar_path(Path::new(entry_name))?;
    let file = fs::File::open(archive_path).with_context(|| {
        format!(
            "Failed to open saved image archive {}",
            archive_path.display()
        )
    })?;
    let mut archive = tar::Archive::new(file);
    for entry in archive
        .entries()
        .context("Failed to read saved image archive entries")?
    {
        let mut entry = entry.context("Failed to read saved image archive entry")?;
        if clean_tar_path(&entry.path().context("Failed to read archive entry path")?)? != target {
            continue;
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("Failed to read archive entry {entry_name}"))?;
        return Ok(bytes);
    }
    bail!(
        "Saved image archive {} did not contain {}.",
        archive_path.display(),
        entry_name
    )
}

fn build_saved_image_command(config: &SavedImageRuntimeConfig) -> Result<Vec<String>> {
    let mut command = config.entrypoint.clone().unwrap_or_default();
    if command.is_empty() {
        command = config.cmd.clone().unwrap_or_default();
    } else if let Some(cmd) = config.cmd.as_ref() {
        command.extend(cmd.iter().cloned());
    }
    if command.is_empty() {
        bail!("Saved image has neither an entrypoint nor a command.");
    }
    Ok(command)
}

fn resolve_saved_image_executable_path(
    command: &str,
    working_dir: Option<&str>,
) -> Result<PathBuf> {
    if command.starts_with('/') {
        return clean_tar_path(Path::new(command.trim_start_matches('/')));
    }
    if command.contains('/') {
        let relative =
            if let Some(working_dir) = working_dir.filter(|value| !value.trim().is_empty()) {
                Path::new(working_dir.trim_start_matches('/')).join(command)
            } else {
                PathBuf::from(command)
            };
        return clean_tar_path(&relative);
    }
    bail!(
        "Unpack mode only supports images whose executable is an absolute path or a relative path with `/`. Bare command `{command}` is ambiguous."
    );
}

fn find_entrypoint_layer_index(
    archive_path: &Path,
    layers: &[String],
    executable_path: &Path,
) -> Result<usize> {
    for (index, layer) in layers.iter().enumerate() {
        if saved_layer_contains_path(archive_path, layer, executable_path)? {
            return Ok(index);
        }
    }
    bail!(
        "Could not find executable {} in any saved image layer from {}.",
        executable_path.display(),
        archive_path.display()
    )
}

fn saved_layer_contains_path(
    archive_path: &Path,
    layer_name: &str,
    target_path: &Path,
) -> Result<bool> {
    let target = clean_tar_path(target_path)?;
    let file = fs::File::open(archive_path).with_context(|| {
        format!(
            "Failed to open saved image archive {}",
            archive_path.display()
        )
    })?;
    let mut archive = tar::Archive::new(file);
    for entry in archive
        .entries()
        .context("Failed to read saved image archive entries")?
    {
        let entry = entry.context("Failed to read saved image archive entry")?;
        if clean_tar_path(&entry.path().context("Failed to read archive entry path")?)?
            != clean_tar_path(Path::new(layer_name))?
        {
            continue;
        }
        let mut layer_archive = tar::Archive::new(entry);
        for layer_entry in layer_archive
            .entries()
            .with_context(|| format!("Failed to enumerate image layer {layer_name}"))?
        {
            let layer_entry = layer_entry
                .with_context(|| format!("Failed to read image layer entry from {layer_name}"))?;
            if clean_tar_path(
                &layer_entry
                    .path()
                    .context("Failed to read layer entry path")?,
            )? == target
            {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    bail!(
        "Saved image archive {} did not contain layer {}.",
        archive_path.display(),
        layer_name
    )
}

pub(crate) fn extract_saved_image_layers(
    archive_path: &Path,
    layers: &[String],
    output_dir: &Path,
) -> Result<()> {
    for layer in layers {
        apply_saved_image_layer(archive_path, layer, output_dir)?;
    }
    Ok(())
}

fn apply_saved_image_layer(archive_path: &Path, layer_name: &str, output_dir: &Path) -> Result<()> {
    let file = fs::File::open(archive_path).with_context(|| {
        format!(
            "Failed to open saved image archive {}",
            archive_path.display()
        )
    })?;
    let mut archive = tar::Archive::new(file);
    for entry in archive
        .entries()
        .context("Failed to read saved image archive entries")?
    {
        let entry = entry.context("Failed to read saved image archive entry")?;
        if clean_tar_path(&entry.path().context("Failed to read archive entry path")?)?
            != clean_tar_path(Path::new(layer_name))?
        {
            continue;
        }
        let mut layer_archive = tar::Archive::new(entry);
        for layer_entry in layer_archive
            .entries()
            .with_context(|| format!("Failed to enumerate image layer {layer_name}"))?
        {
            let layer_entry = layer_entry
                .with_context(|| format!("Failed to read image layer entry from {layer_name}"))?;
            apply_saved_image_entry(layer_entry, output_dir)?;
        }
        return Ok(());
    }
    bail!(
        "Saved image archive {} did not contain layer {}.",
        archive_path.display(),
        layer_name
    )
}

fn apply_saved_image_entry<R: Read>(mut entry: tar::Entry<'_, R>, output_dir: &Path) -> Result<()> {
    let path = clean_tar_path(&entry.path().context("Failed to read layer entry path")?)?;
    if path.as_os_str().is_empty() {
        return Ok(());
    }

    let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or("");
    if file_name == ".wh..wh..opq" {
        let directory = output_dir.join(path.parent().unwrap_or_else(|| Path::new("")));
        clear_directory_children(&directory)?;
        return Ok(());
    }
    if let Some(target_name) = file_name.strip_prefix(".wh.") {
        let whiteout_target = output_dir
            .join(path.parent().unwrap_or_else(|| Path::new("")))
            .join(target_name);
        remove_path_if_exists(&whiteout_target)?;
        return Ok(());
    }

    let destination = output_dir.join(&path);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    let entry_type = entry.header().entry_type();
    if destination.exists() && !entry_type.is_dir() {
        remove_path_if_exists(&destination)?;
    } else if destination.exists() && entry_type.is_dir() && !destination.is_dir() {
        remove_path_if_exists(&destination)?;
    }
    entry.unpack(&destination).with_context(|| {
        format!(
            "Failed to unpack layer entry into {}",
            destination.display()
        )
    })?;
    Ok(())
}

pub(crate) fn clean_tar_path(path: &Path) -> Result<PathBuf> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir | std::path::Component::RootDir => {}
            std::path::Component::Normal(part) => clean.push(part),
            std::path::Component::ParentDir => {
                bail!(
                    "Saved image archive contained a parent-directory path: {}",
                    path.display()
                )
            }
            std::path::Component::Prefix(_) => {
                bail!(
                    "Saved image archive contained a prefixed path: {}",
                    path.display()
                )
            }
        }
    }
    Ok(clean)
}

fn clear_directory_children(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path).with_context(|| format!("Failed to read {}", path.display()))? {
        let entry = entry.with_context(|| format!("Failed to read child of {}", path.display()))?;
        remove_path_if_exists(&entry.path())?;
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("Failed to stat {}", path.display()))?;
    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path).with_context(|| format!("Failed to remove {}", path.display()))?;
        return Ok(());
    }
    if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("Failed to remove {}", path.display()))?;
    }
    Ok(())
}

fn render_unpack_env_script(env: &[String], working_dir: Option<&str>) -> Result<String> {
    let mut script = String::from(
        "ROOT=\"${ICE_UNPACK_STATE_DIR:?missing ICE_UNPACK_STATE_DIR}\"\nexport ICE_UNPACK_ROOTFS=\"$ROOT/rootfs\"\n",
    );
    script.push_str(&format!(
        "export ICE_UNPACK_WORKDIR={}\n",
        shell_quote_single(working_dir.unwrap_or(""))
    ));
    for assignment in env {
        let (key, value) = assignment
            .split_once('=')
            .unwrap_or((assignment.as_str(), ""));
        if !is_valid_shell_env_name(key) {
            bail!("Saved image contained unsupported environment variable name `{key}`.");
        }
        script.push_str(&format!("export {key}={}\n", shell_quote_single(value)));
    }
    Ok(script)
}

pub(crate) fn render_unpack_run_script(command: &[String]) -> String {
    let executable = unpack_command_executable(command.first().map(String::as_str).unwrap_or(""));
    let args = command
        .iter()
        .skip(1)
        .map(|arg| shell_quote_single(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let args_line = if args.is_empty() {
        String::new()
    } else {
        format!(" {args}")
    };
    format!(
        r#"#!/bin/sh
set -eu
ICE_UNPACK_STATE_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
export ICE_UNPACK_STATE_DIR
. "$ICE_UNPACK_STATE_DIR/{env_script}"
rm -f "$ICE_UNPACK_STATE_DIR/{pid_file}" "$ICE_UNPACK_STATE_DIR/{exit_code_file}"
: > "$ICE_UNPACK_STATE_DIR/{log_file}"
if [ -n "$ICE_UNPACK_WORKDIR" ] && [ -d "$ICE_UNPACK_ROOTFS$ICE_UNPACK_WORKDIR" ]; then
  cd "$ICE_UNPACK_ROOTFS$ICE_UNPACK_WORKDIR"
else
  cd "$ICE_UNPACK_ROOTFS"
fi
exec_path={executable}
set +e
"$exec_path"{args_line} >>"$ICE_UNPACK_STATE_DIR/{log_file}" 2>&1 &
child_pid=$!
printf '%s\n' "$child_pid" > "$ICE_UNPACK_STATE_DIR/{pid_file}"
trap 'kill "$child_pid" >/dev/null 2>&1 || true' INT TERM HUP
wait "$child_pid"
status=$?
set -e
rm -f "$ICE_UNPACK_STATE_DIR/{pid_file}"
printf '%s\n' "$status" > "$ICE_UNPACK_STATE_DIR/{exit_code_file}"
exit "$status"
"#,
        env_script = ICE_UNPACK_ENV_SCRIPT,
        pid_file = ICE_UNPACK_PID_FILE,
        exit_code_file = ICE_UNPACK_EXIT_CODE_FILE,
        log_file = ICE_UNPACK_LOG_FILE,
        executable = executable,
        args_line = args_line,
    )
}

fn unpack_command_executable(command: &str) -> String {
    if command.starts_with('/') {
        return format!("\"$ICE_UNPACK_ROOTFS/{}\"", command.trim_start_matches('/'));
    }
    shell_quote_single(command)
}

fn render_unpack_shell_script() -> String {
    format!(
        r#"#!/bin/sh
set -eu
ICE_UNPACK_STATE_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
export ICE_UNPACK_STATE_DIR
. "$ICE_UNPACK_STATE_DIR/{env_script}"
if [ -n "$ICE_UNPACK_WORKDIR" ] && [ -d "$ICE_UNPACK_ROOTFS$ICE_UNPACK_WORKDIR" ]; then
  cd "$ICE_UNPACK_ROOTFS$ICE_UNPACK_WORKDIR"
else
  cd "$ICE_UNPACK_ROOTFS"
fi
if command -v bash >/dev/null 2>&1; then
  exec bash -l
fi
exec sh
"#,
        env_script = ICE_UNPACK_ENV_SCRIPT,
    )
}

fn is_valid_shell_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

pub(crate) fn pack_directory_as_tar(input_dir: &Path, output_path: &Path) -> Result<()> {
    let file = fs::File::create(output_path)
        .with_context(|| format!("Failed to create {}", output_path.display()))?;
    let mut archive = tar::Builder::new(file);
    archive.follow_symlinks(false);
    archive
        .append_dir_all(".", input_dir)
        .with_context(|| format!("Failed to pack {}", input_dir.display()))?;
    archive
        .finish()
        .with_context(|| format!("Failed to finalize {}", output_path.display()))
}

pub(crate) fn remote_unpack_dir_for_vast(instance: &VastInstance) -> String {
    format!(
        "{}{}",
        ICE_UNPACK_ROOT_DIR,
        format!("/vast-{}", instance.id)
    )
}

pub(crate) fn remote_unpack_dir_for_gcp(instance: &GcpInstance) -> String {
    format!(
        "{}{}",
        ICE_UNPACK_ROOT_DIR,
        format!("/gcp-{}", instance.name)
    )
}

pub(crate) fn remote_unpack_dir_for_aws(instance: &AwsInstance) -> String {
    format!(
        "{}{}",
        ICE_UNPACK_ROOT_DIR,
        format!("/aws-{}", instance.instance_id)
    )
}

pub(crate) fn unpack_prepare_remote_dir_command(remote_dir: &str) -> String {
    let state_dir = remote_shell_path(remote_dir);
    wrap_remote_shell_script(&format!("rm -rf {state_dir} && mkdir -p {state_dir}"))
}

pub(crate) fn unpack_start_remote_command(remote_dir: &str) -> String {
    wrap_remote_shell_script(&format!(
        r#"set -eu
state_dir={state_dir}
rm -f "$state_dir/{pid_file}" "$state_dir/{exit_code_file}"
nohup sh "$state_dir/{run_script}" >/dev/null 2>&1 < /dev/null &
echo $! > "$state_dir/{pid_file}"
"#,
        state_dir = remote_shell_path(remote_dir),
        pid_file = ICE_UNPACK_PID_FILE,
        exit_code_file = ICE_UNPACK_EXIT_CODE_FILE,
        run_script = ICE_UNPACK_RUN_SCRIPT,
    ))
}

pub(crate) fn unpack_logs_remote_command(remote_dir: &str, tail: u32, follow: bool) -> String {
    wrap_remote_shell_script(&format!(
        r#"set -eu
state_dir={state_dir}
log_file="$state_dir/{log_file}"
exit_code_file="$state_dir/{exit_code_file}"
if [ "{follow}" = "true" ]; then
  sent_bytes=0
  if [ -f "$log_file" ]; then
    line_count="$(wc -l < "$log_file" | tr -d '[:space:]')"
    if [ "$line_count" -gt "{tail}" ]; then
      tail -n "{tail}" "$log_file"
    else
      cat "$log_file"
    fi
    sent_bytes="$(wc -c < "$log_file" | tr -d '[:space:]')"
  fi
  while :; do
    if [ -f "$log_file" ]; then
      current_bytes="$(wc -c < "$log_file" | tr -d '[:space:]')"
      if [ "$current_bytes" -lt "$sent_bytes" ]; then
        sent_bytes=0
        current_bytes="$(wc -c < "$log_file" | tr -d '[:space:]')"
      fi
      if [ "$current_bytes" -gt "$sent_bytes" ]; then
        tail -c "+$((sent_bytes + 1))" "$log_file"
        sent_bytes="$current_bytes"
      fi
    fi
    if [ -f "$exit_code_file" ]; then
      final_bytes=0
      if [ -f "$log_file" ]; then
        final_bytes="$(wc -c < "$log_file" | tr -d '[:space:]')"
      fi
      if [ "$final_bytes" -le "$sent_bytes" ]; then
        printf '\n(exited with status %s)\n' "$(cat "$exit_code_file")"
        exit 0
      fi
    fi
    sleep 1
  done
fi
if [ ! -f "$log_file" ]; then
  if [ -f "$exit_code_file" ]; then
    printf '(exited with status %s)\n' "$(cat "$exit_code_file")"
  else
    echo "(no logs yet)"
  fi
  exit 0
fi
tail -n {tail} "$log_file"
if [ -f "$exit_code_file" ]; then
  printf '\n(exited with status %s)\n' "$(cat "$exit_code_file")"
fi
"#,
        state_dir = remote_shell_path(remote_dir),
        log_file = ICE_UNPACK_LOG_FILE,
        exit_code_file = ICE_UNPACK_EXIT_CODE_FILE,
        follow = if follow { "true" } else { "false" },
        tail = tail,
    ))
}

pub(crate) fn unpack_shell_remote_command(remote_dir: &str) -> String {
    wrap_remote_shell_script(&format!(
        "exec sh {}",
        remote_shell_child_path(remote_dir, ICE_UNPACK_SHELL_SCRIPT)
    ))
}

fn remote_shell_path(remote_dir: &str) -> String {
    if let Some(rest) = remote_dir.strip_prefix("~/") {
        return format!("$HOME/{rest}");
    }
    shell_quote_single(remote_dir)
}

fn remote_shell_child_path(remote_dir: &str, child: &str) -> String {
    if let Some(rest) = remote_dir.strip_prefix("~/") {
        return format!("$HOME/{rest}/{child}");
    }
    shell_quote_single(&format!("{remote_dir}/{child}"))
}
