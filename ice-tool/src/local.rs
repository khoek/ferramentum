use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use capulus::containers::{ContainerRuntime, DetectionMode};
use serde_json::Value;

use crate::config_store::ice_root_dir;
use crate::model::{IceConfig, PrefixLookup, RegistryAuth};
use crate::support::{
    ICE_LOCAL_CLOUD_LABEL_KEY, ICE_LOCAL_CLOUD_LABEL_VALUE, ICE_LOCAL_UNPACK_METADATA_FILE,
    ICE_RUNTIME_SECONDS_LABEL_KEY, ICE_UNPACK_EXIT_CODE_FILE, ICE_UNPACK_LOG_FILE,
    ICE_UNPACK_PID_FILE, ICE_UNPACK_ROOTFS_DIR, ICE_UNPACK_RUN_SCRIPT, ICE_UNPACK_SHELL_SCRIPT,
    ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, PROVIDER_DIR_NAME,
    build_cloud_instance_name, elapsed_hours_from_rfc3339, now_rfc3339, prefix_lookup_indices,
    required_runtime_seconds, run_command_json, run_command_status, run_command_status_with_stdin,
    run_command_text, shell_quote_single, spinner, truncate_ellipsis, visible_instance_name,
};
use crate::unpack::{clean_tar_path, materialize_unpack_bundle_in};
use crate::workload::{
    ContainerImageReference, InstanceWorkload, display_unpack_source, normalize_workload_source,
    parse_workload_metadata, registry_auth_for_workload, workload_display_value,
    workload_metadata_values,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalContainerRuntime {
    runtime: ContainerRuntime,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalContext {
    pub(crate) runtime: Option<LocalContainerRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalInstanceBackend {
    Container,
    Unpack,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalInstance {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) backend: LocalInstanceBackend,
    pub(crate) state: String,
    pub(crate) image: String,
    pub(crate) created_at: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) health: Option<String>,
    pub(crate) workload: Option<InstanceWorkload>,
    pub(crate) runtime_seconds: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LocalUnpackMetadata {
    id: String,
    name: String,
    source: String,
    created_at: String,
    started_at: Option<String>,
    runtime_seconds: Option<u64>,
    working_dir: Option<String>,
}

impl LocalContainerRuntime {
    pub(crate) fn command(&self) -> Command {
        self.runtime.command()
    }

    pub(crate) fn binary(&self) -> &'static str {
        self.runtime.name()
    }

    pub(crate) fn shell_prefix(&self) -> String {
        self.runtime.shell_prefix()
    }
}

impl LocalContext {
    pub(crate) fn require_runtime(self) -> Result<LocalContainerRuntime> {
        self.runtime.ok_or_else(|| {
            anyhow!(
                "Missing supported local container runtime. Install `docker` or `podman`, or use `ice create --unpack ...`."
            )
        })
    }
}

impl LocalInstance {
    pub(crate) fn label_str(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_running(&self) -> bool {
        self.state.eq_ignore_ascii_case("running")
    }

    pub(crate) fn is_stopped(&self) -> bool {
        !self.is_running()
    }

    pub(crate) fn runtime_hours(&self) -> f64 {
        if !self.is_running() {
            return 0.0;
        }
        let Some(started_at) = self.started_at.as_deref().or(self.created_at.as_deref()) else {
            return 0.0;
        };
        elapsed_hours_from_rfc3339(started_at).unwrap_or(0.0)
    }

    pub(crate) fn remaining_hours(&self) -> Option<f64> {
        if !self.is_running() {
            return None;
        }
        let runtime_seconds = self.runtime_seconds?;
        let started_at = self.started_at.as_deref().or(self.created_at.as_deref())?;
        let elapsed_seconds = elapsed_hours_from_rfc3339(started_at)? * 3600.0;
        Some(((runtime_seconds as f64) - elapsed_seconds).max(0.0) / 3600.0)
    }

    pub(crate) fn health_hint(&self) -> String {
        if !self.is_running() {
            return "-".to_owned();
        }
        self.health.clone().unwrap_or_else(|| "ok".to_owned())
    }
}

pub(crate) fn local_context() -> LocalContext {
    LocalContext {
        runtime: detect_local_container_runtime().ok(),
    }
}

pub(crate) fn detect_local_container_runtime() -> Result<LocalContainerRuntime> {
    Ok(LocalContainerRuntime {
        runtime: ContainerRuntime::detect_with_mode(DetectionMode::UserOrSudo)?,
    })
}

fn local_unpack_root_dir() -> Result<PathBuf> {
    Ok(ice_root_dir()?
        .join(PROVIDER_DIR_NAME)
        .join("local")
        .join("unpack"))
}

fn local_unpack_instance_dir(name: &str) -> Result<PathBuf> {
    Ok(local_unpack_root_dir()?.join(name))
}

pub(crate) fn local_list_instances(context: &LocalContext) -> Result<Vec<LocalInstance>> {
    let mut instances = load_local_unpack_instances()?;
    if let Some(runtime) = context.runtime {
        instances.extend(local_list_container_instances(&runtime)?);
    }
    Ok(instances)
}

fn local_list_container_instances(runtime: &LocalContainerRuntime) -> Result<Vec<LocalInstance>> {
    let mut ids_command = runtime.command();
    ids_command.args([
        "ps",
        "-a",
        "--filter",
        "label=ice-managed=true",
        "--filter",
        &format!(
            "label={}={}",
            ICE_LOCAL_CLOUD_LABEL_KEY, ICE_LOCAL_CLOUD_LABEL_VALUE
        ),
        "--format",
        "{{.ID}}",
    ]);
    let ids_output = run_command_text(&mut ids_command, "list local managed containers")?;
    let ids = ids_output
        .lines()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut inspect_command = runtime.command();
    inspect_command.arg("inspect");
    for id in &ids {
        inspect_command.arg(id);
    }
    let value = run_command_json(&mut inspect_command, "inspect local managed containers")?;
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("Expected JSON array from local container inspect output."))?;

    let mut instances = Vec::new();
    for row in rows {
        let labels = local_instance_labels(row);
        if !local_instance_is_managed(&labels) {
            continue;
        }
        if let Some(instance) = parse_local_instance_row(row, &labels) {
            instances.push(instance);
        }
    }
    Ok(instances)
}

pub(crate) fn local_instance_labels(row: &Value) -> HashMap<String, String> {
    row.get("Config")
        .and_then(|value| value.get("Labels"))
        .and_then(Value::as_object)
        .map(|labels| {
            labels
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_owned()))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

pub(crate) fn local_instance_is_managed(labels: &HashMap<String, String>) -> bool {
    labels
        .get("ice-managed")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
        && labels
            .get(ICE_LOCAL_CLOUD_LABEL_KEY)
            .map(|value| value.eq_ignore_ascii_case(ICE_LOCAL_CLOUD_LABEL_VALUE))
            .unwrap_or(false)
}

pub(crate) fn parse_local_instance_row(
    row: &Value,
    labels: &HashMap<String, String>,
) -> Option<LocalInstance> {
    let id = row.get("Id").and_then(Value::as_str)?.to_owned();
    let name = row
        .get("Name")
        .and_then(Value::as_str)?
        .trim_start_matches('/')
        .to_owned();
    let state = row
        .get("State")
        .and_then(|value| value.get("Status"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let image = row
        .get("Config")
        .and_then(|value| value.get("Image"))
        .and_then(Value::as_str)
        .or_else(|| row.get("ImageName").and_then(Value::as_str))
        .or_else(|| row.get("Image").and_then(Value::as_str))
        .unwrap_or("")
        .to_owned();
    let created_at = row
        .get("Created")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let started_at = row
        .get("State")
        .and_then(|value| value.get("StartedAt"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let health = row
        .get("State")
        .and_then(|value| value.get("Health"))
        .and_then(|value| value.get("Status"))
        .and_then(Value::as_str)
        .or_else(|| {
            row.get("State")
                .and_then(|value| value.get("Healthcheck"))
                .and_then(|value| value.get("Status"))
                .and_then(Value::as_str)
        })
        .map(str::to_owned);
    let workload = parse_workload_metadata(
        labels
            .get(ICE_WORKLOAD_KIND_METADATA_KEY)
            .map(String::as_str),
        labels
            .get(ICE_WORKLOAD_REGISTRY_METADATA_KEY)
            .map(String::as_str),
        labels
            .get(ICE_WORKLOAD_CONTAINER_METADATA_KEY)
            .map(String::as_str),
        labels
            .get(ICE_WORKLOAD_SOURCE_METADATA_KEY)
            .map(String::as_str),
    );
    let runtime_seconds = labels
        .get(ICE_RUNTIME_SECONDS_LABEL_KEY)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0);

    Some(LocalInstance {
        id,
        name,
        backend: LocalInstanceBackend::Container,
        state,
        image,
        created_at,
        started_at,
        health,
        workload,
        runtime_seconds,
    })
}

fn load_local_unpack_instances() -> Result<Vec<LocalInstance>> {
    let root = local_unpack_root_dir()?;
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut instances = Vec::new();
    for entry in
        fs::read_dir(&root).with_context(|| format!("Failed to read {}", root.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read child of {}", root.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(instance) = local_unpack_instance_from_dir(&path)? {
            instances.push(instance);
        }
    }
    Ok(instances)
}

fn local_unpack_instance_from_dir(dir: &Path) -> Result<Option<LocalInstance>> {
    let metadata_path = dir.join(ICE_LOCAL_UNPACK_METADATA_FILE);
    if !metadata_path.is_file() {
        return Ok(None);
    }
    let metadata = load_local_unpack_metadata_from_dir(dir)?;
    Ok(Some(local_unpack_metadata_to_instance(dir, &metadata)?))
}

fn load_local_unpack_metadata_from_dir(dir: &Path) -> Result<LocalUnpackMetadata> {
    let path = dir.join(ICE_LOCAL_UNPACK_METADATA_FILE);
    let content =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))?;
    toml::from_str::<LocalUnpackMetadata>(&content)
        .with_context(|| format!("Failed to parse {}", path.display()))
}

fn save_local_unpack_metadata_to_dir(dir: &Path, metadata: &LocalUnpackMetadata) -> Result<()> {
    let path = dir.join(ICE_LOCAL_UNPACK_METADATA_FILE);
    let content =
        toml::to_string_pretty(metadata).context("Failed to serialize local unpack metadata")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))
}

fn local_unpack_metadata_to_instance(
    dir: &Path,
    metadata: &LocalUnpackMetadata,
) -> Result<LocalInstance> {
    let state = local_unpack_state(dir)?;
    Ok(LocalInstance {
        id: metadata.id.clone(),
        name: metadata.name.clone(),
        backend: LocalInstanceBackend::Unpack,
        state,
        image: metadata.source.clone(),
        created_at: Some(metadata.created_at.clone()),
        started_at: metadata.started_at.clone(),
        health: None,
        workload: Some(InstanceWorkload::Unpack(metadata.source.clone())),
        runtime_seconds: metadata.runtime_seconds,
    })
}

fn local_unpack_state(dir: &Path) -> Result<String> {
    if let Some(pid) = local_unpack_pid(dir)?
        && local_pid_is_running(pid)
    {
        return Ok("running".to_owned());
    }
    let exit_code_path = dir.join(ICE_UNPACK_EXIT_CODE_FILE);
    if exit_code_path.is_file() {
        return Ok("exited".to_owned());
    }
    Ok("stopped".to_owned())
}

fn local_unpack_pid(dir: &Path) -> Result<Option<u32>> {
    let path = dir.join(ICE_UNPACK_PID_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))?;
    Ok(raw.trim().parse::<u32>().ok().filter(|pid| *pid > 0))
}

fn local_pid_is_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

pub(crate) fn local_workload_display(instance: &LocalInstance) -> String {
    if let Some(workload) = instance.workload.as_ref() {
        return workload_display_value(Some(workload));
    }
    if instance.image.trim().is_empty() {
        "-".to_owned()
    } else {
        instance.image.clone()
    }
}

pub(crate) fn local_backend_display(context: &LocalContext, instance: &LocalInstance) -> String {
    match instance.backend {
        LocalInstanceBackend::Container => context
            .runtime
            .map(|runtime| runtime.binary().to_owned())
            .unwrap_or_else(|| "container".to_owned()),
        LocalInstanceBackend::Unpack => "host".to_owned(),
    }
}

pub(crate) fn resolve_local_instance(
    context: &LocalContext,
    identifier: &str,
) -> Result<LocalInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }

    let instances = local_list_instances(context)?;
    if instances.is_empty() {
        bail!("No instance matched `{identifier}`.");
    }

    let needle = identifier.to_ascii_lowercase();
    let mut exact_id = Vec::new();
    let mut prefixed_id = Vec::new();
    for (index, instance) in instances.iter().enumerate() {
        let candidate = instance.id.to_ascii_lowercase();
        if candidate == needle {
            exact_id.push(index);
        } else if candidate.starts_with(&needle) {
            prefixed_id.push(index);
        }
    }
    match exact_id.len() {
        1 => return Ok(instances[exact_id[0]].clone()),
        n if n > 1 => {
            let listing = exact_id
                .into_iter()
                .map(|index| {
                    let instance = &instances[index];
                    format!(
                        "{} ({})",
                        truncate_ellipsis(&instance.id, 12),
                        visible_instance_name(instance.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        _ => {}
    }
    match prefixed_id.len() {
        1 => return Ok(instances[prefixed_id[0]].clone()),
        n if n > 1 => {
            let listing = prefixed_id
                .into_iter()
                .map(|index| {
                    let instance = &instances[index];
                    format!(
                        "{} ({})",
                        truncate_ellipsis(&instance.id, 12),
                        visible_instance_name(instance.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        _ => {}
    }

    match prefix_lookup_indices(&instances, identifier, |instance| instance.label_str())? {
        PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        PrefixLookup::Ambiguous(indices) => {
            let listing = indices
                .into_iter()
                .map(|index| {
                    let instance = &instances[index];
                    format!(
                        "{} ({})",
                        truncate_ellipsis(&instance.id, 12),
                        visible_instance_name(instance.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

pub(crate) fn local_describe_instance(context: &LocalContext, name: &str) -> Result<LocalInstance> {
    let unpack_dir = local_unpack_instance_dir(name)?;
    if let Some(instance) = local_unpack_instance_from_dir(&unpack_dir)? {
        return Ok(instance);
    }

    let runtime = context.require_runtime()?;
    let mut command = runtime.command();
    command.arg("inspect").arg(name);
    let value = run_command_json(&mut command, "inspect local container")?;
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("Expected JSON array from local container inspect output."))?;
    let row = rows
        .first()
        .ok_or_else(|| anyhow!("No local container inspect row returned for `{name}`."))?;
    let labels = local_instance_labels(row);
    if !local_instance_is_managed(&labels) {
        bail!("Container `{name}` is not managed by `ice`.");
    }
    parse_local_instance_row(row, &labels)
        .ok_or_else(|| anyhow!("Failed to parse local container inspect row for `{name}`."))
}

fn collect_local_existing_visible_names(context: &LocalContext) -> Result<HashSet<String>> {
    Ok(local_list_instances(context)?
        .iter()
        .map(|instance| visible_instance_name(instance.label_str()).to_owned())
        .collect())
}

pub(crate) fn local_create_instance(
    config: &IceConfig,
    context: &LocalContext,
    hours: f64,
    workload: &InstanceWorkload,
) -> Result<LocalInstance> {
    match workload {
        InstanceWorkload::Container(container) => {
            let runtime = context.require_runtime()?;
            let existing_names = collect_local_existing_visible_names(context)?;
            let name = build_cloud_instance_name(&existing_names)?;
            let runtime_seconds = required_runtime_seconds(hours);
            let registry_auth = registry_auth_for_workload(config, workload)?.ok_or_else(|| {
                anyhow!(
                    "Missing registry auth for container workload {}.",
                    container.container_ref()
                )
            })?;

            let login_spinner = spinner("Logging into the container registry...");
            local_registry_login(&runtime, container, &registry_auth)?;
            login_spinner.finish_with_message("Registry login succeeded.");

            let pull_spinner = spinner(&format!("Pulling {}...", container.container_ref()));
            let mut pull_command = runtime.command();
            pull_command.arg("pull").arg(container.container_ref());
            run_command_status(&mut pull_command, "pull local workload container")?;
            pull_spinner.finish_with_message(format!("Pulled {}.", container.container_ref()));

            let create_spinner = spinner("Starting local container...");
            let mut create_command = runtime.command();
            create_command
                .arg("run")
                .arg("-d")
                .arg("--name")
                .arg(&name)
                .arg("--restart")
                .arg("unless-stopped")
                .arg("--label")
                .arg("ice-managed=true")
                .arg("--label")
                .arg("ice-created-by=ice")
                .arg("--label")
                .arg(format!(
                    "{}={}",
                    ICE_LOCAL_CLOUD_LABEL_KEY, ICE_LOCAL_CLOUD_LABEL_VALUE
                ))
                .arg("--label")
                .arg(format!(
                    "{}={runtime_seconds}",
                    ICE_RUNTIME_SECONDS_LABEL_KEY
                ));
            for (key, value) in workload_metadata_values(workload) {
                create_command.arg("--label").arg(format!("{key}={value}"));
            }
            create_command.arg(container.container_ref());
            run_command_status(&mut create_command, "create local container")?;
            create_spinner.finish_with_message(format!(
                "Started local container {}.",
                visible_instance_name(&name)
            ));

            let instance = local_describe_instance(context, &name)?;
            if let Some(started_at) = instance.started_at.as_deref() {
                spawn_local_container_autostop(
                    &runtime,
                    &instance.name,
                    started_at,
                    runtime_seconds,
                )?;
            }
            Ok(instance)
        }
        InstanceWorkload::Unpack(source) => {
            local_create_unpack_instance(config, context, hours, source)
        }
        InstanceWorkload::Shell => bail!(
            "`ice create --cloud local` requires `--container`, `--unpack`, or `--arca`; there is no host VM for `--ssh`."
        ),
    }
}

fn local_create_unpack_instance(
    config: &IceConfig,
    context: &LocalContext,
    hours: f64,
    source: &str,
) -> Result<LocalInstance> {
    let existing_names = collect_local_existing_visible_names(context)?;
    let name = build_cloud_instance_name(&existing_names)?;
    let dir = local_unpack_instance_dir(&name)?;
    if dir.exists() {
        bail!("Local unpack dir already exists: {}", dir.display());
    }
    let runtime_seconds = required_runtime_seconds(hours);
    let created_at = now_rfc3339();

    let create_result = (|| {
        fs::create_dir_all(&dir).with_context(|| format!("Failed to create {}", dir.display()))?;

        let materialize_spinner = spinner(&format!(
            "Materializing unpack workload {}...",
            display_unpack_source(source)
        ));
        let bundle = materialize_unpack_bundle_in(config, source, &dir)?;
        materialize_spinner.finish_with_message("Unpack bundle materialized.");

        let archive_path = dir.join("image.tar");
        if archive_path.is_file() {
            fs::remove_file(&archive_path)
                .with_context(|| format!("Failed to remove {}", archive_path.display()))?;
        }

        let mut metadata = LocalUnpackMetadata {
            id: name.clone(),
            name: name.clone(),
            source: normalize_workload_source(source)?,
            created_at,
            started_at: None,
            runtime_seconds: Some(runtime_seconds),
            working_dir: bundle.working_dir,
        };
        save_local_unpack_metadata_to_dir(&dir, &metadata)?;

        let start_spinner = spinner("Starting local unpack workload...");
        let pid = launch_local_unpack_instance(&dir)?;
        metadata.started_at = Some(now_rfc3339());
        save_local_unpack_metadata_to_dir(&dir, &metadata)?;
        if runtime_seconds > 0 {
            spawn_local_unpack_autostop(&dir, pid, runtime_seconds)?;
        }
        start_spinner.finish_with_message(format!(
            "Started local unpack workload {}.",
            visible_instance_name(&name)
        ));

        local_unpack_metadata_to_instance(&dir, &metadata)
    })();

    if create_result.is_err() {
        let _ = fs::remove_dir_all(&dir);
    }
    create_result
}

pub(crate) fn local_registry_login(
    runtime: &LocalContainerRuntime,
    container: &ContainerImageReference,
    registry_auth: &RegistryAuth,
) -> Result<()> {
    let mut command = runtime.command();
    command
        .arg("login")
        .arg("-u")
        .arg(registry_auth.username)
        .arg("--password-stdin")
        .arg(format!("https://{}", container.registry_host()));
    run_command_status_with_stdin(
        &mut command,
        "log into the private container registry",
        &registry_auth.secret,
    )
}

fn launch_local_unpack_instance(dir: &Path) -> Result<u32> {
    let script = format!(
        r#"set -eu
state_dir={}
rm -f "$state_dir/{}" "$state_dir/{}"
nohup sh "$state_dir/{}" >/dev/null 2>&1 < /dev/null &
shell_pid=$!
printf '%s\n' "$shell_pid" > "$state_dir/{}"
attempt=0
while [ "$attempt" -lt 200 ]; do
  if [ -s "$state_dir/{}" ]; then
    pid="$(cat "$state_dir/{}")"
    if [ -n "$pid" ]; then
      printf '%s' "$pid"
      exit 0
    fi
  fi
  attempt=$((attempt + 1))
  sleep 0.05
done
echo "timed out waiting for unpack pid file" >&2
exit 1
"#,
        shell_quote_single(&dir.display().to_string()),
        ICE_UNPACK_PID_FILE,
        ICE_UNPACK_EXIT_CODE_FILE,
        ICE_UNPACK_RUN_SCRIPT,
        ICE_UNPACK_PID_FILE,
        ICE_UNPACK_PID_FILE,
        ICE_UNPACK_PID_FILE,
    );
    let mut command = Command::new("sh");
    command.arg("-lc").arg(script);
    let pid = run_command_text(&mut command, "start local unpack workload")?;
    pid.parse::<u32>()
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| anyhow!("Invalid local unpack pid `{pid}`."))
}

fn spawn_local_unpack_autostop(dir: &Path, expected_pid: u32, runtime_seconds: u64) -> Result<()> {
    let script = format!(
        r#"sleep {runtime_seconds}
state_dir={}
current_pid="$(cat "$state_dir/{}" 2>/dev/null || true)"
if [ "$current_pid" = "{}" ] && [ -n "$current_pid" ]; then
  kill "$current_pid" >/dev/null 2>&1 || true
fi
"#,
        shell_quote_single(&dir.display().to_string()),
        ICE_UNPACK_PID_FILE,
        expected_pid,
    );
    let mut command = Command::new("sh");
    command.arg("-lc").arg(script);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command
        .spawn()
        .context("Failed to spawn local unpack auto-stop helper")?;
    Ok(())
}

fn spawn_local_container_autostop(
    runtime: &LocalContainerRuntime,
    name: &str,
    expected_started_at: &str,
    runtime_seconds: u64,
) -> Result<()> {
    let quoted_name = shell_quote_single(name);
    let quoted_started_at = shell_quote_single(expected_started_at);
    let script = format!(
        r#"sleep {runtime_seconds}
current_started="$({runtime} inspect --format '{{{{.State.StartedAt}}}}' {quoted_name} 2>/dev/null || true)"
if [ "$current_started" = {quoted_started_at} ]; then
  {runtime} stop {quoted_name} >/dev/null 2>&1 || true
fi
"#,
        runtime = runtime.shell_prefix()
    );
    let mut command = Command::new("sh");
    command.arg("-lc").arg(script);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command
        .spawn()
        .context("Failed to spawn local auto-stop helper")?;
    Ok(())
}

fn local_stop_container_instance(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
) -> Result<()> {
    let spinner = spinner(&format!(
        "Stopping local container {}...",
        visible_instance_name(&instance.name)
    ));
    let mut command = runtime.command();
    command.arg("stop").arg(&instance.name);
    run_command_status(&mut command, "stop local container")?;
    spinner.finish_with_message("Stopped.");
    Ok(())
}

fn local_start_container_instance(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
) -> Result<LocalInstance> {
    let spinner = spinner(&format!(
        "Starting local container {}...",
        visible_instance_name(&instance.name)
    ));
    let mut command = runtime.command();
    command.arg("start").arg(&instance.name);
    run_command_status(&mut command, "start local container")?;
    spinner.finish_with_message("Started.");

    let instance = local_describe_instance(
        &LocalContext {
            runtime: Some(*runtime),
        },
        &instance.name,
    )?;
    if let (Some(started_at), Some(runtime_seconds)) =
        (instance.started_at.as_deref(), instance.runtime_seconds)
    {
        spawn_local_container_autostop(runtime, &instance.name, started_at, runtime_seconds)?;
    }
    Ok(instance)
}

fn local_delete_container_instance(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
) -> Result<()> {
    let spinner = spinner(&format!(
        "Deleting local container {}...",
        visible_instance_name(&instance.name)
    ));
    let mut command = runtime.command();
    command.arg("rm");
    if !instance.is_stopped() {
        command.arg("-f");
    }
    command.arg(&instance.name);
    run_command_status(&mut command, "delete local container")?;
    spinner.finish_with_message("Deleted.");
    Ok(())
}

fn local_open_container_shell(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
) -> Result<()> {
    let mut probe = runtime.command();
    probe
        .arg("exec")
        .arg(&instance.name)
        .arg("sh")
        .arg("-lc")
        .arg("command -v bash >/dev/null 2>&1");
    let shell = if probe
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
    {
        "bash"
    } else {
        "sh"
    };

    let mut command = runtime.command();
    command
        .arg("exec")
        .arg("-it")
        .arg(&instance.name)
        .arg(shell);
    if shell == "bash" {
        command.arg("-l");
    }
    run_command_status(&mut command, "open local container shell")
}

fn local_download_from_container(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut command = runtime.command();
    command
        .arg("cp")
        .arg(format!("{}:{}", instance.name, remote_path))
        .arg(destination);
    run_command_status(&mut command, "download from local container")
}

pub(crate) fn local_stop_instance(context: &LocalContext, instance: &LocalInstance) -> Result<()> {
    match instance.backend {
        LocalInstanceBackend::Container => {
            local_stop_container_instance(&context.require_runtime()?, instance)
        }
        LocalInstanceBackend::Unpack => local_stop_unpack_instance(instance),
    }
}

pub(crate) fn local_start_instance(
    context: &LocalContext,
    instance: &LocalInstance,
) -> Result<LocalInstance> {
    match instance.backend {
        LocalInstanceBackend::Container => {
            local_start_container_instance(&context.require_runtime()?, instance)
        }
        LocalInstanceBackend::Unpack => local_start_unpack_instance(instance),
    }
}

pub(crate) fn local_delete_instance(
    context: &LocalContext,
    instance: &LocalInstance,
) -> Result<()> {
    match instance.backend {
        LocalInstanceBackend::Container => {
            local_delete_container_instance(&context.require_runtime()?, instance)
        }
        LocalInstanceBackend::Unpack => local_delete_unpack_instance(instance),
    }
}

pub(crate) fn local_open_shell(context: &LocalContext, instance: &LocalInstance) -> Result<()> {
    match instance.backend {
        LocalInstanceBackend::Container => {
            local_open_container_shell(&context.require_runtime()?, instance)
        }
        LocalInstanceBackend::Unpack => local_open_unpack_shell(instance),
    }
}

pub(crate) fn local_download(
    context: &LocalContext,
    instance: &LocalInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    match instance.backend {
        LocalInstanceBackend::Container => local_download_from_container(
            &context.require_runtime()?,
            instance,
            remote_path,
            local_path,
        ),
        LocalInstanceBackend::Unpack => {
            local_download_from_unpack(instance, remote_path, local_path)
        }
    }
}

pub(crate) fn local_stream_logs(
    context: &LocalContext,
    instance: &LocalInstance,
    tail: u32,
    follow: bool,
) -> Result<()> {
    match instance.backend {
        LocalInstanceBackend::Container => {
            local_stream_container_logs(&context.require_runtime()?, instance, tail, follow)
        }
        LocalInstanceBackend::Unpack => local_stream_unpack_logs(instance, tail, follow),
    }
}

fn local_start_unpack_instance(instance: &LocalInstance) -> Result<LocalInstance> {
    let dir = local_unpack_instance_dir(&instance.name)?;
    let mut metadata = load_local_unpack_metadata_from_dir(&dir)?;
    let spinner = spinner(&format!(
        "Starting local unpack workload {}...",
        visible_instance_name(&instance.name)
    ));
    let pid = launch_local_unpack_instance(&dir)?;
    metadata.started_at = Some(now_rfc3339());
    save_local_unpack_metadata_to_dir(&dir, &metadata)?;
    if let Some(runtime_seconds) = metadata.runtime_seconds {
        spawn_local_unpack_autostop(&dir, pid, runtime_seconds)?;
    }
    spinner.finish_with_message("Started.");
    local_unpack_metadata_to_instance(&dir, &metadata)
}

fn local_stop_unpack_instance(instance: &LocalInstance) -> Result<()> {
    let dir = local_unpack_instance_dir(&instance.name)?;
    let spinner = spinner(&format!(
        "Stopping local unpack workload {}...",
        visible_instance_name(&instance.name)
    ));
    if let Some(pid) = local_unpack_pid(&dir)?
        && local_pid_is_running(pid)
    {
        let mut command = Command::new("kill");
        command.arg(pid.to_string());
        let _ = command.status();
        for _ in 0..50 {
            if !local_pid_is_running(pid) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        if local_pid_is_running(pid) {
            let mut force = Command::new("kill");
            force.arg("-9").arg(pid.to_string());
            let _ = force.status();
        }
    }
    spinner.finish_with_message("Stopped.");
    Ok(())
}

fn local_delete_unpack_instance(instance: &LocalInstance) -> Result<()> {
    if instance.is_running() {
        local_stop_unpack_instance(instance)?;
    }
    let dir = local_unpack_instance_dir(&instance.name)?;
    let spinner = spinner(&format!(
        "Deleting local unpack workload {}...",
        visible_instance_name(&instance.name)
    ));
    if dir.exists() {
        fs::remove_dir_all(&dir).with_context(|| format!("Failed to remove {}", dir.display()))?;
    }
    spinner.finish_with_message("Deleted.");
    Ok(())
}

fn local_open_unpack_shell(instance: &LocalInstance) -> Result<()> {
    let mut command = Command::new("sh");
    command.arg(local_unpack_instance_dir(&instance.name)?.join(ICE_UNPACK_SHELL_SCRIPT));
    run_command_status(&mut command, "open local unpack shell")
}

fn local_download_from_unpack(
    instance: &LocalInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let source = local_unpack_requested_path(instance, remote_path)?;
    if !source.exists() {
        bail!("No unpacked path matched `{remote_path}`.");
    }
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut command = Command::new("cp");
    command.arg("-a").arg(&source).arg(destination);
    run_command_status(&mut command, "download from local unpack workload")
}

fn local_stream_container_logs(
    runtime: &LocalContainerRuntime,
    instance: &LocalInstance,
    tail: u32,
    follow: bool,
) -> Result<()> {
    let mut command = runtime.command();
    command.arg("logs").arg("--tail").arg(tail.to_string());
    if follow {
        command.arg("-f");
    }
    command.arg(&instance.name);
    run_command_status(&mut command, "stream local container logs")
}

fn local_stream_unpack_logs(instance: &LocalInstance, tail: u32, follow: bool) -> Result<()> {
    let dir = local_unpack_instance_dir(&instance.name)?;
    let log_path = dir.join(ICE_UNPACK_LOG_FILE);
    let exit_code_path = dir.join(ICE_UNPACK_EXIT_CODE_FILE);
    if !log_path.is_file() {
        if exit_code_path.is_file() {
            println!(
                "(exited with status {})",
                fs::read_to_string(&exit_code_path)
                    .with_context(|| format!("Failed to read {}", exit_code_path.display()))?
                    .trim()
            );
        } else {
            println!("(no logs yet)");
        }
        return Ok(());
    }

    if follow {
        return follow_local_unpack_logs(&log_path, &exit_code_path, tail);
    }

    let mut command = Command::new("tail");
    command.arg("-n").arg(tail.to_string()).arg(&log_path);
    run_command_status(&mut command, "stream local unpack logs")?;

    if !follow && exit_code_path.is_file() {
        println!(
            "\n(exited with status {})",
            fs::read_to_string(&exit_code_path)
                .with_context(|| format!("Failed to read {}", exit_code_path.display()))?
                .trim()
        );
    }
    Ok(())
}

fn follow_local_unpack_logs(log_path: &Path, exit_code_path: &Path, tail: u32) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    let mut printed_bytes = 0usize;

    if log_path.is_file() {
        let bytes =
            fs::read(log_path).with_context(|| format!("Failed to read {}", log_path.display()))?;
        let start = tail_start_offset(&bytes, tail);
        if start < bytes.len() {
            stdout.write_all(&bytes[start..])?;
            if !bytes.ends_with(b"\n") {
                stdout.write_all(b"\n")?;
            }
            stdout.flush()?;
        }
        printed_bytes = bytes.len();
    }

    loop {
        if log_path.is_file() {
            let bytes = fs::read(log_path)
                .with_context(|| format!("Failed to read {}", log_path.display()))?;
            if bytes.len() < printed_bytes {
                printed_bytes = 0;
            }
            if bytes.len() > printed_bytes {
                stdout.write_all(&bytes[printed_bytes..])?;
                if !bytes.ends_with(b"\n") {
                    stdout.write_all(b"\n")?;
                }
                stdout.flush()?;
                printed_bytes = bytes.len();
            }
        }

        if exit_code_path.is_file() {
            let bytes = if log_path.is_file() {
                fs::read(log_path)
                    .with_context(|| format!("Failed to read {}", log_path.display()))?
            } else {
                Vec::new()
            };
            if bytes.len() <= printed_bytes {
                println!(
                    "\n(exited with status {})",
                    fs::read_to_string(exit_code_path)
                        .with_context(|| format!("Failed to read {}", exit_code_path.display()))?
                        .trim()
                );
                return Ok(());
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn tail_start_offset(bytes: &[u8], tail: u32) -> usize {
    if tail == 0 {
        return bytes.len();
    }
    let mut newlines_seen = 0u32;
    for (index, byte) in bytes.iter().enumerate().rev() {
        if *byte == b'\n' {
            newlines_seen += 1;
            if newlines_seen > tail {
                return index + 1;
            }
        }
    }
    0
}

fn local_unpack_requested_path(instance: &LocalInstance, remote_path: &str) -> Result<PathBuf> {
    let dir = local_unpack_instance_dir(&instance.name)?;
    let metadata = load_local_unpack_metadata_from_dir(&dir)?;
    let rootfs = dir.join(ICE_UNPACK_ROOTFS_DIR);
    let requested = Path::new(remote_path);
    let relative = if requested.is_absolute() {
        clean_tar_path(requested)?
    } else if let Some(working_dir) = metadata.working_dir.as_deref() {
        clean_tar_path(&Path::new(working_dir).join(requested))?
    } else {
        clean_tar_path(requested)?
    };
    Ok(rootfs.join(relative))
}
