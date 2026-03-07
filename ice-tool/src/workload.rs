use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::json;

use crate::arca::{arca_source, parse_arca_source};
use crate::model::{DeployTargetRequest, IceConfig, RegistryAuth};
use crate::providers::gcp;
use crate::support::{
    ICE_MANAGED_CONTAINER_NAME, ICE_WORKLOAD_CONTAINER_METADATA_KEY,
    ICE_WORKLOAD_KIND_METADATA_KEY, ICE_WORKLOAD_REGISTRY_METADATA_KEY,
    ICE_WORKLOAD_SOURCE_METADATA_KEY, required_runtime_seconds, shell_quote_single,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum InstanceWorkload {
    Shell,
    Container(ContainerImageReference),
    Unpack(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ContainerImageReference {
    pub(crate) registry: String,
    pub(crate) container: String,
}

impl ContainerImageReference {
    pub(crate) fn new(registry: String, container: String) -> Result<Self> {
        let registry = normalize_gcp_registry(&registry)?;
        Ok(Self {
            container: normalize_container_name(&container, &registry)?,
            registry,
        })
    }

    pub(crate) fn from_container_ref(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            bail!("Container ref cannot be empty.");
        }

        let segments = value.split('/').collect::<Vec<_>>();
        if segments.len() < 3 {
            bail!("Container ref must include a registry and container path: `{value}`.");
        }

        let host = segments[0];
        if host.ends_with("-docker.pkg.dev") {
            if segments.len() < 4 {
                bail!(
                    "Artifact Registry refs must look like `LOCATION-docker.pkg.dev/PROJECT/REPOSITORY/IMAGE[:TAG]`."
                );
            }
            return Self::new(segments[..3].join("/"), segments[3..].join("/"));
        }

        if is_supported_gcp_registry_host(host) {
            return Self::new(segments[..2].join("/"), segments[2..].join("/"));
        }

        bail!("Unsupported registry host in container ref `{value}`.");
    }

    pub(crate) fn container_ref(&self) -> String {
        format!("{}/{}", self.registry, self.container)
    }

    pub(crate) fn registry_host(&self) -> &str {
        self.registry.split('/').next().unwrap_or("")
    }
}

pub(crate) fn resolve_deploy_workload(request: &DeployTargetRequest) -> Result<InstanceWorkload> {
    let explicit_modes = usize::from(request.ssh)
        + usize::from(request.container.is_some())
        + usize::from(request.unpack.is_some())
        + usize::from(request.arca.is_some());
    if explicit_modes > 1 {
        bail!("Pass exactly one of `--ssh`, `--container`, `--unpack`, or `--arca`.");
    }
    if explicit_modes > 0 && request.positional.is_some() {
        bail!(
            "Positional TARGET cannot be combined with `--ssh`, `--container`, `--unpack`, or `--arca`."
        );
    }
    if request.ssh {
        return Ok(InstanceWorkload::Shell);
    }
    if let Some(container_ref) = request.container.as_deref() {
        return resolve_container_deploy_target(container_ref);
    }
    if let Some(source) = request.unpack.as_deref() {
        return Ok(InstanceWorkload::Unpack(resolve_unpack_deploy_source(
            source,
        )?));
    }
    if let Some(selector) = request.arca.as_deref() {
        return Ok(InstanceWorkload::Unpack(arca_source(Some(selector))));
    }
    Ok(InstanceWorkload::Unpack(arca_source(
        request.positional.as_deref(),
    )))
}

pub(crate) fn resolve_deploy_hours(config: &IceConfig, override_hours: Option<f64>) -> Result<f64> {
    let hours = override_hours
        .or(config.default.runtime_hours)
        .unwrap_or(1.0);
    if !(hours.is_finite() && hours > 0.0) {
        bail!("Deployment runtime must be a finite number of hours greater than zero.");
    }
    Ok(hours)
}

fn resolve_container_deploy_target(container_ref: &str) -> Result<InstanceWorkload> {
    if parse_arca_source(container_ref).is_some() {
        bail!("`--container arca:...` is invalid. Use `--arca ...` or `--unpack arca:...`.");
    }
    Ok(InstanceWorkload::Container(
        ContainerImageReference::from_container_ref(container_ref)?,
    ))
}

fn resolve_unpack_deploy_source(source: &str) -> Result<String> {
    if let Some(selector) = parse_arca_source(source) {
        return Ok(arca_source(selector));
    }
    normalize_workload_source(source)
}

pub(crate) fn wrap_remote_shell_script(script: &str) -> String {
    format!("sh -lc {}", shell_quote_single(script))
}

pub(crate) fn normalize_gcp_registry(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("Registry cannot be empty.");
    }

    let value = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(value)
        .trim_end_matches('/')
        .to_ascii_lowercase();
    if value.is_empty() {
        bail!("Registry cannot be empty.");
    }
    if value.contains(char::is_whitespace) {
        bail!("Registry must not contain whitespace.");
    }
    let (host, path) = value
        .split_once('/')
        .ok_or_else(|| anyhow!("Registry must include a host and repository path."))?;
    if !is_supported_gcp_registry_host(host) {
        bail!(
            "Unsupported registry host `{host}`. Use a GCP registry such as `gcr.io/project` or `LOCATION-docker.pkg.dev/project/repository`."
        );
    }
    if path.split('/').any(|segment| segment.is_empty()) {
        bail!("Registry path must not contain empty segments: `{value}`.");
    }
    if host.ends_with("-docker.pkg.dev") && path.split('/').count() < 2 {
        bail!(
            "Artifact Registry values must look like `LOCATION-docker.pkg.dev/PROJECT/REPOSITORY`."
        );
    }

    Ok(value)
}

pub(crate) fn is_supported_gcp_registry_host(host: &str) -> bool {
    matches!(host, "gcr.io" | "us.gcr.io" | "eu.gcr.io" | "asia.gcr.io")
        || host.ends_with(".gcr.io")
        || host
            .strip_suffix("-docker.pkg.dev")
            .map(|prefix| {
                !prefix.is_empty()
                    && prefix
                        .chars()
                        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
            })
            .unwrap_or(false)
}

pub(crate) fn normalize_container_name(value: &str, registry: &str) -> Result<String> {
    let value = value.trim().trim_start_matches('/');
    if value.is_empty() {
        bail!("Container name cannot be empty.");
    }
    if value.contains(char::is_whitespace) {
        bail!("Container name must not contain whitespace.");
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        bail!("Container name must not include a URL scheme.");
    }

    let value = if !registry.is_empty() {
        value
            .strip_prefix(&format!("{registry}/"))
            .unwrap_or(value)
            .to_owned()
    } else {
        value.to_owned()
    };
    let first_segment = value.split('/').next().unwrap_or("");
    if first_segment.contains('.') || (first_segment.contains(':') && value.contains('/')) {
        bail!("Container name must not include a registry host; configure it separately.");
    }

    Ok(value)
}

pub(crate) fn normalize_workload_source(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("Workload source cannot be empty.");
    }

    let candidate_path = Path::new(trimmed);
    if candidate_path.exists() {
        if !candidate_path.is_file() {
            bail!(
                "Workload source path must point to a file archive: {}",
                candidate_path.display()
            );
        }
        return Ok(fs::canonicalize(candidate_path)
            .with_context(|| {
                format!(
                    "Failed to canonicalize workload source path: {}",
                    candidate_path.display()
                )
            })?
            .display()
            .to_string());
    }

    if candidate_path.is_absolute()
        || trimmed.starts_with("./")
        || trimmed.starts_with("../")
        || trimmed.ends_with(".tar")
    {
        bail!("Local workload archive path does not exist: {trimmed}");
    }

    Ok(trimmed.to_owned())
}

pub(crate) fn workload_metadata_values(workload: &InstanceWorkload) -> Vec<(&'static str, String)> {
    let mut values = vec![(
        ICE_WORKLOAD_KIND_METADATA_KEY,
        workload_kind_value(workload),
    )];
    match workload {
        InstanceWorkload::Shell => {}
        InstanceWorkload::Container(container) => {
            values.push((
                ICE_WORKLOAD_REGISTRY_METADATA_KEY,
                container.registry.clone(),
            ));
            values.push((
                ICE_WORKLOAD_CONTAINER_METADATA_KEY,
                container.container.clone(),
            ));
        }
        InstanceWorkload::Unpack(source) => {
            values.push((ICE_WORKLOAD_SOURCE_METADATA_KEY, source.clone()));
        }
    }
    values
}

pub(crate) fn gcp_workload_metadata_arg(workload: &InstanceWorkload) -> String {
    workload_metadata_values(workload)
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn aws_instance_tag_specification(
    name: &str,
    workload: &InstanceWorkload,
) -> Result<String> {
    let mut tags = vec![
        json!({"Key": "Name", "Value": name}),
        json!({"Key": "ice-managed", "Value": "true"}),
        json!({"Key": "ice-created-by", "Value": "ice"}),
    ];
    for (key, value) in workload_metadata_values(workload) {
        tags.push(json!({"Key": key, "Value": value}));
    }
    serde_json::to_string(&vec![json!({
        "ResourceType": "instance",
        "Tags": tags,
    })])
    .context("Failed to serialize AWS instance tag specification")
}

pub(crate) fn workload_kind_value(workload: &InstanceWorkload) -> String {
    match workload {
        InstanceWorkload::Shell => "shell".to_owned(),
        InstanceWorkload::Container(_) => "container".to_owned(),
        InstanceWorkload::Unpack(_) => "unpack".to_owned(),
    }
}

pub(crate) fn parse_workload_metadata(
    kind: Option<&str>,
    registry: Option<&str>,
    container: Option<&str>,
    source: Option<&str>,
) -> Option<InstanceWorkload> {
    match kind?.trim().to_ascii_lowercase().as_str() {
        "shell" => Some(InstanceWorkload::Shell),
        "container" => Some(InstanceWorkload::Container(
            ContainerImageReference::new(registry?.to_owned(), container?.to_owned()).ok()?,
        )),
        "unpack" => Some(InstanceWorkload::Unpack(parse_unpack_workload_metadata(
            source?,
        )?)),
        _ => None,
    }
}

pub(crate) fn workload_display_value(workload: Option<&InstanceWorkload>) -> String {
    match workload {
        Some(InstanceWorkload::Shell) => "shell".to_owned(),
        Some(InstanceWorkload::Container(container)) => container.container_ref(),
        Some(InstanceWorkload::Unpack(source)) => {
            format!("unpack {}", display_unpack_source(source))
        }
        None => "-".to_owned(),
    }
}

pub(crate) fn display_unpack_source(source: &str) -> String {
    if let Some(selector) = parse_arca_source(source) {
        return arca_source(selector);
    }
    let path = Path::new(source);
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| source.to_owned())
}

pub(crate) fn parse_unpack_workload_metadata(source: &str) -> Option<String> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

pub(crate) fn registry_auth_for_workload(
    config: &IceConfig,
    workload: &InstanceWorkload,
) -> Result<Option<RegistryAuth>> {
    match workload {
        InstanceWorkload::Shell | InstanceWorkload::Unpack(_) => Ok(None),
        InstanceWorkload::Container(_) => Ok(Some(gcp::registry_login(config)?)),
    }
}

pub(crate) fn build_linux_startup_script(
    shutdown_command: &str,
    hours: f64,
    workload: &InstanceWorkload,
    registry_auth: Option<&RegistryAuth>,
) -> Result<String> {
    let auto_stop = shell_quote_single(&format!(
        "sleep {}; {shutdown_command}",
        required_runtime_seconds(hours)
    ));
    let workload_script = match workload {
        InstanceWorkload::Shell | InstanceWorkload::Unpack(_) => String::new(),
        InstanceWorkload::Container(container) => {
            let Some(registry_auth) = registry_auth else {
                bail!(
                    "Missing registry auth for container workload {}.",
                    container.container_ref()
                );
            };
            container_workload_startup_script(container, registry_auth)
        }
    };

    Ok(format!(
        r#"#!/bin/bash
set -euxo pipefail
exec > >(tee -a /var/log/ice-startup.log) 2>&1
nohup bash -lc {auto_stop} >/var/log/ice-autostop.log 2>&1 &
{workload_script}
"#
    ))
}

fn container_workload_startup_script(
    container: &ContainerImageReference,
    registry_auth: &RegistryAuth,
) -> String {
    let quoted_container_name = shell_quote_single(ICE_MANAGED_CONTAINER_NAME);
    let quoted_registry = shell_quote_single(container.registry_host());
    let quoted_image_ref = shell_quote_single(&container.container_ref());
    let quoted_registry_user = shell_quote_single(registry_auth.username);
    let quoted_registry_secret = shell_quote_single(&registry_auth.secret);
    format!(
        r#"set -eu
container_name={quoted_container_name}
registry_host={quoted_registry}
image_ref={quoted_image_ref}
registry_user={quoted_registry_user}
registry_secret={quoted_registry_secret}

ensure_docker() {{
  if command -v docker >/dev/null 2>&1; then
    systemctl enable --now docker
    return
  fi

  if command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y docker.io
  elif command -v dnf >/dev/null 2>&1; then
    dnf -y install docker
  elif command -v yum >/dev/null 2>&1; then
    yum -y install docker
  else
    echo "Unsupported package manager for docker install." >&2
    exit 1
  fi

  systemctl enable --now docker
}}

ensure_docker
printf '%s\n' "$registry_secret" | docker login -u "$registry_user" --password-stdin "https://$registry_host"

if docker ps -a --format '{{{{.Names}}}}' | grep -Fxq "$container_name"; then
  if ! docker ps --format '{{{{.Names}}}}' | grep -Fxq "$container_name"; then
    docker start "$container_name" >/dev/null
  fi
else
  docker pull "$image_ref"
  docker run -d --name "$container_name" --restart unless-stopped "$image_ref" >/dev/null
fi

echo "Container $container_name is ready from $image_ref."
"#
    )
}

fn host_shell_remote_command() -> String {
    wrap_remote_shell_script(
        r#"set -eu
if command -v bash >/dev/null 2>&1; then
  exec bash -l
fi
exec sh
"#,
    )
}

fn container_shell_remote_command(container: &ContainerImageReference) -> String {
    let quoted_container = shell_quote_single(ICE_MANAGED_CONTAINER_NAME);
    let quoted_registry = shell_quote_single(&container.registry);
    let quoted_container_ref = shell_quote_single(&container.container);
    let script = format!(
        r#"set -eu
container_name={quoted_container}
registry={quoted_registry}
container_ref={quoted_container_ref}
image_ref="$registry/$container_ref"

runtime=""
if command -v docker >/dev/null 2>&1; then
  runtime="docker"
elif command -v podman >/dev/null 2>&1; then
  runtime="podman"
fi

if [ -z "$runtime" ]; then
  echo "No supported container runtime found on the instance." >&2
  exit 1
fi

runtime_cmd="$runtime"
if ! $runtime_cmd version >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
  runtime_cmd="sudo $runtime"
fi
if ! $runtime_cmd version >/dev/null 2>&1; then
  echo "Container runtime $runtime is unavailable." >&2
  exit 1
fi

selected_container=""
if $runtime_cmd ps -a --format '{{{{.Names}}}}' | grep -Fxq "$container_name"; then
  selected_container="$container_name"
else
  selected_container="$($runtime_cmd ps -a --format '{{{{.Names}}}} {{{{.Image}}}}' | awk -v image=\"$image_ref\" '$2 == image {{ print $1; exit }}')"
fi
if [ -z "$selected_container" ]; then
  container_count="$($runtime_cmd ps -a --format '{{{{.Names}}}}' | awk 'END {{ print NR }}')"
  if [ "$container_count" = "1" ]; then
    selected_container="$($runtime_cmd ps -a --format '{{{{.Names}}}}' | head -n1)"
  fi
fi
if [ -z "$selected_container" ]; then
  echo "Expected managed container $image_ref was not found." >&2
  exit 1
fi

if ! $runtime_cmd ps --format '{{{{.Names}}}}' | grep -Fxq "$selected_container"; then
  $runtime_cmd start "$selected_container" >/dev/null
fi

if $runtime_cmd exec "$selected_container" sh -lc 'command -v bash >/dev/null 2>&1'; then
  exec $runtime_cmd exec -it "$selected_container" bash -l
fi
exec $runtime_cmd exec -it "$selected_container" sh
"#
    );
    wrap_remote_shell_script(&script)
}

pub(crate) fn instance_shell_remote_command(workload: &InstanceWorkload) -> String {
    match workload {
        InstanceWorkload::Shell | InstanceWorkload::Unpack(_) => host_shell_remote_command(),
        InstanceWorkload::Container(container) => container_shell_remote_command(container),
    }
}
