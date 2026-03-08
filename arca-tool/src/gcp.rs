use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use capulus::gcp::{self, AccessTokenRequest};
use dialoguer::Input;
use reqwest::Url;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::artifact::{
    ArtifactMetadata, ArtifactSummary, StoredArtifact, artifact_id_from_tracking_tag,
    artifact_summary_from_labels, save_metadata, tracking_tag,
};
use crate::command::{ensure_command_available, run_command_status, run_command_text};
use crate::config::{ArcaConfig, ensure_cache_root, save_global_config};
use crate::runtime::ContainerRuntime;
use crate::ui::{
    detail, maybe_open_browser, prompt_theme, require_interactive, spinner, stage, success,
};

const ACCESS_TOKEN_CACHE_FILE_NAME: &str = "gcp-access-token.toml";

#[derive(Debug, Clone, Copy)]
pub enum LoginMethod {
    AutoDetected,
    Prompted,
}

#[derive(Debug, Clone)]
pub struct LoginOutcome {
    pub method: LoginMethod,
    pub saved_path: Option<PathBuf>,
    pub repository: String,
    pub registry_host: String,
}

#[derive(Debug, Clone)]
pub struct RemoteArtifact {
    pub artifact_id: String,
    pub remote_ref: String,
    pub summary: Option<ArtifactSummary>,
    pub uploaded_at_epoch_ms: u128,
    pub uploaded_at_text: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactRegistryDockerImagesPage {
    #[serde(default)]
    #[serde(rename = "dockerImages")]
    docker_images: Vec<ArtifactRegistryDockerImage>,
    #[serde(default)]
    #[serde(rename = "nextPageToken")]
    next_page_token: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactRegistryDockerImage {
    uri: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(rename = "uploadTime")]
    upload_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcrImageListing {
    digest: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum RemoteArtifactEvent {
    InventoryLoaded {
        matched_artifacts: Vec<RemoteArtifact>,
        pending_remote_only: Vec<RemoteArtifact>,
    },
    RemoteOnlyResolved {
        artifact: RemoteArtifact,
        resolved: usize,
        total: usize,
    },
}

#[derive(Debug, Clone)]
struct RemoteArtifactCandidate {
    artifact: RemoteArtifact,
    digest_ref: String,
}

#[derive(Debug, Deserialize)]
struct RegistryManifest {
    config: RegistryManifestConfig,
}

#[derive(Debug, Deserialize)]
struct RegistryManifestConfig {
    digest: String,
}

#[derive(Debug, Deserialize)]
struct RegistryConfigBlob {
    config: RegistryRuntimeConfig,
}

#[derive(Debug, Deserialize)]
struct RegistryRuntimeConfig {
    #[serde(rename = "Labels")]
    labels: Option<HashMap<String, String>>,
}

pub fn login(
    config: &mut ArcaConfig,
    force: bool,
    repo_override: Option<String>,
    runtime: ContainerRuntime,
) -> Result<LoginOutcome> {
    ensure_command_available("gcloud")?;

    let detected_project = gcp::detect_project(config.auth.gcp.project.as_deref(), !force);
    let detected_creds_path =
        gcp::detect_credentials_path(config.auth.gcp.service_account_json.as_deref(), !force);
    let has_active_account = gcp::has_active_account()?;

    let mut changed = false;
    let mut method = LoginMethod::AutoDetected;

    if has_active_account || detected_creds_path.is_some() {
        if force {
            if detected_project.is_none() && config.auth.gcp.project.take().is_some() {
                changed = true;
            }
            if detected_creds_path.is_none()
                && config.auth.gcp.service_account_json.take().is_some()
            {
                changed = true;
            }
        }
        if let Some(project) = detected_project
            && config.auth.gcp.project.as_deref() != Some(project.as_str())
        {
            config.auth.gcp.project = Some(project);
            changed = true;
        }
        if let Some(path) = detected_creds_path
            && config.auth.gcp.service_account_json.as_deref() != Some(path.as_str())
        {
            config.auth.gcp.service_account_json = Some(path);
            changed = true;
        }
    } else {
        method = LoginMethod::Prompted;
        prompt_for_gcp_credentials(config, force)?;
        changed = true;
    }

    if !has_nonempty(config.auth.gcp.project.as_deref()) && repo_override.is_none() {
        require_interactive("`arca login` requires interactive stdin to choose a GCP project.")?;
        method = LoginMethod::Prompted;
        let project_seed = gcp::detect_project(config.auth.gcp.project.as_deref(), true)
            .or_else(|| config.auth.gcp.project.clone())
            .unwrap_or_default();
        let project = Input::<String>::with_theme(prompt_theme())
            .with_prompt("GCP project ID")
            .with_initial_text(project_seed)
            .interact_text()
            .context("Failed to read GCP project ID")?;
        let project = nonempty_string(project).ok_or_else(|| {
            anyhow!("A GCP project ID is required when no registry repository is provided.")
        })?;
        if config.auth.gcp.project.as_deref() != Some(project.as_str()) {
            config.auth.gcp.project = Some(project);
            changed = true;
        }
    }

    let repository = if let Some(repository) =
        repo_override.or_else(|| config.publish.repository.clone())
    {
        normalize_registry_repository(repository)?
    } else {
        let project = config
            .auth
            .gcp
            .project
            .as_deref()
            .ok_or_else(|| {
                anyhow!(
                    "A GCP project ID is required unless you provide `--repo HOST/PROJECT/REPOSITORY/IMAGE`."
                )
            })?
            .trim();
        normalize_registry_repository(format!("gcr.io/{project}/arca"))?
    };
    let registry_host = registry_host(&repository)?.to_owned();

    if config.publish.repository.as_deref() != Some(repository.as_str()) {
        config.publish.repository = Some(repository.clone());
        changed = true;
    }

    let saved_path = if changed {
        Some(save_global_config(config)?)
    } else {
        None
    };

    let login_spinner = spinner(&format!(
        "Configuring container runtime login for `{registry_host}`..."
    ));
    configure_registry_login(config, runtime, &registry_host)?;
    login_spinner.finish_with_message(format!(
        "Container runtime login configured for `{registry_host}`."
    ));

    Ok(LoginOutcome {
        method,
        saved_path,
        repository,
        registry_host,
    })
}

pub fn print_login_outcome(outcome: &LoginOutcome) {
    let source = match outcome.method {
        LoginMethod::AutoDetected => "using auto-detected credentials",
        LoginMethod::Prompted => "using newly entered credentials",
    };
    if let Some(path) = outcome.saved_path.as_deref() {
        println!(
            "Login `{}`: {source}. Repository `{}`. Updated {}.",
            outcome.registry_host,
            outcome.repository,
            path.display()
        );
    } else {
        println!(
            "Login `{}`: {source}. Repository `{}`. No config changes.",
            outcome.registry_host, outcome.repository
        );
    }
}

pub fn is_publish_configured(config: &ArcaConfig) -> bool {
    has_nonempty(config.publish.repository.as_deref())
}

pub fn push_artifact(
    config: &ArcaConfig,
    runtime: ContainerRuntime,
    artifact: &mut StoredArtifact,
) -> Result<String> {
    ensure_command_available("gcloud")?;
    let repository = config
        .publish
        .repository
        .clone()
        .ok_or_else(|| anyhow!("Missing registry repository. Run `arca login` first."))?;
    let repository = normalize_registry_repository(repository)?;
    let registry_host = registry_host(&repository)?;

    let archive_path = artifact.archive_path();
    if !archive_path.is_file() {
        bail!("Stored archive is missing: {}", archive_path.display());
    }

    stage("Refreshing container registry login");
    detail(registry_host);
    let login_spinner = spinner(&format!(
        "Refreshing container-runtime login for `{registry_host}`..."
    ));
    configure_registry_login(config, runtime, registry_host)?;
    login_spinner.finish_with_message(format!(
        "Container runtime login refreshed for `{registry_host}`."
    ));

    stage("Loading local image archive");
    detail(&archive_path.display().to_string());
    runtime.load(&archive_path)?;
    success("Local image archive loaded.");

    let remote_ref = remote_reference(&repository, &artifact.metadata);
    let tracking_remote_ref =
        tracking_remote_reference(&repository, &artifact.metadata.artifact_id);

    stage("Tagging image for push");
    detail(&remote_ref);
    detail(&tracking_remote_ref);
    let tag_spinner = spinner(&format!(
        "Tagging image as `{remote_ref}` and `{tracking_remote_ref}`..."
    ));
    runtime.tag(&artifact.metadata.local_tag, &remote_ref)?;
    runtime.tag(&artifact.metadata.local_tag, &tracking_remote_ref)?;
    tag_spinner.finish_with_message(format!("Image tagged for `{registry_host}`."));

    stage("Pushing image to the registry");
    detail(&remote_ref);
    detail(&tracking_remote_ref);
    runtime.push(&remote_ref)?;
    runtime.push(&tracking_remote_ref)?;
    success(&format!("Pushed `{remote_ref}`."));

    artifact.metadata.uploaded_ref = Some(remote_ref.clone());
    artifact.metadata.uploaded_at_epoch_ms = Some(now_epoch_ms()?);
    save_metadata(&artifact.dir, &artifact.metadata)?;

    Ok(remote_ref)
}

pub fn load_remote_artifacts<F>(
    config: &ArcaConfig,
    local_artifacts: &[StoredArtifact],
    mut on_event: F,
) -> Result<Vec<RemoteArtifact>>
where
    F: FnMut(RemoteArtifactEvent),
{
    let Some(repository) = config.publish.repository.clone() else {
        return Ok(Vec::new());
    };
    let repository = normalize_registry_repository(repository)?;
    let token = gcp_access_token(config)?;
    let client = Client::builder()
        .build()
        .context("Failed to initialize the registry inspection client")?;

    let mut candidates = if is_artifact_registry_host(registry_host(&repository)?) {
        load_artifact_registry_remote_artifact_candidates(
            &client,
            &repository,
            &token,
            local_artifacts,
        )?
    } else {
        load_gcr_remote_artifact_candidates(config, &client, &repository, &token, local_artifacts)?
    };
    hydrate_candidate_summaries_from_local_artifacts(&mut candidates, local_artifacts);

    let local_artifact_ids = local_artifacts
        .iter()
        .map(|artifact| artifact.metadata.artifact_id.clone())
        .collect::<std::collections::HashSet<_>>();
    let matched_artifacts = candidates
        .iter()
        .filter(|candidate| local_artifact_ids.contains(&candidate.artifact.artifact_id))
        .map(|candidate| candidate.artifact.clone())
        .collect::<Vec<_>>();
    let pending_remote_only = candidates
        .iter()
        .filter(|candidate| !local_artifact_ids.contains(&candidate.artifact.artifact_id))
        .map(|candidate| candidate.artifact.clone())
        .collect::<Vec<_>>();

    on_event(RemoteArtifactEvent::InventoryLoaded {
        matched_artifacts,
        pending_remote_only: pending_remote_only.clone(),
    });

    if pending_remote_only.is_empty() {
        return Ok(candidates
            .into_iter()
            .map(|candidate| candidate.artifact)
            .collect());
    }

    enrich_remote_only_artifacts(
        &client,
        &token,
        candidates,
        &local_artifact_ids,
        &mut on_event,
    )
}

fn hydrate_candidate_summaries_from_local_artifacts(
    candidates: &mut [RemoteArtifactCandidate],
    local_artifacts: &[StoredArtifact],
) {
    let local_summaries = local_artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.metadata.artifact_id.clone(),
                ArtifactSummary::from(&artifact.metadata),
            )
        })
        .collect::<HashMap<_, _>>();
    for candidate in candidates {
        if candidate.artifact.summary.is_none()
            && let Some(summary) = local_summaries.get(&candidate.artifact.artifact_id)
        {
            candidate.artifact.summary = Some(summary.clone());
        }
    }
}

fn prompt_for_gcp_credentials(config: &mut ArcaConfig, force: bool) -> Result<()> {
    require_interactive("`arca login` requires interactive stdin.")?;
    maybe_open_browser("https://console.cloud.google.com/");
    eprintln!(
        "Could not auto-detect GCP credentials. Provide a service-account JSON path, or run `gcloud auth login` and retry."
    );

    let project_seed = gcp::detect_project(config.auth.gcp.project.as_deref(), true)
        .or_else(|| (!force).then(|| config.auth.gcp.project.clone()).flatten())
        .unwrap_or_default();
    let creds_seed = gcp::detect_credentials_path(config.auth.gcp.service_account_json.as_deref(), true)
        .or_else(|| {
            (!force)
                .then(|| config.auth.gcp.service_account_json.clone())
                .flatten()
        })
        .unwrap_or_default();

    let project = Input::<String>::with_theme(prompt_theme())
        .with_prompt("GCP project ID")
        .with_initial_text(project_seed)
        .interact_text()
        .context("Failed to read GCP project ID")?;
    let service_account_json = Input::<String>::with_theme(prompt_theme())
        .with_prompt(
            "Service-account JSON path (leave blank if `gcloud auth login` is already active)",
        )
        .with_initial_text(creds_seed)
        .allow_empty(true)
        .interact_text()
        .context("Failed to read GCP credentials path")?;

    let project = nonempty_string(project)
        .ok_or_else(|| anyhow!("A GCP project ID is required for Google registry publishing."))?;
    let service_account_json = nonempty_string(service_account_json);
    if service_account_json.is_none() && !gcp::has_active_account()? {
        bail!(
            "No credentials configured. Provide a service-account JSON path, or run `gcloud auth login` and retry."
        );
    }

    config.auth.gcp.project = Some(project);
    config.auth.gcp.service_account_json = service_account_json;
    Ok(())
}

fn load_artifact_registry_remote_artifact_candidates(
    client: &Client,
    repository: &str,
    token: &str,
    local_artifacts: &[StoredArtifact],
) -> Result<Vec<RemoteArtifactCandidate>> {
    let parent = artifact_registry_parent(repository)?;
    let package_uri = repository.to_owned();
    let local_remote_tags = local_artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.metadata.remote_tag.clone(),
                artifact.metadata.artifact_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut page_token = String::new();
    let mut candidates = HashMap::<String, RemoteArtifactCandidate>::new();

    loop {
        let mut page_url = Url::parse(&format!(
            "https://artifactregistry.googleapis.com/v1/{parent}/dockerImages"
        ))
        .context("Failed to construct Artifact Registry list URL")?;
        page_url.query_pairs_mut().append_pair("pageSize", "1000");
        if !page_token.is_empty() {
            page_url
                .query_pairs_mut()
                .append_pair("pageToken", &page_token);
        }
        let request = client.get(page_url).bearer_auth(token);
        let page = request
            .send()
            .context("Failed to list Artifact Registry images")?
            .error_for_status()
            .context("Artifact Registry rejected the image list request")?
            .json::<ArtifactRegistryDockerImagesPage>()
            .context("Failed to parse Artifact Registry image list response")?;

        for image in page.docker_images {
            let Some((image_uri, _)) = image.uri.rsplit_once('@') else {
                continue;
            };
            if image_uri != package_uri || image.tags.is_empty() {
                continue;
            }
            if let Some(candidate) = build_remote_artifact_candidate(
                &image.uri,
                &image.tags,
                image.upload_time.as_deref(),
                &local_remote_tags,
            )? {
                merge_remote_artifact_candidate(&mut candidates, candidate);
            }
        }

        if page.next_page_token.is_empty() {
            break;
        }
        page_token = page.next_page_token;
    }

    Ok(candidates.into_values().collect())
}

fn load_gcr_remote_artifact_candidates(
    config: &ArcaConfig,
    _client: &Client,
    repository: &str,
    _token: &str,
    local_artifacts: &[StoredArtifact],
) -> Result<Vec<RemoteArtifactCandidate>> {
    let local_remote_tags = local_artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.metadata.remote_tag.clone(),
                artifact.metadata.artifact_id.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut command = gcloud_command(config);
    command.args([
        "container",
        "images",
        "list-tags",
        repository,
        "--format=json",
    ]);
    let output = run_command_text(&mut command, "list gcr image tags")?;
    let images = serde_json::from_str::<Vec<GcrImageListing>>(&output)
        .context("Failed to parse gcr image listing JSON")?;
    let mut candidates = HashMap::<String, RemoteArtifactCandidate>::new();

    for image in images {
        if image.tags.is_empty() {
            continue;
        }
        let digest_ref = format!("{repository}@{}", image.digest);
        if let Some(candidate) =
            build_remote_artifact_candidate(&digest_ref, &image.tags, None, &local_remote_tags)?
        {
            merge_remote_artifact_candidate(&mut candidates, candidate);
        }
    }

    Ok(candidates.into_values().collect())
}

fn enrich_remote_only_artifacts<F>(
    client: &Client,
    token: &str,
    candidates: Vec<RemoteArtifactCandidate>,
    local_artifact_ids: &std::collections::HashSet<String>,
    on_event: &mut F,
) -> Result<Vec<RemoteArtifact>>
where
    F: FnMut(RemoteArtifactEvent),
{
    let remote_only_candidates = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| !local_artifact_ids.contains(&candidate.artifact.artifact_id))
        .map(|(index, candidate)| (index, candidate.digest_ref.clone()))
        .collect::<Vec<_>>();
    if remote_only_candidates.is_empty() {
        return Ok(candidates
            .into_iter()
            .map(|candidate| candidate.artifact)
            .collect());
    }
    let total = remote_only_candidates.len();

    let worker_count = thread::available_parallelism()
        .map(|count| count.get().min(8))
        .unwrap_or(4)
        .max(1);
    let shared_jobs = Arc::new(Mutex::new(remote_only_candidates));
    let (result_tx, result_rx) = mpsc::channel();

    for _ in 0..worker_count {
        let client = client.clone();
        let token = token.to_owned();
        let shared_jobs = Arc::clone(&shared_jobs);
        let result_tx = result_tx.clone();
        thread::spawn(move || {
            loop {
                let Some((index, digest_ref)) =
                    shared_jobs.lock().ok().and_then(|mut jobs| jobs.pop())
                else {
                    break;
                };
                let summary = load_remote_summary(&client, &token, &digest_ref)
                    .ok()
                    .flatten();
                if result_tx.send((index, summary)).is_err() {
                    break;
                }
            }
        });
    }
    drop(result_tx);

    let mut resolved = 0usize;
    let mut artifacts = candidates
        .into_iter()
        .map(|candidate| candidate.artifact)
        .collect::<Vec<_>>();

    for (index, summary) in result_rx {
        if let Some(summary) =
            summary.filter(|summary| summary.artifact_id == artifacts[index].artifact_id)
        {
            artifacts[index].summary = Some(summary);
        }
        resolved += 1;
        on_event(RemoteArtifactEvent::RemoteOnlyResolved {
            artifact: artifacts[index].clone(),
            resolved,
            total,
        });
    }

    Ok(artifacts)
}

fn build_remote_artifact_candidate(
    digest_ref: &str,
    tags: &[String],
    uploaded_at_text: Option<&str>,
    local_remote_tags: &HashMap<String, String>,
) -> Result<Option<RemoteArtifactCandidate>> {
    let artifact_id = tags
        .iter()
        .find_map(|tag| artifact_id_from_tracking_tag(tag))
        .map(str::to_owned)
        .or_else(|| {
            tags.iter()
                .find_map(|tag| local_remote_tags.get(tag))
                .cloned()
        });
    let Some(artifact_id) = artifact_id else {
        return Ok(None);
    };
    let remote_tag =
        preferred_display_tag(tags, &artifact_id).unwrap_or_else(|| tracking_tag(&artifact_id));
    Ok(Some(RemoteArtifactCandidate {
        digest_ref: digest_ref.to_owned(),
        artifact: RemoteArtifact {
            artifact_id,
            remote_ref: format!("{}:{remote_tag}", strip_digest(digest_ref)?),
            summary: None,
            uploaded_at_epoch_ms: parse_remote_timestamp(uploaded_at_text)?,
            uploaded_at_text: uploaded_at_text.unwrap_or("unknown upload time").to_owned(),
        },
    }))
}

fn merge_remote_artifact_candidate(
    candidates: &mut HashMap<String, RemoteArtifactCandidate>,
    candidate: RemoteArtifactCandidate,
) {
    match candidates.get(&candidate.artifact.artifact_id) {
        Some(existing)
            if existing.artifact.uploaded_at_epoch_ms
                >= candidate.artifact.uploaded_at_epoch_ms => {}
        _ => {
            candidates.insert(candidate.artifact.artifact_id.clone(), candidate);
        }
    }
}

fn preferred_display_tag(tags: &[String], artifact_id: &str) -> Option<String> {
    let tracking = tracking_tag(artifact_id);
    tags.iter()
        .find(|tag| tag.as_str() != tracking)
        .cloned()
        .or_else(|| tags.iter().find(|tag| tag.as_str() == tracking).cloned())
}

fn artifact_registry_parent(repository: &str) -> Result<String> {
    let segments = repository.split('/').collect::<Vec<_>>();
    if segments.len() < 4 {
        bail!(
            "Artifact Registry image prefixes must look like `LOCATION-docker.pkg.dev/PROJECT/REPOSITORY/IMAGE`."
        );
    }
    let location = registry_host(repository)?
        .strip_suffix("-docker.pkg.dev")
        .ok_or_else(|| anyhow!("Artifact Registry hostname is missing its location prefix."))?;
    Ok(format!(
        "projects/{}/locations/{location}/repositories/{}",
        segments[1], segments[2]
    ))
}

fn strip_digest(image_ref: &str) -> Result<&str> {
    image_ref
        .rsplit_once('@')
        .map(|(image, _)| image)
        .ok_or_else(|| anyhow!("Registry image reference is missing a digest: `{image_ref}`"))
}

fn parse_remote_timestamp(value: Option<&str>) -> Result<u128> {
    let Some(value) = value else {
        return Ok(0);
    };
    Ok(OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("Failed to parse remote timestamp `{value}`"))?
        .unix_timestamp_nanos()
        .max(0) as u128
        / 1_000_000)
}

fn load_remote_summary(
    client: &Client,
    token: &str,
    image_ref: &str,
) -> Result<Option<ArtifactSummary>> {
    let (host, image_path, reference) = split_image_reference(image_ref)?;
    let manifest = client
        .get(format!("https://{host}/v2/{image_path}/manifests/{reference}"))
        .bearer_auth(token)
        .header(
            ACCEPT,
            "application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json",
        )
        .send()
        .with_context(|| format!("Failed to fetch registry manifest for `{image_ref}`"))?
        .error_for_status()
        .with_context(|| format!("Registry rejected manifest request for `{image_ref}`"))?
        .json::<RegistryManifest>()
        .with_context(|| format!("Failed to parse registry manifest for `{image_ref}`"))?;
    let blob = client
        .get(format!(
            "https://{host}/v2/{image_path}/blobs/{}",
            manifest.config.digest
        ))
        .bearer_auth(token)
        .send()
        .with_context(|| format!("Failed to fetch image config blob for `{image_ref}`"))?
        .error_for_status()
        .with_context(|| format!("Registry rejected image config request for `{image_ref}`"))?
        .json::<RegistryConfigBlob>()
        .with_context(|| format!("Failed to parse image config blob for `{image_ref}`"))?;
    Ok(blob
        .config
        .labels
        .and_then(|labels| artifact_summary_from_labels(&labels)))
}

fn split_image_reference(reference: &str) -> Result<(&str, &str, &str)> {
    let slash = reference
        .find('/')
        .ok_or_else(|| anyhow!("Registry image reference is missing a hostname: `{reference}`"))?;
    let host = &reference[..slash];
    let remainder = &reference[slash + 1..];

    if let Some((image_path, digest)) = remainder.rsplit_once('@') {
        if image_path.is_empty() || digest.trim().is_empty() {
            bail!("Invalid image digest reference `{reference}`.");
        }
        return Ok((host, image_path, digest));
    }

    let colon = remainder
        .rfind(':')
        .ok_or_else(|| anyhow!("Registry image reference is missing a tag: `{reference}`"))?;
    let image_path = &remainder[..colon];
    let tag = &remainder[colon + 1..];
    if image_path.is_empty() || tag.trim().is_empty() || tag.contains('/') {
        bail!("Invalid image tag reference `{reference}`.");
    }
    Ok((host, image_path, tag))
}

fn configure_registry_login(
    config: &ArcaConfig,
    runtime: ContainerRuntime,
    registry_host: &str,
) -> Result<()> {
    let token = gcp_access_token(config)?;
    runtime.login_password_stdin(
        &format!("https://{registry_host}"),
        "oauth2accesstoken",
        &token,
    )
}

fn gcp_access_token(config: &ArcaConfig) -> Result<String> {
    let cache_path = access_token_cache_path()?;
    gcp::access_token(AccessTokenRequest {
        configured_credentials_path: config.auth.gcp.service_account_json.as_deref(),
        cache_path: &cache_path,
    })
}

fn access_token_cache_path() -> Result<PathBuf> {
    Ok(ensure_cache_root()?.join(ACCESS_TOKEN_CACHE_FILE_NAME))
}

fn gcloud_command(config: &ArcaConfig) -> Command {
    gcp::command(config.auth.gcp.service_account_json.as_deref())
}

fn normalize_registry_repository(repository: String) -> Result<String> {
    let trimmed = repository
        .trim()
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .to_owned();
    if trimmed.is_empty() {
        bail!("Registry repository cannot be empty.");
    }
    if trimmed.contains('@') {
        bail!("Registry repository must be an image prefix without a digest.");
    }
    if trimmed.contains(':') {
        bail!("Registry repository must be an image prefix without a tag.");
    }

    let registry_host = registry_host(&trimmed)?;
    let segments = trimmed.split('/').collect::<Vec<_>>();
    if is_artifact_registry_host(registry_host) {
        if segments.len() < 4 {
            bail!(
                "Artifact Registry image prefixes must look like `LOCATION-docker.pkg.dev/PROJECT/REPOSITORY/IMAGE`."
            );
        }
    } else if is_gcr_host(registry_host) {
        if segments.len() < 3 {
            bail!("gcr image prefixes must look like `gcr.io/PROJECT/IMAGE`.");
        }
    } else {
        bail!(
            "Unsupported Google container registry host `{registry_host}`. Use `*.gcr.io` or `LOCATION-docker.pkg.dev`."
        );
    }
    Ok(trimmed)
}

fn remote_reference(repository: &str, metadata: &ArtifactMetadata) -> String {
    format!("{repository}:{}", metadata.remote_tag)
}

fn tracking_remote_reference(repository: &str, artifact_id: &str) -> String {
    format!("{repository}:{}", tracking_tag(artifact_id))
}

pub fn delete_remote_artifact(config: &ArcaConfig, remote_ref: &str) -> Result<()> {
    ensure_command_available("gcloud")?;
    let (host, _, _) = split_image_reference(remote_ref)?;
    let mut command = gcloud_command(config);
    if is_artifact_registry_host(host) {
        command.args([
            "artifacts",
            "docker",
            "images",
            "delete",
            remote_ref,
            "--delete-tags",
            "--quiet",
        ]);
        return run_command_status(&mut command, "delete Artifact Registry image");
    }

    command.args([
        "container",
        "images",
        "delete",
        remote_ref,
        "--force-delete-tags",
        "--quiet",
    ]);
    run_command_status(&mut command, "delete gcr image")
}

pub fn console_url_for_remote_ref(remote_ref: &str) -> Option<String> {
    let (host, image_path, _) = split_image_reference(remote_ref).ok()?;
    if is_artifact_registry_host(host) {
        let mut segments = image_path.split('/');
        let project = segments.next()?;
        let repository = segments.next()?;
        let package = segments.collect::<Vec<_>>().join("/");
        let location = host.strip_suffix("-docker.pkg.dev")?;
        let base = format!(
            "https://console.cloud.google.com/artifacts/docker/{project}/{location}/{repository}"
        );
        return Some(if package.is_empty() {
            format!("{base}?project={project}")
        } else {
            format!("{base}/{package}?project={project}")
        });
    }

    if is_gcr_host(host) {
        let (project, image) = image_path.split_once('/')?;
        return Some(format!(
            "https://console.cloud.google.com/gcr/images/{project}/{host}/{image}?project={project}"
        ));
    }

    None
}

fn registry_host(repository: &str) -> Result<&str> {
    repository
        .split('/')
        .next()
        .filter(|host| !host.trim().is_empty())
        .ok_or_else(|| anyhow!("Registry repository is missing a hostname."))
}

fn is_artifact_registry_host(host: &str) -> bool {
    host.ends_with("-docker.pkg.dev")
}

fn is_gcr_host(host: &str) -> bool {
    host == "gcr.io" || host == "us.gcr.io" || host == "eu.gcr.io" || host == "asia.gcr.io"
}

fn has_nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn nonempty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn now_epoch_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before the Unix epoch")?
        .as_millis())
}

#[cfg(test)]
mod tests {
    use super::{
        ACCESS_TOKEN_CACHE_FILE_NAME, CachedAccessToken, RemoteArtifact, RemoteArtifactCandidate,
        hydrate_candidate_summaries_from_local_artifacts, normalize_registry_repository,
        registry_host, split_image_reference,
    };
    use crate::artifact::{ArtifactMetadata, StoredArtifact};
    use std::path::PathBuf;

    #[test]
    fn accepts_artifact_registry_prefix() {
        let repository = normalize_registry_repository(
            "us-west1-docker.pkg.dev/my-project/my-repo/my-image".to_owned(),
        )
        .expect("artifact registry prefix should validate");
        assert_eq!(
            repository,
            "us-west1-docker.pkg.dev/my-project/my-repo/my-image"
        );
        assert_eq!(
            registry_host(&repository).expect("registry host should parse"),
            "us-west1-docker.pkg.dev"
        );
    }

    #[test]
    fn rejects_non_google_registry_hosts() {
        let error = normalize_registry_repository("ghcr.io/acme/image".to_owned())
            .expect_err("non-google registries should be rejected");
        assert!(
            error
                .to_string()
                .contains("Unsupported Google container registry host"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn splits_tag_and_digest_references() {
        let (host, image, tag) = split_image_reference(
            "us-west1-docker.pkg.dev/my-project/my-repo/my-image:deadbeef-demo",
        )
        .expect("tag reference should parse");
        assert_eq!(host, "us-west1-docker.pkg.dev");
        assert_eq!(image, "my-project/my-repo/my-image");
        assert_eq!(tag, "deadbeef-demo");

        let (_, image, digest) =
            split_image_reference("gcr.io/my-project/my-image@sha256:1234abcd")
                .expect("digest reference should parse");
        assert_eq!(image, "my-project/my-image");
        assert_eq!(digest, "sha256:1234abcd");
    }

    #[test]
    fn hydrates_matched_remote_candidates_with_local_summary() {
        let mut candidates = vec![RemoteArtifactCandidate {
            artifact: RemoteArtifact {
                artifact_id: "deadbeef".to_owned(),
                remote_ref: "repo/image:demo-deadbeef".to_owned(),
                summary: None,
                uploaded_at_epoch_ms: 9,
                uploaded_at_text: "later".to_owned(),
            },
            digest_ref: "repo/image@sha256:1234".to_owned(),
        }];
        let local_artifacts = vec![StoredArtifact {
            dir: PathBuf::from("/tmp/deadbeef"),
            metadata: ArtifactMetadata {
                schema_version: 3,
                artifact_id: "deadbeef".to_owned(),
                remote_tag: "demo-deadbeef".to_owned(),
                build_fingerprint: "fingerprint".to_owned(),
                kind: "rust".to_owned(),
                created_at_epoch_ms: 1,
                crate_name: "demo".to_owned(),
                crate_version: "0.1.0".to_owned(),
                binary_name: "demo".to_owned(),
                source_path: "/tmp/demo".to_owned(),
                manifest_path: "/tmp/demo/Cargo.toml".to_owned(),
                cargo_profile: "dev".to_owned(),
                cargo_features: Vec::new(),
                base_image: "ubuntu:24.04".to_owned(),
                runtime: "docker".to_owned(),
                local_tag: "arca-local:demo-deadbeef".to_owned(),
                archive_file: "image.tar".to_owned(),
                uploaded_ref: Some("repo/image:demo-deadbeef".to_owned()),
                uploaded_at_epoch_ms: Some(9),
            },
        }];

        hydrate_candidate_summaries_from_local_artifacts(&mut candidates, &local_artifacts);

        assert_eq!(
            candidates[0]
                .artifact
                .summary
                .as_ref()
                .expect("summary should be hydrated")
                .created_at_epoch_ms,
            1
        );
    }

    #[test]
    fn cached_access_token_round_trips_through_toml() {
        let cached = CachedAccessToken {
            token: "token".to_owned(),
            fetched_at_epoch_ms: 42,
            credential_source: "adc:/tmp/creds.json".to_owned(),
        };

        let serialized = toml::to_string_pretty(&cached).expect("cache should serialize as TOML");
        assert!(serialized.contains("token = "));

        let decoded =
            toml::from_str::<CachedAccessToken>(&serialized).expect("cache should parse as TOML");
        assert_eq!(decoded.token, cached.token);
        assert_eq!(decoded.fetched_at_epoch_ms, cached.fetched_at_epoch_ms);
        assert_eq!(decoded.credential_source, cached.credential_source);
    }

    #[test]
    fn access_token_cache_file_uses_toml_extension() {
        assert_eq!(ACCESS_TOKEN_CACHE_FILE_NAME, "gcp-access-token.toml");
    }
}
