use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

const ARCA_DIR_NAME: &str = ".arca";
const ARCA_CONTAINERS_DIR_NAME: &str = "containers";
const ARCA_METADATA_FILE_NAME: &str = "metadata.toml";
const ARCA_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone)]
pub(crate) struct LocalArcaArtifact {
    pub(crate) artifact_id: String,
    pub(crate) crate_name: String,
    pub(crate) local_tag: String,
    pub(crate) created_at_epoch_ms: u128,
}

#[derive(Debug, Deserialize)]
struct LocalArcaMetadata {
    schema_version: u32,
    artifact_id: String,
    crate_name: String,
    local_tag: String,
    created_at_epoch_ms: u128,
}

pub(crate) fn arca_source(selector: Option<&str>) -> String {
    match selector.map(str::trim).filter(|value| !value.is_empty()) {
        Some(selector) => format!("arca:{selector}"),
        None => "arca".to_owned(),
    }
}

pub(crate) fn parse_arca_source(value: &str) -> Option<Option<&str>> {
    let value = value.trim();
    if value == "arca" || value == "arca:" {
        return Some(None);
    }
    value.strip_prefix("arca:").map(|selector| {
        let selector = selector.trim();
        if selector.is_empty() {
            None
        } else {
            Some(selector)
        }
    })
}

pub(crate) fn resolve_local_arca_artifact(selector: Option<&str>) -> Result<LocalArcaArtifact> {
    let artifacts = load_local_arca_artifacts()?;
    if artifacts.is_empty() {
        bail!("No local `arca` artifacts were found in `~/.arca/containers`.");
    }
    let Some(selector) = selector.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(artifacts[0].clone());
    };

    if let Some(artifact) = artifacts
        .iter()
        .find(|artifact| artifact.artifact_id == selector)
    {
        return Ok(artifact.clone());
    }

    let id_prefix_matches = artifacts
        .iter()
        .filter(|artifact| artifact.artifact_id.starts_with(selector))
        .cloned()
        .collect::<Vec<_>>();
    if id_prefix_matches.len() == 1 {
        return Ok(id_prefix_matches[0].clone());
    }
    if id_prefix_matches.len() > 1 {
        bail!(
            "Local `arca` selector `{selector}` is ambiguous: {}",
            id_prefix_matches
                .iter()
                .map(|artifact| artifact.artifact_id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if let Some(artifact) = artifacts
        .iter()
        .find(|artifact| artifact.crate_name == selector)
        .cloned()
    {
        return Ok(artifact);
    }

    Err(anyhow!(
        "No local `arca` artifact matched `{selector}`. Run `arca list`."
    ))
}

fn load_local_arca_artifacts() -> Result<Vec<LocalArcaArtifact>> {
    let root = local_arca_containers_root()?;
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut artifacts = Vec::new();
    for entry in
        fs::read_dir(&root).with_context(|| format!("Failed to read {}", root.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read {}", root.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let metadata_path = path.join(ARCA_METADATA_FILE_NAME);
        if !metadata_path.is_file() {
            continue;
        }
        let content = fs::read_to_string(&metadata_path)
            .with_context(|| format!("Failed to read {}", metadata_path.display()))?;
        let Ok(metadata) = toml::from_str::<LocalArcaMetadata>(&content) else {
            continue;
        };
        if metadata.schema_version != ARCA_SCHEMA_VERSION {
            continue;
        }
        artifacts.push(LocalArcaArtifact {
            artifact_id: metadata.artifact_id,
            crate_name: metadata.crate_name,
            local_tag: metadata.local_tag,
            created_at_epoch_ms: metadata.created_at_epoch_ms,
        });
    }
    artifacts.sort_by(|left, right| {
        right
            .created_at_epoch_ms
            .cmp(&left.created_at_epoch_ms)
            .then_with(|| left.artifact_id.cmp(&right.artifact_id))
    });
    Ok(artifacts)
}

fn local_arca_containers_root() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(ARCA_DIR_NAME).join(ARCA_CONTAINERS_DIR_NAME))
}
