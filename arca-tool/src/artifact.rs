use std::path::PathBuf;

use anyhow::{Context, Result};

pub use capulus::artifact_store::{
    ArtifactMetadata, ArtifactSummary, StoredArtifact, artifact_id_from_tracking_tag,
    default_archive_file_name, remote_tag, save_metadata, tracking_tag,
};
pub const CURRENT_SCHEMA_VERSION: u32 = capulus::artifact_store::CURRENT_SCHEMA_VERSION;

pub fn arca_image_labels(metadata: &ArtifactMetadata) -> Vec<(String, String)> {
    capulus::artifact_store::image_labels(TOOL_NAME, metadata)
}

pub fn artifact_summary_from_labels(
    labels: &std::collections::HashMap<String, String>,
) -> Option<ArtifactSummary> {
    capulus::artifact_store::artifact_summary_from_labels(TOOL_NAME, labels)
}

const TOOL_NAME: &str = "arca";

fn data_root() -> Result<PathBuf> {
    dirs::data_local_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("share")))
        .context("Failed to determine the local data directory")
}

pub fn create_artifact_dir() -> Result<(String, PathBuf)> {
    capulus::artifact_store::create_artifact_dir(data_root()?, TOOL_NAME)
}

pub fn load_stored_artifacts() -> Result<Vec<StoredArtifact>> {
    capulus::artifact_store::load_stored_artifacts(data_root()?, TOOL_NAME)
}

pub fn resolve_artifact(selector: Option<&str>) -> Result<StoredArtifact> {
    capulus::artifact_store::resolve_artifact(data_root()?, TOOL_NAME, selector)
}
