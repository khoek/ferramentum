use anyhow::Result;

#[derive(Debug, Clone)]
pub(crate) struct LocalArcaArtifact {
    pub(crate) local_tag: String,
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
    let artifact = capulus::artifact_store::resolve_artifact(
        dirs::data_local_dir()
            .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("share")))
            .ok_or_else(|| anyhow::anyhow!("Failed to determine the local data directory"))?,
        "arca",
        selector,
    )?;
    Ok(LocalArcaArtifact {
        local_tag: artifact.metadata.local_tag,
    })
}
