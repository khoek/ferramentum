use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

pub const CONFIG_DIR_NAME: &str = ".arca";
pub const CONFIG_FILE_NAME: &str = "config.toml";
pub const CONTAINERS_DIR_NAME: &str = "containers";
pub const CACHE_DIR_NAME: &str = "cache";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArcaConfig {
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub publish: PublishConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub gcp: GcpAuth,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcpAuth {
    pub project: Option<String>,
    pub service_account_json: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PublishConfig {
    #[serde(alias = "gcr_repository")]
    pub repository: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub rust: RustProjectConfig,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RustProjectConfig {
    pub profile: Option<String>,
    pub bin: Option<String>,
    pub features: Option<Vec<String>>,
    pub base_image: Option<String>,
}

pub fn global_config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME).join(CONFIG_FILE_NAME))
}

pub fn load_global_config() -> Result<ArcaConfig> {
    let path = global_config_path()?;
    if !path.exists() {
        return Ok(ArcaConfig::default());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(ArcaConfig::default());
    }

    toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))
}

pub fn save_global_config(config: &ArcaConfig) -> Result<PathBuf> {
    let path = global_config_path()?;
    write_toml_file(&path, config)?;
    Ok(path)
}

pub fn containers_root() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME).join(CONTAINERS_DIR_NAME))
}

pub fn ensure_containers_root() -> Result<PathBuf> {
    let path = containers_root()?;
    fs::create_dir_all(&path)
        .with_context(|| format!("Failed to create container directory: {}", path.display()))?;
    Ok(path)
}

pub fn cache_root() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME).join(CACHE_DIR_NAME))
}

pub fn ensure_cache_root() -> Result<PathBuf> {
    let path = cache_root()?;
    fs::create_dir_all(&path)
        .with_context(|| format!("Failed to create cache directory: {}", path.display()))?;
    Ok(path)
}

pub fn load_project_config(crate_dir: &Path) -> Result<ProjectConfig> {
    let path = project_config_path(crate_dir);
    if !path.exists() {
        return Ok(ProjectConfig::default());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(ProjectConfig::default());
    }

    toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))
}

pub fn save_project_config(crate_dir: &Path, config: &ProjectConfig) -> Result<PathBuf> {
    let path = project_config_path(crate_dir);
    write_toml_file(&path, config)?;
    Ok(path)
}

pub fn project_config_path(crate_dir: &Path) -> PathBuf {
    crate_dir.join(CONFIG_DIR_NAME).join(CONFIG_FILE_NAME)
}

fn write_toml_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
    }
    let content = toml::to_string_pretty(value).context("Failed to serialize config TOML")?;
    fs::write(path, content)
        .with_context(|| format!("Failed to write config file: {}", path.display()))?;
    Ok(())
}
