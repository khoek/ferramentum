use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use capulus::paths::app_dir;
use capulus::store::{
    ensure_directory, load_toml_or_default, write_toml_file as write_shared_toml_file,
};
use serde::{Deserialize, Serialize};

pub const CONFIG_DIR_NAME: &str = ".arca";
pub const CONFIG_FILE_NAME: &str = "config.toml";
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
    Ok(app_dir("arca")?.join(CONFIG_FILE_NAME))
}

pub fn load_global_config() -> Result<ArcaConfig> {
    load_toml_or_default(&global_config_path()?)
}

pub fn save_global_config(config: &ArcaConfig) -> Result<PathBuf> {
    let path = global_config_path()?;
    write_toml_file(&path, config)?;
    Ok(path)
}

pub fn cache_root() -> Result<PathBuf> {
    Ok(app_dir("arca")?.join(CACHE_DIR_NAME))
}

pub fn ensure_cache_root() -> Result<PathBuf> {
    let path = cache_root()?;
    ensure_directory(&path, None)?;
    Ok(path)
}

pub fn load_project_config(crate_dir: &Path) -> Result<ProjectConfig> {
    load_toml_or_default(&project_config_path(crate_dir))
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
    write_shared_toml_file(path, value, None, None)
        .with_context(|| format!("Failed to write config file: {}", path.display()))
}
