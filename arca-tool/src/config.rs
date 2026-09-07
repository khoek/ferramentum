use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
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
    Ok(capulus::paths::app_dir(config_root()?, "arca").join(CONFIG_FILE_NAME))
}

fn config_root() -> Result<PathBuf> {
    dirs::config_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
        .context("Failed to determine the configuration directory")
}

fn lock_root() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .map(|path| path.join("capulus"))
        .or_else(|| dirs::home_dir().map(|home| home.join(".capulus").join("locks")))
        .unwrap_or_else(|| std::env::temp_dir().join("capulus"))
}

fn acquire_named(name: &str, wait: bool) -> Result<capulus::InvocationLock> {
    Ok(capulus::acquire_named_in(lock_root(), name, wait)?)
}

pub fn load_global_config() -> Result<ArcaConfig> {
    load_toml_or_default(&global_config_path()?)
}

pub fn acquire_global_config_lock(wait: bool) -> Result<capulus::InvocationLock> {
    acquire_named("arca.config", wait)
}

pub fn save_global_config(config: &ArcaConfig) -> Result<PathBuf> {
    let path = global_config_path()?;
    write_toml_file(&path, config)?;
    Ok(path)
}

pub fn cache_root() -> Result<PathBuf> {
    Ok(capulus::paths::app_dir(config_root()?, "arca").join(CACHE_DIR_NAME))
}

pub fn ensure_cache_root() -> Result<PathBuf> {
    let path = cache_root()?;
    ensure_directory(&path, None)?;
    Ok(path)
}

pub fn load_project_config(crate_dir: &Path) -> Result<ProjectConfig> {
    load_toml_or_default(&project_config_path(crate_dir))
}

pub fn acquire_project_config_lock(
    crate_dir: &Path,
    wait: bool,
) -> Result<capulus::InvocationLock> {
    let lock_id = hash_path_component(crate_dir);
    acquire_named(&format!("arca.project-config.{lock_id}"), wait)
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

pub fn acquire_gcp_token_lock(wait: bool) -> Result<capulus::InvocationLock> {
    acquire_named("arca.gcp-token", wait)
}

pub fn acquire_artifact_lock(artifact_id: &str, wait: bool) -> Result<capulus::InvocationLock> {
    acquire_named(&format!("arca.artifact.{artifact_id}"), wait)
}

fn hash_path_component(path: &Path) -> String {
    let normalized = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    normalized.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
