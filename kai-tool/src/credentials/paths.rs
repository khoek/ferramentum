use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub credentials_home: PathBuf,
    pub codex_home: PathBuf,
    active_auth: Option<PathBuf>,
}

impl RuntimePaths {
    pub fn from_env() -> Result<Self> {
        let home = capulus::paths::home_dir()
            .context("could not determine the current user's home directory")?;
        Self::new(
            env_path("KAI_CREDENTIALS_HOME")
                .unwrap_or_else(|| home.join(".kai").join("credentials")),
            env_path("CODEX_HOME").unwrap_or_else(|| home.join(".codex")),
        )
    }

    pub fn new(credentials_home: PathBuf, codex_home: PathBuf) -> Result<Self> {
        Ok(Self {
            credentials_home: absolute(&credentials_home)
                .context("could not resolve KAI_CREDENTIALS_HOME")?,
            codex_home: absolute(&codex_home).context("could not resolve CODEX_HOME")?,
            active_auth: None,
        })
    }

    pub fn active_auth(&self) -> PathBuf {
        self.active_auth
            .clone()
            .unwrap_or_else(|| self.codex_home.join("auth.json"))
    }

    pub fn with_active_auth(&self, active_auth: PathBuf) -> Result<Self> {
        let mut paths = self.clone();
        paths.active_auth = Some(absolute(&active_auth).context("could not resolve active auth")?);
        Ok(paths)
    }

    pub fn codex_config(&self) -> PathBuf {
        self.codex_home.join("config.toml")
    }

    pub fn read_codex_config(&self) -> Result<Option<toml::Table>> {
        let path = self.codex_config();
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("could not read {}", path.display()));
            }
        };
        toml::from_str(&contents)
            .with_context(|| format!("could not parse {}", path.display()))
            .map(Some)
    }

    pub fn profiles_dir(&self) -> PathBuf {
        self.credentials_home.join("profiles")
    }

    pub fn state_file(&self) -> PathBuf {
        self.credentials_home.join("state.json")
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("path cannot be empty");
    }
    std::path::absolute(path).map_err(Into::into)
}
