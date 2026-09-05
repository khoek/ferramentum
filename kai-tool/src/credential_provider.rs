use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use crate::codex::{CredentialFailure, CredentialUseGuard};

const MAX_RESPONSE_BYTES: u64 = 64 * 1024;
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const MANAGED_AUTH_ENV_VARS: &[&str] = &[
    "CODEX_HOME",
    "CODEX_SQLITE_HOME",
    "CODEX_AUTH_FILE",
    "CODEX_ACCESS_TOKEN",
    "CODEX_API_KEY",
    "OPENAI_API_KEY",
    "CODEX_REFRESH_TOKEN_URL_OVERRIDE",
    "CODEX_REVOKE_TOKEN_URL_OVERRIDE",
    "CODEX_APP_SERVER_LOGIN_CLIENT_ID",
    "CODEX_INTERNAL_ORIGINATOR_OVERRIDE",
    "CODEX_AUTHAPI_BASE_URL",
    "CODEX_AGENT_IDENTITY_AUTHAPI_BASE_URL",
    "CODEX_AGENT_IDENTITY_JWKS_BASE_URL",
    "OPENAI_FEDERATION_RULE_ID",
    "OPENAI_IDENTITY_TOKEN_FILE",
    "OPENAI_WORKLOAD_IDENTITY_CONTEXT",
];

#[derive(Debug)]
pub(crate) struct CodexEnvironment {
    pub(crate) codex_home: PathBuf,
    pub(crate) sqlite_home: PathBuf,
}

impl CodexEnvironment {
    pub(crate) fn from_env(cwd: &Path) -> Result<Self> {
        let codex_home = absolute(
            env_path("CODEX_HOME").unwrap_or(home_dir()?.join(".codex")),
            cwd,
        )?;
        let sqlite_home = absolute(
            env_path("CODEX_SQLITE_HOME").unwrap_or_else(|| codex_home.clone()),
            cwd,
        )?;
        Ok(Self {
            codex_home,
            sqlite_home,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CredentialPaths {
    pub(crate) auth_file: PathBuf,
    pub(crate) credential_use_lock: PathBuf,
    pub(crate) credential_use_lock_mode: LockMode,
    pub(crate) credential_mutation_lock: PathBuf,
    pub(crate) available_file: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
        }
    }
}

impl CredentialPaths {
    fn validate(&self) -> Result<()> {
        let paths = [
            &self.auth_file,
            &self.credential_use_lock,
            &self.credential_mutation_lock,
            &self.available_file,
        ];
        for (index, path) in paths.iter().enumerate() {
            if !path.is_absolute() {
                bail!(
                    "credential provider returned a relative path: {}",
                    path.display()
                );
            }
            if paths[..index].contains(path) {
                bail!("credential provider returned overlapping paths");
            }
        }
        Ok(())
    }

    fn is_available(&self) -> Result<bool> {
        for path in [
            &self.available_file,
            &self.auth_file,
            &self.credential_mutation_lock,
        ] {
            match fs::symlink_metadata(path) {
                Ok(metadata) => validate_private_metadata(path, &metadata)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("could not inspect {}", path.display()));
                }
            }
        }
        Ok(true)
    }
}

pub(crate) struct SelectedCredential {
    pub(crate) paths: CredentialPaths,
    pub(crate) guard: CredentialUseGuard,
}

#[derive(Debug)]
pub(crate) struct CredentialProvider {
    script: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserConfig {
    credential_provider: Option<PathBuf>,
}

impl CredentialProvider {
    pub(crate) fn load(script: Option<&Path>) -> Result<Option<Self>> {
        let cwd = env::current_dir().context("could not read the current directory")?;
        let config_path = user_config_path()?;
        let config = match fs::read_to_string(&config_path) {
            Ok(contents) => {
                toml::from_str::<UserConfig>(&contents)
                    .with_context(|| format!("could not parse {}", config_path.display()))?
                    .credential_provider
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not read {}", config_path.display()));
            }
        };
        let script = if let Some(script) = script {
            Some(absolute(script.to_owned(), &cwd)?)
        } else {
            config
                .map(|script| absolute(script, config_path.parent().unwrap_or(&cwd)))
                .transpose()?
        };
        let Some(script) = script else {
            return Ok(None);
        };
        let metadata = fs::metadata(&script)
            .with_context(|| format!("could not inspect provider script {}", script.display()))?;
        if !metadata.is_file() {
            bail!(
                "credential provider script is not a file: {}",
                script.display()
            );
        }
        Ok(Some(Self { script }))
    }

    pub(crate) fn select(
        &self,
        environment: &CodexEnvironment,
        previous: Option<(&CredentialPaths, &CredentialFailure)>,
    ) -> Result<SelectedCredential> {
        let deadline = Instant::now() + PROVIDER_TIMEOUT;
        let mut paths = self.invoke(environment, previous, deadline)?;
        loop {
            if let Some(guard) = CredentialUseGuard::try_acquire(
                &paths.credential_use_lock,
                paths.credential_use_lock_mode,
            )? && paths.is_available()?
            {
                return Ok(SelectedCredential { paths, guard });
            }
            paths = self.invoke(environment, None, deadline)?;
        }
    }

    fn invoke(
        &self,
        environment: &CodexEnvironment,
        previous: Option<(&CredentialPaths, &CredentialFailure)>,
        deadline: Instant,
    ) -> Result<CredentialPaths> {
        if Instant::now() >= deadline {
            bail!("credential provider selection did not finish within 30 seconds");
        }
        let mut command = Command::new("bash");
        command
            .arg(&self.script)
            .arg(if previous.is_some() {
                "next"
            } else {
                "acquire"
            })
            .arg("--codex-home")
            .arg(&environment.codex_home)
            .arg("--sqlite-home")
            .arg(&environment.sqlite_home)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some((paths, failure)) = previous {
            command
                .arg("--auth-file")
                .arg(&paths.auth_file)
                .arg("--credential-use-lock")
                .arg(&paths.credential_use_lock)
                .arg("--credential-use-lock-mode")
                .arg(paths.credential_use_lock_mode.as_str())
                .arg("--credential-mutation-lock")
                .arg(&paths.credential_mutation_lock)
                .arg("--available-file")
                .arg(&paths.available_file)
                .arg("--cause")
                .arg(failure.cause.as_str());
            if let Some(until) = failure.unavailable_until {
                command.arg("--unavailable-until").arg(until.to_string());
            }
        }
        for name in MANAGED_AUTH_ENV_VARS {
            command.env_remove(name);
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("could not run {}", self.script.display()))?;
        let stdout = child.stdout.take().context("provider stdout is missing")?;
        let (send, receive) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let result = stdout
                .take(MAX_RESPONSE_BYTES + 1)
                .read_to_end(&mut bytes)
                .map(|_| bytes);
            let _ = send.send(result);
        });
        let response = match receive
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(Ok(response)) if response.len() as u64 <= MAX_RESPONSE_BYTES => response,
            response => {
                child.kill().ok();
                child.wait().ok();
                match response {
                    Ok(Err(error)) => {
                        return Err(error).context("could not read credential provider response");
                    }
                    Ok(Ok(_)) => bail!("credential provider response exceeds 64 KiB"),
                    Err(_) => bail!("credential provider did not respond within 30 seconds"),
                }
            }
        };
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .context("could not wait for credential provider")?
            {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().ok();
                child.wait().ok();
                bail!("credential provider did not exit within 30 seconds");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        if !status.success() {
            bail!("credential provider exited with {status}");
        }
        let paths: CredentialPaths = serde_json::from_slice(&response)
            .context("credential provider returned invalid JSON")?;
        paths.validate()?;
        Ok(paths)
    }
}

pub(crate) fn open_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("could not open {}", path.display()))?;
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    validate_private_metadata(path, &opened)?;
    validate_private_metadata(path, &named)?;
    #[cfg(unix)]
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
        bail!(
            "credential lock file changed while opening: {}",
            path.display()
        );
    }
    Ok(file)
}

fn validate_private_metadata(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    if metadata.nlink() == 0 {
        return Err(io::Error::new(io::ErrorKind::NotFound, "credential file was removed").into());
    }
    if !metadata.is_file() {
        bail!("credential path is not a regular file: {}", path.display());
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        bail!(
            "credential path must be a user-owned file with mode 0600 and one hard link: {}",
            path.display()
        );
    }
    Ok(())
}

fn home_dir() -> Result<PathBuf> {
    capulus::paths::home_dir().context("could not determine the current user's home directory")
}

fn user_config_path() -> Result<PathBuf> {
    Ok(env_path("XDG_CONFIG_HOME")
        .unwrap_or(home_dir()?.join(".config"))
        .join("kai/config.toml"))
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn absolute(path: PathBuf, base: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("configured path cannot be empty");
    }
    std::path::absolute(if path.is_absolute() {
        path
    } else {
        base.join(path)
    })
    .context("could not resolve configured path")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn a_removed_selection_is_retried_without_recreating_its_files() {
        let root = tempfile::tempdir().unwrap();
        let current = root.path().join("current");
        let removed = root.path().join("removed");
        fs::create_dir(&current).unwrap();
        fs::create_dir(&removed).unwrap();
        let paths = |directory: &Path| {
            serde_json::json!({
                "auth_file": directory.join("auth.json"),
            "credential_use_lock": directory.join("use.lock"),
            "credential_use_lock_mode": "shared",
                "credential_mutation_lock": directory.join("mutation.lock"),
                "available_file": directory.join("available"),
            })
        };
        for directory in [&current, &removed] {
            for (key, file) in paths(directory).as_object().unwrap() {
                if key == "credential_use_lock_mode" {
                    continue;
                }
                let path = file.as_str().unwrap();
                fs::write(path, b"").unwrap();
                fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        fs::remove_dir_all(&removed).unwrap();
        let script = root.path().join("provider.sh");
        let counter = root.path().join("selected");
        fs::write(&script, format!(
            "if test -e '{counter}'; then printf '%s\\n' '{current}'; else touch '{counter}'; printf '%s\\n' '{removed}'; fi\n",
            counter = counter.display(), current = paths(&current), removed = paths(&removed),
        )).unwrap();
        let provider = CredentialProvider { script };
        let selected = provider
            .select(
                &CodexEnvironment {
                    codex_home: root.path().join("codex"),
                    sqlite_home: root.path().join("sqlite"),
                },
                None,
            )
            .unwrap();
        assert_eq!(selected.paths.auth_file, current.join("auth.json"));
        assert!(!removed.exists());
    }

    #[test]
    fn provider_response_requires_distinct_absolute_paths() {
        let valid = CredentialPaths {
            auth_file: "/pool/credential/auth.json".into(),
            credential_use_lock: "/pool/credential/use.lock".into(),
            credential_use_lock_mode: LockMode::Shared,
            credential_mutation_lock: "/pool/credential/mutation.lock".into(),
            available_file: "/pool/credential/available".into(),
        };
        valid.validate().unwrap();
        let mut relative = valid.clone();
        relative.auth_file = "auth.json".into();
        assert!(relative.validate().is_err());
        let mut overlap = valid;
        overlap.credential_mutation_lock = overlap.credential_use_lock.clone();
        assert!(overlap.validate().is_err());
        assert!(serde_json::from_str::<CredentialPaths>(r#"{"auth_file":"/auth","credential_use_lock":"/use","credential_mutation_lock":"/mutation","available_file":"/available","unexpected":true}"#).is_err());
    }
}
