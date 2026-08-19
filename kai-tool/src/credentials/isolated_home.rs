use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tempfile::{Builder, TempDir};

use super::auth::Credential;
use super::paths::RuntimePaths;
use super::store::atomic_write;

const AUTH_CONFIG_KEYS: &[&str] = &[
    "chatgpt_base_url",
    "forced_chatgpt_workspace_id",
    "forced_login_method",
];

/// A short-lived, file-backed Codex home that cannot affect the user's active account.
///
/// Each instance has its own `auth.json` and minimal auth/network configuration. `TempDir`
/// removes the entire home on drop, including a token that Codex may have rotated in place.
pub struct IsolatedCodexHome {
    directory: TempDir,
}

/// A process-lifetime private credential file for a supervised Codex process.
///
/// The supervised process keeps the user's real `CODEX_HOME` so sessions, configuration, plugins,
/// skills, and SQLite state remain canonical. The downstream `+k` Codex build reads and refreshes
/// authentication from this separate file through `CODEX_AUTH_FILE`.
pub struct SupervisedAuthHome {
    directory: TempDir,
}

impl IsolatedCodexHome {
    pub fn create(paths: &RuntimePaths, purpose: &str) -> Result<Self> {
        let directory = Builder::new()
            .prefix(&format!(".{purpose}-"))
            .tempdir_in(&paths.credentials_home)
            .with_context(|| format!("could not create isolated Codex {purpose} directory"))?;
        set_private_directory(&directory)?;

        let home = Self { directory };
        home.write_config(paths)?;
        Ok(home)
    }

    pub fn with_credential(
        paths: &RuntimePaths,
        purpose: &str,
        credential: &Credential,
    ) -> Result<Self> {
        let home = Self::create(paths, purpose)?;
        atomic_write(&home.auth_path(), credential.as_bytes())
            .with_context(|| format!("could not seed isolated Codex {purpose} credential"))?;
        Ok(home)
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn auth_path(&self) -> std::path::PathBuf {
        self.path().join("auth.json")
    }

    pub fn credential(&self) -> Result<Credential> {
        Credential::read(&self.auth_path())
            .context("could not read credential returned by isolated Codex process")
    }

    fn write_config(&self, paths: &RuntimePaths) -> Result<()> {
        let mut isolated = toml::Table::new();
        if let Some(source) = paths.read_codex_config()? {
            for key in AUTH_CONFIG_KEYS {
                if let Some(value) = source.get(*key) {
                    isolated.insert((*key).to_owned(), value.clone());
                }
            }
            copy_respect_system_proxy(&source, &mut isolated);
        }
        isolated.insert(
            "cli_auth_credentials_store".to_owned(),
            toml::Value::String("file".to_owned()),
        );
        atomic_write(
            &self.path().join("config.toml"),
            toml::to_string_pretty(&isolated)?.as_bytes(),
        )
        .context("could not configure isolated Codex home")
    }
}

impl SupervisedAuthHome {
    pub fn with_credential(paths: &RuntimePaths, credential: &Credential) -> Result<Self> {
        let directory = Builder::new()
            .prefix(".agent-auth-")
            .tempdir_in(&paths.credentials_home)
            .context("could not create supervised Codex auth directory")?;
        set_private_directory(&directory)?;

        let home = Self { directory };
        atomic_write(&home.auth_path(), credential.as_bytes())
            .context("could not seed supervised Codex auth file")?;
        Ok(home)
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn auth_path(&self) -> PathBuf {
        self.path().join("auth.json")
    }
}

fn copy_respect_system_proxy(source: &toml::Table, destination: &mut toml::Table) {
    let Some(value) = source
        .get("features")
        .and_then(toml::Value::as_table)
        .and_then(|features| features.get("respect_system_proxy"))
    else {
        return;
    };
    destination.insert(
        "features".to_owned(),
        toml::Value::Table(toml::Table::from_iter([(
            "respect_system_proxy".to_owned(),
            value.clone(),
        )])),
    );
}

fn set_private_directory(directory: &TempDir) -> Result<()> {
    #[cfg(unix)]
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::super::auth::tests::auth_json;
    use super::*;

    #[test]
    fn homes_are_distinct_private_and_removed_on_drop() {
        let root = tempdir().unwrap();
        let paths = RuntimePaths::new(
            root.path().join("credentials"),
            root.path().join("real-codex"),
        )
        .unwrap();
        fs::create_dir_all(&paths.credentials_home).unwrap();
        fs::create_dir_all(&paths.codex_home).unwrap();
        fs::write(
            paths.codex_config(),
            concat!(
                "chatgpt_base_url = \"https://example.test/backend-api\"\n",
                "model = \"do-not-copy\"\n",
                "[features]\nrespect_system_proxy = true\n",
            ),
        )
        .unwrap();
        let credential = Credential::from_bytes(auth_json(
            "alice@example.com",
            "alice-id",
            "pro",
            2_000_000_000,
            "alice-refresh",
        ))
        .unwrap();

        let first = IsolatedCodexHome::with_credential(&paths, "quota", &credential).unwrap();
        let second = IsolatedCodexHome::with_credential(&paths, "quota", &credential).unwrap();
        let first_path = first.path().to_owned();
        let second_path = second.path().to_owned();

        assert_ne!(first_path, second_path);
        assert!(first_path.starts_with(&paths.credentials_home));
        assert_eq!(first.credential().unwrap().facts.email, "alice@example.com");
        let config = fs::read_to_string(first.path().join("config.toml")).unwrap();
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
        assert!(config.contains("chatgpt_base_url"));
        assert!(config.contains("respect_system_proxy = true"));
        assert!(!config.contains("do-not-copy"));
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(&first_path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(first.auth_path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        drop(first);
        assert!(!first_path.exists());
        assert!(second_path.exists());
        drop(second);
        assert!(!second_path.exists());
    }

    #[test]
    fn supervised_auth_home_contains_only_private_auth() {
        let root = tempdir().unwrap();
        let paths = RuntimePaths::new(
            root.path().join("credentials"),
            root.path().join("real-codex"),
        )
        .unwrap();
        fs::create_dir_all(&paths.credentials_home).unwrap();
        let credential = Credential::from_bytes(auth_json(
            "alice@example.com",
            "alice-id",
            "pro",
            2_000_000_000,
            "alice-refresh",
        ))
        .unwrap();

        let home = SupervisedAuthHome::with_credential(&paths, &credential).unwrap();
        let home_path = home.path().to_owned();
        assert!(home.path().starts_with(&paths.credentials_home));
        assert_eq!(
            Credential::read(&home.auth_path()).unwrap().facts.email,
            "alice@example.com"
        );
        assert!(!home.path().join("config.toml").exists());
        assert!(!home.path().join("sessions").exists());
        assert_eq!(fs::read_dir(home.path()).unwrap().count(), 1);
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(&home_path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(home.auth_path()).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        drop(home);
        assert!(!home_path.exists());
    }
}
