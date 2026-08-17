use std::ffi::OsStr;
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

/// A process-lifetime Codex home with private authentication and shared user state.
///
/// Codex keeps sessions, plugins, skills, and other user state below `CODEX_HOME`, so an empty
/// temporary home would break session resume. This home gives a supervised agent its own
/// `auth.json` and `config.toml` while linking the remaining top-level entries back to the user's
/// real Codex home. SQLite state is shared separately through `CODEX_SQLITE_HOME`.
pub struct SupervisedCodexHome {
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

impl SupervisedCodexHome {
    pub fn with_credential(paths: &RuntimePaths, credential: &Credential) -> Result<Self> {
        let directory = Builder::new()
            .prefix(".agent-")
            .tempdir_in(&paths.credentials_home)
            .context("could not create supervised Codex home")?;
        set_private_directory(&directory)?;

        let home = Self { directory };
        home.link_shared_state(paths)?;
        home.write_config(paths)?;
        atomic_write(&home.auth_path(), credential.as_bytes())
            .context("could not seed supervised Codex credential")?;
        Ok(home)
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn auth_path(&self) -> PathBuf {
        self.path().join("auth.json")
    }

    fn link_shared_state(&self, paths: &RuntimePaths) -> Result<()> {
        for entry in fs::read_dir(&paths.codex_home)
            .with_context(|| format!("could not read {}", paths.codex_home.display()))?
        {
            let entry = entry?;
            let name = entry.file_name();
            if supervised_private_entry(&name) {
                continue;
            }
            link_shared_entry(&entry.path(), &self.path().join(&name), &entry.file_type()?)?;
        }
        Ok(())
    }

    fn write_config(&self, paths: &RuntimePaths) -> Result<()> {
        let mut config = paths.read_codex_config()?.unwrap_or_default();
        config.insert(
            "cli_auth_credentials_store".to_owned(),
            toml::Value::String("file".to_owned()),
        );
        atomic_write(
            &self.path().join("config.toml"),
            toml::to_string_pretty(&config)?.as_bytes(),
        )
        .context("could not configure supervised Codex home")
    }
}

fn supervised_private_entry(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    matches!(name.as_ref(), "auth.json" | "config.toml") || name.contains(".sqlite")
}

#[cfg(unix)]
fn link_shared_entry(source: &Path, destination: &Path, _kind: &fs::FileType) -> Result<()> {
    std::os::unix::fs::symlink(source, destination).with_context(|| {
        format!(
            "could not share Codex state {} through {}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(windows)]
fn link_shared_entry(source: &Path, destination: &Path, kind: &fs::FileType) -> Result<()> {
    let result = if kind.is_dir() {
        std::os::windows::fs::symlink_dir(source, destination)
    } else {
        std::os::windows::fs::symlink_file(source, destination)
    };
    result.with_context(|| {
        format!(
            "could not share Codex state {} through {}",
            source.display(),
            destination.display()
        )
    })
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

    #[cfg(unix)]
    #[test]
    fn supervised_home_isolates_auth_while_sharing_session_state() {
        let root = tempdir().unwrap();
        let paths = RuntimePaths::new(
            root.path().join("credentials"),
            root.path().join("real-codex"),
        )
        .unwrap();
        fs::create_dir_all(&paths.credentials_home).unwrap();
        fs::create_dir_all(paths.codex_home.join("sessions")).unwrap();
        fs::write(
            paths.codex_config(),
            "model = \"gpt-test\"\ncli_auth_credentials_store = \"file\"\n",
        )
        .unwrap();
        fs::write(paths.codex_home.join("sessions/thread.jsonl"), "session").unwrap();
        fs::write(paths.codex_home.join("state_5.sqlite"), "sqlite").unwrap();
        let credential = Credential::from_bytes(auth_json(
            "alice@example.com",
            "alice-id",
            "pro",
            2_000_000_000,
            "alice-refresh",
        ))
        .unwrap();

        let home = SupervisedCodexHome::with_credential(&paths, &credential).unwrap();
        let home_path = home.path().to_owned();
        assert_ne!(home.path(), paths.codex_home);
        assert_eq!(
            Credential::read(&home.auth_path()).unwrap().facts.email,
            "alice@example.com"
        );
        let config = fs::read_to_string(home.path().join("config.toml")).unwrap();
        assert!(config.contains("model = \"gpt-test\""));
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
        assert_eq!(
            fs::read_to_string(home.path().join("sessions/thread.jsonl")).unwrap(),
            "session"
        );
        assert!(!home.path().join("state_5.sqlite").exists());

        drop(home);
        assert!(!home_path.exists());
        assert!(paths.codex_home.join("sessions/thread.jsonl").exists());
    }
}
