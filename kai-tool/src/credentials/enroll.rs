#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use tempfile::{Builder, TempDir};

use super::auth::Credential;
use super::paths::RuntimePaths;
use super::store::atomic_write;

const AUTH_CONFIG_KEYS: &[&str] = &[
    "chatgpt_base_url",
    "forced_chatgpt_workspace_id",
    "forced_login_method",
];

pub fn run(paths: &RuntimePaths, expected_email: &str, device_auth: bool) -> Result<Credential> {
    let codex = which::which("codex").context("could not find `codex` on PATH")?;
    run_with_binary(paths, expected_email, device_auth, &codex)
}

fn run_with_binary(
    paths: &RuntimePaths,
    expected_email: &str,
    device_auth: bool,
    codex: &Path,
) -> Result<Credential> {
    let temporary_home = Builder::new()
        .prefix(".enroll-")
        .tempdir_in(&paths.credentials_home)
        .context("could not create isolated Codex login directory")?;
    set_private_directory(&temporary_home)?;
    write_login_config(paths, &temporary_home)?;

    capulus::ui::stage(&format!(
        "Starting isolated Codex sign-in for {expected_email}"
    ));
    capulus::ui::detail(
        "The account currently active in Codex will not be logged out or replaced.",
    );

    let mut command = Command::new(codex);
    command
        .arg("login")
        .arg("-c")
        .arg("cli_auth_credentials_store=\"file\"")
        .env("CODEX_HOME", temporary_home.path())
        .current_dir(temporary_home.path())
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if device_auth {
        command.arg("--device-auth");
    }

    let status = command
        .status()
        .with_context(|| format!("could not start {}", codex.display()))?;
    if !status.success() {
        match status.code() {
            Some(code) => bail!("Codex login exited with status {code}; no credential was added"),
            None => bail!("Codex login was interrupted; no credential was added"),
        }
    }

    let credential = Credential::read(&temporary_home.path().join("auth.json"))
        .context("Codex login completed but did not produce a file-backed ChatGPT credential")?;
    if !credential.matches_email(expected_email) {
        bail!(
            concat!(
                "signed in as {}, but {} was requested; the new credential was discarded ",
                "and the active account was not changed",
            ),
            credential.facts.email,
            expected_email
        );
    }
    Ok(credential)
}

fn write_login_config(paths: &RuntimePaths, temporary_home: &TempDir) -> Result<()> {
    let mut isolated = toml::Table::new();
    if let Some(source) = paths.read_codex_config()? {
        for key in AUTH_CONFIG_KEYS {
            if let Some(value) = source.get(*key) {
                isolated.insert((*key).to_owned(), value.clone());
            }
        }
    }
    isolated.insert(
        "cli_auth_credentials_store".to_owned(),
        toml::Value::String("file".to_owned()),
    );
    atomic_write(
        &temporary_home.path().join("config.toml"),
        toml::to_string_pretty(&isolated)?.as_bytes(),
    )
}

fn set_private_directory(directory: &TempDir) -> Result<()> {
    #[cfg(unix)]
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;

    use super::super::auth::tests::auth_json;
    use super::*;

    #[cfg(unix)]
    #[test]
    fn login_uses_an_isolated_codex_home() {
        let root = tempdir().unwrap();
        let paths = RuntimePaths::new(
            root.path().join("credentials"),
            root.path().join("real-codex"),
        )
        .unwrap();
        fs::create_dir_all(&paths.credentials_home).unwrap();
        fs::create_dir_all(&paths.codex_home).unwrap();
        let original = auth_json(
            "active@example.com",
            "active-account",
            "pro",
            2_000_000_000,
            "active-refresh",
        );
        fs::write(paths.active_auth(), &original).unwrap();

        let enrolled = auth_json(
            "new@example.com",
            "new-account",
            "plus",
            2_000_000_000,
            "new-refresh",
        );
        let enrolled_path = root.path().join("enrolled.json");
        fs::write(&enrolled_path, enrolled).unwrap();
        let script = root.path().join("fake-codex");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\ncp '{}' \"$CODEX_HOME/auth.json\"\n",
                enrolled_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

        let credential = run_with_binary(&paths, "new@example.com", false, &script).unwrap();

        assert_eq!(credential.facts.email, "new@example.com");
        assert_eq!(fs::read(paths.active_auth()).unwrap(), original);
    }
}
