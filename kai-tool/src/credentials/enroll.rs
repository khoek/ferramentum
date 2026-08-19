use std::env;
#[cfg(unix)]
#[cfg(target_os = "linux")]
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

use super::auth::Credential;
use super::isolated_home::IsolatedCodexHome;
use super::paths::RuntimePaths;
use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthPreference {
    Auto,
    Browser,
    Device,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedAuth {
    Browser,
    Device {
        automatic_reason: Option<&'static str>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LoginEnvironment {
    browser_relay: bool,
    ssh: bool,
    ci: bool,
    linux_without_display: bool,
    wsl_browser_interop: bool,
}

pub fn run(
    paths: &RuntimePaths,
    expected_email: &str,
    preference: AuthPreference,
) -> Result<Credential> {
    let codex = which::which("codex").context("could not find `codex` on PATH")?;
    run_with_binary(paths, expected_email, preference, &codex)
}

fn run_with_binary(
    paths: &RuntimePaths,
    expected_email: &str,
    preference: AuthPreference,
    codex: &Path,
) -> Result<Credential> {
    let auth = resolve_auth(preference, LoginEnvironment::current());
    let temporary_home = IsolatedCodexHome::create(paths, "enroll")?;

    capulus::ui::stage(&format!(
        "Starting isolated Codex sign-in for {expected_email}"
    ));
    if let ResolvedAuth::Device {
        automatic_reason: Some(reason),
    } = auth
    {
        capulus::ui::detail(&format!(
            "Using device-code authentication ({reason}); use `--browser-auth` to override."
        ));
    }
    capulus::ui::detail(
        "The account currently active in Codex will not be logged out or replaced.",
    );

    let mut command = Command::new(codex);
    command
        .arg("login")
        .arg("-c")
        .arg("cli_auth_credentials_store=\"file\"")
        .env("CODEX_HOME", temporary_home.path())
        .env_remove("CODEX_AUTH_FILE")
        .current_dir(temporary_home.path())
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if matches!(auth, ResolvedAuth::Device { .. }) {
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

    let credential = temporary_home
        .credential()
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

impl LoginEnvironment {
    fn current() -> Self {
        Self {
            browser_relay: browser_relay_configured(),
            ssh: ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"]
                .into_iter()
                .any(env_var_nonempty),
            ci: env_var_nonempty("CI"),
            linux_without_display: linux_without_display(),
            wsl_browser_interop: wsl_browser_interop_available(),
        }
    }
}

fn resolve_auth(preference: AuthPreference, environment: LoginEnvironment) -> ResolvedAuth {
    match preference {
        AuthPreference::Browser => ResolvedAuth::Browser,
        AuthPreference::Device => ResolvedAuth::Device {
            automatic_reason: None,
        },
        AuthPreference::Auto if environment.browser_relay => ResolvedAuth::Browser,
        AuthPreference::Auto if environment.ssh => ResolvedAuth::Device {
            automatic_reason: Some("SSH session detected"),
        },
        AuthPreference::Auto if environment.ci => ResolvedAuth::Device {
            automatic_reason: Some("CI environment detected"),
        },
        AuthPreference::Auto
            if environment.linux_without_display && !environment.wsl_browser_interop =>
        {
            ResolvedAuth::Device {
                automatic_reason: Some("no graphical display detected"),
            }
        }
        AuthPreference::Auto => ResolvedAuth::Browser,
    }
}

fn env_var_nonempty(key: &str) -> bool {
    env::var_os(key).is_some_and(|value| !value.to_string_lossy().trim().is_empty())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn browser_relay_configured() -> bool {
    env_var_nonempty("BROWSER")
}

#[cfg(not(all(unix, not(target_os = "macos"))))]
fn browser_relay_configured() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn linux_without_display() -> bool {
    !env_var_nonempty("DISPLAY") && !env_var_nonempty("WAYLAND_DISPLAY")
}

#[cfg(not(target_os = "linux"))]
fn linux_without_display() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn wsl_browser_interop_available() -> bool {
    if env_var_nonempty("WSL_INTEROP") {
        return true;
    }
    fs::read_to_string("/proc/sys/fs/binfmt_misc/WSLInterop")
        .is_ok_and(|contents| contents.contains("enabled"))
}

#[cfg(not(target_os = "linux"))]
fn wsl_browser_interop_available() -> bool {
    false
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

        let credential =
            run_with_binary(&paths, "new@example.com", AuthPreference::Browser, &script).unwrap();

        assert_eq!(credential.facts.email, "new@example.com");
        assert_eq!(fs::read(paths.active_auth()).unwrap(), original);
    }

    #[test]
    fn auth_preferences_override_environment_detection() {
        let headless_ssh = LoginEnvironment {
            ssh: true,
            linux_without_display: true,
            ..LoginEnvironment::default()
        };

        assert_eq!(
            resolve_auth(AuthPreference::Browser, headless_ssh),
            ResolvedAuth::Browser
        );
        assert_eq!(
            resolve_auth(AuthPreference::Device, LoginEnvironment::default()),
            ResolvedAuth::Device {
                automatic_reason: None
            }
        );
    }

    #[test]
    fn auto_auth_prefers_configured_browser_relays() {
        let environment = LoginEnvironment {
            browser_relay: true,
            ssh: true,
            ci: true,
            linux_without_display: true,
            ..LoginEnvironment::default()
        };

        assert_eq!(
            resolve_auth(AuthPreference::Auto, environment),
            ResolvedAuth::Browser
        );
    }

    #[test]
    fn auto_auth_uses_device_code_for_remote_and_headless_environments() {
        for environment in [
            LoginEnvironment {
                ssh: true,
                ..LoginEnvironment::default()
            },
            LoginEnvironment {
                ci: true,
                ..LoginEnvironment::default()
            },
            LoginEnvironment {
                linux_without_display: true,
                ..LoginEnvironment::default()
            },
        ] {
            assert!(matches!(
                resolve_auth(AuthPreference::Auto, environment),
                ResolvedAuth::Device {
                    automatic_reason: Some(_)
                }
            ));
        }
    }

    #[test]
    fn auto_auth_keeps_browser_flow_for_local_wsl_interop() {
        let environment = LoginEnvironment {
            linux_without_display: true,
            wsl_browser_interop: true,
            ..LoginEnvironment::default()
        };

        assert_eq!(
            resolve_auth(AuthPreference::Auto, environment),
            ResolvedAuth::Browser
        );
    }
}
