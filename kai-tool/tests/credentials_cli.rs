use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use assert_cmd::Command;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use predicates::prelude::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::{TempDir, tempdir};

#[cfg(unix)]
struct MockQuotaServer {
    base_url: String,
    _root: TempDir,
    bin_dir: PathBuf,
    arrivals: PathBuf,
    expected_requests: usize,
}

#[cfg(unix)]
impl MockQuotaServer {
    fn start(expected_requests: usize, response_delay: Duration) -> Self {
        Self::start_with_quotas(
            expected_requests,
            response_delay,
            &[("alice-id", 25.0), ("bob-id", 80.0)],
        )
    }

    fn start_with_quotas(
        expected_requests: usize,
        response_delay: Duration,
        quotas: &[(&str, f64)],
    ) -> Self {
        Self::start_with_fixtures(expected_requests, response_delay, quotas, &[], &[], &[])
    }

    fn start_with_countdowns(
        expected_requests: usize,
        quotas: &[(&str, f64)],
        reset_after_seconds: &[(&str, i64)],
    ) -> Self {
        Self::start_with_fixtures(
            expected_requests,
            Duration::ZERO,
            quotas,
            &[],
            &[],
            reset_after_seconds,
        )
    }

    fn start_with_reset_credits(
        expected_requests: usize,
        response_delay: Duration,
        quotas: &[(&str, f64)],
        reset_credits: &[(&str, i64, Vec<serde_json::Value>)],
    ) -> Self {
        Self::start_with_fixtures(
            expected_requests,
            response_delay,
            quotas,
            reset_credits,
            &[],
            &[],
        )
    }

    fn start_with_rejected_accounts(
        expected_requests: usize,
        quotas: &[(&str, f64)],
        rejected_accounts: &[&str],
    ) -> Self {
        Self::start_with_fixtures(
            expected_requests,
            Duration::ZERO,
            quotas,
            &[],
            rejected_accounts,
            &[],
        )
    }

    fn start_with_fixtures(
        expected_requests: usize,
        response_delay: Duration,
        quotas: &[(&str, f64)],
        reset_credits: &[(&str, i64, Vec<serde_json::Value>)],
        rejected_accounts: &[&str],
        reset_after_seconds: &[(&str, i64)],
    ) -> Self {
        let root = tempdir().unwrap();
        let bin_dir = root.path().join("bin");
        let fixtures = root.path().join("fixtures");
        let arrivals = root.path().join("arrivals");
        fs::create_dir_all(&bin_dir).unwrap();
        fs::create_dir_all(&fixtures).unwrap();
        fs::write(
            fixtures.join("delay"),
            format!("{}", response_delay.as_secs_f64()),
        )
        .unwrap();

        let now = chrono::Utc::now().timestamp();
        for (account_id, used_percent) in quotas {
            let reset_after = reset_after_seconds
                .iter()
                .find_map(|(candidate, seconds)| (candidate == account_id).then_some(*seconds));
            let reset_after = reset_after.unwrap_or(600);
            let window_minutes = if reset_after > 7 * 24 * 60 * 60 - 60 {
                7 * 24 * 60
            } else {
                5 * 60
            };
            let resets_at = if reset_after_seconds
                .iter()
                .any(|(candidate, _)| candidate == account_id)
            {
                now + reset_after
            } else {
                2_000_000_000_i64
            };
            let reset_credit_fixture = reset_credits
                .iter()
                .find(|(candidate, _, _)| candidate == account_id);
            let rpc = if rejected_accounts.contains(account_id) {
                json!({
                    "id": 1,
                    "error": {
                        "code": -32603,
                        "message": "failed to fetch codex rate limits: HTTP 401 Unauthorized"
                    }
                })
            } else {
                let reset_credit_summary =
                    reset_credit_fixture.map(|(_, available_count, credits)| {
                        json!({
                            "availableCount": available_count,
                            "credits": credits
                        })
                    });
                json!({
                    "id": 1,
                    "result": {
                        "rateLimits": {
                            "primary": {
                                "usedPercent": used_percent,
                                "windowDurationMins": window_minutes,
                                "resetsAt": resets_at
                            },
                            "secondary": null
                        },
                        "rateLimitsByLimitId": null,
                        "rateLimitResetCredits": reset_credit_summary
                    }
                })
            };
            fs::write(
                fixtures.join(format!("{account_id}.rpc")),
                format!("{rpc}\n"),
            )
            .unwrap();
        }

        let fake_codex = bin_dir.join("codex");
        fs::write(
            &fake_codex,
            format!(
                concat!(
                    "#!/bin/sh\n",
                    "set -eu\n",
                    "case \"${{1-}}\" in\n",
                    "app-server)\n",
                    "  IFS= read -r initialize\n",
                    "  printf '%s\\n' '{{\"id\":0,\"result\":{{}}}}'\n",
                    "  IFS= read -r initialized\n",
                    "  IFS= read -r quota_request\n",
                    "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$CODEX_HOME/auth.json\" | head -n 1)\n",
                    "  printf '%s\\t%s\\n' \"$account_id\" \"$CODEX_HOME\" >> '{}'\n",
                    "  delay=$(cat '{}')\n",
                    "  if [ \"$delay\" != 0 ]; then sleep \"$delay\"; fi\n",
                    "  if [ \"${{KAI_TEST_CONCURRENT_ACCOUNT-}}\" = \"$account_id\" ]; then cp \"$KAI_TEST_CONCURRENT_AUTH\" \"$KAI_TEST_LIVE_AUTH\"; fi\n",
                    "  if [ -f '{}/'$account_id'.auth.json' ]; then cp '{}/'$account_id'.auth.json' \"$CODEX_HOME/auth.json\"; fi\n",
                    "  if [ -f '{}/'$account_id'.first.rpc' ] && [ ! -f '{}/'$account_id'.attempted' ]; then\n",
                    "    : > '{}/'$account_id'.attempted'\n",
                    "    cat '{}/'$account_id'.first.rpc'\n",
                    "  else\n",
                    "    cat '{}/'$account_id'.rpc'\n",
                    "  fi\n",
                    "  ;;\n",
                    "login)\n",
                    "  printf '%s\\n' \"$@\" > \"$KAI_TEST_ARGS\"\n",
                    "  cp \"$KAI_TEST_CREDENTIAL\" \"$CODEX_HOME/auth.json\"\n",
                    "  ;;\n",
                    "exec)\n",
                    "  if [ \"${{KAI_TEST_TICKLE_FAIL-}}\" = 1 ]; then printf 'probe failed\\n' >&2; exit 7; fi\n",
                    "  printf 'cwd:%s\\n' \"$PWD\" >> \"$KAI_TEST_TICKLE_LOG\"\n",
                    "  for arg in \"$@\"; do printf 'arg:%s\\n' \"$arg\" >> \"$KAI_TEST_TICKLE_LOG\"; done\n",
                    "  grep '\"refresh_token\"' \"$CODEX_HOME/auth.json\" >> \"$KAI_TEST_TICKLE_LOG\"\n",
                    "  printf 'end\\n' >> \"$KAI_TEST_TICKLE_LOG\"\n",
                    "  printf 'discarded codex response\\n'\n",
                    "  ;;\n",
                    "*) exit 64 ;;\n",
                    "esac\n",
                ),
                arrivals.display(),
                fixtures.join("delay").display(),
                fixtures.display(),
                fixtures.display(),
                fixtures.display(),
                fixtures.display(),
                fixtures.display(),
                fixtures.display(),
                fixtures.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();

        Self {
            base_url: "https://example.test/backend-api".to_owned(),
            _root: root,
            bin_dir,
            arrivals,
            expected_requests,
        }
    }

    fn path(&self) -> std::ffi::OsString {
        env::join_paths(
            std::iter::once(self.bin_dir.clone())
                .chain(env::split_paths(&env::var_os("PATH").unwrap())),
        )
        .unwrap()
    }

    fn rotate_to(&self, account_id: &str, credential: &[u8]) {
        fs::write(
            self._root
                .path()
                .join("fixtures")
                .join(format!("{account_id}.auth.json")),
            credential,
        )
        .unwrap();
    }

    fn fail_once(&self, account_id: &str) {
        fs::write(
            self._root
                .path()
                .join("fixtures")
                .join(format!("{account_id}.first.rpc")),
            concat!(
                "{\"id\":1,\"error\":{\"code\":-32603,",
                "\"message\":\"temporary backend failure\"}}\n",
            ),
        )
        .unwrap();
    }

    fn finish(self) -> Vec<(String, PathBuf)> {
        let arrivals = fs::read_to_string(&self.arrivals).unwrap_or_default();
        let arrivals = arrivals
            .lines()
            .map(|line| {
                let (account, home) = line.split_once('\t').unwrap();
                (account.to_owned(), PathBuf::from(home))
            })
            .collect::<Vec<_>>();
        assert_eq!(arrivals.len(), self.expected_requests);
        arrivals
    }
}

fn command(credentials_home: &Path, codex_home: &Path, runtime_dir: &Path) -> Command {
    let mut command = Command::cargo_bin("kai").unwrap();
    command
        .env("KAI_CREDENTIALS_HOME", credentials_home)
        .env("CODEX_HOME", codex_home)
        .env("XDG_RUNTIME_DIR", runtime_dir);
    command
}

fn auth_json(email: &str, account_id: &str, refresh_token: &str) -> Vec<u8> {
    let jwt = |claims: serde_json::Value| {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        format!("{header}.{payload}.signature")
    };
    serde_json::to_vec_pretty(&json!({
        "auth_mode": "chatgpt",
        "tokens": {
            "id_token": jwt(json!({
                "email": email,
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": account_id,
                    "chatgpt_plan_type": "pro"
                }
            })),
            "access_token": jwt(json!({"exp": 2_000_000_000_i64})),
            "account_id": account_id,
            "refresh_token": refresh_token
        },
        "last_refresh": "2026-07-29T00:00:00Z"
    }))
    .unwrap()
}

fn reset_credit(status: &str, expires_at: Option<&str>) -> serde_json::Value {
    let id = format!("{status}-{}", expires_at.unwrap_or("never"));
    let expires_at = expires_at.map(|timestamp| {
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .unwrap()
            .timestamp()
    });
    json!({
        "id": id,
        "resetType": "codex_rate_limits",
        "status": status,
        "grantedAt": 1_751_322_400_i64,
        "expiresAt": expires_at
    })
}

fn profile_id(email: &str) -> String {
    Sha256::digest(email.to_ascii_lowercase().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn seed_accounts(credentials_home: &Path, codex_home: &Path, quota_base_url: &str) {
    seed_account_set(
        credentials_home,
        codex_home,
        quota_base_url,
        &[
            ("alice@example.com", "alice-id", "alice-refresh"),
            ("bob@example.com", "bob-id", "bob-refresh"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
}

fn seed_account_set(
    credentials_home: &Path,
    codex_home: &Path,
    quota_base_url: &str,
    accounts: &[(&str, &str, &str)],
    active: Option<(&str, &str, &str)>,
) {
    let profiles_dir = credentials_home.join("profiles");
    fs::create_dir_all(&profiles_dir).unwrap();
    fs::create_dir_all(codex_home).unwrap();
    for (email, account_id, refresh_token) in accounts {
        fs::write(
            profiles_dir.join(format!("{}.json", profile_id(email))),
            auth_json(email, account_id, refresh_token),
        )
        .unwrap();
    }
    fs::write(
        credentials_home.join("state.json"),
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "codex_home": codex_home,
            "profiles": accounts
                .iter()
                .map(|(email, account_id, _)| json!({
                    "id": profile_id(email),
                    "email": email,
                    "account_id": account_id,
                    "enrolled_at": 0
                }))
                .collect::<Vec<_>>()
        }))
        .unwrap(),
    )
    .unwrap();
    if let Some((email, account_id, refresh_token)) = active {
        fs::write(
            codex_home.join("auth.json"),
            auth_json(email, account_id, refresh_token),
        )
        .unwrap();
    }
    fs::write(
        codex_home.join("config.toml"),
        format!("chatgpt_base_url = {quota_base_url:?}\n"),
    )
    .unwrap();
}

#[cfg(unix)]
fn fake_codex_path(root: &Path, credential: &Path) -> std::ffi::OsString {
    let fake_bin = root.join("bin");
    fs::create_dir_all(&fake_bin).unwrap();
    let fake_codex = fake_bin.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "printf '%s\\n' \"$@\" > \"$KAI_TEST_ARGS\"\n",
            "cp \"$KAI_TEST_CREDENTIAL\" \"$CODEX_HOME/auth.json\"\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(fake_bin).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();
    fs::write(
        credential,
        auth_json("bob@example.com", "bob-id", "bob-refresh"),
    )
    .unwrap();
    path
}

#[test]
fn help_orders_commands_logically_and_exposes_the_account_workflow() {
    let output = Command::cargo_bin("kai")
        .unwrap()
        .arg("help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    let commands = help
        .split_once("Commands:")
        .map(|(_, commands)| commands)
        .unwrap();
    let mut previous = 0;
    for command in [
        "agent", "worktree", "cred", "next", "llm-get", "init", "bump",
    ] {
        let position = commands
            .find(&format!("\n  {command}"))
            .unwrap_or_else(|| panic!("{command} missing from help:\n{help}"));
        assert!(
            position >= previous,
            "{command} is out of order in help:\n{help}"
        );
        previous = position;
    }

    Command::cargo_bin("kai")
        .unwrap()
        .args(["cred", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("add"))
        .stdout(predicate::str::contains("fix"))
        .stdout(predicate::str::contains("tickle"))
        .stdout(predicate::str::contains("next"))
        .stdout(predicate::str::contains("activate"));

    Command::cargo_bin("kai")
        .unwrap()
        .args([
            "cred",
            "add",
            "person@example.com",
            "--device-auth",
            "--browser-auth",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "cannot be used with '--browser-auth'",
        ));
}

#[cfg(unix)]
#[test]
fn add_automatically_uses_device_auth_over_ssh() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let enrolled_path = root.path().join("bob.json");
    let args_path = root.path().join("codex-args");
    let path = fake_codex_path(root.path(), &enrolled_path);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .env("SSH_CONNECTION", "192.0.2.1 1234 192.0.2.2 22")
        .env_remove("BROWSER")
        .env_remove("CI")
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .args(["cred", "add", "bob@example.com"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Using device-code authentication (SSH session detected)",
        ));

    let args = fs::read_to_string(args_path).unwrap();
    assert!(args.lines().any(|arg| arg == "--device-auth"));
}

#[cfg(unix)]
#[test]
fn browser_auth_explicitly_overrides_remote_environment_detection() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let enrolled_path = root.path().join("bob.json");
    let args_path = root.path().join("codex-args");
    let path = fake_codex_path(root.path(), &enrolled_path);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .env("SSH_CONNECTION", "192.0.2.1 1234 192.0.2.2 22")
        .env_remove("BROWSER")
        .env_remove("DISPLAY")
        .env_remove("WAYLAND_DISPLAY")
        .args(["cred", "add", "bob@example.com", "--browser-auth"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Using device-code authentication").not());

    let args = fs::read_to_string(args_path).unwrap();
    assert!(!args.lines().any(|arg| arg == "--device-auth"));
}

#[cfg(unix)]
#[test]
fn add_force_reauthenticates_an_existing_account_without_changing_its_active_state() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    seed_account_set(
        &credentials_home,
        &codex_home,
        "https://example.test/backend-api",
        &[
            ("alice@example.com", "alice-id", "alice-old"),
            ("bob@example.com", "bob-id", "bob-old"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let enrolled_path = root.path().join("reauth.json");
    let args_path = root.path().join("codex-args");
    let path = fake_codex_path(root.path(), &enrolled_path);

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "add", "bob@example.com"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--force"));

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", &path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args([
            "cred",
            "add",
            "bob@example.com",
            "--force",
            "--browser-auth",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Updated credentials for bob@example.com",
        ));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("alice@example.com", "alice-id", "alice-live")
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("bob@example.com")))
        )
        .unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );

    fs::write(
        &enrolled_path,
        auth_json("alice@example.com", "alice-id", "alice-refreshed"),
    )
    .unwrap();
    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args([
            "cred",
            "add",
            "alice@example.com",
            "--force",
            "--browser-auth",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "refreshed the active Codex credential",
        ));
    let refreshed = auth_json("alice@example.com", "alice-id", "alice-refreshed");
    assert_eq!(fs::read(codex_home.join("auth.json")).unwrap(), refreshed);
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com")))
        )
        .unwrap(),
        refreshed
    );
}

#[cfg(unix)]
#[test]
fn add_force_rejects_a_different_account_workspace_identity() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    seed_account_set(
        &credentials_home,
        &codex_home,
        "https://example.test/backend-api",
        &[("alice@example.com", "alice-id", "alice-old")],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let enrolled_path = root.path().join("reauth.json");
    let args_path = root.path().join("codex-args");
    let path = fake_codex_path(root.path(), &enrolled_path);
    fs::write(
        &enrolled_path,
        auth_json("alice@example.com", "different-id", "new-refresh"),
    )
    .unwrap();
    let original_active = fs::read(codex_home.join("auth.json")).unwrap();
    let profile_path = credentials_home
        .join("profiles")
        .join(format!("{}.json", profile_id("alice@example.com")));
    let original_profile = fs::read(&profile_path).unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args([
            "cred",
            "add",
            "alice@example.com",
            "--force",
            "--browser-auth",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "account/workspace ID does not match",
        ));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        original_active
    );
    assert_eq!(fs::read(profile_path).unwrap(), original_profile);
}

#[cfg(unix)]
#[test]
fn fix_detects_and_reauthenticates_credentials_rejected_by_the_service() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_rejected_accounts(
        4,
        &[("alice-id", 25.0), ("bob-id", 80.0)],
        &["bob-id"],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-old"),
            ("bob@example.com", "bob-id", "bob-old"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let enrolled_path = root.path().join("reauth.json");
    let args_path = root.path().join("codex-args");
    fs::write(
        &enrolled_path,
        auth_json("bob@example.com", "bob-id", "bob-refresh"),
    )
    .unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "quota unavailable: Codex app-server could not read quota",
        ))
        .stdout(predicate::str::contains("run `kai cred fix`"))
        .stdout(predicate::str::contains("next: bob@example.com").not());

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["cred", "fix", "--browser-auth"])
        .write_stdin("\n")
        .assert()
        .success()
        .stderr(predicate::str::contains("Repairing 1 broken credential"))
        .stderr(predicate::str::contains(
            "Press Enter to open sign-in for bob@example.com",
        ))
        .stderr(predicate::str::contains(
            "Updated credentials for bob@example.com",
        ));
    let arrivals = server.finish();
    assert_eq!(arrivals.len(), 4);
    assert!(arrivals.iter().all(|(_, home)| !home.exists()));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("alice@example.com", "alice-id", "alice-live")
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("bob@example.com")))
        )
        .unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );
}

#[cfg(unix)]
#[test]
fn fix_reauthenticates_a_structurally_invalid_stored_credential() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(
        2,
        Duration::ZERO,
        &[("alice-id", 25.0), ("bob-id", 80.0)],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-old"),
            ("bob@example.com", "bob-id", "bob-old"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let bob_profile = credentials_home
        .join("profiles")
        .join(format!("{}.json", profile_id("bob@example.com")));
    fs::write(&bob_profile, b"not JSON").unwrap();
    let enrolled_path = root.path().join("fixed.json");
    let args_path = root.path().join("codex-args");
    fs::write(
        &enrolled_path,
        auth_json("bob@example.com", "bob-id", "bob-refresh"),
    )
    .unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["cred", "fix", "--browser-auth"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Press Enter to open sign-in for bob@example.com",
        ))
        .stderr(predicate::str::contains(
            "confirmation ended before sign-in started",
        ));
    assert!(
        !args_path.exists(),
        "Codex login started before confirmation"
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["cred", "fix", "--browser-auth"])
        .write_stdin("\n")
        .assert()
        .success()
        .stderr(predicate::str::contains("Repairing 1 broken credential"))
        .stderr(predicate::str::contains(
            "Press Enter to open sign-in for bob@example.com",
        ));
    assert_eq!(server.finish().len(), 2);
    assert_eq!(
        fs::read(bob_profile).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );
}

#[test]
fn empty_list_has_human_and_json_output() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No Codex accounts enrolled"));

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"accounts\": []"))
        .stdout(predicate::str::contains("refresh_token").not());
}

#[cfg(unix)]
#[test]
fn list_fetches_account_quotas_concurrently_and_renders_them_inline() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start(2, Duration::from_millis(600));
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    let started = Instant::now();
    let output = command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "ls"])
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "kai cred ls failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let alice = stdout
        .lines()
        .find(|line| line.contains("alice@example.com"))
        .unwrap();
    let bob = stdout
        .lines()
        .find(|line| line.contains("bob@example.com"))
        .unwrap();
    assert!(alice.contains("75% remaining"));
    assert!(bob.contains("20% remaining"));
    assert!(stdout.contains("[████"));
    assert!(stdout.contains("resets in "));
    assert!(stdout.ends_with(concat!(
        "\n\n2 accounts enrolled · next: bob@example.com\n\n",
        "total: [███████▓░░░░░░░░]  48% remaining · ",
        "usage: [░░░▓████│░░░░░░░░] -0.53\n",
    )));
    assert!(!stdout.contains("access "));

    let arrivals = server.finish();
    assert_eq!(arrivals.len(), 2);
    assert!(
        elapsed < Duration::from_millis(1_050),
        "quota requests were not started concurrently ({elapsed:?})"
    );
    assert_ne!(arrivals[0].1, arrivals[1].1);
    assert!(arrivals.iter().all(|(_, home)| !home.exists()));
}

#[cfg(unix)]
#[test]
fn transient_app_server_failures_retry_in_a_fresh_isolated_home() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(2, Duration::ZERO, &[("alice-id", 25.0)]);
    server.fail_once("alice-id");
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[("alice@example.com", "alice-id", "alice-refresh")],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"remaining_percent\": 75.0"));

    let arrivals = server.finish();
    assert_eq!(arrivals.len(), 2);
    assert_ne!(arrivals[0].1, arrivals[1].1);
    assert!(arrivals.iter().all(|(_, home)| !home.exists()));
}

#[cfg(unix)]
#[test]
fn list_persists_codex_token_rotations_from_parallel_isolated_homes() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start(2, Duration::ZERO);
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-stored"),
            ("bob@example.com", "bob-id", "bob-stored"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let alice_rotated = auth_json("alice@example.com", "alice-id", "alice-rotated");
    let bob_rotated = auth_json("bob@example.com", "bob-id", "bob-rotated");
    server.rotate_to("alice-id", &alice_rotated);
    server.rotate_to("bob-id", &bob_rotated);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "list", "--json"])
        .assert()
        .success();

    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        alice_rotated
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com")))
        )
        .unwrap(),
        alice_rotated
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("bob@example.com")))
        )
        .unwrap(),
        bob_rotated
    );
    let arrivals = server.finish();
    assert_ne!(arrivals[0].1, arrivals[1].1);
    assert!(arrivals.iter().all(|(_, home)| !home.exists()));
}

#[cfg(unix)]
#[test]
fn a_concurrent_live_rotation_wins_over_an_isolated_rotation() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(1, Duration::ZERO, &[("alice-id", 25.0)]);
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[("alice@example.com", "alice-id", "alice-source")],
        Some(("alice@example.com", "alice-id", "alice-source")),
    );
    let isolated_rotation = auth_json("alice@example.com", "alice-id", "alice-isolated");
    let concurrent_rotation = auth_json("alice@example.com", "alice-id", "alice-concurrent");
    let concurrent_path = root.path().join("concurrent-auth.json");
    fs::write(&concurrent_path, &concurrent_rotation).unwrap();
    server.rotate_to("alice-id", &isolated_rotation);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .env("KAI_TEST_CONCURRENT_ACCOUNT", "alice-id")
        .env("KAI_TEST_CONCURRENT_AUTH", &concurrent_path)
        .env("KAI_TEST_LIVE_AUTH", codex_home.join("auth.json"))
        .args(["cred", "list", "--json"])
        .assert()
        .success();

    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        concurrent_rotation
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com")))
        )
        .unwrap(),
        concurrent_rotation
    );
    assert_ne!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        isolated_rotation
    );
    assert!(server.finish().iter().all(|(_, home)| !home.exists()));
}

#[cfg(unix)]
#[test]
fn list_reports_usable_reset_credits_and_the_latest_relative_expiry() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_reset_credits(
        4,
        Duration::ZERO,
        &[("alice-id", 100.0), ("bob-id", 100.0)],
        &[(
            "bob-id",
            2,
            vec![
                reset_credit("available", Some("2033-05-18T03:33:20Z")),
                reset_credit("available", Some("2036-07-18T13:20:00Z")),
                reset_credit("available", Some("2020-01-01T00:00:00Z")),
                reset_credit("redeemed", Some("2040-01-01T00:00:00Z")),
            ],
        )],
    );
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    let output = command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "list"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let bob = stdout
        .lines()
        .find(|line| line.contains("bob@example.com"))
        .unwrap();
    assert!(bob.contains("  0% remaining"));
    assert!(bob.contains("2 reset credits"));
    assert!(bob.contains("latest expires in "));
    assert!(stdout.contains("next: bob@example.com"));

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"rate_limit_reset_credits\""))
        .stdout(predicate::str::contains("\"available_count\": 2"))
        .stdout(predicate::str::contains("\"latest_expires_at\""));
    assert_eq!(server.finish().len(), 4);
}

#[cfg(unix)]
#[test]
fn tickle_probes_only_exact_seven_day_countdowns_and_restores_the_active_credential() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let server = MockQuotaServer::start_with_countdowns(
        3,
        &[("alice-id", 0.0), ("bob-id", 0.0), ("carol-id", 0.0)],
        &[
            ("alice-id", 604_800),
            ("bob-id", 604_700),
            ("carol-id", 604_800),
        ],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-refresh"),
            ("bob@example.com", "bob-id", "bob-refresh"),
            ("carol@example.com", "carol-id", "carol-refresh"),
        ],
        Some(("bob@example.com", "bob-id", "bob-live")),
    );

    let log_path = root.path().join("tickle.log");

    let output = command(&credentials_home, &codex_home, &runtime_dir)
        .env("HOME", &home)
        .env("PATH", server.path())
        .env("KAI_TEST_TICKLE_LOG", &log_path)
        .args(["cred", "tickle"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "kai cred tickle failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("discarded codex response"));

    let log = fs::read_to_string(log_path).unwrap();
    assert_eq!(log.matches(&format!("cwd:{}\n", home.display())).count(), 2);
    assert_eq!(log.matches("arg:exec\n").count(), 2);
    assert_eq!(log.matches("arg:--skip-git-repo-check\n").count(), 2);
    assert_eq!(log.matches("arg:--ephemeral\n").count(), 2);
    assert_eq!(
        log.matches(
            "arg:What is the current system `gcc` version? (Reply with only the version number.)\n"
        )
        .count(),
        2
    );
    let alice = log.find("alice-refresh").unwrap();
    let carol = log.find("carol-refresh").unwrap();
    assert!(
        alice < carol,
        "credentials were not tickled in enrollment order:\n{log}"
    );
    assert!(!log.contains("bob-live"));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-live")
    );
    assert_eq!(server.finish().len(), 3);
}

#[cfg(unix)]
#[test]
fn tickle_restores_the_active_credential_after_a_codex_failure() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let server = MockQuotaServer::start_with_countdowns(
        2,
        &[("alice-id", 0.0), ("bob-id", 0.0)],
        &[("alice-id", 604_800), ("bob-id", 600)],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-refresh"),
            ("bob@example.com", "bob-id", "bob-refresh"),
        ],
        Some(("bob@example.com", "bob-id", "bob-live")),
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("HOME", &home)
        .env("PATH", server.path())
        .env("KAI_TEST_TICKLE_FAIL", "1")
        .args(["cred", "tickle"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Codex request failed for alice@example.com",
        ));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-live")
    );
    assert_eq!(server.finish().len(), 2);
}

#[cfg(unix)]
#[test]
fn next_alias_reports_the_activated_accounts_quota() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start(1, Duration::ZERO);
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .arg("next")
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Codex is now using bob@example.com",
        ))
        .stderr(predicate::str::contains("Quota: 5h quota"))
        .stderr(predicate::str::contains("20% remaining"))
        .stderr(predicate::str::contains("resets in "));
    assert_eq!(server.finish().len(), 1);
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );
}

#[cfg(unix)]
#[test]
fn next_skips_exhausted_accounts_in_cyclic_order() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(
        2,
        Duration::from_millis(300),
        &[("bob-id", 100.0), ("carol-id", 40.0)],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-refresh"),
            ("bob@example.com", "bob-id", "bob-refresh"),
            ("carol@example.com", "carol-id", "carol-refresh"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );

    let started = Instant::now();
    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .arg("next")
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Codex is now using carol@example.com",
        ))
        .stderr(predicate::str::contains("60% remaining"));
    let elapsed = started.elapsed();
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("carol@example.com", "carol-id", "carol-refresh")
    );

    let arrivals = server.finish();
    assert_eq!(arrivals.len(), 2);
    assert!(
        elapsed < Duration::from_millis(550),
        "candidate quota requests were not started concurrently ({elapsed:?})"
    );
}

#[cfg(unix)]
#[test]
fn next_selects_an_exhausted_account_with_a_usable_reset_credit_and_prints_a_notice() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_reset_credits(
        1,
        Duration::ZERO,
        &[("alice-id", 100.0), ("bob-id", 100.0)],
        &[(
            "bob-id",
            1,
            vec![reset_credit("available", Some("2033-05-18T03:33:20Z"))],
        )],
    );
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .args(["cred", "next"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Codex is now using bob@example.com",
        ))
        .stderr(predicate::str::contains("1 reset credit"))
        .stderr(predicate::str::contains("notice: bob@example.com"))
        .stderr(predicate::str::contains("latest expires in "))
        .stderr(predicate::str::contains("`/usage`"));
    assert_eq!(server.finish().len(), 1);
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );
}

#[cfg(unix)]
#[test]
fn next_does_not_switch_when_every_candidate_is_exhausted() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(
        2,
        Duration::ZERO,
        &[("bob-id", 100.0), ("carol-id", 100.0)],
    );
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[
            ("alice@example.com", "alice-id", "alice-refresh"),
            ("bob@example.com", "bob-id", "bob-refresh"),
            ("carol@example.com", "carol-id", "carol-refresh"),
        ],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .arg("next")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "no other enrolled account has remaining Codex quota",
        ));
    assert_eq!(server.finish().len(), 2);
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("alice@example.com", "alice-id", "alice-live")
    );
}

#[cfg(unix)]
#[test]
fn add_activates_the_new_account_when_the_current_quota_is_exhausted() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start_with_quotas(1, Duration::ZERO, &[("alice-id", 100.0)]);
    seed_account_set(
        &credentials_home,
        &codex_home,
        &server.base_url,
        &[("alice@example.com", "alice-id", "alice-refresh")],
        Some(("alice@example.com", "alice-id", "alice-live")),
    );
    let enrolled_path = root.path().join("bob.json");
    let args_path = root.path().join("codex-args");
    fs::write(
        &enrolled_path,
        auth_json("bob@example.com", "bob-id", "bob-refresh"),
    )
    .unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", server.path())
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["cred", "add", "bob@example.com", "--browser-auth"])
        .assert()
        .success()
        .stderr(predicate::str::contains(concat!(
            "Enrolled and activated bob@example.com because ",
            "alice@example.com has no remaining quota",
        )));
    assert_eq!(server.finish().len(), 1);
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-refresh")
    );
}

#[cfg(unix)]
#[test]
fn complete_rotation_preserves_a_live_refreshed_credential() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    fs::create_dir_all(&codex_home).unwrap();
    let quota_server = MockQuotaServer::start(6, Duration::ZERO);
    fs::write(
        codex_home.join("config.toml"),
        format!("chatgpt_base_url = {:?}\n", quota_server.base_url),
    )
    .unwrap();

    let alice_original = auth_json("alice@example.com", "alice-id", "alice-original");
    let alice_refreshed = auth_json("alice@example.com", "alice-id", "alice-refreshed");
    let bob = auth_json("bob@example.com", "bob-id", "bob-refresh");
    fs::write(codex_home.join("auth.json"), &alice_original).unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "add", "alice@example.com"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Imported the active Codex account",
        ));

    fs::write(codex_home.join("auth.json"), &alice_refreshed).unwrap();
    let enrolled_path = root.path().join("bob.json");
    let args_path = root.path().join("codex-args");
    fs::write(&enrolled_path, &bob).unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", quota_server.path())
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["cred", "add", "bob@example.com"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Enrolled bob@example.com"));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        alice_refreshed
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", quota_server.path())
        .arg("next")
        .assert()
        .success();
    assert_eq!(fs::read(codex_home.join("auth.json")).unwrap(), bob);

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", quota_server.path())
        .args(["cred", "next"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Quota:"));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        alice_refreshed
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", quota_server.path())
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"active\": \"alice@example.com\"",
        ))
        .stdout(predicate::str::contains("\"remaining_percent\": 75.0"))
        .stdout(predicate::str::contains("\"remaining_percent\": 20.0"))
        .stdout(predicate::str::contains("\"resets_at\": 2000000000"))
        .stdout(predicate::str::contains("alice-refreshed").not())
        .stdout(predicate::str::contains("bob-refresh").not());

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "remove", "bob@example.com", "--yes"])
        .assert()
        .success();
    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", quota_server.path())
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"email\": \"alice@example.com\""))
        .stdout(predicate::str::contains("bob@example.com").not());
    assert_eq!(quota_server.finish().len(), 6);
}
