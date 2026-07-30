use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use predicates::prelude::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

struct MockQuotaServer {
    base_url: String,
    arrivals: Arc<Mutex<Vec<Instant>>>,
    thread: thread::JoinHandle<()>,
}

impl MockQuotaServer {
    fn start(expected_requests: usize, response_delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}/backend-api", listener.local_addr().unwrap());
        let arrivals = Arc::new(Mutex::new(Vec::with_capacity(expected_requests)));
        let server_arrivals = arrivals.clone();
        let thread = thread::spawn(move || {
            let mut handlers = Vec::with_capacity(expected_requests);
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let arrivals = server_arrivals.clone();
                handlers.push(thread::spawn(move || {
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                        let read = stream.read(&mut buffer).unwrap();
                        assert!(read > 0, "client closed before sending HTTP headers");
                        request.extend_from_slice(&buffer[..read]);
                    }
                    arrivals.lock().unwrap().push(Instant::now());
                    let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
                    assert!(request.starts_with("get /backend-api/wham/usage "));
                    assert!(request.contains("\r\nauthorization: bearer "));
                    let used_percent = if request.contains("chatgpt-account-id: alice-id") {
                        25
                    } else if request.contains("chatgpt-account-id: bob-id") {
                        80
                    } else {
                        panic!("request did not contain an expected account ID:\n{request}");
                    };
                    thread::sleep(response_delay);
                    let body = json!({
                        "plan_type": "pro",
                        "rate_limit": {
                            "allowed": true,
                            "limit_reached": false,
                            "primary_window": {
                                "used_percent": used_percent,
                                "limit_window_seconds": 18_000,
                                "reset_after_seconds": 600,
                                "reset_at": 2_000_000_000_i64
                            },
                            "secondary_window": null
                        }
                    })
                    .to_string();
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
        Self {
            base_url,
            arrivals,
            thread,
        }
    }

    fn finish(self) -> Vec<Instant> {
        self.thread.join().unwrap();
        Arc::try_unwrap(self.arrivals)
            .unwrap()
            .into_inner()
            .unwrap()
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

fn profile_id(email: &str) -> String {
    Sha256::digest(email.to_ascii_lowercase().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn seed_accounts(credentials_home: &Path, codex_home: &Path, quota_base_url: &str) {
    let profiles_dir = credentials_home.join("profiles");
    fs::create_dir_all(&profiles_dir).unwrap();
    fs::create_dir_all(codex_home).unwrap();
    let accounts = [
        (
            "alice@example.com",
            "alice-id",
            "alice-refresh",
            profile_id("alice@example.com"),
        ),
        (
            "bob@example.com",
            "bob-id",
            "bob-refresh",
            profile_id("bob@example.com"),
        ),
    ];
    for (email, account_id, refresh_token, id) in &accounts {
        fs::write(
            profiles_dir.join(format!("{id}.json")),
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
                .map(|(email, account_id, _, id)| json!({
                    "id": id,
                    "email": email,
                    "account_id": account_id,
                    "enrolled_at": 0
                }))
                .collect::<Vec<_>>()
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        codex_home.join("auth.json"),
        auth_json("alice@example.com", "alice-id", "alice-live"),
    )
    .unwrap();
    fs::write(
        codex_home.join("config.toml"),
        format!("chatgpt_base_url = {quota_base_url:?}\n"),
    )
    .unwrap();
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
        .stdout(predicate::str::contains("next"))
        .stdout(predicate::str::contains("activate"));
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

#[test]
fn list_fetches_account_quotas_concurrently_and_renders_them_inline() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start(2, Duration::from_millis(600));
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    let output = command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "ls"])
        .output()
        .unwrap();
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
    assert!(stdout.contains("resets 2033-"));

    let mut arrivals = server.finish();
    arrivals.sort_unstable();
    assert_eq!(arrivals.len(), 2);
    assert!(
        arrivals[1].duration_since(arrivals[0]) < Duration::from_millis(400),
        "quota requests were not started concurrently"
    );
}

#[test]
fn next_alias_reports_the_activated_accounts_quota() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let server = MockQuotaServer::start(1, Duration::ZERO);
    seed_accounts(&credentials_home, &codex_home, &server.base_url);

    command(&credentials_home, &codex_home, &runtime_dir)
        .arg("next")
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Codex is now using bob@example.com",
        ))
        .stderr(predicate::str::contains("Quota: 5h quota"))
        .stderr(predicate::str::contains("20% remaining"))
        .stderr(predicate::str::contains("resets 2033-"));
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
    let quota_server = MockQuotaServer::start(5, Duration::ZERO);
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
    let fake_bin = root.path().join("bin");
    fs::create_dir_all(&fake_bin).unwrap();
    let enrolled_path = root.path().join("bob.json");
    fs::write(&enrolled_path, &bob).unwrap();
    let fake_codex = fake_bin.join("codex");
    fs::write(
        &fake_codex,
        "#!/bin/sh\ncp \"$KAI_TEST_CREDENTIAL\" \"$CODEX_HOME/auth.json\"\n",
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(fake_bin.clone()).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    command(&credentials_home, &codex_home, &runtime_dir)
        .env("PATH", path)
        .env("KAI_TEST_CREDENTIAL", &enrolled_path)
        .args(["cred", "add", "bob@example.com"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Enrolled bob@example.com"));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        alice_refreshed
    );

    command(&credentials_home, &codex_home, &runtime_dir)
        .arg("next")
        .assert()
        .success();
    assert_eq!(fs::read(codex_home.join("auth.json")).unwrap(), bob);

    command(&credentials_home, &codex_home, &runtime_dir)
        .args(["cred", "next"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Quota:"));
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        alice_refreshed
    );

    command(&credentials_home, &codex_home, &runtime_dir)
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
        .args(["cred", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"email\": \"alice@example.com\""))
        .stdout(predicate::str::contains("bob@example.com").not());
    assert_eq!(quota_server.finish().len(), 5);
}
