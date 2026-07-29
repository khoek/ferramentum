use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use assert_cmd::Command;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use predicates::prelude::*;
use serde_json::json;
use tempfile::tempdir;

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

#[cfg(unix)]
#[test]
fn complete_rotation_preserves_a_live_refreshed_credential() {
    let root = tempdir().unwrap();
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    fs::create_dir_all(&codex_home).unwrap();

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
        .success();
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
}
