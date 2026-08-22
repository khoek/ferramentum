#![cfg(unix)]

use std::env;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use rusqlite::{Connection, params};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

#[test]
fn supervised_fast_agent_preserves_invocation_directory() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let launch_dir = root.path().join("workspace");
    let cwd_path = root.path().join("cwd");
    let args_path = root.path().join("args");
    let observed_codex_home_path = root.path().join("observed-codex-home");
    let auth_file_path = root.path().join("auth-file");
    let sqlite_home_path = root.path().join("sqlite-home");
    let config_path = root.path().join("agent-config");
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&launch_dir).unwrap();
    seed_account(&credentials_home, &codex_home);

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "if [ \"${1-}\" = --version ]; then\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  exit 0\n",
            "fi\n",
            "test \"${1-}\" = --auth-file\n",
            "auth_file=$2\n",
            "shift 2\n",
            "test -z \"${CODEX_AUTH_FILE-}\"\n",
            "printf '%s\\n' \"$PWD\" > \"$KAI_TEST_CWD\"\n",
            "printf '%s\\n' \"$@\" > \"$KAI_TEST_ARGS\"\n",
            "printf '%s\\n' \"$CODEX_HOME\" > \"$KAI_TEST_AGENT_HOME\"\n",
            "printf '%s\\n' \"$CODEX_SQLITE_HOME\" > \"$KAI_TEST_SQLITE_HOME\"\n",
            "printf '%s\\n' \"$auth_file\" > \"$KAI_TEST_AUTH_FILE\"\n",
            "test \"$CODEX_HOME\" = \"$KAI_EXPECTED_CODEX_HOME\"\n",
            "test -f \"$auth_file\"\n",
            "grep '^model' \"$CODEX_HOME/config.toml\" > \"$KAI_TEST_CONFIG\"\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    Command::cargo_bin("kai")
        .unwrap()
        .current_dir(&launch_dir)
        .env("PATH", path)
        .env("KAI_CREDENTIALS_HOME", &credentials_home)
        .env("CODEX_HOME", &codex_home)
        .env("CODEX_SQLITE_HOME", &codex_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("KAI_TEST_CWD", &cwd_path)
        .env("KAI_TEST_ARGS", &args_path)
        .env("KAI_TEST_AGENT_HOME", &observed_codex_home_path)
        .env("KAI_TEST_SQLITE_HOME", &sqlite_home_path)
        .env("KAI_TEST_AUTH_FILE", &auth_file_path)
        .env("KAI_EXPECTED_CODEX_HOME", &codex_home)
        .env("KAI_TEST_CONFIG", &config_path)
        .args(["--quota-auto-restart", "yes", "a", "--fast"])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(cwd_path).unwrap(),
        format!("{}\n", launch_dir.display())
    );
    assert_eq!(
        fs::read_to_string(args_path).unwrap(),
        concat!(
            "--dangerously-bypass-approvals-and-sandbox\n",
            "-c\n",
            "service_tier=fast\n",
            "--exit-on-quota-exceeded\n",
        )
    );
    assert_eq!(
        fs::read_to_string(observed_codex_home_path).unwrap(),
        format!("{}\n", codex_home.display())
    );
    let auth_file = fs::read_to_string(auth_file_path).unwrap();
    let auth_file = Path::new(auth_file.trim());
    assert_eq!(
        auth_file,
        credentials_home
            .join("profiles")
            .join(format!("{}.json", profile_id("alice@example.com")))
    );
    assert!(auth_file.exists());
    assert_eq!(
        fs::read_to_string(sqlite_home_path).unwrap(),
        format!("{}\n", codex_home.display())
    );
    assert_eq!(
        fs::read_to_string(config_path).unwrap(),
        "model = \"gpt-test\"\n"
    );
}

#[test]
fn supervised_agent_repairs_stale_rollout_paths() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let launch_dir = root.path().join("workspace");
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&launch_dir).unwrap();
    seed_account(&credentials_home, &codex_home);
    fs::create_dir_all(codex_home.join("sessions")).unwrap();
    fs::write(codex_home.join("sessions/thread.jsonl"), "session\n").unwrap();

    let connection = Connection::open(codex_home.join("state_5.sqlite")).unwrap();
    connection
        .execute_batch("CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL);")
        .unwrap();
    let stale_path = credentials_home.join(".agent-gone/sessions/thread.jsonl");
    connection
        .execute(
            "INSERT INTO threads VALUES (?1, ?2)",
            params!["thread-id", stale_path.to_str().unwrap()],
        )
        .unwrap();
    connection.close().unwrap();

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "if [ \"${1-}\" = --version ]; then\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  exit 0\n",
            "fi\n",
            "test -f \"$CODEX_HOME/sessions/thread.jsonl\"\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    Command::cargo_bin("kai")
        .unwrap()
        .current_dir(&launch_dir)
        .env("PATH", path)
        .env("KAI_CREDENTIALS_HOME", &credentials_home)
        .env("CODEX_HOME", &codex_home)
        .env("CODEX_SQLITE_HOME", &codex_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .args(["--quota-auto-restart", "yes", "ar", "thread-id"])
        .assert()
        .success();

    let connection = Connection::open(codex_home.join("state_5.sqlite")).unwrap();
    let rollout_path: String = connection
        .query_row(
            "SELECT rollout_path FROM threads WHERE id = 'thread-id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rollout_path,
        codex_home
            .join("sessions/thread.jsonl")
            .display()
            .to_string()
    );
}

#[test]
fn automatic_recovery_keeps_global_auth_when_it_still_has_quota() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let workspace = root.path().join("workspace");
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let agent_log = root.path().join("agent-log");
    let agent_count = root.path().join("agent-count");
    let quota_log = root.path().join("quota-log");
    let finished_auth = root.path().join("finished-auth.json");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&workspace).unwrap();
    seed_account_set(
        &credentials_home,
        &codex_home,
        &[
            ("alice@example.com", "alice-id", "alice-stored"),
            ("bob@example.com", "bob-id", "bob-stored"),
        ],
        ("alice@example.com", "alice-id", "alice-live"),
    );
    fs::write(
        &finished_auth,
        auth_json("alice@example.com", "alice-id", "alice-finished"),
    )
    .unwrap();

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "auth_file=\n",
            "if [ \"${1-}\" = --auth-file ]; then auth_file=$2; shift 2; fi\n",
            "case \"${1-}\" in\n",
            "--version)\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  ;;\n",
            "app-server)\n",
            "  IFS= read -r initialize\n",
            "  printf '%s\\n' '{\"id\":0,\"result\":{}}'\n",
            "  IFS= read -r initialized\n",
            "  IFS= read -r quota_request\n",
            "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$auth_file\" | head -n 1)\n",
            "  printf '%s\\n' \"$account_id\" >> \"$KAI_TEST_QUOTA_LOG\"\n",
            "  printf '%s\\n' '{\"id\":1,\"result\":{\"rateLimits\":{\"primary\":{\"usedPercent\":25,\"windowDurationMins\":300,\"resetsAt\":2000000000},\"secondary\":null},\"rateLimitsByLimitId\":null,\"rateLimitResetCredits\":null}}'\n",
            "  ;;\n",
            "*)\n",
            "  test -z \"${CODEX_ACCESS_TOKEN-}\"\n",
            "  test -z \"${CODEX_API_KEY-}\"\n",
            "  test -z \"${OPENAI_API_KEY-}\"\n",
            "  count=$(cat \"$KAI_TEST_AGENT_COUNT\" 2>/dev/null || printf 0)\n",
            "  count=$((count + 1))\n",
            "  printf '%s\\n' \"$count\" > \"$KAI_TEST_AGENT_COUNT\"\n",
            "  test -n \"$auth_file\"\n",
            "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$auth_file\" | head -n 1)\n",
            "  printf '%s\\t%s\\t%s\\n' \"$account_id\" \"$CODEX_HOME\" \"$auth_file\" >> \"$KAI_TEST_AGENT_LOG\"\n",
            "  if [ \"$count\" -eq 3 ]; then cp \"$KAI_TEST_FINISHED_AUTH\" \"$auth_file\"; fi\n",
            "  if [ \"$count\" -lt 3 ]; then\n",
            "    printf '%s\\n' 'codex+k (123e4567-e89b-12d3-a456-426614174000): quota exceeded {\"version\":1,\"resume_args\":[\"-c\",\"service_tier=\\\"default\\\"\"]}'\n",
            "  fi\n",
            "  ;;\n",
            "esac\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    Command::cargo_bin("kai")
        .unwrap()
        .current_dir(&workspace)
        .env("PATH", path)
        .env("KAI_CREDENTIALS_HOME", &credentials_home)
        .env("CODEX_HOME", &codex_home)
        .env("CODEX_SQLITE_HOME", &codex_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("KAI_TEST_AGENT_LOG", &agent_log)
        .env("KAI_TEST_AGENT_COUNT", &agent_count)
        .env("KAI_TEST_QUOTA_LOG", &quota_log)
        .env("KAI_TEST_FINISHED_AUTH", &finished_auth)
        .env("CODEX_ACCESS_TOKEN", "must-not-leak")
        .env("CODEX_API_KEY", "must-not-leak")
        .env("OPENAI_API_KEY", "must-not-leak")
        .args(["--quota-auto-restart", "yes", "a"])
        .assert()
        .success();

    let agents = fs::read_to_string(agent_log).unwrap();
    let agents = agents
        .lines()
        .map(|line| {
            let mut fields = line.split('\t');
            (
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        agents
            .iter()
            .map(|(account, _, _)| *account)
            .collect::<Vec<_>>(),
        ["alice-id", "bob-id", "alice-id"]
    );
    assert!(agents.windows(2).all(|rows| rows[0].1 == rows[1].1));
    let expected_codex_home = codex_home.to_str().unwrap();
    assert!(
        agents
            .iter()
            .all(|(_, home, _)| *home == expected_codex_home)
    );
    assert!(agents.iter().all(|(account, _, auth)| {
        let auth = Path::new(auth);
        let expected = match *account {
            "alice-id" => credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com"))),
            "bob-id" => credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("bob@example.com"))),
            _ => return false,
        };
        auth == expected && auth.exists()
    }));
    // The final lookup is the systemwide account's exhaustion guard.
    assert_eq!(
        fs::read_to_string(quota_log).unwrap(),
        "bob-id\nalice-id\nalice-id\n"
    );
    assert_eq!(
        fs::read(codex_home.join("auth.json")).unwrap(),
        auth_json("alice@example.com", "alice-id", "alice-finished")
    );
    assert_eq!(
        fs::read(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com")))
        )
        .unwrap(),
        auth_json("alice@example.com", "alice-id", "alice-finished")
    );
}

#[test]
fn automatic_recovery_promotes_global_auth_when_it_is_exhausted() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let workspace = root.path().join("workspace");
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let agent_log = root.path().join("agent-log");
    let agent_count = root.path().join("agent-count");
    let quota_log = root.path().join("quota-log");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&workspace).unwrap();
    seed_account_set(
        &credentials_home,
        &codex_home,
        &[
            ("alice@example.com", "alice-id", "alice-stored"),
            ("bob@example.com", "bob-id", "bob-stored"),
        ],
        ("alice@example.com", "alice-id", "alice-live"),
    );

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "auth_file=\n",
            "if [ \"${1-}\" = --auth-file ]; then auth_file=$2; shift 2; fi\n",
            "case \"${1-}\" in\n",
            "--version)\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  ;;\n",
            "app-server)\n",
            "  IFS= read -r initialize\n",
            "  printf '%s\\n' '{\"id\":0,\"result\":{}}'\n",
            "  IFS= read -r initialized\n",
            "  IFS= read -r quota_request\n",
            "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$auth_file\" | head -n 1)\n",
            "  printf '%s\\n' \"$account_id\" >> \"$KAI_TEST_QUOTA_LOG\"\n",
            "  case \"$account_id\" in\n",
            "    alice-id) used=100 ;;\n",
            "    bob-id) used=25 ;;\n",
            "    *) used=100 ;;\n",
            "  esac\n",
            "  printf '%s\\n' \"{\\\"id\\\":1,\\\"result\\\":{\\\"rateLimits\\\":{\\\"primary\\\":{\\\"usedPercent\\\":$used,\\\"windowDurationMins\\\":300,\\\"resetsAt\\\":2000000000},\\\"secondary\\\":null},\\\"rateLimitsByLimitId\\\":null,\\\"rateLimitResetCredits\\\":null}}\"\n",
            "  ;;\n",
            "*)\n",
            "  count=$(cat \"$KAI_TEST_AGENT_COUNT\" 2>/dev/null || printf 0)\n",
            "  count=$((count + 1))\n",
            "  printf '%s\\n' \"$count\" > \"$KAI_TEST_AGENT_COUNT\"\n",
            "  test -n \"$auth_file\"\n",
            "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$auth_file\" | head -n 1)\n",
            "  printf '%s\\t%s\\t%s\\n' \"$account_id\" \"$CODEX_HOME\" \"$auth_file\" >> \"$KAI_TEST_AGENT_LOG\"\n",
            "  if [ \"$count\" -eq 1 ]; then\n",
            "    printf '%s\\n' 'codex+k (123e4567-e89b-12d3-a456-426614174000): quota exceeded {\"version\":1,\"resume_args\":[\"-c\",\"service_tier=\\\"default\\\"\"]}'\n",
            "  fi\n",
            "  ;;\n",
            "esac\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    Command::cargo_bin("kai")
        .unwrap()
        .current_dir(&workspace)
        .env("PATH", path)
        .env("KAI_CREDENTIALS_HOME", &credentials_home)
        .env("CODEX_HOME", &codex_home)
        .env("CODEX_SQLITE_HOME", &codex_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("KAI_TEST_AGENT_LOG", &agent_log)
        .env("KAI_TEST_AGENT_COUNT", &agent_count)
        .env("KAI_TEST_QUOTA_LOG", &quota_log)
        .args(["--quota-auto-restart", "yes", "a"])
        .assert()
        .success();

    let agents = fs::read_to_string(agent_log).unwrap();
    let agents = agents
        .lines()
        .map(|line| {
            let mut fields = line.split('\t');
            (
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        agents
            .iter()
            .map(|(account, _, _)| *account)
            .collect::<Vec<_>>(),
        ["alice-id", "bob-id"]
    );
    let global_auth = codex_home.join("auth.json");
    assert!(agents.iter().all(|(account, _, auth)| {
        let auth = Path::new(auth);
        let expected = match *account {
            "alice-id" => credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("alice@example.com"))),
            "bob-id" => credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id("bob@example.com"))),
            _ => return false,
        };
        auth == expected && auth.exists()
    }));
    assert_eq!(fs::read_to_string(quota_log).unwrap(), "bob-id\nalice-id\n");
    assert_eq!(
        fs::read(global_auth).unwrap(),
        auth_json("bob@example.com", "bob-id", "bob-stored")
    );
}

#[test]
fn automatic_recovery_prompts_and_rechecks_when_no_account_has_quota() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let workspace = root.path().join("workspace");
    let credentials_home = root.path().join("credentials");
    let codex_home = root.path().join("codex");
    let runtime_dir = root.path().join("runtime");
    let agent_log = root.path().join("agent-log");
    let agent_count = root.path().join("agent-count");
    let quota_count = root.path().join("quota-count");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&workspace).unwrap();
    seed_account_set(
        &credentials_home,
        &codex_home,
        &[
            ("alice@example.com", "alice-id", "alice-stored"),
            ("bob@example.com", "bob-id", "bob-stored"),
        ],
        ("alice@example.com", "alice-id", "alice-live"),
    );

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "auth_file=\n",
            "if [ \"${1-}\" = --auth-file ]; then auth_file=$2; shift 2; fi\n",
            "case \"${1-}\" in\n",
            "--version)\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  ;;\n",
            "app-server)\n",
            "  IFS= read -r initialize\n",
            "  printf '%s\\n' '{\"id\":0,\"result\":{}}'\n",
            "  IFS= read -r initialized\n",
            "  IFS= read -r quota_request\n",
            "  count=$(cat \"$KAI_TEST_QUOTA_COUNT\" 2>/dev/null || printf 0)\n",
            "  count=$((count + 1))\n",
            "  printf '%s\\n' \"$count\" > \"$KAI_TEST_QUOTA_COUNT\"\n",
            "  if [ \"$count\" -eq 1 ]; then used=100; else used=25; fi\n",
            "  printf '%s\\n' \"{\\\"id\\\":1,\\\"result\\\":{\\\"rateLimits\\\":{\\\"primary\\\":{\\\"usedPercent\\\":$used,\\\"windowDurationMins\\\":300,\\\"resetsAt\\\":2000000000},\\\"secondary\\\":null},\\\"rateLimitsByLimitId\\\":null,\\\"rateLimitResetCredits\\\":null}}\"\n",
            "  ;;\n",
            "*)\n",
            "  count=$(cat \"$KAI_TEST_AGENT_COUNT\" 2>/dev/null || printf 0)\n",
            "  count=$((count + 1))\n",
            "  printf '%s\\n' \"$count\" > \"$KAI_TEST_AGENT_COUNT\"\n",
            "  account_id=$(sed -n 's/^[[:space:]]*\"account_id\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' \"$auth_file\" | head -n 1)\n",
            "  printf '%s\\n' \"$account_id\" >> \"$KAI_TEST_AGENT_LOG\"\n",
            "  if [ \"$count\" -eq 1 ]; then\n",
            "    printf '%s\\n' 'codex+k (123e4567-e89b-12d3-a456-426614174000): quota exceeded {\"version\":1,\"resume_args\":[\"-c\",\"service_tier=\\\"default\\\"\"]}'\n",
            "  fi\n",
            "  ;;\n",
            "esac\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 40,
            cols: 160,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_kai"));
    command.args(["--quota-auto-restart", "yes", "a"]);
    command.cwd(&workspace);
    command.env("PATH", path);
    command.env("TERM", "xterm-256color");
    command.env("KAI_CREDENTIALS_HOME", &credentials_home);
    command.env("CODEX_HOME", &codex_home);
    command.env("CODEX_SQLITE_HOME", &codex_home);
    command.env("XDG_RUNTIME_DIR", &runtime_dir);
    command.env("KAI_TEST_AGENT_LOG", &agent_log);
    command.env("KAI_TEST_AGENT_COUNT", &agent_count);
    command.env("KAI_TEST_QUOTA_COUNT", &quota_count);
    let mut child = pair.slave.spawn_command(command).unwrap();
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader().unwrap();
    let mut writer = pair.master.take_writer().unwrap();
    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&output);
    let reader_thread = thread::spawn(move || {
        let mut buffer = [0; 4096];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 {
                break;
            }
            captured.lock().unwrap().extend_from_slice(&buffer[..count]);
        }
    });

    let prompt = "No enrolled account with usable Codex quota was found; retry? (Y/n)";
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let seen = String::from_utf8_lossy(&output.lock().unwrap()).contains(prompt);
        if seen {
            break;
        }
        assert!(Instant::now() < deadline, "retry prompt was not rendered");
        thread::sleep(Duration::from_millis(10));
    }
    writer.write_all(b"\r").unwrap();
    writer.flush().unwrap();

    let status = child.wait().unwrap();
    drop(writer);
    drop(pair.master);
    reader_thread.join().unwrap();

    assert!(
        status.success(),
        "kai failed with output: {}",
        String::from_utf8_lossy(&output.lock().unwrap())
    );
    assert_eq!(fs::read_to_string(agent_log).unwrap(), "alice-id\nbob-id\n");
    // The successful retry also checks the still-active systemwide account.
    assert_eq!(fs::read_to_string(quota_count).unwrap(), "3\n");
}

fn seed_account(credentials_home: &Path, codex_home: &Path) {
    seed_account_set(
        credentials_home,
        codex_home,
        &[("alice@example.com", "alice-id", "alice-refresh")],
        ("alice@example.com", "alice-id", "alice-live"),
    );
}

fn seed_account_set(
    credentials_home: &Path,
    codex_home: &Path,
    accounts: &[(&str, &str, &str)],
    active: (&str, &str, &str),
) {
    fs::create_dir_all(credentials_home.join("profiles")).unwrap();
    fs::create_dir_all(codex_home).unwrap();
    for (email, account_id, refresh_token) in accounts {
        fs::write(
            credentials_home
                .join("profiles")
                .join(format!("{}.json", profile_id(email))),
            auth_json(email, account_id, refresh_token),
        )
        .unwrap();
    }
    fs::write(
        credentials_home.join("state.json"),
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "codex_home": codex_home,
            "profiles": accounts.iter().map(|(email, account_id, _)| json!({
                "id": profile_id(email),
                "email": email,
                "account_id": account_id,
                "enrolled_at": 0
            })).collect::<Vec<_>>()
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        codex_home.join("auth.json"),
        auth_json(active.0, active.1, active.2),
    )
    .unwrap();
    fs::write(
        codex_home.join("config.toml"),
        "model = \"gpt-test\"\ncli_auth_credentials_store = \"file\"\n",
    )
    .unwrap();
}

fn profile_id(email: &str) -> String {
    Sha256::digest(email.to_ascii_lowercase().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
