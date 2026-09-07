#![cfg(unix)]
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs};
use tempfile::{TempDir, tempdir};

const THREAD_ID: &str = "123e4567-e89b-12d3-a456-426614174000";

struct Fixture {
    root: TempDir,
    provider: PathBuf,
    paths: Value,
}

impl Fixture {
    fn new() -> Self {
        let root = tempdir().unwrap();
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let paths = json!({
            "auth_file": root.path().join("auth.json"),
            "credential_use_lock": root.path().join("use.lock"),
            "credential_use_lock_mode": "shared",
            "credential_mutation_lock": root.path().join("mutation.lock"),
            "available_file": root.path().join("available"),
        });
        for (key, path) in paths.as_object().unwrap() {
            if key == "credential_use_lock_mode" {
                continue;
            }
            fs::write(path.as_str().unwrap(), "{}").unwrap();
            fs::set_permissions(path.as_str().unwrap(), fs::Permissions::from_mode(0o600)).unwrap();
        }
        fs::write(root.path().join("paths.json"), paths.to_string()).unwrap();
        fs::write(
            root.path().join("handoff.json"),
            json!({
                "format": "codex+k-input-handoff", "version": 1,
                "thread_id": THREAD_ID,
                "resume_args": ["--model", "gpt-test", "-c", "tui.theme=\"other\""],
            })
            .to_string(),
        )
        .unwrap();
        fs::write(bin.join("codex"), r#"#!/usr/bin/env python3
import array, fcntl, json, os, pathlib, socket, sys
args = sys.argv[1:]
if args == ["--version"]:
    print("codex-cli " + os.environ.get("KAI_TEST_VERSION", "0.154.0-k.ac192cd7"))
    sys.exit(0)
def value(flag):
    return args[args.index(flag) + 1]
if "--credential-startup-socket" in args:
    assert value("--credential-protocol-version") == "2"
    startup = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    startup.connect(value("--credential-startup-socket"))
    nonce = value("--credential-startup-nonce")
    startup.sendall(("READY " + nonce + "\n").encode())
    expected = ("GO " + nonce + "\n").encode()
    frame, ancillary, flags, _ = startup.recvmsg(len(expected), socket.CMSG_SPACE(array.array("i").itemsize))
    while len(frame) < len(expected):
        frame += startup.recv(len(expected) - len(frame))
    assert frame == expected and len(ancillary) == 1
    level, kind, data = ancillary[0]
    assert level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS
    descriptor = array.array("i", data)[0]
    assert os.fstat(descriptor).st_ino == os.stat(value("--credential-use-lock")).st_ino
    with open(value("--credential-use-lock"), "r+") as probe:
        try:
            operation = fcntl.LOCK_SH if value("--credential-use-lock-mode") == "exclusive" else fcntl.LOCK_EX
            fcntl.flock(probe, operation | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise AssertionError("credential use lock was not retained")
    assert "CODEX_ACCESS_TOKEN" not in os.environ
else:
    assert os.environ.get("CODEX_ACCESS_TOKEN") == "ordinary-access"
root = pathlib.Path(os.environ["KAI_TEST_ROOT"])
with (root / "codex-calls").open("a") as output:
    output.write(json.dumps({"args": args, "codex_home": os.environ.get("CODEX_HOME"),
                             "sqlite_home": os.environ.get("CODEX_SQLITE_HOME")}) + "\n")
cause = os.environ.get("KAI_TEST_CAUSE")
if cause and args[0] != "resume":
    payload = {"version": 3, "outcome": cause, "handoff_path": str(root / "handoff.json"),
               "unavailable_until": 1800000000 if cause == "quota-exhausted" else None}
    print("codex+k (123e4567-e89b-12d3-a456-426614174000): supervised exit " + json.dumps(payload))
    sys.exit(75)
sys.exit(int(os.environ.get("KAI_TEST_EXIT", "0")))
"#).unwrap();
        fs::set_permissions(bin.join("codex"), fs::Permissions::from_mode(0o700)).unwrap();
        let provider = root.path().join("provider.sh");
        fs::write(
            &provider,
            r#"set -euo pipefail
exec python3 - "$@" <<'PY'
import json, os, pathlib, sys
assert "CODEX_HOME" not in os.environ and "CODEX_ACCESS_TOKEN" not in os.environ
root = pathlib.Path(os.environ["KAI_TEST_ROOT"])
with (root / "provider-calls").open("a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\n")
print((root / "paths.json").read_text())
PY
"#,
        )
        .unwrap();
        Self {
            root,
            provider,
            paths,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::cargo_bin("kai").unwrap();
        command
            .timeout(Duration::from_secs(5))
            .current_dir(self.root.path())
            .env(
                "PATH",
                env::join_paths(
                    std::iter::once(self.root.path().join("bin"))
                        .chain(env::split_paths(&env::var_os("PATH").unwrap())),
                )
                .unwrap(),
            )
            .env("KAI_TEST_ROOT", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("CODEX_HOME", self.root.path().join("codex-home"))
            .env("CODEX_SQLITE_HOME", self.root.path().join("sqlite-home"))
            .env("CODEX_ACCESS_TOKEN", "ordinary-access");
        command
    }

    fn managed(&self) -> Command {
        let mut command = self.command();
        command.arg("--credential-provider").arg(&self.provider);
        command
    }

    fn calls(&self, filename: &str) -> Vec<Value> {
        fs::read_to_string(self.root.path().join(filename))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn argument<'a>(args: &'a [Value], name: &str) -> &'a Value {
    &args[args.iter().position(|value| value == name).unwrap() + 1]
}

fn assert_launch_preferences(call: &Value) {
    let preferences = call["args"]
        .as_array()
        .unwrap()
        .windows(2)
        .filter(|pair| pair[0] == "-c")
        .filter_map(|pair| {
            let (key, value) = pair[1].as_str().unwrap().split_once('=').unwrap();
            if key == "service_tier" {
                return None;
            }
            let parsed: toml::Table = toml::from_str(&format!("value={value}")).unwrap();
            Some((
                key.to_owned(),
                serde_json::to_value(&parsed["value"]).unwrap(),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    assert_eq!(
        Value::Object(preferences),
        json!({
            "agents.max_concurrent_threads_per_session": 16,
            "tui.theme": "monokai-extended",
            "tui.status_line_use_colors": true,
            "tui.resume_cwd": "session",
            "notice.hide_rate_limit_model_nudge": true,
            "tui.status_line": [
                "model-with-reasoning", "run-state", "context-remaining", "weekly-limit",
                "total-input-tokens", "total-output-tokens", "fast-mode",
            ],
        })
    );
}

#[test]
fn ordinary_exit_keeps_the_selected_credential_without_calling_a_hook() {
    for exit in [0, 12] {
        let fixture = Fixture::new();
        fixture
            .managed()
            .env("KAI_TEST_EXIT", exit.to_string())
            .assert()
            .code(exit);
        let calls = fixture.calls("provider-calls");
        assert_eq!(calls.len(), 1);
        let args = calls[0].as_array().unwrap();
        assert_eq!(args[0], "acquire");
        assert!(!args.iter().any(|arg| arg == "--unique"));
        assert_eq!(
            argument(args, "--codex-home"),
            fixture.root.path().join("codex-home").to_str().unwrap()
        );
        assert_eq!(
            argument(args, "--sqlite-home"),
            fixture.root.path().join("sqlite-home").to_str().unwrap()
        );
        assert!(std::path::Path::new(fixture.paths["auth_file"].as_str().unwrap()).exists());
        let codex = fixture.calls("codex-calls");
        assert_eq!(
            argument(codex[0]["args"].as_array().unwrap(), "--auth-file"),
            &fixture.paths["auth_file"]
        );
        assert_eq!(
            argument(
                codex[0]["args"].as_array().unwrap(),
                "--credential-use-lock-mode"
            ),
            "shared"
        );
    }
}

#[test]
fn provider_progress_passes_through_stderr_without_contaminating_json() {
    let fixture = Fixture::new();
    let script = fs::read_to_string(&fixture.provider).unwrap();
    fs::write(
        &fixture.provider,
        format!("printf '\\rFetching credential…\\033[K\\n' >&2\n{script}"),
    )
    .unwrap();
    fixture
        .managed()
        .assert()
        .success()
        .stderr(predicate::str::contains("\rFetching credential…\x1b[K\n"))
        .stdout(predicate::str::contains("Fetching credential").not());
    assert_eq!(fixture.calls("provider-calls").len(), 1);
}

#[test]
fn provider_must_supply_a_valid_lock_mode() {
    for mode in [None, Some("random")] {
        let mut fixture = Fixture::new();
        match mode {
            Some(mode) => fixture.paths["credential_use_lock_mode"] = json!(mode),
            None => {
                fixture
                    .paths
                    .as_object_mut()
                    .unwrap()
                    .remove("credential_use_lock_mode");
            }
        }
        fs::write(
            fixture.root.path().join("paths.json"),
            fixture.paths.to_string(),
        )
        .unwrap();
        fixture
            .managed()
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "credential provider returned invalid JSON",
            ));
        assert_eq!(fixture.calls("provider-calls").len(), 1);
        assert!(!fixture.root.path().join("codex-calls").exists());
    }
}

#[test]
fn unavailable_credential_calls_next_with_the_cause_and_resumes_the_thread() {
    for cause in ["quota-exhausted", "credential-invalid"] {
        let mut fixture = Fixture::new();
        fixture.paths["credential_use_lock_mode"] = json!("exclusive");
        fs::write(
            fixture.root.path().join("paths.json"),
            fixture.paths.to_string(),
        )
        .unwrap();
        fixture
            .managed()
            .env("KAI_TEST_CAUSE", cause)
            .assert()
            .success();
        let calls = fixture.calls("provider-calls");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0][0], "acquire");
        let next = calls[1].as_array().unwrap();
        assert_eq!(next[0], "next");
        assert_eq!(argument(next, "--credential-use-lock-mode"), "exclusive");
        assert_eq!(argument(next, "--cause"), cause);
        assert_eq!(argument(next, "--auth-file"), &fixture.paths["auth_file"]);
        assert_eq!(
            argument(next, "--available-file"),
            &fixture.paths["available_file"]
        );
        if cause == "quota-exhausted" {
            assert_eq!(argument(next, "--unavailable-until"), "1800000000");
        } else {
            assert!(!next.iter().any(|value| value == "--unavailable-until"));
        }
        let codex = fixture.calls("codex-calls");
        assert_eq!(codex.len(), 2);
        for call in &codex {
            assert_launch_preferences(call);
            assert_eq!(
                argument(
                    call["args"].as_array().unwrap(),
                    "--credential-use-lock-mode"
                ),
                "exclusive"
            );
        }
        assert_eq!(codex[1]["args"][0], "resume");
        assert_eq!(codex[1]["args"][1], THREAD_ID);
        assert_eq!(
            argument(
                codex[1]["args"].as_array().unwrap(),
                "--restore-input-handoff"
            ),
            fixture.root.path().join("handoff.json").to_str().unwrap()
        );
    }
}

#[test]
fn unset_provider_preserves_auth_and_aborts_when_rotation_is_required() {
    let fixture = Fixture::new();
    fixture.command().assert().success();
    fixture
        .command()
        .env("KAI_TEST_CAUSE", "quota-exhausted")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "automatic credential rotation requires a configured credential provider",
        ));
    assert!(!fixture.root.path().join("provider-calls").exists());
    for call in fixture.calls("codex-calls") {
        assert_eq!(
            call["codex_home"],
            fixture.root.path().join("codex-home").to_str().unwrap()
        );
        assert_eq!(
            call["sqlite_home"],
            fixture.root.path().join("sqlite-home").to_str().unwrap()
        );
        assert!(
            !call["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg == "--auth-file")
        );
    }
}

#[test]
fn stock_codex_and_disabled_supervision_do_not_invoke_a_provider() {
    for (version, disabled) in [("0.154.0", false), ("0.154.0+k", true)] {
        let fixture = Fixture::new();
        let mut command = fixture.managed();
        command.env("KAI_TEST_VERSION", version);
        if disabled {
            command.args(["--no-auto-restart", "--fast"]);
        }
        command.assert().success();
        assert!(!fixture.root.path().join("provider-calls").exists());
        let calls = fixture.calls("codex-calls");
        assert_launch_preferences(&calls[0]);
        assert_eq!(
            argument(calls[0]["args"].as_array().unwrap(), "-c"),
            if disabled {
                "service_tier=fast"
            } else {
                "service_tier=default"
            }
        );
        for flag in [
            "--exit-on-quota-exceeded",
            "--auth-file",
            "--credential-use-lock",
        ] {
            assert!(
                !calls[0]["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|arg| arg == flag)
            );
        }
    }
}

#[test]
fn failed_acquire_aborts_before_codex_and_does_not_invoke_another_hook() {
    let fixture = Fixture::new();
    fs::write(
        &fixture.provider,
        r#"printf '%s\n' "$1" >> "$KAI_TEST_ROOT/failed-calls"
exit 12
"#,
    )
    .unwrap();
    fixture
        .managed()
        .assert()
        .failure()
        .stderr(predicate::str::contains("credential provider exited with"));
    assert_eq!(
        fs::read_to_string(fixture.root.path().join("failed-calls")).unwrap(),
        "acquire\n"
    );
    assert!(!fixture.root.path().join("codex-calls").exists());
}

#[test]
fn cli_provider_overrides_the_flat_user_configuration() {
    let fixture = Fixture::new();
    let config_dir = fixture.root.path().join("config/kai");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("config.toml"),
        "credential_provider = \"../../provider.sh\"\n",
    )
    .unwrap();
    fixture.command().assert().success();
    assert_eq!(fixture.calls("provider-calls").len(), 1);

    fs::write(config_dir.join("bad.sh"), "exit 19\n").unwrap();
    fs::write(
        config_dir.join("config.toml"),
        "credential_provider = \"bad.sh\"\n",
    )
    .unwrap();
    fixture
        .command()
        .assert()
        .failure()
        .stderr(predicate::str::contains("exit status: 19"));
    fixture.managed().assert().success();
}

#[test]
fn resume_uses_all_sessions_or_the_supplied_id() {
    for command in ["r", "res", "resume"] {
        let fixture = Fixture::new();
        fixture.command().arg(command).assert().success();
        fixture
            .command()
            .args([command, THREAD_ID])
            .assert()
            .success();
        let calls = fixture.calls("codex-calls");
        for call in &calls {
            assert_launch_preferences(call);
        }
        assert_eq!(
            &calls[0]["args"].as_array().unwrap()[..2],
            &[json!("resume"), json!("--all")]
        );
        assert_eq!(
            &calls[1]["args"].as_array().unwrap()[..2],
            &[json!("resume"), json!(THREAD_ID)]
        );
    }
}

#[test]
fn llm_get_remains_available_without_codex_or_provider() {
    let fixture = Fixture::new();
    let source = fixture.root.path().join("sample.rs");
    fs::write(&source, "fn sample() {}\n").unwrap();
    fixture
        .command()
        .args(["l", "--slim", "--out", "-"])
        .arg(source)
        .assert()
        .success()
        .stdout(predicate::str::contains("fn sample() {}"));
    assert!(!fixture.root.path().join("codex-calls").exists());
    assert!(!fixture.root.path().join("provider-calls").exists());
}

#[test]
fn removed_credential_commands_are_rejected() {
    for command in ["cred", "next"] {
        Command::cargo_bin("kai")
            .unwrap()
            .arg(command)
            .assert()
            .failure()
            .stderr(predicate::str::contains(format!(
                "unrecognized subcommand '{command}'"
            )));
    }
}

#[test]
fn version_reports_the_package_version() {
    Command::cargo_bin("kai")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(format!("kai {}\n", env!("CARGO_PKG_VERSION")));
}
