use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::tempdir;

fn think() -> Command {
    Command::new(env!("CARGO_BIN_EXE_think"))
}

fn run(args: &[&str], cwd: Option<&Path>) -> Result<Output, Box<dyn Error>> {
    run_with_env(args, cwd, &[])
}

fn run_with_env(
    args: &[&str],
    cwd: Option<&Path>,
    envs: &[(&str, &str)],
) -> Result<Output, Box<dyn Error>> {
    let mut command = think();
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    if !envs.iter().any(|(key, _)| *key == "THINK_HOME") {
        command.env(
            "THINK_HOME",
            std::env::temp_dir().join(format!("think-test-home-{}", std::process::id())),
        );
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    Ok(command.output()?)
}

fn assert_success(output: Output) -> Result<Output, Box<dyn Error>> {
    if output.status.success() {
        return Ok(output);
    }
    Err(format!(
        "command failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into())
}

fn write_agent_state(
    project: &Path,
    role: &str,
    agent: &str,
    status: &str,
    archived: bool,
) -> Result<(), Box<dyn Error>> {
    write_agent_state_with_run_count(project, role, agent, status, archived, 0)
}

fn write_agent_state_with_run_count(
    project: &Path,
    role: &str,
    agent: &str,
    status: &str,
    archived: bool,
    run_count: u64,
) -> Result<(), Box<dyn Error>> {
    let dir = project.join("roles").join(role).join("agents").join(agent);
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join("agent.toml"),
        format!(
            r#"version = 1
role = "{role}"
agent = "{agent}"
backend = "codex"
mode = "repeatable"
status = "{status}"
archived = {archived}
current_step = 0
run_count = {run_count}
channels = ["report", "report-single"]
created_at = 1
updated_at = 2
"#
        ),
    )?;
    Ok(())
}

#[cfg(unix)]
fn install_fake_codex(bin_dir: &Path) -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    fs::create_dir_all(bin_dir)?;
    let codex = bin_dir.join("codex");
    fs::write(
        &codex,
        r#"#!/bin/sh
if [ -n "$THINK_FAKE_CODEX_LOG" ]; then
  printf '%s\n' "$*" >> "$THINK_FAKE_CODEX_LOG"
fi
if [ "$1" = "--version" ]; then
  printf 'codex-fake 0.0.0\n'
  exit 0
fi
if [ "$1" = "update" ]; then
  exit 0
fi
if [ "$1" = "login" ]; then
  exit 0
fi
case " $* " in
  *" debug models "*)
    cat <<'JSON'
{
  "models": [
    {
      "slug": "gpt-5-codex",
      "display_name": "GPT-5 Codex",
      "visibility": "list",
      "supported_reasoning_levels": [
        {"effort": "low"},
        {"effort": "medium"},
        {"effort": "high"}
      ]
    }
  ]
}
JSON
    exit 0
    ;;
esac
for arg in "$@"; do
  if [ "$arg" = "app-server" ]; then
    fake_codex_app_server="${TMPDIR:-/tmp}/think-fake-codex-app-server-$$.py"
    cat > "$fake_codex_app_server" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

LOG = os.environ.get("THINK_FAKE_CODEX_LOG", "")

def log(text):
    if LOG:
        with open(LOG, "a", encoding="utf-8") as handle:
            handle.write(text.rstrip() + "\n")

def marker(name):
    if not LOG:
        return None
    return Path(f"{LOG}.{name}")

def first_time(name):
    path = marker(name)
    if path is None:
        return True
    if path.exists():
        return False
    path.write_text("", encoding="utf-8")
    return True

def send(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)

def is_agent_dir(path):
    parts = Path(path).parts
    return len(parts) >= 4 and parts[-4] == "roles" and parts[-2] == "agents"

def role_and_agent(path):
    parts = Path(path).parts
    return parts[-3], parts[-1]

def write_episode_work(cwd):
    if os.environ.get("THINK_FAKE_CODEX_TOUCH_EPISODE_WORK") != "1" or not is_agent_dir(cwd):
        return
    role, agent = role_and_agent(cwd)
    if role != "episode":
        return
    Path("work/own/episodes").mkdir(parents=True, exist_ok=True)
    Path("channels/report-single").mkdir(parents=True, exist_ok=True)
    Path(f"work/own/episodes/{agent}.tex").write_text(
        f"\\episodeprojecttitle{{Smoke}}\\section{{Smoke {agent}}}\\n",
        encoding="utf-8",
    )
    Path(f"channels/report-single/{agent}.pdf").write_text("fake pdf\n", encoding="utf-8")

def write_manifest(cwd):
    if not is_agent_dir(cwd):
        return
    if os.environ.get("THINK_FAKE_CODEX_BAD_MANIFEST_ONCE") == "1" and first_time("bad-manifest-once"):
        Path("manifest.toml").write_text(
            'role_summary = "fake supervisor"\ndisposition = "done"\n',
            encoding="utf-8",
        )
        return
    if os.environ.get("THINK_FAKE_CODEX_WRITE_MANIFEST") == "1":
        Path("manifest.toml").write_text(
            'role_summary = "fake supervisor"\ndisposition = "stop"\n',
            encoding="utf-8",
        )

def write_channel_outbox():
    if os.environ.get("THINK_FAKE_CODEX_BAD_CHANNEL_ONCE") != "1":
        return
    Path("channels/report").mkdir(parents=True, exist_ok=True)
    if first_time("bad-channel-once"):
        link = Path("channels/report/bad-link")
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to("../work")
        return
    link = Path("channels/report/bad-link")
    if link.exists() or link.is_symlink():
        link.unlink()
    Path("channels/report/good.txt").write_text("fake report artifact\n", encoding="utf-8")

def write_agent_state_corruption():
    if os.environ.get("THINK_FAKE_CODEX_BAD_AGENT_TOML_ONCE") == "1" and first_time("bad-agent-toml-once"):
        Path("agent.toml").write_text("this is not toml\n", encoding="utf-8")

def should_emit_reply():
    return not (
        os.environ.get("THINK_FAKE_CODEX_SKIP_REPLY_ONCE") == "1"
        and first_time("skip-reply-once")
    )

def finish_agent_run(cwd):
    write_episode_work(cwd)
    write_manifest(cwd)
    write_channel_outbox()
    write_agent_state_corruption()

def prompt_text(params):
    texts = []
    for item in params.get("input", []):
        if item.get("type") == "text":
            texts.append(item.get("text", ""))
    return "\n".join(texts)

def log_prompt(prompt, cwd):
    log(prompt)
    for match in re.finditer(r"Read `([^`]+)`", prompt):
        path = Path(match.group(1))
        if not path.is_absolute():
            path = Path(cwd) / path
        if path.exists():
            log(path.read_text(encoding="utf-8"))

cwd = os.getcwd()
thread_id = "thread-fake"
turn_id = "turn-fake"
for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    method = request.get("method")
    log(method or "")
    request_id = request.get("id")
    params = request.get("params") or {}
    if method == "initialize":
        send({"id": request_id, "result": {"serverInfo": {"name": "fake-codex-app-server"}}})
    elif method == "initialized":
        continue
    elif method in ("thread/start", "thread/resume"):
        thread_id = params.get("threadId") or thread_id
        log(thread_id)
        send({"id": request_id, "result": {"thread": {"id": thread_id}}})
    elif method == "turn/start":
        prompt = prompt_text(params)
        log_prompt(prompt, cwd)
        send({"id": request_id, "result": {"turn": {"id": turn_id}}})
        if os.environ.get("THINK_FAKE_CODEX_SIGKILL_ONCE") == "1" and first_time("sigkill-once"):
            sys.exit(137)
        if os.environ.get("THINK_FAKE_CODEX_QUOTA_ONCE") == "1" and first_time("quota-once"):
            send({
                "method": "turn/completed",
                "params": {
                    "threadId": thread_id,
                    "turn": {"id": turn_id, "status": "failed", "error": "quota exceeded: rate limit reached"},
                },
            })
            continue
        finish_agent_run(cwd)
        if should_emit_reply():
            send({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": "item-fake",
                    "delta": "fake final reply\n",
                },
            })
        send({
            "method": "turn/completed",
            "params": {"threadId": thread_id, "turn": {"id": turn_id, "status": "completed"}},
        })
    elif method == "turn/steer":
        log_prompt(prompt_text(params), cwd)
        send({"id": request_id, "result": {}})
    else:
        if request_id is not None:
            send({"id": request_id, "result": {}})
PY
    python3 "$fake_codex_app_server" "$@"
    status=$?
    rm -f "$fake_codex_app_server"
    exit $status
  fi
done
printf 'unexpected fake codex invocation: %s\n' "$*" >&2
exit 2
"#,
    )?;
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

#[cfg(unix)]
fn install_fake_open(bin_dir: &Path) -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    fs::create_dir_all(bin_dir)?;
    for command in ["open", "xdg-open"] {
        let open = bin_dir.join(command);
        fs::write(
            &open,
            r#"#!/bin/sh
if [ "$#" -ne 1 ]; then
  printf 'expected exactly one path, got %s\n' "$#" >&2
  exit 2
fi
printf '%s\n' "$1" > "$THINK_FAKE_OPEN_LOG"
"#,
        )?;
        fs::set_permissions(&open, fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

fn newest_command_prompt(project: &Path, command: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(
        newest_command_turn(project, command)?.join("PROMPT.md"),
    )?)
}

fn newest_command_reply(project: &Path, command: &str) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(
        newest_command_turn(project, command)?.join("REPLY.md"),
    )?)
}

fn newest_command_turn(
    project: &Path,
    command: &str,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let root = project.join("runtime").join("commands").join(command);
    let mut runs = fs::read_dir(&root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    runs.sort();
    let Some(run) = runs.pop() else {
        return Err(format!("no `{command}` command run found under {}", root.display()).into());
    };
    Ok(run.join("1"))
}

fn only_agent_dir(project: &Path, role: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let agents = project.join("roles").join(role).join("agents");
    let mut entries = fs::read_dir(&agents)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    if entries.len() != 1 {
        return Err(format!("expected exactly one agent under {}", agents.display()).into());
    }
    Ok(entries.remove(0))
}

fn wait_for_only_agent_dir(
    project: &Path,
    role: &str,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match only_agent_dir(project, role) {
            Ok(path) => return Ok(path),
            Err(err) if Instant::now() >= deadline => {
                return Err(err);
            }
            Err(_) => {}
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_path(path: &Path) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("timed out waiting for {}", path.display()).into())
}

fn wait_for_file_contains(path: &Path, needle: &str) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if path.exists() {
            let text = fs::read_to_string(path)?;
            if text.contains(needle) {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "timed out waiting for {} to contain {needle:?}",
        path.display()
    )
    .into())
}

#[test]
fn top_level_help_hides_advanced_namespaces_until_all() -> Result<(), Box<dyn Error>> {
    let output = assert_success(run(&["--help"], None)?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("more"));
    assert!(stdout.contains("status"));
    assert!(stdout.contains("open"));
    assert!(stdout.contains("assist"));
    assert!(!stdout.contains("check"));
    assert!(stdout.contains("help"));
    assert!(stdout.contains("Print help (use --all for more options)"));
    assert!(stdout.contains("Running `think` with no command is shorthand for `think status`."));
    assert!(!stdout.contains("Use `think help --all`"));
    assert!(!stdout.contains("list"));
    assert!(!stdout.contains("retry"));
    assert!(!stdout.contains("trigger"));
    assert!(!stdout.contains("  agent "));
    assert!(!stdout.contains("  role "));
    assert!(!stdout.contains("  channel "));

    let output = assert_success(run(&["help", "--all"], None)?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("  agent"));
    assert!(stdout.contains("  role"));
    assert!(stdout.contains("  channel"));
    assert!(stdout.contains("  advanced"));
    assert!(!stdout.contains("run-child"));
    assert!(!stdout.contains("run-orchestrator"));
    assert!(!stdout.contains("run-notices"));
    assert!(!stdout.contains("run-merge-queue"));

    let output = assert_success(run(&["advanced", "--help"], None)?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("retry-errored"));
    assert!(stdout.contains("trigger"));

    let output = run(&["list"], None)?;
    assert!(!output.status.success());

    let output = run(&["new", "--help"], None)?;
    assert!(!output.status.success());

    let output = run(&["fix", "--help"], None)?;
    assert!(!output.status.success());

    let output = assert_success(run(&["more", "--help"], None)?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("Usage: think more [OPTIONS] [AGENT]"));
    assert!(stdout.contains("--new"));
    assert!(!stdout.contains("--role"));

    let output = assert_success(run(&["sta", "--help"], None)?)?;
    assert!(String::from_utf8(output.stdout)?.contains("Usage: think status"));

    let output = assert_success(run(&["proj", "n", "--help"], None)?)?;
    assert!(String::from_utf8(output.stdout)?.contains("Usage: think project new"));

    let output = assert_success(run(&["adv", "prov", "cod", "c", "--help"], None)?)?;
    assert!(
        String::from_utf8(output.stdout)?.contains("Usage: think advanced provider codex config")
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn open_command_invokes_system_open_on_project_root() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    let think_home = temp.path().join("think-home");
    assert_success(run_with_env(
        &[
            "project",
            "new",
            "--no-template",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
        &[(
            "THINK_HOME",
            think_home.to_str().expect("temporary path is valid UTF-8"),
        )],
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_open(&fake_bin)?;
    let log = temp.path().join("open.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    assert_success(run_with_env(
        &["open"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            (
                "THINK_HOME",
                think_home.to_str().expect("temporary path is valid UTF-8"),
            ),
            (
                "THINK_FAKE_OPEN_LOG",
                log.to_str().expect("temporary path is valid UTF-8"),
            ),
        ],
    )?)?;

    assert_eq!(
        fs::read_to_string(log)?.trim(),
        project.canonicalize()?.display().to_string()
    );
    Ok(())
}

#[test]
fn status_reads_existing_raw_pty_exit_files() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state_with_run_count(&project, "episode", "ep1", "done", false, 1)?;
    let run_dir = project
        .join("roles")
        .join("episode")
        .join("agents")
        .join("ep1")
        .join("runs")
        .join("1");
    fs::create_dir_all(&run_dir)?;
    fs::write(
        run_dir.join("exit.toml"),
        "success = true\ncode = 0\npid = 170283\n",
    )?;

    let output = assert_success(run(&["status"], Some(&project))?)?;
    assert!(!String::from_utf8(output.stderr)?.contains("missing field `run_id`"));
    Ok(())
}

#[test]
fn status_surfaces_invalid_manifest_without_failing() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state(&project, "episode", "ep1", "done", false)?;
    fs::write(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("ep1")
            .join("manifest.toml"),
        "role_summary = \"bad manifest\"\ndisposition = \"done\"\n",
    )?;

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("*manifest error*"));
    assert!(stdout.contains("manifest error:"));
    assert!(stdout.contains("unknown variant `done`"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn assist_prompt_exposes_project_and_cli_context() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    let output = assert_success(run_with_env(
        &[
            "assist",
            "start two episode agents investigating the next cases",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
        ],
    )?)?;
    assert!(!String::from_utf8(output.stdout)?.contains("session id:"));

    let prompt = newest_command_prompt(&project, "assist")?;
    let reply = newest_command_reply(&project, "assist")?;
    assert!(prompt.contains("# think assist"));
    assert!(prompt.contains("run `think` CLI commands"));
    assert!(prompt.contains("# think-tool source"));
    assert!(prompt.contains("# Current think project"));
    assert!(prompt.contains("# Current operational snapshot"));
    assert!(prompt.contains("start two episode agents"));
    assert!(reply.contains("fake final reply"));
    Ok(())
}

#[test]
fn supervisor_records_failed_app_server_turn() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state(&project, "episode", "1", "running", false)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    let output = run_with_env(
        &[
            "run-child",
            "orchestrator",
            "--project",
            project.to_str().expect("temporary path is valid UTF-8"),
            "--role",
            "episode",
            "--agent",
            "1",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_QUOTA_ONCE", "1"),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
        ],
    )?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("status code 1"));

    let supervisor = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("orchestrator.toml"),
    )?;
    assert!(supervisor.contains("status = \"needs-attention\""));
    assert!(supervisor.contains("app-server turn failed with status code 1"));
    let log = fs::read_to_string(log)?;
    assert!(log.contains("app-server --stdio"));
    assert!(log.contains("thread/start"));
    assert!(log.contains("turn/start"));

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("needs-attention"));
    assert!(stdout.contains("usage unavailable"));
    assert!(!stdout.contains("quota · ok"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn retry_updates_active_waiting_supervisors() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state(&project, "episode", "1", "running", false)?;
    fs::write(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("orchestrator.toml"),
        r#"version = 1
status = "waiting-for-quota"
oom_restarts = 0
quota_retries = 1
last_run_id = 1
child_pid = 123
next_retry_at = 9999999999
updated_at = 1
"#,
    )?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = assert_success(run_with_env(
        &["advanced", "retry-errored"],
        Some(&project),
        &[("PATH", path.as_str())],
    )?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("updated 1 active runtime orchestrator"));
    assert!(
        project
            .join("runtime")
            .join("sessions")
            .join("episode")
            .join("1")
            .join("session.toml")
            .exists()
    );
    assert!(
        fs::read_to_string(
            project
                .join("roles")
                .join("episode")
                .join("agents")
                .join("1")
                .join("agent.toml"),
        )?
        .contains("pane_id = \"native:episode/1\"")
    );
    let supervisor = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("orchestrator.toml"),
    )?;
    assert!(!supervisor.contains("9999999999"));
    assert!(project.join("runtime").join("retry.toml").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn supervisor_restarts_after_sigkill_oom_like_exit() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state(&project, "episode", "1", "running", false)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "run-child",
            "orchestrator",
            "--project",
            project.to_str().expect("temporary path is valid UTF-8"),
            "--role",
            "episode",
            "--agent",
            "1",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_SIGKILL_ONCE", "1"),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_ORCHESTRATOR_OOM_RESTART_DELAY_SECONDS", "1"),
        ],
    )?)?;

    let supervisor = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("orchestrator.toml"),
    )?;
    assert!(supervisor.contains("status = \"idle\""));
    assert!(supervisor.contains("oom_restarts = 1"));
    let log = fs::read_to_string(log)?;
    assert!(
        log.lines()
            .filter(|line| line.starts_with("app-server"))
            .count()
            >= 2
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn queued_triggers_start_agents_with_trigger_context() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    assert_success(run(
        &[
            "role",
            "new",
            "orchestrator",
            "--active",
            "--parallel",
            "infinite",
        ],
        Some(&project),
    )?)?;
    let orchestrator_config = project
        .join("roles")
        .join("orchestrator")
        .join("config.toml");
    fs::write(
        &orchestrator_config,
        format!(
            concat!(
                "{}\n",
                "[[triggers]]\n",
                "kind = \"role-agent-finished\"\n",
                "role = \"episode\"\n",
                "launch = \"queued\"\n",
                "queue = \"orchestrators\"\n",
            ),
            fs::read_to_string(&orchestrator_config)?
        ),
    )?;
    write_agent_state(&project, "episode", "1", "running", false)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "run-child",
            "orchestrator",
            "--project",
            project.to_str().expect("temporary path is valid UTF-8"),
            "--role",
            "episode",
            "--agent",
            "1",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_FAKE_CODEX_TOUCH_EPISODE_WORK", "1"),
        ],
    )?)?;

    let orchestrator_agent = wait_for_only_agent_dir(&project, "orchestrator")?;
    wait_for_path(&orchestrator_agent.join("TRIGGER.md"))?;
    let orchestrator_trigger = fs::read_to_string(orchestrator_agent.join("TRIGGER.md"))?;
    assert!(orchestrator_trigger.contains("trigger kind: role-agent-finished"));
    assert!(orchestrator_trigger.contains("source role: `episode`"));
    assert!(orchestrator_trigger.contains("source agent: `1`"));
    let orchestrator_prompt =
        fs::read_to_string(orchestrator_agent.join("runs").join("1").join("PROMPT.md"))?;
    assert!(orchestrator_prompt.contains("# Trigger Context"));
    assert!(orchestrator_prompt.contains("role-agent-finished"));

    let publisher_agent = wait_for_only_agent_dir(&project, "publisher")?;
    wait_for_path(&publisher_agent.join("TRIGGER.md"))?;
    let publisher_trigger = fs::read_to_string(publisher_agent.join("TRIGGER.md"))?;
    assert!(publisher_trigger.contains("trigger kind: role-agent-finished"));
    assert!(publisher_trigger.contains("source role: `episode`"));
    assert!(publisher_trigger.contains("source agent: `1`"));
    assert!(publisher_agent.join("EXPOSED.md").exists());
    wait_for_path(
        &project
            .join("channels")
            .join("report-single")
            .join("episode-1-1-1.pdf"),
    )?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn invalid_manifest_is_reported_to_agent_and_retried() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "advanced",
            "trigger",
            "publisher",
            "--reason",
            "manifest retry smoke",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_FAKE_CODEX_BAD_MANIFEST_ONCE", "1"),
            ("THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS", "1"),
        ],
    )?)?;

    let publisher_agent = wait_for_only_agent_dir(&project, "publisher")?;
    wait_for_file_contains(&publisher_agent.join("agent.toml"), "run_count = 1")?;
    let agent = fs::read_to_string(publisher_agent.join("agent.toml"))?;
    assert!(agent.contains("status = \"done\""));
    assert!(agent.contains("archived = true"));
    assert!(agent.contains("run_count = 1"));
    let manifest = fs::read_to_string(publisher_agent.join("manifest.toml"))?;
    assert!(manifest.contains("disposition = \"stop\""));
    let supervisor = fs::read_to_string(publisher_agent.join("orchestrator.toml"))?;
    assert!(supervisor.contains("status = \"idle\""));
    assert!(supervisor.contains("repair_retries = 1"));

    let log = fs::read_to_string(log)?;
    assert!(
        log.lines()
            .filter(|line| line.starts_with("app-server"))
            .count()
            >= 2
    );
    assert!(log.contains("manifest.toml"));
    assert!(log.contains("Runtime error"));
    assert!(log.contains("done"));
    assert_success(run(&["status"], Some(&project))?)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn invalid_channel_outbox_is_reported_to_agent_and_retried() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "advanced",
            "trigger",
            "publisher",
            "--reason",
            "channel retry smoke",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_FAKE_CODEX_BAD_CHANNEL_ONCE", "1"),
            ("THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS", "1"),
        ],
    )?)?;

    let publisher_agent = wait_for_only_agent_dir(&project, "publisher")?;
    wait_for_file_contains(
        &publisher_agent.join("orchestrator.toml"),
        "repair_retries = 1",
    )?;
    wait_for_file_contains(
        &publisher_agent.join("orchestrator.toml"),
        "status = \"idle\"",
    )?;
    let supervisor = fs::read_to_string(publisher_agent.join("orchestrator.toml"))?;
    assert!(supervisor.contains("status = \"idle\""));
    assert!(supervisor.contains("repair_retries = 1"));
    wait_for_path(
        &project
            .join("channels")
            .join("report")
            .join("publisher-pub1-1-good.txt"),
    )?;
    let log = fs::read_to_string(log)?;
    assert!(log.contains("channel outbox"));
    assert!(log.contains("Refusing to publish symlink"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn corrupted_agent_state_is_restored_reported_and_retried() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "advanced",
            "trigger",
            "publisher",
            "--reason",
            "agent state retry smoke",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_FAKE_CODEX_BAD_AGENT_TOML_ONCE", "1"),
            ("THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS", "1"),
        ],
    )?)?;

    let publisher_agent = wait_for_only_agent_dir(&project, "publisher")?;
    wait_for_file_contains(&publisher_agent.join("agent.toml"), "run_count = 1")?;
    let agent = fs::read_to_string(publisher_agent.join("agent.toml"))?;
    assert!(agent.contains("status = \"done\""));
    assert!(agent.contains("run_count = 1"));
    let supervisor = fs::read_to_string(publisher_agent.join("orchestrator.toml"))?;
    assert!(supervisor.contains("status = \"idle\""));
    assert!(supervisor.contains("repair_retries = 1"));
    let log = fs::read_to_string(log)?;
    assert!(log.contains("agent state"));
    assert!(log.contains("agent.toml is invalid"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn missing_reply_is_reported_to_agent_and_retried() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "advanced",
            "trigger",
            "publisher",
            "--reason",
            "reply retry smoke",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_FAKE_CODEX_SKIP_REPLY_ONCE", "1"),
            ("THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS", "1"),
        ],
    )?)?;

    let publisher_agent = wait_for_only_agent_dir(&project, "publisher")?;
    wait_for_path(&publisher_agent.join("runs").join("1").join("REPLY.md"))?;
    wait_for_file_contains(
        &publisher_agent.join("orchestrator.toml"),
        "status = \"idle\"",
    )?;
    wait_for_file_contains(
        &publisher_agent.join("orchestrator.toml"),
        "repair_retries = 1",
    )?;
    let supervisor = fs::read_to_string(publisher_agent.join("orchestrator.toml"))?;
    assert!(supervisor.contains("status = \"idle\""));
    assert!(supervisor.contains("repair_retries = 1"));
    let log = fs::read_to_string(log)?;
    assert!(log.contains("reply"));
    assert!(log.contains("run reply file"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn manual_and_idle_triggers_start_prefixed_auto_archived_supervisors() -> Result<(), Box<dyn Error>>
{
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let supervisor_config = project.join("roles").join("supervisor").join("config.toml");
    fs::write(
        &supervisor_config,
        fs::read_to_string(&supervisor_config)?
            .replace("status = \"paused\"", "status = \"active\""),
    )?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &[
            "advanced",
            "trigger",
            "supervisor",
            "--reason",
            "manual smoke review",
        ],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
        ],
    )?)?;

    let manual_agent = project
        .join("roles")
        .join("supervisor")
        .join("agents")
        .join("o1");
    wait_for_path(&manual_agent.join("TRIGGER.md"))?;
    let manual_trigger = fs::read_to_string(manual_agent.join("TRIGGER.md"))?;
    assert!(manual_trigger.contains("trigger kind: manual"));
    assert!(manual_trigger.contains("manual smoke review"));
    wait_for_file_contains(&manual_agent.join("agent.toml"), "archived = true")?;

    fs::write(
        &supervisor_config,
        format!(
            concat!(
                "{}\n",
                "[[triggers]]\n",
                "kind = \"queue-idle\"\n",
                "idle_queue = \"publisher\"\n",
                "idle_seconds = 1\n",
                "launch = \"queued\"\n",
                "queue = \"supervisor\"\n",
            ),
            fs::read_to_string(&supervisor_config)?
        ),
    )?;
    fs::create_dir_all(project.join("runtime").join("queue-runtime"))?;
    fs::write(
        project
            .join("runtime")
            .join("queue-runtime")
            .join("publisher.toml"),
        "empty_since = 1\n",
    )?;
    let status_output = assert_success(run_with_env(
        &["status", "--plain"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
        ],
    )?)?;
    assert!(!String::from_utf8_lossy(&status_output.stdout).contains("starting app-server run"));

    let idle_agent = project
        .join("roles")
        .join("supervisor")
        .join("agents")
        .join("o2");
    wait_for_path(&idle_agent.join("TRIGGER.md"))?;
    let idle_trigger = fs::read_to_string(idle_agent.join("TRIGGER.md"))?;
    assert!(idle_trigger.contains("trigger kind: queue-idle"));
    assert!(idle_trigger.contains("queue: `publisher`"));
    wait_for_file_contains(&idle_agent.join("agent.toml"), "archived = true")?;
    Ok(())
}

#[test]
fn episodes_math_project_has_compact_episode_defaults() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let config = fs::read_to_string(project.join("roles").join("episode").join("config.toml"))?;
    assert!(!project.join("runtime").join("reports").exists());
    assert!(fs::read_to_string(project.join("think.toml"))?.contains("default_role = \"episode\""));
    assert!(config.contains("status = \"active\""));
    assert!(config.contains("parallel = \"infinite\""));
    assert!(config.contains("agent_names = \"sequential\""));
    assert!(config.contains("agent_prefix = \"ep\""));
    let project_config = fs::read_to_string(project.join("think.toml"))?;
    assert!(project_config.contains("channels = ["));
    assert!(project_config.contains("\"alerts\""));
    assert!(project_config.contains("\"report\""));
    assert!(project_config.contains("\"report-single\""));
    let publisher_config =
        fs::read_to_string(project.join("roles").join("publisher").join("config.toml"))?;
    assert!(publisher_config.contains("agent_prefix = \"pub\""));
    assert!(publisher_config.contains("expose = [\"last-agent-finished\"]"));
    assert!(publisher_config.contains("kind = \"role-agent-finished\""));
    assert!(publisher_config.contains("role = \"episode\""));
    assert!(publisher_config.contains("queue = \"publisher\""));
    let supervisor_config =
        fs::read_to_string(project.join("roles").join("supervisor").join("config.toml"))?;
    assert!(supervisor_config.contains("status = \"paused\""));
    assert!(supervisor_config.contains("agent_prefix = \"o\""));
    assert!(supervisor_config.contains("auto_archive = true"));
    assert!(supervisor_config.contains("kind = \"role-agent-finished\""));
    assert!(supervisor_config.contains("role = \"episode\""));
    assert!(supervisor_config.contains("queue = \"supervisor\""));
    let auditor_config =
        fs::read_to_string(project.join("roles").join("auditor").join("config.toml"))?;
    assert!(auditor_config.contains("agent_prefix = \"audit\""));
    assert!(auditor_config.contains("kind = \"queue-idle\""));
    assert!(auditor_config.contains("idle_queue = \"auditor\""));

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("● episode"));
    assert!(stdout.contains("● auditor"));
    assert!(stdout.contains("● publisher"));
    assert!(stdout.contains("● supervisor"));
    assert!(!stdout.contains("project:"));

    let seed = project.join("templates").join("episodes-math");
    assert!(seed.join(".gitignore").exists());
    assert!(
        project
            .join("channels")
            .join("alerts")
            .join(".git")
            .exists()
    );
    assert!(
        project
            .join("channels")
            .join("report")
            .join(".git")
            .exists()
    );
    assert!(
        project
            .join("channels")
            .join("report-single")
            .join(".git")
            .exists()
    );
    let report_tex = fs::read_to_string(seed.join("report.tex"))?;
    let preamble_tex = fs::read_to_string(seed.join("preamble.tex"))?;
    let makefile = fs::read_to_string(seed.join("Makefile"))?;
    assert!(report_tex.contains("\\input{preamble}"));
    assert!(report_tex.contains("\\newcommand{\\daycard}"));
    assert!(preamble_tex.contains("\\usepackage{newpxtext,newpxmath}"));
    assert!(preamble_tex.contains("\\fancyhead[L]"));
    assert!(preamble_tex.contains("\\ThinkVersion"));
    assert!(seed.join("episode-standalone.tex").exists());
    assert!(!seed.join("episode-summary.md").exists());
    assert!(makefile.contains("episode-standalone.tex preamble.tex"));
    assert!(seed.join("episodes").is_dir());
    assert!(seed.join("papers").is_dir());
    assert!(
        !seed
            .join("scripts")
            .join("validate-episode-summaries.py")
            .exists()
    );
    let project_md = fs::read_to_string(project.join("PROJECT.md"))?;
    assert!(project_md.contains("every terminal episode agent with a usable TeX"));
    let publisher_step = fs::read_to_string(
        project
            .join("roles")
            .join("publisher")
            .join("steps")
            .join("publish.md"),
    )?;
    assert!(publisher_step.contains("status is done or stopped"));
    assert!(publisher_step.contains("Do not include sources from agents"));
    assert!(publisher_step.contains("ep5` goes between `ep4` and `ep6"));
    assert!(seed.join("experiments").join("Cargo.toml").exists());
    assert!(
        seed.join("experiments")
            .join("src")
            .join("commands")
            .join("mod.rs")
            .exists()
    );
    Ok(())
}

#[test]
fn episodes_code_project_has_branch_merge_defaults() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-code",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let project_config = fs::read_to_string(project.join("think.toml"))?;
    assert!(!project.join("runtime").join("reports").exists());
    assert!(project_config.contains("template = \"episodes-code\""));
    assert!(project_config.contains("default_role = \"episode\""));
    assert!(project_config.contains("\"alerts\""));
    assert!(project_config.contains("\"branches\""));
    assert!(project_config.contains("\"merges\""));

    let project_md = fs::read_to_string(project.join("PROJECT.md"))?;
    assert!(project_md.contains("worktree"));
    assert!(project_md.contains("repo/"));
    assert!(project_md.contains("master"));
    assert!(project_md.contains("Rust 2024"));
    assert!(project_md.contains("There is no pushing"));
    assert!(project_md.contains("reasonable care"));

    let episode_config =
        fs::read_to_string(project.join("roles").join("episode").join("config.toml"))?;
    assert!(episode_config.contains("status = \"active\""));
    assert!(episode_config.contains("parallel = \"infinite\""));
    assert!(episode_config.contains("agent_prefix = \"ep\""));
    let episode_step = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("steps")
            .join("work.md"),
    )?;
    assert!(episode_step.contains("repo/.git"));
    assert!(episode_step.contains("git -C repo worktree add"));
    assert!(episode_step.contains("episodes/<agent-id>"));
    assert!(episode_step.contains("channels/branches/<agent-id>.md"));
    assert!(!episode_step.contains("<source-repo>"));
    assert!(!episode_step.contains("<task-label>"));

    let merger_config =
        fs::read_to_string(project.join("roles").join("merger").join("config.toml"))?;
    assert!(merger_config.contains("parallel = 1"));
    assert!(merger_config.contains("agent_prefix = \"merge\""));
    assert!(merger_config.contains("expose = [\"last-agent-finished\"]"));
    assert!(merger_config.contains("kind = \"role-agent-finished\""));
    assert!(merger_config.contains("role = \"episode\""));
    assert!(merger_config.contains("queue = \"merger\""));
    assert!(!merger_config.contains("role = \"merge-tranche\""));
    let merger_step = fs::read_to_string(
        project
            .join("roles")
            .join("merger")
            .join("steps")
            .join("merge.md"),
    )?;
    assert!(merger_step.contains("git merge --no-ff --no-commit"));
    assert!(merger_step.contains("git commit"));
    assert!(merger_step.contains("octopus"));
    assert!(merger_step.contains("consolidation branch"));
    assert!(merger_step.contains("Consolidation branches are immutable outputs"));
    assert!(merger_step.contains("If this is a resumed run and `work/own/repo` already exists"));
    assert!(merger_step.contains("result = \"already-integrated\""));
    assert!(merger_step.contains("skipped source branches"));
    assert!(merger_step.contains("request source, requester agent if any"));
    assert!(merger_step.contains("merges/<agent-id>"));
    assert!(merger_step.contains("tranche prompt"));
    assert!(merger_step.contains("reasonable care"));
    assert!(merger_step.contains("channels/merges/<agent-id>.md"));
    assert!(merger_step.contains("number of entries in"));

    let supervisor_config =
        fs::read_to_string(project.join("roles").join("supervisor").join("config.toml"))?;
    assert!(supervisor_config.contains("status = \"paused\""));
    assert!(supervisor_config.contains("agent_prefix = \"sup\""));
    assert!(
        supervisor_config
            .matches("kind = \"role-agent-finished\"")
            .count()
            >= 2
    );
    assert!(supervisor_config.contains("role = \"episode\""));
    assert!(supervisor_config.contains("role = \"merger\""));
    assert!(!supervisor_config.contains("role = \"merge-tranche\""));
    assert!(supervisor_config.contains("queue = \"supervisor\""));
    let supervisor_role =
        fs::read_to_string(project.join("roles").join("supervisor").join("ROLE.md"))?;
    assert!(supervisor_role.contains("maximum safe parallelism"));
    let supervisor_step = fs::read_to_string(
        project
            .join("roles")
            .join("supervisor")
            .join("steps")
            .join("work.md"),
    )?;
    assert!(supervisor_step.contains("think agent new merger --prompt"));
    assert!(supervisor_step.contains("tranche prompt"));
    assert!(supervisor_step.contains("update `master` or leave a new"));
    assert!(supervisor_step.contains("consolidation branches being consumed as inputs"));
    assert!(supervisor_step.contains("reasonable care"));
    assert!(supervisor_step.contains("think role pause supervisor"));
    assert!(supervisor_step.contains("repo/.git"));

    let auditor_config =
        fs::read_to_string(project.join("roles").join("auditor").join("config.toml"))?;
    assert!(auditor_config.contains("kind = \"queue-idle\""));
    assert!(auditor_config.contains("idle_queue = \"auditor\""));

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("● episode"));
    assert!(stdout.contains("● auditor"));
    assert!(stdout.contains("● merger"));
    assert!(stdout.contains("● supervisor"));
    assert!(!stdout.contains("merge-tranche"));

    let seed = project.join("templates").join("episodes-code");
    assert!(seed.join("README.md").exists());
    assert!(seed.join("branch-handoff.md").exists());
    assert!(seed.join("merge-handoff.md").exists());
    assert!(!seed.join("branch-report.md").exists());
    assert!(!seed.join("merge-report.md").exists());
    assert!(!seed.join("tranche-report.md").exists());
    assert!(!seed.join("scripts").join("validate-reports.py").exists());
    let branch_handoff = fs::read_to_string(seed.join("branch-handoff.md"))?;
    assert!(branch_handoff.contains("kind = \"branch\""));
    assert!(branch_handoff.contains("branch = \"episodes/<agent-id>\""));
    assert!(branch_handoff.contains("ready_for_merge = true"));
    let merge_handoff = fs::read_to_string(seed.join("merge-handoff.md"))?;
    assert!(merge_handoff.contains("request_source = \"episode-finished-trigger\""));
    assert!(merge_handoff.contains("requester_agent = \"episode/<requester-agent-id>\""));
    assert!(merge_handoff.contains("source_branches = [\"<branch-name>\"]"));
    assert!(merge_handoff.contains("skipped_branches = []"));
    assert!(merge_handoff.contains("output_branch = \"merges/<agent-id>\""));
    assert!(merge_handoff.contains("target_mode = \"master\""));
    assert!(merge_handoff.contains("master_updated = true"));
    assert!(merge_handoff.contains("master_after = \"<commit>\""));
    assert!(!merge_handoff.contains("trigger_role"));
    assert!(!merge_handoff.contains("trigger_agent"));
    assert!(!merge_handoff.lines().any(|line| line == "mode ="));
    assert!(
        project
            .join("channels")
            .join("branches")
            .join(".git")
            .exists()
    );
    assert!(
        project
            .join("channels")
            .join("merges")
            .join(".git")
            .exists()
    );
    Ok(())
}

#[test]
fn project_new_registers_project_in_think_home() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    let think_home = temp.path().join("think-home");
    assert_success(run_with_env(
        &[
            "project",
            "new",
            "--no-template",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
        &[(
            "THINK_HOME",
            think_home.to_str().expect("temporary path is valid UTF-8"),
        )],
    )?)?;

    let registry = fs::read_to_string(think_home.join("projects.toml"))?;
    assert!(registry.contains("version = 1"));
    assert!(
        registry.contains(
            project
                .canonicalize()?
                .to_str()
                .expect("path is valid UTF-8")
        )
    );
    assert!(registry.contains("last_used"));
    Ok(())
}

#[test]
fn status_all_includes_archived_agents_with_summary_before_status() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let agent = project
        .join("roles")
        .join("episode")
        .join("agents")
        .join("1");
    fs::create_dir_all(&agent)?;
    fs::write(
        agent.join("agent.toml"),
        r#"version = 1
role = "episode"
agent = "1"
backend = "codex"
mode = "repeatable"
status = "done"
archived = true
current_step = 0
run_count = 1
pane_id = "tab-name:episode/1"
channels = ["report", "report-single"]
created_at = 1
updated_at = 2

[last_exit]
run_id = 1
step = "work"
started_at = 1
finished_at = 2
success = true
code = 0
disposition = "stop"
"#,
    )?;
    fs::write(
        agent.join("manifest.toml"),
        "role_summary = \"tab smoke\"\n",
    )?;
    let agent = project
        .join("roles")
        .join("episode")
        .join("agents")
        .join("2");
    fs::create_dir_all(&agent)?;
    fs::write(
        agent.join("agent.toml"),
        r#"version = 1
role = "episode"
agent = "2"
backend = "codex"
mode = "repeatable"
status = "stopped"
archived = true
current_step = 0
run_count = 0
channels = ["report", "report-single"]
created_at = 1
updated_at = 2
"#,
    )?;
    fs::write(
        agent.join("manifest.toml"),
        "role_summary = \"longer visible summary\"\n",
    )?;

    let output = assert_success(run(&["status"], Some(&project))?)?;
    assert!(!String::from_utf8(output.stdout)?.contains("tab smoke"));

    let output = assert_success(run(&["status", "--all"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("● episode"));
    assert!(stdout.contains("● auditor"));
    assert!(stdout.contains("● publisher"));
    assert!(stdout.contains("● supervisor"));
    assert!(stdout.contains("● 1 · tab smoke"));
    assert!(stdout.contains("● 2 · longer visible summary"));
    let summary = stdout.find("tab smoke").expect("summary is shown");
    let done = stdout.find("done").expect("status is shown");
    assert!(summary < done);
    assert!(stdout.contains(" · done"));
    assert!(stdout.contains(" · archived · step: work · disposition: stop"));
    assert!(!stdout.contains("tab smoke -"));
    Ok(())
}

#[test]
fn status_uses_manifest_disposition_for_resumed_agents() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    write_agent_state(&project, "episode", "1", "done", false)?;
    fs::write(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("manifest.toml"),
        "role_summary = \"resumed summary\"\ndisposition = \"stop\"\n",
    )?;

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("resumed summary"));
    assert!(stdout.contains("disposition: stop"));
    Ok(())
}

#[test]
fn agent_selectors_accept_role_qualified_form_and_reject_ambiguous_bare_ids()
-> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    assert_success(run(
        &["role", "new", "other", "--active", "--parallel", "infinite"],
        Some(&project),
    )?)?;
    write_agent_state(&project, "episode", "1", "done", false)?;
    write_agent_state(&project, "other", "1", "done", false)?;

    let output = run(&["agent", "stop", "1"], Some(&project))?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("ambiguous"));
    assert!(stderr.contains("episode/1"));
    assert!(stderr.contains("other/1"));

    assert_success(run(&["agent", "stop", "episode/1"], Some(&project))?)?;
    let stopped = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("agent.toml"),
    )?;
    assert!(stopped.contains("status = \"stopped\""));
    Ok(())
}

#[cfg(unix)]
#[test]
fn more_resumes_the_recorded_backend_thread_id() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state_with_run_count(&project, "episode", "1", "done", false, 1)?;
    let run = project
        .join("roles")
        .join("episode")
        .join("agents")
        .join("1")
        .join("runs")
        .join("1");
    fs::create_dir_all(&run)?;
    fs::write(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("backend-thread.toml"),
        "version = 1\nthread_id = \"thread-recorded\"\nupdated_at = 1\n",
    )?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    assert_success(run_with_env(
        &["more", "episode/1", "--query", "continue this"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
        ],
    )?)?;
    let log = fs::read_to_string(log)?;
    assert!(log.contains("thread/resume"));
    assert!(log.contains("thread-recorded"));
    assert!(log.contains("Read runs/2/PROMPT.md"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn more_starts_thread_when_app_server_thread_metadata_is_unavailable() -> Result<(), Box<dyn Error>>
{
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;
    write_agent_state(&project, "episode", "1", "done", false)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let log = temp.path().join("codex.log");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let think_home = temp.path().join("think-home").display().to_string();
    let log_path = log.display().to_string();
    let output = assert_success(run_with_env(
        &["more", "episode/1", "--query", "continue without session"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
        ],
    )?)?;
    assert!(
        String::from_utf8(output.stdout)?.contains("continuing agent `episode/1` with app-server")
    );
    let log = fs::read_to_string(log)?;
    assert!(log.contains("thread/start"));
    assert!(!log.contains("thread/resume"));

    let state = fs::read_to_string(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("1")
            .join("agent.toml"),
    )?;
    assert!(state.contains("status = \"done\""));
    assert!(state.contains("run_count = 1"));
    Ok(())
}

#[test]
fn blank_agent_prompt_cancels_create() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let output = assert_success(run(&["agent", "new", "--prompt", ""], Some(&project))?)?;
    assert!(String::from_utf8(output.stdout)?.contains("agent creation cancelled"));
    assert!(
        fs::read_dir(project.join("roles").join("episode").join("agents"))?
            .next()
            .is_none()
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn bare_think_shows_status_and_more_new_creates_default_role_agent() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "episodes-math",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let fake_bin = temp.path().join("bin");
    install_fake_codex(&fake_bin)?;
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = assert_success(run_with_env(
        &[],
        Some(&project),
        &[("PATH", path.as_str())],
    )?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("roles"));
    assert!(stdout.contains("episode"));
    assert!(
        fs::read_dir(project.join("roles").join("episode").join("agents"))?
            .next()
            .is_none()
    );
    assert_success(run_with_env(
        &["more", "--new", "--query", "explore the first special case"],
        Some(&project),
        &[("PATH", path.as_str())],
    )?)?;

    assert!(
        project
            .join("roles")
            .join("episode")
            .join("agents")
            .join("ep1")
            .exists()
    );
    assert_eq!(
        fs::read_to_string(
            project
                .join("roles")
                .join("episode")
                .join("agents")
                .join("ep1")
                .join("AGENT_PROMPT.md"),
        )?,
        "explore the first special case"
    );
    assert!(
        project
            .join("runtime")
            .join("sessions")
            .join("episode")
            .join("ep1")
            .join("session.toml")
            .exists()
    );
    Ok(())
}
