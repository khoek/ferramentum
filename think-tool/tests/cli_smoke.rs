use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

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
printf '%s\n' "$*" >> "$THINK_FAKE_CODEX_LOG"
reply_path=""
expect_reply_path=0
for arg in "$@"; do
  if [ "$expect_reply_path" = "1" ]; then
    reply_path="$arg"
    expect_reply_path=0
    continue
  fi
  if [ "$arg" = "-o" ] || [ "$arg" = "--output-last-message" ]; then
    expect_reply_path=1
  fi
done
write_reply() {
  if [ -n "$reply_path" ]; then
    mkdir -p "$(dirname "$reply_path")"
    printf 'fake final reply\n' > "$reply_path"
  fi
}
if [ "$1" = "update" ]; then
  exit 0
fi
if [ "$THINK_FAKE_CODEX_QUOTA_ONCE" = "1" ]; then
  marker="$THINK_FAKE_CODEX_LOG.quota-once"
  if [ ! -e "$marker" ]; then
    : > "$marker"
    printf 'session id: 019e7b82-2aad-7540-a6da-f8fc17d5977e\n'
    printf 'quota exceeded: rate limit reached\n' >&2
    exit 1
  fi
fi
if [ "$THINK_FAKE_CODEX_SIGKILL_ONCE" = "1" ]; then
  marker="$THINK_FAKE_CODEX_LOG.sigkill-once"
  if [ ! -e "$marker" ]; then
    : > "$marker"
    printf 'session id: 019e7b82-2aad-7540-a6da-f8fc17d5977f\n'
    exit 137
  fi
fi
if [ "$1" = "exec" ] && [ "$2" = "resume" ]; then
  if [ "$THINK_FAKE_CODEX_RESUME" = "missing" ]; then
    printf 'No sessions found for this workspace\n' >&2
    exit 1
  fi
  if [ "$THINK_FAKE_CODEX_WRITE_MANIFEST" = "1" ]; then
    case "$PWD" in
      */roles/*/agents/*)
        printf 'role_summary = "fake supervisor"\ndisposition = "stop"\n' > manifest.toml
        ;;
    esac
  fi
  if [ "$THINK_FAKE_CODEX_TOUCH_EPISODE_WORK" = "1" ]; then
    case "$PWD" in
      */roles/episode/agents/*)
        agent_id="${PWD##*/}"
        mkdir -p work/own/episodes channels/report-single
        printf '\\episodeprojecttitle{Smoke}\\section{Smoke %s}\\n' "$agent_id" > "work/own/episodes/$agent_id.tex"
        printf 'fake pdf\n' > "channels/report-single/$agent_id.pdf"
        ;;
    esac
  fi
  write_reply
  printf 'session id: 019e7b82-2aad-7540-a6da-f8fc17d5977c\n'
  exit 0
fi
if [ "$1" = "exec" ]; then
  if [ "$THINK_FAKE_CODEX_WRITE_MANIFEST" = "1" ]; then
    case "$PWD" in
      */roles/*/agents/*)
        printf 'role_summary = "fake supervisor"\ndisposition = "stop"\n' > manifest.toml
        ;;
    esac
  fi
  if [ "$THINK_FAKE_CODEX_TOUCH_EPISODE_WORK" = "1" ]; then
    case "$PWD" in
      */roles/episode/agents/*)
        agent_id="${PWD##*/}"
        mkdir -p work/own/episodes channels/report-single
        printf '\\episodeprojecttitle{Smoke}\\section{Smoke %s}\\n' "$agent_id" > "work/own/episodes/$agent_id.tex"
        printf 'fake pdf\n' > "channels/report-single/$agent_id.pdf"
        ;;
      esac
  fi
  write_reply
  printf 'session id: 019e7b82-2aad-7540-a6da-f8fc17d5977d\n'
  exit 0
fi
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

#[test]
fn top_level_help_hides_advanced_namespaces_until_all() -> Result<(), Box<dyn Error>> {
    let output = assert_success(run(&["--help"], None)?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("more"));
    assert!(stdout.contains("status"));
    assert!(stdout.contains("open"));
    assert!(stdout.contains("fix"));
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
    assert!(stdout.contains("  list"));
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
            "math-episodes",
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
            "math-episodes",
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

#[cfg(unix)]
#[test]
fn fix_prompt_explains_best_effort_and_uses_tight_think_context() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "math-episodes",
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
        &["fix", "repair the report"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
        ],
    )?)?;

    let prompt = newest_command_prompt(&project, "fix")?;
    assert!(prompt.contains("# think fix"));
    assert!(prompt.contains("Make a best-effort attempt"));
    assert!(prompt.contains("ask a concise clarification question"));
    assert!(prompt.contains("think-tool source"));
    assert!(prompt.contains("# Current think project"));
    assert!(!prompt.contains("# ferramentum"));
    assert!(!prompt.contains("A repository housing various useful Rust CLI utilities."));
    Ok(())
}

#[cfg(unix)]
#[test]
fn supervisor_waits_and_resumes_after_quota_backoff() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "math-episodes",
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
            ("THINK_FAKE_CODEX_QUOTA_ONCE", "1"),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
            ("THINK_ORCHESTRATOR_QUOTA_RETRY_SECONDS", "1"),
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
    assert!(supervisor.contains("quota_retries = 1"));
    let log = fs::read_to_string(log)?;
    assert!(log.contains("exec --cd"));
    assert!(log.contains("exec resume"));

    let output = assert_success(run(&["status"], Some(&project))?)?;
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("fake supervisor"));
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
            "math-episodes",
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
            "math-episodes",
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
    assert!(log.lines().filter(|line| line.starts_with("exec")).count() >= 2);
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
            "math-episodes",
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
            "{}\n[[triggers]]\nkind = \"role-agent-finished\"\nrole = \"episode\"\nlaunch = \"queued\"\nqueue = \"orchestrators\"\n",
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

    let orchestrator_agent = only_agent_dir(&project, "orchestrator")?;
    let orchestrator_trigger = fs::read_to_string(orchestrator_agent.join("TRIGGER.md"))?;
    assert!(orchestrator_trigger.contains("trigger kind: role-agent-finished"));
    assert!(orchestrator_trigger.contains("source role: `episode`"));
    assert!(orchestrator_trigger.contains("source agent: `1`"));
    let orchestrator_prompt =
        fs::read_to_string(orchestrator_agent.join("runs").join("1").join("PROMPT.md"))?;
    assert!(orchestrator_prompt.contains("# Trigger Context"));
    assert!(orchestrator_prompt.contains("role-agent-finished"));

    let publisher_agent = only_agent_dir(&project, "publisher")?;
    let publisher_trigger = fs::read_to_string(publisher_agent.join("TRIGGER.md"))?;
    assert!(publisher_trigger.contains("trigger kind: role-agent-finished"));
    assert!(publisher_trigger.contains("source role: `episode`"));
    assert!(publisher_trigger.contains("source agent: `1`"));
    assert!(publisher_agent.join("EXPOSED.md").exists());
    assert!(
        project
            .join("channels")
            .join("report-single")
            .join("episode-1-1-1.pdf")
            .exists()
    );
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
            "math-episodes",
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
    assert!(manual_agent.exists());
    let manual_trigger = fs::read_to_string(manual_agent.join("TRIGGER.md"))?;
    assert!(manual_trigger.contains("trigger kind: manual"));
    assert!(manual_trigger.contains("manual smoke review"));
    assert!(fs::read_to_string(manual_agent.join("agent.toml"))?.contains("archived = true"));

    fs::write(
        &supervisor_config,
        format!(
            "{}\n[[triggers]]\nkind = \"queue-idle\"\nidle_queue = \"publisher\"\nidle_seconds = 1\nlaunch = \"queued\"\nqueue = \"supervisor\"\n",
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
    assert_success(run_with_env(
        &["list"],
        Some(&project),
        &[
            ("PATH", path.as_str()),
            ("THINK_HOME", think_home.as_str()),
            ("THINK_FAKE_CODEX_LOG", log_path.as_str()),
            ("THINK_FAKE_CODEX_WRITE_MANIFEST", "1"),
        ],
    )?)?;

    let idle_agent = project
        .join("roles")
        .join("supervisor")
        .join("agents")
        .join("o2");
    assert!(idle_agent.exists());
    let idle_trigger = fs::read_to_string(idle_agent.join("TRIGGER.md"))?;
    assert!(idle_trigger.contains("trigger kind: queue-idle"));
    assert!(idle_trigger.contains("queue: `publisher`"));
    assert!(fs::read_to_string(idle_agent.join("agent.toml"))?.contains("archived = true"));
    Ok(())
}

#[test]
fn math_episodes_project_has_compact_episode_defaults() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "math-episodes",
            project.to_str().expect("temporary path is valid UTF-8"),
        ],
        None,
    )?)?;

    let config = fs::read_to_string(project.join("roles").join("episode").join("config.toml"))?;
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

    let seed = project.join("templates").join("math-episodes");
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
    assert!(makefile.contains("episode-standalone.tex preamble.tex"));
    assert!(seed.join("episodes").is_dir());
    assert!(seed.join("papers").is_dir());
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
            "math-episodes",
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
            "math-episodes",
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
            "math-episodes",
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
fn more_resumes_the_recorded_codex_session_id() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "math-episodes",
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
        run.join("TRANSCRIPT.txt"),
        "session id: 019e7b82-2aad-7540-a6da-f8fc17d5977c\n",
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
    assert!(log.contains("exec resume"));
    assert!(log.contains("019e7b82-2aad-7540-a6da-f8fc17d5977c"));
    assert!(!log.contains("--last"));
    assert!(log.contains("Read runs/2/PROMPT.md"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn more_falls_back_to_fresh_exec_when_resume_metadata_is_unavailable() -> Result<(), Box<dyn Error>>
{
    let temp = tempdir()?;
    let project = temp.path().join("project");
    assert_success(run(
        &[
            "project",
            "new",
            "--template",
            "math-episodes",
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
            ("THINK_FAKE_CODEX_RESUME", "missing"),
        ],
    )?)?;
    assert!(String::from_utf8(output.stdout)?.contains("starting a fresh Codex exec"));
    let log = fs::read_to_string(log)?;
    assert!(log.contains("exec resume"));
    assert!(log.contains("--last"));
    assert!(log.contains("exec --cd"));

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
            "math-episodes",
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
            "math-episodes",
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
