use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::io;

const CODEX_UPDATE_INTERVAL_SECONDS: u64 = 24 * 60 * 60;
const CODEX_UPDATE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Default, Serialize, Deserialize)]
struct ThinkState {
    #[serde(default)]
    codex: CodexState,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CodexState {
    last_update_check: Option<u64>,
    last_update_result: Option<String>,
}

pub fn ensure_codex_update_checked() -> Result<()> {
    let home = think_home()?;
    io::ensure_dir(&home)?;
    let state_path = home.join("state.toml");
    let state = if state_path.exists() {
        io::read_toml(&state_path)?
    } else {
        ThinkState::default()
    };
    let now = unix_timestamp()?;
    if state
        .codex
        .last_update_check
        .is_some_and(|last| now.saturating_sub(last) < CODEX_UPDATE_INTERVAL_SECONDS)
    {
        return Ok(());
    }

    let Some(_lock) =
        crate::lock::FileLock::try_acquire(home.join("codex-update.lock"), "codex update lock")?
    else {
        return Ok(());
    };
    let mut state = if state_path.exists() {
        io::read_toml(&state_path)?
    } else {
        ThinkState::default()
    };
    if state
        .codex
        .last_update_check
        .is_some_and(|last| now.saturating_sub(last) < CODEX_UPDATE_INTERVAL_SECONDS)
    {
        return Ok(());
    }

    let result = run_codex_update();
    state.codex.last_update_check = Some(unix_timestamp()?);
    state.codex.last_update_result = Some(match &result {
        Ok(()) => "ok".to_owned(),
        Err(err) => format!("error: {err:#}"),
    });
    io::write_toml(&state_path, &state)?;
    result
}

fn run_codex_update() -> Result<()> {
    let mut child = Command::new("codex")
        .arg("update")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("Failed to start `codex update`")?;
    let started = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .context("Failed to wait for `codex update`")?
        {
            if status.success() {
                return Ok(());
            }
            bail!("`codex update` exited with status {status}");
        }
        if started.elapsed() >= CODEX_UPDATE_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "`codex update` did not finish within {} seconds",
                CODEX_UPDATE_TIMEOUT.as_secs()
            );
        }
        thread::sleep(Duration::from_millis(250));
    }
}

pub fn think_home() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("THINK_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("HOME is not set; cannot locate ~/.think")?)
            .join(".think"),
    )
}

fn unix_timestamp() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before Unix epoch")?
        .as_secs())
}
