use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::io;

pub trait UpdateTask {
    fn key(&self) -> &'static str;
    fn label(&self) -> &'static str;
    fn interval(&self) -> Duration;
    fn timeout(&self) -> Duration;
    fn command(&self) -> Command;
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ThinkState {
    #[serde(default)]
    update_checks: BTreeMap<String, UpdateCheckState>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct UpdateCheckState {
    last_check: Option<u64>,
    last_result: Option<String>,
}

pub fn ensure_update_checked(task: &impl UpdateTask) -> Result<()> {
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
        .update_checks
        .get(task.key())
        .and_then(|state| state.last_check)
        .is_some_and(|last| Duration::from_secs(now.saturating_sub(last)) < task.interval())
    {
        return Ok(());
    }

    let Some(_lock) = crate::lock::FileLock::try_acquire(
        home.join(format!("{}-update.lock", task.key())),
        task.label(),
    )?
    else {
        return Ok(());
    };
    let mut state = if state_path.exists() {
        io::read_toml(&state_path)?
    } else {
        ThinkState::default()
    };
    if state
        .update_checks
        .get(task.key())
        .and_then(|state| state.last_check)
        .is_some_and(|last| Duration::from_secs(now.saturating_sub(last)) < task.interval())
    {
        return Ok(());
    }

    let result = run_update(task);
    let check = state
        .update_checks
        .entry(task.key().to_owned())
        .or_default();
    check.last_check = Some(unix_timestamp()?);
    check.last_result = Some(match &result {
        Ok(()) => "ok".to_owned(),
        Err(err) => format!("error: {err:#}"),
    });
    io::write_toml(&state_path, &state)?;
    result
}

fn run_update(task: &impl UpdateTask) -> Result<()> {
    let mut command = task.command();
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("Failed to start `{}`", task.label()))?;
    let started = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("Failed to wait for `{}`", task.label()))?
        {
            if status.success() {
                return Ok(());
            }
            bail!("`{}` exited with status {status}", task.label());
        }
        if started.elapsed() >= task.timeout() {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "`{}` did not finish within {} seconds",
                task.label(),
                task.timeout().as_secs()
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
