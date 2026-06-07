use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::io;

const LOCK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct FileLock {
    path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LockBusy {
    label: String,
    path: PathBuf,
}

impl Display for LockBusy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is already locked by another process: {}",
            self.label,
            self.path.display()
        )
    }
}

impl Error for LockBusy {}

impl FileLock {
    pub fn acquire(path: PathBuf, label: &str) -> Result<Self> {
        let Some(lock) = Self::try_acquire(path.clone(), label)? else {
            return Err(LockBusy {
                label: label.to_owned(),
                path,
            }
            .into());
        };
        Ok(lock)
    }

    pub fn acquire_wait(path: PathBuf, label: &str, timeout: Duration) -> Result<Self> {
        let started = Instant::now();
        loop {
            if let Some(lock) = Self::try_acquire(path.clone(), label)? {
                return Ok(lock);
            }
            if started.elapsed() >= timeout {
                return Err(LockBusy {
                    label: label.to_owned(),
                    path,
                }
                .into());
            }
            thread::sleep(LOCK_WAIT_POLL_INTERVAL);
        }
    }

    pub fn try_acquire(path: PathBuf, label: &str) -> Result<Option<Self>> {
        if let Some(parent) = path.parent() {
            io::ensure_dir(parent)?;
        }
        match create_pid_file(&path) {
            Ok(()) => Ok(Some(Self { path })),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                remove_stale_lock(&path)?;
                match create_pid_file(&path) {
                    Ok(()) => Ok(Some(Self { path })),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
                    Err(err) => Err(err)
                        .with_context(|| format!("Failed to create {label} `{}`", path.display())),
                }
            }
            Err(err) => {
                Err(err).with_context(|| format!("Failed to create {label} `{}`", path.display()))
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn claim_once(path: &Path, label: &str) -> Result<bool> {
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    match create_pid_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => {
            Err(err).with_context(|| format!("Failed to create {label} `{}`", path.display()))
        }
    }
}

pub fn is_active(path: &Path) -> Result<bool> {
    if path.exists() {
        remove_stale_lock(path)?;
    }
    Ok(path.exists())
}

fn create_pid_file(path: &Path) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    writeln!(file, "pid = {}", std::process::id())
}

fn remove_stale_lock(path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let contents = io::read_optional_text(path)?.unwrap_or_default();
        let Some(pid) = contents
            .lines()
            .find_map(|line| line.strip_prefix("pid = "))
            .and_then(|pid| pid.trim().parse::<u32>().ok())
        else {
            return Ok(());
        };
        if !Path::new("/proc").join(pid.to_string()).exists() {
            fs::remove_file(path)
                .with_context(|| format!("Failed to remove stale lock `{}`", path.display()))?;
        }
    }
    Ok(())
}
