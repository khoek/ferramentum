//! Platform process inspection. Birth tokens distinguish reused PIDs.
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) struct ProcessIdentity {
    pub pid: u32,
    // Linux start ticks; on macOS, process start time in microseconds.
    pub start_ticks: u64,
}

#[derive(Clone)]
pub(super) struct Window {
    pub identity: ProcessIdentity,
    pub tty: Option<String>,
    pub cwd: Option<PathBuf>,
    pub transcripts: Vec<PathBuf>,
    pub warning: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Ancestor {
    pub identity: ProcessIdentity,
    pub name: String,
}

#[cfg(target_os = "linux")]
pub(super) fn discover(
    observer_pid: u32,
    cancel: &super::worker::Cancellation,
) -> anyhow::Result<(Vec<Window>, Vec<String>)> {
    super::process_linux::discover(std::path::Path::new("/proc"), observer_pid, cancel)
}
#[cfg(target_os = "linux")]
pub(super) fn same_process(identity: ProcessIdentity) -> std::io::Result<bool> {
    super::process_linux::same_process(std::path::Path::new("/proc"), identity)
}
#[cfg(target_os = "macos")]
pub(super) use super::process_macos::{discover, same_process};

pub(super) fn is_launch(command: &[u8]) -> bool {
    let mut args = command
        .split(|byte| *byte == 0)
        .skip(1)
        .filter(|arg| !arg.is_empty());
    while let Some(arg) = args.next() {
        if arg == b"--credential-provider" {
            args.next();
            continue;
        }
        if arg.starts_with(b"--credential-provider=")
            || matches!(arg, b"--fast" | b"--no-auto-restart")
        {
            continue;
        }
        if arg.starts_with(b"-") {
            return false;
        }
        return b"resume".starts_with(arg);
    }
    true
}

pub(super) fn is_rollout(path: &std::path::Path) -> bool {
    path.is_absolute()
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
}
