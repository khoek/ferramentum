use std::fs::OpenOptions;
use std::io::{self, IsTerminal, Read, Write};
use std::path::Path;
use std::thread;

use anyhow::{Context, Result};
use crossterm::terminal;
use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};
use serde::{Deserialize, Serialize};

use crate::agent::CommandSpec;
use crate::io as file_io;
use crate::state::RunPaths;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PtyExit {
    pub success: bool,
    pub code: u32,
    pub signal: Option<String>,
    pub pid: Option<u32>,
}

pub fn run_command_with_spawn_callback(
    spec: CommandSpec,
    paths: &RunPaths,
    on_spawn: impl FnOnce(Option<u32>) -> Result<()>,
) -> Result<PtyExit> {
    run_command_in_dir_with_stdin(spec, &paths.root(), true, true, on_spawn)
}

pub fn run_command_no_stdin(spec: CommandSpec, paths: &RunPaths) -> Result<PtyExit> {
    run_command_in_dir_with_stdin(spec, &paths.root(), false, true, |_| Ok(()))
}

pub fn run_command_in_dir_no_stdin_quiet(spec: CommandSpec, root: &Path) -> Result<PtyExit> {
    run_command_in_dir_with_stdin(spec, root, false, false, |_| Ok(()))
}

fn run_command_in_dir_with_stdin(
    spec: CommandSpec,
    root: &Path,
    forward_stdin: bool,
    echo_output: bool,
    on_spawn: impl FnOnce(Option<u32>) -> Result<()>,
) -> Result<PtyExit> {
    file_io::ensure_dir(root)?;
    let (cols, rows) = terminal::size().unwrap_or((120, 40));
    let pty_system = NativePtySystem::default();
    let pair = pty_system
        .openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("Failed to open PTY")?;

    let mut command = CommandBuilder::new(&spec.program);
    command.args(&spec.args);
    command.cwd(spec.cwd.as_os_str());
    for (key, value) in &spec.env {
        command.env(key, value);
    }

    let mut child = pair
        .slave
        .spawn_command(command)
        .with_context(|| format!("Failed to spawn `{}`", spec.program))?;
    let child_pid = child.process_id();
    bias_child_oom_score(child_pid);
    on_spawn(child_pid)?;
    drop(pair.slave);

    let mut reader = pair
        .master
        .try_clone_reader()
        .context("Failed to clone PTY reader")?;
    let writer = pair
        .master
        .take_writer()
        .context("Failed to take PTY writer")?;
    let raw_path = root.join("TRANSCRIPT.raw");
    let text_path = root.join("TRANSCRIPT.txt");
    let output_thread =
        thread::spawn(move || copy_output(&mut reader, &raw_path, &text_path, echo_output));

    let _raw_mode = if forward_stdin {
        RawModeGuard::enable_if_terminal()?
    } else {
        RawModeGuard { enabled: false }
    };
    if forward_stdin {
        thread::spawn(move || copy_input(writer));
    }

    let status = child.wait().context("Failed to wait for child process")?;
    output_thread
        .join()
        .unwrap_or_else(|_| Err(anyhow::anyhow!("PTY output thread panicked")))?;
    Ok(PtyExit {
        success: status.success(),
        code: status.exit_code(),
        signal: status.signal().map(str::to_owned),
        pid: child_pid,
    })
}

fn bias_child_oom_score(pid: Option<u32>) {
    #[cfg(target_os = "linux")]
    if let Some(pid) = pid
        && let Err(err) = std::fs::write(format!("/proc/{pid}/oom_score_adj"), b"500\n")
    {
        eprintln!("warning: failed to bias child OOM score for pid {pid}: {err}");
    }

    #[cfg(not(target_os = "linux"))]
    let _ = pid;
}

fn copy_output(
    reader: &mut Box<dyn Read + Send>,
    raw_path: &std::path::Path,
    text_path: &std::path::Path,
    echo_output: bool,
) -> Result<()> {
    let mut raw = OpenOptions::new()
        .create(true)
        .append(true)
        .open(raw_path)
        .with_context(|| format!("Failed to open `{}`", raw_path.display()))?;
    let mut text = OpenOptions::new()
        .create(true)
        .append(true)
        .open(text_path)
        .with_context(|| format!("Failed to open `{}`", text_path.display()))?;
    let mut stdout = echo_output.then(|| io::stdout().lock());
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let bytes = &buffer[..count];
                raw.write_all(bytes)?;
                text.write_all(String::from_utf8_lossy(bytes).as_bytes())?;
                if let Some(stdout) = &mut stdout {
                    stdout.write_all(bytes)?;
                    stdout.flush()?;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("Failed to read PTY output"),
        }
    }
    Ok(())
}

fn copy_input(mut writer: Box<dyn Write + Send>) {
    let mut stdin = io::stdin();
    let mut buffer = [0u8; 8192];
    loop {
        match stdin.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if writer.write_all(&buffer[..count]).is_err() {
                    break;
                }
                let _ = writer.flush();
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}

struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    fn enable_if_terminal() -> Result<Self> {
        if io::stdin().is_terminal() && io::stdout().is_terminal() {
            terminal::enable_raw_mode().context("Failed to enable raw terminal mode")?;
            Ok(Self { enabled: true })
        } else {
            Ok(Self { enabled: false })
        }
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = terminal::disable_raw_mode();
        }
    }
}
