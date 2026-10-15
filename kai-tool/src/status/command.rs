//! Bounded desktop helper execution. Helpers never inherit the dashboard's terminal.
use super::worker::Cancellation;
use anyhow::{Context, Result, ensure};
use std::{
    io::{self, Read},
    os::{fd::AsRawFd, unix::process::CommandExt},
    process::{Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

struct ChildGuard<'a>(&'a Cancellation);
impl Drop for ChildGuard<'_> {
    fn drop(&mut self) {
        self.0.stop_helper();
    }
}

pub(super) struct Output {
    pub stdout: Vec<u8>,
    pub status: ExitStatus,
}

pub(super) fn run(
    program: &str,
    args: &[&str],
    timeout: Duration,
    cancel: &Cancellation,
) -> Result<Vec<u8>> {
    let output = capture(program, args, timeout, cancel)?;
    ensure!(
        output.status.success(),
        "{program} could not complete the request ({})",
        output.status
    );
    Ok(output.stdout)
}

// lsof can report a failed PID alongside valid records for other PIDs. Its caller
// needs the exit status AND the bounded output; focus helpers still require success.
pub(super) fn capture(
    program: &str,
    args: &[&str],
    timeout: Duration,
    cancel: &Cancellation,
) -> Result<Output> {
    cancel.check()?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .with_context(|| format!("Cannot start {program}; check that it is installed."))?;
    let stdout = child.stdout.take();
    cancel.track_helper(child)?;
    let _child = ChildGuard(cancel);
    let mut stdout = stdout.context("Missing helper output")?;
    // SAFETY: stdout owns the fd for this scope; only its nonblocking flag is changed.
    let flags = unsafe { libc::fcntl(stdout.as_raw_fd(), libc::F_GETFL) };
    ensure!(
        flags >= 0
            && unsafe { libc::fcntl(stdout.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                >= 0,
        "Cannot configure helper output"
    );
    let deadline = Instant::now() + timeout;
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        cancel.check()?;
        ensure!(Instant::now() < deadline, "{program} timed out; try again.");
        match stdout.read(&mut buffer) {
            Ok(count) if count > 0 => {
                ensure!(
                    output.len() + count <= 8 * 1024 * 1024,
                    "{program} returned too much output"
                );
                output.extend_from_slice(&buffer[..count]);
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() != io::ErrorKind::WouldBlock => return Err(error.into()),
            _ => (),
        }
        if let Some(status) = cancel.helper_status()? {
            // Once reaped, drain bytes that arrived between the read and try_wait.
            loop {
                match stdout.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => {
                        ensure!(
                            output.len() + count <= 8 * 1024 * 1024,
                            "{program} returned too much output"
                        );
                        output.extend_from_slice(&buffer[..count]);
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(Output {
                stdout: output,
                status,
            });
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_output_reports_failures_and_reaps_timed_out_helpers() {
        let cancel = Cancellation::default();
        assert_eq!(
            run(
                "sh",
                &["-c", "printf focused"],
                Duration::from_secs(1),
                &cancel
            )
            .unwrap(),
            b"focused"
        );
        assert!(run("sh", &["-c", "exit 1"], Duration::from_secs(1), &cancel).is_err());
        let partial = capture(
            "sh",
            &["-c", "printf partial; exit 1"],
            Duration::from_secs(1),
            &cancel,
        )
        .unwrap();
        assert_eq!(partial.stdout, b"partial");
        assert_eq!(partial.status.code(), Some(1));
        let start = Instant::now();
        assert!(
            run(
                "sh",
                &["-c", "sleep 10"],
                Duration::from_millis(50),
                &cancel
            )
            .unwrap_err()
            .to_string()
            .contains("timed out")
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cancellation_reaps_a_running_helper_and_prevents_further_commands() {
        use std::{fs, thread};
        let root = tempfile::tempdir().unwrap();
        let marker = root.path().join("pid");
        let cancel = Cancellation::default();
        let worker_cancel = cancel.clone();
        let path = marker.clone();
        let worker = thread::spawn(move || {
            run(
                "sh",
                &[
                    "-c",
                    "echo $$ > \"$1\"; sleep 30",
                    "sh",
                    path.to_str().unwrap(),
                ],
                Duration::from_secs(60),
                &worker_cancel,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !marker.exists() || fs::read_to_string(&marker).unwrap().trim().is_empty() {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let pid = fs::read_to_string(&marker)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        let start = Instant::now();
        cancel.cancel();
        assert!(worker.join().unwrap().is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        // SAFETY: signal 0 only checks whether our reaped helper still exists.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert!(run("sh", &["-c", "exit 0"], Duration::from_secs(1), &cancel).is_err());
    }
}
