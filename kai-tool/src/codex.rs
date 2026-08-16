use std::ffi::OsString;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use semver::Version;

use super::GIT_TERMINAL_PROMPT_ENV;

pub(crate) const APPROVAL_BYPASS_FLAG: &str = "--dangerously-bypass-approvals-and-sandbox";
const EXIT_ON_QUOTA_FLAG: &str = "--exit-on-quota-exceeded";
const START_IMMEDIATELY_FLAG: &str = "--start-immediately";
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(20);
const RESIZE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_TAIL_BYTES: usize = 16 * 1024;

pub struct Launcher {
    binary: PathBuf,
    plus_k: bool,
}

impl Launcher {
    pub fn detect() -> Result<Self> {
        let binary = which::which("codex").context("could not find `codex` on PATH")?;
        Self::from_binary(binary)
    }

    fn from_binary(binary: PathBuf) -> Result<Self> {
        let output = Command::new(&binary)
            .arg("--version")
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .with_context(|| format!("could not run `{} --version`", binary.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "`{} --version` failed{}",
                binary.display(),
                if stderr.trim().is_empty() {
                    String::new()
                } else {
                    format!(": {}", stderr.trim())
                }
            );
        }
        Ok(Self {
            binary,
            plus_k: version_is_plus_k(&output.stdout)?,
        })
    }

    pub fn run(
        &self,
        initial_args: Vec<OsString>,
        cwd: Option<&Path>,
        quota_auto_restart: Option<bool>,
        rotate_account: impl FnMut() -> Result<()>,
    ) -> Result<u8> {
        let quota_auto_restart = match quota_auto_restart {
            Some(true) if !self.plus_k => {
                bail!("--quota-auto-restart yes requires a +k Codex build")
            }
            Some(enabled) => enabled,
            None => self.plus_k,
        };
        if !quota_auto_restart {
            return run_direct(&self.binary, &initial_args, cwd);
        }
        self.run_supervised(initial_args, cwd, rotate_account, SupervisedIo::terminal())
    }

    fn run_supervised(
        &self,
        initial_args: Vec<OsString>,
        cwd: Option<&Path>,
        mut rotate_account: impl FnMut() -> Result<()>,
        io: SupervisedIo,
    ) -> Result<u8> {
        let mut args = initial_args;
        args.push(EXIT_ON_QUOTA_FLAG.into());
        let SupervisedIo {
            input,
            output,
            raw_terminal,
        } = io;
        let input = InputRouter::start(input);

        loop {
            match run_pty_session(&self.binary, &args, cwd, &input, &output, raw_terminal)? {
                PtyOutcome::Exited(code) => return Ok(code),
                PtyOutcome::QuotaExceeded(thread_id) => {
                    rotate_account()?;
                    args = recovery_args(&thread_id);
                }
            }
        }
    }
}

fn version_is_plus_k(stdout: &[u8]) -> Result<bool> {
    let output =
        std::str::from_utf8(stdout).context("`codex --version` returned non-UTF-8 output")?;
    let raw_version = output
        .split_ascii_whitespace()
        .next_back()
        .context("`codex --version` returned no version")?;
    let version = Version::parse(raw_version.trim_start_matches('v'))
        .with_context(|| format!("could not parse Codex version `{raw_version}`"))?;
    Ok(version.build.as_str().split('.').any(|part| part == "k"))
}

fn run_direct(binary: &Path, args: &[OsString], cwd: Option<&Path>) -> Result<u8> {
    let mut command = Command::new(binary);
    command
        .args(args)
        .env(GIT_TERMINAL_PROMPT_ENV, "0")
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let status = command
        .status()
        .with_context(|| format!("Failed to run `{}`", binary.display()))?;
    Ok(status
        .code()
        .and_then(|code| u8::try_from(code).ok())
        .unwrap_or(1))
}

fn recovery_args(thread_id: &str) -> Vec<OsString> {
    [
        "resume".into(),
        thread_id.into(),
        START_IMMEDIATELY_FLAG.into(),
        APPROVAL_BYPASS_FLAG.into(),
        EXIT_ON_QUOTA_FLAG.into(),
    ]
    .into()
}

enum PtyOutcome {
    Exited(u8),
    QuotaExceeded(String),
}

fn run_pty_session(
    binary: &Path,
    args: &[OsString],
    cwd: Option<&Path>,
    input: &InputRouter,
    output: &Arc<Mutex<Box<dyn Write + Send>>>,
    raw_terminal: bool,
) -> Result<PtyOutcome> {
    let _raw_mode = RawModeGuard::enter(raw_terminal)?;
    let initial_size = terminal_size(raw_terminal);
    let pair = native_pty_system()
        .openpty(initial_size)
        .context("could not open a pseudo-terminal for Codex")?;
    let mut command = CommandBuilder::new(binary);
    command.args(args);
    command.env(GIT_TERMINAL_PROMPT_ENV, "0");
    if let Some(cwd) = cwd {
        command.cwd(cwd);
    }
    let reader = pair
        .master
        .try_clone_reader()
        .context("could not read Codex pseudo-terminal output")?;
    let writer = pair
        .master
        .take_writer()
        .context("could not write to the Codex pseudo-terminal")?;
    let mut child = pair
        .slave
        .spawn_command(command)
        .with_context(|| format!("Failed to run `{}`", binary.display()))?;
    drop(pair.slave);
    if let Err(error) = input.attach(writer) {
        child.kill().ok();
        child.wait().ok();
        return Err(error);
    }
    let observed = Arc::new(Mutex::new(OutputObservation::new()));
    let relay = spawn_output_relay(reader, Arc::clone(output), Arc::clone(&observed));
    let mut last_size = initial_size;
    let mut last_resize_poll = Instant::now();

    let supervision = (|| -> Result<_> {
        loop {
            if let Some(status) = child.try_wait().context("could not poll Codex")? {
                break Ok(status);
            }

            if last_resize_poll.elapsed() >= RESIZE_POLL_INTERVAL {
                let size = terminal_size(raw_terminal);
                if size != last_size {
                    pair.master
                        .resize(size)
                        .context("could not resize the Codex pseudo-terminal")?;
                    last_size = size;
                }
                last_resize_poll = Instant::now();
            }

            if let Some(error) = input.error()? {
                bail!("could not forward terminal input to Codex: {error}");
            }
            if let Some(error) = observed
                .lock()
                .map_err(|_| anyhow!("Codex output observer lock was poisoned"))?
                .error()
            {
                bail!("could not relay Codex terminal output: {error}");
            }
            thread::sleep(PROCESS_POLL_INTERVAL);
        }
    })();

    if supervision.is_err() {
        child.kill().ok();
        child.wait().ok();
    }
    let detach = input.detach();
    let relay = relay
        .join()
        .map_err(|_| anyhow!("Codex output relay thread panicked"));
    let status = supervision?;
    detach?;
    relay?;
    let observation = observed
        .lock()
        .map_err(|_| anyhow!("Codex output observer lock was poisoned"))?;
    if let Some(error) = observation.error() {
        bail!("could not relay Codex terminal output: {error}");
    }
    if let Some(thread_id) = observation.quota_thread_id() {
        return Ok(PtyOutcome::QuotaExceeded(thread_id));
    }

    Ok(PtyOutcome::Exited(
        u8::try_from(status.exit_code()).unwrap_or(1),
    ))
}

struct SupervisedIo {
    input: Box<dyn Read + Send>,
    output: Arc<Mutex<Box<dyn Write + Send>>>,
    raw_terminal: bool,
}

impl SupervisedIo {
    fn terminal() -> Self {
        Self {
            input: Box::new(io::stdin()),
            output: Arc::new(Mutex::new(Box::new(io::stdout()))),
            raw_terminal: io::stdin().is_terminal() && io::stdout().is_terminal(),
        }
    }
}

struct InputRouter {
    state: Arc<Mutex<InputState>>,
}

struct InputState {
    writer: Option<Box<dyn Write + Send>>,
    error: Option<String>,
}

impl InputRouter {
    fn start(mut reader: Box<dyn Read + Send>) -> Self {
        let state = Arc::new(Mutex::new(InputState {
            writer: None,
            error: None,
        }));
        let thread_state = Arc::clone(&state);
        thread::spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(count) => count,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        if let Ok(mut state) = thread_state.lock() {
                            state.error = Some(error.to_string());
                        }
                        return;
                    }
                };
                let Ok(mut state) = thread_state.lock() else {
                    return;
                };
                if let Some(writer) = state.writer.as_mut()
                    && let Err(error) = writer.write_all(&buffer[..count])
                    && error.kind() != io::ErrorKind::BrokenPipe
                {
                    state.error = Some(error.to_string());
                }
            }
        });
        Self { state }
    }

    fn attach(&self, writer: Box<dyn Write + Send>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?;
        state.writer = Some(writer);
        Ok(())
    }

    fn detach(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?;
        state.writer = None;
        if let Some(error) = state.error.take() {
            bail!("could not forward terminal input to Codex: {error}");
        }
        Ok(())
    }

    fn error(&self) -> Result<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?
            .error
            .clone())
    }
}

struct RawModeGuard {
    disable_on_drop: bool,
}

impl RawModeGuard {
    fn enter(enabled: bool) -> Result<Self> {
        let already_raw = enabled
            && crossterm::terminal::is_raw_mode_enabled()
                .context("could not inspect terminal input mode")?;
        if enabled && !already_raw {
            crossterm::terminal::enable_raw_mode()
                .context("could not enable raw terminal input")?;
        }
        Ok(Self {
            disable_on_drop: enabled && !already_raw,
        })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.disable_on_drop {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

fn terminal_size(use_terminal: bool) -> PtySize {
    if use_terminal && let Ok(size) = crossterm::terminal::window_size() {
        return PtySize {
            rows: size.rows.max(1),
            cols: size.columns.max(1),
            pixel_width: size.width,
            pixel_height: size.height,
        };
    }
    PtySize::default()
}

fn spawn_output_relay(
    mut reader: Box<dyn Read + Send>,
    output: Arc<Mutex<Box<dyn Write + Send>>>,
    observed: Arc<Mutex<OutputObservation>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0; 16 * 1024];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => return,
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if pty_eof(&error) => return,
                Err(error) => {
                    set_output_error(&observed, error.to_string());
                    return;
                }
            };
            let bytes = &buffer[..count];
            let relay_result = output
                .lock()
                .map_err(|_| anyhow!("terminal output lock was poisoned"))
                .and_then(|mut output| {
                    output.write_all(bytes)?;
                    output.flush()?;
                    Ok(())
                });
            if let Err(error) = relay_result {
                set_output_error(&observed, error.to_string());
                return;
            }
            let Ok(mut observed) = observed.lock() else {
                return;
            };
            observed.process(bytes);
        }
    })
}

fn set_output_error(observed: &Arc<Mutex<OutputObservation>>, error: String) {
    if let Ok(mut observed) = observed.lock() {
        observed.error = Some(error);
    }
}

#[cfg(unix)]
fn pty_eof(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::EIO)
}

#[cfg(not(unix))]
fn pty_eof(_error: &io::Error) -> bool {
    false
}

struct OutputObservation {
    tail: Vec<u8>,
    error: Option<String>,
}

impl OutputObservation {
    fn new() -> Self {
        Self {
            tail: Vec::with_capacity(OUTPUT_TAIL_BYTES),
            error: None,
        }
    }

    fn process(&mut self, bytes: &[u8]) {
        if bytes.len() >= OUTPUT_TAIL_BYTES {
            self.tail.clear();
            self.tail
                .extend_from_slice(&bytes[bytes.len() - OUTPUT_TAIL_BYTES..]);
            return;
        }
        let overflow = self
            .tail
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(OUTPUT_TAIL_BYTES);
        if overflow > 0 {
            self.tail.drain(..overflow);
        }
        self.tail.extend_from_slice(bytes);
    }

    fn error(&self) -> Option<String> {
        self.error.clone()
    }

    fn quota_thread_id(&self) -> Option<String> {
        quota_thread_id_from_tail(&self.tail)
    }
}

fn quota_thread_id_from_tail(tail: &[u8]) -> Option<String> {
    let tail = tail
        .strip_suffix(b"\r\n")
        .or_else(|| tail.strip_suffix(b"\n"))?;
    let prefix = b"codex+k (";
    let start = tail
        .windows(prefix.len())
        .rposition(|window| window == prefix)?;
    let marker = std::str::from_utf8(&tail[start..]).ok()?;
    let thread_id = marker
        .strip_prefix("codex+k (")?
        .strip_suffix("): quota exceeded")?;
    valid_uuid(thread_id).then(|| thread_id.to_owned())
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;

    use super::*;

    #[cfg(unix)]
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    #[cfg(unix)]
    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn detects_only_k_build_metadata() {
        assert!(version_is_plus_k(b"codex-cli 0.147.0+k\n").unwrap());
        assert!(version_is_plus_k(b"codex-cli 0.147.0+release.k\n").unwrap());
        assert!(!version_is_plus_k(b"codex-cli 0.147.0\n").unwrap());
        assert!(!version_is_plus_k(b"codex-cli 0.147.0+kestrel\n").unwrap());
    }

    #[test]
    fn parses_only_a_terminal_final_quota_marker_with_a_uuid() {
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let output =
            format!("old screen text\x1b[?1049l\x1b[?25hcodex+k ({thread_id}): quota exceeded\r\n");
        assert_eq!(
            quota_thread_id_from_tail(output.as_bytes()).as_deref(),
            Some(thread_id)
        );
        assert_eq!(
            quota_thread_id_from_tail(b"codex+k (not-a-uuid): quota exceeded\r\n"),
            None
        );
        assert_eq!(
            quota_thread_id_from_tail(
                format!("codex+k ({thread_id}): quota exceeded\r\nmore output").as_bytes()
            ),
            None
        );
    }

    #[test]
    fn recovery_command_resumes_the_exact_thread_and_keeps_quota_detection_enabled() {
        let args = recovery_args("123e4567-e89b-12d3-a456-426614174000")
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            [
                "resume",
                "123e4567-e89b-12d3-a456-426614174000",
                START_IMMEDIATELY_FLAG,
                APPROVAL_BYPASS_FLAG,
                EXIT_ON_QUOTA_FLAG,
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn official_codex_keeps_direct_launch_without_the_quota_flag() {
        let root = tempdir().unwrap();
        let script = root.path().join("codex");
        let arguments = root.path().join("arguments");
        fs::write(
            &script,
            format!(
                concat!(
                    "#!/bin/sh\n",
                    "set -eu\n",
                    "if [ \"${{1-}}\" = --version ]; then\n",
                    "  printf 'codex-cli 0.147.0\\n'\n",
                    "  exit 0\n",
                    "fi\n",
                    "printf '%s\\n' \"$*\" > '{}'\n",
                ),
                arguments.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

        let launcher = Launcher::from_binary(script).unwrap();
        assert!(!launcher.plus_k);
        let mut rotations = 0;
        assert_eq!(
            launcher
                .run(
                    vec![APPROVAL_BYPASS_FLAG.into()],
                    Some(root.path()),
                    None,
                    || {
                        rotations += 1;
                        Ok(())
                    },
                )
                .unwrap(),
            0
        );
        assert_eq!(rotations, 0);
        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            format!("{APPROVAL_BYPASS_FLAG}\n")
        );

        let error = launcher
            .run(Vec::new(), Some(root.path()), Some(true), || Ok(()))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "--quota-auto-restart yes requires a +k Codex build"
        );
    }

    #[cfg(unix)]
    #[test]
    fn plus_k_codex_can_disable_quota_auto_restart() {
        let root = tempdir().unwrap();
        let script = root.path().join("codex");
        let arguments = root.path().join("arguments");
        fs::write(
            &script,
            format!(
                concat!(
                    "#!/bin/sh\n",
                    "set -eu\n",
                    "if [ \"${{1-}}\" = --version ]; then\n",
                    "  printf 'codex-cli 0.147.0+k\\n'\n",
                    "  exit 0\n",
                    "fi\n",
                    "printf '%s\\n' \"$*\" > '{}'\n",
                ),
                arguments.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();

        let launcher = Launcher::from_binary(script).unwrap();
        let mut rotations = 0;
        assert_eq!(
            launcher
                .run(Vec::new(), Some(root.path()), Some(false), || {
                    rotations += 1;
                    Ok(())
                })
                .unwrap(),
            0
        );
        assert_eq!(rotations, 0);
        assert_eq!(fs::read_to_string(arguments).unwrap(), "\n");
    }

    #[cfg(unix)]
    #[test]
    fn supervised_codex_rotates_and_restarts_the_exact_session() {
        let root = tempdir().unwrap();
        let source = root.path().join("fake_codex.rs");
        let binary = root.path().join("codex");
        let state = root.path().join("state");
        let arguments = root.path().join("arguments");
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let root_literal = format!("{:?}", root.path().to_str().unwrap());
        fs::write(
            &source,
            format!(
                r#"
use std::env;
use std::fs::{{self, OpenOptions}};
use std::io::Write;
use std::path::Path;

fn main() {{
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("--version") {{
        println!("codex-cli 0.147.0+k");
        return;
    }}

    let root = Path::new({root});
    let state = root.join("state");
    let count = fs::read_to_string(&state)
        .ok()
        .and_then(|value| value.trim().parse::<u8>().ok())
        .unwrap_or(0)
        + 1;
    fs::write(&state, format!("{{count}}\n")).unwrap();
    writeln!(
        OpenOptions::new().create(true).append(true).open(root.join("arguments")).unwrap(),
        "{{}}",
        args.join(" ")
    )
    .unwrap();

    if count == 1 {{
        println!("codex+k ({thread_id}): quota exceeded");
    }}
}}
"#,
                root = root_literal,
                thread_id = thread_id,
            ),
        )
        .unwrap();
        let compiled =
            Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc")))
                .args(["--edition=2024"])
                .arg(&source)
                .arg("-o")
                .arg(&binary)
                .output()
                .unwrap();
        assert!(
            compiled.status.success(),
            "fake Codex failed to compile: {}",
            String::from_utf8_lossy(&compiled.stderr)
        );

        let launcher = Launcher::from_binary(binary).unwrap();
        assert!(launcher.plus_k);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut rotations = 0;
        let code = launcher
            .run_supervised(
                vec![APPROVAL_BYPASS_FLAG.into()],
                Some(root.path()),
                || {
                    rotations += 1;
                    Ok(())
                },
                SupervisedIo {
                    input: Box::new(io::empty()),
                    output: Arc::new(Mutex::new(Box::new(SharedWriter(Arc::clone(&captured))))),
                    raw_terminal: false,
                },
            )
            .unwrap();

        assert_eq!(code, 0);
        assert_eq!(rotations, 1);
        assert_eq!(fs::read_to_string(state).unwrap(), "2\n");
        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            format!(
                concat!("{} {}\n", "resume {} {} {} {}\n"),
                APPROVAL_BYPASS_FLAG,
                EXIT_ON_QUOTA_FLAG,
                thread_id,
                START_IMMEDIATELY_FLAG,
                APPROVAL_BYPASS_FLAG,
                EXIT_ON_QUOTA_FLAG,
            )
        );
        assert!(
            String::from_utf8_lossy(&captured.lock().unwrap())
                .contains(&format!("codex+k ({thread_id}): quota exceeded"))
        );
    }
}
