use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use semver::Version;
use serde::Deserialize;

use super::GIT_TERMINAL_PROMPT_ENV;

pub(crate) const APPROVAL_BYPASS_FLAG: &str = "--dangerously-bypass-approvals-and-sandbox";
const CONFIG_OVERRIDE_FLAG: &str = "-c";
const DEFAULT_SERVICE_TIER_OVERRIDE: &str = "service_tier=default";
const FAST_SERVICE_TIER_OVERRIDE: &str = "service_tier=fast";
const EXIT_ON_QUOTA_FLAG: &str = "--exit-on-quota-exceeded";
const START_IMMEDIATELY_FLAG: &str = "--start-immediately";
const RESTORE_INPUT_HANDOFF_FLAG: &str = "--restore-input-handoff";
const INPUT_HANDOFF_FORMAT: &str = "codex+k-input-handoff";
const INPUT_HANDOFF_VERSION: u8 = 1;
const MAX_INPUT_HANDOFF_BYTES: u64 = 16 * 1024 * 1024;
const AUTH_FILE_FLAG: &str = "--auth-file";
const LEGACY_AUTH_FILE_ENV_VAR: &str = "CODEX_AUTH_FILE";
const EXTERNAL_AUTH_ENV_VARS: &[&str] = &["CODEX_ACCESS_TOKEN", "CODEX_API_KEY", "OPENAI_API_KEY"];
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(20);
const RESIZE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_TAIL_BYTES: usize = 16 * 1024;
const NO_QUOTA_RETRY_PROMPT: &str =
    "No enrolled account with usable Codex quota was found; retry? (Y/n)";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccountRotation {
    Rotated { auth_file: PathBuf },
    NoQuota(String),
}

#[derive(Clone, Copy)]
pub(crate) enum ServiceTier {
    Default,
    Fast,
}

#[derive(Clone, Copy)]
pub(crate) struct SupervisedEnvironment<'a> {
    codex_home: &'a Path,
    sqlite_home: &'a Path,
    auth_file: Option<&'a Path>,
}

impl<'a> SupervisedEnvironment<'a> {
    pub(crate) fn new(
        codex_home: &'a Path,
        sqlite_home: &'a Path,
        auth_file: Option<&'a Path>,
    ) -> Self {
        Self {
            codex_home,
            sqlite_home,
            auth_file,
        }
    }
}

impl ServiceTier {
    fn config_override(self) -> &'static str {
        match self {
            Self::Default => DEFAULT_SERVICE_TIER_OVERRIDE,
            Self::Fast => FAST_SERVICE_TIER_OVERRIDE,
        }
    }
}

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
            .env_remove(LEGACY_AUTH_FILE_ENV_VAR)
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

    pub fn quota_auto_restart_enabled(&self, requested: Option<bool>) -> Result<bool> {
        match requested {
            Some(true) if !self.plus_k => {
                bail!("--quota-auto-restart yes requires a +k Codex build")
            }
            Some(enabled) => Ok(enabled),
            None => Ok(self.plus_k),
        }
    }

    pub fn run_direct(
        &self,
        mut args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
    ) -> Result<u8> {
        force_service_tier(&mut args, service_tier);
        run_direct(&self.binary, &args, cwd)
    }

    pub fn run_supervised(
        &self,
        initial_args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
        environment: SupervisedEnvironment<'_>,
        rotate_account: impl FnMut() -> Result<AccountRotation>,
    ) -> Result<u8> {
        self.run_supervised_with_io(
            initial_args,
            cwd,
            service_tier,
            environment,
            rotate_account,
            SupervisedIo::terminal(),
        )
    }

    fn run_supervised_with_io(
        &self,
        initial_args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
        environment: SupervisedEnvironment<'_>,
        mut rotate_account: impl FnMut() -> Result<AccountRotation>,
        io: SupervisedIo,
    ) -> Result<u8> {
        let mut args = initial_args;
        force_service_tier(&mut args, service_tier);
        args.push(EXIT_ON_QUOTA_FLAG.into());
        let SupervisedIo {
            input,
            output,
            raw_terminal,
            prompt_terminal,
        } = io;
        let input = InputRouter::start(input);
        let mut auth_file = environment.auth_file.map(Path::to_owned);

        loop {
            let child_environment = SupervisedEnvironment::new(
                environment.codex_home,
                environment.sqlite_home,
                auth_file.as_deref(),
            );
            match run_pty_session(
                &self.binary,
                &args,
                cwd,
                child_environment,
                &input,
                &output,
                raw_terminal,
            )? {
                PtyOutcome::Exited(code) => return Ok(code),
                PtyOutcome::QuotaExceeded(recovery) => {
                    let rotation =
                        rotate_account_with_retry(&mut rotate_account, &mut |details| {
                            prompt_no_quota_retry(&input, prompt_terminal, details)
                        })?;
                    let AccountRotation::Rotated { auth_file: next } = rotation else {
                        unreachable!("quota retry returned without a rotated account")
                    };
                    auth_file = Some(next);
                    args = recovery_args(recovery);
                }
            }
        }
    }
}

fn rotate_account_with_retry(
    rotate_account: &mut impl FnMut() -> Result<AccountRotation>,
    retry: &mut impl FnMut(&str) -> Result<bool>,
) -> Result<AccountRotation> {
    loop {
        match rotate_account()? {
            rotation @ AccountRotation::Rotated { .. } => return Ok(rotation),
            AccountRotation::NoQuota(details) => {
                if !retry(&details)? {
                    bail!(details);
                }
            }
        }
    }
}

fn prompt_no_quota_retry(
    input: &InputRouter,
    prompt_terminal: bool,
    details: &str,
) -> Result<bool> {
    if !prompt_terminal {
        bail!(details.to_owned());
    }

    let _raw_mode = RawModeGuard::enter(true)?;
    let prompt = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    prompt.set_style(
        ProgressStyle::with_template("{msg}").context("could not configure quota retry prompt")?,
    );
    prompt.set_message(NO_QUOTA_RETRY_PROMPT);
    prompt.tick();

    let answer = match input.read_confirmation(|selection| {
        prompt.set_message(match selection {
            Some(true) => format!("{NO_QUOTA_RETRY_PROMPT} y"),
            Some(false) => format!("{NO_QUOTA_RETRY_PROMPT} n"),
            None => NO_QUOTA_RETRY_PROMPT.to_owned(),
        });
    }) {
        Ok(answer) => answer,
        Err(error) => {
            prompt.finish_and_clear();
            return Err(error).context("could not read quota retry confirmation");
        }
    };
    prompt.finish_with_message(format!(
        "{NO_QUOTA_RETRY_PROMPT} {}",
        if answer { "yes" } else { "no" }
    ));
    Ok(answer)
}

fn force_service_tier(args: &mut Vec<OsString>, service_tier: ServiceTier) {
    args.extend([
        CONFIG_OVERRIDE_FLAG.into(),
        service_tier.config_override().into(),
    ]);
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

fn run_direct(binary: &Path, args: &[OsString], cwd: &Path) -> Result<u8> {
    let mut command = Command::new(binary);
    command
        .args(args)
        .env(GIT_TERMINAL_PROMPT_ENV, "0")
        .env_remove(LEGACY_AUTH_FILE_ENV_VAR)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command.current_dir(cwd);
    let status = command
        .status()
        .with_context(|| format!("Failed to run `{}`", binary.display()))?;
    Ok(status
        .code()
        .and_then(|code| u8::try_from(code).ok())
        .unwrap_or(1))
}

fn recovery_args(recovery: QuotaRecovery) -> Vec<OsString> {
    let QuotaRecovery {
        thread_id,
        resume_args,
        handoff_path,
    } = recovery;
    let mut args: Vec<OsString> = [
        "resume".into(),
        thread_id.into(),
        START_IMMEDIATELY_FLAG.into(),
        APPROVAL_BYPASS_FLAG.into(),
        EXIT_ON_QUOTA_FLAG.into(),
    ]
    .into();
    if let Some(path) = handoff_path {
        args.extend([RESTORE_INPUT_HANDOFF_FLAG.into(), path.into_os_string()]);
    }
    args.extend(resume_args.into_iter().map(OsString::from));
    args
}

enum PtyOutcome {
    Exited(u8),
    QuotaExceeded(QuotaRecovery),
}

#[derive(Debug, PartialEq, Eq)]
struct QuotaRecovery {
    thread_id: String,
    resume_args: Vec<String>,
    handoff_path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct QuotaRecoveryPayload {
    version: u8,
    #[serde(default)]
    resume_args: Vec<String>,
    handoff_path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct InputHandoffHeader {
    format: String,
    version: u8,
    thread_id: String,
    resume_args: Vec<String>,
}

fn run_pty_session(
    binary: &Path,
    args: &[OsString],
    cwd: &Path,
    environment: SupervisedEnvironment<'_>,
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
    if let Some(auth_file) = environment.auth_file {
        command.arg(AUTH_FILE_FLAG);
        command.arg(auth_file);
    }
    command.args(args);
    command.env(GIT_TERMINAL_PROMPT_ENV, "0");
    command.env("CODEX_HOME", environment.codex_home);
    command.env("CODEX_SQLITE_HOME", environment.sqlite_home);
    command.env_remove(LEGACY_AUTH_FILE_ENV_VAR);
    for name in EXTERNAL_AUTH_ENV_VARS {
        command.env_remove(name);
    }
    command.cwd(cwd);
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
    if let Some(recovery) = observation.quota_recovery()? {
        return Ok(PtyOutcome::QuotaExceeded(recovery));
    }

    Ok(PtyOutcome::Exited(
        u8::try_from(status.exit_code()).unwrap_or(1),
    ))
}

struct SupervisedIo {
    input: Box<dyn Read + Send>,
    output: Arc<Mutex<Box<dyn Write + Send>>>,
    raw_terminal: bool,
    prompt_terminal: bool,
}

impl SupervisedIo {
    fn terminal() -> Self {
        Self {
            input: Box::new(io::stdin()),
            output: Arc::new(Mutex::new(Box::new(io::stdout()))),
            raw_terminal: io::stdin().is_terminal() && io::stdout().is_terminal(),
            prompt_terminal: io::stdin().is_terminal() && io::stderr().is_terminal(),
        }
    }
}

struct InputRouter {
    shared: Arc<SharedInputState>,
}

struct SharedInputState {
    state: Mutex<InputState>,
    ready: Condvar,
}

struct InputState {
    writer: Option<Box<dyn Write + Send>>,
    pending: VecDeque<u8>,
    error: Option<String>,
    closed: bool,
}

impl InputRouter {
    fn start(mut reader: Box<dyn Read + Send>) -> Self {
        let shared = Arc::new(SharedInputState {
            state: Mutex::new(InputState {
                writer: None,
                pending: VecDeque::new(),
                error: None,
                closed: false,
            }),
            ready: Condvar::new(),
        });
        let thread_shared = Arc::clone(&shared);
        thread::spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) => {
                        if let Ok(mut state) = thread_shared.state.lock() {
                            state.closed = true;
                            thread_shared.ready.notify_all();
                        }
                        return;
                    }
                    Ok(count) => count,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        if let Ok(mut state) = thread_shared.state.lock() {
                            state.error = Some(error.to_string());
                            thread_shared.ready.notify_all();
                        }
                        return;
                    }
                };
                let Ok(mut state) = thread_shared.state.lock() else {
                    return;
                };
                if let Some(writer) = state.writer.as_mut() {
                    if let Err(error) = writer.write_all(&buffer[..count])
                        && error.kind() != io::ErrorKind::BrokenPipe
                    {
                        state.error = Some(error.to_string());
                    }
                } else {
                    state.pending.extend(&buffer[..count]);
                }
                thread_shared.ready.notify_all();
            }
        });
        Self { shared }
    }

    fn attach(&self, mut writer: Box<dyn Write + Send>) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?;
        if !state.pending.is_empty() {
            let pending = state.pending.drain(..).collect::<Vec<_>>();
            writer
                .write_all(&pending)
                .context("could not forward buffered terminal input to Codex")?;
        }
        state.writer = Some(writer);
        Ok(())
    }

    fn detach(&self) -> Result<()> {
        let mut state = self
            .shared
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
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?
            .error
            .clone())
    }

    fn read_confirmation(&self, mut selection_changed: impl FnMut(Option<bool>)) -> Result<bool> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("terminal input router lock was poisoned"))?;
        let mut answer = None;
        loop {
            while let Some(byte) = state.pending.pop_front() {
                match byte {
                    b'y' | b'Y' => {
                        answer = Some(true);
                        selection_changed(answer);
                    }
                    b'n' | b'N' => {
                        answer = Some(false);
                        selection_changed(answer);
                    }
                    b'\r' => {
                        if state.pending.front() == Some(&b'\n') {
                            state.pending.pop_front();
                        }
                        return Ok(answer.unwrap_or(true));
                    }
                    b'\n' => return Ok(answer.unwrap_or(true)),
                    8 | 127 => {
                        answer = None;
                        selection_changed(answer);
                    }
                    3 | 4 | 27 => bail!("quota retry prompt was interrupted"),
                    _ => {}
                }
            }
            if let Some(error) = &state.error {
                bail!("could not read terminal input: {error}");
            }
            if state.closed {
                bail!("terminal input closed while waiting for retry confirmation");
            }
            state = self
                .shared
                .ready
                .wait(state)
                .map_err(|_| anyhow!("terminal input router lock was poisoned"))?;
        }
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

    fn quota_recovery(&self) -> Result<Option<QuotaRecovery>> {
        quota_recovery_from_tail(&self.tail)
    }
}

fn quota_recovery_from_tail(tail: &[u8]) -> Result<Option<QuotaRecovery>> {
    let tail = tail
        .strip_suffix(b"\r\n")
        .or_else(|| tail.strip_suffix(b"\n"));
    let Some(tail) = tail else {
        return Ok(None);
    };
    let final_line_start = tail
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let final_line = &tail[final_line_start..];
    let prefix = b"codex+k (";
    let Some(start) = final_line
        .windows(prefix.len())
        .rposition(|window| window == prefix)
    else {
        return Ok(None);
    };
    let marker = std::str::from_utf8(&final_line[start..])
        .context("+k quota recovery marker was not UTF-8")?;
    let marker = marker
        .strip_prefix("codex+k (")
        .expect("marker start was located by its prefix");
    let Some((thread_id, payload)) = marker.split_once("): quota exceeded ") else {
        if marker.ends_with("): quota exceeded") {
            bail!("+k quota recovery marker did not include recovery settings");
        }
        return Ok(None);
    };
    if !valid_uuid(thread_id) {
        bail!("+k quota recovery marker contained invalid thread ID `{thread_id}`");
    }
    let payload: QuotaRecoveryPayload =
        serde_json::from_str(payload).context("could not parse +k quota recovery settings")?;
    let recovery = match payload.version {
        1 => {
            if payload.resume_args.is_empty() {
                bail!("+k quota recovery settings contained no resume arguments");
            }
            QuotaRecovery {
                thread_id: thread_id.to_owned(),
                resume_args: payload.resume_args,
                handoff_path: None,
            }
        }
        2 => {
            let handoff_path = payload
                .handoff_path
                .context("+k quota recovery settings contained no input handoff path")?;
            if !handoff_path.is_absolute() {
                bail!(
                    "+k input handoff path was not absolute: {}",
                    handoff_path.display()
                );
            }
            let handoff = read_input_handoff_header(&handoff_path)?;
            if handoff.format != INPUT_HANDOFF_FORMAT {
                bail!("unsupported +k input handoff format `{}`", handoff.format);
            }
            if handoff.version != INPUT_HANDOFF_VERSION {
                bail!(
                    "unsupported +k input handoff version {}; expected {INPUT_HANDOFF_VERSION}",
                    handoff.version
                );
            }
            if handoff.thread_id != thread_id {
                bail!(
                    "+k input handoff belongs to thread {}, not {thread_id}",
                    handoff.thread_id
                );
            }
            if handoff.resume_args.is_empty() {
                bail!("+k input handoff contained no resume arguments");
            }
            QuotaRecovery {
                thread_id: thread_id.to_owned(),
                resume_args: handoff.resume_args,
                handoff_path: Some(handoff_path),
            }
        }
        version => bail!("unsupported +k quota recovery settings version {version}"),
    };
    Ok(Some(recovery))
}

fn read_input_handoff_header(path: &Path) -> Result<InputHandoffHeader> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("could not inspect +k input handoff {}", path.display()))?;
    if !metadata.is_file() {
        bail!("+k input handoff {} is not a regular file", path.display());
    }
    if metadata.len() > MAX_INPUT_HANDOFF_BYTES {
        bail!(
            "+k input handoff {} is larger than the {} MiB safety limit",
            path.display(),
            MAX_INPUT_HANDOFF_BYTES / (1024 * 1024)
        );
    }
    let bytes = std::fs::read(path)
        .with_context(|| format!("could not read +k input handoff {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("could not parse +k input handoff {}", path.display()))
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
    fn parses_only_a_terminal_final_quota_marker_with_recovery_arguments() {
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let payload =
            r#"{"version":1,"resume_args":["--model","gpt-5.6","-c","service_tier=\"fast\""]}"#;
        let output = format!(
            "old screen text\x1b[?1049l\x1b[?25hcodex+k ({thread_id}): quota exceeded {payload}\r\n"
        );
        assert_eq!(
            quota_recovery_from_tail(output.as_bytes()).unwrap(),
            Some(QuotaRecovery {
                thread_id: thread_id.to_string(),
                resume_args: vec![
                    "--model".to_string(),
                    "gpt-5.6".to_string(),
                    "-c".to_string(),
                    "service_tier=\"fast\"".to_string(),
                ],
                handoff_path: None,
            })
        );
        assert_eq!(
            quota_recovery_from_tail(
                format!("codex+k ({thread_id}): quota exceeded {payload}\r\nmore output")
                    .as_bytes()
            )
            .unwrap(),
            None
        );
        assert!(
            quota_recovery_from_tail(
                format!("codex+k (not-a-uuid): quota exceeded {payload}\r\n").as_bytes()
            )
            .unwrap_err()
            .to_string()
            .contains("invalid thread ID")
        );
        assert!(
            quota_recovery_from_tail(
                format!("codex+k ({thread_id}): quota exceeded\r\n").as_bytes()
            )
            .unwrap_err()
            .to_string()
            .contains("did not include recovery settings")
        );
    }

    #[test]
    fn recovery_command_resumes_the_exact_thread_with_reported_settings() {
        let args = recovery_args(QuotaRecovery {
            thread_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            resume_args: vec![
                "--model".to_string(),
                "gpt-5.6".to_string(),
                CONFIG_OVERRIDE_FLAG.to_string(),
                "service_tier=\"fast\"".to_string(),
                CONFIG_OVERRIDE_FLAG.to_string(),
                "model_reasoning_effort=\"xhigh\"".to_string(),
            ],
            handoff_path: None,
        })
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
                "--model",
                "gpt-5.6",
                CONFIG_OVERRIDE_FLAG,
                "service_tier=\"fast\"",
                CONFIG_OVERRIDE_FLAG,
                "model_reasoning_effort=\"xhigh\"",
            ]
        );
    }

    #[test]
    fn version_two_marker_loads_resume_settings_from_the_handoff() {
        let directory = tempdir().unwrap();
        let handoff_path = directory.path().join("session.codex+k-handoff.json");
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        fs::write(
            &handoff_path,
            serde_json::to_vec(&serde_json::json!({
                "format": INPUT_HANDOFF_FORMAT,
                "version": INPUT_HANDOFF_VERSION,
                "thread_id": thread_id,
                "resume_args": ["--model", "gpt-5.6"],
                "messages": [],
                "draft": null,
            }))
            .unwrap(),
        )
        .unwrap();
        let marker = serde_json::json!({
            "version": 2,
            "handoff_path": handoff_path,
        });
        let output = format!("codex+k ({thread_id}): quota exceeded {marker}\r\n");

        assert_eq!(
            quota_recovery_from_tail(output.as_bytes()).unwrap(),
            Some(QuotaRecovery {
                thread_id: thread_id.to_string(),
                resume_args: vec!["--model".to_string(), "gpt-5.6".to_string()],
                handoff_path: Some(handoff_path),
            })
        );
    }

    #[test]
    fn version_two_marker_rejects_a_handoff_for_another_thread() {
        let directory = tempdir().unwrap();
        let handoff_path = directory.path().join("session.codex+k-handoff.json");
        fs::write(
            &handoff_path,
            serde_json::to_vec(&serde_json::json!({
                "format": INPUT_HANDOFF_FORMAT,
                "version": INPUT_HANDOFF_VERSION,
                "thread_id": "123e4567-e89b-12d3-a456-426614174001",
                "resume_args": ["--model", "gpt-5.6"],
            }))
            .unwrap(),
        )
        .unwrap();
        let marker = serde_json::json!({
            "version": 2,
            "handoff_path": handoff_path,
        });
        let output =
            format!("codex+k (123e4567-e89b-12d3-a456-426614174000): quota exceeded {marker}\r\n");

        assert!(
            quota_recovery_from_tail(output.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("belongs to thread")
        );
    }

    #[test]
    fn recovery_command_passes_the_handoff_back_to_codex() {
        let args = recovery_args(QuotaRecovery {
            thread_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            resume_args: vec!["--model".to_string(), "gpt-5.6".to_string()],
            handoff_path: Some(PathBuf::from("/tmp/session.codex+k-handoff.json")),
        })
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
                RESTORE_INPUT_HANDOFF_FLAG,
                "/tmp/session.codex+k-handoff.json",
                "--model",
                "gpt-5.6",
            ]
        );
    }

    #[test]
    fn unavailable_quota_reprompts_until_rotation_succeeds() {
        let mut outcomes = VecDeque::from([
            AccountRotation::NoQuota("first check found no quota".to_owned()),
            AccountRotation::NoQuota("second check found no quota".to_owned()),
            AccountRotation::Rotated {
                auth_file: PathBuf::from("/tmp/rotated-auth.json"),
            },
        ]);
        let mut prompts = Vec::new();

        rotate_account_with_retry(&mut || Ok(outcomes.pop_front().unwrap()), &mut |details| {
            prompts.push(details.to_owned());
            Ok(true)
        })
        .unwrap();

        assert!(outcomes.is_empty());
        assert_eq!(
            prompts,
            ["first check found no quota", "second check found no quota"]
        );
    }

    #[test]
    fn declining_quota_retry_returns_the_latest_no_quota_error() {
        let mut rotations = 0;
        let error = rotate_account_with_retry(
            &mut || {
                rotations += 1;
                Ok(AccountRotation::NoQuota(
                    "all checked accounts are exhausted".to_owned(),
                ))
            },
            &mut |_| Ok(false),
        )
        .unwrap_err();

        assert_eq!(rotations, 1);
        assert_eq!(error.to_string(), "all checked accounts are exhausted");
    }

    #[test]
    fn quota_rotation_errors_do_not_prompt() {
        let mut prompts = 0;
        let error = rotate_account_with_retry(
            &mut || Err(anyhow!("credential store is unavailable")),
            &mut |_| {
                prompts += 1;
                Ok(true)
            },
        )
        .unwrap_err();

        assert_eq!(prompts, 0);
        assert_eq!(error.to_string(), "credential store is unavailable");
    }

    #[test]
    fn input_router_reads_default_yes_and_explicit_answers_while_detached() {
        let default_yes = InputRouter::start(Box::new(io::Cursor::new(b"\r")));
        let explicit_yes = InputRouter::start(Box::new(io::Cursor::new(b"Y\r")));
        let explicit_no = InputRouter::start(Box::new(io::Cursor::new(b"n\r")));

        assert!(default_yes.read_confirmation(|_| {}).unwrap());
        assert!(explicit_yes.read_confirmation(|_| {}).unwrap());
        assert!(!explicit_no.read_confirmation(|_| {}).unwrap());
    }

    #[test]
    fn noninteractive_quota_retry_preserves_the_selection_details() {
        let input = InputRouter::start(Box::new(io::empty()));
        let error =
            prompt_no_quota_retry(&input, false, "checked: alice (quota exhausted)").unwrap_err();

        assert_eq!(error.to_string(), "checked: alice (quota exhausted)");
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
        assert!(!launcher.quota_auto_restart_enabled(None).unwrap());
        assert_eq!(
            launcher
                .run_direct(
                    vec![APPROVAL_BYPASS_FLAG.into()],
                    root.path(),
                    ServiceTier::Default,
                )
                .unwrap(),
            0
        );
        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            format!(
                "{APPROVAL_BYPASS_FLAG} {CONFIG_OVERRIDE_FLAG} {DEFAULT_SERVICE_TIER_OVERRIDE}\n"
            )
        );

        let error = launcher.quota_auto_restart_enabled(Some(true)).unwrap_err();
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
        assert!(!launcher.quota_auto_restart_enabled(Some(false)).unwrap());
        assert_eq!(
            launcher
                .run_direct(Vec::new(), root.path(), ServiceTier::Fast)
                .unwrap(),
            0
        );
        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            format!("{CONFIG_OVERRIDE_FLAG} {FAST_SERVICE_TIER_OVERRIDE}\n")
        );
    }

    #[cfg(unix)]
    #[test]
    fn supervised_codex_rotates_restarts_exact_session_and_preserves_cwd() {
        let root = tempdir().unwrap();
        let source = root.path().join("fake_codex.rs");
        let binary = root.path().join("codex");
        let state = root.path().join("state");
        let arguments = root.path().join("arguments");
        let working_directories = root.path().join("working-directories");
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let root_literal = format!("{:?}", root.path().to_str().unwrap());
        let recovery_payload_literal = format!(
            "{:?}",
            r#"{"version":1,"resume_args":["--model","gpt-5.6","-c","model_provider=\"openai\"","-c","service_tier=\"default\"","-c","model_reasoning_effort=\"xhigh\""]}"#
        );
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
    writeln!(
        OpenOptions::new().create(true).append(true).open(root.join("working-directories")).unwrap(),
        "{{}}",
        env::current_dir().unwrap().display()
    )
    .unwrap();

    if count == 1 {{
        println!("codex+k ({thread_id}): quota exceeded {{}}", {recovery_payload});
    }}
}}
"#,
                root = root_literal,
                thread_id = thread_id,
                recovery_payload = recovery_payload_literal,
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
            .run_supervised_with_io(
                vec![APPROVAL_BYPASS_FLAG.into()],
                root.path(),
                ServiceTier::Fast,
                SupervisedEnvironment::new(root.path(), root.path(), None),
                || {
                    rotations += 1;
                    Ok(AccountRotation::Rotated {
                        auth_file: root.path().join("rotated-auth.json"),
                    })
                },
                SupervisedIo {
                    input: Box::new(io::empty()),
                    output: Arc::new(Mutex::new(Box::new(SharedWriter(Arc::clone(&captured))))),
                    raw_terminal: false,
                    prompt_terminal: false,
                },
            )
            .unwrap();

        assert_eq!(code, 0);
        assert_eq!(rotations, 1);
        assert_eq!(fs::read_to_string(state).unwrap(), "2\n");
        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            format!(
                concat!(
                    "{} {} {} {}\n",
                    "{} {} resume {} {} {} {} --model gpt-5.6 {} model_provider=\"openai\" {} service_tier=\"default\" {} model_reasoning_effort=\"xhigh\"\n"
                ),
                APPROVAL_BYPASS_FLAG,
                CONFIG_OVERRIDE_FLAG,
                FAST_SERVICE_TIER_OVERRIDE,
                EXIT_ON_QUOTA_FLAG,
                AUTH_FILE_FLAG,
                root.path().join("rotated-auth.json").display(),
                thread_id,
                START_IMMEDIATELY_FLAG,
                APPROVAL_BYPASS_FLAG,
                EXIT_ON_QUOTA_FLAG,
                CONFIG_OVERRIDE_FLAG,
                CONFIG_OVERRIDE_FLAG,
                CONFIG_OVERRIDE_FLAG,
            )
        );
        assert_eq!(
            fs::read_to_string(working_directories).unwrap(),
            format!("{0}\n{0}\n", root.path().display())
        );
        assert!(
            String::from_utf8_lossy(&captured.lock().unwrap())
                .contains(&format!("codex+k ({thread_id}): quota exceeded"))
        );
    }
}
