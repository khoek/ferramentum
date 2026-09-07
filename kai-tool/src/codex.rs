use std::collections::VecDeque;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::BufRead;
use std::io::{self, IsTerminal, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use semver::Version;
use serde::Deserialize;

use super::GIT_TERMINAL_PROMPT_ENV;
use super::credential_provider::{
    CredentialPaths, LockMode, MANAGED_AUTH_ENV_VARS, SelectedCredential, open_private_file,
};

pub(crate) const APPROVAL_BYPASS_FLAG: &str = "--dangerously-bypass-approvals-and-sandbox";
const CONFIG_OVERRIDE_FLAG: &str = "-c";
const DEFAULT_SERVICE_TIER_OVERRIDE: &str = "service_tier=default";
const FAST_SERVICE_TIER_OVERRIDE: &str = "service_tier=fast";
const EXIT_ON_QUOTA_FLAG: &str = "--exit-on-quota-exceeded";
const SUPERVISED_EXIT_CODE: u8 = 75;
const START_IMMEDIATELY_FLAG: &str = "--start-immediately";
const RESTORE_INPUT_HANDOFF_FLAG: &str = "--restore-input-handoff";
const INPUT_HANDOFF_FORMAT: &str = "codex+k-input-handoff";
const INPUT_HANDOFF_VERSION: u8 = 1;
const MAX_INPUT_HANDOFF_BYTES: u64 = 16 * 1024 * 1024;
const AUTH_FILE_FLAG: &str = "--auth-file";
const CREDENTIAL_PROTOCOL_VERSION_FLAG: &str = "--credential-protocol-version";
const CREDENTIAL_PROTOCOL_VERSION: &str = "2";
const CREDENTIAL_USE_LOCK_FLAG: &str = "--credential-use-lock";
const CREDENTIAL_USE_LOCK_MODE_FLAG: &str = "--credential-use-lock-mode";
const CREDENTIAL_MUTATION_LOCK_FLAG: &str = "--credential-mutation-lock";
const CREDENTIAL_STARTUP_SOCKET_FLAG: &str = "--credential-startup-socket";
const CREDENTIAL_STARTUP_NONCE_FLAG: &str = "--credential-startup-nonce";
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(20);
const RESIZE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_TAIL_BYTES: usize = 16 * 1024;
const STARTUP_NONCE_BYTES: usize = 32;
const STARTUP_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_ACCEPT_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
pub(crate) enum ServiceTier {
    Default,
    Fast,
}

#[derive(Clone, Copy)]
pub(crate) struct SupervisedEnvironment<'a> {
    codex_home: &'a Path,
    sqlite_home: &'a Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CredentialFailureCause {
    QuotaExhausted,
    CredentialInvalid,
}

impl CredentialFailureCause {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::QuotaExhausted => "quota-exhausted",
            Self::CredentialInvalid => "credential-invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CredentialFailure {
    pub(crate) cause: CredentialFailureCause,
    pub(crate) unavailable_until: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct CredentialUseGuard {
    _file: File,
    path: PathBuf,
    mode: LockMode,
}

impl CredentialUseGuard {
    pub(crate) fn try_acquire(path: &Path, mode: LockMode) -> Result<Option<Self>> {
        match Self::try_acquire_existing(path, mode) {
            Err(error)
                if error
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
            {
                Ok(None)
            }
            result => result,
        }
    }

    fn try_acquire_existing(path: &Path, mode: LockMode) -> Result<Option<Self>> {
        let file = open_private_file(path)?;
        let locked = match mode {
            LockMode::Exclusive => FileExt::try_lock_exclusive(&file),
            LockMode::Shared => FileExt::try_lock_shared(&file),
        };
        match locked {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error).context("could not lock credential use file"),
        }
        validate_credential_use_lock(path, &file)?;
        Ok(Some(Self {
            _file: file,
            path: path.to_owned(),
            mode,
        }))
    }

    fn validate_current(&self) -> Result<()> {
        validate_credential_use_lock(&self.path, &self._file)
    }
}

fn validate_credential_use_lock(path: &Path, file: &File) -> Result<()> {
    let opened = file
        .metadata()
        .with_context(|| format!("could not inspect credential use lock {}", path.display()))?;
    let named = std::fs::symlink_metadata(path)
        .with_context(|| format!("could not inspect credential use lock {}", path.display()))?;
    if named.file_type().is_symlink() || !opened.is_file() || !named.is_file() {
        bail!(
            "credential use lock {} is not a regular file",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        if opened.dev() != named.dev() || opened.ino() != named.ino() {
            bail!(
                "credential use lock {} changed while opening",
                path.display()
            );
        }
        let mode = opened.permissions().mode();
        if mode & 0o777 != 0o600 || mode & 0o7000 != 0 {
            bail!("credential use lock {} must have mode 0600", path.display());
        }
        if opened.nlink() != 1 {
            bail!(
                "credential use lock {} must have exactly one hard link",
                path.display()
            );
        }
        // SAFETY: geteuid takes no pointers and has no preconditions.
        if opened.uid() != unsafe { libc::geteuid() } {
            bail!(
                "credential use lock {} is owned by another user",
                path.display()
            );
        }
    }
    Ok(())
}

impl<'a> SupervisedEnvironment<'a> {
    pub(crate) fn new(codex_home: &'a Path, sqlite_home: &'a Path) -> Self {
        Self {
            codex_home,
            sqlite_home,
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
    custom: bool,
}

impl Launcher {
    pub fn detect() -> Result<Self> {
        let binary = which::which("codex").context("could not find `codex` on PATH")?;
        Self::from_binary(binary)
    }

    fn from_binary(binary: PathBuf) -> Result<Self> {
        let mut version_command = Command::new(&binary);
        version_command.arg("--version");
        let output = version_command
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
            custom: version_is_custom(&output.stdout)?,
        })
    }

    pub fn supervision_enabled(&self, disabled: bool) -> bool {
        self.custom && !disabled
    }

    pub fn run_direct(
        &self,
        mut args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
    ) -> Result<u8> {
        force_service_tier(&mut args, service_tier);
        apply_launch_preferences(&mut args);
        run_direct(&self.binary, &args, cwd)
    }

    pub fn run_supervised(
        &self,
        initial_args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
        environment: SupervisedEnvironment<'_>,
        select_credential: impl FnMut(
            Option<(&CredentialPaths, &CredentialFailure)>,
        ) -> Result<Option<SelectedCredential>>,
    ) -> Result<u8> {
        self.run_supervised_with_io(
            initial_args,
            cwd,
            service_tier,
            environment,
            select_credential,
            SupervisedIo::terminal(),
        )
    }

    fn run_supervised_with_io(
        &self,
        initial_args: Vec<OsString>,
        cwd: &Path,
        service_tier: ServiceTier,
        environment: SupervisedEnvironment<'_>,
        mut select_credential: impl FnMut(
            Option<(&CredentialPaths, &CredentialFailure)>,
        ) -> Result<Option<SelectedCredential>>,
        io: SupervisedIo,
    ) -> Result<u8> {
        let mut args = initial_args;
        force_service_tier(&mut args, service_tier);
        args.push(EXIT_ON_QUOTA_FLAG.into());
        let SupervisedIo {
            input,
            output,
            raw_terminal,
        } = io;
        let input = InputRouter::start(input);
        let mut selected = select_credential(None)?;
        loop {
            apply_launch_preferences(&mut args);
            let (credential, use_guard) = match selected.take() {
                Some(SelectedCredential { paths, guard }) => {
                    validate_managed_launch_args(&args)?;
                    (Some(paths), Some(guard))
                }
                None => (None, None),
            };
            let outcome = run_pty_session(
                &self.binary,
                &args,
                cwd,
                PtySessionOptions {
                    environment,
                    credential: credential.as_ref(),
                    input: &input,
                    output: &output,
                    raw_terminal,
                    use_guard,
                },
            )?;
            match outcome {
                PtyOutcome::Exited(code) => return Ok(code),
                PtyOutcome::CredentialUnavailable(recovery, failure) => {
                    let paths = credential.as_ref().context(
                        "automatic credential rotation requires a configured credential provider",
                    )?;
                    selected = select_credential(Some((paths, &failure)))?;
                    if selected.is_none() {
                        bail!("credential provider returned no replacement credential");
                    }
                    args = recovery_args(recovery);
                }
            }
        }
    }
}

fn apply_launch_preferences(args: &mut Vec<OsString>) {
    for value in [
        "agents.max_concurrent_threads_per_session=16",
        "tui.theme=\"monokai-extended\"",
        "tui.whimsy=false",
        "tui.status_line_use_colors=true",
        "tui.resume_cwd=\"session\"",
        "notice.hide_rate_limit_model_nudge=true",
        concat!(
            "tui.status_line=[\"model-with-reasoning\",\"run-state\",\"context-remaining\",",
            "\"weekly-limit\",\"total-input-tokens\",\"total-output-tokens\",\"fast-mode\"]",
        ),
    ] {
        args.extend([CONFIG_OVERRIDE_FLAG.into(), value.into()]);
    }
}

fn force_service_tier(args: &mut Vec<OsString>, service_tier: ServiceTier) {
    args.extend([
        CONFIG_OVERRIDE_FLAG.into(),
        service_tier.config_override().into(),
    ]);
}

fn version_is_custom(stdout: &[u8]) -> Result<bool> {
    let output =
        std::str::from_utf8(stdout).context("`codex --version` returned non-UTF-8 output")?;
    let raw_version = output
        .split_ascii_whitespace()
        .next_back()
        .context("`codex --version` returned no version")?;
    let version = Version::parse(raw_version.trim_start_matches('v'))
        .with_context(|| format!("could not parse Codex version `{raw_version}`"))?;
    Ok(version.build.is_empty()
        && version
            .pre
            .as_str()
            .strip_prefix("k.")
            .is_some_and(|commit| {
                commit.len() == 8 && commit.bytes().all(|byte| byte.is_ascii_hexdigit())
            }))
}

fn run_direct(binary: &Path, args: &[OsString], cwd: &Path) -> Result<u8> {
    let mut command = Command::new(binary);
    command
        .args(args)
        .env(GIT_TERMINAL_PROMPT_ENV, "0")
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

fn recovery_args(recovery: ResumeHandoff) -> Vec<OsString> {
    let ResumeHandoff {
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
    args.extend([
        RESTORE_INPUT_HANDOFF_FLAG.into(),
        handoff_path.into_os_string(),
    ]);
    args.extend(resume_args.into_iter().map(OsString::from));
    args
}

#[cfg(unix)]
struct CredentialStartupBarrier {
    _directory: tempfile::TempDir,
    listener: UnixListener,
    socket_path: PathBuf,
    nonce: String,
    _parent_use_guard: CredentialUseGuard,
}

#[cfg(unix)]
impl CredentialStartupBarrier {
    fn bind(parent_use_guard: CredentialUseGuard) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("kai-credential-startup-")
            .tempdir()
            .context("could not create private credential startup directory")?;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .context("could not protect credential startup directory")?;
        let socket_path = directory.path().join("barrier.sock");
        let listener =
            UnixListener::bind(&socket_path).context("could not bind credential startup socket")?;
        listener
            .set_nonblocking(true)
            .context("could not configure credential startup socket")?;
        Ok(Self {
            _directory: directory,
            listener,
            socket_path,
            nonce: hex::encode(rand::random::<[u8; STARTUP_NONCE_BYTES]>()),
            _parent_use_guard: parent_use_guard,
        })
    }

    fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    fn nonce(&self) -> &str {
        &self.nonce
    }

    fn complete(&mut self, child: &mut dyn portable_pty::Child) -> Result<()> {
        let started = Instant::now();
        let mut stream = loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if startup_peer_matches_child(&stream, child)? {
                        break stream;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= STARTUP_ACCEPT_TIMEOUT {
                        bail!("timed out waiting for Codex credential startup");
                    }
                    if let Some(status) = child
                        .try_wait()
                        .context("could not poll Codex during credential startup")?
                    {
                        bail!(
                            "Codex exited with status {} before joining the credential startup barrier",
                            status.exit_code()
                        );
                    }
                    thread::sleep(PROCESS_POLL_INTERVAL);
                }
                Err(error) => {
                    return Err(error).context("could not accept Codex credential startup");
                }
            }
        };
        stream
            .set_read_timeout(Some(STARTUP_FRAME_TIMEOUT))
            .context("could not configure credential startup read timeout")?;
        stream
            .set_write_timeout(Some(STARTUP_FRAME_TIMEOUT))
            .context("could not configure credential startup write timeout")?;
        let expected_ready = format!("READY {}\n", self.nonce);
        read_exact_startup_frame(&mut stream, &expected_ready)
            .context("Codex returned an invalid credential startup readiness frame")?;
        self._parent_use_guard
            .validate_current()
            .context("credential use lock changed while Codex joined the startup barrier")?;
        let go = format!("GO {}\n", self.nonce);
        send_startup_guard(&mut stream, go.as_bytes(), &self._parent_use_guard._file)
            .context("could not transfer credential use lock to Codex")?;
        Ok(())
    }
}

#[cfg(unix)]
fn send_startup_guard(stream: &mut UnixStream, frame: &[u8], file: &File) -> io::Result<()> {
    use std::mem::{size_of, zeroed};
    use std::os::fd::AsRawFd;

    let mut control = vec![
        0usize;
        (unsafe { libc::CMSG_SPACE(size_of::<libc::c_int>() as _) } as usize)
            .div_ceil(size_of::<usize>())
    ];
    let mut payload = libc::iovec {
        iov_base: frame.as_ptr().cast_mut().cast(),
        iov_len: frame.len(),
    };
    // SAFETY: all pointers refer to live, suitably aligned buffers for this sendmsg call.
    let sent = unsafe {
        let mut message: libc::msghdr = zeroed();
        message.msg_iov = &mut payload;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = libc::CMSG_SPACE(size_of::<libc::c_int>() as _) as _;
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(size_of::<libc::c_int>() as _) as _;
        libc::CMSG_DATA(header)
            .cast::<libc::c_int>()
            .write(file.as_raw_fd());
        libc::sendmsg(stream.as_raw_fd(), &message, 0)
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    if sent == 0 {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "startup socket closed",
        ));
    }
    stream.write_all(&frame[sent as usize..])
}

#[cfg(target_os = "linux")]
fn startup_peer_matches_child(
    stream: &UnixStream,
    child: &mut dyn portable_pty::Child,
) -> Result<bool> {
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;

    let mut credentials = MaybeUninit::<libc::ucred>::uninit();
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `credentials` points to writable storage of `length` bytes and
    // `stream` owns a live Unix socket for the duration of the call.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error())
            .context("could not authenticate credential startup peer");
    }
    if length as usize != std::mem::size_of::<libc::ucred>() {
        bail!("credential startup peer returned malformed credentials");
    }
    // SAFETY: a successful SO_PEERCRED call initialized the complete `ucred`.
    let credentials = unsafe { credentials.assume_init() };
    let Some(child_pid) = child.process_id() else {
        bail!("could not determine Codex PID for credential startup");
    };
    Ok(credentials.uid == unsafe { libc::geteuid() }
        && u32::try_from(credentials.pid).ok() == Some(child_pid))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn startup_peer_matches_child(
    _stream: &UnixStream,
    _child: &mut dyn portable_pty::Child,
) -> Result<bool> {
    // The socket lives in a fresh mode-0700 directory and the nonce authenticates
    // the protocol on Unix platforms without Linux SO_PEERCRED PID support.
    Ok(true)
}

#[cfg(unix)]
fn read_exact_startup_frame(stream: &mut UnixStream, expected: &str) -> Result<()> {
    let mut frame = Vec::with_capacity(expected.len());
    let limit = u64::try_from(expected.len() + 1).unwrap_or(u64::MAX);
    io::BufReader::new(stream.take(limit)).read_until(b'\n', &mut frame)?;
    if frame != expected.as_bytes() {
        bail!("credential startup frame did not match its nonce");
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum PtyOutcome {
    Exited(u8),
    CredentialUnavailable(ResumeHandoff, CredentialFailure),
}

#[derive(Debug, PartialEq, Eq)]
struct ResumeHandoff {
    thread_id: String,
    resume_args: Vec<String>,
    handoff_path: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisedExitPayload {
    version: u8,
    outcome: CredentialFailureCause,
    unavailable_until: Option<i64>,
    handoff_path: PathBuf,
}

#[derive(Deserialize)]
struct InputHandoffHeader {
    format: String,
    version: u8,
    thread_id: String,
    resume_args: Vec<String>,
}

struct PtySessionOptions<'a> {
    environment: SupervisedEnvironment<'a>,
    credential: Option<&'a CredentialPaths>,
    input: &'a InputRouter,
    output: &'a Arc<Mutex<Box<dyn Write + Send>>>,
    raw_terminal: bool,
    use_guard: Option<CredentialUseGuard>,
}

fn run_pty_session(
    binary: &Path,
    args: &[OsString],
    cwd: &Path,
    options: PtySessionOptions<'_>,
) -> Result<PtyOutcome> {
    let PtySessionOptions {
        environment,
        credential,
        input,
        output,
        raw_terminal,
        use_guard,
    } = options;
    let _raw_mode = RawModeGuard::enter(raw_terminal)?;
    let initial_size = terminal_size(raw_terminal);
    let pair = native_pty_system()
        .openpty(initial_size)
        .context("could not open a pseudo-terminal for Codex")?;
    let mut command = CommandBuilder::new(binary);
    command.args(args);
    let lock_mode = use_guard
        .as_ref()
        .map_or(LockMode::Shared, |guard| guard.mode);
    #[cfg(unix)]
    let mut startup = use_guard.map(CredentialStartupBarrier::bind).transpose()?;
    #[cfg(not(unix))]
    if use_guard.is_some() {
        bail!("managed credential startup barriers require Unix sockets");
    }
    if let Some(credential) = credential {
        command.args([
            OsString::from(AUTH_FILE_FLAG),
            credential.auth_file.as_os_str().to_owned(),
            OsString::from(CREDENTIAL_PROTOCOL_VERSION_FLAG),
            OsString::from(CREDENTIAL_PROTOCOL_VERSION),
            OsString::from(CREDENTIAL_USE_LOCK_FLAG),
            credential.credential_use_lock.as_os_str().to_owned(),
            OsString::from(CREDENTIAL_USE_LOCK_MODE_FLAG),
            OsString::from(lock_mode.as_str()),
            OsString::from(CREDENTIAL_MUTATION_LOCK_FLAG),
            credential.credential_mutation_lock.as_os_str().to_owned(),
        ]);
        #[cfg(unix)]
        {
            let startup = startup
                .as_ref()
                .context("managed credential launch is missing its credential startup barrier")?;
            command.args([
                OsString::from(CREDENTIAL_STARTUP_SOCKET_FLAG),
                startup.socket_path().as_os_str().to_owned(),
                OsString::from(CREDENTIAL_STARTUP_NONCE_FLAG),
                OsString::from(startup.nonce()),
            ]);
        }
        for name in MANAGED_AUTH_ENV_VARS {
            command.env_remove(name);
        }
        command.env("CODEX_HOME", environment.codex_home);
        command.env("CODEX_SQLITE_HOME", environment.sqlite_home);
    }
    command.env(GIT_TERMINAL_PROMPT_ENV, "0");
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
    #[cfg(unix)]
    if let Some(startup) = startup.as_mut()
        && let Err(error) = startup.complete(&mut *child)
    {
        child.kill().ok();
        child.wait().ok();
        return Err(error).context("Codex credential startup barrier failed");
    }
    #[cfg(unix)]
    drop(startup);
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
    observation.outcome(status.exit_code())
}

struct SupervisedIo {
    input: Box<dyn Read + Send>,
    output: Arc<Mutex<Box<dyn Write + Send>>>,
    raw_terminal: bool,
}

impl SupervisedIo {
    fn terminal() -> Self {
        let stdin_terminal = io::stdin().is_terminal();
        let stdout_terminal = io::stdout().is_terminal();
        let stderr_terminal = io::stderr().is_terminal();
        // Interactive Codex output and Kai's recovery/status messages must share one terminal
        // stream.  Keep stdout when it is being piped so callers still receive Codex output
        // there, even if stdin and stderr are interactive.
        let use_stderr = stdin_terminal && stdout_terminal && stderr_terminal;
        let output_terminal = if use_stderr {
            stderr_terminal
        } else {
            stdout_terminal
        };
        let output: Box<dyn Write + Send> = if use_stderr {
            Box::new(io::stderr())
        } else {
            Box::new(io::stdout())
        };
        Self {
            input: Box::new(io::stdin()),
            output: Arc::new(Mutex::new(output)),
            raw_terminal: stdin_terminal && output_terminal,
        }
    }
}

struct InputRouter {
    shared: Arc<SharedInputState>,
}

struct SharedInputState {
    state: Mutex<InputState>,
}

struct InputState {
    writer: Option<Box<dyn Write + Send>>,
    pending: VecDeque<u8>,
    error: Option<String>,
}

impl InputRouter {
    fn start(mut reader: Box<dyn Read + Send>) -> Self {
        let shared = Arc::new(SharedInputState {
            state: Mutex::new(InputState {
                writer: None,
                pending: VecDeque::new(),
                error: None,
            }),
        });
        let thread_shared = Arc::clone(&shared);
        thread::spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) => {
                        return;
                    }
                    Ok(count) => count,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        if let Ok(mut state) = thread_shared.state.lock() {
                            state.error = Some(error.to_string());
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
}

struct RawModeGuard {
    disable_on_drop: bool,
}

impl RawModeGuard {
    fn enter(enabled: bool) -> Result<Self> {
        // crossterm tracks only mode changes made by this process; inspect the pty itself so an
        // outer terminal UI's raw mode remains owned by that UI.
        let already_raw = enabled
            && crate::terminal::stdin_is_raw().context("could not inspect terminal input mode")?;
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

    fn outcome(&self, exit_code: u32) -> Result<PtyOutcome> {
        if exit_code == u32::from(SUPERVISED_EXIT_CODE) {
            let (recovery, failure) = supervised_exit_from_tail(&self.tail).with_context(|| {
                    format!(
                        "+k Codex exited with status {SUPERVISED_EXIT_CODE} without usable recovery data"
                    )
                })?;
            return Ok(PtyOutcome::CredentialUnavailable(recovery, failure));
        }
        Ok(PtyOutcome::Exited(u8::try_from(exit_code).unwrap_or(1)))
    }
}

fn supervised_exit_from_tail(tail: &[u8]) -> Result<(ResumeHandoff, CredentialFailure)> {
    let prefix = b"codex+k (";
    let marker = tail
        .split(|byte| *byte == b'\n')
        .rev()
        .find_map(|line| {
            line.windows(prefix.len())
                .rposition(|window| window == prefix)
                .map(|start| &line[start + prefix.len()..])
        })
        .context("+k supervised exit marker was not found in captured output")?;
    let delimiter = b"): supervised exit";
    let delimiter_start = marker
        .windows(delimiter.len())
        .position(|window| window == delimiter)
        .context("+k supervised exit marker was malformed")?;
    let thread_id = std::str::from_utf8(&marker[..delimiter_start])
        .context("+k supervised exit marker thread ID was not UTF-8")?;
    if !valid_uuid(thread_id) {
        bail!("+k supervised exit marker contained invalid thread ID `{thread_id}`");
    }
    let payload = &marker[delimiter_start + delimiter.len()..];
    let Some(payload) = payload.strip_prefix(b" ") else {
        bail!("+k supervised exit marker did not include recovery settings");
    };
    let payload: SupervisedExitPayload = serde_json::Deserializer::from_slice(payload)
        .into_iter()
        .next()
        .context("+k supervised exit marker did not include recovery settings")?
        .context("could not parse +k supervised exit settings")?;
    if payload.version != 3 {
        bail!("unsupported +k supervised exit version {}", payload.version);
    }
    let handoff_path = payload.handoff_path;
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
    let recovery = ResumeHandoff {
        thread_id: thread_id.to_owned(),
        resume_args: handoff.resume_args,
        handoff_path,
    };
    validate_recovery_args(&recovery.resume_args)?;
    Ok((
        recovery,
        CredentialFailure {
            cause: payload.outcome,
            unavailable_until: payload.unavailable_until,
        },
    ))
}

fn validate_recovery_args(args: &[String]) -> Result<()> {
    for argument in args {
        if is_managed_launch_argument(OsStr::new(argument)) {
            bail!("+k supervised exit settings contain a managed credential argument");
        }
    }
    Ok(())
}

fn validate_managed_launch_args(args: &[OsString]) -> Result<()> {
    if args
        .iter()
        .any(|argument| is_managed_launch_argument(argument))
    {
        bail!("managed credential launch arguments may only be supplied by Kai");
    }
    Ok(())
}

fn is_managed_launch_argument(argument: &OsStr) -> bool {
    let argument = argument.to_string_lossy();
    [
        AUTH_FILE_FLAG,
        CREDENTIAL_PROTOCOL_VERSION_FLAG,
        CREDENTIAL_USE_LOCK_FLAG,
        CREDENTIAL_USE_LOCK_MODE_FLAG,
        CREDENTIAL_MUTATION_LOCK_FLAG,
        CREDENTIAL_STARTUP_SOCKET_FLAG,
        CREDENTIAL_STARTUP_NONCE_FLAG,
    ]
    .iter()
    .any(|flag| argument == *flag || argument.starts_with(&format!("{flag}=")))
}

fn read_input_handoff_header(path: &Path) -> Result<InputHandoffHeader> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("could not inspect +k input handoff {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("+k input handoff {} is not a regular file", path.display());
    }
    if metadata.len() > MAX_INPUT_HANDOFF_BYTES {
        bail!(
            "+k input handoff {} is larger than the {} MiB safety limit",
            path.display(),
            MAX_INPUT_HANDOFF_BYTES / (1024 * 1024)
        );
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("could not read +k input handoff {}", path.display()))?;
    let opened = file
        .metadata()
        .with_context(|| format!("could not inspect +k input handoff {}", path.display()))?;
    if !opened.is_file() {
        bail!("+k input handoff {} is not a regular file", path.display());
    }
    #[cfg(unix)]
    {
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
            bail!("+k input handoff {} changed while opening", path.display());
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_HANDOFF_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("could not read +k input handoff {}", path.display()))?;
    if bytes.len() as u64 > MAX_INPUT_HANDOFF_BYTES {
        bail!(
            "+k input handoff {} is larger than the {} MiB safety limit",
            path.display(),
            MAX_INPUT_HANDOFF_BYTES / (1024 * 1024)
        );
    }
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
    fn detects_custom_commit_versions() {
        for version in ["0.154.0-k.ac192cd7", "0.154.0-k.12345678"] {
            assert!(version_is_custom(format!("codex-cli {version}\n").as_bytes()).unwrap());
        }
        for version in [
            "0.153.4+k",
            "0.153.4+release.k",
            "0.154.0",
            "0.154.0-alpha.3",
            "0.154.0-kestrel.ac192cd7",
            "0.154.0-k.ac192cd",
            "0.154.0-k.ac192cd79",
            "0.154.0-k.ac192cdz",
            "0.154.0-k.ac192cd7.extra",
            "0.154.0-k.ac192cd7+metadata",
        ] {
            assert!(!version_is_custom(format!("codex-cli {version}\n").as_bytes()).unwrap());
        }
    }

    #[cfg(unix)]
    #[test]
    fn credential_use_guard_requires_a_private_single_link_file() {
        let root = tempdir().unwrap();
        let use_lock = root.path().join("use.lock");
        fs::write(&use_lock, b"").unwrap();
        fs::set_permissions(&use_lock, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            CredentialUseGuard::try_acquire(&use_lock, LockMode::Shared)
                .unwrap_err()
                .to_string()
                .contains("mode 0600")
        );

        fs::set_permissions(&use_lock, fs::Permissions::from_mode(0o600)).unwrap();
        let alias = root.path().join("alias.lock");
        fs::hard_link(&use_lock, &alias).unwrap();
        assert!(
            CredentialUseGuard::try_acquire(&use_lock, LockMode::Shared)
                .unwrap_err()
                .to_string()
                .contains("one hard link")
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_consumers_coexist_and_exclusive_consumers_exclude_everyone() {
        let root = tempdir().unwrap();
        let path = root.path().join("use.lock");
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let first = CredentialUseGuard::try_acquire(&path, LockMode::Shared)
            .unwrap()
            .unwrap();
        let second = CredentialUseGuard::try_acquire(&path, LockMode::Shared)
            .unwrap()
            .unwrap();
        assert!(
            CredentialUseGuard::try_acquire(&path, LockMode::Exclusive)
                .unwrap()
                .is_none()
        );
        drop(first);
        assert!(
            CredentialUseGuard::try_acquire(&path, LockMode::Exclusive)
                .unwrap()
                .is_none()
        );
        drop(second);
        let deadline = Instant::now() + Duration::from_secs(1);
        let exclusive = loop {
            if let Some(guard) =
                CredentialUseGuard::try_acquire(&path, LockMode::Exclusive).unwrap()
            {
                break guard;
            }
            // Concurrent process-spawning tests briefly inherit descriptors until exec closes them.
            assert!(
                Instant::now() < deadline,
                "use lock remained held after consumers exited"
            );
            thread::sleep(PROCESS_POLL_INTERVAL);
        };
        assert!(
            CredentialUseGuard::try_acquire(&path, LockMode::Shared)
                .unwrap()
                .is_none()
        );
        assert!(
            CredentialUseGuard::try_acquire(&path, LockMode::Exclusive)
                .unwrap()
                .is_none()
        );
        drop(exclusive);
        assert!(
            CredentialUseGuard::try_acquire(&path, LockMode::Shared)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn parses_the_latest_supervised_exit_before_trailing_terminal_output() {
        let old_thread_id = "123e4567-e89b-12d3-a456-426614174001";
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let directory = tempdir().unwrap();
        let handoff_path = directory.path().join("handoff.json");
        fs::write(
            &handoff_path,
            serde_json::to_vec(&serde_json::json!({
                "format": INPUT_HANDOFF_FORMAT,
                "version": INPUT_HANDOFF_VERSION,
                "thread_id": thread_id,
                "resume_args": ["--model", "gpt-5.6", "-c", "service_tier=\"fast\""],
            }))
            .unwrap(),
        )
        .unwrap();
        let payload = serde_json::json!({ "version": 3, "outcome": "quota-exhausted", "unavailable_until": 1_800_000_000, "handoff_path": handoff_path });
        let output = format!(
            concat!(
                "codex+k ({old_thread_id}): supervised exit {payload}\r",
                "old screen text\x1b[?1049l\x1b[?25h",
                "codex+k ({thread_id}): supervised exit {payload}\x1b[?25h\r\n",
                "\r\nlate terminal cleanup"
            ),
            old_thread_id = old_thread_id,
            payload = payload,
            thread_id = thread_id
        );
        assert_eq!(
            supervised_exit_from_tail(output.as_bytes()).unwrap(),
            (
                ResumeHandoff {
                    thread_id: thread_id.to_string(),
                    resume_args: vec![
                        "--model".to_string(),
                        "gpt-5.6".to_string(),
                        "-c".to_string(),
                        "service_tier=\"fast\"".to_string(),
                    ],
                    handoff_path,
                },
                CredentialFailure {
                    cause: CredentialFailureCause::QuotaExhausted,
                    unavailable_until: Some(1_800_000_000)
                }
            )
        );
        assert!(
            supervised_exit_from_tail(
                format!("codex+k (not-a-uuid): supervised exit {payload}\r\n").as_bytes()
            )
            .unwrap_err()
            .to_string()
            .contains("invalid thread ID")
        );
        assert!(
            supervised_exit_from_tail(
                format!("codex+k ({thread_id}): supervised exit\r\n").as_bytes()
            )
            .unwrap_err()
            .to_string()
            .contains("did not include recovery settings")
        );
        assert!(validate_recovery_args(&[AUTH_FILE_FLAG.into(), "/tmp/secret".into()]).is_err());
        assert!(validate_managed_launch_args(&[AUTH_FILE_FLAG.into()]).is_err());
        assert!(validate_managed_launch_args(&["--model".into()]).is_ok());
    }

    #[test]
    fn supervised_status_requires_a_valid_handoff() {
        let mut missing = OutputObservation::new();
        missing.process(b"ordinary Codex output\r\n");
        let missing_error = missing
            .outcome(u32::from(SUPERVISED_EXIT_CODE))
            .unwrap_err();
        assert!(
            format!("{missing_error:#}").contains(
                "+k Codex exited with status 75 without usable recovery data: +k supervised exit marker was not found"
            )
        );

        let mut malformed = OutputObservation::new();
        malformed.process(
            b"codex+k (123e4567-e89b-12d3-a456-426614174000): supervised exit not-json\r\n",
        );
        let malformed_error = malformed
            .outcome(u32::from(SUPERVISED_EXIT_CODE))
            .unwrap_err();
        assert!(
            format!("{malformed_error:#}").contains("could not parse +k supervised exit settings")
        );
    }

    #[test]
    fn normal_status_ignores_marker_looking_output() {
        let mut observation = OutputObservation::new();
        observation.process(
            b"codex+k (123e4567-e89b-12d3-a456-426614174000): supervised exit not-json\r\n",
        );

        assert_eq!(observation.outcome(0).unwrap(), PtyOutcome::Exited(0));
    }

    #[test]
    fn recovery_command_resumes_the_exact_thread_with_reported_settings() {
        let args = recovery_args(ResumeHandoff {
            thread_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            resume_args: vec![
                "--model".to_string(),
                "gpt-5.6".to_string(),
                CONFIG_OVERRIDE_FLAG.to_string(),
                "service_tier=\"fast\"".to_string(),
                CONFIG_OVERRIDE_FLAG.to_string(),
                "model_reasoning_effort=\"xhigh\"".to_string(),
            ],
            handoff_path: PathBuf::from("/tmp/session.codex+k-handoff.json"),
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
                CONFIG_OVERRIDE_FLAG,
                "service_tier=\"fast\"",
                CONFIG_OVERRIDE_FLAG,
                "model_reasoning_effort=\"xhigh\"",
            ]
        );
    }

    #[test]
    fn supervised_exit_loads_resume_settings_from_the_handoff() {
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
            "version": 3,
            "outcome": "credential-invalid",
            "handoff_path": handoff_path,
        });
        let output = format!("codex+k ({thread_id}): supervised exit {marker}\r\n");
        let mut observation = OutputObservation::new();
        observation.process(output.as_bytes());

        assert_eq!(
            observation
                .outcome(u32::from(SUPERVISED_EXIT_CODE))
                .unwrap(),
            PtyOutcome::CredentialUnavailable(
                ResumeHandoff {
                    thread_id: thread_id.to_string(),
                    resume_args: vec!["--model".to_string(), "gpt-5.6".to_string()],
                    handoff_path,
                },
                CredentialFailure {
                    cause: CredentialFailureCause::CredentialInvalid,
                    unavailable_until: None
                }
            )
        );
    }

    #[test]
    fn supervised_exit_rejects_a_handoff_for_another_thread() {
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
            "version": 3,
            "outcome": "credential-invalid",
            "handoff_path": handoff_path,
        });
        let output =
            format!("codex+k (123e4567-e89b-12d3-a456-426614174000): supervised exit {marker}\r\n");

        assert!(
            supervised_exit_from_tail(output.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("belongs to thread")
        );
    }

    #[test]
    fn recovery_command_passes_the_handoff_back_to_codex() {
        let args = recovery_args(ResumeHandoff {
            thread_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            resume_args: vec!["--model".to_string(), "gpt-5.6".to_string()],
            handoff_path: PathBuf::from("/tmp/session.codex+k-handoff.json"),
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

    #[cfg(unix)]
    #[test]
    fn supervised_codex_restarts_exact_session_and_preserves_cwd() {
        let root = tempdir().unwrap();
        let binary = root.path().join("codex");
        let paths = CredentialPaths {
            auth_file: root.path().join("auth.json"),
            credential_use_lock: root.path().join("use.lock"),
            credential_use_lock_mode: LockMode::Exclusive,
            credential_mutation_lock: root.path().join("mutation.lock"),
            available_file: root.path().join("available"),
        };
        for path in [
            &paths.auth_file,
            &paths.credential_use_lock,
            &paths.credential_mutation_lock,
            &paths.available_file,
        ] {
            fs::write(path, b"{}").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let thread_id = "123e4567-e89b-12d3-a456-426614174000";
        let handoff_path = root.path().join("handoff.json");
        fs::write(
            &handoff_path,
            serde_json::to_vec(&serde_json::json!({
                "format": INPUT_HANDOFF_FORMAT,
                "version": INPUT_HANDOFF_VERSION,
                "thread_id": thread_id,
                "resume_args": ["--model", "gpt-5.6", "-c", "service_tier=\"default\""],
            }))
            .unwrap(),
        )
        .unwrap();
        let payload = serde_json::json!({
            "version": 3, "outcome": "quota-exhausted",
            "unavailable_until": 1_800_000_000, "handoff_path": handoff_path,
        });
        fs::write(
            &binary,
            format!(r#"#!/usr/bin/env python3
import array, fcntl, json, os, pathlib, socket, sys
args = sys.argv[1:]
if args == ["--version"]:
    print("codex-cli 0.154.0-k.ac192cd7")
    sys.exit(0)
def value(flag):
    return args[args.index(flag) + 1]
assert value("--credential-protocol-version") == "2"
assert value("--credential-use-lock-mode") == "exclusive"
startup = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
startup.connect(value("--credential-startup-socket"))
nonce = value("--credential-startup-nonce")
startup.sendall(("READY " + nonce + "\n").encode())
expected = ("GO " + nonce + "\n").encode()
frame, ancillary, flags, _ = startup.recvmsg(len(expected), socket.CMSG_SPACE(array.array("i").itemsize))
while len(frame) < len(expected):
    frame += startup.recv(len(expected) - len(frame))
assert frame == expected
assert len(ancillary) == 1
level, kind, data = ancillary[0]
assert level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS
descriptor = array.array("i", data)[0]
assert os.fstat(descriptor).st_ino == os.stat(value("--credential-use-lock")).st_ino
with open(value("--credential-use-lock"), "r+") as probe:
    try:
        fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        pass
    else:
        raise AssertionError("exclusive use lock was not retained")
root = pathlib.Path({root})
with (root / "arguments").open("a") as output:
    output.write(json.dumps(args) + "\n")
with (root / "working-directories").open("a") as output:
    output.write(os.getcwd() + "\n")
state = root / "state"
if not state.exists():
    state.write_text("1")
    print("codex+k ({thread_id}): supervised exit " + {payload})
    print("\x1b[?25h\nlate terminal cleanup")
    sys.exit(75)
state.write_text("2")
"#,
                root = serde_json::to_string(root.path()).unwrap(),
                payload = serde_json::to_string(&payload.to_string()).unwrap(),
            ),
        ).unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();

        let launcher = Launcher::from_binary(binary).unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut selections = Vec::new();
        let code = launcher
            .run_supervised_with_io(
                vec![APPROVAL_BYPASS_FLAG.into()],
                root.path(),
                ServiceTier::Fast,
                SupervisedEnvironment::new(root.path(), root.path()),
                |previous| {
                    selections.push(previous.map(|(_, failure)| *failure));
                    Ok(Some(SelectedCredential {
                        paths: paths.clone(),
                        guard: CredentialUseGuard::try_acquire(
                            &paths.credential_use_lock,
                            LockMode::Exclusive,
                        )?
                        .unwrap(),
                    }))
                },
                SupervisedIo {
                    input: Box::new(io::empty()),
                    output: Arc::new(Mutex::new(Box::new(SharedWriter(Arc::clone(&captured))))),
                    raw_terminal: false,
                },
            )
            .unwrap();

        assert_eq!(code, 0);
        assert_eq!(
            selections,
            [
                None,
                Some(CredentialFailure {
                    cause: CredentialFailureCause::QuotaExhausted,
                    unavailable_until: Some(1_800_000_000),
                }),
            ]
        );
        assert_eq!(fs::read_to_string(root.path().join("state")).unwrap(), "2");
        let calls = fs::read_to_string(root.path().join("arguments"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Vec<String>>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[0]
                .windows(2)
                .any(|args| args == ["-c", FAST_SERVICE_TIER_OVERRIDE])
        );
        assert_eq!(&calls[1][..2], ["resume", thread_id]);
        assert!(
            calls[1]
                .windows(2)
                .any(|args| args == [RESTORE_INPUT_HANDOFF_FLAG, handoff_path.to_str().unwrap()])
        );
        assert!(
            calls[1]
                .windows(2)
                .any(|args| args == ["-c", "service_tier=\"default\""])
        );
        assert_eq!(
            fs::read_to_string(root.path().join("working-directories")).unwrap(),
            format!("{0}\n{0}\n", root.path().display()),
        );
        assert!(
            CredentialUseGuard::try_acquire(&paths.credential_use_lock, LockMode::Exclusive)
                .unwrap()
                .is_some()
        );
    }
}
