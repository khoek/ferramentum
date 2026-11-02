use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use semver::Version;
use serde::{Deserialize, Serialize};

const SESSION: &str = "kai";
const ERROR_HOLD_ENV: &str = "KAI_ZELLIJ_ERROR_HOLD";
const CONFIG: &str = "mirror_session false\nshow_startup_tips false\nshow_release_notes false\n\
    session_serialization false\non_force_close \"detach\"\n";
const PANE_ENV: &[&str] = &[
    "ZELLIJ",
    "ZELLIJ_SESSION_NAME",
    "ZELLIJ_PANE_ID",
    "ZELLIJ_ORIGIN_PANE_ID",
];

#[derive(Serialize, Deserialize)]
struct LaunchContext {
    arguments: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
    cwd: PathBuf,
}

struct Zellij {
    binary: PathBuf,
    config: PathBuf,
}

#[derive(Deserialize)]
struct Pane {
    id: u32,
    is_plugin: bool,
    tab_id: usize,
}

pub(crate) fn is_interactive() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

pub(crate) fn hold_errors(launch_context: bool) -> bool {
    launch_context || env::var_os(ERROR_HOLD_ENV).is_some()
}

pub(crate) fn wait_for_error() {
    if !io::stdin().is_terminal() {
        return;
    }

    let _ = crate::terminal::write_stderr_line("Press Enter to close this Kai pane.");
    let mut input = [0u8; 1];
    let _ = io::stdin().read(&mut input);
}

pub(crate) fn detect() -> Option<PathBuf> {
    let binary = which::which("zellij").ok()?;
    let output = Command::new(&binary).arg("--version").output().ok()?;
    output.status.success().then_some(())?;
    is_custom_version(&output.stdout).then_some(binary)
}

pub(crate) fn launch(cwd: &Path, binary: PathBuf) -> Result<ExitCode> {
    let runtime = runtime_dir()?;
    let zellij = Zellij {
        binary,
        config: runtime.join("zellij.kdl"),
    };
    let help = zellij
        .command()
        .args(["attach", "--help"])
        .output()
        .context("could not inspect Zellij's attach support")?;
    if !help.status.success() || !String::from_utf8_lossy(&help.stdout).contains("--pane") {
        bail!(
            "interactive Kai requires the khoek/zellij fork with `attach --pane`; see kai-tool/README.md"
        );
    }

    let context_dir = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(&runtime)?;
    let context_path = context_dir.path().join("launch.json");
    serde_json::to_writer(
        private_file(&context_path)?,
        &LaunchContext {
            arguments: env::args_os().skip(1).collect(),
            environment: env::vars_os()
                .filter(|(key, _)| !PANE_ENV.iter().any(|name| key == name))
                .collect(),
            cwd: cwd.to_owned(),
        },
    )
    .context("could not save the Kai launch context")?;

    let lock = private_file(&runtime.join("launch.lock"))?;
    fs2::FileExt::lock_exclusive(&lock).context("could not lock Kai session startup")?;
    if !zellij.config.exists() {
        use std::io::Write;
        private_file(&zellij.config)?.write_all(CONFIG.as_bytes())?;
    }

    let worker = env::current_exe().context("could not locate Kai's executable")?;
    let tab = if zellij.session_exists()? {
        zellij
            .output(
                zellij
                    .action()
                    .args([
                        "new-tab",
                        "--no-focus",
                        "--start-suspended",
                        "--close-on-exit",
                        "--cwd",
                    ])
                    .arg(cwd)
                    .arg("--")
                    .arg(&worker)
                    .arg("--launch-context")
                    .arg(&context_path),
            )?
            .trim()
            .parse::<usize>()
            .context("Zellij did not return a tab ID")?
    } else {
        zellij.output(
            zellij
                .command()
                .current_dir(cwd)
                .args([
                    "attach",
                    "--create-background",
                    SESSION,
                    "--start-suspended",
                    "--close-on-exit",
                    "--",
                ])
                .arg(&worker)
                .arg("--launch-context")
                .arg(&context_path),
        )?;
        0
    };

    let prepared = (|| -> Result<u32> {
        let pane = zellij.wait_for_pane(tab)?;
        let label = format!(
            "{} · {}",
            cwd.file_name().unwrap_or(cwd.as_os_str()).to_string_lossy(),
            pane
        );
        zellij.output(
            zellij
                .action()
                .args(["rename-tab-by-id", &tab.to_string(), &label]),
        )?;
        Ok(pane)
    })();
    fs2::FileExt::unlock(&lock).context("could not release Kai session startup lock")?;
    let pane = match prepared {
        Ok(pane) => pane,
        Err(error) => {
            zellij.close_tab(tab);
            return Err(error);
        }
    };

    let status = zellij
        .command()
        .args(["attach", SESSION, "--pane", &pane.to_string()])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("could not attach to the Kai pane");
    // The worker consumes its context before running. A remaining context means attachment
    // failed before the suspended command started, so this launch owns the abandoned tab.
    if context_path.exists() {
        zellij.close_tab(tab);
    }
    Ok(exit_code(status?))
}

pub(crate) fn run_worker(path: &Path) -> Result<ExitCode> {
    let context: LaunchContext =
        serde_json::from_reader(File::open(path).context("could not open the Kai launch context")?)
            .context("invalid Kai launch context")?;
    fs::remove_file(path).context("could not remove the consumed Kai launch context")?;
    if let Some(parent) = path.parent() {
        fs::remove_dir(parent).context("could not remove the Kai launch directory")?;
    }
    let mut command = Command::new(env::current_exe()?);
    command
        .arg("--internal-run")
        .args(context.arguments)
        .current_dir(context.cwd)
        .env_clear()
        .envs(context.environment);
    // The worker's supervisor may fail after it has consumed the launch context. Mark that
    // process so the top-level error handler can keep this close-on-exit pane visible.
    command.env(ERROR_HOLD_ENV, "1");
    for name in PANE_ENV.iter().copied().chain(["TERM"]) {
        if let Some(value) = env::var_os(name) {
            command.env(name, value);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(command.exec()).context("could not execute the Kai supervisor")
    }
    #[cfg(not(unix))]
    Ok(exit_code(
        command
            .status()
            .context("could not execute the Kai supervisor")?,
    ))
}

impl Zellij {
    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command.arg("--config").arg(&self.config);
        for name in PANE_ENV {
            command.env_remove(name);
        }
        command
    }

    fn action(&self) -> Command {
        let mut command = self.command();
        command.args(["--session", SESSION, "action"]);
        command
    }

    fn output(&self, command: &mut Command) -> Result<String> {
        let output = command
            .stdin(Stdio::null())
            .output()
            .context("could not run Zellij")?;
        if !output.status.success() {
            bail!(
                "Zellij failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).context("Zellij returned invalid UTF-8")
    }

    fn session_exists(&self) -> Result<bool> {
        let output = self
            .command()
            .args(["list-sessions", "--no-formatting"])
            .output()
            .context("could not list Zellij sessions")?;
        if !output.status.success() && output.status.code() != Some(1) {
            bail!(
                "could not list Zellij sessions: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if line.split_whitespace().next() == Some(SESSION) {
                if line.contains("EXITED") {
                    bail!(
                        "an exited Zellij session named `kai` already exists; remove it with `zellij delete-session kai`"
                    );
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn wait_for_pane(&self, tab: usize) -> Result<u32> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let panes: Vec<Pane> =
                serde_json::from_str(&self.output(self.action().args(["list-panes", "--json"]))?)
                    .context("Zellij returned invalid pane metadata")?;
            let mut matching = panes
                .into_iter()
                .filter(|pane| pane.tab_id == tab && !pane.is_plugin);
            if let Some(pane) = matching.next() {
                if matching.next().is_some() {
                    bail!("Kai's new tab unexpectedly contains multiple terminal panes");
                }
                return Ok(pane.id);
            }
            if Instant::now() >= deadline {
                bail!("Zellij did not create Kai's command pane within 10 seconds");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn close_tab(&self, tab: usize) {
        let _ = self
            .action()
            .args(["close-tab", "--tab-id", &tab.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("could not open {}", path.display()))
}

fn runtime_dir() -> Result<PathBuf> {
    let base = env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            #[cfg(unix)]
            {
                env::temp_dir().join(format!("kai-{}", unsafe { libc::geteuid() }))
            }
            #[cfg(not(unix))]
            {
                env::temp_dir().join("kai")
            }
        });
    let directory = base.join("kai");
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(&directory)
        .context("could not create Kai's runtime directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            bail!(
                "Kai runtime directory must be private and owned by the current user: {}",
                directory.display()
            );
        }
    }
    Ok(directory)
}

fn exit_code(status: ExitStatus) -> ExitCode {
    ExitCode::from(
        status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .unwrap_or(1),
    )
}

fn is_custom_version(stdout: &[u8]) -> bool {
    let Ok(output) = std::str::from_utf8(stdout) else {
        return false;
    };
    let Some(raw_version) = output.split_ascii_whitespace().next_back() else {
        return false;
    };
    let Ok(version) = Version::parse(raw_version.trim_start_matches('v')) else {
        return false;
    };
    version
        .pre
        .as_str()
        .strip_prefix("k.")
        .is_some_and(|commit| {
            commit.len() == 8 && commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

#[cfg(test)]
mod tests {
    use super::is_custom_version;

    #[test]
    fn detects_the_kai_zellij_build() {
        assert!(is_custom_version(b"zellij 0.46.0-k.5931af3d\n"));
        assert!(!is_custom_version(b"zellij 0.46.0\n"));
        assert!(!is_custom_version(b"zellij 0.46.0-k.5931af\n"));
        assert!(!is_custom_version(b"not zellij\n"));
    }
}
