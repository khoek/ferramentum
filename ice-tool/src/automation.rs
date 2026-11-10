//! Invocation policy shared by prompts, subprocesses and recovery diagnostics.
use std::fmt;
use std::io::IsTerminal;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};

use crate::cli::{Cli, Commands};

static NON_INTERACTIVE: OnceLock<bool> = OnceLock::new();
static STARTUP_TIMEOUT: OnceLock<u64> = OnceLock::new();
static RECOVERY: Mutex<Option<Value>> = Mutex::new(None);

pub(crate) fn initialize(cli: &Cli) {
    let _ = NON_INTERACTIVE.set(cli.non_interactive || !std::io::stdin().is_terminal());
    if non_interactive() {
        // SAFETY: main calls this once, before initializing providers, starting
        // workers, or installing signal handlers. This also covers subprocesses
        // launched inside credential helpers that do not use our wrappers.
        unsafe {
            std::env::set_var("CLOUDSDK_CORE_DISABLE_PROMPTS", "1");
            std::env::set_var("AWS_CLI_AUTO_PROMPT", "off");
            std::env::set_var("AWS_PAGER", "");
            std::env::set_var("SSH_ASKPASS_REQUIRE", "never");
        }
    }
    let timeout = match &cli.command {
        Commands::Create(args) => args.startup_timeout,
        _ => 900,
    };
    let _ = STARTUP_TIMEOUT.set(timeout);
}

pub(crate) fn non_interactive() -> bool {
    NON_INTERACTIVE.get().copied().unwrap_or(false)
}

pub(crate) fn startup_timeout() -> Duration {
    Duration::from_secs(STARTUP_TIMEOUT.get().copied().unwrap_or(900))
}

#[derive(Debug)]
pub(crate) struct AgentError {
    pub(crate) code: &'static str,
    pub(crate) details: Value,
    message: String,
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for AgentError {}

pub(crate) fn error(
    code: &'static str,
    message: impl Into<String>,
    details: Value,
) -> anyhow::Error {
    AgentError {
        code,
        details,
        message: message.into(),
    }
    .into()
}

pub(crate) fn require_interactive(message: &str) -> Result<()> {
    if non_interactive() {
        return Err(error("interaction_required", message, json!({})));
    }
    Ok(())
}

pub(crate) fn require_running(instance: impl ToString) -> Result<()> {
    if non_interactive() {
        return Err(error(
            "instance_stopped",
            "Instance is stopped. Run `ice start` explicitly before requesting a connection.",
            json!({"instance_id": instance.to_string(), "next_command": "start"}),
        ));
    }
    Ok(())
}

/// Record known resource identifiers before waiting or performing follow-up work.
/// A failed command must not encourage blindly issuing another create request.
pub(crate) fn recovery(stage: &str, fields: Value) {
    let mut state = RECOVERY.lock().unwrap_or_else(|err| err.into_inner());
    let value = state.get_or_insert_with(|| json!({}));
    value["stage"] = json!(stage);
    if let Some(fields) = fields.as_object() {
        for (key, field) in fields {
            value[key] = field.clone();
        }
    }
}

pub(crate) fn recovery_details() -> Value {
    RECOVERY
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone()
        .unwrap_or(Value::Null)
}

pub(crate) fn prepare_command(command: &mut Command) {
    if non_interactive() {
        command
            .stdin(Stdio::null())
            .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
            .env("AWS_CLI_AUTO_PROMPT", "off")
            .env("AWS_PAGER", "")
            .env("SSH_ASKPASS_REQUIRE", "never");
        if std::path::Path::new(command.get_program())
            .file_name()
            .is_some_and(|name| name == "gcloud")
        {
            let args = command.get_args().collect::<Vec<_>>();
            let transport = if args.first().is_some_and(|arg| *arg == "compute") {
                args.get(1)
                    .and_then(|arg| arg.to_str())
                    .filter(|arg| matches!(*arg, "ssh" | "scp"))
            } else {
                None
            };
            let flag = transport.map(|transport| format!("--{transport}-flag="));
            if let Some(flag) = flag
                && !args
                    .iter()
                    .any(|arg| arg.to_string_lossy().contains("BatchMode=yes"))
            {
                for pair in ssh_options().chunks_exact(2) {
                    command.arg(format!("{flag}-o{}", pair[1]));
                }
            }
        }
    }
}

pub(crate) fn ssh_options() -> Vec<String> {
    if !non_interactive() {
        return Vec::new();
    }
    [
        "BatchMode=yes",
        "ConnectTimeout=20",
        "ServerAliveInterval=15",
        "ServerAliveCountMax=3",
    ]
    .into_iter()
    .flat_map(|option| ["-o".to_owned(), option.to_owned()])
    .collect()
}

pub(crate) fn validate_command(command: &Commands) -> Result<()> {
    if !non_interactive() {
        return Ok(());
    }
    match command {
        Commands::Create(args) if args.custom => Err(error(
            "invalid_arguments",
            "--custom requires interactive use; supply filter flags instead.",
            json!({"flags": ["--custom"]}),
        )),
        Commands::Shell(args) if !args.print_creds => Err(error(
            "interaction_required",
            "Use `shell --print-creds` for non-interactive connection details.",
            json!({"required_flags": ["--print-creds"]}),
        )),
        _ => Ok(()),
    }
}
