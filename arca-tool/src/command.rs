use std::io::{self, IsTerminal, Write};
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, bail};
pub fn ensure_command_available(command: &str) -> Result<()> {
    if Command::new(command).arg("--version").output().is_ok() {
        Ok(())
    } else {
        bail!("Missing required command `{command}` in `PATH`.");
    }
}

pub fn run_command_output(command: &mut Command, context: &str) -> Result<Output> {
    let output = command
        .output()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !output.status.success() {
        bail!("{}", failure_message(&output, context));
    }
    Ok(output)
}

pub fn run_command_status(command: &mut Command, context: &str) -> Result<()> {
    run_command_output(command, context).map(|_| ())
}

pub fn run_command_status_streaming(command: &mut Command, context: &str) -> Result<()> {
    if !(io::stdout().is_terminal() || io::stderr().is_terminal()) {
        return run_command_status(command, context);
    }

    command.stdin(Stdio::inherit());
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());
    let status = command
        .status()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !status.success() {
        bail!("Failed to {context}: exit status {status}");
    }
    Ok(())
}

pub fn run_command_text(command: &mut Command, context: &str) -> Result<String> {
    let output = run_command_output(command, context)?;
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("Non-UTF8 command output while trying to {context}"))?;
    Ok(stdout.trim().to_owned())
}

pub fn run_command_status_with_input(
    command: &mut Command,
    context: &str,
    input: &[u8],
) -> Result<()> {
    command.stdin(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open stdin while trying to {context}"))?;
    stdin
        .write_all(input)
        .with_context(|| format!("Failed to write command input while trying to {context}"))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .with_context(|| format!("Failed waiting for command while trying to {context}"))?;
    if !output.status.success() {
        bail!("{}", failure_message(&output, context));
    }
    Ok(())
}

fn failure_message(output: &Output, context: &str) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("exit status {}", output.status)
    };
    format!("Failed to {context}: {detail}")
}
