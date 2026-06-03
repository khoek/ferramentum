use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process::{Command, Output};

use anyhow::{Context, Result, bail};

use crate::io;

pub fn ensure_available() -> Result<()> {
    which::which("git").context("`git` is required but was not found on PATH")?;
    Ok(())
}

pub fn init_channel(path: &Path) -> Result<()> {
    ensure_available()?;
    io::ensure_dir(path)?;
    io::write_text_if_missing(&path.join(".think-channel"), "think channel artifact log\n")?;
    if !path.join(".git").exists() {
        run(path, ["init", "-b", "main"])?;
    }
    if !has_commits(path)? {
        run(path, ["add", "-A"])?;
        commit(path, "chore: initialize think channel").with_context(|| {
            format!(
                "Failed to create the initial commit in channel `{}`. Fix git identity and rerun the command.",
                path.display()
            )
        })?;
    }
    Ok(())
}

pub fn commit_all(path: &Path, message: &str) -> Result<bool> {
    if !is_dirty(path)? {
        return Ok(false);
    }
    run(path, ["add", "-A"])?;
    commit(path, message).with_context(|| {
        format!(
            "Failed to commit changes in `{}`. Fix git identity and commit manually if needed.",
            path.display()
        )
    })?;
    Ok(true)
}

fn has_commits(path: &Path) -> Result<bool> {
    Ok(status(path, ["rev-parse", "--verify", "HEAD"])?.success())
}

fn is_dirty(path: &Path) -> Result<bool> {
    Ok(!text(path, ["status", "--porcelain"])?.trim().is_empty())
}

fn commit(path: &Path, message: &str) -> Result<()> {
    run(
        path,
        [
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "commit",
            "-m",
            message,
        ],
    )
}

fn run<I, S>(path: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args = collect_args(args);
    let output = output_with_args(path, &args)?;
    if output.status.success() {
        return Ok(());
    }
    bail_git(path, &args, &output)
}

fn status<I, S>(path: &Path, args: I) -> Result<std::process::ExitStatus>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Ok(output_with_args(path, &collect_args(args))?.status)
}

fn text<I, S>(path: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args = collect_args(args);
    let output = output_with_args(path, &args)?;
    if !output.status.success() {
        return bail_git(path, &args, &output);
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn collect_args<I, S>(args: I) -> Vec<OsString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    args.into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect()
}

fn output_with_args(path: &Path, args: &[OsString]) -> Result<Output> {
    Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .with_context(|| format!("Failed to run git in `{}`", path.display()))
}

fn bail_git<T>(path: &Path, args: &[OsString], output: &Output) -> Result<T> {
    let command = args
        .iter()
        .map(|arg| arg.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    bail!(
        "git -C {} {} failed\nstdout:\n{}\nstderr:\n{}",
        path.display(),
        command,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
