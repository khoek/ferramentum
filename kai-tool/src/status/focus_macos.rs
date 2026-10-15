//! Beta macOS activation. Match the session's exact tty, including inactive tabs/panes.
use super::attention::{Focused, Target, now_ms};
use super::{command, process::ProcessIdentity, process_macos as process, worker::Cancellation};
use anyhow::{Context, Result, bail, ensure};
use std::time::Duration;

pub(super) fn focus(identity: ProcessIdentity, cancel: &Cancellation) -> Result<()> {
    cancel.check()?;
    let (chain, script) = checked_terminal(identity)?;
    let mut files = process::files(&[identity.pid], cancel)?;
    let tty = files
        .remove(&identity.pid)
        .and_then(|files| files.tty)
        .context("Cannot identify this session's terminal. Press Ctrl+E for terminal details.")?;
    ensure!(
        process::ancestry(identity)? == chain,
        "The session changed. Refresh and try again."
    );
    let reply = command::run("/usr/bin/osascript", &["-e", script, &tty], Duration::from_secs(8), cancel)
        .context("Cannot switch terminal windows. Allow terminal automation in System Settings > Privacy & Security > Automation, then try again.")?;
    match reply.trim_ascii() {
        b"focused" => Ok(()),
        b"ambiguous" => bail!(
            "Several terminal sessions share this tty. Press Ctrl+E to locate the session manually."
        ),
        _ => bail!("The terminal session is no longer available. Refresh and try again."),
    }
}

fn checked_terminal(
    identity: ProcessIdentity,
) -> Result<(Vec<super::process::Ancestor>, &'static str)> {
    let chain =
        process::ancestry(identity).context("The session changed. Refresh and try again.")?;
    let args = process::process_arguments(identity.pid)?;
    let environment: Vec<_> = args.environment.split(|byte| *byte == 0).collect();
    ensure!(
        !environment
            .iter()
            .any(|entry| [b"TMUX=".as_slice(), b"STY=", b"ZELLIJ="]
                .iter()
                .any(|key| entry.starts_with(key) && entry.len() > key.len()))
            && !chain
                .iter()
                .any(|ancestor| ancestor.name.starts_with("tmux")
                    || matches!(ancestor.name.as_str(), "screen" | "zellij")),
        "This session needs a multiplexer integration before Kai can select its pane. Press Ctrl+E for terminal details."
    );
    let program = environment
        .iter()
        .find_map(|entry| entry.strip_prefix(b"TERM_PROGRAM="))
        .unwrap_or_default();
    let script = match program {
        b"Apple_Terminal" => include_str!("scripts/terminal.applescript"),
        b"iTerm.app" => include_str!("scripts/iterm.applescript"),
        _ => bail!(
            "Window switching on macOS currently supports Terminal and iTerm2. Press Ctrl+E for terminal details."
        ),
    };
    Ok((chain, script))
}

pub(super) fn focused(targets: Vec<Target>, cancel: &Cancellation) -> Result<Focused> {
    let at_ms = now_ms();
    let candidates: Vec<_> = targets
        .into_iter()
        .filter_map(|target| {
            checked_terminal(target.identity)
                .ok()
                .map(|(chain, script)| (target, chain, script))
        })
        .collect();
    let mut identities = Vec::new();
    // Query only applications with supported target sessions. Referencing an absent
    // application in AppleScript can otherwise prompt the user to locate it.
    for (activation, observation) in [
        (
            include_str!("scripts/terminal.applescript"),
            include_str!("scripts/terminal-focused.applescript"),
        ),
        (
            include_str!("scripts/iterm.applescript"),
            include_str!("scripts/iterm-focused.applescript"),
        ),
    ] {
        cancel.check()?;
        if !candidates
            .iter()
            .any(|(_, _, script)| *script == activation)
        {
            continue;
        }
        let reply = command::run(
            "/usr/bin/osascript",
            &["-e", observation],
            Duration::from_secs(1),
            cancel,
        )?;
        let tty = std::str::from_utf8(&reply)?.trim();
        for (target, chain, script) in &candidates {
            if *script == activation
                && tty.starts_with("/dev/tty")
                && target.tty.as_deref() == Some(tty)
                && process::ancestry(target.identity).ok().as_ref() == Some(chain)
            {
                identities.push(target.identity);
            }
        }
    }
    Ok(Focused { at_ms, identities })
}
