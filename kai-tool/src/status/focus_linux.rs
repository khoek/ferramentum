//! Desktop backends live here; the picker only supplies a stable process identity.
use super::attention::{Focused, Target, now_ms};
use super::command;
use super::process::{Ancestor, ProcessIdentity};
use super::process_linux as process;
use super::worker::Cancellation;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{ffi::OsString, path::Path, time::Duration};

const DETAILS: &str = "Press Ctrl+E for terminal details.";
const PANE_MESSAGE: &str = "This terminal needs a tab/pane integration before Kai can switch to it. Linux switching currently supports separate Foot, Alacritty, xterm, st, or urxvt windows. Press Ctrl+E for terminal details to switch manually.";
const CHANGED: &str = "The session or terminal changed. Refresh and try again.";

#[derive(Debug, PartialEq, Eq)]
enum Backend {
    Sway,
    Hyprland,
    Gnome,
    X11,
}

impl Backend {
    fn current() -> Result<Self> {
        Self::detect(
            std::env::var_os("SWAYSOCK"),
            &std::env::var("XDG_CURRENT_DESKTOP").unwrap_or_default(),
            std::env::var("XDG_SESSION_TYPE").is_ok_and(|value| value == "wayland")
                || std::env::var_os("WAYLAND_DISPLAY").is_some(),
            std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE").is_some(),
            std::env::var_os("DISPLAY").is_some(),
        )
    }

    fn detect(
        socket: Option<OsString>,
        desktop: &str,
        wayland: bool,
        hyprland: bool,
        x11: bool,
    ) -> Result<Self> {
        let desktop = desktop.to_ascii_lowercase();
        if wayland && desktop.split(':').any(|name| name.contains("gnome")) {
            return Ok(Self::Gnome);
        }
        if socket.is_some_and(|socket| !socket.is_empty()) {
            return Ok(Self::Sway);
        }
        if desktop.split(':').any(|name| name == "sway") {
            bail!(
                "Cannot connect to Sway: SWAYSOCK is missing. Open the dashboard from a terminal in Sway."
            );
        }
        if hyprland || desktop == "hyprland" {
            return Ok(Self::Hyprland);
        }
        if x11 && !wayland {
            return Ok(Self::X11);
        }
        bail!(
            "Window switching is unavailable on this desktop. Supported backends: Sway, Hyprland, X11, and GNOME with the Kai Window Focus extension. {DETAILS}"
        )
    }
}

pub(super) fn focus(identity: ProcessIdentity, cancel: &Cancellation) -> Result<()> {
    cancel.check()?;
    let backend = Backend::current()?;
    match backend {
        Backend::Sway => focus_sway(Path::new("/proc"), identity, |kind, payload| {
            let reply = command::run("swaymsg", &["-r", "-t", if kind == 4 { "get_tree" } else { "command" }, payload], Duration::from_secs(2), cancel)
                    .context("Cannot contact Sway. Check that swaymsg is installed and run the dashboard inside Sway.")?;
            Ok(serde_json::from_slice(&reply)?)
        }),
        Backend::Hyprland => focus_hyprland(identity, cancel),
        Backend::Gnome => focus_gnome(identity, cancel),
        Backend::X11 => focus_x11(identity, cancel),
    }
}

/// Read desktop focus without activating a window. Resolve the same unambiguous,
/// birth-checked terminal ancestry used by Enter-to-focus.
pub(super) fn focused(targets: Vec<Target>, cancel: &Cancellation) -> Result<Focused> {
    focused_with(
        Path::new("/proc"),
        &targets,
        Backend::current()?,
        |program, args| command::run(program, args, Duration::from_secs(1), cancel),
    )
}

fn focused_with(
    proc_root: &Path,
    targets: &[Target],
    backend: Backend,
    mut run: impl FnMut(&str, &[&str]) -> Result<Vec<u8>>,
) -> Result<Focused> {
    let at_ms = now_ms();
    let chains: Vec<_> = targets
        .iter()
        .filter_map(|target| {
            checked_chain(proc_root, target.identity)
                .ok()
                .map(|chain| (target.identity, chain))
        })
        .collect();
    let tree: Node = match backend {
        Backend::Sway => serde_json::from_slice(&run("swaymsg", &["-r", "-t", "get_tree"])?)?,
        Backend::Hyprland => {
            let mut tree = hyprland_tree(&run("hyprctl", &["-j", "clients"])?)?;
            let active: Value = serde_json::from_slice(&run("hyprctl", &["-j", "activewindow"])?)?;
            if let (Some(address), Some(pid)) = (active["address"].as_str(), active["pid"].as_u64())
            {
                let id = hex_id(address)?;
                for node in &mut tree.nodes {
                    node.focused = node.id == id && node.pid.map(u64::from) == Some(pid);
                }
            }
            tree
        }
        Backend::X11 => {
            let before = run("xprop", &["-root", "_NET_ACTIVE_WINDOW"])?;
            let mut tree = x11_tree(&run("wmctrl", &["-lp"])?)?;
            let active = run("xprop", &["-root", "_NET_ACTIVE_WINDOW"])?;
            let id = std::str::from_utf8(&active)?
                .split_whitespace()
                .last()
                .and_then(|id| hex_id(id).ok());
            for node in &mut tree.nodes {
                node.focused = before == active && Some(node.id) == id;
            }
            tree
        }
        Backend::Gnome => {
            let reply = run(
                "gdbus",
                &[
                    "call",
                    "--session",
                    "--dest",
                    "org.gnome.Shell",
                    "--object-path",
                    "/org/gnome/Shell/Extensions/KaiWindowFocus",
                    "--method",
                    "org.gnome.Shell.Extensions.KaiWindowFocus.GetWindows",
                ],
            )
            .context("Update and enable the Kai Window Focus GNOME extension for focus tracking")?;
            // The extension returns numeric IDs/PIDs and booleans only, so its JSON
            // needs no GVariant string unescaping. Never interpret titles or shell text.
            let json = std::str::from_utf8(&reply)?
                .trim()
                .strip_prefix("('")
                .and_then(|value| value.strip_suffix("',)"))
                .context("Invalid GNOME window snapshot")?;
            serde_json::from_str(json)?
        }
    };
    let identities = chains
        .into_iter()
        .filter_map(|(identity, chain)| {
            let window = resolve(&tree, &chain).ok()?;
            (window.focused && process::ancestry(proc_root, identity).ok().as_ref() == Some(&chain))
                .then_some(identity)
        })
        .collect();
    Ok(Focused { at_ms, identities })
}

fn focus_sway(
    proc_root: &Path,
    identity: ProcessIdentity,
    mut request: impl FnMut(u32, &str) -> Result<Value>,
) -> Result<()> {
    let chain = checked_chain(proc_root, identity)?;
    let tree: Node = serde_json::from_value(request(4, "")?)
        .context("Cannot read Sway's window list. Try again after checking the desktop.")?;
    let target = resolve(&tree, &chain)?;
    // Recheck the whole lineage immediately before dispatch, including terminal start time.
    ensure!(
        process::ancestry(proc_root, identity).context(CHANGED)? == chain,
        CHANGED
    );
    let reply = request(0, &format!("[con_id={}] focus", target.id))?;
    ensure!(
        reply.as_array().is_some_and(|replies| {
            replies.len() == 1 && replies[0].get("success") == Some(&Value::Bool(true))
        }),
        "Sway could not focus this window. It may have closed; refresh and try again."
    );
    let tree: Node = serde_json::from_value(request(4, "")?)
        .context("Cannot confirm the focused window. Try again.")?;
    ensure!(
        tree.windows()
            .any(|node| node.id == target.id && node.pid == target.pid && node.focused),
        "The window did not receive focus. Check for a desktop lock or another window taking focus."
    );
    Ok(())
}

fn checked_chain(proc_root: &Path, identity: ProcessIdentity) -> Result<Vec<Ancestor>> {
    let chain = process::ancestry(proc_root, identity).context(CHANGED)?;
    ensure!(
        !process::needs_pane_integration(proc_root, identity).context(
            "Cannot read this session's terminal details. Press Ctrl+E to locate it manually."
        )? && !chain.iter().any(|ancestor| {
            ancestor.name.starts_with("tmux")
                || matches!(ancestor.name.as_str(), "screen" | "zellij")
        }),
        PANE_MESSAGE
    );
    Ok(chain)
}

#[derive(Deserialize)]
struct Node {
    id: u64,
    pid: Option<u32>,
    #[serde(default)]
    focused: bool,
    #[serde(default)]
    nodes: Vec<Node>,
    #[serde(default)]
    floating_nodes: Vec<Node>,
}

impl Node {
    fn windows(&self) -> impl Iterator<Item = &Self> {
        let mut pending = vec![self];
        std::iter::from_fn(move || {
            while let Some(node) = pending.pop() {
                pending.extend(&node.nodes);
                pending.extend(&node.floating_nodes);
                if node.pid.is_some() {
                    return Some(node);
                }
            }
            None
        })
    }
}

fn resolve<'a>(tree: &'a Node, chain: &[Ancestor]) -> Result<&'a Node> {
    for ancestor in chain {
        let mut matches = tree
            .windows()
            .filter(|node| node.pid == Some(ancestor.identity.pid));
        if let Some(target) = matches.next() {
            ensure!(
                matches.next().is_none(),
                "Several windows share this terminal process. Kai cannot select one reliably yet. {DETAILS}"
            );
            // These terminals have no internal tabs or panes. Other terminals need their own
            // integration even when the desktop exposes just one top-level window.
            ensure!(single_pane_terminal(&ancestor.name), PANE_MESSAGE);
            return Ok(target);
        }
    }
    bail!(
        "Cannot find this session's terminal window. Detached, remote, or shared-server terminals may need a separate integration. {DETAILS}"
    )
}

fn single_pane_terminal(name: &str) -> bool {
    matches!(name, "foot" | "alacritty" | "xterm" | "st" | "urxvt")
}

fn recheck(identity: ProcessIdentity, chain: &[Ancestor]) -> Result<()> {
    ensure!(
        process::ancestry(Path::new("/proc"), identity).context(CHANGED)? == chain,
        CHANGED
    );
    Ok(())
}

fn focus_hyprland(identity: ProcessIdentity, cancel: &Cancellation) -> Result<()> {
    focus_hyprland_with(Path::new("/proc"), identity, |args: &[&str]| {
        command::run("hyprctl", args, Duration::from_secs(2), cancel)
        .context("Cannot contact Hyprland. Check that hyprctl is installed and run the dashboard inside Hyprland.")
    })
}

fn focus_hyprland_with(
    proc_root: &Path,
    identity: ProcessIdentity,
    mut run: impl FnMut(&[&str]) -> Result<Vec<u8>>,
) -> Result<()> {
    let chain = checked_chain(proc_root, identity)?;
    let recheck = || -> Result<()> {
        ensure!(
            process::ancestry(proc_root, identity).context(CHANGED)? == chain,
            CHANGED
        );
        Ok(())
    };
    let tree = hyprland_tree(&run(&["-j", "clients"])?)?;
    let target = resolve(&tree, &chain)?;
    recheck()?;
    let address = format!("address:0x{:x}", target.id);
    // Current Hyprland uses Lua dispatchers; older releases use named dispatchers.
    // Both forms contain only our validated numeric address.
    let lua = format!("hl.dsp.focus({{ window = \"{address}\" }})");
    let modern = run(&["dispatch", &lua]);
    if !modern.is_ok_and(|reply| reply.trim_ascii() == b"ok") {
        recheck()?;
        ensure!(
            run(&["dispatch", "focuswindow", &address])?.trim_ascii() == b"ok",
            "Hyprland could not focus this window. Refresh and try again."
        );
    }
    let active: HyprWindow = serde_json::from_slice(&run(&["-j", "activewindow"])?)?;
    ensure!(
        hex_id(&active.address)? == target.id && Some(active.pid) == target.pid,
        "The window did not receive focus. Check for a desktop lock or another window taking focus."
    );
    Ok(())
}

#[derive(Deserialize)]
struct HyprWindow {
    address: String,
    pid: u32,
    mapped: Option<bool>,
}

fn hex_id(value: &str) -> Result<u64> {
    let digits = value
        .strip_prefix("0x")
        .context("Invalid desktop window ID")?;
    ensure!(
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Invalid desktop window ID"
    );
    Ok(u64::from_str_radix(digits, 16)?)
}

fn root(nodes: Vec<Node>) -> Node {
    Node {
        id: 0,
        pid: None,
        focused: false,
        nodes,
        floating_nodes: Vec::new(),
    }
}

fn window(id: u64, pid: u32) -> Node {
    Node {
        id,
        pid: Some(pid),
        focused: false,
        nodes: Vec::new(),
        floating_nodes: Vec::new(),
    }
}

fn hyprland_tree(bytes: &[u8]) -> Result<Node> {
    let windows: Vec<HyprWindow> = serde_json::from_slice(bytes)?;
    Ok(root(
        windows
            .into_iter()
            .filter(|entry| entry.mapped != Some(false))
            .map(|entry| Ok(window(hex_id(&entry.address)?, entry.pid)))
            .collect::<Result<_>>()?,
    ))
}

fn focus_x11(identity: ProcessIdentity, cancel: &Cancellation) -> Result<()> {
    let chain = checked_chain(Path::new("/proc"), identity)?;
    let run = |args: &[&str]| {
        command::run("wmctrl", args, Duration::from_secs(2), cancel)
        .context("Cannot switch X11 windows. Install wmctrl and check that the desktop supports EWMH window activation.")
    };
    let tree = x11_tree(&run(&["-lp"])?)?;
    let target = resolve(&tree, &chain)?;
    recheck(identity, &chain)?;
    run(&["-i", "-a", &format!("0x{:x}", target.id)])?;
    // EWMH activation is asynchronous. Verify it, allowing a short compositor delay.
    for _ in 0..5 {
        let active = command::run(
            "xprop",
            &["-root", "_NET_ACTIVE_WINDOW"],
            Duration::from_secs(1),
            cancel,
        )
        .context("Cannot confirm X11 focus. Install xprop to verify window activation.")?;
        if std::str::from_utf8(&active)?
            .split_whitespace()
            .last()
            .and_then(|id| hex_id(id).ok())
            == Some(target.id)
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    bail!("The desktop did not focus this window. Check its focus-stealing settings and try again.")
}

fn x11_tree(bytes: &[u8]) -> Result<Node> {
    let nodes = std::str::from_utf8(bytes)?
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let id = hex_id(fields.next().context("Missing X11 window ID")?)?;
            fields.next().context("Missing X11 desktop")?;
            let pid = fields.next().context("Missing X11 process ID")?.parse()?;
            Ok(window(id, pid))
        })
        .collect::<Result<_>>()?;
    Ok(root(nodes))
}

fn focus_gnome(identity: ProcessIdentity, cancel: &Cancellation) -> Result<()> {
    let chain = checked_chain(Path::new("/proc"), identity)?;
    let terminal = chain
        .iter()
        .find(|ancestor| single_pane_terminal(&ancestor.name))
        .context(PANE_MESSAGE)?;
    recheck(identity, &chain)?;
    let response = command::run("gdbus", &[
        "call", "--session", "--dest", "org.gnome.Shell",
        "--object-path", "/org/gnome/Shell/Extensions/KaiWindowFocus",
        "--method", "org.gnome.Shell.Extensions.KaiWindowFocus.Focus",
        &terminal.identity.pid.to_string(), &terminal.identity.start_ticks.to_string(),
    ], Duration::from_secs(2), cancel).context(
        "GNOME Wayland needs the Kai Window Focus extension. Install and enable kai-tool/integrations/gnome; ensure gdbus is installed. Press Ctrl+E for terminal details.",
    )?;
    match response.trim_ascii() {
        b"('focused',)" => Ok(()),
        b"('ambiguous',)" => bail!(
            "Several windows share this terminal process. Kai cannot select one reliably yet. {DETAILS}"
        ),
        b"('changed',)" => bail!(CHANGED),
        b"('locked',)" => bail!("Unlock the desktop before switching windows."),
        _ => bail!("GNOME could not focus this terminal window. Refresh and try again. {DETAILS}"),
    }
}

#[cfg(test)]
#[path = "focus_tests.rs"]
mod tests;
