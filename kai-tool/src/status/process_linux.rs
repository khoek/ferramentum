use super::process::{Ancestor, ProcessIdentity, Window, is_launch, is_rollout};
use anyhow::{Context, Result};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{self, Read},
    os::unix::fs::MetadataExt,
    path::Path,
};

struct Process {
    identity: ProcessIdentity,
    parent: u32,
    name: String,
    zombie: bool,
}

/// Walk only live, same-user processes, from Kai towards its terminal.
pub(super) fn ancestry(proc_root: &Path, identity: ProcessIdentity) -> io::Result<Vec<Ancestor>> {
    let owner = fs::metadata(proc_root.join("self"))?.uid();
    let mut chain = Vec::new();
    let mut pid = identity.pid;
    while pid != 0 && chain.len() < 64 {
        if chain
            .iter()
            .any(|ancestor: &Ancestor| ancestor.identity.pid == pid)
        {
            return Err(io::Error::other("process ancestry contains a cycle"));
        }
        let dir = proc_root.join(pid.to_string());
        if fs::metadata(&dir)?.uid() != owner {
            break;
        }
        let process = read_process(&dir, pid)?;
        if process.zombie || (chain.is_empty() && process.identity != identity) {
            return Err(io::Error::other("process has exited or changed"));
        }
        pid = process.parent;
        chain.push(Ancestor {
            identity: process.identity,
            name: process.name,
        });
    }
    if chain.is_empty() || chain.len() == 64 {
        return Err(io::Error::other("cannot verify process ancestry"));
    }
    Ok(chain)
}

pub(super) fn needs_pane_integration(
    proc_root: &Path,
    identity: ProcessIdentity,
) -> io::Result<bool> {
    let environment = read_bounded(
        &proc_root.join(identity.pid.to_string()).join("environ"),
        256 * 1024,
    )?;
    Ok(environment.split(|byte| *byte == 0).any(|entry| {
        let Some(separator) = entry.iter().position(|byte| *byte == b'=') else {
            return false;
        };
        let (key, value) = (&entry[..separator], &entry[separator + 1..]);
        !value.is_empty()
            && (matches!(
                key,
                b"TMUX"
                    | b"STY"
                    | b"ZELLIJ"
                    | b"WEZTERM_PANE"
                    | b"KITTY_WINDOW_ID"
                    | b"KONSOLE_DBUS_SESSION"
                    | b"GNOME_TERMINAL_SCREEN"
            ) || (key == b"TERM_PROGRAM" && value == b"vscode"))
    }))
}

pub(super) fn discover(
    proc_root: &Path,
    observer_pid: u32,
    cancel: &super::worker::Cancellation,
) -> Result<(Vec<Window>, Vec<String>)> {
    let owner = fs::metadata(proc_root.join("self"))
        .context("cannot inspect this process in /proc")?
        .uid();
    let mut processes = HashMap::new();
    let mut warnings = Vec::new();
    for entry in fs::read_dir(proc_root).context("cannot enumerate Linux processes")? {
        cancel.check()?;
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == observer_pid {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.uid() != owner {
            continue;
        }
        match read_process(&entry.path(), pid) {
            Ok(process) => {
                processes.insert(pid, process);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(_) => warnings
                .push("Some process metadata could not be read; the list may be incomplete".into()),
        }
    }
    let mut windows = Vec::new();
    for process in processes
        .values()
        .filter(|process| process.name == "kai" && !process.zombie)
    {
        let dir = proc_root.join(process.identity.pid.to_string());
        cancel.check()?;
        let command = match read_bounded(&dir.join("cmdline"), 64 * 1024) {
            Ok(command) => command,
            Err(_) => {
                warnings.push(
                    "Some Kai command lines could not be read; the list may be incomplete".into(),
                );
                continue;
            }
        };
        if !is_launch(&command) {
            continue;
        }
        let mut transcripts = BTreeSet::new();
        let mut warning = None;
        for child in processes.values().filter(|child| {
            child.name == "codex" && !child.zombie && child.parent == process.identity.pid
        }) {
            let fd_dir = proc_root.join(child.identity.pid.to_string()).join("fd");
            match fs::read_dir(&fd_dir) {
                Ok(entries) => {
                    for entry in entries.flatten() {
                        cancel.check()?;
                        let Ok(path) = fs::read_link(entry.path()) else {
                            continue;
                        };
                        if is_rollout(&path) {
                            transcripts.insert(path);
                        }
                    }
                }
                Err(_) => warning = Some("Cannot inspect Codex's open session logs".into()),
            }
            if !matches!(same_process(proc_root, child.identity), Ok(true)) {
                transcripts.clear();
                warning =
                    Some("Codex restarted during observation; waiting for the next refresh".into());
            }
        }
        // The owner may have exited or the PID may have been reused during the scan.
        match same_process(proc_root, process.identity) {
            Ok(false) => continue,
            Err(_) => warning = Some("Cannot verify the window process during this refresh".into()),
            Ok(true) => (),
        }
        windows.push(Window {
            identity: process.identity,
            tty: fs::read_link(dir.join("fd/0")).ok().and_then(|path| {
                let path = path.to_str()?;
                (path.starts_with("/dev/pts/") || path.starts_with("/dev/tty"))
                    .then(|| path.trim_start_matches("/dev/").to_owned())
            }),
            cwd: fs::read_link(dir.join("cwd")).ok(),
            transcripts: transcripts.into_iter().collect(),
            warning,
        });
    }
    Ok((windows, warnings))
}

pub(super) fn same_process(proc_root: &Path, identity: ProcessIdentity) -> io::Result<bool> {
    match read_process(&proc_root.join(identity.pid.to_string()), identity.pid) {
        Ok(process) => Ok(process.identity == identity && !process.zombie),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn read_process(dir: &Path, pid: u32) -> io::Result<Process> {
    let stat = read_bounded(&dir.join("stat"), 16 * 1024)?;
    let stat = std::str::from_utf8(&stat).map_err(|_| io::Error::other("invalid process stat"))?;
    parse_stat(pid, stat).ok_or_else(|| io::Error::other("invalid process stat"))
}

fn parse_stat(pid: u32, stat: &str) -> Option<Process> {
    let start = stat.find('(')?;
    let end = stat.rfind(')')?;
    if end <= start || stat[..start].trim().parse::<u32>().ok()? != pid {
        return None;
    }
    let fields: Vec<_> = stat[end + 1..].split_ascii_whitespace().collect();
    Some(Process {
        identity: ProcessIdentity {
            pid,
            start_ticks: fields.get(19)?.parse().ok()?,
        },
        parent: fields.get(1)?.parse().ok()?,
        name: stat[start + 1..end].to_owned(),
        zombie: matches!(*fields.first()?, "Z" | "X"),
    })
}

fn read_bounded(path: &Path, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::other("process metadata exceeds size limit"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_exclude_status_watchers_and_helpers_but_accept_launch_options() {
        for command in [
            b"kai\0".as_slice(),
            b"kai\0r\0id\0",
            b"kai\0--fast\0resume\0",
            b"kai\0--credential-provider\0status\0",
        ] {
            assert!(is_launch(command));
        }
        for command in [
            b"kai\0status\0--watch\0".as_slice(),
            b"kai\0s\0",
            b"kai\0help\0",
            b"kai\0llm\0file\0",
            b"kai\0--version\0",
        ] {
            assert!(!is_launch(command));
        }
    }

    #[test]
    fn stat_parser_handles_parentheses_and_tracks_start_time_and_zombies() {
        let stat = "42 (kai (test)) S 7 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 123 0";
        let process = parse_stat(42, stat).unwrap();
        assert_eq!(process.identity.start_ticks, 123);
        assert_eq!(process.parent, 7);
        assert_eq!(process.name, "kai (test)");
        assert!(!process.zombie);
        assert!(parse_stat(42, &stat.replace(" S ", " Z ")).unwrap().zombie);
        assert!(parse_stat(43, stat).is_none());
        assert!(parse_stat(42, "42 (kai) S").is_none());
    }
}
