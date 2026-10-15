//! Local observation and explicit window switching. Process and turn states are separate facts.
//! Internal feature status: beta; native validation varies by desktop backend.

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod agents;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod attention;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod command;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod focus;
#[cfg(target_os = "linux")]
mod focus_linux;
#[cfg(target_os = "macos")]
mod focus_macos;
#[cfg(any(target_os = "macos", all(test, target_os = "linux")))]
mod macos_data;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod names;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod process;
#[cfg(target_os = "linux")]
mod process_linux;
#[cfg(target_os = "macos")]
mod process_macos;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod tokens;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod transcript;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod ui;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod worker;

use anyhow::{Result, bail};
use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// Open the live session picker; press Esc or Ctrl-C to quit.
    #[arg(long)]
    watch: bool,
    /// Print JSON; with --watch, emit one snapshot per line.
    #[arg(long)]
    json: bool,
    /// Seconds between refreshes in watch mode.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=3600))]
    interval: u64,
}

pub(crate) fn run(args: StatusArgs) -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let result = observer::run(args);
        if result.as_ref().err().is_some_and(|error| {
            error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
                || error
                    .downcast_ref::<serde_json::Error>()
                    .is_some_and(|error| {
                        error.io_error_kind() == Some(std::io::ErrorKind::BrokenPipe)
                    })
        }) {
            return Ok(());
        }
        result
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = args;
        bail!("kai status currently supports Linux and macOS process inspection")
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod observer {
    use super::names::Names;
    use super::process::{ProcessIdentity, Window, discover, same_process};
    use super::transcript::{Transcript, TurnState};
    use super::worker::Cancellation;
    use super::*;
    use crossterm::terminal;
    use serde::Serialize;
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        io::{self, IsTerminal, Write},
        path::PathBuf,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use unicode_width::UnicodeWidthChar;

    pub(super) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

    #[derive(Clone, Serialize)]
    pub(super) struct Row {
        #[serde(skip)]
        pub(super) identity: ProcessIdentity,
        pub(super) pid: u32,
        pub(super) tty: Option<String>,
        pub(super) cwd: Option<String>,
        pub(super) thread_id: Option<String>,
        pub(super) thread_name: Option<String>,
        pub(super) run_time_ms: Option<u64>,
        pub(super) state: TurnState,
        pub(super) agents: Option<super::agents::Summary>,
        pub(super) token_usage: Option<super::tokens::Usage>,
        pub(super) last_finished_at: Option<i64>,
        #[serde(skip)]
        pub(super) input_requested_at: Option<i64>,
        #[serde(skip)]
        pub(super) completion: Option<super::transcript::Completion>,
        // Retained as null for JSON v1 compatibility; exited processes are removed.
        pub(super) exited_at: Option<u64>,
        pub(super) detail: Option<String>,
    }

    #[derive(Serialize)]
    pub(super) struct Snapshot {
        pub(super) version: u8,
        pub(super) observed_at: u64,
        pub(super) windows: Vec<Row>,
        pub(super) warnings: Vec<String>,
    }

    #[derive(Default)]
    pub(super) struct Observer {
        transcripts: HashMap<PathBuf, Transcript>,
        windows: BTreeMap<ProcessIdentity, Row>,
        names: Names,
    }

    impl Observer {
        pub(super) fn snapshot(&mut self, now: u64, cancel: &Cancellation) -> Result<Snapshot> {
            let (windows, warnings) = discover(std::process::id(), cancel)?;
            self.observe_with_cancel(windows, warnings, now, same_process, cancel)
        }

        #[cfg(test)]
        fn observe(
            &mut self,
            windows: Vec<Window>,
            warnings: Vec<String>,
            now: u64,
            still_alive: impl FnMut(ProcessIdentity) -> io::Result<bool>,
        ) -> Snapshot {
            self.observe_with_cancel(
                windows,
                warnings,
                now,
                still_alive,
                &Cancellation::default(),
            )
            .unwrap()
        }

        fn observe_with_cancel(
            &mut self,
            windows: Vec<Window>,
            mut warnings: Vec<String>,
            now: u64,
            mut still_alive: impl FnMut(ProcessIdentity) -> io::Result<bool>,
            cancel: &Cancellation,
        ) -> Result<Snapshot> {
            cancel.check()?;
            let mut live = HashSet::new();
            let mut paths = HashSet::new();
            self.names
                .refresh(windows.iter().flat_map(|window| window.transcripts.iter()));
            for window in windows {
                cancel.check()?;
                live.insert(window.identity);
                let mut roots = Vec::new();
                let mut children = Vec::new();
                let mut errors = Vec::new();
                for path in &window.transcripts {
                    cancel.check()?;
                    paths.insert(path.clone());
                    let transcript = self.transcripts.entry(path.clone()).or_default();
                    let result = transcript.refresh(path);
                    if transcript.is_subagent() {
                        let mut child = transcript.view();
                        if result.is_err() {
                            child.state = TurnState::Unknown;
                        }
                        children.push(child);
                    } else {
                        match result {
                            Ok(()) if transcript.is_main() => roots.push(transcript.view()),
                            Ok(()) => (),
                            Err(_) => errors.push("Could not read a session log"),
                        }
                    }
                }
                // A process can retain multiple root threads after switching conversations.
                // Without a UI attachment signal, do not guess which one is on screen.
                let mut row = Row {
                    identity: window.identity,
                    pid: window.identity.pid,
                    tty: window.tty,
                    cwd: window.cwd.map(|path| path.to_string_lossy().into_owned()),
                    thread_id: None,
                    thread_name: None,
                    run_time_ms: None,
                    state: TurnState::Unknown,
                    agents: None,
                    token_usage: None,
                    last_finished_at: None,
                    input_requested_at: None,
                    completion: None,
                    exited_at: None,
                    detail: None,
                };
                match roots.as_slice() {
                    [root] if errors.is_empty() => {
                        row.thread_id = Some(root.id.clone());
                        row.thread_name = window
                            .transcripts
                            .iter()
                            .find_map(|path| self.names.lookup(path, &root.id));
                        row.run_time_ms = root.run_time_ms(now);
                        row.cwd = root
                            .cwd
                            .as_ref()
                            .map(|path| path.to_string_lossy().into_owned())
                            .or(row.cwd);
                        row.state = root.state;
                        row.last_finished_at = root.last_finished_at;
                        row.input_requested_at = root.input_requested_at;
                        row.completion = root.completion.clone();
                        row.detail.clone_from(&root.detail);
                        row.agents = Some(super::agents::summarize(&root.id, children, true));
                        row.token_usage = root.token_usage;
                    }
                    [] => row.detail = Some(
                        "No readable main session log (starting, recovering, or unsupported build)"
                            .into(),
                    ),
                    [_] => row.detail = Some("Some session logs could not be read".into()),
                    _ => {
                        row.detail =
                            Some("Multiple main conversations are open in this process".into())
                    }
                }
                if row.thread_id.is_none()
                    && let Some(previous) = self.windows.get(&window.identity)
                {
                    row.thread_id.clone_from(&previous.thread_id);
                    row.thread_name.clone_from(&previous.thread_name);
                    row.last_finished_at = previous.last_finished_at;
                }
                if let Some(warning) = window.warning {
                    row.state = TurnState::Unknown;
                    row.run_time_ms = None;
                    row.agents = None;
                    row.token_usage = None;
                    row.detail = Some(warning);
                }
                self.windows.insert(window.identity, row);
            }
            self.windows.retain(|identity, row| {
                if !live.contains(identity) {
                    if matches!(still_alive(*identity), Ok(false)) {
                        return false;
                    }
                    row.state = TurnState::Unknown;
                    row.run_time_ms = None;
                    row.agents = None;
                    row.token_usage = None;
                    row.detail = Some("Window could not be inspected during this refresh".into());
                }
                true
            });
            self.transcripts.retain(|path, _| paths.contains(path));
            let mut rows: Vec<_> = self.windows.values().cloned().collect();
            rows.sort_by_key(|row| {
                (
                    state_order(row.state),
                    std::cmp::Reverse(row.last_finished_at),
                    row.pid,
                )
            });
            warnings.sort();
            warnings.dedup();
            Ok(Snapshot {
                version: 1,
                observed_at: now,
                windows: rows,
                warnings,
            })
        }
    }

    fn state_order(state: TurnState) -> u8 {
        match state {
            TurnState::NeedsInput => 0,
            TurnState::Working => 1,
            TurnState::Ready => 2,
            TurnState::Interrupted | TurnState::Error => 3,
            TurnState::Unknown => 4,
        }
    }

    pub(super) fn run(args: StatusArgs) -> Result<()> {
        if args.watch && !args.json && (!io::stdout().is_terminal() || !io::stdin().is_terminal()) {
            bail!(
                "kai status --watch requires a terminal; use --watch --json for streaming output"
            );
        }
        if args.watch && !args.json {
            return super::ui::watch(Duration::from_secs(args.interval));
        }
        let mut observer = Observer::default();
        let mut observation =
            super::worker::Worker::new(move |(), cancel| observer.snapshot(now(), cancel));
        loop {
            let started = Instant::now();
            observation.start((), DISCOVERY_TIMEOUT)?;
            let snapshot = loop {
                if let Some(result) = observation.poll() {
                    break result?;
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            if args.json {
                let mut stdout = io::stdout().lock();
                serde_json::to_writer(&mut stdout, &snapshot)?;
                writeln!(stdout)?;
                stdout.flush()?;
            } else {
                let size = io::stdout()
                    .is_terminal()
                    .then(terminal::size)
                    .transpose()?;
                let text = render(&snapshot, size);
                let mut stdout = io::stdout().lock();
                write!(stdout, "{text}")?;
                stdout.flush()?;
            }
            if !args.watch {
                break;
            }
            let delay = Duration::from_secs(args.interval).saturating_sub(started.elapsed());
            std::thread::sleep(delay);
        }
        Ok(())
    }

    pub(super) fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn render(snapshot: &Snapshot, size: Option<(u16, u16)>) -> String {
        let width = size.map_or(usize::MAX, |(width, _)| {
            usize::from(width).saturating_sub(1)
        });
        let mut lines = vec![format!("Kai windows: {} open", snapshot.windows.len(),)];
        lines.push(format!(
            "{:<11} {:<8} {:<9} {:<10} {:<15} {:<10} {:<14} {}",
            "STATE",
            "PID",
            "TTY",
            "ELAPSED",
            "SUBAGENTS",
            "TOKENS",
            "LAST ENDED",
            "THREAD / DIRECTORY"
        ));
        if snapshot.windows.is_empty() {
            lines.push("No Kai windows found for this user.".into());
        }
        for row in &snapshot.windows {
            let finished = row
                .last_finished_at
                .map_or_else(|| "—".into(), |at| age(snapshot.observed_at, at));
            lines.push(format!(
                "{:<11} {:<8} {:<9} {:<10} {:<15} {:<10} {:<14} {}  {}",
                row.state.label(),
                row.pid,
                row.tty.as_deref().unwrap_or("—"),
                super::ui::run_time(row.run_time_ms),
                row.agents
                    .as_ref()
                    .map_or_else(|| "?".into(), super::agents::Summary::label),
                row.token_usage
                    .as_ref()
                    .map_or_else(|| "—".into(), super::tokens::Usage::label),
                finished,
                row.thread_name.as_deref().unwrap_or("Unnamed thread"),
                row.cwd.as_deref().unwrap_or("?"),
            ));
        }
        for warning in &snapshot.warnings {
            lines.push(format!("Warning: {warning}"));
        }
        lines
            .into_iter()
            .map(|line| format!("{}\n", safe_text(&line, width)))
            .collect()
    }

    pub(super) fn age(now: u64, at: i64) -> String {
        let seconds = now.saturating_sub(u64::try_from(at).unwrap_or_default());
        format!("{} ago", elapsed(seconds))
    }

    pub(super) fn elapsed(seconds: u64) -> String {
        // Coarse elapsed units, not calendar arithmetic: months are 30 days and years 365.
        for (unit, size) in [
            ("y", 365 * 86_400),
            ("mo", 30 * 86_400),
            ("w", 7 * 86_400),
            ("d", 86_400),
            ("h", 3600),
            ("m", 60),
        ] {
            if seconds >= size {
                return format!("{}{unit}", seconds / size);
            }
        }
        "<1m".into()
    }

    pub(super) fn safe_text(value: &str, max_width: usize) -> String {
        let mut width = 0;
        value
            .chars()
            .filter(|ch| {
                !ch.is_control() && !matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            })
            .take_while(|ch| {
                width += ch.width().unwrap_or_default();
                width <= max_width
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn window(start_ticks: u64) -> Window {
            Window {
                identity: ProcessIdentity {
                    pid: 42,
                    start_ticks,
                },
                tty: None,
                cwd: None,
                transcripts: Vec::new(),
                warning: None,
            }
        }

        #[test]
        fn exits_are_removed_and_pid_reuse_does_not_inherit_state() {
            let mut observer = Observer::default();
            observer.observe(vec![window(1)], vec![], 100, |_| Ok(true));
            let unavailable = observer.observe(vec![], vec![], 101, |_| {
                Err(io::ErrorKind::PermissionDenied.into())
            });
            assert_eq!(unavailable.windows[0].state, TurnState::Unknown);
            assert_eq!(unavailable.windows[0].exited_at, None);
            let reused = observer.observe(vec![window(2)], vec![], 102, |_| Ok(false));
            assert_eq!(reused.windows.len(), 1);
            assert_eq!(reused.windows[0].state, TurnState::Unknown);
            assert_eq!(reused.windows[0].identity.start_ticks, 2);
            let closed = observer.observe(vec![], vec![], 103, |_| Ok(false));
            assert!(closed.windows.is_empty());
        }

        #[test]
        fn agent_updates_do_not_overwrite_the_parent_and_closed_logs_are_removed() {
            use serde_json::json;
            use std::fs;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("root.jsonl");
            let child = dir.path().join("child.jsonl");
            let nested = dir.path().join("nested.jsonl");
            let write = |path: &PathBuf, id: &str, source, event| {
                fs::write(
                    path,
                    format!(
                        "{}\n{}\n",
                        json!({"type":"session_meta", "payload":{"id":id, "source":source}}),
                        json!({"type":"event_msg", "payload":{"type":event, "turn_id":id}})
                    ),
                )
                .unwrap();
            };
            write(&root, "root", json!("cli"), "task_complete");
            write(
                &child,
                "child",
                json!({"subagent":{"thread_spawn":{"parent_thread_id":"root"}}}),
                "task_started",
            );
            write(
                &nested,
                "nested",
                json!({"subagent":{"thread_spawn":{"parent_thread_id":"child"}}}),
                "task_started",
            );
            let mut observer = Observer::default();
            let mut live = window(1);
            live.transcripts = vec![root.clone(), child.clone(), nested.clone()];
            let first = observer.observe(vec![live.clone()], vec![], 100, |_| Ok(true));
            assert_eq!(first.windows[0].state, TurnState::Ready);
            assert_eq!(first.windows[0].agents.as_ref().unwrap().running, 2);
            // A known child's unreadable log affects its count, not the parent's state.
            fs::remove_file(&nested).unwrap();
            let failed = observer.observe(vec![live.clone()], vec![], 101, |_| Ok(true));
            assert_eq!(failed.windows[0].state, TurnState::Ready);
            assert_eq!(failed.windows[0].agents.as_ref().unwrap().running, 1);
            assert_eq!(failed.windows[0].agents.as_ref().unwrap().unknown, 1);
            live.transcripts = vec![root, child.clone()];
            let mut file = fs::OpenOptions::new().append(true).open(&child).unwrap();
            writeln!(
                file,
                "{}",
                json!({"type":"event_msg", "payload":{"type":"task_complete", "turn_id":"child"}})
            )
            .unwrap();
            let finished = observer.observe(vec![live.clone()], vec![], 102, |_| Ok(true));
            let agents = finished.windows[0].agents.as_ref().unwrap();
            assert_eq!(
                (agents.total, agents.running, agents.ready, agents.unknown),
                (1, 0, 1, 0)
            );
            live.warning = Some("Cannot inspect Codex's open session logs".into());
            let partial = observer.observe(vec![live], vec![], 103, |_| Ok(true));
            assert!(partial.windows[0].agents.is_none());
        }

        #[test]
        fn blocking_question_time_reaches_the_dashboard_without_changing_json() {
            use serde_json::json;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("root.jsonl");
            std::fs::write(
                &root,
                concat!(
                    "{\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"source\":\"cli\"}}\n",
                    "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"one\"}}\n",
                    "{\"type\":\"response_item\",\"timestamp\":\"1970-01-01T00:01:00Z\",\"payload\":{\"type\":\"function_call\",\"name\":\"request_user_input\",\"call_id\":\"question\"}}\n"
                ),
            )
            .unwrap();
            let mut live = window(1);
            live.transcripts = vec![root];
            let mut observer = Observer::default();
            let snapshot = observer.observe(vec![live], vec![], 100, |_| Ok(true));
            let row = &snapshot.windows[0];
            assert_eq!(row.state, TurnState::NeedsInput);
            assert_eq!(row.input_requested_at, Some(60));
            assert_eq!(row.last_finished_at, None);
            let output = serde_json::to_value(row).unwrap();
            assert_eq!(output["state"], json!("needs_input"));
            assert!(output.get("input_requested_at").is_none());
        }

        #[test]
        fn rendering_removes_terminal_controls_and_respects_cell_width() {
            assert_eq!(safe_text("a\n\u{1b}[31m\u{202e}b", 99), "a[31mb");
            assert_eq!(safe_text("a界b", 3), "a界");
            let snapshot = Snapshot {
                version: 1,
                observed_at: 100,
                windows: vec![],
                warnings: vec![],
            };
            let text = render(&snapshot, Some((20, 3)));
            assert!(
                text.lines()
                    .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 19)
            );
        }
    }
}
