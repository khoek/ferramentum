use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_SCAN_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LINE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TurnState {
    Working,
    NeedsInput,
    Ready,
    Interrupted,
    Error,
    #[default]
    Unknown,
}

impl TurnState {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Working => "Active",
            Self::NeedsInput => "Needs input",
            Self::Ready => "Ready",
            Self::Interrupted => "Interrupted",
            Self::Error => "Error",
            Self::Unknown => "Unknown",
        }
    }
}

/// Identity and precise log time of the last terminal event; never contains response text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Completion {
    pub turn_id: Option<String>,
    pub at_ms: Option<i64>,
}

#[derive(Clone, Default)]
pub(super) struct View {
    pub id: String,
    pub parent_id: Option<String>,
    pub root_id: Option<String>,
    pub cwd: Option<PathBuf>,
    pub state: TurnState,
    pub last_finished_at: Option<i64>,
    pub input_requested_at: Option<i64>,
    pub completion: Option<Completion>,
    pub started_at: Option<i64>,
    pub duration_ms: Option<u64>,
    pub detail: Option<String>,
    pub token_usage: Option<super::tokens::Usage>,
}

impl View {
    pub(super) fn run_time_ms(&self, now: u64) -> Option<u64> {
        match self.state {
            TurnState::Working | TurnState::NeedsInput => self
                .started_at
                .and_then(|at| u64::try_from(at).ok())
                .map(|at| now.saturating_sub(at).saturating_mul(1000)),
            TurnState::Ready | TurnState::Interrupted | TurnState::Error => self.duration_ms,
            TurnState::Unknown => None,
        }
    }
}

#[derive(Default)]
pub(super) struct Transcript {
    identity: Option<(u64, u64)>,
    offset: u64,
    main: bool,
    subagent: bool,
    inherited_records: u64,
    view: View,
    turn: Option<String>,
    input_requests: HashSet<String>,
    skipped_line: bool,
    native_usage: bool,
}

// Deserialize only observation metadata. Prompts, auth data, tool arguments and output
// are ignored rather than retained in the dashboard's memory or JSON response.
#[derive(Deserialize)]
struct Record {
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    payload: Payload,
}

#[derive(Default, Deserialize)]
struct Payload {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    source: Option<Value>,
    #[serde(default)]
    parent_thread_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    subagent_history_start_ordinal: Option<u64>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    turn_id: Option<String>,
    #[serde(default)]
    completed_at: Option<i64>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    duration_ms: Option<i64>,
    #[serde(default)]
    error: Option<Present>,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    thread_token_usage: Option<super::tokens::Usage>,
    #[serde(default)]
    latest_token_usage_record: Option<super::tokens::Record>,
    #[serde(default)]
    info: Option<super::tokens::Info>,
}

// Avoid retaining or displaying upstream error text, which can include arbitrary content.
struct Present;
impl<'de> Deserialize<'de> for Present {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

impl Transcript {
    pub(super) fn is_main(&self) -> bool {
        self.main
    }
    pub(super) fn is_subagent(&self) -> bool {
        self.subagent
    }
    pub(super) fn view(&self) -> View {
        self.view.clone()
    }

    pub(super) fn refresh(&mut self, path: &Path) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .context("cannot open session log")?;
        let metadata = file.metadata()?;
        // Never follow a renamed log path into another user's file or a special device.
        // SAFETY: geteuid has no preconditions.
        if !metadata.is_file() || metadata.uid() != unsafe { libc::geteuid() } {
            bail!("unsafe session log");
        }
        let identity = (metadata.dev(), metadata.ino());
        if self.identity != Some(identity) || metadata.len() < self.offset {
            *self = Self::default();
            let mut head = Vec::new();
            BufReader::new((&mut file).take(MAX_LINE_BYTES)).read_until(b'\n', &mut head)?;
            if !head.ends_with(b"\n") {
                bail!("incomplete session metadata");
            }
            let record: Record =
                serde_json::from_slice(&head).context("invalid session metadata")?;
            if record.kind != "session_meta" {
                bail!("session log has no metadata header");
            }
            self.main = matches!(
                record.payload.source.as_ref().and_then(Value::as_str),
                Some("cli" | "tui")
            );
            let spawn = record
                .payload
                .source
                .as_ref()
                .and_then(|source| source.pointer("/subagent/thread_spawn"));
            self.subagent = spawn.is_some_and(Value::is_object);
            if self.subagent {
                self.view.parent_id = record.payload.parent_thread_id.or_else(|| {
                    spawn
                        .and_then(|spawn| spawn.get("parent_thread_id"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
                self.view.root_id = record.payload.session_id;
                // Ordinals include the metadata header itself. Copied parent
                // events do not describe the child's own activity.
                self.inherited_records = record
                    .payload
                    .subagent_history_start_ordinal
                    .unwrap_or(1)
                    .saturating_sub(1);
            }
            self.view.id = record.payload.id.context("session metadata has no ID")?;
            self.view.cwd = record.payload.cwd;
            self.offset = head.len() as u64;
            self.identity = Some(identity);
        }
        if !self.main && !self.subagent {
            return Ok(());
        }
        let mut budget = MAX_SCAN_BYTES;
        if self.inherited_records > 0 {
            file.seek(SeekFrom::Start(self.offset))?;
            let mut reader = BufReader::new((&mut file).take(budget));
            while self.inherited_records > 0 {
                let (count, complete) = skip_line(&mut reader)?;
                self.offset += count;
                budget -= count;
                if !complete {
                    return Ok(());
                }
                self.inherited_records -= 1;
            }
            if budget == 0 {
                return Ok(());
            }
        }
        // Bound startup work for very long conversations and every subsequent refresh.
        // A missing lifecycle marker remains Unknown, never inferred from file activity.
        if metadata.len().saturating_sub(self.offset) > budget {
            self.offset = metadata.len() - budget;
            file.seek(SeekFrom::Start(self.offset))?;
            let (skipped, complete_line) =
                skip_line(&mut BufReader::new((&mut file).take(budget)))?;
            self.offset += skipped;
            budget -= skipped;
            self.skipped_line = !complete_line;
            self.turn = None;
            self.input_requests.clear();
            self.view.input_requested_at = None;
            self.view.state = TurnState::Unknown;
            self.view.last_finished_at = None;
            self.view.started_at = None;
            self.view.duration_ms = None;
            self.view.detail =
                Some("Older history omitted; showing available recent turn events".into());
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let scan_end = self.offset + budget;
        let mut reader = BufReader::new(file.take(budget));
        loop {
            if self.skipped_line {
                let (count, complete) = skip_line(&mut reader)?;
                self.offset += count;
                self.skipped_line = !complete;
                if !complete {
                    break;
                }
                continue;
            }
            let mut line = Vec::new();
            let count = (&mut reader)
                .take(MAX_LINE_BYTES + 1)
                .read_until(b'\n', &mut line)?;
            if count == 0 {
                break;
            }
            if line.len() as u64 > MAX_LINE_BYTES {
                // Stream large records through serde's selective deserializer.
                // Ignored strings (compaction history, tool output, etc.) are
                // consumed without retaining their contents. Stop at this line.
                let mut rest = LineReader {
                    reader: &mut reader,
                    complete: line.ends_with(b"\n"),
                    bytes: 0,
                };
                let result = {
                    let mut input = line.as_slice().chain(&mut rest);
                    let result = serde_json::from_reader::<_, Record>(&mut input);
                    io::copy(&mut input, &mut io::sink())?;
                    result
                };
                let end = self.offset + count as u64 + rest.bytes;
                if !rest.complete {
                    if end >= scan_end {
                        self.offset = end;
                        self.skipped_line = true;
                        self.unknown("A log record exceeded the refresh read limit; waiting for a complete turn event");
                    } else if result.as_ref().is_err_and(|error| !error.is_eof()) {
                        self.unknown(
                            "A log record could not be read; waiting for a complete turn event",
                        );
                    }
                    // An unfinished append within the budget is retried in full.
                    break;
                }
                self.offset = end;
                self.apply_result(result);
                continue;
            }
            if !line.ends_with(b"\n") {
                break;
            } // Retry an unfinished append on the next refresh.
            self.offset += count as u64;
            self.apply_result(serde_json::from_slice::<Record>(&line));
        }
        Ok(())
    }

    fn unknown(&mut self, detail: &str) {
        self.view.state = TurnState::Unknown;
        self.turn = None;
        self.input_requests.clear();
        self.view.input_requested_at = None;
        self.view.started_at = None;
        self.view.duration_ms = None;
        self.view.detail = Some(detail.into());
    }

    fn apply_result(&mut self, result: serde_json::Result<Record>) {
        match result {
            Ok(record) => self.apply(record),
            Err(_) => {
                self.unknown("A log record could not be read; waiting for a complete turn event")
            }
        }
    }

    fn apply(&mut self, record: Record) {
        let timestamp = record
            .timestamp
            .as_deref()
            .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
        let at = timestamp.map(OffsetDateTime::unix_timestamp);
        let payload = record.payload;
        match (record.kind.as_str(), payload.kind.as_str()) {
            ("token_usage_record", _) if payload.thread_id.as_deref() == Some(&self.view.id) => {
                if let Some(usage) = payload.thread_token_usage {
                    self.native_usage = true;
                    self.view.token_usage = usage.validated();
                }
            }
            ("compacted", _) => {
                if let Some(usage) = payload.latest_token_usage_record
                    && usage.thread_id == self.view.id
                {
                    self.native_usage = true;
                    self.view.token_usage = usage.thread_token_usage.validated();
                }
            }
            ("event_msg", "token_count") if !self.native_usage => {
                if let Some(info) = payload.info {
                    self.view.token_usage = info.total_token_usage.validated();
                }
            }
            ("event_msg", "task_started" | "turn_started") => {
                self.turn = payload.turn_id;
                self.input_requests.clear();
                self.view.input_requested_at = None;
                self.view.state = TurnState::Working;
                self.view.started_at = payload.started_at.or(at);
                self.view.duration_ms = None;
                self.view.detail = None;
            }
            ("event_msg", "task_complete" | "turn_complete" | "turn_aborted") => {
                // Ignore a delayed terminal event for an older turn.
                if self.turn.is_some() && payload.turn_id.is_some() && self.turn != payload.turn_id
                {
                    return;
                }
                self.view.state = if payload.kind == "turn_aborted" {
                    TurnState::Interrupted
                } else if payload.error.is_some() {
                    TurnState::Error
                } else {
                    TurnState::Ready
                };
                self.view.last_finished_at = payload.completed_at.or(at);
                self.view.completion = Some(Completion {
                    turn_id: payload.turn_id.clone().or_else(|| self.turn.clone()),
                    at_ms: timestamp
                        .and_then(|time| (time.unix_timestamp_nanos() / 1_000_000).try_into().ok()),
                });
                self.view.started_at = payload.started_at.or(self.view.started_at);
                self.view.duration_ms = payload
                    .duration_ms
                    .and_then(|value| u64::try_from(value).ok())
                    .or_else(|| {
                        self.view
                            .last_finished_at
                            .zip(self.view.started_at)
                            .and_then(|(end, start)| u64::try_from(end.checked_sub(start)?).ok())
                            .map(|seconds| seconds.saturating_mul(1000))
                    });
                self.view.detail = None;
                self.turn = None;
                self.input_requests.clear();
                self.view.input_requested_at = None;
            }
            ("turn_context", _) => {
                if let Some(cwd) = payload.cwd {
                    self.view.cwd = Some(cwd);
                }
            }
            ("response_item", "function_call")
                if self.main && payload.name.as_deref().is_some_and(is_input_request) =>
            {
                if let Some(id) = payload.call_id {
                    // Measure the continuous wait, without restarting it for duplicate
                    // records or another question while a response is still pending.
                    if self.input_requests.is_empty() {
                        self.view.input_requested_at = at;
                    }
                    self.input_requests.insert(id);
                    self.view.state = TurnState::NeedsInput;
                }
            }
            ("response_item", "function_call_output")
                if payload
                    .call_id
                    .as_ref()
                    .is_some_and(|id| self.input_requests.remove(id))
                    && self.input_requests.is_empty() =>
            {
                self.view.input_requested_at = None;
                self.view.state = if self.turn.is_some() {
                    TurnState::Working
                } else {
                    TurnState::Unknown
                };
            }
            _ => (),
        }
    }
}

// Read exactly one JSONL record without allocating space for its large payload.
struct LineReader<'a, R> {
    reader: &'a mut R,
    complete: bool,
    bytes: u64,
}

impl<R: BufRead> Read for LineReader<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.complete || out.is_empty() {
            return Ok(0);
        }
        let available = self.reader.fill_buf()?;
        let available = &available[..available.len().min(out.len())];
        let end = available.iter().position(|byte| *byte == b'\n');
        let count = out
            .len()
            .min(end.map_or(available.len(), |index| index + 1));
        out[..count].copy_from_slice(&available[..count]);
        self.complete = end.is_some_and(|index| count == index + 1);
        self.reader.consume(count);
        self.bytes += count as u64;
        Ok(count)
    }
}

fn is_input_request(name: &str) -> bool {
    matches!(name.rsplit('.').next(), Some("request_user_input"))
}

fn skip_line(reader: &mut impl BufRead) -> Result<(u64, bool)> {
    let mut skipped = 0;
    loop {
        let bytes = reader.fill_buf()?;
        if bytes.is_empty() {
            return Ok((skipped, false));
        }
        let newline = bytes.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(bytes.len(), |index| index + 1);
        reader.consume(count);
        skipped += count as u64;
        if newline.is_some() {
            return Ok((skipped, true));
        }
    }
}

#[cfg(test)]
#[path = "transcript_tests.rs"]
mod tests;
