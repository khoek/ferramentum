use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ExitStatus};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::io;
use crate::state::unix_timestamp;

const CLIENT_NAME: &str = "think_tool";
const CLIENT_TITLE: &str = "think-tool";
const APP_SERVER_POLL_INTERVAL: Duration = Duration::from_millis(200);
const APP_SERVER_STATE_VERSION: u32 = 1;
const STEER_REQUEST_VERSION: u32 = 1;
const STEER_DELIVERY_STATE_VERSION: u32 = 1;
const STEER_STATE_FILE: &str = "state.toml";

#[derive(Debug, Clone, Copy)]
pub enum AppServerPolicy {
    WorkspaceWrite,
    ReadOnly,
}

pub trait AppServerBackend {
    type Config;

    fn name(&self) -> &'static str;
    fn spawn_app_server(&self, cwd: &Path, config: &Self::Config) -> Result<Child>;
    fn model(&self, config: &Self::Config) -> Option<String>;
    fn thread_config(&self, config: &Self::Config) -> Value;
}

#[derive(Debug, Clone)]
pub struct AppServerTurnRequest<'a, B: AppServerBackend + ?Sized> {
    pub backend: &'a B,
    pub cwd: &'a Path,
    pub prompt: &'a str,
    pub run_root: &'a Path,
    pub transcript_path: &'a Path,
    pub reply_path: &'a Path,
    pub state_path: &'a Path,
    pub steer_dir: Option<&'a Path>,
    pub policy: AppServerPolicy,
    pub config: &'a B::Config,
}

#[derive(Debug, Clone)]
pub struct AppServerTurnExit {
    pub success: bool,
    pub code: u32,
    pub signal: Option<String>,
    pub pid: Option<u32>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppServerThreadState {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_turn_id: Option<String>,
    pub updated_at: u64,
}

impl Default for AppServerThreadState {
    fn default() -> Self {
        Self {
            version: APP_SERVER_STATE_VERSION,
            thread_id: None,
            active_turn_id: None,
            last_turn_id: None,
            updated_at: unix_timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SteerRequest {
    pub version: u32,
    pub created_at: u64,
    pub text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SteerDeliveryState {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_sent_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_sent_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_error_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SteerStatus {
    pub pending_count: usize,
    pub oldest_pending_at: Option<u64>,
    pub latest_pending_at: Option<u64>,
    pub latest_pending_text: Option<String>,
    pub last_sent_at: Option<u64>,
    pub last_sent_text: Option<String>,
    pub last_error_at: Option<u64>,
    pub last_error: Option<String>,
}

impl SteerRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            version: STEER_REQUEST_VERSION,
            created_at: unix_timestamp(),
            text: text.into(),
        }
    }
}

pub fn run_turn<B: AppServerBackend + ?Sized>(
    request: AppServerTurnRequest<'_, B>,
) -> Result<AppServerTurnExit> {
    io::ensure_dir(request.run_root)?;
    let mut child = request
        .backend
        .spawn_app_server(request.cwd, request.config)?;
    let pid = child.id();
    let stdin = Arc::new(Mutex::new(child.stdin.take().with_context(|| {
        format!(
            "{} app-server stdin was unavailable",
            request.backend.name()
        )
    })?));
    let stdout = child.stdout.take().with_context(|| {
        format!(
            "{} app-server stdout was unavailable",
            request.backend.name()
        )
    })?;
    let mut reader = BufReader::new(stdout);
    let next_id = Arc::new(AtomicU64::new(1));
    let transcript_path = request.transcript_path.to_owned();
    let raw_path = request.run_root.join("TRANSCRIPT.raw");
    let mut transcript = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&transcript_path)
        .with_context(|| format!("Failed to open `{}`", transcript_path.display()))?;
    let mut raw = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&raw_path)
        .with_context(|| format!("Failed to open `{}`", raw_path.display()))?;

    send_request(
        &stdin,
        0,
        "initialize",
        json!({
            "clientInfo": {
                "name": CLIENT_NAME,
                "title": CLIENT_TITLE,
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "experimentalApi": true,
            },
        }),
    )?;
    send_notification(&stdin, "initialized", json!({}))?;
    wait_for_response(&mut reader, &mut raw, &mut transcript, 0)?;

    let mut state = load_thread_state(request.state_path)?;
    let (thread_id, turn_id) = {
        let mut rpc = AppServerRpc {
            stdin: &stdin,
            reader: &mut reader,
            raw: &mut raw,
            transcript: &mut transcript,
            next_id: &next_id,
            cwd: request.cwd,
            policy: request.policy,
            backend: request.backend,
            config: request.config,
        };
        let thread_id = match state.thread_id.clone() {
            Some(thread_id) => rpc
                .resume_thread(&thread_id)
                .or_else(|_| rpc.start_thread())?,
            None => rpc.start_thread()?,
        };
        state.thread_id = Some(thread_id.clone());
        save_thread_state(request.state_path, &state)?;
        let turn_id = rpc.start_turn(&thread_id, request.prompt)?;
        (thread_id, turn_id)
    };
    state.active_turn_id = Some(turn_id.clone());
    save_thread_state(request.state_path, &state)?;

    let done = Arc::new(AtomicBool::new(false));
    let steer_handle = request.steer_dir.map(|steer_dir| {
        start_steer_thread(
            steer_dir.to_owned(),
            stdin.clone(),
            next_id.clone(),
            done.clone(),
            thread_id.clone(),
            turn_id.clone(),
            transcript_path.clone(),
        )
    });

    let mut reply = String::new();
    let mut message = None;
    let success = loop {
        let value = match read_message(&mut reader, &mut raw) {
            Ok(value) => value,
            Err(err) => {
                done.store(true, Ordering::Relaxed);
                if let Some(handle) = steer_handle {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(anyhow!("steer thread panicked")))?;
                }
                drop(stdin);
                let status = child.wait().context("Failed to wait for app-server")?;
                return Ok(AppServerTurnExit {
                    success: false,
                    code: exit_status_code(&status),
                    signal: exit_status_signal(&status),
                    pid: Some(pid),
                    message: Some(err.to_string()),
                });
            }
        };
        write_transcript_message(&mut transcript, &value)?;
        if value.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta")
            && let Some(delta) = value
                .get("params")
                .and_then(|params| params.get("delta"))
                .and_then(Value::as_str)
        {
            reply.push_str(delta);
            transcript.write_all(delta.as_bytes())?;
            transcript.flush()?;
        }
        if value.get("method").and_then(Value::as_str) == Some("turn/completed") {
            let turn = value
                .get("params")
                .and_then(|params| params.get("turn"))
                .cloned()
                .unwrap_or(Value::Null);
            let success = turn
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status == "completed");
            if !success {
                message = Some(
                    turn.get("error")
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "app-server turn did not complete".to_owned()),
                );
            }
            break success;
        }
        if value.get("method").and_then(Value::as_str) == Some("error") {
            message = value.get("params").map(ToString::to_string);
        }
    };

    done.store(true, Ordering::Relaxed);
    if let Some(handle) = steer_handle {
        handle
            .join()
            .unwrap_or_else(|_| Err(anyhow!("steer thread panicked")))?;
    }

    io::write_text(request.reply_path, reply.trim())?;
    state.active_turn_id = None;
    state.last_turn_id = Some(turn_id);
    save_thread_state(request.state_path, &state)?;
    drop(stdin);
    let status = child.wait().context("Failed to wait for app-server")?;
    Ok(AppServerTurnExit {
        success: success && status.success(),
        code: if !success {
            1
        } else if status.success() {
            0
        } else if let Some(code) = status.code() {
            code as u32
        } else {
            1
        },
        signal: exit_status_signal(&status),
        pid: Some(pid),
        message,
    })
}

pub fn enqueue_steer(dir: &Path, text: &str) -> Result<PathBuf> {
    io::ensure_dir(dir)?;
    let path = dir.join(format!(
        "{}-{}.toml",
        unix_timestamp_nanos(),
        std::process::id()
    ));
    io::write_toml(&path, &SteerRequest::new(text.trim()))?;
    Ok(path)
}

pub fn steer_status(dir: &Path) -> Result<SteerStatus> {
    let state = load_steer_delivery_state(dir)?;
    let mut status = SteerStatus {
        last_sent_at: state.last_sent_at,
        last_sent_text: state.last_sent_text,
        last_error_at: state.last_error_at,
        last_error: state.last_error,
        ..SteerStatus::default()
    };
    for path in pending_steer_requests(dir)? {
        let request = io::read_toml::<SteerRequest>(&path)?;
        status.pending_count += 1;
        status.oldest_pending_at = Some(
            status
                .oldest_pending_at
                .map_or(request.created_at, |oldest| oldest.min(request.created_at)),
        );
        if status
            .latest_pending_at
            .is_none_or(|latest| request.created_at >= latest)
        {
            status.latest_pending_at = Some(request.created_at);
            status.latest_pending_text = Some(request.text);
        }
    }
    Ok(status)
}

pub fn load_thread_state(path: &Path) -> Result<AppServerThreadState> {
    if path.exists() {
        io::read_toml(path)
    } else {
        Ok(AppServerThreadState::default())
    }
}

pub fn save_thread_state(path: &Path, state: &AppServerThreadState) -> Result<()> {
    let mut state = state.clone();
    state.version = APP_SERVER_STATE_VERSION;
    state.updated_at = unix_timestamp();
    io::write_toml(path, &state)
}

fn exit_status_code(status: &ExitStatus) -> u32 {
    status.code().map(|code| code as u32).unwrap_or(1)
}

#[cfg(unix)]
fn exit_status_signal(status: &ExitStatus) -> Option<String> {
    use std::os::unix::process::ExitStatusExt;

    status.signal().map(|signal| signal.to_string())
}

#[cfg(not(unix))]
fn exit_status_signal(_status: &ExitStatus) -> Option<String> {
    None
}

struct AppServerRpc<'a, B, R, W, T>
where
    B: AppServerBackend + ?Sized,
    R: BufRead + ?Sized,
    W: Write + ?Sized,
    T: Write + ?Sized,
{
    stdin: &'a Arc<Mutex<ChildStdin>>,
    reader: &'a mut R,
    raw: &'a mut W,
    transcript: &'a mut T,
    next_id: &'a AtomicU64,
    cwd: &'a Path,
    policy: AppServerPolicy,
    backend: &'a B,
    config: &'a B::Config,
}

impl<B, R, W, T> AppServerRpc<'_, B, R, W, T>
where
    B: AppServerBackend + ?Sized,
    R: BufRead + ?Sized,
    W: Write + ?Sized,
    T: Write + ?Sized,
{
    fn start_thread(&mut self) -> Result<String> {
        self.request("thread/start", self.thread_params(None))
            .and_then(|response| self.response_id(response, "thread/start", "thread"))
    }

    fn resume_thread(&mut self, thread_id: &str) -> Result<String> {
        self.request("thread/resume", self.thread_params(Some(thread_id)))
            .and_then(|response| self.response_id(response, "thread/resume", "thread"))
    }

    fn start_turn(&mut self, thread_id: &str, prompt: &str) -> Result<String> {
        self.request("turn/start", self.turn_start_params(thread_id, prompt))
            .and_then(|response| self.response_id(response, "turn/start", "turn"))
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = next_request_id(self.next_id);
        send_request(self.stdin, id, method, params)?;
        wait_for_response(self.reader, self.raw, self.transcript, id)
    }

    fn thread_params(&self, thread_id: Option<&str>) -> Value {
        let mut params = json!({
            "cwd": self.cwd.display().to_string(),
            "approvalPolicy": approval_policy(self.policy),
            "sandbox": sandbox_mode(self.policy),
            "config": self.backend.thread_config(self.config),
        });
        if let Some(model) = self.backend.model(self.config) {
            params["model"] = json!(model);
        }
        if let Some(thread_id) = thread_id {
            params["threadId"] = json!(thread_id);
        }
        params
    }

    fn turn_start_params(&self, thread_id: &str, prompt: &str) -> Value {
        let mut params = json!({
            "threadId": thread_id,
            "cwd": self.cwd.display().to_string(),
            "approvalPolicy": approval_policy(self.policy),
            "sandboxPolicy": sandbox_policy(self.policy),
            "input": [{"type": "text", "text": prompt}],
        });
        if let Some(model) = self.backend.model(self.config) {
            params["model"] = json!(model);
        }
        params
    }

    fn response_id(&self, response: Value, method: &str, result_key: &str) -> Result<String> {
        response
            .get("result")
            .and_then(|result| result.get(result_key))
            .and_then(|value| value.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .with_context(|| {
                format!(
                    "{} app-server {method} response did not include {result_key}.id",
                    self.backend.name()
                )
            })
    }
}

fn approval_policy(policy: AppServerPolicy) -> Value {
    match policy {
        AppServerPolicy::WorkspaceWrite => json!("never"),
        AppServerPolicy::ReadOnly => json!("never"),
    }
}

fn sandbox_mode(policy: AppServerPolicy) -> Value {
    match policy {
        AppServerPolicy::WorkspaceWrite => json!("danger-full-access"),
        AppServerPolicy::ReadOnly => json!("read-only"),
    }
}

fn sandbox_policy(policy: AppServerPolicy) -> Value {
    match policy {
        AppServerPolicy::WorkspaceWrite => json!({"type": "dangerFullAccess"}),
        AppServerPolicy::ReadOnly => json!({"type": "readOnly"}),
    }
}

fn start_steer_thread(
    steer_dir: PathBuf,
    stdin: Arc<Mutex<ChildStdin>>,
    next_id: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    thread_id: String,
    turn_id: String,
    transcript_path: PathBuf,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || {
        io::ensure_dir(&steer_dir)?;
        while !done.load(Ordering::Relaxed) {
            for path in pending_steer_requests(&steer_dir)? {
                let request = io::read_toml::<SteerRequest>(&path)?;
                if request.text.trim().is_empty() {
                    fs::remove_file(&path)?;
                    continue;
                }
                let id = next_request_id(&next_id);
                send_request(
                    &stdin,
                    id,
                    "turn/steer",
                    json!({
                        "threadId": thread_id,
                        "expectedTurnId": turn_id,
                        "input": [{"type": "text", "text": request.text}],
                    }),
                )
                .inspect_err(|err| {
                    let _ = record_steer_error(&steer_dir, &request, &err.to_string());
                })?;
                append_operator_steer(&transcript_path, &request.text)?;
                record_steer_sent(&steer_dir, &request)?;
                fs::remove_file(&path)?;
            }
            thread::sleep(APP_SERVER_POLL_INTERVAL);
        }
        Ok(())
    })
}

fn pending_steer_requests(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = io::collect_existing_dir(dir, |entry| {
        let path = entry.path();
        if path.file_name().and_then(|name| name.to_str()) != Some(STEER_STATE_FILE)
            && path.extension().and_then(|ext| ext.to_str()) == Some("toml")
        {
            return Ok(Some(path));
        }
        Ok(None)
    })?;
    paths.sort();
    Ok(paths)
}

fn load_steer_delivery_state(dir: &Path) -> Result<SteerDeliveryState> {
    let path = steer_state_path(dir);
    if path.exists() {
        io::read_toml(&path)
    } else {
        Ok(SteerDeliveryState {
            version: STEER_DELIVERY_STATE_VERSION,
            ..SteerDeliveryState::default()
        })
    }
}

fn save_steer_delivery_state(dir: &Path, state: &SteerDeliveryState) -> Result<()> {
    io::ensure_dir(dir)?;
    let mut state = state.clone();
    state.version = STEER_DELIVERY_STATE_VERSION;
    io::write_toml(&steer_state_path(dir), &state)
}

fn record_steer_sent(dir: &Path, request: &SteerRequest) -> Result<()> {
    let mut state = load_steer_delivery_state(dir)?;
    state.last_sent_at = Some(unix_timestamp());
    state.last_sent_text = Some(request.text.trim().to_owned());
    state.last_error_at = None;
    state.last_error = None;
    save_steer_delivery_state(dir, &state)
}

fn record_steer_error(dir: &Path, request: &SteerRequest, error: &str) -> Result<()> {
    let mut state = load_steer_delivery_state(dir)?;
    state.last_error_at = Some(unix_timestamp());
    state.last_error = Some(format!("{}: {}", request.text.trim(), error.trim()));
    save_steer_delivery_state(dir, &state)
}

fn steer_state_path(dir: &Path) -> PathBuf {
    dir.join(STEER_STATE_FILE)
}

fn append_operator_steer(path: &Path, text: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open `{}`", path.display()))?;
    writeln!(file, "\nuser")?;
    writeln!(file, "{}", text.trim())?;
    Ok(())
}

fn next_request_id(next_id: &AtomicU64) -> u64 {
    next_id.fetch_add(1, Ordering::Relaxed)
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn send_request(
    stdin: &Arc<Mutex<ChildStdin>>,
    id: u64,
    method: &str,
    params: Value,
) -> Result<()> {
    send_json(
        stdin,
        json!({
            "id": id,
            "method": method,
            "params": params,
        }),
    )
}

fn send_notification(stdin: &Arc<Mutex<ChildStdin>>, method: &str, params: Value) -> Result<()> {
    send_json(stdin, json!({ "method": method, "params": params }))
}

fn send_json(stdin: &Arc<Mutex<ChildStdin>>, value: Value) -> Result<()> {
    let mut stdin = stdin
        .lock()
        .map_err(|_| anyhow!("app-server stdin lock was poisoned"))?;
    serde_json::to_writer(&mut *stdin, &value)?;
    stdin.write_all(b"\n")?;
    stdin.flush()?;
    Ok(())
}

fn wait_for_response<R, W, T>(
    reader: &mut R,
    raw: &mut W,
    transcript: &mut T,
    id: u64,
) -> Result<Value>
where
    R: BufRead + ?Sized,
    W: Write + ?Sized,
    T: Write + ?Sized,
{
    loop {
        let value = read_message(reader, raw)?;
        write_transcript_message(transcript, &value)?;
        if value.get("id").and_then(Value::as_u64) != Some(id) {
            continue;
        }
        if let Some(error) = value.get("error") {
            bail!("app-server request {id} failed: {error}");
        }
        return Ok(value);
    }
}

fn read_message<R, W>(reader: &mut R, raw: &mut W) -> Result<Value>
where
    R: BufRead + ?Sized,
    W: Write + ?Sized,
{
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .context("Failed to read app-server message")?;
    if read == 0 {
        bail!("app-server closed stdout");
    }
    raw.write_all(line.as_bytes())?;
    raw.flush()?;
    serde_json::from_str(&line).with_context(|| format!("Invalid app-server JSON: {line}"))
}

fn write_transcript_message<W: Write + ?Sized>(transcript: &mut W, value: &Value) -> Result<()> {
    if value.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta") {
        return Ok(());
    }
    if let Some(method) = value.get("method").and_then(Value::as_str) {
        writeln!(transcript, "\n{method}")?;
    } else if value.get("id").is_some() {
        writeln!(transcript, "\nresponse {}", value["id"])?;
    } else {
        writeln!(transcript, "\nmessage")?;
    }
    writeln!(transcript, "{value}")?;
    transcript.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn steer_status_tracks_pending_and_last_sent() {
        let dir = tempdir().unwrap();
        let request_path = enqueue_steer(dir.path(), "check the failing run").unwrap();

        let pending = steer_status(dir.path()).unwrap();
        assert_eq!(pending.pending_count, 1);
        assert_eq!(
            pending.latest_pending_text.as_deref(),
            Some("check the failing run")
        );
        assert!(pending.last_sent_at.is_none());

        let request = io::read_toml::<SteerRequest>(&request_path).unwrap();
        record_steer_sent(dir.path(), &request).unwrap();
        fs::remove_file(request_path).unwrap();

        let sent = steer_status(dir.path()).unwrap();
        assert_eq!(sent.pending_count, 0);
        assert_eq!(
            sent.last_sent_text.as_deref(),
            Some("check the failing run")
        );
        assert!(sent.last_sent_at.is_some());
    }
}
