use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

use super::auth::Credential;
use super::auth_lock;
use super::isolated_home::IsolatedCodexHome;
use super::paths::RuntimePaths;

const APP_SERVER_TIMEOUT: Duration = Duration::from_secs(15);
const APP_SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const APP_SERVER_RETRY_DELAY: Duration = Duration::from_millis(100);
const APP_SERVER_ATTEMPTS: usize = 3;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_RPC_MESSAGES: usize = 1_000;
const UNSTARTED_COUNTDOWN_SECONDS: i64 = 7 * 24 * 60 * 60;
const COUNTDOWN_INFERENCE_TOLERANCE_SECONDS: i64 = 30;

#[derive(Clone)]
pub struct Client {
    codex: PathBuf,
    paths: RuntimePaths,
}

pub struct Outcome {
    pub snapshot: Result<Snapshot>,
    pub credential: Credential,
    source_fingerprint: [u8; 32],
    credential_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Snapshot {
    pub remaining_percent: f64,
    pub resets_at: i64,
    pub window_seconds: Option<i64>,
    #[serde(skip)]
    pub(super) reset_after_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_reset_credits: Option<ResetCredits>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResetCredits {
    pub available_count: u64,
    pub latest_expires_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RateLimitsResponse {
    rate_limits: RateLimitSnapshot,
    rate_limit_reset_credits: Option<ResetCreditsSummary>,
}

#[derive(Debug, Deserialize)]
struct RateLimitSnapshot {
    primary: Option<RateLimitWindow>,
    secondary: Option<RateLimitWindow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RateLimitWindow {
    used_percent: f64,
    window_duration_mins: Option<i64>,
    resets_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResetCreditsSummary {
    available_count: i64,
    credits: Option<Vec<ResetCredit>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResetCredit {
    reset_type: ResetType,
    status: ResetCreditStatus,
    expires_at: Option<i64>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum ResetType {
    CodexRateLimits,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum ResetCreditStatus {
    Available,
    #[serde(other)]
    Unavailable,
}

#[derive(Debug, Deserialize)]
struct RpcMessage {
    id: Option<Value>,
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug)]
struct AuthenticationRequired(String);

#[derive(Debug)]
struct RpcFailure {
    code: i64,
    message: String,
}

impl fmt::Display for AuthenticationRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for AuthenticationRequired {}

impl fmt::Display for RpcFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Codex app-server could not read quota (RPC {}): {}",
            self.code, self.message
        )
    }
}

impl Error for RpcFailure {}

impl Client {
    pub fn new(paths: &RuntimePaths) -> Result<Self> {
        Ok(Self {
            codex: which::which("codex").context("could not find `codex` on PATH")?,
            paths: paths.clone(),
        })
    }

    /// Fetch quota through Codex itself while pointing the downstream app-server at the
    /// account's canonical credential file. The isolated home contains only configuration;
    /// authentication is never copied into a second writable file.
    pub async fn fetch(&self, credential: Credential, auth_file: PathBuf) -> Outcome {
        let source_fingerprint = credential_fingerprint(&credential);
        let source_email = credential.facts.email.clone();
        let source_account_id = credential.facts.account_id.clone();
        let mut returned_credential = credential;
        let mut final_snapshot = None;

        for attempt in 0..APP_SERVER_ATTEMPTS {
            let home = match IsolatedCodexHome::create(&self.paths, "quota") {
                Ok(home) => home,
                Err(err) => {
                    final_snapshot = Some(Err(err));
                    break;
                }
            };

            let mut snapshot = run_app_server(&self.codex, home.path(), &auth_file).await;
            let mut retry_allowed = true;
            let updated_credential =
                auth_lock::acquire(&auth_file).and_then(|_lock| Credential::read(&auth_file));
            match updated_credential {
                Ok(updated)
                    if updated.facts.account_id == source_account_id
                        && updated.facts.email.eq_ignore_ascii_case(&source_email) =>
                {
                    returned_credential = updated;
                }
                Ok(updated) => {
                    snapshot = Err(anyhow!(
                        "Codex returned credentials for {} while checking {}; refusing to save them",
                        updated.facts.email,
                        source_email
                    ));
                    retry_allowed = false;
                }
                Err(err) if snapshot.is_ok() => {
                    snapshot = Err(err);
                    retry_allowed = false;
                }
                Err(_) => {}
            }

            let should_retry = attempt + 1 < APP_SERVER_ATTEMPTS
                && retry_allowed
                && snapshot.as_ref().is_err_and(retryable_error);
            final_snapshot = Some(snapshot);
            if !should_retry {
                break;
            }
            tokio::time::sleep(APP_SERVER_RETRY_DELAY).await;
        }

        Outcome {
            snapshot: final_snapshot.expect("at least one Codex app-server attempt is configured"),
            credential_changed: source_fingerprint != credential_fingerprint(&returned_credential),
            credential: returned_credential,
            source_fingerprint,
        }
    }
}

impl Outcome {
    pub fn credential_changed(&self) -> bool {
        self.credential_changed
    }

    pub fn source_matches(&self, credential: &Credential) -> bool {
        self.source_fingerprint == credential_fingerprint(credential)
    }
}

pub fn requires_authentication(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AuthenticationRequired>().is_some()
}

pub fn countdown_has_not_started(snapshot: &Snapshot, now: i64) -> bool {
    if let Some(reset_after_seconds) = snapshot.reset_after_seconds {
        return reset_after_seconds == UNSTARTED_COUNTDOWN_SECONDS;
    }
    if snapshot.window_seconds != Some(UNSTARTED_COUNTDOWN_SECONDS) {
        return false;
    }
    let inferred = snapshot.resets_at.saturating_sub(now);
    (UNSTARTED_COUNTDOWN_SECONDS - COUNTDOWN_INFERENCE_TOLERANCE_SECONDS
        ..=UNSTARTED_COUNTDOWN_SECONDS + COUNTDOWN_INFERENCE_TOLERANCE_SECONDS)
        .contains(&inferred)
}

async fn run_app_server(codex: &Path, codex_home: &Path, auth_file: &Path) -> Result<Snapshot> {
    let mut command = Command::new(codex);
    command
        .arg("--auth-file")
        .arg(auth_file)
        .args([
            "app-server",
            "--stdio",
            "-c",
            "cli_auth_credentials_store=\"file\"",
        ])
        .env("CODEX_HOME", codex_home)
        // An external bearer token takes precedence over auth.json. Quota workers must always
        // represent the one credential installed in their isolated home.
        .env_remove("CODEX_ACCESS_TOKEN")
        .env_remove("CODEX_API_KEY")
        .env_remove("OPENAI_API_KEY")
        .env_remove("CODEX_AUTH_FILE")
        .current_dir(codex_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .with_context(|| format!("could not start {} app-server", codex.display()))?;
    let mut stdin = child
        .stdin
        .take()
        .context("Codex app-server stdin was not available")?;
    let stdout = child
        .stdout
        .take()
        .context("Codex app-server stdout was not available")?;
    let stderr = child
        .stderr
        .take()
        .context("Codex app-server stderr was not available")?;
    let stderr_task = tokio::spawn(read_bounded_stderr(stderr));

    let exchange = timeout(
        APP_SERVER_TIMEOUT,
        exchange_messages(&mut stdin, BufReader::new(stdout)),
    )
    .await;
    drop(stdin);

    let timed_out = exchange.is_err();
    if timed_out {
        let _ = child.kill().await;
    }
    let status = match timeout(APP_SERVER_SHUTDOWN_TIMEOUT, child.wait()).await {
        Ok(status) => Some(status.context("could not wait for Codex app-server")?),
        Err(_) => {
            let _ = child.kill().await;
            child.wait().await.ok()
        }
    };
    let stderr = stderr_task.await.unwrap_or_else(|_| Vec::new());

    let response = match exchange {
        Err(_) => bail!(
            "Codex quota request timed out after {} seconds",
            APP_SERVER_TIMEOUT.as_secs()
        ),
        Ok(Err(err)) => {
            let complete_rpc_error =
                requires_authentication(&err) || err.downcast_ref::<RpcFailure>().is_some();
            if !complete_rpc_error && status.as_ref().is_some_and(|status| !status.success()) {
                return Err(err).context(process_failure(status.as_ref(), &stderr));
            }
            return Err(err);
        }
        Ok(Ok(response)) => response,
    };
    // A complete protocol response is authoritative. Some wrappers exit non-zero when stdin or
    // stdout closes immediately afterward; that cannot invalidate data already returned.
    response.into_snapshot()
}

async fn exchange_messages<W, R>(
    writer: &mut W,
    mut reader: BufReader<R>,
) -> Result<RateLimitsResponse>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    write_rpc(
        writer,
        &json!({
            "method": "initialize",
            "id": 0,
            "params": {
                "clientInfo": {
                    "name": "kai",
                    "title": "Kai",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        }),
    )
    .await?;
    let _ = read_rpc_response(&mut reader, 0).await?;
    write_rpc(writer, &json!({"method": "initialized"})).await?;
    write_rpc(
        writer,
        &json!({"method": "account/rateLimits/read", "id": 1}),
    )
    .await?;
    let result = read_rpc_response(&mut reader, 1).await?;
    serde_json::from_value(result).context("Codex app-server returned invalid rate-limit data")
}

async fn write_rpc<W>(writer: &mut W, message: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut encoded = serde_json::to_vec(message)?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("could not write to Codex app-server")?;
    writer
        .flush()
        .await
        .context("could not flush Codex app-server request")
}

async fn read_rpc_response<R>(reader: &mut BufReader<R>, expected_id: i64) -> Result<Value>
where
    R: AsyncRead + Unpin,
{
    for _ in 0..MAX_RPC_MESSAGES {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .await
            .context("could not read from Codex app-server")?;
        if bytes == 0 {
            bail!("Codex app-server exited before replying to request {expected_id}");
        }
        if line.trim().is_empty() {
            continue;
        }
        let message: RpcMessage = serde_json::from_str(&line)
            .context("Codex app-server emitted a non-JSON protocol message")?;
        if message.id.as_ref().and_then(Value::as_i64) != Some(expected_id) {
            continue;
        }
        if let Some(error) = message.error {
            return Err(rpc_error(error));
        }
        return message
            .result
            .context("Codex app-server response did not contain a result");
    }
    bail!("Codex app-server sent too many messages without replying to request {expected_id}")
}

async fn read_bounded_stderr<R>(mut stderr: R) -> Vec<u8>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    while let Ok(bytes) = stderr.read(&mut buffer).await {
        if bytes == 0 {
            break;
        }
        let remaining = MAX_STDERR_BYTES.saturating_sub(captured.len());
        captured.extend_from_slice(&buffer[..bytes.min(remaining)]);
    }
    captured
}

fn rpc_error(error: RpcError) -> anyhow::Error {
    if authentication_error_message(&error.message) {
        AuthenticationRequired(format!(
            "Codex app-server could not read quota (RPC {}): {}",
            error.code, error.message
        ))
        .into()
    } else {
        RpcFailure {
            code: error.code,
            message: error.message,
        }
        .into()
    }
}

fn retryable_error(error: &anyhow::Error) -> bool {
    if requires_authentication(error) {
        return false;
    }
    error
        .downcast_ref::<RpcFailure>()
        .is_none_or(|failure| matches!(failure.code, -32603 | -32001))
}

fn authentication_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("401")
        || message.contains("unauthorized")
        || message.contains("authentication required")
        || message.contains("refresh token expired")
        || message.contains("refresh token was already used")
        || message.contains("refresh token was revoked")
}

fn process_failure(status: Option<&ExitStatus>, stderr: &[u8]) -> String {
    let status = status
        .map(ToString::to_string)
        .unwrap_or_else(|| "without an exit status".to_owned());
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!("Codex app-server exited {status}")
    } else {
        format!("Codex app-server exited {status}: {stderr}")
    }
}

impl RateLimitsResponse {
    fn into_snapshot(self) -> Result<Snapshot> {
        let window = self
            .rate_limits
            .primary
            .or(self.rate_limits.secondary)
            .context("Codex quota response did not include a quota window")?;
        if !window.used_percent.is_finite() {
            bail!("Codex quota response contained a non-finite percentage");
        }
        let resets_at = window
            .resets_at
            .filter(|resets_at| *resets_at > 0)
            .context("Codex quota response did not include a valid reset datetime")?;
        let window_seconds = window
            .window_duration_mins
            .filter(|minutes| *minutes > 0)
            .and_then(|minutes| minutes.checked_mul(60));
        Ok(Snapshot {
            remaining_percent: (100.0 - window.used_percent).clamp(0.0, 100.0),
            resets_at,
            window_seconds,
            reset_after_seconds: None,
            rate_limit_reset_credits: self
                .rate_limit_reset_credits
                .and_then(ResetCreditsSummary::usable),
        })
    }
}

impl ResetCreditsSummary {
    fn usable(self) -> Option<ResetCredits> {
        let reported_count = self.available_count.max(0) as u64;
        let now = chrono::Utc::now().timestamp();
        let (available_count, latest_expires_at) = match self.credits {
            None => (reported_count, None),
            Some(credits) => {
                let returned_count = credits.len() as u64;
                let usable_expiries = credits
                    .into_iter()
                    .filter_map(|credit| credit.usable_expiry(now))
                    .collect::<Vec<_>>();
                let usable_count = usable_expiries.len() as u64;
                let count = if returned_count >= reported_count {
                    usable_count
                } else {
                    reported_count.max(usable_count)
                };
                (count, usable_expiries.into_iter().flatten().max())
            }
        };
        (available_count > 0).then_some(ResetCredits {
            available_count,
            latest_expires_at,
        })
    }
}

impl ResetCredit {
    fn usable_expiry(self, now: i64) -> Option<Option<i64>> {
        if self.reset_type != ResetType::CodexRateLimits
            || self.status != ResetCreditStatus::Available
        {
            return None;
        }
        match self.expires_at {
            None => Some(None),
            Some(expires_at) if expires_at > now => Some(Some(expires_at)),
            Some(_) => None,
        }
    }
}

fn credential_fingerprint(credential: &Credential) -> [u8; 32] {
    Sha256::digest(credential.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_app_server_rate_limits_and_usable_reset_credits() {
        let response: RateLimitsResponse = serde_json::from_value(json!({
            "rateLimits": {
                "primary": {
                    "usedPercent": 25,
                    "windowDurationMins": 300,
                    "resetsAt": 2_000_000_000_i64
                },
                "secondary": null
            },
            "rateLimitResetCredits": {
                "availableCount": 2,
                "credits": [
                    {
                        "resetType": "codexRateLimits",
                        "status": "available",
                        "expiresAt": 2_100_000_000_i64
                    },
                    {
                        "resetType": "codexRateLimits",
                        "status": "redeemed",
                        "expiresAt": 2_200_000_000_i64
                    }
                ]
            }
        }))
        .unwrap();
        let snapshot = response.into_snapshot().unwrap();
        assert_eq!(snapshot.remaining_percent, 75.0);
        assert_eq!(snapshot.window_seconds, Some(18_000));
        assert_eq!(snapshot.resets_at, 2_000_000_000);
        assert_eq!(
            snapshot.rate_limit_reset_credits,
            Some(ResetCredits {
                available_count: 1,
                latest_expires_at: Some(2_100_000_000)
            })
        );
    }

    #[test]
    fn recognizes_authentication_errors_from_app_server() {
        let error = rpc_error(RpcError {
            code: -32603,
            message: "failed to fetch codex rate limits: HTTP 401 Unauthorized".to_owned(),
        });
        assert!(requires_authentication(&error));
        assert!(!retryable_error(&error));

        let other = rpc_error(RpcError {
            code: -32603,
            message: "service unavailable".to_owned(),
        });
        assert!(!requires_authentication(&other));
        assert!(retryable_error(&other));

        let unsupported = rpc_error(RpcError {
            code: -32601,
            message: "method not found".to_owned(),
        });
        assert!(!retryable_error(&unsupported));
    }

    #[test]
    fn infers_a_fresh_seven_day_countdown_from_app_server_fields() {
        let now = 2_000_000_000;
        let snapshot = Snapshot {
            remaining_percent: 100.0,
            resets_at: now + UNSTARTED_COUNTDOWN_SECONDS - 2,
            window_seconds: Some(UNSTARTED_COUNTDOWN_SECONDS),
            reset_after_seconds: None,
            rate_limit_reset_credits: None,
        };
        assert!(countdown_has_not_started(&snapshot, now));

        let mut started = snapshot;
        started.resets_at -= COUNTDOWN_INFERENCE_TOLERANCE_SECONDS;
        assert!(!countdown_has_not_started(&started, now));
    }
}
