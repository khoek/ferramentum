use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use serde_json::{Value, json};

use crate::backend::AppServerBackend;
use crate::config::{CodexProviderConfig, CodexThinkingLevel};
use crate::time::{format_unix_time, human_duration};
use crate::{io, lock, maintenance};

const STATE_VERSION: u32 = 1;
const DEFAULT_ACCOUNT_ID: &str = "default";
const CODEX_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
const CODEX_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CODEX_SESSION_SCAN_DEPTH: usize = 5;
const CODEX_ARCHIVED_SESSION_SCAN_DEPTH: usize = 1;
const CODEX_RATE_LIMIT_MAX_SESSION_FILES: usize = 40;
const CODEX_RATE_LIMIT_MAX_EVENT_LINES: usize = 2048;
const CODEX_UPDATE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const CODEX_UPDATE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const QUOTA_FULL_THRESHOLD: f64 = 99.9;
const QUOTA_BAR_WIDTH: usize = 12;
const PROVIDER_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderState {
    pub version: u32,
    pub active_account: String,
    pub accounts: BTreeMap<String, AccountState>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub codex_home: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_wait_until: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_quota_event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RateLimits {
    pub account: String,
    pub primary: RateLimit,
    pub secondary: RateLimit,
}

#[derive(Debug, Clone)]
pub struct RateLimit {
    pub used_percent: f64,
    pub resets_in_seconds: u64,
    pub resets_at: u64,
}

#[derive(Debug, Clone)]
pub enum Health {
    Ok,
    Unavailable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCatalogEntry {
    pub slug: String,
    pub display_name: String,
    pub description: Option<String>,
    pub supported_reasoning_levels: Vec<CodexThinkingLevel>,
}

#[derive(Debug, Clone)]
pub struct AppServerInvocation {
    pub program: PathBuf,
    pub codex_home: PathBuf,
    pub common_args: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct CodexAppServerBackend;

pub const APP_SERVER_BACKEND: CodexAppServerBackend = CodexAppServerBackend;

struct CodexUpdateTask;

impl maintenance::UpdateTask for CodexUpdateTask {
    fn key(&self) -> &'static str {
        "codex"
    }

    fn label(&self) -> &'static str {
        "codex update"
    }

    fn interval(&self) -> Duration {
        CODEX_UPDATE_INTERVAL
    }

    fn timeout(&self) -> Duration {
        CODEX_UPDATE_TIMEOUT
    }

    fn command(&self) -> Command {
        let mut command = Command::new("codex");
        command.arg("update");
        command
    }
}

impl AppServerBackend for CodexAppServerBackend {
    type Config = CodexProviderConfig;

    fn name(&self) -> &'static str {
        "Codex"
    }

    fn spawn_app_server(&self, cwd: &Path, config: &Self::Config) -> Result<Child> {
        let invocation = app_server_invocation(config)?;
        let mut command = Command::new(&invocation.program);
        command
            .args(["app-server", "--stdio"])
            .args(&invocation.common_args)
            .current_dir(cwd)
            .env("CODEX_HOME", &invocation.codex_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command
            .spawn()
            .context("Failed to spawn `codex app-server --stdio`")
    }

    fn model(&self, config: &Self::Config) -> Option<String> {
        config
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(str::to_owned)
    }

    fn thread_config(&self, config: &Self::Config) -> Value {
        let mut values = BTreeMap::new();
        values.insert("check_for_update_on_startup", json!(false));
        if let Some(thinking_level) = config.thinking_level {
            values.insert("model_reasoning_effort", json!(thinking_level.to_string()));
        }
        json!(values)
    }
}

impl std::fmt::Display for RateLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "account {} · primary {} · secondary {}",
            self.account, self.primary, self.secondary
        )
    }
}

impl std::fmt::Display for RateLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {:>4.1}% reset {} ({})",
            quota_bar(self.used_percent),
            self.used_percent,
            human_duration(self.resets_in_seconds),
            format_unix_time(self.resets_at)
        )
    }
}

pub fn load_active_rate_limits(config: &CodexProviderConfig) -> Option<RateLimits> {
    active_account(config)
        .ok()
        .and_then(|(account, home)| load_rate_limits_for_home(&account, &home))
}

pub fn load_account_rate_limits(account: &str, home: &Path) -> Option<RateLimits> {
    load_rate_limits_for_home(account, home)
}

pub fn probe_health(config: &CodexProviderConfig) -> Health {
    let home = match active_account(config) {
        Ok((_, home)) => home,
        Err(err) => return Health::Unavailable(err.to_string()),
    };
    let mut command = Command::new(match which::which("codex") {
        Ok(program) => program,
        Err(err) => return Health::Unavailable(format!("codex failed to resolve: {err}")),
    });
    command
        .arg("--version")
        .env("CODEX_HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => return Health::Unavailable(format!("codex failed to start: {err}")),
    };
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Health::Ok,
            Ok(Some(status)) => return Health::Unavailable(format!("codex exited {status}")),
            Ok(None) if started.elapsed() >= CODEX_HEALTH_CHECK_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                return Health::Unavailable("codex version check timed out".to_owned());
            }
            Ok(None) => std::thread::sleep(CODEX_HEALTH_POLL_INTERVAL),
            Err(err) => return Health::Unavailable(format!("codex wait failed: {err}")),
        }
    }
}

pub fn app_server_invocation(config: &CodexProviderConfig) -> Result<AppServerInvocation> {
    let codex_home = prepare_invocation(config)?;
    let mut common_args = Vec::new();
    push_common_args(&mut common_args, config);
    Ok(AppServerInvocation {
        program: which::which("codex").context("`codex` is required but was not found on PATH")?,
        codex_home,
        common_args,
    })
}

pub fn list_accounts() -> Result<ProviderState> {
    with_state(|state| Ok(state.clone()))
}

pub fn load_model_catalog(config: &CodexProviderConfig) -> Result<Vec<ModelCatalogEntry>> {
    let (_, home) = active_account(config)?;
    let output = Command::new("codex")
        .args(["-c", "check_for_update_on_startup=false", "debug", "models"])
        .env("CODEX_HOME", &home)
        .current_dir(&home)
        .output()
        .context("Failed to run `codex debug models`")?;
    if !output.status.success() {
        bail!(
            "`codex debug models` exited with {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let models = parse_model_catalog_json(&output.stdout)?;
    if models.is_empty() {
        bail!("`codex debug models` did not return any selectable models");
    }
    Ok(models)
}

pub fn authenticate_account(account: &str, home: Option<PathBuf>) -> Result<()> {
    validate_account(account)?;
    let home = home.unwrap_or(default_named_account_home(account)?);
    io::ensure_dir(&home)?;
    with_state(|state| {
        state.accounts.insert(
            account.to_owned(),
            AccountState {
                codex_home: home.clone(),
                quota_wait_until: None,
                last_quota_event: None,
                last_used_at: None,
            },
        );
        state.active_account = account.to_owned();
        Ok(())
    })?;
    let status = Command::new("codex")
        .arg("login")
        .env("CODEX_HOME", &home)
        .current_dir(&home)
        .status()
        .context("Failed to run `codex login`")?;
    if !status.success() {
        bail!("`codex login` exited with {status}");
    }
    Ok(())
}

pub fn set_active_account(account: &str) -> Result<()> {
    validate_account(account)?;
    with_state(|state| {
        ensure_account_exists(state, account)?;
        state.active_account = account.to_owned();
        Ok(())
    })
}

pub fn remove_account(account: &str) -> Result<()> {
    validate_account(account)?;
    with_state(|state| remove_account_from_state(state, account))
}

fn prepare_invocation(config: &CodexProviderConfig) -> Result<PathBuf> {
    maintenance::ensure_update_checked(&CodexUpdateTask)?;
    active_account(config)
        .map(|(_, home)| home)
        .with_context(|| "Failed to prepare Codex provider invocation")
}

fn active_account(_config: &CodexProviderConfig) -> Result<(String, PathBuf)> {
    with_state(|state| {
        let now = unix_timestamp();
        refresh_state_from_rate_limits(state, now);
        let account = if account_is_available(state, &state.active_account, now) {
            state.active_account.clone()
        } else {
            select_free_account(state, now, None)
                .or_else(|| earliest_retry(state).map(|(account, _)| account))
                .unwrap_or_else(|| DEFAULT_ACCOUNT_ID.to_owned())
        };
        ensure_account_exists(state, &account)?;
        state.active_account = account.clone();
        if let Some(account_state) = state.accounts.get_mut(&account) {
            account_state.last_used_at = Some(now);
            Ok((account, account_state.codex_home.clone()))
        } else {
            unreachable!("account was just ensured")
        }
    })
}

fn push_common_args(args: &mut Vec<String>, config: &CodexProviderConfig) {
    args.extend([
        "-c".to_owned(),
        "check_for_update_on_startup=false".to_owned(),
    ]);
    if let Some(model) = config
        .model
        .as_deref()
        .filter(|model| !model.trim().is_empty())
    {
        args.extend(["--model".to_owned(), model.trim().to_owned()]);
    }
    if let Some(thinking_level) = config.thinking_level {
        args.extend([
            "-c".to_owned(),
            format!("model_reasoning_effort=\"{thinking_level}\""),
        ]);
    }
}

fn with_state<T>(update: impl FnOnce(&mut ProviderState) -> Result<T>) -> Result<T> {
    let home = provider_home()?;
    io::ensure_dir(&home)?;
    let _lock = lock::FileLock::acquire_wait(
        home.join("codex.lock"),
        "Codex provider lock",
        PROVIDER_STATE_LOCK_TIMEOUT,
    )?;
    let path = home.join("codex-state.toml");
    let mut state = if path.exists() {
        io::read_toml(&path)?
    } else {
        default_state()?
    };
    normalize_state(&mut state)?;
    let result = update(&mut state)?;
    state.version = STATE_VERSION;
    state.updated_at = unix_timestamp();
    io::write_toml(&path, &state)?;
    Ok(result)
}

fn default_state() -> Result<ProviderState> {
    let mut accounts = BTreeMap::new();
    accounts.insert(
        DEFAULT_ACCOUNT_ID.to_owned(),
        AccountState {
            codex_home: default_codex_home()?,
            quota_wait_until: None,
            last_quota_event: None,
            last_used_at: None,
        },
    );
    Ok(ProviderState {
        version: STATE_VERSION,
        active_account: DEFAULT_ACCOUNT_ID.to_owned(),
        accounts,
        updated_at: unix_timestamp(),
    })
}

fn normalize_state(state: &mut ProviderState) -> Result<()> {
    if !state.accounts.contains_key(DEFAULT_ACCOUNT_ID) {
        state.accounts.insert(
            DEFAULT_ACCOUNT_ID.to_owned(),
            AccountState {
                codex_home: default_codex_home()?,
                quota_wait_until: None,
                last_quota_event: None,
                last_used_at: None,
            },
        );
    }
    if !state.accounts.contains_key(&state.active_account) {
        state.active_account = DEFAULT_ACCOUNT_ID.to_owned();
    }
    Ok(())
}

fn ensure_account_exists(state: &mut ProviderState, account: &str) -> Result<()> {
    validate_account(account)?;
    if !state.accounts.contains_key(account) && account == DEFAULT_ACCOUNT_ID {
        normalize_state(state)?;
    }
    if !state.accounts.contains_key(account) {
        bail!("Codex provider account `{account}` is not configured");
    }
    Ok(())
}

fn remove_account_from_state(state: &mut ProviderState, account: &str) -> Result<()> {
    if account == DEFAULT_ACCOUNT_ID {
        bail!("The built-in Codex account `{DEFAULT_ACCOUNT_ID}` cannot be deleted");
    }
    if state.accounts.remove(account).is_none() {
        bail!("Codex provider account `{account}` is not configured");
    }
    if state.active_account == account {
        state.active_account = state
            .accounts
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| DEFAULT_ACCOUNT_ID.to_owned());
    }
    Ok(())
}

fn refresh_state_from_rate_limits(state: &mut ProviderState, now: u64) {
    for (name, account) in &mut state.accounts {
        if account
            .quota_wait_until
            .is_some_and(|timestamp| timestamp <= now)
        {
            account.quota_wait_until = None;
            account.last_quota_event = None;
        }
        if let Some(limits) = load_rate_limits_for_home(name, &account.codex_home)
            && let Some(blocked_until) = limits.exhausted_until()
        {
            account.quota_wait_until =
                Some(account.quota_wait_until.unwrap_or(0).max(blocked_until));
        }
    }
}

fn select_free_account(state: &ProviderState, now: u64, excluded: Option<&str>) -> Option<String> {
    if excluded != Some(state.active_account.as_str())
        && account_is_available(state, &state.active_account, now)
    {
        return Some(state.active_account.clone());
    }
    state
        .accounts
        .keys()
        .filter(|account| excluded != Some(account.as_str()))
        .find(|account| account_is_available(state, account, now))
        .cloned()
}

fn account_is_available(state: &ProviderState, account: &str, now: u64) -> bool {
    state
        .accounts
        .get(account)
        .is_some_and(|account| account.quota_wait_until.is_none_or(|until| until <= now))
}

fn earliest_retry(state: &ProviderState) -> Option<(String, u64)> {
    state
        .accounts
        .iter()
        .filter_map(|(account, state)| {
            state
                .quota_wait_until
                .map(|retry_at| (account.clone(), retry_at))
        })
        .min_by_key(|(_, retry_at)| *retry_at)
}

fn load_rate_limits_for_home(account: &str, home: &Path) -> Option<RateLimits> {
    let mut files = Vec::new();
    collect_jsonl_files(&home.join("sessions"), &mut files, CODEX_SESSION_SCAN_DEPTH).ok()?;
    collect_jsonl_files(
        &home.join("archived_sessions"),
        &mut files,
        CODEX_ARCHIVED_SESSION_SCAN_DEPTH,
    )
    .ok()?;
    files.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok()
    });
    files.reverse();
    for path in files.into_iter().take(CODEX_RATE_LIMIT_MAX_SESSION_FILES) {
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines().rev().take(CODEX_RATE_LIMIT_MAX_EVENT_LINES) {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(mut limits) = parse_rate_limits(&value) {
                limits.account = account.to_owned();
                return Some(limits);
            }
        }
    }
    None
}

fn collect_jsonl_files(dir: &Path, files: &mut Vec<PathBuf>, depth: usize) -> Result<()> {
    if depth == 0 || !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("Failed to read `{}`", dir.display()))? {
        let entry = entry.with_context(|| format!("Failed to read `{}`", dir.display()))?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_jsonl_files(&path, files, depth - 1)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }
    Ok(())
}

pub fn parse_rate_limits(value: &serde_json::Value) -> Option<RateLimits> {
    let limits = value
        .get("rate_limits")
        .or_else(|| value.pointer("/payload/rate_limits"))?;
    if limits.is_null() {
        return None;
    }
    Some(RateLimits {
        account: String::new(),
        primary: parse_rate_limit(limits.get("primary")?)?,
        secondary: parse_rate_limit(limits.get("secondary")?)?,
    })
}

fn parse_rate_limit(value: &serde_json::Value) -> Option<RateLimit> {
    let now = unix_timestamp();
    let resets_at = value
        .get("resets_at")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("resets_in_seconds")
                .and_then(serde_json::Value::as_u64)
                .map(|seconds| now.saturating_add(seconds))
        })?;
    Some(RateLimit {
        used_percent: value.get("used_percent")?.as_f64()?,
        resets_in_seconds: resets_at.saturating_sub(now),
        resets_at,
    })
}

impl RateLimits {
    fn exhausted_until(&self) -> Option<u64> {
        [self.primary.clone(), self.secondary.clone()]
            .into_iter()
            .filter(|limit| limit.used_percent >= QUOTA_FULL_THRESHOLD)
            .map(|limit| limit.resets_at)
            .max()
    }
}

fn provider_home() -> Result<PathBuf> {
    Ok(maintenance::think_home()?.join("providers"))
}

fn default_codex_home() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("CODEX_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("HOME is not set; cannot locate ~/.codex")?)
            .join(".codex"),
    )
}

fn default_named_account_home(account: &str) -> Result<PathBuf> {
    Ok(provider_home()?
        .join("codex-accounts")
        .join(account)
        .join("codex-home"))
}

fn validate_account(account: &str) -> Result<()> {
    if !account.is_empty()
        && account.len() <= 64
        && account
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
    {
        return Ok(());
    }
    bail!("provider account names must be 1-64 ASCII letters, digits, hyphens, or underscores")
}

fn parse_model_catalog_json(bytes: &[u8]) -> Result<Vec<ModelCatalogEntry>> {
    let catalog = serde_json::from_slice::<ModelCatalogResponse>(bytes)
        .context("Failed to parse Codex model catalog JSON")?;
    Ok(catalog
        .models
        .into_iter()
        .filter(|model| {
            !model.slug.trim().is_empty()
                && model
                    .visibility
                    .as_deref()
                    .is_none_or(|visibility| visibility == "list")
        })
        .map(|model| ModelCatalogEntry {
            display_name: model.display_name.unwrap_or_else(|| model.slug.clone()),
            slug: model.slug,
            description: model
                .description
                .filter(|description| !description.trim().is_empty()),
            supported_reasoning_levels: model
                .supported_reasoning_levels
                .into_iter()
                .filter_map(|level| level.effort.parse::<CodexThinkingLevel>().ok())
                .collect(),
        })
        .collect())
}

#[derive(Deserialize)]
struct ModelCatalogResponse {
    #[serde(default)]
    models: Vec<ModelCatalogModel>,
}

#[derive(Deserialize)]
struct ModelCatalogModel {
    slug: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    visibility: Option<String>,
    #[serde(default)]
    supported_reasoning_levels: Vec<ModelReasoningLevel>,
}

#[derive(Deserialize)]
struct ModelReasoningLevel {
    effort: String,
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn quota_bar(used_percent: f64) -> String {
    let filled =
        ((used_percent.clamp(0.0, 100.0) / 100.0) * QUOTA_BAR_WIDTH as f64).round() as usize;
    format!(
        "[{}{}]",
        "█".repeat(filled),
        "░".repeat(QUOTA_BAR_WIDTH.saturating_sub(filled))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_codex_rate_limit_events() {
        let reset_at = unix_timestamp() + 600;
        let event = serde_json::json!({
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "rate_limits": {
                    "primary": {
                        "used_percent": 3.0,
                        "resets_at": reset_at
                    },
                    "secondary": {
                        "used_percent": 14.0,
                        "resets_in_seconds": 1200
                    }
                }
            }
        });
        let limits = parse_rate_limits(&event).expect("nested limits parsed");
        assert_eq!(limits.primary.used_percent, 3.0);
        assert_eq!(limits.primary.resets_at, reset_at);
        assert!((590..=600).contains(&limits.primary.resets_in_seconds));
        assert_eq!(limits.secondary.used_percent, 14.0);
        assert!((1190..=1200).contains(&limits.secondary.resets_in_seconds));
        assert!(
            parse_rate_limits(&serde_json::json!({
                "payload": { "rate_limits": null }
            }))
            .is_none()
        );
    }

    #[test]
    fn account_selection_skips_waiting_active_account() {
        let now = unix_timestamp();
        let state = ProviderState {
            version: STATE_VERSION,
            active_account: "a".to_owned(),
            accounts: BTreeMap::from([
                (
                    "a".to_owned(),
                    AccountState {
                        codex_home: PathBuf::from("/tmp/a"),
                        quota_wait_until: Some(now + 60),
                        last_quota_event: None,
                        last_used_at: None,
                    },
                ),
                (
                    "b".to_owned(),
                    AccountState {
                        codex_home: PathBuf::from("/tmp/b"),
                        quota_wait_until: None,
                        last_quota_event: None,
                        last_used_at: None,
                    },
                ),
            ]),
            updated_at: now,
        };
        assert_eq!(select_free_account(&state, now, None).as_deref(), Some("b"));
        assert_eq!(earliest_retry(&state), Some(("a".to_owned(), now + 60)));
    }

    #[test]
    fn removing_active_account_selects_remaining_account() {
        let mut state = ProviderState {
            version: STATE_VERSION,
            active_account: "work".to_owned(),
            accounts: BTreeMap::from([
                (
                    DEFAULT_ACCOUNT_ID.to_owned(),
                    AccountState {
                        codex_home: PathBuf::from("/tmp/default"),
                        quota_wait_until: None,
                        last_quota_event: None,
                        last_used_at: None,
                    },
                ),
                (
                    "work".to_owned(),
                    AccountState {
                        codex_home: PathBuf::from("/tmp/work"),
                        quota_wait_until: None,
                        last_quota_event: None,
                        last_used_at: None,
                    },
                ),
            ]),
            updated_at: unix_timestamp(),
        };

        remove_account_from_state(&mut state, "work").unwrap();

        assert!(!state.accounts.contains_key("work"));
        assert_eq!(state.active_account, DEFAULT_ACCOUNT_ID);
        assert!(remove_account_from_state(&mut state, DEFAULT_ACCOUNT_ID).is_err());
    }

    #[test]
    fn parses_visible_model_catalog() {
        let models = parse_model_catalog_json(
            br#"{
                "models": [
                    {
                        "slug": "gpt-5.5",
                        "display_name": "GPT-5.5",
                        "description": "Frontier model",
                        "visibility": "list",
                        "supported_reasoning_levels": [
                            { "effort": "low" },
                            { "effort": "medium" },
                            { "effort": "unknown" }
                        ],
                        "base_instructions": "ignored"
                    },
                    {
                        "slug": "hidden-model",
                        "visibility": "hidden"
                    }
                ]
            }"#,
        )
        .expect("model catalog parses");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].slug, "gpt-5.5");
        assert_eq!(models[0].display_name, "GPT-5.5");
        assert_eq!(
            models[0].supported_reasoning_levels,
            vec![CodexThinkingLevel::Low, CodexThinkingLevel::Medium]
        );
    }
}
