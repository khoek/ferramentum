use std::error::Error;
use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::DateTime;
use reqwest::Url;
use reqwest::header::{HeaderValue, USER_AGENT};
use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::auth::Credential;
use super::paths::RuntimePaths;

const DEFAULT_CHATGPT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const QUOTA_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const QUOTA_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const UNSTARTED_COUNTDOWN_SECONDS: i64 = 7 * 24 * 60 * 60;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    usage_url: Url,
    reset_credits_url: Url,
}

pub struct Request {
    access_token: Zeroizing<String>,
    account_id: HeaderValue,
    account_is_fedramp: bool,
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
struct UsageResponse {
    rate_limit: Option<RateLimitStatus>,
    rate_limit_reset_credits: Option<ResetCreditsSummary>,
}

#[derive(Debug, Deserialize)]
struct ResetCreditsSummary {
    available_count: i64,
}

#[derive(Debug, Deserialize)]
struct ResetCreditsDetails {
    #[serde(default)]
    credits: Vec<ResetCredit>,
    available_count: i64,
}

#[derive(Debug, Deserialize)]
struct ResetCredit {
    reset_type: String,
    status: String,
    expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RateLimitStatus {
    primary_window: Option<Window>,
    secondary_window: Option<Window>,
}

#[derive(Debug, Deserialize)]
struct Window {
    used_percent: f64,
    #[serde(default)]
    limit_window_seconds: Option<i64>,
    #[serde(default)]
    reset_after_seconds: Option<i64>,
    reset_at: i64,
}

#[derive(Debug)]
struct HttpStatusError(StatusCode);

impl fmt::Display for HttpStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Codex quota service returned HTTP {}", self.0)
    }
}

impl Error for HttpStatusError {}

impl Client {
    pub fn new(paths: &RuntimePaths) -> Result<Self> {
        let (usage_url, reset_credits_url) = quota_urls(paths)?;
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(QUOTA_CONNECT_TIMEOUT)
                .timeout(QUOTA_REQUEST_TIMEOUT)
                .build()
                .context("could not initialize the quota HTTP client")?,
            usage_url,
            reset_credits_url,
        })
    }

    pub async fn fetch(&self, request: Request) -> Result<Snapshot> {
        let response = checked_response(
            self.authenticated_get(self.usage_url.clone(), &request)
                .send()
                .await
                .context("could not reach the Codex quota service")?,
        )?;
        let response = response
            .json::<UsageResponse>()
            .await
            .context("Codex quota service returned an invalid response")?;
        let reset_credit_count = response
            .rate_limit_reset_credits
            .map(|summary| summary.available_count.max(0) as u64)
            .unwrap_or_default();
        let rate_limit = response
            .rate_limit
            .context("Codex quota response did not include a rate limit")?;
        let window = rate_limit
            .primary_window
            .or(rate_limit.secondary_window)
            .context("Codex quota response did not include a quota window")?;
        if !window.used_percent.is_finite() {
            bail!("Codex quota response contained a non-finite percentage");
        }
        if window.reset_at <= 0 {
            bail!("Codex quota response did not include a valid reset datetime");
        }
        let rate_limit_reset_credits = if reset_credit_count == 0 {
            None
        } else {
            Some(match self.fetch_reset_credits(&request).await {
                Ok(credits) => credits,
                Err(_) => ResetCredits {
                    available_count: reset_credit_count,
                    latest_expires_at: None,
                },
            })
            .filter(|credits| credits.available_count > 0)
        };
        Ok(Snapshot {
            remaining_percent: (100.0 - window.used_percent).clamp(0.0, 100.0),
            resets_at: window.reset_at,
            window_seconds: window.limit_window_seconds.filter(|seconds| *seconds > 0),
            reset_after_seconds: window.reset_after_seconds.filter(|seconds| *seconds >= 0),
            rate_limit_reset_credits,
        })
    }

    fn authenticated_get(&self, url: Url, request: &Request) -> reqwest::RequestBuilder {
        let mut request_builder = self
            .http
            .get(url)
            .header(USER_AGENT, concat!("kai/", env!("CARGO_PKG_VERSION")))
            .header("ChatGPT-Account-Id", request.account_id.clone())
            .bearer_auth(request.access_token.as_str());
        if request.account_is_fedramp {
            request_builder = request_builder.header("X-OpenAI-Fedramp", "true");
        }
        request_builder
    }

    async fn fetch_reset_credits(&self, request: &Request) -> Result<ResetCredits> {
        let details = checked_response(
            self.authenticated_get(self.reset_credits_url.clone(), request)
                .send()
                .await
                .context("could not reach the Codex reset-credit service")?,
        )?
        .json::<ResetCreditsDetails>()
        .await
        .context("Codex reset-credit service returned an invalid response")?;
        let now = chrono::Utc::now().timestamp();
        let returned_count = details.credits.len() as u64;
        let usable_expiries = details
            .credits
            .iter()
            .filter_map(|credit| credit.usable_expiry(now))
            .collect::<Vec<_>>();
        let reported_count = details.available_count.max(0) as u64;
        Ok(ResetCredits {
            available_count: if returned_count >= reported_count {
                usable_expiries.len() as u64
            } else {
                reported_count.max(usable_expiries.len() as u64)
            },
            latest_expires_at: usable_expiries.into_iter().flatten().max(),
        })
    }
}

impl ResetCredit {
    fn usable_expiry(&self, now: i64) -> Option<Option<i64>> {
        if self.reset_type != "codex_rate_limits" || self.status != "available" {
            return None;
        }
        match self.expires_at.as_deref() {
            None => Some(None),
            Some(expires_at) => DateTime::parse_from_rfc3339(expires_at)
                .ok()
                .map(|expires_at| expires_at.timestamp())
                .filter(|expires_at| *expires_at > now)
                .map(Some),
        }
    }
}

impl Request {
    pub fn from_credential(credential: &Credential) -> Result<Self> {
        Ok(Self {
            access_token: credential.access_token()?,
            account_id: HeaderValue::from_str(&credential.facts.account_id)
                .context("credential account ID cannot be sent as an HTTP header")?,
            account_is_fedramp: credential.account_is_fedramp(),
        })
    }
}

pub fn requires_authentication(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<HttpStatusError>()
        .is_some_and(|error| error.0 == StatusCode::UNAUTHORIZED)
}

pub fn countdown_has_not_started(snapshot: &Snapshot, now: i64) -> bool {
    snapshot
        .reset_after_seconds
        .unwrap_or_else(|| snapshot.resets_at.saturating_sub(now))
        == UNSTARTED_COUNTDOWN_SECONDS
}

fn checked_response(response: Response) -> Result<Response> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(HttpStatusError(response.status()).into())
    }
}

fn quota_urls(paths: &RuntimePaths) -> Result<(Url, Url)> {
    let config = paths.read_codex_config()?;
    let base_url = config
        .as_ref()
        .and_then(|config| config.get("chatgpt_base_url"))
        .and_then(toml::Value::as_str)
        .filter(|url| !url.trim().is_empty())
        .unwrap_or(DEFAULT_CHATGPT_BASE_URL)
        .trim_end_matches('/');
    let (usage_path, reset_credits_path) = if base_url.contains("/backend-api") {
        ("wham/usage", "wham/rate-limit-reset-credits")
    } else {
        ("api/codex/usage", "api/codex/rate-limit-reset-credits")
    };
    Ok((
        Url::parse(&format!("{base_url}/{usage_path}"))
            .context("Codex `chatgpt_base_url` is not a valid URL")?,
        Url::parse(&format!("{base_url}/{reset_credits_path}"))
            .context("Codex `chatgpt_base_url` is not a valid URL")?,
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn derives_chatgpt_and_codex_api_usage_urls() {
        let root = tempdir().unwrap();
        let paths =
            RuntimePaths::new(root.path().join("credentials"), root.path().join("codex")).unwrap();
        fs::create_dir_all(&paths.codex_home).unwrap();

        assert_eq!(
            quota_urls(&paths).unwrap().0.as_str(),
            "https://chatgpt.com/backend-api/wham/usage"
        );
        assert_eq!(
            quota_urls(&paths).unwrap().1.as_str(),
            "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
        );

        fs::write(
            paths.codex_config(),
            "chatgpt_base_url = \"https://example.test/codex\"\n",
        )
        .unwrap();
        assert_eq!(
            quota_urls(&paths).unwrap().0.as_str(),
            "https://example.test/codex/api/codex/usage"
        );
        assert_eq!(
            quota_urls(&paths).unwrap().1.as_str(),
            "https://example.test/codex/api/codex/rate-limit-reset-credits"
        );

        fs::write(
            paths.codex_config(),
            "chatgpt_base_url = \"https://example.test/backend-api/\"\n",
        )
        .unwrap();
        assert_eq!(
            quota_urls(&paths).unwrap().0.as_str(),
            "https://example.test/backend-api/wham/usage"
        );
    }
}
