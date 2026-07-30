use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use reqwest::header::{HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::auth::Credential;
use super::paths::RuntimePaths;

const DEFAULT_CHATGPT_BASE_URL: &str = "https://chatgpt.com/backend-api";
const QUOTA_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const QUOTA_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    usage_url: Url,
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
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    rate_limit: Option<RateLimitStatus>,
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
    reset_at: i64,
}

impl Client {
    pub fn new(paths: &RuntimePaths) -> Result<Self> {
        let usage_url = usage_url(paths)?;
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(QUOTA_CONNECT_TIMEOUT)
                .timeout(QUOTA_REQUEST_TIMEOUT)
                .build()
                .context("could not initialize the quota HTTP client")?,
            usage_url,
        })
    }

    pub async fn fetch(&self, request: Request) -> Result<Snapshot> {
        let mut request_builder = self
            .http
            .get(self.usage_url.clone())
            .header(USER_AGENT, concat!("kai/", env!("CARGO_PKG_VERSION")))
            .header("ChatGPT-Account-Id", request.account_id)
            .bearer_auth(request.access_token.as_str());
        if request.account_is_fedramp {
            request_builder = request_builder.header("X-OpenAI-Fedramp", "true");
        }
        let response = request_builder
            .send()
            .await
            .context("could not reach the Codex quota service")?;
        let status = response.status();
        if !status.is_success() {
            bail!("Codex quota service returned HTTP {status}");
        }
        let response = response
            .json::<UsageResponse>()
            .await
            .context("Codex quota service returned an invalid response")?;
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
        Ok(Snapshot {
            remaining_percent: (100.0 - window.used_percent).clamp(0.0, 100.0),
            resets_at: window.reset_at,
            window_seconds: window.limit_window_seconds.filter(|seconds| *seconds > 0),
        })
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

fn usage_url(paths: &RuntimePaths) -> Result<Url> {
    let config = paths.read_codex_config()?;
    let base_url = config
        .as_ref()
        .and_then(|config| config.get("chatgpt_base_url"))
        .and_then(toml::Value::as_str)
        .filter(|url| !url.trim().is_empty())
        .unwrap_or(DEFAULT_CHATGPT_BASE_URL)
        .trim_end_matches('/');
    let path = if base_url.contains("/backend-api") {
        "wham/usage"
    } else {
        "api/codex/usage"
    };
    Url::parse(&format!("{base_url}/{path}")).context("Codex `chatgpt_base_url` is not a valid URL")
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
            usage_url(&paths).unwrap().as_str(),
            "https://chatgpt.com/backend-api/wham/usage"
        );

        fs::write(
            paths.codex_config(),
            "chatgpt_base_url = \"https://example.test/codex\"\n",
        )
        .unwrap();
        assert_eq!(
            usage_url(&paths).unwrap().as_str(),
            "https://example.test/codex/api/codex/usage"
        );

        fs::write(
            paths.codex_config(),
            "chatgpt_base_url = \"https://example.test/backend-api/\"\n",
        )
        .unwrap();
        assert_eq!(
            usage_url(&paths).unwrap().as_str(),
            "https://example.test/backend-api/wham/usage"
        );
    }
}
