//! Verda v1 API. Mutations are sent once: an ambiguous response is not a retry signal.
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::{Method, StatusCode, blocking::Client as Http};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::automation::error;
use crate::http_retry::{BackoffPolicy, retry_after_delay};
use crate::model::IceConfig;
use crate::support::nonempty_string;

const API: &str = "https://api.verda.com/v1";
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(super) struct Credentials {
    pub(super) client_id: String,
    pub(super) client_secret: String,
}

impl Credentials {
    pub(super) fn environment() -> Result<Option<Self>> {
        Self::environment_pair(
            std::env::var("VERDA_CLIENT_ID").ok(),
            std::env::var("VERDA_CLIENT_SECRET").ok(),
        )
    }

    fn environment_pair(id: Option<String>, secret: Option<String>) -> Result<Option<Self>> {
        match (
            id.and_then(nonempty_string),
            secret.and_then(nonempty_string),
        ) {
            (None, None) => Ok(None),
            (Some(client_id), Some(client_secret)) => Ok(Some(Self {
                client_id,
                client_secret,
            })),
            _ => Err(missing(
                "Supply both VERDA_CLIENT_ID and VERDA_CLIENT_SECRET, or unset both to use saved credentials or interactive login.",
            )),
        }
    }

    pub(super) fn saved(config: &IceConfig) -> Option<Self> {
        Some(Self {
            client_id: nonempty_string(config.auth.verda.client_id.clone()?)?,
            client_secret: nonempty_string(config.auth.verda.client_secret.clone()?)?,
        })
    }
}

struct Token {
    value: String,
    expires: Instant,
}

pub(crate) struct Client {
    http: Http,
    base: String,
    client_id: String,
    client_secret: String,
    token: Mutex<Option<Token>>,
    retry_policy: BackoffPolicy,
}

pub(super) fn missing(message: &str) -> anyhow::Error {
    error(
        "missing_credentials",
        message,
        json!({"environment": ["VERDA_CLIENT_ID", "VERDA_CLIENT_SECRET"]}),
    )
}

impl Client {
    pub(super) fn from_config(config: &IceConfig) -> Result<Self> {
        let credentials = Credentials::environment()?.or_else(|| Credentials::saved(config))
            .ok_or_else(|| missing("Run `ice login --cloud verda` interactively, or supply both VERDA_CLIENT_ID and VERDA_CLIENT_SECRET (auth.verda.client_id/client_secret in config)."))?;
        Self::from_credentials(&credentials)
    }

    pub(super) fn from_credentials(credentials: &Credentials) -> Result<Self> {
        Self::new(
            API,
            credentials.client_id.clone(),
            credentials.client_secret.clone(),
        )
    }

    fn new(base: &str, client_id: String, client_secret: String) -> Result<Self> {
        Ok(Self {
            http: Http::builder()
                .timeout(TIMEOUT)
                .connect_timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("ice/verda")
                .build()?,
            base: base.to_owned(),
            client_id,
            client_secret,
            token: Mutex::new(None),
            retry_policy: BackoffPolicy::default(),
        })
    }

    #[cfg(test)]
    pub(super) fn mock(base: &str) -> Result<Self> {
        let mut client = Self::new(base, "test-client".into(), "test-secret".into())?;
        client.retry_policy.initial_delay = Duration::from_millis(1);
        client.retry_policy.max_delay = Duration::from_millis(2);
        Ok(client)
    }

    fn remaining(deadline: Instant) -> Result<Duration> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .context("Verda request deadline elapsed")
    }

    fn token(&self, deadline: Instant) -> Result<String> {
        let mut token = self
            .token
            .lock()
            .map_err(|_| anyhow::anyhow!("Verda token lock failed"))?;
        if let Some(token) = token.as_ref().filter(|t| t.expires > Instant::now()) {
            return Ok(token.value.clone());
        }
        let response = self.http.post(format!("{}/oauth2/token", self.base))
            .timeout(Self::remaining(deadline)?)
            .json(&json!({"grant_type":"client_credentials", "client_id":self.client_id, "client_secret":self.client_secret}))
            .send().map_err(|_| missing("Verda token request failed; check credentials and connectivity."))?;
        if !response.status().is_success() {
            return Err(missing(
                "Verda authentication failed; check the configured client credentials.",
            ));
        }
        let body: Value = response.json().context("Invalid Verda token response")?;
        let value = body["access_token"]
            .as_str()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| missing("Verda token response omitted access_token."))?
            .to_owned();
        let lifetime = body["expires_in"]
            .as_u64()
            .unwrap_or(60)
            .min(86_400)
            .saturating_sub(30);
        *token = Some(Token {
            value: value.clone(),
            expires: Instant::now() + Duration::from_secs(lifetime),
        });
        Ok(value)
    }

    pub(super) fn authenticate(&self) -> Result<()> {
        self.token(Instant::now() + TIMEOUT).map(|_| ())
    }

    fn wait_for_retry(
        &self,
        path: &str,
        attempt: u32,
        retry_after: Option<Duration>,
        deadline: Instant,
    ) -> Result<()> {
        let delay = retry_after.unwrap_or_else(|| self.retry_policy.delay_for_retry(attempt - 1));
        // Never cap a provider Retry-After to an earlier time. If it cannot fit
        // inside the caller's budget, stop without sending another request.
        if delay >= deadline.saturating_duration_since(Instant::now()) {
            return Err(error(
                "verda_retry_deadline",
                "Verda read retry would exceed the request deadline; try again later.",
                json!({"path":path, "attempts":attempt, "retry_after_seconds":delay.as_secs_f64(), "mutation_retried":false}),
            ));
        }
        let cancellation = capulus::Cancellation::install()?;
        cancellation.sleep(delay)?;
        Ok(())
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<(Value, Option<usize>)> {
        let deadline = Instant::now() + crate::lifecycle::remaining_timeout(timeout)?;
        let read = method == Method::GET;
        let max_attempts = if read {
            self.retry_policy.max_attempts.max(1)
        } else {
            1
        };
        let mut refreshed = false;
        for attempt in 1..=max_attempts {
            let token = self.token(deadline)?;
            let mut request = self
                .http
                .request(method.clone(), format!("{}{path}", self.base))
                .bearer_auth(token)
                .timeout(Self::remaining(deadline)?);
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = match request.send() {
                Ok(response) => response,
                Err(err) => {
                    if read
                        && attempt < max_attempts
                        && (err.is_connect() || err.is_timeout() || err.is_body())
                    {
                        self.wait_for_retry(path, attempt, None, deadline)?;
                        continue;
                    }
                    return Err(error(
                        "verda_request_failed",
                        if read {
                            "Verda read request failed; check connectivity and try again."
                                .to_owned()
                        } else {
                            format!(
                                "Verda {method} request failed; mutation outcome may be unknown. Reconcile resources before retrying."
                            )
                        },
                        json!({"path":path, "attempts":attempt, "mutation_retried":false}),
                    ));
                }
            };
            let status = response.status();
            if status == StatusCode::UNAUTHORIZED && read && !refreshed && attempt < max_attempts {
                *self
                    .token
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Verda token lock failed"))? = None;
                refreshed = true;
                continue;
            }
            let retry_after = retry_after_delay(&response);
            if read
                && attempt < max_attempts
                && matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
            {
                drop(response);
                self.wait_for_retry(path, attempt, retry_after, deadline)?;
                continue;
            }
            if !status.is_success() {
                // Provider messages can echo submitted credentials or user data.
                return Err(error(
                    if status == StatusCode::NOT_FOUND {
                        "verda_not_found"
                    } else {
                        "verda_api_error"
                    },
                    format!(
                        "Verda {method} returned HTTP {status}; check capacity, quota, credentials and the selected resource."
                    ),
                    json!({"http_status": status.as_u16(), "path": path, "attempts":attempt, "retry_after_seconds":retry_after.map(|delay| delay.as_secs_f64()), "mutation_retried": false}),
                ));
            }
            let total = response
                .headers()
                .get("X-Total-Count")
                .map(|header| {
                    header
                        .to_str()
                        .ok()
                        .and_then(|v| v.parse::<usize>().ok())
                        .context("Invalid Verda pagination header")
                })
                .transpose()?;
            let text = match response.text() {
                Ok(text) => text,
                Err(_) if read && attempt < max_attempts => {
                    self.wait_for_retry(path, attempt, None, deadline)?;
                    continue;
                }
                Err(_) => {
                    return Err(error(
                        "verda_response_unreadable",
                        if read {
                            "Unable to read Verda response; try again later."
                        } else {
                            "Unable to read Verda response; reconcile the mutation before retrying."
                        },
                        json!({"path":path,"attempts":attempt,"mutation_retried":false}),
                    ));
                }
            };
            let value = if text.trim().is_empty() {
                Value::Null
            } else if method == Method::POST && path == "/instances" && super::valid_id(text.trim())
            {
                // Verda deploy receipts may be plain UUID text. Do not broaden
                // this exception to reads or unrelated mutation responses.
                Value::String(text.trim().to_owned())
            } else {
                serde_json::from_str(&text).context(if read {
                    "Invalid Verda JSON in read response"
                } else {
                    "Invalid Verda JSON; reconcile a mutation before retrying"
                })?
            };
            return Ok((value, total));
        }
        unreachable!()
    }

    pub(super) fn get<T: DeserializeOwned>(&self, path: &str, timeout: Duration) -> Result<T> {
        let (value, _) = self.request(Method::GET, path, None, timeout)?;
        serde_json::from_value(value).context("Unexpected Verda response schema")
    }

    pub(super) fn list<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        self.list_until(path, Instant::now() + TIMEOUT)
    }

    pub(super) fn list_until<T: DeserializeOwned>(
        &self,
        path: &str,
        deadline: Instant,
    ) -> Result<Vec<T>> {
        let mut result = Vec::new();
        for page in 1..=1000 {
            let separator = if path.contains('?') { '&' } else { '?' };
            let (value, total) = self.request(
                Method::GET,
                &format!("{path}{separator}page={page}&pageSize=100"),
                None,
                Self::remaining(deadline)?,
            )?;
            let rows: Vec<T> =
                serde_json::from_value(value).context("Expected Verda array response")?;
            let empty = rows.is_empty();
            result.extend(rows);
            if total.is_none_or(|n| result.len() >= n) {
                return Ok(result);
            }
            if empty {
                anyhow::bail!("Incomplete Verda pagination response");
            }
        }
        anyhow::bail!("Verda pagination limit exceeded")
    }

    pub(super) fn mutate(&self, method: Method, path: &str, body: &Value) -> Result<Value> {
        self.request(method, path, Some(body), TIMEOUT)
            .map(|(value, _)| value)
    }
}

#[cfg(test)]
mod credential_tests {
    use super::*;

    #[test]
    fn environment_requires_one_complete_pair() {
        for (id, secret) in [(None, None), (Some(" ".into()), Some("".into()))] {
            assert!(Credentials::environment_pair(id, secret).unwrap().is_none());
        }
        for (id, secret) in [
            (Some("id".into()), None),
            (None, Some("secret".into())),
            (Some("id".into()), Some(" ".into())),
        ] {
            assert!(Credentials::environment_pair(id, secret).is_err());
        }
        let pair = Credentials::environment_pair(Some(" id ".into()), Some(" secret ".into()))
            .unwrap()
            .unwrap();
        assert_eq!(pair.client_id, "id");
        assert_eq!(pair.client_secret, "secret");
    }

    #[test]
    fn incomplete_saved_credentials_require_a_new_pair() {
        let mut config = IceConfig::default();
        config.auth.verda.client_id = Some("old-id".into());
        assert!(Credentials::saved(&config).is_none());
        config.auth.verda.client_secret = Some(" ".into());
        assert!(Credentials::saved(&config).is_none());
        config.auth.verda.client_secret = Some("secret".into());
        let pair = Credentials::saved(&config).unwrap();
        assert_eq!(pair.client_id, "old-id");
        assert_eq!(pair.client_secret, "secret");
    }
}
