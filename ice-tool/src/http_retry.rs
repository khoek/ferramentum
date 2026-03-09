use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use reqwest::blocking::{RequestBuilder, Response};
use reqwest::header::RETRY_AFTER;

#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 6,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
        }
    }
}

impl BackoffPolicy {
    pub(crate) fn delay_for_retry(self, retry_index: u32) -> Duration {
        let capped_shift = retry_index.min(20);
        let base_ms = self.initial_delay.as_millis() as u64;
        let max_ms = self.max_delay.as_millis() as u64;
        let factor = 1u64 << capped_shift;
        let millis = base_ms.saturating_mul(factor).clamp(1, max_ms.max(1));
        Duration::from_millis(millis)
    }
}

pub fn send_with_429_backoff<F>(
    mut make_request: F,
    context: &str,
    policy: BackoffPolicy,
) -> Result<Response>
where
    F: FnMut() -> RequestBuilder,
{
    let attempts = policy.max_attempts.max(1);
    for attempt in 1..=attempts {
        let response = make_request()
            .send()
            .with_context(|| format!("Failed to {context} (request send error)"))?;

        if response.status() != StatusCode::TOO_MANY_REQUESTS || attempt == attempts {
            return Ok(response);
        }

        let retry_index = attempt.saturating_sub(1);
        let delay =
            retry_after_delay(&response).unwrap_or_else(|| policy.delay_for_retry(retry_index));
        thread::sleep(delay.min(policy.max_delay));
    }

    unreachable!("send_with_429_backoff loop must return before exhaustion")
}

fn retry_after_delay(response: &Response) -> Option<Duration> {
    let raw = response.headers().get(RETRY_AFTER)?.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }

    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds.max(1)));
    }

    let parsed = DateTime::parse_from_rfc2822(raw).ok()?;
    let target = parsed.with_timezone(&Utc);
    let now = Utc::now();
    let secs = (target - now).num_seconds();
    if secs <= 0 {
        Some(Duration::from_secs(1))
    } else {
        Some(Duration::from_secs(secs as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_delay_grows_exponentially_and_caps() {
        let policy = BackoffPolicy {
            max_attempts: 6,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_millis(1000),
        };

        assert_eq!(policy.delay_for_retry(0), Duration::from_millis(200));
        assert_eq!(policy.delay_for_retry(1), Duration::from_millis(400));
        assert_eq!(policy.delay_for_retry(2), Duration::from_millis(800));
        assert_eq!(policy.delay_for_retry(3), Duration::from_millis(1000));
        assert_eq!(policy.delay_for_retry(8), Duration::from_millis(1000));
    }
}
