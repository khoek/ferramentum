//! Latest reported cumulative usage for one thread, never a sum of snapshots.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct Usage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    #[serde(default)]
    pub cached_input_tokens: i64,
    #[serde(default)]
    pub cache_write_input_tokens: i64,
    #[serde(default)]
    pub reasoning_output_tokens: i64,
}

impl Usage {
    pub(super) fn validated(self) -> Option<Self> {
        [
            self.input_tokens,
            self.output_tokens,
            self.total_tokens,
            self.cached_input_tokens,
            self.cache_write_input_tokens,
            self.reasoning_output_tokens,
        ]
        .into_iter()
        .all(|value| value >= 0)
        .then_some(self)
    }

    pub(super) fn label(&self) -> String {
        for (divisor, suffix) in [
            (1_000_000_000_000, "T"),
            (1_000_000_000, "B"),
            (1_000_000, "M"),
            (1_000, "K"),
        ] {
            if self.total_tokens >= divisor {
                return format!("{:.1}{suffix}", self.total_tokens as f64 / divisor as f64);
            }
        }
        self.total_tokens.to_string()
    }

    pub(super) fn details(&self) -> [String; 3] {
        [
            format!(
                "Tokens: {} total · {} input · {} output",
                self.total_tokens, self.input_tokens, self.output_tokens
            ),
            format!(
                "Input tokens: {} cached · {} cache write",
                self.cached_input_tokens, self.cache_write_input_tokens
            ),
            format!(
                "Reasoning tokens: {} (included in output)",
                self.reasoning_output_tokens
            ),
        ]
    }
}

#[derive(Deserialize)]
pub(super) struct Record {
    pub thread_id: String,
    pub thread_token_usage: Usage,
}

#[derive(Deserialize)]
pub(super) struct Info {
    pub total_token_usage: Usage,
}
