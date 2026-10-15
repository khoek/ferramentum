//! Build identity is compiled in; reporting it never reads configuration or the network.
use std::sync::OnceLock;

use anyhow::Result;
use serde_json::json;

fn revision() -> Option<&'static str> {
    option_env!("ICE_BUILD_REVISION").filter(|value| {
        matches!(value.len(), 40 | 64) && value.bytes().all(|c| c.is_ascii_hexdigit())
    })
}

fn dirty() -> Option<bool> {
    revision()?;
    match option_env!("ICE_BUILD_DIRTY") {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    }
}

pub(crate) fn display() -> &'static str {
    static DISPLAY: OnceLock<String> = OnceLock::new();
    DISPLAY.get_or_init(|| {
        let source = revision().map_or_else(
            || "source revision unknown".to_owned(),
            |revision| {
                format!(
                    "{}{}",
                    &revision[..12],
                    match dirty() {
                        Some(true) => ", modified source",
                        Some(false) => "",
                        None => ", source cleanliness unknown",
                    }
                )
            },
        );
        format!(
            "{} ({source}; JSON schema {})",
            env!("CARGO_PKG_VERSION"),
            crate::output::SCHEMA_VERSION
        )
    })
}

pub(crate) fn print(json_output: bool) -> Result<()> {
    if json_output {
        return crate::output::emit(
            "version",
            None,
            json!({
                "version":env!("CARGO_PKG_VERSION"),
                "source_revision":revision(), "source_dirty":dirty(),
                "json_schema_version":crate::output::SCHEMA_VERSION,
            }),
        );
    }
    println!("ice {}", display());
    Ok(())
}
