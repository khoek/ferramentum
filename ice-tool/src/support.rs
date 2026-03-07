use std::collections::HashSet;
use std::io::{self, IsTerminal, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use dialoguer::{Confirm, Input, theme::ColorfulTheme};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use names::{ADJECTIVES as NAMES_ADJECTIVES, Generator, NOUNS as NAMES_NOUNS, Name};
use reqwest::blocking::Response;
use serde_json::Value;

use crate::model::{Cloud, IceConfig, PrefixLookup};

pub(crate) const CONFIG_DIR_NAME: &str = ".ice";
pub(crate) const CONFIG_FILE_NAME: &str = "config.toml";
pub(crate) const ICE_LABEL_PREFIX: &str = "ice-";
pub(crate) const VAST_DEFAULT_IMAGE: &str = "vastai/base-image:@vastai-automatic-tag";
pub(crate) const VAST_DEFAULT_DISK_GB: f64 = 32.0;
pub(crate) const VAST_WAIT_TIMEOUT_SECS: u64 = 900;
pub(crate) const VAST_POLL_INTERVAL_SECS: u64 = 5;
pub(crate) const VAST_LOG_READY_TIMEOUT_SECS: u64 = 30;
pub(crate) const VAST_LOG_READY_POLL_INTERVAL_MILLIS: u64 = 1000;
pub(crate) const ICE_MANAGED_CONTAINER_NAME: &str = "ice-workload";
pub(crate) const ICE_WORKLOAD_KIND_METADATA_KEY: &str = "ice-workload-kind";
pub(crate) const ICE_WORKLOAD_REGISTRY_METADATA_KEY: &str = "ice-workload-registry";
pub(crate) const ICE_WORKLOAD_CONTAINER_METADATA_KEY: &str = "ice-workload-container";
pub(crate) const ICE_WORKLOAD_SOURCE_METADATA_KEY: &str = "ice-workload-source";
pub(crate) const ICE_LOCAL_CLOUD_LABEL_KEY: &str = "ice-cloud";
pub(crate) const ICE_LOCAL_CLOUD_LABEL_VALUE: &str = "local";
pub(crate) const ICE_RUNTIME_SECONDS_LABEL_KEY: &str = "ice-runtime-seconds";
pub(crate) const ICE_LOCAL_UNPACK_METADATA_FILE: &str = "instance.json";
pub(crate) const ICE_UNPACK_ROOT_DIR: &str = "~/.ice/unpack";
pub(crate) const ICE_UNPACK_ROOTFS_DIR: &str = "rootfs";
pub(crate) const ICE_UNPACK_RUN_SCRIPT: &str = "run.sh";
pub(crate) const ICE_UNPACK_SHELL_SCRIPT: &str = "shell.sh";
pub(crate) const ICE_UNPACK_ENV_SCRIPT: &str = "env.sh";
pub(crate) const ICE_UNPACK_LOG_FILE: &str = "stdio.log";
pub(crate) const ICE_UNPACK_PID_FILE: &str = "pid";
pub(crate) const ICE_UNPACK_EXIT_CODE_FILE: &str = "exit-code";
pub(crate) const GCP_CONTAINER_IMAGE_FAMILY: &str = "cos-stable";
pub(crate) const GCP_CONTAINER_IMAGE_PROJECT: &str = "cos-cloud";
pub(crate) const GCP_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

const RANDOM_NAME_COLLISION_RETRIES: usize = 256;
const NUMBERED_NAME_COLLISION_RETRIES: usize = 2048;
const NUMBERED_NAME_SUFFIX_MAX: u16 = 9_999;

pub(crate) const KNOWN_VAST_GPU_MODELS: &[&str] = &[
    "A10",
    "A100 PCIE",
    "A100 SXM4",
    "A100X",
    "A40",
    "A800 PCIE",
    "B200",
    "CMP 50HX",
    "GTX 1050",
    "GTX 1050 Ti",
    "GTX 1060",
    "GTX 1070",
    "GTX 1070 Ti",
    "GTX 1080",
    "GTX 1080 Ti",
    "GTX 1650",
    "GTX 1650 S",
    "GTX 1660",
    "GTX 1660 S",
    "GTX 1660 Ti",
    "H100 NVL",
    "H100 PCIE",
    "H100 SXM",
    "H200",
    "H200 NVL",
    "L4",
    "L40",
    "L40S",
    "Q RTX 4000",
    "Q RTX 6000",
    "Q RTX 8000",
    "Quadro P2000",
    "Quadro P4000",
    "Radeon VII",
    "RTX 2000Ada",
    "RTX 2060",
    "RTX 2060S",
    "RTX 2070",
    "RTX 2070S",
    "RTX 2080",
    "RTX 2080 Ti",
    "RTX 3050",
    "RTX 3060",
    "RTX 3060 laptop",
    "RTX 3060 Ti",
    "RTX 3070",
    "RTX 3070 laptop",
    "RTX 3070 Ti",
    "RTX 3080",
    "RTX 3080 Ti",
    "RTX 3090",
    "RTX 3090 Ti",
    "RTX 4000Ada",
    "RTX 4060",
    "RTX 4060 Ti",
    "RTX 4070",
    "RTX 4070 laptop",
    "RTX 4070S",
    "RTX 4070S Ti",
    "RTX 4070 Ti",
    "RTX 4080",
    "RTX 4080S",
    "RTX 4090",
    "RTX 4090D",
    "RTX 4500Ada",
    "RTX 5000Ada",
    "RTX 5060",
    "RTX 5060 Ti",
    "RTX 5070",
    "RTX 5070 Ti",
    "RTX 5080",
    "RTX 5090",
    "RTX 5880Ada",
    "RTX 6000Ada",
    "RTX A2000",
    "RTX A4000",
    "RTX A4500",
    "RTX A5000",
    "RTX A6000",
    "RTX PRO 4000",
    "RTX PRO 4500",
    "RTX PRO 5000",
    "RTX PRO 6000 S",
    "RTX PRO 6000 WS",
    "RX 6950 XT",
    "Tesla P100",
    "Tesla P4",
    "Tesla P40",
    "Tesla T4",
    "Tesla V100",
    "Titan RTX",
    "Titan V",
    "Titan Xp",
];

const NAMEGEN_ADJECTIVES: &[&str] = &[
    "agile",
    "adamant",
    "adept",
    "adventurous",
    "airy",
    "amber",
    "balanced",
    "arcadian",
    "auspicious",
    "awesome",
    "blossoming",
    "brave",
    "bright",
    "calm",
    "candid",
    "careful",
    "celestial",
    "charming",
    "chatty",
    "circular",
    "clever",
    "coastal",
    "considerate",
    "cosmic",
    "cubic",
    "curious",
    "dapper",
    "delighted",
    "didactic",
    "diligent",
    "eager",
    "earnest",
    "effulgent",
    "erudite",
    "excellent",
    "exquisite",
    "fabulous",
    "fascinating",
    "fluent",
    "forgiving",
    "friendly",
    "gallant",
    "gentle",
    "golden",
    "glowing",
    "gracious",
    "gregarious",
    "harmonic",
    "hearty",
    "honest",
    "hopeful",
    "humble",
    "implacable",
    "inventive",
    "jovial",
    "joyous",
    "judicious",
    "jumping",
    "keen",
    "kind",
    "likable",
    "lively",
    "lucid",
    "loyal",
    "lucky",
    "marvellous",
    "mellifluous",
    "nimble",
    "nautical",
    "oblong",
    "outstanding",
    "patient",
    "playful",
    "polished",
    "polite",
    "profound",
    "quadratic",
    "quiet",
    "radiant",
    "rectangular",
    "remarkable",
    "resolute",
    "rusty",
    "sensible",
    "serene",
    "shining",
    "sincere",
    "sparkling",
    "splendid",
    "spry",
    "steady",
    "stellar",
    "sunny",
    "tenacious",
    "tidy",
    "tremendous",
    "triangular",
    "undulating",
    "unflappable",
    "upbeat",
    "unique",
    "verdant",
    "vivid",
    "vitreous",
    "whimsical",
    "witty",
    "wise",
    "zippy",
];

const NAMEGEN_NOUNS: &[&str] = &[
    "aardvark",
    "accordion",
    "albatross",
    "apple",
    "apricot",
    "anvil",
    "asteroid",
    "banjo",
    "beacon",
    "bee",
    "beetle",
    "bison",
    "bonsai",
    "brachiosaur",
    "breeze",
    "brook",
    "cactus",
    "canary",
    "capsicum",
    "cedar",
    "chisel",
    "clarinet",
    "comet",
    "coral",
    "cowbell",
    "crab",
    "cuckoo",
    "cymbal",
    "dahlia",
    "diplodocus",
    "dingo",
    "donkey",
    "drum",
    "duck",
    "echidna",
    "elephant",
    "falcon",
    "fern",
    "firefly",
    "fjord",
    "foxglove",
    "galaxy",
    "geyser",
    "glockenspiel",
    "goose",
    "hammer",
    "harbor",
    "hazelnut",
    "heron",
    "hill",
    "horizon",
    "horse",
    "hyacinth",
    "iguanadon",
    "jasmine",
    "jellyfish",
    "kangaroo",
    "kestrel",
    "lake",
    "lantern",
    "lark",
    "lemon",
    "lemur",
    "lotus",
    "lyrebird",
    "magpie",
    "megalodon",
    "meteor",
    "mongoose",
    "mountain",
    "mouse",
    "muskrat",
    "nebula",
    "newt",
    "oboe",
    "ocelot",
    "otter",
    "orange",
    "owl",
    "panda",
    "peach",
    "pebble",
    "pelican",
    "pepper",
    "pinecone",
    "plum",
    "poppy",
    "prairie",
    "petunia",
    "pheasant",
    "piano",
    "pigeon",
    "platypus",
    "quasar",
    "quokka",
    "raven",
    "reef",
    "rhinoceros",
    "river",
    "rustacean",
    "saffron",
    "salamander",
    "seahorse",
    "sitar",
    "sparrow",
    "spruce",
    "starling",
    "stegosaurus",
    "sunflower",
    "tambourine",
    "thistle",
    "tiger",
    "tomato",
    "toucan",
    "triceratops",
    "turnip",
    "ukulele",
    "viola",
    "violet",
    "walrus",
    "weasel",
    "willow",
    "wombat",
    "xylophone",
    "yak",
    "zebra",
];

pub(crate) fn resolve_cloud(explicit_cloud: Option<Cloud>, config: &IceConfig) -> Result<Cloud> {
    if let Some(cloud) = explicit_cloud {
        return Ok(cloud);
    }
    if let Some(cloud) = config.default.cloud {
        return Ok(cloud);
    }
    bail!(
        "Missing `--cloud CLOUD`, or set a default with e.g. `ice config set default.cloud=vast.ai` (or `gcp`, `aws`, `local`, etc.)."
    )
}

pub(crate) fn prompt_theme() -> &'static ColorfulTheme {
    static THEME: LazyLock<ColorfulTheme> = LazyLock::new(ColorfulTheme::default);
    &THEME
}

pub(crate) fn prompt_confirm(prompt: &str, default: bool) -> Result<bool> {
    require_interactive("Interactive confirmation required.")?;
    Confirm::with_theme(prompt_theme())
        .with_prompt(prompt)
        .default(default)
        .interact()
        .context("Failed to read confirmation")
}

pub(crate) fn prompt_u32(prompt: &str, default: Option<u32>, min_value: u32) -> Result<u32> {
    require_interactive("Interactive numeric input required.")?;
    let mut input = Input::<u32>::with_theme(prompt_theme());
    input = input.with_prompt(prompt);
    if let Some(value) = default {
        input = input.default(value);
    }
    let value = input
        .interact_text()
        .context("Failed to read integer input")?;
    if value < min_value {
        bail!("{prompt} must be >= {min_value}");
    }
    Ok(value)
}

pub(crate) fn prompt_f64(prompt: &str, default: Option<f64>, min_value: f64) -> Result<f64> {
    require_interactive("Interactive numeric input required.")?;
    let mut input = Input::<f64>::with_theme(prompt_theme());
    input = input.with_prompt(prompt);
    if let Some(value) = default {
        input = input.default(value);
    }
    let value = input
        .interact_text()
        .context("Failed to read numeric input")?;
    if !(value.is_finite() && value >= min_value) {
        bail!("{prompt} must be a finite value >= {min_value}");
    }
    Ok(value)
}

pub(crate) fn ensure_provider_cli_installed(cloud: Cloud) -> Result<()> {
    match cloud {
        Cloud::VastAi | Cloud::Local => Ok(()),
        Cloud::Gcp => ensure_command_available("gcloud"),
        Cloud::Aws => ensure_command_available("aws"),
    }
}

pub(crate) fn ensure_command_available(command: &str) -> Result<()> {
    if Command::new(command).arg("--version").output().is_ok() {
        return Ok(());
    }
    bail!("Missing required command `{command}` in `PATH`.");
}

pub(crate) fn run_command_output(
    command: &mut Command,
    context: &str,
) -> Result<std::process::Output> {
    let output = command
        .output()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let message = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit status {}", output.status)
        };
        bail!("Failed to {context}: {message}");
    }
    Ok(output)
}

pub(crate) fn run_command_json(command: &mut Command, context: &str) -> Result<Value> {
    let output = run_command_output(command, context)?;
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("Non-UTF8 command output while trying to {context}"))?;
    serde_json::from_str::<Value>(&stdout).with_context(|| {
        format!(
            "Failed to parse JSON output while trying to {context}: {}",
            truncate_ellipsis(&stdout, 280)
        )
    })
}

pub(crate) fn run_command_text(command: &mut Command, context: &str) -> Result<String> {
    let output = run_command_output(command, context)?;
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("Non-UTF8 command output while trying to {context}"))?;
    Ok(stdout.trim().to_owned())
}

pub(crate) fn run_command_status_with_stdin(
    command: &mut Command,
    context: &str,
    stdin_data: &str,
) -> Result<()> {
    command.stdin(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_data.as_bytes())
            .with_context(|| format!("Failed to write stdin while trying to {context}"))?;
    }
    let status = child
        .wait()
        .with_context(|| format!("Failed while waiting for command to {context}"))?;
    if !status.success() {
        bail!("Failed to {context}: command exited with status {status}");
    }
    Ok(())
}

pub(crate) fn run_command_status(command: &mut Command, context: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !status.success() {
        bail!("Failed to {context}: command exited with status {status}");
    }
    Ok(())
}

pub(crate) fn required_runtime_seconds(hours: f64) -> u64 {
    ((hours * 3600.0).ceil().max(1.0)) as u64
}

pub(crate) fn normalize_instance_identifier_for_name_match(identifier: &str) -> Result<String> {
    let needle = normalize_instance_name_for_match(identifier);
    if needle.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }
    Ok(needle)
}

pub(crate) fn normalize_instance_name_for_match(name: &str) -> String {
    let lowered = name.trim().to_ascii_lowercase();
    lowered
        .strip_prefix(ICE_LABEL_PREFIX)
        .unwrap_or(&lowered)
        .to_owned()
}

pub(crate) fn visible_instance_name(name: &str) -> &str {
    name.strip_prefix(ICE_LABEL_PREFIX).unwrap_or(name)
}

pub(crate) fn prefix_lookup_indices<T, F>(
    items: &[T],
    identifier: &str,
    name_of: F,
) -> Result<PrefixLookup>
where
    F: Fn(&T) -> &str,
{
    let needle = normalize_instance_identifier_for_name_match(identifier)?;
    let mut exact = Vec::new();
    let mut prefixed = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let candidate = normalize_instance_name_for_match(name_of(item));
        if candidate.is_empty() {
            continue;
        }
        if candidate == needle {
            exact.push(index);
        } else if candidate.starts_with(&needle) {
            prefixed.push(index);
        }
    }
    Ok(match exact.len() {
        1 => PrefixLookup::Unique(exact[0]),
        n if n > 1 => PrefixLookup::Ambiguous(exact),
        _ => match prefixed.len() {
            1 => PrefixLookup::Unique(prefixed[0]),
            n if n > 1 => PrefixLookup::Ambiguous(prefixed),
            _ => PrefixLookup::None,
        },
    })
}

pub(crate) fn build_cloud_instance_name(existing_names: &HashSet<String>) -> Result<String> {
    Ok(format!(
        "{ICE_LABEL_PREFIX}{}",
        generate_unique_verb_noun_name(existing_names)?
    ))
}

pub(crate) fn generate_unique_verb_noun_name(existing_names: &HashSet<String>) -> Result<String> {
    let adjectives = extended_namegen_adjectives();
    let nouns = extended_namegen_nouns();
    if adjectives.is_empty() || nouns.is_empty() {
        bail!("Name generator has no words configured.");
    }

    let taken = existing_names
        .iter()
        .map(|value| normalize_instance_name_for_match(value))
        .collect::<HashSet<_>>();

    let plain_total = adjectives.len().saturating_mul(nouns.len());
    let plain_retry_budget = plain_total.min(RANDOM_NAME_COLLISION_RETRIES).max(1);
    let mut plain_generator = Generator::new(adjectives, nouns, Name::Plain);
    let mut seen_plain = HashSet::with_capacity(plain_retry_budget);
    for _ in 0..plain_retry_budget {
        let candidate = plain_generator
            .next()
            .ok_or_else(|| anyhow!("Name generator exhausted while generating plain names."))?;
        let key = normalize_instance_name_for_match(&candidate);
        if !seen_plain.insert(key.clone()) {
            continue;
        }
        if !taken.contains(&key) {
            return Ok(candidate);
        }
    }

    for adjective in adjectives {
        for noun in nouns {
            let candidate = format!("{adjective}-{noun}");
            if !taken.contains(&normalize_instance_name_for_match(&candidate)) {
                return Ok(candidate);
            }
        }
    }

    let numbered_total = plain_total.saturating_mul(NUMBERED_NAME_SUFFIX_MAX as usize);
    let numbered_retry_budget = numbered_total.min(NUMBERED_NAME_COLLISION_RETRIES).max(1);
    let mut numbered_generator = Generator::new(adjectives, nouns, Name::Numbered);
    let mut seen_numbered = HashSet::with_capacity(numbered_retry_budget);
    for _ in 0..numbered_retry_budget {
        let candidate = numbered_generator
            .next()
            .ok_or_else(|| anyhow!("Name generator exhausted while generating numbered names."))?;
        let key = normalize_instance_name_for_match(&candidate);
        if !seen_numbered.insert(key.clone()) {
            continue;
        }
        if !taken.contains(&key) {
            return Ok(candidate);
        }
    }

    for adjective in adjectives {
        for noun in nouns {
            for suffix in 1..=NUMBERED_NAME_SUFFIX_MAX {
                let candidate = format!("{adjective}-{noun}-{suffix:04}");
                if !taken.contains(&normalize_instance_name_for_match(&candidate)) {
                    return Ok(candidate);
                }
            }
        }
    }

    bail!("Could not generate a unique instance name (all adjective-noun combinations are taken).")
}

fn extended_namegen_adjectives() -> &'static [&'static str] {
    static ADJECTIVES: LazyLock<Vec<&'static str>> =
        LazyLock::new(|| merge_unique_words(NAMEGEN_ADJECTIVES, NAMES_ADJECTIVES));
    ADJECTIVES.as_slice()
}

fn extended_namegen_nouns() -> &'static [&'static str] {
    static NOUNS: LazyLock<Vec<&'static str>> =
        LazyLock::new(|| merge_unique_words(NAMEGEN_NOUNS, NAMES_NOUNS));
    NOUNS.as_slice()
}

fn merge_unique_words(primary: &[&'static str], extra: &[&'static str]) -> Vec<&'static str> {
    let mut merged = Vec::with_capacity(primary.len().saturating_add(extra.len()));
    let mut seen = HashSet::with_capacity(primary.len().saturating_add(extra.len()));
    for word in primary.iter().chain(extra.iter()) {
        if seen.insert(*word) {
            merged.push(*word);
        }
    }
    merged
}

pub(crate) fn elapsed_hours_from_rfc3339(ts: &str) -> Option<f64> {
    let parsed = DateTime::parse_from_rfc3339(ts).ok()?;
    let parsed_utc = parsed.with_timezone(&Utc);
    let elapsed = Utc::now().signed_duration_since(parsed_utc);
    Some(elapsed.num_seconds().max(0) as f64 / 3600.0)
}

pub(crate) fn parse_json_response(response: Response, context: &str) -> Result<Value> {
    let status = response.status();
    let text = response
        .text()
        .with_context(|| format!("Failed to read response body while trying to {context}"))?;
    if !status.is_success() {
        bail!(
            "Failed to {context}: HTTP {} {}",
            status.as_u16(),
            extract_api_error_message(&text)
        );
    }
    serde_json::from_str::<Value>(&text).with_context(|| {
        format!(
            "Failed to parse JSON response while trying to {context}. Body: {}",
            truncate_ellipsis(&text, 300)
        )
    })
}

pub(crate) fn extract_api_error_message(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        if let Some(message) = value
            .get("msg")
            .and_then(Value::as_str)
            .or_else(|| value.get("detail").and_then(Value::as_str))
            .or_else(|| value.get("error").and_then(Value::as_str))
        {
            return message.to_owned();
        }
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        "empty response body".to_owned()
    } else {
        truncate_ellipsis(trimmed, 280)
    }
}

pub(crate) fn spinner(message: &str) -> ProgressBar {
    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    let style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    progress.set_style(style);
    progress.enable_steady_tick(Duration::from_millis(90));
    progress.set_message(message.to_owned());
    progress
}

pub(crate) fn require_interactive(message: &str) -> Result<()> {
    if io::stdin().is_terminal() {
        return Ok(());
    }
    bail!("{message}");
}

pub(crate) fn maybe_open_browser(url: &str) {
    if let Err(err) = webbrowser::open(url) {
        eprintln!("Could not open browser automatically: {err}");
        eprintln!("Open this URL manually: {url}");
    }
}

pub(crate) fn nonempty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

pub(crate) fn now_unix_secs() -> u64 {
    now_unix_secs_f64() as u64
}

pub(crate) fn now_unix_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs_f64()
}

pub(crate) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub(crate) fn format_unix_utc(unix_ts: u64) -> String {
    if let Some(dt) = DateTime::<Utc>::from_timestamp(unix_ts as i64, 0) {
        return dt.format("%Y-%m-%d %H:%M UTC").to_string();
    }
    format!("{unix_ts} (unix)")
}

pub(crate) fn elapsed_since(start: SystemTime) -> Result<Duration> {
    SystemTime::now()
        .duration_since(start)
        .context("System clock moved backwards")
}

pub(crate) fn truncate_ellipsis(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    if max_chars <= 1 {
        return "…".to_owned();
    }
    let mut output = value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    output.push('…');
    output
}

pub(crate) fn shell_quote_single(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(crate) fn write_temp_file(prefix: &str, suffix: &str, contents: &str) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "{prefix}-{}-{}{}",
        now_unix_secs(),
        std::process::id(),
        suffix
    ));
    std::fs::write(&path, contents)
        .with_context(|| format!("Failed to write temporary file: {}", path.display()))?;
    Ok(path)
}

pub(crate) fn tcp_port_open(host: &str, port: u16, timeout: Duration) -> Result<()> {
    let addrs = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("Could not resolve {host}:{port}"))?;
    let mut attempted = false;
    let mut last_err = None;
    for addr in addrs {
        attempted = true;
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(_stream) => return Ok(()),
            Err(err) => last_err = Some(format!("{addr}: {err}")),
        }
    }
    if !attempted {
        bail!("No network address resolved for {host}:{port}");
    }
    bail!(
        "Failed to connect to {host}:{port}: {}",
        last_err.unwrap_or_else(|| "unknown network error".to_owned())
    );
}
