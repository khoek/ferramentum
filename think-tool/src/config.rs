use std::fmt;
use std::str::FromStr;

use clap::ValueEnum;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::{ChannelSlug, RoleSlug, StepSlug};

pub const PROJECT_CONFIG_VERSION: u32 = 1;
pub const ROLE_CONFIG_VERSION: u32 = 1;
pub const ALERTS_CHANNEL: &str = "alerts";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<ProjectTemplate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_role: Option<RoleSlug>,
    pub default_backend: BackendName,
    #[serde(default)]
    pub providers: ProvidersConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<ChannelSlug>,
    pub transcripts: TranscriptConfig,
    pub ui: UiConfig,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            version: PROJECT_CONFIG_VERSION,
            template: None,
            default_role: None,
            default_backend: BackendName::Codex,
            providers: ProvidersConfig::default(),
            channels: builtin_channels(),
            transcripts: TranscriptConfig::default(),
            ui: UiConfig::default(),
        }
    }
}

impl ProjectConfig {
    pub fn with_template(template: Option<ProjectTemplate>) -> Self {
        Self {
            default_role: match template {
                Some(ProjectTemplate::MathEpisodes) => RoleSlug::parse("episode").ok(),
                None => None,
            },
            channels: match template {
                Some(ProjectTemplate::MathEpisodes) => [ALERTS_CHANNEL, "report", "report-single"]
                    .into_iter()
                    .map(ChannelSlug::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("built-in math-episodes channels must be valid slugs"),
                None => builtin_channels(),
            },
            template,
            ..Self::default()
        }
    }
}

fn builtin_channels() -> Vec<ChannelSlug> {
    vec![ChannelSlug::parse(ALERTS_CHANNEL).expect("built-in alerts channel must be a valid slug")]
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProvidersConfig {
    #[serde(default)]
    pub codex: CodexProviderConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodexProviderConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_level: Option<CodexThinkingLevel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum CodexThinkingLevel {
    Low,
    Medium,
    High,
    Xhigh,
}

impl fmt::Display for CodexThinkingLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => formatter.write_str("low"),
            Self::Medium => formatter.write_str("medium"),
            Self::High => formatter.write_str("high"),
            Self::Xhigh => formatter.write_str("xhigh"),
        }
    }
}

impl FromStr for CodexThinkingLevel {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" => Ok(Self::Xhigh),
            _ => anyhow::bail!("unknown Codex thinking level `{value}`"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptConfig {
    pub record_raw: bool,
    pub record_text: bool,
}

impl Default for TranscriptConfig {
    fn default() -> Self {
        Self {
            record_raw: true,
            record_text: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    pub theme: UiTheme,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            theme: UiTheme::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum UiTheme {
    Auto,
    Plain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum ProjectTemplate {
    MathEpisodes,
}

impl fmt::Display for ProjectTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MathEpisodes => formatter.write_str("math-episodes"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum BackendName {
    Codex,
}

impl fmt::Display for BackendName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codex => formatter.write_str("codex"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleConfig {
    pub version: u32,
    pub status: RoleStatus,
    pub backend: BackendName,
    pub mode: RoleMode,
    pub parallel: RoleParallelism,
    pub agent_names: AgentNameScheme,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_prefix: Option<String>,
    #[serde(default)]
    pub auto_archive: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expose: Vec<ExposedContext>,
    pub steps: Vec<StepSlug>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<TriggerConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleParallelism {
    Count(usize),
    Infinite,
}

impl RoleParallelism {
    pub fn target_count(self) -> Option<usize> {
        match self {
            Self::Count(value) => Some(value),
            Self::Infinite => None,
        }
    }
}

impl fmt::Display for RoleParallelism {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(value) => write!(formatter, "{value}"),
            Self::Infinite => formatter.write_str("∞"),
        }
    }
}

impl FromStr for RoleParallelism {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "infinite" | "inf" | "∞" => Ok(Self::Infinite),
            _ => {
                let count = value.parse::<usize>().map_err(|_| {
                    anyhow::anyhow!("parallel must be a positive integer or `infinite`")
                })?;
                if count == 0 {
                    anyhow::bail!("parallel count must be at least 1");
                }
                Ok(Self::Count(count))
            }
        }
    }
}

impl Serialize for RoleParallelism {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Count(value) => serializer.serialize_u64(*value as u64),
            Self::Infinite => serializer.serialize_str("infinite"),
        }
    }
}

impl<'de> Deserialize<'de> for RoleParallelism {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = RoleParallelism;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a positive integer or the string `infinite`")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let count = usize::try_from(value).map_err(E::custom)?;
                if count == 0 {
                    return Err(E::custom("parallel count must be at least 1"));
                }
                Ok(RoleParallelism::Count(count))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value <= 0 {
                    return Err(E::custom("parallel count must be at least 1"));
                }
                let count = usize::try_from(value).map_err(E::custom)?;
                Ok(RoleParallelism::Count(count))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                RoleParallelism::from_str(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TriggerConfig {
    RoleStepFinished {
        role: RoleSlug,
        step: StepSlug,
        #[serde(default, flatten)]
        launch: TriggerLaunch,
    },
    RoleAgentFinished {
        role: RoleSlug,
        #[serde(default, flatten)]
        launch: TriggerLaunch,
    },
    QueueIdle {
        idle_queue: String,
        idle_seconds: u64,
        #[serde(default, flatten)]
        launch: TriggerLaunch,
    },
    Elapsed {
        role: RoleSlug,
        interval_seconds: u64,
        #[serde(default, flatten)]
        launch: TriggerLaunch,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum ExposedContext {
    LastAgentFinished,
    LastAgentStarted,
}

impl fmt::Display for ExposedContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LastAgentFinished => formatter.write_str("last-agent-finished"),
            Self::LastAgentStarted => formatter.write_str("last-agent-started"),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "launch", rename_all = "kebab-case")]
pub enum TriggerLaunch {
    #[default]
    Async,
    Queued {
        queue: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum RoleStatus {
    Draft,
    Active,
    Paused,
}

impl fmt::Display for RoleStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draft => formatter.write_str("draft"),
            Self::Active => formatter.write_str("active"),
            Self::Paused => formatter.write_str("paused"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum RoleMode {
    Oneshot,
    Repeatable,
    Infinite,
}

impl fmt::Display for RoleMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Oneshot => formatter.write_str("oneshot"),
            Self::Repeatable => formatter.write_str("repeatable"),
            Self::Infinite => formatter.write_str("infinite"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum AgentNameScheme {
    Sequential,
    #[value(name = "random-8")]
    Random8,
    AdjectiveNoun,
}

impl fmt::Display for AgentNameScheme {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequential => formatter.write_str("sequential"),
            Self::Random8 => formatter.write_str("random-8"),
            Self::AdjectiveNoun => formatter.write_str("adjective-noun"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_trigger_launches() {
        let config: RoleConfig = toml::from_str(
            r#"
version = 1
status = "active"
backend = "codex"
mode = "repeatable"
parallel = 1
agent_names = "sequential"
steps = ["work"]

[[triggers]]
kind = "role-step-finished"
role = "source-role"
step = "review"
launch = "async"

[[triggers]]
kind = "elapsed"
role = "source-role"
interval_seconds = 60
launch = "queued"
queue = "checkers"

[[triggers]]
kind = "role-agent-finished"
role = "source-role"
launch = "async"

[[triggers]]
kind = "queue-idle"
idle_queue = "publisher"
idle_seconds = 600
launch = "queued"
queue = "supervisor"
"#,
        )
        .expect("role config should parse");

        assert_eq!(config.triggers.len(), 4);
        assert_eq!(config.parallel, RoleParallelism::Count(1));
        assert!(matches!(
            &config.triggers[0],
            TriggerConfig::RoleStepFinished {
                role,
                step,
                launch: TriggerLaunch::Async,
            } if role.to_string() == "source-role" && step.to_string() == "review"
        ));
        assert!(matches!(
            &config.triggers[1],
            TriggerConfig::Elapsed {
                role,
                interval_seconds: 60,
                launch: TriggerLaunch::Queued { queue },
            } if role.to_string() == "source-role" && queue == "checkers"
        ));
        assert!(matches!(
            &config.triggers[2],
            TriggerConfig::RoleAgentFinished {
                role,
                launch: TriggerLaunch::Async,
            } if role.to_string() == "source-role"
        ));
        assert!(matches!(
            &config.triggers[3],
            TriggerConfig::QueueIdle {
                idle_queue,
                idle_seconds: 600,
                launch: TriggerLaunch::Queued { queue: launch_queue },
            } if idle_queue == "publisher" && launch_queue == "supervisor"
        ));
    }

    #[test]
    fn parses_infinite_parallelism() {
        let config: RoleConfig = toml::from_str(
            r#"
version = 1
status = "active"
backend = "codex"
mode = "repeatable"
parallel = "infinite"
agent_names = "sequential"
steps = ["work"]
"#,
        )
        .expect("role config should parse");

        assert_eq!(config.parallel, RoleParallelism::Infinite);
        assert!(
            toml::to_string(&config)
                .expect("role config should serialize")
                .contains("parallel = \"infinite\"")
        );
    }

    #[test]
    fn parses_codex_provider_config() {
        let config: ProjectConfig = toml::from_str(
            r#"
version = 1
default_backend = "codex"
channels = ["report", "report-single"]

[providers.codex]
model = "gpt-5.5"
thinking_level = "xhigh"

[transcripts]
record_raw = true
record_text = true

[ui]
theme = "auto"
"#,
        )
        .expect("project config should parse");

        assert_eq!(config.providers.codex.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(config.channels.len(), 2);
        assert_eq!(
            config.providers.codex.thinking_level,
            Some(CodexThinkingLevel::Xhigh)
        );
    }
}
