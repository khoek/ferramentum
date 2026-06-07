use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{IsTerminal, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use clap::CommandFactory;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use rand::prelude::IndexedRandom;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Clear, Gauge, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Wrap,
};
use serde::{Deserialize, Serialize};

use crate::attach::{
    AttachDocument, AttachReplyMarker, attach_markdown_line, attach_marker_is_assistant,
    attach_match_position, attach_search_matches, current_attach_marker_index,
    push_attach_transcript_block,
};
use crate::backend::{AppServerPolicy, AppServerTurnExit, AppServerTurnRequest, SteerStatus};
use crate::cli::{
    AdvancedCommand, AgentCommand, AgentSelectorArgs, AssistArgs, AttachArgs, ChannelCommand, Cli,
    CodexConfigArgs, CodexLoginArgs, CodexProviderCommand, CodexUseArgs, Commands, HelpArgs,
    MoreArgs, NewAgentArgs, ProjectCommand, ProjectNewArgs, ProviderCommand, RoleCommand,
    RoleDraftArgs, RoleNewArgs, RunChildCommand, RunNoticesArgs, RunOrchestratorArgs, TriggerArgs,
};
use crate::config::{
    ALERTS_CHANNEL, AgentNameScheme, CodexThinkingLevel, ExposedContext, ProjectConfig,
    ProjectTemplate, ROLE_CONFIG_VERSION, RoleConfig, RoleMode, RoleStatus, TriggerConfig,
    TriggerLaunch,
};
use crate::dashboard::{
    FooterPair, PaletteGroup, draw_dashboard_footer, footer_key, footer_line_from_items,
    footer_line_from_pairs, palette_detail,
};
use crate::ids::{AgentId, ChannelSlug, RoleSlug, StepSlug};
use crate::input::editor::{
    ChoicePrompt, ConfirmPrompt, PromptEditor, TerminalSession, UserCancelled,
};
use crate::io;
use crate::prompt;
use crate::provider::codex::{
    Health as CodexHealth, RateLimit as CodexRateLimit, RateLimits as CodexRateLimits,
};
use crate::selection::{self, AgentSpec, AttachTarget, ResolvedAgent};
use crate::session::{NativeSessionHost, PaneSpawnRequest, SessionHost};
use crate::state::{
    AgentManifest, AgentState, AgentStatus, Disposition, ProjectPaths, RolePaths, RunExitState,
    list_agents, list_channels, list_roles, load_agent, save_agent, unix_timestamp,
};
use crate::time::{
    event_age, file_modified_unix, format_unix_time, format_unix_time_compact, human_duration,
    system_time_to_unix,
};
use crate::tui::{ellipsize_display, text_matches_query};
use crate::{git, lock, template, ui};

mod commands;
mod conversation;
mod dash;
mod project;
mod role;
mod runtime;
mod status;
mod terminal;

use self::commands::*;
use self::conversation::*;
use self::dash::*;
use self::project::*;
use self::role::*;
use self::runtime::*;
use self::status::*;
use self::terminal::*;

const RANDOM_NAME_ATTEMPTS: usize = 512;
const ADJECTIVES: &[&str] = &[
    "amber", "apt", "brisk", "calm", "clear", "cobalt", "daring", "direct", "exact", "fair",
    "fresh", "golden", "keen", "lucid", "nimble", "prime", "quiet", "rapid", "steady", "tidy",
    "vivid", "wise",
];

const NOUNS: &[&str] = &[
    "axiom", "branch", "cipher", "delta", "engine", "filter", "graph", "lemma", "matrix", "module",
    "orbit", "proof", "query", "tensor", "vector", "vertex",
];

const RANDOM_ALPHANUM: &[&str] = &[
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "m", "n", "p", "q", "r", "s", "t", "u",
    "v", "w", "x", "y", "z", "2", "3", "4", "5", "6", "7", "8", "9",
];

trait QueryMatch {
    fn matches_query(&self, query: &str) -> bool;
}

fn matching_indices<T: QueryMatch>(items: &[T], query: &str) -> Vec<usize> {
    let query = query.trim();
    items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| item.matches_query(query).then_some(index))
        .collect()
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TriggerQueueState {
    #[serde(default)]
    items: Vec<TriggerQueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TriggerQueueItem {
    role: RoleSlug,
    enqueued_at: u64,
    cause: TriggerCause,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoleRuntimeState {
    started_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueRuntimeState {
    empty_since: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum TriggerCause {
    Manual {
        reason: Option<String>,
    },
    RoleStepFinished {
        source_role: RoleSlug,
        source_step: StepSlug,
    },
    RoleAgentFinished {
        source_role: RoleSlug,
        source_agent: AgentId,
        run_id: u64,
        step: StepSlug,
    },
    QueueIdle {
        queue: String,
        idle_seconds: u64,
        empty_since: u64,
    },
    Elapsed {
        source_role: RoleSlug,
        source_started_at: u64,
        interval_seconds: u64,
        event_index: u64,
    },
}

impl TriggerCause {
    fn render(&self, target_role: &RoleSlug) -> String {
        let mut text = String::new();
        let _ = writeln!(
            text,
            "This agent was started by a think trigger targeting role `{target_role}`."
        );
        match self {
            Self::Manual { reason } => {
                let _ = writeln!(text, "- trigger kind: manual");
                if let Some(reason) = reason {
                    let _ = writeln!(text, "- reason: {reason}");
                }
            }
            Self::RoleStepFinished {
                source_role,
                source_step,
            } => {
                let _ = writeln!(text, "- trigger kind: role-step-finished");
                let _ = writeln!(text, "- source role: `{source_role}`");
                let _ = writeln!(text, "- source step: `{source_step}`");
            }
            Self::RoleAgentFinished {
                source_role,
                source_agent,
                run_id,
                step,
            } => {
                let _ = writeln!(text, "- trigger kind: role-agent-finished");
                let _ = writeln!(text, "- source role: `{source_role}`");
                let _ = writeln!(text, "- source agent: `{source_agent}`");
                let _ = writeln!(text, "- source run: `{run_id}`");
                let _ = writeln!(text, "- source step: `{step}`");
            }
            Self::QueueIdle {
                queue,
                idle_seconds,
                empty_since,
            } => {
                let _ = writeln!(text, "- trigger kind: queue-idle");
                let _ = writeln!(text, "- queue: `{queue}`");
                let _ = writeln!(text, "- idle seconds: `{idle_seconds}`");
                let _ = writeln!(text, "- empty since: `{}`", format_unix_time(*empty_since));
            }
            Self::Elapsed {
                source_role,
                source_started_at,
                interval_seconds,
                event_index,
            } => {
                let _ = writeln!(text, "- trigger kind: elapsed");
                let _ = writeln!(text, "- source role: `{source_role}`");
                let _ = writeln!(text, "- source started at: `{source_started_at}`");
                let _ = writeln!(text, "- interval seconds: `{interval_seconds}`");
                let _ = writeln!(text, "- event index: `{event_index}`");
            }
        }
        text
    }
}

const SUPERVISOR_STATE_VERSION: u32 = 1;
const RETRY_REQUEST_VERSION: u32 = 1;
const PROJECT_REGISTRY_VERSION: u32 = 1;
const MAX_OOM_RESTARTS: u32 = 3;
const MAX_AGENT_REPAIR_RETRIES: u32 = 3;
const DEFAULT_OOM_RESTART_DELAY_SECONDS: u64 = 5;
const DEFAULT_AGENT_REPAIR_RETRY_SECONDS: u64 = 1;
const MIN_RETRY_DELAY_SECONDS: u64 = 1;
const RETRY_POLL_MIN_SECONDS: u64 = 1;
const RETRY_POLL_MAX_SECONDS: u64 = 5;
const NOTICE_REFRESH_INTERVAL: Duration = Duration::from_secs(120);
const NOTICE_LINE_LIMIT: usize = 4;
const ORCHESTRATOR_WAKE_GRACE_SECONDS: u64 = 15;
const ORCHESTRATOR_WAKE_EVENT: &str = "spawned by project wake";
const MIN_ROLE_STEP_COUNT: usize = 1;
const AGENT_HISTORY_TRANSCRIPT_TAIL_LINES: usize = 120;
const MIN_AGENT_HISTORY_TRANSCRIPT_TAIL_LINES: usize = 1;
const DASHBOARD_TAB_BAR_HEIGHT: u16 = 1;
const DASHBOARD_HEALTH_STRIP_HEIGHT: u16 = 1;
const DASHBOARD_STATE_BAND_HEIGHT: u16 = 7;
const DASHBOARD_AGENTS_MIN_HEIGHT: u16 = 12;
const DASHBOARD_RUNTIME_BAND_HEIGHT: u16 = 5;
const DASHBOARD_FOOTER_HEIGHT: u16 = 1;
const DASHBOARD_PANEL_BORDER_ROWS: u16 = 2;
const DASHBOARD_PAGE_OVERLAP_ROWS: usize = 2;
const DASHBOARD_MODAL_HORIZONTAL_MARGIN: u16 = 6;
const DASHBOARD_MODAL_VERTICAL_MARGIN: u16 = 2;
const DASHBOARD_PALETTE_MAX_WIDTH: u16 = 112;
const DASHBOARD_PALETTE_MAX_HEIGHT: u16 = 18;
const DASHBOARD_NARROW_WIDTH: u16 = 104;
const DASHBOARD_NARROW_NOTICE_HEIGHT: u16 = 3;
const DASHBOARD_NARROW_STATE_HEIGHT: u16 = 4;
const DASHBOARD_NARROW_AGENTS_MIN_HEIGHT: u16 = 8;
const DASHBOARD_NARROW_RUNTIME_HEIGHT: u16 = 7;
const DASHBOARD_ADVANCED_MAX_WIDTH: u16 = 58;
const DASHBOARD_ADVANCED_MAX_HEIGHT: u16 = 9;
const DASHBOARD_SEARCH_MAX_WIDTH: u16 = 86;
const DASHBOARD_SEARCH_HEIGHT: u16 = 3;
const DASHBOARD_HELP_MAX_WIDTH: u16 = 88;
const DASHBOARD_HELP_MAX_HEIGHT: u16 = 24;
const DASHBOARD_EVENT_LOG_MAX_EVENTS: usize = 80;
const DASHBOARD_EVENT_TIMELINE_WIDTH: usize = 28;
const DASHBOARD_EVENT_SCROLL_STEP: u16 = 1;
const DASHBOARD_QUOTA_GAUGE_ROWS: u16 = 1;
const DASHBOARD_MIN_VISIBLE_ROWS: usize = 1;
const DASHBOARD_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DASHBOARD_FRAME_INTERVAL: Duration = Duration::from_millis(200);
const DASHBOARD_TOAST_TTL: Duration = Duration::from_secs(3);
const DASHBOARD_CENTERING_DIVISOR: u16 = 2;
const DASHBOARD_DETAIL_SCROLL_STEP: u16 = 1;
const PERCENT_FULL: f64 = 100.0;
const RATIO_EMPTY: f64 = 0.0;
const RATIO_FULL: f64 = 1.0;
const QUEUE_WARNING_MAX_COUNT: usize = 2;
const CHANNEL_DETAIL_ARTIFACT_LIMIT: usize = 80;
const QUOTA_PROBE_SPINNER_INTERVAL: Duration = Duration::from_millis(120);
const QUOTA_STATUS_CLEAR_PADDING_MAX: usize = 12;
const ATTACH_SCROLL_STEP: usize = 1;
const ATTACH_PAGE_STEP: usize = 12;
const ATTACH_REPLY_RAIL_WIDTH: u16 = 3;
const ATTACH_COMPOSER_HEIGHT: u16 = 6;
const ATTACH_FOOTER_COMPOSER: &[(&str, &str)] = &[
    ("Ctrl-D", "send"),
    ("Esc", "cancel"),
    ("Enter", "newline"),
    ("↑↓", "cursor"),
    ("Ctrl-↑↓", "history"),
];
const ATTACH_FOOTER_SEARCH: &[(&str, &str)] = &[
    ("type", "search"),
    ("Enter", "jump"),
    ("Esc", "close"),
    ("Ctrl-u", "clear"),
];
const ATTACH_FOOTER_FOLLOW_ON: &[(&str, &str)] = &[
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:on"),
    ("t", "thinking"),
    ("m", "more"),
    ("s", "switch"),
    ("o", "open"),
    ("y/Y", "copy"),
    ("q", "detach"),
];
const ATTACH_FOOTER_FOLLOW_OFF: &[(&str, &str)] = &[
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:off"),
    ("t", "thinking"),
    ("m", "more"),
    ("s", "switch"),
    ("o", "open"),
    ("y/Y", "copy"),
    ("q", "detach"),
];
const COMMAND_CONVERSATION_FOOTER_RUNNING: &[(&str, &str)] = &[
    ("m", "steer"),
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:on"),
    ("t", "thinking"),
    ("o", "open"),
    ("y/Y", "copy"),
    ("q", "detach"),
];
const COMMAND_CONVERSATION_FOOTER_STEER_COMPOSER: &[(&str, &str)] = &[
    ("Ctrl-D", "steer"),
    ("Esc", "cancel"),
    ("Enter", "newline"),
    ("↑↓", "cursor"),
    ("Ctrl-↑↓", "history"),
];
const COMMAND_CONVERSATION_FOOTER_DONE_FOLLOW_ON: &[(&str, &str)] = &[
    ("r", "follow-up"),
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "turn"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:on"),
    ("t", "thinking"),
    ("o", "open"),
    ("y/Y", "copy"),
    ("q", "finish"),
];
const COMMAND_CONVERSATION_FOOTER_DONE_FOLLOW_OFF: &[(&str, &str)] = &[
    ("r", "follow-up"),
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "turn"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:off"),
    ("t", "thinking"),
    ("o", "open"),
    ("y/Y", "copy"),
    ("q", "finish"),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupervisorState {
    version: u32,
    #[serde(default)]
    status: SupervisorStatus,
    #[serde(default)]
    oom_restarts: u32,
    #[serde(default)]
    quota_retries: u32,
    #[serde(default)]
    provider_retries: u32,
    #[serde(default)]
    repair_retries: u32,
    #[serde(default)]
    last_run_id: Option<u64>,
    #[serde(default)]
    child_pid: Option<u32>,
    #[serde(default)]
    next_retry_at: Option<u64>,
    #[serde(default)]
    last_event: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryRequestState {
    version: u32,
    requested_at: u64,
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            version: SUPERVISOR_STATE_VERSION,
            status: SupervisorStatus::Idle,
            oom_restarts: 0,
            quota_retries: 0,
            provider_retries: 0,
            repair_retries: 0,
            last_run_id: None,
            child_pid: None,
            next_retry_at: None,
            last_event: None,
            updated_at: unix_timestamp(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum SupervisorStatus {
    #[default]
    Idle,
    Running,
    Restarting,
    WaitingForQuota,
    WaitingForProvider,
    NeedsAttention,
}

impl std::fmt::Display for SupervisorStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => formatter.write_str("idle"),
            Self::Running => formatter.write_str("running"),
            Self::Restarting => formatter.write_str("restarting"),
            Self::WaitingForQuota => formatter.write_str("waiting-quota"),
            Self::WaitingForProvider => formatter.write_str("waiting-provider"),
            Self::NeedsAttention => formatter.write_str("needs-attention"),
        }
    }
}

struct SupervisorListPresentation {
    label: &'static str,
    include_retry: bool,
    clean_event_timestamps: bool,
}

impl SupervisorStatus {
    fn list_presentation(self) -> Option<SupervisorListPresentation> {
        match self {
            Self::Idle | Self::Running => None,
            Self::Restarting => Some(SupervisorListPresentation {
                label: "runtime orchestrator restarting",
                include_retry: true,
                clean_event_timestamps: true,
            }),
            Self::WaitingForQuota => Some(SupervisorListPresentation {
                label: "quota",
                include_retry: true,
                clean_event_timestamps: true,
            }),
            Self::WaitingForProvider => Some(SupervisorListPresentation {
                label: "provider",
                include_retry: true,
                clean_event_timestamps: true,
            }),
            Self::NeedsAttention => Some(SupervisorListPresentation {
                label: "runtime orchestrator needs attention",
                include_retry: false,
                clean_event_timestamps: false,
            }),
        }
    }
}

pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        None => status_project(&current_project_awake()?, None, false, true),
        Some(Commands::More(args)) => more_agent(args),
        Some(Commands::Status(args)) => status_project(
            &current_project_awake()?,
            args.role.as_ref(),
            args.all,
            !args.plain,
        ),
        Some(Commands::Open) => open_project_directory(),
        Some(Commands::Assist(args)) => assist_project(args),
        Some(Commands::Advanced(command)) => run_advanced_command(command),
        Some(Commands::Project(command)) => run_project_command(command),
        Some(Commands::Role(command)) => run_role_command(command),
        Some(Commands::Agent(command)) => run_agent_command(command),
        Some(Commands::Channel(command)) => run_channel_command(command),
        Some(Commands::Help(args)) => print_help(args),
        Some(Commands::RunChild(command)) => run_child_command(command),
    }
}

fn active_agent_count(role_paths: &RolePaths) -> Result<usize> {
    let mut count = 0;
    for agent in list_agents(role_paths)? {
        let state = load_agent(&role_paths.agent(agent))?;
        if !state.archived && matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
            count += 1;
        }
    }
    Ok(count)
}

fn allocate_agent_id(
    role_paths: &RolePaths,
    scheme: AgentNameScheme,
    prefix: Option<&str>,
) -> Result<AgentId> {
    match scheme {
        AgentNameScheme::Sequential => {
            let prefix = prefix.unwrap_or_default();
            for index in 1..=999_999 {
                let candidate = AgentId::parse(format!("{prefix}{index}"))?;
                if !role_paths.agent(candidate.clone()).root().exists() {
                    return Ok(candidate);
                }
            }
            bail!("Failed to allocate a sequential agent id.");
        }
        AgentNameScheme::Random8 => allocate_random(role_paths, prefix, random_8),
        AgentNameScheme::AdjectiveNoun => allocate_random(role_paths, prefix, adjective_noun),
    }
}

fn prepare_agent_data(agent_paths: &crate::state::AgentPaths) -> Result<()> {
    let own_target = agent_paths
        .role
        .project
        .agent_data_root(&agent_paths.role.role, &agent_paths.agent);
    io::ensure_dir(&own_target)?;
    io::ensure_dir(&agent_paths.data_dir())?;
    replace_symlink(&agent_paths.data_own(), &own_target)?;

    let all_root = agent_paths.data_all();
    if all_root.exists() {
        fs::remove_dir_all(&all_root)
            .with_context(|| format!("Failed to refresh `{}`", all_root.display()))?;
    }
    io::ensure_dir(&all_root)?;
    let roles_data_dir = agent_paths.role.project.data_dir().join("roles");
    if !roles_data_dir.exists() {
        return Ok(());
    }
    for role_entry in fs::read_dir(&roles_data_dir)
        .with_context(|| format!("Failed to read `{}`", roles_data_dir.display()))?
    {
        let role_entry = role_entry.context("Failed to read project data role entry")?;
        if !role_entry.file_type()?.is_dir() {
            continue;
        }
        let Some(role_name) = role_entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let agents_dir = role_entry.path().join("agents");
        if !agents_dir.exists() {
            continue;
        }
        for data_entry in fs::read_dir(&agents_dir)
            .with_context(|| format!("Failed to read `{}`", agents_dir.display()))?
        {
            let data_entry = data_entry.context("Failed to read agent data entry")?;
            if !data_entry.file_type()?.is_dir() {
                continue;
            }
            let Some(agent_name) = data_entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if role_name == agent_paths.role.role.as_str()
                && agent_name == agent_paths.agent.as_str()
            {
                continue;
            }
            let link_parent = all_root.join(&role_name).join("agents");
            io::ensure_dir(&link_parent)?;
            replace_symlink(&link_parent.join(agent_name), &data_entry.path())?;
        }
    }
    Ok(())
}

fn prepare_agent_work(agent_paths: &crate::state::AgentPaths) -> Result<()> {
    io::ensure_dir(&agent_paths.work_own())?;
    let all_root = agent_paths.work_all();
    if all_root.exists() {
        fs::remove_dir_all(&all_root)
            .with_context(|| format!("Failed to refresh `{}`", all_root.display()))?;
    }
    io::ensure_dir(&all_root)?;
    for role in list_roles(&agent_paths.role.project)? {
        let role_paths = RolePaths::new(agent_paths.role.project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            if role == agent_paths.role.role && agent == agent_paths.agent {
                continue;
            }
            let target = role_paths.agent(agent.clone()).work_own();
            if !target.exists() {
                continue;
            }
            let link_parent = all_root.join(role.as_str()).join("agents");
            io::ensure_dir(&link_parent)?;
            replace_symlink(&link_parent.join(agent.as_str()), &target)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn replace_symlink(link: &Path, target: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(link) {
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(link)
                .with_context(|| format!("Failed to remove `{}`", link.display()))?;
        } else {
            fs::remove_file(link)
                .with_context(|| format!("Failed to remove `{}`", link.display()))?;
        }
    }
    if let Some(parent) = link.parent() {
        io::ensure_dir(parent)?;
    }
    std::os::unix::fs::symlink(target, link).with_context(|| {
        format!(
            "Failed to create symlink `{}` -> `{}`",
            link.display(),
            target.display()
        )
    })
}

#[cfg(not(unix))]
fn replace_symlink(_link: &Path, _target: &Path) -> Result<()> {
    bail!("think agent data symlinks currently require a Unix-like platform.")
}

fn allocate_random(
    role_paths: &RolePaths,
    prefix: Option<&str>,
    mut generator: impl FnMut() -> Result<AgentId>,
) -> Result<AgentId> {
    for _ in 0..RANDOM_NAME_ATTEMPTS {
        let candidate = generator()?;
        let candidate = if let Some(prefix) = prefix {
            AgentId::parse(format!("{prefix}{}", candidate.as_str()))?
        } else {
            candidate
        };
        if !role_paths.agent(candidate.clone()).root().exists() {
            return Ok(candidate);
        }
    }
    bail!("Failed to allocate an unused agent id after {RANDOM_NAME_ATTEMPTS} attempts.")
}

fn random_8() -> Result<AgentId> {
    let mut rng = rand::rng();
    let mut value = String::with_capacity(8);
    for _ in 0..8 {
        value.push_str(
            RANDOM_ALPHANUM
                .choose(&mut rng)
                .ok_or_else(|| anyhow!("random alphabet is empty"))?,
        );
    }
    AgentId::parse(value)
}

fn adjective_noun() -> Result<AgentId> {
    let mut rng = rand::rng();
    let adjective = ADJECTIVES
        .choose(&mut rng)
        .ok_or_else(|| anyhow!("adjective list is empty"))?;
    let noun = NOUNS
        .choose(&mut rng)
        .ok_or_else(|| anyhow!("noun list is empty"))?;
    AgentId::parse(format!("{adjective}-{noun}"))
}

fn project_config(project: &ProjectPaths) -> Result<ProjectConfig> {
    io::read_toml(&project.config())
}

fn load_role_config(role_paths: &RolePaths) -> Result<RoleConfig> {
    let config: RoleConfig = io::read_toml(&role_paths.config())?;
    validate_role_config(&config)?;
    Ok(config)
}

fn save_role_config(role_paths: &RolePaths, config: &RoleConfig) -> Result<()> {
    validate_role_config(config)?;
    io::write_toml(&role_paths.config(), config)
}

fn validate_role_config(config: &RoleConfig) -> Result<()> {
    if config.version != ROLE_CONFIG_VERSION {
        bail!(
            "Unsupported role config version {}; expected {ROLE_CONFIG_VERSION}.",
            config.version
        );
    }
    if config.steps.is_empty() {
        bail!("Role must define at least one step.");
    }
    if let Some(prefix) = &config.agent_prefix {
        if prefix.is_empty() {
            bail!("Agent prefix must not be empty.");
        }
        if !prefix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            bail!("Agent prefix must contain only lowercase ASCII letters and digits.");
        }
    }
    if config.mode == RoleMode::Oneshot && config.steps.len() != 1 {
        bail!("Oneshot roles must define exactly one step.");
    }
    for trigger in &config.triggers {
        match trigger {
            TriggerConfig::RoleStepFinished { launch, .. } => validate_trigger_launch(launch)?,
            TriggerConfig::RoleAgentFinished { launch, .. } => validate_trigger_launch(launch)?,
            TriggerConfig::QueueIdle {
                idle_queue,
                idle_seconds,
                launch,
            } => {
                validate_trigger_queue(idle_queue)?;
                if *idle_seconds == 0 {
                    bail!("Queue idle triggers must have idle_seconds greater than 0.");
                }
                validate_trigger_launch(launch)?;
            }
            TriggerConfig::Elapsed {
                interval_seconds,
                launch,
                ..
            } => {
                if *interval_seconds == 0 {
                    bail!("Elapsed triggers must have interval_seconds greater than 0.");
                }
                validate_trigger_launch(launch)?;
            }
        }
    }
    Ok(())
}

fn validate_trigger_launch(launch: &TriggerLaunch) -> Result<()> {
    if let TriggerLaunch::Queued { queue } = launch {
        validate_trigger_queue(queue)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcript::TranscriptKind;

    #[test]
    fn notice_reader_rejects_placeholder_status_lines() {
        assert!(!notice_line_is_actionable("notice scan active"));
        assert!(!notice_line_is_actionable("scanning channel state"));
        assert!(!notice_line_is_actionable("queues empty"));
        assert!(!notice_line_is_actionable("info: queues empty"));
        assert!(!notice_line_is_actionable("idle"));
        assert!(notice_line_is_actionable(
            "episode/3 is paused and needs operator review"
        ));
        assert!(notice_line_is_actionable(
            "ep4 is waiting for quota until 17:40"
        ));
        let notice = parse_notice_line("warn: ep4 is paused").unwrap();
        assert_eq!(notice.text, "ep4 is paused");
        assert!(matches!(notice.severity, NoticeSeverity::Warn));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wake_cleanup_removes_inert_stale_orchestrator_locks() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path().to_owned());
        let lock_path = project
            .runtime_dir()
            .join("locks")
            .join("orchestrators")
            .join("episode")
            .join("ep11.lock");
        std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        std::fs::write(&lock_path, "pid = 4294967295\n").unwrap();

        clean_stale_orchestrator_locks(&project).unwrap();

        assert!(!lock_path.exists());
    }

    #[test]
    fn attach_transcript_parser_finds_reply_and_thinking_blocks() {
        let blocks = crate::transcript::parse(
            "OpenAI Codex\nuser\nRead PROMPT.md\ncodex\nI will inspect it.\nthinking\nprivate notes\nexec\nls\n",
        );

        assert!(
            blocks
                .iter()
                .any(|block| block.kind == TranscriptKind::Assistant)
        );
        assert!(
            blocks
                .iter()
                .any(|block| block.kind == TranscriptKind::Thinking)
        );
    }

    #[test]
    fn transcript_scroll_clamps_to_last_full_viewport() {
        assert_eq!(max_scroll_offset(100, 20), 80);
        assert_eq!(max_scroll_offset(20, 20), 0);
        assert_eq!(max_scroll_offset(4, 20), 0);
        assert_eq!(clamped_scroll_offset(99, 100, 20), 80);
        assert_eq!(clamped_scroll_offset(7, 100, 20), 7);
    }

    #[test]
    fn agent_counter_ignores_roles_and_spacers() {
        let rows = vec![
            DashboardSelection::Role(0),
            DashboardSelection::Agent(0, 0),
            DashboardSelection::Agent(0, 1),
            DashboardSelection::Spacer,
            DashboardSelection::Role(1),
            DashboardSelection::Agent(1, 0),
        ];

        assert_eq!(agent_count_in_rows(&rows), 3);
        assert_eq!(selected_agent_position_in_rows(&rows, 0), None);
        assert_eq!(selected_agent_position_in_rows(&rows, 1), Some((1, 3)));
        assert_eq!(selected_agent_position_in_rows(&rows, 2), Some((2, 3)));
        assert_eq!(selected_agent_position_in_rows(&rows, 5), Some((3, 3)));
    }

    #[test]
    fn raw_pty_exit_files_are_recovered_for_existing_projects() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path().to_owned());
        let role_paths = RolePaths::new(project, RoleSlug::parse("episode").unwrap());
        let agent_paths = role_paths.agent(AgentId::parse("ep1").unwrap());
        let run_paths = agent_paths.run(1);
        std::fs::create_dir_all(run_paths.root()).unwrap();
        std::fs::write(run_paths.prompt(), "Read PROMPT.md").unwrap();
        std::fs::write(run_paths.exit(), "success = true\ncode = 0\npid = 170283\n").unwrap();
        let mut state = AgentState::new(
            RoleSlug::parse("episode").unwrap(),
            AgentId::parse("ep1").unwrap(),
            crate::config::BackendName::Codex,
            RoleMode::Repeatable,
            Vec::new(),
        );
        state.run_count = 1;

        let exit = read_run_exit(&run_paths, &state).unwrap().unwrap();

        assert_eq!(exit.run_id, 1);
        assert_eq!(exit.step, StepSlug::parse("unknown").unwrap());
        assert!(exit.success);
        assert_eq!(exit.code, 0);
        assert_eq!(
            exit.message.as_deref(),
            Some("recovered from a raw PTY exit file")
        );
    }

    #[test]
    fn user_paused_agent_is_not_woken_by_active_role() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path().to_owned());
        let role = RoleSlug::parse("episode").unwrap();
        let agent = AgentId::parse("ep2").unwrap();
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let agent_paths = role_paths.agent(agent.clone());
        std::fs::create_dir_all(agent_paths.root()).unwrap();
        let config = RoleConfig {
            version: ROLE_CONFIG_VERSION,
            status: RoleStatus::Active,
            backend: crate::config::BackendName::Codex,
            mode: RoleMode::Repeatable,
            parallel: crate::config::RoleParallelism::Infinite,
            agent_names: AgentNameScheme::Sequential,
            agent_prefix: None,
            auto_archive: false,
            expose: Vec::new(),
            steps: vec![StepSlug::parse("work").unwrap()],
            triggers: Vec::new(),
        };
        save_role_config(&role_paths, &config).unwrap();
        let mut state = AgentState::new(
            role.clone(),
            agent.clone(),
            crate::config::BackendName::Codex,
            RoleMode::Repeatable,
            Vec::new(),
        );
        state.status = AgentStatus::Running;
        save_agent(&agent_paths, &mut state).unwrap();

        pause_agent_inner(
            &project,
            &ResolvedAgent {
                role: role.clone(),
                agent: agent.clone(),
            },
            false,
        )
        .unwrap();

        let state = load_agent(&agent_paths).unwrap();
        assert_eq!(state.status, AgentStatus::Paused);
        assert!(state.paused_by_user);
        assert!(!orchestrator_should_be_running(&config, &state));
    }

    #[test]
    fn agent_history_context_includes_prior_prompts_and_transcripts() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path().to_owned());
        let role_paths = RolePaths::new(project, RoleSlug::parse("worker").unwrap());
        let agent_paths = role_paths.agent(AgentId::parse("1").unwrap());
        std::fs::create_dir_all(agent_paths.run(1).root()).unwrap();
        std::fs::write(
            agent_paths.root().join("AGENT_PROMPT.md"),
            "check the base case",
        )
        .unwrap();
        std::fs::write(
            agent_paths.run(1).prompt(),
            "# think more follow-up\n\n# User follow-up\n\nextend to n=5",
        )
        .unwrap();
        std::fs::write(
            agent_paths.run(1).transcript_text(),
            "final answer: the construction works for n=5",
        )
        .unwrap();
        std::fs::write(
            agent_paths.run(1).reply(),
            "I checked n=5 and found the construction works.",
        )
        .unwrap();

        let context = agent_history_context(&agent_paths, 1).unwrap();
        assert!(context.contains("check the base case"));
        assert!(context.contains("extend to n=5"));
        assert!(context.contains("I checked n=5 and found the construction works."));
        assert!(!context.contains("final answer: the construction works for n=5"));

        assert_eq!(
            agent_prompt_history(&agent_paths, 1).unwrap(),
            vec!["check the base case".to_owned(), "extend to n=5".to_owned()]
        );
    }

    #[test]
    fn agent_history_context_falls_back_to_transcript_tail() {
        let temp = tempfile::tempdir().unwrap();
        let project = ProjectPaths::new(temp.path().to_owned());
        let role_paths = RolePaths::new(project, RoleSlug::parse("worker").unwrap());
        let agent_paths = role_paths.agent(AgentId::parse("1").unwrap());
        std::fs::create_dir_all(agent_paths.run(1).root()).unwrap();
        std::fs::write(agent_paths.run(1).prompt(), "# User follow-up\n\ncontinue").unwrap();
        std::fs::write(
            agent_paths.run(1).transcript_text(),
            "line one\n\x1b[31mfinal answer from transcript\x1b[0m\n",
        )
        .unwrap();

        let context = agent_history_context(&agent_paths, 1).unwrap();
        assert!(context.contains("transcript tail"));
        assert!(context.contains("final answer from transcript"));
        assert!(!context.contains("\x1b[31m"));
    }
}
