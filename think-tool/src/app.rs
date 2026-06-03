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
use jiff::{Timestamp, tz::TimeZone};
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

use crate::agent::{AgentBackend, AgentCommandRequest, CodexAgent, CommandSpec};
use crate::cli::{
    AdvancedCommand, AgentCommand, AgentSelectorArgs, AssistArgs, AttachArgs, ChannelCommand, Cli,
    CodexConfigArgs, CodexLoginArgs, CodexProviderCommand, CodexUseArgs, Commands, FixArgs,
    HelpArgs, MoreArgs, NewAgentArgs, ProjectCommand, ProjectNewArgs, ProviderCommand, RoleCommand,
    RoleDraftArgs, RoleNewArgs, RunChildCommand, RunNoticesArgs, RunOrchestratorArgs, TriggerArgs,
};
use crate::config::{
    ALERTS_CHANNEL, AgentNameScheme, CodexThinkingLevel, ExposedContext, ProjectConfig,
    ProjectTemplate, ROLE_CONFIG_VERSION, RoleConfig, RoleMode, RoleStatus, TriggerConfig,
    TriggerLaunch,
};
use crate::ids::{AgentId, ChannelSlug, RoleSlug, StepSlug};
use crate::io;
use crate::prompt;
use crate::provider::QuotaDecision;
use crate::provider::codex::{
    ConversationPolicy as CodexConversationPolicy, Health as CodexHealth,
    RateLimit as CodexRateLimit, RateLimits as CodexRateLimits,
};
use crate::runner;
use crate::selection::{self, AgentSpec, AttachTarget, ResolvedAgent};
use crate::session::{NativeSessionHost, PaneSpawnRequest, SessionHost};
use crate::state::{
    AgentManifest, AgentState, AgentStatus, Disposition, ProjectPaths, RolePaths, RunExitState,
    list_agents, list_channels, list_roles, load_agent, save_agent, unix_timestamp,
};
use crate::terminal_editor::{
    ChoicePrompt, ConfirmPrompt, PromptEditor, TerminalSession, UserCancelled,
};
use crate::transcript::{TranscriptBlock, TranscriptKind, TranscriptLineKind};
use crate::tui_text::{ellipsize_display, text_matches_query};
use crate::{git, lock, template, ui};

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
const UPDATE_INDEX_STATE_VERSION: u32 = 1;
const PROJECT_REGISTRY_VERSION: u32 = 1;
const MAX_OOM_RESTARTS: u32 = 3;
const MAX_AGENT_REPAIR_RETRIES: u32 = 3;
const DEFAULT_OOM_RESTART_DELAY_SECONDS: u64 = 5;
const DEFAULT_PROVIDER_PREP_RETRY_SECONDS: u64 = 5;
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
const UPDATE_CONTEXT_LIMIT: usize = 12;
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
    ("q", "quit"),
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
    ("q", "quit"),
];
const CODEX_CONVERSATION_FOOTER_RUNNING: &[(&str, &str)] = &[
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:on"),
    ("t", "thinking"),
];
const CODEX_CONVERSATION_FOOTER_DONE_FOLLOW_ON: &[(&str, &str)] = &[
    ("r", "reply"),
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:on"),
    ("t", "thinking"),
    ("q", "finish"),
];
const CODEX_CONVERSATION_FOOTER_DONE_FOLLOW_OFF: &[(&str, &str)] = &[
    ("r", "reply"),
    ("↑↓", "scroll"),
    ("Ctrl-↑↓", "reply"),
    ("/", "search"),
    ("n/N", "match"),
    ("PgUp/PgDn", "page"),
    ("F", "follow:off"),
    ("t", "thinking"),
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
    last_session_id: Option<String>,
    #[serde(default)]
    last_event: Option<String>,
    updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryRequestState {
    version: u32,
    requested_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateIndexState {
    version: u32,
    last_generated_at: Option<u64>,
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
            last_session_id: None,
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

pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        None => status_project(&current_project_awake()?, None, false, true),
        Some(Commands::New(args)) => new_agent(&current_project_awake()?, args),
        Some(Commands::More(args)) => more_agent(args),
        Some(Commands::Status(args)) => status_project(
            &current_project_awake()?,
            args.role.as_ref(),
            args.all,
            true,
        ),
        Some(Commands::Open) => open_project_directory(),
        Some(Commands::Fix(args)) => fix_workspace(args),
        Some(Commands::Assist(args)) => assist_project(args),
        Some(Commands::List(args)) => status_project(
            &current_project_awake()?,
            args.role.as_ref(),
            args.all,
            false,
        ),
        Some(Commands::Advanced(command)) => run_advanced_command(command),
        Some(Commands::Project(command)) => run_project_command(command),
        Some(Commands::Role(command)) => run_role_command(command),
        Some(Commands::Agent(command)) => run_agent_command(command),
        Some(Commands::Channel(command)) => run_channel_command(command),
        Some(Commands::Help(args)) => print_help(args),
        Some(Commands::RunChild(command)) => run_child_command(command),
    }
}

fn run_child_command(command: RunChildCommand) -> Result<()> {
    match command {
        RunChildCommand::Orchestrator(args) => run_orchestrator(args),
        RunChildCommand::Notices(args) => run_notices(args),
    }
}

fn run_advanced_command(command: AdvancedCommand) -> Result<()> {
    match command {
        AdvancedCommand::RetryErrored => {
            let project = current_project()?;
            retry_waits_now(&project)?;
            wake_project(&project)
        }
        AdvancedCommand::Trigger(args) => trigger_role_manually(args),
        AdvancedCommand::Provider(command) => run_provider_command(command),
    }
}

fn open_project_directory() -> Result<()> {
    let project = current_project()?;
    open_project_directory_at(&project)
}

fn open_project_directory_at(project: &ProjectPaths) -> Result<()> {
    open_path(&project.root)
}

fn open_path(path: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = Command::new("open");
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command.arg("/C").arg("start").arg("");
        command
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    compile_error!("Unsupported platform for opening directories");

    let status = command.arg(path).status().with_context(|| {
        format!(
            "Failed to open `{}` with the platform opener",
            path.display()
        )
    })?;
    if !status.success() {
        bail!(
            "platform opener for `{}` exited with {status}",
            path.display()
        );
    }
    Ok(())
}

fn run_provider_command(command: ProviderCommand) -> Result<()> {
    match command {
        ProviderCommand::Codex(command) => run_codex_provider_command(command),
    }
}

fn run_codex_provider_command(command: CodexProviderCommand) -> Result<()> {
    match command {
        CodexProviderCommand::Login(args) => codex_provider_login(args),
        CodexProviderCommand::List => codex_provider_list(),
        CodexProviderCommand::Use(args) => codex_provider_use(args),
        CodexProviderCommand::Config(args) => codex_provider_config(args),
    }
}

fn codex_provider_login(args: CodexLoginArgs) -> Result<()> {
    let account = read_codex_account_name(args.account, "Codex Account")?;
    crate::provider::codex::authenticate_account(&account, args.home)?;
    println!("authenticated Codex account `{account}`");
    Ok(())
}

fn codex_provider_list() -> Result<()> {
    let state = crate::provider::codex::list_accounts()?;
    for (account, data) in &state.accounts {
        let marker = if *account == state.active_account {
            "*"
        } else {
            " "
        };
        let quota = crate::provider::codex::load_account_rate_limits(account, &data.codex_home)
            .map(|limits| limits.to_string())
            .unwrap_or_else(|| "usage unavailable".to_owned());
        let wait = data
            .quota_wait_until
            .map(|timestamp| {
                format!(
                    "{}quota wait until {}",
                    ui::FIELD_SEPARATOR,
                    format_unix_time(timestamp)
                )
            })
            .unwrap_or_default();
        println!(
            "{marker} {account:<16} {}{}{}",
            data.codex_home.display(),
            ui::FIELD_SEPARATOR,
            quota
        );
        if !wait.is_empty() {
            println!("  {wait}");
        }
    }
    Ok(())
}

fn codex_provider_use(args: CodexUseArgs) -> Result<()> {
    let account = match args.account {
        Some(account) => account,
        None => choose_codex_account()?,
    };
    crate::provider::codex::set_active_account(&account)?;
    println!("active Codex account: `{account}`");
    Ok(())
}

fn codex_provider_config(args: CodexConfigArgs) -> Result<()> {
    let project = current_project()?;
    let mut config = project_config(&project)?;
    if args.model.is_none() && args.thinking.is_none() {
        println!(
            "model: {}",
            config
                .providers
                .codex
                .model
                .as_deref()
                .unwrap_or("(Codex default)")
        );
        println!(
            "thinking: {}",
            config
                .providers
                .codex
                .thinking_level
                .map(|level| level.to_string())
                .unwrap_or_else(|| "(Codex default)".to_owned())
        );
        return Ok(());
    }
    if let Some(model) = args.model {
        config.providers.codex.model = Some(model);
    }
    if let Some(thinking_level) = args.thinking {
        config.providers.codex.thinking_level = Some(thinking_level);
    }
    io::write_toml(&project.config(), &config)?;
    println!(
        "updated Codex provider config in {}",
        project.config().display()
    );
    Ok(())
}

fn codex_provider_config_interactive(project: &ProjectPaths) -> Result<()> {
    let mut config = project_config(project)?;
    let catalog = crate::provider::codex::load_model_catalog(&config.providers.codex)
        .context("Failed to load Codex model catalog")?;
    let (model_labels, model_choices, model_default) =
        codex_model_choices(config.providers.codex.model.as_deref(), &catalog);
    let model_selection = ChoicePrompt::new("Codex Model", model_labels)
        .default(model_default)
        .select()
        .context("Failed to choose Codex model")?;
    let selected_model = match model_choices[model_selection] {
        CodexModelChoice::KeepCurrent => config.providers.codex.model.clone(),
        CodexModelChoice::Default => None,
        CodexModelChoice::Catalog(index) => Some(catalog[index].slug.clone()),
    };
    config.providers.codex.model = selected_model;

    let current_thinking = config
        .providers
        .codex
        .thinking_level
        .map(|level| level.to_string())
        .unwrap_or_else(|| "Codex default".to_owned());
    let supported_levels =
        codex_supported_thinking_levels(config.providers.codex.model.as_deref(), &catalog);
    let mut choices = vec![format!("keep current ({current_thinking})")];
    choices.extend(supported_levels.iter().map(ToString::to_string));
    choices.push("unset to Codex default".to_owned());
    let selection = ChoicePrompt::new("Codex Thinking", choices)
        .default(0)
        .select()
        .context("Failed to read Codex thinking level")?;
    config.providers.codex.thinking_level = if selection == 0 {
        config.providers.codex.thinking_level
    } else if selection == supported_levels.len() + 1 {
        None
    } else {
        Some(supported_levels[selection - 1])
    };
    io::write_toml(&project.config(), &config)
}

#[derive(Clone, Copy)]
enum CodexModelChoice {
    KeepCurrent,
    Default,
    Catalog(usize),
}

fn codex_model_choices(
    current: Option<&str>,
    catalog: &[crate::provider::codex::ModelCatalogEntry],
) -> (Vec<String>, Vec<CodexModelChoice>, usize) {
    let current_index =
        current.and_then(|current| catalog.iter().position(|model| model.slug == current));
    let mut labels = Vec::new();
    let mut choices = Vec::new();
    if let Some(current) = current.filter(|_| current_index.is_none()) {
        labels.push(format!("keep current ({current})"));
        choices.push(CodexModelChoice::KeepCurrent);
    }
    let default_index = labels.len();
    labels.push("Codex default".to_owned());
    choices.push(CodexModelChoice::Default);
    for (index, model) in catalog.iter().enumerate() {
        labels.push(codex_model_label(model));
        choices.push(CodexModelChoice::Catalog(index));
    }
    let selected = current_index
        .map(|index| default_index + 1 + index)
        .unwrap_or(default_index.min(labels.len().saturating_sub(1)));
    (labels, choices, selected)
}

fn codex_model_label(model: &crate::provider::codex::ModelCatalogEntry) -> String {
    let name = if model.display_name == model.slug {
        model.slug.clone()
    } else {
        format!("{} ({})", model.display_name, model.slug)
    };
    match &model.description {
        Some(description) => format!("{name} · {}", ellipsize_display(description, 80)),
        None => name,
    }
}

fn codex_supported_thinking_levels(
    model: Option<&str>,
    catalog: &[crate::provider::codex::ModelCatalogEntry],
) -> Vec<CodexThinkingLevel> {
    let levels = model
        .and_then(|model| catalog.iter().find(|entry| entry.slug == model))
        .map(|entry| entry.supported_reasoning_levels.clone())
        .unwrap_or_else(|| {
            catalog
                .iter()
                .flat_map(|entry| entry.supported_reasoning_levels.iter().copied())
                .collect()
        });
    let mut unique = Vec::new();
    for level in levels {
        if !unique.contains(&level) {
            unique.push(level);
        }
    }
    unique
}

fn read_codex_account_name(account: Option<String>, title: &str) -> Result<String> {
    if let Some(account) = account {
        let account = account.trim().to_owned();
        if !account.is_empty() {
            return Ok(account);
        }
    }
    let Some(account) = PromptEditor::new(title)
        .help("Enter a short account name, for example `work` or `personal`.")
        .help("Leave blank and submit to cancel.")
        .edit()
        .context("Failed to read Codex account name")?
    else {
        return Err(UserCancelled::new("Codex account selection cancelled").into());
    };
    let account = account.trim().to_owned();
    if account.is_empty() {
        return Err(UserCancelled::new("Codex account selection cancelled").into());
    }
    Ok(account)
}

fn choose_codex_account() -> Result<String> {
    let state = crate::provider::codex::list_accounts()?;
    let accounts = state.accounts.keys().cloned().collect::<Vec<_>>();
    let default = accounts
        .iter()
        .position(|account| *account == state.active_account)
        .unwrap_or(0);
    let selected = ChoicePrompt::new("Codex Account", accounts.clone())
        .default(default)
        .select()
        .context("Failed to choose Codex account")?;
    Ok(accounts[selected].clone())
}

fn print_help(args: HelpArgs) -> Result<()> {
    let mut command = if args.all {
        Cli::command().mut_subcommands(|subcommand| match subcommand.get_name() {
            name if name.starts_with("__") => subcommand,
            "run-child" => subcommand,
            _ => subcommand.hide(false),
        })
    } else {
        Cli::command()
    };
    command.print_long_help()?;
    Ok(())
}

fn current_project() -> Result<ProjectPaths> {
    let cwd = std::env::current_dir().context("Failed to read current directory")?;
    match ProjectPaths::find_from(&cwd) {
        Ok(project) => {
            let _ = remember_project(&project);
            Ok(project)
        }
        Err(err) => choose_remembered_project(err),
    }
}

fn current_project_awake() -> Result<ProjectPaths> {
    let project = current_project()?;
    wake_project(&project)?;
    Ok(project)
}

fn wake_current_project_if_present(cwd: &Path) -> Result<()> {
    if let Ok(project) = ProjectPaths::find_from(cwd) {
        wake_project(&project)?;
    }
    Ok(())
}

fn wake_project(project: &ProjectPaths) -> Result<()> {
    let Some(_lock) =
        lock::FileLock::try_acquire(project_wake_lock_path(project), "project wake lock")?
    else {
        return Ok(());
    };
    clean_stale_orchestrator_locks(project)?;
    wake_missing_orchestrators(project)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProjectRegistry {
    version: u32,
    #[serde(default)]
    projects: Vec<ProjectRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectRegistryEntry {
    path: PathBuf,
    last_used: u64,
}

fn choose_remembered_project(original_error: anyhow::Error) -> Result<ProjectPaths> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(original_error);
    }
    let projects = load_remembered_projects()?;
    if projects.is_empty() {
        bail!(
            "{original_error:#}\nNo remembered projects are available in `{}`. Create one with `think project new <path>`.",
            project_registry_path()?.display()
        );
    }
    let labels = projects
        .iter()
        .map(project_choice_label)
        .collect::<Vec<_>>();
    let selected = ChoicePrompt::new("Think Project", labels)
        .select()
        .context("Failed to select think project")?;
    let project = projects
        .get(selected)
        .map(|entry| ProjectPaths::new(entry.path.clone()))
        .context("Selected project disappeared")?;
    remember_project(&project)?;
    Ok(project)
}

fn load_remembered_projects() -> Result<Vec<ProjectRegistryEntry>> {
    let mut registry = read_project_registry()?;
    registry
        .projects
        .retain(|entry| entry.path.join("think.toml").exists());
    registry
        .projects
        .sort_by(|left, right| right.last_used.cmp(&left.last_used));
    write_project_registry(&registry)?;
    Ok(registry.projects)
}

fn remember_project(project: &ProjectPaths) -> Result<()> {
    let path = project
        .root
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize `{}`", project.root.display()))?;
    let mut registry = read_project_registry()?;
    registry.projects.retain(|entry| entry.path != path);
    registry.projects.push(ProjectRegistryEntry {
        path,
        last_used: unix_timestamp(),
    });
    registry
        .projects
        .sort_by(|left, right| right.last_used.cmp(&left.last_used));
    write_project_registry(&registry)
}

fn read_project_registry() -> Result<ProjectRegistry> {
    let path = project_registry_path()?;
    if !path.exists() {
        return Ok(ProjectRegistry {
            version: PROJECT_REGISTRY_VERSION,
            projects: Vec::new(),
        });
    }
    let mut registry = io::read_toml::<ProjectRegistry>(&path)?;
    if registry.version != PROJECT_REGISTRY_VERSION {
        registry = ProjectRegistry {
            version: PROJECT_REGISTRY_VERSION,
            projects: Vec::new(),
        };
    }
    Ok(registry)
}

fn write_project_registry(registry: &ProjectRegistry) -> Result<()> {
    let path = project_registry_path()?;
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    io::write_toml(&path, registry)
}

fn project_registry_path() -> Result<PathBuf> {
    Ok(crate::maintenance::think_home()?.join("projects.toml"))
}

fn project_choice_label(entry: &ProjectRegistryEntry) -> String {
    let project = ProjectPaths::new(entry.path.clone());
    let name = project
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project");
    let summary = project_picker_summary(&project).unwrap_or_else(|_| ProjectPickerSummary {
        badge: '!',
        parts: vec!["state unavailable".to_owned()],
    });
    let mut parts = vec![
        format!("{} {name}", summary.badge),
        format!("opened {}", event_age(entry.last_used)),
    ];
    parts.extend(summary.parts);
    format!("{}\n  {}", parts.join(" · "), project.root.display())
}

struct ProjectPickerSummary {
    badge: char,
    parts: Vec<String>,
}

fn project_picker_summary(project: &ProjectPaths) -> Result<ProjectPickerSummary> {
    let mut running = 0;
    let mut needs_attention = 0;
    let mut paused = 0;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for agent in list_agents(&role_paths)? {
            let state = load_agent(&role_paths.agent(agent))?;
            if state.archived {
                continue;
            }
            match state.status {
                AgentStatus::Starting | AgentStatus::Running => running += 1,
                AgentStatus::NeedsAttention => needs_attention += 1,
                AgentStatus::Paused => paused += 1,
                AgentStatus::Done | AgentStatus::Stopped => {}
            }
        }
    }

    let queued = load_status_queue_rows(project)?
        .into_iter()
        .map(|queue| queue.count)
        .sum::<usize>();

    let badge = if needs_attention > 0 {
        '!'
    } else if running > 0 {
        '●'
    } else if paused > 0 {
        '◐'
    } else {
        '○'
    };
    let mut parts = Vec::new();
    if running > 0 {
        parts.push(format!("{running} running"));
    }
    if needs_attention > 0 {
        parts.push(format!("{needs_attention} attention"));
    }
    if paused > 0 {
        parts.push(format!("{paused} paused"));
    }
    if queued > 0 {
        parts.push(format!("{queued} queued"));
    }
    if parts.is_empty() {
        parts.push("idle".to_owned());
    }
    Ok(ProjectPickerSummary { badge, parts })
}

fn wake_missing_orchestrators(project: &ProjectPaths) -> Result<()> {
    let mut host = None;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let mut state = load_agent(&agent_paths)?;
            repair_paused_agent_state(&agent_paths, &mut state)?;
            if !orchestrator_should_be_running(&config, &state)
                || lock::is_active(&orchestrator_lock_path(project, &role, &agent))?
                || orchestrator_wake_is_recent(&agent_paths)?
            {
                continue;
            }
            let host = match &host {
                Some(host) => host,
                None => {
                    host = Some(NativeSessionHost::new()?);
                    host.as_ref()
                        .expect("native session host was just initialized")
                }
            };
            let pane = host.spawn_pane(PaneSpawnRequest {
                project,
                role: &role,
                agent: &agent,
            })?;
            state.pane_id = Some(pane.pane_id);
            state.status = AgentStatus::Running;
            state.paused_by_user = false;
            state.note = Some("runtime orchestrator restarted by project wake".to_owned());
            save_agent(&agent_paths, &mut state)?;
            let mut supervisor = load_supervisor_state(&agent_paths)?;
            supervisor.last_event = Some(ORCHESTRATOR_WAKE_EVENT.to_owned());
            save_supervisor_state(&agent_paths, &supervisor)?;
            ensure_role_runtime_started(&role_paths)?;
        }
    }
    Ok(())
}

fn clean_stale_orchestrator_locks(project: &ProjectPaths) -> Result<()> {
    let root = project.runtime_dir().join("locks").join("orchestrators");
    let Ok(role_entries) = fs::read_dir(&root) else {
        return Ok(());
    };
    for role_entry in role_entries {
        let role_entry = role_entry
            .with_context(|| format!("Failed to read lock directory `{}`", root.display()))?;
        if !role_entry
            .file_type()
            .with_context(|| format!("Failed to inspect `{}`", role_entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        for lock_entry in fs::read_dir(role_entry.path()).with_context(|| {
            format!(
                "Failed to read lock directory `{}`",
                role_entry.path().display()
            )
        })? {
            let lock_entry = lock_entry.with_context(|| {
                format!(
                    "Failed to read lock under `{}`",
                    role_entry.path().display()
                )
            })?;
            if lock_entry
                .file_type()
                .with_context(|| format!("Failed to inspect `{}`", lock_entry.path().display()))?
                .is_file()
            {
                lock::is_active(&lock_entry.path())?;
            }
        }
    }
    Ok(())
}

fn repair_paused_agent_state(
    agent_paths: &crate::state::AgentPaths,
    state: &mut AgentState,
) -> Result<()> {
    if state.status == AgentStatus::Done
        && state
            .note
            .as_deref()
            .is_some_and(|note| note.starts_with("role is paused"))
    {
        state.status = AgentStatus::Paused;
        save_agent(agent_paths, state)?;
    }
    Ok(())
}

fn orchestrator_should_be_running(config: &RoleConfig, state: &AgentState) -> bool {
    config.status == RoleStatus::Active
        && !state.archived
        && !state.paused_by_user
        && matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
}

fn orchestrator_wake_is_recent(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let supervisor = load_supervisor_state(agent_paths)?;
    Ok(
        supervisor.last_event.as_deref() == Some(ORCHESTRATOR_WAKE_EVENT)
            && unix_timestamp().saturating_sub(supervisor.updated_at)
                < ORCHESTRATOR_WAKE_GRACE_SECONDS,
    )
}

fn project_wake_lock_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("locks").join("wake.lock")
}

fn run_project_command(command: ProjectCommand) -> Result<()> {
    match command {
        ProjectCommand::New(args) => create_project(args),
        ProjectCommand::Init(args) => init_project(
            &std::env::current_dir().context("Failed to read current directory")?,
            choose_template(args.template, args.no_template)?,
        ),
    }
}

fn create_project(args: ProjectNewArgs) -> Result<()> {
    let template = choose_template(args.template, args.no_template)?;
    io::require_empty_or_missing_dir(&args.path)?;
    io::ensure_dir(&args.path)?;
    init_project(&args.path, template)
}

fn init_project(path: &Path, template_choice: Option<ProjectTemplate>) -> Result<()> {
    io::ensure_dir(path)?;
    let root = io::canonicalize_existing(path)?;
    let project = ProjectPaths::new(root);
    if project.config().exists() {
        bail!("`{}` is already a think project.", project.root.display());
    }
    io::write_text_if_missing(&project.project_md(), prompt::DEFAULT_PROJECT_MD)?;
    let config = ProjectConfig::with_template(template_choice);
    io::write_toml(&project.config(), &config)?;
    io::ensure_dir(&project.roles_dir())?;
    io::ensure_dir(&project.channels_dir())?;
    io::ensure_dir(&project.data_dir())?;
    io::ensure_dir(&project.runtime_dir().join("locks"))?;
    io::ensure_dir(&project.runtime_dir().join("locks").join("channels"))?;
    io::ensure_dir(&project.runtime_dir().join("locks").join("trigger-queues"))?;
    io::ensure_dir(&project.runtime_dir().join("trigger-queues"))?;
    io::ensure_dir(&project.runtime_dir().join("trigger-events"))?;
    io::ensure_dir(&project.runtime_dir().join("role-runtime"))?;
    io::ensure_dir(&project.runtime_dir().join("queue-runtime"))?;
    io::ensure_dir(&project.runtime_dir().join("sessions"))?;
    io::ensure_dir(&project.runtime_dir().join("updates"))?;
    for channel in &config.channels {
        git::init_channel(&project.channel_dir(channel))?;
    }
    if let Some(template_choice) = template_choice {
        template::apply(&project, template_choice)?;
        println!("applied project template `{template_choice}`");
        maybe_draft_project_setup(&project, template_choice)?;
    }
    remember_project(&project)?;
    println!("initialized think project at {}", project.root.display());
    Ok(())
}

fn maybe_draft_project_setup(
    project: &ProjectPaths,
    template_choice: ProjectTemplate,
) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(());
    }
    let Some(brief) = PromptEditor::new("Project Setup")
        .help(format!("Template: {template_choice}"))
        .help("Optionally describe the actual project so Codex can tailor PROJECT.md and prompts.")
        .help("Leave blank and submit to keep the generated scaffold unchanged.")
        .edit()
        .context("Failed to read project setup brief")?
    else {
        return Ok(());
    };
    run_codex_conversation(
        &project.root,
        "project-setup",
        assemble_project_setup_prompt(project, template_choice, &brief)?,
        CodexConversationPolicy::WorkspaceWrite,
    )
}

fn assemble_project_setup_prompt(
    project: &ProjectPaths,
    template_choice: ProjectTemplate,
    brief: &str,
) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think project setup")?;
    writeln!(
        prompt,
        "\nYou are running inside the optional Codex-assisted project setup pass immediately after `think project new` or `think project init`."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(prompt, "Template: `{template_choice}`")?;
    writeln!(
        prompt,
        "\nUse the user's brief to tailor the generated project files. Preserve the template's intended workflow and avoid deleting scaffolded structure unless the brief clearly makes it irrelevant."
    )?;
    writeln!(
        prompt,
        "You may edit `PROJECT.md`, role prompt files, role configs, channel lists in \
         `think.toml`, and template TeX scaffold files when doing so improves the project setup."
    )?;
    writeln!(
        prompt,
        "Do not start agents, modify runtime state, publish channel artifacts, or perform long computations. Leave the project in a coherent initialized state."
    )?;
    writeln!(
        prompt,
        "\nBefore editing, read the generated `PROJECT.md`, `think.toml`, roles, and channel configuration so your changes fit the scaffold."
    )?;
    writeln!(
        prompt,
        "End with a compact operator-facing summary: what you changed, important files touched, \
         checks run or skipped, and any setup decisions still needed."
    )?;
    writeln!(prompt, "\n# User project brief\n\n{}", brief.trim())?;
    Ok(prompt)
}

fn choose_template(
    explicit: Option<ProjectTemplate>,
    no_template: bool,
) -> Result<Option<ProjectTemplate>> {
    if explicit.is_some() || no_template {
        return Ok(explicit);
    }
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(None);
    }
    let selection = ChoicePrompt::new("Project Template", ["none", "math-episodes"])
        .default(0)
        .select()
        .context("Failed to read project template selection")?;
    Ok(match selection {
        0 => None,
        1 => Some(ProjectTemplate::MathEpisodes),
        _ => unreachable!("choice prompt returned an invalid selection"),
    })
}

fn new_agent(project: &ProjectPaths, args: NewAgentArgs) -> Result<()> {
    let role = match args.role {
        Some(role) => role,
        None => choose_role_for_agent(project)?,
    };
    let custom_prompt = match custom_agent_prompt(&role, args.prompt, args.no_prompt)? {
        CustomAgentPrompt::Prompt(prompt) => Some(prompt),
        CustomAgentPrompt::Default => None,
        CustomAgentPrompt::Cancel => {
            println!("agent creation cancelled");
            return Ok(());
        }
    };
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    config.status = RoleStatus::Active;
    save_role_config(&role_paths, &config)?;
    let agent = start_one_agent(&role_paths, &config, custom_prompt.as_deref())?;
    if args.attach {
        run_attach_viewer(project, AttachTarget::Agent(ResolvedAgent { role, agent }))?;
    }
    Ok(())
}

fn choose_role_for_agent(project: &ProjectPaths) -> Result<RoleSlug> {
    if let Some(default_role) = project_config(project)?.default_role {
        let paths = RolePaths::new(project.clone(), default_role.clone());
        if paths.config().exists() && load_role_config(&paths)?.status != RoleStatus::Paused {
            return Ok(default_role);
        }
    }
    let mut roles = list_roles(project)?;
    roles.retain(|role| {
        let paths = RolePaths::new(project.clone(), role.clone());
        load_role_config(&paths)
            .map(|config| config.status != RoleStatus::Paused)
            .unwrap_or(false)
    });
    match roles.len() {
        0 => bail!("No runnable roles found. Create one with `think role new <slug>` first."),
        1 => Ok(roles.remove(0)),
        _ if std::io::stdin().is_terminal() && std::io::stderr().is_terminal() => {
            let labels = roles.iter().map(ToString::to_string).collect::<Vec<_>>();
            let selection = ChoicePrompt::new("Role", labels)
                .default(0)
                .select()
                .context("Failed to read role selection")?;
            Ok(roles.remove(selection))
        }
        _ => bail!("Pass a role slug when running `think agent new` noninteractively."),
    }
}

enum CustomAgentPrompt {
    Prompt(String),
    Default,
    Cancel,
}

fn custom_agent_prompt(
    role: &RoleSlug,
    prompt: Option<String>,
    no_prompt: bool,
) -> Result<CustomAgentPrompt> {
    if let Some(prompt) = prompt {
        let prompt = prompt.trim().to_owned();
        return Ok(if prompt.is_empty() {
            CustomAgentPrompt::Cancel
        } else {
            CustomAgentPrompt::Prompt(prompt)
        });
    }
    if no_prompt || !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(CustomAgentPrompt::Default);
    }
    let prompt = PromptEditor::new("Custom Agent Prompt")
        .help(&format!("New agent role: {role}"))
        .help("Write instructions only for this agent. Leave blank and submit to cancel.")
        .edit()
        .context("Failed to read custom agent prompt")?;
    Ok(if let Some(prompt) = prompt {
        CustomAgentPrompt::Prompt(prompt)
    } else {
        CustomAgentPrompt::Cancel
    })
}

fn fix_workspace(args: FixArgs) -> Result<()> {
    let Some(query) = read_query(PromptRequest::fix(), args.query)? else {
        return Err(UserCancelled::new("fix cancelled").into());
    };
    let cwd = std::env::current_dir().context("Failed to read current directory")?;
    wake_current_project_if_present(&cwd)?;
    run_codex_conversation(
        &cwd,
        "fix",
        assemble_fix_prompt(&cwd, &query)?,
        CodexConversationPolicy::WorkspaceWrite,
    )
}

fn check_project(project: &ProjectPaths) -> Result<()> {
    run_codex_conversation(
        &project.root,
        "check",
        assemble_check_prompt(project)?,
        CodexConversationPolicy::ReadOnly,
    )
}

fn assist_project(args: AssistArgs) -> Result<()> {
    let project = current_project_awake()?;
    let Some(query) = read_query(PromptRequest::assist(), args.query)? else {
        return Err(UserCancelled::new("assist cancelled").into());
    };
    assist_project_with_query(&project, &query)
}

fn assist_project_interactive(project: &ProjectPaths) -> Result<()> {
    let Some(query) = read_query(PromptRequest::assist(), None)? else {
        return Err(UserCancelled::new("assist cancelled").into());
    };
    assist_project_with_query(project, &query)
}

fn assist_project_with_query(project: &ProjectPaths, query: &str) -> Result<()> {
    let prompt = assemble_assist_prompt(project, query)?;
    if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        run_codex_conversation_tui(
            &project.root,
            "assist",
            prompt,
            CodexConversationPolicy::WorkspaceWrite,
        )
    } else {
        run_codex_conversation(
            &project.root,
            "assist",
            prompt,
            CodexConversationPolicy::WorkspaceWrite,
        )
    }
}

fn run_notices(args: RunNoticesArgs) -> Result<()> {
    let project = ProjectPaths::new(
        args.project
            .canonicalize()
            .context("Invalid project path")?,
    );
    let Some(_lock) = lock::FileLock::try_acquire(notice_lock_path(&project), "notice lock")?
    else {
        return Ok(());
    };
    io::ensure_dir(&notice_dir(&project))?;
    io::write_text(&notice_current_path(&project), "")?;
    let prompt_path = notice_dir(&project).join("PROMPT.md");
    io::write_text(&prompt_path, &assemble_notices_prompt(&project)?)?;
    let log_path = notice_dir(&project).join(format!("codex-{}.log", unix_timestamp()));
    let stdout = fs::File::create(&log_path)
        .with_context(|| format!("Failed to create `{}`", log_path.display()))?;
    let spec = crate::provider::codex::exec_file_command(
        &project.root,
        &prompt_path,
        None,
        CodexConversationPolicy::WorkspaceWrite,
        &project_config(&project)?.providers.codex,
    )?;
    let status = command_from_spec(&spec)
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            stdout.try_clone().context("Failed to clone notice log")?,
        ))
        .stderr(Stdio::from(stdout))
        .current_dir(&project.root)
        .status()
        .context("Failed to run Codex notice task")?;

    if !status.success() {
        io::write_text(
            &notice_current_path(&project),
            &format!("notice generator failed: codex exited with {status}\n"),
        )?;
    }
    Ok(())
}

fn assemble_notices_prompt(project: &ProjectPaths) -> Result<String> {
    let current = notice_current_path(project);
    let journal = notice_journal_path(project);
    let mut prompt = String::new();
    writeln!(prompt, "# think dashboard notices")?;
    writeln!(
        prompt,
        "\nYou are running as the lightweight notice generator for the `think status` dashboard."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(
        prompt,
        "\nYou may only edit these files:\n- `{}`\n- `{}`",
        current.display(),
        journal.display()
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nThe current notice file must contain zero or more complete operator notice lines. ",
            "Empty means there are no abnormal, operator-actionable notices. If you identify a ",
            "complete notice while inspecting, immediately rewrite the current notice file with ",
            "the complete set of notices known so far. Each notice line must begin with exactly ",
            "`error:`, `warn:`, `action:`, or `info:`, be evidence-based, under 110 characters, ",
            "and be useful to an operator deciding what to inspect."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Only report abnormal or actionable conditions: paused roles, agents needing attention, ",
            "quota waits with retry time, locks blocking active work, failed channel publishing, ",
            "that block automation, missing orchestrators for active agents, or other technical ",
            "conditions the operator should inspect."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Never write progress, placeholder, meta, or business-as-usual lines to the current ",
            "notice file. Forbidden examples include `notice scan active`, `scanning`, `checking`, ",
            "`all clear`, `queues empty`, `idle`, `waiting`, and ordinary empty-queue or idle-orchestrator ",
            "status. If there are no abnormal notices, leave the current notice file empty."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Do not report inert stale lock files for done, stopped, paused, archived, or inactive-role ",
            "agents. Lock files are actionable only when they block work that durable state says should ",
            "currently be running."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Append a timestamped compact entry to the journal file after the scan. Check agent ",
            "health, runtime orchestrator state, trigger queues, channel publishing, quota waits, ",
            "and locks blocking active work. ",
            "Do less thinking than a full project update and finish quickly."
        )
    )?;
    writeln!(
        prompt,
        "Do not start or stop agents, publish channel artifacts, edit role files, or change project configuration."
    )?;
    append_current_project_context(&mut prompt, &project.root)?;
    Ok(prompt)
}

fn trigger_role_manually(args: TriggerArgs) -> Result<()> {
    let project = current_project_awake()?;
    let role = selection::resolve_or_choose_role(&project, args.role, "Role to trigger")?;
    let launch = if args.async_launch {
        TriggerLaunch::Async
    } else {
        TriggerLaunch::Queued {
            queue: role.to_string(),
        }
    };
    launch_triggered_role(
        &project,
        &role,
        &launch,
        TriggerCause::Manual {
            reason: args.reason,
        },
    )?;
    println!("triggered role `{role}`");
    Ok(())
}

fn more_agent(args: MoreArgs) -> Result<()> {
    let project = current_project_awake()?;
    if args.new {
        return new_agent(
            &project,
            NewAgentArgs {
                role: None,
                prompt: args.query,
                no_prompt: false,
                attach: false,
            },
        );
    }
    let choice = selection::resolve_or_choose_agent_or_new(&project, args.agent, "Agent")?;
    let selection::AgentChoice::Existing(resolved) = choice else {
        return new_agent(
            &project,
            NewAgentArgs {
                role: None,
                prompt: args.query,
                no_prompt: false,
                attach: false,
            },
        );
    };
    let label = resolved.label();
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let agent_paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&agent_paths)?;
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        bail!("Agent `{label}` is currently active; attach to it instead.");
    }
    let Some(query) = read_query(
        more_prompt_request(&label, &agent_paths, state.run_count)?,
        args.query,
    )?
    else {
        println!("more cancelled");
        return Ok(());
    };

    let mut query = query;
    let mut query_history = agent_prompt_history(&agent_paths, state.run_count)?;
    loop {
        let transcript_path = run_more_turn(
            &project,
            &role_paths,
            &agent_paths,
            &mut state,
            &label,
            &query,
        )?;
        query_history.push(query.clone());
        if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
            break;
        }
        let Some(follow_up) = read_multiline_request(
            PromptRequest::codex_reply("more", &transcript_path)
                .with_context(
                    "Agent Reply History",
                    agent_history_context(&agent_paths, state.run_count)?,
                )
                .with_history(query_history.clone()),
        )?
        else {
            break;
        };
        query = follow_up;
    }

    Ok(())
}

fn more_prompt_request(
    label: &str,
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<PromptRequest> {
    Ok(PromptRequest::more(label)
        .with_context(
            "Agent Reply History",
            agent_history_context(agent_paths, run_count)?,
        )
        .with_history(agent_prompt_history(agent_paths, run_count)?))
}

fn agent_history_context(agent_paths: &crate::state::AgentPaths, run_count: u64) -> Result<String> {
    let mut context = String::new();
    if let Some(agent_prompt) = io::read_optional_text(&agent_paths.root().join("AGENT_PROMPT.md"))?
        && !agent_prompt.trim().is_empty()
    {
        writeln!(
            context,
            "# Agent-specific prompt\n\n{}",
            agent_prompt.trim()
        )?;
    }
    if run_count == 0
        && let Some(root_prompt) = io::read_optional_text(&agent_paths.root().join("PROMPT.md"))?
        && !root_prompt.trim().is_empty()
    {
        writeln!(context, "\n# Initial prompt\n\n{}", root_prompt.trim())?;
    }
    for run_id in 1..=run_count {
        let run_paths = agent_paths.run(run_id);
        if let Some(prompt) = io::read_optional_text(&run_paths.prompt())?
            && !prompt.trim().is_empty()
        {
            if let Some(follow_up) = extract_user_follow_up(&prompt) {
                writeln!(context, "\n# Run {run_id} user follow-up\n\n{follow_up}")?;
            } else {
                writeln!(
                    context,
                    "\n# Run {run_id} prompt\n\n{}",
                    summarize_prompt_for_history(&prompt, &run_paths.prompt())
                )?;
            }
        }
        if let Some(reply) = io::read_optional_text(&run_paths.reply())?
            && !reply.trim().is_empty()
        {
            writeln!(context, "\n# Run {run_id} agent reply\n\n{}", reply.trim())?;
        } else if let Some(transcript) = io::read_optional_text(&run_paths.transcript_text())?
            && !transcript.trim().is_empty()
        {
            writeln!(
                context,
                "\n# Run {run_id} transcript tail (no REPLY.md was recorded)\n\n{}",
                transcript_tail(&transcript, AGENT_HISTORY_TRANSCRIPT_TAIL_LINES)
            )?;
        }
    }
    if context.trim().is_empty() {
        Ok("No prior prompt or transcript output has been recorded for this agent yet.".to_owned())
    } else {
        Ok(context)
    }
}

fn summarize_prompt_for_history(prompt: &str, path: &Path) -> String {
    let title = prompt
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with('#') && !line.trim_start_matches('#').trim().is_empty())
        .map(|line| line.trim_start_matches('#').trim().to_owned())
        .unwrap_or_else(|| "managed think prompt".to_owned());
    format!("{title}\nPrompt file: {}", path.display())
}

fn agent_prompt_history(
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<Vec<String>> {
    let mut history = Vec::new();
    if let Some(agent_prompt) = io::read_optional_text(&agent_paths.root().join("AGENT_PROMPT.md"))?
        && !agent_prompt.trim().is_empty()
    {
        history.push(agent_prompt.trim().to_owned());
    }
    for run_id in 1..=run_count {
        let Some(prompt) = io::read_optional_text(&agent_paths.run(run_id).prompt())? else {
            continue;
        };
        if let Some(follow_up) = extract_user_follow_up(&prompt) {
            history.push(follow_up);
        }
    }
    Ok(history)
}

fn extract_user_follow_up(prompt: &str) -> Option<String> {
    let (_, follow_up) = prompt.split_once("# User follow-up")?;
    let follow_up = follow_up.trim();
    (!follow_up.is_empty()).then(|| follow_up.to_owned())
}

fn transcript_tail(transcript: &str, max_lines: usize) -> String {
    let lines = transcript
        .lines()
        .map(strip_ansi_for_context)
        .map(|line| line.trim_end().to_owned())
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let start = lines
        .len()
        .saturating_sub(max_lines.max(MIN_AGENT_HISTORY_TRANSCRIPT_TAIL_LINES));
    lines[start..].join("\n")
}

fn strip_ansi_for_context(line: &str) -> String {
    let mut output = String::new();
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else if ch != '\r' {
            output.push(ch);
        }
    }
    output
}

fn run_more_turn(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    state: &mut AgentState,
    label: &str,
    query: &str,
) -> Result<PathBuf> {
    let run_id = state.run_count + 1;
    let run_paths = agent_paths.run(run_id);
    io::ensure_dir(&run_paths.root())?;
    let prompt = assemble_more_prompt(project, role_paths, agent_paths, state, query)?;
    io::write_text(&run_paths.prompt(), &prompt)?;

    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.archived = false;
    state.note = Some("think more follow-up".to_owned());
    save_agent(agent_paths, state)?;
    ensure_role_runtime_started(role_paths)?;

    let session_id = latest_codex_session_id(agent_paths, state.run_count)?;
    println!(
        "continuing agent `{}` with {}",
        label,
        if session_id.is_some() {
            "codex resume"
        } else {
            "codex resume --last"
        }
    );
    let started_at = unix_timestamp();
    let more_run = run_more_codex_command(agent_paths, &run_paths, session_id.as_deref());
    let finished_at = unix_timestamp();
    let more_run = match more_run {
        Ok(more_run) if more_run.exit.success => more_run,
        Ok(more_run) => {
            let exit = run_exit_from_pty(
                run_id,
                StepSlug::parse("more")?,
                started_at,
                finished_at,
                more_run.exit,
                None,
                more_run.message,
            );
            io::write_toml(&run_paths.exit(), &exit)?;
            let mut state = load_agent(agent_paths)?;
            let code = exit.code;
            state.status = AgentStatus::NeedsAttention;
            state.note = Some(format!("think more exited with status code {}", exit.code));
            state.last_exit = Some(exit);
            save_agent(agent_paths, &mut state)?;
            refresh_role_runtime_started(role_paths)?;
            bail!("think more exited with status code {code}");
        }
        Err(err) => {
            let mut state = load_agent(agent_paths)?;
            state.status = AgentStatus::NeedsAttention;
            state.note = Some(err.to_string());
            save_agent(agent_paths, &mut state)?;
            refresh_role_runtime_started(role_paths)?;
            return Err(err);
        }
    };

    let mut updated_state = load_agent(agent_paths)?;
    finalize_channels(project, agent_paths, &updated_state, run_id)?;
    let exit = run_exit_from_pty(
        run_id,
        StepSlug::parse("more")?,
        started_at,
        finished_at,
        more_run.exit,
        None,
        more_run.message,
    );
    io::write_toml(&run_paths.exit(), &exit)?;
    updated_state.run_count = run_id;
    updated_state.status = AgentStatus::Done;
    updated_state.paused_by_user = false;
    updated_state.note = None;
    updated_state.last_exit = Some(exit);
    save_agent(agent_paths, &mut updated_state)?;
    refresh_role_runtime_started(role_paths)?;
    *state = updated_state;
    Ok(run_paths.transcript_text())
}

struct PromptRequest {
    title: String,
    help: Vec<String>,
    context: Option<(String, String)>,
    history: Vec<String>,
}

impl PromptRequest {
    fn fix() -> Self {
        Self {
            title: "Fix Request".to_owned(),
            help: vec![
                "Describe the problem you want Codex to fix.".to_owned(),
                "The request is sent as the user prompt for `think fix`; blank submit cancels."
                    .to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn assist() -> Self {
        Self {
            title: "Project Assist".to_owned(),
            help: vec![
                "Describe what you want the project assistant to configure or inspect.".to_owned(),
                "It may run think CLI commands and edit project configuration as needed."
                    .to_owned(),
                "Blank submit cancels.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn more(agent: &str) -> Self {
        Self {
            title: format!("More For {agent}"),
            help: vec![
                "Write the follow-up query for this agent.".to_owned(),
                "The query is sent in the same Codex session when one can be resumed.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn draft(label: &str) -> Self {
        Self {
            title: label.to_owned(),
            help: vec![
                "Describe the requested role draft or revision.".to_owned(),
                "Blank submit cancels this step.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn codex_reply(command: &str, transcript: &Path) -> Self {
        Self {
            title: format!("Reply To think {command}"),
            help: vec![
                "Codex's latest output is shown above the editor buffer; PageUp/PageDown scrolls it."
                    .to_owned(),
                format!("Transcript: {}", transcript.display()),
                "Write a follow-up reply, or submit blank to finish.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn with_context(mut self, title: impl Into<String>, text: String) -> Self {
        self.context = Some((title.into(), text));
        self
    }

    fn with_history(mut self, history: Vec<String>) -> Self {
        self.history = history;
        self
    }
}

fn read_query(request: PromptRequest, query: Option<String>) -> Result<Option<String>> {
    if let Some(query) = query {
        let query = query.trim().to_owned();
        return Ok((!query.is_empty()).then_some(query));
    }
    if std::io::stdin().is_terminal() && std::io::stderr().is_terminal() {
        return read_multiline_request(request);
    }
    bail!("Pass a query when running noninteractively.")
}

fn assemble_more_prompt(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    query: &str,
) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think more follow-up")?;
    writeln!(
        prompt,
        "\nYou are continuing via `think more` for agent `{}/{}`.",
        role_paths.role, agent_paths.agent
    )?;
    writeln!(
        prompt,
        "You may be in a resumed Codex session or a fresh Codex session if resume metadata was unavailable."
    )?;
    writeln!(
        prompt,
        "Continue in the same project context, workspace, data directories, channels, and conventions as the original run."
    )?;
    writeln!(prompt, "\nImportant files in this agent root:")?;
    writeln!(prompt, "- `PROMPT.md`: original assembled think prompt")?;
    writeln!(
        prompt,
        "- `AGENT_PROMPT.md`: original agent-specific prompt, if present"
    )?;
    writeln!(
        prompt,
        "- `agent.toml` and `manifest.toml`: current agent state and manifest"
    )?;
    writeln!(prompt, "- `runs/`: prior prompts and transcripts")?;
    writeln!(
        prompt,
        "\nDo not start over. Read enough of the prior prompts, transcripts, manifests, work directory, and channel outboxes to answer the user's follow-up in continuity."
    )?;
    writeln!(
        prompt,
        "Publish any newly finished artifacts through `channels/` before exiting, as in the original think prompt."
    )?;
    writeln!(prompt, "\n# Runtime Summary")?;
    writeln!(prompt, "- project: `{}`", project.root.display())?;
    writeln!(prompt, "- role: `{}`", role_paths.role)?;
    writeln!(prompt, "- agent: `{}`", agent_paths.agent)?;
    writeln!(prompt, "- prior run count: `{}`", state.run_count)?;
    writeln!(
        prompt,
        "- run reply file: `{}`",
        agent_paths.run(state.run_count + 1).reply().display()
    )?;
    writeln!(
        prompt,
        "\nBefore exiting, write a compact final reply to the run reply file. It should read like \
         the final answer you would give the operator for this follow-up."
    )?;
    writeln!(prompt, "\n# User follow-up\n\n{}", query.trim())?;
    Ok(prompt)
}

fn run_more_codex_command(
    agent_paths: &crate::state::AgentPaths,
    run_paths: &crate::state::RunPaths,
    session_id: Option<&str>,
) -> Result<MoreCodexRun> {
    let resume_exit = runner::run_command_no_stdin(
        codex_resume_command(
            &agent_paths.root(),
            run_paths.run_id,
            session_id,
            &run_paths.reply(),
        )?,
        run_paths,
    )?;
    if resume_exit.success {
        return Ok(MoreCodexRun {
            exit: resume_exit,
            message: Some(match session_id {
                Some(session_id) => format!("resumed codex session {session_id}"),
                None => "resumed latest codex session in agent root".to_owned(),
            }),
        });
    }
    if !codex_resume_unavailable(run_paths)? {
        return Ok(MoreCodexRun {
            exit: resume_exit,
            message: None,
        });
    }

    println!(
        "\nCodex resume was unavailable; starting a fresh Codex exec with the saved continuity prompt."
    );
    Ok(MoreCodexRun {
        exit: runner::run_command_no_stdin(
            codex_more_fresh_command(&agent_paths.root(), run_paths.run_id, &run_paths.reply())?,
            run_paths,
        )?,
        message: Some(match session_id {
            Some(session_id) => {
                format!("started fresh codex exec after session {session_id} was unavailable")
            }
            None => "started fresh codex exec after resume --last was unavailable".to_owned(),
        }),
    })
}

struct MoreCodexRun {
    exit: runner::PtyExit,
    message: Option<String>,
}

fn run_exit_from_pty(
    run_id: u64,
    step: StepSlug,
    started_at: u64,
    finished_at: u64,
    exit: runner::PtyExit,
    disposition: Option<Disposition>,
    message: Option<String>,
) -> RunExitState {
    RunExitState {
        run_id,
        step,
        started_at,
        finished_at,
        success: exit.success,
        code: exit.code,
        signal: exit.signal,
        disposition,
        message,
    }
}

fn codex_resume_command(
    agent_dir: &Path,
    run_id: u64,
    session_id: Option<&str>,
    reply_path: &Path,
) -> Result<CommandSpec> {
    crate::provider::codex::resume_command(
        agent_dir,
        session_id,
        more_prompt_instruction(run_id),
        Some(reply_path),
        &codex_config_for_cwd(agent_dir)?,
    )
}

fn codex_more_fresh_command(
    agent_dir: &Path,
    run_id: u64,
    reply_path: &Path,
) -> Result<CommandSpec> {
    crate::provider::codex::fresh_more_command(
        agent_dir,
        more_prompt_instruction(run_id),
        Some(reply_path),
        &codex_config_for_cwd(agent_dir)?,
    )
}

fn more_prompt_instruction(run_id: u64) -> String {
    format!("Read runs/{run_id}/PROMPT.md in the current directory and follow it exactly.")
}

fn latest_codex_session_id(
    agent_paths: &crate::state::AgentPaths,
    completed_runs: u64,
) -> Result<Option<String>> {
    for run_id in (1..=completed_runs).rev() {
        let Some(text) = io::read_optional_text(&agent_paths.run(run_id).transcript_text())? else {
            continue;
        };
        if let Some(session_id) = extract_latest_codex_session_id(&text) {
            return Ok(Some(session_id));
        }
    }
    Ok(None)
}

fn extract_latest_codex_session_id(text: &str) -> Option<String> {
    text.lines().rev().find_map(extract_codex_session_id)
}

fn extract_codex_session_id(line: &str) -> Option<String> {
    let start = line.find("session id:")? + "session id:".len();
    let candidate = line[start..].split_whitespace().next()?;
    if candidate.len() >= 16
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        Some(candidate.to_owned())
    } else {
        None
    }
}

fn codex_resume_unavailable(run_paths: &crate::state::RunPaths) -> Result<bool> {
    let text = io::read_optional_text(&run_paths.transcript_text())?
        .unwrap_or_default()
        .to_lowercase();
    Ok([
        "no session",
        "no sessions",
        "no recorded session",
        "no recorded sessions",
        "session not found",
        "could not find session",
        "couldn't find session",
        "unable to find session",
        "conversation not found",
    ]
    .iter()
    .any(|pattern| text.contains(pattern)))
}

fn assemble_fix_prompt(cwd: &Path, query: &str) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think fix")?;
    writeln!(
        prompt,
        "\nYou are running inside the `think fix` subcommand. The user asked think to fix something in the current workspace."
    )?;
    writeln!(prompt, "\nWorking directory: `{}`", cwd.display())?;
    writeln!(
        prompt,
        "\nMake a best-effort attempt to fix the requested problem. Inspect the tree, make the requested fix, preserve unrelated changes, and run focused verification."
    )?;
    writeln!(
        prompt,
        "If the request is genuinely ambiguous or blocked, ask a concise clarification question and stop after explaining exactly what you need."
    )?;
    writeln!(
        prompt,
        "The sections below explain `think` and, when available, the current think project. They are included by the harness, not by the user."
    )?;
    append_think_doc_context(&mut prompt, cwd)?;
    append_current_project_context(&mut prompt, cwd)?;
    writeln!(prompt, "\n# User fix request\n\n{}", query.trim())?;
    Ok(prompt)
}

fn assemble_check_prompt(project: &ProjectPaths) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think check")?;
    writeln!(
        prompt,
        "\nYou are running from the `think status` dashboard check action. Audit the current think project for technical health."
    )?;
    writeln!(
        prompt,
        "\nProject root: `{}`\n\nTake no permanent actions: do not edit files, do not start or stop agents, publish channel artifacts, or kill processes.",
        project.root.display()
    )?;
    writeln!(
        prompt,
        "Inspect project state, agent manifests, run transcripts, channel logs, trigger queues, and obvious runtime problems. If the project is technically healthy, summarize the nontechnical research/progress state."
    )?;
    writeln!(
        prompt,
        "If anything looks unhealthy, report the issue, evidence, severity, and the next command or fix that would address it."
    )?;
    append_think_doc_context(&mut prompt, &project.root)?;
    append_current_project_context(&mut prompt, &project.root)?;
    Ok(prompt)
}

fn assemble_assist_prompt(project: &ProjectPaths, query: &str) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think assist")?;
    writeln!(
        prompt,
        "\nYou are running from the `think assist` operator workflow. The user wants help operating this think project."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nYou may take permanent project-management actions when they directly serve the user's request: ",
            "run `think` CLI commands, create or configure roles, create or continue agents, trigger roles, ",
            "change provider settings, and inspect runtime state. Prefer the public `think` CLI for operational ",
            "changes. Edit project files directly only when the CLI does not expose the needed operation or the ",
            "docs make direct editing the intended interface."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Use the same operational awareness expected of the dashboard notices agent: inspect agent health, ",
            "runtime orchestrator state, trigger queues, channel logs, quota waits, locks, manifests, transcripts, ",
            "and project docs as needed. Unlike notices/check/update, you are allowed to act."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Do not invent hidden state. Read files and run commands to verify assumptions. Keep changes scoped, ",
            "preserve unrelated work, and avoid doing the mathematical research yourself unless the user's request ",
            "explicitly asks for a small direct inspection. When launching or modifying agents, make the prompt and ",
            "role choices explicit and coherent."
        )
    )?;
    writeln!(
        prompt,
        "\nBefore exiting, report exactly what you changed or launched, which commands you ran, and any remaining operator decisions."
    )?;
    append_think_doc_context(&mut prompt, &project.root)?;
    append_current_project_context(&mut prompt, &project.root)?;
    append_project_operation_snapshot(&mut prompt, project)?;
    writeln!(prompt, "\n# User assist request\n\n{}", query.trim())?;
    Ok(prompt)
}

fn append_think_doc_context(prompt: &mut String, cwd: &Path) -> Result<()> {
    let Some(source_dir) = find_think_source_dir(cwd)? else {
        writeln!(
            prompt,
            "\n# think-tool docs\n\nNo local think-tool source directory was found; inspect the workspace directly."
        )?;
        return Ok(());
    };
    writeln!(
        prompt,
        "\n# think-tool source\n\n`{}`",
        source_dir.display()
    )?;
    append_file_section(prompt, &source_dir.join("README.md"))?;
    append_file_section(prompt, &source_dir.join("DESIGN.md"))?;
    Ok(())
}

fn append_current_project_context(prompt: &mut String, cwd: &Path) -> Result<()> {
    let Ok(project) = ProjectPaths::find_from(cwd) else {
        writeln!(
            prompt,
            "\n# Current think project\n\nNo containing think project was found from `{}`.",
            cwd.display()
        )?;
        return Ok(());
    };
    writeln!(
        prompt,
        "\n# Current think project\n\nRoot: `{}`",
        project.root.display()
    )?;
    append_file_section(prompt, &project.project_md())?;
    append_file_section(prompt, &project.config())?;
    writeln!(
        prompt,
        "\nAgents, channels, runtime files, and transcripts live under this project root; inspect them directly when needed."
    )?;
    Ok(())
}

fn append_project_operation_snapshot(prompt: &mut String, project: &ProjectPaths) -> Result<()> {
    writeln!(prompt, "\n# Current operational snapshot")?;
    writeln!(prompt, "\n## Roles and agents")?;
    for role in load_status_roles(project, None, true)? {
        writeln!(
            prompt,
            "- role `{}`: status {}, mode {}, parallel {}, expose {}",
            role.name, role.status, role.mode, role.parallel, role.expose
        )?;
        for agent in role.agents {
            let detail = if agent.detail.trim().is_empty() {
                String::new()
            } else {
                format!("; {}", agent.detail)
            };
            writeln!(
                prompt,
                "  - agent `{}`: status {}; summary `{}`{}",
                agent.name, agent.status, agent.summary, detail
            )?;
        }
    }
    writeln!(prompt, "\n## Channels")?;
    let channels = load_status_channel_rows(project)?;
    if channels.is_empty() {
        writeln!(prompt, "No channels.")?;
    } else {
        for channel in channels {
            writeln!(
                prompt,
                "- `{}`: {} artifacts, latest {}",
                channel.name,
                channel.artifacts,
                channel.latest.as_deref().unwrap_or("-")
            )?;
        }
    }
    writeln!(prompt, "\n## Queues")?;
    let queues = load_all_status_queue_rows(project)?;
    if queues.is_empty() {
        writeln!(prompt, "No nonempty or locked queues.")?;
    } else {
        for queue in queues {
            let active = queue
                .active
                .as_ref()
                .map(|active| format!(", active {}", active.label))
                .unwrap_or_default();
            writeln!(
                prompt,
                "- {} `{}`: pending {}, locked {}{}",
                queue.kind.label(),
                queue.name,
                queue.count,
                queue.locked,
                active
            )?;
        }
    }
    let notices = load_notice_lines(project)?.0;
    if !notices.is_empty() {
        writeln!(prompt, "\n## Dashboard notices")?;
        for notice in notices {
            writeln!(
                prompt,
                "- {}: {}",
                notice_severity_label(notice.severity),
                notice.text
            )?;
        }
    }
    Ok(())
}

fn append_file_section(prompt: &mut String, path: &Path) -> Result<()> {
    if path.exists() {
        writeln!(
            prompt,
            "\n## `{}`\n\n{}",
            path.display(),
            io::read_text(path)?
        )?;
    }
    Ok(())
}

fn find_think_source_dir(cwd: &Path) -> Result<Option<PathBuf>> {
    for ancestor in cwd.ancestors() {
        let candidate = ancestor.join("think-tool");
        if candidate.join("DESIGN.md").exists() && candidate.join("README.md").exists() {
            return Ok(Some(candidate));
        }
        if ancestor.join("DESIGN.md").exists()
            && ancestor.join("README.md").exists()
            && ancestor.join("Cargo.toml").exists()
            && ancestor.file_name().and_then(|name| name.to_str()) == Some("think-tool")
        {
            return Ok(Some(ancestor.to_owned()));
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if manifest_dir.join("DESIGN.md").exists() && manifest_dir.join("README.md").exists() {
        return Ok(Some(manifest_dir));
    }
    Ok(None)
}

fn run_codex_conversation(
    cwd: &Path,
    command_name: &str,
    initial_prompt: String,
    policy: CodexConversationPolicy,
) -> Result<()> {
    let root = command_run_root(cwd, command_name)?;
    io::ensure_dir(&root)?;
    let mut turn = 1;
    let mut session_id = None;
    let mut prompt = initial_prompt;
    let mut follow_up_history = Vec::new();
    loop {
        let turn_root = root.join(turn.to_string());
        io::ensure_dir(&turn_root)?;
        let prompt_path = turn_root.join("PROMPT.md");
        let reply_path = turn_root.join("REPLY.md");
        io::write_text(&prompt_path, &prompt)?;
        print_codex_conversation_progress(command_name, turn)?;
        let exit = runner::run_command_in_dir_no_stdin_quiet(
            codex_exec_file_command(
                cwd,
                &prompt_path,
                session_id.as_deref(),
                policy,
                &reply_path,
            )?,
            &turn_root,
        )?;
        let transcript =
            io::read_optional_text(&turn_root.join("TRANSCRIPT.txt"))?.unwrap_or_default();
        session_id = extract_latest_codex_session_id(&transcript).or(session_id);
        if !exit.success {
            bail!("`codex exec` failed with status code {}", exit.code);
        }
        if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
            return Ok(());
        }
        clear_codex_conversation_progress()?;
        let follow_up = read_multiline_request(
            PromptRequest::codex_reply(command_name, &turn_root.join("TRANSCRIPT.txt"))
                .with_context(
                    "Codex Reply",
                    codex_conversation_context(&reply_path, &transcript)?,
                )
                .with_history(follow_up_history.clone()),
        )?;
        let Some(follow_up) = follow_up else {
            return Ok(());
        };
        follow_up_history.push(follow_up.clone());
        turn += 1;
        prompt = format!(
            "# think {command_name} follow-up\n\nContinue the same `think {command_name}` session. The user replied:\n\n{follow_up}\n"
        );
    }
}

fn run_codex_conversation_tui(
    cwd: &Path,
    command_name: &str,
    initial_prompt: String,
    policy: CodexConversationPolicy,
) -> Result<()> {
    let root = command_run_root(cwd, command_name)?;
    io::ensure_dir(&root)?;
    let mut terminal = TerminalSession::enter()?;
    let mut turn = 1;
    let mut session_id = None;
    let mut prompt = initial_prompt;
    let mut follow_up_history = Vec::new();
    loop {
        let turn_root = root.join(turn.to_string());
        io::ensure_dir(&turn_root)?;
        let prompt_path = turn_root.join("PROMPT.md");
        let reply_path = turn_root.join("REPLY.md");
        io::write_text(&prompt_path, &prompt)?;
        let spec = codex_exec_file_command(
            cwd,
            &prompt_path,
            session_id.as_deref(),
            policy,
            &reply_path,
        )?;
        let runner_root = turn_root.clone();
        let handle =
            thread::spawn(move || runner::run_command_in_dir_no_stdin_quiet(spec, &runner_root));
        let mut app = CodexConversationApp::new(
            command_name,
            root.clone(),
            turn,
            turn_root.clone(),
            follow_up_history.clone(),
        );
        let outcome = run_codex_conversation_turn(&mut terminal, &mut app, handle)?;
        let transcript =
            io::read_optional_text(&turn_root.join("TRANSCRIPT.txt"))?.unwrap_or_default();
        session_id = extract_latest_codex_session_id(&transcript).or(session_id);
        match outcome {
            CodexConversationOutcome::Finish => return Ok(()),
            CodexConversationOutcome::FollowUp(follow_up) => {
                follow_up_history.push(follow_up.clone());
                turn += 1;
                prompt = format!(
                    "# think {command_name} follow-up\n\nContinue the same `think {command_name}` session. The user replied:\n\n{follow_up}\n"
                );
            }
        }
    }
}

fn run_codex_conversation_turn(
    terminal: &mut TerminalSession,
    app: &mut CodexConversationApp,
    handle: thread::JoinHandle<Result<runner::PtyExit>>,
) -> Result<CodexConversationOutcome> {
    loop {
        if handle.is_finished() {
            match handle
                .join()
                .unwrap_or_else(|_| Err(anyhow!("Codex runner thread panicked")))
            {
                Ok(exit) => app.set_exit(exit),
                Err(err) => app.set_error(err.to_string()),
            }
            break;
        }
        terminal.draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(outcome) = app.handle_key(key)?
        {
            return Ok(outcome);
        }
    }
    loop {
        terminal.draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(outcome) = app.handle_key(key)?
        {
            return match outcome {
                CodexConversationOutcome::FollowUp(_) if app.turn_failed() => {
                    app.message =
                        Some("turn failed; quit after reviewing the transcript".to_owned());
                    Ok(CodexConversationOutcome::Finish)
                }
                outcome => Ok(outcome),
            };
        }
    }
}

fn print_codex_conversation_progress(command_name: &str, turn: u64) -> Result<()> {
    if std::io::stdout().is_terminal() {
        let mut stdout = std::io::stdout().lock();
        write!(
            stdout,
            "\r\x1b[2K{} running Codex for `think {command_name}` turn {turn}...",
            ui::spinner_frame(turn as usize)
        )?;
        stdout.flush()?;
    }
    Ok(())
}

fn clear_codex_conversation_progress() -> Result<()> {
    if std::io::stdout().is_terminal() {
        let mut stdout = std::io::stdout().lock();
        write!(stdout, "\r\x1b[2K")?;
        stdout.flush()?;
    }
    Ok(())
}

fn codex_conversation_context(reply_path: &Path, transcript: &str) -> Result<String> {
    if let Some(reply) = io::read_optional_text(reply_path)?
        && !reply.trim().is_empty()
    {
        return Ok(format!(
            "{}\n\nTranscript: {}",
            reply.trim(),
            reply_path
                .parent()
                .map(|path| path.join("TRANSCRIPT.txt").display().to_string())
                .unwrap_or_else(|| "(unavailable)".to_owned())
        ));
    }
    let tail = transcript_tail(transcript, AGENT_HISTORY_TRANSCRIPT_TAIL_LINES);
    Ok(if tail.is_empty() {
        "Codex produced no final reply. See TRANSCRIPT.txt for raw output.".to_owned()
    } else {
        format!("Codex final reply was unavailable; transcript tail:\n\n{tail}")
    })
}

fn command_run_root(cwd: &Path, command_name: &str) -> Result<PathBuf> {
    let timestamp = unix_timestamp();
    if let Ok(project) = ProjectPaths::find_from(cwd) {
        Ok(project
            .runtime_dir()
            .join("commands")
            .join(command_name)
            .join(timestamp.to_string()))
    } else {
        Ok(crate::maintenance::think_home()?
            .join("commands")
            .join(command_name)
            .join(timestamp.to_string()))
    }
}

fn codex_exec_file_command(
    cwd: &Path,
    prompt_path: &Path,
    session_id: Option<&str>,
    policy: CodexConversationPolicy,
    reply_path: &Path,
) -> Result<CommandSpec> {
    let mut spec = crate::provider::codex::exec_file_command(
        cwd,
        prompt_path,
        session_id,
        policy,
        &codex_config_for_cwd(cwd)?,
    )?;
    let prompt = spec
        .args
        .pop()
        .context("Codex command was missing its prompt argument")?;
    spec.args.extend([
        "--output-last-message".to_owned(),
        reply_path.display().to_string(),
    ]);
    spec.args.push(prompt);
    Ok(spec)
}

fn command_from_spec(spec: &CommandSpec) -> Command {
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).current_dir(&spec.cwd);
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    command
}

fn codex_config_for_cwd(cwd: &Path) -> Result<crate::config::CodexProviderConfig> {
    Ok(ProjectPaths::find_from(cwd)
        .ok()
        .and_then(|project| project_config(&project).ok())
        .map(|config| config.providers.codex)
        .unwrap_or_default())
}

fn run_role_command(command: RoleCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        RoleCommand::New(args) => create_role(&project, args),
        RoleCommand::Draft(args) => draft_role(&project, args),
        RoleCommand::Edit(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to edit")?;
            edit_role(&project, &role)
        }
        RoleCommand::Activate(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to activate")?;
            activate_role(&project, &role)
        }
        RoleCommand::Pause(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to pause")?;
            set_role_status(&project, &role, RoleStatus::Paused)
        }
    }
}

fn run_agent_command(command: AgentCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        AgentCommand::New(args) => new_agent(&project, args),
        AgentCommand::Attach(args) => attach(args),
        AgentCommand::Archive(args) => archive_agent(&project, args),
        AgentCommand::Pause(args) => pause_agent(&project, args),
        AgentCommand::Stop(args) => stop_agent(&project, args),
        AgentCommand::Resume(args) => resume_agent(&project, args),
    }
}

fn run_channel_command(command: ChannelCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        ChannelCommand::New(args) => {
            let channel = selection::resolve_or_prompt_new_slug(args.channel, "Channel to create")?;
            create_channel(&project, &channel)
        }
        ChannelCommand::List => list_channels_only(&project),
    }
}

fn create_channel(project: &ProjectPaths, channel: &ChannelSlug) -> Result<()> {
    git::init_channel(&project.channel_dir(channel))?;
    let mut config = project_config(project)?;
    if !config.channels.iter().any(|existing| existing == channel) {
        config.channels.push(channel.clone());
        config.channels.sort();
        io::write_toml(&project.config(), &config)?;
    }
    println!("channel `{channel}` ready");
    Ok(())
}

fn create_role(project: &ProjectPaths, args: RoleNewArgs) -> Result<()> {
    let role = selection::resolve_or_prompt_new_slug(args.role, "Role to create")?;
    let project_config = project_config(project)?;
    let steps = if args.steps.is_empty() {
        vec![template::default_step_slug(project_config.template)?]
    } else {
        args.steps
    };
    let agent_names = args
        .agent_names
        .unwrap_or_else(|| template::default_agent_names(project_config.template));
    let parallel = args
        .parallel
        .unwrap_or_else(|| template::default_parallel(project_config.template));

    let role_paths = RolePaths::new(project.clone(), role.clone());
    if role_paths.root().exists() {
        bail!("Role `{role}` already exists.");
    }
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text(
        &role_paths.role_md(),
        template::default_role_md(project_config.template),
    )?;
    for step in &steps {
        io::write_text_if_missing(
            &role_paths.step_path(step),
            template::default_step_md(project_config.template, step),
        )?;
    }
    let config = RoleConfig {
        version: ROLE_CONFIG_VERSION,
        status: if args.active {
            RoleStatus::Active
        } else {
            RoleStatus::Draft
        },
        backend: project_config.default_backend,
        mode: args.mode,
        parallel,
        agent_names,
        agent_prefix: args.agent_prefix,
        auto_archive: args.auto_archive,
        expose: args.expose,
        steps,
        triggers: Vec::new(),
    };
    validate_role_config(&config)?;
    save_role_config(&role_paths, &config)?;
    println!("role `{role}` created");
    if args.active {
        activate_role(project, &role)?;
    }
    Ok(())
}

fn draft_role(project: &ProjectPaths, args: RoleDraftArgs) -> Result<()> {
    let role = selection::resolve_or_prompt_new_slug(args.role, "Role to draft")?;
    ensure_draft_role(project, &role)?;

    if let Some(request) = args.request.as_deref() {
        revise_role_draft(project, &role, "initial request", request)?;
    } else if should_use_interactive_review(args.no_review)
        && let Some(request) = read_multiline_request(PromptRequest::draft("Draft Role"))?
    {
        revise_role_draft(project, &role, "initial request", &request)?;
    }

    for feedback in &args.feedback {
        revise_role_draft(project, &role, "revision feedback", feedback)?;
    }

    if should_use_interactive_review(args.no_review) && !args.active {
        review_role_draft(project, &role)?;
        return Ok(());
    }

    if args.active {
        set_role_status(project, &role, RoleStatus::Active)?;
        activate_role(project, &role)?;
    }
    Ok(())
}

fn ensure_draft_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    if role_paths.root().exists() {
        let project_config = project_config(project)?;
        io::ensure_dir(&role_paths.steps_dir())?;
        io::ensure_dir(&role_paths.agents_dir())?;
        io::write_text_if_missing(
            &role_paths.role_md(),
            template::default_role_md(project_config.template),
        )?;
        let step = template::default_step_slug(project_config.template)?;
        io::write_text_if_missing(
            &role_paths.step_path(&step),
            template::default_step_md(project_config.template, &step),
        )?;
        if !role_paths.config().exists() {
            save_role_config(
                &role_paths,
                &RoleConfig {
                    version: ROLE_CONFIG_VERSION,
                    status: RoleStatus::Draft,
                    backend: project_config.default_backend,
                    mode: RoleMode::Repeatable,
                    parallel: template::default_parallel(project_config.template),
                    agent_names: template::default_agent_names(project_config.template),
                    agent_prefix: None,
                    auto_archive: false,
                    expose: Vec::new(),
                    steps: vec![step],
                    triggers: Vec::new(),
                },
            )?;
        }
        println!("role `{role}` ready for draft revision");
        return Ok(());
    }

    create_role(
        project,
        RoleNewArgs {
            role: Some(role.clone()),
            mode: RoleMode::Repeatable,
            parallel: None,
            expose: Vec::new(),
            steps: Vec::new(),
            agent_names: None,
            agent_prefix: None,
            auto_archive: false,
            active: false,
        },
    )
}

fn revise_role_draft(
    project: &ProjectPaths,
    role: &RoleSlug,
    label: &str,
    request: &str,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    append_draft_request(&role_paths, label, request)?;
    run_codex_role_draft(project, role, label, request)
}

fn append_draft_request(role_paths: &RolePaths, label: &str, request: &str) -> Result<()> {
    let path = role_paths.root().join("DRAFT_REQUEST.md");
    let mut text = io::read_optional_text(&path)?.unwrap_or_default();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    writeln!(text, "## {label} ({})\n", unix_timestamp())?;
    writeln!(text, "{}\n", request.trim())?;
    io::write_text(&path, &text)
}

fn should_use_interactive_review(no_review: bool) -> bool {
    !no_review && std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

fn editor_command() -> String {
    std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "nano".to_owned())
}

fn read_multiline_request(request: PromptRequest) -> Result<Option<String>> {
    let mut editor = PromptEditor::new(request.title);
    for line in request.help {
        editor = editor.help(line);
    }
    if let Some((title, text)) = request.context {
        editor = editor.context_text(title, &text);
    }
    editor = editor.history(request.history);
    editor.edit()
}

fn review_role_draft(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    loop {
        let choices = [
            "keep draft",
            "revise with Codex",
            "show generated files",
            "edit ROLE.md",
            "start role now",
            "delete draft",
        ];
        match ChoicePrompt::new(format!("Review draft role `{role}`"), choices)
            .default(0)
            .select()
            .context("Failed to read draft review selection")?
        {
            0 => return Ok(()),
            1 => {
                if let Some(feedback) =
                    read_multiline_request(PromptRequest::draft("Revise Draft Role"))?
                {
                    revise_role_draft(project, role, "revision feedback", &feedback)?;
                }
            }
            2 => show_draft_files(&RolePaths::new(project.clone(), role.clone()))?,
            3 => edit_role(project, role)?,
            4 => {
                set_role_status(project, role, RoleStatus::Active)?;
                activate_role(project, role)?;
                return Ok(());
            }
            5 => {
                delete_draft_role(project, role)?;
                return Ok(());
            }
            _ => unreachable!("choice prompt returned an invalid selection"),
        }
    }
}

fn show_draft_files(role_paths: &RolePaths) -> Result<()> {
    print_marked_file(&role_paths.role_md())?;
    print_marked_file(&role_paths.config())?;
    if role_paths.steps_dir().exists() {
        let mut steps = fs::read_dir(role_paths.steps_dir())
            .with_context(|| format!("Failed to read `{}`", role_paths.steps_dir().display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("Failed to read `{}`", role_paths.steps_dir().display()))?;
        steps.sort();
        for step in steps {
            if step.extension().and_then(|extension| extension.to_str()) == Some("md") {
                print_marked_file(&step)?;
            }
        }
    }
    Ok(())
}

fn print_marked_file(path: &Path) -> Result<()> {
    println!("\n--- {} ---\n{}", path.display(), io::read_text(path)?);
    Ok(())
}

fn delete_draft_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let agents = list_agents(&role_paths)?;
    if !agents.is_empty() {
        bail!("Refusing to delete role `{role}` because it already has agents.");
    }
    fs::remove_dir_all(role_paths.root())
        .with_context(|| format!("Failed to delete draft role `{role}`"))?;
    println!("deleted draft role `{role}`");
    Ok(())
}

fn run_codex_role_draft(
    project: &ProjectPaths,
    role: &RoleSlug,
    label: &str,
    request: &str,
) -> Result<()> {
    let project_config = project_config(project)?;
    let template = project_config
        .template
        .map(|template| template.to_string())
        .unwrap_or_else(|| "none".to_owned());
    let prompt = format!(
        "You are drafting or revising a think role under roles/{role}.\n\n\
         Read PROJECT.md, think.toml, and the existing files in roles/{role}/. Apply the {label} \
         below by editing only these role files:\n\
         - roles/{role}/ROLE.md\n\
         - roles/{role}/config.toml\n\
         - roles/{role}/steps/*.md\n\
         - roles/{role}/DRAFT_NOTES.md if useful\n\n\
         Do not edit PROJECT.md, channels/, runtime/, data/, or any agent directory. Do not start \
         any agents. Leave status = \"draft\" unless the request explicitly asks for a \
         different status.\n\n\
         The role config schema is:\n\
         version = {ROLE_CONFIG_VERSION}\n\
         status = \"draft\" | \"active\" | \"paused\"\n\
         backend = \"codex\"\n\
         mode = \"oneshot\" | \"repeatable\" | \"infinite\"\n\
         parallel = positive integer, or parallel = \"infinite\"\n\
         agent_names = \"sequential\" | \"random-8\" | \"adjective-noun\"\n\
         expose = [\"last-agent-finished\" | \"last-agent-started\"] # optional\n\
         steps = [step slugs whose files exist in steps/<slug>.md]\n\
         triggers are optional [[triggers]] tables. A triggered role declares when it should be \
         launched. Supported trigger tables are:\n\
         kind = \"role-step-finished\", role = \"source-role\", step = \"source-step\", \
         launch = \"async\"\n\
         or kind = \"role-agent-finished\", role = \"source-role\", launch = \"queued\", \
         queue = \"queue-name\"\n\
         or kind = \"elapsed\", role = \"source-role\", interval_seconds = 3600, \
         launch = \"queued\", queue = \"queue-name\"\n\n\
         Project template: {template}. For math-episodes episode roles, normally use \
         steps = [\"work\"], mode = \"repeatable\", parallel = \"infinite\", and a concise work \
         step that treats each agent as one episode, writes durable material in work/, and \
         publishes selected artifacts through channels/.\n\n\
         {label}:\n{request}\n"
    );
    run_codex_conversation(
        &project.root,
        "role-draft",
        prompt,
        CodexConversationPolicy::WorkspaceWrite,
    )
}

fn edit_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let editor = editor_command();
    let mut parts = shell_words::split(&editor).context("Failed to parse editor command")?;
    if parts.is_empty() {
        parts.push("nano".to_owned());
    }
    let program = parts.remove(0);
    let status = Command::new(program)
        .args(parts)
        .arg(role_paths.role_md())
        .status()
        .context("Failed to start editor")?;
    if status.success() {
        Ok(())
    } else {
        bail!("Editor exited with status {status}")
    }
}

fn set_role_status(project: &ProjectPaths, role: &RoleSlug, status: RoleStatus) -> Result<()> {
    set_role_status_inner(project, role, status)?;
    println!("role `{role}` is now {status}");
    Ok(())
}

fn set_role_status_inner(
    project: &ProjectPaths,
    role: &RoleSlug,
    status: RoleStatus,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    config.status = status;
    save_role_config(&role_paths, &config)?;
    if status == RoleStatus::Paused {
        pause_agents_for_role_pause(&role_paths)?;
        refresh_role_runtime_started(&role_paths)?;
    }
    Ok(())
}

fn pause_agents_for_role_pause(role_paths: &RolePaths) -> Result<()> {
    for agent in list_agents(role_paths)? {
        let agent_paths = role_paths.agent(agent.clone());
        let mut state = load_agent(&agent_paths)?;
        if state.archived
            || matches!(
                state.status,
                AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention
            )
        {
            continue;
        }
        state.status = AgentStatus::Paused;
        state.note = Some(role_pause_note(RoleStatus::Paused, state.paused_by_user));
        save_agent(&agent_paths, &mut state)?;
        terminate_live_agent_child(&agent_paths)?;
    }
    Ok(())
}

fn role_pause_note(status: RoleStatus, paused_by_user: bool) -> String {
    if paused_by_user {
        format!("role is {status}; agent is paused by user")
    } else {
        format!("role is {status}")
    }
}

fn activate_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    activate_role_inner(project, role, true)
}

fn activate_role_inner(project: &ProjectPaths, role: &RoleSlug, verbose: bool) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    validate_role_config(&config)?;
    config.status = RoleStatus::Active;
    save_role_config(&role_paths, &config)?;
    resume_paused_agents(&role_paths)?;

    if let Some(target) = config.parallel.target_count() {
        let active = active_agent_count(&role_paths)?;
        let missing = target.saturating_sub(active);
        if missing == 0 {
            if verbose {
                println!("role `{role}` already has {active}/{target} active agents");
            }
            return Ok(());
        }
        for _ in 0..missing {
            start_one_agent_inner(&role_paths, &config, None, None, verbose)?;
        }
    } else if verbose {
        println!("role `{role}` is active with infinite parallelism");
    }
    Ok(())
}

fn resume_paused_agents(role_paths: &RolePaths) -> Result<usize> {
    let mut host = None;
    let mut resumed = 0;
    for agent in list_agents(role_paths)? {
        let agent_paths = role_paths.agent(agent.clone());
        let mut state = load_agent(&agent_paths)?;
        if state.archived || state.status != AgentStatus::Paused || state.paused_by_user {
            continue;
        }
        let host = match &host {
            Some(host) => host,
            None => {
                host = Some(NativeSessionHost::new()?);
                host.as_ref()
                    .expect("native session host was just initialized")
            }
        };
        let pane = host.spawn_pane(PaneSpawnRequest {
            project: &role_paths.project,
            role: &role_paths.role,
            agent: &agent,
        })?;
        state.status = AgentStatus::Running;
        state.paused_by_user = false;
        state.note = Some("resumed after role unpause".to_owned());
        state.pane_id = Some(pane.pane_id);
        save_agent(&agent_paths, &mut state)?;
        resumed += 1;
    }
    if resumed > 0 {
        ensure_role_runtime_started(role_paths)?;
    }
    Ok(resumed)
}

fn toggle_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    match load_role_config(&role_paths)?.status {
        RoleStatus::Active => set_role_status_inner(project, role, RoleStatus::Paused),
        RoleStatus::Paused => activate_role_inner(project, role, false),
        RoleStatus::Draft => Ok(()),
    }
}

fn toggle_all_roles(project: &ProjectPaths) -> Result<()> {
    let roles = list_roles(project)?;
    let mut active = Vec::new();
    let mut paused = Vec::new();
    for role in roles {
        match load_role_config(&RolePaths::new(project.clone(), role.clone()))?.status {
            RoleStatus::Active => active.push(role),
            RoleStatus::Paused => paused.push(role),
            RoleStatus::Draft => {}
        }
    }
    if !active.is_empty() {
        if ConfirmPrompt::new(
            "Pause Roles",
            format!(
                "Pause {} active role{}?",
                active.len(),
                plural(active.len())
            ),
        )
        .default(false)
        .confirm()?
        {
            for role in active {
                set_role_status_inner(project, &role, RoleStatus::Paused)?;
            }
        }
        return Ok(());
    }
    if paused.is_empty() {
        return Ok(());
    }
    if ConfirmPrompt::new(
        "Unpause Roles",
        format!(
            "Unpause {} paused role{}?",
            paused.len(),
            plural(paused.len())
        ),
    )
    .default(false)
    .confirm()?
    {
        for role in paused {
            activate_role_inner(project, &role, false)?;
        }
    }
    Ok(())
}

fn plural(value: usize) -> &'static str {
    if value == 1 { "" } else { "s" }
}

fn start_one_agent(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
) -> Result<AgentId> {
    start_one_agent_inner(role_paths, config, custom_prompt, None, true)
}

fn start_one_agent_with_trigger(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
    trigger_cause: Option<&TriggerCause>,
) -> Result<AgentId> {
    start_one_agent_inner(role_paths, config, custom_prompt, trigger_cause, true)
}

fn start_one_agent_inner(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
    trigger_cause: Option<&TriggerCause>,
    verbose: bool,
) -> Result<AgentId> {
    let (agent, agent_paths) = create_agent(role_paths, config)?;
    if let Some(custom_prompt) = custom_prompt {
        io::write_text(&agent_paths.agent_prompt(), custom_prompt)?;
    }
    if let Some(trigger_cause) = trigger_cause {
        io::write_text(
            &agent_paths.trigger_context(),
            &trigger_cause.render(&role_paths.role),
        )?;
    }
    let pane = NativeSessionHost::new()?.spawn_pane(PaneSpawnRequest {
        project: &role_paths.project,
        role: &role_paths.role,
        agent: &agent,
    })?;
    let mut state = load_agent(&agent_paths)?;
    state.pane_id = Some(pane.pane_id);
    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;
    if verbose {
        println!("started agent `{}` for role `{}`", agent, role_paths.role);
    }
    Ok(agent)
}

fn start_one_agent_sync_with_trigger(
    role_paths: &RolePaths,
    config: &RoleConfig,
    trigger_cause: Option<&TriggerCause>,
) -> Result<AgentId> {
    let (agent, agent_paths) = create_agent(role_paths, config)?;
    if let Some(trigger_cause) = trigger_cause {
        io::write_text(
            &agent_paths.trigger_context(),
            &trigger_cause.render(&role_paths.role),
        )?;
    }
    let mut state = load_agent(&agent_paths)?;
    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.note = Some("started by queued trigger".to_owned());
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;
    run_orchestrator(RunOrchestratorArgs {
        project: role_paths.project.root.clone(),
        role: role_paths.role.clone(),
        agent: agent.clone(),
    })?;
    Ok(agent)
}

fn create_agent(
    role_paths: &RolePaths,
    config: &RoleConfig,
) -> Result<(AgentId, crate::state::AgentPaths)> {
    let _lock = lock::FileLock::acquire(
        role_paths.project.agent_lock_path(),
        "agent allocation lock",
    )?;
    let agent = allocate_agent_id(
        role_paths,
        config.agent_names,
        config.agent_prefix.as_deref(),
    )?;
    let agent_paths = role_paths.agent(agent.clone());
    prepare_agent_work(&agent_paths)?;
    io::ensure_dir(&agent_paths.channels_dir())?;
    io::ensure_dir(&agent_paths.runs_dir())?;
    prepare_agent_data(&agent_paths)?;
    io::write_toml(&agent_paths.manifest(), &AgentManifest::default())?;

    let channels = project_config(&role_paths.project)?.channels;
    for channel in &channels {
        git::init_channel(&role_paths.project.channel_dir(channel))?;
        io::ensure_dir(&agent_paths.channel_dir(channel))?;
    }

    let mut state = AgentState::new(
        role_paths.role.clone(),
        agent.clone(),
        config.backend,
        config.mode,
        channels,
    );
    save_agent(&agent_paths, &mut state)?;
    write_exposure_context(&role_paths.project, &agent_paths, config)?;
    Ok((agent, agent_paths))
}

fn write_exposure_context(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    config: &RoleConfig,
) -> Result<()> {
    if config.expose.is_empty() {
        return Ok(());
    }
    let mut text = String::new();
    for exposed in &config.expose {
        match exposed {
            ExposedContext::LastAgentFinished => {
                writeln!(text, "## last-agent-finished")?;
                match last_role_agent_by(project, agent_paths, LastAgentSelector::Finished)? {
                    Some(summary) => write_agent_exposure_summary(&mut text, &summary)?,
                    None => writeln!(
                        text,
                        "No previous finished agent exists for role `{}`.",
                        agent_paths.role.role
                    )?,
                }
            }
            ExposedContext::LastAgentStarted => {
                writeln!(text, "## last-agent-started")?;
                match last_role_agent_by(project, agent_paths, LastAgentSelector::Started)? {
                    Some(summary) => write_agent_exposure_summary(&mut text, &summary)?,
                    None => writeln!(
                        text,
                        "No previous started agent exists for role `{}`.",
                        agent_paths.role.role
                    )?,
                }
            }
        }
        writeln!(text)?;
    }
    io::write_text(&agent_paths.exposure_context(), text.trim_end())?;
    Ok(())
}

enum LastAgentSelector {
    Finished,
    Started,
}

struct AgentExposureSummary {
    role: RoleSlug,
    agent: AgentId,
    status: AgentStatus,
    run_count: u64,
    timestamp: u64,
    work_dir: PathBuf,
    root: PathBuf,
}

fn last_role_agent_by(
    project: &ProjectPaths,
    current: &crate::state::AgentPaths,
    selector: LastAgentSelector,
) -> Result<Option<AgentExposureSummary>> {
    let mut best = None;
    for agent in list_agents(&current.role)? {
        if agent == current.agent {
            continue;
        }
        let paths = current.role.agent(agent.clone());
        let state = load_agent(&paths)?;
        let timestamp = match selector {
            LastAgentSelector::Started => state.created_at,
            LastAgentSelector::Finished => {
                let Some(exit) = state.last_exit.as_ref().filter(|exit| exit.success) else {
                    continue;
                };
                if state.status != AgentStatus::Done {
                    continue;
                }
                exit.finished_at
            }
        };
        let summary = AgentExposureSummary {
            role: current.role.role.clone(),
            agent,
            status: state.status,
            run_count: state.run_count,
            timestamp,
            work_dir: paths.work_own(),
            root: paths.root(),
        };
        if best
            .as_ref()
            .is_none_or(|best: &AgentExposureSummary| summary.timestamp > best.timestamp)
        {
            best = Some(summary);
        }
    }
    let _ = project;
    Ok(best)
}

fn write_agent_exposure_summary(
    text: &mut String,
    summary: &AgentExposureSummary,
) -> std::fmt::Result {
    writeln!(text, "- role: `{}`", summary.role)?;
    writeln!(text, "- agent: `{}`", summary.agent)?;
    writeln!(text, "- status: `{}`", summary.status)?;
    writeln!(text, "- run count: `{}`", summary.run_count)?;
    writeln!(text, "- timestamp: `{}`", summary.timestamp)?;
    writeln!(text, "- agent root: `{}`", summary.root.display())?;
    writeln!(
        text,
        "- work/own directory: `{}`",
        summary.work_dir.display()
    )
}

fn stop_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to stop")?;
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    state.status = AgentStatus::Stopped;
    state.paused_by_user = false;
    state.note = Some("stopped by user".to_owned());
    save_agent(&paths, &mut state)?;
    terminate_live_agent_child(&paths)?;
    refresh_role_runtime_started(&role_paths)?;
    println!("agent `{}` marked stopped", resolved.label());
    Ok(())
}

fn pause_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to pause")?;
    pause_agent_inner(project, &resolved, true)
}

fn pause_agent_inner(
    project: &ProjectPaths,
    resolved: &ResolvedAgent,
    verbose: bool,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    if matches!(
        state.status,
        AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention
    ) {
        bail!(
            "Agent `{}` is {}; there is no active execution to pause.",
            resolved.label(),
            state.status
        );
    }
    state.status = AgentStatus::Paused;
    state.paused_by_user = true;
    state.note = Some("agent paused by user".to_owned());
    save_agent(&paths, &mut state)?;
    let terminated = terminate_live_agent_child(&paths)?;
    refresh_role_runtime_started(&role_paths)?;
    if verbose {
        if terminated {
            println!(
                "agent `{}` paused; live child was signalled",
                resolved.label()
            );
        } else {
            println!("agent `{}` paused", resolved.label());
        }
    }
    Ok(())
}

fn archive_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to archive")?;
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        bail!(
            "Refusing to archive active agent `{}`. Stop it or wait until it is done first.",
            resolved.label()
        );
    }
    state.archived = true;
    state.paused_by_user = false;
    state.note = Some("archived by user".to_owned());
    save_agent(&paths, &mut state)?;
    refresh_role_runtime_started(&role_paths)?;
    println!(
        "agent `{}/{}` archived; files remain at {}",
        resolved.role,
        resolved.agent,
        paths.root().display()
    );
    Ok(())
}

fn resume_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to resume")?;
    resume_agent_inner(project, &resolved, true)
}

fn resume_agent_inner(
    project: &ProjectPaths,
    resolved: &ResolvedAgent,
    verbose: bool,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let config = load_role_config(&role_paths)?;
    let agent_paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&agent_paths)?;
    state.paused_by_user = false;
    state.archived = false;
    if config.status != RoleStatus::Active {
        state.status = AgentStatus::Paused;
        state.note = Some(role_pause_note(config.status, false));
        save_agent(&agent_paths, &mut state)?;
        refresh_role_runtime_started(&role_paths)?;
        if verbose {
            println!(
                "agent `{}` unpaused but role `{}` is {}",
                resolved.label(),
                resolved.role,
                config.status
            );
        }
        return Ok(());
    }
    state.status = AgentStatus::Running;
    state.note = None;
    let pane = NativeSessionHost::new()?.spawn_pane(PaneSpawnRequest {
        project,
        role: &resolved.role,
        agent: &resolved.agent,
    })?;
    state.pane_id = Some(pane.pane_id);
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(&role_paths)?;
    if verbose {
        println!("resumed agent `{}`", resolved.label());
    }
    Ok(())
}

fn toggle_agent(project: &ProjectPaths, resolved: &ResolvedAgent) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let state = load_agent(&role_paths.agent(resolved.agent.clone()))?;
    if state.paused_by_user {
        resume_agent_inner(project, resolved, false)
    } else {
        pause_agent_inner(project, resolved, false)
    }
}

fn terminate_live_agent_child(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let supervisor = load_supervisor_state(agent_paths)?;
    if supervisor.status != SupervisorStatus::Running {
        return Ok(false);
    }
    let Some(pid) = supervisor.child_pid else {
        return Ok(false);
    };
    terminate_process(pid)
}

fn terminate_process(pid: u32) -> Result<bool> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if result == 0 {
            return Ok(true);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(err).with_context(|| format!("Failed to signal child process {pid}"))
    }

    #[cfg(not(unix))]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status()
            .with_context(|| format!("Failed to signal child process {pid}"))?;
        Ok(status.success())
    }
}

fn run_orchestrator(args: RunOrchestratorArgs) -> Result<()> {
    let project = ProjectPaths::new(
        args.project
            .canonicalize()
            .context("Invalid project path")?,
    );
    let role_paths = RolePaths::new(project.clone(), args.role.clone());
    let agent_paths = role_paths.agent(args.agent.clone());
    let Some(_orchestrator_lock) = lock::FileLock::try_acquire(
        orchestrator_lock_path(&project, &args.role, &args.agent),
        "runtime orchestrator lock",
    )?
    else {
        return Ok(());
    };
    loop {
        let config = load_role_config(&role_paths)?;
        if config.status != RoleStatus::Active {
            let mut state = load_agent(&agent_paths)?;
            state.status = AgentStatus::Paused;
            state.note = Some(role_pause_note(config.status, state.paused_by_user));
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        let mut state = load_agent(&agent_paths)?;
        if state.archived {
            break;
        }
        if state.paused_by_user || state.status == AgentStatus::Paused {
            state.status = AgentStatus::Paused;
            state.note = Some(if state.paused_by_user {
                "agent paused by user".to_owned()
            } else {
                "agent paused".to_owned()
            });
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        if matches!(
            state.status,
            AgentStatus::Stopped | AgentStatus::NeedsAttention
        ) {
            break;
        }
        if config.steps.is_empty() {
            state.status = AgentStatus::NeedsAttention;
            state.note = Some("role has no steps".to_owned());
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        match run_agent_step(&project, &role_paths, &agent_paths, &config, state) {
            Ok(ContinueDecision::Continue) => continue,
            Ok(ContinueDecision::Stop) => break,
            Err(err) => {
                let mut state = load_agent(&agent_paths)?;
                state.status = AgentStatus::NeedsAttention;
                state.note = Some(err.to_string());
                save_agent(&agent_paths, &mut state)?;
                refresh_role_runtime_started(&role_paths)?;
                return Err(err);
            }
        }
    }
    println!(
        "\nthink agent `{}/{}` is done. Attach later with `think agent attach {}/{}`.",
        args.role, args.agent, args.role, args.agent
    );
    Ok(())
}

enum ContinueDecision {
    Continue,
    Stop,
}

enum SupervisedCommandOutcome {
    Exit(runner::PtyExit),
    Stopped,
}

struct SupervisedCommandResult {
    outcome: SupervisedCommandOutcome,
    resume_session: Option<String>,
}

struct FinalizationInput {
    exit: runner::PtyExit,
    disposition: Option<Disposition>,
    state: AgentState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentRepairKind {
    AgentState,
    ChannelOutbox,
    Manifest,
    Reply,
}

impl std::fmt::Display for AgentRepairKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AgentState => formatter.write_str("agent state"),
            Self::ChannelOutbox => formatter.write_str("channel outbox"),
            Self::Manifest => formatter.write_str("manifest"),
            Self::Reply => formatter.write_str("reply"),
        }
    }
}

#[derive(Debug)]
struct AgentRepairError {
    kind: AgentRepairKind,
    message: String,
}

impl std::fmt::Display for AgentRepairError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentRepairError {}

fn run_agent_step(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    config: &RoleConfig,
    mut state: AgentState,
) -> Result<ContinueDecision> {
    let run_id = state.run_count + 1;
    let step = config.steps[state.current_step % config.steps.len()].clone();
    let run_paths = agent_paths.run(run_id);
    prepare_agent_work(agent_paths)?;
    prepare_agent_data(agent_paths)?;
    io::ensure_dir(&run_paths.root())?;
    prepare_agent_manifest(agent_paths)?;
    io::write_text(
        &run_paths.step(),
        &io::read_text(&role_paths.step_path(&step))?,
    )?;
    let assembled = prompt::assemble(project, role_paths, config, &state, &step)?;
    io::write_text(&agent_paths.prompt(), &assembled)?;
    io::write_text(&run_paths.prompt(), &assembled)?;

    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.note = None;
    save_agent(agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;

    let started_at = unix_timestamp();
    let elapsed_triggers_done = Arc::new(AtomicBool::new(false));
    let elapsed_trigger_thread = start_elapsed_trigger_thread(
        project.clone(),
        role_paths.role.clone(),
        elapsed_triggers_done.clone(),
    )?;
    let agent = CodexAgent::new(project_config(project)?.providers.codex);
    println!(
        "starting {} run {} for `{}/{}`",
        agent.name(),
        run_id,
        role_paths.role,
        agent_paths.agent
    );
    let mut repair_attempts = 0;
    let mut resume_session = None;
    let mut restart_notice = None;
    let step_result: Result<Option<FinalizationInput>> = loop {
        let result = run_supervised_agent_command(
            &agent,
            role_paths,
            agent_paths,
            &run_paths,
            resume_session.take(),
            restart_notice.take(),
        );
        let result = match result {
            Ok(result) => result,
            Err(err) => break Err(err),
        };
        resume_session = result.resume_session;
        let SupervisedCommandOutcome::Exit(exit) = result.outcome else {
            break Ok(None);
        };
        if !exit.success {
            match validate_agent_state_after_run(agent_paths, &state) {
                Ok(state) => {
                    break Ok(Some(FinalizationInput {
                        exit,
                        disposition: None,
                        state,
                    }));
                }
                Err(err) => break Err(err),
            }
        }
        match validate_agent_finalization(
            project,
            agent_paths,
            config.mode,
            &state,
            run_id,
            &run_paths,
        ) {
            Ok((state, disposition)) => {
                break Ok(Some(FinalizationInput {
                    exit,
                    disposition,
                    state,
                }));
            }
            Err(err)
                if is_agent_repair_error(&err) && repair_attempts < MAX_AGENT_REPAIR_RETRIES =>
            {
                repair_attempts += 1;
                let retry_at = unix_timestamp() + agent_repair_retry_delay_seconds();
                let event = agent_repair_event(&err, repair_attempts, retry_at);
                let kind = agent_repair_kind(&err).expect("repair errors have a kind");
                if let Err(err) = record_agent_repair_retry(
                    agent_paths,
                    run_id,
                    retry_at,
                    resume_session.clone(),
                    &event,
                    &state,
                    kind,
                ) {
                    break Err(err);
                }
                match wait_until_retry(agent_paths, role_paths, retry_at) {
                    Ok(true) => {}
                    Ok(false) => break Ok(None),
                    Err(err) => break Err(err),
                }
                restart_notice = Some(agent_repair_notice(
                    &err,
                    kind,
                    config.mode,
                    repair_attempts,
                ));
            }
            Err(err) => {
                let event = if is_agent_repair_error(&err) {
                    agent_repair_exhausted_event(&err)
                } else {
                    err.to_string()
                };
                if let Err(err) = save_supervisor_needs_attention(
                    agent_paths,
                    run_id,
                    resume_session.clone(),
                    &event,
                ) {
                    break Err(err);
                }
                if let Err(err) = update_agent_note(agent_paths, &event) {
                    break Err(err);
                }
                break Err(err);
            }
        }
    };
    elapsed_triggers_done.store(true, Ordering::Relaxed);
    if let Some(handle) = elapsed_trigger_thread {
        handle
            .join()
            .unwrap_or_else(|_| Err(anyhow!("elapsed trigger thread panicked")))?;
    }
    let Some(finalization) = step_result? else {
        return Ok(ContinueDecision::Stop);
    };
    let FinalizationInput {
        exit,
        disposition,
        mut state,
    } = finalization;
    let finished_at = unix_timestamp();
    if !exit.success {
        let exit = run_exit_from_pty(
            run_id,
            step.clone(),
            started_at,
            finished_at,
            exit,
            None,
            None,
        );
        io::write_toml(&run_paths.exit(), &exit)?;
        bail!("agent exited unsuccessfully with status code {}", exit.code);
    }

    let pause_requested = state.paused_by_user || state.status == AgentStatus::Paused;
    let pause_note = state.note.clone().unwrap_or_else(|| {
        if state.paused_by_user {
            "agent paused by user".to_owned()
        } else {
            "agent paused".to_owned()
        }
    });
    finalize_channels(project, agent_paths, &state, run_id)?;
    state.run_count = run_id;
    state.note = None;
    let exit = run_exit_from_pty(
        run_id,
        step.clone(),
        started_at,
        finished_at,
        exit,
        disposition,
        None,
    );
    io::write_toml(&run_paths.exit(), &exit)?;
    state.last_exit = Some(exit);

    let decision = match config.mode {
        RoleMode::Oneshot => {
            state.status = AgentStatus::Done;
            state.paused_by_user = false;
            ContinueDecision::Stop
        }
        RoleMode::Repeatable => match disposition {
            Some(Disposition::Continue) => {
                state.current_step = (state.current_step + 1) % config.steps.len();
                if pause_requested {
                    state.status = AgentStatus::Paused;
                    state.note = Some(pause_note.clone());
                    ContinueDecision::Stop
                } else {
                    state.status = AgentStatus::Running;
                    ContinueDecision::Continue
                }
            }
            Some(Disposition::Stop) => {
                state.status = AgentStatus::Done;
                state.paused_by_user = false;
                ContinueDecision::Stop
            }
            None => unreachable!("repeatable disposition was validated above"),
        },
        RoleMode::Infinite => {
            state.current_step = (state.current_step + 1) % config.steps.len();
            if pause_requested {
                state.status = AgentStatus::Paused;
                state.note = Some(pause_note);
                ContinueDecision::Stop
            } else {
                state.status = AgentStatus::Running;
                ContinueDecision::Continue
            }
        }
    };
    if config.auto_archive && matches!(decision, ContinueDecision::Stop) {
        state.archived = true;
    }
    save_agent(agent_paths, &mut state)?;
    if matches!(decision, ContinueDecision::Stop) {
        refresh_role_runtime_started(role_paths)?;
    }
    fire_role_step_finished_triggers(project, &role_paths.role, &step)?;
    if state.status == AgentStatus::Done {
        fire_role_agent_finished_triggers(project, &role_paths.role, &state.agent, run_id, &step)?;
    }
    Ok(decision)
}

fn run_supervised_agent_command<A: AgentBackend + ?Sized>(
    agent: &A,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    run_paths: &crate::state::RunPaths,
    initial_resume_session: Option<String>,
    initial_restart_notice: Option<String>,
) -> Result<SupervisedCommandResult> {
    let mut oom_restarts = 0;
    let mut quota_retries = 0;
    let mut resume_session = initial_resume_session;
    let mut restart_notice = initial_restart_notice;

    loop {
        let mut supervisor = load_supervisor_state(agent_paths)?;
        supervisor.status = SupervisorStatus::Running;
        supervisor.last_run_id = Some(run_paths.run_id);
        supervisor.child_pid = None;
        supervisor.next_retry_at = None;
        supervisor.last_event = restart_notice.clone();
        supervisor.last_session_id = resume_session.clone();
        save_supervisor_state(agent_paths, &supervisor)?;

        let command = match agent.command(AgentCommandRequest {
            agent_dir: &agent_paths.root(),
            reply_path: &run_paths.reply(),
            resume_session: resume_session.as_deref(),
            restart_notice: restart_notice.as_deref(),
        }) {
            Ok(command) => command,
            Err(err) if lock::is_lock_busy_error(&err) => {
                let retry_at = unix_timestamp() + provider_prep_retry_delay_seconds();
                let event = format!(
                    "provider state is busy while preparing the agent command; retrying at {}",
                    format_unix_time(retry_at)
                );
                save_supervisor_wait(
                    agent_paths,
                    SupervisorStatus::WaitingForProvider,
                    run_paths.run_id,
                    None,
                    retry_at,
                    resume_session.clone(),
                    &event,
                )?;
                update_agent_note(agent_paths, &event)?;
                if !wait_until_retry(agent_paths, role_paths, retry_at)? {
                    return Ok(SupervisedCommandResult {
                        outcome: SupervisedCommandOutcome::Stopped,
                        resume_session,
                    });
                }
                restart_notice = Some(
                    concat!(
                        "The previous attempt could not prepare the provider invocation because ",
                        "provider state was busy. The think runtime waited and is retrying now."
                    )
                    .to_owned(),
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        let provider_invocation = command.provider.clone();
        let exit = runner::run_command_with_spawn_callback(command, run_paths, |child_pid| {
            let mut supervisor = load_supervisor_state(agent_paths)?;
            supervisor.status = SupervisorStatus::Running;
            supervisor.last_run_id = Some(run_paths.run_id);
            supervisor.child_pid = child_pid;
            supervisor.next_retry_at = None;
            save_supervisor_state(agent_paths, &supervisor)
        });
        let transcript = io::read_optional_text(&run_paths.transcript_text())?.unwrap_or_default();
        resume_session = extract_latest_codex_session_id(&transcript).or(resume_session);
        match exit {
            Ok(exit) if exit.success => {
                let mut supervisor = load_supervisor_state(agent_paths)?;
                supervisor.status = SupervisorStatus::Idle;
                supervisor.last_run_id = Some(run_paths.run_id);
                supervisor.child_pid = None;
                supervisor.next_retry_at = None;
                supervisor.last_session_id = resume_session.clone();
                supervisor.last_event = Some("child exited successfully".to_owned());
                save_supervisor_state(agent_paths, &supervisor)?;
                return Ok(SupervisedCommandResult {
                    outcome: SupervisedCommandOutcome::Exit(exit),
                    resume_session,
                });
            }
            Ok(_exit) if agent_execution_was_paused(agent_paths)? => {
                let mut supervisor = load_supervisor_state(agent_paths)?;
                supervisor.status = SupervisorStatus::Idle;
                supervisor.last_run_id = Some(run_paths.run_id);
                supervisor.child_pid = None;
                supervisor.next_retry_at = None;
                supervisor.last_session_id = resume_session.clone();
                supervisor.last_event = Some("child stopped after agent pause".to_owned());
                save_supervisor_state(agent_paths, &supervisor)?;
                return Ok(SupervisedCommandResult {
                    outcome: SupervisedCommandOutcome::Stopped,
                    resume_session,
                });
            }
            Ok(exit) => {
                if let Some(decision) = agent.quota_decision(
                    provider_invocation.as_ref(),
                    &transcript,
                    quota_retries + 1,
                )? {
                    quota_retries += 1;
                    let previous_account = provider_invocation
                        .as_ref()
                        .and_then(|invocation| invocation.account.as_deref());
                    match decision {
                        QuotaDecision::RetryNow { account } => {
                            let event = format!(
                                "Codex quota or rate limit reached; switched to account `{account}`"
                            );
                            update_agent_note(agent_paths, &event)?;
                            restart_notice = Some(event);
                            if previous_account != Some(account.as_str()) {
                                resume_session = None;
                            }
                        }
                        QuotaDecision::Wait { account, retry_at } => {
                            let event = format!(
                                "all Codex accounts are quota limited; next account `{account}` retries at {}",
                                format_unix_time(retry_at)
                            );
                            save_supervisor_wait(
                                agent_paths,
                                SupervisorStatus::WaitingForQuota,
                                run_paths.run_id,
                                None,
                                retry_at,
                                resume_session.clone(),
                                &event,
                            )?;
                            update_agent_note(agent_paths, &event)?;
                            if !wait_until_retry(agent_paths, role_paths, retry_at)? {
                                return Ok(SupervisedCommandResult {
                                    outcome: SupervisedCommandOutcome::Stopped,
                                    resume_session,
                                });
                            }
                            restart_notice = Some(format!(
                                concat!(
                                    "The previous attempt hit a Codex quota or rate limit. The think runtime ",
                                    "orchestrator waited until {} and is now resuming the run with account `{}`."
                                ),
                                format_unix_time(retry_at),
                                account
                            ));
                            if previous_account != Some(account.as_str()) {
                                resume_session = None;
                            }
                        }
                    }
                } else if exit_looks_like_oom(&exit) && oom_restarts < MAX_OOM_RESTARTS {
                    oom_restarts += 1;
                    let retry_at = unix_timestamp() + oom_restart_delay_seconds();
                    let event = format!(
                        "child was killed by SIGKILL/137, probably by OOM; restart {oom_restarts}/{MAX_OOM_RESTARTS} at {}",
                        format_unix_time(retry_at)
                    );
                    save_supervisor_wait(
                        agent_paths,
                        SupervisorStatus::Restarting,
                        run_paths.run_id,
                        None,
                        retry_at,
                        resume_session.clone(),
                        &event,
                    )?;
                    update_agent_note(agent_paths, &event)?;
                    if !wait_until_retry(agent_paths, role_paths, retry_at)? {
                        return Ok(SupervisedCommandResult {
                            outcome: SupervisedCommandOutcome::Stopped,
                            resume_session,
                        });
                    }
                    restart_notice = Some(format!(
                        "The previous Codex child was killed by SIGKILL/137, likely due to memory pressure. Avoid repeating the memory-heavy operation; use smaller batches, streaming, checkpoints, or external artifacts in data/own/."
                    ));
                } else {
                    let mut supervisor = load_supervisor_state(agent_paths)?;
                    supervisor.status = SupervisorStatus::NeedsAttention;
                    supervisor.last_run_id = Some(run_paths.run_id);
                    supervisor.child_pid = None;
                    supervisor.next_retry_at = None;
                    supervisor.last_session_id = resume_session.clone();
                    supervisor.last_event = Some(format!(
                        "child exited unsuccessfully with status code {}",
                        exit.code
                    ));
                    save_supervisor_state(agent_paths, &supervisor)?;
                    return Ok(SupervisedCommandResult {
                        outcome: SupervisedCommandOutcome::Exit(exit),
                        resume_session,
                    });
                }
            }
            Err(err) => {
                if agent_execution_was_paused(agent_paths)? {
                    let mut supervisor = load_supervisor_state(agent_paths)?;
                    supervisor.status = SupervisorStatus::Idle;
                    supervisor.last_run_id = Some(run_paths.run_id);
                    supervisor.child_pid = None;
                    supervisor.next_retry_at = None;
                    supervisor.last_session_id = resume_session.clone();
                    supervisor.last_event = Some("child stopped after agent pause".to_owned());
                    save_supervisor_state(agent_paths, &supervisor)?;
                    return Ok(SupervisedCommandResult {
                        outcome: SupervisedCommandOutcome::Stopped,
                        resume_session,
                    });
                }
                let mut supervisor = load_supervisor_state(agent_paths)?;
                supervisor.status = SupervisorStatus::NeedsAttention;
                supervisor.last_run_id = Some(run_paths.run_id);
                supervisor.next_retry_at = None;
                supervisor.last_session_id = resume_session.clone();
                supervisor.last_event = Some(err.to_string());
                save_supervisor_state(agent_paths, &supervisor)?;
                return Err(err);
            }
        }
    }
}

fn supervisor_path(agent_paths: &crate::state::AgentPaths) -> PathBuf {
    agent_paths.root().join("orchestrator.toml")
}

fn agent_execution_was_paused(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let state = load_agent(agent_paths)?;
    Ok(state.archived || state.paused_by_user || state.status == AgentStatus::Paused)
}

fn orchestrator_lock_path(project: &ProjectPaths, role: &RoleSlug, agent: &AgentId) -> PathBuf {
    project
        .runtime_dir()
        .join("locks")
        .join("orchestrators")
        .join(role.as_str())
        .join(format!("{}.lock", agent.as_str()))
}

fn load_supervisor_state(agent_paths: &crate::state::AgentPaths) -> Result<SupervisorState> {
    let path = supervisor_path(agent_paths);
    if path.exists() {
        io::read_toml(&path)
    } else {
        Ok(SupervisorState::default())
    }
}

fn save_supervisor_state(
    agent_paths: &crate::state::AgentPaths,
    state: &SupervisorState,
) -> Result<()> {
    let mut state = state.clone();
    state.version = SUPERVISOR_STATE_VERSION;
    state.updated_at = unix_timestamp();
    io::write_toml(&supervisor_path(agent_paths), &state)
}

fn save_supervisor_wait(
    agent_paths: &crate::state::AgentPaths,
    status: SupervisorStatus,
    run_id: u64,
    child_pid: Option<u32>,
    retry_at: u64,
    session_id: Option<String>,
    event: &str,
) -> Result<()> {
    let mut state = load_supervisor_state(agent_paths)?;
    state.status = status;
    state.last_run_id = Some(run_id);
    state.child_pid = child_pid;
    state.next_retry_at = Some(retry_at);
    state.last_session_id = session_id;
    state.last_event = Some(event.to_owned());
    match status {
        SupervisorStatus::Restarting => state.oom_restarts += 1,
        SupervisorStatus::WaitingForQuota => state.quota_retries += 1,
        SupervisorStatus::WaitingForProvider => state.provider_retries += 1,
        SupervisorStatus::Idle | SupervisorStatus::Running | SupervisorStatus::NeedsAttention => {}
    }
    save_supervisor_state(agent_paths, &state)
}

fn save_supervisor_repair_retry(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    retry_at: u64,
    session_id: Option<String>,
    event: &str,
) -> Result<()> {
    let mut state = load_supervisor_state(agent_paths)?;
    state.status = SupervisorStatus::Restarting;
    state.last_run_id = Some(run_id);
    state.child_pid = None;
    state.next_retry_at = Some(retry_at);
    state.last_session_id = session_id;
    state.last_event = Some(event.to_owned());
    state.repair_retries += 1;
    save_supervisor_state(agent_paths, &state)
}

fn save_supervisor_needs_attention(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    session_id: Option<String>,
    event: &str,
) -> Result<()> {
    let mut state = load_supervisor_state(agent_paths)?;
    state.status = SupervisorStatus::NeedsAttention;
    state.last_run_id = Some(run_id);
    state.child_pid = None;
    state.next_retry_at = None;
    state.last_session_id = session_id;
    state.last_event = Some(event.to_owned());
    save_supervisor_state(agent_paths, &state)
}

fn update_agent_note(agent_paths: &crate::state::AgentPaths, note: &str) -> Result<()> {
    let mut state = load_agent(agent_paths)?;
    state.note = Some(note.to_owned());
    save_agent(agent_paths, &mut state)
}

fn record_agent_repair_retry(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    retry_at: u64,
    session_id: Option<String>,
    event: &str,
    expected_state: &AgentState,
    kind: AgentRepairKind,
) -> Result<()> {
    save_supervisor_repair_retry(agent_paths, run_id, retry_at, session_id, event)?;
    if update_agent_note(agent_paths, event).is_ok() {
        return Ok(());
    }
    if kind != AgentRepairKind::AgentState {
        return update_agent_note(agent_paths, event);
    }
    let mut restored = expected_state.clone();
    restored.status = AgentStatus::Running;
    restored.paused_by_user = false;
    restored.note = Some(event.to_owned());
    save_agent(agent_paths, &mut restored)
}

fn wait_until_retry(
    agent_paths: &crate::state::AgentPaths,
    role_paths: &RolePaths,
    retry_at: u64,
) -> Result<bool> {
    let wait_started_at = unix_timestamp();
    loop {
        let state = load_agent(agent_paths)?;
        if state.archived
            || matches!(
                state.status,
                AgentStatus::Stopped | AgentStatus::Paused | AgentStatus::Done
            )
        {
            return Ok(false);
        }
        if load_role_config(role_paths)?.status != RoleStatus::Active {
            return Ok(false);
        }
        let now = unix_timestamp();
        if now >= retry_at {
            return Ok(true);
        }
        let supervisor = load_supervisor_state(agent_paths)?;
        if supervisor
            .next_retry_at
            .is_some_and(|timestamp| timestamp <= now)
            || retry_requested_after(&role_paths.project, wait_started_at)?
        {
            return Ok(true);
        }
        thread::sleep(Duration::from_secs(
            (retry_at - now).clamp(RETRY_POLL_MIN_SECONDS, RETRY_POLL_MAX_SECONDS),
        ));
    }
}

fn retry_requested_after(project: &ProjectPaths, timestamp: u64) -> Result<bool> {
    let path = retry_request_path(project);
    if !path.exists() {
        return Ok(false);
    }
    Ok(io::read_toml::<RetryRequestState>(&path)?.requested_at > timestamp)
}

fn retry_request_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("retry.toml")
}

fn exit_looks_like_oom(exit: &runner::PtyExit) -> bool {
    exit.code == 137
        || exit
            .signal
            .as_deref()
            .is_some_and(|signal| matches!(signal, "KILL" | "SIGKILL" | "9"))
}

fn oom_restart_delay_seconds() -> u64 {
    if let Ok(value) = std::env::var("THINK_ORCHESTRATOR_OOM_RESTART_DELAY_SECONDS")
        && let Ok(value) = value.parse::<u64>()
    {
        return value.max(MIN_RETRY_DELAY_SECONDS);
    }
    DEFAULT_OOM_RESTART_DELAY_SECONDS
}

fn provider_prep_retry_delay_seconds() -> u64 {
    if let Ok(value) = std::env::var("THINK_ORCHESTRATOR_PROVIDER_PREP_RETRY_SECONDS")
        && let Ok(value) = value.parse::<u64>()
    {
        return value.max(MIN_RETRY_DELAY_SECONDS);
    }
    DEFAULT_PROVIDER_PREP_RETRY_SECONDS
}

fn agent_repair_retry_delay_seconds() -> u64 {
    if let Ok(value) = std::env::var("THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS")
        && let Ok(value) = value.parse::<u64>()
    {
        return value.max(MIN_RETRY_DELAY_SECONDS);
    }
    DEFAULT_AGENT_REPAIR_RETRY_SECONDS
}

fn format_unix_time(timestamp: u64) -> String {
    let Ok(second) = i64::try_from(timestamp) else {
        return timestamp.to_string();
    };
    Timestamp::from_second(second)
        .map(|timestamp| {
            timestamp
                .to_zoned(TimeZone::system())
                .strftime("%F %T %Z")
                .to_string()
        })
        .unwrap_or_else(|_| timestamp.to_string())
}

fn format_unix_time_compact(timestamp: u64) -> String {
    let Ok(second) = i64::try_from(timestamp) else {
        return timestamp.to_string();
    };
    Timestamp::from_second(second)
        .map(|timestamp| {
            timestamp
                .to_zoned(TimeZone::system())
                .strftime("%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|_| timestamp.to_string())
}

fn finalize_channels(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
) -> Result<()> {
    for channel in &state.channels {
        publish_channel_outbox(project, agent_paths, state, run_id, channel)?;
    }
    Ok(())
}

fn validate_channel_outboxes(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
) -> Result<()> {
    for channel in &state.channels {
        let outbox = agent_paths.channel_dir(channel);
        if !outbox.exists() || directory_is_empty(&outbox)? {
            continue;
        }
        let channel_dir = project.channel_dir(channel);
        for entry in fs::read_dir(&outbox)
            .with_context(|| format!("Failed to read channel outbox `{}`", outbox.display()))?
        {
            let entry = entry.with_context(|| format!("Failed to read `{}`", outbox.display()))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(agent_repair_error(
                    AgentRepairKind::ChannelOutbox,
                    format!(
                        "Channel outbox `{}` contains a non-UTF-8 top-level name.",
                        outbox.display()
                    ),
                ));
            };
            if name == "." || name == ".." {
                continue;
            }
            validate_publish_entry(
                &entry.path(),
                &channel_dir.join(format!(
                    "{}-{}-{}-{}",
                    state.role, state.agent, run_id, name
                )),
            )?;
        }
    }
    Ok(())
}

fn publish_channel_outbox(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
    channel: &ChannelSlug,
) -> Result<()> {
    let outbox = agent_paths.channel_dir(channel);
    if !outbox.exists() || directory_is_empty(&outbox)? {
        return Ok(());
    }
    let _lock = lock::FileLock::acquire(
        project.channel_lock_path(channel),
        &format!("channel `{channel}` publish lock"),
    )?;
    let channel_dir = project.channel_dir(channel);
    git::init_channel(&channel_dir)?;
    let mut published = false;
    for entry in fs::read_dir(&outbox)
        .with_context(|| format!("Failed to read channel outbox `{}`", outbox.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", outbox.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!(
                    "Channel outbox `{}` contains a non-UTF-8 top-level name.",
                    outbox.display()
                ),
            ));
        };
        if name == "." || name == ".." {
            continue;
        }
        let target = channel_dir.join(format!(
            "{}-{}-{}-{}",
            state.role, state.agent, run_id, name
        ));
        published |= copy_publish_entry(&entry.path(), &target)?;
    }
    if published {
        git::commit_all(
            &channel_dir,
            &format!(
                "think: publish {}/{} run {} to {}",
                state.role, state.agent, run_id, channel
            ),
        )?;
    }
    clear_directory_contents(&outbox)?;
    Ok(())
}

fn directory_is_empty(path: &Path) -> Result<bool> {
    Ok(fs::read_dir(path)
        .with_context(|| format!("Failed to read `{}`", path.display()))?
        .next()
        .is_none())
}

fn validate_publish_entry(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect `{}`", source.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish symlink `{}`.", source.display()),
        ));
    }
    if metadata.is_dir() {
        if target.exists() {
            return ensure_directories_identical(source, target);
        }
        return validate_publish_directory(source, target);
    }
    if metadata.is_file() {
        if target.exists() {
            ensure_files_identical(source, target)?;
        }
        return Ok(());
    }
    Err(agent_repair_error(
        AgentRepairKind::ChannelOutbox,
        format!("Refusing to publish non-file `{}`.", source.display()),
    ))
}

fn validate_publish_directory(source: &Path, target: &Path) -> Result<()> {
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        validate_publish_entry(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

fn copy_publish_entry(source: &Path, target: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect `{}`", source.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish symlink `{}`.", source.display()),
        ));
    }
    if metadata.is_dir() {
        if target.exists() {
            ensure_directories_identical(source, target)?;
            return Ok(false);
        }
        copy_directory(source, target)?;
        return Ok(true);
    }
    if !metadata.is_file() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish non-file `{}`.", source.display()),
        ));
    }
    if target.exists() {
        ensure_files_identical(source, target)?;
        return Ok(false);
    }
    if let Some(parent) = target.parent() {
        io::ensure_dir(parent)?;
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "Failed to publish `{}` to `{}`",
            source.display(),
            target.display()
        )
    })?;
    Ok(true)
}

fn copy_directory(source: &Path, target: &Path) -> Result<()> {
    io::ensure_dir(target)?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        copy_publish_entry(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

fn ensure_directories_identical(source: &Path, target: &Path) -> Result<()> {
    if !target.is_dir() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` exists but is not a directory matching `{}`.",
                target.display(),
                source.display()
            ),
        ));
    }
    let mut source_entries = BTreeSet::new();
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        source_entries.insert(entry.file_name());
        let target_entry = target.join(entry.file_name());
        let source_path = entry.path();
        let source_type = fs::symlink_metadata(&source_path)
            .with_context(|| format!("Failed to inspect `{}`", source_path.display()))?;
        if source_type.file_type().is_symlink() {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!("Refusing to publish symlink `{}`.", source_path.display()),
            ));
        }
        if source_type.is_dir() {
            ensure_directories_identical(&source_path, &target_entry)?;
        } else if source_type.is_file() {
            ensure_files_identical(&source_path, &target_entry)?;
        } else {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!("Refusing to publish non-file `{}`.", source_path.display()),
            ));
        }
    }
    for entry in fs::read_dir(target)
        .with_context(|| format!("Failed to read directory `{}`", target.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", target.display()))?;
        if !source_entries.contains(&entry.file_name()) {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!(
                    "Publish target `{}` contains extra entry `{}`.",
                    target.display(),
                    entry.file_name().to_string_lossy()
                ),
            ));
        }
    }
    Ok(())
}

fn ensure_files_identical(source: &Path, target: &Path) -> Result<()> {
    if !target.is_file() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` exists but is not a file matching `{}`.",
                target.display(),
                source.display()
            ),
        ));
    }
    if fs::read(source).with_context(|| format!("Failed to read `{}`", source.display()))?
        != fs::read(target).with_context(|| format!("Failed to read `{}`", target.display()))?
    {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` already exists with different content.",
                target.display()
            ),
        ));
    }
    Ok(())
}

fn clear_directory_contents(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read directory `{}`", path.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", path.display()))?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path)
            .with_context(|| format!("Failed to inspect `{}`", entry_path.display()))?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&entry_path)
                .with_context(|| format!("Failed to remove `{}`", entry_path.display()))?;
        } else {
            fs::remove_file(&entry_path)
                .with_context(|| format!("Failed to remove `{}`", entry_path.display()))?;
        }
    }
    Ok(())
}

fn ensure_role_runtime_started(role_paths: &RolePaths) -> Result<u64> {
    let path = role_runtime_path(&role_paths.project, &role_paths.role);
    if path.exists() {
        return Ok(io::read_toml::<RoleRuntimeState>(&path)?.started_at);
    }
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    let state = RoleRuntimeState {
        started_at: unix_timestamp(),
    };
    io::write_toml(&path, &state)?;
    Ok(state.started_at)
}

fn refresh_role_runtime_started(role_paths: &RolePaths) -> Result<()> {
    let path = role_runtime_path(&role_paths.project, &role_paths.role);
    let config = load_role_config(role_paths)?;
    if config.status == RoleStatus::Active && active_agent_count(role_paths)? > 0 {
        ensure_role_runtime_started(role_paths)?;
    } else if path.exists() {
        fs::remove_file(&path).with_context(|| format!("Failed to remove `{}`", path.display()))?;
    }
    Ok(())
}

fn load_role_runtime_started(project: &ProjectPaths, role: &RoleSlug) -> Result<Option<u64>> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    if load_role_config(&role_paths)?.status != RoleStatus::Active {
        return Ok(None);
    }
    let path = role_runtime_path(project, role);
    if path.exists() {
        Ok(Some(io::read_toml::<RoleRuntimeState>(&path)?.started_at))
    } else {
        Ok(None)
    }
}

fn role_runtime_path(project: &ProjectPaths, role: &RoleSlug) -> PathBuf {
    project
        .runtime_dir()
        .join("role-runtime")
        .join(format!("{role}.toml"))
}

fn load_agent_manifest(paths: &crate::state::AgentPaths) -> Result<AgentManifest> {
    if paths.manifest().exists() {
        io::read_toml(&paths.manifest())
    } else {
        Ok(AgentManifest::default())
    }
}

fn load_agent_manifest_for_display(
    paths: &crate::state::AgentPaths,
) -> (AgentManifest, Option<String>) {
    match load_agent_manifest(paths) {
        Ok(manifest) => (manifest, None),
        Err(err) => (
            AgentManifest::default(),
            Some(compact_single_line(&format!("{err:#}"), 180)),
        ),
    }
}

fn save_agent_manifest(paths: &crate::state::AgentPaths, manifest: &AgentManifest) -> Result<()> {
    io::write_toml(&paths.manifest(), manifest)
}

fn prepare_agent_manifest(paths: &crate::state::AgentPaths) -> Result<()> {
    let mut manifest = match load_agent_manifest(paths) {
        Ok(manifest) => manifest,
        Err(err) if is_toml_parse_error(&err) => AgentManifest::default(),
        Err(err) => return Err(err),
    };
    manifest.disposition = None;
    save_agent_manifest(paths, &manifest)
}

fn validate_agent_finalization(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    mode: RoleMode,
    expected_state: &AgentState,
    run_id: u64,
    run_paths: &crate::state::RunPaths,
) -> Result<(AgentState, Option<Disposition>)> {
    let state = validate_agent_state_after_run(agent_paths, expected_state)?;
    let disposition = validate_agent_manifest_for_mode(agent_paths, mode)?;
    validate_run_reply(run_paths)?;
    validate_channel_outboxes(project, agent_paths, &state, run_id)?;
    Ok((state, disposition))
}

fn validate_agent_state_after_run(
    paths: &crate::state::AgentPaths,
    expected: &AgentState,
) -> Result<AgentState> {
    let state = match load_agent(paths) {
        Ok(state) => state,
        Err(err) if is_toml_parse_error(&err) => {
            return Err(agent_repair_error(
                AgentRepairKind::AgentState,
                format!("agent.toml is invalid: {err:#}"),
            ));
        }
        Err(err) => return Err(err),
    };
    if state.role != expected.role {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed role from `{}` to `{}`",
            expected.role, state.role
        )));
    }
    if state.agent != expected.agent {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed agent from `{}` to `{}`",
            expected.agent, state.agent
        )));
    }
    if state.backend != expected.backend {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed backend from `{}` to `{}`",
            expected.backend, state.backend
        )));
    }
    if state.mode != expected.mode {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed mode from `{}` to `{}`",
            expected.mode, state.mode
        )));
    }
    if state.current_step != expected.current_step {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed current_step from `{}` to `{}`",
            expected.current_step, state.current_step
        )));
    }
    if state.run_count != expected.run_count {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed run_count from `{}` to `{}`",
            expected.run_count, state.run_count
        )));
    }
    if state.channels != expected.channels {
        return Err(agent_state_repair_error(
            "agent.toml changed the channel list".to_owned(),
        ));
    }
    if !matches!(
        state.status,
        AgentStatus::Running
            | AgentStatus::Paused
            | AgentStatus::Stopped
            | AgentStatus::NeedsAttention
    ) {
        return Err(agent_state_repair_error(format!(
            "agent.toml has invalid in-run status `{}`",
            state.status
        )));
    }
    Ok(state)
}

fn agent_state_repair_error(message: String) -> anyhow::Error {
    agent_repair_error(AgentRepairKind::AgentState, message)
}

fn validate_agent_manifest_for_mode(
    paths: &crate::state::AgentPaths,
    mode: RoleMode,
) -> Result<Option<Disposition>> {
    let manifest = match load_agent_manifest(paths) {
        Ok(manifest) => manifest,
        Err(err) if is_toml_parse_error(&err) => {
            return Err(agent_repair_error(
                AgentRepairKind::Manifest,
                format!("manifest.toml is invalid: {err:#}"),
            ));
        }
        Err(err) => return Err(err),
    };
    match mode {
        RoleMode::Repeatable => manifest.disposition.map(Some).ok_or_else(|| {
            agent_repair_error(
                AgentRepairKind::Manifest,
                concat!(
                    "repeatable role run ended without `disposition = \"continue\"` or \
                 `disposition = \"stop\"` in manifest.toml"
                )
                .to_owned(),
            )
        }),
        RoleMode::Oneshot | RoleMode::Infinite => Ok(None),
    }
}

fn validate_run_reply(run_paths: &crate::state::RunPaths) -> Result<()> {
    let Some(reply) = io::read_optional_text(&run_paths.reply())? else {
        return Err(agent_repair_error(
            AgentRepairKind::Reply,
            format!(
                "run reply file `{}` is missing",
                run_paths.reply().display()
            ),
        ));
    };
    if reply.trim().is_empty() {
        return Err(agent_repair_error(
            AgentRepairKind::Reply,
            format!("run reply file `{}` is empty", run_paths.reply().display()),
        ));
    }
    Ok(())
}

fn agent_repair_event(err: &anyhow::Error, attempt: u32, retry_at: u64) -> String {
    let kind = agent_repair_kind(err)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "agent output".to_owned());
    format!(
        "{kind} is invalid; repair {attempt}/{MAX_AGENT_REPAIR_RETRIES} at {}: {}",
        format_unix_time(retry_at),
        compact_single_line(&format!("{err:#}"), 160)
    )
}

fn agent_repair_exhausted_event(err: &anyhow::Error) -> String {
    let kind = agent_repair_kind(err)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "agent output".to_owned());
    format!(
        "{kind} is still invalid after {MAX_AGENT_REPAIR_RETRIES} repair attempts: {}",
        compact_single_line(&format!("{err:#}"), 160)
    )
}

fn agent_repair_notice(
    err: &anyhow::Error,
    kind: AgentRepairKind,
    mode: RoleMode,
    attempt: u32,
) -> String {
    let instruction = match kind {
        AgentRepairKind::AgentState => {
            "The runtime restored `agent.toml` to a valid state. Do not edit `agent.toml`, \
             `orchestrator.toml`, runtime files, or project channel directories."
        }
        AgentRepairKind::ChannelOutbox => {
            "Fix only your local `channels/<channel>/` outbox. Remove symlinks and special files, \
             rename or remove colliding artifacts, and leave project channel directories alone."
        }
        AgentRepairKind::Manifest => manifest_mode_instruction(mode),
        AgentRepairKind::Reply => {
            "Provide a compact final reply. It must not be empty; summarize what you did, what \
             changed, what is known now, and the most important next step if one exists."
        }
    };
    format!(
        concat!(
            "The previous run finished, but think could not finalize it because the agent's ",
            "{kind} output is invalid.\n\n",
            "# Runtime error\n\n",
            "{err:#}\n\n",
            "{instruction}\n\n",
            "Do not redo unrelated work. Repair the issue and exit again.\n\n",
            "This is repair attempt {attempt}/{max_attempts}."
        ),
        kind = kind,
        err = err,
        instruction = instruction,
        attempt = attempt,
        max_attempts = MAX_AGENT_REPAIR_RETRIES
    )
}

fn manifest_mode_instruction(mode: RoleMode) -> &'static str {
    match mode {
        RoleMode::Repeatable => {
            "Because this is a repeatable role, choose exactly one of \
             `disposition = \"continue\"` or `disposition = \"stop\"` before exiting."
        }
        RoleMode::Oneshot => {
            "Because this is a one-shot role, remove the `disposition` key. Completion is recorded \
             by the think runtime; do not write `disposition = \"done\"`."
        }
        RoleMode::Infinite => {
            "Because this is an infinite role, remove the `disposition` key. The think runtime \
             will start the next run automatically."
        }
    }
}

fn agent_repair_error(kind: AgentRepairKind, message: String) -> anyhow::Error {
    anyhow!(AgentRepairError { kind, message })
}

fn is_agent_repair_error(err: &anyhow::Error) -> bool {
    agent_repair_kind(err).is_some()
}

fn agent_repair_kind(err: &anyhow::Error) -> Option<AgentRepairKind> {
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<AgentRepairError>()
            .map(|error| error.kind)
    })
}

fn is_toml_parse_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<toml::de::Error>().is_some())
}

fn fire_role_step_finished_triggers(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_step: &crate::ids::StepSlug,
) -> Result<()> {
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::RoleStepFinished { role, step, launch } = trigger else {
                continue;
            };
            if role == *source_role && step == *source_step {
                launch_triggered_role(
                    project,
                    &target,
                    &launch,
                    TriggerCause::RoleStepFinished {
                        source_role: source_role.clone(),
                        source_step: source_step.clone(),
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn fire_role_agent_finished_triggers(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_agent: &AgentId,
    run_id: u64,
    step: &StepSlug,
) -> Result<()> {
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::RoleAgentFinished { role, launch } = trigger else {
                continue;
            };
            if role == *source_role
                && claim_role_agent_finished_trigger(
                    project,
                    source_role,
                    source_agent,
                    run_id,
                    &target,
                )?
            {
                launch_triggered_role(
                    project,
                    &target,
                    &launch,
                    TriggerCause::RoleAgentFinished {
                        source_role: source_role.clone(),
                        source_agent: source_agent.clone(),
                        run_id,
                        step: step.clone(),
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn fire_queue_idle_triggers(project: &ProjectPaths) -> Result<()> {
    let mut trigger_specs = Vec::new();
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::QueueIdle {
                idle_queue,
                idle_seconds,
                launch,
            } = trigger
            else {
                continue;
            };
            trigger_specs.push((target.clone(), idle_queue, idle_seconds, launch));
        }
    }

    for (target, queue, idle_seconds, launch) in trigger_specs {
        validate_trigger_queue(&queue)?;
        let Some(empty_since) = refresh_queue_empty_since(project, &queue)? else {
            continue;
        };
        if unix_timestamp().saturating_sub(empty_since) < idle_seconds {
            continue;
        }
        if claim_queue_idle_trigger(project, &queue, empty_since, &target, idle_seconds)? {
            launch_triggered_role(
                project,
                &target,
                &launch,
                TriggerCause::QueueIdle {
                    queue,
                    idle_seconds,
                    empty_since,
                },
            )?;
        }
    }
    Ok(())
}

fn start_elapsed_trigger_thread(
    project: ProjectPaths,
    source_role: RoleSlug,
    done: Arc<AtomicBool>,
) -> Result<Option<thread::JoinHandle<Result<()>>>> {
    let mut triggers = Vec::new();
    for target in list_roles(&project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::Elapsed {
                role,
                interval_seconds,
                launch,
            } = trigger
            else {
                continue;
            };
            if role == source_role && interval_seconds > 0 {
                triggers.push((target.clone(), interval_seconds, launch));
            }
        }
    }
    if triggers.is_empty() {
        return Ok(None);
    }
    Ok(Some(thread::spawn(move || {
        while !done.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            let Some(started_at) = load_role_runtime_started(&project, &source_role)? else {
                return Ok(());
            };
            let elapsed = unix_timestamp().saturating_sub(started_at);
            for (target, interval_seconds, launch) in &triggers {
                let count = elapsed / *interval_seconds;
                for event_index in 1..=count {
                    if claim_elapsed_trigger(
                        &project,
                        &source_role,
                        started_at,
                        target,
                        *interval_seconds,
                        event_index,
                    )? {
                        launch_triggered_role(
                            &project,
                            target,
                            launch,
                            TriggerCause::Elapsed {
                                source_role: source_role.clone(),
                                source_started_at: started_at,
                                interval_seconds: *interval_seconds,
                                event_index,
                            },
                        )?;
                    }
                }
            }
        }
        Ok(())
    })))
}

fn claim_elapsed_trigger(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_started_at: u64,
    target_role: &RoleSlug,
    interval_seconds: u64,
    event_index: u64,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("elapsed")
        .join(source_role.as_str())
        .join(source_started_at.to_string())
        .join(target_role.as_str())
        .join(interval_seconds.to_string())
        .join(format!("{event_index}.lock"));
    lock::claim_once(&path, "elapsed trigger event")
}

fn claim_role_agent_finished_trigger(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_agent: &AgentId,
    run_id: u64,
    target_role: &RoleSlug,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("role-agent-finished")
        .join(source_role.as_str())
        .join(source_agent.as_str())
        .join(run_id.to_string())
        .join(target_role.as_str())
        .join("claimed.lock");
    lock::claim_once(&path, "role agent finished trigger event")
}

fn claim_queue_idle_trigger(
    project: &ProjectPaths,
    queue: &str,
    empty_since: u64,
    target_role: &RoleSlug,
    idle_seconds: u64,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("queue-idle")
        .join(queue)
        .join(empty_since.to_string())
        .join(target_role.as_str())
        .join(idle_seconds.to_string())
        .join("claimed.lock");
    lock::claim_once(&path, "queue idle trigger event")
}

fn launch_triggered_role(
    project: &ProjectPaths,
    role: &RoleSlug,
    launch: &TriggerLaunch,
    cause: TriggerCause,
) -> Result<()> {
    match launch {
        TriggerLaunch::Async => {
            let role_paths = RolePaths::new(project.clone(), role.clone());
            let mut config = load_role_config(&role_paths)?;
            if config.status == RoleStatus::Paused {
                return Ok(());
            }
            config.status = RoleStatus::Active;
            save_role_config(&role_paths, &config)?;
            start_one_agent_with_trigger(&role_paths, &config, None, Some(&cause))?;
            Ok(())
        }
        TriggerLaunch::Queued { queue } => {
            enqueue_triggered_role(project, queue, role, cause)?;
            drain_trigger_queue(project, queue)
        }
    }
}

fn enqueue_triggered_role(
    project: &ProjectPaths,
    queue: &str,
    role: &RoleSlug,
    cause: TriggerCause,
) -> Result<()> {
    validate_trigger_queue(queue)?;
    let path = trigger_queue_path(project, queue);
    let mut state = load_trigger_queue(project, queue)?;
    state.items.push(TriggerQueueItem {
        role: role.clone(),
        enqueued_at: unix_timestamp(),
        cause,
    });
    io::write_toml(&path, &state)
}

fn drain_trigger_queue(project: &ProjectPaths, queue: &str) -> Result<()> {
    validate_trigger_queue(queue)?;
    let Some(_lock) = lock::FileLock::try_acquire(
        trigger_queue_lock_path(project, queue),
        "trigger queue lock",
    )?
    else {
        return Ok(());
    };
    loop {
        let mut queue_state = load_trigger_queue(project, queue)?;
        let Some(item) = queue_state.items.first().cloned() else {
            return Ok(());
        };
        let role_paths = RolePaths::new(project.clone(), item.role.clone());
        let mut config = load_role_config(&role_paths)?;
        if config.status != RoleStatus::Paused {
            config.status = RoleStatus::Active;
            save_role_config(&role_paths, &config)?;
            start_one_agent_sync_with_trigger(&role_paths, &config, Some(&item.cause))?;
        }
        queue_state.items.remove(0);
        io::write_toml(&trigger_queue_path(project, queue), &queue_state)?;
    }
}

fn load_trigger_queue(project: &ProjectPaths, queue: &str) -> Result<TriggerQueueState> {
    let path = trigger_queue_path(project, queue);
    if path.exists() {
        io::read_toml(&path)
    } else {
        Ok(TriggerQueueState::default())
    }
}

fn trigger_queue_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("trigger-queues")
        .join(format!("{queue}.toml"))
}

fn trigger_queue_lock_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("locks")
        .join("trigger-queues")
        .join(format!("{queue}.lock"))
}

fn queue_runtime_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("queue-runtime")
        .join(format!("{queue}.toml"))
}

fn refresh_queue_empty_since(project: &ProjectPaths, queue: &str) -> Result<Option<u64>> {
    let path = queue_runtime_path(project, queue);
    if !queue_is_empty(project, queue)? {
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("Failed to remove `{}`", path.display()))?;
        }
        return Ok(None);
    }

    if path.exists() {
        return Ok(Some(io::read_toml::<QueueRuntimeState>(&path)?.empty_since));
    }
    let state = QueueRuntimeState {
        empty_since: unix_timestamp(),
    };
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    io::write_toml(&path, &state)?;
    Ok(Some(state.empty_since))
}

fn queue_is_empty(project: &ProjectPaths, queue: &str) -> Result<bool> {
    if !load_trigger_queue(project, queue)?.items.is_empty() {
        return Ok(false);
    }
    Ok(!lock::is_active(&trigger_queue_lock_path(project, queue))?)
}

fn validate_trigger_queue(queue: &str) -> Result<()> {
    if queue.is_empty()
        || queue.starts_with('-')
        || queue.ends_with('-')
        || queue.contains("--")
        || !queue
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("trigger queue `{queue}` must be a lowercase ASCII slug");
    }
    Ok(())
}

fn attach(args: AttachArgs) -> Result<()> {
    let project = current_project_awake()?;
    run_attach_viewer(&project, selection::resolve_attach(&project, args.target)?)
}

#[derive(Clone)]
enum AttachScope {
    Project,
    Role(RoleSlug),
    Agent(ResolvedAgent),
}

impl From<AttachTarget> for AttachScope {
    fn from(value: AttachTarget) -> Self {
        match value {
            AttachTarget::Project => Self::Project,
            AttachTarget::Role(role) => Self::Role(role),
            AttachTarget::Agent(agent) => Self::Agent(agent),
        }
    }
}

struct AttachViewerApp {
    project: ProjectPaths,
    scope: AttachScope,
    agent: Option<AttachViewerAgent>,
    scroll: usize,
    follow: bool,
    last_transcript_rows: usize,
    collapse_thinking: bool,
    composer: Option<AttachComposer>,
    search: AttachSearch,
    switcher: Option<AttachSwitcher>,
    message: Option<String>,
}

#[derive(Clone)]
struct AttachViewerAgent {
    resolved: ResolvedAgent,
    status: AgentStatus,
    summary: String,
    detail: String,
    run_count: u64,
    updated_at: u64,
}

struct AttachDocument {
    lines: Vec<Line<'static>>,
    markers: Vec<AttachReplyMarker>,
}

struct AttachReplyMarker {
    offset: usize,
    label: String,
}

struct AttachComposer {
    lines: Vec<String>,
    cursor_line: usize,
    cursor_col: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    draft_before_history: String,
}

#[derive(Default)]
struct AttachSearch {
    active: bool,
    query: String,
}

struct AttachSwitcher {
    query: String,
    selected: usize,
}

enum AttachViewerEvent {
    Continue,
    FollowUp(ResolvedAgent, String),
    Quit,
}

struct CodexConversationApp {
    command_name: String,
    root: PathBuf,
    turn: u64,
    turn_root: PathBuf,
    scroll: usize,
    follow: bool,
    last_transcript_rows: usize,
    collapse_thinking: bool,
    composer: Option<AttachComposer>,
    search: AttachSearch,
    message: Option<String>,
    exit: Option<runner::PtyExit>,
    error: Option<String>,
    follow_up_history: Vec<String>,
}

enum CodexConversationOutcome {
    Finish,
    FollowUp(String),
}

fn run_attach_viewer(project: &ProjectPaths, target: AttachTarget) -> Result<()> {
    let mut app = AttachViewerApp {
        project: project.clone(),
        scope: target.into(),
        agent: None,
        scroll: 0,
        follow: true,
        last_transcript_rows: DASHBOARD_MIN_VISIBLE_ROWS,
        collapse_thinking: true,
        composer: None,
        search: AttachSearch::default(),
        switcher: None,
        message: None,
    };
    let mut terminal = Some(TerminalSession::enter()?);
    loop {
        app.refresh()?;
        terminal
            .as_mut()
            .expect("attach terminal is active")
            .draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
        {
            match app.handle_key(key)? {
                AttachViewerEvent::Continue => {}
                AttachViewerEvent::Quit => return Ok(()),
                AttachViewerEvent::FollowUp(agent, query) => {
                    drop(terminal.take());
                    more_agent(MoreArgs {
                        agent: Some(AgentSpec {
                            role: Some(agent.role),
                            agent: agent.agent,
                        }),
                        query: Some(query),
                        new: false,
                    })?;
                    terminal = Some(TerminalSession::enter()?);
                    app.message = Some("follow-up completed".to_owned());
                    app.scroll_to_latest_reply()?;
                }
            }
        }
    }
}

impl CodexConversationApp {
    fn new(
        command_name: &str,
        root: PathBuf,
        turn: u64,
        turn_root: PathBuf,
        follow_up_history: Vec<String>,
    ) -> Self {
        Self {
            command_name: command_name.to_owned(),
            root,
            turn,
            turn_root,
            scroll: 0,
            follow: true,
            last_transcript_rows: DASHBOARD_MIN_VISIBLE_ROWS,
            collapse_thinking: true,
            composer: None,
            search: AttachSearch::default(),
            message: None,
            exit: None,
            error: None,
            follow_up_history,
        }
    }

    fn set_exit(&mut self, exit: runner::PtyExit) {
        self.message = Some(if exit.success {
            "Codex turn completed; press r to reply or q to finish".to_owned()
        } else {
            format!(
                "Codex exited with status {}; press q after reviewing",
                exit.code
            )
        });
        self.exit = Some(exit);
    }

    fn set_error(&mut self, error: String) {
        self.message = Some(format!("Codex runner failed: {error}"));
        self.error = Some(error);
    }

    fn turn_running(&self) -> bool {
        self.exit.is_none() && self.error.is_none()
    }

    fn turn_failed(&self) -> bool {
        self.error.is_some() || self.exit.as_ref().is_some_and(|exit| !exit.success)
    }

    fn handle_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<CodexConversationOutcome>> {
        if self.composer.is_some() {
            return self.handle_composer_key(key);
        }
        if self.search.active {
            self.handle_search_key(key)?;
            return Ok(None);
        }
        let document = self.document()?;
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if self.turn_running() {
                    self.message =
                        Some("Codex is still running; wait for this turn to finish".to_owned());
                    Ok(None)
                } else if self.turn_failed() {
                    self.raise_turn_failure()
                } else {
                    Ok(Some(CodexConversationOutcome::Finish))
                }
            }
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(&document, -1);
                Ok(None)
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(&document, 1);
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_up(ATTACH_SCROLL_STEP);
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_down(ATTACH_SCROLL_STEP, document.lines.len());
                Ok(None)
            }
            KeyCode::PageUp => {
                self.scroll_up(ATTACH_PAGE_STEP);
                Ok(None)
            }
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.scroll_down(ATTACH_PAGE_STEP, document.lines.len());
                Ok(None)
            }
            KeyCode::Home => {
                self.scroll = 0;
                self.follow = false;
                Ok(None)
            }
            KeyCode::End => {
                self.scroll_to_bottom(document.lines.len());
                Ok(None)
            }
            KeyCode::Char('F') => {
                self.toggle_follow(document.lines.len());
                Ok(None)
            }
            KeyCode::Char('t') => {
                self.toggle_thinking(&document)?;
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.search.active = true;
                self.message = None;
                Ok(None)
            }
            KeyCode::Char('n') => {
                self.navigate_search(&document, 1);
                Ok(None)
            }
            KeyCode::Char('N') => {
                self.navigate_search(&document, -1);
                Ok(None)
            }
            KeyCode::Char('r') | KeyCode::Char('m') => {
                if self.turn_running() {
                    self.message =
                        Some("reply is available after Codex finishes the turn".to_owned());
                } else if self.turn_failed() {
                    self.message =
                        Some("turn failed; quit after reviewing the transcript".to_owned());
                } else {
                    self.open_composer();
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_composer_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<CodexConversationOutcome>> {
        match key.code {
            KeyCode::Esc => {
                self.composer = None;
                self.message = Some("reply cancelled".to_owned());
                Ok(None)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let Some(composer) = self.composer.take() else {
                    return Ok(None);
                };
                let query = composer.text();
                if query.trim().is_empty() {
                    self.message = Some("blank reply cancelled".to_owned());
                    return Ok(None);
                }
                Ok(Some(CodexConversationOutcome::FollowUp(query)))
            }
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.history_previous();
                }
                Ok(None)
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.history_next();
                }
                Ok(None)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.clear();
                }
                Ok(None)
            }
            KeyCode::Enter => {
                if let Some(composer) = &mut self.composer {
                    composer.insert_newline();
                }
                Ok(None)
            }
            KeyCode::Backspace => {
                if let Some(composer) = &mut self.composer {
                    composer.backspace();
                }
                Ok(None)
            }
            KeyCode::Left => {
                if let Some(composer) = &mut self.composer {
                    composer.move_left();
                }
                Ok(None)
            }
            KeyCode::Right => {
                if let Some(composer) = &mut self.composer {
                    composer.move_right();
                }
                Ok(None)
            }
            KeyCode::Up => {
                if let Some(composer) = &mut self.composer {
                    composer.move_vertical(-1);
                }
                Ok(None)
            }
            KeyCode::Down => {
                if let Some(composer) = &mut self.composer {
                    composer.move_vertical(1);
                }
                Ok(None)
            }
            KeyCode::Char(ch) => {
                if let Some(composer) = &mut self.composer {
                    composer.insert(ch);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_search_key(&mut self, key: crossterm::event::KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => {
                self.search.active = false;
            }
            KeyCode::Enter => {
                let document = self.document()?;
                self.search.active = false;
                self.navigate_search(&document, 1);
            }
            KeyCode::Backspace => {
                self.search.query.pop();
                self.scroll_to_first_search_match()?;
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.search.query.clear();
            }
            KeyCode::Char(ch) => {
                self.search.query.push(ch);
                self.scroll_to_first_search_match()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn raise_turn_failure(&self) -> Result<Option<CodexConversationOutcome>> {
        if let Some(error) = &self.error {
            bail!(
                "`codex exec` failed while running `think {}`: {error}",
                self.command_name
            );
        }
        if let Some(exit) = &self.exit
            && !exit.success
        {
            bail!(
                "`codex exec` for `think {}` exited with status code {}",
                self.command_name,
                exit.code
            );
        }
        Ok(Some(CodexConversationOutcome::Finish))
    }

    fn open_composer(&mut self) {
        self.composer = Some(AttachComposer::new(self.follow_up_history.clone()));
        self.message = None;
    }

    fn scroll_to_first_search_match(&mut self) -> Result<()> {
        if self.search.query.trim().is_empty() {
            return Ok(());
        }
        let document = self.document()?;
        if let Some(offset) = attach_search_matches(&document, &self.search.query)
            .first()
            .copied()
        {
            self.scroll = offset;
            self.clamp_manual_scroll(document.lines.len());
        }
        Ok(())
    }

    fn navigate_search(&mut self, document: &AttachDocument, direction: i8) {
        if self.search.query.trim().is_empty() {
            self.message = Some("no transcript search query".to_owned());
            return;
        }
        let matches = attach_search_matches(document, &self.search.query);
        if matches.is_empty() {
            self.message = Some(format!("no matches for `{}`", self.search.query));
            return;
        }
        self.scroll = if direction < 0 {
            matches
                .iter()
                .rev()
                .copied()
                .find(|offset| *offset < self.scroll)
                .unwrap_or_else(|| *matches.last().expect("matches is nonempty"))
        } else {
            matches
                .iter()
                .copied()
                .find(|offset| *offset > self.scroll)
                .unwrap_or(matches[0])
        };
        self.clamp_manual_scroll(document.lines.len());
        self.message = Some(format!(
            "match {}/{} for `{}`",
            attach_match_position(&matches, self.scroll),
            matches.len(),
            self.search.query
        ));
    }

    fn toggle_thinking(&mut self, document: &AttachDocument) -> Result<()> {
        let was_following = self.follow;
        let marker_index = current_attach_marker_index(&document.markers, self.scroll);
        let marker_offset = marker_index
            .and_then(|index| document.markers.get(index))
            .map(|marker| marker.offset)
            .unwrap_or(0);
        let marker_delta = self.scroll.saturating_sub(marker_offset);
        self.collapse_thinking = !self.collapse_thinking;
        let next_document = self.document()?;
        if was_following {
            self.scroll_to_bottom(next_document.lines.len());
        } else {
            self.scroll = marker_index
                .and_then(|index| next_document.markers.get(index))
                .map(|marker| marker.offset.saturating_add(marker_delta))
                .unwrap_or(self.scroll);
            self.clamp_manual_scroll(next_document.lines.len());
        }
        Ok(())
    }

    fn jump_reply(&mut self, document: &AttachDocument, direction: i8) {
        if document.markers.is_empty() {
            return;
        }
        let current = current_attach_marker_index(&document.markers, self.scroll).unwrap_or(0);
        let next = if direction < 0 {
            current.saturating_sub(1)
        } else {
            (current + 1).min(document.markers.len() - 1)
        };
        self.scroll = document.markers[next].offset;
        self.clamp_manual_scroll(document.lines.len());
    }

    fn scroll_up(&mut self, amount: usize) {
        self.scroll = self.scroll.saturating_sub(amount);
        self.follow = false;
        self.message = None;
    }

    fn scroll_down(&mut self, amount: usize, line_count: usize) {
        self.scroll = self.scroll.saturating_add(amount);
        self.follow = false;
        self.clamp_manual_scroll(line_count);
        self.message = None;
    }

    fn toggle_follow(&mut self, line_count: usize) {
        if self.follow {
            self.follow = false;
            self.message = Some("follow disabled".to_owned());
        } else {
            self.scroll_to_bottom(line_count);
            self.message = Some("follow enabled".to_owned());
        }
    }

    fn scroll_to_bottom(&mut self, line_count: usize) {
        self.follow = true;
        self.scroll = self.max_transcript_scroll(line_count);
    }

    fn sync_transcript_scroll(&mut self, line_count: usize) {
        if self.follow {
            self.scroll = self.max_transcript_scroll(line_count);
        } else {
            self.clamp_manual_scroll(line_count);
        }
    }

    fn clamp_manual_scroll(&mut self, line_count: usize) {
        let max_scroll = self.max_transcript_scroll(line_count);
        self.scroll = self.scroll.min(max_scroll);
        self.follow = self.scroll == max_scroll;
    }

    fn max_transcript_scroll(&self, line_count: usize) -> usize {
        max_scroll_offset(line_count, self.last_transcript_rows)
    }

    fn document(&self) -> Result<AttachDocument> {
        let mut document = AttachDocument {
            lines: vec![
                Line::from(vec![
                    Span::styled("command ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("think {}", self.command_name),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(" · turn ", Style::default().fg(Color::DarkGray)),
                    Span::styled(self.turn.to_string(), Style::default().fg(Color::White)),
                    Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                    Span::styled(self.turn_status_label(), self.turn_status_style()),
                ]),
                Line::from(vec![
                    Span::styled("root ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        self.root.display().to_string(),
                        Style::default().fg(Color::Gray),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("files ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        self.turn_root.display().to_string(),
                        Style::default().fg(Color::Gray),
                    ),
                ]),
                Line::from(""),
            ],
            markers: Vec::new(),
        };
        if let Some(reply) = io::read_optional_text(&self.turn_root.join("REPLY.md"))?
            && !reply.trim().is_empty()
        {
            document.markers.push(AttachReplyMarker {
                offset: document.lines.len(),
                label: format!("turn {} reply", self.turn),
            });
            document.lines.push(section_line("latest reply"));
            document.lines.extend(
                reply
                    .lines()
                    .map(|line| attach_markdown_line(line, Color::White)),
            );
            document.lines.push(Line::from(""));
        }
        let transcript =
            io::read_optional_text(&self.turn_root.join("TRANSCRIPT.txt"))?.unwrap_or_default();
        document.lines.push(section_line("live transcript"));
        if transcript.trim().is_empty() {
            document.lines.push(Line::from(Span::styled(
                if self.turn_running() {
                    "waiting for Codex output..."
                } else {
                    "Codex produced no transcript output."
                },
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for block in crate::transcript::parse(&transcript) {
                push_attach_transcript_block(
                    &mut document,
                    self.turn,
                    block,
                    self.collapse_thinking,
                );
            }
        }
        Ok(document)
    }

    fn turn_status_label(&self) -> String {
        if let Some(error) = &self.error {
            return format!("failed: {error}");
        }
        match &self.exit {
            None => "running".to_owned(),
            Some(exit) if exit.success => "done".to_owned(),
            Some(exit) => format!("failed {}", exit.code),
        }
    }

    fn turn_status_style(&self) -> Style {
        if self.turn_running() {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else if self.turn_failed() {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let bottom_height = if self.composer.is_some() {
            ATTACH_COMPOSER_HEIGHT
        } else if self.search.active {
            DASHBOARD_SEARCH_HEIGHT
        } else {
            0
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(bottom_height),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(ATTACH_REPLY_RAIL_WIDTH),
                Constraint::Min(1),
            ])
            .split(chunks[0]);
        let document = self.document().unwrap_or_else(|err| AttachDocument {
            lines: vec![Line::from(format!("Failed to load transcript: {err:#}"))],
            markers: Vec::new(),
        });
        self.last_transcript_rows = visible_panel_rows(body[1]);
        self.sync_transcript_scroll(document.lines.len());
        self.draw_reply_rail(frame, body[0], &document);
        self.draw_transcript(frame, body[1], &document);
        if let Some(composer) = &self.composer {
            self.draw_composer(frame, chunks[1], composer);
        } else if self.search.active {
            self.draw_search_bar(frame, chunks[1], &document);
        }
        let footer_pairs = if self.composer.is_some() {
            ATTACH_FOOTER_COMPOSER
        } else if self.search.active {
            ATTACH_FOOTER_SEARCH
        } else if self.turn_running() {
            CODEX_CONVERSATION_FOOTER_RUNNING
        } else if self.follow {
            CODEX_CONVERSATION_FOOTER_DONE_FOLLOW_ON
        } else {
            CODEX_CONVERSATION_FOOTER_DONE_FOLLOW_OFF
        };
        draw_dashboard_footer(
            frame,
            chunks[2],
            footer_line_from_pairs(footer_pairs, usize::from(chunks[2].width)),
        );
    }

    fn draw_reply_rail(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let current = current_attach_marker_index(&document.markers, self.scroll);
        let height = usize::from(area.height);
        let start = current
            .map(|index| index.saturating_sub(height / 2))
            .unwrap_or(0)
            .min(document.markers.len().saturating_sub(height));
        let lines = document
            .markers
            .iter()
            .enumerate()
            .skip(start)
            .take(height)
            .map(|(index, marker)| {
                let selected = Some(index) == current;
                let assistant_marker =
                    marker.label.contains("reply") || marker.label.contains("codex");
                Line::from(Span::styled(
                    if selected { "●" } else { "•" },
                    Style::default()
                        .fg(if assistant_marker || selected {
                            Color::Green
                        } else {
                            Color::DarkGray
                        })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ))
                .alignment(ratatui::layout::Alignment::Center)
            })
            .collect::<Vec<_>>();
        frame.render_widget(Paragraph::new(Text::from(lines)), area);
    }

    fn draw_transcript(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let title = dynamic_panel_title(
            format!(
                "Codex Assist · turn {} · follow {} · thinking {}",
                self.turn,
                if self.follow { "on" } else { "off" },
                if self.collapse_thinking {
                    "collapsed"
                } else {
                    "shown"
                }
            ),
            (!document.lines.is_empty()).then_some((
                (self.scroll + 1).min(document.lines.len()),
                document.lines.len(),
            )),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let matches = attach_search_matches(document, &self.search.query);
        let mut visible = document
            .lines
            .iter()
            .enumerate()
            .skip(self.scroll)
            .map(|(index, line)| {
                let mut line = line.clone();
                if matches.contains(&index) {
                    line = line.patch_style(
                        Style::default()
                            .bg(if index == self.scroll {
                                Color::Blue
                            } else {
                                Color::DarkGray
                            })
                            .add_modifier(if index == self.scroll {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    );
                }
                line
            })
            .collect::<Vec<_>>();
        if let Some(message) = &self.message {
            visible.insert(
                0,
                Line::from(vec![
                    Span::styled("notice ", Style::default().fg(Color::Yellow)),
                    Span::styled(message.clone(), Style::default().fg(Color::White)),
                ]),
            );
        }
        frame.render_widget(
            Paragraph::new(Text::from(crate::tui_text::wrap_lines(
                &visible,
                usize::from(inner.width),
            ))),
            inner,
        );
        render_scrollbar(frame, area, document.lines.len(), self.scroll);
    }

    fn draw_composer(&self, frame: &mut Frame<'_>, area: Rect, composer: &AttachComposer) {
        let block = dashboard_block("Reply").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let lines = composer
            .lines
            .iter()
            .enumerate()
            .map(|(line_index, line)| {
                let mut text = line.clone();
                if line_index == composer.cursor_line {
                    insert_cursor_marker(&mut text, composer.cursor_col);
                }
                Line::from(Span::styled(text, Style::default().fg(Color::White)))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_search_bar(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let matches = attach_search_matches(document, &self.search.query);
        let status = if self.search.query.trim().is_empty() {
            "type to search".to_owned()
        } else if matches.is_empty() {
            "no matches".to_owned()
        } else {
            format!(
                "{} matches · current {}",
                matches.len(),
                attach_match_position(&matches, self.scroll)
            )
        };
        let block =
            dashboard_block("Transcript Search").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("/", Style::default().fg(Color::Magenta)),
                Span::styled(self.search.query.clone(), Style::default().fg(Color::White)),
                Span::styled("  ", Style::default()),
                Span::styled(status, Style::default().fg(Color::DarkGray)),
            ])),
            inner,
        );
    }
}

impl AttachViewerApp {
    fn refresh(&mut self) -> Result<()> {
        let current = self.agent.as_ref().map(|agent| agent.resolved.label());
        let agents = load_attach_viewer_agents(&self.project, &self.scope)?;
        self.agent = current
            .and_then(|label| {
                agents
                    .iter()
                    .find(|agent| agent.resolved.label() == label)
                    .cloned()
            })
            .or_else(|| agents.into_iter().next());
        let document = self.document()?;
        self.sync_transcript_scroll(document.lines.len());
        Ok(())
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Result<AttachViewerEvent> {
        if self.switcher.is_some() {
            return self.handle_switcher_key(key);
        }
        if self.composer.is_some() {
            return self.handle_composer_key(key);
        }
        if self.search.active {
            return self.handle_search_key(key);
        }
        let document = self.document()?;
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(AttachViewerEvent::Quit),
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(&document, -1);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(&document, 1);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_up(ATTACH_SCROLL_STEP);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_down(ATTACH_SCROLL_STEP, document.lines.len());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Home => {
                self.scroll = 0;
                self.follow = false;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::End => {
                self.scroll_to_bottom(document.lines.len());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::PageUp => {
                self.scroll_up(ATTACH_PAGE_STEP);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.scroll_down(ATTACH_PAGE_STEP, document.lines.len());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('F') => {
                self.toggle_follow(document.lines.len());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('t') => {
                self.toggle_thinking(&document)?;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('m') | KeyCode::Char('f') => {
                self.open_composer()?;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('/') => {
                self.search.active = true;
                self.message = None;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('n') => {
                self.navigate_search(&document, 1);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('N') => {
                self.navigate_search(&document, -1);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('s') | KeyCode::Char('o') => {
                self.switcher = Some(AttachSwitcher {
                    query: String::new(),
                    selected: 0,
                });
                Ok(AttachViewerEvent::Continue)
            }
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn handle_composer_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<AttachViewerEvent> {
        match key.code {
            KeyCode::Esc => {
                self.composer = None;
                self.message = Some("follow-up cancelled".to_owned());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let Some(agent) = self.agent.clone() else {
                    self.composer = None;
                    self.message = Some("no agent selected".to_owned());
                    return Ok(AttachViewerEvent::Continue);
                };
                if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
                    self.message =
                        Some("agent is active; wait for it to finish before more".to_owned());
                    return Ok(AttachViewerEvent::Continue);
                }
                let Some(composer) = self.composer.take() else {
                    return Ok(AttachViewerEvent::Continue);
                };
                let query = composer.text();
                if query.trim().is_empty() {
                    self.message = Some("blank follow-up cancelled".to_owned());
                    return Ok(AttachViewerEvent::Continue);
                }
                crate::input_history::append(&self.project, "followups", &query)?;
                Ok(AttachViewerEvent::FollowUp(agent.resolved, query))
            }
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.history_previous();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.history_next();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(composer) = &mut self.composer {
                    composer.clear();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Enter => {
                if let Some(composer) = &mut self.composer {
                    composer.insert_newline();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Backspace => {
                if let Some(composer) = &mut self.composer {
                    composer.backspace();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Left => {
                if let Some(composer) = &mut self.composer {
                    composer.move_left();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Right => {
                if let Some(composer) = &mut self.composer {
                    composer.move_right();
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Up => {
                if let Some(composer) = &mut self.composer {
                    composer.move_vertical(-1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down => {
                if let Some(composer) = &mut self.composer {
                    composer.move_vertical(1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char(ch) => {
                if let Some(composer) = &mut self.composer {
                    composer.insert(ch);
                }
                Ok(AttachViewerEvent::Continue)
            }
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn handle_search_key(&mut self, key: crossterm::event::KeyEvent) -> Result<AttachViewerEvent> {
        match key.code {
            KeyCode::Esc => {
                self.search.active = false;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Enter => {
                let document = self.document()?;
                self.search.active = false;
                self.navigate_search(&document, 1);
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Backspace => {
                self.search.query.pop();
                self.scroll_to_first_search_match()?;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.search.query.clear();
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char(ch) => {
                self.search.query.push(ch);
                self.scroll_to_first_search_match()?;
                Ok(AttachViewerEvent::Continue)
            }
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn handle_switcher_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<AttachViewerEvent> {
        match key.code {
            KeyCode::Esc => {
                self.switcher = None;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Enter => {
                let agents = self.switcher_agents()?;
                let Some(switcher) = self.switcher.take() else {
                    return Ok(AttachViewerEvent::Continue);
                };
                if let Some(agent) =
                    agents.get(switcher.selected.min(agents.len().saturating_sub(1)))
                {
                    self.scope = AttachScope::Agent(agent.resolved.clone());
                    self.agent = Some(agent.clone());
                    self.follow = true;
                    let document = self.document()?;
                    self.scroll_to_bottom(document.lines.len());
                    self.message = Some(format!("attached to {}", agent.resolved.label()));
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Backspace => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.query.pop();
                    switcher.selected = 0;
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Up => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.selected = switcher.selected.saturating_sub(1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down => {
                let agents = self.switcher_agents()?;
                if let Some(switcher) = &mut self.switcher
                    && !agents.is_empty()
                {
                    switcher.selected = (switcher.selected + 1).min(agents.len() - 1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char(ch) => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.query.push(ch);
                    switcher.selected = 0;
                }
                Ok(AttachViewerEvent::Continue)
            }
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn open_composer(&mut self) -> Result<()> {
        self.composer = Some(AttachComposer::new(crate::input_history::load(
            &self.project,
            "followups",
        )?));
        self.message = None;
        Ok(())
    }

    fn toggle_thinking(&mut self, document: &AttachDocument) -> Result<()> {
        let was_following = self.follow;
        let marker_index = current_attach_marker_index(&document.markers, self.scroll);
        let marker_offset = marker_index
            .and_then(|index| document.markers.get(index))
            .map(|marker| marker.offset)
            .unwrap_or(0);
        let marker_delta = self.scroll.saturating_sub(marker_offset);
        self.collapse_thinking = !self.collapse_thinking;
        let next_document = self.document()?;
        if was_following {
            self.scroll_to_bottom(next_document.lines.len());
        } else {
            self.scroll = marker_index
                .and_then(|index| next_document.markers.get(index))
                .map(|marker| marker.offset.saturating_add(marker_delta))
                .unwrap_or(self.scroll);
            self.clamp_manual_scroll(next_document.lines.len());
        }
        Ok(())
    }

    fn jump_reply(&mut self, document: &AttachDocument, direction: i8) {
        if document.markers.is_empty() {
            return;
        }
        let current = current_attach_marker_index(&document.markers, self.scroll).unwrap_or(0);
        let next = if direction < 0 {
            current.saturating_sub(1)
        } else {
            (current + 1).min(document.markers.len() - 1)
        };
        self.scroll = document.markers[next].offset;
        self.clamp_manual_scroll(document.lines.len());
    }

    fn navigate_search(&mut self, document: &AttachDocument, direction: i8) {
        if self.search.query.trim().is_empty() {
            self.message = Some("no transcript search query".to_owned());
            return;
        }
        let matches = attach_search_matches(document, &self.search.query);
        if matches.is_empty() {
            self.message = Some(format!("no matches for `{}`", self.search.query));
            return;
        }
        self.scroll = if direction < 0 {
            matches
                .iter()
                .rev()
                .copied()
                .find(|offset| *offset < self.scroll)
                .unwrap_or_else(|| *matches.last().expect("matches is nonempty"))
        } else {
            matches
                .iter()
                .copied()
                .find(|offset| *offset > self.scroll)
                .unwrap_or(matches[0])
        };
        self.clamp_manual_scroll(document.lines.len());
        self.message = Some(format!(
            "match {}/{} for `{}`",
            attach_match_position(&matches, self.scroll),
            matches.len(),
            self.search.query
        ));
    }

    fn scroll_to_first_search_match(&mut self) -> Result<()> {
        if self.search.query.trim().is_empty() {
            return Ok(());
        }
        let document = self.document()?;
        let matches = attach_search_matches(&document, &self.search.query);
        if let Some(offset) = matches.first().copied() {
            self.scroll = offset;
            self.clamp_manual_scroll(document.lines.len());
        }
        Ok(())
    }

    fn scroll_to_latest_reply(&mut self) -> Result<()> {
        let document = self.document()?;
        if let Some(marker) = document.markers.last() {
            self.scroll = marker.offset;
            self.clamp_manual_scroll(document.lines.len());
        }
        Ok(())
    }

    fn scroll_up(&mut self, amount: usize) {
        self.scroll = self.scroll.saturating_sub(amount);
        self.follow = false;
        self.message = None;
    }

    fn scroll_down(&mut self, amount: usize, line_count: usize) {
        self.scroll = self.scroll.saturating_add(amount);
        self.follow = false;
        self.clamp_manual_scroll(line_count);
        self.message = None;
    }

    fn toggle_follow(&mut self, line_count: usize) {
        if self.follow {
            self.follow = false;
            self.message = Some("follow disabled".to_owned());
        } else {
            self.scroll_to_bottom(line_count);
            self.message = Some("follow enabled".to_owned());
        }
    }

    fn scroll_to_bottom(&mut self, line_count: usize) {
        self.follow = true;
        self.scroll = self.max_transcript_scroll(line_count);
    }

    fn sync_transcript_scroll(&mut self, line_count: usize) {
        if self.follow {
            self.scroll = self.max_transcript_scroll(line_count);
        } else {
            self.clamp_manual_scroll(line_count);
        }
    }

    fn clamp_manual_scroll(&mut self, line_count: usize) {
        let max_scroll = self.max_transcript_scroll(line_count);
        self.scroll = self.scroll.min(max_scroll);
        self.follow = self.scroll == max_scroll;
    }

    fn max_transcript_scroll(&self, line_count: usize) -> usize {
        max_scroll_offset(line_count, self.last_transcript_rows)
    }

    fn document(&self) -> Result<AttachDocument> {
        match &self.agent {
            Some(agent) => attach_document(&self.project, agent, self.collapse_thinking),
            None => Ok(AttachDocument {
                lines: vec![Line::from(Span::styled(
                    "No agent matches this attach target.",
                    Style::default().fg(Color::DarkGray),
                ))],
                markers: Vec::new(),
            }),
        }
    }

    fn switcher_agents(&self) -> Result<Vec<AttachViewerAgent>> {
        let query = self
            .switcher
            .as_ref()
            .map(|switcher| switcher.query.trim().to_owned())
            .unwrap_or_default();
        let mut agents = load_attach_viewer_agents(&self.project, &AttachScope::Project)?;
        if !query.is_empty() {
            agents.retain(|agent| {
                text_matches_query(
                    [
                        agent.resolved.label(),
                        agent.status.to_string(),
                        agent.summary.clone(),
                        agent.detail.clone(),
                    ],
                    &query,
                )
            });
        }
        Ok(agents)
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let bottom_height = if self.composer.is_some() {
            ATTACH_COMPOSER_HEIGHT
        } else if self.search.active {
            DASHBOARD_SEARCH_HEIGHT
        } else {
            0
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(bottom_height),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(ATTACH_REPLY_RAIL_WIDTH),
                Constraint::Min(1),
            ])
            .split(chunks[0]);
        let document = self.document().unwrap_or_else(|err| AttachDocument {
            lines: vec![Line::from(format!("Failed to load transcript: {err:#}"))],
            markers: Vec::new(),
        });
        self.last_transcript_rows = visible_panel_rows(body[1]);
        self.sync_transcript_scroll(document.lines.len());
        self.draw_reply_rail(frame, body[0], &document);
        self.draw_transcript(frame, body[1], &document);
        if let Some(composer) = &self.composer {
            self.draw_composer(frame, chunks[1], composer);
        } else if self.search.active {
            self.draw_search_bar(frame, chunks[1], &document);
        }
        if self.switcher.is_some() {
            self.draw_switcher(frame, chunks[0]);
        }
        let footer_pairs = if self.composer.is_some() {
            ATTACH_FOOTER_COMPOSER
        } else if self.search.active {
            ATTACH_FOOTER_SEARCH
        } else if self.follow {
            ATTACH_FOOTER_FOLLOW_ON
        } else {
            ATTACH_FOOTER_FOLLOW_OFF
        };
        draw_dashboard_footer(
            frame,
            chunks[2],
            footer_line_from_pairs(footer_pairs, usize::from(chunks[2].width)),
        );
    }

    fn draw_reply_rail(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let current = current_attach_marker_index(&document.markers, self.scroll);
        let height = usize::from(area.height);
        let start = current
            .map(|index| index.saturating_sub(height / 2))
            .unwrap_or(0)
            .min(document.markers.len().saturating_sub(height));
        let lines = document
            .markers
            .iter()
            .enumerate()
            .skip(start)
            .take(height)
            .map(|(index, marker)| {
                let selected = Some(index) == current;
                let reply_marker = marker.label.contains("reply") || marker.label.contains("codex");
                Line::from(Span::styled(
                    if selected { "●" } else { "•" },
                    Style::default()
                        .fg(if reply_marker {
                            if selected {
                                Color::Green
                            } else {
                                Color::DarkGray
                            }
                        } else if selected {
                            Color::Green
                        } else {
                            Color::DarkGray
                        })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ))
                .alignment(ratatui::layout::Alignment::Center)
            })
            .collect::<Vec<_>>();
        frame.render_widget(Paragraph::new(Text::from(lines)), area);
    }

    fn draw_transcript(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let agent_label = self
            .agent
            .as_ref()
            .map(|agent| agent.resolved.label())
            .unwrap_or_else(|| "no agent".to_owned());
        let title = dynamic_panel_title(
            format!(
                "Transcript · {agent_label} · follow {} · thinking {}",
                if self.follow { "on" } else { "off" },
                if self.collapse_thinking {
                    "collapsed"
                } else {
                    "shown"
                }
            ),
            (!document.lines.is_empty()).then_some((
                (self.scroll + 1).min(document.lines.len()),
                document.lines.len(),
            )),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let matches = attach_search_matches(document, &self.search.query);
        let mut visible = document
            .lines
            .iter()
            .enumerate()
            .skip(self.scroll)
            .map(|(index, line)| {
                let mut line = line.clone();
                if matches.contains(&index) {
                    let selected = index == self.scroll;
                    line = line.patch_style(
                        Style::default()
                            .bg(if selected {
                                Color::Blue
                            } else {
                                Color::DarkGray
                            })
                            .add_modifier(if selected {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    );
                }
                line
            })
            .collect::<Vec<_>>();
        if let Some(message) = &self.message {
            visible.insert(
                0,
                Line::from(vec![
                    Span::styled("notice ", Style::default().fg(Color::Yellow)),
                    Span::styled(message.clone(), Style::default().fg(Color::White)),
                ]),
            );
        }
        let visible = crate::tui_text::wrap_lines(&visible, usize::from(inner.width));
        frame.render_widget(Paragraph::new(Text::from(visible)), inner);
        render_scrollbar(frame, area, document.lines.len(), self.scroll);
    }

    fn draw_composer(&self, frame: &mut Frame<'_>, area: Rect, composer: &AttachComposer) {
        let block = dashboard_block("Follow-Up").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let lines = composer
            .lines
            .iter()
            .enumerate()
            .map(|(line_index, line)| {
                let mut text = line.clone();
                if line_index == composer.cursor_line {
                    insert_cursor_marker(&mut text, composer.cursor_col);
                }
                Line::from(Span::styled(text, Style::default().fg(Color::White)))
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_search_bar(&self, frame: &mut Frame<'_>, area: Rect, document: &AttachDocument) {
        let matches = attach_search_matches(document, &self.search.query);
        let status = if self.search.query.trim().is_empty() {
            "type to search".to_owned()
        } else if matches.is_empty() {
            "no matches".to_owned()
        } else {
            format!(
                "{} matches · current {}",
                matches.len(),
                attach_match_position(&matches, self.scroll)
            )
        };
        let block =
            dashboard_block("Transcript Search").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("/", Style::default().fg(Color::Magenta)),
                Span::styled(self.search.query.clone(), Style::default().fg(Color::White)),
                Span::styled("  ", Style::default()),
                Span::styled(status, Style::default().fg(Color::DarkGray)),
            ])),
            inner,
        );
    }

    fn draw_switcher(&self, frame: &mut Frame<'_>, area: Rect) {
        let agents = self.switcher_agents().unwrap_or_default();
        let switcher = self.switcher.as_ref().expect("switcher is active");
        let popup = centered_popup(
            area,
            DASHBOARD_PALETTE_MAX_WIDTH.min(area.width.saturating_sub(4)),
            DASHBOARD_PALETTE_MAX_HEIGHT.min(area.height.saturating_sub(4)),
        );
        let block =
            dashboard_block("Switch Agent").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        let mut lines = vec![Line::from(vec![
            Span::styled("query ", Style::default().fg(Color::DarkGray)),
            Span::styled(switcher.query.clone(), Style::default().fg(Color::White)),
        ])];
        lines.push(Line::from(""));
        lines.extend(
            agents
                .iter()
                .enumerate()
                .take(usize::from(inner.height.saturating_sub(2)))
                .map(|(index, agent)| {
                    let selected = index == switcher.selected;
                    Line::from(vec![
                        dashboard_span(
                            if selected { "▸ " } else { "  " },
                            Style::default().fg(Color::White),
                            selected,
                        ),
                        dashboard_span(
                            agent.resolved.label(),
                            dashboard_agent_style(agent.status, false),
                            selected,
                        ),
                        dashboard_span(" · ", Style::default().fg(Color::DarkGray), selected),
                        dashboard_span(
                            agent.summary.clone(),
                            Style::default().fg(Color::Gray),
                            selected,
                        ),
                    ])
                }),
        );
        frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    }
}

impl AttachComposer {
    fn new(history: Vec<String>) -> Self {
        Self {
            lines: vec![String::new()],
            cursor_line: 0,
            cursor_col: 0,
            history,
            history_index: None,
            draft_before_history: String::new(),
        }
    }

    fn text(&self) -> String {
        self.lines.join("\n")
    }

    fn insert(&mut self, ch: char) {
        self.history_index = None;
        let line = &mut self.lines[self.cursor_line];
        let byte = char_to_byte_index(line, self.cursor_col);
        line.insert(byte, ch);
        self.cursor_col += 1;
    }

    fn insert_newline(&mut self) {
        self.history_index = None;
        let line = &mut self.lines[self.cursor_line];
        let byte = char_to_byte_index(line, self.cursor_col);
        let tail = line.split_off(byte);
        self.cursor_line += 1;
        self.cursor_col = 0;
        self.lines.insert(self.cursor_line, tail);
    }

    fn backspace(&mut self) {
        self.history_index = None;
        if self.cursor_col > 0 {
            let line = &mut self.lines[self.cursor_line];
            let start = char_to_byte_index(line, self.cursor_col - 1);
            let end = char_to_byte_index(line, self.cursor_col);
            line.replace_range(start..end, "");
            self.cursor_col -= 1;
        } else if self.cursor_line > 0 {
            let removed = self.lines.remove(self.cursor_line);
            self.cursor_line -= 1;
            self.cursor_col = self.lines[self.cursor_line].chars().count();
            self.lines[self.cursor_line].push_str(&removed);
        }
    }

    fn move_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor_line > 0 {
            self.cursor_line -= 1;
            self.cursor_col = self.lines[self.cursor_line].chars().count();
        }
    }

    fn move_right(&mut self) {
        if self.cursor_col < self.lines[self.cursor_line].chars().count() {
            self.cursor_col += 1;
        } else if self.cursor_line + 1 < self.lines.len() {
            self.cursor_line += 1;
            self.cursor_col = 0;
        }
    }

    fn move_vertical(&mut self, delta: isize) {
        let next = if delta < 0 {
            self.cursor_line.saturating_sub(delta.unsigned_abs())
        } else {
            (self.cursor_line + delta as usize).min(self.lines.len() - 1)
        };
        self.cursor_line = next;
        self.cursor_col = self
            .cursor_col
            .min(self.lines[self.cursor_line].chars().count());
    }

    fn clear(&mut self) {
        self.history_index = None;
        self.lines = vec![String::new()];
        self.cursor_line = 0;
        self.cursor_col = 0;
    }

    fn history_previous(&mut self) {
        if self.history.is_empty() {
            return;
        }
        if self.history_index.is_none() {
            self.draft_before_history = self.text();
            self.history_index = Some(self.history.len() - 1);
        } else if let Some(index) = self.history_index {
            self.history_index = Some(index.saturating_sub(1));
        }
        self.load_history_entry();
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            self.history_index = Some(index + 1);
            self.load_history_entry();
        } else {
            let draft = self.draft_before_history.clone();
            self.history_index = None;
            self.set_text(&draft);
        }
    }

    fn load_history_entry(&mut self) {
        if let Some(index) = self.history_index {
            let value = self.history[index].clone();
            self.set_text(&value);
        }
    }

    fn set_text(&mut self, value: &str) {
        self.lines = value.lines().map(str::to_owned).collect::<Vec<_>>();
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        self.cursor_line = self.lines.len() - 1;
        self.cursor_col = self.lines[self.cursor_line].chars().count();
    }
}

fn char_to_byte_index(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(index, _)| index)
        .unwrap_or(text.len())
}

fn insert_cursor_marker(text: &mut String, cursor_col: usize) {
    let byte = char_to_byte_index(text, cursor_col);
    text.insert(byte, '▏');
}

fn load_attach_viewer_agents(
    project: &ProjectPaths,
    scope: &AttachScope,
) -> Result<Vec<AttachViewerAgent>> {
    let roles = match scope {
        AttachScope::Project => list_roles(project)?,
        AttachScope::Role(role) => vec![role.clone()],
        AttachScope::Agent(agent) => vec![agent.role.clone()],
    };
    let mut agents = Vec::new();
    for role in roles {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        for agent in list_agents(&role_paths)? {
            if let AttachScope::Agent(target) = scope
                && (target.role != role || target.agent != agent)
            {
                continue;
            }
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived {
                continue;
            }
            let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
            agents.push(AttachViewerAgent {
                resolved: ResolvedAgent {
                    role: role.clone(),
                    agent,
                },
                status: state.status,
                summary,
                detail,
                run_count: state.run_count,
                updated_at: state.updated_at,
            });
        }
    }
    agents.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.resolved.label().cmp(&right.resolved.label()))
    });
    Ok(agents)
}

fn attach_document(
    project: &ProjectPaths,
    agent: &AttachViewerAgent,
    collapse_thinking: bool,
) -> Result<AttachDocument> {
    let role_paths = RolePaths::new(project.clone(), agent.resolved.role.clone());
    let agent_paths = role_paths.agent(agent.resolved.agent.clone());
    let mut document = AttachDocument {
        lines: vec![
            Line::from(vec![
                Span::styled("agent ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    agent.resolved.label(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    agent.status.to_string(),
                    dashboard_agent_style(agent.status, false),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(agent.summary.clone(), Style::default().fg(Color::Gray)),
            ]),
            Line::from(vec![
                Span::styled("detail ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&agent.detail).to_owned(),
                    Style::default().fg(Color::Gray),
                ),
                Span::styled(" · updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    event_age(agent.updated_at),
                    Style::default().fg(Color::Gray),
                ),
            ]),
            Line::from(""),
        ],
        markers: Vec::new(),
    };
    let mut any_run = false;
    for run_id in attach_run_ids(agent) {
        let run_paths = agent_paths.run(run_id);
        if !run_paths.root().exists() {
            continue;
        }
        any_run = true;
        push_attach_run(&mut document, &run_paths, run_id, agent, collapse_thinking)?;
    }
    if !any_run {
        document.lines.push(Line::from(Span::styled(
            "No run history has been recorded yet.",
            Style::default().fg(Color::DarkGray),
        )));
    }
    Ok(document)
}

fn attach_run_ids(agent: &AttachViewerAgent) -> Vec<u64> {
    let end = if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
        agent.run_count + 1
    } else {
        agent.run_count
    };
    (1..=end).collect()
}

fn push_attach_run(
    document: &mut AttachDocument,
    run_paths: &crate::state::RunPaths,
    run_id: u64,
    agent: &AttachViewerAgent,
    collapse_thinking: bool,
) -> Result<()> {
    let state = load_agent(&run_paths.agent)?;
    let exit = read_run_exit(run_paths, &state)?;
    let status = exit
        .as_ref()
        .map(|exit| {
            if exit.success {
                "success".to_owned()
            } else {
                format!("failed {}", exit.code)
            }
        })
        .unwrap_or_else(|| {
            if run_id == agent.run_count + 1 {
                "active".to_owned()
            } else {
                "unfinished".to_owned()
            }
        });
    document.markers.push(AttachReplyMarker {
        offset: document.lines.len(),
        label: format!("run {run_id}"),
    });
    document.lines.push(Line::from(vec![
        Span::styled("run ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            run_id.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
        Span::styled(status, Style::default().fg(Color::White)),
        Span::styled(
            exit.as_ref()
                .map(|exit| {
                    format!(
                        " · {} to {}",
                        format_unix_time(exit.started_at),
                        format_unix_time(exit.finished_at)
                    )
                })
                .unwrap_or_default(),
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    if let Some(prompt) = io::read_optional_text(&run_paths.prompt())?
        && let Some(summary) = attach_prompt_summary(&prompt)
    {
        document.lines.push(Line::from(vec![
            Span::styled("prompt ", Style::default().fg(Color::DarkGray)),
            Span::styled(summary, Style::default().fg(Color::Gray)),
        ]));
    }
    if let Some(reply) = io::read_optional_text(&run_paths.reply())?
        && !reply.trim().is_empty()
    {
        document.markers.push(AttachReplyMarker {
            offset: document.lines.len(),
            label: format!("run {run_id} reply"),
        });
        document.lines.push(section_line("reply"));
        document.lines.extend(
            reply
                .lines()
                .map(|line| attach_markdown_line(line, Color::White)),
        );
    }
    let transcript = io::read_optional_text(&run_paths.transcript_text())?.unwrap_or_default();
    if transcript.trim().is_empty() {
        document.lines.push(Line::from(Span::styled(
            "No transcript has been recorded for this run.",
            Style::default().fg(Color::DarkGray),
        )));
        document.lines.push(Line::from(""));
        return Ok(());
    }
    document.lines.push(section_line("transcript"));
    for block in crate::transcript::parse(&transcript) {
        push_attach_transcript_block(document, run_id, block, collapse_thinking);
    }
    document.lines.push(Line::from(""));
    Ok(())
}

fn attach_prompt_summary(prompt: &str) -> Option<String> {
    extract_user_follow_up(prompt)
        .or_else(|| {
            let (_, prompt) = prompt.split_once("# Agent-specific prompt")?;
            Some(prompt.trim().to_owned())
        })
        .or_else(|| {
            prompt
                .lines()
                .find(|line| {
                    let line = line.trim();
                    !line.is_empty() && !line.starts_with('#')
                })
                .map(str::to_owned)
        })
        .map(|summary| compact_single_line(&summary, 180))
}

fn compact_single_line(text: &str, limit: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        return compact;
    }
    let mut shortened = compact
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    shortened.push('…');
    shortened
}

fn push_attach_transcript_block(
    document: &mut AttachDocument,
    run_id: u64,
    block: TranscriptBlock,
    collapse_thinking: bool,
) {
    let kind = if block.kind == TranscriptKind::Assistant
        && crate::transcript::block_looks_like_thinking(&block)
    {
        TranscriptKind::Thinking
    } else {
        block.kind
    };
    if kind == TranscriptKind::Thinking && collapse_thinking {
        let line_count = block
            .lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .count();
        document.lines.push(Line::from(vec![
            Span::styled("  ◌ ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("thinking collapsed · {line_count} lines · press t to show"),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        return;
    }
    if matches!(kind, TranscriptKind::Assistant | TranscriptKind::Thinking) {
        document.markers.push(AttachReplyMarker {
            offset: document.lines.len(),
            label: format!("run {run_id} {}", block.label),
        });
    }
    document
        .lines
        .push(attach_transcript_header_line(kind, &block.label));
    document.lines.extend(
        block
            .lines
            .iter()
            .map(|line| attach_transcript_content_line(kind, line)),
    );
}

fn attach_transcript_header_line(kind: TranscriptKind, label: &str) -> Line<'static> {
    let color = match kind {
        TranscriptKind::Header => Color::Blue,
        TranscriptKind::User => Color::Magenta,
        TranscriptKind::Assistant => Color::Green,
        TranscriptKind::Exec => Color::Yellow,
        TranscriptKind::Thinking => Color::DarkGray,
    };
    Line::from(vec![
        Span::styled("  ▸ ", Style::default().fg(color)),
        Span::styled(
            label.to_owned(),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

fn attach_transcript_content_line(kind: TranscriptKind, line: &str) -> Line<'static> {
    match kind {
        TranscriptKind::Assistant => attach_markdown_line(line, Color::White),
        TranscriptKind::User => prefixed_attach_line("    ", line, Color::Gray),
        TranscriptKind::Exec => attach_exec_line(line),
        TranscriptKind::Thinking => prefixed_attach_line("    ", line, Color::DarkGray),
        TranscriptKind::Header => prefixed_attach_line("    ", line, Color::DarkGray),
    }
}

fn attach_markdown_line(line: &str, color: Color) -> Line<'static> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') {
        Line::from(Span::styled(
            line.to_owned(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
    } else if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
        prefixed_attach_line("  ", line, Color::White)
    } else if trimmed.starts_with("```") {
        prefixed_attach_line("", line, Color::Yellow)
    } else {
        prefixed_attach_line("", line, color)
    }
}

fn attach_exec_line(line: &str) -> Line<'static> {
    let color = match crate::transcript::classify_line(line) {
        TranscriptLineKind::Success => Color::Green,
        TranscriptLineKind::Failure | TranscriptLineKind::Error => Color::Red,
        TranscriptLineKind::Quota => Color::Yellow,
        TranscriptLineKind::Command => Color::Cyan,
        TranscriptLineKind::Path => Color::Blue,
        TranscriptLineKind::Plain => Color::Gray,
    };
    prefixed_attach_line("    ", line, color)
}

fn prefixed_attach_line(prefix: &str, line: &str, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(prefix.to_owned(), Style::default().fg(Color::DarkGray)),
        Span::styled(line.to_owned(), Style::default().fg(color)),
    ])
}

fn current_attach_marker_index(markers: &[AttachReplyMarker], scroll: usize) -> Option<usize> {
    markers
        .iter()
        .enumerate()
        .take_while(|(_, marker)| marker.offset <= scroll)
        .map(|(index, _)| index)
        .last()
}

fn attach_search_matches(document: &AttachDocument, query: &str) -> Vec<usize> {
    crate::tui_text::search_matches(&document.lines, query)
}

fn attach_match_position(matches: &[usize], scroll: usize) -> usize {
    crate::tui_text::match_position(matches, scroll)
}

fn run_dashboard(
    project: ProjectPaths,
    role_filter: Option<RoleSlug>,
    include_archived: bool,
    refresh: Duration,
) -> Result<()> {
    let mut app = DashboardApp::new(project.clone(), role_filter, include_archived);
    let mut terminal = Some(TerminalSession::enter()?);
    let mut next_refresh = Instant::now();
    loop {
        app.tick_frame();
        if Instant::now() >= next_refresh {
            app.refresh()?;
            next_refresh = Instant::now() + refresh;
        }
        terminal
            .as_mut()
            .expect("dashboard terminal is active")
            .draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_FRAME_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(action) = app.handle_key(key)?
        {
            let suspend_terminal = action.suspends_terminal();
            if suspend_terminal {
                drop(terminal.take());
            }
            match run_dashboard_action(&project, action)? {
                DashboardActionOutcome::Continue(toast) => {
                    app.toast = toast;
                    if suspend_terminal {
                        terminal = Some(TerminalSession::enter()?);
                    }
                }
                DashboardActionOutcome::Exit => return Ok(()),
            }
        }
    }
}

enum DashboardActionOutcome {
    Continue(Option<DashboardToast>),
    Exit,
}

fn run_dashboard_action(
    project: &ProjectPaths,
    action: DashboardAction,
) -> Result<DashboardActionOutcome> {
    let outcome = (|| -> Result<DashboardActionOutcome> {
        Ok(match action {
            DashboardAction::Quit => DashboardActionOutcome::Exit,
            DashboardAction::AttachRole(role) => {
                run_attach_viewer(project, AttachTarget::Role(role))?;
                DashboardActionOutcome::Continue(Some(DashboardToast::info(
                    "attach session returned",
                )))
            }
            DashboardAction::AttachAgent(agent) => {
                run_attach_viewer(project, AttachTarget::Agent(agent))?;
                DashboardActionOutcome::Continue(Some(DashboardToast::info(
                    "attach session returned",
                )))
            }
            DashboardAction::More(agent) => {
                more_agent(MoreArgs {
                    agent: Some(AgentSpec {
                        role: Some(agent.role),
                        agent: agent.agent,
                    }),
                    query: None,
                    new: false,
                })?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "agent follow-up completed",
                )))
            }
            DashboardAction::New(role) => {
                let label = role.to_string();
                new_agent(
                    project,
                    NewAgentArgs {
                        role: Some(role),
                        prompt: None,
                        no_prompt: false,
                        attach: false,
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "new `{label}` agent started"
                ))))
            }
            DashboardAction::NewRole => {
                draft_role(
                    project,
                    RoleDraftArgs {
                        role: None,
                        request: None,
                        feedback: Vec::new(),
                        no_review: false,
                        active: false,
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "role draft workflow completed",
                )))
            }
            DashboardAction::Archive(agent) => {
                let label = agent.label();
                archive_agent(
                    project,
                    AgentSelectorArgs {
                        agent: Some(AgentSpec {
                            role: Some(agent.role),
                            agent: agent.agent,
                        }),
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "archived {label}"
                ))))
            }
            DashboardAction::TriggerRole(role) => {
                launch_triggered_role(
                    project,
                    &role,
                    &TriggerLaunch::Queued {
                        queue: role.to_string(),
                    },
                    TriggerCause::Manual {
                        reason: Some("triggered from think status".to_owned()),
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "trigger queued for `{role}`"
                ))))
            }
            DashboardAction::Update => {
                generate_project_update(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project update generated",
                )))
            }
            DashboardAction::Check => {
                check_project(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project check completed",
                )))
            }
            DashboardAction::Assist => {
                assist_project_interactive(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project assist completed",
                )))
            }
            DashboardAction::OpenProject => {
                open_project_directory_at(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project directory opened",
                )))
            }
            DashboardAction::OpenPath { path, label } => {
                open_path(&path)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "opened {label}"
                ))))
            }
            DashboardAction::NewChannel => {
                let channel = selection::resolve_or_prompt_new_slug(
                    None::<ChannelSlug>,
                    "Channel to create",
                )?;
                create_channel(project, &channel)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "channel `{channel}` ready"
                ))))
            }
            DashboardAction::CodexLogin => {
                codex_provider_login(CodexLoginArgs {
                    account: None,
                    home: None,
                })?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "Codex account login completed",
                )))
            }
            DashboardAction::CodexConfig => {
                codex_provider_config_interactive(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "Codex provider config updated",
                )))
            }
            DashboardAction::ToggleRole(role) => {
                toggle_role(project, &role)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "toggled role `{role}`"
                ))))
            }
            DashboardAction::ToggleAgent(agent) => {
                let label = agent.label();
                toggle_agent(project, &agent)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "toggled agent `{label}`"
                ))))
            }
            DashboardAction::ToggleAllRoles => {
                toggle_all_roles(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "role toggle completed",
                )))
            }
        })
    })();
    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(err) if crate::terminal_editor::is_cancelled(&err) => Ok(
            DashboardActionOutcome::Continue(Some(DashboardToast::info(
                crate::terminal_editor::cancellation_message(&err)
                    .unwrap_or_else(|| "cancelled".to_owned()),
            ))),
        ),
        Err(err) => Err(err),
    }
}

#[derive(Clone)]
enum DashboardAction {
    Quit,
    AttachRole(RoleSlug),
    AttachAgent(ResolvedAgent),
    More(ResolvedAgent),
    New(RoleSlug),
    NewRole,
    Archive(ResolvedAgent),
    TriggerRole(RoleSlug),
    Update,
    Check,
    Assist,
    OpenProject,
    OpenPath { path: PathBuf, label: String },
    NewChannel,
    CodexLogin,
    CodexConfig,
    ToggleRole(RoleSlug),
    ToggleAgent(ResolvedAgent),
    ToggleAllRoles,
}

impl DashboardAction {
    fn suspends_terminal(&self) -> bool {
        matches!(
            self,
            Self::AttachRole(_)
                | Self::AttachAgent(_)
                | Self::More(_)
                | Self::New(_)
                | Self::NewRole
                | Self::Update
                | Self::Check
                | Self::Assist
                | Self::NewChannel
                | Self::CodexLogin
                | Self::CodexConfig
        )
    }
}

#[derive(Default)]
enum DashboardOverlay {
    #[default]
    None,
    Advanced,
    Help,
    Search {
        target: SearchTarget,
        query: String,
    },
    Palette {
        query: String,
        selected: usize,
    },
    Detail {
        extended: bool,
        scroll: u16,
    },
    QueueDetail {
        selection: QueueSelection,
        scroll: u16,
    },
    ChannelDetail {
        channel_index: usize,
        scroll: u16,
    },
    NoticeDetail {
        scroll: u16,
    },
    UpdateDetail {
        update: String,
        scroll: u16,
    },
    ProviderSettings {
        selected: usize,
    },
}

#[derive(Clone, Copy)]
enum DashboardTab {
    Dashboard,
    Channels,
    Queues,
    EventTimeline,
    Updates,
}

impl DashboardTab {
    fn next(self) -> Self {
        match self {
            Self::Dashboard => Self::Channels,
            Self::Channels => Self::Queues,
            Self::Queues => Self::EventTimeline,
            Self::EventTimeline => Self::Updates,
            Self::Updates => Self::Dashboard,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Dashboard => Self::Updates,
            Self::Channels => Self::Dashboard,
            Self::Queues => Self::Channels,
            Self::EventTimeline => Self::Queues,
            Self::Updates => Self::EventTimeline,
        }
    }
}

#[derive(Clone, Copy)]
enum SearchTarget {
    Agents,
    Channels,
    Queues,
    Events,
}

impl SearchTarget {
    fn title(self) -> &'static str {
        match self {
            Self::Agents => "Filter Agents",
            Self::Channels => "Filter Channels",
            Self::Queues => "Filter Queues",
            Self::Events => "Filter Events",
        }
    }

    fn visible_label(self) -> &'static str {
        match self {
            Self::Agents => "visible agents",
            Self::Channels => "visible channels",
            Self::Queues => "visible rows",
            Self::Events => "visible events",
        }
    }
}

#[derive(Clone, Copy)]
enum PaletteCommand {
    Detail,
    New,
    More,
    Attach,
    Search,
    Channels,
    Queues,
    ToggleQueueCollapsed,
    Events,
    Updates,
    Advanced,
    Update,
    Check,
    Assist,
    OpenProject,
    OpenSelectedDir,
    NewChannel,
    ToggleRole,
    ToggleAllRoles,
    TriggerRole,
    ArchiveAgent,
    ToggleArchived,
    RetryErrored,
    ProviderSettings,
    Help,
    Quit,
}

struct PaletteEntry {
    command: PaletteCommand,
    label: String,
    detail: String,
    key: Option<&'static str>,
}

struct DashboardCommandSpec {
    command: PaletteCommand,
    key: Option<&'static str>,
    label: &'static str,
    detail: &'static str,
}

const DASHBOARD_COMMANDS: &[DashboardCommandSpec] = &[
    DashboardCommandSpec {
        command: PaletteCommand::Detail,
        key: Some("Enter"),
        label: "detail",
        detail: "open focused detail for the selected role or agent",
    },
    DashboardCommandSpec {
        command: PaletteCommand::New,
        key: Some("n"),
        label: "new agent",
        detail: "create an agent for the selected or default role",
    },
    DashboardCommandSpec {
        command: PaletteCommand::More,
        key: Some("m"),
        label: "more",
        detail: "continue the selected inactive agent with a follow-up query",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Attach,
        key: Some("a"),
        label: "attach",
        detail: "open the transcript viewer for the selected role or agent",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Search,
        key: Some("/"),
        label: "search",
        detail: "filter the current dashboard tab",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Channels,
        key: Some("l"),
        label: "channels",
        detail: "show publish channels and alert artifacts",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Queues,
        key: Some("o"),
        label: "queues",
        detail: "show ordered trigger queues",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ToggleQueueCollapsed,
        key: Some("Space"),
        label: "collapse queue",
        detail: "collapse or expand the selected queue",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Events,
        key: Some("Tab"),
        label: "timeline",
        detail: "switch to the full-width timeline of recent project events",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Updates,
        key: Some("u"),
        label: "updates",
        detail: "open persisted project updates",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Advanced,
        key: Some("A"),
        label: "advanced",
        detail: "open advanced maintenance actions",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Update,
        key: Some("u"),
        label: "new update",
        detail: "ask Codex for a compact critical project update",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Check,
        key: Some("c"),
        label: "technical check",
        detail: "ask Codex to inspect project health without permanent changes",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Assist,
        key: Some("i"),
        label: "assist",
        detail: "ask Codex to operate this think project with full context",
    },
    DashboardCommandSpec {
        command: PaletteCommand::OpenProject,
        key: Some("A o"),
        label: "open project",
        detail: "open the current project directory",
    },
    DashboardCommandSpec {
        command: PaletteCommand::OpenSelectedDir,
        key: None,
        label: "open selected dir",
        detail: "open the selected role, agent, or channel directory",
    },
    DashboardCommandSpec {
        command: PaletteCommand::NewChannel,
        key: None,
        label: "new channel",
        detail: "create a publish channel",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ToggleRole,
        key: Some("Space"),
        label: "pause/unpause",
        detail: "pause or unpause the selected role or agent",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ToggleAllRoles,
        key: Some("U"),
        label: "toggle all roles",
        detail: "pause all active roles or unpause all paused roles",
    },
    DashboardCommandSpec {
        command: PaletteCommand::TriggerRole,
        key: None,
        label: "trigger role",
        detail: "enqueue a manual trigger for the selected role",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ArchiveAgent,
        key: None,
        label: "archive agent",
        detail: "hide an inactive selected agent while keeping its files",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ToggleArchived,
        key: Some("A x"),
        label: "toggle archived agents",
        detail: "show or hide archived agents in the table",
    },
    DashboardCommandSpec {
        command: PaletteCommand::RetryErrored,
        key: Some("A r"),
        label: "retry errored waits",
        detail: "wake quota, rate-limit, and OOM retry backoffs now",
    },
    DashboardCommandSpec {
        command: PaletteCommand::ProviderSettings,
        key: Some("A p"),
        label: "provider settings",
        detail: "open Codex account, model, and thinking settings",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Help,
        key: Some("?"),
        label: "help",
        detail: "show dashboard keys",
    },
    DashboardCommandSpec {
        command: PaletteCommand::Quit,
        key: Some("q"),
        label: "quit",
        detail: "leave the dashboard",
    },
];

#[derive(Clone)]
struct ProviderAccountRow {
    name: String,
    active: bool,
    codex_home: PathBuf,
    quota_wait_until: Option<u64>,
    last_quota_event: Option<String>,
    last_used_at: Option<u64>,
}

enum QuotaLoadState {
    Loading,
    Ready(Option<CodexRateLimits>),
}

#[derive(Clone)]
struct DashboardToast {
    level: ToastLevel,
    text: String,
    created_at: Instant,
}

#[derive(Clone, Copy)]
enum ToastLevel {
    Info,
    Success,
    Warn,
}

impl DashboardToast {
    fn info(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Info,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn success(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Success,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn warn(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Warn,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn is_visible(&self) -> bool {
        self.created_at.elapsed() < DASHBOARD_TOAST_TTL
    }
}

impl ToastLevel {
    fn style(self) -> Style {
        match self {
            Self::Info => Style::default().fg(Color::Cyan).bg(Color::Black),
            Self::Success => Style::default().fg(Color::Green).bg(Color::Black),
            Self::Warn => Style::default().fg(Color::Yellow).bg(Color::Black),
        }
    }
}

const DASHBOARD_UI_STATE_VERSION: u32 = 2;

#[derive(Debug, Default, Serialize, Deserialize)]
struct DashboardUiState {
    version: u32,
    selected_role: Option<RoleSlug>,
    selected_agent: Option<AgentId>,
    agents_scroll: usize,
    selected_offset: usize,
    #[serde(default)]
    filter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seen_alert_latest: Option<String>,
}

#[derive(Clone)]
struct DashboardSelectionKey {
    role: RoleSlug,
    agent: Option<AgentId>,
    selected_offset: usize,
}

struct DashboardApp {
    project: ProjectPaths,
    role_filter: Option<RoleSlug>,
    include_archived: bool,
    selected: usize,
    agents_scroll: usize,
    last_agents_height: usize,
    restore_selection: Option<DashboardSelectionKey>,
    filter: String,
    active_tab: DashboardTab,
    overlay: DashboardOverlay,
    toast: Option<DashboardToast>,
    snapshot: DashboardSnapshot,
    quota: QuotaLoadState,
    quota_probe: Option<thread::JoinHandle<Option<CodexRateLimits>>>,
    event_scroll: u16,
    event_selected: usize,
    event_filter: EventFilter,
    event_query: String,
    event_selected_key: Option<ProjectEventKey>,
    update_scroll: u16,
    update_selected: usize,
    update_selected_key: Option<String>,
    channel_query: String,
    channel_scroll: u16,
    channel_selected: usize,
    queue_query: String,
    queue_scroll: u16,
    queue_selected: usize,
    queue_selected_key: Option<StatusQueueKey>,
    collapsed_queues: BTreeSet<String>,
    detail_scrolls: BTreeMap<String, u16>,
    last_queue_height: usize,
    last_state_signature: Option<String>,
    frame_tick: usize,
    seen_alert_latest: Option<String>,
}

impl DashboardApp {
    fn new(project: ProjectPaths, role_filter: Option<RoleSlug>, include_archived: bool) -> Self {
        let ui_state = load_dashboard_ui_state(&project).unwrap_or_default();
        let DashboardUiState {
            selected_role,
            selected_agent,
            agents_scroll,
            selected_offset,
            filter,
            seen_alert_latest,
            ..
        } = ui_state;
        let restore_selection = selected_role.map(|role| DashboardSelectionKey {
            role,
            agent: selected_agent,
            selected_offset,
        });
        let codex_config = project_config(&project)
            .map(|config| config.providers.codex)
            .unwrap_or_default();
        Self {
            project,
            role_filter,
            include_archived,
            selected: 0,
            agents_scroll,
            last_agents_height: DASHBOARD_AGENTS_MIN_HEIGHT as usize,
            restore_selection,
            filter,
            active_tab: DashboardTab::Dashboard,
            overlay: DashboardOverlay::None,
            toast: None,
            snapshot: DashboardSnapshot::default(),
            quota: QuotaLoadState::Loading,
            quota_probe: Some(thread::spawn(move || {
                crate::provider::codex::load_active_rate_limits(&codex_config)
            })),
            event_scroll: 0,
            event_selected: 0,
            event_filter: EventFilter::All,
            event_query: String::new(),
            event_selected_key: None,
            update_scroll: 0,
            update_selected: 0,
            update_selected_key: None,
            channel_query: String::new(),
            channel_scroll: 0,
            channel_selected: 0,
            queue_query: String::new(),
            queue_scroll: 0,
            queue_selected: 0,
            queue_selected_key: None,
            collapsed_queues: BTreeSet::new(),
            detail_scrolls: BTreeMap::new(),
            last_queue_height: DASHBOARD_AGENTS_MIN_HEIGHT as usize,
            last_state_signature: None,
            frame_tick: 0,
            seen_alert_latest,
        }
    }

    fn tick_frame(&mut self) {
        self.frame_tick = self.frame_tick.wrapping_add(1);
        self.expire_toast();
    }

    fn refresh(&mut self) -> Result<()> {
        self.expire_toast();
        let selected = self.selected_key();
        let queue_key = self.selected_queue_key();
        wake_project(&self.project)?;
        fire_queue_idle_triggers(&self.project)?;
        ensure_notice_task_started(&self.project)?;
        self.refresh_quota();
        self.snapshot = load_dashboard_snapshot(
            &self.project,
            self.role_filter.as_ref(),
            self.include_archived,
        )?;
        let state_signature = self.snapshot.state_signature();
        if self
            .last_state_signature
            .as_ref()
            .is_some_and(|last| *last != state_signature)
        {
            force_notice_task_started(&self.project)?;
            self.snapshot.notices_loading = true;
        }
        self.last_state_signature = Some(state_signature);
        if self.restore_selection.is_none() {
            self.restore_selection = selected;
        }
        self.restore_selection_if_pending();
        self.restore_queue_selection(queue_key);
        self.ensure_selectable_selection();
        Ok(())
    }

    fn expire_toast(&mut self) {
        if self.toast.as_ref().is_some_and(|toast| !toast.is_visible()) {
            self.toast = None;
        }
    }

    fn refresh_quota(&mut self) {
        if self
            .quota_probe
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            let handle = self.quota_probe.take().expect("quota probe was checked");
            self.quota = QuotaLoadState::Ready(handle.join().unwrap_or(None));
        }
    }

    fn set_toast(&mut self, toast: DashboardToast) {
        self.toast = Some(toast);
    }

    fn open_events(&mut self) {
        self.overlay = DashboardOverlay::None;
        self.active_tab = DashboardTab::EventTimeline;
        self.restore_event_selection();
    }

    fn open_queues(&mut self) {
        self.overlay = DashboardOverlay::None;
        self.active_tab = DashboardTab::Queues;
        self.restore_queue_selection(self.selected_queue_key());
    }

    fn open_channels(&mut self) {
        self.overlay = DashboardOverlay::None;
        self.active_tab = DashboardTab::Channels;
        self.restore_channel_selection();
        self.mark_alerts_seen();
    }

    fn open_dashboard(&mut self) {
        self.overlay = DashboardOverlay::None;
        self.active_tab = DashboardTab::Dashboard;
    }

    fn open_updates(&mut self) {
        self.overlay = DashboardOverlay::None;
        self.active_tab = DashboardTab::Updates;
        self.restore_update_selection();
    }

    fn open_provider_settings(&mut self) {
        self.overlay = DashboardOverlay::ProviderSettings {
            selected: self.provider_active_account_index(),
        };
    }

    fn provider_account_rows(&self) -> Result<Vec<ProviderAccountRow>> {
        let state = crate::provider::codex::list_accounts()?;
        Ok(state
            .accounts
            .into_iter()
            .map(|(name, account)| ProviderAccountRow {
                active: name == state.active_account,
                name,
                codex_home: account.codex_home,
                quota_wait_until: account.quota_wait_until,
                last_quota_event: account.last_quota_event,
                last_used_at: account.last_used_at,
            })
            .collect())
    }

    fn provider_active_account_index(&self) -> usize {
        self.provider_account_rows()
            .ok()
            .and_then(|rows| rows.iter().position(|row| row.active))
            .unwrap_or(0)
    }

    fn open_detail_overlay(&mut self, extended: bool) {
        self.overlay = DashboardOverlay::Detail {
            extended,
            scroll: self.remembered_detail_scroll(),
        };
    }

    fn keep_detail_overlay(&mut self, extended: bool, scroll: u16) {
        self.remember_detail_scroll(scroll);
        self.overlay = DashboardOverlay::Detail { extended, scroll };
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Result<Option<DashboardAction>> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            self.persist_ui_state()?;
            return Ok(Some(DashboardAction::Quit));
        }
        if !matches!(self.overlay, DashboardOverlay::None) {
            return self.handle_overlay_key(key);
        }
        if matches!(key.code, KeyCode::Tab | KeyCode::BackTab) {
            self.active_tab = if matches!(key.code, KeyCode::BackTab) {
                self.active_tab.previous()
            } else {
                self.active_tab.next()
            };
            if matches!(self.active_tab, DashboardTab::EventTimeline) {
                self.restore_event_selection();
            } else if matches!(self.active_tab, DashboardTab::Channels) {
                self.restore_channel_selection();
                self.mark_alerts_seen();
            } else if matches!(self.active_tab, DashboardTab::Updates) {
                self.restore_update_selection();
            }
            return Ok(None);
        }
        if matches!(self.active_tab, DashboardTab::Channels) {
            return self.handle_channel_tab_key(key);
        }
        if matches!(self.active_tab, DashboardTab::Queues) {
            return self.handle_queue_tab_key(key);
        }
        if matches!(self.active_tab, DashboardTab::EventTimeline) {
            return self.handle_event_tab_key(key);
        }
        if matches!(self.active_tab, DashboardTab::Updates) {
            return self.handle_update_tab_key(key);
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Home => {
                self.select_first();
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::End => {
                self.select_last();
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection_by(-1);
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection_by(1);
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::PageUp => {
                self.move_selection_by(-self.page_step());
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::PageDown => {
                self.move_selection_by(self.page_step());
                self.persist_ui_state()?;
                Ok(None)
            }
            KeyCode::Char('?') => {
                self.overlay = DashboardOverlay::Help;
                Ok(None)
            }
            KeyCode::Char(':') => {
                self.overlay = DashboardOverlay::Palette {
                    query: String::new(),
                    selected: 0,
                };
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Agents,
                    query: self.filter.clone(),
                };
                Ok(None)
            }
            KeyCode::Char('o') => {
                self.open_queues();
                Ok(None)
            }
            KeyCode::Char('l') => {
                self.open_channels();
                Ok(None)
            }
            KeyCode::Char('u') => {
                self.open_updates();
                Ok(None)
            }
            KeyCode::Char('!') => {
                if self.snapshot.notices.is_empty() && self.has_unseen_alerts() {
                    self.open_channels();
                } else if self.snapshot.notices.is_empty() {
                    self.set_toast(DashboardToast::info("no operator notices"));
                } else {
                    self.overlay = DashboardOverlay::NoticeDetail { scroll: 0 };
                }
                Ok(None)
            }
            KeyCode::Enter => {
                self.open_detail_overlay(false);
                Ok(None)
            }
            KeyCode::Char('a') => Ok(self.selected_attach_action()),
            KeyCode::Char('m') => Ok(self.selected_more_action()),
            KeyCode::Char('n') => Ok(self.selected_new_action()),
            KeyCode::Char('c') => Ok(Some(DashboardAction::Check)),
            KeyCode::Char('i') => Ok(Some(DashboardAction::Assist)),
            KeyCode::Char(' ') => Ok(self.selected_pause_action()),
            KeyCode::Char('U') => Ok(Some(DashboardAction::ToggleAllRoles)),
            KeyCode::Char('A') => {
                self.overlay = DashboardOverlay::Advanced;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_overlay_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        match std::mem::take(&mut self.overlay) {
            DashboardOverlay::None => Ok(None),
            DashboardOverlay::Advanced => self.handle_advanced_key(key),
            DashboardOverlay::Help => self.handle_help_key(key),
            DashboardOverlay::Search { target, mut query } => {
                self.handle_search_key(key, target, &mut query)
            }
            DashboardOverlay::Palette {
                mut query,
                mut selected,
            } => self.handle_palette_key(key, &mut query, &mut selected),
            DashboardOverlay::Detail {
                mut extended,
                mut scroll,
            } => self.handle_detail_key(key, &mut extended, &mut scroll),
            DashboardOverlay::QueueDetail {
                selection,
                mut scroll,
            } => self.handle_queue_detail_key(key, selection, &mut scroll),
            DashboardOverlay::ChannelDetail {
                channel_index,
                mut scroll,
            } => self.handle_channel_detail_key(key, channel_index, &mut scroll),
            DashboardOverlay::NoticeDetail { mut scroll } => {
                self.handle_notice_detail_key(key, &mut scroll)
            }
            DashboardOverlay::UpdateDetail { update, mut scroll } => {
                self.handle_update_detail_key(key, update, &mut scroll)
            }
            DashboardOverlay::ProviderSettings { mut selected } => {
                self.handle_provider_settings_key(key, &mut selected)
            }
        }
    }

    fn handle_advanced_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('A') => Ok(None),
            KeyCode::Char('r') => {
                let updated = retry_waits_now_inner(&self.project)?;
                self.set_toast(DashboardToast::success(format!(
                    "retry errored requested; updated {updated} agents"
                )));
                Ok(None)
            }
            KeyCode::Char('x') => {
                self.toggle_archived()?;
                Ok(None)
            }
            KeyCode::Char('n') => Ok(Some(DashboardAction::NewRole)),
            KeyCode::Char('o') => Ok(Some(DashboardAction::OpenProject)),
            KeyCode::Char('p') => {
                self.open_provider_settings();
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::Advanced;
                Ok(None)
            }
        }
    }

    fn handle_provider_settings_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        selected: &mut usize,
    ) -> Result<Option<DashboardAction>> {
        let rows = self.provider_account_rows().unwrap_or_default();
        *selected = (*selected).min(rows.len().saturating_sub(1));
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Up | KeyCode::Char('k') => {
                *selected = selected.saturating_sub(1);
                self.overlay = DashboardOverlay::ProviderSettings {
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *selected = (*selected + 1).min(rows.len().saturating_sub(1));
                self.overlay = DashboardOverlay::ProviderSettings {
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Enter | KeyCode::Char('s') => {
                if let Some(account) = rows.get(*selected) {
                    crate::provider::codex::set_active_account(&account.name)?;
                    self.set_toast(DashboardToast::success(format!(
                        "active Codex account: `{}`",
                        account.name
                    )));
                    self.overlay = DashboardOverlay::ProviderSettings {
                        selected: self.provider_active_account_index(),
                    };
                } else {
                    self.set_toast(DashboardToast::warn("no Codex account selected"));
                    self.overlay = DashboardOverlay::ProviderSettings { selected: 0 };
                }
                Ok(None)
            }
            KeyCode::Char('a') => Ok(Some(DashboardAction::CodexLogin)),
            KeyCode::Char('d') | KeyCode::Delete => {
                if let Some(account) = rows.get(*selected) {
                    match crate::provider::codex::remove_account(&account.name) {
                        Ok(()) => {
                            self.set_toast(DashboardToast::success(format!(
                                "removed Codex account `{}`",
                                account.name
                            )));
                            *selected = (*selected).min(
                                self.provider_account_rows()
                                    .map_or(0, |rows| rows.len())
                                    .saturating_sub(1),
                            );
                        }
                        Err(err) => self.set_toast(DashboardToast::warn(format!("{err:#}"))),
                    }
                } else {
                    self.set_toast(DashboardToast::warn("no Codex account selected"));
                    *selected = 0;
                }
                self.overlay = DashboardOverlay::ProviderSettings {
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Char('m') | KeyCode::Char('c') => Ok(Some(DashboardAction::CodexConfig)),
            KeyCode::Char('r') => {
                *selected = self.provider_active_account_index();
                self.overlay = DashboardOverlay::ProviderSettings {
                    selected: *selected,
                };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::ProviderSettings {
                    selected: *selected,
                };
                Ok(None)
            }
        }
    }

    fn handle_help_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if matches!(
            key.code,
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('?')
        ) {
            Ok(None)
        } else {
            self.overlay = DashboardOverlay::Help;
            Ok(None)
        }
    }

    fn handle_search_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        target: SearchTarget,
        query: &mut String,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.apply_search(target, query)?;
                Ok(None)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                query.clear();
                self.apply_search(target, query)?;
                self.overlay = DashboardOverlay::Search {
                    target,
                    query: query.clone(),
                };
                Ok(None)
            }
            KeyCode::Backspace => {
                query.pop();
                self.apply_search(target, query)?;
                self.overlay = DashboardOverlay::Search {
                    target,
                    query: query.clone(),
                };
                Ok(None)
            }
            KeyCode::Char(ch) => {
                query.push(ch);
                self.apply_search(target, query)?;
                self.overlay = DashboardOverlay::Search {
                    target,
                    query: query.clone(),
                };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::Search {
                    target,
                    query: query.clone(),
                };
                Ok(None)
            }
        }
    }

    fn handle_palette_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        query: &mut String,
        selected: &mut usize,
    ) -> Result<Option<DashboardAction>> {
        let entries = self.palette_entries(query);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Up | KeyCode::Char('k') => {
                *selected = selected.saturating_sub(1);
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !entries.is_empty() {
                    *selected = (*selected + 1).min(entries.len() - 1);
                }
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Backspace => {
                query.pop();
                *selected = (*selected).min(self.palette_entries(query).len().saturating_sub(1));
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                query.clear();
                *selected = 0;
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Char(ch) => {
                query.push(ch);
                *selected = 0;
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
            KeyCode::Enter => {
                let Some(entry) = entries.get((*selected).min(entries.len().saturating_sub(1)))
                else {
                    return Ok(None);
                };
                self.execute_palette_command(entry.command)
            }
            _ => {
                self.overlay = DashboardOverlay::Palette {
                    query: query.clone(),
                    selected: *selected,
                };
                Ok(None)
            }
        }
    }

    fn handle_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        extended: &mut bool,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Char('x') => {
                *extended = !*extended;
                *scroll = 0;
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::Char('a') => Ok(self.selected_attach_action()),
            KeyCode::Char('m') => Ok(self.selected_more_action()),
            KeyCode::Char('n') => Ok(self.selected_new_action()),
            KeyCode::Char(' ') => Ok(self.selected_pause_action()),
            KeyCode::Up | KeyCode::Char('k') => {
                *scroll = scroll.saturating_sub(DASHBOARD_DETAIL_SCROLL_STEP);
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *scroll = scroll.saturating_add(DASHBOARD_DETAIL_SCROLL_STEP);
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::PageUp => {
                *scroll = scroll.saturating_sub(self.last_agents_height as u16);
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::PageDown => {
                *scroll = scroll.saturating_add(self.last_agents_height as u16);
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::Home => {
                *scroll = 0;
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            _ => {
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
        }
    }

    fn handle_queue_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        selection: QueueSelection,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Up | KeyCode::Char('k') => {
                *scroll = scroll.saturating_sub(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *scroll = scroll.saturating_add(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageUp => {
                *scroll = scroll.saturating_sub(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageDown => {
                *scroll = scroll.saturating_add(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::Home => {
                *scroll = 0;
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::QueueDetail {
                    selection,
                    scroll: *scroll,
                };
                Ok(None)
            }
        }
    }

    fn handle_channel_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        channel_index: usize,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Char('o') => {
                let channel = self.snapshot.channels.get(channel_index);
                Ok(channel.map(|channel| DashboardAction::OpenPath {
                    path: self.project.channels_dir().join(&channel.name),
                    label: format!("channel `{}`", channel.name),
                }))
            }
            KeyCode::Up | KeyCode::Char('k') => {
                *scroll = scroll.saturating_sub(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *scroll = scroll.saturating_add(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageUp => {
                *scroll = scroll.saturating_sub(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageDown => {
                *scroll = scroll.saturating_add(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: *scroll,
                };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: *scroll,
                };
                Ok(None)
            }
        }
    }

    fn handle_notice_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Up | KeyCode::Char('k') => {
                *scroll = scroll.saturating_sub(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *scroll = scroll.saturating_add(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
            KeyCode::PageUp => {
                *scroll = scroll.saturating_sub(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
            KeyCode::PageDown => {
                *scroll = scroll.saturating_add(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
            KeyCode::Home => {
                *scroll = 0;
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::NoticeDetail { scroll: *scroll };
                Ok(None)
            }
        }
    }

    fn handle_queue_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Enter => {
                if let Some(selection) = self.queue_selection_rows().get(self.queue_selected) {
                    self.overlay = DashboardOverlay::QueueDetail {
                        selection: *selection,
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Char(' ') => {
                self.toggle_selected_queue_collapsed();
                self.center_queue_selection();
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Queues,
                    query: self.queue_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Esc | KeyCode::Char('o') => {
                self.open_dashboard();
                Ok(None)
            }
            KeyCode::Home => {
                self.queue_selected = 0;
                self.remember_queue_selection();
                Ok(None)
            }
            KeyCode::End => {
                self.queue_selected = self.queue_selection_rows().len().saturating_sub(1);
                self.remember_queue_selection();
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.queue_selected = self.queue_selected.saturating_sub(1);
                self.remember_queue_selection();
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let row_count = self.queue_selection_rows().len();
                if row_count > 0 {
                    self.queue_selected = (self.queue_selected + 1).min(row_count - 1);
                }
                self.remember_queue_selection();
                Ok(None)
            }
            KeyCode::PageUp => {
                self.queue_scroll = self
                    .queue_scroll
                    .saturating_sub(self.last_agents_height as u16);
                Ok(None)
            }
            KeyCode::PageDown => {
                self.queue_scroll = self
                    .queue_scroll
                    .saturating_add(self.last_agents_height as u16);
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_channel_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        let indices = self.filtered_channel_indices();
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Enter => {
                if let Some(channel_index) = self.selected_channel_index() {
                    self.overlay = DashboardOverlay::ChannelDetail {
                        channel_index,
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Char('o') => Ok(self.selected_open_dir_action()),
            KeyCode::Char('n') => Ok(Some(DashboardAction::NewChannel)),
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Channels,
                    query: self.channel_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Esc | KeyCode::Char('l') => {
                self.open_dashboard();
                Ok(None)
            }
            KeyCode::Home => {
                self.channel_selected = 0;
                Ok(None)
            }
            KeyCode::End => {
                self.channel_selected = indices.len().saturating_sub(1);
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.channel_selected = self.channel_selected.saturating_sub(1);
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !indices.is_empty() {
                    self.channel_selected = (self.channel_selected + 1).min(indices.len() - 1);
                }
                Ok(None)
            }
            KeyCode::PageUp => {
                self.channel_scroll = self
                    .channel_scroll
                    .saturating_sub(self.last_agents_height as u16);
                self.channel_selected = self
                    .channel_selected
                    .saturating_sub(self.last_agents_height);
                Ok(None)
            }
            KeyCode::PageDown => {
                self.channel_scroll = self
                    .channel_scroll
                    .saturating_add(self.last_agents_height as u16);
                if !indices.is_empty() {
                    self.channel_selected =
                        (self.channel_selected + self.last_agents_height).min(indices.len() - 1);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_event_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        let events = self.filtered_project_events(self.event_filter, &self.event_query)?;
        restore_event_selection(
            &events,
            &mut self.event_selected,
            self.event_selected_key.as_ref(),
        );
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Esc => {
                self.open_dashboard();
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Events,
                    query: self.event_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Char('0') => {
                self.set_event_filter(EventFilter::All)?;
                Ok(None)
            }
            KeyCode::Char('a') => {
                self.set_event_filter(EventFilter::Agents)?;
                Ok(None)
            }
            KeyCode::Char('r') => {
                self.set_event_filter(EventFilter::Runs)?;
                Ok(None)
            }
            KeyCode::Char('t') => {
                self.set_event_filter(EventFilter::Triggers)?;
                Ok(None)
            }
            KeyCode::Char('n') => {
                self.set_event_filter(EventFilter::Notices)?;
                Ok(None)
            }
            KeyCode::Enter => {
                if let Some(event) = events.get(self.event_selected).cloned() {
                    self.jump_to_event(&event)?;
                } else {
                    self.set_toast(DashboardToast::warn("no event selected"));
                }
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.event_selected = self.event_selected.saturating_sub(1);
                self.event_scroll = self
                    .event_scroll
                    .saturating_sub(DASHBOARD_EVENT_SCROLL_STEP);
                self.remember_event_selection(&events);
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !events.is_empty() {
                    self.event_selected = (self.event_selected + 1).min(events.len() - 1);
                }
                self.event_scroll = self
                    .event_scroll
                    .saturating_add(DASHBOARD_EVENT_SCROLL_STEP);
                self.remember_event_selection(&events);
                Ok(None)
            }
            KeyCode::PageUp => {
                self.event_scroll = self
                    .event_scroll
                    .saturating_sub(self.last_agents_height as u16);
                self.event_selected = self.event_selected.saturating_sub(self.last_agents_height);
                self.remember_event_selection(&events);
                Ok(None)
            }
            KeyCode::PageDown => {
                self.event_scroll = self
                    .event_scroll
                    .saturating_add(self.last_agents_height as u16);
                if !events.is_empty() {
                    self.event_selected =
                        (self.event_selected + self.last_agents_height).min(events.len() - 1);
                }
                self.remember_event_selection(&events);
                Ok(None)
            }
            KeyCode::Home => {
                self.event_scroll = 0;
                self.event_selected = 0;
                self.remember_event_selection(&events);
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_update_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        self.restore_update_selection();
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Esc => {
                self.open_dashboard();
                Ok(None)
            }
            KeyCode::Char('u') => Ok(Some(DashboardAction::Update)),
            KeyCode::Enter => {
                if let Some(update) = self.snapshot.updates.get(self.update_selected) {
                    self.overlay = DashboardOverlay::UpdateDetail {
                        update: update.id.clone(),
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Home => {
                self.update_selected = 0;
                self.update_scroll = 0;
                self.remember_update_selection();
                Ok(None)
            }
            KeyCode::End => {
                self.update_selected = self.snapshot.updates.len().saturating_sub(1);
                self.center_update_selection();
                self.remember_update_selection();
                Ok(None)
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.update_selected = self.update_selected.saturating_sub(1);
                self.center_update_selection();
                self.remember_update_selection();
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !self.snapshot.updates.is_empty() {
                    self.update_selected =
                        (self.update_selected + 1).min(self.snapshot.updates.len() - 1);
                }
                self.center_update_selection();
                self.remember_update_selection();
                Ok(None)
            }
            KeyCode::PageUp => {
                self.update_scroll = self
                    .update_scroll
                    .saturating_sub(self.last_agents_height as u16);
                self.update_selected = self.update_selected.saturating_sub(self.last_agents_height);
                self.remember_update_selection();
                Ok(None)
            }
            KeyCode::PageDown => {
                self.update_scroll = self
                    .update_scroll
                    .saturating_add(self.last_agents_height as u16);
                if !self.snapshot.updates.is_empty() {
                    self.update_selected = (self.update_selected + self.last_agents_height)
                        .min(self.snapshot.updates.len() - 1);
                }
                self.remember_update_selection();
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_update_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        update: String,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Up | KeyCode::Char('k') => {
                *scroll = scroll.saturating_sub(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                *scroll = scroll.saturating_add(DASHBOARD_DETAIL_SCROLL_STEP);
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageUp => {
                *scroll = scroll.saturating_sub(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::PageDown => {
                *scroll = scroll.saturating_add(self.last_agents_height as u16);
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
            KeyCode::Home => {
                *scroll = 0;
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::UpdateDetail {
                    update,
                    scroll: *scroll,
                };
                Ok(None)
            }
        }
    }

    fn restore_update_selection(&mut self) {
        if let Some(key) = self.update_selected_key.as_deref()
            && let Some(index) = self
                .snapshot
                .updates
                .iter()
                .position(|update| update.id == key)
        {
            self.update_selected = index;
        }
        self.update_selected = self
            .update_selected
            .min(self.snapshot.updates.len().saturating_sub(1));
        self.center_update_selection();
        self.remember_update_selection();
    }

    fn remember_update_selection(&mut self) {
        self.update_selected_key = self
            .snapshot
            .updates
            .get(self.update_selected)
            .map(|update| update.id.clone());
    }

    fn center_update_selection(&mut self) {
        let height = self.last_agents_height.max(DASHBOARD_MIN_VISIBLE_ROWS);
        let selected = self.update_selected;
        let scroll = usize::from(self.update_scroll);
        if selected < scroll {
            self.update_scroll = selected.min(u16::MAX as usize) as u16;
        } else if selected >= scroll + height {
            self.update_scroll = selected
                .saturating_add(1)
                .saturating_sub(height)
                .min(u16::MAX as usize) as u16;
        }
    }

    fn set_event_filter(&mut self, filter: EventFilter) -> Result<()> {
        self.event_filter = filter;
        self.event_selected = 0;
        self.event_scroll = 0;
        self.restore_event_selection();
        Ok(())
    }

    fn restore_event_selection(&mut self) {
        if let Ok(events) = self.filtered_project_events(self.event_filter, &self.event_query) {
            restore_event_selection(
                &events,
                &mut self.event_selected,
                self.event_selected_key.as_ref(),
            );
            self.remember_event_selection(&events);
        }
    }

    fn remember_event_selection(&mut self, events: &[ProjectEvent]) {
        self.event_selected_key = events.get(self.event_selected).map(ProjectEvent::key);
    }

    fn apply_filter(&mut self, query: &str) {
        let selected = self.selected_key();
        self.filter = query.trim().to_owned();
        if let Some(key) = selected {
            self.restore_selection = Some(key);
            self.restore_selection_if_pending();
        }
        self.ensure_selectable_selection();
        self.ensure_selected_visible();
    }

    fn apply_search(&mut self, target: SearchTarget, query: &str) -> Result<()> {
        match target {
            SearchTarget::Agents => {
                self.apply_filter(query);
                self.persist_ui_state()?;
            }
            SearchTarget::Channels => {
                self.channel_query = query.trim().to_owned();
                self.restore_channel_selection();
            }
            SearchTarget::Queues => {
                self.queue_query = query.trim().to_owned();
                self.restore_queue_selection(self.queue_selected_key.clone());
                self.center_queue_selection();
            }
            SearchTarget::Events => {
                self.event_query = query.trim().to_owned();
                self.event_selected = 0;
                self.event_scroll = 0;
                self.restore_event_selection();
            }
        }
        Ok(())
    }

    fn search_visible_count(&self, target: SearchTarget) -> usize {
        match target {
            SearchTarget::Agents => self.visible_agent_count(),
            SearchTarget::Channels => self.filtered_channel_indices().len(),
            SearchTarget::Queues => self.queue_selection_rows().len(),
            SearchTarget::Events => self
                .filtered_project_events(self.event_filter, &self.event_query)
                .map(|events| events.len())
                .unwrap_or_default(),
        }
    }

    fn toggle_archived(&mut self) -> Result<()> {
        self.include_archived = !self.include_archived;
        self.set_toast(DashboardToast::info(if self.include_archived {
            "showing archived agents"
        } else {
            "hiding archived agents"
        }));
        self.refresh()?;
        self.persist_ui_state()
    }

    fn palette_entries(&self, query: &str) -> Vec<PaletteEntry> {
        let command_order = self.palette_command_order();
        let mut entries = command_order
            .into_iter()
            .filter_map(|command| {
                DASHBOARD_COMMANDS
                    .iter()
                    .find(|spec| matches_palette_command(spec.command, command))
            })
            .filter(|spec| self.palette_command_available(spec.command))
            .map(|spec| PaletteEntry {
                command: spec.command,
                label: spec.label.to_owned(),
                detail: spec.detail.to_owned(),
                key: spec.key,
            })
            .collect::<Vec<_>>();
        let query = query.trim().to_ascii_lowercase();
        if !query.is_empty() {
            entries.retain(|entry| {
                query.split_whitespace().all(|term| {
                    entry.label.to_ascii_lowercase().contains(term)
                        || entry.detail.to_ascii_lowercase().contains(term)
                        || entry
                            .key
                            .is_some_and(|key| key.to_ascii_lowercase().contains(term))
                        || palette_command_aliases(entry.command)
                            .iter()
                            .any(|alias| alias.contains(term))
                })
            });
        }
        entries
    }

    fn palette_command_order(&self) -> Vec<PaletteCommand> {
        let mut commands = Vec::new();
        match self.active_tab {
            DashboardTab::Dashboard => match self.visible_rows().get(self.selected) {
                Some(DashboardSelection::Agent(_, _)) => {
                    commands.extend([
                        PaletteCommand::More,
                        PaletteCommand::Attach,
                        PaletteCommand::Detail,
                        PaletteCommand::OpenSelectedDir,
                        PaletteCommand::ArchiveAgent,
                        PaletteCommand::ToggleRole,
                        PaletteCommand::New,
                    ]);
                }
                Some(DashboardSelection::Role(_)) => {
                    commands.extend([
                        PaletteCommand::New,
                        PaletteCommand::Attach,
                        PaletteCommand::ToggleRole,
                        PaletteCommand::TriggerRole,
                        PaletteCommand::OpenSelectedDir,
                        PaletteCommand::Detail,
                    ]);
                }
                Some(DashboardSelection::Spacer) | None => {}
            },
            DashboardTab::Channels => {
                commands.extend([
                    PaletteCommand::OpenSelectedDir,
                    PaletteCommand::NewChannel,
                    PaletteCommand::Channels,
                ]);
            }
            DashboardTab::Queues => {
                commands.extend([PaletteCommand::ToggleQueueCollapsed, PaletteCommand::Queues]);
            }
            DashboardTab::EventTimeline => commands.push(PaletteCommand::Events),
            DashboardTab::Updates => {
                commands.extend([PaletteCommand::Update, PaletteCommand::Updates])
            }
        }
        for spec in DASHBOARD_COMMANDS {
            if !commands
                .iter()
                .any(|command| matches_palette_command(*command, spec.command))
            {
                commands.push(spec.command);
            }
        }
        commands
    }

    fn palette_command_available(&self, command: PaletteCommand) -> bool {
        match command {
            PaletteCommand::Detail => self.visible_rows().get(self.selected).is_some(),
            PaletteCommand::New => self.selected_role_slug().is_some(),
            PaletteCommand::More => self.selected_inactive_agent().is_some(),
            PaletteCommand::Attach => self.selected_attach_action().is_some(),
            PaletteCommand::Search
            | PaletteCommand::Channels
            | PaletteCommand::Events
            | PaletteCommand::Updates
            | PaletteCommand::Advanced
            | PaletteCommand::Update
            | PaletteCommand::Check
            | PaletteCommand::Assist
            | PaletteCommand::OpenProject
            | PaletteCommand::ToggleAllRoles
            | PaletteCommand::ToggleArchived
            | PaletteCommand::RetryErrored
            | PaletteCommand::ProviderSettings
            | PaletteCommand::Help
            | PaletteCommand::Quit => true,
            PaletteCommand::Queues => !self.snapshot.queues.is_empty(),
            PaletteCommand::OpenSelectedDir => self.selected_open_dir_action().is_some(),
            PaletteCommand::NewChannel => true,
            PaletteCommand::ToggleQueueCollapsed => {
                matches!(self.active_tab, DashboardTab::Queues) && !self.snapshot.queues.is_empty()
            }
            PaletteCommand::ToggleRole => self.selected_pause_action().is_some(),
            PaletteCommand::TriggerRole => self.selected_role_slug().is_some(),
            PaletteCommand::ArchiveAgent => self.selected_archivable_agent().is_some(),
        }
    }

    fn execute_palette_command(
        &mut self,
        command: PaletteCommand,
    ) -> Result<Option<DashboardAction>> {
        match command {
            PaletteCommand::Detail => {
                self.open_detail_overlay(false);
                Ok(None)
            }
            PaletteCommand::New => Ok(self.selected_new_action()),
            PaletteCommand::More => Ok(self.selected_more_action()),
            PaletteCommand::Attach => Ok(self.selected_attach_action()),
            PaletteCommand::Search => {
                self.overlay = DashboardOverlay::Search {
                    target: match self.active_tab {
                        DashboardTab::Dashboard => SearchTarget::Agents,
                        DashboardTab::Channels => SearchTarget::Channels,
                        DashboardTab::Queues => SearchTarget::Queues,
                        DashboardTab::EventTimeline => SearchTarget::Events,
                        DashboardTab::Updates => SearchTarget::Agents,
                    },
                    query: match self.active_tab {
                        DashboardTab::Dashboard => self.filter.clone(),
                        DashboardTab::Channels => self.channel_query.clone(),
                        DashboardTab::Queues => self.queue_query.clone(),
                        DashboardTab::EventTimeline => self.event_query.clone(),
                        DashboardTab::Updates => String::new(),
                    },
                };
                Ok(None)
            }
            PaletteCommand::Channels => {
                self.open_channels();
                Ok(None)
            }
            PaletteCommand::Queues => {
                self.open_queues();
                Ok(None)
            }
            PaletteCommand::ToggleQueueCollapsed => {
                self.toggle_selected_queue_collapsed();
                Ok(None)
            }
            PaletteCommand::Events => {
                self.open_events();
                Ok(None)
            }
            PaletteCommand::Updates => {
                self.open_updates();
                Ok(None)
            }
            PaletteCommand::Advanced => {
                self.overlay = DashboardOverlay::Advanced;
                Ok(None)
            }
            PaletteCommand::Update => Ok(Some(DashboardAction::Update)),
            PaletteCommand::Check => Ok(Some(DashboardAction::Check)),
            PaletteCommand::Assist => Ok(Some(DashboardAction::Assist)),
            PaletteCommand::OpenProject => Ok(Some(DashboardAction::OpenProject)),
            PaletteCommand::OpenSelectedDir => Ok(self.selected_open_dir_action()),
            PaletteCommand::NewChannel => Ok(Some(DashboardAction::NewChannel)),
            PaletteCommand::ToggleRole => Ok(self.selected_pause_action()),
            PaletteCommand::ToggleAllRoles => Ok(Some(DashboardAction::ToggleAllRoles)),
            PaletteCommand::TriggerRole => {
                Ok(self.selected_role_slug().map(DashboardAction::TriggerRole))
            }
            PaletteCommand::ArchiveAgent => {
                Ok(self.selected_agent_action(DashboardAction::Archive))
            }
            PaletteCommand::ToggleArchived => {
                self.toggle_archived()?;
                Ok(None)
            }
            PaletteCommand::RetryErrored => {
                let updated = retry_waits_now_inner(&self.project)?;
                self.set_toast(DashboardToast::success(format!(
                    "retry errored requested; updated {updated} agents"
                )));
                Ok(None)
            }
            PaletteCommand::ProviderSettings => {
                self.open_provider_settings();
                Ok(None)
            }
            PaletteCommand::Help => {
                self.overlay = DashboardOverlay::Help;
                Ok(None)
            }
            PaletteCommand::Quit => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
        }
    }

    fn restore_selection_if_pending(&mut self) {
        let Some(key) = self.restore_selection.take() else {
            return;
        };
        let rows = self.visible_rows();
        if let Some(index) = rows
            .iter()
            .position(|row| self.selection_matches_key(*row, &key))
        {
            self.selected = index;
            self.agents_scroll = self
                .selected
                .saturating_sub(self.restore_anchor_offset(&key));
            return;
        }
        if key.agent.is_some()
            && let Some(index) = rows.iter().position(|row| {
                matches!(
                    row,
                    DashboardSelection::Role(role_index)
                        if self.snapshot.roles[*role_index].slug == key.role
                )
            })
        {
            self.selected = index;
            self.agents_scroll = self
                .selected
                .saturating_sub(self.restore_anchor_offset(&key));
        }
    }

    fn restore_anchor_offset(&self, key: &DashboardSelectionKey) -> usize {
        let contextual_offset = self
            .last_agents_height
            .saturating_sub(1)
            .min(self.last_agents_height / 3)
            .max(3);
        key.selected_offset
            .max(contextual_offset)
            .min(self.selected)
    }

    fn selection_matches_key(&self, row: DashboardSelection, key: &DashboardSelectionKey) -> bool {
        match row {
            DashboardSelection::Role(role_index) => {
                key.agent.is_none() && self.snapshot.roles[role_index].slug == key.role
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                key.agent.as_ref().is_some_and(|agent| {
                    let selected = &self.snapshot.roles[role_index].agents[agent_index];
                    selected.role == key.role && selected.agent == *agent
                })
            }
            DashboardSelection::Spacer => false,
        }
    }

    fn ensure_selectable_selection(&mut self) {
        let rows = self.visible_rows();
        if rows.is_empty() {
            self.selected = 0;
            self.agents_scroll = 0;
            return;
        }
        if self.selected < rows.len() && rows[self.selected].is_selectable() {
            return;
        }
        self.selected = rows.iter().position(|row| row.is_selectable()).unwrap_or(0);
    }

    fn selectable_indices(&self) -> Vec<usize> {
        self.visible_rows()
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.is_selectable().then_some(index))
            .collect()
    }

    fn select_first(&mut self) {
        if let Some(index) = self.selectable_indices().first() {
            self.selected = *index;
            self.ensure_selected_visible();
        }
    }

    fn select_last(&mut self) {
        if let Some(index) = self.selectable_indices().last() {
            self.selected = *index;
            self.ensure_selected_visible();
        }
    }

    fn move_selection_by(&mut self, delta: isize) {
        let indices = self.selectable_indices();
        if indices.is_empty() {
            self.selected = 0;
            self.agents_scroll = 0;
            return;
        }
        let position = indices
            .iter()
            .position(|index| *index == self.selected)
            .unwrap_or(0);
        let next = if delta.is_negative() {
            position.saturating_sub(delta.unsigned_abs())
        } else {
            (position + delta as usize).min(indices.len() - 1)
        };
        self.selected = indices[next];
        self.ensure_selected_visible();
    }

    fn page_step(&self) -> isize {
        self.last_agents_height
            .saturating_sub(DASHBOARD_PAGE_OVERLAP_ROWS)
            .max(DASHBOARD_MIN_VISIBLE_ROWS) as isize
    }

    fn ensure_selected_visible(&mut self) {
        let rows_len = self.visible_rows().len();
        if rows_len == 0 {
            self.agents_scroll = 0;
            return;
        }
        self.agents_scroll = self.agents_scroll.min(rows_len - 1);
        let height = self.last_agents_height.max(DASHBOARD_MIN_VISIBLE_ROWS);
        if self.selected < self.agents_scroll {
            self.agents_scroll = self.selected;
        } else if self.selected >= self.agents_scroll + height {
            self.agents_scroll = self.selected + 1 - height;
        }
    }

    fn selected_key(&self) -> Option<DashboardSelectionKey> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardSelectionKey {
                role: self.snapshot.roles[*role_index].slug.clone(),
                agent: None,
                selected_offset: self.selected.saturating_sub(self.agents_scroll),
            }),
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                Some(DashboardSelectionKey {
                    role: agent.role.clone(),
                    agent: Some(agent.agent.clone()),
                    selected_offset: self.selected.saturating_sub(self.agents_scroll),
                })
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn remembered_detail_scroll(&self) -> u16 {
        self.detail_scroll_key()
            .and_then(|key| self.detail_scrolls.get(&key).copied())
            .unwrap_or(0)
    }

    fn remember_detail_scroll(&mut self, scroll: u16) {
        if let Some(key) = self.detail_scroll_key() {
            self.detail_scrolls.insert(key, scroll);
        }
    }

    fn detail_scroll_key(&self) -> Option<String> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => {
                Some(format!("role/{}", self.snapshot.roles[*role_index].slug))
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                Some(format!("agent/{}/{}", agent.role, agent.agent))
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_role_slug(&self) -> Option<RoleSlug> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => {
                Some(self.snapshot.roles[*role_index].slug.clone())
            }
            DashboardSelection::Agent(role_index, _) => {
                Some(self.snapshot.roles[*role_index].slug.clone())
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn persist_ui_state(&self) -> Result<()> {
        let selected = self.selected_key();
        let selected_role = selected.as_ref().map(|key| key.role.clone());
        let selected_agent = selected.and_then(|key| key.agent);
        io::write_toml(
            &dashboard_ui_state_path(&self.project),
            &DashboardUiState {
                version: DASHBOARD_UI_STATE_VERSION,
                selected_role,
                selected_agent,
                agents_scroll: self.agents_scroll,
                selected_offset: self.selected.saturating_sub(self.agents_scroll),
                filter: self.filter.clone(),
                seen_alert_latest: self.seen_alert_latest.clone(),
            },
        )
    }

    fn selected_attach_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::AttachRole(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, agent_index) => Some(
                DashboardAction::AttachAgent(self.resolved_agent(*role_index, *agent_index)),
            ),
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_more_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(_) => None,
            DashboardSelection::Agent(role_index, agent_index) => Some(DashboardAction::More(
                self.resolved_agent(*role_index, *agent_index),
            )),
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_new_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::New(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, _) => Some(DashboardAction::New(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_pause_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::ToggleRole(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                (agent.paused_by_user
                    || matches!(
                        agent.status,
                        AgentStatus::Starting | AgentStatus::Running | AgentStatus::Paused
                    ))
                .then(|| {
                    DashboardAction::ToggleAgent(self.resolved_agent(*role_index, *agent_index))
                })
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn resolved_agent(&self, role_index: usize, agent_index: usize) -> ResolvedAgent {
        let agent = &self.snapshot.roles[role_index].agents[agent_index];
        ResolvedAgent {
            role: agent.role.clone(),
            agent: agent.agent.clone(),
        }
    }

    fn selected_agent_action(
        &self,
        action: impl FnOnce(ResolvedAgent) -> DashboardAction,
    ) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Agent(role_index, agent_index) => {
                Some(action(self.resolved_agent(*role_index, *agent_index)))
            }
            DashboardSelection::Role(_) | DashboardSelection::Spacer => None,
        }
    }

    fn selected_open_dir_action(&self) -> Option<DashboardAction> {
        match self.active_tab {
            DashboardTab::Channels => {
                let index = *self.filtered_channel_indices().get(self.channel_selected)?;
                let channel = self.snapshot.channels.get(index)?;
                Some(DashboardAction::OpenPath {
                    path: self.project.channels_dir().join(&channel.name),
                    label: format!("channel `{}`", channel.name),
                })
            }
            DashboardTab::Dashboard => match self.visible_rows().get(self.selected)? {
                DashboardSelection::Role(role_index) => {
                    let role = &self.snapshot.roles[*role_index];
                    Some(DashboardAction::OpenPath {
                        path: self.project.role_dir(&role.slug),
                        label: format!("role `{}`", role.slug),
                    })
                }
                DashboardSelection::Agent(role_index, agent_index) => {
                    let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                    Some(DashboardAction::OpenPath {
                        path: RolePaths::new(self.project.clone(), agent.role.clone())
                            .agent(agent.agent.clone())
                            .root(),
                        label: format!("agent `{}/{}`", agent.role, agent.agent),
                    })
                }
                DashboardSelection::Spacer => None,
            },
            DashboardTab::Queues | DashboardTab::EventTimeline | DashboardTab::Updates => None,
        }
    }

    fn selected_agent(&self) -> Option<&DashboardAgent> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Agent(role_index, agent_index) => {
                Some(&self.snapshot.roles[*role_index].agents[*agent_index])
            }
            DashboardSelection::Role(_) | DashboardSelection::Spacer => None,
        }
    }

    fn selected_inactive_agent(&self) -> Option<&DashboardAgent> {
        self.selected_agent()
            .filter(|agent| !matches!(agent.status, AgentStatus::Starting | AgentStatus::Running))
    }

    fn selected_archivable_agent(&self) -> Option<&DashboardAgent> {
        self.selected_inactive_agent()
    }

    fn visible_rows(&self) -> Vec<DashboardSelection> {
        self.snapshot.rows_matching(&self.filter)
    }

    fn visible_agent_count(&self) -> usize {
        agent_count_in_rows(&self.visible_rows())
    }

    fn filtered_channel_indices(&self) -> Vec<usize> {
        let query = self.channel_query.trim();
        self.snapshot
            .channels
            .iter()
            .enumerate()
            .filter_map(|(index, channel)| channel.matches_query(query).then_some(index))
            .collect()
    }

    fn restore_channel_selection(&mut self) {
        let indices = self.filtered_channel_indices();
        self.channel_selected = self.channel_selected.min(indices.len().saturating_sub(1));
        self.channel_scroll = self.channel_scroll.min(self.channel_selected as u16);
    }

    fn selected_channel_index(&self) -> Option<usize> {
        self.filtered_channel_indices()
            .get(self.channel_selected)
            .copied()
    }

    fn alert_latest(&self) -> Option<String> {
        self.snapshot
            .channels
            .iter()
            .find(|channel| channel.name == ALERTS_CHANNEL)
            .and_then(|channel| channel.latest.clone())
    }

    fn has_unseen_alerts(&self) -> bool {
        self.alert_latest()
            .is_some_and(|latest| self.seen_alert_latest.as_ref() != Some(&latest))
    }

    fn mark_alerts_seen(&mut self) {
        if let Some(latest) = self.alert_latest() {
            self.seen_alert_latest = Some(latest);
            let _ = self.persist_ui_state();
        }
    }

    fn selected_agent_position(&self, rows: &[DashboardSelection]) -> Option<(usize, usize)> {
        selected_agent_position_in_rows(rows, self.selected)
    }

    fn selected_queue_key(&self) -> Option<StatusQueueKey> {
        self.selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
            .map(StatusQueueKey::from)
            .or_else(|| self.queue_selected_key.clone())
    }

    fn restore_queue_selection(&mut self, key: Option<StatusQueueKey>) {
        let rows = self.queue_selection_rows();
        if let Some(key) = key {
            self.queue_selected_key = Some(key.clone());
            if let Some(index) = rows.iter().position(|row| {
                self.queue_index_for_selection(*row)
                    .and_then(|queue_index| self.snapshot.queues.get(queue_index))
                    .map(StatusQueueKey::from)
                    == Some(key.clone())
            }) {
                self.queue_selected = index;
                self.center_queue_selection();
                return;
            }
        }
        self.queue_selected = self.queue_selected.min(rows.len().saturating_sub(1));
        self.queue_selected_key = self
            .selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
            .map(StatusQueueKey::from);
        self.center_queue_selection();
    }

    fn remember_queue_selection(&mut self) {
        self.queue_selected_key = self.selected_queue_key();
        self.center_queue_selection();
    }

    fn queue_selection_rows(&self) -> Vec<QueueSelection> {
        let mut rows = Vec::new();
        for queue_index in self.filtered_queue_indices() {
            let queue = &self.snapshot.queues[queue_index];
            rows.push(QueueSelection::Header(queue_index));
            if !self.queue_collapsed(queue) {
                rows.extend((0..queue.count).map(|item_index| QueueSelection::Item {
                    queue_index,
                    item_index,
                }));
            }
        }
        rows
    }

    fn filtered_queue_indices(&self) -> Vec<usize> {
        let query = self.queue_query.trim();
        self.snapshot
            .queues
            .iter()
            .enumerate()
            .filter_map(|(index, _)| self.queue_matches_query(index, query).then_some(index))
            .collect()
    }

    fn queue_matches_query(&self, queue_index: usize, query: &str) -> bool {
        let Some(queue) = self.snapshot.queues.get(queue_index) else {
            return false;
        };
        if query.is_empty()
            || text_matches_query(
                [
                    queue.kind.label().to_owned(),
                    queue.name.clone(),
                    queue.count.to_string(),
                    if queue.locked { "locked" } else { "unlocked" }.to_owned(),
                    queue
                        .active
                        .as_ref()
                        .map(|active| active.label.clone())
                        .unwrap_or_default(),
                ],
                query,
            )
        {
            return true;
        }
        load_trigger_queue(&self.project, &queue.name)
            .ok()
            .is_some_and(|state| {
                state.items.into_iter().any(|item| {
                    text_matches_query(
                        [
                            item.role.to_string(),
                            format_unix_time(item.enqueued_at),
                            trigger_cause_summary(&item.cause),
                        ],
                        query,
                    )
                })
            })
    }

    fn selected_queue_index(&self) -> Option<usize> {
        self.queue_index_for_selection(*self.queue_selection_rows().get(self.queue_selected)?)
    }

    fn queue_index_for_selection(&self, selection: QueueSelection) -> Option<usize> {
        match selection {
            QueueSelection::Header(index) => Some(index),
            QueueSelection::Item { queue_index, .. } => Some(queue_index),
        }
    }

    fn toggle_selected_queue_collapsed(&mut self) {
        let Some(queue) = self
            .selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
        else {
            return;
        };
        let key = queue_key_string(queue);
        if !self.collapsed_queues.remove(&key) {
            self.collapsed_queues.insert(key);
        }
        self.queue_selected = self
            .queue_selected
            .min(self.queue_selection_rows().len().saturating_sub(1));
    }

    fn queue_collapsed(&self, queue: &StatusQueueRow) -> bool {
        self.collapsed_queues.contains(&queue_key_string(queue))
    }

    fn center_queue_selection(&mut self) {
        let offset = self.queue_selection_line_offset(self.queue_selected);
        self.queue_scroll = offset.saturating_sub(self.last_queue_height / 2) as u16;
    }

    fn queue_selection_line_offset(&self, selected_row: usize) -> usize {
        let mut row_index = 0;
        let mut line_offset = self.active_queue_line_count();
        for (visible_index, queue_index) in self.filtered_queue_indices().into_iter().enumerate() {
            let queue = &self.snapshot.queues[queue_index];
            if visible_index > 0 {
                line_offset += 1;
            }
            if row_index == selected_row {
                return line_offset;
            }
            row_index += 1;
            line_offset += 1;
            if self.queue_collapsed(queue) {
                line_offset += 1;
                continue;
            }
            if queue.count == 0 {
                line_offset += 1;
                continue;
            }
            for item_index in 0..queue.count {
                if row_index == selected_row {
                    return line_offset;
                }
                row_index += 1;
                line_offset += self.queue_item_line_count(queue, item_index);
            }
        }
        line_offset
    }

    fn active_queue_line_count(&self) -> usize {
        active_queue_item_line(&self.snapshot.queues, usize::MAX)
            .map(|_| 2)
            .unwrap_or_default()
    }

    fn queue_item_line_count(&self, _queue: &StatusQueueRow, _item_index: usize) -> usize {
        2
    }

    fn filtered_project_events(
        &self,
        filter: EventFilter,
        query: &str,
    ) -> Result<Vec<ProjectEvent>> {
        let query = query.trim().to_ascii_lowercase();
        Ok(load_project_events(&self.project)?
            .into_iter()
            .filter(|event| filter.matches(event.lane))
            .filter(|event| {
                query.is_empty()
                    || [
                        event.title.as_str(),
                        event.detail.as_str(),
                        event.lane.label(),
                    ]
                    .join(" ")
                    .to_ascii_lowercase()
                    .contains(&query)
            })
            .collect())
    }

    fn jump_to_event(&mut self, event: &ProjectEvent) -> Result<()> {
        match &event.target {
            EventTarget::Agent { role, agent } => {
                self.active_tab = DashboardTab::Dashboard;
                self.filter.clear();
                let key = DashboardSelectionKey {
                    role: role.clone(),
                    agent: Some(agent.clone()),
                    selected_offset: 0,
                };
                self.restore_selection = Some(key);
                self.restore_selection_if_pending();
                self.ensure_selectable_selection();
                self.ensure_selected_visible();
                self.open_detail_overlay(false);
                self.set_toast(DashboardToast::info(format!("opened {role}/{agent}")));
                self.persist_ui_state()?;
            }
            EventTarget::Queue(key) => {
                self.active_tab = DashboardTab::Queues;
                self.restore_queue_selection(Some(key.clone()));
                self.set_toast(DashboardToast::info(format!("opened queue `{}`", key.name)));
            }
            EventTarget::Notice => {
                self.active_tab = DashboardTab::Dashboard;
                self.overlay = DashboardOverlay::None;
                self.set_toast(DashboardToast::info("opened notices"));
            }
            EventTarget::None => {
                self.set_toast(DashboardToast::warn("event has no jump target"));
                self.open_events();
            }
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        self.expire_toast();
        let area = frame.area();
        let narrow = area.width < DASHBOARD_NARROW_WIDTH;
        let root = dashboard_layout(area, narrow);
        self.draw_tabs(frame, root.tabs);
        match self.active_tab {
            DashboardTab::Channels => self.draw_channel_tab(frame, root.main),
            DashboardTab::Queues => self.draw_queue_tab(frame, root.main),
            DashboardTab::EventTimeline => self.draw_event_log(frame, root.main),
            DashboardTab::Updates => self.draw_updates_tab(frame, root.main),
            DashboardTab::Dashboard => {
                self.draw_notices(frame, root.notices);
                self.draw_detail(frame, root.state);
                self.draw_tree(frame, root.agents);
                self.draw_runtime(frame, root.runtime, narrow);
            }
        }
        draw_dashboard_footer(
            frame,
            root.footer,
            self.footer_line(usize::from(root.footer.width)),
        );
        match &self.overlay {
            DashboardOverlay::None => {}
            DashboardOverlay::Advanced => draw_advanced_menu(frame, area),
            DashboardOverlay::Help => draw_help_overlay(frame, area),
            DashboardOverlay::Search { target, query } => draw_search_overlay(
                frame,
                area,
                *target,
                query,
                self.search_visible_count(*target),
            ),
            DashboardOverlay::Palette { query, selected } => {
                self.draw_palette(frame, area, query, *selected);
            }
            DashboardOverlay::Detail { extended, scroll } => {
                self.draw_focused_detail(frame, area, *extended, *scroll);
            }
            DashboardOverlay::QueueDetail { selection, scroll } => {
                self.draw_queue_detail(frame, area, *selection, *scroll);
            }
            DashboardOverlay::ChannelDetail {
                channel_index,
                scroll,
            } => {
                self.draw_channel_detail(frame, area, *channel_index, *scroll);
            }
            DashboardOverlay::NoticeDetail { scroll } => {
                self.draw_notice_detail(frame, area, *scroll);
            }
            DashboardOverlay::UpdateDetail { update, scroll } => {
                self.draw_update_detail(frame, area, update, *scroll);
            }
            DashboardOverlay::ProviderSettings { selected } => {
                self.draw_provider_settings(frame, area, *selected);
            }
        }
    }

    fn draw_tabs(&self, frame: &mut Frame<'_>, area: Rect) {
        let dashboard_style = tab_style(matches!(self.active_tab, DashboardTab::Dashboard));
        let channels_style =
            if self.has_unseen_alerts() && !matches!(self.active_tab, DashboardTab::Channels) {
                Style::default()
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            } else {
                tab_style(matches!(self.active_tab, DashboardTab::Channels))
            };
        let queues_style = tab_style(matches!(self.active_tab, DashboardTab::Queues));
        let events_style = tab_style(matches!(self.active_tab, DashboardTab::EventTimeline));
        let updates_style = tab_style(matches!(self.active_tab, DashboardTab::Updates));
        let channels_label = if self.has_unseen_alerts() {
            " Channels! "
        } else {
            " Channels "
        };
        let mut spans = vec![
            Span::raw(" "),
            Span::styled(" Dashboard ", dashboard_style),
            Span::raw(" "),
            Span::styled(channels_label, channels_style),
            Span::raw(" "),
            Span::styled(" Queues ", queues_style),
            Span::raw(" "),
            Span::styled(" Timeline ", events_style),
            Span::raw(" "),
            Span::styled(" Updates ", updates_style),
            Span::styled("  ", Style::default().fg(Color::DarkGray)),
            footer_key("Tab"),
            Span::styled(" switch", Style::default().fg(Color::DarkGray)),
        ];
        if let Some(toast) = self.toast.as_ref().filter(|toast| toast.is_visible()) {
            spans.extend([
                Span::styled("  ", Style::default().fg(Color::DarkGray)),
                Span::styled("● ", toast.level.style()),
                Span::styled(
                    toast.text.clone(),
                    toast.level.style().add_modifier(Modifier::BOLD),
                ),
            ]);
        }
        frame.render_widget(
            Paragraph::new(Line::from(spans))
                .style(Style::default().fg(Color::White).bg(Color::Black)),
            area,
        );
    }

    fn draw_notices(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut title = vec![Span::styled(
            "Notices",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )];
        if self.snapshot.notices_loading {
            title.extend([
                Span::styled(" refreshing ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    ui::spinner_frame(self.frame_tick),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]);
        }
        if let Some(updated_at) = self.snapshot.notices_updated_at {
            title.extend([
                Span::styled(" · updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(event_age(updated_at), Style::default().fg(Color::DarkGray)),
            ]);
        }
        let title = Line::from(title);
        let mut lines = Vec::new();
        if let Some(latest) = self.alert_latest().filter(|_| self.has_unseen_alerts()) {
            lines.push(Line::from(vec![
                Span::styled("! ", Style::default().fg(Color::Yellow)),
                Span::styled("unopened alert ", Style::default().fg(Color::Yellow)),
                Span::styled(latest, Style::default().fg(Color::White)),
            ]));
        }
        if self.snapshot.notices.is_empty() {
            if lines.is_empty() {
                lines.push(Line::from(Span::styled(
                    if self.snapshot.notices_loading {
                        "refreshing..."
                    } else {
                        "no operator notices"
                    },
                    Style::default().fg(Color::DarkGray),
                )));
            }
        } else {
            lines.extend(self.snapshot.notices.iter().map(|notice| {
                Line::from(vec![
                    Span::styled("● ", notice_style(notice.severity)),
                    Span::styled(notice.text.clone(), Style::default().fg(Color::White)),
                ])
            }));
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(title)
                        .border_style(Style::default().fg(Color::Yellow)),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn draw_tree(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.ensure_selected_visible();
        let rows = self.visible_rows();
        let rows_len = rows.len();
        let position = self.selected_agent_position(&rows);
        let content_width = usize::from(area.width.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS));
        let agent_width = self.snapshot.agent_width_for_rows(&rows);
        let summary_width = self.snapshot.summary_width_for_rows(&rows);
        let items = if rows.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                if self.filter.is_empty() {
                    "No roles found. Run `think role new` or create a templated project."
                } else {
                    "No matching agents. Press / to clear or refine the filter."
                },
                Style::default().fg(Color::DarkGray),
            )))]
        } else {
            rows.into_iter()
                .enumerate()
                .skip(self.agents_scroll)
                .take(self.last_agents_height)
                .map(|(index, row)| {
                    ListItem::new(self.row_line(
                        row,
                        index == self.selected,
                        agent_width,
                        summary_width,
                        content_width,
                    ))
                })
                .collect::<Vec<_>>()
        };
        frame.render_widget(
            List::new(items).block(
                dashboard_block_with_title(panel_title("Agents", position))
                    .border_style(Style::default().fg(Color::Blue)),
            ),
            area,
        );
        render_scrollbar(frame, area, rows_len, self.agents_scroll);
    }

    fn row_line(
        &self,
        row: DashboardSelection,
        selected: bool,
        agent_width: usize,
        summary_width: usize,
        content_width: usize,
    ) -> Line<'static> {
        match row {
            DashboardSelection::Spacer => Line::from(""),
            DashboardSelection::Role(role_index) => {
                let role = &self.snapshot.roles[role_index];
                let style = dashboard_role_style(role.status);
                let label = if content_width < 64 {
                    role.slug.to_string()
                } else if role.agents.is_empty() {
                    format!(
                        "{}{}{} · (n new agent)",
                        role.slug,
                        ui::FIELD_SEPARATOR,
                        role.parallel
                    )
                } else {
                    format!("{}{}{}", role.slug, ui::FIELD_SEPARATOR, role.parallel)
                };
                Line::from(vec![
                    dashboard_span(
                        if selected { "▸" } else { " " },
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(" ", style, selected),
                    dashboard_span(
                        ellipsize_display(&label, content_width.saturating_sub(2)),
                        style.add_modifier(Modifier::BOLD),
                        selected,
                    ),
                ])
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[role_index].agents[agent_index];
                let status_style = dashboard_agent_style(agent.status, agent.quota_waiting);
                let summary_style = if agent.summary == "*name loading*" {
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC)
                } else {
                    Style::default().fg(Color::White)
                };
                let detail = if content_width < 96 || agent.detail.is_empty() {
                    String::new()
                } else {
                    format!("{}{}", ui::FIELD_SEPARATOR, agent.detail)
                };
                let fixed_width = 1
                    + 3
                    + agent_width
                    + ui::FIELD_SEPARATOR.chars().count()
                    + ui::FIELD_SEPARATOR.chars().count()
                    + agent.status.to_string().chars().count()
                    + detail.chars().count();
                let usable_summary_width =
                    summary_width.min(content_width.saturating_sub(fixed_width));
                let summary = pad_display(
                    &ellipsize_display(&agent.summary, usable_summary_width),
                    usable_summary_width,
                );
                let status_glyph = match agent.status {
                    AgentStatus::Starting | AgentStatus::Running => {
                        format!(" {} ", ui::spinner_frame(self.frame_tick))
                    }
                    AgentStatus::Paused => " ‖ ".to_owned(),
                    AgentStatus::Done => " ✓ ".to_owned(),
                    AgentStatus::Stopped => " × ".to_owned(),
                    AgentStatus::NeedsAttention => " ! ".to_owned(),
                };
                Line::from(vec![
                    dashboard_span(
                        if selected { "▸" } else { " " },
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(status_glyph, status_style, selected),
                    dashboard_span(
                        pad_display(agent.agent.as_str(), agent_width),
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(
                        ui::FIELD_SEPARATOR,
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                    dashboard_span(summary, summary_style, selected),
                    dashboard_span(
                        ui::FIELD_SEPARATOR,
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                    dashboard_span(agent.status.to_string(), status_style, selected),
                    dashboard_span(detail, Style::default().fg(Color::DarkGray), selected),
                ])
            }
        }
    }

    fn draw_detail(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = dashboard_block("State").border_style(Style::default().fg(Color::Green));
        let inner = block.inner(area);
        let lines = match self.visible_rows().get(self.selected) {
            Some(DashboardSelection::Role(role_index)) => {
                self.snapshot.roles[*role_index].state_lines(usize::from(inner.width))
            }
            Some(DashboardSelection::Agent(role_index, agent_index)) => {
                self.snapshot.roles[*role_index].agents[*agent_index]
                    .state_lines(usize::from(inner.width))
            }
            Some(DashboardSelection::Spacer) => vec![Line::from("")],
            None => vec![Line::from("No roles or agents.")],
        };
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_runtime(&self, frame: &mut Frame<'_>, area: Rect, narrow: bool) {
        let chunks = Layout::default()
            .direction(if narrow {
                Direction::Vertical
            } else {
                Direction::Horizontal
            })
            .constraints(if narrow {
                [Constraint::Percentage(50), Constraint::Percentage(50)]
            } else {
                [Constraint::Percentage(45), Constraint::Percentage(55)]
            })
            .split(area);
        self.draw_channels(frame, chunks[0]);
        self.draw_quota(frame, chunks[1]);
    }

    fn draw_channel_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.restore_channel_selection();
        let filtered_indices = self.filtered_channel_indices();
        let position = (!filtered_indices.is_empty()).then_some((
            self.channel_selected
                .min(filtered_indices.len().saturating_sub(1))
                + 1,
            filtered_indices.len(),
        ));
        let block = dashboard_block_with_title(panel_title("Channels", position))
            .border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(area);
        let visible = visible_panel_rows(area);
        if self.channel_selected < usize::from(self.channel_scroll) {
            self.channel_scroll = self.channel_selected as u16;
        }
        if self.channel_selected >= usize::from(self.channel_scroll) + visible {
            self.channel_scroll = self
                .channel_selected
                .saturating_add(1)
                .saturating_sub(visible) as u16;
        }
        let lines = self.channel_tab_lines(usize::from(inner.width), &filtered_indices, visible);
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(
            frame,
            area,
            filtered_indices.len(),
            usize::from(self.channel_scroll),
        );
    }

    fn channel_tab_lines(
        &self,
        width: usize,
        filtered_indices: &[usize],
        visible: usize,
    ) -> Vec<Line<'static>> {
        if self.snapshot.channels.is_empty() {
            return vec![Line::from(Span::styled(
                "No channels configured. Use the palette to create one.",
                Style::default().fg(Color::DarkGray),
            ))];
        }
        if filtered_indices.is_empty() {
            return vec![Line::from(Span::styled(
                format!("No channels match `{}`.", self.channel_query),
                Style::default().fg(Color::DarkGray),
            ))];
        }
        filtered_indices
            .iter()
            .enumerate()
            .skip(usize::from(self.channel_scroll))
            .take(visible)
            .filter_map(|(visible_index, channel_index)| {
                self.snapshot.channels.get(*channel_index).map(|channel| {
                    channel_tab_line(
                        channel,
                        visible_index == self.channel_selected,
                        width,
                        channel.name == ALERTS_CHANNEL && self.has_unseen_alerts(),
                    )
                })
            })
            .collect()
    }

    fn draw_channels(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = if self.snapshot.channels.is_empty() {
            vec![Line::from(Span::styled(
                "no channels",
                Style::default().fg(Color::DarkGray),
            ))]
        } else {
            self.snapshot
                .channels
                .iter()
                .flat_map(channel_lines)
                .collect::<Vec<_>>()
        };
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(
                dashboard_block("Channels").border_style(Style::default().fg(Color::Yellow)),
            ),
            area,
        );
    }

    fn draw_quota(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = dashboard_block("Quota").border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        match &self.quota {
            QuotaLoadState::Loading => {
                frame.render_widget(
                    Paragraph::new("loading...").style(Style::default().fg(Color::DarkGray)),
                    inner,
                );
            }
            QuotaLoadState::Ready(Some(limits)) => {
                let rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(DASHBOARD_QUOTA_GAUGE_ROWS),
                        Constraint::Length(DASHBOARD_QUOTA_GAUGE_ROWS),
                    ])
                    .split(inner);
                frame.render_widget(quota_gauge("primary", &limits.primary), rows[0]);
                frame.render_widget(quota_gauge("secondary", &limits.secondary), rows[1]);
            }
            QuotaLoadState::Ready(None) => {
                frame.render_widget(
                    Paragraph::new("usage unavailable").style(Style::default().fg(Color::DarkGray)),
                    inner,
                );
            }
        }
    }

    fn draw_palette(&self, frame: &mut Frame<'_>, area: Rect, query: &str, selected: usize) {
        let popup = centered_popup(
            area,
            DASHBOARD_PALETTE_MAX_WIDTH,
            DASHBOARD_PALETTE_MAX_HEIGHT,
        );
        let block =
            dashboard_block("Command Palette").border_style(Style::default().fg(Color::Magenta));
        let inner_width = usize::from(block.inner(popup).width);
        let entries = self.palette_entries(query);
        let query_line = Line::from(vec![
            Span::styled(
                ":",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(query.to_owned()),
        ]);
        let mut lines = vec![query_line, Line::from("")];
        if entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No matching commands.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for (index, entry) in entries.iter().enumerate() {
                let selected = index == selected.min(entries.len().saturating_sub(1));
                lines.push(Line::from(vec![
                    dashboard_span(
                        if selected { "▸ " } else { "  " },
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(
                        entry.label.clone(),
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(
                        entry
                            .key
                            .map(|key| format!("  [{key}]"))
                            .unwrap_or_default(),
                        Style::default().fg(Color::Cyan),
                        selected,
                    ),
                    dashboard_span(
                        palette_detail(&entry, inner_width),
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                ]));
            }
        }
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(block)
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_focused_detail(&self, frame: &mut Frame<'_>, area: Rect, extended: bool, scroll: u16) {
        let popup = inset_rect(
            area,
            DASHBOARD_MODAL_HORIZONTAL_MARGIN,
            DASHBOARD_MODAL_VERTICAL_MARGIN,
        );
        let lines = self.focused_detail_lines(extended);
        let line_count = lines.len();
        let visible = visible_panel_rows(popup);
        let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
        let title = panel_title(
            "Detail",
            (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        if line_count > visible {
            render_scrollbar(frame, popup, line_count, usize::from(scroll));
        }
    }

    fn focused_detail_lines(&self, extended: bool) -> Vec<Line<'static>> {
        match self.visible_rows().get(self.selected) {
            Some(DashboardSelection::Role(role_index)) => {
                let role = &self.snapshot.roles[*role_index];
                let mut lines = role.detail_lines();
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    footer_key("n"),
                    Span::raw(" new agent  "),
                    footer_key("Space"),
                    Span::raw(" pause/unpause  "),
                    footer_key("Esc"),
                    Span::raw(" close"),
                ]));
                lines
            }
            Some(DashboardSelection::Agent(role_index, agent_index)) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                load_agent_detail_lines(
                    &self.project,
                    agent,
                    extended,
                    !matches!(agent.status, AgentStatus::Starting | AgentStatus::Running),
                )
                .unwrap_or_else(|err| {
                    vec![Line::from(format!("Failed to load agent detail: {err:#}"))]
                })
            }
            Some(DashboardSelection::Spacer) | None => vec![Line::from("No selection.")],
        }
    }

    fn draw_queue_detail(
        &self,
        frame: &mut Frame<'_>,
        area: Rect,
        selection: QueueSelection,
        scroll: u16,
    ) {
        let popup = inset_rect(
            area,
            DASHBOARD_MODAL_HORIZONTAL_MARGIN,
            DASHBOARD_MODAL_VERTICAL_MARGIN,
        );
        let lines = load_queue_detail_lines(&self.project, &self.snapshot.queues, selection)
            .unwrap_or_else(|err| {
                vec![Line::from(format!("Failed to load queue detail: {err:#}"))]
            });
        let line_count = lines.len();
        let visible = visible_panel_rows(popup);
        let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
        let title = panel_title(
            "Queue Detail",
            (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, popup, line_count, usize::from(scroll));
    }

    fn draw_channel_detail(
        &self,
        frame: &mut Frame<'_>,
        area: Rect,
        channel_index: usize,
        scroll: u16,
    ) {
        let popup = inset_rect(
            area,
            DASHBOARD_MODAL_HORIZONTAL_MARGIN,
            DASHBOARD_MODAL_VERTICAL_MARGIN,
        );
        let lines = self
            .snapshot
            .channels
            .get(channel_index)
            .map(|channel| load_channel_detail_lines(&self.project, channel))
            .transpose()
            .unwrap_or_else(|err| {
                Some(vec![Line::from(format!(
                    "Failed to load channel detail: {err:#}"
                ))])
            })
            .unwrap_or_else(|| vec![Line::from("No channel selected.")]);
        let line_count = lines.len();
        let visible = visible_panel_rows(popup);
        let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
        let title = panel_title(
            "Channel Detail",
            (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, popup, line_count, usize::from(scroll));
    }

    fn draw_notice_detail(&self, frame: &mut Frame<'_>, area: Rect, scroll: u16) {
        let popup = inset_rect(
            area,
            DASHBOARD_MODAL_HORIZONTAL_MARGIN,
            DASHBOARD_MODAL_VERTICAL_MARGIN,
        );
        let lines = load_notice_detail_lines(&self.project, &self.snapshot).unwrap_or_else(|err| {
            vec![Line::from(format!("Failed to load notice detail: {err:#}"))]
        });
        let line_count = lines.len();
        let visible = visible_panel_rows(popup);
        let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
        let title = panel_title(
            "Notice Provenance",
            (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, popup, line_count, usize::from(scroll));
    }

    fn draw_update_detail(&self, frame: &mut Frame<'_>, area: Rect, update: &str, scroll: u16) {
        let popup = inset_rect(
            area,
            DASHBOARD_MODAL_HORIZONTAL_MARGIN,
            DASHBOARD_MODAL_VERTICAL_MARGIN,
        );
        let lines = load_update_detail_lines(&self.project, update)
            .unwrap_or_else(|err| vec![Line::from(format!("Failed to load update: {err:#}"))]);
        let line_count = lines.len();
        let visible = visible_panel_rows(popup);
        let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
        let title = panel_title(
            "Update",
            (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, popup, line_count, usize::from(scroll));
    }

    fn draw_provider_settings(&self, frame: &mut Frame<'_>, area: Rect, selected: usize) {
        let popup = inset_rect(area, 4, 2);
        let block =
            dashboard_block("Provider Accounts").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(popup);
        let width = usize::from(inner.width);
        let mut lines = vec![
            provider_settings_project_config_line(&self.project),
            provider_settings_action_line(),
            Line::from(""),
        ];
        match self.provider_account_rows() {
            Ok(rows) => {
                let selected = selected.min(rows.len().saturating_sub(1));
                lines.push(provider_accounts_heading(rows.len(), selected));
                let visible = usize::from(inner.height).saturating_sub(lines.len()).max(1);
                let start = selected
                    .saturating_sub(visible / 2)
                    .min(rows.len().saturating_sub(visible));
                let name_width = provider_account_name_width(&rows);
                lines.extend(rows.iter().enumerate().skip(start).take(visible).map(
                    |(index, account)| {
                        provider_account_line(account, index == selected, width, name_width)
                    },
                ));
            }
            Err(err) => {
                lines.push(Line::from(vec![
                    Span::styled("accounts unavailable: ", Style::default().fg(Color::Red)),
                    Span::styled(format!("{err:#}"), Style::default().fg(Color::White)),
                ]));
            }
        }
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_event_log(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let events = self
            .filtered_project_events(self.event_filter, &self.event_query)
            .unwrap_or_default();
        let title = panel_title(
            "Timeline",
            (!events.is_empty()).then_some((
                self.event_selected.min(events.len().saturating_sub(1)) + 1,
                events.len(),
            )),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        let lines = if events.is_empty() {
            match self.filtered_project_events(self.event_filter, &self.event_query) {
                Ok(events) => project_event_lines(
                    &events,
                    usize::from(inner.width),
                    self.event_selected.min(events.len().saturating_sub(1)),
                    self.event_filter,
                    &self.event_query,
                    false,
                ),
                Err(err) => vec![Line::from(format!("Failed to load timeline: {err:#}"))],
            }
        } else {
            project_event_lines(
                &events,
                usize::from(inner.width),
                self.event_selected.min(events.len().saturating_sub(1)),
                self.event_filter,
                &self.event_query,
                false,
            )
        };
        let line_count = lines.len();
        self.event_scroll =
            clamped_scroll_offset_u16(self.event_scroll, line_count, visible_panel_rows(area));
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((self.event_scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, area, line_count, usize::from(self.event_scroll));
    }

    fn draw_updates_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.restore_update_selection();
        let title = panel_title(
            "Updates",
            (!self.snapshot.updates.is_empty()).then_some((
                self.update_selected
                    .min(self.snapshot.updates.len().saturating_sub(1))
                    + 1,
                self.snapshot.updates.len(),
            )),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        let lines = self.update_tab_lines(usize::from(inner.width));
        let line_count = lines.len();
        self.update_scroll =
            clamped_scroll_offset_u16(self.update_scroll, line_count, visible_panel_rows(area));
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((self.update_scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, area, line_count, usize::from(self.update_scroll));
    }

    fn update_tab_lines(&self, width: usize) -> Vec<Line<'static>> {
        let mut lines = vec![Line::from(vec![
            footer_key("u"),
            Span::raw(" generate update  "),
            footer_key("Enter"),
            Span::raw(" open  "),
            footer_key("Esc"),
            Span::raw(" dashboard"),
        ])];
        if self.snapshot.updates.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "No updates yet. Press u to generate the first project update.",
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        lines.push(Line::from(""));
        for (index, update) in self.snapshot.updates.iter().enumerate() {
            let selected = index == self.update_selected;
            let title_width = width.saturating_sub(28).max(16);
            lines.push(Line::from(vec![
                dashboard_span(
                    if selected { "▸ " } else { "  " },
                    Style::default().fg(Color::White),
                    selected,
                ),
                dashboard_span(
                    format_unix_time_compact(update.created_at),
                    Style::default().fg(Color::Cyan),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    ellipsize_display(&update.title, title_width),
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                    selected,
                ),
            ]));
            if !update.preview.is_empty() {
                lines.push(Line::from(vec![
                    dashboard_span("    ", Style::default().fg(Color::DarkGray), selected),
                    dashboard_span(
                        ellipsize_display(&update.preview, width.saturating_sub(4)),
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                ]));
            }
        }
        lines
    }

    fn draw_queue_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let rows = self.queue_selection_rows();
        let title = panel_title(
            "Queues",
            (!rows.is_empty()).then_some((self.queue_selected.min(rows.len() - 1) + 1, rows.len())),
        );
        let block =
            dashboard_block_with_title(title).border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(area);
        self.last_queue_height = usize::from(inner.height).max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.center_queue_selection();
        let lines = self.queue_tab_lines(usize::from(inner.width));
        let line_count = lines.len();
        self.queue_scroll =
            clamped_scroll_offset_u16(self.queue_scroll, line_count, visible_panel_rows(area));
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .scroll((self.queue_scroll, 0))
                .wrap(Wrap { trim: false }),
            inner,
        );
        render_scrollbar(frame, area, line_count, usize::from(self.queue_scroll));
    }

    fn queue_tab_lines(&self, width: usize) -> Vec<Line<'static>> {
        if self.snapshot.queues.is_empty() {
            return vec![Line::from(Span::styled(
                "No queues are configured.",
                Style::default().fg(Color::DarkGray),
            ))];
        }
        let mut lines = Vec::new();
        if let Some(active) = active_queue_item_line(&self.snapshot.queues, width) {
            lines.push(active);
            lines.push(Line::from(""));
        }
        let filtered_indices = self.filtered_queue_indices();
        if filtered_indices.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("No queues match `{}`.", self.queue_query),
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        for (visible_index, index) in filtered_indices.into_iter().enumerate() {
            let queue = &self.snapshot.queues[index];
            if visible_index > 0 {
                lines.push(Line::from(""));
            }
            let header_selected = matches!(
                self.queue_selection_rows().get(self.queue_selected),
                Some(QueueSelection::Header(queue_index)) if *queue_index == index
            );
            let collapsed = self.queue_collapsed(queue);
            lines.push(queue_header_line(queue, header_selected, collapsed));
            if collapsed {
                lines.push(queue_empty_line("collapsed", header_selected));
            } else {
                let selected_item = match self.queue_selection_rows().get(self.queue_selected) {
                    Some(QueueSelection::Item {
                        queue_index,
                        item_index,
                    }) if *queue_index == index => Some(*item_index),
                    _ => None,
                };
                lines.extend(queue_item_lines(&self.project, queue, width, selected_item));
            }
        }
        lines
    }

    fn footer_line(&self, width: usize) -> Line<'static> {
        match &self.overlay {
            DashboardOverlay::None => match self.active_tab {
                DashboardTab::Dashboard => self.main_footer_line(width),
                DashboardTab::Channels => self.channel_footer_line(width),
                DashboardTab::Queues => self.queue_footer_line(width),
                DashboardTab::EventTimeline => self.event_footer_line(width),
                DashboardTab::Updates => self.update_footer_line(width),
            },
            DashboardOverlay::Advanced => footer_line_from_pairs(
                &[
                    ("n", "new role"),
                    ("r", "retry errored"),
                    ("o", "open project"),
                    ("p", "provider settings"),
                    ("x", "archived"),
                    ("Esc", "close"),
                ],
                width,
            ),
            DashboardOverlay::ProviderSettings { .. } => footer_line_from_pairs(
                &[
                    ("↑↓", "select"),
                    ("Enter/s", "switch"),
                    ("a", "add"),
                    ("d", "delete"),
                    ("m", "model"),
                    ("Esc", "close"),
                ],
                width,
            ),
            DashboardOverlay::Help => {
                footer_line_from_pairs(&[("Esc", "close"), ("q", "close")], width)
            }
            DashboardOverlay::Search { .. } => footer_line_from_pairs(
                &[
                    ("type", "filter"),
                    ("Ctrl-u", "clear"),
                    ("Enter", "apply"),
                    ("Esc", "close"),
                ],
                width,
            ),
            DashboardOverlay::Palette { .. } => footer_line_from_pairs(
                &[
                    ("type", "search"),
                    ("↑↓", "select"),
                    ("Enter", "run"),
                    ("Esc", "close"),
                ],
                width,
            ),
            DashboardOverlay::Detail { extended, .. } => {
                let mut pairs = vec![("↑↓", "scroll"), ("a", "attach")];
                if self.selected_inactive_agent().is_some() {
                    pairs.push(("m", "more"));
                }
                pairs.extend([
                    ("n", "new"),
                    ("Space", "pause/unpause"),
                    (
                        "x",
                        if *extended {
                            "hide timeline"
                        } else {
                            "timeline"
                        },
                    ),
                    ("Esc", "close"),
                ]);
                footer_line_from_pairs(&pairs, width)
            }
            DashboardOverlay::QueueDetail { .. } | DashboardOverlay::NoticeDetail { .. } => {
                footer_line_from_pairs(
                    &[("↑↓", "scroll"), ("PgUp/PgDn", "scroll"), ("Esc", "close")],
                    width,
                )
            }
            DashboardOverlay::ChannelDetail { .. } => footer_line_from_pairs(
                &[
                    ("↑↓", "scroll"),
                    ("PgUp/PgDn", "scroll"),
                    ("o", "open dir"),
                    ("Esc", "close"),
                ],
                width,
            ),
            DashboardOverlay::UpdateDetail { .. } => footer_line_from_pairs(
                &[("↑↓", "scroll"), ("PgUp/PgDn", "scroll"), ("Esc", "close")],
                width,
            ),
        }
    }

    fn channel_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_pairs(
            &[
                ("Enter", "detail"),
                ("o", "open dir"),
                ("/", "search"),
                ("n", "new channel"),
                ("↑↓", "select"),
                ("PgUp/PgDn", "scroll"),
                ("Esc", "dashboard"),
                ("q", "quit"),
            ],
            width,
        )
    }

    fn queue_footer_line(&self, width: usize) -> Line<'static> {
        let pairs = vec![
            ("↑↓", "select queue"),
            ("/", "search"),
            ("Space", "collapse"),
            ("Enter", "detail"),
            ("PgUp/PgDn", "scroll"),
            ("Esc", "dashboard"),
            ("q", "quit"),
        ];
        footer_line_from_pairs(&pairs, width)
    }

    fn event_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_pairs(
            &[
                ("Enter", "jump"),
                ("a/r/t/n/0", "filter"),
                ("/", "search"),
                ("↑↓", "select"),
                ("Esc", "dashboard"),
                ("q", "quit"),
            ],
            width,
        )
    }

    fn update_footer_line(&self, width: usize) -> Line<'static> {
        let mut pairs = vec![
            ("u", "generate update"),
            ("↑↓", "select"),
            ("PgUp/PgDn", "scroll"),
            ("Esc", "dashboard"),
            ("q", "quit"),
        ];
        if !self.snapshot.updates.is_empty() {
            pairs.insert(1, ("Enter", "open"));
        }
        footer_line_from_pairs(&pairs, width)
    }

    fn main_footer_line(&self, width: usize) -> Line<'static> {
        let mut pairs = vec![("↑↓", "select")];
        match self.visible_rows().get(self.selected) {
            Some(DashboardSelection::Role(_)) => {
                pairs.extend(
                    [
                        PaletteCommand::ToggleRole,
                        PaletteCommand::New,
                        PaletteCommand::Attach,
                    ]
                    .into_iter()
                    .filter_map(command_footer_pair),
                );
            }
            Some(DashboardSelection::Agent(_, _)) => {
                if self.selected_pause_action().is_some()
                    && let Some(pair) = command_footer_pair(PaletteCommand::ToggleRole)
                {
                    pairs.push(pair);
                }
                if let Some(pair) = command_footer_pair(PaletteCommand::Attach) {
                    pairs.push(pair);
                }
                if self.selected_inactive_agent().is_some() {
                    if let Some(pair) = command_footer_pair(PaletteCommand::More) {
                        pairs.push(pair);
                    }
                }
                pairs.extend(
                    [PaletteCommand::New]
                        .into_iter()
                        .filter_map(command_footer_pair),
                );
            }
            Some(DashboardSelection::Spacer) | None => {}
        }
        pairs.extend(
            [PaletteCommand::Detail, PaletteCommand::Search]
                .into_iter()
                .filter_map(command_footer_pair),
        );
        pairs.push((":", "palette"));
        if !self.snapshot.notices.is_empty() || self.has_unseen_alerts() {
            pairs.push(("!", "notices"));
        }
        pairs.extend(
            [
                PaletteCommand::Updates,
                PaletteCommand::Check,
                PaletteCommand::Assist,
                PaletteCommand::Queues,
                PaletteCommand::Advanced,
                PaletteCommand::Help,
                PaletteCommand::Quit,
            ]
            .into_iter()
            .filter(|command| self.palette_command_available(*command))
            .filter_map(command_footer_pair),
        );
        footer_line_from_pairs(&pairs, width)
    }
}

#[derive(Default)]
struct DashboardSnapshot {
    roles: Vec<DashboardRole>,
    channels: Vec<StatusChannelRow>,
    queues: Vec<StatusQueueRow>,
    updates: Vec<DashboardUpdate>,
    notices: Vec<DashboardNotice>,
    notices_loading: bool,
    notices_updated_at: Option<u64>,
}

#[derive(Clone)]
struct DashboardNotice {
    severity: NoticeSeverity,
    text: String,
}

#[derive(Clone)]
struct DashboardUpdate {
    id: String,
    created_at: u64,
    title: String,
    preview: String,
    path: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NoticeSeverity {
    Info,
    Action,
    Warn,
    Error,
}

impl DashboardSnapshot {
    fn state_signature(&self) -> String {
        let mut signature = String::new();
        for role in &self.roles {
            let _ = write!(
                signature,
                "role\0{}\0{}\0{}\0{}\0{}\0{};",
                role.slug,
                role.status,
                role.mode,
                role.parallel,
                role.auto_archive,
                role.trigger_count
            );
            for agent in &role.agents {
                let _ = write!(
                    signature,
                    "agent\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{};",
                    agent.role,
                    agent.agent,
                    agent.status,
                    agent.summary,
                    agent.detail,
                    agent.current_step,
                    agent.run_count,
                    agent.quota_waiting,
                    agent.supervisor_status,
                    agent.next_retry_at.unwrap_or_default(),
                    agent.latest_output_at.unwrap_or_default(),
                    agent.paused_by_user,
                    agent.created_at,
                    agent.updated_at
                );
            }
        }
        for queue in &self.queues {
            let _ = write!(
                signature,
                "queue\0{}\0{}\0{}\0{}\0{}\0{};",
                queue.kind.label(),
                queue.name,
                queue.count,
                queue.locked,
                queue
                    .active
                    .as_ref()
                    .map(|active| active.label.as_str())
                    .unwrap_or(""),
                queue.locked_at.unwrap_or_default()
            );
        }
        for channel in &self.channels {
            let _ = write!(
                signature,
                "channel\0{}\0{}\0{};",
                channel.name,
                channel.artifacts,
                channel.latest.as_deref().unwrap_or("")
            );
        }
        signature
    }

    fn rows(&self) -> Vec<DashboardSelection> {
        let mut rows = Vec::new();
        for (role_index, role) in self.roles.iter().enumerate() {
            if role_index > 0 {
                rows.push(DashboardSelection::Spacer);
            }
            rows.push(DashboardSelection::Role(role_index));
            for agent_index in 0..role.agents.len() {
                rows.push(DashboardSelection::Agent(role_index, agent_index));
            }
        }
        rows
    }

    fn rows_matching(&self, query: &str) -> Vec<DashboardSelection> {
        let query = query.trim();
        if query.is_empty() {
            return self.rows();
        }
        let mut rows = Vec::new();
        for (role_index, role) in self.roles.iter().enumerate() {
            let role_matches = role.matches_query(query);
            let matching_agents = role
                .agents
                .iter()
                .enumerate()
                .filter_map(|(agent_index, agent)| {
                    (role_matches || agent.matches_query(query)).then_some(agent_index)
                })
                .collect::<Vec<_>>();
            if !role_matches && matching_agents.is_empty() {
                continue;
            }
            if !rows.is_empty() {
                rows.push(DashboardSelection::Spacer);
            }
            rows.push(DashboardSelection::Role(role_index));
            rows.extend(
                matching_agents
                    .into_iter()
                    .map(|agent_index| DashboardSelection::Agent(role_index, agent_index)),
            );
        }
        rows
    }

    fn summary_width_for_rows(&self, rows: &[DashboardSelection]) -> usize {
        rows.iter()
            .filter_map(|row| match row {
                DashboardSelection::Agent(role_index, agent_index) => {
                    Some(&self.roles[*role_index].agents[*agent_index].summary)
                }
                DashboardSelection::Spacer | DashboardSelection::Role(_) => None,
            })
            .map(|summary| summary.chars().count())
            .max()
            .unwrap_or(0)
    }

    fn agent_width_for_rows(&self, rows: &[DashboardSelection]) -> usize {
        rows.iter()
            .filter_map(|row| match row {
                DashboardSelection::Agent(role_index, agent_index) => {
                    Some(self.roles[*role_index].agents[*agent_index].agent.as_str())
                }
                DashboardSelection::Spacer | DashboardSelection::Role(_) => None,
            })
            .map(str::chars)
            .map(Iterator::count)
            .max()
            .unwrap_or(0)
    }
}

struct DashboardRole {
    slug: RoleSlug,
    status: RoleStatus,
    mode: RoleMode,
    parallel: String,
    expose: String,
    auto_archive: bool,
    trigger_count: usize,
    agents: Vec<DashboardAgent>,
}

impl DashboardRole {
    fn matches_query(&self, query: &str) -> bool {
        text_matches_query(
            [
                self.slug.to_string(),
                self.status.to_string(),
                self.mode.to_string(),
                self.parallel.clone(),
                self.expose.clone(),
            ],
            query,
        )
    }

    fn state_lines(&self, width: usize) -> Vec<Line<'static>> {
        let mut lines = vec![
            state_heading_line(
                "role",
                &self.slug.to_string(),
                &self.status.to_string(),
                dashboard_role_style(self.status),
                width,
            ),
            Line::from(vec![
                Span::styled("mode ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.mode.to_string(), Style::default().fg(Color::White)),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("parallel ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.parallel.clone(), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(vec![
                Span::styled("expose ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.expose).to_owned(),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::styled("auto archive ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.auto_archive.to_string(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("triggers ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.trigger_count.to_string(),
                    Style::default().fg(Color::White),
                ),
            ]),
        ];
        lines.push(self.action_hint_line());
        lines
    }

    fn action_hint_line(&self) -> Line<'static> {
        let mut spans = vec![Span::styled(
            "actions ",
            Style::default().fg(Color::DarkGray),
        )];
        spans.extend([
            footer_key("n"),
            Span::raw(" new agent  "),
            footer_key("Enter"),
        ]);
        spans.push(Span::raw(" detail"));
        match self.status {
            RoleStatus::Active => {
                spans.extend([Span::raw("  "), footer_key("Space"), Span::raw(" pause")])
            }
            RoleStatus::Paused => {
                spans.extend([Span::raw("  "), footer_key("Space"), Span::raw(" unpause")]);
            }
            RoleStatus::Draft => {}
        }
        Line::from(spans)
    }

    fn detail_lines(&self) -> Vec<Line<'static>> {
        vec![
            Line::from(vec![
                Span::styled("role ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.slug.to_string(), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(format!("status: {}", self.status)),
            Line::from(format!("mode: {}", self.mode)),
            Line::from(format!("parallel: {}", self.parallel)),
            Line::from(format!("expose: {}", empty_dash(&self.expose))),
            Line::from(format!("auto archive: {}", self.auto_archive)),
            Line::from(format!("triggers: {}", self.trigger_count)),
        ]
    }
}

struct DashboardAgent {
    role: RoleSlug,
    agent: AgentId,
    status: AgentStatus,
    summary: String,
    detail: String,
    channels: String,
    current_step: String,
    run_count: u64,
    pane_id: Option<String>,
    quota_waiting: bool,
    supervisor_status: SupervisorStatus,
    supervisor_updated_at: u64,
    child_pid: Option<u32>,
    next_retry_at: Option<u64>,
    latest_output_at: Option<u64>,
    running_cause: Option<String>,
    paused_by_user: bool,
    created_at: u64,
    updated_at: u64,
}

impl DashboardAgent {
    fn matches_query(&self, query: &str) -> bool {
        text_matches_query(
            [
                self.role.to_string(),
                self.agent.to_string(),
                self.status.to_string(),
                self.summary.clone(),
                self.detail.clone(),
                self.channels.clone(),
                self.current_step.clone(),
                self.supervisor_status.to_string(),
                self.running_cause.clone().unwrap_or_default(),
            ],
            query,
        )
    }

    fn state_lines(&self, width: usize) -> Vec<Line<'static>> {
        let summary_style = if self.summary == "*name loading*" {
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC)
        } else {
            Style::default().fg(Color::White)
        };
        let status_style = dashboard_agent_style(self.status, self.quota_waiting);
        let mut lines = vec![
            state_heading_line(
                "agent",
                &format!("{}/{}", self.role, self.agent),
                &self.summary,
                summary_style,
                width,
            ),
            Line::from(vec![
                Span::styled("status ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.status.to_string(), status_style),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("detail ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.detail).to_owned(),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::styled("step ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.current_step.clone(), Style::default().fg(Color::Cyan)),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    event_age(self.updated_at),
                    Style::default().fg(Color::White),
                ),
            ]),
            self.runtime_progress_line(),
            Line::from(vec![
                Span::styled("channels ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.channels).to_owned(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("runs ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.run_count.to_string(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("session ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.pane_id.clone().unwrap_or_else(|| "-".to_owned()),
                    Style::default().fg(Color::White),
                ),
            ]),
        ];
        if let Some(cause) = self.running_cause.as_deref() {
            lines.push(Line::from(vec![
                Span::styled("why ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    ellipsize_display(cause, width.saturating_sub(4)),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }
        lines.push(self.action_hint_line());
        lines
    }

    fn runtime_progress_line(&self) -> Line<'static> {
        let mut spans = vec![
            Span::styled("runtime ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.supervisor_status.to_string(),
                supervisor_status_style(self.supervisor_status),
            ),
            Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
            Span::styled("seen ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.latest_output_at
                    .map(event_age)
                    .unwrap_or_else(|| "-".to_owned()),
                Style::default().fg(Color::White),
            ),
            Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
            Span::styled("supervisor ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                event_age(self.supervisor_updated_at),
                Style::default().fg(Color::White),
            ),
        ];
        if let Some(pid) = self.child_pid {
            spans.extend([
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("pid ", Style::default().fg(Color::DarkGray)),
                Span::styled(pid.to_string(), Style::default().fg(Color::White)),
            ]);
        }
        if let Some(retry_at) = self.next_retry_at {
            spans.extend([
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("retry ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format_unix_time_compact(retry_at),
                    Style::default().fg(Color::Yellow),
                ),
            ]);
        }
        Line::from(spans)
    }

    fn action_hint_line(&self) -> Line<'static> {
        let mut spans = vec![Span::styled(
            "actions ",
            Style::default().fg(Color::DarkGray),
        )];
        match self.status {
            AgentStatus::Starting | AgentStatus::Running => {
                spans.extend([
                    footer_key("a"),
                    Span::raw(" attach  "),
                    footer_key("Space"),
                    Span::raw(" pause  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                ]);
            }
            AgentStatus::Paused => {
                spans.extend([
                    footer_key("Space"),
                    Span::raw(" unpause  "),
                    footer_key("m"),
                    Span::raw(" more  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                ]);
            }
            AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention => {
                spans.extend([
                    footer_key("m"),
                    Span::raw(" more  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                    Span::raw("  "),
                    footer_key(":"),
                    Span::raw(" commands"),
                ]);
            }
        }
        Line::from(spans)
    }
}

#[derive(Clone, Copy)]
enum DashboardSelection {
    Spacer,
    Role(usize),
    Agent(usize, usize),
}

impl DashboardSelection {
    fn is_selectable(self) -> bool {
        !matches!(self, Self::Spacer)
    }
}

fn agent_count_in_rows(rows: &[DashboardSelection]) -> usize {
    rows.iter()
        .filter(|row| matches!(row, DashboardSelection::Agent(_, _)))
        .count()
}

fn selected_agent_position_in_rows(
    rows: &[DashboardSelection],
    selected: usize,
) -> Option<(usize, usize)> {
    let total = agent_count_in_rows(rows);
    if total == 0 || !matches!(rows.get(selected), Some(DashboardSelection::Agent(_, _))) {
        return None;
    }
    let current = rows
        .iter()
        .take(selected + 1)
        .filter(|row| matches!(row, DashboardSelection::Agent(_, _)))
        .count();
    Some((current, total))
}

#[derive(Clone, Copy)]
enum QueueSelection {
    Header(usize),
    Item {
        queue_index: usize,
        item_index: usize,
    },
}

fn load_dashboard_snapshot(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
) -> Result<DashboardSnapshot> {
    let roles = if let Some(role) = role_filter {
        vec![role.clone()]
    } else {
        list_roles(project)?
    };
    let (notices, notices_loading, notices_updated_at) = load_notice_lines(project)?;
    let mut snapshot = DashboardSnapshot {
        channels: load_status_channel_rows(project)?,
        queues: load_all_status_queue_rows(project)?,
        updates: load_dashboard_updates(project)?,
        notices,
        notices_loading,
        notices_updated_at,
        ..DashboardSnapshot::default()
    };
    for role in roles {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        let active = active_agent_count(&role_paths)?;
        let mut agents = Vec::new();
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived && !include_archived {
                continue;
            }
            let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
            let supervisor = load_supervisor_state(&agent_paths)?;
            let quota_waiting = matches!(
                supervisor.status,
                SupervisorStatus::WaitingForQuota | SupervisorStatus::WaitingForProvider
            );
            let running_cause = dashboard_agent_running_cause(&agent_paths, &state, &supervisor)?;
            let current_step = config
                .steps
                .get(state.current_step % config.steps.len().max(MIN_ROLE_STEP_COUNT))
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned());
            agents.push(DashboardAgent {
                role: role.clone(),
                agent: agent.clone(),
                status: state.status,
                summary,
                detail,
                channels: state
                    .channels
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                current_step,
                run_count: state.run_count,
                pane_id: state.pane_id.clone(),
                quota_waiting,
                supervisor_status: supervisor.status,
                supervisor_updated_at: supervisor.updated_at,
                child_pid: supervisor.child_pid,
                next_retry_at: supervisor.next_retry_at,
                latest_output_at: latest_agent_output_at(&agent_paths, &state, &supervisor),
                running_cause,
                paused_by_user: state.paused_by_user,
                created_at: state.created_at,
                updated_at: state.updated_at,
            });
        }
        snapshot.roles.push(DashboardRole {
            slug: role,
            status: config.status,
            mode: config.mode,
            parallel: format!("{active}/{}", config.parallel),
            expose: config
                .expose
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            auto_archive: config.auto_archive,
            trigger_count: config.triggers.len(),
            agents,
        });
    }
    Ok(snapshot)
}

fn dashboard_agent_running_cause(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    supervisor: &SupervisorState,
) -> Result<Option<String>> {
    if !matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        return Ok(None);
    }
    if supervisor.status == SupervisorStatus::WaitingForQuota {
        return Ok(Some(
            supervisor
                .last_event
                .as_ref()
                .map(|event| format!("quota wait · {event}"))
                .unwrap_or_else(|| "quota wait".to_owned()),
        ));
    }
    if supervisor.status == SupervisorStatus::WaitingForProvider {
        return Ok(Some(
            supervisor
                .last_event
                .as_ref()
                .map(|event| format!("provider wait · {event}"))
                .unwrap_or_else(|| "provider wait".to_owned()),
        ));
    }
    if let Some(trigger) = trigger_context_markdown_summary(&agent_paths.trigger_context())? {
        return Ok(Some(format!("triggered · {trigger}")));
    }
    if let Some(note) = state.note.as_deref()
        && !note.trim().is_empty()
    {
        return Ok(Some(note.trim().to_owned()));
    }
    if supervisor.status == SupervisorStatus::Restarting {
        return Ok(Some("runtime restart".to_owned()));
    }
    Ok(Some("user-started or resumed manually".to_owned()))
}

fn latest_agent_output_at(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    supervisor: &SupervisorState,
) -> Option<u64> {
    supervisor
        .last_run_id
        .or_else(|| {
            matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
                .then_some(state.run_count + 1)
        })
        .and_then(|run_id| file_modified_unix(&agent_paths.run(run_id).transcript_text()))
        .or_else(|| {
            (1..=state.run_count)
                .rev()
                .find_map(|run_id| file_modified_unix(&agent_paths.run(run_id).transcript_text()))
        })
}

fn trigger_context_markdown_summary(path: &Path) -> Result<Option<String>> {
    let Some(text) = io::read_optional_text(path)? else {
        return Ok(None);
    };
    let mut kind = None;
    let mut details = Vec::new();
    for line in text.lines() {
        let Some((key, value)) = line
            .trim()
            .strip_prefix("- ")
            .and_then(|line| line.split_once(':'))
        else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('`');
        match key {
            "trigger kind" => kind = Some(value.to_owned()),
            "source role" | "source agent" | "queue" | "reason" => {
                details.push(value.to_owned());
            }
            _ => {}
        }
    }
    Ok(kind.map(|kind| {
        if details.is_empty() {
            kind
        } else {
            format!("{kind} · {}", details.join("/"))
        }
    }))
}

fn load_dashboard_ui_state(project: &ProjectPaths) -> Result<DashboardUiState> {
    let path = dashboard_ui_state_path(project);
    let Some(state) = io::read_optional_text(&path)? else {
        return Ok(DashboardUiState::default());
    };
    let parsed = toml::from_str::<DashboardUiState>(&state)
        .with_context(|| format!("Failed to parse `{}`", path.display()))?;
    if parsed.version == DASHBOARD_UI_STATE_VERSION {
        Ok(parsed)
    } else {
        Ok(DashboardUiState::default())
    }
}

fn dashboard_ui_state_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("dashboard.toml")
}

fn pad_display(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        value.to_owned()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

fn state_heading_line(
    label: &'static str,
    slug: &str,
    value: &str,
    value_style: Style,
    width: usize,
) -> Line<'static> {
    let prefix = format!("{label} {slug}: ");
    let value = ellipsize_display(value, width.saturating_sub(prefix.chars().count()));
    Line::from(vec![
        Span::styled(format!("{label} "), Style::default().fg(Color::DarkGray)),
        Span::styled(
            slug.to_owned(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(": ", Style::default().fg(Color::DarkGray)),
        Span::styled(value, value_style),
    ])
}

fn notice_style(severity: NoticeSeverity) -> Style {
    match severity {
        NoticeSeverity::Info => Style::default().fg(Color::Cyan),
        NoticeSeverity::Action => Style::default().fg(Color::Blue),
        NoticeSeverity::Warn => Style::default().fg(Color::Yellow),
        NoticeSeverity::Error => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

fn centered_popup(area: Rect, max_width: u16, max_height: u16) -> Rect {
    let width = area
        .width
        .saturating_sub(DASHBOARD_MODAL_HORIZONTAL_MARGIN)
        .min(max_width);
    let height = area
        .height
        .saturating_sub(DASHBOARD_MODAL_VERTICAL_MARGIN)
        .min(max_height);
    Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    }
}

fn inset_rect(area: Rect, horizontal_margin: u16, vertical_margin: u16) -> Rect {
    let width = area.width.saturating_sub(horizontal_margin);
    let height = area.height.saturating_sub(vertical_margin);
    Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    }
}

fn draw_search_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    target: SearchTarget,
    query: &str,
    visible_rows: usize,
) {
    let popup = Rect {
        x: area.x
            + area
                .width
                .saturating_sub(area.width.min(DASHBOARD_SEARCH_MAX_WIDTH))
                / DASHBOARD_CENTERING_DIVISOR,
        y: area.y
            + area
                .height
                .saturating_sub(DASHBOARD_SEARCH_HEIGHT + DASHBOARD_FOOTER_HEIGHT),
        width: area.width.min(DASHBOARD_SEARCH_MAX_WIDTH),
        height: DASHBOARD_SEARCH_HEIGHT.min(area.height),
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "/",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(query.to_owned()),
            Span::styled(
                format!("  {visible_rows} {}", target.visible_label()),
                Style::default().fg(Color::DarkGray),
            ),
        ]))
        .block(dashboard_block(target.title()).border_style(Style::default().fg(Color::Cyan))),
        popup,
    );
}

fn draw_help_overlay(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_popup(area, DASHBOARD_HELP_MAX_WIDTH, DASHBOARD_HELP_MAX_HEIGHT);
    let mut lines = vec![Line::from(Span::styled(
        "dashboard commands",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ))];
    for chunk in DASHBOARD_COMMANDS.chunks(3) {
        let mut spans = Vec::new();
        for spec in chunk {
            if let Some(key) = spec.key {
                spans.push(footer_key(key));
                spans.push(Span::raw(" "));
            }
            spans.push(Span::styled(spec.label, Style::default().fg(Color::White)));
            spans.push(Span::styled("   ", Style::default().fg(Color::DarkGray)));
        }
        lines.push(Line::from(spans));
    }
    lines.extend([
        Line::from(""),
        Line::from(vec![
            footer_key("x"),
            Span::raw(" toggles the extended run timeline only inside the detail screen."),
        ]),
        Line::from(vec![
            footer_key("a/r/t/n/0"),
            Span::raw(" filter the timeline by lane while it is active."),
        ]),
    ]);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(dashboard_block("Help").border_style(Style::default().fg(Color::Cyan)))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn draw_advanced_menu(frame: &mut Frame<'_>, area: Rect) {
    let width = area.width.min(DASHBOARD_ADVANCED_MAX_WIDTH);
    let height = area.height.min(DASHBOARD_ADVANCED_MAX_HEIGHT);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    };
    let block = dashboard_block("Advanced").border_style(Style::default().fg(Color::Magenta));
    let inner = block.inner(popup);
    frame.render_widget(Clear, popup);
    frame.render_widget(block, popup);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from(vec![
                footer_key("n"),
                Span::raw(" "),
                Span::styled("new role", Style::default().fg(Color::White)),
                Span::styled(
                    "  draft a role through the standard review workflow",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("r"),
                Span::raw(" "),
                Span::styled("retry errored", Style::default().fg(Color::White)),
                Span::styled(
                    "  wake quota/rate-limit/OOM retry backoffs",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("o"),
                Span::raw(" "),
                Span::styled("open project", Style::default().fg(Color::White)),
                Span::styled(
                    "  open this project directory",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("p"),
                Span::raw(" "),
                Span::styled("provider settings", Style::default().fg(Color::White)),
                Span::styled(
                    "  Codex accounts, model, and thinking level",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("x"),
                Span::raw(" "),
                Span::styled("archived", Style::default().fg(Color::White)),
                Span::styled(
                    "  toggle archived agents",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("Esc"),
                Span::raw(" "),
                Span::styled("close", Style::default().fg(Color::White)),
            ]),
        ])),
        inner,
    );
}

fn provider_settings_action_line() -> Line<'static> {
    Line::from(vec![
        footer_key("Enter"),
        Span::raw(" switch  "),
        footer_key("a"),
        Span::raw(" add  "),
        footer_key("d"),
        Span::raw(" delete  "),
        footer_key("m"),
        Span::raw(" model/thinking  "),
        footer_key("r"),
        Span::raw(" active  "),
        footer_key("Esc"),
        Span::raw(" close"),
    ])
}

fn provider_accounts_heading(count: usize, selected: usize) -> Line<'static> {
    let position = if count == 0 {
        0
    } else {
        selected.saturating_add(1).min(count)
    };
    Line::from(vec![
        Span::styled(
            "accounts",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" {position}/{count}"),
            Style::default().fg(Color::DarkGray),
        ),
    ])
}

fn provider_account_name_width(rows: &[ProviderAccountRow]) -> usize {
    rows.iter()
        .map(|row| row.name.chars().count())
        .max()
        .unwrap_or(7)
        .clamp(7, 24)
}

fn provider_account_line(
    account: &ProviderAccountRow,
    selected: bool,
    width: usize,
    name_width: usize,
) -> Line<'static> {
    let (status, status_style) = provider_account_status(account);
    let prefix_style = if account.active {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let fixed_width = name_width + status.chars().count() + 13;
    let detail = provider_account_detail(account, width.saturating_sub(fixed_width).max(12));
    Line::from(vec![
        dashboard_span(
            if account.active { "● " } else { "• " },
            prefix_style,
            selected,
        ),
        dashboard_span(
            format!("{:<name_width$}", account.name),
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(status, status_style, selected),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(detail, Style::default().fg(Color::Gray), selected),
    ])
}

fn provider_account_status(account: &ProviderAccountRow) -> (String, Style) {
    if let Some(wait_until) = account.quota_wait_until {
        (
            format!("quota until {}", format_unix_time_compact(wait_until)),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else if account.active {
        (
            "active".to_owned(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        ("ready".to_owned(), Style::default().fg(Color::Cyan))
    }
}

fn provider_account_detail(account: &ProviderAccountRow, width: usize) -> String {
    let mut detail = account.codex_home.display().to_string();
    if let Some(last_used_at) = account.last_used_at {
        let _ = write!(detail, " · used {}", format_unix_time_compact(last_used_at));
    }
    if let Some(event) = &account.last_quota_event {
        let _ = write!(detail, " · {}", compact_single_line(event, 80));
    }
    ellipsize_display(&detail, width)
}

fn provider_settings_project_config_line(project: &ProjectPaths) -> Line<'static> {
    match project_config(project) {
        Ok(config) => {
            let model = config
                .providers
                .codex
                .model
                .unwrap_or_else(|| "Codex default model".to_owned());
            let thinking = config
                .providers
                .codex
                .thinking_level
                .map(|level| level.to_string())
                .unwrap_or_else(|| "Codex default".to_owned());
            Line::from(vec![
                Span::styled("project: ", Style::default().fg(Color::DarkGray)),
                Span::styled(model, Style::default().fg(Color::White)),
                Span::styled(" · thinking ", Style::default().fg(Color::DarkGray)),
                Span::styled(thinking, Style::default().fg(Color::White)),
            ])
        }
        Err(err) => Line::from(vec![
            Span::styled("project: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("unavailable ({err:#})"),
                Style::default().fg(Color::Red),
            ),
        ]),
    }
}

struct DashboardLayoutAreas {
    tabs: Rect,
    notices: Rect,
    state: Rect,
    agents: Rect,
    runtime: Rect,
    main: Rect,
    footer: Rect,
}

fn dashboard_layout(area: Rect, narrow: bool) -> DashboardLayoutAreas {
    if narrow {
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(DASHBOARD_TAB_BAR_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_NOTICE_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_STATE_HEIGHT),
                Constraint::Min(DASHBOARD_NARROW_AGENTS_MIN_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_RUNTIME_HEIGHT),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        DashboardLayoutAreas {
            tabs: root[0],
            notices: root[1],
            state: root[2],
            agents: root[3],
            runtime: root[4],
            main: root[1].union(root[2]).union(root[3]).union(root[4]),
            footer: root[5],
        }
    } else {
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(DASHBOARD_TAB_BAR_HEIGHT),
                Constraint::Length(DASHBOARD_STATE_BAND_HEIGHT),
                Constraint::Min(DASHBOARD_AGENTS_MIN_HEIGHT),
                Constraint::Length(DASHBOARD_RUNTIME_BAND_HEIGHT),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let state_band = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(root[1]);
        DashboardLayoutAreas {
            tabs: root[0],
            notices: state_band[0],
            state: state_band[1],
            agents: root[2],
            runtime: root[3],
            main: root[1].union(root[2]).union(root[3]),
            footer: root[4],
        }
    }
}

fn dashboard_block(title: &'static str) -> Block<'static> {
    dashboard_block_with_title(Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )))
}

fn dashboard_block_with_title(title: Line<'static>) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

fn panel_title(label: &'static str, position: Option<(usize, usize)>) -> Line<'static> {
    let mut spans = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )];
    if let Some((current, total)) = position
        && total > 0
    {
        spans.push(Span::styled(
            format!(" {current}/{total}"),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(spans)
}

fn dynamic_panel_title(label: String, position: Option<(usize, usize)>) -> Line<'static> {
    let mut spans = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )];
    if let Some((current, total)) = position
        && total > 0
    {
        spans.push(Span::styled(
            format!(" {current}/{total}"),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(spans)
}

fn visible_panel_rows(area: Rect) -> usize {
    usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
        .max(DASHBOARD_MIN_VISIBLE_ROWS)
}

fn max_scroll_offset(content_len: usize, visible_rows: usize) -> usize {
    content_len.saturating_sub(visible_rows.max(DASHBOARD_MIN_VISIBLE_ROWS))
}

fn clamped_scroll_offset(scroll: usize, content_len: usize, visible_rows: usize) -> usize {
    scroll.min(max_scroll_offset(content_len, visible_rows))
}

fn clamped_scroll_offset_u16(scroll: u16, content_len: usize, visible_rows: usize) -> u16 {
    clamped_scroll_offset(usize::from(scroll), content_len, visible_rows).min(usize::from(u16::MAX))
        as u16
}

fn render_scrollbar(frame: &mut Frame<'_>, area: Rect, content_len: usize, scroll: usize) {
    let viewport = visible_panel_rows(area);
    if content_len <= viewport || area.width < 3 || area.height <= DASHBOARD_PANEL_BORDER_ROWS {
        return;
    }
    let mut state = ScrollbarState::new(content_len)
        .position(clamped_scroll_offset(scroll, content_len, viewport))
        .viewport_content_length(viewport);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .thumb_style(Style::default().fg(Color::Cyan)),
        area,
        &mut state,
    );
}

fn tab_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray).bg(Color::Black)
    }
}

fn draw_dashboard_footer(frame: &mut Frame<'_>, area: Rect, fallback: Line<'static>) {
    frame.render_widget(
        Paragraph::new(fallback).style(Style::default().fg(Color::White).bg(Color::Black)),
        area,
    );
}

fn footer_line_from_pairs(pairs: &[(&'static str, &'static str)], width: usize) -> Line<'static> {
    let mut accepted = Vec::new();
    let mut used = 0;
    let mut omitted = false;
    for pair in pairs {
        let pair_width = footer_pair_width(*pair, !accepted.is_empty());
        if used + pair_width <= width {
            accepted.push(*pair);
            used += pair_width;
        } else {
            omitted = true;
        }
    }
    if omitted {
        let ellipsis_width = if accepted.is_empty() { 1 } else { 3 };
        while !accepted.is_empty() && used + ellipsis_width > width {
            let pair = accepted.pop().expect("accepted was checked non-empty");
            used = used.saturating_sub(footer_pair_width(pair, !accepted.is_empty()));
        }
    }

    let mut spans = Vec::new();
    for (key, label) in accepted {
        if !spans.is_empty() {
            spans.push(footer_text("  "));
        }
        spans.push(footer_text("("));
        spans.push(footer_key(key));
        spans.push(footer_text(" "));
        spans.push(footer_hint_label(label));
        spans.push(footer_text(")"));
    }
    let ellipsis_width = if spans.is_empty() { 1 } else { 3 };
    if omitted && used + ellipsis_width <= width {
        if !spans.is_empty() {
            spans.push(footer_text("  "));
        }
        spans.push(footer_text("…"));
    }
    Line::from(spans)
}

fn footer_pair_width((key, label): (&str, &str), needs_separator: bool) -> usize {
    usize::from(needs_separator) * 2 + key.chars().count() + 3 + label.chars().count()
}

fn command_footer_pair(command: PaletteCommand) -> Option<(&'static str, &'static str)> {
    DASHBOARD_COMMANDS
        .iter()
        .find(|spec| matches_palette_command(spec.command, command))
        .and_then(|spec| spec.key.map(|key| (key, spec.label)))
}

fn matches_palette_command(left: PaletteCommand, right: PaletteCommand) -> bool {
    std::mem::discriminant(&left) == std::mem::discriminant(&right)
}

fn palette_command_aliases(command: PaletteCommand) -> &'static [&'static str] {
    match command {
        PaletteCommand::Detail => &["open", "inspect", "enter"],
        PaletteCommand::New => &["create", "agent", "n"],
        PaletteCommand::More => &["continue", "query", "reply", "m"],
        PaletteCommand::Attach => &["attach", "terminal", "session", "transcript", "a"],
        PaletteCommand::Search => &["filter", "find", "/", "slash"],
        PaletteCommand::Channels => &["channel", "channels", "artifact", "artifacts", "alert"],
        PaletteCommand::Queues => &["queue", "trigger", "o"],
        PaletteCommand::ToggleQueueCollapsed => &["collapse", "expand", "space"],
        PaletteCommand::Events => &["events", "timeline", "history"],
        PaletteCommand::Updates => &["updates", "report list", "journal"],
        PaletteCommand::Advanced => &["maintenance", "technical"],
        PaletteCommand::Update => &["progress", "summary", "report", "u"],
        PaletteCommand::Check => &["health", "audit", "c"],
        PaletteCommand::Assist => &["operator", "configure", "manage", "i"],
        PaletteCommand::OpenProject => &["open", "project", "directory", "folder", "finder"],
        PaletteCommand::OpenSelectedDir => &["open", "directory", "folder", "agent", "channel"],
        PaletteCommand::NewChannel => &["create", "channel", "artifact"],
        PaletteCommand::ToggleRole => &["pause", "unpause", "resume", "space"],
        PaletteCommand::ToggleAllRoles => &["pause all", "unpause all", "shift u"],
        PaletteCommand::TriggerRole => &["manual", "launch"],
        PaletteCommand::ArchiveAgent => &["hide", "archive"],
        PaletteCommand::ToggleArchived => &["archived", "show archived"],
        PaletteCommand::RetryErrored => &["retry", "quota", "rate limit", "error"],
        PaletteCommand::ProviderSettings => &[
            "codex", "provider", "settings", "login", "account", "model", "thinking",
        ],
        PaletteCommand::Help => &["?", "keys"],
        PaletteCommand::Quit => &["exit", "close", "q"],
    }
}

fn palette_detail(entry: &PaletteEntry, width: usize) -> String {
    let key_width = entry.key.map(|key| key.chars().count() + 4).unwrap_or(0);
    let fixed_width =
        2 + entry.label.chars().count() + key_width + ui::FIELD_SEPARATOR.chars().count();
    let detail_width = width.saturating_sub(fixed_width);
    if detail_width == 0 {
        String::new()
    } else {
        format!(
            "{}{}",
            ui::FIELD_SEPARATOR,
            ellipsize_display(&entry.detail, detail_width)
        )
    }
}

fn footer_key(value: &'static str) -> Span<'static> {
    Span::styled(
        value,
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn footer_text(value: &'static str) -> Span<'static> {
    Span::styled(value, Style::default().fg(Color::Gray).bg(Color::Black))
}

fn footer_hint_label(value: &'static str) -> Span<'static> {
    Span::styled(
        value,
        Style::default()
            .fg(Color::DarkGray)
            .bg(Color::Black)
            .add_modifier(Modifier::ITALIC),
    )
}

fn queue_count_style(value: usize) -> Style {
    match value {
        0 => Style::default().fg(Color::DarkGray),
        1..=QUEUE_WARNING_MAX_COUNT => Style::default().fg(Color::Yellow),
        _ => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

fn channel_lines(channel: &StatusChannelRow) -> Vec<Line<'static>> {
    let spans = vec![
        Span::styled("● ", Style::default().fg(Color::Green)),
        Span::styled(
            channel.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
        Span::styled(
            channel.artifacts.to_string(),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(" artifacts", Style::default().fg(Color::DarkGray)),
    ];
    let mut lines = vec![Line::from(spans)];
    if let Some(latest) = channel.latest.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default().fg(Color::DarkGray)),
            Span::styled("latest ", Style::default().fg(Color::DarkGray)),
            Span::styled(latest.to_owned(), Style::default().fg(Color::White)),
        ]));
    }
    lines
}

fn channel_tab_line(
    channel: &StatusChannelRow,
    selected: bool,
    width: usize,
    unseen_alert: bool,
) -> Line<'static> {
    let marker_style = if unseen_alert {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Green)
    };
    let marker = if unseen_alert { " ! " } else { " ● " };
    let latest = channel.latest.as_deref().unwrap_or("-");
    let fixed_width = 1
        + marker.chars().count()
        + channel.name.chars().count()
        + ui::FIELD_SEPARATOR.chars().count()
        + channel.artifacts.to_string().chars().count()
        + " artifacts".chars().count()
        + ui::FIELD_SEPARATOR.chars().count()
        + "latest ".chars().count();
    Line::from(vec![
        dashboard_span(
            if selected { "▸" } else { " " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(marker, marker_style, selected),
        dashboard_span(
            channel.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            channel.artifacts.to_string(),
            Style::default().fg(Color::Cyan),
            selected,
        ),
        dashboard_span(" artifacts", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span("latest ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            ellipsize_display(latest, width.saturating_sub(fixed_width).max(1)),
            Style::default().fg(Color::White),
            selected,
        ),
    ])
}

fn queue_header_line(queue: &StatusQueueRow, selected: bool, collapsed: bool) -> Line<'static> {
    let mut spans = vec![
        dashboard_span(
            if selected { "▸ " } else { "  " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(
            queue.kind.label(),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(" ", Style::default(), selected),
        dashboard_span(
            queue.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            queue.count.to_string(),
            queue_count_style(queue.count),
            selected,
        ),
        dashboard_span(" pending", Style::default().fg(Color::DarkGray), selected),
    ];
    if queue.locked {
        spans.extend([
            dashboard_span(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray),
                selected,
            ),
            dashboard_span(
                queue
                    .active
                    .as_ref()
                    .map(|active| format!("merging {}", active.label))
                    .unwrap_or_else(|| "locked".to_owned()),
                Style::default().fg(Color::Yellow),
                selected,
            ),
        ]);
    }
    if collapsed {
        spans.extend([
            dashboard_span(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray),
                selected,
            ),
            dashboard_span("collapsed", Style::default().fg(Color::DarkGray), selected),
        ]);
    }
    Line::from(spans)
}

fn active_queue_item_line(queues: &[StatusQueueRow], width: usize) -> Option<Line<'static>> {
    queues.iter().find(|queue| queue.locked).and_then(|queue| {
        let active = queue.active.as_ref()?;
        let mut label = format!("currently running queued trigger {}", active.label);
        if let Some(locked_at) = queue.locked_at {
            label.push_str(&format!(" · {} elapsed", event_age(locked_at)));
        }
        if queue.count > 0 {
            label.push_str(&format!(" · {} pending", queue.count));
        }
        Some(Line::from(vec![
            Span::styled(
                "● ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                ellipsize_display(&label, width.saturating_sub(2)),
                Style::default().fg(Color::Yellow),
            ),
        ]))
    })
}

fn queue_item_lines(
    project: &ProjectPaths,
    queue: &StatusQueueRow,
    width: usize,
    selected_item: Option<usize>,
) -> Vec<Line<'static>> {
    match queue.kind {
        StatusQueueKind::Trigger => queue_trigger_item_lines(project, queue, width, selected_item),
    }
}

fn queue_trigger_item_lines(
    project: &ProjectPaths,
    queue: &StatusQueueRow,
    width: usize,
    selected_item: Option<usize>,
) -> Vec<Line<'static>> {
    let state = match load_trigger_queue(project, &queue.name) {
        Ok(state) => state,
        Err(err) => return queue_error_lines(err),
    };
    if state.items.is_empty() {
        return vec![queue_empty_line(
            "no queued trigger items",
            selected_item.is_none(),
        )];
    }
    state
        .items
        .iter()
        .enumerate()
        .flat_map(|(index, item)| {
            let selected = selected_item == Some(index);
            [
                queue_child_line(
                    &ellipsize_display(
                        &format!(
                            "{}. {} · enqueued {}",
                            index + 1,
                            item.role,
                            format_unix_time(item.enqueued_at)
                        ),
                        width.saturating_sub(3),
                    ),
                    selected,
                ),
                queue_child_meta_line(
                    &ellipsize_display(
                        &trigger_cause_summary(&item.cause),
                        width.saturating_sub(5),
                    ),
                    selected,
                ),
            ]
        })
        .collect()
}

fn queue_child_line(value: &str, selected: bool) -> Line<'static> {
    Line::from(vec![
        dashboard_span("   • ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            value.to_owned(),
            Style::default().fg(Color::White),
            selected,
        ),
    ])
}

fn queue_child_meta_line(value: &str, selected: bool) -> Line<'static> {
    Line::from(vec![
        dashboard_span("     ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            value.to_owned(),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
    ])
}

fn queue_empty_line(value: &'static str, _selected: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled("   ", Style::default().fg(Color::DarkGray)),
        Span::styled(value, Style::default().fg(Color::DarkGray)),
    ])
}

fn queue_error_lines(error: anyhow::Error) -> Vec<Line<'static>> {
    vec![Line::from(Span::styled(
        format!("   failed to load queue: {error:#}"),
        Style::default().fg(Color::Red),
    ))]
}

fn load_queue_detail_lines(
    project: &ProjectPaths,
    queues: &[StatusQueueRow],
    selection: QueueSelection,
) -> Result<Vec<Line<'static>>> {
    let Some(queue) = selection.queue_index().and_then(|index| queues.get(index)) else {
        return Ok(vec![Line::from("No queue selected.")]);
    };
    let mut lines = vec![
        section_line("queue"),
        Line::from(format!("name: {}", queue.name)),
        Line::from(format!("kind: {}", queue.kind.label())),
        Line::from(format!("items: {}", queue.count)),
        Line::from(format!("locked: {}", queue.locked)),
    ];
    if let Some(active) = &queue.active {
        lines.push(Line::from(format!("active: {}", active.label)));
    }
    if let Some(locked_at) = queue.locked_at {
        lines.push(Line::from(format!(
            "locked at: {} · {} elapsed",
            format_unix_time(locked_at),
            event_age(locked_at)
        )));
    }
    lines.push(Line::from(""));
    match selection {
        QueueSelection::Header(_) => {
            lines.push(section_line("overview"));
            lines.extend(queue_item_lines(project, queue, usize::MAX, None));
        }
        QueueSelection::Item { item_index, .. } => {
            let state = load_trigger_queue(project, &queue.name)?;
            let Some(item) = state.items.get(item_index) else {
                lines.push(Line::from("queue item is no longer present"));
                return Ok(lines);
            };
            lines.push(section_line("trigger item"));
            lines.push(Line::from(format!("position: {}", item_index + 1)));
            lines.push(Line::from(format!("role: {}", item.role)));
            lines.push(Line::from(format!(
                "enqueued: {}",
                format_unix_time(item.enqueued_at)
            )));
            lines.push(Line::from(format!(
                "cause: {}",
                trigger_cause_summary(&item.cause)
            )));
        }
    }
    Ok(lines)
}

fn load_channel_detail_lines(
    project: &ProjectPaths,
    channel: &StatusChannelRow,
) -> Result<Vec<Line<'static>>> {
    let path = project.channel_dir(&ChannelSlug::parse(&channel.name)?);
    let entries = channel_artifact_entries(&path)?;
    let mut lines = vec![
        section_line("channel"),
        Line::from(format!("name: {}", channel.name)),
        Line::from(format!("path: {}", path.display())),
        Line::from(format!("artifacts: {}", entries.len())),
    ];
    if let Some(latest) = channel.latest.as_deref() {
        lines.push(Line::from(format!("latest: {latest}")));
    }
    lines.push(Line::from(""));
    lines.push(section_line("recent artifacts"));
    if entries.is_empty() {
        lines.push(Line::from(Span::styled(
            "no published artifacts",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(
            entries
                .into_iter()
                .take(CHANNEL_DETAIL_ARTIFACT_LIMIT)
                .map(|entry| {
                    Line::from(vec![
                        Span::styled(
                            if entry.is_dir { "dir " } else { "file" },
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(entry.name, Style::default().fg(Color::White)),
                        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(event_age(entry.modified), Style::default().fg(Color::Cyan)),
                    ])
                }),
        );
    }
    Ok(lines)
}

impl QueueSelection {
    fn queue_index(self) -> Option<usize> {
        match self {
            Self::Header(index) => Some(index),
            Self::Item { queue_index, .. } => Some(queue_index),
        }
    }
}

fn load_notice_detail_lines(
    project: &ProjectPaths,
    snapshot: &DashboardSnapshot,
) -> Result<Vec<Line<'static>>> {
    let mut lines = vec![
        section_line("current notices"),
        Line::from(format!(
            "current file: {}",
            notice_current_path(project).display()
        )),
        Line::from(format!(
            "journal: {}",
            notice_journal_path(project).display()
        )),
    ];
    if let Some(updated_at) = snapshot.notices_updated_at {
        lines.push(Line::from(format!(
            "updated: {}",
            format_unix_time(updated_at)
        )));
    }
    lines.push(Line::from(format!("loading: {}", snapshot.notices_loading)));
    lines.push(Line::from(""));
    if snapshot.notices.is_empty() {
        lines.push(Line::from(Span::styled(
            "no current operator notices",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(snapshot.notices.iter().map(|notice| {
            Line::from(vec![
                Span::styled("● ", notice_style(notice.severity)),
                Span::styled(
                    notice_severity_label(notice.severity),
                    notice_style(notice.severity),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(notice.text.clone(), Style::default().fg(Color::White)),
            ])
        }));
    }
    lines.push(Line::from(""));
    lines.push(section_line("journal tail"));
    let journal = io::read_optional_text(&notice_journal_path(project))?.unwrap_or_default();
    let tail = journal
        .lines()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    if tail.is_empty() {
        lines.push(Line::from(Span::styled(
            "no journal entries",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(tail.into_iter().map(|line| {
            Line::from(Span::styled(
                line.to_owned(),
                Style::default().fg(Color::Gray),
            ))
        }));
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest scan logs"));
    let logs = latest_notice_logs(project)?;
    if logs.is_empty() {
        lines.push(Line::from(Span::styled(
            "no notice generator logs",
            Style::default().fg(Color::DarkGray),
        )));
    }
    for log in logs {
        let modified = file_modified_unix(&log)
            .map(format_unix_time)
            .unwrap_or_else(|| "unknown time".to_owned());
        lines.push(Line::from(vec![
            Span::styled("log ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                log.display().to_string(),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(" · ", Style::default().fg(Color::DarkGray)),
            Span::styled(modified, Style::default().fg(Color::Gray)),
        ]));
        let text = io::read_optional_text(&log)?.unwrap_or_default();
        for line in text
            .lines()
            .rev()
            .take(6)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            lines.push(Line::from(Span::styled(
                compact_single_line(line, 180),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }
    Ok(lines)
}

fn latest_notice_logs(project: &ProjectPaths) -> Result<Vec<PathBuf>> {
    let dir = notice_dir(project);
    let Ok(entries) = fs::read_dir(&dir) else {
        return Ok(Vec::new());
    };
    let mut logs = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("Failed to read `{}`", dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("codex-") && name.ends_with(".log") {
            logs.push(path);
        }
    }
    logs.sort_by_key(|path| std::cmp::Reverse(file_modified_unix(path).unwrap_or_default()));
    logs.truncate(2);
    Ok(logs)
}

fn quota_gauge(label: &'static str, limit: &CodexRateLimit) -> Gauge<'static> {
    Gauge::default()
        .gauge_style(
            Style::default()
                .fg(quota_color(limit.used_percent))
                .bg(Color::Black),
        )
        .ratio((limit.used_percent / PERCENT_FULL).clamp(RATIO_EMPTY, RATIO_FULL))
        .label(format!(
            "{label} {:>4.1}% reset {} ({})",
            limit.used_percent,
            human_duration(limit.resets_in_seconds),
            format_unix_time(limit.resets_at)
        ))
}

fn dashboard_role_style(status: RoleStatus) -> Style {
    match status {
        RoleStatus::Draft => Style::default().fg(Color::DarkGray),
        RoleStatus::Active => Style::default().fg(Color::Green),
        RoleStatus::Paused => Style::default().fg(Color::Yellow),
    }
}

fn dashboard_agent_style(status: AgentStatus, quota_waiting: bool) -> Style {
    match status {
        AgentStatus::Starting => Style::default().fg(Color::Cyan),
        AgentStatus::Running if quota_waiting => Style::default().fg(Color::Yellow),
        AgentStatus::Running => Style::default().fg(Color::Green),
        AgentStatus::Paused => Style::default().fg(Color::Yellow),
        AgentStatus::Done => Style::default().fg(Color::Blue),
        AgentStatus::Stopped => Style::default().fg(Color::DarkGray),
        AgentStatus::NeedsAttention => Style::default().fg(Color::Red),
    }
}

fn supervisor_status_style(status: SupervisorStatus) -> Style {
    match status {
        SupervisorStatus::Idle => Style::default().fg(Color::DarkGray),
        SupervisorStatus::Running => Style::default().fg(Color::Green),
        SupervisorStatus::Restarting => Style::default().fg(Color::Yellow),
        SupervisorStatus::WaitingForQuota => Style::default().fg(Color::Yellow),
        SupervisorStatus::WaitingForProvider => Style::default().fg(Color::Yellow),
        SupervisorStatus::NeedsAttention => Style::default().fg(Color::Red),
    }
}

fn quota_color(used_percent: f64) -> Color {
    if used_percent >= 90.0 {
        Color::Red
    } else if used_percent >= 70.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}

fn dashboard_span(text: impl Into<String>, style: Style, selected: bool) -> Span<'static> {
    let style = if selected {
        style
            .add_modifier(Modifier::REVERSED)
            .add_modifier(Modifier::BOLD)
    } else {
        style
    };
    Span::styled(text.into(), style)
}

fn empty_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn load_agent_detail_lines(
    project: &ProjectPaths,
    agent: &DashboardAgent,
    extended: bool,
    can_continue: bool,
) -> Result<Vec<Line<'static>>> {
    let role_paths = RolePaths::new(project.clone(), agent.role.clone());
    let agent_paths = role_paths.agent(agent.agent.clone());
    let state = load_agent(&agent_paths)?;
    let (manifest, manifest_error) = load_agent_manifest_for_display(&agent_paths);
    let supervisor = load_supervisor_state(&agent_paths)?;
    let mut lines = vec![
        Line::from(vec![
            Span::styled("agent ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}/{}", agent.role, agent.agent),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("status: {}", state.status)),
        Line::from(format!("summary: {}", agent.summary)),
        Line::from(format!("detail: {}", empty_dash(&agent.detail))),
        Line::from(format!("runs: {}", state.run_count)),
        Line::from(format!(
            "session: {}",
            state.pane_id.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "data: {}",
            project.agent_data_root(&agent.role, &agent.agent).display()
        )),
        Line::from(format!("paused by user: {}", state.paused_by_user)),
        Line::from(format!("runtime: {}", supervisor.status)),
        Line::from(format!(
            "runtime updated: {}",
            event_age(supervisor.updated_at)
        )),
        Line::from(format!(
            "latest output: {}",
            latest_agent_output_at(&agent_paths, &state, &supervisor)
                .map(event_age)
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!("created: {}", format_unix_time(state.created_at))),
        Line::from(format!("updated: {}", format_unix_time(state.updated_at))),
    ];
    if let Some(pid) = supervisor.child_pid {
        lines.push(Line::from(format!("child pid: {pid}")));
    }
    if let Some(retry_at) = supervisor.next_retry_at {
        lines.push(Line::from(format!(
            "next retry: {} ({})",
            format_unix_time(retry_at),
            event_age(retry_at)
        )));
    }
    if let Some(summary) = manifest.role_summary.as_deref() {
        lines.push(Line::from(format!("manifest summary: {}", summary.trim())));
    }
    if let Some(error) = manifest_error.as_deref() {
        lines.push(Line::from(Span::styled(
            format!("manifest error: {error}"),
            Style::default().fg(Color::Red),
        )));
    }
    if let Some(disposition) = manifest
        .disposition
        .or_else(|| state.last_exit.as_ref().and_then(|exit| exit.disposition))
    {
        lines.push(Line::from(format!("disposition: {disposition}")));
    }
    if let Some(note) = state.note.as_deref() {
        lines.push(Line::from(format!("note: {note}")));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "channels",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )));
    if state.channels.is_empty() {
        lines.push(Line::from(Span::styled(
            "-",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for channel in &state.channels {
            lines.push(Line::from(format!(
                "{} · outbox {}",
                channel,
                agent_paths.channel_dir(channel).display()
            )));
        }
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest run"));
    if let Some(exit) = state.last_exit.as_ref() {
        lines.push(Line::from(format!(
            "{} · {} · {} to {}{}",
            exit.step,
            if exit.success { "success" } else { "failed" },
            format_unix_time(exit.started_at),
            format_unix_time(exit.finished_at),
            exit.disposition
                .map(|disposition| format!(" · disposition: {disposition}"))
                .unwrap_or_default()
        )));
        if let Some(message) = exit.message.as_deref()
            && !message.trim().is_empty()
        {
            lines.push(Line::from(Span::styled(
                message.trim().to_owned(),
                Style::default().fg(Color::DarkGray),
            )));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "no completed run yet",
            Style::default().fg(Color::DarkGray),
        )));
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest reply"));
    match latest_agent_reply(&agent_paths, state.run_count)? {
        Some(reply) => {
            let line_count = reply.lines().count();
            let preview = reply
                .lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| compact_single_line(line, 140))
                .unwrap_or_else(|| "recorded".to_owned());
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{line_count} lines"),
                    Style::default().fg(Color::Green),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(preview, Style::default().fg(Color::Gray)),
            ]));
        }
        None => lines.push(Line::from(Span::styled(
            "No REPLY.md has been recorded yet.",
            Style::default().fg(Color::DarkGray),
        ))),
    }
    lines.push(Line::from(""));
    lines.push(section_line("recent events"));
    let recent_events = load_project_events(project)?
        .into_iter()
        .filter(|event| {
            matches!(
                &event.target,
                EventTarget::Agent { role, agent: event_agent }
                    if *role == agent.role && *event_agent == agent.agent
            )
        })
        .take(8)
        .collect::<Vec<_>>();
    if recent_events.is_empty() {
        lines.push(Line::from(Span::styled(
            "none",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(recent_events.iter().map(|event| event_line(event, false)));
    }
    lines.push(Line::from(""));
    let mut action_spans = vec![footer_key("a"), Span::raw(" attach  ")];
    if can_continue {
        action_spans.extend([footer_key("m"), Span::raw(" more  ")]);
    }
    action_spans.extend([
        footer_key("x"),
        Span::raw(if extended {
            " hide timeline  "
        } else {
            " show timeline  "
        }),
        footer_key("Esc"),
        Span::raw(" close"),
    ]);
    lines.push(Line::from(action_spans));
    if extended {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "run timeline",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )));
        lines.extend(load_run_timeline_lines(&agent_paths, &state)?);
    }
    Ok(lines)
}

fn section_line(title: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ))
}

fn latest_agent_reply(
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<Option<String>> {
    for run_id in (1..=run_count).rev() {
        let Some(reply) = io::read_optional_text(&agent_paths.run(run_id).reply())? else {
            continue;
        };
        let reply = reply.trim();
        if !reply.is_empty() {
            return Ok(Some(reply.to_owned()));
        }
    }
    Ok(None)
}

fn load_run_timeline_lines(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
) -> Result<Vec<Line<'static>>> {
    let mut lines = Vec::new();
    for run_id in 1..=state.run_count {
        let run_paths = agent_paths.run(run_id);
        let exit = read_run_exit(&run_paths, state)?;
        let reply_state = if run_paths.reply().exists() {
            "reply"
        } else {
            "no reply"
        };
        if let Some(exit) = exit {
            lines.push(Line::from(format!(
                "#{run_id} · {} · {} · {} to {} · {}{}",
                exit.step,
                if exit.success { "success" } else { "failed" },
                format_unix_time(exit.started_at),
                format_unix_time(exit.finished_at),
                reply_state,
                exit.disposition
                    .map(|disposition| format!(" · disposition: {disposition}"))
                    .unwrap_or_default()
            )));
            if let Some(message) = exit.message {
                lines.push(Line::from(Span::styled(
                    format!("  {message}"),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        } else {
            lines.push(Line::from(format!(
                "#{run_id} · exit state missing · {reply_state}"
            )));
        }
    }
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
        && agent_paths.run(state.run_count + 1).root().exists()
    {
        lines.push(Line::from(format!(
            "#{} · active or interrupted run",
            state.run_count + 1
        )));
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "No runs have been recorded yet.",
            Style::default().fg(Color::DarkGray),
        )));
    }
    Ok(lines)
}

#[derive(Deserialize)]
struct LegacyPtyRunExit {
    success: bool,
    code: u32,
    signal: Option<String>,
}

fn read_run_exit(
    run_paths: &crate::state::RunPaths,
    state: &AgentState,
) -> Result<Option<RunExitState>> {
    let path = run_paths.exit();
    if !path.exists() {
        return Ok(None);
    }
    let text = io::read_text(&path)?;
    match toml::from_str::<RunExitState>(&text) {
        Ok(exit) => Ok(Some(exit)),
        Err(full_error) => match toml::from_str::<LegacyPtyRunExit>(&text) {
            Ok(exit) => Ok(Some(recover_legacy_run_exit(run_paths, state, exit))),
            Err(_) => Err(anyhow!(full_error))
                .with_context(|| format!("Failed to parse `{}`", path.display())),
        },
    }
}

fn recover_legacy_run_exit(
    run_paths: &crate::state::RunPaths,
    state: &AgentState,
    exit: LegacyPtyRunExit,
) -> RunExitState {
    if let Some(last_exit) = state
        .last_exit
        .as_ref()
        .filter(|last_exit| last_exit.run_id == run_paths.run_id)
    {
        return last_exit.clone();
    }
    let finished_at = file_modified_unix(&run_paths.exit()).unwrap_or_else(unix_timestamp);
    RunExitState {
        run_id: run_paths.run_id,
        step: StepSlug::parse("unknown").expect("static fallback step slug is valid"),
        started_at: file_modified_unix(&run_paths.prompt())
            .or_else(|| file_modified_unix(&run_paths.step()))
            .unwrap_or(finished_at),
        finished_at,
        success: exit.success,
        code: exit.code,
        signal: exit.signal,
        disposition: None,
        message: Some("recovered from a raw PTY exit file".to_owned()),
    }
}

fn trigger_cause_summary(cause: &TriggerCause) -> String {
    match cause {
        TriggerCause::Manual { reason } => format!(
            "manual{}",
            reason
                .as_deref()
                .map(|reason| format!(" · {reason}"))
                .unwrap_or_default()
        ),
        TriggerCause::RoleStepFinished {
            source_role,
            source_step,
        } => format!("role step finished · {source_role}/{source_step}"),
        TriggerCause::RoleAgentFinished {
            source_role,
            source_agent,
            run_id,
            step,
        } => format!("agent finished · {source_role}/{source_agent} run {run_id} step {step}"),
        TriggerCause::QueueIdle {
            queue,
            idle_seconds,
            ..
        } => format!("queue idle · {queue} · {}", human_duration(*idle_seconds)),
        TriggerCause::Elapsed {
            source_role,
            interval_seconds,
            event_index,
            ..
        } => format!(
            "elapsed · {source_role} · {} · event {event_index}",
            human_duration(*interval_seconds)
        ),
    }
}

#[derive(Clone)]
struct ProjectEvent {
    timestamp: u64,
    lane: EventLane,
    title: String,
    detail: String,
    target: EventTarget,
}

#[derive(Clone, PartialEq, Eq)]
struct ProjectEventKey {
    timestamp: u64,
    lane: EventLane,
    title: String,
    detail: String,
}

#[derive(Clone)]
enum EventTarget {
    Agent { role: RoleSlug, agent: AgentId },
    Queue(StatusQueueKey),
    Notice,
    None,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventFilter {
    All,
    Agents,
    Runs,
    Triggers,
    Notices,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventLane {
    Agent,
    Run,
    Trigger,
    Notice,
}

impl EventLane {
    fn label(self) -> &'static str {
        match self {
            Self::Agent => "agents",
            Self::Run => "runs",
            Self::Trigger => "triggers",
            Self::Notice => "notices",
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Self::Agent => "●",
            Self::Run => "◆",
            Self::Trigger => "▲",
            Self::Notice => "!",
        }
    }

    fn style(self) -> Style {
        match self {
            Self::Agent => Style::default().fg(Color::Blue),
            Self::Run => Style::default().fg(Color::Green),
            Self::Trigger => Style::default().fg(Color::Magenta),
            Self::Notice => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        }
    }

    fn all() -> &'static [Self] {
        &[Self::Agent, Self::Run, Self::Trigger, Self::Notice]
    }
}

impl EventFilter {
    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Agents => "agents",
            Self::Runs => "runs",
            Self::Triggers => "triggers",
            Self::Notices => "notices",
        }
    }

    fn matches(self, lane: EventLane) -> bool {
        match self {
            Self::All => true,
            Self::Agents => lane == EventLane::Agent,
            Self::Runs => lane == EventLane::Run,
            Self::Triggers => lane == EventLane::Trigger,
            Self::Notices => lane == EventLane::Notice,
        }
    }
}

impl ProjectEvent {
    fn key(&self) -> ProjectEventKey {
        ProjectEventKey {
            timestamp: self.timestamp,
            lane: self.lane,
            title: self.title.clone(),
            detail: self.detail.clone(),
        }
    }
}

fn restore_event_selection(
    events: &[ProjectEvent],
    selected: &mut usize,
    key: Option<&ProjectEventKey>,
) {
    if let Some(key) = key
        && let Some(index) = events.iter().position(|event| event.key() == *key)
    {
        *selected = index;
        return;
    }
    *selected = (*selected).min(events.len().saturating_sub(1));
}

fn project_event_lines(
    events: &[ProjectEvent],
    width: usize,
    selected: usize,
    filter: EventFilter,
    query: &str,
    search_active: bool,
) -> Vec<Line<'static>> {
    if events.is_empty() {
        return vec![
            event_filter_line(filter, query, search_active),
            Line::from(Span::styled(
                "No matching project events.",
                Style::default().fg(Color::DarkGray),
            )),
        ];
    }
    let mut lines = vec![event_filter_line(filter, query, search_active)];
    lines.extend(event_graph_lines(events, width));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "recent events",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )));
    for (index, event) in events
        .iter()
        .take(DASHBOARD_EVENT_LOG_MAX_EVENTS)
        .enumerate()
    {
        lines.push(event_line(event, index == selected));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        footer_key("Enter"),
        Span::raw(" jump  "),
        footer_key("a/r/t/n/0"),
        Span::raw(" filter  "),
        footer_key("/"),
        Span::raw(" search  "),
        footer_key("↑↓"),
        Span::raw(" scroll  "),
        footer_key("Esc"),
        Span::raw(" dashboard"),
    ]));
    lines
}

fn event_filter_line(filter: EventFilter, query: &str, search_active: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled("filter ", Style::default().fg(Color::DarkGray)),
        Span::styled(filter.label(), Style::default().fg(Color::Cyan)),
        Span::styled(" · search ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if query.trim().is_empty() {
                if search_active { "" } else { "-" }
            } else {
                query.trim()
            }
            .to_owned(),
            if search_active {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            },
        ),
    ])
}

fn load_project_events(project: &ProjectPaths) -> Result<Vec<ProjectEvent>> {
    let mut events = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            events.push(ProjectEvent {
                timestamp: state.created_at,
                lane: EventLane::Agent,
                title: format!("{role}/{agent} created"),
                detail: String::new(),
                target: EventTarget::Agent {
                    role: role.clone(),
                    agent: agent.clone(),
                },
            });
            events.push(ProjectEvent {
                timestamp: state.updated_at,
                lane: EventLane::Agent,
                title: format!("{role}/{agent} {}", state.status),
                detail: state.note.clone().unwrap_or_default(),
                target: EventTarget::Agent {
                    role: role.clone(),
                    agent: agent.clone(),
                },
            });
            for run_id in 1..=state.run_count {
                let exit_path = agent_paths.run(run_id).exit();
                match read_run_exit(&agent_paths.run(run_id), &state) {
                    Ok(Some(exit)) => events.push(ProjectEvent {
                        timestamp: exit.finished_at,
                        lane: EventLane::Run,
                        title: format!("{role}/{agent} run {run_id} {}", exit.step),
                        detail: run_event_detail(&exit),
                        target: EventTarget::Agent {
                            role: role.clone(),
                            agent: agent.clone(),
                        },
                    }),
                    Ok(None) => {}
                    Err(err) => events.push(ProjectEvent {
                        timestamp: fs::metadata(&exit_path)
                            .and_then(|metadata| metadata.modified())
                            .ok()
                            .and_then(system_time_to_unix)
                            .unwrap_or(state.updated_at),
                        lane: EventLane::Run,
                        title: format!("{role}/{agent} run {run_id} exit unreadable"),
                        detail: err.to_string(),
                        target: EventTarget::Agent {
                            role: role.clone(),
                            agent: agent.clone(),
                        },
                    }),
                }
            }
        }
    }
    load_queue_events(project, &mut events)?;
    load_notice_events(project, &mut events)?;
    for update in load_dashboard_updates(project)? {
        events.push(ProjectEvent {
            timestamp: update.created_at,
            lane: EventLane::Notice,
            title: format!("update {}", update.id),
            detail: update.title,
            target: EventTarget::None,
        });
    }
    events.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| left.title.cmp(&right.title))
    });
    events.dedup_by(|left, right| {
        left.timestamp == right.timestamp
            && left.lane == right.lane
            && left.title == right.title
            && left.detail == right.detail
    });
    Ok(events)
}

fn run_event_detail(exit: &RunExitState) -> String {
    let mut detail = format!(
        "{}{}",
        if exit.success { "success" } else { "failed" },
        exit.disposition
            .map(|disposition| format!(" · disposition: {disposition}"))
            .unwrap_or_default()
    );
    if let Some(message) = &exit.message
        && !message.trim().is_empty()
    {
        detail.push_str(ui::FIELD_SEPARATOR);
        detail.push_str(message.trim());
    }
    detail
}

fn load_queue_events(project: &ProjectPaths, events: &mut Vec<ProjectEvent>) -> Result<()> {
    let queue_dir = project.runtime_dir().join("trigger-queues");
    if queue_dir.exists() {
        for entry in fs::read_dir(&queue_dir)
            .with_context(|| format!("Failed to read `{}`", queue_dir.display()))?
        {
            let entry = entry.context("Failed to read trigger queue entry")?;
            if entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                != Some("toml")
            {
                continue;
            }
            let path = entry.path();
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            for item in load_trigger_queue(project, name)?.items {
                events.push(ProjectEvent {
                    timestamp: item.enqueued_at,
                    lane: EventLane::Trigger,
                    title: format!("{} trigger queued", item.role),
                    detail: trigger_cause_summary(&item.cause),
                    target: EventTarget::Queue(StatusQueueKey {
                        kind: StatusQueueKind::Trigger,
                        name: name.to_owned(),
                    }),
                });
            }
        }
    }
    Ok(())
}

fn load_notice_events(project: &ProjectPaths, events: &mut Vec<ProjectEvent>) -> Result<()> {
    let Some(text) = io::read_optional_text(&notice_current_path(project))? else {
        return Ok(());
    };
    let timestamp = fs::metadata(notice_current_path(project))
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(system_time_to_unix)
        .unwrap_or_else(unix_timestamp);
    for notice in text.lines().filter_map(parse_notice_line) {
        events.push(ProjectEvent {
            timestamp,
            lane: EventLane::Notice,
            title: notice.text,
            detail: notice_severity_label(notice.severity).to_owned(),
            target: EventTarget::Notice,
        });
    }
    Ok(())
}

fn system_time_to_unix(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn file_modified_unix(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(system_time_to_unix)
}

fn notice_severity_label(severity: NoticeSeverity) -> &'static str {
    match severity {
        NoticeSeverity::Info => "info",
        NoticeSeverity::Action => "action",
        NoticeSeverity::Warn => "warn",
        NoticeSeverity::Error => "error",
    }
}

fn event_graph_lines(events: &[ProjectEvent], width: usize) -> Vec<Line<'static>> {
    let graph_width = width
        .saturating_sub(DASHBOARD_EVENT_TIMELINE_WIDTH)
        .max(DASHBOARD_EVENT_TIMELINE_WIDTH)
        .min(width.saturating_sub(12).max(DASHBOARD_EVENT_TIMELINE_WIDTH));
    let newest = events
        .iter()
        .map(|event| event.timestamp)
        .max()
        .unwrap_or(0);
    let oldest = events
        .iter()
        .map(|event| event.timestamp)
        .min()
        .unwrap_or(newest);
    let span = newest.saturating_sub(oldest).max(1);
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                "activity map",
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(
                    "  oldest {} · newest {}",
                    format_unix_time(oldest),
                    format_unix_time(newest)
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        Line::from(Span::styled(
            format!("        {}▶", "─".repeat(graph_width)),
            Style::default().fg(Color::DarkGray),
        )),
    ];
    for lane in EventLane::all() {
        let mut cells = vec![None; graph_width.max(1)];
        for event in events.iter().filter(|event| event.lane == *lane) {
            let offset = event.timestamp.saturating_sub(oldest);
            let index = ((offset as f64 / span as f64) * (cells.len().saturating_sub(1) as f64))
                .round() as usize;
            let last = cells.len() - 1;
            cells[index.min(last)] = Some(*lane);
        }
        let mut spans = vec![Span::styled(
            format!("{:<8}", lane.label()),
            Style::default().fg(Color::DarkGray),
        )];
        for cell in cells {
            if let Some(lane) = cell {
                spans.push(Span::styled(lane.symbol(), lane.style()));
            } else {
                spans.push(Span::styled("·", Style::default().fg(Color::DarkGray)));
            }
        }
        lines.push(Line::from(spans));
    }
    lines
}

fn event_line(event: &ProjectEvent, selected: bool) -> Line<'static> {
    let detail = if event.detail.trim().is_empty() {
        String::new()
    } else {
        format!("{}{}", ui::FIELD_SEPARATOR, event.detail.trim())
    };
    Line::from(vec![
        dashboard_span(
            if selected { "▸ " } else { "  " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(event.lane.symbol(), event.lane.style(), selected),
        dashboard_span(" ", Style::default(), selected),
        dashboard_span(
            format!("{:<16}", event_age(event.timestamp)),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            event.title.clone(),
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(detail, Style::default().fg(Color::DarkGray), selected),
    ])
}

fn event_age(timestamp: u64) -> String {
    let now = unix_timestamp();
    if timestamp == now {
        "now".to_owned()
    } else if timestamp > now {
        format!("in {}", human_duration(timestamp - now))
    } else {
        format!("{} ago", human_duration(now - timestamp))
    }
}

fn status_project(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
    offer_report: bool,
) -> Result<()> {
    if offer_report && std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        return run_dashboard(
            project.clone(),
            role_filter.cloned(),
            include_archived,
            DASHBOARD_REFRESH_INTERVAL,
        );
    }
    fire_queue_idle_triggers(project)?;
    let roles = load_status_roles(project, role_filter, include_archived)?;
    let queue_rows = load_status_queue_rows(project)?;
    let widths = StatusWidths::from_rows(&roles, &queue_rows);

    println!("{}", ui::section_heading("roles"));
    for (index, role) in roles.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_role_row(role, &widths);
        for agent in &role.agents {
            print_agent_row(agent, &widths);
        }
    }

    if !queue_rows.is_empty() {
        println!("\n{}", ui::section_heading("queues"));
        for row in &queue_rows {
            print_queue_row(row, &widths);
        }
    }

    println!("\n{}", ui::section_heading("quota"));
    print_quota_footer(project)?;
    Ok(())
}

fn generate_project_update(project: &ProjectPaths) -> Result<()> {
    let id = unix_timestamp().to_string();
    let root = update_root(project, &id);
    io::ensure_dir(&root)?;
    let update_path = update_markdown_path(project, &id);
    let prompt_path = root.join("PROMPT.md");
    let reply_path = root.join("REPLY.md");
    io::write_text(
        &prompt_path,
        &assemble_project_update_prompt(project, &update_path)?,
    )?;
    let exit = runner::run_command_in_dir_no_stdin_quiet(
        codex_exec_file_command(
            &project.root,
            &prompt_path,
            None,
            CodexConversationPolicy::WorkspaceWrite,
            &reply_path,
        )?,
        &root,
    )?;
    if !exit.success {
        bail!("`codex exec` failed while generating project update");
    }
    let update = io::read_optional_text(&update_path)?.unwrap_or_default();
    if update.trim().is_empty() {
        bail!(
            "update generator did not write a nonempty `{}`",
            update_path.display()
        );
    }
    io::write_toml(
        &update_index_path(project),
        &UpdateIndexState {
            version: UPDATE_INDEX_STATE_VERSION,
            last_generated_at: id.parse().ok(),
        },
    )
}

fn load_dashboard_updates(project: &ProjectPaths) -> Result<Vec<DashboardUpdate>> {
    let dir = update_dir(project);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut updates = Vec::new();
    for entry in
        fs::read_dir(&dir).with_context(|| format!("Failed to read `{}`", dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let id = entry.file_name().to_string_lossy().into_owned();
        let path = update_markdown_path(project, &id);
        let Some(text) = io::read_optional_text(&path)? else {
            continue;
        };
        let created_at = id
            .parse::<u64>()
            .ok()
            .or_else(|| file_modified_unix(&path))
            .unwrap_or_default();
        let (title, preview) = update_title_and_preview(&text);
        updates.push(DashboardUpdate {
            id,
            created_at,
            title,
            preview,
            path,
        });
    }
    updates.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    Ok(updates)
}

fn load_update_detail_lines(project: &ProjectPaths, update: &str) -> Result<Vec<Line<'static>>> {
    let path = update_markdown_path(project, update);
    let text = io::read_text(&path)?;
    let mut lines = vec![
        Line::from(vec![
            Span::styled("update ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                update.to_owned(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("path: {}", path.display())),
        Line::from(""),
    ];
    lines.extend(text.lines().map(|line| Line::from(line.to_owned())));
    Ok(lines)
}

fn update_title_and_preview(text: &str) -> (String, String) {
    let mut nonempty = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let title = nonempty
        .next()
        .map(|line| line.trim_start_matches('#').trim().to_owned())
        .filter(|line| !line.is_empty())
        .unwrap_or_else(|| "Untitled update".to_owned());
    let preview = nonempty
        .next()
        .map(|line| line.trim_start_matches('#').trim().to_owned())
        .unwrap_or_default();
    (title, preview)
}

fn assemble_project_update_prompt(project: &ProjectPaths, update_path: &Path) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think project update")?;
    writeln!(
        prompt,
        "\nYou are running as the update generator for the `think status` Updates tab."
    )?;
    writeln!(
        prompt,
        "Only write the new update file. Do not edit channel artifacts, start or stop agents, publish artifacts, or kill processes."
    )?;
    writeln!(
        prompt,
        "\nProject root: `{}`\nUpdate file to write: `{}`",
        project.root.display(),
        update_path.display()
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nWrite a compact Markdown update for the operator. It should summarize the absolute ",
            "state of the overall project task, the biggest advances since the previous update, ",
            "and the current state of promising prongs, blockers, and next actions. Be plain, ",
            "critical, and evidence-based; do not be obsequious or exaggerate progress."
        )
    )?;
    append_previous_updates(&mut prompt, project)?;
    append_current_project_context(&mut prompt, &project.root)?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nInspect role/agent manifests, transcripts, trigger queues, channel logs, project data, ",
            "and prior updates directly. Prefer evidence over optimism. Put the complete update ",
            "in the requested update file before exiting."
        )
    )?;
    Ok(prompt)
}

fn append_previous_updates(prompt: &mut String, project: &ProjectPaths) -> Result<()> {
    let updates = load_dashboard_updates(project)?;
    if updates.is_empty() {
        writeln!(prompt, "\n# Previous updates\n\nNone.")?;
        return Ok(());
    }
    writeln!(prompt, "\n# Previous updates")?;
    for update in updates.iter().take(UPDATE_CONTEXT_LIMIT).rev() {
        writeln!(
            prompt,
            "\n## {} · {}\n",
            update.id,
            format_unix_time(update.created_at)
        )?;
        let text = io::read_optional_text(&update.path)?.unwrap_or_default();
        writeln!(prompt, "{}", text.trim())?;
    }
    Ok(())
}

fn update_dir(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("updates")
}

fn update_root(project: &ProjectPaths, id: &str) -> PathBuf {
    update_dir(project).join(id)
}

fn update_markdown_path(project: &ProjectPaths, id: &str) -> PathBuf {
    update_root(project, id).join("UPDATE.md")
}

fn update_index_path(project: &ProjectPaths) -> PathBuf {
    update_dir(project).join("index.toml")
}

fn notice_dir(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("notices")
}

fn notice_current_path(project: &ProjectPaths) -> PathBuf {
    notice_dir(project).join("current.md")
}

fn notice_journal_path(project: &ProjectPaths) -> PathBuf {
    notice_dir(project).join("journal.md")
}

fn notice_lock_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("locks").join("notices.lock")
}

fn ensure_notice_task_started(project: &ProjectPaths) -> Result<()> {
    if let Err(err) = start_notice_task(project, false) {
        record_notice_generator_start_failure(project, &err);
    }
    Ok(())
}

fn force_notice_task_started(project: &ProjectPaths) -> Result<()> {
    if let Err(err) = start_notice_task(project, true) {
        record_notice_generator_start_failure(project, &err);
    }
    Ok(())
}

fn start_notice_task(project: &ProjectPaths, force: bool) -> Result<()> {
    if lock::is_active(&notice_lock_path(project))? || (!force && notice_is_fresh(project)?) {
        return Ok(());
    }
    Command::new(think_child_executable()?)
        .arg("run-child")
        .arg("notices")
        .arg("--project")
        .arg(&project.root)
        .current_dir(&project.root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("Failed to start notice generator")?;
    Ok(())
}

fn think_child_executable() -> Result<PathBuf> {
    let current = std::env::current_exe().context("Failed to locate current executable")?;
    if current.exists() {
        return Ok(current);
    }
    which::which("think").context("Failed to locate `think` on PATH")
}

fn record_notice_generator_start_failure(project: &ProjectPaths, error: &anyhow::Error) {
    let line = format!("warn: notice generator failed to start: {error:#}");
    let path = notice_current_path(project);
    let write_result = (|| -> Result<()> {
        io::ensure_dir(&notice_dir(project))?;
        let current = io::read_optional_text(&path)?.unwrap_or_default();
        if current.lines().any(|existing| existing == line) {
            return Ok(());
        }
        let mut next = current
            .lines()
            .filter(|existing| !existing.contains("notice generator failed to start"))
            .collect::<Vec<_>>()
            .join("\n");
        if !next.is_empty() {
            next.push('\n');
        }
        next.push_str(&line);
        next.push('\n');
        io::write_text(&path, &next)
    })();
    let _ = write_result;
}

fn notice_is_fresh(project: &ProjectPaths) -> Result<bool> {
    let Ok(metadata) = fs::metadata(notice_current_path(project)) else {
        return Ok(false);
    };
    let Ok(modified) = metadata.modified() else {
        return Ok(false);
    };
    Ok(SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default()
        < NOTICE_REFRESH_INTERVAL)
}

fn load_notice_lines(project: &ProjectPaths) -> Result<(Vec<DashboardNotice>, bool, Option<u64>)> {
    let loading = lock::is_active(&notice_lock_path(project))?;
    let path = notice_current_path(project);
    let updated_at = file_modified_unix(&path);
    let lines = io::read_optional_text(&path)?
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter_map(parse_notice_line)
        .take(NOTICE_LINE_LIMIT)
        .collect::<Vec<_>>();
    Ok((lines, loading, updated_at))
}

fn notice_line_is_actionable(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    let lower = strip_notice_prefix(line).to_ascii_lowercase();
    !matches!(
        lower.as_str(),
        "notice scan active"
            | "scan active"
            | "scanning"
            | "checking"
            | "loading"
            | "loading..."
            | "all clear"
            | "queues empty"
            | "idle"
            | "waiting"
            | "no notices"
            | "no operator notices"
    ) && !lower.starts_with("scanning ")
        && !lower.starts_with("checking ")
        && !lower.starts_with("notice scan ")
}

fn strip_notice_prefix(line: &str) -> &str {
    let Some((prefix, text)) = line.split_once(':') else {
        return line.trim();
    };
    match prefix.trim().to_ascii_lowercase().as_str() {
        "error" | "warn" | "warning" | "action" | "info" | "note" => text.trim(),
        _ => line.trim(),
    }
}

fn parse_notice_line(line: &str) -> Option<DashboardNotice> {
    if !notice_line_is_actionable(line) {
        return None;
    }
    let (severity, text) = if let Some((prefix, text)) = line.split_once(':') {
        match prefix.trim().to_ascii_lowercase().as_str() {
            "error" => (NoticeSeverity::Error, text.trim()),
            "warn" | "warning" => (NoticeSeverity::Warn, text.trim()),
            "action" => (NoticeSeverity::Action, text.trim()),
            "info" | "note" => (NoticeSeverity::Info, text.trim()),
            _ => (NoticeSeverity::Action, line),
        }
    } else {
        (NoticeSeverity::Action, line)
    };
    (!text.is_empty()).then(|| DashboardNotice {
        severity,
        text: text.to_owned(),
    })
}

fn retry_waits_now(project: &ProjectPaths) -> Result<()> {
    let updated = retry_waits_now_inner(project)?;
    println!(
        "retry errored requested; updated {updated} active runtime orchestrator{}",
        if updated == 1 { "" } else { "s" }
    );
    Ok(())
}

fn retry_waits_now_inner(project: &ProjectPaths) -> Result<usize> {
    let now = unix_timestamp();
    io::write_toml(
        &retry_request_path(project),
        &RetryRequestState {
            version: RETRY_REQUEST_VERSION,
            requested_at: now,
        },
    )?;

    let mut updated = 0;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent);
            let state = load_agent(&agent_paths)?;
            if state.archived
                || !matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
            {
                continue;
            }
            let mut supervisor = load_supervisor_state(&agent_paths)?;
            if !matches!(
                supervisor.status,
                SupervisorStatus::WaitingForQuota
                    | SupervisorStatus::WaitingForProvider
                    | SupervisorStatus::Restarting
            ) {
                continue;
            }
            let event = format!("retry requested by user at {}", format_unix_time(now));
            supervisor.next_retry_at = Some(now);
            supervisor.last_event = Some(event.clone());
            save_supervisor_state(&agent_paths, &supervisor)?;
            update_agent_note(&agent_paths, &event)?;
            updated += 1;
        }
    }
    Ok(updated)
}

struct StatusWidths {
    role: usize,
    role_status: usize,
    role_mode: usize,
    parallel: usize,
    agent: usize,
    agent_summary: usize,
    agent_status: usize,
    queue_name: usize,
}

impl StatusWidths {
    fn from_rows(roles: &[StatusRole], queue_rows: &[StatusQueueRow]) -> Self {
        Self {
            role: roles
                .iter()
                .map(|role| role.name.chars().count())
                .max()
                .unwrap_or(1),
            role_status: roles
                .iter()
                .map(|role| role.status.to_string().chars().count())
                .max()
                .unwrap_or(1),
            role_mode: roles
                .iter()
                .map(|role| role.mode.to_string().chars().count())
                .max()
                .unwrap_or(1),
            parallel: roles
                .iter()
                .map(|role| role.parallel.chars().count())
                .max()
                .unwrap_or(1),
            agent: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.name.chars().count())
                .max()
                .unwrap_or(1),
            agent_summary: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.summary.chars().count())
                .max()
                .unwrap_or(1),
            agent_status: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.status.to_string().chars().count())
                .max()
                .unwrap_or(1),
            queue_name: queue_rows
                .iter()
                .map(|row| row.name.chars().count())
                .max()
                .unwrap_or(1),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StatusQueueKind {
    Trigger,
}

impl StatusQueueKind {
    fn label(self) -> &'static str {
        match self {
            Self::Trigger => "queue",
        }
    }
}

struct StatusQueueRow {
    kind: StatusQueueKind,
    name: String,
    count: usize,
    locked: bool,
    active: Option<StatusQueueActive>,
    locked_at: Option<u64>,
}

#[derive(Clone)]
struct StatusQueueActive {
    label: String,
}

struct StatusChannelRow {
    name: String,
    artifacts: usize,
    latest: Option<String>,
}

struct ChannelArtifactEntry {
    name: String,
    modified: u64,
    is_dir: bool,
}

impl StatusChannelRow {
    fn matches_query(&self, query: &str) -> bool {
        query.trim().is_empty()
            || text_matches_query(
                [
                    self.name.clone(),
                    self.artifacts.to_string(),
                    self.latest.clone().unwrap_or_default(),
                ],
                query,
            )
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StatusQueueKey {
    kind: StatusQueueKind,
    name: String,
}

impl From<&StatusQueueRow> for StatusQueueKey {
    fn from(row: &StatusQueueRow) -> Self {
        Self {
            kind: row.kind,
            name: row.name.clone(),
        }
    }
}

fn queue_key_string(queue: &StatusQueueRow) -> String {
    format!("{}:{}", queue.kind.label(), queue.name)
}

fn load_status_channel_rows(project: &ProjectPaths) -> Result<Vec<StatusChannelRow>> {
    list_channels(project)?
        .into_iter()
        .map(|channel| {
            let channel_dir = project.channel_dir(&channel);
            Ok(StatusChannelRow {
                name: channel.to_string(),
                artifacts: count_channel_artifacts(&channel_dir)?,
                latest: latest_channel_artifact(&channel_dir)?,
            })
        })
        .collect()
}

fn count_channel_artifacts(channel_dir: &Path) -> Result<usize> {
    Ok(channel_artifact_entries(channel_dir)?.len())
}

fn latest_channel_artifact(channel_dir: &Path) -> Result<Option<String>> {
    Ok(channel_artifact_entries(channel_dir)?
        .into_iter()
        .next()
        .map(|entry| entry.name))
}

fn channel_artifact_entries(channel_dir: &Path) -> Result<Vec<ChannelArtifactEntry>> {
    if !channel_dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(channel_dir)
        .with_context(|| format!("Failed to read channel `{}`", channel_dir.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", channel_dir.display()))?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        let metadata = entry
            .metadata()
            .with_context(|| format!("Failed to stat `{}`", entry.path().display()))?;
        entries.push(ChannelArtifactEntry {
            name: name.to_string_lossy().to_string(),
            modified: metadata
                .modified()
                .ok()
                .and_then(system_time_to_unix)
                .unwrap_or_default(),
            is_dir: metadata.is_dir(),
        });
    }
    entries.sort_by(|left, right| {
        right
            .modified
            .cmp(&left.modified)
            .then_with(|| right.name.cmp(&left.name))
    });
    Ok(entries)
}

fn load_status_queue_rows(project: &ProjectPaths) -> Result<Vec<StatusQueueRow>> {
    load_status_queue_rows_inner(project, false)
}

fn load_all_status_queue_rows(project: &ProjectPaths) -> Result<Vec<StatusQueueRow>> {
    load_status_queue_rows_inner(project, true)
}

fn load_status_queue_rows_inner(
    project: &ProjectPaths,
    include_empty: bool,
) -> Result<Vec<StatusQueueRow>> {
    let mut rows = Vec::new();
    for name in trigger_queue_names(project)? {
        validate_trigger_queue(&name)?;
        let state = load_trigger_queue(project, &name)?;
        let lock_path = trigger_queue_lock_path(project, &name);
        let locked = lock::is_active(&lock_path)?;
        if !include_empty && state.items.is_empty() && !locked {
            continue;
        }
        rows.push(StatusQueueRow {
            kind: StatusQueueKind::Trigger,
            name,
            count: state.items.len(),
            locked,
            active: None,
            locked_at: locked.then(|| file_modified_unix(&lock_path)).flatten(),
        });
    }
    Ok(rows)
}

fn trigger_queue_names(project: &ProjectPaths) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for trigger in load_role_config(&role_paths)?.triggers {
            if let TriggerConfig::QueueIdle { idle_queue, .. } = &trigger {
                names.insert(idle_queue.clone());
            }
            if let TriggerLaunch::Queued { queue } = trigger_launch(&trigger) {
                names.insert(queue.to_owned());
            }
        }
    }
    collect_queue_names_from_dir(
        &project.runtime_dir().join("trigger-queues"),
        "toml",
        &mut names,
    )?;
    collect_queue_names_from_dir(
        &project.runtime_dir().join("locks").join("trigger-queues"),
        "lock",
        &mut names,
    )?;
    Ok(names)
}

fn trigger_launch(trigger: &TriggerConfig) -> &TriggerLaunch {
    match trigger {
        TriggerConfig::RoleStepFinished { launch, .. }
        | TriggerConfig::RoleAgentFinished { launch, .. }
        | TriggerConfig::QueueIdle { launch, .. }
        | TriggerConfig::Elapsed { launch, .. } => launch,
    }
}

fn collect_queue_names_from_dir(
    dir: &Path,
    extension: &str,
    names: &mut BTreeSet<String>,
) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("Failed to read `{}`", dir.display()))? {
        let entry = entry.context("Failed to read queue entry")?;
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            == Some(extension)
            && let Some(name) = entry.path().file_stem().and_then(|stem| stem.to_str())
        {
            names.insert(name.to_owned());
        }
    }
    Ok(())
}

struct StatusRole {
    name: String,
    status: RoleStatus,
    mode: RoleMode,
    parallel: String,
    expose: String,
    agents: Vec<StatusAgent>,
}

struct StatusAgent {
    name: String,
    status: AgentStatus,
    summary: String,
    detail: String,
}

fn load_status_roles(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
) -> Result<Vec<StatusRole>> {
    let roles = if let Some(role) = role_filter {
        vec![role.clone()]
    } else {
        list_roles(project)?
    };
    roles
        .into_iter()
        .map(|role| load_status_role(project, &role, include_archived))
        .collect()
}

fn load_status_role(
    project: &ProjectPaths,
    role: &RoleSlug,
    include_archived: bool,
) -> Result<StatusRole> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let config = load_role_config(&role_paths)?;
    let active = active_agent_count(&role_paths)?;
    let mut agents = Vec::new();
    for agent in list_agents(&role_paths)? {
        let state = load_agent(&role_paths.agent(agent.clone()))?;
        if state.archived && !include_archived {
            continue;
        }
        let agent_paths = role_paths.agent(agent.clone());
        let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
        agents.push(StatusAgent {
            name: agent.to_string(),
            status: state.status,
            summary,
            detail,
        });
    }
    Ok(StatusRole {
        name: role.to_string(),
        status: config.status,
        mode: config.mode,
        parallel: format!("{active}/{}", config.parallel),
        expose: config
            .expose
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
        agents,
    })
}

fn status_agent_summary_and_detail(
    config: &RoleConfig,
    state: &AgentState,
    agent_paths: &crate::state::AgentPaths,
) -> Result<(String, String)> {
    let (manifest, manifest_error) = load_agent_manifest_for_display(agent_paths);
    let supervisor = load_supervisor_state(agent_paths)?;
    let step = config
        .steps
        .get(state.current_step % config.steps.len().max(MIN_ROLE_STEP_COUNT))
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let supervisor_detail = supervisor_list_detail(&supervisor, state.status);
    let note_detail = state
        .note
        .clone()
        .or_else(|| {
            state
                .paused_by_user
                .then(|| "agent paused by user".to_owned())
        })
        .filter(|note| {
            !((supervisor.status == SupervisorStatus::WaitingForQuota
                && note.to_ascii_lowercase().contains("quota"))
                || (supervisor.status == SupervisorStatus::WaitingForProvider
                    && note.to_ascii_lowercase().contains("provider")))
        })
        .filter(|note| {
            !supervisor_detail
                .as_deref()
                .is_some_and(|detail| detail.contains(note.as_str()))
        });
    let disposition_detail = manifest
        .disposition
        .or_else(|| state.last_exit.as_ref().and_then(|exit| exit.disposition))
        .map(|value| format!("disposition: {value}"));
    let manifest_error_detail = manifest_error
        .as_deref()
        .map(|error| format!("manifest error: {error}"));
    let primary_detail = combine_optional_details(
        combine_optional_details(note_detail, disposition_detail),
        manifest_error_detail,
    );
    let detail = combine_optional_details(
        combine_optional_details(Some(format!("step: {step}")), primary_detail),
        supervisor_detail,
    );
    let summary = manifest_error
        .as_ref()
        .map(|_| "*manifest error*".to_owned())
        .or_else(|| {
            manifest
                .role_summary
                .map(|summary| summary.trim().to_owned())
                .filter(|summary| !summary.is_empty())
        })
        .unwrap_or_else(|| "*name loading*".to_owned());
    Ok((
        summary,
        match (state.archived, detail) {
            (true, Some(detail)) => format!("archived{}{}", ui::FIELD_SEPARATOR, detail),
            (true, None) => "archived".to_owned(),
            (false, Some(detail)) => detail,
            (false, None) => String::new(),
        },
    ))
}

fn print_role_row(role: &StatusRole, widths: &StatusWidths) {
    println!(
        "{} {:<role_width$}{}{}{}{:<role_mode_width$}{}{:<parallel_width$}{}expose: {}",
        ui::role_bullet(role.status),
        role.name,
        ui::FIELD_SEPARATOR,
        ui::status_role_padded(role.status, widths.role_status),
        ui::FIELD_SEPARATOR,
        role.mode,
        ui::FIELD_SEPARATOR,
        role.parallel,
        ui::FIELD_SEPARATOR,
        empty_dash(&role.expose),
        role_width = widths.role,
        role_mode_width = widths.role_mode,
        parallel_width = widths.parallel
    );
}

fn print_agent_row(agent: &StatusAgent, widths: &StatusWidths) {
    let detail = if agent.detail.is_empty() {
        String::new()
    } else {
        format!("{}{}", ui::FIELD_SEPARATOR, agent.detail)
    };

    println!(
        "  {} {:<agent_width$}{}{:<summary_width$}{}{}{}",
        ui::agent_bullet(agent.status),
        agent.name,
        ui::FIELD_SEPARATOR,
        ui::agent_summary(&agent.summary),
        ui::FIELD_SEPARATOR,
        ui::status_agent_padded(agent.status, widths.agent_status),
        detail,
        agent_width = widths.agent,
        summary_width = widths.agent_summary
    );
}

fn combine_optional_details(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) if first == second => Some(first),
        (Some(first), Some(second)) => Some(format!("{first}{}{second}", ui::FIELD_SEPARATOR)),
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(second),
        (None, None) => None,
    }
}

fn supervisor_list_detail(state: &SupervisorState, agent_status: AgentStatus) -> Option<String> {
    if !matches!(
        agent_status,
        AgentStatus::Starting | AgentStatus::Running | AgentStatus::NeedsAttention
    ) {
        return None;
    }
    match state.status {
        SupervisorStatus::Idle | SupervisorStatus::Running => None,
        SupervisorStatus::Restarting => Some(format!(
            "runtime orchestrator restarting{}{}",
            retry_detail(state.next_retry_at),
            state
                .last_event
                .as_deref()
                .map(clean_runtime_event)
                .filter(|event| !event.is_empty())
                .map(|event| format!(" ({event})"))
                .unwrap_or_default()
        )),
        SupervisorStatus::WaitingForQuota => Some(format!(
            "quota{}{}",
            retry_detail(state.next_retry_at),
            state
                .last_event
                .as_deref()
                .map(clean_runtime_event)
                .filter(|event| !event.is_empty())
                .map(|event| format!(" ({event})"))
                .unwrap_or_default()
        )),
        SupervisorStatus::WaitingForProvider => Some(format!(
            "provider{}{}",
            retry_detail(state.next_retry_at),
            state
                .last_event
                .as_deref()
                .map(clean_runtime_event)
                .filter(|event| !event.is_empty())
                .map(|event| format!(" ({event})"))
                .unwrap_or_default()
        )),
        SupervisorStatus::NeedsAttention => Some(format!(
            "runtime orchestrator needs attention{}",
            state
                .last_event
                .as_deref()
                .map(|event| format!(" ({event})"))
                .unwrap_or_default()
        )),
    }
}

fn clean_runtime_event(event: &str) -> String {
    let Some((head, timestamp)) = event.rsplit_once(" at ") else {
        return event.to_owned();
    };
    let Ok(timestamp) = timestamp.parse::<u64>() else {
        return event.to_owned();
    };
    format!("{head} at {}", format_unix_time(timestamp))
}

fn retry_detail(next_retry_at: Option<u64>) -> String {
    next_retry_at
        .map(|timestamp| {
            format!(
                " retry in {}; next {}",
                human_duration(timestamp.saturating_sub(unix_timestamp())),
                format_unix_time(timestamp)
            )
        })
        .unwrap_or_default()
}

fn print_queue_row(row: &StatusQueueRow, widths: &StatusWidths) {
    let detail = format!(
        "{}{}",
        ui::queue_count(row.count),
        if row.locked { " locked" } else { "" }
    );
    println!(
        "{} {:<queue_name_width$}{}{}",
        row.kind.label(),
        row.name,
        ui::FIELD_SEPARATOR,
        detail,
        queue_name_width = widths.queue_name
    );
}

fn print_quota_footer(project: &ProjectPaths) -> Result<()> {
    let cached = load_cached_quota_footer(project)?;
    if !std::io::stdout().is_terminal() || cached.waiting {
        println!("{}", cached.line);
        return Ok(());
    }

    let mut stdout = std::io::stdout().lock();
    let codex_config = project_config(project)?.providers.codex;
    let handle = thread::spawn(move || crate::provider::codex::probe_health(&codex_config));
    let started = Instant::now();
    let mut frame = 0;
    while !handle.is_finished() {
        write!(
            stdout,
            "\r\x1b[2Kquota{}checking {}",
            ui::FIELD_SEPARATOR,
            ui::spinner_frame(frame)
        )?;
        stdout.flush()?;
        frame += 1;
        thread::sleep(QUOTA_PROBE_SPINNER_INTERVAL);
    }
    let probe = handle
        .join()
        .unwrap_or_else(|_| CodexHealth::Unavailable("health thread panicked".to_owned()));
    let line = match probe {
        CodexHealth::Ok => cached.line,
        CodexHealth::Unavailable(reason) => format!(
            "{}{}codex health: {}",
            cached.line,
            ui::FIELD_SEPARATOR,
            reason
        ),
    };
    let elapsed_padding = " ".repeat(
        ((started.elapsed().as_millis() / QUOTA_PROBE_SPINNER_INTERVAL.as_millis()) as usize)
            .min(QUOTA_STATUS_CLEAR_PADDING_MAX),
    );
    writeln!(stdout, "\r\x1b[2K{line}{elapsed_padding}")?;
    Ok(())
}

struct CachedQuotaFooter {
    line: String,
    waiting: bool,
}

fn load_cached_quota_footer(project: &ProjectPaths) -> Result<CachedQuotaFooter> {
    let codex_config = project_config(project)?.providers.codex;
    let mut waiting = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived
                || !matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
            {
                continue;
            }
            let supervisor = load_supervisor_state(&agent_paths)?;
            if supervisor.status == SupervisorStatus::WaitingForQuota {
                waiting.push((
                    format!("{role}/{agent}"),
                    supervisor.next_retry_at,
                    supervisor
                        .last_event
                        .unwrap_or_else(|| "quota backoff".to_owned()),
                ));
            }
        }
    }
    if waiting.is_empty() {
        return Ok(CachedQuotaFooter {
            line: match load_codex_rate_limits_if_interactive(&codex_config) {
                Some(limits) => limits.to_string(),
                None => "usage unavailable".to_owned(),
            },
            waiting: false,
        });
    }
    waiting.sort_by_key(|(_, retry_at, _)| *retry_at);
    let (agent, retry_at, event) = &waiting[0];
    let extra = if waiting.len() > 1 {
        format!("{}{} more", ui::FIELD_SEPARATOR, waiting.len() - 1)
    } else {
        String::new()
    };
    Ok(CachedQuotaFooter {
        line: format!(
            "waiting{}{}{}{}{}{}",
            ui::FIELD_SEPARATOR,
            retry_detail(*retry_at).trim(),
            ui::FIELD_SEPARATOR,
            format!("{agent}: {}{extra}", clean_runtime_event(event)),
            ui::FIELD_SEPARATOR,
            load_codex_rate_limits_if_interactive(&codex_config)
                .map(|limits| limits.to_string())
                .unwrap_or_else(|| "usage unavailable".to_owned())
        ),
        waiting: true,
    })
}

fn load_codex_rate_limits_if_interactive(
    config: &crate::config::CodexProviderConfig,
) -> Option<CodexRateLimits> {
    std::io::stdout()
        .is_terminal()
        .then(|| crate::provider::codex::load_active_rate_limits(config))
        .flatten()
}

fn human_duration(seconds: u64) -> String {
    match seconds {
        0 => "now".to_owned(),
        1..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m{}s", seconds / 60, seconds % 60),
        _ => format!("{}h{}m", seconds / 3600, seconds % 3600 / 60),
    }
}

fn list_channels_only(project: &ProjectPaths) -> Result<()> {
    for channel in list_channels(project)? {
        let path = project.channel_dir(&channel);
        println!(
            "{:<32} artifacts: {}{}",
            channel,
            count_channel_artifacts(&path)?,
            latest_channel_artifact(&path)?
                .map(|artifact| format!(" latest: {artifact}"))
                .unwrap_or_default()
        );
    }
    Ok(())
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
        let markers = vec![
            AttachReplyMarker {
                offset: 3,
                label: "run 1".to_owned(),
            },
            AttachReplyMarker {
                offset: 9,
                label: "run 1 codex".to_owned(),
            },
        ];
        assert_eq!(current_attach_marker_index(&markers, 8), Some(0));
        assert_eq!(current_attach_marker_index(&markers, 9), Some(1));
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
