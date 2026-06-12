use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::config::{
    AgentNameScheme, CodexThinkingLevel, ExposedContext, ProjectTemplate, RoleMode, RoleParallelism,
};
use crate::ids::{ChannelSlug, RoleSlug, StepSlug};
use crate::selection::{AgentSpec, AttachSpec};

#[derive(Debug, Parser)]
#[command(
    name = "think",
    about = "CLI for coordinating persistent agent sessions on complex projects.",
    after_help = "Running `think` with no command is shorthand for `think status`.",
    disable_help_subcommand = true,
    infer_subcommands = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(about = "Send a query a new or existing agent.")]
    More(MoreArgs),
    #[command(about = "Show roles, agents, queues, and runtime state.")]
    Status(StatusArgs),
    #[command(about = "Open the current project directory.")]
    Open,
    #[command(about = "Ask the configured backend to operate this think project.")]
    Assist(AssistArgs),
    #[command(about = "Advanced maintenance commands.", subcommand, hide = true)]
    Advanced(AdvancedCommand),
    #[command(about = "Manage projects.", subcommand, hide = true)]
    Project(ProjectCommand),
    #[command(about = "Manage roles.", subcommand, hide = true)]
    Role(RoleCommand),
    #[command(about = "Manage agents.", subcommand, hide = true)]
    Agent(AgentCommand),
    #[command(about = "Manage publish channels.", subcommand, hide = true)]
    Channel(ChannelCommand),
    #[command(about = "Print help (use --all for more options).")]
    Help(HelpArgs),
    #[command(
        name = "run-child",
        about = "Run internal child process entrypoints.",
        subcommand,
        hide = true
    )]
    RunChild(RunChildCommand),
}

#[derive(Debug, Args)]
pub struct NewAgentArgs {
    #[arg(help = "Role to start an agent for; prompts interactively when omitted.")]
    pub role: Option<RoleSlug>,
    #[arg(
        long,
        conflicts_with = "no_prompt",
        help = "Custom prompt included only for this new agent."
    )]
    pub prompt: Option<String>,
    #[arg(long, help = "Skip the interactive custom-prompt editor.")]
    pub no_prompt: bool,
    #[arg(long, help = "Attach to the new agent after starting it.")]
    pub attach: bool,
}

#[derive(Debug, Args)]
pub struct AssistArgs {
    #[arg(help = "Project operation request; opens an editor when omitted.")]
    pub query: Option<String>,
}

#[derive(Debug, Args)]
pub struct MoreArgs {
    #[arg(help = "Agent to continue, as AGENT or ROLE/AGENT; prompts when omitted.")]
    pub agent: Option<AgentSpec>,
    #[arg(long, help = "Follow-up query; opens an editor when omitted.")]
    pub query: Option<String>,
    #[arg(
        long,
        conflicts_with = "agent",
        help = "Create a new default-role agent instead."
    )]
    pub new: bool,
}

#[derive(Debug, Args)]
pub struct TriggerArgs {
    #[arg(help = "Role to trigger; prompts interactively when omitted.")]
    pub role: Option<RoleSlug>,
    #[arg(long, help = "Reason shown to the triggered agent.")]
    pub reason: Option<String>,
    #[arg(
        long,
        help = "Run asynchronously instead of through the role-named queue."
    )]
    pub async_launch: bool,
}

#[derive(Debug, Args)]
pub struct HelpArgs {
    #[arg(long, help = "Show advanced role, agent, and channel commands.")]
    pub all: bool,
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum RunChildCommand {
    #[command(about = "Run one agent runtime orchestrator.")]
    Orchestrator(RunOrchestratorArgs),
    #[command(about = "Run one trigger queue worker.")]
    Queue(RunQueueArgs),
    #[command(about = "Run the dashboard notice generator.")]
    Notices(RunNoticesArgs),
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum AdvancedCommand {
    #[command(name = "retry-errored", about = "Retry errored runtime backoffs now.")]
    RetryErrored,
    #[command(about = "Manually trigger a role.")]
    Trigger(TriggerArgs),
    #[command(about = "Manage provider configuration.", subcommand)]
    Provider(ProviderCommand),
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum ProviderCommand {
    #[command(about = "Manage the Codex provider.", subcommand)]
    Codex(CodexProviderCommand),
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum CodexProviderCommand {
    #[command(about = "Authenticate or refresh one Codex account.")]
    Login(CodexLoginArgs),
    #[command(about = "List configured Codex accounts and quota state.")]
    List,
    #[command(about = "Select the active Codex account.")]
    Use(CodexUseArgs),
    #[command(about = "Set the current project's Codex model configuration.")]
    Config(CodexConfigArgs),
}

#[derive(Debug, Args)]
pub struct CodexLoginArgs {
    #[arg(help = "Provider account name; prompts when omitted.")]
    pub account: Option<String>,
    #[arg(long, help = "CODEX_HOME directory for this account.")]
    pub home: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct CodexUseArgs {
    #[arg(help = "Provider account name; prompts when omitted.")]
    pub account: Option<String>,
}

#[derive(Debug, Args)]
pub struct CodexConfigArgs {
    #[arg(long, help = "Codex model passed with --model.")]
    pub model: Option<String>,
    #[arg(long, value_enum, help = "Codex model_reasoning_effort value.")]
    pub thinking: Option<CodexThinkingLevel>,
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum AgentCommand {
    #[command(about = "Create and start one new agent.")]
    New(NewAgentArgs),
    #[command(about = "Open the transcript viewer for the project, a role, or an agent.")]
    Attach(AttachArgs),
    #[command(about = "Hide an inactive agent from status output without deleting its files.")]
    Archive(AgentSelectorArgs),
    #[command(about = "Pause an agent without pausing its whole role.")]
    Pause(AgentSelectorArgs),
    #[command(about = "Mark an agent stopped.")]
    Stop(AgentSelectorArgs),
    #[command(about = "Resume an existing paused, stopped, or done agent.")]
    Resume(AgentSelectorArgs),
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum ProjectCommand {
    #[command(about = "Create a new think project directory.")]
    New(ProjectNewArgs),
    #[command(about = "Initialize the current directory as a think project.")]
    Init(ProjectInitArgs),
}

#[derive(Debug, Args)]
pub struct ProjectNewArgs {
    #[arg(help = "Directory to create as a think project.")]
    pub path: PathBuf,
    #[arg(long, value_enum, conflicts_with = "no_template")]
    pub template: Option<ProjectTemplate>,
    #[arg(long)]
    pub no_template: bool,
}

#[derive(Debug, Args)]
pub struct ProjectInitArgs {
    #[arg(long, value_enum, conflicts_with = "no_template")]
    pub template: Option<ProjectTemplate>,
    #[arg(long)]
    pub no_template: bool,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    #[arg(help = "Optional role to focus on.")]
    pub role: Option<RoleSlug>,
    #[arg(long, help = "Include archived agents.")]
    pub all: bool,
    #[arg(
        long,
        help = "Print plain status output instead of opening the dashboard."
    )]
    pub plain: bool,
}

#[derive(Debug, Args)]
pub struct RoleSelectorArgs {
    #[arg(help = "Role to use; prompts interactively when omitted.")]
    pub role: Option<RoleSlug>,
}

#[derive(Debug, Args)]
pub struct AttachArgs {
    #[arg(help = "Project, role, agent, or ROLE/AGENT target; attaches to project when omitted.")]
    pub target: Option<AttachSpec>,
}

#[derive(Debug, Args)]
pub struct RunOrchestratorArgs {
    #[arg(long)]
    pub project: PathBuf,
    #[arg(long)]
    pub role: RoleSlug,
    #[arg(long)]
    pub agent: crate::ids::AgentId,
}

#[derive(Debug, Args)]
pub struct RunQueueArgs {
    #[arg(long)]
    pub project: PathBuf,
    #[arg(long)]
    pub queue: String,
}

#[derive(Debug, Args)]
pub struct RunNoticesArgs {
    #[arg(long)]
    pub project: PathBuf,
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum RoleCommand {
    #[command(about = "Create a role from local arguments and default prompts.")]
    New(RoleNewArgs),
    #[command(
        about = "Create a draft role, optionally asking the configured backend to refine it."
    )]
    Draft(RoleDraftArgs),
    #[command(about = "Open ROLE.md in $EDITOR.")]
    Edit(RoleSelectorArgs),
    #[command(about = "Activate a role and reconcile finite parallelism.")]
    Activate(RoleSelectorArgs),
    #[command(about = "Pause a role.")]
    Pause(RoleSelectorArgs),
}

#[derive(Debug, Args)]
pub struct RoleNewArgs {
    #[arg(help = "Role to create; prompts interactively when omitted.")]
    pub role: Option<RoleSlug>,
    #[arg(long, value_enum, default_value_t = RoleMode::Repeatable)]
    pub mode: RoleMode,
    #[arg(long, value_name = "N|infinite")]
    pub parallel: Option<RoleParallelism>,
    #[arg(
        long = "expose",
        value_enum,
        help = "Expose role-local runtime context to new agents; repeatable."
    )]
    pub expose: Vec<ExposedContext>,
    #[arg(
        long = "step",
        help = "Step slug; repeat to define the ordered step loop."
    )]
    pub steps: Vec<StepSlug>,
    #[arg(long = "names", value_enum, help = "Agent id allocation scheme.")]
    pub agent_names: Option<AgentNameScheme>,
    #[arg(long = "prefix", help = "Prefix prepended to generated agent ids.")]
    pub agent_prefix: Option<String>,
    #[arg(long, help = "Archive successful done agents automatically.")]
    pub auto_archive: bool,
    #[arg(long)]
    pub active: bool,
}

#[derive(Debug, Args)]
pub struct RoleDraftArgs {
    #[arg(help = "Role to draft; prompts interactively when omitted.")]
    pub role: Option<RoleSlug>,
    #[arg(long)]
    pub request: Option<String>,
    #[arg(long)]
    pub feedback: Vec<String>,
    #[arg(long)]
    pub no_review: bool,
    #[arg(long)]
    pub active: bool,
}

#[derive(Debug, Subcommand)]
#[command(infer_subcommands = true)]
pub enum ChannelCommand {
    #[command(about = "Create a publish channel.")]
    New(ChannelSelectorArgs),
    #[command(about = "List publish channels.")]
    List,
}

#[derive(Debug, Args)]
pub struct ChannelSelectorArgs {
    #[arg(help = "Channel to use; prompts interactively when omitted.")]
    pub channel: Option<ChannelSlug>,
}

#[derive(Debug, Args)]
pub struct AgentSelectorArgs {
    #[arg(help = "Agent as AGENT or ROLE/AGENT; prompts when omitted.")]
    pub agent: Option<AgentSpec>,
}
