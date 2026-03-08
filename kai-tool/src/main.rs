use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rand::prelude::IndexedRandom;
use tempfile::NamedTempFile;
use walkdir::WalkDir;

const COMMON_WORDS_MAX_ATTEMPTS: usize = 512;
const DEFAULT_WORKTREE_ROOT: &str = ".worktrees";
const KAI_CONFIG_REL_PATH: &str = ".kai/config.toml";
const SUBMODULE_POINTER_COMMIT_MESSAGE: &str = "chore: bump submodule commit pointers";
const WARNING_ANSI_BOLD_YELLOW: &str = "\x1b[1;33m";
const WARNING_ANSI_RESET: &str = "\x1b[0m";
const SUBMODULE_CHANGED_FILES_PREVIEW_LIMIT: usize = 8;
const WORKTREE_CREATE_PROGRESS_BASE_STEPS: u64 = 5;
const COMMON_WORDS: &[&str] = &[
    "acorn", "amber", "angel", "apple", "april", "argon", "arrow", "atlas", "bacon", "badge",
    "baker", "beach", "beard", "berry", "black", "blaze", "bloom", "blue", "boat", "breeze",
    "brick", "brook", "brush", "cable", "camel", "candy", "caper", "cello", "charm", "chess",
    "cider", "cloud", "clover", "coffee", "comet", "coral", "cotton", "crane", "cream", "crest",
    "cross", "crown", "dance", "dawn", "delta", "dingo", "drift", "dusk", "eagle", "earth",
    "ember", "entry", "equal", "fairy", "fable", "faith", "fawn", "feast", "fern", "field",
    "flame", "flint", "flora", "flute", "focus", "forge", "fox", "frame", "fresh", "frost",
    "fruit", "glade", "glint", "globe", "grace", "grain", "grand", "grape", "grass", "green",
    "grove", "happy", "harbor", "haze", "heart", "honey", "horse", "hotel", "house", "hover",
    "human", "hush", "icicle", "iris", "island", "jewel", "jolly", "judge", "juice", "kayak",
    "kettle", "kitty", "knight", "laser", "latch", "laugh", "leaf", "lemon", "light", "lilac",
    "lunar", "mango", "maple", "march", "meadow", "merit", "metal", "midday", "mint", "model",
    "monkey", "moose", "moss", "mouse", "music", "myth", "navy", "nectar", "nickel", "noble",
    "noodle", "north", "oasis", "ocean", "olive", "opal", "orbit", "otter", "paper", "pearl",
    "pepper", "petal", "piano", "pilot", "pixel", "plaza", "plume", "pocket", "pond", "poppy",
    "prism", "pulse", "quiet", "quilt", "radar", "rain", "raven", "red", "ridge", "river",
    "rocket", "rose", "round", "royal", "ruby", "sable", "salad", "salt", "satin", "scale",
    "scene", "scout", "shade", "shadow", "shark", "sheen", "shell", "shore", "silver", "sketch",
    "sky", "smile", "solar", "sound", "spark", "spice", "spike", "spoon", "sport", "spring",
    "square", "stack", "star", "stone", "storm", "straw", "sun", "swirl", "table", "tango",
    "taper", "tide", "tiger", "toast", "trail", "train", "treat", "tulip", "union", "urban",
    "vapor", "velvet", "verse", "vivid", "voice", "water", "whale", "wheat", "white", "wind",
    "winter", "wisdom", "wood", "world", "wrench", "yarrow", "yellow", "young", "zebra", "zest",
];

#[derive(Debug, Parser)]
#[command(
    name = "kai",
    about = "Utilities for AI and coding workflows.",
    after_help = "Shorthands:\n  lg  llm-get\n  a   agent\n  wc  worktree create\n  wa  worktree agent\n  wo  worktree open\n  wd  worktree delete\n\nWorkspace setup:\n  init        Create .kai/config.toml in the current repo root\n"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(
        name = "llm-get",
        alias = "lg",
        about = "Concatenate files into a formatted listing for LLM consumption.",
        after_help = "Shorthand: lg",
        long_about = "Concatenate files into a formatted listing for LLM consumption. Directories are traversed recursively; by default only .py,.cpp,.rs are included from directories. If present, AGENTS.md and DESIGN.md are automatically prepended to the listing (use -S/--slim to disable)."
    )]
    LlmGet(LlmGetArgs),

    #[command(
        name = "init",
        about = "Initialize `.kai/config.toml` in the current repository root."
    )]
    Init(InitArgs),

    #[command(
        name = "bump",
        about = "Stage updated submodule pointers in the parent repo (if needed), commit, and push."
    )]
    Bump(BumpArgs),

    #[command(
        name = "agent",
        alias = "a",
        about = "Run codex or claude in the current directory.",
        after_help = "Shorthand: a"
    )]
    Agent(AgentArgs),

    #[command(
        name = "worktree",
        about = "Work with git worktrees under configured `worktree_root` (`.kai/config.toml`)."
    )]
    Worktree(WorktreeArgs),

    #[command(name = "wc", hide = true, about = "Shorthand for `worktree create`.")]
    Wc(WorktreeCreateArgs),

    #[command(name = "wa", hide = true, about = "Shorthand for `worktree agent`.")]
    Wa(WorktreeAgentArgs),

    #[command(name = "wo", hide = true, about = "Shorthand for `worktree open`.")]
    Wo(WorktreeOpenArgs),

    #[command(name = "wd", hide = true, about = "Shorthand for `worktree delete`.")]
    Wd(WorktreeDeleteArgs),
}

#[derive(Debug, Args)]
struct WorktreeArgs {
    #[command(subcommand)]
    command: WorktreeCommands,
}

#[derive(Debug, Subcommand)]
enum WorktreeCommands {
    #[command(
        name = "create",
        about = "Create a git worktree at WORKTREE_ROOT/BRANCH (and a branch BRANCH).",
        after_help = "Shorthand: wc\n\nIf the new worktree contains Cargo.toml, kai starts a background `cargo build` (uses `--workspace` for workspaces). Output is suppressed; failures to spawn are warned."
    )]
    Create(WorktreeCreateArgs),

    #[command(
        name = "agent",
        about = "Open the git worktree at WORKTREE_ROOT/BRANCH with codex or claude.",
        after_help = "Shorthand: wa\n\nIf a new worktree is created and it contains Cargo.toml, kai starts a background `cargo build` (uses `--workspace` for workspaces). Output is suppressed; failures to spawn are warned."
    )]
    Agent(WorktreeAgentArgs),

    #[command(
        name = "open",
        about = "Open the git worktree at WORKTREE_ROOT/BRANCH with bash.",
        after_help = "Shorthand: wo\n\nIf a new worktree is created and it contains Cargo.toml, kai starts a background `cargo build` (uses `--workspace` for workspaces). Output is suppressed; failures to spawn are warned."
    )]
    Open(WorktreeOpenArgs),

    #[command(
        name = "delete",
        about = "Delete the git worktree and local branch at WORKTREE_ROOT/BRANCH.",
        after_help = "Shorthand: wd\n\nBy default this refuses unless the worktree has no unstaged changes and all commits are already merged into `master`. Use --force to bypass safety checks."
    )]
    Delete(WorktreeDeleteArgs),
}

#[derive(Debug, Args)]
struct LlmGetArgs {
    /// Files and/or directories to include. Files are included regardless of extension.
    #[arg(required = true)]
    paths: Vec<PathBuf>,

    /// File extension to include when traversing directories (repeatable). Default: .py .cpp .rs
    #[arg(long = "ext", action = clap::ArgAction::Append)]
    exts: Vec<String>,

    /// Output file path; '-' prints to stdout. Default: copy to clipboard.
    #[arg(short = 'o', long = "out")]
    out: Option<String>,

    /// Slim output: do not include AGENTS.md and DESIGN.md when present.
    #[arg(short = 'S', long = "slim")]
    slim: bool,

    /// Base directory used to render file names (default: CWD).
    #[arg(long = "relative-to")]
    relative_to: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct InitArgs {
    /// Worktree root path written to `.kai/config.toml` (default: .worktrees)
    #[arg(long = "worktree-root", value_name = "PATH", default_value = DEFAULT_WORKTREE_ROOT)]
    worktree_root: String,

    /// Skip interactive confirmation prompts.
    #[arg(short = 'y', long = "yes")]
    yes: bool,
}

#[derive(Debug, Args)]
struct BumpArgs {}

#[derive(Debug, Args)]
struct WorktreeCreateArgs {
    /// Worktree/branch name (default: random `s-WORD`).
    #[arg(value_name = "BRANCH")]
    name: Option<String>,

    /// Use an existing local branch instead of creating a new one (fails if missing).
    #[arg(short = 'e', long = "existing", conflicts_with_all = ["delete", "delete_force"])]
    existing: bool,

    /// Delete an existing local branch before creating a new one.
    #[arg(short = 'd', long = "delete", conflicts_with_all = ["existing", "delete_force"])]
    delete: bool,

    /// Force deletion when --delete would be refused (e.g. if the branch is not fully merged).
    #[arg(
        short = 'f',
        long = "force",
        requires = "delete",
        conflicts_with = "delete_force"
    )]
    force: bool,

    /// Shorthand for `--delete --force`.
    #[arg(short = 'D', conflicts_with_all = ["existing", "delete", "force"])]
    delete_force: bool,

    /// Allow multiple kai sessions in the same worktree.
    #[arg(short = 'M', long = "multiple")]
    multiple: bool,
}

#[derive(Debug, Args)]
struct AgentArgs {
    /// Which tool to run (default: codex).
    #[arg(long = "model", value_enum, default_value_t = WorktreeAgentModel::Codex)]
    model: WorktreeAgentModel,

    /// Open the resume picker instead of starting a new conversation.
    #[arg(long = "resume", conflicts_with = "resume_all")]
    resume: bool,

    /// Resume picker across all conversations (codex: `resume --all`).
    #[arg(long = "resume-all", conflicts_with = "resume")]
    resume_all: bool,
}

#[derive(Debug, Args)]
struct WorktreeAgentArgs {
    /// Worktree/branch name (default: random `s-WORD`).
    #[arg(value_name = "BRANCH")]
    name: Option<String>,

    /// Which tool to open the worktree with (default: codex).
    #[arg(long = "model", value_enum, default_value_t = WorktreeAgentModel::Codex)]
    model: WorktreeAgentModel,

    /// Use an existing local branch instead of creating a new one (fails if missing).
    #[arg(short = 'e', long = "existing", conflicts_with_all = ["delete", "delete_force"])]
    existing: bool,

    /// Delete an existing local branch before creating a new one.
    #[arg(short = 'd', long = "delete", conflicts_with_all = ["existing", "delete_force"])]
    delete: bool,

    /// Force deletion when --delete would be refused (e.g. if the branch is not fully merged).
    #[arg(
        short = 'f',
        long = "force",
        requires = "delete",
        conflicts_with = "delete_force"
    )]
    force: bool,

    /// Shorthand for `--delete --force`.
    #[arg(short = 'D', conflicts_with_all = ["existing", "delete", "force"])]
    delete_force: bool,

    /// Allow multiple kai sessions in the same worktree.
    #[arg(short = 'M', long = "multiple")]
    multiple: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorktreeAgentModel {
    Codex,
    Claude,
}

#[derive(Debug, Args)]
struct WorktreeOpenArgs {
    /// Worktree/branch name (default: random `s-WORD`).
    #[arg(value_name = "BRANCH")]
    name: Option<String>,

    /// Use an existing local branch instead of creating a new one (fails if missing).
    #[arg(short = 'e', long = "existing", conflicts_with_all = ["delete", "delete_force"])]
    existing: bool,

    /// Delete an existing local branch before creating a new one.
    #[arg(short = 'd', long = "delete", conflicts_with_all = ["existing", "delete_force"])]
    delete: bool,

    /// Force deletion when --delete would be refused (e.g. if the branch is not fully merged).
    #[arg(
        short = 'f',
        long = "force",
        requires = "delete",
        conflicts_with = "delete_force"
    )]
    force: bool,

    /// Shorthand for `--delete --force`.
    #[arg(short = 'D', conflicts_with_all = ["existing", "delete", "force"])]
    delete_force: bool,

    /// Allow multiple kai sessions in the same worktree.
    #[arg(short = 'M', long = "multiple")]
    multiple: bool,
}

#[derive(Debug, Args)]
struct WorktreeDeleteArgs {
    /// Worktree/branch name to delete.
    #[arg(value_name = "BRANCH")]
    name: String,

    /// Bypass safety checks and force deletion.
    #[arg(short = 'f', long = "force")]
    force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorktreeBranchMode {
    CreateNew,
    UseExisting,
    DeleteAndRecreate,
}

#[derive(Debug, Clone)]
struct WorktreeWorkspace {
    repo_root: PathBuf,
    worktrees_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct WorktreeInvocationContext {
    workspace: WorktreeWorkspace,
    spawn_rel_to_repo_root: PathBuf,
    invoked_from_submodule: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentResumeMode {
    Off,
    Picker,
    PickerAll,
}

fn worktree_branch_mode(existing: bool, delete: bool) -> WorktreeBranchMode {
    if existing {
        WorktreeBranchMode::UseExisting
    } else if delete {
        WorktreeBranchMode::DeleteAndRecreate
    } else {
        WorktreeBranchMode::CreateNew
    }
}

fn find_workspace_root(start: &Path) -> Result<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let config_path = current.join(KAI_CONFIG_REL_PATH);
        if config_path.is_file() {
            return Ok(current);
        }
        if !current.pop() {
            break;
        }
    }

    bail!(
        "No workspace config found. Walked up from {} looking for `{}`.",
        start.display(),
        KAI_CONFIG_REL_PATH
    );
}

fn parse_worktree_root_from_kai_config(contents: &str, config_path: &Path) -> Result<PathBuf> {
    let parsed = toml::from_str::<toml::Table>(contents)
        .with_context(|| format!("Failed to parse TOML: {}", config_path.display()))?;

    let worktree_root = match parsed.get("worktree_root") {
        Some(value) => value
            .as_str()
            .ok_or_else(|| {
                anyhow!(
                    "`worktree_root` must be a string in {}",
                    config_path.display()
                )
            })?
            .trim()
            .to_owned(),
        None => DEFAULT_WORKTREE_ROOT.to_owned(),
    };

    if worktree_root.is_empty() {
        bail!(
            "`worktree_root` cannot be empty in {}",
            config_path.display()
        );
    }

    Ok(PathBuf::from(worktree_root))
}

fn load_worktree_workspace(cwd: &Path) -> Result<WorktreeWorkspace> {
    let workspace_root = find_workspace_root(cwd)?;
    let workspace_root = workspace_root.canonicalize().with_context(|| {
        format!(
            "Failed to resolve workspace root: {}",
            workspace_root.display()
        )
    })?;
    let config_path = workspace_root.join(KAI_CONFIG_REL_PATH);
    let config_contents = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read workspace config: {}", config_path.display()))?;

    let configured_worktree_root =
        parse_worktree_root_from_kai_config(&config_contents, &config_path)?;
    let worktrees_dir = if configured_worktree_root.is_absolute() {
        configured_worktree_root
    } else {
        workspace_root.join(configured_worktree_root)
    };

    let repo_root = git_repo_root(&workspace_root)?;
    if repo_root != workspace_root {
        bail!(
            "Workspace root from `{}` is not the git repository root: workspace={} repo_root={}",
            KAI_CONFIG_REL_PATH,
            workspace_root.display(),
            repo_root.display()
        );
    }

    Ok(WorktreeWorkspace {
        repo_root,
        worktrees_dir,
    })
}

fn load_worktree_invocation_context() -> Result<WorktreeInvocationContext> {
    let cwd = std::env::current_dir().context("Failed to read current working directory")?;
    load_worktree_invocation_context_from_cwd(&cwd)
}

fn load_worktree_invocation_context_from_cwd(cwd: &Path) -> Result<WorktreeInvocationContext> {
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("Failed to resolve current directory: {}", cwd.display()))?;
    let workspace = load_worktree_workspace(&cwd)?;
    let spawn_root = nearest_enclosing_dot_git_root(&cwd)?;
    let spawn_rel_to_repo_root = spawn_root
        .strip_prefix(&workspace.repo_root)
        .with_context(|| {
            format!(
                "Leaf git root is outside workspace repo root: leaf_repo_root={} repo_root={}",
                spawn_root.display(),
                workspace.repo_root.display()
            )
        })?
        .to_path_buf();
    let invoked_from_submodule = cwd_is_submodule_of_workspace_repo(&cwd, &workspace.repo_root)?;

    Ok(WorktreeInvocationContext {
        workspace,
        spawn_rel_to_repo_root,
        invoked_from_submodule,
    })
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<ExitCode> {
    let command = command_or_default(Cli::parse());
    match command {
        Commands::LlmGet(args) => {
            llm_get(args)?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Init(args) => {
            kai_init(args)?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Bump(args) => {
            kai_bump(args)?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Agent(args) => agent(args),
        Commands::Worktree(args) => match args.command {
            WorktreeCommands::Create(args) => {
                worktree_create(args)?;
                Ok(ExitCode::SUCCESS)
            }
            WorktreeCommands::Agent(args) => worktree_agent(args),
            WorktreeCommands::Open(args) => worktree_open(args),
            WorktreeCommands::Delete(args) => {
                worktree_delete(args)?;
                Ok(ExitCode::SUCCESS)
            }
        },
        Commands::Wc(args) => {
            worktree_create(args)?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Wa(args) => worktree_agent(args),
        Commands::Wo(args) => worktree_open(args),
        Commands::Wd(args) => {
            worktree_delete(args)?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn command_or_default(cli: Cli) -> Commands {
    cli.command.unwrap_or(Commands::Agent(AgentArgs {
        model: WorktreeAgentModel::Codex,
        resume: false,
        resume_all: false,
    }))
}

fn llm_get(args: LlmGetArgs) -> Result<()> {
    let relative_to_input = match args.relative_to {
        Some(p) => p,
        None => std::env::current_dir().context("Failed to read current working directory")?,
    };
    let relative_to_abs = absolute_path(&relative_to_input)?;
    if !relative_to_abs.exists() || !relative_to_abs.is_dir() {
        bail!(
            "--relative-to must be an existing directory: {}",
            relative_to_abs.display()
        );
    }
    let rel_base = relative_to_abs
        .canonicalize()
        .context("Failed to resolve --relative-to")?;

    let exts = normalize_exts(&args.exts);

    let mut files = gather_files(&args.paths, &exts)?;
    if !args.slim {
        files = prepend_special_files(files, &rel_base)?;
    }
    if files.is_empty() {
        bail!("No files matched the criteria.");
    }

    let listing = build_listing(&files, &rel_base)?;

    match args.out.as_deref() {
        None => {
            copy_to_clipboard(&listing)?;
            eprintln!(
                "Copied listing ({} chars) for {} file(s) to clipboard.",
                listing.chars().count(),
                files.len()
            );
        }
        Some("-") => {
            print!("{listing}");
        }
        Some(path) => {
            let out_path = PathBuf::from(path);
            if let Some(parent) = out_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create output directory: {}", parent.display())
                })?;
            }
            fs::write(&out_path, &listing)
                .with_context(|| format!("Failed to write output file: {}", out_path.display()))?;
            eprintln!(
                "Wrote listing ({} chars) for {} file(s) to {}.",
                listing.chars().count(),
                files.len(),
                out_path.display()
            );
        }
    }

    Ok(())
}

fn prompt_yes_no(prompt: &str) -> Result<bool> {
    prompt_yes_no_with_noninteractive_message(
        prompt,
        "Confirmation required, but stdin is not interactive. Re-run with --yes.",
    )
}

fn prompt_yes_no_with_noninteractive_message(
    prompt: &str,
    non_interactive_message: &str,
) -> Result<bool> {
    capulus::ui::prompt_confirm_with_message(prompt, false, non_interactive_message)
}

fn prompt_yes_no_default_yes_with_noninteractive_message(
    prompt: &str,
    non_interactive_message: &str,
) -> Result<bool> {
    capulus::ui::prompt_confirm_with_message(prompt, true, non_interactive_message)
}

#[cfg(test)]
fn parse_yes_no_answer(answer: &str, default_yes: bool) -> bool {
    let answer = answer.trim().to_ascii_lowercase();
    if answer.is_empty() {
        return default_yes;
    }
    matches!(answer.as_str(), "y" | "yes")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PointerCommitPromptChoice {
    CommitAndContinue,
    SkipCommitAndContinue,
    Abort,
}

fn parse_pointer_commit_prompt_choice(answer: &str) -> PointerCommitPromptChoice {
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => PointerCommitPromptChoice::CommitAndContinue,
        "!" => PointerCommitPromptChoice::SkipCommitAndContinue,
        _ => PointerCommitPromptChoice::Abort,
    }
}

fn prompt_submodule_pointer_commit_choice(
    non_interactive_message: &str,
) -> Result<PointerCommitPromptChoice> {
    capulus::ui::require_interactive(non_interactive_message)?;

    eprint!("Stage and commit these submodule pointer updates now, then continue? [y/N/!] ");
    io::stderr().flush().context("Failed to flush stderr")?;

    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("Failed to read confirmation from stdin")?;

    let choice = parse_pointer_commit_prompt_choice(&answer);
    if matches!(
        choice,
        PointerCommitPromptChoice::CommitAndContinue
            | PointerCommitPromptChoice::SkipCommitAndContinue
    ) {
        eprintln!();
    }
    Ok(choice)
}

fn warning_label() -> String {
    if io::stderr().is_terminal() {
        format!("{WARNING_ANSI_BOLD_YELLOW}Warning:{WARNING_ANSI_RESET}")
    } else {
        "Warning:".to_owned()
    }
}

fn eprintln_warning(message: &str) {
    eprintln!("{} {message}", warning_label());
}

fn git_superproject_root(repo_root: &Path) -> Result<Option<PathBuf>> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-superproject-working-tree"])
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git rev-parse --show-superproject-working-tree`")?;

    if !output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8(output.stdout)
        .context("git rev-parse --show-superproject-working-tree output was not UTF-8")?;
    let stdout = stdout.trim();
    if stdout.is_empty() {
        return Ok(None);
    }

    let root = PathBuf::from(stdout)
        .canonicalize()
        .with_context(|| format!("Failed to resolve superproject path: {stdout}"))?;
    Ok(Some(root))
}

fn try_git_toplevel(cwd: &Path) -> Result<Option<PathBuf>> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(cwd)
        .output()
        .context("Failed to run `git rev-parse --show-toplevel`")?;

    if !output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8(output.stdout)
        .context("git rev-parse --show-toplevel output was not UTF-8")?;
    let stdout = stdout.trim();
    if stdout.is_empty() {
        return Ok(None);
    }

    let top = PathBuf::from(stdout)
        .canonicalize()
        .with_context(|| format!("Failed to resolve git toplevel path: {stdout}"))?;
    Ok(Some(top))
}

fn nearest_parent_repo_root(repo_root: &Path) -> Result<Option<PathBuf>> {
    let mut parent = repo_root.parent();
    while let Some(dir) = parent {
        if let Some(top) = try_git_toplevel(dir)?
            && top != repo_root
            && repo_root.starts_with(&top)
        {
            return Ok(Some(top));
        }
        parent = dir.parent();
    }
    Ok(None)
}

fn repo_context_warnings(repo_root: &Path) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    let superproject = git_superproject_root(repo_root)?;

    if let Some(superproject) = &superproject {
        warnings.push(format!(
            "This repo appears to be a submodule of another repo: {}",
            superproject.display()
        ));
    }

    if let Some(parent_repo) = nearest_parent_repo_root(repo_root)? {
        if superproject.as_ref() == Some(&parent_repo) {
            return Ok(warnings);
        }
        warnings.push(format!(
            "This repo is nested inside another git repo: {} (possible subtree/vendor checkout).",
            parent_repo.display()
        ));
    }

    Ok(warnings)
}

fn normalized_worktree_root(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("--worktree-root cannot be empty.");
    }
    Ok(trimmed.to_owned())
}

fn build_kai_config_toml(worktree_root: &str) -> String {
    let value = toml::Value::String(worktree_root.to_owned());
    format!("worktree_root = {}\n", value)
}

fn kai_init(args: InitArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to read current working directory")?;
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("Failed to resolve current directory: {}", cwd.display()))?;
    let repo_root = git_repo_root(&cwd)?;
    let worktree_root = normalized_worktree_root(&args.worktree_root)?;

    if cwd != repo_root {
        bail!(
            "`kai init` must be run from the repository root because it writes `.kai/config.toml` in the current directory.\nCurrent directory: {}\nRepository root: {}",
            cwd.display(),
            repo_root.display()
        );
    }

    let warnings = repo_context_warnings(&repo_root)?;
    eprintln!("Repository root: {}", repo_root.display());
    if warnings.is_empty() {
        eprintln!("No parent-repo nesting detected.");
    } else {
        for warning in &warnings {
            eprintln_warning(warning);
        }
    }

    if !args.yes && !prompt_yes_no("Create `.kai/config.toml` here? [y/N] ")? {
        bail!("Aborted.");
    }

    let kai_dir = cwd.join(".kai");
    fs::create_dir_all(&kai_dir)
        .with_context(|| format!("Failed to create directory: {}", kai_dir.display()))?;
    let config_path = kai_dir.join("config.toml");
    if config_path.exists() {
        bail!(
            "Config already exists: {} (delete it first or update it manually).",
            config_path.display()
        );
    }

    let content = build_kai_config_toml(&worktree_root);
    fs::write(&config_path, content)
        .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

    eprintln!("Created {}", config_path.display());
    Ok(())
}

fn kai_bump(_args: BumpArgs) -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to read current working directory")?;
    kai_bump_from_cwd(&cwd)
}

fn kai_bump_from_cwd(cwd: &Path) -> Result<()> {
    let cwd = cwd
        .canonicalize()
        .with_context(|| format!("Failed to resolve current directory: {}", cwd.display()))?;
    let target_repo = bump_target_repo_from_cwd(&cwd)?;

    let staged = git_staged_changes(&target_repo)?;
    if !staged.is_empty() {
        bail!(
            "Refusing `kai bump` because target repo already has staged changes ({}): {}\nTarget repo: {}",
            staged.len(),
            preview_items(&staged, 8),
            target_repo.display()
        );
    }

    let changed_submodules = git_unstaged_submodule_pointer_paths(&target_repo)?;
    if changed_submodules.is_empty() {
        eprintln!(
            "No unstaged submodule pointer updates found in {}. Nothing to bump.",
            target_repo.display()
        );
        return Ok(());
    }

    git_stage_paths(&target_repo, &changed_submodules)?;
    let message = bump_commit_message(&changed_submodules);
    git_commit(&target_repo, &message)?;
    git_push(&target_repo)?;

    eprintln!(
        "Committed and pushed submodule bumps in {}: {}",
        target_repo.display(),
        changed_submodules.join(", ")
    );
    Ok(())
}

fn bump_target_repo_from_cwd(cwd: &Path) -> Result<PathBuf> {
    let repo_root = try_git_toplevel(cwd)?.ok_or_else(|| {
        anyhow!(
            "`kai bump` must be run from inside a git repository. Current path: {}",
            cwd.display()
        )
    })?;

    Ok(git_superproject_root(&repo_root)?.unwrap_or(repo_root))
}

fn bump_commit_message(submodule_paths: &[String]) -> String {
    format!("Bump {}", submodule_paths.join(", "))
}

fn worktree_create(args: WorktreeCreateArgs) -> Result<()> {
    let context = load_worktree_invocation_context()?;
    let workspace = &context.workspace;

    let delete = args.delete || args.delete_force;
    let force = args.force || args.delete_force;
    let branch_mode = worktree_branch_mode(args.existing, delete);
    if branch_mode == WorktreeBranchMode::UseExisting && args.name.is_none() {
        bail!("--existing requires an explicit BRANCH name.");
    }

    let name = resolve_worktree_name(args.name, &workspace.repo_root, &workspace.worktrees_dir)?;

    let worktree_path = workspace.worktrees_dir.join(&name);
    if !args.multiple && worktree_path.is_dir() && worktree_is_locked(&worktree_path)? {
        bail!(
            "Worktree is already open in kai: {}",
            worktree_path.display()
        );
    }

    create_worktree(
        &workspace.repo_root,
        &workspace.worktrees_dir,
        &name,
        branch_mode,
        force,
    )?;
    maybe_spawn_background_cargo_build(&worktree_path);
    eprintln!("Worktree ready: {}", worktree_path.display());
    Ok(())
}

fn resolve_agent_resume_mode(resume: bool, resume_all: bool) -> AgentResumeMode {
    if resume_all {
        AgentResumeMode::PickerAll
    } else if resume {
        AgentResumeMode::Picker
    } else {
        AgentResumeMode::Off
    }
}

fn build_agent_command(
    model: WorktreeAgentModel,
    resume_mode: AgentResumeMode,
) -> (&'static str, Command) {
    match model {
        WorktreeAgentModel::Codex => {
            let mut command = Command::new("codex");
            if matches!(
                resume_mode,
                AgentResumeMode::Picker | AgentResumeMode::PickerAll
            ) {
                command.arg("resume");
                if matches!(resume_mode, AgentResumeMode::PickerAll) {
                    command.arg("--all");
                }
            }
            command.arg("--dangerously-bypass-approvals-and-sandbox");
            ("codex", command)
        }
        WorktreeAgentModel::Claude => {
            let mut command = Command::new("claude");
            if matches!(
                resume_mode,
                AgentResumeMode::Picker | AgentResumeMode::PickerAll
            ) {
                command.arg("--resume");
            }
            command.arg("--dangerously-skip-permissions");
            ("claude", command)
        }
    }
}

fn agent(args: AgentArgs) -> Result<ExitCode> {
    let resume_mode = resolve_agent_resume_mode(args.resume, args.resume_all);
    if matches!(args.model, WorktreeAgentModel::Claude)
        && matches!(resume_mode, AgentResumeMode::PickerAll)
    {
        eprintln_warning(
            "`claude` does not support a separate `--all` resume scope; using `--resume`.",
        );
    }

    let (tool, mut command) = build_agent_command(args.model, resume_mode);
    let status = command
        .status()
        .with_context(|| format!("Failed to run `{tool}`"))?;
    Ok(exit_code_from_status(status))
}

fn worktree_agent(args: WorktreeAgentArgs) -> Result<ExitCode> {
    let delete = args.delete || args.delete_force;
    let force = args.force || args.delete_force;
    let branch_mode = worktree_branch_mode(args.existing, delete);
    if branch_mode == WorktreeBranchMode::UseExisting && args.name.is_none() {
        bail!("--existing requires an explicit BRANCH name.");
    }

    let context = load_worktree_invocation_context()?;
    let existing_worktree =
        named_worktree_path_if_exists(&context.workspace, args.name.as_deref())?;
    if let Some(worktree_path) = existing_worktree.as_deref() {
        maybe_confirm_open_existing_worktree(worktree_path, branch_mode)?;
    }
    if existing_worktree.is_none() {
        enforce_submodule_pointer_guard(&context)?;
    }

    let (worktree_path, created) =
        ensure_worktree(&context.workspace, args.name, branch_mode, force)?;
    if created {
        maybe_spawn_background_cargo_build(&worktree_path);
    }

    let _lock = acquire_worktree_lock(&worktree_path, args.multiple)?;

    let start_dir = resolve_worktree_start_dir(&worktree_path, &context.spawn_rel_to_repo_root);

    let (tool, mut command) = build_agent_command(args.model, AgentResumeMode::Off);
    command.current_dir(&start_dir);

    let status = command
        .status()
        .with_context(|| format!("Failed to run `{tool}` in {}", start_dir.display()))?;

    Ok(exit_code_from_status(status))
}

fn worktree_open(args: WorktreeOpenArgs) -> Result<ExitCode> {
    let delete = args.delete || args.delete_force;
    let force = args.force || args.delete_force;
    let branch_mode = worktree_branch_mode(args.existing, delete);
    if branch_mode == WorktreeBranchMode::UseExisting && args.name.is_none() {
        bail!("--existing requires an explicit BRANCH name.");
    }

    let context = load_worktree_invocation_context()?;
    let existing_worktree =
        named_worktree_path_if_exists(&context.workspace, args.name.as_deref())?;
    if let Some(worktree_path) = existing_worktree.as_deref() {
        maybe_confirm_open_existing_worktree(worktree_path, branch_mode)?;
    }
    if existing_worktree.is_none() {
        enforce_submodule_pointer_guard(&context)?;
    }

    worktree_run(
        &context,
        args.name,
        "bash",
        branch_mode,
        force,
        args.multiple,
    )
}

fn worktree_delete(args: WorktreeDeleteArgs) -> Result<()> {
    let context = load_worktree_invocation_context()?;
    worktree_delete_in_workspace(args, &context.workspace)
}

fn worktree_delete_in_workspace(
    args: WorktreeDeleteArgs,
    workspace: &WorktreeWorkspace,
) -> Result<()> {
    validate_worktree_name(&args.name, &workspace.repo_root)?;
    let worktree_path = workspace.worktrees_dir.join(&args.name);
    let metadata = fs::symlink_metadata(&worktree_path).with_context(|| {
        format!(
            "Worktree does not exist at configured worktree root: {}",
            worktree_path.display()
        )
    })?;
    if !metadata.is_dir() {
        bail!(
            "Worktree path exists but is not a directory: {}",
            worktree_path.display()
        );
    }

    if !args.force {
        let blockers = worktree_delete_blockers(&workspace.repo_root, &worktree_path)?;
        if !blockers.is_empty() {
            let mut message = format!("Refusing to delete worktree `{}`:\n", args.name);
            for blocker in blockers {
                message.push_str("  - ");
                message.push_str(&blocker);
                message.push('\n');
            }
            message.push_str("Use --force to bypass these safety checks.");
            bail!("{message}");
        }
    }

    let worktree_repo_root = git_repo_root(&worktree_path).with_context(|| {
        format!(
            "Failed to resolve git repository for worktree path: {}",
            worktree_path.display()
        )
    })?;
    if worktree_repo_root != workspace.repo_root {
        bail!(
            "Refusing to delete worktree `{}` because it belongs to repo {} but resolved workspace repo is {}. Re-run from the intended workspace.",
            args.name,
            worktree_repo_root.display(),
            workspace.repo_root.display()
        );
    }

    let branch_exists = git_local_branch_exists(&workspace.repo_root, &args.name)?;
    if !branch_exists && !args.force {
        bail!(
            "Refusing to delete worktree `{}` because local branch `{}` does not exist in resolved workspace repo: {}.\nThis usually means you're in a different workspace/submodule than expected. Re-run from the intended workspace, or use --force to delete only the worktree directory.",
            args.name,
            args.name,
            workspace.repo_root.display()
        );
    }

    git_worktree_remove(&workspace.repo_root, &worktree_path, args.force)?;
    git_worktree_prune(&workspace.repo_root)?;
    if branch_exists {
        // Safety checks have already run against `master` when `--force` is not set.
        delete_local_branch(&workspace.repo_root, &args.name, true)?;
    } else {
        eprintln_warning(&format!(
            "Deleted worktree `{}` but did not delete branch `{}` because it was not found in {}.",
            worktree_path.display(),
            args.name,
            workspace.repo_root.display()
        ));
    }

    eprintln!("Deleted worktree: {}", worktree_path.display());
    Ok(())
}

fn named_worktree_path_if_exists(
    workspace: &WorktreeWorkspace,
    name: Option<&str>,
) -> Result<Option<PathBuf>> {
    let Some(name) = name else {
        return Ok(None);
    };

    validate_worktree_name(name, &workspace.repo_root)?;
    let worktree_path = workspace.worktrees_dir.join(name);
    match fs::symlink_metadata(&worktree_path) {
        Ok(metadata) if metadata.is_dir() => Ok(Some(worktree_path)),
        Ok(_) => bail!(
            "Worktree path exists but is not a directory: {}",
            worktree_path.display()
        ),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| {
            format!(
                "Failed to inspect worktree path: {}",
                worktree_path.display()
            )
        }),
    }
}

fn maybe_confirm_open_existing_worktree(
    worktree_path: &Path,
    branch_mode: WorktreeBranchMode,
) -> Result<()> {
    if branch_mode == WorktreeBranchMode::UseExisting {
        return Ok(());
    }

    eprintln_warning(&format!(
        "worktree already exists: {}",
        worktree_path.display()
    ));
    if !prompt_yes_no_default_yes_with_noninteractive_message(
        "Open existing worktree anyway? [Y/n] ",
        "Opening an existing worktree requires interactive confirmation. Re-run interactively, or pass --existing to acknowledge reusing it.",
    )? {
        bail!("Aborted.");
    }

    Ok(())
}

fn worktree_run(
    context: &WorktreeInvocationContext,
    name: Option<String>,
    prog: &str,
    branch_mode: WorktreeBranchMode,
    force: bool,
    multiple: bool,
) -> Result<ExitCode> {
    let (worktree_path, created) = ensure_worktree(&context.workspace, name, branch_mode, force)?;
    if created {
        maybe_spawn_background_cargo_build(&worktree_path);
    }

    let _lock = acquire_worktree_lock(&worktree_path, multiple)?;
    let start_dir = resolve_worktree_start_dir(&worktree_path, &context.spawn_rel_to_repo_root);

    let status = Command::new(prog)
        .current_dir(&start_dir)
        .status()
        .with_context(|| {
            format!(
                "Failed to run program `{}` in {}",
                prog,
                start_dir.display()
            )
        })?;

    Ok(exit_code_from_status(status))
}

fn ensure_worktree(
    workspace: &WorktreeWorkspace,
    name: Option<String>,
    branch_mode: WorktreeBranchMode,
    force: bool,
) -> Result<(PathBuf, bool)> {
    let name = resolve_worktree_name(name, &workspace.repo_root, &workspace.worktrees_dir)?;
    let worktree_path = workspace.worktrees_dir.join(&name);

    let mut created = false;
    if fs::symlink_metadata(&worktree_path).is_err() {
        create_worktree(
            &workspace.repo_root,
            &workspace.worktrees_dir,
            &name,
            branch_mode,
            force,
        )?;
        created = true;
    } else if !worktree_path.is_dir() {
        bail!(
            "Worktree path exists but is not a directory: {}",
            worktree_path.display()
        );
    }

    Ok((worktree_path, created))
}

fn resolve_worktree_start_dir(worktree_path: &Path, spawn_rel_to_repo_root: &Path) -> PathBuf {
    if spawn_rel_to_repo_root.as_os_str().is_empty() {
        return worktree_path.to_path_buf();
    }

    let candidate = worktree_path.join(spawn_rel_to_repo_root);
    if candidate.is_dir() {
        return candidate;
    }

    eprintln_warning(&format!(
        "current directory does not exist in target worktree ({}). Falling back to worktree root: {}",
        candidate.display(),
        worktree_path.display()
    ));
    worktree_path.to_path_buf()
}

fn nearest_enclosing_dot_git_root(start: &Path) -> Result<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let dot_git = current.join(".git");
        match fs::symlink_metadata(&dot_git) {
            Ok(_) => return Ok(current),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Failed to check path: {}", dot_git.display()));
            }
        }

        if !current.pop() {
            break;
        }
    }

    bail!(
        "Could not find enclosing git root by walking up from {} looking for `.git`",
        start.display()
    );
}

fn enforce_submodule_pointer_guard(context: &WorktreeInvocationContext) -> Result<()> {
    if !context.invoked_from_submodule {
        return Ok(());
    }

    maybe_commit_submodule_pointer_drift(&context.workspace.repo_root)?;
    confirm_when_submodules_have_uncommitted_changes(&context.workspace.repo_root)?;
    Ok(())
}

fn maybe_commit_submodule_pointer_drift(repo_root: &Path) -> Result<()> {
    let drift = git_uncommitted_submodule_pointer_changes(repo_root)?;
    if drift.is_empty() {
        return Ok(());
    }

    eprintln_warning("submodule commit-pointer changes are not committed in the workspace repo:");
    for item in &drift {
        eprintln!("  - {item}");
    }
    eprintln!();

    let staged = git_staged_changes(repo_root)?;
    if !staged.is_empty() {
        bail!(
            "Refusing automatic submodule-pointer commit because the workspace repo already has staged changes ({}): {}\nCommit or unstage these changes in the workspace repo, then retry `kai wa`/`kai wo` from a submodule.",
            staged.len(),
            preview_items(&staged, 8)
        );
    }

    let pointer_paths = git_uncommitted_submodule_pointer_paths(repo_root)?;
    if pointer_paths.is_empty() {
        bail!(
            "Submodule pointer drift was detected, but changed submodule paths could not be resolved."
        );
    }

    eprintln!("`kai` can stage and commit only these submodule pointers:");
    for path in &pointer_paths {
        eprintln!("  - {path}");
    }
    eprintln!();

    let choice = prompt_submodule_pointer_commit_choice(
        "Submodule pointer updates must be confirmed interactively. Commit them manually in the workspace repo, then retry `kai wa`/`kai wo` from a submodule.",
    )?;
    if choice == PointerCommitPromptChoice::Abort {
        bail!("Aborted.");
    }
    if choice == PointerCommitPromptChoice::CommitAndContinue {
        git_stage_paths(repo_root, &pointer_paths)?;
        git_commit(repo_root, SUBMODULE_POINTER_COMMIT_MESSAGE)?;

        eprintln!(
            "Committed submodule pointer updates in workspace repo ({} path(s)).",
            pointer_paths.len()
        );
    } else {
        eprintln!("Skipped submodule-pointer auto-commit; continuing anyway.");
    }
    eprintln!();
    Ok(())
}

#[derive(Debug, Clone)]
struct SubmoduleUncommittedChanges {
    path: String,
    staged: usize,
    unstaged: usize,
    untracked: usize,
    files: Vec<String>,
}

impl SubmoduleUncommittedChanges {
    fn total_files_changed(&self) -> usize {
        self.files.len()
    }
}

fn confirm_when_submodules_have_uncommitted_changes(repo_root: &Path) -> Result<()> {
    let dirty = git_submodules_with_uncommitted_changes(repo_root)?;
    if dirty.is_empty() {
        return Ok(());
    }

    eprintln_warning("some submodules in the workspace have uncommitted changes:");
    for summary in &dirty {
        let (line1, line2) = submodule_detail_lines(summary);
        let interactive = io::stderr().is_terminal();
        let spinner = if interactive {
            Some(submodule_summary_spinner(&summary.path))
        } else {
            None
        };

        let semantic_summary = summarize_submodule_semantic_with_codex(repo_root, summary)
            .unwrap_or_else(|_| fallback_submodule_semantic_summary(summary));
        if let Some(spinner) = spinner {
            spinner.println(format!("  - {}: {}", summary.path, semantic_summary));
            spinner.println(format!("    {line1}"));
            spinner.println(format!("    {line2}"));
            spinner.finish_and_clear();
        } else {
            eprintln!("  - {}: {}", summary.path, semantic_summary);
            eprintln!("    {line1}");
            eprintln!("    {line2}");
        }
    }

    if !prompt_yes_no_with_noninteractive_message(
        "Are you sure you want to proceed anyway? [y/N] ",
        "Proceeding with submodules that have uncommitted changes must be confirmed interactively. Re-run interactively or clean/stash/commit submodule changes first.",
    )? {
        bail!("Aborted.");
    }

    Ok(())
}

fn submodule_summary_spinner(submodule_path: &str) -> ProgressBar {
    let spinner = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    let style = ProgressStyle::with_template("  - {prefix}: {spinner}{msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    spinner.set_style(style);
    spinner.set_prefix(submodule_path.to_owned());
    spinner.set_message(String::new());
    spinner.enable_steady_tick(Duration::from_millis(80));
    spinner
}

fn summarize_submodule_semantic_with_codex(
    repo_root: &Path,
    change: &SubmoduleUncommittedChanges,
) -> Result<String> {
    let codex_path = which::which("codex").context("`codex` not found in PATH")?;
    let submodule_root = repo_root.join(&change.path);
    let prompt = "You are summarizing uncommitted git changes in the current repository.
Rules:
- Work very quickly.
- Never request permission escalation. If any command is blocked, return a best-effort summary.
- You may run only read-only git commands.
- Never change files.
- Output exactly one plain-text line, no markdown, max 100 characters.
- Focus on the semantic intent of the changes (what they appear to do), not raw counts.
- If intent is unclear, say so briefly.
";

    let output_file = NamedTempFile::new().context("Failed to create temporary codex output")?;
    let status = Command::new(codex_path)
        .arg("exec")
        .args(["--sandbox", "read-only"])
        .arg("-c")
        .arg("approval_policy=\"never\"")
        .arg("-c")
        .arg("model_reasoning_effort=\"low\"")
        .args(["--skip-git-repo-check", "--cd"])
        .arg(&submodule_root)
        .arg("--output-last-message")
        .arg(output_file.path())
        .arg(prompt)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("Failed to run inline codex summary")?;

    if !status.success() {
        bail!("Inline codex summary exited with non-zero status");
    }

    let raw = fs::read_to_string(output_file.path())
        .context("Failed to read inline codex summary output")?;
    let summary = raw
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .context("Inline codex semantic summary output was empty")?;
    Ok(truncate_with_ellipsis(summary, 100))
}

fn fallback_submodule_semantic_summary(change: &SubmoduleUncommittedChanges) -> String {
    if change.files.is_empty() {
        return "local uncommitted changes".to_owned();
    }
    if change.files.len() == 1 {
        return format!("local edits to {}", change.files[0]);
    }
    format!("local edits across {} files", change.files.len())
}

fn submodule_detail_lines(change: &SubmoduleUncommittedChanges) -> (String, String) {
    let line1 = format!(
        "{} files changed (staged {}, unstaged {}, untracked {})",
        change.total_files_changed(),
        change.staged,
        change.unstaged,
        change.untracked
    );
    let line2 = format!(
        "files: {}",
        preview_paths_with_ellipsis(&change.files, SUBMODULE_CHANGED_FILES_PREVIEW_LIMIT)
    );
    (line1, line2)
}

fn preview_paths_with_ellipsis(paths: &[String], max_items: usize) -> String {
    if paths.is_empty() {
        return "(none)".to_owned();
    }

    let mut preview = paths
        .iter()
        .take(max_items)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if paths.len() > max_items {
        preview.push_str(", ...");
    }
    preview
}

fn truncate_with_ellipsis(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_owned();
    }

    let mut out = String::new();
    for ch in input.chars().take(max_chars.saturating_sub(3)) {
        out.push(ch);
    }
    out.push_str("...");
    out
}

struct WorktreeLockGuard {
    _file: fs::File,
}

fn worktree_lock_path(worktree_path: &Path) -> PathBuf {
    worktree_path.join(".kai").join(".lock")
}

fn worktree_startup_lock_path(worktree_path: &Path) -> PathBuf {
    worktree_path.join(".kai").join(".lock.startup")
}

fn confirm_continue_when_worktree_already_open(worktree_path: &Path) -> Result<bool> {
    if !io::stdin().is_terminal() {
        bail!(
            "Worktree is already open in kai: {} (refusing without interactive confirmation; re-run with -M/--multiple to continue).",
            worktree_path.display()
        );
    }

    eprintln_warning(&format!(
        "Worktree is already open in kai: {}",
        worktree_path.display()
    ));
    eprint!("Continue anyway (take a shared lock)? [y/N] ");
    io::stderr().flush().context("Failed to flush stderr")?;

    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("Failed to read confirmation from stdin")?;
    let answer = answer.trim().to_ascii_lowercase();
    let confirmed = matches!(answer.as_str(), "y" | "yes");
    if confirmed {
        eprintln!();
    }
    Ok(confirmed)
}

fn try_lock_shared_with_retry(file: &fs::File, retries: usize) -> io::Result<()> {
    for attempt in 0..=retries {
        match fs2::FileExt::try_lock_shared(file) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock && attempt < retries => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn acquire_worktree_lock(worktree_path: &Path, multiple: bool) -> Result<WorktreeLockGuard> {
    let kai_dir = worktree_path.join(".kai");
    fs::create_dir_all(&kai_dir)
        .with_context(|| format!("Failed to create directory: {}", kai_dir.display()))?;

    let startup_lock_path = worktree_startup_lock_path(worktree_path);
    let startup_file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&startup_lock_path)
        .with_context(|| {
            format!(
                "Failed to open startup lock file: {}",
                startup_lock_path.display()
            )
        })?;

    let lock_path = worktree_lock_path(worktree_path);
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;

    if multiple {
        fs2::FileExt::lock_shared(&startup_file).with_context(|| {
            format!(
                "Failed to acquire startup lock (shared): {}",
                startup_lock_path.display()
            )
        })?;

        match try_lock_shared_with_retry(&file, 40) {
            Ok(()) => {
                drop(startup_file);
                return Ok(WorktreeLockGuard { _file: file });
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                bail!(
                    "Worktree is already open in kai: {} (another session holds an exclusive lock; close it or wait for it to finish starting).",
                    worktree_path.display()
                );
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("Failed to lock: {}", lock_path.display()));
            }
        }
    }

    fs2::FileExt::lock_exclusive(&startup_file).with_context(|| {
        format!(
            "Failed to acquire startup lock (exclusive): {}",
            startup_lock_path.display()
        )
    })?;

    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            fs2::FileExt::unlock(&file)
                .with_context(|| format!("Failed to unlock: {}", lock_path.display()))?;

            match try_lock_shared_with_retry(&file, 40) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    bail!(
                        "Worktree is already open in kai: {} (another session holds an exclusive lock; close it or wait for it to finish starting).",
                        worktree_path.display()
                    );
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("Failed to lock: {}", lock_path.display()));
                }
            }

            drop(startup_file);
        }
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
            drop(startup_file);
            if !confirm_continue_when_worktree_already_open(worktree_path)? {
                bail!("Aborted.");
            }
            match try_lock_shared_with_retry(&file, 40) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    bail!(
                        "Worktree is already open in kai: {} (another session holds an exclusive lock; close it or wait for it to finish starting).",
                        worktree_path.display()
                    );
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("Failed to lock: {}", lock_path.display()));
                }
            }
        }
        Err(err) => {
            return Err(err).with_context(|| format!("Failed to lock: {}", lock_path.display()));
        }
    }

    Ok(WorktreeLockGuard { _file: file })
}

fn worktree_is_locked(worktree_path: &Path) -> Result<bool> {
    let lock_path = worktree_lock_path(worktree_path);
    let file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("Failed to open lock file: {}", lock_path.display()));
        }
    };

    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            fs2::FileExt::unlock(&file)
                .with_context(|| format!("Failed to unlock: {}", lock_path.display()))?;
            Ok(false)
        }
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(true),
        Err(err) => {
            Err(err).with_context(|| format!("Failed to probe lock: {}", lock_path.display()))
        }
    }
}

fn git_repo_root(cwd: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(cwd)
        .output()
        .context("Failed to run `git rev-parse --git-common-dir`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Not inside a git repo (git rev-parse failed): {}",
            stderr.trim()
        );
    }

    let raw = String::from_utf8(output.stdout).context("git rev-parse output was not UTF-8")?;
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("git rev-parse returned an empty git common dir path");
    }

    let git_common_dir = PathBuf::from(raw);
    let git_common_abs = if git_common_dir.is_absolute() {
        git_common_dir
    } else {
        cwd.join(git_common_dir)
    };

    let git_common_abs = git_common_abs.canonicalize().with_context(|| {
        format!(
            "Failed to resolve git common dir: {}",
            git_common_abs.display()
        )
    })?;

    let repo_root = git_common_abs
        .parent()
        .context("git common dir has no parent directory")?
        .to_path_buf();

    Ok(repo_root)
}

fn cwd_is_submodule_of_workspace_repo(cwd: &Path, workspace_repo_root: &Path) -> Result<bool> {
    let Some(mut repo) = try_git_toplevel(cwd)? else {
        return Ok(false);
    };
    if repo == workspace_repo_root {
        return Ok(false);
    }

    loop {
        let Some(superproject) = git_superproject_root(&repo)? else {
            return Ok(false);
        };
        if superproject == workspace_repo_root {
            return Ok(true);
        }
        if superproject == repo {
            return Ok(false);
        }
        repo = superproject;
    }
}

fn git_uncommitted_submodule_pointer_changes(repo_root: &Path) -> Result<Vec<String>> {
    let mut issues = Vec::new();
    issues.extend(git_submodule_pointer_changes_from_diff(
        repo_root,
        &[
            "diff",
            "--raw",
            "--no-abbrev",
            "--ignore-submodules=none",
            "--",
        ],
        "unstaged submodule commit change (working tree vs index)",
    )?);
    issues.extend(git_submodule_pointer_changes_from_diff(
        repo_root,
        &[
            "diff",
            "--cached",
            "--raw",
            "--no-abbrev",
            "--ignore-submodules=none",
            "--",
        ],
        "staged submodule commit change (index vs HEAD, not yet committed)",
    )?);

    issues.sort();
    issues.dedup();
    Ok(issues)
}

fn git_uncommitted_submodule_pointer_paths(repo_root: &Path) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    paths.extend(git_submodule_pointer_paths_from_diff(
        repo_root,
        &[
            "diff",
            "--raw",
            "--no-abbrev",
            "--ignore-submodules=none",
            "--",
        ],
    )?);
    paths.extend(git_submodule_pointer_paths_from_diff(
        repo_root,
        &[
            "diff",
            "--cached",
            "--raw",
            "--no-abbrev",
            "--ignore-submodules=none",
            "--",
        ],
    )?);

    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn git_unstaged_submodule_pointer_paths(repo_root: &Path) -> Result<Vec<String>> {
    let mut paths = git_submodule_pointer_paths_from_diff(
        repo_root,
        &[
            "diff",
            "--raw",
            "--no-abbrev",
            "--ignore-submodules=none",
            "--",
        ],
    )?;
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn git_submodule_pointer_changes_from_diff(
    repo_root: &Path,
    args: &[&str],
    reason: &str,
) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git {}`", args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.join(" "), stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("git diff output was not UTF-8")?;
    let mut changes = Vec::new();
    for line in stdout.lines() {
        if let Some(path) = parse_submodule_path_from_raw_diff_line(line) {
            changes.push(format!("{path} ({reason})"));
        }
    }
    Ok(changes)
}

fn git_submodule_pointer_paths_from_diff(repo_root: &Path, args: &[&str]) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git {}`", args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.join(" "), stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("git diff output was not UTF-8")?;
    let mut paths = Vec::new();
    for line in stdout.lines() {
        if let Some(path) = parse_submodule_path_from_raw_diff_line(line) {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn git_staged_changes(repo_root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["diff", "--cached", "--name-status", "--"])
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git diff --cached --name-status --`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "git diff --cached --name-status -- failed: {}",
            stderr.trim()
        );
    }

    let stdout = String::from_utf8(output.stdout).context("git diff output was not UTF-8")?;
    Ok(stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn git_stage_paths(repo_root: &Path, paths: &[String]) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }

    let mut command = Command::new("git");
    command.arg("add").arg("--");
    for path in paths {
        command.arg(path);
    }

    let output = command
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git add -- <paths>`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git add failed: {}", stderr.trim());
    }

    Ok(())
}

fn git_commit(repo_root: &Path, message: &str) -> Result<()> {
    let output = Command::new("git")
        .args(["commit", "-m", message])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git commit -m {message:?}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            bail!("git commit failed: {}", stdout.trim());
        }
        bail!("git commit failed: {stderr}");
    }

    Ok(())
}

fn git_push(repo_root: &Path) -> Result<()> {
    let output = Command::new("git")
        .arg("-c")
        .arg("push.recurseSubmodules=no")
        .arg("push")
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git push`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            bail!("git push failed: {}", stdout.trim());
        }
        bail!("git push failed: {stderr}");
    }

    Ok(())
}

fn git_submodule_paths(repo_root: &Path) -> Result<Vec<String>> {
    let probe = r#"printf "%s\n" "$sm_path""#;
    let output = Command::new("git")
        .args(["submodule", "foreach", "--recursive", "--quiet", probe])
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git submodule foreach --recursive`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git submodule foreach failed: {}", stderr.trim());
    }

    let stdout =
        String::from_utf8(output.stdout).context("git submodule foreach output was not UTF-8")?;
    let mut paths = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn git_submodule_uncommitted_changes(
    repo_root: &Path,
    submodule_path: &str,
) -> Result<Option<SubmoduleUncommittedChanges>> {
    let submodule_root = repo_root.join(submodule_path);
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .current_dir(&submodule_root)
        .output()
        .with_context(|| {
            format!(
                "Failed to run `git status --porcelain --untracked-files=all` in submodule {}",
                submodule_path
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "git status failed for submodule {}: {}",
            submodule_path,
            stderr.trim()
        );
    }

    let stdout =
        String::from_utf8(output.stdout).context("git status --porcelain output was not UTF-8")?;
    let mut staged = 0usize;
    let mut unstaged = 0usize;
    let mut untracked = 0usize;
    let mut files = BTreeSet::new();

    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }

        if let Some(path_part) = line.strip_prefix("?? ") {
            untracked += 1;
            let path = parse_status_path(path_part);
            if !path.is_empty() {
                files.insert(path);
            }
            continue;
        }

        let bytes = line.as_bytes();
        if bytes.len() < 3 {
            continue;
        }

        if bytes[0] != b' ' {
            staged += 1;
        }
        if bytes[1] != b' ' {
            unstaged += 1;
        }
        if bytes[0] != b' ' || bytes[1] != b' ' {
            let path = parse_status_path(line[3..].trim());
            if !path.is_empty() {
                files.insert(path);
            }
        }
    }

    if staged == 0 && unstaged == 0 && untracked == 0 {
        return Ok(None);
    }

    Ok(Some(SubmoduleUncommittedChanges {
        path: submodule_path.to_owned(),
        staged,
        unstaged,
        untracked,
        files: files.into_iter().collect(),
    }))
}

fn git_submodules_with_uncommitted_changes(
    repo_root: &Path,
) -> Result<Vec<SubmoduleUncommittedChanges>> {
    let paths = git_submodule_paths(repo_root)?;
    let mut dirty = Vec::new();

    for path in paths {
        if let Some(summary) = git_submodule_uncommitted_changes(repo_root, &path)? {
            dirty.push(summary);
        }
    }

    dirty.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(dirty)
}

fn parse_status_path(path_part: &str) -> String {
    let candidate = path_part.rsplit(" -> ").next().unwrap_or(path_part).trim();
    candidate.to_owned()
}

fn parse_submodule_path_from_raw_diff_line(line: &str) -> Option<String> {
    if !line.starts_with(':') {
        return None;
    }

    let tab_pos = line.find('\t')?;
    let (meta, path_part) = line.split_at(tab_pos);
    let path_part = path_part.trim_start_matches('\t');

    let mut fields = meta.split_whitespace();
    let old_mode = fields.next()?.strip_prefix(':')?;
    let new_mode = fields.next()?;
    let old_oid = fields.next()?;
    let new_oid = fields.next()?;
    let _status = fields.next()?;
    if old_mode != "160000" && new_mode != "160000" {
        return None;
    }
    if old_oid == new_oid {
        return None;
    }

    let path = path_part.rsplit('\t').next().unwrap_or(path_part).trim();
    if path.is_empty() {
        return None;
    }
    Some(path.to_owned())
}

fn resolve_worktree_name(
    name: Option<String>,
    repo_root: &Path,
    worktrees_dir: &Path,
) -> Result<String> {
    match name {
        Some(name) => {
            validate_worktree_name(&name, repo_root)?;
            Ok(name)
        }
        None => {
            let name = pick_random_session_name(repo_root, worktrees_dir)?;
            validate_worktree_name(&name, repo_root)?;
            eprintln!("Picked worktree name: {name}");
            Ok(name)
        }
    }
}

fn validate_worktree_name(name: &str, repo_root: &Path) -> Result<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        bail!("Worktree/branch name cannot be empty.");
    }
    if trimmed != name {
        bail!(
            "Invalid worktree/branch name: {name:?} (must not have leading/trailing whitespace)."
        );
    }
    if name.contains('\0') {
        bail!("Invalid worktree/branch name: {name:?} (contains NUL).");
    }
    if name == "." || name == ".." {
        bail!("Invalid worktree/branch name: {name:?} (must not be '.' or '..').");
    }
    if name.chars().any(|c| c == '/' || c == '\\') {
        bail!(
            "Invalid worktree/branch name: {name:?} (must be a single directory name; no '/' or '\\\\')."
        );
    }

    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => {}
        _ => {
            bail!("Invalid worktree/branch name: {name:?} (must be a single directory name).");
        }
    }

    validate_git_branch_name(repo_root, name)
}

fn validate_git_branch_name(repo_root: &Path, name: &str) -> Result<()> {
    let output = Command::new("git")
        .args(["check-ref-format", "--branch", name])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git check-ref-format --branch {name}`"))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        bail!("Invalid git branch name: {name:?}");
    }
    bail!("Invalid git branch name: {name:?} ({stderr})");
}

fn pick_random_session_name(repo_root: &Path, worktrees_dir: &Path) -> Result<String> {
    let mut rng = rand::rng();

    for _ in 0..COMMON_WORDS_MAX_ATTEMPTS {
        let word = COMMON_WORDS
            .choose(&mut rng)
            .context("Common word list is empty")?;
        let name = format!("s-{word}");
        let path = worktrees_dir.join(&name);

        if fs::symlink_metadata(&path).is_ok() {
            continue;
        }
        if git_local_branch_exists(repo_root, &name)? {
            continue;
        }

        return Ok(name);
    }

    bail!("Failed to pick an unused session name after {COMMON_WORDS_MAX_ATTEMPTS} attempts.");
}

fn git_local_branch_exists(repo_root: &Path, name: &str) -> Result<bool> {
    let ref_name = format!("refs/heads/{name}");
    let output = Command::new("git")
        .args(["show-ref", "--verify", "--quiet", &ref_name])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git show-ref --verify {ref_name}`"))?;

    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("git show-ref failed: {}", stderr.trim())
        }
    }
}

fn worktree_delete_blockers(repo_root: &Path, worktree_path: &Path) -> Result<Vec<String>> {
    let mut blockers = Vec::new();

    let unstaged_changes = git_worktree_unstaged_changes(worktree_path)?;
    if !unstaged_changes.is_empty() {
        blockers.push(format!(
            "unstaged changes detected ({}): {}",
            unstaged_changes.len(),
            preview_items(&unstaged_changes, 8)
        ));
    }

    if !git_local_branch_exists(repo_root, "master")? {
        blockers.push("branch `master` does not exist, cannot verify merge status".to_owned());
        return Ok(blockers);
    }

    let unmerged_commits = git_commits_not_merged_into_master(worktree_path)?;
    if !unmerged_commits.is_empty() {
        blockers.push(format!(
            "commits not merged into `master` ({}): {}",
            unmerged_commits.len(),
            preview_items(&unmerged_commits, 8)
        ));
    }

    Ok(blockers)
}

fn git_worktree_unstaged_changes(worktree_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(worktree_path)
        .output()
        .context("Failed to run `git status --porcelain` in worktree")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git status failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("git status output was not UTF-8")?;
    let mut unstaged = Vec::new();
    for line in stdout.lines() {
        if line.starts_with("?? ") {
            unstaged.push(line.to_owned());
            continue;
        }
        let bytes = line.as_bytes();
        if bytes.len() >= 2 && bytes[1] != b' ' {
            unstaged.push(line.to_owned());
        }
    }

    Ok(unstaged)
}

fn git_commits_not_merged_into_master(worktree_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["log", "--oneline", "--reverse", "master..HEAD"])
        .current_dir(worktree_path)
        .output()
        .context("Failed to run `git log --oneline --reverse master..HEAD` in worktree")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git log failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8(output.stdout).context("git log output was not UTF-8")?;
    Ok(stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn preview_items(items: &[String], max_items: usize) -> String {
    let shown: Vec<String> = items.iter().take(max_items).cloned().collect();
    let mut preview = shown.join(", ");
    let remaining = items.len().saturating_sub(max_items);
    if remaining > 0 {
        preview.push_str(&format!(", +{remaining} more"));
    }
    preview
}

fn git_worktree_remove(repo_root: &Path, worktree_path: &Path, force: bool) -> Result<()> {
    let mut command = Command::new("git");
    command.arg("worktree").arg("remove");
    if force {
        command.arg("--force");
    }
    command.arg(worktree_path);

    let output = command
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git worktree remove`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git worktree remove failed: {}", stderr.trim());
    }

    if worktree_path.exists() {
        bail!(
            "Worktree removal completed but directory still exists: {}",
            worktree_path.display()
        );
    }

    Ok(())
}

fn git_worktree_prune(repo_root: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(repo_root)
        .output()
        .context("Failed to run `git worktree prune`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git worktree prune failed: {}", stderr.trim());
    }

    Ok(())
}

fn git_top_level_submodule_paths(worktree_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .arg("-c")
        .arg("protocol.file.allow=always")
        .args(["submodule", "status"])
        .current_dir(worktree_path)
        .output()
        .context("Failed to run `git submodule status`")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git submodule status failed: {}", stderr.trim());
    }

    let raw =
        String::from_utf8(output.stdout).context("git submodule status output was not UTF-8")?;
    let mut paths = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut fields = line.split_whitespace();
        let _sha = fields.next();
        let path = fields.next().ok_or_else(|| {
            anyhow!("Unable to parse submodule path from `git submodule status` line: {line}")
        })?;
        paths.push(path.to_owned());
    }

    Ok(paths)
}

fn submodule_reference_repo_for_path(repo_root: &Path, submodule_path: &str) -> Option<PathBuf> {
    let candidate = repo_root.join(submodule_path);
    if !candidate.is_dir() {
        return None;
    }

    let candidate = candidate.canonicalize().ok()?;
    let top = try_git_toplevel(&candidate).ok()??;
    if top == candidate { Some(top) } else { None }
}

fn git_submodule_update_init_recursive_for_path(
    worktree_path: &Path,
    submodule_path: &str,
    reference_repo: Option<&Path>,
) -> Result<()> {
    let mut command = Command::new("git");
    command
        .arg("-c")
        .arg("protocol.file.allow=always")
        .arg("-c")
        .arg("submodule.alternateLocation=superproject")
        .arg("-c")
        .arg("submodule.alternateErrorStrategy=info")
        .args(["submodule", "update", "--init", "--recursive"]);
    if let Some(reference_repo) = reference_repo {
        command.arg("--reference").arg(reference_repo);
    }
    command.args(["--"]).arg(submodule_path);

    let command_display = match reference_repo {
        Some(reference_repo) => format!(
            "git submodule update --init --recursive --reference {} -- {submodule_path}",
            reference_repo.display()
        ),
        None => format!("git submodule update --init --recursive -- {submodule_path}"),
    };

    let output = command
        .current_dir(worktree_path)
        .output()
        .with_context(|| format!("Failed to run `{command_display}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{command_display} failed: {}", stderr.trim());
    }

    Ok(())
}

fn delete_local_branch(repo_root: &Path, name: &str, force: bool) -> Result<()> {
    let delete_flag = if force { "-D" } else { "-d" };
    let output = Command::new("git")
        .args(["branch", delete_flag, name])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("Failed to run `git branch {delete_flag} {name}`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git branch {delete_flag} {name} failed: {}", stderr.trim());
    }

    Ok(())
}

fn worktree_create_progress_bar(name: &str) -> Option<ProgressBar> {
    if !io::stderr().is_terminal() {
        return None;
    }

    let progress = ProgressBar::with_draw_target(
        Some(WORKTREE_CREATE_PROGRESS_BASE_STEPS),
        ProgressDrawTarget::stderr(),
    );
    let style = ProgressStyle::with_template(
        "{spinner:.cyan} [{elapsed_precise}] [{bar:30.cyan/blue}] {pos}/{len} {prefix} {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("=>-");
    progress.set_style(style);
    progress.set_prefix(name.to_owned());
    progress.enable_steady_tick(Duration::from_millis(100));
    Some(progress)
}

fn worktree_create_progress_update(progress: &Option<ProgressBar>, step: u64, message: &str) {
    if let Some(progress) = progress {
        progress.set_position(step);
        progress.set_message(message.to_owned());
        progress.tick();
    }
}

fn create_worktree(
    repo_root: &Path,
    worktrees_dir: &Path,
    name: &str,
    branch_mode: WorktreeBranchMode,
    force: bool,
) -> Result<()> {
    let progress = worktree_create_progress_bar(name);
    let result = (|| -> Result<()> {
        worktree_create_progress_update(&progress, 0, "Validating worktree paths");
        if fs::symlink_metadata(worktrees_dir).is_ok() && !worktrees_dir.is_dir() {
            bail!(
                "Expected configured worktree root to be a directory: {}",
                worktrees_dir.display()
            );
        }

        let worktree_path = worktrees_dir.join(name);
        if fs::symlink_metadata(&worktree_path).is_ok() {
            bail!("Worktree already exists: {}", worktree_path.display());
        }

        worktree_create_progress_update(&progress, 1, "Preparing worktree root and .gitignore");
        fs::create_dir_all(worktrees_dir).with_context(|| {
            format!(
                "Failed to create configured worktree root directory: {}",
                worktrees_dir.display()
            )
        })?;
        ensure_gitignore_has_worktrees(repo_root, worktrees_dir)?;

        worktree_create_progress_update(&progress, 2, "Checking branch state");
        let branch_exists = git_local_branch_exists(repo_root, name)?;
        match branch_mode {
            WorktreeBranchMode::CreateNew => {
                if branch_exists {
                    bail!(
                        "Branch already exists: {name}. Use --existing (-e) to use it, or --delete (-d) / -D to delete and recreate."
                    );
                }
            }
            WorktreeBranchMode::UseExisting => {
                if !branch_exists {
                    bail!("Branch does not exist: {name} (required by --existing).");
                }
            }
            WorktreeBranchMode::DeleteAndRecreate => {
                if branch_exists {
                    delete_local_branch(repo_root, name, force)?;
                }
            }
        }

        worktree_create_progress_update(&progress, 3, "Running `git worktree add`");
        let mut command = Command::new("git");
        command.arg("worktree").arg("add");
        match branch_mode {
            WorktreeBranchMode::UseExisting => {
                command.arg(worktree_path.as_os_str()).arg(name);
            }
            WorktreeBranchMode::CreateNew | WorktreeBranchMode::DeleteAndRecreate => {
                command.arg("-b").arg(name).arg(worktree_path.as_os_str());
            }
        }
        let output = command
            .current_dir(repo_root)
            .output()
            .context("Failed to run `git worktree add`")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("git worktree add failed: {}", stderr.trim());
        }

        worktree_create_progress_update(&progress, 4, "Discovering submodules");
        let submodule_paths = git_top_level_submodule_paths(&worktree_path)?;
        if let Some(progress) = &progress {
            progress.set_length(WORKTREE_CREATE_PROGRESS_BASE_STEPS + submodule_paths.len() as u64);
        }
        if submodule_paths.is_empty() {
            worktree_create_progress_update(&progress, 4, "No submodules to update");
        } else {
            for (index, submodule_path) in submodule_paths.iter().enumerate() {
                let step = WORKTREE_CREATE_PROGRESS_BASE_STEPS + index as u64;
                worktree_create_progress_update(
                    &progress,
                    step,
                    &format!(
                        "Updating submodule {}/{}: {}",
                        index + 1,
                        submodule_paths.len(),
                        submodule_path
                    ),
                );
                let reference_repo = submodule_reference_repo_for_path(repo_root, submodule_path);
                git_submodule_update_init_recursive_for_path(
                    &worktree_path,
                    submodule_path,
                    reference_repo.as_deref(),
                )?;
            }
        }
        worktree_create_progress_update(
            &progress,
            WORKTREE_CREATE_PROGRESS_BASE_STEPS + submodule_paths.len() as u64,
            "Done",
        );

        Ok(())
    })();

    if let Some(progress) = progress {
        progress.finish_and_clear();
    }

    result
}

fn maybe_spawn_background_cargo_build(worktree_path: &Path) {
    let manifest_path = worktree_path.join("Cargo.toml");
    if !manifest_path.is_file() {
        return;
    }

    let cargo_path = match which::which("cargo") {
        Ok(path) => path,
        Err(err) => {
            eprintln_warning(&format!(
                "Cargo.toml found in {}, but `cargo` was not found in PATH ({err}).",
                worktree_path.display()
            ));
            return;
        }
    };

    let manifest = fs::read(&manifest_path).unwrap_or_default();
    let manifest = String::from_utf8_lossy(&manifest);
    let is_workspace = manifest.lines().any(|line| {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() || line.starts_with('#') {
            return false;
        }
        line == "[workspace]" || (line.starts_with("[workspace.") && line.ends_with(']'))
    });

    let mut command = Command::new(cargo_path);
    command.arg("build");
    if is_workspace {
        command.arg("--workspace");
    }
    command
        .current_dir(worktree_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.start_new_session();

    if let Err(err) = command.spawn() {
        eprintln_warning(&format!(
            "failed to spawn background `cargo build{}` in {} ({err}).",
            if is_workspace { " --workspace" } else { "" },
            worktree_path.display()
        ));
    }
}

fn ensure_gitignore_has_worktrees(repo_root: &Path, worktrees_dir: &Path) -> Result<()> {
    let rel_worktrees_dir = match worktrees_dir.strip_prefix(repo_root) {
        Ok(rel) => rel,
        Err(_) => return Ok(()),
    };
    if rel_worktrees_dir.as_os_str().is_empty() {
        return Ok(());
    }

    let mut gitignore_entry = to_posix_path(rel_worktrees_dir);
    while gitignore_entry.ends_with('/') {
        gitignore_entry.pop();
    }
    if gitignore_entry.is_empty() {
        return Ok(());
    }
    let gitignore_entry_with_slash = format!("{gitignore_entry}/");
    let gitignore_entry_abs = format!("/{gitignore_entry}");
    let gitignore_entry_abs_with_slash = format!("/{gitignore_entry}/");

    let gitignore_path = repo_root.join(".gitignore");
    let existing = match fs::read_to_string(&gitignore_path) {
        Ok(s) => s,
        Err(err) if err.kind() == io::ErrorKind::NotFound => String::new(),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("Failed to read .gitignore: {}", gitignore_path.display())
            });
        }
    };

    if existing.lines().any(|line| {
        matches!(
            line.trim(),
            trimmed if trimmed == gitignore_entry
                || trimmed == gitignore_entry_with_slash
                || trimmed == gitignore_entry_abs
                || trimmed == gitignore_entry_abs_with_slash
        )
    }) {
        return Ok(());
    }

    let mut updated = existing;
    if !updated.is_empty() && !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push_str(&gitignore_entry_with_slash);
    updated.push('\n');

    fs::write(&gitignore_path, updated)
        .with_context(|| format!("Failed to write .gitignore: {}", gitignore_path.display()))?;

    Ok(())
}

fn exit_code_from_status(status: std::process::ExitStatus) -> ExitCode {
    match status.code() {
        Some(code) => ExitCode::from(u8::try_from(code).unwrap_or(1)),
        None => ExitCode::from(1),
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("Failed to read current working directory")?
            .join(path))
    }
}

fn normalize_exts(user_exts: &[String]) -> HashSet<String> {
    let mut exts: HashSet<String> = HashSet::new();
    for ext in [".py", ".cpp", ".rs"] {
        exts.insert(ext.to_owned());
    }
    for raw in user_exts {
        let lower = raw.to_lowercase();
        if lower.starts_with('.') {
            exts.insert(lower);
        } else {
            exts.insert(format!(".{lower}"));
        }
    }
    exts
}

fn gather_files(paths: &[PathBuf], exts: &HashSet<String>) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    for raw in paths {
        if !raw.exists() {
            bail!("Path does not exist: {}", raw.display());
        }

        if raw.is_dir() {
            let mut dir_files: Vec<PathBuf> = WalkDir::new(raw)
                .min_depth(1)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter_map(|entry| {
                    let metadata = entry.metadata().ok()?;
                    if !metadata.is_file() {
                        return None;
                    }
                    let ext = entry.path().extension()?.to_str()?;
                    let suffix = format!(".{ext}");
                    if exts.contains(&suffix) {
                        Some(entry.path().to_path_buf())
                    } else {
                        None
                    }
                })
                .collect();

            dir_files.sort_by_key(|p| p.to_string_lossy().into_owned());

            for fp in dir_files {
                if seen.insert(fp.clone()) {
                    files.push(fp);
                }
            }
        } else if seen.insert(raw.clone()) {
            files.push(raw.clone());
        }
    }

    Ok(files)
}

fn prepend_special_files(mut files: Vec<PathBuf>, rel_base: &Path) -> Result<Vec<PathBuf>> {
    let mut special: Vec<PathBuf> = Vec::new();
    for name in ["AGENTS.md", "DESIGN.md"] {
        let cand = rel_base.join(name);
        if cand.is_file() {
            special.push(
                cand.canonicalize().with_context(|| {
                    format!("Failed to resolve special file: {}", cand.display())
                })?,
            );
        }
    }

    if special.is_empty() {
        return Ok(files);
    }

    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut ordered: Vec<PathBuf> = Vec::new();

    for fp in special {
        if seen.insert(fp.clone()) {
            ordered.push(fp);
        }
    }

    for fp in &mut files {
        let resolved = fp.canonicalize().with_context(|| {
            format!("Failed to resolve input file for listing: {}", fp.display())
        })?;
        if seen.insert(resolved.clone()) {
            ordered.push(resolved);
        }
    }

    Ok(ordered)
}

fn build_listing(paths: &[PathBuf], rel_base: &Path) -> Result<String> {
    let mut out = String::new();
    out.push_str("***** BEGIN FILE LISTING *****\n\n");

    for fp in paths {
        let resolved = fp
            .canonicalize()
            .with_context(|| format!("Failed to resolve path for listing: {}", fp.display()))?;
        let rel = pathdiff::diff_paths(&resolved, rel_base).unwrap_or(resolved);
        let rel_posix = to_posix_path(&rel);

        out.push_str(&rel_posix);
        out.push_str(":\n\n");

        let content = fs::read_to_string(fp)
            .with_context(|| format!("Failed to read file as UTF-8: {}", fp.display()))?;
        out.push_str(&content);
        out.push_str("\n\n\n\n\n");
    }

    out.push_str("***** END FILE LISTING *****");
    Ok(out)
}

fn to_posix_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn copy_to_clipboard(text: &str) -> Result<()> {
    let mut tmp = NamedTempFile::new().context("Failed to create temporary file")?;
    tmp.write_all(text.as_bytes())
        .context("Failed to write listing to temporary file")?;
    tmp.flush()
        .context("Failed to flush listing to temporary file")?;

    let stdin = tmp
        .reopen()
        .context("Failed to reopen temporary file for reading")?;

    if which::which("wl-copy").is_ok() {
        return spawn_clipboard_command("wl-copy", &[], stdin);
    }
    if which::which("xclip").is_ok() {
        return spawn_clipboard_command("xclip", &["-selection", "clipboard", "-in"], stdin);
    }
    if which::which("xsel").is_ok() {
        return spawn_clipboard_command("xsel", &["--clipboard", "--input"], stdin);
    }

    bail!("No clipboard utility found. Install one of: wl-clipboard, xclip, xsel.");
}

fn spawn_clipboard_command(program: &str, args: &[&str], stdin: fs::File) -> Result<()> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command.start_new_session();

    let mut child = command
        .spawn()
        .with_context(|| format!("Failed to spawn {program}"))?;

    match child
        .try_wait()
        .context("Failed to poll clipboard process")?
    {
        None => Ok(()),
        Some(status) if status.success() => Ok(()),
        Some(_) => {
            let mut err = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut err);
            }
            bail!("{program} failed: {err}");
        }
    }
}

trait CommandExtStartNewSession {
    fn start_new_session(&mut self) -> &mut Self;
}

impl CommandExtStartNewSession for Command {
    fn start_new_session(&mut self) -> &mut Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            unsafe {
                self.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_git(path: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-c")
            .arg("commit.gpgsign=false")
            .args(args)
            .current_dir(path)
            .status()
            .expect("git command");
        assert!(
            status.success(),
            "git {:?} failed in {}",
            args,
            path.display()
        );
    }

    fn run_git_allow_file(path: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-c")
            .arg("commit.gpgsign=false")
            .arg("-c")
            .arg("protocol.file.allow=always")
            .args(args)
            .current_dir(path)
            .status()
            .expect("git command");
        assert!(
            status.success(),
            "git {:?} failed in {}",
            args,
            path.display()
        );
    }

    fn run_git_output(path: &Path, args: &[&str]) -> std::process::Output {
        Command::new("git")
            .arg("-c")
            .arg("commit.gpgsign=false")
            .args(args)
            .current_dir(path)
            .output()
            .expect("git command output")
    }

    fn init_git_repo(path: &Path) {
        run_git(path, &["init", "-q"]);
    }

    fn init_git_repo_with_master(path: &Path) {
        init_git_repo(path);
        run_git(path, &["config", "user.name", "Kai Test"]);
        run_git(path, &["config", "user.email", "kai@example.com"]);
        run_git(path, &["config", "commit.gpgsign", "false"]);
        fs::write(path.join("README.md"), "init\n").expect("write readme");
        run_git(path, &["add", "README.md"]);
        run_git(path, &["commit", "-q", "-m", "init"]);
        run_git(path, &["branch", "-M", "master"]);
    }

    fn create_repo_with_submodule() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().expect("tempdir");
        let super_repo = temp.path().join("super");
        let submodule_source = temp.path().join("submodule-source");
        fs::create_dir_all(&super_repo).expect("create super repo dir");
        fs::create_dir_all(&submodule_source).expect("create submodule source dir");

        init_git_repo_with_master(&super_repo);
        init_git_repo_with_master(&submodule_source);

        let source = submodule_source.to_string_lossy().into_owned();
        run_git_allow_file(&super_repo, &["submodule", "add", &source, "deps/child"]);
        run_git(&super_repo, &["commit", "-q", "-m", "add submodule"]);

        let submodule_checkout = super_repo.join("deps").join("child");
        (temp, super_repo, submodule_checkout)
    }

    fn create_repo_with_submodule_and_remote() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().expect("tempdir");
        let remote_name = "super-remote.git";
        run_git(temp.path(), &["init", "--bare", "-q", remote_name]);
        let remote_path = temp.path().join(remote_name);

        let super_repo = temp.path().join("super");
        let submodule_source = temp.path().join("submodule-source");
        fs::create_dir_all(&super_repo).expect("create super repo dir");
        fs::create_dir_all(&submodule_source).expect("create submodule source dir");

        init_git_repo_with_master(&super_repo);
        init_git_repo_with_master(&submodule_source);

        let remote_s = remote_path.to_string_lossy().into_owned();
        run_git(&super_repo, &["remote", "add", "origin", &remote_s]);
        run_git(&super_repo, &["push", "-u", "origin", "master"]);

        let source = submodule_source.to_string_lossy().into_owned();
        run_git_allow_file(&super_repo, &["submodule", "add", &source, "deps/child"]);
        run_git(&super_repo, &["commit", "-q", "-m", "add submodule"]);
        run_git(&super_repo, &["push"]);

        let submodule_checkout = super_repo.join("deps").join("child");
        (temp, super_repo, submodule_checkout)
    }

    fn create_repo_with_nested_submodules() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().expect("tempdir");
        let super_repo = temp.path().join("super");
        let child_source = temp.path().join("child-source");
        let grandchild_source = temp.path().join("grandchild-source");
        fs::create_dir_all(&super_repo).expect("create super repo dir");
        fs::create_dir_all(&child_source).expect("create child source dir");
        fs::create_dir_all(&grandchild_source).expect("create grandchild source dir");

        init_git_repo_with_master(&super_repo);
        init_git_repo_with_master(&child_source);
        init_git_repo_with_master(&grandchild_source);

        let grandchild_source_s = grandchild_source.to_string_lossy().into_owned();
        run_git_allow_file(
            &child_source,
            &["submodule", "add", &grandchild_source_s, "deps/grand"],
        );
        run_git(
            &child_source,
            &["commit", "-q", "-m", "add grandchild submodule"],
        );

        let child_source_s = child_source.to_string_lossy().into_owned();
        run_git_allow_file(
            &super_repo,
            &["submodule", "add", &child_source_s, "deps/child"],
        );
        run_git(&super_repo, &["commit", "-q", "-m", "add child submodule"]);

        run_git_allow_file(
            &super_repo,
            &["submodule", "update", "--init", "--recursive"],
        );

        (temp, super_repo)
    }

    fn advance_submodule_commit(submodule_checkout: &Path, label: &str) {
        run_git(submodule_checkout, &["config", "user.name", "Kai Test"]);
        run_git(
            submodule_checkout,
            &["config", "user.email", "kai@example.com"],
        );
        run_git(submodule_checkout, &["config", "commit.gpgsign", "false"]);

        let filename = format!("{label}.txt");
        fs::write(submodule_checkout.join(filename), format!("{label}\n")).expect("write file");
        run_git(submodule_checkout, &["add", "."]);
        run_git(submodule_checkout, &["commit", "-q", "-m", label]);
    }

    fn add_worktree(repo_root: &Path, name: &str) -> PathBuf {
        let worktree_path = repo_root.join(".worktrees").join(name);
        fs::create_dir_all(repo_root.join(".worktrees")).expect("create worktrees dir");
        let worktree_path_s = worktree_path.to_string_lossy().into_owned();
        run_git(
            repo_root,
            &["worktree", "add", "-b", name, &worktree_path_s],
        );
        worktree_path
    }

    #[test]
    fn create_worktree_initializes_submodules_recursively() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo) = create_repo_with_nested_submodules();
        let worktrees_dir = super_repo.join(".worktrees");
        create_worktree(
            &super_repo,
            &worktrees_dir,
            "feature",
            WorktreeBranchMode::CreateNew,
            false,
        )
        .expect("create worktree");

        let worktree_path = worktrees_dir.join("feature");
        assert!(
            worktree_path
                .join("deps")
                .join("child")
                .join(".git")
                .exists()
        );
        assert!(
            worktree_path
                .join("deps")
                .join("child")
                .join("deps")
                .join("grand")
                .join(".git")
                .exists()
        );
    }

    #[test]
    fn submodule_reference_repo_for_path_uses_existing_checkout() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        let reference = submodule_reference_repo_for_path(&super_repo, "deps/child");
        assert_eq!(
            reference,
            Some(
                submodule_checkout
                    .canonicalize()
                    .expect("canonicalize submodule checkout"),
            )
        );
    }

    #[test]
    fn submodule_reference_repo_for_path_returns_none_when_missing() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        fs::remove_dir_all(&submodule_checkout).expect("remove submodule checkout");

        let reference = submodule_reference_repo_for_path(&super_repo, "deps/child");
        assert!(reference.is_none());
    }

    #[test]
    fn normalized_worktree_root_rejects_empty() {
        let err = normalized_worktree_root("  ").expect_err("should reject empty");
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn normalized_worktree_root_trims_whitespace() {
        let root = normalized_worktree_root("  sandbox/worktrees  ").expect("trimmed");
        assert_eq!(root, "sandbox/worktrees");
    }

    #[test]
    fn build_kai_config_toml_roundtrips_through_parser() {
        let content = build_kai_config_toml("sandbox/worktrees");
        let parsed =
            parse_worktree_root_from_kai_config(&content, Path::new("/tmp/.kai/config.toml"))
                .expect("roundtrip parse");
        assert_eq!(parsed, PathBuf::from("sandbox/worktrees"));
    }

    #[test]
    fn parse_worktree_root_defaults_when_unset() {
        let parsed = parse_worktree_root_from_kai_config("", Path::new("/tmp/.kai/config.toml"))
            .expect("default");
        assert_eq!(parsed, PathBuf::from(DEFAULT_WORKTREE_ROOT));
    }

    #[test]
    fn parse_worktree_root_reads_string_value() {
        let parsed = parse_worktree_root_from_kai_config(
            "worktree_root = \"sandbox/worktrees\"",
            Path::new("/tmp/.kai/config.toml"),
        )
        .expect("parse");
        assert_eq!(parsed, PathBuf::from("sandbox/worktrees"));
    }

    #[test]
    fn parse_worktree_root_rejects_non_string() {
        let err = parse_worktree_root_from_kai_config(
            "worktree_root = 42",
            Path::new("/tmp/.kai/config.toml"),
        )
        .expect_err("should reject non-string");
        assert!(err.to_string().contains("must be a string"));
    }

    #[test]
    fn find_workspace_root_walks_up_directories() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        fs::create_dir_all(root.join(".kai")).expect("mkdir .kai");
        fs::write(root.join(".kai").join("config.toml"), "").expect("write config");
        let nested = root.join("a").join("b").join("c");
        fs::create_dir_all(&nested).expect("mkdir nested");

        let found = find_workspace_root(&nested).expect("find workspace root");
        assert_eq!(found, root);
    }

    #[test]
    fn load_worktree_workspace_uses_configured_root() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_git_repo(root);
        fs::create_dir_all(root.join(".kai")).expect("mkdir .kai");
        fs::write(
            root.join(".kai").join("config.toml"),
            "worktree_root = \"sandbox/worktrees\"\n",
        )
        .expect("write config");
        let nested = root.join("pkg").join("subpkg");
        fs::create_dir_all(&nested).expect("mkdir nested");

        let workspace = load_worktree_workspace(&nested).expect("load workspace");
        let root = root.canonicalize().expect("canonical root");
        assert_eq!(workspace.repo_root, root);
        assert_eq!(workspace.worktrees_dir, root.join("sandbox/worktrees"));
    }

    #[test]
    fn load_worktree_workspace_requires_workspace_to_be_repo_root() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        init_git_repo(root);
        let nested_workspace = root.join("nested-workspace");
        fs::create_dir_all(nested_workspace.join(".kai")).expect("mkdir nested .kai");
        fs::write(nested_workspace.join(".kai").join("config.toml"), "").expect("write config");

        let err =
            load_worktree_workspace(&nested_workspace).expect_err("nested workspace should fail");
        assert!(err.to_string().contains("not the git repository root"));
    }

    #[test]
    fn nearest_parent_repo_root_detects_nested_repo() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let parent = temp.path();
        init_git_repo(parent);
        let nested = parent.join("nested");
        fs::create_dir_all(&nested).expect("mkdir nested");
        init_git_repo(&nested);
        let nested = nested.canonicalize().expect("canonical nested");

        let detected = nearest_parent_repo_root(&nested).expect("detect parent");
        assert_eq!(
            detected,
            Some(parent.canonicalize().expect("canonical parent"))
        );
    }

    #[test]
    fn repo_context_warnings_reports_nested_repo() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let parent = temp.path();
        init_git_repo(parent);
        let nested = parent.join("nested");
        fs::create_dir_all(&nested).expect("mkdir nested");
        init_git_repo(&nested);
        let nested = nested.canonicalize().expect("canonical nested");

        let warnings = repo_context_warnings(&nested).expect("warnings");
        assert!(!warnings.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("nested inside another git repo"))
        );
    }

    #[test]
    fn worktree_name_accepts_simple_names() {
        if which::which("git").is_err() {
            return;
        }

        let repo_root = tempfile::tempdir().expect("tempdir");
        validate_worktree_name("s-apple", repo_root.path()).expect("valid name");
        validate_worktree_name("feature_123", repo_root.path()).expect("valid name");
        validate_worktree_name("bugfix.fix-1", repo_root.path()).expect("valid name");
    }

    #[test]
    fn worktree_name_rejects_path_like_inputs() {
        if which::which("git").is_err() {
            return;
        }

        let repo_root = tempfile::tempdir().expect("tempdir");
        assert!(validate_worktree_name("feature/foo", repo_root.path()).is_err());
        assert!(validate_worktree_name("feature\\foo", repo_root.path()).is_err());
        assert!(validate_worktree_name("/tmp", repo_root.path()).is_err());
        assert!(validate_worktree_name(".", repo_root.path()).is_err());
        assert!(validate_worktree_name("..", repo_root.path()).is_err());
    }

    #[test]
    fn worktree_name_rejects_invalid_git_branch_names() {
        if which::which("git").is_err() {
            return;
        }

        let repo_root = tempfile::tempdir().expect("tempdir");
        assert!(validate_worktree_name("foo..bar", repo_root.path()).is_err());
        assert!(validate_worktree_name("foo~bar", repo_root.path()).is_err());
    }

    #[test]
    fn build_agent_command_codex_uses_expected_flags() {
        let (tool, command) = build_agent_command(WorktreeAgentModel::Codex, AgentResumeMode::Off);
        assert_eq!(tool, "codex");
        assert_eq!(command.get_program(), std::ffi::OsStr::new("codex"));
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["--dangerously-bypass-approvals-and-sandbox"]);
    }

    #[test]
    fn build_agent_command_claude_uses_expected_flags() {
        let (tool, command) = build_agent_command(WorktreeAgentModel::Claude, AgentResumeMode::Off);
        assert_eq!(tool, "claude");
        assert_eq!(command.get_program(), std::ffi::OsStr::new("claude"));
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["--dangerously-skip-permissions"]);
    }

    #[test]
    fn build_agent_command_codex_resume_uses_picker() {
        let (tool, command) =
            build_agent_command(WorktreeAgentModel::Codex, AgentResumeMode::Picker);
        assert_eq!(tool, "codex");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec!["resume", "--dangerously-bypass-approvals-and-sandbox"]
        );
    }

    #[test]
    fn build_agent_command_codex_resume_all_uses_global_picker() {
        let (tool, command) =
            build_agent_command(WorktreeAgentModel::Codex, AgentResumeMode::PickerAll);
        assert_eq!(tool, "codex");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec![
                "resume",
                "--all",
                "--dangerously-bypass-approvals-and-sandbox"
            ]
        );
    }

    #[test]
    fn build_agent_command_claude_resume_uses_picker() {
        let (tool, command) =
            build_agent_command(WorktreeAgentModel::Claude, AgentResumeMode::Picker);
        assert_eq!(tool, "claude");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["--resume", "--dangerously-skip-permissions"]);
    }

    #[test]
    fn build_agent_command_claude_resume_all_falls_back_to_resume() {
        let (tool, command) =
            build_agent_command(WorktreeAgentModel::Claude, AgentResumeMode::PickerAll);
        assert_eq!(tool, "claude");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["--resume", "--dangerously-skip-permissions"]);
    }

    #[test]
    fn resolve_worktree_start_dir_uses_relative_path_when_present() {
        let temp = tempfile::tempdir().expect("tempdir");
        let worktree = temp.path().join("worktree");
        fs::create_dir_all(worktree.join("deps").join("child").join("src")).expect("mkdir");

        let resolved =
            resolve_worktree_start_dir(&worktree, Path::new("deps/child/src")).canonicalize();
        assert_eq!(
            resolved.expect("resolve start dir"),
            worktree
                .join("deps")
                .join("child")
                .join("src")
                .canonicalize()
                .expect("canonical child dir")
        );
    }

    #[test]
    fn resolve_worktree_start_dir_falls_back_when_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let worktree = temp.path().join("worktree");
        fs::create_dir_all(&worktree).expect("mkdir worktree");

        let resolved =
            resolve_worktree_start_dir(&worktree, Path::new("deps/child/src")).canonicalize();
        assert_eq!(
            resolved.expect("resolve fallback"),
            worktree.canonicalize().expect("canonical worktree")
        );
    }

    #[test]
    fn parse_submodule_path_from_raw_diff_line_extracts_changed_gitlink() {
        let line = ":160000 160000 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 M\tdeps/child";
        let parsed = parse_submodule_path_from_raw_diff_line(line).expect("parse path");
        assert_eq!(parsed, "deps/child");
    }

    #[test]
    fn parse_submodule_path_from_raw_diff_line_ignores_unchanged_oid() {
        let line = ":160000 160000 1111111111111111111111111111111111111111 1111111111111111111111111111111111111111 M\tdeps/child";
        let parsed = parse_submodule_path_from_raw_diff_line(line);
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_submodule_path_from_raw_diff_line_ignores_non_submodules() {
        let line = ":100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 M\tREADME.md";
        let parsed = parse_submodule_path_from_raw_diff_line(line);
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_pointer_commit_prompt_choice_supports_skip_option() {
        assert_eq!(
            parse_pointer_commit_prompt_choice("!"),
            PointerCommitPromptChoice::SkipCommitAndContinue
        );
    }

    #[test]
    fn parse_pointer_commit_prompt_choice_defaults_to_abort() {
        assert_eq!(
            parse_pointer_commit_prompt_choice("nope"),
            PointerCommitPromptChoice::Abort
        );
    }

    #[test]
    fn parse_yes_no_answer_honors_default_yes_on_empty_input() {
        assert!(parse_yes_no_answer("", true));
    }

    #[test]
    fn parse_yes_no_answer_honors_default_no_on_empty_input() {
        assert!(!parse_yes_no_answer("", false));
    }

    #[test]
    fn parse_yes_no_answer_accepts_yes_variants() {
        assert!(parse_yes_no_answer("y", false));
        assert!(parse_yes_no_answer("YES", false));
    }

    #[test]
    fn nearest_enclosing_dot_git_root_prefers_leaf_repo_root() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, _super_repo, submodule_checkout) = create_repo_with_submodule();
        let nested = submodule_checkout.join("src").join("nested");
        fs::create_dir_all(&nested).expect("create nested dir");

        let detected = nearest_enclosing_dot_git_root(&nested).expect("detect leaf git root");
        assert_eq!(
            detected.canonicalize().expect("canonical detected"),
            submodule_checkout
                .canonicalize()
                .expect("canonical submodule")
        );
    }

    #[test]
    fn cwd_is_submodule_of_workspace_repo_detects_true_for_submodule() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        let workspace = super_repo.canonicalize().expect("canonical super repo");
        let nested = submodule_checkout
            .join("nested")
            .canonicalize()
            .unwrap_or_else(|_| submodule_checkout.join("nested"));
        fs::create_dir_all(&nested).expect("create nested dir");

        let is_submodule =
            cwd_is_submodule_of_workspace_repo(&nested, &workspace).expect("detect submodule");
        assert!(is_submodule);
    }

    #[test]
    fn cwd_is_submodule_of_workspace_repo_is_false_for_workspace_root() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, _) = create_repo_with_submodule();
        let workspace = super_repo.canonicalize().expect("canonical super repo");

        let is_submodule =
            cwd_is_submodule_of_workspace_repo(&workspace, &workspace).expect("detect workspace");
        assert!(!is_submodule);
    }

    #[test]
    fn git_uncommitted_submodule_pointer_changes_detects_unstaged_change() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        advance_submodule_commit(&submodule_checkout, "advance-unstaged");

        let changes = git_uncommitted_submodule_pointer_changes(&super_repo)
            .expect("detect submodule pointer changes");
        assert!(
            changes
                .iter()
                .any(|line| line.contains("deps/child") && line.contains("unstaged"))
        );
    }

    #[test]
    fn git_uncommitted_submodule_pointer_changes_detects_staged_change() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        advance_submodule_commit(&submodule_checkout, "advance-staged");
        run_git(&super_repo, &["add", "deps/child"]);

        let changes = git_uncommitted_submodule_pointer_changes(&super_repo)
            .expect("detect submodule pointer changes");
        assert!(
            changes
                .iter()
                .any(|line| line.contains("deps/child") && line.contains("staged"))
        );
    }

    #[test]
    fn git_uncommitted_submodule_pointer_paths_detects_changed_path() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        advance_submodule_commit(&submodule_checkout, "advance-paths");

        let paths =
            git_uncommitted_submodule_pointer_paths(&super_repo).expect("detect changed paths");
        assert_eq!(paths, vec!["deps/child".to_owned()]);
    }

    #[test]
    fn git_staged_changes_detects_staged_file() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        fs::write(repo_root.join("staged.txt"), "staged\n").expect("write staged file");
        run_git(repo_root, &["add", "staged.txt"]);

        let staged = git_staged_changes(repo_root).expect("detect staged changes");
        assert!(staged.iter().any(|line| line.ends_with("staged.txt")));
    }

    #[test]
    fn git_submodules_with_uncommitted_changes_detects_dirty_submodule() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        fs::write(submodule_checkout.join("local-dirty.txt"), "dirty\n").expect("write dirty file");

        let dirty =
            git_submodules_with_uncommitted_changes(&super_repo).expect("detect dirty submodules");
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].path, "deps/child");
        assert_eq!(dirty[0].staged, 0);
        assert_eq!(dirty[0].unstaged, 0);
        assert_eq!(dirty[0].untracked, 1);
        assert_eq!(dirty[0].files, vec!["local-dirty.txt".to_owned()]);
    }

    #[test]
    fn git_stage_paths_and_commit_clears_pointer_drift() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        advance_submodule_commit(&submodule_checkout, "advance-commit");

        let paths =
            git_uncommitted_submodule_pointer_paths(&super_repo).expect("detect changed paths");
        assert_eq!(paths, vec!["deps/child".to_owned()]);

        git_stage_paths(&super_repo, &paths).expect("stage submodule pointers");
        git_commit(&super_repo, SUBMODULE_POINTER_COMMIT_MESSAGE)
            .expect("commit submodule pointers");

        let remaining = git_uncommitted_submodule_pointer_changes(&super_repo)
            .expect("check remaining pointer changes");
        assert!(remaining.is_empty());
    }

    #[test]
    fn bump_target_repo_from_cwd_prefers_superproject_when_in_submodule() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        let nested = submodule_checkout.join("nested");
        fs::create_dir_all(&nested).expect("create nested dir");

        let target = bump_target_repo_from_cwd(&nested).expect("resolve bump target");
        assert_eq!(
            target.canonicalize().expect("canonical target"),
            super_repo.canonicalize().expect("canonical super repo")
        );
    }

    #[test]
    fn kai_bump_from_cwd_aborts_when_target_has_staged_changes() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule();
        fs::write(super_repo.join("staged.txt"), "staged\n").expect("write staged file");
        run_git(&super_repo, &["add", "staged.txt"]);

        let err = kai_bump_from_cwd(&submodule_checkout).expect_err("bump should fail");
        let message = err.to_string();
        assert!(message.contains("already has staged changes"));
        assert!(message.contains("staged.txt"));
    }

    #[test]
    fn kai_bump_from_cwd_stages_commits_and_pushes_submodule_pointers() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, super_repo, submodule_checkout) = create_repo_with_submodule_and_remote();
        advance_submodule_commit(&submodule_checkout, "advance-for-bump");

        kai_bump_from_cwd(&submodule_checkout).expect("run bump");

        let remaining = git_uncommitted_submodule_pointer_changes(&super_repo)
            .expect("check remaining pointer changes");
        assert!(remaining.is_empty());

        let commit_subject = run_git_output(&super_repo, &["log", "-1", "--pretty=%s"]);
        assert!(commit_subject.status.success());
        let subject = String::from_utf8_lossy(&commit_subject.stdout);
        assert_eq!(subject.trim(), "Bump deps/child");

        let local_head = run_git_output(&super_repo, &["rev-parse", "HEAD"]);
        assert!(local_head.status.success());
        let local_head = String::from_utf8_lossy(&local_head.stdout)
            .trim()
            .to_owned();

        let remote_head =
            run_git_output(&super_repo, &["ls-remote", "origin", "refs/heads/master"]);
        assert!(remote_head.status.success());
        let remote_head = String::from_utf8_lossy(&remote_head.stdout);
        let remote_sha = remote_head.split_whitespace().next().expect("remote sha");

        assert_eq!(local_head, remote_sha);
    }

    #[test]
    fn top_level_agent_alias_parses() {
        let cli = Cli::try_parse_from(["kai", "a"]).expect("parse shorthand");
        assert!(matches!(
            command_or_default(cli),
            Commands::Agent(AgentArgs {
                model: WorktreeAgentModel::Codex,
                ..
            })
        ));
    }

    #[test]
    fn top_level_without_subcommand_defaults_to_agent() {
        let cli = Cli::try_parse_from(["kai"]).expect("parse bare kai command");
        assert!(matches!(
            command_or_default(cli),
            Commands::Agent(AgentArgs {
                model: WorktreeAgentModel::Codex,
                ..
            })
        ));
    }

    #[test]
    fn top_level_agent_resume_flag_parses() {
        let cli = Cli::try_parse_from(["kai", "agent", "--resume"]).expect("parse --resume");
        assert!(matches!(
            command_or_default(cli),
            Commands::Agent(AgentArgs {
                resume: true,
                resume_all: false,
                ..
            })
        ));
    }

    #[test]
    fn top_level_agent_resume_all_flag_parses() {
        let cli =
            Cli::try_parse_from(["kai", "agent", "--resume-all"]).expect("parse --resume-all");
        assert!(matches!(
            command_or_default(cli),
            Commands::Agent(AgentArgs {
                resume: false,
                resume_all: true,
                ..
            })
        ));
    }

    #[test]
    fn top_level_agent_resume_flags_conflict() {
        let parsed = Cli::try_parse_from(["kai", "agent", "--resume", "--resume-all"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn top_level_worktree_delete_alias_parses() {
        let cli = Cli::try_parse_from(["kai", "wd", "feature"]).expect("parse wd shorthand");
        match command_or_default(cli) {
            Commands::Wd(args) => {
                assert_eq!(args.name, "feature");
                assert!(!args.force);
            }
            other => panic!("expected wd command, got {other:?}"),
        }
    }

    #[test]
    fn top_level_bump_parses() {
        let cli = Cli::try_parse_from(["kai", "bump"]).expect("parse bump command");
        assert!(matches!(command_or_default(cli), Commands::Bump(_)));
    }

    #[test]
    fn git_worktree_unstaged_changes_detects_unstaged_files() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktree_path = add_worktree(repo_root, "feature");
        fs::write(worktree_path.join("README.md"), "changed\n").expect("modify readme");

        let unstaged =
            git_worktree_unstaged_changes(&worktree_path).expect("detect unstaged changes");
        assert!(!unstaged.is_empty());
        assert!(unstaged.iter().any(|line| line.contains("README.md")));
    }

    #[test]
    fn git_commits_not_merged_into_master_detects_unmerged_commit() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktree_path = add_worktree(repo_root, "feature");
        fs::write(worktree_path.join("feature.txt"), "new\n").expect("write feature file");
        run_git(&worktree_path, &["add", "feature.txt"]);
        run_git(&worktree_path, &["commit", "-q", "-m", "feature commit"]);

        let commits = git_commits_not_merged_into_master(&worktree_path)
            .expect("find unmerged commits against master");
        assert_eq!(commits.len(), 1);
        assert!(commits[0].contains("feature commit"));
    }

    #[test]
    fn worktree_delete_blockers_report_reasons() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktree_path = add_worktree(repo_root, "feature");
        fs::write(worktree_path.join("feature.txt"), "new\n").expect("write feature file");
        run_git(&worktree_path, &["add", "feature.txt"]);
        run_git(&worktree_path, &["commit", "-q", "-m", "feature commit"]);
        fs::write(worktree_path.join("README.md"), "dirty\n").expect("modify readme");

        let blockers =
            worktree_delete_blockers(repo_root, &worktree_path).expect("collect delete blockers");
        assert!(
            blockers
                .iter()
                .any(|line| line.contains("unstaged changes"))
        );
        assert!(
            blockers
                .iter()
                .any(|line| line.contains("not merged into `master`"))
        );
    }

    #[test]
    fn worktree_delete_blockers_are_empty_when_clean_and_merged() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktree_path = add_worktree(repo_root, "feature");
        fs::write(worktree_path.join("feature.txt"), "new\n").expect("write feature file");
        run_git(&worktree_path, &["add", "feature.txt"]);
        run_git(&worktree_path, &["commit", "-q", "-m", "feature commit"]);
        run_git(
            repo_root,
            &["merge", "--no-ff", "-m", "merge feature", "feature"],
        );

        let blockers =
            worktree_delete_blockers(repo_root, &worktree_path).expect("collect delete blockers");
        assert!(blockers.is_empty());
    }

    #[test]
    fn git_worktree_remove_deletes_directory() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktree_path = add_worktree(repo_root, "feature");

        git_worktree_remove(repo_root, &worktree_path, false).expect("remove worktree");
        git_worktree_prune(repo_root).expect("prune worktrees");
        assert!(!worktree_path.exists());

        let output = run_git_output(repo_root, &["worktree", "list", "--porcelain"]);
        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.contains("feature"));
    }

    #[test]
    fn worktree_delete_removes_local_branch() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        fs::create_dir_all(repo_root.join(".kai")).expect("mkdir .kai");
        fs::write(repo_root.join(".kai").join("config.toml"), "").expect("write config");
        let worktree_path = add_worktree(repo_root, "feature");
        assert!(
            git_local_branch_exists(repo_root, "feature").expect("detect feature branch before")
        );

        let context = load_worktree_invocation_context_from_cwd(repo_root)
            .expect("resolve workspace context from repo root");
        worktree_delete_in_workspace(
            WorktreeDeleteArgs {
                name: "feature".to_owned(),
                force: false,
            },
            &context.workspace,
        )
        .expect("delete worktree and branch");

        assert!(!worktree_path.exists());
        assert!(
            !git_local_branch_exists(repo_root, "feature").expect("detect feature branch after")
        );
    }

    #[test]
    fn worktree_delete_from_submodule_path_targets_workspace_repo() {
        if which::which("git").is_err() {
            return;
        }

        let (_temp, workspace_repo, submodule_checkout) = create_repo_with_submodule();
        fs::create_dir_all(workspace_repo.join(".kai")).expect("mkdir .kai");
        fs::write(workspace_repo.join(".kai").join("config.toml"), "").expect("write config");

        let worktree_path = add_worktree(&workspace_repo, "mcts");
        assert!(
            git_local_branch_exists(&workspace_repo, "mcts")
                .expect("workspace branch should exist before deletion")
        );

        run_git(&submodule_checkout, &["branch", "mcts"]);
        assert!(
            git_local_branch_exists(&submodule_checkout, "mcts")
                .expect("submodule branch should exist before deletion")
        );

        let nested_in_submodule = submodule_checkout.join("nested");
        fs::create_dir_all(&nested_in_submodule).expect("mkdir nested in submodule");

        let context = load_worktree_invocation_context_from_cwd(&nested_in_submodule)
            .expect("resolve workspace context from submodule path");
        worktree_delete_in_workspace(
            WorktreeDeleteArgs {
                name: "mcts".to_owned(),
                force: false,
            },
            &context.workspace,
        )
        .expect("delete worktree from submodule path");

        assert!(!worktree_path.exists());
        assert!(
            !git_local_branch_exists(&workspace_repo, "mcts")
                .expect("workspace branch should be deleted")
        );
        assert!(
            git_local_branch_exists(&submodule_checkout, "mcts")
                .expect("submodule branch should remain untouched")
        );
    }

    #[test]
    fn named_worktree_path_if_exists_is_none_without_explicit_name() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = WorktreeWorkspace {
            repo_root: temp.path().to_path_buf(),
            worktrees_dir: temp.path().join(".worktrees"),
        };
        assert!(
            named_worktree_path_if_exists(&workspace, None)
                .expect("no explicit name should skip existence check")
                .is_none()
        );
    }

    #[test]
    fn named_worktree_path_if_exists_detects_named_worktree_directory() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktrees_dir = repo_root.join(".worktrees");
        fs::create_dir_all(worktrees_dir.join("feature")).expect("mkdir worktree");
        let workspace = WorktreeWorkspace {
            repo_root: repo_root.to_path_buf(),
            worktrees_dir,
        };

        assert!(
            named_worktree_path_if_exists(&workspace, Some("feature"))
                .expect("existing worktree path should be detected")
                .is_some()
        );
        assert!(
            named_worktree_path_if_exists(&workspace, Some("missing"))
                .expect("missing worktree path should not be detected")
                .is_none()
        );
    }

    #[test]
    fn named_worktree_path_if_exists_rejects_non_directory_path() {
        if which::which("git").is_err() {
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let repo_root = temp.path();
        init_git_repo_with_master(repo_root);
        let worktrees_dir = repo_root.join(".worktrees");
        fs::create_dir_all(&worktrees_dir).expect("mkdir worktrees");
        fs::write(worktrees_dir.join("feature"), "not a directory").expect("write file");
        let workspace = WorktreeWorkspace {
            repo_root: repo_root.to_path_buf(),
            worktrees_dir,
        };

        let err = named_worktree_path_if_exists(&workspace, Some("feature"))
            .expect_err("non-directory path should be rejected");
        assert!(err.to_string().contains("not a directory"));
    }

    #[test]
    fn maybe_confirm_open_existing_worktree_skips_prompt_for_existing_mode() {
        let temp = tempfile::tempdir().expect("tempdir");
        let worktree_path = temp.path().join("feature");
        fs::create_dir_all(&worktree_path).expect("mkdir worktree");

        maybe_confirm_open_existing_worktree(&worktree_path, WorktreeBranchMode::UseExisting)
            .expect("explicit --existing should skip confirmation prompt");
    }
}
