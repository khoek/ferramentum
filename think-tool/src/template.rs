use anyhow::Result;

use crate::config::{
    AgentNameScheme, BackendName, ExposedContext, ProjectTemplate, ROLE_CONFIG_VERSION, RoleConfig,
    RoleMode, RoleParallelism, RoleStatus, TriggerConfig, TriggerLaunch,
};
use crate::ids::{ChannelSlug, RoleSlug, StepSlug};
use crate::state::{ProjectPaths, RolePaths};
use crate::{git, io, prompt};

pub fn apply(project: &ProjectPaths, template: ProjectTemplate) -> Result<()> {
    match template {
        ProjectTemplate::EpisodesMath => apply_episodes_math(project),
        ProjectTemplate::EpisodesCode => apply_episodes_code(project),
    }
}

pub fn default_role_md(template: Option<ProjectTemplate>) -> &'static str {
    match template {
        Some(ProjectTemplate::EpisodesMath) => EPISODES_MATH_ROLE_MD,
        Some(ProjectTemplate::EpisodesCode) => EPISODES_CODE_EPISODE_ROLE_MD,
        None => prompt::DEFAULT_ROLE_MD,
    }
}

pub fn default_step_slug(template: Option<ProjectTemplate>) -> Result<StepSlug> {
    match template {
        Some(ProjectTemplate::EpisodesMath | ProjectTemplate::EpisodesCode) => {
            StepSlug::parse("work")
        }
        None => StepSlug::parse("work"),
    }
}

pub fn default_step_md(template: Option<ProjectTemplate>, step: &StepSlug) -> &'static str {
    match (template, step.as_str()) {
        (Some(ProjectTemplate::EpisodesMath), "work") => EPISODES_MATH_STEP_MD,
        (Some(ProjectTemplate::EpisodesCode), "work") => EPISODES_CODE_EPISODE_STEP_MD,
        _ => prompt::DEFAULT_STEP_MD,
    }
}

pub fn default_agent_names(template: Option<ProjectTemplate>) -> AgentNameScheme {
    match template {
        Some(ProjectTemplate::EpisodesMath | ProjectTemplate::EpisodesCode) => {
            AgentNameScheme::Sequential
        }
        None => AgentNameScheme::AdjectiveNoun,
    }
}

pub fn default_parallel(template: Option<ProjectTemplate>) -> RoleParallelism {
    match template {
        Some(ProjectTemplate::EpisodesMath | ProjectTemplate::EpisodesCode) => {
            RoleParallelism::Infinite
        }
        None => RoleParallelism::Count(1),
    }
}

fn apply_episodes_math(project: &ProjectPaths) -> Result<()> {
    io::write_text(&project.project_md(), EPISODES_MATH_PROJECT_MD)?;
    for channel in ["alerts", "report", "report-single"] {
        git::init_channel(&project.channel_dir(&ChannelSlug::parse(channel)?))?;
    }

    let template_dir = project.root.join("templates").join("episodes-math");
    io::ensure_dir(&template_dir)?;
    io::ensure_dir(&template_dir.join("episodes"))?;
    io::ensure_dir(&template_dir.join("papers"))?;
    io::ensure_dir(&template_dir.join("scripts"))?;
    io::ensure_dir(
        &template_dir
            .join("experiments")
            .join("src")
            .join("commands"),
    )?;
    io::ensure_dir(&template_dir.join("experiments").join("src").join("core"))?;
    io::write_text_if_missing(&template_dir.join(".gitignore"), EPISODES_MATH_GITIGNORE)?;
    io::write_text_if_missing(&template_dir.join("report.tex"), EPISODES_MATH_REPORT_TEX)?;
    io::write_text_if_missing(
        &template_dir.join("preamble.tex"),
        EPISODES_MATH_PREAMBLE_TEX,
    )?;
    io::write_text_if_missing(
        &template_dir.join("episode-standalone.tex"),
        EPISODES_MATH_EPISODE_STANDALONE_TEX,
    )?;
    io::write_text_if_missing(
        &template_dir.join("episodes").join("README.md"),
        EPISODES_MATH_EPISODES_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("papers").join("README.md"),
        EPISODES_MATH_PAPERS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("scripts").join("README.md"),
        EPISODES_MATH_SCRIPTS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("README.md"),
        EPISODES_MATH_EXPERIMENTS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("Cargo.toml"),
        EPISODES_MATH_EXPERIMENTS_CARGO_TOML,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("src").join("main.rs"),
        EPISODES_MATH_EXPERIMENTS_MAIN_RS,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("src").join("lib.rs"),
        EPISODES_MATH_EXPERIMENTS_LIB_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("commands")
            .join("mod.rs"),
        EPISODES_MATH_EXPERIMENTS_COMMANDS_MOD_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("commands")
            .join("smoke.rs"),
        EPISODES_MATH_EXPERIMENTS_SMOKE_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("core")
            .join("mod.rs"),
        EPISODES_MATH_EXPERIMENTS_CORE_MOD_RS,
    )?;
    io::write_text_if_missing(&template_dir.join("Makefile"), EPISODES_MATH_MAKEFILE)?;
    create_math_episode_role(project)?;
    create_math_publisher_role(project)?;
    create_math_auditor_role(project)?;
    create_math_supervisor_role(project)?;
    Ok(())
}

fn apply_episodes_code(project: &ProjectPaths) -> Result<()> {
    io::write_text(&project.project_md(), EPISODES_CODE_PROJECT_MD)?;
    for channel in ["alerts", "branches", "merges"] {
        git::init_channel(&project.channel_dir(&ChannelSlug::parse(channel)?))?;
    }

    let template_dir = project.root.join("templates").join("episodes-code");
    io::write_text_if_missing(
        &template_dir.join("README.md"),
        EPISODES_CODE_TEMPLATE_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("branch-handoff.md"),
        EPISODES_CODE_BRANCH_HANDOFF_TEMPLATE,
    )?;
    io::write_text_if_missing(
        &template_dir.join("merge-handoff.md"),
        EPISODES_CODE_MERGE_HANDOFF_TEMPLATE,
    )?;
    create_code_episode_role(project)?;
    create_code_merger_role(project)?;
    create_code_supervisor_role(project)?;
    create_code_auditor_role(project)?;
    Ok(())
}

fn create_math_episode_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("episode")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_MATH_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(&role_paths.step_path(&step), EPISODES_MATH_STEP_MD)?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Repeatable,
                parallel: RoleParallelism::Infinite,
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("ep".to_owned()),
                auto_archive: false,
                expose: Vec::new(),
                steps: vec![step],
                triggers: Vec::new(),
            },
        )?;
    }
    Ok(())
}

fn create_code_episode_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("episode")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_CODE_EPISODE_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(&role_paths.step_path(&step), EPISODES_CODE_EPISODE_STEP_MD)?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Repeatable,
                parallel: RoleParallelism::Infinite,
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("ep".to_owned()),
                auto_archive: false,
                expose: Vec::new(),
                steps: vec![step],
                triggers: Vec::new(),
            },
        )?;
    }
    Ok(())
}

fn create_code_merger_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("merger")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_CODE_MERGER_ROLE_MD)?;
    let step = StepSlug::parse("merge")?;
    io::write_text_if_missing(&role_paths.step_path(&step), EPISODES_CODE_MERGER_STEP_MD)?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("merge".to_owned()),
                auto_archive: true,
                expose: vec![ExposedContext::LastAgentFinished],
                steps: vec![step],
                triggers: vec![TriggerConfig::RoleAgentFinished {
                    role: RoleSlug::parse("episode")?,
                    launch: TriggerLaunch::Queued {
                        queue: "merger".to_owned(),
                    },
                }],
            },
        )?;
    }
    Ok(())
}

fn create_code_supervisor_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("supervisor")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_CODE_SUPERVISOR_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(
        &role_paths.step_path(&step),
        EPISODES_CODE_SUPERVISOR_STEP_MD,
    )?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Paused,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("sup".to_owned()),
                auto_archive: true,
                expose: Vec::new(),
                steps: vec![step],
                triggers: vec![
                    TriggerConfig::RoleAgentFinished {
                        role: RoleSlug::parse("episode")?,
                        launch: TriggerLaunch::Queued {
                            queue: "supervisor".to_owned(),
                        },
                    },
                    TriggerConfig::RoleAgentFinished {
                        role: RoleSlug::parse("merger")?,
                        launch: TriggerLaunch::Queued {
                            queue: "supervisor".to_owned(),
                        },
                    },
                ],
            },
        )?;
    }
    Ok(())
}

fn create_code_auditor_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("auditor")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_CODE_AUDITOR_ROLE_MD)?;
    let step = StepSlug::parse("audit")?;
    io::write_text_if_missing(&role_paths.step_path(&step), EPISODES_CODE_AUDITOR_STEP_MD)?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("audit".to_owned()),
                auto_archive: true,
                expose: Vec::new(),
                steps: vec![step],
                triggers: vec![TriggerConfig::QueueIdle {
                    idle_queue: "auditor".to_owned(),
                    idle_seconds: 1800,
                    launch: TriggerLaunch::Queued {
                        queue: "auditor".to_owned(),
                    },
                }],
            },
        )?;
    }
    Ok(())
}

fn create_math_publisher_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("publisher")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_MATH_PUBLISHER_ROLE_MD)?;
    let step = StepSlug::parse("publish")?;
    io::write_text_if_missing(
        &role_paths.step_path(&step),
        EPISODES_MATH_PUBLISHER_STEP_MD,
    )?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("pub".to_owned()),
                auto_archive: true,
                expose: vec![ExposedContext::LastAgentFinished],
                steps: vec![step],
                triggers: vec![TriggerConfig::RoleAgentFinished {
                    role: RoleSlug::parse("episode")?,
                    launch: TriggerLaunch::Queued {
                        queue: "publisher".to_owned(),
                    },
                }],
            },
        )?;
    }
    Ok(())
}

fn create_math_supervisor_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("supervisor")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_MATH_SUPERVISOR_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(
        &role_paths.step_path(&step),
        EPISODES_MATH_SUPERVISOR_STEP_MD,
    )?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Paused,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("o".to_owned()),
                auto_archive: true,
                expose: Vec::new(),
                steps: vec![step],
                triggers: vec![TriggerConfig::RoleAgentFinished {
                    role: RoleSlug::parse("episode")?,
                    launch: TriggerLaunch::Queued {
                        queue: "supervisor".to_owned(),
                    },
                }],
            },
        )?;
    }
    Ok(())
}

fn create_math_auditor_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("auditor")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), EPISODES_MATH_AUDITOR_ROLE_MD)?;
    let step = StepSlug::parse("audit")?;
    io::write_text_if_missing(&role_paths.step_path(&step), EPISODES_MATH_AUDITOR_STEP_MD)?;
    if !role_paths.config().exists() {
        io::write_toml(
            &role_paths.config(),
            &RoleConfig {
                version: ROLE_CONFIG_VERSION,
                status: RoleStatus::Active,
                backend: BackendName::Codex,
                mode: RoleMode::Oneshot,
                parallel: RoleParallelism::Count(1),
                agent_names: AgentNameScheme::Sequential,
                agent_prefix: Some("audit".to_owned()),
                auto_archive: true,
                expose: Vec::new(),
                steps: vec![step],
                triggers: vec![TriggerConfig::QueueIdle {
                    idle_queue: "auditor".to_owned(),
                    idle_seconds: 1800,
                    launch: TriggerLaunch::Queued {
                        queue: "auditor".to_owned(),
                    },
                }],
            },
        )?;
    }
    Ok(())
}

pub const EPISODES_CODE_PROJECT_MD: &str = r#"# Episodes Code Project

This is a production coding project organized as parallel implementation episodes.

This project was created from the `episodes-code` template. The intended workflow is:

- The target source repository lives at `repo/` in the think project root. Project setup must either
  `git clone` a repository URL there or copy a local filesystem directory there in its entirety
  before the supervisor is activated.
- `PROJECT.md` is the shared source of truth for product goals, target repository layout, check
  policy, non-goals, and merge policy.
- Implementation work is based on `master`.
- There is no pushing in this workflow. Episodes prepare local pull-style branches in `repo/`; the
  serial merger later integrates those branches locally.
- The `episode` role is the ordinary implementation role. Each episode owns one isolated git
  worktree and one local branch named `episodes/<agent-id>`.
- Finished `episode` agents publish structured branch handoffs to `channels/branches/<agent-id>.md`.
  The handoff names the local branch, base commit, head commit, checks, dependencies, and merge
  notes so the merger can integrate the work without reverse-engineering the whole transcript.
- The queued serial `merger` role integrates handed-off branches using the minimum analysis needed
  for a responsible merge. A merger can update local `master`, or it can produce a new committed
  consolidation branch for a later merger to integrate into `master`, as directed by its prompt.
  Consolidation branches are immutable outputs: later mergers consume them as source branches rather
  than extending or amending them in place. A merger can handle one branch or a compatible tranche of
  several branches. It exercises reasonable care by rerunning checks that could have broken because
  of the merged changes, including formatting when relevant, and avoids wasting time on checks for
  untouched surfaces. It resolves small conflicts, publishes a structured merge handoff to
  `channels/merges/`, and refuses unsafe merges.
- The `supervisor` role starts paused. When activated, it reads the full project design, `repo/`
  docs, finished branch handoffs, merge handoffs, and current status; then it launches orthogonal
  implementation episodes with precise prompts.
- If the merge queue grows faster than the serial merger can handle, the supervisor can group
  compatible completed branches by code area and launch one `merger` agent with a tranche prompt
  naming the exact branches to integrate.
- The `auditor` role performs periodic operational checks and publishes Markdown alerts only for
  actionable workflow problems.

Quality bar for implementation episodes:

- Read the repository's `AGENTS.md`, `DESIGN.md`, README files, docs, package manifests, and local
  test instructions before changing code.
- Preserve unrelated work. Never revert another agent's changes. If unexpected local changes block
  the task, report the conflict instead of overwriting them.
- Use the project language and framework idioms. Prefer existing local helpers and abstractions
  over inventing parallel APIs.
- Keep edits scoped to the assigned feature branch. If the assignment requires a shared interface,
  make the interface small, typed, and explicit so parallel agents can build against it.
- For Rust, use Rust 2024. Use traits, generics, exhaustive enums, newtypes, and domain-specific
  value objects where they clarify invariants or remove duplicated control flow. Keep DTOs and
  transport types separate from domain/application/provider internals.
- For typed REST systems, keep Rust as the source of backend DTO/schema truth, derive serde and
  schema traits where appropriate, expose OpenAPI, and regenerate frontend types rather than
  hand-copying payload shapes.
- Keep backend layers separated: domain objects and invariants, application use cases, concrete
  infrastructure/providers, and HTTP or CLI adapters. Do not leak transport details into provider
  traits.
- Prefer structured parsers, typed IRs, and command APIs over ad hoc string manipulation.
- Avoid long functions with repeated side-case branching. Add a trait, enum, table, or small data
  structure when that makes the flow clearer and cheaper to extend.
- Do not add silent fallback behavior for misconfiguration. Fail loudly with useful diagnostics.
- Use reasonable care when checking work. Rerun formatting and tests/builds that could have broken
  because of the touched code, and avoid wasting time on checks for surfaces the branch could not
  have affected. Record every command you ran and every relevant check you intentionally skipped.
- Frontend work should build the actual product surface first, not a marketing page. Use dense,
  operational UI with status attached to the object it explains, generated API types where present,
  and icons from the configured icon library.

Channels:

- `channels/branches` contains append-only structured branch handoffs from implementation agents.
- `channels/merges` contains append-only structured merge handoffs from the serial merger.
- `channels/alerts` contains actionable operational alerts from auditors or agents.

Large logs, benchmark outputs, caches, and raw artifacts that should be preserved but not published
belong in `data/own/`. Mention important artifacts in your final reply and branch handoff so future
agents can find them through `data/all/`.
"#;

pub const EPISODES_CODE_EPISODE_ROLE_MD: &str = r#"# Episode

You implement one production code task on one isolated git worktree and one local pull-style branch.

Your prompt should define the feature, bug fix, or refactor precisely. If the prompt is too broad,
first carve out the smallest coherent slice that can be completed, committed, checked, and merged
without blocking unrelated agents.
"#;

pub const EPISODES_CODE_EPISODE_STEP_MD: &str = r#"# Work

Work as one coding episode.

First read `PROJECT.md`, `repo/AGENTS.md`, `repo/DESIGN.md` when present, repository README files,
docs, manifests, and the files needed to understand your assigned slice.

If `repo/.git` does not exist, do not start coding. Publish an alert explaining that project setup
must copy the target source repository into `repo/`, write `manifest.toml` with
`disposition = "stop"`, and stop.

Set up exactly one private worktree before editing code. Use your agent id from the runtime summary
as the branch identity.

```bash
mkdir -p work/own
agent_root=$PWD
git -C repo fetch --all --prune
git -C repo worktree add "$agent_root/work/own/repo" -b episodes/<agent-id> master
printf '%s\n' 'episodes/<agent-id>' > work/own/BRANCH
```

If this is a resumed run and `work/own/repo` already exists, reuse it. Do not create a second branch
for the same episode unless the operator explicitly asks for that.

Implement the assigned code in `work/own/repo/`. Keep the branch focused:

- Prefer local patterns, existing helper APIs, and established module boundaries.
- Keep shared interfaces small and typed. If other agents may depend on an interface, document the
  contract in code or a concise note.
- For Rust, use Rust 2024 and the type system aggressively where it improves clarity: traits for
  swappable behavior, generics for reusable flows, exhaustive enums for states, newtypes for
  identifiers and units, and typed errors for real failure modes.
- Remove duplication when it is part of your assigned surface, but do not start unrelated rewrites.
- Avoid broad file churn. Avoid editing generated files unless the repository's normal command
  regenerates them.
- Add or update focused tests for the behavior you changed.

Before finishing, the worktree must be clean and the branch must be committed. Exercise reasonable
care with checks: rerun formatting and tests/builds that could have broken because of your changes,
and do not waste time rechecking unrelated surfaces that your branch could not have affected. If a
relevant check is too expensive or unavailable, run the best targeted subset and state exactly what
remains unchecked.

Publish a branch handoff to your outbox using `templates/episodes-code/branch-handoff.md` as the
shape:

```bash
mkdir -p channels/branches
cp templates/episodes-code/branch-handoff.md channels/branches/<agent-id>.md
```

The handoff must name the local branch, base commit, head commit, status, checks run,
dependencies, and merge notes. Never publish a branch handoff for uncommitted WIP. If you cannot
commit a clean branch, publish an alert instead.

Do not merge your implementation branch into `master`. Do not push anything. Leave `master`
integration to the `merger` role.

For repeatable roles, write `manifest.toml` with `role_summary` and `disposition = "stop"` unless
you truly need another run of the same episode.
"#;

pub const EPISODES_CODE_MERGER_ROLE_MD: &str = r#"# Merger

You are the serial local integration role for the episodes-code project.

Your job is to integrate finished local implementation branches responsibly. Unless your prompt
states otherwise, an ordinary triggered merge updates local `master`. A supervisor prompt may
instead ask you to merge several branches into a new committed consolidation branch, usually
`merges/<agent-id>`, so a later merger can integrate that consolidated branch into `master`.
Consolidation branches are immutable outputs: consume prior consolidation branches as source
branches, but do not extend, amend, or reuse them as the output branch for a later merge.

Do the minimum analysis required for the branch or tranche: inspect structured branch handoffs,
merge handoffs, touched files, current target state, conflicts, and relevant tests. Do not perform
broad feature development, and never push.
"#;

pub const EPISODES_CODE_MERGER_STEP_MD: &str = r#"# Merge

First read `TRIGGER.md`, `EXPOSED.md`, and your prompt. Ordinary triggers name one completed
`episode` agent; tranche prompts name a compatible group of completed local branches or prior
immutable consolidation branches to merge in one run. Find the relevant structured handoffs through the
`branches` and `merges` channels, or through
`work/all/<role>/agents/<agent>/channels/<channel>/`.

Read the finished agents' final replies, handoffs, and relevant changed files. Determine source
branches, base commits, head commits, checks already run, dependencies, target mode, and merge
risks. The target mode is either:

- update local `master` after the integration branch is committed and checked;
- leave a new committed consolidation branch for a later merger to integrate into `master`.

If the prompt does not specify a target mode, use `master` integration for ordinary single-episode
triggers and consolidation-branch output for explicit tranche prompts.
Consolidation branches are immutable outputs. Later mergers may consume them as source branches, but
must not extend, amend, or reuse an existing consolidation branch as their output branch.

Before creating a worktree, check whether each source branch head is already reachable from current
local `master`:

```bash
git -C repo fetch --all --prune
git -C repo merge-base --is-ancestor <head-commit> master
```

If every source branch is already reachable, do not create an empty merge commit and do not update
`master`. Publish a merge handoff with `result = "already-integrated"`, `master_updated = false`,
and the skipped source branches recorded.

For a fresh run, create your own merge worktree under `work/own/repo/` from the current local
`master`. The merger agent id is unique, so use it as the integration branch name and default
consolidation branch:

```bash
mkdir -p work/own
agent_root=$PWD
git -C repo fetch --all --prune
git -C repo worktree add "$agent_root/work/own/repo" -b merges/<agent-id> master
printf '%s\n' 'merges/<agent-id>' > work/own/BRANCH
```

If this is a resumed run and `work/own/repo` already exists, reuse it. Do not create another
`merges/<agent-id>` branch. Read `work/own/BRANCH`, inspect the existing worktree, and continue from
the clean committed state or clearly diagnose the blocker.

Keep integration serial: do not share your worktree, and do not start a second independent merge in
this run.

Merge each remaining candidate branch with explicit review. Before merging a candidate, check
whether its handoff head is already reachable from the current integration branch:

```bash
git merge-base --is-ancestor <head-commit> HEAD
```

If it is already reachable, record it as skipped and do not merge it again. The default tranche
strategy is one merge commit per source branch:

```bash
cd work/own/repo
git merge --no-ff --no-commit <branch-name>
# inspect, resolve if needed, then commit before merging another branch
git commit
```

Do not start the next source-branch merge while the index contains an uncommitted previous merge.
If several branches are obviously compatible and you expect a clean octopus merge, you may instead
merge them in one commit:

```bash
git merge --no-ff --no-commit <branch-a> <branch-b> <branch-c>
git commit
```

Use the octopus form only for clean compatible branches. If the input set is wrong, a branch is
missing, or the conflicts are too large for a focused integration run, abort and publish a blocked
merge handoff or alert.

If the merge is clean, inspect the resulting diff and exercise reasonable care with checks. Rerun
formatting and tests/builds that could have broken because of the merged changes, including broader
checks for shared interfaces. Do not waste time rechecking unrelated surfaces that the branch or
tranche could not have affected. Finish with a clean committed integration branch. If every source
branch was skipped as already reachable and no merge commit was needed, publish
`result = "already-integrated"` instead of treating the no-op as a problem.

If no merge commit was created because every source branch was already reachable, skip the target
update regardless of target mode. If at least one merge commit was created and the target mode is
`master` integration, update local `master` from the project-root `repo/` checkout. Ensure `repo/`
is on `master` and clean, then merge `merges/<agent-id>` into `master`. Do not push.

If the target mode is consolidation only, do not update `master`. Leave `merges/<agent-id>` as the
committed output branch, ready for a later merger. Do not reuse an existing consolidation branch as
your output. The supervisor may ask a later `merger` agent to use any prior consolidation branch as
one of its source branches.

If the merge conflicts or appears semantically unsafe, do not force it. Abort or leave a clearly
diagnosed local state, publish an alert or blocked merge handoff explaining the blocker, and stop.

Publish a structured merge handoff to `channels/merges/<agent-id>.md` using
`templates/episodes-code/merge-handoff.md`. Include the source branch or tranche input branches,
skipped branches, request source, requester agent if any, output branch, whether `master` was
updated, `master` before and after when applicable, merge commit or blocker, conflicts and
resolutions, checks run, and any follow-up work the supervisor should launch.
Set `request_source` to the concrete launch source; `episode-finished-trigger`,
`supervisor-prompt`, and `manual-prompt` are typical values. Set `requester_agent` to `role/agent`
when known, and leave it empty only when no requester is identified.
For a consolidation-only run, set `target_mode = "consolidation"`, `master_updated = false`, and
put the committed consolidation branch in `output_branch`.
Whether the merge is single-branch or tranche is determined by the number of entries in
`source_branches`.

When the merge queue is too large to keep up with one-by-one merges, do not manually absorb the
whole backlog unless your prompt explicitly named a compatible tranche. Otherwise report the backlog
shape so the supervisor can launch a focused tranche merger prompt.
"#;

pub const EPISODES_CODE_SUPERVISOR_ROLE_MD: &str = r#"# Supervisor

You supervise a parallel coding programme.

Your job is to read the large project goals, repository design, current code architecture, open
branch handoffs, merge handoffs, and think status, then allocate work to implementation episodes so
the project advances with maximum safe parallelism.

Optimize for throughput, not merely the number of agents. Tasks that are too small flood the merge
queue and waste review overhead. Tasks that are too large leave agents waiting on hidden
dependencies. Prefer slices that can be implemented and merged independently: one API boundary, one
UI workflow, one provider implementation, one storage adapter, one test family, or one refactor
across a tightly bounded module family.
"#;

pub const EPISODES_CODE_SUPERVISOR_STEP_MD: &str = r#"# Supervise

First read the trigger context. Then read `PROJECT.md`, repository `repo/AGENTS.md`,
`repo/DESIGN.md`, README files, relevant docs, current branch handoffs, merge handoffs, and
`think status --plain --all`.

Before any planning, verify that `repo/.git` exists. If it does not exist, project setup is
incomplete. Write an alert to `channels/alerts/<agent-id>-missing-repo.md`, write a final summary
explaining that the target source repository must be copied into `repo/`, then run
`think role pause supervisor` from the project root as your final command and exit without launching
agents.

Maintain `data/own/supervisor-journal.md` as a compact project map:

- current product and architecture goals;
- active implementation prongs;
- running and recently finished episodes;
- branches waiting for merge;
- conflicts, blockers, and shared interfaces;
- next useful work to launch.

When launching implementation episodes, use `think agent new episode --prompt '...'` from the
project root. Each prompt should include:

- the feature or refactor title;
- the exact code surface and expected output;
- docs and files to read first;
- files or modules that should be avoided to preserve parallelism;
- the expectation that the episode exercise reasonable care by rerunning formatting and tests/builds
  that could have broken because of its changes, without wasting time on unrelated checks;
- the requirement to publish a structured branch handoff using
  `templates/episodes-code/branch-handoff.md`;
- the explicit instruction to commit a clean local branch and never push.

Keep a default cap of eight active implementation episodes unless `PROJECT.md` says otherwise.
Before launching work, count non-archived `episode` agents with status `starting` or `running`.
Launch only the remaining useful capacity. It is correct to launch no agents when the merge queue is
the bottleneck or when the next slice is not clear.

Merge backlog strategy:

If completed branch handoffs are accumulating faster than the serial `merger` can integrate them,
do not ask one agent to process the whole backlog indiscriminately. Instead:

1. Group completed branches by compatibility: same subsystem, no overlapping edits, or cleanly
   layered dependencies.
2. Launch a `merger` agent with a tranche prompt using `think agent new merger --prompt '...'`.
3. In the tranche prompt, name the exact input branches, the current `master` commit, any prior
   consolidation branches being consumed as inputs, likely conflict areas, the expectation to rerun
   only checks that could have broken, and whether the merger should update `master` or leave a new
   committed consolidation branch for a later merger.
4. Do not request a tranche for branches that touch the same fragile code unless one focused
   human-sized conflict resolution is clearly better than separate merges.

Production coding maxims:

- Use the repository's existing architecture as the starting point.
- Favor typed boundaries over informal conventions.
- Ask agents to implement reusable abstractions only where they remove real duplication or clarify
  data flow.
- For Rust, ask for traits, generics, exhaustive enums, typed identifiers, and provider boundaries
  where they make the code simpler and more maintainable.
- For frontend work, ask for dense operational UI and generated API types; avoid marketing surfaces.
- Tell agents to rerun relevant tests and formatting with reasonable care.
- Preserve negative findings: if a slice is blocked, record why and launch the smallest unblocker.

Before exiting, update `data/own/supervisor-journal.md` and summarize what you launched, what you
intentionally did not launch, and the current merge bottleneck if any.
"#;

pub const EPISODES_CODE_AUDITOR_ROLE_MD: &str = r#"# Auditor

You perform a light operational audit for the episodes-code template. This is workflow health
checking; the core think engine does not know what a code branch, worktree, merge handoff, or merge
group means.

Only publish an alert when you find a real actionable problem. Do not publish routine "all clear"
messages. Alerts are Markdown files written to `channels/alerts/`.
"#;

pub const EPISODES_CODE_AUDITOR_STEP_MD: &str = r#"# Audit

Inspect the current project state for episodes-code workflow problems:

- missing `repo/.git` while the supervisor is active;
- finished episode agents without a branch handoff in `channels/branches`;
- branch handoffs missing TOML frontmatter, branch name, head commit, status, checks, or readiness;
- finished agents whose handed-off branch no longer exists in `repo`;
- branch handoffs for uncommitted WIP or dirty worktrees;
- merger agents that failed to publish a structured merge handoff after being triggered;
- branches waiting too long for merge while the supervisor is inactive;
- a merge queue large enough to justify a grouped tranche prompt to the `merger` role;
- stale running episodes that appear blocked rather than merely long-running;
- dirty shared `repo/` checkout, dirty `master`, or worktree misuse that could corrupt another
  agent's branch;
- alerts or merge blockers that require operator action.

Use judgment. Do not manufacture alerts from incomplete evidence, and do not block the project for
minor style issues. If you find an actionable problem, write one concise Markdown file to
`channels/alerts/`, with a filename beginning with the current auditor agent id and a short slug.
Explain the evidence, why it matters, and the next operator action. If nothing needs operator
attention, publish nothing.
"#;

const EPISODES_CODE_TEMPLATE_README: &str = r#"# episodes-code Template

This directory contains scaffold notes for episodes-code agents. The live instructions are in
`PROJECT.md` and the role prompt files under `roles/`.

Use this template when the project is a production codebase that benefits from many isolated local
feature branches, a serial merger, and a supervisor that keeps implementation and merge
throughput balanced.
"#;

const EPISODES_CODE_BRANCH_HANDOFF_TEMPLATE: &str = r#"# Branch Handoff

+++
kind = "branch"
role = "episode"
agent = "<agent-id>"
branch = "episodes/<agent-id>"
target_branch = "master"
base = "<base-commit>"
head = "<head-commit>"
status = "ready"
ready_for_merge = true
depends_on = []
checks = ["<command>: <pass|fail|skipped>"]
+++

## Objective

## Summary

## Files

## Checks

## Merge Notes
"#;

const EPISODES_CODE_MERGE_HANDOFF_TEMPLATE: &str = r#"# Merge Handoff

+++
kind = "merge"
agent = "<agent-id>"
request_source = "episode-finished-trigger"
requester_agent = "episode/<requester-agent-id>"
source_branches = ["<branch-name>"]
skipped_branches = []
output_branch = "merges/<agent-id>"
target_mode = "master"
master_updated = true
master_before = "<commit>"
master_after = "<commit>"
result = "merged"
merge_commit = "<commit>"
checks = ["<command>: <pass|fail|skipped>"]
+++

## Summary

## Conflicts

## Checks

## Follow-Up
"#;

pub const EPISODES_MATH_PROJECT_MD: &str = r#"# Episodes Math Project

This is a research mathematics project organized as persistent exploratory episodes.

This project was created from the `episodes-math` template. The intended workflow is:

- `PROJECT.md` gives the shared research culture and operating rules.
- The `episode` role is the ordinary research role. Each agent created from it is one durable
  episode, and the agent id is the episode slug.
- Every agent has a private writable workspace at `work/own/`. Other agents' workspaces are visible
  through read-only `work/all/<role>/agents/<agent>/` symlinks. There is no shared mutable work tree.
- The `publisher` role is queued and serial. It wakes after each completed episode, copies the prior
  publisher manuscript when one exists, includes every terminal episode agent with a usable TeX
  source exactly once in episode order, checks the build, and publishes the finished PDF to the
  `report` channel.
- The `supervisor` role starts paused. When activated, it reads finished work and channel outputs,
  maintains a high-level map of active prongs, and launches new `episode` agents when that is the
  best next move.
- `channels/report-single` is for standalone episode PDFs. `channels/report` is for the publisher's
  complete manuscript PDF. Channel directories are append-only artifact logs managed by think; agents
  publish by writing to their local `channels/<channel>/` outbox.
- `channels/alerts` is a generic notification channel. The template auditor may publish Markdown
  alerts there when it finds a real operational problem.
- Static TeX and experiment seeds live in `templates/episodes-math/`. Copy them into `work/own/`
  when useful; do not treat that template directory as a shared workspace.

Work ambitiously, comprehensively, and mathematically. The goal is not merely to collect examples,
but to find elegant conceptual rules, test them hard, separate evidence from proof, and record
enough detail that later agents can reproduce and extend the work.

When assigned a specific possibility or hypothesis, conduct an exhaustive experimental and
theoretical investigation of it in the case at hand. Study full numerical output when it exists,
look for the most likely combinatorial rules or formulae suggested by the data, and rigorously
test, refine, or discard those conjectures.

The `episode` role defines the shared episode programme. Each agent spawned from that role is one
actual episode. Use the agent id from the runtime summary as the durable episode identity. Name the
main episode file `work/own/episodes/<agent-id>.tex`, and use that same episode file across loops
for the same agent. Do not create fresh episode files for each new prompt, query, or refinement.

The report uses the house TeX layout inherited from `acalc`: `report.tex` inputs `preamble.tex`,
episodes are numbered `\section{...}` entries, day cards divide phases of work, and standalone
episode PDFs are built through `episode-standalone.tex`. Each episode source should set the compact
running project title with `\episodeprojecttitle{...}` near the top of
`work/own/episodes/<agent-id>.tex`; this keeps standalone episode headers sourced from the episode
agents themselves as the project title sharpens over time. The right header is supplied by the
template as the running `think` version.

When papers are needed, place them under `work/own/papers/`. Prefer TeX manuscripts when they are
available, keep TeX source alongside PDFs, and save decoded PDF text beside the PDF when that makes
the paper easier to search, quote, or audit.

Prefer reproducible computations and broad audits over anecdotal examples. Keep scripts, data
summaries, and enough command information to make checks repeatable. Numerical experiments expected
to run for more than one second should include useful progress reporting with minimal performance
impact. Run large tests with generous memory limits, but low enough that they cannot exhaust RAM.
If a calculation would benefit from Python packages such as SymPy, install and use them in a local
micromamba environment rather than avoiding the computation.

When an episode adds new computational functionality, prefer implementing it as a new subcommand or
clearly isolated script entry point where that is natural, so parallel agents can work independently
with fewer file and interface conflicts.

For serious numerical experiments, copy the seed Rust experiment crate into `work/own/experiments/`
and extend it there. Put reusable algorithms, data structures, parsers, and performance-critical
kernels in `work/own/experiments/src/core/` modules rather than duplicating them inside command
files. Keep command-line interfaces deterministic and record exact commands and meaningful output
summaries in the episode TeX file.

Build or sanity-check TeX output after substantial episode edits. Episodes publish only the
standalone PDF they want preserved in `channels/report-single/`. The publisher owns the combined
manuscript and publishes the combined PDF in `channels/report/`.

Large raw outputs, caches, logs, and precomputed tables that should be preserved but not published
belong in `data/own/`. Mention important data artifacts in the episode so future agents can find
them through `data/all/`.

Never undo another agent's work. If overlapping edits or incompatible approaches occur,
deconflict explicitly and preserve the other work wherever possible.
Never kill another running process unless you are sure you spawned it yourself.
"#;

pub const EPISODES_MATH_ROLE_MD: &str = r#"# Episode

State the research objective precisely. Include the open problem, known subcases, relevant
definitions, allowed references, computational scope, and what durable progress would count as a
successful episode.
"#;

pub const EPISODES_MATH_STEP_MD: &str = r#"# Work

Work in your assigned episode. Develop definitions, examples, computations, conjectures, proof
attempts, counterexamples, literature notes, failed approaches, and open questions as appropriate.

Your agent id from the runtime summary is the episode identity. Write the main TeX episode as
`work/own/episodes/<agent-id>.tex` unless the role explicitly instructs otherwise. Keep using that
same file if this agent loops through multiple runs.

When starting a fresh workspace, copy the useful seed files from `templates/episodes-math/` into
`work/own/`, then create `work/own/episodes/`, `work/own/papers/`, and any computation directories
you need. Do not edit the seed template directory.

Use the standard episode TeX shape:

```
\episodeprojecttitle{Short Project Title}
\section{Episode title}
...
```

The `\episodeprojecttitle{...}` line is the episode-owned source for the small running header in
standalone episode PDFs and in the combined report from that point onward. Keep it short and update
it if the project title becomes clearer. Use numbered `\section{...}` and `\subsection{...}`; do
not introduce a separate preamble in episode files.

Write durable mathematical content in `work/own/`. Put large raw data or generated artifacts that
should be preserved but not published in `data/own/`. Mention important data artifacts in your
episode so future agents can find them.

Prefer reproducible scripts and command-line entry points for computations. If new code is useful
outside a one-off experiment, make it easy for later agents to run. When Python packages are needed,
use a local micromamba environment. Use numbered TeX sections/subsections. After substantial TeX
edits, build the standalone episode PDF from `work/own/`, for example:

```
cd work/own
make episodes/<agent-id>.pdf
```

Before exiting, publish your standalone PDF by copying it to `channels/report-single/<agent-id>.pdf`.
Think will append it to the `report-single` channel after your successful run. Do not write directly
to the project channel directory.

For repeatable roles, set `disposition = "stop"` or `disposition = "continue"` in manifest.toml as
directed by the runtime mode instructions.
"#;

pub const EPISODES_MATH_PUBLISHER_ROLE_MD: &str = r#"# Publisher

You maintain the combined manuscript for the math episodes project. This role is deliberately
serial: every publisher agent receives the prior publisher's completed workspace through
`EXPOSED.md` when one exists, integrates every terminal episode agent with a usable TeX source in
the correct order, checks the TeX build, and publishes the new combined PDF.

You do not do mathematical research and you do not rewrite episode content except for minimal TeX
repairs needed for compilation. Preserve episode order and include paths carefully. Your durable
workspace is `work/own/`; the combined manuscript should live in `work/own/manuscript/`.
"#;

pub const EPISODES_MATH_PUBLISHER_STEP_MD: &str = r#"# Publish

First read `TRIGGER.md`. For the normal trigger, it names the `episode` agent that just finished.
Use that as a hint that publication may be stale, not as a limit on what to include.

Read `EXPOSED.md`. If it names a previous finished publisher and gives a `work/own` directory,
copy that previous `manuscript/` directory into your own `work/own/manuscript/` before making any
changes. If there is no previous publisher manuscript, initialize `work/own/manuscript/` from
`templates/episodes-math/`.

Use `think status --plain --all` to identify terminal `episode` agents. Include only agents whose
episode status is done or stopped and whose TeX source exists. Do not include sources from agents
that are still starting, running, waiting, errored, or otherwise incomplete.

Find episode sources through your refreshed workspace links:

```
work/all/episode/agents/<episode-agent>/episodes/<episode-agent>.tex
```

Include every terminal usable episode exactly once in `work/own/manuscript/report.tex`. Put new
sequential episodes in their natural position, for example `ep5` goes between `ep4` and `ep6`. If
the project uses non-sequential names such as random or verb-noun names, sort by each episode
agent's finish time when available, falling back to the channel artifact time if needed.

Use relative inputs of this form:

```tex
\input{../../all/episode/agents/<episode-agent>/episodes/<episode-agent>}
```

Avoid duplicate includes. If an episode source is missing or malformed, inspect the episode agent
root and transcript enough to diagnose the problem, make only minimal compile repairs in your own
manuscript workspace when possible, and record the issue in your final reply.

Compile-check from `work/own/manuscript/`, preferably with:

```
make main
```

After a successful build, publish the combined manuscript PDF by copying
`work/own/manuscript/report.pdf` to `channels/report/report.pdf`. Do not publish TeX logs, caches,
or large scratch outputs. Do not edit project channel directories directly.
"#;

pub const EPISODES_MATH_SUPERVISOR_ROLE_MD: &str = r#"# Supervisor

You supervise a mathematical episode programme. Your job is to keep the research moving by reading
finished episode work, published PDFs, and publisher outputs, maintaining a high-level map of
active prongs, and launching carefully chosen new episode agents.

The `episode` role is the unit of research work. Each episode agent should receive a precise,
self-contained prompt: a conjecture to test, a subcase to compute, a proof route to pursue, a
counterexample search, a literature check, or a continuation of a successful previous episode. If
an episode made good progress and there is an obvious next step, simply launch a new episode to
continue that thread. If the work suggests distinct branches, launch forked prongs rather than
blending them prematurely into one story.

You wake independently when an episode finishes. Publication may still be pending; do not wait for
the publisher. Read all previous episode work that matters, then read the current triggering
episode particularly carefully. Decide whether there are natural further points of exploration
worth launching. If not, say so and stop.

Never let more than eight episode agents be running at the same time. Inspect `think status --plain --all`
before launching work, count non-archived `episode` agents with status `starting` or `running`, and
launch at most the remaining capacity. It is fine to launch no agents if the project is saturated
or if the next mathematical move is unclear.

Maintain a high-level journal of your own thinking in `data/own/supervisor-journal.md`. Use it for
the research map: current prongs, what each prong is trying to decide, which completed episodes
mattered, what is blocked, and what should be tried next. Keep it compact and update it
iteratively; do not turn it into a substitute for the episode writeups.

Mathematical research maxims for this role:

- Prefer concrete, falsifiable episode prompts over broad mandates.
- Separate prongs of inquiry until there is real evidence that they belong together.
- Preserve negative results, failed heuristics, and dead ends when they narrow the search.
- Launch episodes that try to see why the situation is elegant, canonical, or forced when the data
  points that way. The right prompt can ask an agent to assume the structure is elegant and find
  the hidden reason.
- Balance proof attempts with computation, examples, and sanity checks.
- Ask for exact small cases before extrapolating from large noisy experiments.
- Continue promising work in a new episode instead of asking one agent to own the whole project.
- Fork interesting side-branches early when they are cheap, especially when they could invalidate
  the current picture.
- Leave report integration to the queued publisher; your own durable contribution is orchestration
  and high-level synthesis.

When launching an episode, use `think agent new episode --prompt '...'` from the project root, or
the equivalent interactive command if you need a longer prompt. Include the previous episode id or
trigger cause when continuing a prong. Ask the episode to name its TeX file after its own agent id,
use `manifest.toml` properly, publish its standalone PDF to `report-single`, and keep large raw
artifacts in `data/own/`.
"#;

pub const EPISODES_MATH_SUPERVISOR_STEP_MD: &str = r#"# Supervise

First read the trigger context.

If the trigger kind is `role-agent-finished` for `episode`, focus on the newly finished episode.
Read its `work/own/` material through `work/all/episode/agents/<agent>/`, inspect its transcript
and final reply if needed, and compare it against the important previous episodes. Place that
progress in the global project map. Decide whether to continue that prong, fork related prongs, ask
for verification, or pause because enough agents are already running.

If the trigger kind is `manual`, honor the manual reason if present and otherwise perform a compact
supervisor review.

Use `think status --plain --all` to inspect roles and agents. If you launch new episode agents, be specific:
each prompt should name the prong, the immediate mathematical objective, the expected artifacts,
and what would count as useful progress. Do not launch more than eight running episode agents in
total.

Before exiting, update `data/own/supervisor-journal.md` with a compact high-level summary of what
you saw and what you launched. Your journal in `data/own/` is preserved outside the published
channels.
"#;

pub const EPISODES_MATH_AUDITOR_ROLE_MD: &str = r#"# Auditor

You perform a light operational audit for the episodes-math template. This is template-specific
health checking; the core think engine does not know what an episode, publisher, or report means.

Only publish an alert if you find a real actionable problem. Do not publish routine "all clear"
messages. Alerts are Markdown files written to `channels/alerts/`.
"#;

pub const EPISODES_MATH_AUDITOR_STEP_MD: &str = r#"# Audit

Inspect the current project state for basic episodes-math workflow problems:

- finished episode agents whose `work/own/episodes/<agent>.tex` appears missing;
- finished episode agents that did not publish an obvious standalone artifact to `report-single`;
- publisher agents that were triggered but did not publish a combined report artifact;
- supervisor agents that appear stuck, errored, or unable to launch obvious follow-up work;
- stale trigger queues that look operationally blocked rather than merely idle.

Use judgment. Do not manufacture alerts from incomplete evidence, and do not encode narrow
assumptions about the mathematics. If you find an actionable issue, write one concise Markdown file
to `channels/alerts/`, with a filename beginning with the current auditor agent id and a short slug.
Explain the evidence, why it matters, and the next operator action. If nothing needs operator
attention, publish nothing.
"#;

const EPISODES_MATH_REPORT_TEX: &str = r#"\input{preamble}

\pretocmd{\section}{\clearpage}{}{}

% Unnumbered dividers; keep them outside the section counter so episodes
% continue numbering sequentially across phases.
\newcommand{\daycard}[2]{%
  \setreportday{#1}%
  \clearpage
  \phantomsection
  \pdfbookmark[1]{**********\space\space\space\space#1\space\space\space\space**********}{#2}%
  \thispagestyle{empty}%
  \vspace*{\fill}%
  \begin{center}
    \begingroup
    \setlength{\fboxrule}{0.8pt}%
    \setlength{\fboxsep}{2.5em}%
    \fbox{%
      \begin{minipage}{0.68\textwidth}
        \centering
        {\normalfont\bfseries\scshape\Huge #1\par}
      \end{minipage}%
    }%
    \endgroup
  \end{center}
  \vspace*{\fill}%
}

\title{\ThinkProjectTitle}
\author{}
\date{\today}

\begin{document}

\maketitle

\pdfbookmark[1]{Orientation}{orientation}
\section*{Orientation}

This report is the canonical ledger for persistent research episodes.  Each
episode is a numbered section with enough mathematical context, computations,
negative evidence, and proof obligations for later agents to audit and extend
the work.

\setcounter{section}{0}

% Publisher agents add episode inputs below, grouped by day cards when the
% project history calls for them.

\end{document}
"#;

const EPISODES_MATH_PREAMBLE_TEX: &str = concat!(
    r#"\documentclass[11pt]{article}

\usepackage[margin=1in,headheight=14pt]{geometry}
\usepackage{amsmath,amssymb,amsthm,array,booktabs,longtable}
\usepackage{graphicx}
\usepackage{etoolbox}
\usepackage{titlesec}
\usepackage{xcolor}
\usepackage{fancyhdr}
\usepackage[T1]{fontenc}
\usepackage{newpxtext,newpxmath}
\usepackage[hidelinks,bookmarksnumbered,bookmarksopen]{hyperref}
\usepackage{bookmark}

\bookmarksetup{open,numbered}

\newcommand{\ThinkProjectTitle}{Research Episodes}
\newcommand{\setthinkprojecttitle}[1]{\gdef\ThinkProjectTitle{#1}}
\newcommand{\episodeprojecttitle}[1]{\setthinkprojecttitle{#1}}
\newcommand{\ThinkVersion}{think v"#,
    env!("CARGO_PKG_VERSION"),
    r#"}

\pagestyle{fancy}
\fancyhf{}
\fancyhead[L]{\footnotesize\scshape\ThinkProjectTitle}
\fancyhead[R]{\footnotesize\texttt{\ThinkVersion}}
\fancyfoot[C]{\footnotesize\thepage}
\renewcommand{\headrulewidth}{0.4pt}
\renewcommand{\footrulewidth}{0pt}

\newcommand{\ReportCurrentDay}{}
\newcommand{\setreportday}[1]{\gdef\ReportCurrentDay{#1}}
\newcommand{\episodesectionlabel}{%
  \makebox[\linewidth][s]{%
    {\LARGE\scshape Episode \thesection}%
    \hfill
    \ifx\ReportCurrentDay\empty
    \else
      {\LARGE\scshape\textcolor{black!50}{\ReportCurrentDay}}%
    \fi
  }%
}

\titleformat{name=\section}[display]
  {\normalfont\bfseries\raggedright}
  {\episodesectionlabel}
  {1.2ex}
  {\titlerule[0.8pt]\vspace{1.8ex}\huge\raggedright}
  [\vspace{1.2ex}\titlerule]
\titleformat{name=\section,numberless}[display]
  {\normalfont\bfseries\raggedright}
  {}
  {0pt}
  {\titlerule[0.8pt]\vspace{1.8ex}\huge\raggedright}
  [\vspace{1.2ex}\titlerule]
\titlespacing*{\section}{0pt}{0pt}{3.5ex}

\newcommand{\Cstar}{\mathbb{C}^{*}}
\newcommand{\CC}{\mathbb{C}}
\newcommand{\PP}{\mathbb{P}}
\newcommand{\St}{\operatorname{St}}
\newcommand{\BB}{\operatorname{BB}}

\newtheorem{theorem}{Theorem}[section]
\newtheorem{lemma}[theorem]{Lemma}
\newtheorem{proposition}[theorem]{Proposition}
\newtheorem{corollary}[theorem]{Corollary}
\newtheorem{definition}[theorem]{Definition}
\theoremstyle{remark}
\newtheorem{remark}[theorem]{Remark}
"#
);

const EPISODES_MATH_EPISODE_STANDALONE_TEX: &str = r#"\input{preamble}

\providecommand{\EpisodeFile}{episodes/ep1}
\providecommand{\EpisodeSectionOffset}{0}

\begin{document}

\setcounter{section}{\EpisodeSectionOffset}
\input{\EpisodeFile}

\end{document}
"#;

const EPISODES_MATH_EPISODES_README: &str = r#"# Episodes

Each think agent should write its main TeX episode here using its agent id, for example
`episodes/ep1.tex`.

Start each episode with the shared header source and a numbered section:

```tex
\episodeprojecttitle{Short Project Title}
\section{Episode title}
```

Do not add a documentclass or local preamble to episode files. The combined report and standalone
episode PDFs both use the root `preamble.tex` through `report.tex` and `episode-standalone.tex`.
"#;

const EPISODES_MATH_PAPERS_README: &str = r#"# Papers

Place papers used by the project here. Prefer TeX manuscripts when available. Keep TeX source
alongside PDFs, and save decoded PDF text beside PDFs when that helps search or audit.
"#;

const EPISODES_MATH_SCRIPTS_README: &str = r#"# Scripts

Place reusable or report-worthy computation scripts here. Large generated outputs should usually go
in the project-managed agent data directory instead of published channels.
"#;

const EPISODES_MATH_GITIGNORE: &str = r#"build/
*.aux
*.fdb_latexmk
*.fls
*.log
*.out
*.toc
experiments/target/
"#;

const EPISODES_MATH_EXPERIMENTS_README: &str = r#"# Experiments

This is a seed Rust crate for serious numerical experiments in an episode workspace.

Each episode should add one or more subcommands it controls, usually in
`src/commands/episode_<agent-id>.rs`, normalizing any invalid Rust module-name characters in the
agent id, and should touch `src/commands/mod.rs` only enough to expose those commands through Clap.
Keep the dispatcher minimal so parallel agents rarely conflict there.

Move reusable algorithms, data structures, parsers, enumerators, and performance-critical kernels
into `src/core/` rather than duplicating them across episode commands. Optimize shared core code
aggressively, keep command-line interfaces deterministic, and record exact cargo invocations in the
episode TeX file. Commit `Cargo.lock` whenever Cargo creates or updates it.
"#;

const EPISODES_MATH_EXPERIMENTS_CARGO_TOML: &str = r#"[package]
name = "work-experiments"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
anyhow = "1"
clap = { version = "4", features = ["derive"] }
rayon = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
"#;

const EPISODES_MATH_EXPERIMENTS_MAIN_RS: &str = r#"use clap::Parser;
use work_experiments::Cli;

fn main() -> anyhow::Result<()> {
    Cli::parse().run()
}
"#;

const EPISODES_MATH_EXPERIMENTS_LIB_RS: &str = r#"pub mod commands;
pub mod core;

use clap::Parser;

#[derive(Parser)]
#[command(name = "work-experiments")]
#[command(about = "Shared high-performance computations for research episodes")]
pub struct Cli {
    #[command(subcommand)]
    command: commands::Command,
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        self.command.run()
    }
}
"#;

const EPISODES_MATH_EXPERIMENTS_COMMANDS_MOD_RS: &str = r#"mod smoke;

#[derive(clap::Subcommand)]
pub enum Command {
    Smoke(smoke::Args),
}

impl Command {
    pub fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Smoke(args) => smoke::run(args),
        }
    }
}
"#;

const EPISODES_MATH_EXPERIMENTS_SMOKE_RS: &str = r#"#[derive(clap::Args)]
pub struct Args {
    #[arg(long, default_value_t = 1)]
    repetitions: usize,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    for index in 1..=args.repetitions {
        println!("smoke {index}");
    }
    Ok(())
}
"#;

const EPISODES_MATH_EXPERIMENTS_CORE_MOD_RS: &str = r#""#;

const EPISODES_MATH_MAKEFILE: &str = r#"PDFLATEX ?= pdflatex
LATEXFLAGS ?= -interaction=nonstopmode -halt-on-error
FLOCK ?= flock
LATEX_LOCK_DIR ?= build/locks
LATEX_LOCK = $(FLOCK) $(LATEX_LOCK_DIR)/$(subst /,_,$@).lock
LOCKED_PDFLATEX = $(LATEX_LOCK) $(PDFLATEX) $(LATEXFLAGS)

EPISODES := $(wildcard episodes/*.tex)
EPISODE_PDFS := $(patsubst episodes/%.tex,episodes/%.pdf,$(EPISODES))

.PHONY: report main episodes episode-pdfs pdfs experiments-check experiments-smoke clean

report: pdfs

main: report.pdf

episodes: $(EPISODE_PDFS)

episode-pdfs: episodes

pdfs: report.pdf episodes

build/episodes:
	mkdir -p $@

$(LATEX_LOCK_DIR):
	mkdir -p $@

report.pdf: report.tex preamble.tex $(EPISODES) | $(LATEX_LOCK_DIR)
	$(LOCKED_PDFLATEX) report.tex

episodes/%.pdf: episodes/%.tex episode-standalone.tex preamble.tex | build/episodes $(LATEX_LOCK_DIR)
	episode_number=$$(printf '%s' '$*' | sed 's/^[^0-9]*//'); \
	if [ -z "$$episode_number" ]; then \
	  section_offset=0; \
	else \
	  section_offset=$$((episode_number - 1)); \
	fi; \
	$(LOCKED_PDFLATEX) -output-directory=build/episodes -jobname=$* \
	  "\\def\\EpisodeFile{episodes/$*}\\def\\EpisodeSectionOffset{$$section_offset}\\input{episode-standalone.tex}"
	cp build/episodes/$*.pdf $@

experiments-check:
	cargo check --manifest-path experiments/Cargo.toml

experiments-smoke:
	cargo run --manifest-path experiments/Cargo.toml -- smoke

clean:
	rm -f report.aux report.log report.out report.toc report.pdf
	rm -f episodes/*.aux episodes/*.log episodes/*.out episodes/*.toc episodes/*.pdf
	rm -rf build
"#;
