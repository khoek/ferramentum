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
        ProjectTemplate::MathEpisodes => apply_math_episodes(project),
    }
}

pub fn default_role_md(template: Option<ProjectTemplate>) -> &'static str {
    match template {
        Some(ProjectTemplate::MathEpisodes) => MATH_EPISODES_ROLE_MD,
        None => prompt::DEFAULT_ROLE_MD,
    }
}

pub fn default_step_slug(template: Option<ProjectTemplate>) -> Result<StepSlug> {
    match template {
        Some(ProjectTemplate::MathEpisodes) => StepSlug::parse("work"),
        None => StepSlug::parse("work"),
    }
}

pub fn default_step_md(template: Option<ProjectTemplate>, step: &StepSlug) -> &'static str {
    match (template, step.as_str()) {
        (Some(ProjectTemplate::MathEpisodes), "work") => MATH_EPISODES_STEP_MD,
        _ => prompt::DEFAULT_STEP_MD,
    }
}

pub fn default_agent_names(template: Option<ProjectTemplate>) -> AgentNameScheme {
    match template {
        Some(ProjectTemplate::MathEpisodes) => AgentNameScheme::Sequential,
        None => AgentNameScheme::AdjectiveNoun,
    }
}

pub fn default_parallel(template: Option<ProjectTemplate>) -> RoleParallelism {
    match template {
        Some(ProjectTemplate::MathEpisodes) => RoleParallelism::Infinite,
        None => RoleParallelism::Count(1),
    }
}

fn apply_math_episodes(project: &ProjectPaths) -> Result<()> {
    io::write_text(&project.project_md(), MATH_EPISODES_PROJECT_MD)?;
    for channel in ["alerts", "report", "report-single"] {
        git::init_channel(&project.channel_dir(&ChannelSlug::parse(channel)?))?;
    }

    let template_dir = project.root.join("templates").join("math-episodes");
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
    io::write_text_if_missing(&template_dir.join(".gitignore"), MATH_EPISODES_GITIGNORE)?;
    io::write_text_if_missing(&template_dir.join("report.tex"), MATH_EPISODES_REPORT_TEX)?;
    io::write_text_if_missing(
        &template_dir.join("preamble.tex"),
        MATH_EPISODES_PREAMBLE_TEX,
    )?;
    io::write_text_if_missing(
        &template_dir.join("episode-standalone.tex"),
        MATH_EPISODES_EPISODE_STANDALONE_TEX,
    )?;
    io::write_text_if_missing(
        &template_dir.join("episodes").join("README.md"),
        MATH_EPISODES_EPISODES_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("papers").join("README.md"),
        MATH_EPISODES_PAPERS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("scripts").join("README.md"),
        MATH_EPISODES_SCRIPTS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("README.md"),
        MATH_EPISODES_EXPERIMENTS_README,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("Cargo.toml"),
        MATH_EPISODES_EXPERIMENTS_CARGO_TOML,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("src").join("main.rs"),
        MATH_EPISODES_EXPERIMENTS_MAIN_RS,
    )?;
    io::write_text_if_missing(
        &template_dir.join("experiments").join("src").join("lib.rs"),
        MATH_EPISODES_EXPERIMENTS_LIB_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("commands")
            .join("mod.rs"),
        MATH_EPISODES_EXPERIMENTS_COMMANDS_MOD_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("commands")
            .join("smoke.rs"),
        MATH_EPISODES_EXPERIMENTS_SMOKE_RS,
    )?;
    io::write_text_if_missing(
        &template_dir
            .join("experiments")
            .join("src")
            .join("core")
            .join("mod.rs"),
        MATH_EPISODES_EXPERIMENTS_CORE_MOD_RS,
    )?;
    io::write_text_if_missing(&template_dir.join("Makefile"), MATH_EPISODES_MAKEFILE)?;
    create_math_episode_role(project)?;
    create_math_publisher_role(project)?;
    create_math_auditor_role(project)?;
    create_math_supervisor_role(project)?;
    Ok(())
}

fn create_math_episode_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("episode")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), MATH_EPISODES_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(&role_paths.step_path(&step), MATH_EPISODES_STEP_MD)?;
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

fn create_math_publisher_role(project: &ProjectPaths) -> Result<()> {
    let role = RoleSlug::parse("publisher")?;
    let role_paths = RolePaths::new(project.clone(), role);
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text_if_missing(&role_paths.role_md(), MATH_EPISODES_PUBLISHER_ROLE_MD)?;
    let step = StepSlug::parse("publish")?;
    io::write_text_if_missing(
        &role_paths.step_path(&step),
        MATH_EPISODES_PUBLISHER_STEP_MD,
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
    io::write_text_if_missing(&role_paths.role_md(), MATH_EPISODES_SUPERVISOR_ROLE_MD)?;
    let step = StepSlug::parse("work")?;
    io::write_text_if_missing(
        &role_paths.step_path(&step),
        MATH_EPISODES_SUPERVISOR_STEP_MD,
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
    io::write_text_if_missing(&role_paths.role_md(), MATH_EPISODES_AUDITOR_ROLE_MD)?;
    let step = StepSlug::parse("audit")?;
    io::write_text_if_missing(&role_paths.step_path(&step), MATH_EPISODES_AUDITOR_STEP_MD)?;
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

pub const MATH_EPISODES_PROJECT_MD: &str = r#"# Math Episodes Project

This is a research mathematics project organized as persistent exploratory episodes.

This project was created from the `math-episodes` template. The intended workflow is:

- `PROJECT.md` gives the shared research culture and operating rules.
- The `episode` role is the ordinary research role. Each agent created from it is one durable
  episode, and the agent id is the episode slug.
- Every agent has a private writable workspace at `work/own/`. Other agents' workspaces are visible
  through read-only `work/all/<role>/agents/<agent>/` symlinks. There is no shared mutable work tree.
- The `publisher` role is queued and serial. It wakes after each completed episode, copies the prior
  publisher manuscript when one exists, adds the triggering episode by a relative `\input`, checks
  the build, and publishes the finished PDF to the `report` channel.
- The `supervisor` role reads finished work and channel outputs, maintains a high-level map of
  active prongs, and launches new `episode` agents when that is the best next move.
- `channels/report-single` is for standalone episode PDFs. `channels/report` is for the publisher's
  complete manuscript PDF. Channel directories are append-only artifact logs managed by think; agents
  publish by writing to their local `channels/<channel>/` outbox.
- `channels/alerts` is a generic notification channel. The template auditor may publish Markdown
  alerts there when it finds a real operational problem.
- Static TeX and experiment seeds live in `templates/math-episodes/`. Copy them into `work/own/`
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

pub const MATH_EPISODES_ROLE_MD: &str = r#"# Episode

State the research objective precisely. Include the open problem, known subcases, relevant
definitions, allowed references, computational scope, and what durable progress would count as a
successful episode.
"#;

pub const MATH_EPISODES_STEP_MD: &str = r#"# Work

Work in your assigned episode. Develop definitions, examples, computations, conjectures, proof
attempts, counterexamples, literature notes, failed approaches, and open questions as appropriate.

Your agent id from the runtime summary is the episode identity. Write the main TeX episode as
`work/own/episodes/<agent-id>.tex` unless the role explicitly instructs otherwise. Keep using that
same file if this agent loops through multiple runs.

When starting a fresh workspace, copy the useful seed files from `templates/math-episodes/` into
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

pub const MATH_EPISODES_PUBLISHER_ROLE_MD: &str = r#"# Publisher

You maintain the combined manuscript for the math episodes project. This role is deliberately
serial: every publisher agent receives the prior publisher's completed workspace through
`EXPOSED.md` when one exists, integrates exactly the episode that triggered the current run, checks
the TeX build, and publishes the new combined PDF.

You do not do mathematical research and you do not rewrite episode content except for minimal TeX
repairs needed for compilation. Preserve episode order and include paths carefully. Your durable
workspace is `work/own/`; the combined manuscript should live in `work/own/manuscript/`.
"#;

pub const MATH_EPISODES_PUBLISHER_STEP_MD: &str = r#"# Publish

First read `TRIGGER.md`. For the normal trigger, it names the `episode` agent that just finished.
That source episode is the only new episode you should integrate in this run.

Read `EXPOSED.md`. If it names a previous finished publisher and gives a `work/own` directory,
copy that previous `manuscript/` directory into your own `work/own/manuscript/` before making any
changes. If there is no previous publisher manuscript, initialize `work/own/manuscript/` from
`templates/math-episodes/`.

Find the triggering episode source through your refreshed workspace links:

```
work/all/episode/agents/<episode-agent>/episodes/<episode-agent>.tex
```

In `work/own/manuscript/report.tex`, add exactly one relative input for that episode, normally:

```tex
\input{../../all/episode/agents/<episode-agent>/episodes/<episode-agent>}
```

Avoid duplicate includes. If the episode source is missing or malformed, inspect the triggering
agent root and transcript enough to diagnose the problem, make only minimal compile repairs in your
own manuscript workspace when possible, and record the issue in your final reply.

Compile-check from `work/own/manuscript/`, preferably with:

```
make main
```

After a successful build, publish the combined manuscript PDF by copying
`work/own/manuscript/report.pdf` to `channels/report/report.pdf`. Do not publish TeX logs, caches,
or large scratch outputs. Do not edit project channel directories directly.
"#;

pub const MATH_EPISODES_SUPERVISOR_ROLE_MD: &str = r#"# Supervisor

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

Never let more than eight episode agents be running at the same time. Inspect `think list --all`
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

pub const MATH_EPISODES_SUPERVISOR_STEP_MD: &str = r#"# Supervise

First read the trigger context.

If the trigger kind is `role-agent-finished` for `episode`, focus on the newly finished episode.
Read its `work/own/` material through `work/all/episode/agents/<agent>/`, inspect its transcript
and final reply if needed, and compare it against the important previous episodes. Place that
progress in the global project map. Decide whether to continue that prong, fork related prongs, ask
for verification, or pause because enough agents are already running.

If the trigger kind is `manual`, honor the manual reason if present and otherwise perform a compact
supervisor review.

Use `think list --all` to inspect roles and agents. If you launch new episode agents, be specific:
each prompt should name the prong, the immediate mathematical objective, the expected artifacts,
and what would count as useful progress. Do not launch more than eight running episode agents in
total.

Before exiting, update `data/own/supervisor-journal.md` with a compact high-level summary of what
you saw and what you launched. Your journal in `data/own/` is preserved outside the published
channels.
"#;

pub const MATH_EPISODES_AUDITOR_ROLE_MD: &str = r#"# Auditor

You perform a light operational audit for the math-episodes template. This is template-specific
health checking; the core think engine does not know what an episode, publisher, or report means.

Only publish an alert if you find a real actionable problem. Do not publish routine "all clear"
messages. Alerts are Markdown files written to `channels/alerts/`.
"#;

pub const MATH_EPISODES_AUDITOR_STEP_MD: &str = r#"# Audit

Inspect the current project state for basic math-episodes workflow problems:

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

const MATH_EPISODES_REPORT_TEX: &str = r#"\input{preamble}

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

const MATH_EPISODES_PREAMBLE_TEX: &str = concat!(
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

const MATH_EPISODES_EPISODE_STANDALONE_TEX: &str = r#"\input{preamble}

\providecommand{\EpisodeFile}{episodes/ep1}
\providecommand{\EpisodeSectionOffset}{0}

\begin{document}

\setcounter{section}{\EpisodeSectionOffset}
\input{\EpisodeFile}

\end{document}
"#;

const MATH_EPISODES_EPISODES_README: &str = r#"# Episodes

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

const MATH_EPISODES_PAPERS_README: &str = r#"# Papers

Place papers used by the project here. Prefer TeX manuscripts when available. Keep TeX source
alongside PDFs, and save decoded PDF text beside PDFs when that helps search or audit.
"#;

const MATH_EPISODES_SCRIPTS_README: &str = r#"# Scripts

Place reusable or report-worthy computation scripts here. Large generated outputs should usually go
in the project-managed agent data directory instead of published channels.
"#;

const MATH_EPISODES_GITIGNORE: &str = r#"build/
*.aux
*.fdb_latexmk
*.fls
*.log
*.out
*.toc
experiments/target/
"#;

const MATH_EPISODES_EXPERIMENTS_README: &str = r#"# Experiments

This is a seed Rust crate for serious numerical experiments in an episode workspace.

Each episode should add one or more subcommands it controls, usually in
`src/commands/episode_<agent-id>.rs`, and should touch `src/commands/mod.rs` only enough to expose
those commands through Clap. Keep the dispatcher minimal so parallel agents rarely conflict there.

Move reusable algorithms, data structures, parsers, enumerators, and performance-critical kernels
into `src/core/` rather than duplicating them across episode commands. Optimize shared core code
aggressively, keep command-line interfaces deterministic, and record exact cargo invocations in the
episode TeX file. Commit `Cargo.lock` whenever Cargo creates or updates it.
"#;

const MATH_EPISODES_EXPERIMENTS_CARGO_TOML: &str = r#"[package]
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

const MATH_EPISODES_EXPERIMENTS_MAIN_RS: &str = r#"use clap::Parser;
use work_experiments::Cli;

fn main() -> anyhow::Result<()> {
    Cli::parse().run()
}
"#;

const MATH_EPISODES_EXPERIMENTS_LIB_RS: &str = r#"pub mod commands;
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

const MATH_EPISODES_EXPERIMENTS_COMMANDS_MOD_RS: &str = r#"mod smoke;

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

const MATH_EPISODES_EXPERIMENTS_SMOKE_RS: &str = r#"#[derive(clap::Args)]
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

const MATH_EPISODES_EXPERIMENTS_CORE_MOD_RS: &str = r#""#;

const MATH_EPISODES_MAKEFILE: &str = r#"PDFLATEX ?= pdflatex
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
