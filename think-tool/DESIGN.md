# think-tool Design

`think` coordinates long-running agent work without giving agents shared mutable repositories.
Agents get private durable workspaces, read-only visibility into other agents' work, and append-only
publication channels for selected artifacts.

## Goals

- Keep each agent's writable state isolated.
- Preserve enough workspace and data history for future agents to inspect prior work.
- Publish final artifacts without committing complete generated outputs into agent workspaces.
- Make queued synchronous roles explicit through trigger queues, not through branch merge queues.
- Keep project state recoverable from ordinary files.

## Core Objects

- Project: a directory containing `think.toml`, `PROJECT.md`, roles, agents, channels, data, and
  runtime state.
- Role: a configured agent type under `roles/<role>/`.
- Agent: one durable role instance under `roles/<role>/agents/<agent>/`.
- Workspace: the agent's writable `work/own/` directory.
- Workspace view: `work/all/<role>/agents/<agent>/` symlinks to other agents' workspaces.
- Data: `data/own/` and `data/all/`, using the same own/all visibility model for large preserved
  data that should not be published.
- Channel: an append-only project artifact log under `channels/<channel>/`, backed by git and
  managed automatically by think.
- Channel outbox: an agent-local `channels/<channel>/` directory. Agents publish by writing files
  there before exiting successfully.

## Project Layout

```text
PROJECT/
  think.toml
  PROJECT.md
  channels/
    <channel>/
      .git/
      .think-channel
      <published artifacts>
  data/
    roles/<role>/agents/<agent>/
  roles/
    <role>/
      ROLE.md
      config.toml
      steps/<step>.md
      agents/<agent>/
        PROMPT.md
        AGENT_PROMPT.md
        TRIGGER.md
        EXPOSED.md
        manifest.toml
        agent.toml
        orchestrator.toml
        work/
          own/
          all/<role>/agents/<agent> -> ...
        data/
          own -> PROJECT/data/roles/<role>/agents/<agent>
          all/<role>/agents/<agent> -> ...
        channels/<channel>/
        runs/<n>/
          PROMPT.md
          STEP.md
          TRANSCRIPT.txt
          REPLY.md
          exit.toml
  runtime/
    trigger-queues/
    locks/
      agents.lock
      channels/<channel>.lock
      trigger-queues/<queue>.lock
      orchestrators/<role>/<agent>.lock
    notices/
```

There is intentionally no `repos/` directory and no project-level `published/` directory. Channels
are the publication mechanism.

## Config

`think.toml`:

```toml
version = 1
default_backend = "codex"
default_role = "episode"
channels = ["report", "report-single"]

[providers.codex]
model = "gpt-5"
thinking_level = "high"
```

Role config:

```toml
version = 1
status = "active"
backend = "codex"
mode = "oneshot"
parallel = 1
agent_names = "sequential"
agent_prefix = "pub"
auto_archive = true
expose = ["last-agent-finished"]
steps = ["publish"]

[[triggers]]
kind = "role-agent-finished"
role = "episode"
launch = "queued"
queue = "publisher"
```

`expose` is role-local predecessor state made available to newly launched agents in `EXPOSED.md`.
Supported values:

- `last-agent-finished`: the latest successful done agent of the same role.
- `last-agent-started`: the latest created agent of the same role.

This is separate from launch provenance. Triggered agents always receive `TRIGGER.md` describing why
they were created.

## Lifecycle

1. `think agent new` allocates an agent id and creates the agent directory.
2. `work/own`, `work/all`, `data/own`, `data/all`, and channel outboxes are prepared.
3. If the agent was trigger-launched, think writes `TRIGGER.md`.
4. If the role has `expose`, think writes `EXPOSED.md`.
5. The assembled prompt includes project, role, runtime paths, trigger context, exposed state, data
   and workspace rules, and the current step.
6. The orchestrator runs the backend, records transcripts, and writes run exit state.
7. After a successful run, think publishes nonempty channel outboxes.
8. Repeatable agents advance or stop according to `manifest.toml` disposition.
9. Terminal successful agents fire configured downstream triggers.

Before each run, think refreshes `work/all` and `data/all` symlinks so resumed agents can see newer
prior work.

## Channel Publication

Each configured channel has a project directory `channels/<channel>/` initialized as a git
repository by think. Agents never operate on that repository directly. They write artifacts into
their own outbox:

```text
roles/<role>/agents/<agent>/channels/<channel>/
```

On successful run finalization, think locks the channel, copies each top-level outbox entry to:

```text
<role>-<agent>-<run>-<top-level-name>/<remaining-path-if-any>
```

Examples:

- outbox `report.pdf` from `publisher/pub3` run 1 becomes
  `publisher-pub3-1-report.pdf`.
- outbox directory `tables/raw/a.csv` from `episode/ep7` run 2 becomes
  `episode-ep7-2-tables/raw/a.csv`.

Think refuses to publish symlinks. If the destination already exists with identical contents, the
publish is idempotent. If it exists with different contents, finalization fails. After a successful
copy, think commits the channel update automatically and clears the outbox.

## Triggers

Supported trigger kinds:

- `role-step-finished`
- `role-agent-finished`
- `queue-idle`
- `elapsed`
- manual triggers from the CLI

Launch modes:

- `async`: start immediately when capacity allows.
- `queued`: append to a named trigger queue guarded by `runtime/locks/trigger-queues/<queue>.lock`.

`role-agent-finished` fires after successful run finalization and channel publication. This lets
downstream agents inspect published artifacts and source work without racing incomplete outboxes.

`queue-idle` observes named trigger queues. It does not observe channel repositories or hidden merge
queues.

## Agent Prompt Contract

Agents are told:

- Write durable work in `work/own/`.
- Treat `work/all/` and `data/all/` as read-only views of other agents.
- Put large preserved data in `data/own/`.
- Publish only selected final artifacts through `channels/<channel>/`.
- Do not assume a think-managed git repository exists.
- Write `manifest.toml` and the run `REPLY.md` before exiting.

## TUI

The dashboard surfaces:

- roles and agents, including exposed-state config;
- channel artifact counts and latest artifact names;
- trigger queues and active queue locks;
- project events derived from agents, run exits, trigger queues, notices, and updates;
- Codex quota and provider state;
- focused role/agent detail, including channel outboxes and latest replies.

The queue tab is for trigger queues only. The timeline has lanes for agents, runs, triggers, and
notices.

## Math Episodes Template

The template creates:

- channels `report` and `report-single`;
- role `episode`, active repeatable, sequential `ep` ids;
- role `publisher`, active oneshot, serial queued, `expose = ["last-agent-finished"]`;
- role `supervisor`, active oneshot, queued after publisher completion;
- static seeds under `templates/math-episodes/`.

Episode agents write `work/own/episodes/<agent>.tex`, build a standalone PDF from their private
workspace, and publish it to `channels/report-single/<agent>.pdf`.

Publisher agents read `TRIGGER.md` to identify the finished episode. They read `EXPOSED.md` to find
the previous publisher. If one exists, they copy its `work/own/manuscript/`; otherwise they seed a
new manuscript from `templates/math-episodes/`. They add the triggering episode through a relative
input such as:

```tex
\input{../../all/episode/agents/ep7/episodes/ep7}
```

Then they run a TeX build and publish `work/own/manuscript/report.pdf` to `channels/report/report.pdf`.

## Recovery

The project should remain recoverable by editing TOML and files directly. Runtime locks are plain
files. Channel logs are ordinary git repositories, but only think should mutate them during normal
operation. Agent workspaces and data directories are ordinary directories retained after restarts.
