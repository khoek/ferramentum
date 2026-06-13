# think-tool

CLI for coordinating persistent agent sessions on complex projects.

## Install

```bash
cargo install think-tool
```

Installed command: `think`

## Design

See [DESIGN.md](https://github.com/khoek/ferramentum/blob/master/think-tool/DESIGN.md).

## Quick Start

```bash
think project new ./my-project --no-template
cd ./my-project
think role new prove-lemma
think agent new prove-lemma
think status
think agent attach prove-lemma/1
```

## Project Model

- `PROJECT.md` contains shared context visible to every agent.
- `think.toml` stores project config, default backend, UI settings, provider settings, and channel names.
- `roles/<slug>/ROLE.md` defines one role objective.
- `roles/<slug>/config.toml` controls mode, parallelism, steps, triggers, and exposed predecessor state.
- `roles/<slug>/steps/*.md` contains ordered per-run prompts.
- `roles/<slug>/agents/<agent>/work/own/` is that agent's writable durable workspace.
- `roles/<slug>/agents/<agent>/work/all/` contains symlinks to other agents' `work/own/` directories.
- `roles/<slug>/agents/<agent>/channels/<channel>/` is the agent's publish outbox for that channel.
- `channels/<channel>/` is a think-managed append-only artifact log backed by git.
- `data/roles/<role>/agents/<agent>/own/` is large agent-local preserved data that should not be published.
- `data/roles/<role>/agents/<agent>/all/` contains symlinks to other agents' data `own/` directories.
- `TRIGGER.md` records why a triggered agent was launched.
- `EXPOSED.md` records configured role-local predecessor state such as `last-agent-finished`.
- `manifest.toml` is written by the agent with `role_summary`, and repeatable roles also write
  `disposition = "continue"` or `"stop"`.
- `runs/<n>/REPLY.md` is the agent's compact final reply for that run.

The core engine has no built-in working repository or branch-merge semantics. Agents work in their
own workspace and publish selected outputs through channels. Templates such as `episodes-code` may
prompt agents to create git worktrees and merge roles, but those are ordinary project roles rather
than special runtime primitives. Think copies each successful run's outbox entries into the project
channel using:

```text
<role>-<agent>-<run>-<top-level-name>/<remaining-path-if-any>
```

Publishing is append-only. If a destination already exists with identical bytes, think treats it as
idempotent; if it differs, the run finalization fails.

## Templates

```bash
think project new ./math-project --template episodes-math
think project new ./code-project --template episodes-code
```

The `episodes-math` template configures channels `alerts`, `report`, and `report-single`, plus
roles `episode`, `publisher`, `supervisor`, and `auditor`. Episodes write TeX and computations in
`work/own/` and publish standalone PDFs to `report-single`. The queued serial publisher wakes after
each finished episode, copies the previous publisher manuscript when exposed, includes every
terminal episode agent with a usable TeX source exactly once in episode order, compile-checks, and
publishes the combined PDF to `report`.
The supervisor starts paused; after activation, it wakes after each finished episode and can launch
follow-up episodes without waiting on publication. The auditor wakes periodically and publishes
Markdown alerts only for actionable health issues.

The `episodes-code` template configures channels `alerts`, `branches`, and `merges`, plus roles
`episode`, `merger`, `supervisor`, and `auditor`. The target source repository lives in project-root
`repo/`; setup should clone a repo URL or copy a local directory there. Episodes create private git
worktrees in `work/own/repo`, implement one focused local branch named after the agent, commit a
clean worktree, and publish structured branch handoffs to `branches`. Nothing pushes. The queued
serial merger integrates finished branches either into local `master` or into new committed
consolidation branches, singly or as compatible tranches, with risk-appropriate checks and
structured merge handoffs. Merger agents reuse their own worktree on resume and publish
`already-integrated` handoffs instead of re-merging source heads already reachable from the target.
Consolidation branches are immutable outputs: later mergers consume them as source branches instead
of extending them in place. The supervisor starts paused; after activation, it reads the project
design and repository docs, launches orthogonal implementation episodes, balances task size against
merge throughput, and can launch tranche prompts to the `merger` role for compatible backlog groups.
The auditor wakes periodically and publishes Markdown alerts only for actionable branch, worktree, or
merge-queue problems.

## Commands

- `think`
- `think more [--new] [agent|role/agent] [--query QUERY]`
- `think status [role] [--all] [--plain]`
- `think open`
- `think assist [QUERY]`
- `think project new <path> [--template episodes-math|episodes-code|--no-template]`
- `think project init [--template episodes-math|episodes-code|--no-template]`
- `think help --all`

Advanced commands:

- `think agent new [role] [--prompt TEXT] [--no-prompt] [--attach]`
- `think role new [slug] [--active] [--parallel N|infinite] [--display-priority N] [--expose NAME]`
- `think role draft [slug] [--request TEXT] [--feedback TEXT] [--no-review] [--active]`
- `think role activate <role>`
- `think agent attach [role|agent|role/agent]`
- `think agent archive [agent|role/agent]`
- `think agent stop [agent|role/agent]`
- `think agent resume [agent|role/agent]`
- `think advanced retry-errored`
- `think advanced trigger [role] [--reason TEXT] [--async-launch]`
- `think advanced provider codex login [account]`
- `think advanced provider codex list`
- `think advanced provider codex use [account]`
- `think advanced provider codex config [--model MODEL] [--thinking low|medium|high|xhigh]`
- `think channel new [slug]`
- `think channel list`

Unambiguous subcommand prefixes are accepted at every level, so `think sta`, `think proj n`, and
`think adv prov cod c` are equivalent to their full subcommand spellings.

`think` with no subcommand is shorthand for `think status`: in an interactive terminal it opens the
full-screen dashboard, and in noninteractive output it prints the same plain status report. Use
`think status --plain` to force plain output in an interactive terminal.

When `project new` or `project init` selects a template in an interactive terminal, think offers an
optional setup brief and then opens an attached live app-server view. The view follows setup progress,
shows the transcript as it is written, and lets you send follow-up questions or commands before
finishing. The setup turn may tailor `PROJECT.md`, role prompts, role configs, channel lists, and template
files before any agents start. After initialization finishes, the next-action menu offers
`open dashboard (recommended)`, Schema review, or finish. Schema renders `think.toml`, provider
settings, role contracts, steps, triggers, and prompt-file presence as structured dashboard data
instead of raw file text.

`think more` continues the agent through its persisted app-server thread state, or starts a
new app-server thread with a continuity prompt that points at the same workspace, data links,
channel outboxes, manifest, prior prompts, and transcripts. If the agent is already running, the
query is sent as a live steer and the agent keeps going. When run interactively without an agent
selector, its picker includes a first-class `new agent` option at the top; `think more --new` skips
the picker and creates a new default-role agent directly.

`think status` opens the full-screen dashboard. It shows a health strip, Schema, role and agent
state, trigger queues, channel summaries, durable project events, non-agent command Conversations,
notices, and provider quota gauges. Press `s` for Schema, `o` for trigger queue detail, `Tab` to
switch tabs, `C` for Conversations, `Enter` for focused detail, `:` for the command palette, and
`A` for advanced actions. Wide terminals show preview panes in Conversations. Dashboard follow-up
composers support `Ctrl-D` to send/steer and `Ctrl-A` to steer and attach when the selected agent
is running. Attached transcript views support `m` for live steer/follow-up, `o` to open the current
run directory, `y`/`Y` to copy transcript/reply paths, and `q` to detach while a turn keeps
running.

Conversations is the dashboard history/debug surface for non-agent command conversations stored
under `runtime/commands/`.

Role triggers support step completion, terminal agent completion, named queue-idle events, repeated
elapsed-time events, and manual triggers. Queued triggers use named locks under
`runtime/locks/trigger-queues/`. Every triggered agent receives a standard generated `TRIGGER.md`.

## License

AGPL-3.0-only
