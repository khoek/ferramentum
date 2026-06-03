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

There are no think-managed working repositories, branch merges, or merge agents. Agents work in
their own workspace and publish selected outputs through channels. Think copies each successful
run's outbox entries into the project channel using:

```text
<role>-<agent>-<run>-<top-level-name>/<remaining-path-if-any>
```

Publishing is append-only. If a destination already exists with identical bytes, think treats it as
idempotent; if it differs, the run finalization fails.

## Templates

```bash
think project new ./math-project --template math-episodes
```

The `math-episodes` template configures channels `alerts`, `report`, and `report-single`, plus
roles `episode`, `publisher`, `supervisor`, and `auditor`. Episodes write TeX and computations in
`work/own/` and publish standalone PDFs to `report-single`. The queued serial publisher wakes after
each finished episode, copies the previous publisher manuscript when exposed, adds the triggering
episode through a relative `\input`, compile-checks, and publishes the combined PDF to `report`.
The supervisor starts paused; after activation, it wakes after each finished episode and can launch
follow-up episodes without waiting on publication. The auditor wakes periodically and publishes
Markdown alerts only for actionable health issues.

## Commands

- `think`
- `think more [--new] [agent|role/agent] [--query QUERY]`
- `think status [role] [--all]`
- `think open`
- `think fix [QUERY]`
- `think assist [QUERY]`
- `think project new <path> [--template math-episodes|--no-template]`
- `think project init [--template math-episodes|--no-template]`
- `think help --all`

Advanced commands:

- `think agent new [role] [--prompt TEXT] [--no-prompt] [--attach]`
- `think role new [slug] [--active] [--parallel N|infinite] [--expose NAME]`
- `think role draft [slug] [--request TEXT] [--feedback TEXT] [--no-review] [--active]`
- `think role activate <role>`
- `think agent attach [role|agent|role/agent]`
- `think agent archive [agent|role/agent]`
- `think agent stop [agent|role/agent]`
- `think agent resume [agent|role/agent]`
- `think list [role] [--all]`
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
full-screen dashboard, and in noninteractive output it prints the same plain status report.

When `project new` or `project init` selects a template in an interactive terminal, think offers an
optional setup prompt so Codex can tailor `PROJECT.md`, role prompts, role configs, channel lists,
and template files before any agents start.

`think more` resumes the exact Codex session id recovered from prior transcripts when possible, or
starts a fresh Codex run with a continuity prompt that points at the same workspace, data links,
channel outboxes, manifest, prior prompts, and transcripts. When run interactively without an agent
selector, its picker includes a first-class `new agent` option at the top; `think more --new` skips
the picker and creates a new default-role agent directly.

`think status` opens the full-screen dashboard. It shows role and agent state, trigger queues,
channel summaries, durable project events, notices, and Codex quota gauges. Press `o` for trigger
queue detail, `Tab` for the event timeline, `Enter` for focused role/agent detail, `:` for the
command palette, and `A` for advanced actions.

Role triggers support step completion, terminal agent completion, named queue-idle events, repeated
elapsed-time events, and manual triggers. Queued triggers use named locks under
`runtime/locks/trigger-queues/`. Every triggered agent receives a standard generated `TRIGGER.md`.

## License

AGPL-3.0-only
