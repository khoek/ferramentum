# kai-tool

Kai launches Codex and resumes conversations with configurable credential rotation.

## Usage

```text
kai                         Launch Codex
kai resume                  Open the all-sessions picker
kai resume ID               Resume a specific conversation
kai status                  List local Kai windows and their latest turn state (Linux/macOS)
kai status --watch          Refresh the window dashboard every two seconds
kai llm-get PATH...          Assemble a source listing
```

Unambiguous command prefixes work, such as `kai r` and `kai llm`.

`--fast` selects the Fast service tier. Custom builds with a `-k.` prerelease suffix followed
by exactly eight hexadecimal commit characters (for example, `0.154.0-k.ac192cd7`) are supervised
automatically. `--no-auto-restart` disables supervision. Kai runs Codex with its approval/sandbox bypass flag.
With a custom Codex build that reports `stream-disconnected` recovery, the terminal error
`stream disconnected before completion: An error occurred while processing your request.`
also resumes the conversation after Codex finishes retrying, restoring pending input. If a
credential provider is configured, recovery calls `acquire` without reporting a credential failure.
Supervised restart conditions apply to the primary conversation; errors from spawned agents do not
restart the instance. The custom Codex build enforces this before requesting shutdown.
On Linux, Kai best-effort sets each Codex process's `oom_score_adj` to `+800` when procfs permits it.

Every launch and automatic recovery passes Codex config overrides for 16 concurrent spawned agents,
the `monokai-extended` theme, disabled decorative whimsy, colored status indicators, the saved
session's working directory on resume, and hidden rate-limit model-switch reminders. The status
line shows model/reasoning, run
state, remaining context, weekly limit, total input/output tokens, and Fast-mode status, in that order.

Install with `cargo install --path kai-tool --locked --force`.

## Window status

`kai status --watch` shows the current user's local Kai launcher processes, including windows
opened before the dashboard started. Its Ratatui interface follows the `kai r` session picker:
blue selection, a `›` marker, subdued metadata, type-to-search, and keyboard hints below the list.
Rows show thread names and **Elapsed**, a **Subagents** column from 50 columns wide,
and **Tokens** from 70 columns. From 90 columns, **State** includes a relative time:
`Ready · 3m ago`, `Needs input · 3m ago`, or `Interrupted · 3m ago`.
Ready, Interrupted, and Error use the latest turn ending; Needs input measures the continuous
wait since the first pending blocking question. Active and Unknown have no age. Missing timestamps
are omitted. Narrower terminals show only the state, with its age in comfortable rows and Ctrl-E
details. The previous turn ending is retained in details, including while a new turn is active.
Elapsed is time for the current turn, or the final duration of the most recent ended turn;
it is not the age of the whole session.
Durations and ages use one whole unit, rounded down: minutes, hours, days, weeks, months, then
years (`59m`, `3h`, `4d`, `2w`, `1mo`, `1y`). Anything under a minute shows `<1m`;
seconds and mixed units are omitted. Months use 30 days and years use 365 days.
Names come from Codex's `session_index.jsonl`, including subsequent renames.

Newly completed main turns gain a **●** before the thread name, bold yellow emphasis on the title
and completion age, and a subtle tint across the row until their window is seen. Blue selection
takes precedence over the tint; the marker and bold emphasis remain visible without color and
on the selected row.
Focusing the actual session clears it, including switching through the desktop rather than Kai;
successful Enter-to-focus also clears it. Results observed while their session is focused are
already seen. Merely selecting, searching for, or opening details for a dashboard row does not
acknowledge it. Ctrl-R marks the selected displayed completion read manually. New work and
closed windows clear the marker; a ready parent can be unread while subagents continue running.

Unread tracking belongs to the interactive dashboard and starts when it opens. Already-ready
sessions establish a baseline rather than being labelled unread without an interaction history.
Thread switches and reused process IDs establish new baselines. Markers have no time-based
expiry; temporary discovery failures preserve them until the state can be read again. Terminal
titles, plain output, and JSON output are unchanged.

Focus is sampled by a separate worker approximately every half-second, independently of the
session refresh interval. Very brief visits between samples can be missed; Ctrl-R is also useful
on unsupported desktops or terminal arrangements. Sway, Hyprland, X11, and the updated GNOME
companion extension use the same unambiguous terminal matching as window switching. macOS reads
the foreground Terminal/iTerm2 tab's tty using AppleScript and its existing Automation permission.
Focus-query errors appear only in Ctrl-E details; stalled helpers cannot block the dashboard.
GNOME users must update the extension for its read-only `GetWindows` method. The feature adds no
background service or persistent interaction history.

Subagents shows the number of subagents with an active recorded turn (for example, `2 active`),
including nested descendants.
It follows currently open subagent logs, using root/parent thread IDs to associate
them with the main conversation and excluding inherited parent history where marked. The main
State and Elapsed remain those of the parent; a Ready parent can still have running agents.
The Active filter includes either kind of running work. Ctrl-E shows running, ready, interrupted,
error, and unknown agent counts. A `?` marks unavailable or uncertain counts; it does not mean zero.
Below 50 columns, agent counts remain available in Ctrl-E details. Subagent logs that close leave
the counts on the next refresh. This does not keep historical agent totals or add an input flow:
the current agent runtime restricts user-input requests to the root conversation.

Tokens shows the main thread's latest reported cumulative input-plus-output total, abbreviated
with K/M/B/T suffixes. Ctrl-E provides exact input, output, cached-input, cache-write, and reasoning
counts, including on narrow terminals. Cached input is included in input; reasoning is included
in output. The column does not add subagent usage or measure current context occupancy. It uses
matching per-thread usage records (including compaction checkpoints), falling back to the legacy
`token_count` total when per-thread records are unavailable. Counters replace previous snapshots
and persist across turns, so refreshes and duplicate records cannot double count. `—` means usage
is unavailable, while `0` means a reported zero. Counts update when usage is written to the log.

Use ↑/↓, Page Up/Down, or Home/End to browse; ←/→ switches between All and Active. Closed windows
are removed on the next refresh that confirms their process has exited. Type to filter
by name, state, terminal, directory, PID, or conversation ID. Ctrl-O toggles dense/comfortable
rows, and Ctrl-E opens details with the selected row's terminal, directory, PID, last turn ending,
full conversation ID, and the reason for an Unknown state. Details also expose discovery and
focus errors, including when no rows are available; Page Up/Down scrolls this pane while open.
Enter focuses the selected session's existing terminal window. The dashboard stays open.
Esc cancels pending focus, dismisses a message, clears a search, then quits; Ctrl-C quits. Plain letters,
including `q`, belong to search, matching the resume picker. Selection follows the same window
across live refreshes. `--interval SECONDS` sets the refresh rate (1–3600 seconds).

Discovery and activation use separate workers, keeping search, navigation, and quitting responsive
while desktop helpers are slow. Repeated Enter presses cannot queue delayed activations. Cancellation
stops subsequent helper calls and kills/reaps a running helper's process group; a focus request
already delivered to the desktop cannot be undone. Each discovery attempt has a five-second deadline.
A failed or timed-out refresh preserves the previous snapshot with a **Stale** marker and its actual
error under Ctrl-E; the next successful refresh clears the marker automatically. If a filesystem
read remains blocked, no replacement threads or refreshes are queued: retry waits for that read to
return, and its late result is discarded. Quitting restores the terminal and stops running helpers
without waiting for a blocked worker. Helper cleanup waits at most 50 ms before handing any
kernel-blocked child to a background reaper. Plain and JSON modes return an error on discovery timeout.

Development status: **beta**. This designation is kept in documentation and source, not in the
dashboard. Switching failures appear only after pressing Enter, with a suggested next step;
Ctrl-E exposes terminal details for manual switching.

Focus support requires both a desktop backend and a supported terminal arrangement:

| Desktop | Terminal arrangement | Backend / requirements | Validation |
| --- | --- | --- | --- |
| Sway | Separate Foot or Alacritty windows; xterm/st/urxvt via XWayland | Exact container ID through `swaymsg` | Foot tested in an isolated headless compositor, including stalled-helper cancellation. Other terminals await native testing. |
| Hyprland | Separate Foot or Alacritty windows; xterm/st/urxvt via XWayland | Exact address through `hyprctl`, Lua and classic dispatch | Fixture tests; native testing pending. |
| X11, including GNOME X11 and i3 | Separate Alacritty/xterm/st/urxvt windows | EWMH activation through `wmctrl`, confirmed with `xprop`; WM must honor activation | Fixture tests; native testing pending. |
| GNOME Wayland | Separate Foot or Alacritty windows; xterm/st/urxvt via XWayland | [Companion extension](integrations/gnome/README.md) and `gdbus` | Activation fixtures and isolated D-Bus smoke test; native desktop testing pending. **GNOME Terminal and Console are not supported.** |
| macOS | Terminal or iTerm2, including tabs/panes, without a multiplexer | AppleScript matches the exact tty; `lsof` discovery; may require Automation permission | Apple Silicon compile checks and parser fixtures; native testing pending. |
| Windows | Not available | Existing `capulus` dependency fails Windows compilation on Unix APIs | Whole-project port remains separate. |

Linux activation currently supports Foot, Alacritty, xterm, st, and urxvt. Shared-server windows,
GNOME Terminal/Console, Kitty, WezTerm, Konsole, and multiplexers such as tmux/screen/Zellij need
terminal-specific tab/pane integrations. Kai refuses ambiguous targets instead of guessing.
It validates process birth times before activation and passes numeric window IDs to helpers;
thread names and window titles are never interpreted as commands or matching expressions.
Other desktops can still show the Linux dashboard and receive an actionable message on Enter.
Terminal adapters are a separate extension point from desktop backends. For example, a Kitty
adapter would need an explicitly enabled [remote-control socket](https://sw.kovidgoyal.net/kitty/remote-control/#remote-control-via-a-socket),
exact pane identification, and activation verification. Merely adding its process name to the
supported list would not reliably select a tab. Additional terminal adapters remain future work.

Native Sway verification is opt-in and uses its own headless compositor:
`cargo test -p kai-tool --test status_focus_sway -- --ignored`.
GNOME extension fixture tests: `node --test kai-tool/integrations/gnome/focus.test.js`.
The asynchronous D-Bus smoke test runs in a private bus without changing the desktop:
`GIO_USE_VFS=local dbus-run-session -- gjs -m kai-tool/integrations/gnome/dbus-smoke.js`.
It requires GJS and D-Bus; the Sway tests require Sway, swaymsg, and Foot.
If a missing sibling dependency prevents Cargo from loading the workspace, use
`bash scripts/check-cli.sh kai-tool test --test status_focus_sway -- --ignored`;
see the root [development checks](../README.md#development-checks) for the normal
Rust tests, formatting, and Clippy commands.

| State | Meaning |
| --- | --- |
| Active | The main conversation has a recorded turn start without a matching end. This does not assert CPU activity or rule out an unrecorded approval prompt. |
| Ready | The latest recorded turn completed successfully; the window remains open. |
| Needs input | A synchronous `request_user_input` call is awaiting its matching result. |
| Interrupted | The latest recorded turn was aborted. |
| Error | The latest recorded turn completion contains an error. |
| Unknown | There is insufficient evidence to identify the main conversation or its state. |

The State completion age refers to a turn ending, including interruptions and errors; it does not mean
the overall task was accomplished. Only open windows are listed; closed-window history is not kept.
If a process cannot be inspected, it remains Unknown until a refresh can confirm its state or exit.

Discovery uses Linux `/proc` or macOS `libproc` plus `lsof`. It reads same-user Kai/Codex process
metadata and the session logs held open by a direct Codex child. It does not require a Codex binary on PATH,
call a credential provider, start a daemon, or make model/API requests. It works with the custom
Codex build's embedded server; detached servers, remote machines, and processes hidden by OS
permissions may be unavailable. macOS file discovery is batched once per refresh, with bounded
output and a three-second timeout. If `lsof` exits with status 1 while returning valid records,
those records are retained and only sessions with missing process records become Unknown. A helper
timeout or unusable output affects every session in that batch. Subagent activity is counted separately. Multiple open main conversations in
one process are reported as Unknown because the log does not identify the visible conversation.

State follows persisted Codex `task_started`/`task_complete`/`turn_aborted` events (also accepting
`turn_started`/`turn_complete`), so updates can lag buffered log writes. It does not detect every
approval prompt or infer progress from CPU use. Async questions do not imply the agent is blocked.
Unknown is also used during startup/recovery or when logs are missing, unreadable, malformed, or
from an unsupported format. Reads are incremental and bounded to the most recent 16 MiB on startup
or when catching up. Records over the 1 MiB line-buffer threshold are parsed as a stream, retaining
only observation metadata: large compaction records and tool output preserve the current state,
and large lifecycle records still update it. Partial appends are retried. Malformed records,
records exceeding the remaining refresh read budget, or missing lifecycle history make state Unknown
until a later lifecycle event provides evidence. Prompt text, tool arguments, responses, and error
messages are not included in output. LLM progress summaries are a separate feature.

For scripts, `kai status --json` emits one snapshot; `kai status --watch --json` emits one JSON
object per line, including when output is piped. Snapshot `version` is `1`, `observed_at` is Unix
seconds, and `windows` contains `pid`, `tty`, `cwd`, `thread_id`, `thread_name`, `run_time_ms`,
`state`, `agents`, `token_usage`, `last_finished_at`, `exited_at`, and `detail`. Unavailable values are null, timestamps
are Unix seconds, run time is milliseconds, and state names
are `working`, `ready`, `needs_input`, `interrupted`, `error`, or `unknown`. `warnings`
reports incomplete process discovery. No terminal escape sequences are emitted in JSON mode.
The existing JSON names `working`, `run_time_ms`, and `last_finished_at` remain unchanged;
they correspond to the UI's Active, Elapsed, and turn-ending timestamp. UI filters do not remove rows
from JSON output. The legacy `exited_at` field remains present as null for version 1 compatibility;
exited rows are no longer emitted.
The additive `agents` object contains `total`, `running`, `ready`, `interrupted`, `error`,
`unknown`, and `complete`. Counts cover attributable open subagent logs; `complete: false` means
some could not be reliably associated with this root. Unknown states have a separate count, so
`running: 0` does not assert that all work ended when `unknown` is nonzero or `complete` is false.
`agents: null` means discovery could not establish the main session or its open logs.
The additive `token_usage` object contains exact `total_tokens`, `input_tokens`, `output_tokens`,
`cached_input_tokens`, `cache_write_input_tokens`, and `reasoning_output_tokens` for the main thread.
It is null when no valid usage is available; it does not change the main state or active-agent count.

## Credential provider

Set `${XDG_CONFIG_HOME:-~/.config}/kai/config.toml`:

```toml
credential_provider = "/path/to/provider.sh"
```

`--credential-provider SCRIPT` overrides that setting. Configuration-relative paths resolve
from the configuration directory; CLI paths resolve from the current directory.

The script has two operations:

```text
bash SCRIPT acquire --codex-home PATH --sqlite-home PATH

bash SCRIPT next --codex-home PATH --sqlite-home PATH \
  --auth-file PATH --credential-use-lock PATH \
  --credential-use-lock-mode shared|exclusive \
  --credential-mutation-lock PATH --available-file PATH \
  --cause quota-exhausted|credential-invalid|model-capacity [--unavailable-until UNIX_SECONDS]
```

Each returns exactly one JSON object:

```json
{
  "auth_file": "/pool/credential/auth.json",
  "credential_use_lock": "/pool/credential/use.lock",
  "credential_use_lock_mode": "shared",
  "credential_mutation_lock": "/pool/credential/mutation.lock",
  "available_file": "/pool/credential/available"
}
```

The provider chooses the credential and its shared/exclusive use-lock mode. Kai locks the returned
file accordingly, verifies availability, and transfers the held descriptor to Codex. Consumers
retain that lock while using the credential and serialize token changes with the mutation lock,
reloading the shared auth file after locking it.

Paths must be distinct and absolute, with private regular files owned by the user and mode 0600.
The provider owns the files. Removing the availability marker prevents new selections. The two
home arguments are opaque launch context.

Quota exhaustion, permanent credential failure, or model capacity invokes `next`. Kai forwards the reported
reset timestamp, then restores Codex's input handoff and resumes the conversation. Normal exit and
crashes invoke no hook; the operating system releases held locks.

Script stdin is closed, stdout carries the JSON response, and stderr remains visible. Ambient
Codex/OpenAI auth variables are removed. Responses are limited to 64 KiB; selection has a
30-second timeout. Managed Codex uses credential protocol version 2 and transfers its descriptor
through `SCM_RIGHTS` with the private, nonce-authenticated READY/GO startup handshake.

Hooks run only during supervision of these custom builds. Otherwise Codex uses ordinary authentication.
With supervision enabled and no provider, Kai aborts if credential rotation becomes necessary.

## Source listings

`kai llm-get` recursively selects source files, prepends local `AGENTS.md` and `DESIGN.md`,
and copies the listing to the clipboard. Use `--out -` for stdout, `--out PATH` for a file,
or `--slim` to omit instruction files. Run `kai llm-get --help` for filtering options.

## License

AGPL-3.0-only
