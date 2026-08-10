# kai-tool

`kai` is a focused CLI for AI-assisted coding workflows: launching agents, managing git worktrees,
assembling source listings, and rotating between enrolled Codex CLI accounts.

## Install

```bash
cargo install kai-tool
```

Installed command: `kai`

## Codex credentials

Enroll each Codex account once, then switch without logging the previous account out:

```bash
kai cred add personal@example.com
kai cred add work@example.com
kai cred list
kai cred tickle
kai cred fix
kai next
```

`kai next` is shorthand for `kai cred next`. It checks candidate accounts concurrently in cyclic
enrollment order and activates the first account with confirmed remaining quota. If none has
remaining quota, accounts with usable rate-limit reset credits are eligible before accounts whose
quota could not be checked. Exhausted accounts without reset credits and credentials rejected by
the service are skipped. You can also select or remove an account explicitly:

```bash
kai cred activate personal@example.com
kai cred remove work@example.com
```

`kai cred list` fetches every account's current Codex quota concurrently and shows the remaining
percentage, relative time until reset, and an inline progress bar. Usable rate-limit reset credits
are shown with their count and latest relative expiry. On an interactive terminal, each account
appears immediately with a live loading indicator and is rewritten as its quota arrives. After `kai
next` or `kai cred next`, Kai reports the newly selected account's quota as soon as the in-flight
lookup completes. Selecting an exhausted account with reset credits prints a notice directing you
to Codex's `/usage` flow to redeem one. A credential name and its reset time are yellow when the
backend reports exactly seven days remaining, indicating that quota countdown has not started. Once
all quota lookups finish, lists with multiple accounts end with a blank-line-separated total bar
averaging the accounts whose quota is available. A centered signed usage bar on the same line shows
the average quota pace balance: elapsed window fraction minus consumed quota fraction. Positive
values mean consumption is behind the clock, while negative values mean it is ahead of the clock.
Values within ±0.20 are yellow, lower values are red, and higher values are green.

`kai cred tickle` starts those untouched seven-day countdowns. It temporarily activates each
matching credential in enrollment order, runs an ephemeral Codex request whose complete prompt is
``What is the current system `gcc` version? (Reply with only the version number.)`` from the user's
home directory, waits for and discards the response, and restores the original active credential
afterward. Refreshed credentials are saved during each switch, and the original credential is
restored even when a probe fails.

`kai cred add` runs `codex login` with a temporary, isolated `CODEX_HOME`, verifies that the
resulting account has the requested email, and then imports its file-backed credential. The
credential currently used by Codex is not replaced or logged out during enrollment. Kai selects
Codex's device-code flow automatically for SSH sessions, CI, and Linux sessions without a graphical
display. A configured `$BROWSER` relay and WSL browser interop retain the browser flow. Use
`--browser-auth` or `--device-auth` to force either behavior.

Rerun `kai cred add <email> --force` to reauthenticate an already-enrolled account. Kai replaces
the credential only after both its email and account/workspace ID match the enrolled profile, and
preserves whether it was active unless `--activate` is also supplied. `kai cred fix` checks all
enrolled credentials concurrently and starts isolated sign-ins only for credentials that are
invalid or rejected as unauthorized. Both commands accept the same browser/device authentication
overrides. Repairs run one at a time; before each sign-in, Kai shows the email to select and waits
for Enter before opening the browser.

The first enrolled account is activated automatically. When another managed account is already
active, Kai normally leaves it in place; if that account has zero remaining quota, adding a new
account activates the new one automatically. A quota lookup failure is non-fatal and leaves the
current account active. Use `--activate` to switch immediately regardless of its quota.

Before every switch, Kai copies the live `auth.json` back into the active account's vault entry.
This preserves refresh-token changes made by Codex. It then atomically installs the selected
credential. Kai never invokes `codex logout`, so switching does not deliberately revoke the
previous credential.

`kai cred list --json` emits stable, secret-free output for scripts, including each quota's
remaining percentage, reset timestamp, window length, and any usable reset-credit count and latest
expiry.

### Vault location and security

The credential vault is stored at:

```text
~/.kai/credentials/
├── state.json
└── profiles/
    └── <email-derived-id>.json
```

Override it with `KAI_CREDENTIALS_HOME`. Kai reads and updates Codex's
`${CODEX_HOME:-~/.codex}/auth.json`.

The vault is not encrypted; like Codex's own `auth.json`, it contains bearer credentials. On Unix,
Kai enforces mode `0700` on vault directories and `0600` on credential/state files, refuses
credential symlinks, uses atomic durable writes, and serializes credential operations with an
invocation lock. Protect backups accordingly.

Kai requires Codex to use file-backed CLI credentials. If `cli_auth_credentials_store` is set to
`auto` or `keyring`, change it to `file` in the active Codex `config.toml`.

Already-running Codex processes may retain their previous credential in memory. Restart them after
`kai next`, `kai cred activate`, or repairing the active credential.

## Other commands

- `kai agent` (`a`; `ar` opens the all-sessions picker, and `ar SESSION_ID` resumes directly)
  launches Codex or Claude.
- `kai worktree` (`wc`, `wa`, `wo`, `wd`) manages git worktrees.
- `kai llm-get` (`lg`) produces LLM-friendly file listings.
- `kai init` writes `.kai/config.toml`.
- `kai bump` commits and pushes changed submodule pointers.

Run `kai help` or `kai <command> --help` for the complete command surface.

## License

AGPL-3.0-only
