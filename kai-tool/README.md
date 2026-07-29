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
kai next
```

`kai next` is shorthand for `kai cred next`. You can also select or remove an account explicitly:

```bash
kai cred activate personal@example.com
kai cred remove work@example.com
```

`kai cred add` runs `codex login` with a temporary, isolated `CODEX_HOME`, verifies that the
resulting account has the requested email, and then imports its file-backed credential. The
credential currently used by Codex is not replaced or logged out during enrollment. Use
`--device-auth` for Codex's device-code flow and `--activate` to switch immediately after adding an
account.

Before every switch, Kai copies the live `auth.json` back into the active account's vault entry.
This preserves refresh-token changes made by Codex. It then atomically installs the selected
credential. Kai never invokes `codex logout`, so switching does not deliberately revoke the
previous credential.

`kai cred list --json` emits stable, secret-free output for scripts.

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
`kai next` or `kai cred activate`.

## Other commands

- `kai agent` (`a`, or `ar` for `--resume-all`) launches Codex or Claude.
- `kai worktree` (`wc`, `wa`, `wo`, `wd`) manages git worktrees.
- `kai llm-get` (`lg`) produces LLM-friendly file listings.
- `kai init` writes `.kai/config.toml`.
- `kai bump` commits and pushes changed submodule pointers.

Run `kai help` or `kai <command> --help` for the complete command surface.

## License

AGPL-3.0-only
