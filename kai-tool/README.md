# kai-tool

Kai launches Codex and resumes conversations with configurable credential rotation.

## Usage

```text
kai                         Launch Codex
kai resume                  Open the all-sessions picker
kai resume ID               Resume a specific conversation
kai llm-get PATH...          Assemble a source listing
```

Unambiguous command prefixes work, such as `kai r` and `kai llm`.

`--fast` selects the Fast service tier. Compatible `+k` builds are supervised automatically;
`--no-auto-restart` disables supervision. Kai runs Codex with its approval/sandbox bypass flag.

Install with `cargo install --path kai-tool --locked --force`.

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
  --cause quota-exhausted|credential-invalid [--unavailable-until UNIX_SECONDS]
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

Only quota exhaustion or permanent credential failure invokes `next`. Kai forwards the reported
reset timestamp, then restores Codex's input handoff and resumes the conversation. Normal exit and
crashes invoke no hook; the operating system releases held locks.

Script stdin is closed, stdout carries the JSON response, and stderr remains visible. Ambient
Codex/OpenAI auth variables are removed. Responses are limited to 64 KiB; selection has a
30-second timeout. Managed Codex uses credential protocol version 2 and transfers its descriptor
through `SCM_RIGHTS` with the private, nonce-authenticated READY/GO startup handshake.

Hooks run only during compatible `+k` supervision. Otherwise Codex uses ordinary authentication.
With supervision enabled and no provider, Kai aborts if credential rotation becomes necessary.

## Source listings

`kai llm-get` recursively selects source files, prepends local `AGENTS.md` and `DESIGN.md`,
and copies the listing to the clipboard. Use `--out -` for stdout, `--out PATH` for a file,
or `--slim` to omit instruction files. Run `kai llm-get --help` for filtering options.

## License

AGPL-3.0-only
