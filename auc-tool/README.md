# auc

auc is a machine-local Linux software passkey authenticator. A persistent `auc-agent` system
service exposes one external FIDO2 authenticator through Linux UHID; the ordinary `auc touch`
command supplies one explicit user-presence gesture to one pending browser operation.

auc is software, not a hardware security boundary. It does not provide a secure element, physical
tamper resistance, an independent display, FIDO certification, or protection from a compromised
root account. It is useful when a stable roaming-authenticator interface and deliberate local
presence are more important than hardware isolation.

This release is deliberately local to one machine. It has no `pair` or `sync` command, network
replication, import/export path, background synchronization, or synchronization timer.

## Requirements

- Linux with systemd, logind, the UHID kernel driver, and `/dev/uhid`;
- a browser or client that supports external FIDO2/CTAP2 authenticators;
- Rust/Cargo for the initial user installation and registry/toolchain network access for redeploys;
- sudo authorization for the initial system installation and explicit uninstall only.

Root never owns a Rustup installation. Capulus performs later exact-version builds as the shared
`capulus-build` system account and installs the resulting root-owned binaries through a journaled
system transaction.

## Install

Install both same-version executables into the invoking user's Cargo bin directory, then bootstrap
the system service as that user:

```console
cargo install --locked auc-tool
auc system install
```

The installer validates that `auc-agent` is a same-version, executable regular file beside `auc`,
checks the kernel audit login UID, creates the dedicated `auc` access group, installs
`/usr/local/bin/auc` and `/usr/local/bin/auc-agent`, and starts the service. Start a new login
session afterward so the new group membership is active.

The installed topology is:

```text
/run/auc/agent.sock   (application) -----\
                                            +--> auc-agent.service
/run/auc/capulus.sock (management) ------/
```

systemd owns both sockets and passes the named descriptors to the single long-lived agent. The
agent owns `/dev/uhid` and the root-only encrypted vault at `/var/lib/auc`.

## Use

Check that the virtual authenticator is present:

```console
auc status
```

When a browser is waiting for a security-key touch, run:

```console
auc touch
```

The gesture is accepted only from an active, local, non-remote logind session and only while one
matching CTAP operation is pending. A touch is consumed once, expires after 30 seconds, and is never
cached for a later request.

Inspect or permanently tombstone local resident credentials with:

```console
auc credentials list
auc credentials delete CREDENTIAL_ID
```

Deletion cannot be undone by replaying an older local event.

## Terminal output

auc uses a live terminal display for work that can take noticeable time and automatically falls
back to stable, ANSI-free phase records when stderr is redirected. Fast local queries use delayed
visibility, so successful commands do not flash a spinner unless the agent is genuinely slow.
Command payloads such as status and credential rows stay on stdout; progress, confirmations,
warnings, and mutation outcomes use stderr.

Every command accepts `--progress auto|interactive|plain|off` and
`--color auto|always|never`. The default color mode also honors `NO_COLOR`.

## Repair and redeploy

Repair the installed files and units from the current managed installation:

```console
auc system repair
```

Redeploy the latest published release, or require an exact version:

```console
auc system redeploy
auc system redeploy --version 0.1.0
```

Redeploy returns a durable job ID and waits by default. Use `--no-wait` and inspect that exact job
later when desired:

```console
auc system redeploy --version 0.1.0 --no-wait
auc system job JOB_ID
```

The system binary is authoritative after installation. A redeploy also replaces the requesting
user's existing Cargo-installed `auc` at the same exact version, using that user's own validated
Cargo/Rustup environment and UID.

## Uninstall and recovery

The normal uninstall removes the managed service, sockets, and system binaries but preserves the
encrypted vault and access policy:

```console
auc system uninstall
```

Permanent vault destruction is a separate explicit choice:

```console
auc system uninstall --purge-vault
```

`--purge-vault` destroys every credential, tombstone, PIN state, signing key, and encryption key,
then removes the dedicated access group. It is irreversible. `--yes` suppresses both confirmation
prompts and should be used only by unattended workflows that already provide an equivalent safety
gate.

Unknown vault schemas, malformed ownership or modes, symlinks, broken event chains, invalid
signatures, AEAD failures, and live PIN-state version regression all fail closed. See
[DESIGN.md](DESIGN.md) for the protocol, storage, cancellation, and threat-model details.
