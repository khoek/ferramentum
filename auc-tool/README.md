# auc

auc is a machine-local Linux software passkey authenticator. A persistent `auc-agent.service`
exposes one external FIDO2 authenticator through Linux UHID; the ordinary `auc touch` command
supplies one explicit user-presence gesture to one pending browser operation.

## Requirements

- Linux with systemd, logind, and `/dev/uhid`
- a browser or client that supports external FIDO2 authenticators
- Rust and Cargo
- sudo access for installation and removal

## Install

```console
cargo install --locked auc-tool
sudo -i
cargo install --locked --force --root /usr/local --version VERSION auc-tool --bin auc
exit
auc system install
```

Use the same `VERSION` reported by `auc --version`. The explicit root-controlled Cargo step creates
the trusted `/usr/local/bin/auc` bootstrap. `auc system install` then invokes only that root-owned
program through sudo; it never executes the user-owned Cargo copy with privilege.

Start a new login session after installation so membership in the `auc` group takes effect.

## Use

```console
auc status
auc touch
auc credentials list
auc credentials delete CREDENTIAL_ID
```

Run `auc touch` while a browser is waiting for its security-key touch. Credential deletion is
permanent.

## Maintenance

```console
auc system repair
auc system redeploy
auc system redeploy --version VERSION
auc system job JOB_ID
```

Redeploy waits for completion by default. Add `--no-wait` to return after scheduling.

## Uninstall

```console
auc system uninstall
```

This preserves the encrypted vault. To destroy it as well:

```console
auc system uninstall --purge-vault
```

Vault removal is irreversible.

See [DESIGN.md](DESIGN.md) for architecture, protocol, storage, and threat-model details.
