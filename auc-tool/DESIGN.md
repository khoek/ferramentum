# auc design

## Scope and security boundary

auc is a Linux software roaming authenticator. `/usr/local/bin/auc agent serve` is the one root
system process. It owns a kernel UHID descriptor for its complete lifetime, exposes a USB/external
FIDO HID device, stores the authenticator vault, and serves two systemd-owned Unix listeners. A
user's Cargo-installed `auc` is an unprivileged client. It never receives credential private keys,
the vault key, plaintext events, PIN state, or privileged installation controls.

This release is deliberately machine-local. It has no pairing, synchronization, import, export,
remote-event, replication, SSH, timer, or network transport. The event format leaves an explicit
signer and schema domain for a future separately designed synchronization protocol, but this agent
accepts only events signed by its own installation identity. Credentials are backup-eligible because
their representation is intended to remain portable; they are not reported as backed up while no
replica has acknowledged them.

Software emulation cannot provide the physical isolation, tamper resistance, independent display,
or malware resistance of a hardware key. `auc touch` prevents a browser from silently consuming a
presence gesture, but root or code executing as an authorized local user can impersonate that
gesture. The service never claims FIDO certification and uses a fixed auc model AAGUID and
non-vendor USB identity.

## Processes and sockets

`auc-agent.service` stays running so the UHID device remains present. systemd creates
`/run/auc/agent.sock` (`application`) and `/run/auc/capulus.sock` (`capulus`) with `Accept=no` and
passes both descriptors to the service. The application protocol contains status, touch, and local
credential administration. Capulus v2 contains resolve, redeploy, job status, repair, and agent
identity. There is no separate agent executable, presence socket, helper process, update broker, or
permanent redeploy unit.

Both protocols are bounded, length-prefixed CBOR with one request per Unix-stream connection and a
random request ID. `SO_PEERCRED` is authoritative. Socket membership supplies the coarse operator
boundary; `touch` additionally resolves the peer PID through logind and requires an active,
non-remote, local graphical or TTY user session. No UID or session claim in a message is trusted.
Graphical terminal launchers commonly place commands under the per-user systemd manager rather than
the graphical logind session. auc accepts that path only when logind attributes the process to the
same UID and that UID currently owns an active local graphical or TTY session; a caller belonging
directly to a remote session remains ineligible.

## CLI reporting

One invocation-scoped reporter owns terminal capability detection, color policy, cancellation, and
Indicatif rendering. Potentially slow queries use delayed indeterminate progress; repair, redeploy
scheduling, and redeploy following expose their current phase immediately. Non-terminal stderr
receives stable phase changes and periodic heartbeats without ANSI escapes. Fast queries never
invent progress, and no determinate percentage is shown without a known count, byte total, or fixed
duration.

Progress and operational messages are written to stderr. Human-readable query payloads remain on
stdout and only receive color when stdout is itself a terminal, unless the caller explicitly forces
color. Prompts and sudo handoffs suspend live rendering. Redeploy polling has a deadline derived
from the validated managed-product runtime, preserves cancellation as a typed interruption, tolerates
brief agent restarts, and reports the durable job ID plus recorded commit and rollback state on
failure or interruption.

## User presence and CTAPHID cancellation

Only one CTAP command requiring presence may be active. The authenticator callback publishes an
opaque in-memory pending operation containing the CTAPHID channel, RP ID, safe operation label, and
a monotonic deadline. `auc touch` is itself the gesture: it succeeds only while that pending object
exists, prints the accepted operation and RP, wakes the callback exactly once, and is never cached.

CTAP execution occurs on a worker thread while the UHID loop continues processing packets. While
presence is pending the loop emits CTAPHID `KEEPALIVE(UP_NEEDED)`. It handles `CANCEL` immediately by
cancelling the matching pending object and discarding the eventual worker response. PING, INIT,
fragmentation/reassembly, channel locking, errors, busy handling, and message deadlines remain in
the transport loop rather than being blocked behind a presence callback.

## CTAP capabilities

The CTAP core is soft-fido2 0.17, configured for CTAP2 CBOR, ES256, resident credentials, credential
management, ClientPIN/PIN-UV, user presence, a constant zero signature counter, and external/USB
transport. U2F/CTAPHID MSG, EdDSA, enterprise attestation, biometric enrollment, large blobs, and any
capability not exercised by conformance tests are not advertised. Attestation is self/none as
provided by the core; auc never impersonates a certified authenticator model.

## Vault and event log

The first installation creates a 256-bit XChaCha20-Poly1305 vault key, an Ed25519 installation
signing key, and a random stable device identity from the kernel CSPRNG. Files and directories under
`/var/lib/auc` must be real, root-owned, non-symlink paths with directory mode 0700 and secret mode
0600.

Every credential mutation appends one immutable event. An envelope contains a schema version,
monotonic sequence, random event ID, previous-envelope hash, signer public key, random 192-bit AEAD
nonce, ciphertext, and Ed25519 signature. Associated data binds the schema, sequence, event ID,
previous hash, signer, and payload domain. The signature binds that associated data, nonce, and
ciphertext. Events are written with create/no-follow semantics, fsynced, atomically renamed, and
followed by a directory fsync before the in-memory index changes.

Startup sorts and validates every event, requires an unbroken sequence/hash chain and the local
signer, verifies each signature before decryption, authenticates AEAD associated data, rejects
duplicate event IDs, validates credential encodings, and rebuilds the index. Corruption fails startup
with the exact event filename. Upserts replace only live credentials. Deletions create permanent
tombstones; a later upsert for that ID is rejected. No malformed event is skipped.

PIN/UV retry state is device-local and stored in a separately versioned AEAD envelope using the same
vault key and a distinct associated-data domain. Atomic replacement and a monotonically increasing
PIN-state version prevent a normal restart from resetting retries. Credential counters are fixed at
zero because backup-eligible credentials may eventually exist on more than one device.

Schema changes require an explicit offline transactional migration with a verified backup. This
release provides no migration command because it has only schema v1; it never guesses or silently
upgrades unknown data.

## Installation and redeployment

The first installation requires an explicit root-controlled Cargo bootstrap of the exact release to
`/usr/local/bin/auc`. `auc system install` verifies that root-owned executable and its version, then
invokes only `/usr/local/bin/auc agent install` through sudo. The hidden installer verifies its own
canonical path again. The complete system path must be rooted in non-writable root-owned
directories and end in a mode-0755 regular file. Installation creates the `auc` operator group,
adds only the explicitly selected local interactive account, and installs the single program plus
Capulus-rendered units.

Subsequent repairs and redeploys use only `/run/auc/capulus.sock`. The unprivileged client resolves
the target and updates its own Cargo-installed `auc` before requesting the system cutover. Capulus
then builds the exact release as `capulus-build`, commits a journaled system transaction, and
verifies identity, version, and readiness through both sockets. Privileged code never executes or
rebuilds the user-owned program.

Uninstall is an explicit agent operation. It refuses while CTAP presence or a redeploy is active,
stops/disables the units, removes only `/usr/local/bin/auc`, managed units, and runtime state, and
preserves the encrypted vault unless the operator separately confirms vault destruction.
