# ice

Ice manages machines and workloads on Vast, Verda, AWS, GCP and local runtimes.
It helps rent machines for short work sessions: select within explicit resource
and price limits, provision, connect, transfer files, and release resources.
Setup wall time matters. Prefer a provider image containing the needed tools,
reuse a suitable rental across nearby jobs, and delete it at the agreed end.
Application installation, benchmarks and CUDA/profiling admission belong in the
workload project. Ice does not turn SSH readiness into an application guarantee.

## Install

```bash
cargo install ice-tool
```

Installed command: `ice`

Check which binary is installed with `ice --version` or `ice version --json`.
Both work offline, without provider tools or a valid config file. `--version
--json` also returns JSON. The result includes the package version, source
revision, whether that source had local changes, and JSON schema version.
Unknown build metadata is `null`, never an assumed clean release.

## Quick start

```bash
ice login --cloud vast.ai
ice create test-crate --cloud vast.ai --max-price-per-hr 0.60
ice list --cloud vast.ai
ice logs --cloud vast.ai <instance> --follow
ice delete --cloud vast.ai <instance>
```

For Verda, run `ice login --cloud verda` in a terminal. Ice prompts for the
Client ID and Client Secret, validates them, and saves both for subsequent runs.
Vast and Verda share this prompt/validate/save flow. `--force` replaces saved
credentials interactively; environment credentials retain precedence and are
never saved by these providers. AWS/GCP retain their provider CLI discovery.
See [Verda credentials](docs/verda.md#credentials-and-discovery) for automation
and storage details.

## Clouds

Supported cloud identifiers:

- `vast.ai`
- `gcp`
- `aws`
- `verda` — ordinary on-demand GPU VMs with SSH; spot rentals are unsupported.
  See [Verda setup, images and billing](docs/verda.md).
- `local`

## Commands

- `ice version [--json]`
- `ice login [--cloud CLOUD] [--force]`
- `ice config list`
- `ice config get <KEY>`
- `ice config set <KEY=VALUE>`
- `ice config unset <KEY>`
- `ice list [--cloud CLOUD]`
- `ice catalog --cloud verda` — live types, images, on-demand availability and storage prices
- `ice logs [--cloud CLOUD] <INSTANCE> [--tail N] [--follow]`
- `ice shell [--cloud CLOUD] <INSTANCE>`
- `ice pull [--cloud CLOUD] <INSTANCE> <REMOTE_PATH> [LOCAL_PATH]`
- `ice push [--cloud CLOUD] <INSTANCE> <LOCAL_PATH> [REMOTE_PATH]`
- `ice stop [--cloud CLOUD] <INSTANCE>`
- `ice start [--cloud CLOUD] <INSTANCE>`
- `ice delete [--cloud CLOUD] <INSTANCE>`
- `ice create [--cloud CLOUD] [--hours HOURS] [--machine MACHINE] [--custom] [--dry-run] [--ssh | --container IMAGE_REF | --unpack SOURCE | --arca [ARTIFACT] | TARGET]`

`<INSTANCE>` accepts an instance id or label.

## JSON output

`--json` is a global option for every command. It can appear before the command or
after its arguments, including after a nested `config` subcommand:

```sh
ice list --cloud vast.ai --json
ice create --cloud vast.ai --ssh --dry-run --json
ice create --cloud vast.ai --ssh --json
ice shell --cloud vast.ai INSTANCE --print-creds --json
ice --json stop --cloud vast.ai INSTANCE
ice config get default.runtime_hours --json
ice logs --cloud local INSTANCE --follow --json
```

JSON uses the same provider and workload support as each existing command. Actual
creation and previews both support JSON, including managed workloads. Interactive
shell sessions require a terminal: `shell --json` requires `--print-creds`, which
supports Vast, Verda, GCP and AWS. Local shell connection printing remains unsupported.
Clap's `--help`, `help` and argument-validation usage text remain human-readable;
argument errors with `--json` put that text inside a JSON error message.

Successful commands other than `logs` write one JSON object and a newline to stdout:

```json
{"schema_version":2,"command":"list","cloud":"vast.ai","result":{"instances":[]}}
```

- `list` returns an `instances` array with stable string IDs, names, provider state
  and provider-specific fields such as zone, GPU model or SSH endpoint. Missing
  values are `null`; records omit display colors and raw provider metadata.
- `version` returns `version`, `source_revision`, `source_dirty`, and
  `json_schema_version`, with `cloud: null`.
- `create` returns a `status` of `preview`, `created` or `cancelled`, along with
  the selected offer/machine and cost estimate when applicable. A created result
  includes the instance ID. Vast results include allocated disk, the requested
  image reference and the scheduled UTC stop time as Unix seconds. An automatic
  image reference is not a verified image digest or installed toolkit version.
- Vast offers include nullable `machine_id`, `host_id`, `driver_version` and
  `vms_enabled` as reported by the provider. These identify the offer's host
  configuration; they do not select a VM launch mode or verify workload/profiler
  access. `--machine` still selects a GPU model on Vast, not a numeric host ID.
- Verda catalog and create results identify `rental_type: "on_demand"`.
  Selection provenance and `no_matching_offers` errors identify the same scope;
  they make no claim about spot prices or availability.
- Cost values have explicit units. GCP/AWS estimates cover compute. Vast selection,
  `--max-price-per-hr`, `cost` and offer `hourly_usd` use the on-demand quote for
  compute plus the requested storage (`search.totalHour`). Offers without a finite,
  positive quote are excluded. `quoted_total_hourly_usd` reports that same value;
  `offer.provider_hourly_usd` preserves the provider's separate `dph_total` field.
  Vast results set `cost_scope: "compute_and_allocated_storage"` and
  `bandwidth_included: false`; transfer rates appear separately in the offer.
  AWS disk size is `null` when the AMI determines it.
- `shell --print-creds` returns instance details and a `connect_command` string.
  Vast checks readiness and may attach an existing local key to the selected
  instance. `--no-probe` prints reported connection details with
  `readiness: "unchecked"` instead. It can include a local
  identity-file path, but never key contents or provider credentials.
- `start`, `stop` and `delete` share a verified lifecycle result, documented below.
- `pull` and `push` return `status: "completed"`, the requested instance identifier
  and the local/remote paths after the transfer succeeds. Paths retain the user's
  spelling; omitted destinations are represented as `"."`.
- `config list` returns a flat `values` object keyed by supported configuration
  keys. `get`, `set` and `unset` return `key` and `value`; writes also return `path`.
  Values preserve number/array types; unset values are `null`. Configured API and
  AWS keys are `"<redacted>"`. Config commands have `cloud: null`.
- `login` returns `status: "ready"`, a `method` of `cached`, `auto_detected` or
  `prompted`, and an optional `saved_path`, without credentials.
- `refresh-catalog` returns a `catalogs` array with each provider's entry count,
  changed entry count, catalog path and warnings. Without `--cloud`, the envelope
  has `cloud: null` and includes both catalogs after both refreshes succeed.

`logs` writes JSON Lines, including without `--follow`. Each record uses the same
envelope and identifies the requested instance in `result.instance`. Data records
have `event: "data"`, a `stream` of `stdout`, `stderr` or `combined`, `text`, and
`reset`. Records contain chunks, not necessarily complete lines. Concatenate text
per stream; `reset: true` means a provider snapshot or local file was replaced or
truncated. UTF-8 characters spanning reads are preserved; invalid bytes become
replacement characters. Log contents are application output and are not redacted.
Local unpack logs also emit `event: "workload_exit"` with a numeric `exit_code`
(or `null` if unparseable). Successful log retrieval ends with `event: "complete"`;
this does not mean the workload itself succeeded.

Progress, prompts and diagnostics use stderr. Failures write an error envelope to
stdout and return a nonzero exit status:

```json
{"schema_version":2,"command":"list","cloud":"gcp","error":{"code":"command_failed","message":"Failed to list instances"}}
```

Argument parsing errors use `code: "invalid_arguments"` and exit status 2; command
failures use exit status 1 and a specific code when available, otherwise
`code: "command_failed"`. Error envelopes have no `result`;
`command` and `cloud` may be `null` when parsing or configuration loading fails.
Messages are diagnostic text, not stable error identifiers. A log stream may emit
data before its final error record; it will not emit `complete` on failure. An
empty successful listing returns `instances: []`.
Interrupting a subprocess log stream with Ctrl-C stops its log transport and exits
with status 130. Consumers should also handle externally terminated processes
that exit without a final JSON record.
JSON listing reports fresh provider results and does not substitute cached records
when a query fails. Consumers should check the exit status and schema version and
tolerate additional fields.

`--json` selects output format; it does not accept a rental offer or bypass existing
confirmation prompts. Use `create --yes` to accept an offer within the effective
filters and price ceiling. JSON creation finishes the existing deployment and returns
the instance details without offering to open a shell or follow logs. Commands
without `--json` retain normal output.

## Unattended use

Use `--non-interactive --json` for agents and scripts. `--non-interactive` is
global and guarantees that Ice does not prompt or open a browser, even with a
terminal attached. It is also enabled automatically when stdin is not a terminal.
`--json` controls output only; `create --yes` explicitly accepts a rental. Supplying
`--yes` does not change filters, increase the price ceiling, or start a stopped
instance while looking up connection details.

Provide credentials first: Vast accepts `VAST_API_KEY` (which takes precedence
over `auth.vast_ai.api_key` without saving the environment value), GCP uses its
existing credentials/service-account configuration, and AWS uses its existing
credentials/configuration. `ice login --non-interactive --cloud CLOUD --json`
checks credential availability and returns an error when setup is needed
(Vast and AWS also validate the supplied identity). Configured GCP service-account
files are passed to resource commands and returned connection commands through
[gcloud’s credential-file override](https://docs.cloud.google.com/sdk/docs/authenticate).
Missing cloud tools must be installed before use. AWS/GCP catalogs can
be prepared with `ice refresh-catalog --cloud CLOUD --non-interactive --json`.

For example, preview a Vast request:

```sh
ice create --cloud vast.ai --ssh \
  --gpu-count 1 --min-gpu-memory-gb 24 \
  --disk-gb 80 --min-download-mbps 500 \
  --max-price-per-hr 0.60 --hours 1 \
  --no-defaults --non-interactive --dry-run --json
```

Replace `--dry-run` with `--yes` to create. A preview does not reserve an offer;
creation selects against the effective requirements and checks the price again.
The following use the returned instance ID and existing workload capabilities:

```sh
ice shell --cloud vast.ai INSTANCE --print-creds --non-interactive --json
ice push --cloud vast.ai INSTANCE ./input /tmp/input --non-interactive --json
ice pull --cloud vast.ai INSTANCE /tmp/output ./output --non-interactive --json
ice logs --cloud vast.ai INSTANCE --tail 100 --non-interactive --json
ice stop --cloud vast.ai INSTANCE --non-interactive --json
ice delete --cloud vast.ai INSTANCE --non-interactive --json
```

Logs remain workload-dependent: shell-only machines have no managed workload
log. Verified connection printing requires usable SSH credentials. Vast recovery
can attach an existing local keypair to the selected instance; it no longer
creates temporary keys or changes account-wide keys. `--preserve-ephemeral`
remains accepted for compatibility and reports that it is no longer needed.
Reuse a suitable instance across short jobs in a work session.
Use `start` explicitly if it is stopped; connection lookup returns
`instance_stopped` instead of prompting to start it. Stopping and deleting have different billing and storage effects; see below.

Ice disables cloud CLI prompts/pagers and uses SSH batch authentication for
unattended probes and transfers. Interactive shells and `--custom` are rejected
in this mode. Creation returns without offering to open a shell or follow logs.

`create --startup-timeout 30m` changes the readiness wait (default `15m`; seconds
also accepted, e.g. `90s`). It does not extend the existing runtime or stop
deadline. Provider requests/probes have their own timeouts, so an in-flight
request can finish after the readiness deadline. A timeout does not delete the
instance. JSON errors include `details.recovery` with the operation stage and
known instance/offer identifiers, plus Vast's scheduled stop when available.
Inspect that resource before retrying creation; a lost creation response may
leave its outcome uncertain.

Errors retain a nonzero exit status and the JSON envelope. Codes include
`invalid_arguments`, `missing_configuration`, `unsupported_filter`,
`confirmation_required`, `authentication_required`, `interaction_required`,
`instance_stopped`, `no_matching_offers`, `startup_timeout`, `startup_failed`,
`auto_stop_window_unsupported`, and `auto_stop_unverified`.
Other provider failures use `command_failed`. `details` supplies relevant missing
settings, unsupported filters, or recovery information. Partial log output can
precede an error record.

### Vast auto-stop verification

Vast deadlines still round up to the next UTC hour, so a short requested runtime
can have a longer scheduled runtime. Preview both durations and the UTC deadline.
Ice now pins the schedule to that UTC weekday and hour (`WEEKLY`) with a one-minute
start/end window. This avoids wildcard `HOURLY` matches before the deadline: a
previous trial recorded a stop at 02:00 for a requested 05:00 start. The provider's
root cause remains unconfirmed. Weekdays use Vast's Sunday-zero convention.

The rounded deadline must be less than seven days ahead, so there is no earlier
weekly match even if the provider ignores the start boundary. Longer requests
return `auto_stop_window_unsupported` before rental. Offer availability must cover
the rounded deadline, including a check after creation if setup delays cross an
hour boundary.

After scheduling, Ice reads back the provider job and checks its instance, action,
start/end times, UTC calendar, and recorded execution state. JSON creation results
include `auto_stop` with the stored job ID, schedule fields and
`verification: "read_back"`; previews instead say `verification: "planned"`.
This verifies stored configuration, not future execution by Vast. The change
requires a controlled live trial before relying on provider scheduling alone.

If scheduling or verification fails, Ice requests a stop for the just-created
instance and returns `auto_stop_unverified` before proceeding with upload or
workload setup. Error details retain the instance ID, requested schedule, any
observed jobs, and `stop_request` (`accepted` or `failed_or_unknown`). A stop request
is not confirmation of the stopped state. Inspect the instance and delete it when
no longer needed; storage billing continues after stop. Existing provider jobs
are not migrated or removed automatically.

## Verified lifecycle operations

JSON schema version **2** changes lifecycle results. For example:

```json
{"schema_version":2,"command":"delete","cloud":"gcp","result":{"action":"delete","instance_id":"ice-example","outcome":"verified","state":"deleted","request":"acknowledged","verification":"read_back","elapsed_ms":2040,"mutation_retried":false,"instance":null,"storage":{"verification":"unknown","deleted_volume_ids":null,"retained_volume_ids":null}}}
```

A successful provider request is only an acknowledgement. Ice reads the selected
resource again and waits for the requested state; AWS `shutting-down` and GCP
`TERMINATED` do not count as deleted. GCP `TERMINATED` means stopped. Verification
uses resource identity, not the presence of an Ice management label.

`request` is `acknowledged`, `unconfirmed` (the receipt was lost or rejected but
readback proved the state), or `not_needed` (already in the requested state).
When the initial fresh lookup already establishes the desired state, Ice reuses
that observation without another provider read. Cached state cannot satisfy it.
Ice submits each cloud lifecycle request once, then reconciles through reads.
AWS/GCP requests run through their provider CLIs, which manage their own transport.
Start verifies the running state, not SSH; use `shell --print-creds` to verify access.
A failed read or deadline returns a nonzero exit with `operation_unverified`,
known IDs, observed state when available, and reconciliation advice. It never
reports deletion simply because a CLI accepted a request.

`start/stop/delete --timeout 5m` bounds the command's provider requests,
subprocesses and verification, including CLI availability and instance lookup.
The default is five minutes. Deadline expiry kills a hung provider CLI locally;
it cannot cancel an operation already accepted in the cloud. Before a mutation,
a failed lookup/preflight can instead return `operation_timeout` with the input
identifier. Creation retains its separate `--startup-timeout` readiness budget.
Timeouts never trigger automatic deletion.

AWS, GCP and Vast accept deletion directly, so Ice no longer requests stop and
waits for it before deleting. Normal provider shutdown semantics apply; local
containers still receive a graceful stop before removal. Removing the instance
is distinct from removing all disks: generic lifecycle output marks storage
cleanup `unknown`. Verda separately verifies the OS volume and reports retained
attached volumes under `storage`, with detailed evidence under `provider_details`.
Do not interpret VM deletion as proof that all storage charges have ended.
Verda shutdown retains **compute and storage billing**. See its
[cleanup contract](docs/verda.md#deadlines-failures-and-deletion).

## Config

Ice uses the OS configuration directory: on Linux,
`${XDG_CONFIG_HOME:-~/.config}/ice/config.toml`. `ice config list` reports the
actual path. Configuration is per user, with separate search preferences for
each cloud. Provider catalogs/GPU data currently remain under `~/.ice/`.

Creation resolves command-line filters over saved provider preferences. Optional
CPU/RAM/model filters may be unspecified; remote creation requires a positive
hourly price ceiling from a flag or saved preference. Existing AWS selection
defaults to CPU-only when no GPU models are supplied; this is now shown explicitly.
Use `ice config set` to save preferences. Creation and interactive filter editing
do not save answers or command-line overrides into this file.

`create --no-defaults` ignores saved **search filters and disk preferences** for
that invocation. It retains credentials, provider location/image settings, the
default cloud, and the default runtime; pass `--cloud` and `--hours` explicitly
when those must be reproducible. Verda additionally ignores its saved image and
location, so supply `--image` explicitly with `--no-defaults`. Existing disk fallbacks remain Vast 32 GB,
GCP 50 GB, and the AWS AMI's root volume size.

Before searching, human output identifies effective filter values and their
sources. JSON creation results and relevant errors include `selection`, with
`config_path`, `saved_filters_ignored`, `price_scope`, and a `filters` map.
`runtime_hours` records its value/source and `machine` records any explicit
provider machine selector. Filter entries contain `value` and `source`
(`command_line`, `saved_configuration`,
`built_in`, `unspecified`, or `interactive`). Null means unspecified/unknown.

Use:

- `ice config list` to view supported keys and current values
- `ice config get <KEY>` to read one key
- `ice config set <KEY=VALUE>` to write one key
- `ice config unset <KEY>` to clear one key

Auth values are redacted in config output.

### Config keys

- `default.cloud`: `vast.ai|verda|gcp|aws|local`
- `default.runtime_hours`
- `default.vast_ai.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- `default.gcp.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- `default.aws.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- Each provider also accepts `gpu_count`, `min_gpu_memory_gb`, `disk_gb`,
  `min_download_mbps`, and `min_upload_mbps` keys; unsupported provider/filter
  combinations fail during creation rather than being ignored.
- `default.gcp.region|zone|image_family|image_project|boot_disk_gb`
- `default.aws.region|ami|key_name|ssh_key_path|ssh_user|security_group_id|subnet_id|root_disk_gb`
- `default.verda.image|location|ssh_key_id|ssh_key_path`
- `default.verda.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr|gpu_count|min_gpu_memory_gb|disk_gb`
- `auth.verda.client_id|client_secret`
- `auth.vast_ai.api_key`
- `auth.gcp.project|service_account_json`
- `auth.aws.access_key_id|secret_access_key`

## `ice create`

`ice create` now takes exactly one deployment target mode explicitly, or defaults to local `arca`
unpack deployment when none is given.

Target modes:

- `--ssh`
- `--container IMAGE_REF`
- `--unpack SOURCE`
- `--arca [ARTIFACT]`
- bare `TARGET`

Behavior:

- `ice create test-crate` means `ice create --arca test-crate`.
- Bare `ice create` means `ice create --arca`, which selects the newest local `arca` artifact.
- `--arca NAME` is shorthand for `--unpack arca:NAME`.
- `--container` accepts a remote container image ref such as
  `us-central1-docker.pkg.dev/my-project/arca/my-image:tag`.
- `--container arca:...` is rejected because local `arca` artifacts are an unpack-only flow.
- `--unpack` accepts:
  - `arca:selector`
  - a local image name such as `arca-local:my-crate-deadbeef`
  - a saved `image.tar` path
  - a full remote container ref
- `--hours` overrides runtime duration for the deploy. If omitted, `ice` uses
  `default.runtime_hours` and otherwise falls back to `1.0`.
- `--custom` prompts for search filters on marketplace-backed clouds.
- `--no-gpu` clears saved GPU models; it does not require CPU-only hardware.
- `--gpu-count COUNT` requests an exact GPU count. Positive counts are supported
  on Vast/Verda; zero explicitly requires a CPU-only candidate on Vast/GCP/AWS.
  Verda currently requires a GPU VM. Zero conflicts with GPU model/memory requirements.
- `--min-gpu-memory-gb GB` requires memory per card, not summed across GPUs (Vast/Verda).
- `--min-download-mbps MBPS` and `--min-upload-mbps MBPS` constrain reported
  internet bandwidth at the rented host (Vast). They cannot guarantee throughput
  from a particular image registry. Missing measurements cannot satisfy a filter.
- `--disk-gb GB` requests allocation on Vast/Verda/GCP/AWS. Provider disk units and
  rounding apply. Vast search pricing uses the same allocation as creation.
- GPU memory uses Vast's reported GB convention (API memory / 1000), consistent
  with its official CLI. Network units are megabits per second. JSON offers expose
  `gpu_memory_gb`, `download_mbps`, `upload_mbps`, and upload/download USD per GB.
- `--max-price-per-hr` caps the allocation-adjusted compute-plus-storage quote on
  Vast. Selection and estimates use the same quote and disk allocation as creation;
  base-price and reserved-discount fallbacks cannot bypass the ceiling. GCP/AWS
  caps and estimates cover compute. These are hourly limits, not total spending
  caps; bandwidth is billed separately and storage billing continues after stop.
- `--image` selects a Verda provider OS catalog image (required explicitly or as
  a saved default). It does not upload a local image or select a container image.
  See [Verda provisioning and setup latency](docs/verda.md#image-provisioning-and-short-rentals).
- `--machine` pins a type on AWS/GCP/Verda. On Vast it selects a GPU model,
  not a physical host ID or offer ID. Use `--gpu` for a consistent GPU-model
  filter across providers.
- `--dry-run` reports the chosen machine and exits before provisioning.

Examples:

```bash
ice create test-crate
ice create --arca test-crate --hours 0.25
ice create --unpack arca:test-crate --cloud vast.ai
ice create --container us-central1-docker.pkg.dev/my-project/arca/my-image:tag --cloud vast.ai
ice create --ssh --cloud gcp --machine g2-standard-4
```

## Workload behavior

- `container` runs a real container image on the target machine.
- `unpack` extracts the layer that introduced the image entrypoint plus all higher layers, uploads
  only that staged filesystem diff, and starts the image entrypoint detached with persisted logs.
- `local` supports `container` and `unpack`. It does not support `--ssh`.
- `vast.ai` container workloads use entrypoint mode, not SSH mode.
- `vast.ai`, `gcp`, and `aws` unpack workloads boot a shell-capable machine, upload the unpack
  bundle over SSH, and run the workload detached from the SSH session.
- Deploy flows print explicit stages for machine creation, SSH readiness, unpack upload, workload
  start, and log following.
- If Vast offer acceptance fails, `ice create` can immediately retry the search interactively.

## Logs and shell

- `ice logs` shows stdout and stderr for managed workloads.
- For `unpack`, logs come from the persisted detached `stdio.log` and `--follow` exits
  automatically after the workload finishes and the final output is drained.
- For `container` on `vast.ai`, logs use Vast's provider API.
- `ice logs --cloud vast.ai INSTANCE --provider-logs` requests provider container
  logs even for an `unpack` workload. This works without SSH and is useful for
  diagnosing sshd failures. `--daemon` selects provider daemon system logs.
- `ice shell` opens the workload shell when possible and otherwise falls back to the host shell.

Vast connection recovery uses batch-mode SSH authentication probes (`ssh -N`),
without running a remote command, before a shell, transfer, or unpack operation.
It tries the reported SSH endpoint and, if that
fails, a direct endpoint from the current provider response's `public_ipaddr` and
`ports["22/tcp"]`. A changed mapped port is read afresh on the next invocation.
Host-key verification failures stop recovery; exit status 255 alone is not
treated as evidence of a missing key.

If public-key authentication fails, Ice tries an existing local identity and
checks the last 200 provider container log lines for explicit sshd ownership/mode
errors. Such evidence produces `ssh_server_permissions` and stops recovery. Ice
does not change server permissions or startup scripts, or recycle the container.
When no such evidence is available, it can attach the existing key to this
instance once. An already-associated key ends recovery; a newly attached key gets
up to three propagation probes. No account-wide key changes are attempted.

Recovery has a shared 90-second budget and at most seven probes. Each probe is
limited to 10 seconds, provider-log diagnostics to 15 seconds, and key attachment
to 10 seconds, all within the remaining budget. New-instance startup still waits
for a reachable reported port under the existing startup timeout, separately
from SSH authentication recovery. Once a probe
succeeds, the requested operation runs once. A nonzero remote SSH command status
other than 255 is `ssh_remote_command_failed`; ambiguous SSH or transfer failures
are `ssh_operation_failed`. Neither replays the operation or changes keys.

JSON connection results include `readiness`, the selected `endpoint`, and `ssh`
diagnostics. Connection errors include `details.ssh`: ordered probe attempts with
each endpoint and failure category, elapsed time, budget, key-attachment outcome,
provider-log diagnosis, and a next diagnostic command. Codes distinguish
`ssh_transport_failed`, `ssh_authentication_failed`, `ssh_host_key_failed`,
`ssh_remote_command_failed`, `ssh_local_setup_failed`, `ssh_connection_failed`,
`ssh_probe_interrupted`, `ssh_server_permissions`, and `ssh_recovery_timeout`.
Verbose probe output and arbitrary provider logs are not copied into these fields.

To inspect reported endpoints without starting a stopped machine, waiting for
readiness, probing SSH, reading local keys, or initiating recovery:

```sh
ice shell --cloud vast.ai INSTANCE --print-creds --no-probe --json
```

This performs a provider lookup and returns `readiness: "unchecked"`; it does not
prove the connection works. Its `endpoints` array includes any reported direct
alternative. `--no-probe` currently applies only to Vast and requires
`--print-creds`.

## Notes

- `--cloud` can be omitted when `default.cloud` is configured.
- `ice list --cloud local` reports both managed local containers and managed local unpack workloads.
- Private GCP registry pulls use your configured GCP credentials or active `gcloud`
  authentication.
- External commands used by some flows: `ssh`, `rsync`, `gcloud`, `aws`, `docker`, `podman`.

The `scripts/check-cli.sh ice-tool ...` build helper embeds the checkout's revision
and local-change status. Direct Cargo builders can provide `ICE_BUILD_REVISION`
(full Git hash) and `ICE_BUILD_DIRTY` (`true`/`false`) at build time; omitted values
remain unknown. Runtime environment variables cannot change the compiled identity.
