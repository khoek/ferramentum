# ice

Minimal CLI for creating and managing cloud VM instances across `vast.ai`, `gcp`, and `aws`.

## Install

```bash
cargo install ice-tool
```

Installed command: `ice`

## Quick start

```bash
ice login --cloud vast.ai
ice config list
ice create --cloud vast.ai 0.5
ice list --cloud vast.ai
ice shell --cloud vast.ai <instance>
```

## Clouds

Supported cloud identifiers:

- `vast.ai`
- `gcp`
- `aws`

## Commands

- `ice login [--cloud CLOUD] [--force]`
- `ice config list`
- `ice config get <KEY>`
- `ice config set <KEY=VALUE>`
- `ice config unset <KEY>`
- `ice list [--cloud CLOUD]`
- `ice shell|sh [--cloud CLOUD] <INSTANCE>`
- `ice dl [--cloud CLOUD] <INSTANCE> <REMOTE_PATH> [LOCAL_PATH]`
- `ice stop [--cloud CLOUD] <INSTANCE>`
- `ice start [--cloud CLOUD] <INSTANCE>`
- `ice delete [--cloud CLOUD] <INSTANCE>`
- `ice create [--cloud CLOUD] [--machine MACHINE] [--custom] [--dry-run] <HOURS>`

`<INSTANCE>` accepts an instance ID or label.

## Config

Config file: `~/.ice/config.toml`

Use:

- `ice config list` to view supported keys and current values
- `ice config get <KEY>` to read one key
- `ice config set <KEY=VALUE>` to write one key
- `ice config unset <KEY>` to clear one key

Auth values are redacted in config output.

### Config keys

- `default.cloud`: `vast.ai|gcp|aws`
- `default.vast_ai.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- `default.gcp.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- `default.aws.min_cpus|min_ram_gb|allowed_gpus|max_price_per_hr`
- `default.gcp.region|zone|image_family|image_project|boot_disk_gb`
- `default.aws.region|ami|key_name|ssh_key_path|ssh_user|security_group_id|subnet_id|root_disk_gb`
- `auth.vast_ai.api_key`
- `auth.gcp.project|service_account_json`
- `auth.aws.access_key_id|secret_access_key`

Compatibility aliases also work for Vast defaults:

- `default.min_cpus`
- `default.min_ram_gb`
- `default.allowed_gpus`
- `default.max_price_per_hr`
- `default.max_price`

## `create` behavior

- `HOURS` is a float (for example `0.1`).
- Finds the cheapest matching machine for the selected cloud.
- Uses configured defaults, or prompts for missing/default overrides.
- `--custom` always prompts for search filters.
- `--machine` forces a cloud-specific machine type.
- `--dry-run` runs selection/reporting and exits before any create/accept step.
- After choosing an offer/machine, `ice` can optionally upload a local container archive (`docker save`/`podman save`) and auto-start one managed container on the instance.
- If a max price cap is set and cheapest result exceeds it, create fails with pricing details.
- For `vast.ai`, auto-stop is scheduled at the earliest hourly boundary that still satisfies requested runtime.
- For `gcp`/`aws`, search runs across built-in catalogs; region/zone only pin when explicitly configured.

## Notes

- `--cloud` can be omitted when `default.cloud` is configured.
- `ice shell` opens a shell inside the managed container when one exists on the target instance, otherwise it falls back to the host VM shell.
- Long-running operations use progress spinners.
- External commands used by some flows: `ssh`, `rsync`, `gcloud`, `aws`.
