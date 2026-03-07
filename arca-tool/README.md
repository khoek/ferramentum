# arca

CLI for building and publishing managed container artifacts from Rust crates.

## Install

```bash
cargo install arca-tool
```

Installed command: `arca`

Requirements:

- `cargo`
- `docker` with the `buildx` plugin, or `podman`

## Quick start

```bash
arca login --repo us-central1-docker.pkg.dev/my-project/arca/my-image
arca build rust ./my-crate --profile dev --features '' --base-image nvidia/cuda:12.8.1-runtime-ubuntu24.04 --default
arca build rust ./my-crate
arca list
arca push
```

## Commands

- `arca login [--force] [--repo REGISTRY_REPO]`
- `arca build rust [OPTIONS] <PATH>`
- `arca push [ARTIFACT]`
- `arca list`
- `arca prune <local|remote> (--hours HOURS | --days DAYS)`

## `arca login`

`arca login` detects GCP credentials, stores publish config in `~/.arca/config.toml`, and logs the
local container runtime into the configured Google registry.

Example:

```bash
arca login --repo us-central1-docker.pkg.dev/my-project/arca/my-image
```

## `arca build rust`

Builds the Rust crate at `PATH`, packages the selected binary into a container, and stores the
artifact under `~/.arca/containers/<artifact-id>/`.

Relevant options:

- `--profile PROFILE`
- `-F, --features FEATURE[,FEATURE...]`
- `--bin NAME`
- `--base-image IMAGE`
- `--default`
- `--host-build`

Behavior:

- `PATH` may be a crate directory or a `Cargo.toml`.
- `arca` no longer prompts for build settings and no longer injects built-in Cargo/base-image
  defaults.
- `--profile`, `--features`, and `--base-image` must come either from the current command line or
  from previously saved crate-local defaults in `<crate>/.arca/config.toml`.
- `--bin` is only required when the crate exposes multiple binaries. Single-binary crates still
  auto-resolve that one binary.
- `--default` saves the resolved profile, features, binary, and base image into
  `<crate>/.arca/config.toml` for later invocations.
- After you save defaults once, `arca build rust ./my-crate` reuses them with no extra flags.
- The default build path is containerized: `arca` derives a cached builder image from the selected
  runtime base image and runs `cargo build` inside that builder container.
- Official `nvidia/cuda:*-(base|runtime)-*` runtime images automatically build inside the matching
  `nvidia/cuda:*-devel-*` image so the builder and runtime stay on the same CUDA family.
- Builder images are cached per builder base image, and Cargo `target/` caches are reused only
  within the same builder environment.
- The builder mounts the crate's real source hierarchy, including local path dependencies and
  parent `.cargo/config.toml` files.
- Long-running stages are announced explicitly and stream pull/build output so first-time image
  pulls are visible.
- `--host-build` runs `cargo build` on the host instead of inside the cached builder container.

Typical first build:

```bash
arca build rust ./my-crate \
  --profile dev \
  --features '' \
  --base-image nvidia/cuda:12.8.1-runtime-ubuntu24.04 \
  --default
```

Typical later build:

```bash
arca build rust ./my-crate
```

## `arca push`

Pushes local artifacts to the configured Google registry.

- With no argument, `arca push` pushes every local artifact not currently present remotely.
- With `ARTIFACT`, selectors may be a full artifact id, an id prefix, or a unique crate name.

## `arca list`

Lists both stored local artifacts and arca-tracked remote artifacts under the configured registry
prefix. When the same artifact exists in both places, it is merged into one compact entry showing:

- local/remote presence
- profile, binary, base image, and builder-cache status
- current remote presence or last remembered upload ref

## `arca prune`

Deletes arca-tracked artifacts older than the requested age cutoff:

```bash
arca prune local --hours 12
arca prune remote --days 14
```

`local` removes stored artifacts from `~/.arca/containers` and deletes their local runtime images.
`remote` deletes arca-tracked images from the configured Google registry and clears matching upload
refs from local metadata.
