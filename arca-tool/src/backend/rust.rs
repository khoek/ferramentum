use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::artifact::{
    ArtifactMetadata, CURRENT_SCHEMA_VERSION, StoredArtifact, arca_image_labels,
    create_artifact_dir, default_archive_file_name, remote_tag as build_remote_tag, save_metadata,
};
use crate::backend::GeneratorBackend;
use crate::cli::RustArgs;
use crate::command::{
    ensure_command_available, run_command_output, run_command_status_streaming, run_command_text,
};
use crate::config::{
    ProjectConfig, RustProjectConfig, ensure_cache_root, load_project_config, save_project_config,
};
use crate::runtime::{ContainerMount, ContainerRuntime};
use crate::ui::{detail, spinner, stage, success};

pub struct RustBackend {
    plan: RustBuildPlan,
}

struct RustBuildPlan {
    crate_dir: PathBuf,
    manifest_path: PathBuf,
    build_fingerprint: String,
    crate_name: String,
    crate_version: String,
    binary_name: String,
    profile: String,
    features: Vec<String>,
    base_image: String,
    build_mode: RustBuildMode,
}

enum RustBuildMode {
    Container(ContainerizedBuildPlan),
    Host { target_directory: PathBuf },
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    workspace_root: PathBuf,
    target_directory: PathBuf,
    packages: Vec<CargoPackage>,
    resolve: Option<CargoResolve>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    version: String,
    manifest_path: PathBuf,
    source: Option<String>,
    targets: Vec<CargoTarget>,
}

#[derive(Debug, Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CargoResolve {
    nodes: Vec<CargoResolveNode>,
}

#[derive(Debug, Deserialize)]
struct CargoResolveNode {
    id: String,
    dependencies: Vec<String>,
}

const BUILDER_RECIPE_VERSION: &str = "arca-rust-builder-v1";
const BUILDER_IMAGE_PREFIX: &str = "arca-builder";
const BUILDER_CONTAINERFILE_NAME: &str = "Builder.Containerfile";
const CACHE_ROOT_SEGMENT: &str = "rust";
const BUILDERS_SEGMENT: &str = "builders";
const BUILDS_SEGMENT: &str = "builds";
const GLOBAL_CARGO_HOME_SEGMENT: &str = "cargo-home";
const CONTAINER_SOURCE_ROOT: &str = "/work/source";
const CONTAINER_CACHE_ROOT: &str = "/work/cache";
const CONTAINER_CARGO_HOME: &str = "/work/cargo-home";
const CONTAINER_RUSTUP_HOME: &str = "/opt/arca/rustup";

struct ContainerizedBuildPlan {
    runtime_base_image: String,
    builder_base_image: String,
    builder_image_tag: String,
    build_cache_dir: PathBuf,
    cargo_home_dir: PathBuf,
    host_binary_path: PathBuf,
    container_crate_dir: String,
    container_manifest_path: String,
    source_root: PathBuf,
    builder_context_dir: PathBuf,
}

impl ContainerizedBuildPlan {
    fn new(
        workspace_root: &Path,
        crate_dir: &Path,
        manifest_path: &Path,
        package_name: &str,
        binary_name: &str,
        profile: &str,
        features: &[String],
        base_image: &str,
        local_dependency_dirs: &[PathBuf],
    ) -> Result<Self> {
        let source_root = source_mount_root(workspace_root, crate_dir, local_dependency_dirs)?;
        let rust_cache_root = ensure_cache_root()?.join(CACHE_ROOT_SEGMENT);
        let builder_base_image = resolve_builder_base_image(base_image);
        let builder_hash = builder_hash_for_base_image(&builder_base_image);
        let builder_context_dir = rust_cache_root
            .join(BUILDERS_SEGMENT)
            .join(builder_hash.clone());
        fs::create_dir_all(&builder_context_dir).with_context(|| {
            format!(
                "Failed to create builder cache directory: {}",
                builder_context_dir.display()
            )
        })?;
        fs::write(
            builder_context_dir.join(BUILDER_CONTAINERFILE_NAME),
            render_builder_containerfile(&builder_base_image),
        )
        .with_context(|| {
            format!(
                "Failed to write builder containerfile: {}",
                builder_context_dir
                    .join(BUILDER_CONTAINERFILE_NAME)
                    .display()
            )
        })?;

        let build_cache_key = short_hash(&[
            "arca-rust-build-cache-v2".to_owned(),
            manifest_path.display().to_string(),
            package_name.to_owned(),
            binary_name.to_owned(),
            profile.to_owned(),
            normalized_features(features).join(","),
            builder_base_image.clone(),
        ]);
        let build_cache_dir = rust_cache_root.join(BUILDS_SEGMENT).join(build_cache_key);
        fs::create_dir_all(build_cache_dir.join("home")).with_context(|| {
            format!(
                "Failed to create build home directory: {}",
                build_cache_dir.join("home").display()
            )
        })?;
        fs::create_dir_all(build_cache_dir.join("target")).with_context(|| {
            format!(
                "Failed to create build target directory: {}",
                build_cache_dir.join("target").display()
            )
        })?;

        let cargo_home_dir = rust_cache_root.join(GLOBAL_CARGO_HOME_SEGMENT);
        sync_host_cargo_home(&cargo_home_dir)?;

        let crate_relative = container_relative_path(&source_root, crate_dir)?;
        let manifest_relative = container_relative_path(&source_root, manifest_path)?;
        let host_binary_path = build_cache_dir
            .join("target")
            .join(cargo_profile_output_dir(profile))
            .join(binary_name);
        Ok(Self {
            runtime_base_image: base_image.to_owned(),
            builder_base_image,
            builder_image_tag: format!("{BUILDER_IMAGE_PREFIX}:{builder_hash}"),
            build_cache_dir,
            cargo_home_dir,
            host_binary_path,
            container_crate_dir: format!("{CONTAINER_SOURCE_ROOT}/{crate_relative}"),
            container_manifest_path: format!("{CONTAINER_SOURCE_ROOT}/{manifest_relative}"),
            source_root,
            builder_context_dir,
        })
    }

    fn ensure_builder_image(&self, runtime: ContainerRuntime) -> Result<bool> {
        if runtime.image_exists(&self.builder_image_tag)? {
            return Ok(false);
        }
        runtime.build(
            &self.builder_image_tag,
            &self.builder_context_dir,
            &self.builder_context_dir.join(BUILDER_CONTAINERFILE_NAME),
        )?;
        Ok(true)
    }

    fn ensure_builder_base_image(&self, runtime: ContainerRuntime) -> Result<bool> {
        if runtime.image_exists(&self.builder_base_image)? {
            return Ok(false);
        }
        runtime.pull(&self.builder_base_image)?;
        Ok(true)
    }

    fn build_binary(
        &self,
        runtime: ContainerRuntime,
        profile: &str,
        features: &[String],
        binary_name: &str,
    ) -> Result<()> {
        sync_host_cargo_home(&self.cargo_home_dir)?;
        let envs = vec![
            ("HOME".to_owned(), format!("{CONTAINER_CACHE_ROOT}/home")),
            ("CARGO_HOME".to_owned(), CONTAINER_CARGO_HOME.to_owned()),
            (
                "CARGO_TARGET_DIR".to_owned(),
                format!("{CONTAINER_CACHE_ROOT}/target"),
            ),
            ("RUSTUP_HOME".to_owned(), CONTAINER_RUSTUP_HOME.to_owned()),
        ];
        let mounts = [
            ContainerMount {
                source: &self.source_root,
                target: CONTAINER_SOURCE_ROOT,
                read_only: false,
            },
            ContainerMount {
                source: &self.build_cache_dir,
                target: CONTAINER_CACHE_ROOT,
                read_only: false,
            },
            ContainerMount {
                source: &self.cargo_home_dir,
                target: CONTAINER_CARGO_HOME,
                read_only: false,
            },
        ];
        let mut command = vec![
            "cargo".to_owned(),
            "build".to_owned(),
            "--manifest-path".to_owned(),
            self.container_manifest_path.clone(),
            "--profile".to_owned(),
            profile.to_owned(),
            "--bin".to_owned(),
            binary_name.to_owned(),
        ];
        if !features.is_empty() {
            command.push("--features".to_owned());
            command.push(normalized_features(features).join(","));
        }
        runtime.run(
            &self.builder_image_tag,
            Some(&self.container_crate_dir),
            &envs,
            &mounts,
            &command,
        )?;
        if !self.host_binary_path.is_file() {
            return Err(anyhow!(
                "Container build completed but the expected binary is missing: {}",
                self.host_binary_path.display()
            ));
        }
        Ok(())
    }

    fn fingerprint_key(&self) -> String {
        format!("{BUILDER_RECIPE_VERSION}:{}", self.builder_image_tag)
    }
}

impl RustBackend {
    pub fn from_args(args: RustArgs) -> Result<Self> {
        let manifest_path = resolve_manifest_path(&args.path)?;
        let crate_dir = manifest_path
            .parent()
            .ok_or_else(|| anyhow!("Cargo.toml has no parent directory"))?
            .to_path_buf();
        let metadata = load_cargo_metadata(&manifest_path)?;
        let package = package_for_manifest(&metadata, &manifest_path)?;
        let binary_targets = package
            .targets
            .iter()
            .filter(|target| target.kind.iter().any(|kind| kind == "bin"))
            .map(|target| target.name.clone())
            .collect::<Vec<_>>();
        if binary_targets.is_empty() {
            bail!(
                "Crate `{}` has no binary targets. `arca build rust` requires a runnable binary crate.",
                package.name
            );
        }

        let loaded_config = load_project_config(&crate_dir)?;
        let profile = resolve_profile(&args, &loaded_config)?;
        let features = resolve_features(&args, &loaded_config)?;
        let binary_name = resolve_binary_name(&args, &loaded_config, &binary_targets)?;
        let base_image = resolve_base_image(&args, &loaded_config)?;
        let local_dependency_dirs = local_dependency_directories(&metadata, &package.id)?;
        let build_mode = if args.host_build {
            RustBuildMode::Host {
                target_directory: metadata.target_directory.clone(),
            }
        } else {
            RustBuildMode::Container(ContainerizedBuildPlan::new(
                &metadata.workspace_root,
                &crate_dir,
                &manifest_path,
                &package.name,
                &binary_name,
                &profile,
                &features,
                &base_image,
                &local_dependency_dirs,
            )?)
        };
        let build_fingerprint = compute_build_fingerprint(
            &metadata,
            package,
            &crate_dir,
            &local_dependency_dirs,
            &binary_name,
            &profile,
            &features,
            &base_image,
            &build_mode,
        )?;

        if args.save_defaults {
            let mut project_config = loaded_config;
            project_config.rust = RustProjectConfig {
                profile: Some(profile.clone()),
                bin: Some(binary_name.clone()),
                features: Some(features.clone()),
                base_image: Some(base_image.clone()),
            };
            save_project_config(&crate_dir, &project_config)?;
        }

        Ok(Self {
            plan: RustBuildPlan {
                crate_dir,
                manifest_path,
                build_fingerprint,
                crate_name: package.name.clone(),
                crate_version: package.version.clone(),
                binary_name,
                profile,
                features,
                base_image,
                build_mode,
            },
        })
    }
}

impl GeneratorBackend for RustBackend {
    fn kind(&self) -> &'static str {
        "rust"
    }

    fn build(&self, runtime: ContainerRuntime) -> Result<StoredArtifact> {
        ensure_command_available("cargo")?;
        runtime.ensure_build_available()?;

        let binary_path = match &self.plan.build_mode {
            RustBuildMode::Container(plan) => build_binary_in_container(
                runtime,
                plan,
                &self.plan.crate_name,
                &self.plan.profile,
                &self.plan.features,
                &self.plan.binary_name,
            )?,
            RustBuildMode::Host { target_directory } => build_binary_on_host(
                &self.plan.crate_dir,
                &self.plan.manifest_path,
                target_directory,
                &self.plan.crate_name,
                &self.plan.profile,
                &self.plan.features,
                &self.plan.binary_name,
            )?,
        };

        let (artifact_id, artifact_dir) = create_artifact_dir()?;
        let archive_path = artifact_dir.join(default_archive_file_name());
        let remote_tag = build_remote_tag(&self.plan.crate_name, &artifact_id);
        let local_tag = format!("arca-local:{remote_tag}");
        let metadata = ArtifactMetadata {
            schema_version: CURRENT_SCHEMA_VERSION,
            artifact_id,
            remote_tag,
            build_fingerprint: self.plan.build_fingerprint.clone(),
            kind: self.kind().to_owned(),
            created_at_epoch_ms: now_epoch_ms()?,
            crate_name: self.plan.crate_name.clone(),
            crate_version: self.plan.crate_version.clone(),
            binary_name: self.plan.binary_name.clone(),
            source_path: self.plan.crate_dir.display().to_string(),
            manifest_path: self.plan.manifest_path.display().to_string(),
            cargo_profile: self.plan.profile.clone(),
            cargo_features: self.plan.features.clone(),
            base_image: self.plan.base_image.clone(),
            runtime: runtime.name().to_owned(),
            local_tag: local_tag.clone(),
            archive_file: default_archive_file_name().to_owned(),
            uploaded_ref: None,
            uploaded_at_epoch_ms: None,
        };
        let temp_context =
            TempDir::new().context("Failed to create temporary container context")?;
        let staged_binary = temp_context.path().join(&self.plan.binary_name);
        fs::copy(&binary_path, &staged_binary).with_context(|| {
            format!(
                "Failed to copy built binary into container context: {}",
                staged_binary.display()
            )
        })?;
        fs::set_permissions(&staged_binary, fs::Permissions::from_mode(0o755)).with_context(
            || {
                format!(
                    "Failed to mark staged binary executable: {}",
                    staged_binary.display()
                )
            },
        )?;

        let containerfile = render_containerfile(&metadata)?;
        let temp_containerfile = temp_context.path().join("Containerfile");
        fs::write(&temp_containerfile, &containerfile).with_context(|| {
            format!(
                "Failed to write temporary containerfile: {}",
                temp_containerfile.display()
            )
        })?;
        fs::write(artifact_dir.join("Containerfile"), &containerfile).with_context(|| {
            format!(
                "Failed to write stored containerfile: {}",
                artifact_dir.join("Containerfile").display()
            )
        })?;

        stage("Building runtime container image");
        detail(&format!("tag {local_tag}"));
        runtime.build(&local_tag, temp_context.path(), &temp_containerfile)?;
        success("Runtime container image built.");

        stage("Saving artifact archive");
        detail(&archive_path.display().to_string());
        let archive_spinner = spinner("Saving local image archive...");
        runtime.save(&local_tag, &archive_path)?;
        archive_spinner.finish_with_message("Local image archive saved.");

        save_metadata(&artifact_dir, &metadata)?;

        Ok(StoredArtifact {
            dir: artifact_dir,
            metadata,
        })
    }
}

fn build_binary_in_container(
    runtime: ContainerRuntime,
    plan: &ContainerizedBuildPlan,
    crate_name: &str,
    profile: &str,
    features: &[String],
    binary_name: &str,
) -> Result<PathBuf> {
    stage("Ensuring builder base image");
    detail(&format!(
        "builder {} · runtime {}",
        plan.builder_base_image, plan.runtime_base_image
    ));
    let base_pulled = plan.ensure_builder_base_image(runtime)?;
    success(if base_pulled {
        "Builder base image pulled."
    } else {
        "Builder base image cache hit."
    });

    stage("Preparing cached builder image");
    detail(&plan.builder_image_tag);
    let image_built = plan.ensure_builder_image(runtime)?;
    success(if image_built {
        "Builder image prepared."
    } else {
        "Builder image cache hit."
    });

    stage("Building crate inside the builder container");
    detail(&format!(
        "{crate_name} · profile {profile}{}",
        stage_features_suffix(features)
    ));
    plan.build_binary(runtime, profile, features, binary_name)?;
    success(&format!(
        "Containerized cargo build finished for `{crate_name}`."
    ));
    Ok(plan.host_binary_path.clone())
}

fn build_binary_on_host(
    crate_dir: &Path,
    manifest_path: &Path,
    target_directory: &Path,
    crate_name: &str,
    profile: &str,
    features: &[String],
    binary_name: &str,
) -> Result<PathBuf> {
    if std::env::consts::OS != "linux" {
        bail!(
            "`arca build rust --host-build` currently requires a Linux host because it packages the locally built ELF binary into a Linux container."
        );
    }

    stage("Building crate on the host");
    detail(&format!(
        "{crate_name} · profile {profile}{}",
        stage_features_suffix(features)
    ));
    let mut command = Command::new("cargo");
    command.current_dir(crate_dir);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg("--profile")
        .arg(profile)
        .arg("--bin")
        .arg(binary_name);
    if !features.is_empty() {
        command.arg("--features").arg(features.join(","));
    }
    run_command_status_streaming(&mut command, "build the Rust crate on the host")?;
    success(&format!("Host cargo build finished for `{crate_name}`."));

    let binary_path = binary_output_path(target_directory, profile, binary_name);
    if !binary_path.is_file() {
        bail!(
            "Cargo build completed but the expected binary is missing: {}",
            binary_path.display()
        );
    }
    Ok(binary_path)
}

fn resolve_manifest_path(path: &Path) -> Result<PathBuf> {
    let manifest = if path.is_dir() {
        path.join("Cargo.toml")
    } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
        path.to_path_buf()
    } else {
        bail!(
            "Expected a crate directory or Cargo.toml path, but got `{}`.",
            path.display()
        );
    };
    if !manifest.is_file() {
        bail!("No Cargo.toml found at `{}`.", manifest.display());
    }
    manifest.canonicalize().with_context(|| {
        format!(
            "Failed to canonicalize manifest path: {}",
            manifest.display()
        )
    })
}

fn load_cargo_metadata(manifest_path: &Path) -> Result<CargoMetadata> {
    let mut command = Command::new("cargo");
    command.current_dir(
        manifest_path
            .parent()
            .ok_or_else(|| anyhow!("Cargo.toml has no parent directory"))?,
    );
    command.args(["metadata", "--format-version", "1", "--manifest-path"]);
    command.arg(manifest_path);
    let output = run_command_output(&mut command, "read cargo metadata")?;
    let stdout = String::from_utf8(output.stdout).context("Cargo metadata output was not UTF-8")?;
    serde_json::from_str(&stdout).context("Failed to parse cargo metadata JSON")
}

fn package_for_manifest<'a>(
    metadata: &'a CargoMetadata,
    manifest_path: &Path,
) -> Result<&'a CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| paths_match(&package.manifest_path, manifest_path))
        .ok_or_else(|| {
            anyhow!(
                "Path `{}` is not a crate manifest. It may be a virtual workspace manifest.",
                manifest_path.display()
            )
        })
}

fn resolve_profile(args: &RustArgs, loaded_config: &ProjectConfig) -> Result<String> {
    if let Some(profile) = args.profile.as_deref() {
        return nonempty_trimmed(profile).ok_or_else(|| anyhow!("Cargo profile cannot be empty."));
    }
    if let Some(profile) = loaded_config.rust.profile.as_deref() {
        return Ok(profile.to_owned());
    }
    bail!(
        "Missing cargo profile. Pass `--profile PROFILE`, or rerun once with `--profile PROFILE --default` to save it in `.arca/config.toml`."
    )
}

fn resolve_features(args: &RustArgs, loaded_config: &ProjectConfig) -> Result<Vec<String>> {
    if let Some(features) = args.features.as_ref() {
        return Ok(normalize_features(features.iter().cloned()));
    }
    if let Some(features) = loaded_config.rust.features.as_ref() {
        return Ok(features.clone());
    }
    bail!(
        "Missing cargo features. Pass `-F/--features`, or rerun once with `-F/--features ... --default` to save them in `.arca/config.toml`. Use `--features ''` to save an empty feature set."
    )
}

fn resolve_binary_name(
    args: &RustArgs,
    loaded_config: &ProjectConfig,
    binary_targets: &[String],
) -> Result<String> {
    if let Some(bin) = args.bin.as_deref() {
        return validate_binary_choice(bin, binary_targets);
    }
    if let Some(bin) = loaded_config.rust.bin.as_deref() {
        return validate_binary_choice(bin, binary_targets);
    }
    if binary_targets.len() == 1 {
        return Ok(binary_targets[0].clone());
    }
    bail!(
        "Crate exposes multiple binaries. Pass `--bin NAME`, or rerun once with `--bin NAME --default` to save it in `.arca/config.toml`."
    )
}

fn resolve_base_image(args: &RustArgs, loaded_config: &ProjectConfig) -> Result<String> {
    if let Some(base_image) = args.base_image.as_deref() {
        return nonempty_trimmed(base_image)
            .ok_or_else(|| anyhow!("Runtime base image cannot be empty."));
    }
    if let Some(base_image) = loaded_config.rust.base_image.as_deref() {
        return nonempty_trimmed(base_image)
            .ok_or_else(|| anyhow!("Runtime base image cannot be empty."));
    }
    bail!(
        "Missing runtime base image. Pass `--base-image IMAGE`, or rerun once with `--base-image IMAGE --default` to save it in `.arca/config.toml`."
    )
}

fn validate_binary_choice(choice: &str, binary_targets: &[String]) -> Result<String> {
    binary_targets
        .iter()
        .find(|target| target.as_str() == choice)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "Binary target `{choice}` does not exist. Available binaries: {}",
                binary_targets.join(", ")
            )
        })
}

fn normalize_features(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut features = Vec::new();
    for value in values {
        for feature in parse_feature_string(&value) {
            if !features.iter().any(|existing| existing == &feature) {
                features.push(feature);
            }
        }
    }
    features
}

fn parse_feature_string(value: &str) -> Vec<String> {
    value
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .filter_map(nonempty_trimmed)
        .collect()
}

fn nonempty_trimmed(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn stage_features_suffix(features: &[String]) -> String {
    if features.is_empty() {
        String::new()
    } else {
        format!(" · features {}", features.join(","))
    }
}

fn binary_output_path(target_directory: &Path, profile: &str, binary_name: &str) -> PathBuf {
    target_directory
        .join(cargo_profile_output_dir(profile))
        .join(binary_name)
}

pub(super) fn cargo_profile_output_dir(profile: &str) -> &str {
    match profile {
        "dev" => "debug",
        _ => profile,
    }
}

pub(crate) fn builder_image_tag_for_runtime_image(runtime_base_image: &str) -> String {
    let builder_base_image = resolve_builder_base_image(runtime_base_image);
    format!(
        "{BUILDER_IMAGE_PREFIX}:{}",
        builder_hash_for_base_image(&builder_base_image)
    )
}

fn render_builder_containerfile(base_image: &str) -> String {
    format!(
        "FROM {base_image}\n\
RUN set -eux; \\\n\
    if command -v apt-get >/dev/null 2>&1; then \\\n\
      apt-get update; \\\n\
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends bash build-essential ca-certificates curl git pkg-config; \\\n\
      rm -rf /var/lib/apt/lists/*; \\\n\
    elif command -v dnf >/dev/null 2>&1; then \\\n\
      dnf install -y bash gcc gcc-c++ make ca-certificates curl git pkgconf-pkg-config; \\\n\
      dnf clean all; \\\n\
    elif command -v microdnf >/dev/null 2>&1; then \\\n\
      microdnf install -y bash gcc gcc-c++ make ca-certificates curl git pkgconf-pkg-config; \\\n\
      microdnf clean all; \\\n\
    elif command -v apk >/dev/null 2>&1; then \\\n\
      apk add --no-cache bash build-base ca-certificates curl git pkgconf; \\\n\
    else \\\n\
      echo 'Unsupported base image package manager for arca builder.' >&2; \\\n\
      exit 1; \\\n\
    fi\n\
ENV CARGO_HOME=/opt/arca/cargo-home\n\
ENV RUSTUP_HOME=/opt/arca/rustup\n\
ENV PATH=/opt/arca/cargo-home/bin:$PATH\n\
RUN mkdir -p \"$CARGO_HOME\" \"$RUSTUP_HOME\"\n\
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path\n"
    )
}

fn resolve_builder_base_image(runtime_base_image: &str) -> String {
    let Some((repository, tag)) = runtime_base_image.rsplit_once(':') else {
        return runtime_base_image.to_owned();
    };
    if repository != "nvidia/cuda" || tag.contains("-devel-") {
        return runtime_base_image.to_owned();
    }
    if let Some((left, right)) = tag.split_once("-base-") {
        return format!("{repository}:{left}-devel-{right}");
    }
    if let Some((left, right)) = tag.split_once("-runtime-") {
        return format!("{repository}:{left}-devel-{right}");
    }
    runtime_base_image.to_owned()
}

fn builder_hash_for_base_image(builder_base_image: &str) -> String {
    short_hash(&[
        BUILDER_RECIPE_VERSION.to_owned(),
        builder_base_image.to_owned(),
    ])
}

fn sync_host_cargo_home(destination: &Path) -> Result<()> {
    fs::create_dir_all(destination).with_context(|| {
        format!(
            "Failed to create cargo cache directory: {}",
            destination.display()
        )
    })?;
    fs::create_dir_all(destination.join("registry")).with_context(|| {
        format!(
            "Failed to create cargo registry cache directory: {}",
            destination.join("registry").display()
        )
    })?;
    fs::create_dir_all(destination.join("git")).with_context(|| {
        format!(
            "Failed to create cargo git cache directory: {}",
            destination.join("git").display()
        )
    })?;

    let Some(home_dir) = dirs::home_dir() else {
        return Err(anyhow!("Failed to determine home directory"));
    };
    let source_home = home_dir.join(".cargo");
    for file_name in ["config.toml", "config", "credentials.toml", "credentials"] {
        let source = source_home.join(file_name);
        let destination_path = destination.join(file_name);
        if source.is_file() {
            fs::copy(&source, &destination_path).with_context(|| {
                format!(
                    "Failed to sync cargo home file from {} to {}",
                    source.display(),
                    destination_path.display()
                )
            })?;
        } else if destination_path.exists() {
            fs::remove_file(&destination_path).with_context(|| {
                format!(
                    "Failed to remove stale cargo home file: {}",
                    destination_path.display()
                )
            })?;
        }
    }
    Ok(())
}

fn source_mount_root(
    workspace_root: &Path,
    crate_dir: &Path,
    local_dependency_dirs: &[PathBuf],
) -> Result<PathBuf> {
    let mut directories = vec![workspace_root.to_path_buf(), crate_dir.to_path_buf()];
    directories.extend(local_dependency_dirs.iter().cloned());
    for directory in crate_dir.ancestors() {
        let cargo_dir = directory.join(".cargo");
        if cargo_dir.join("config.toml").is_file() || cargo_dir.join("config").is_file() {
            directories.push(directory.to_path_buf());
        }
    }
    common_ancestor(&directories)
}

fn common_ancestor(paths: &[PathBuf]) -> Result<PathBuf> {
    let mut components = paths
        .iter()
        .map(|path| {
            path.canonicalize()
                .with_context(|| format!("Failed to canonicalize path: {}", path.display()))
        })
        .collect::<Result<Vec<_>>>()?;
    let first = components
        .drain(..1)
        .next()
        .ok_or_else(|| anyhow!("Cannot compute a common ancestor for an empty path set."))?;
    let mut prefix = first.components().collect::<Vec<_>>();
    for path in components {
        let path_components = path.components().collect::<Vec<_>>();
        let shared = prefix
            .iter()
            .zip(&path_components)
            .take_while(|(left, right)| left == right)
            .count();
        prefix.truncate(shared);
    }
    if prefix.is_empty() {
        return Err(anyhow!(
            "Could not determine a common source root for the Rust build."
        ));
    }
    let mut root = PathBuf::new();
    for component in prefix {
        root.push(component.as_os_str());
    }
    Ok(root)
}

fn container_relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).with_context(|| {
        format!(
            "Path `{}` is not under source mount root `{}`.",
            path.display(),
            root.display()
        )
    })?;
    Ok(relative.display().to_string())
}

fn normalized_features(features: &[String]) -> Vec<String> {
    let mut values = features.to_vec();
    values.sort();
    values.dedup();
    values
}

fn short_hash(values: &[String]) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn render_containerfile(metadata: &ArtifactMetadata) -> Result<String> {
    let entrypoint =
        serde_json::to_string(&vec![format!("/usr/local/bin/{}", metadata.binary_name)])
            .context("Failed to serialize container entrypoint")?;
    let label_lines = arca_image_labels(metadata)
        .into_iter()
        .map(|(key, value)| {
            serde_json::to_string(&value)
                .map(|encoded| format!("LABEL {key}={encoded}\n"))
                .context("Failed to serialize image label value")
        })
        .collect::<Result<Vec<_>>>()?
        .join("");
    Ok(format!(
        "FROM {}\n\
RUN set -eux; \\\n\
    if command -v apt-get >/dev/null 2>&1; then \\\n\
      apt-get update; \\\n\
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates; \\\n\
      rm -rf /var/lib/apt/lists/*; \\\n\
    elif command -v dnf >/dev/null 2>&1; then \\\n\
      dnf install -y ca-certificates; \\\n\
      dnf clean all; \\\n\
    elif command -v microdnf >/dev/null 2>&1; then \\\n\
      microdnf install -y ca-certificates; \\\n\
      microdnf clean all; \\\n\
    elif command -v apk >/dev/null 2>&1; then \\\n\
      apk add --no-cache ca-certificates; \\\n\
    fi\n\
COPY --chmod=755 {} /usr/local/bin/{}\n\
{label_lines}\
ENTRYPOINT {entrypoint}\n",
        metadata.base_image, metadata.binary_name, metadata.binary_name
    ))
}

fn compute_build_fingerprint(
    metadata: &CargoMetadata,
    package: &CargoPackage,
    crate_dir: &Path,
    local_dependency_dirs: &[PathBuf],
    binary_name: &str,
    profile: &str,
    features: &[String],
    base_image: &str,
    build_mode: &RustBuildMode,
) -> Result<String> {
    let mut hasher = Sha256::new();
    hash_text(&mut hasher, "arca-rust-build-v3");
    hash_text(&mut hasher, &metadata.workspace_root.display().to_string());
    hash_text(&mut hasher, &package.name);
    hash_text(&mut hasher, &package.version);
    hash_text(&mut hasher, binary_name);
    hash_text(&mut hasher, profile);
    let mut sorted_features = features.to_vec();
    sorted_features.sort();
    for feature in sorted_features {
        hash_text(&mut hasher, &feature);
    }
    hash_text(&mut hasher, base_image);
    match build_mode {
        RustBuildMode::Container(plan) => {
            hash_text(&mut hasher, "container-build");
            hash_text(&mut hasher, &plan.fingerprint_key());
        }
        RustBuildMode::Host { .. } => {
            hash_text(&mut hasher, "host-build");
            hash_text(&mut hasher, &rustc_version_text()?);
        }
    }
    for path in build_input_paths(&metadata.workspace_root, crate_dir, local_dependency_dirs) {
        hash_path(&mut hasher, &path)?;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn build_input_paths(
    workspace_root: &Path,
    crate_dir: &Path,
    local_dependency_dirs: &[PathBuf],
) -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    for directory in local_dependency_dirs {
        paths.insert(directory.clone());
    }
    let lockfile = workspace_root.join("Cargo.lock");
    if lockfile.is_file() {
        paths.insert(lockfile);
    }
    for path in cargo_config_paths(crate_dir) {
        if path.is_file() {
            paths.insert(path);
        }
    }
    paths.into_iter().collect()
}

fn local_dependency_directories(
    metadata: &CargoMetadata,
    package_id: &str,
) -> Result<Vec<PathBuf>> {
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| anyhow!("Cargo metadata did not include a resolve graph."))?;
    let packages = metadata
        .packages
        .iter()
        .map(|package| (package.id.as_str(), package))
        .collect::<HashMap<_, _>>();
    let nodes = resolve
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let mut directories = BTreeSet::new();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::from([package_id.to_owned()]);

    while let Some(package_id) = queue.pop_front() {
        if !visited.insert(package_id.clone()) {
            continue;
        }
        let package = packages
            .get(package_id.as_str())
            .ok_or_else(|| anyhow!("Missing package `{package_id}` in cargo metadata."))?;
        if package.source.is_none()
            && let Some(directory) = package.manifest_path.parent()
        {
            directories.insert(directory.to_path_buf());
        }
        if let Some(node) = nodes.get(package_id.as_str()) {
            queue.extend(node.dependencies.iter().cloned());
        }
    }

    Ok(directories.into_iter().collect())
}

fn cargo_config_paths(crate_dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut current = Some(crate_dir);
    while let Some(directory) = current {
        let cargo_dir = directory.join(".cargo");
        paths.push(cargo_dir.join("config.toml"));
        paths.push(cargo_dir.join("config"));
        current = directory.parent();
    }
    if let Some(home) = dirs::home_dir() {
        let cargo_dir = home.join(".cargo");
        paths.push(cargo_dir.join("config.toml"));
        paths.push(cargo_dir.join("config"));
    }
    paths
}

fn rustc_version_text() -> Result<String> {
    let mut command = Command::new("rustc");
    command.arg("-Vv");
    run_command_text(&mut command, "read the Rust toolchain version")
}

fn hash_path(hasher: &mut Sha256, path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Failed to read build input metadata: {}", path.display()))?;
    if metadata.is_dir() {
        hash_directory(hasher, path, path)?;
        return Ok(());
    }
    hash_entry(hasher, path, path, &metadata)
}

fn hash_directory(hasher: &mut Sha256, root: &Path, directory: &Path) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| {
            format!(
                "Failed to read build input directory: {}",
                directory.display()
            )
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("Failed to enumerate build inputs: {}", directory.display()))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("Failed to read build input metadata: {}", path.display()))?;
        if should_skip_hashed_path(root, &path) {
            continue;
        }
        hash_entry(hasher, root, &path, &metadata)?;
        if metadata.is_dir() {
            hash_directory(hasher, root, &path)?;
        }
    }
    Ok(())
}

fn hash_entry(
    hasher: &mut Sha256,
    root: &Path,
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<()> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    if metadata.is_dir() {
        hash_text(hasher, "dir");
        hash_text(hasher, &relative.display().to_string());
        return Ok(());
    }
    if metadata.is_file() {
        hash_text(hasher, "file");
        hash_text(hasher, &relative.display().to_string());
        hasher.update(
            fs::read(path)
                .with_context(|| format!("Failed to read build input file: {}", path.display()))?,
        );
        return Ok(());
    }
    if metadata.file_type().is_symlink() {
        hash_text(hasher, "symlink");
        hash_text(hasher, &relative.display().to_string());
        hash_text(
            hasher,
            &fs::read_link(path)
                .with_context(|| format!("Failed to read build input symlink: {}", path.display()))?
                .display()
                .to_string(),
        );
    }
    Ok(())
}

fn should_skip_hashed_path(root: &Path, path: &Path) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative.components().any(|component| {
        component.as_os_str() == OsStr::new("target")
            || component.as_os_str() == OsStr::new(".git")
            || component.as_os_str() == OsStr::new(".arca")
    })
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn now_epoch_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before the Unix epoch")?
        .as_millis())
}

fn paths_match(left: &Path, right: &Path) -> bool {
    left == right
        || left
            .canonicalize()
            .ok()
            .zip(right.canonicalize().ok())
            .is_some_and(|(left, right)| left == right)
}

#[cfg(test)]
mod builder_tests {
    use super::cargo_profile_output_dir;
    use super::{
        builder_image_tag_for_runtime_image, common_ancestor, normalized_features,
        resolve_builder_base_image, short_hash,
    };
    use tempfile::TempDir;

    #[test]
    fn normalized_features_sort_and_dedup() {
        assert_eq!(
            normalized_features(&["http".to_owned(), "cli".to_owned(), "http".to_owned()]),
            vec!["cli", "http"]
        );
    }

    #[test]
    fn profile_output_dir_maps_dev_to_debug() {
        assert_eq!(cargo_profile_output_dir("dev"), "debug");
        assert_eq!(cargo_profile_output_dir("release-lto"), "release-lto");
    }

    #[test]
    fn common_ancestor_finds_shared_prefix() {
        let root = TempDir::new().expect("tempdir should exist");
        std::fs::create_dir_all(root.path().join("demo/a/b")).expect("test tree should exist");
        std::fs::create_dir_all(root.path().join("demo/a/c")).expect("test tree should exist");
        let ancestor = common_ancestor(&[
            root.path().join("demo/a/b"),
            root.path().join("demo/a/c"),
            root.path().join("demo/a"),
        ])
        .expect("common ancestor should resolve");
        assert_eq!(ancestor, root.path().join("demo/a"));
    }

    #[test]
    fn nvidia_cuda_runtime_images_promote_to_matching_devel_builder() {
        assert_eq!(
            resolve_builder_base_image("nvidia/cuda:12.8.1-base-ubuntu24.04"),
            "nvidia/cuda:12.8.1-devel-ubuntu24.04"
        );
        assert_eq!(
            resolve_builder_base_image("nvidia/cuda:13.1.1-runtime-ubuntu24.04"),
            "nvidia/cuda:13.1.1-devel-ubuntu24.04"
        );
        assert_eq!(
            resolve_builder_base_image("nvidia/cuda:13.1.1-devel-ubuntu24.04"),
            "nvidia/cuda:13.1.1-devel-ubuntu24.04"
        );
    }

    #[test]
    fn non_nvidia_runtime_images_reuse_the_runtime_as_builder_base() {
        assert_eq!(resolve_builder_base_image("ubuntu:24.04"), "ubuntu:24.04");
    }

    #[test]
    fn builder_image_tag_tracks_the_derived_builder_base_image() {
        assert_eq!(
            builder_image_tag_for_runtime_image("nvidia/cuda:12.8.1-runtime-ubuntu24.04"),
            builder_image_tag_for_runtime_image("nvidia/cuda:12.8.1-base-ubuntu24.04")
        );
        assert_ne!(
            builder_image_tag_for_runtime_image("nvidia/cuda:12.8.1-base-ubuntu24.04"),
            builder_image_tag_for_runtime_image("nvidia/cuda:13.1.1-base-ubuntu24.04")
        );
    }

    #[test]
    fn build_cache_key_changes_with_builder_base_image() {
        let cache_a = short_hash(&[
            "arca-rust-build-cache-v2".to_owned(),
            "/tmp/demo/Cargo.toml".to_owned(),
            "demo".to_owned(),
            "demo".to_owned(),
            "dev".to_owned(),
            String::new(),
            "nvidia/cuda:12.8.1-devel-ubuntu24.04".to_owned(),
        ]);
        let cache_b = short_hash(&[
            "arca-rust-build-cache-v2".to_owned(),
            "/tmp/demo/Cargo.toml".to_owned(),
            "demo".to_owned(),
            "demo".to_owned(),
            "dev".to_owned(),
            String::new(),
            "nvidia/cuda:13.1.1-devel-ubuntu24.04".to_owned(),
        ]);
        assert_ne!(cache_a, cache_b);
    }
}

#[cfg(test)]
mod tests {
    use super::{binary_output_path, build_input_paths, render_containerfile};
    use super::{cargo_profile_output_dir, parse_feature_string};
    use crate::artifact::ArtifactMetadata;
    use std::path::{Path, PathBuf};

    #[test]
    fn parse_feature_string_accepts_commas_and_spaces() {
        assert_eq!(
            parse_feature_string("cli,http tracing"),
            vec!["cli", "http", "tracing"]
        );
    }

    #[test]
    fn cargo_profile_output_dir_maps_dev_to_debug() {
        assert_eq!(cargo_profile_output_dir("dev"), "debug");
        assert_eq!(cargo_profile_output_dir("release-lto"), "release-lto");
    }

    #[test]
    fn binary_output_path_uses_profile_output_dir() {
        assert_eq!(
            binary_output_path(Path::new("/tmp/target"), "dev", "demo"),
            PathBuf::from("/tmp/target/debug/demo")
        );
    }

    #[test]
    fn build_input_paths_include_lockfile_and_configs() {
        let paths = build_input_paths(
            Path::new("/tmp/workspace"),
            Path::new("/tmp/workspace/crate"),
            &[
                PathBuf::from("/tmp/workspace/crate"),
                PathBuf::from("/tmp/workspace/dep"),
            ],
        );
        assert!(paths.contains(&PathBuf::from("/tmp/workspace/crate")));
        assert!(paths.contains(&PathBuf::from("/tmp/workspace/dep")));
    }

    #[test]
    fn runtime_containerfile_caches_certificates_before_metadata_labels() {
        let containerfile = render_containerfile(&ArtifactMetadata {
            schema_version: 3,
            artifact_id: "deadbeef".to_owned(),
            remote_tag: "demo-deadbeef".to_owned(),
            build_fingerprint: "fingerprint".to_owned(),
            kind: "rust".to_owned(),
            created_at_epoch_ms: 1,
            crate_name: "demo".to_owned(),
            crate_version: "0.1.0".to_owned(),
            binary_name: "demo".to_owned(),
            source_path: "/tmp/demo".to_owned(),
            manifest_path: "/tmp/demo/Cargo.toml".to_owned(),
            cargo_profile: "dev".to_owned(),
            cargo_features: Vec::new(),
            base_image: "ubuntu:24.04".to_owned(),
            runtime: "docker".to_owned(),
            local_tag: "arca-local:demo-deadbeef".to_owned(),
            archive_file: "image.tar".to_owned(),
            uploaded_ref: None,
            uploaded_at_epoch_ms: None,
        })
        .expect("render containerfile");

        let certs = containerfile
            .find("RUN set -eux;")
            .expect("ca-certificates layer");
        let copy = containerfile
            .find("COPY --chmod=755 demo /usr/local/bin/demo")
            .expect("copy instruction");
        let labels = containerfile
            .find("LABEL io.khoek.arca.schema-version=")
            .expect("labels");

        assert!(certs < copy);
        assert!(copy < labels);
        assert!(!containerfile.contains("RUN chmod"));
    }
}
