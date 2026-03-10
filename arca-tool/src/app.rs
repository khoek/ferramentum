use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};

use crate::artifact::{
    ArtifactSummary, StoredArtifact, load_stored_artifacts, resolve_artifact, save_metadata,
};
use crate::backend::GeneratorBackend;
use crate::backend::rust::{RustBackend, builder_image_tag_for_runtime_image};
use crate::cli::{BuildCommands, Cli, Commands, PruneArgs, PruneTarget};
use crate::config::load_global_config;
use crate::gcp::{
    RemoteArtifact, RemoteArtifactEvent, console_url_for_remote_ref, delete_remote_artifact,
    is_publish_configured, load_remote_artifacts, login, print_login_outcome, push_artifact,
};
use crate::runtime::ContainerRuntime;
use crate::ui::{
    Color, RenderTarget, spinner, stderr_is_interactive, stderr_render_target,
    stdout_render_target, warn,
};

const FIELD_SEPARATOR: &str = " · ";
const LIST_INDENT: &str = "    ";

enum ListedArtifactDetails {
    Known(ArtifactSummary),
    RemoteOnlyMinimal {
        artifact_id: String,
        uploaded_at_epoch_ms: u128,
        uploaded_at_text: String,
    },
}

struct ListedArtifact {
    details: ListedArtifactDetails,
    has_local: bool,
    uploaded_ref: Option<String>,
    remote_ref: Option<String>,
    builder_status: BuilderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuilderStatus {
    Present,
    Missing,
    Unknown,
}

pub fn run(cli: Cli) -> Result<()> {
    let _instance_lock = capulus::acquire("arca", true)?;
    match cli.command {
        Commands::Login(args) => cmd_login(args),
        Commands::Build(command) => cmd_build(command),
        Commands::Push(args) => cmd_push(args),
        Commands::List => cmd_list(),
        Commands::Prune(args) => cmd_prune(args),
    }
}

fn cmd_build(command: BuildCommands) -> Result<()> {
    match command {
        BuildCommands::Rust(args) => cmd_rust(args),
    }
}

fn cmd_rust(args: crate::cli::RustArgs) -> Result<()> {
    let runtime = ContainerRuntime::detect()?;
    runtime.ensure_build_available()?;
    let backend = RustBackend::from_args(args)?;
    let artifact = backend.build(runtime)?;
    println!("Built `{}` locally.", artifact.metadata.crate_name);
    print_artifact_summary(&artifact);
    Ok(())
}

fn cmd_list() -> Result<()> {
    let config = load_global_config()?;
    let local_artifacts = load_stored_artifacts()?;
    let runtime = ContainerRuntime::detect().ok();
    if stderr_is_interactive() {
        return cmd_list_interactive(&config, &local_artifacts, runtime);
    }

    let remote_artifacts = load_remote_artifacts_or_warn(&config, &local_artifacts);
    print_listed_artifacts(&merge_listed_artifacts(
        &local_artifacts,
        &remote_artifacts,
        runtime,
    ))
}

fn cmd_list_interactive(
    config: &crate::config::ArcaConfig,
    local_artifacts: &[StoredArtifact],
    runtime: Option<ContainerRuntime>,
) -> Result<()> {
    let progress = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
    let mut rows = HashMap::<String, ProgressBar>::new();
    let local_builder_statuses = builder_statuses_for_summaries(
        runtime,
        local_artifacts
            .iter()
            .map(|artifact| ArtifactSummary::from(&artifact.metadata)),
    );

    for artifact in local_artifacts {
        let row = progress.add(new_list_row_spinner());
        row.set_message(render_pending_local_row(
            artifact,
            *local_builder_statuses
                .get(artifact.metadata.artifact_id.as_str())
                .unwrap_or(&BuilderStatus::Unknown),
        ));
        activate_list_row_spinner(&row);
        rows.insert(artifact.metadata.artifact_id.clone(), row);
    }

    let footer = progress.add(new_list_row_spinner());
    footer.set_message("Loading remote artifact state...".to_owned());
    activate_list_row_spinner(&footer);
    let spacer = progress.insert_before(&footer, new_list_spacer());
    finish_list_spacer(&spacer);

    let remote_artifacts = if is_publish_configured(config) {
        match load_remote_artifacts(config, local_artifacts, |event| match event {
            RemoteArtifactEvent::InventoryLoaded {
                matched_artifacts,
                pending_remote_only,
            } => {
                let matched_ids = matched_artifacts
                    .iter()
                    .map(|artifact| artifact.artifact_id.as_str())
                    .collect::<HashSet<_>>();
                for artifact in local_artifacts {
                    if let Some(row) = rows.get(&artifact.metadata.artifact_id) {
                        finish_list_row(
                            row,
                            &render_final_local_row(
                                artifact,
                                matched_artifacts.iter().find(|remote| {
                                    remote.artifact_id == artifact.metadata.artifact_id
                                }),
                                matched_ids.contains(artifact.metadata.artifact_id.as_str()),
                                *local_builder_statuses
                                    .get(artifact.metadata.artifact_id.as_str())
                                    .unwrap_or(&BuilderStatus::Unknown),
                            ),
                        );
                    }
                }
                for artifact in &pending_remote_only {
                    let row = progress.insert_before(&spacer, new_list_row_spinner());
                    row.set_message(render_pending_remote_only_row(artifact));
                    activate_list_row_spinner(&row);
                    rows.insert(artifact.artifact_id.clone(), row);
                }
                if !pending_remote_only.is_empty() {
                    footer.set_message(format!(
                        "Resolving metadata for {} remote-only artifact(s)... 0/{}",
                        pending_remote_only.len(),
                        pending_remote_only.len()
                    ));
                }
            }
            RemoteArtifactEvent::RemoteOnlyResolved {
                artifact,
                resolved,
                total,
            } => {
                if let Some(row) = rows.get(&artifact.artifact_id) {
                    finish_list_row(row, &render_final_remote_only_row(&artifact, runtime));
                }
                footer.set_message(format!(
                    "Resolving metadata for {total} remote-only artifact(s)... {resolved}/{total}"
                ));
            }
        }) {
            Ok(artifacts) => artifacts,
            Err(err) => {
                for artifact in local_artifacts {
                    if let Some(row) = rows.get(&artifact.metadata.artifact_id) {
                        finish_list_row(
                            row,
                            &render_remote_unverified_row(
                                artifact,
                                *local_builder_statuses
                                    .get(artifact.metadata.artifact_id.as_str())
                                    .unwrap_or(&BuilderStatus::Unknown),
                            ),
                        );
                    }
                }
                clear_list_spacer(&progress, &spacer);
                footer.finish_with_message(format!("Remote lookup failed: {err:#}"));
                return Ok(());
            }
        }
    } else {
        for artifact in local_artifacts {
            if let Some(row) = rows.get(&artifact.metadata.artifact_id) {
                finish_list_row(
                    row,
                    &render_final_local_row(
                        artifact,
                        None,
                        false,
                        *local_builder_statuses
                            .get(artifact.metadata.artifact_id.as_str())
                            .unwrap_or(&BuilderStatus::Unknown),
                    ),
                );
            }
        }
        Vec::new()
    };

    if local_artifacts.is_empty() && remote_artifacts.is_empty() {
        clear_list_spacer(&progress, &spacer);
        footer.finish_with_message("No `arca` artifacts found.");
    } else {
        clear_list_footer(&progress, &spacer, &footer);
    }
    Ok(())
}

fn cmd_login(args: crate::cli::LoginArgs) -> Result<()> {
    let runtime = ContainerRuntime::detect()?;
    let mut config = load_global_config()?;
    let outcome = login(&mut config, args.force, args.repo, runtime)?;
    print_login_outcome(&outcome);
    Ok(())
}

fn cmd_push(args: crate::cli::PushArgs) -> Result<()> {
    let runtime = ContainerRuntime::detect()?;
    let config = load_global_config()?;
    if !is_publish_configured(&config) {
        bail!("Registry push is not configured. Run `arca login` first.");
    }
    let mut artifacts = push_candidates(&config, args.artifact.as_deref())?;
    if artifacts.is_empty() {
        println!("No local artifacts need pushing.");
        return Ok(());
    }

    for artifact in &mut artifacts {
        let remote_ref = push_artifact(&config, runtime, artifact)?;
        println!(
            "Pushed `{}` as `{remote_ref}`.",
            artifact.metadata.artifact_id
        );
        print_artifact_summary(artifact);
    }
    Ok(())
}

fn cmd_prune(args: PruneArgs) -> Result<()> {
    let cutoff_epoch_ms = prune_cutoff_epoch_ms(&args)?;
    match args.target {
        PruneTarget::All => prune_all_artifacts(cutoff_epoch_ms),
        PruneTarget::Local => prune_local_artifacts(ContainerRuntime::detect()?, cutoff_epoch_ms),
        PruneTarget::Remote => prune_remote_artifacts(cutoff_epoch_ms),
    }
}

fn prune_all_artifacts(cutoff_epoch_ms: u128) -> Result<()> {
    let runtime = ContainerRuntime::detect()?;
    let config = load_global_config()?;
    let local_artifacts = load_stored_artifacts()?;
    let stale_local_artifacts = stale_local_artifacts(&local_artifacts, cutoff_epoch_ms);

    let stale_remote_artifacts = if is_publish_configured(&config) {
        load_stale_remote_artifacts(&config, &local_artifacts, cutoff_epoch_ms)?
    } else {
        println!("Skipping remote prune because registry push is not configured.");
        Vec::new()
    };

    prune_local_artifact_set(runtime, stale_local_artifacts)?;
    if !stale_remote_artifacts.is_empty() {
        prune_remote_artifact_set(&config, stale_remote_artifacts)?;
    } else if is_publish_configured(&config) {
        println!("No remote artifacts matched the requested age cutoff.");
    }
    Ok(())
}

fn print_artifact_summary(artifact: &StoredArtifact) {
    println!("Artifact ID: {}", artifact.metadata.artifact_id);
    println!(
        "Crate: {} ({})",
        artifact.metadata.crate_name, artifact.metadata.binary_name
    );
    println!(
        "Cargo: --profile {}{}",
        artifact.metadata.cargo_profile,
        features_suffix(&artifact.metadata.cargo_features)
    );
    println!("Base image: {}", artifact.metadata.base_image);
    println!("Archive: {}", artifact.archive_path().display());
    println!("Remote tag: {}", artifact.metadata.remote_tag);
    if let Some(uploaded_ref) = artifact.metadata.uploaded_ref.as_deref() {
        println!("Pushed: {uploaded_ref}");
    }
}

fn features_suffix(features: &[String]) -> String {
    if features.is_empty() {
        String::new()
    } else {
        format!(" --features {}", features.join(","))
    }
}

fn display_features(features: &[String]) -> String {
    if features.is_empty() {
        "default features".to_owned()
    } else {
        features.join(",")
    }
}

fn load_remote_artifacts_or_warn(
    config: &crate::config::ArcaConfig,
    local_artifacts: &[StoredArtifact],
) -> Vec<RemoteArtifact> {
    if !is_publish_configured(config) {
        return Vec::new();
    }
    match load_remote_artifacts(config, local_artifacts, |_| {}) {
        Ok(artifacts) => artifacts,
        Err(err) => {
            warn(&format!("Could not inspect remote artifacts: {err:#}"));
            Vec::new()
        }
    }
}

fn push_candidates(
    config: &crate::config::ArcaConfig,
    selector: Option<&str>,
) -> Result<Vec<StoredArtifact>> {
    if let Some(selector) = selector {
        return Ok(vec![resolve_artifact(Some(selector))?]);
    }

    let local_artifacts = load_stored_artifacts()?;
    if local_artifacts.is_empty() {
        return Ok(Vec::new());
    }

    let inventory_spinner = spinner("Loading remote artifact inventory...");
    let remote_artifacts = load_remote_artifacts(config, &local_artifacts, |_| {});
    let remote_artifacts = match remote_artifacts {
        Ok(remote_artifacts) => remote_artifacts,
        Err(err) => {
            inventory_spinner.finish_with_message("Remote artifact inventory failed.");
            return Err(err);
        }
    };
    inventory_spinner.finish_and_clear();

    let remote_ids = remote_artifacts
        .into_iter()
        .map(|artifact| artifact.artifact_id)
        .collect::<HashSet<_>>();

    Ok(local_artifacts
        .into_iter()
        .filter(|artifact| !remote_ids.contains(&artifact.metadata.artifact_id))
        .collect())
}

fn merge_listed_artifacts(
    local_artifacts: &[StoredArtifact],
    remote_artifacts: &[RemoteArtifact],
    runtime: Option<ContainerRuntime>,
) -> Vec<ListedArtifact> {
    let mut merged = HashMap::<String, ListedArtifact>::new();

    for artifact in local_artifacts {
        let summary = ArtifactSummary::from(&artifact.metadata);
        merged.insert(
            summary.artifact_id.clone(),
            ListedArtifact {
                details: ListedArtifactDetails::Known(summary),
                has_local: true,
                uploaded_ref: artifact.metadata.uploaded_ref.clone(),
                remote_ref: None,
                builder_status: BuilderStatus::Unknown,
            },
        );
    }

    for artifact in remote_artifacts {
        if let Some(existing) = merged.get_mut(&artifact.artifact_id) {
            existing.remote_ref = Some(artifact.remote_ref.clone());
            continue;
        }
        merged.insert(
            artifact.artifact_id.clone(),
            ListedArtifact {
                details: artifact
                    .summary
                    .clone()
                    .map(ListedArtifactDetails::Known)
                    .unwrap_or_else(|| ListedArtifactDetails::RemoteOnlyMinimal {
                        artifact_id: artifact.artifact_id.clone(),
                        uploaded_at_epoch_ms: artifact.uploaded_at_epoch_ms,
                        uploaded_at_text: artifact.uploaded_at_text.clone(),
                    }),
                has_local: false,
                uploaded_ref: None,
                remote_ref: Some(artifact.remote_ref.clone()),
                builder_status: BuilderStatus::Unknown,
            },
        );
    }

    let mut artifacts = merged.into_values().collect::<Vec<_>>();
    apply_builder_statuses(&mut artifacts, runtime);
    artifacts.sort_by(|left, right| {
        right
            .sort_epoch_ms()
            .cmp(&left.sort_epoch_ms())
            .then_with(|| left.artifact_id().cmp(right.artifact_id()))
    });
    artifacts
}

fn print_listed_artifacts(artifacts: &[ListedArtifact]) -> Result<()> {
    if artifacts.is_empty() {
        println!("No `arca` artifacts found.");
        return Ok(());
    }
    for artifact in artifacts {
        print_listed_artifact(artifact);
    }
    Ok(())
}

fn print_listed_artifact(artifact: &ListedArtifact) {
    let target = stdout_render_target();
    println!("{}", render_listed_artifact(artifact, &target));
}

fn profile_color(profile: &str) -> Color {
    match profile {
        "dev" => Color::Red,
        "release" => Color::Green,
        "release-lto" => Color::Blue,
        _ => Color::Yellow,
    }
}

fn new_list_row_spinner() -> ProgressBar {
    capulus::ui::new_list_row_spinner()
}

fn new_list_spacer() -> ProgressBar {
    capulus::ui::new_list_spacer()
}

fn activate_list_row_spinner(row: &ProgressBar) {
    capulus::ui::activate_list_row_spinner(row);
}

fn finish_list_spacer(spacer: &ProgressBar) {
    capulus::ui::finish_list_spacer(spacer);
}

fn finish_list_row(row: &ProgressBar, message: &str) {
    capulus::ui::finish_list_row(row, message);
}

fn render_pending_local_row(artifact: &StoredArtifact, builder_status: BuilderStatus) -> String {
    let profile = artifact.metadata.cargo_profile.as_str();
    let target = stderr_render_target();
    compose_list_row(
        &artifact.metadata.artifact_id,
        profile_color(profile),
        &["💻", "⏳"],
        vec![
            render_profile(profile, &target),
            display_features(&artifact.metadata.cargo_features),
            artifact.metadata.base_image.clone(),
            render_builder_status(builder_status, &target),
            "checking remote...".to_owned(),
        ],
        None,
        &target,
    )
}

fn render_final_local_row(
    artifact: &StoredArtifact,
    remote: Option<&RemoteArtifact>,
    is_present: bool,
    builder_status: BuilderStatus,
) -> String {
    let summary = ArtifactSummary::from(&artifact.metadata);
    let target = stderr_render_target();
    let remote_line = if is_present {
        remote
            .map(|artifact| ListRemoteLine::Ref(artifact.remote_ref.clone()))
            .or_else(|| {
                artifact
                    .metadata
                    .uploaded_ref
                    .clone()
                    .map(ListRemoteLine::Ref)
            })
    } else {
        Some(ListRemoteLine::Status {
            text: artifact
                .metadata
                .uploaded_ref
                .as_deref()
                .map(|remote_ref| format!("missing remotely (cached {remote_ref})"))
                .unwrap_or_else(|| "missing remotely".to_owned()),
            color: Color::Red,
        })
    };
    compose_list_row(
        &summary.artifact_id,
        profile_color(&summary.cargo_profile),
        if is_present {
            &["💻", "🌏"]
        } else {
            &["💻"]
        },
        vec![
            render_profile(&summary.cargo_profile, &target),
            display_features(&summary.cargo_features),
            summary.base_image,
            render_builder_status(builder_status, &target),
        ],
        remote_line,
        &target,
    )
}

fn render_pending_remote_only_row(artifact: &RemoteArtifact) -> String {
    let target = stderr_render_target();
    compose_list_row(
        &artifact.artifact_id,
        Color::Yellow,
        &["🌏"],
        vec![
            "remote-only".to_owned(),
            format!("uploaded {}", artifact.uploaded_at_text),
            "loading metadata...".to_owned(),
        ],
        None,
        &target,
    )
}

fn render_final_remote_only_row(
    artifact: &RemoteArtifact,
    runtime: Option<ContainerRuntime>,
) -> String {
    let target = stderr_render_target();
    if let Some(summary) = artifact.summary.as_ref() {
        let builder_status =
            builder_status_for_base_image(runtime, Some(summary.base_image.as_str()));
        return compose_list_row(
            &summary.artifact_id,
            profile_color(&summary.cargo_profile),
            &["🌏"],
            vec![
                render_profile(&summary.cargo_profile, &target),
                display_features(&summary.cargo_features),
                summary.base_image.clone(),
                render_builder_status(builder_status, &target),
            ],
            Some(ListRemoteLine::Ref(artifact.remote_ref.clone())),
            &target,
        );
    }
    compose_list_row(
        &artifact.artifact_id,
        Color::Yellow,
        &["🌏"],
        vec![
            "remote-only".to_owned(),
            format!("uploaded {}", artifact.uploaded_at_text),
        ],
        Some(ListRemoteLine::Ref(artifact.remote_ref.clone())),
        &target,
    )
}

fn render_remote_unverified_row(
    artifact: &StoredArtifact,
    builder_status: BuilderStatus,
) -> String {
    let summary = ArtifactSummary::from(&artifact.metadata);
    let target = stderr_render_target();
    compose_list_row(
        &summary.artifact_id,
        profile_color(&summary.cargo_profile),
        &["💻", "?"],
        vec![
            render_profile(&summary.cargo_profile, &target),
            display_features(&summary.cargo_features),
            summary.base_image,
            render_builder_status(builder_status, &target),
        ],
        Some(ListRemoteLine::Status {
            text: "remote status unavailable".to_owned(),
            color: Color::Yellow,
        }),
        &target,
    )
}

impl ListedArtifact {
    fn artifact_id(&self) -> &str {
        match &self.details {
            ListedArtifactDetails::Known(summary) => &summary.artifact_id,
            ListedArtifactDetails::RemoteOnlyMinimal { artifact_id, .. } => artifact_id,
        }
    }

    fn sort_epoch_ms(&self) -> u128 {
        match &self.details {
            ListedArtifactDetails::Known(summary) => summary.created_at_epoch_ms,
            ListedArtifactDetails::RemoteOnlyMinimal {
                uploaded_at_epoch_ms,
                ..
            } => *uploaded_at_epoch_ms,
        }
    }
}

enum ListRemoteLine {
    Ref(String),
    Status { text: String, color: Color },
}

fn render_listed_artifact(artifact: &ListedArtifact, target: &impl RenderTarget) -> String {
    match &artifact.details {
        ListedArtifactDetails::Known(summary) => {
            let mut markers = Vec::new();
            if artifact.has_local {
                markers.push("💻");
            }
            if artifact.remote_ref.is_some() {
                markers.push("🌏");
            }
            let remote_line = artifact
                .remote_ref
                .clone()
                .map(ListRemoteLine::Ref)
                .or_else(|| {
                    artifact.has_local.then(|| ListRemoteLine::Status {
                        text: artifact
                            .uploaded_ref
                            .as_deref()
                            .map(|remote_ref| format!("missing remotely (cached {remote_ref})"))
                            .unwrap_or_else(|| "missing remotely".to_owned()),
                        color: Color::Red,
                    })
                });
            compose_list_row(
                &summary.artifact_id,
                profile_color(&summary.cargo_profile),
                &markers,
                vec![
                    render_profile(&summary.cargo_profile, target),
                    display_features(&summary.cargo_features),
                    summary.base_image.clone(),
                    render_builder_status(artifact.builder_status, target),
                ],
                remote_line,
                target,
            )
        }
        ListedArtifactDetails::RemoteOnlyMinimal {
            artifact_id,
            uploaded_at_text,
            ..
        } => compose_list_row(
            artifact_id,
            Color::Yellow,
            &["🌏"],
            vec![
                "remote-only".to_owned(),
                format!("uploaded {uploaded_at_text}"),
            ],
            artifact.remote_ref.clone().map(ListRemoteLine::Ref),
            target,
        ),
    }
}

fn compose_list_row(
    artifact_id: &str,
    dot_color: Color,
    markers: &[&str],
    fields: Vec<String>,
    remote_line: Option<ListRemoteLine>,
    target: &impl RenderTarget,
) -> String {
    let mut parts = Vec::with_capacity(2 + fields.len());
    parts.push(artifact_id.to_owned());
    if !markers.is_empty() {
        parts.push(markers.concat());
    }
    parts.extend(fields);

    let bullet = target.paint("●", dot_color);
    let mut row = format!("{bullet} {}", parts.join(FIELD_SEPARATOR));
    if let Some(remote_line) = remote_line {
        row.push('\n');
        row.push_str(&render_list_remote_line(remote_line, target));
    }
    row
}

fn clear_list_spacer(progress: &MultiProgress, spacer: &ProgressBar) {
    capulus::ui::clear_progress_bar(progress, spacer);
}

fn clear_list_footer(progress: &MultiProgress, spacer: &ProgressBar, footer: &ProgressBar) {
    capulus::ui::clear_progress_bar(progress, spacer);
    capulus::ui::clear_progress_bar(progress, footer);
}

fn render_list_remote_line(remote_line: ListRemoteLine, target: &impl RenderTarget) -> String {
    match remote_line {
        ListRemoteLine::Ref(remote_ref) => {
            let console_url = console_url_for_remote_ref(&remote_ref);
            let rendered_remote_ref = target.hyperlink(&remote_ref, console_url.as_deref());
            let colored_remote_ref = target.paint(&rendered_remote_ref, Color::Cyan);
            format!("{LIST_INDENT}{colored_remote_ref}")
        }
        ListRemoteLine::Status { text, color } => {
            let colored_text = target.paint(&text, color);
            format!("{LIST_INDENT}{colored_text}")
        }
    }
}

fn render_profile(profile: &str, target: &impl RenderTarget) -> String {
    target.paint(profile, profile_color(profile))
}

fn render_builder_status(status: BuilderStatus, target: &impl RenderTarget) -> String {
    match status {
        BuilderStatus::Present => target.paint("builder ready", Color::Green),
        BuilderStatus::Missing => target.paint("builder missing", Color::Red),
        BuilderStatus::Unknown => target.paint("builder unknown", Color::Yellow),
    }
}

fn apply_builder_statuses(artifacts: &mut [ListedArtifact], runtime: Option<ContainerRuntime>) {
    let mut statuses = builder_statuses_for_summaries(
        runtime,
        artifacts
            .iter()
            .filter_map(|artifact| match &artifact.details {
                ListedArtifactDetails::Known(summary) => Some(summary.clone()),
                ListedArtifactDetails::RemoteOnlyMinimal { .. } => None,
            }),
    );
    for artifact in artifacts {
        artifact.builder_status = match &artifact.details {
            ListedArtifactDetails::Known(summary) => statuses
                .remove(summary.artifact_id.as_str())
                .unwrap_or(BuilderStatus::Unknown),
            ListedArtifactDetails::RemoteOnlyMinimal { .. } => BuilderStatus::Unknown,
        };
    }
}

fn builder_statuses_for_summaries(
    runtime: Option<ContainerRuntime>,
    summaries: impl IntoIterator<Item = ArtifactSummary>,
) -> HashMap<String, BuilderStatus> {
    let mut base_image_statuses = HashMap::<String, BuilderStatus>::new();
    let mut artifact_statuses = HashMap::new();
    for summary in summaries {
        let status = *base_image_statuses
            .entry(summary.base_image.clone())
            .or_insert_with(|| builder_status_for_base_image(runtime, Some(&summary.base_image)));
        artifact_statuses.insert(summary.artifact_id, status);
    }
    artifact_statuses
}

fn builder_status_for_base_image(
    runtime: Option<ContainerRuntime>,
    base_image: Option<&str>,
) -> BuilderStatus {
    let Some(runtime) = runtime else {
        return BuilderStatus::Unknown;
    };
    let Some(base_image) = base_image else {
        return BuilderStatus::Unknown;
    };
    match runtime.image_exists(&builder_image_tag_for_runtime_image(base_image)) {
        Ok(true) => BuilderStatus::Present,
        Ok(false) => BuilderStatus::Missing,
        Err(_) => BuilderStatus::Unknown,
    }
}

fn prune_cutoff_epoch_ms(args: &PruneArgs) -> Result<u128> {
    let age_ms = match (args.hours, args.days) {
        (Some(hours), None) => u128::from(hours) * 60 * 60 * 1_000,
        (None, Some(days)) => u128::from(days) * 24 * 60 * 60 * 1_000,
        _ => bail!("Pass exactly one of `--hours HOURS` or `--days DAYS`."),
    };
    let now_epoch_ms = now_epoch_ms()?;
    now_epoch_ms
        .checked_sub(age_ms)
        .ok_or_else(|| anyhow!("Requested prune age is older than the Unix epoch."))
}

fn prune_local_artifacts(runtime: ContainerRuntime, cutoff_epoch_ms: u128) -> Result<()> {
    let stale_artifacts = stale_local_artifacts(&load_stored_artifacts()?, cutoff_epoch_ms);
    prune_local_artifact_set(runtime, stale_artifacts)
}

fn stale_local_artifacts(
    artifacts: &[StoredArtifact],
    cutoff_epoch_ms: u128,
) -> Vec<StoredArtifact> {
    artifacts
        .iter()
        .filter(|artifact| artifact.metadata.created_at_epoch_ms < cutoff_epoch_ms)
        .cloned()
        .collect()
}

fn prune_local_artifact_set(
    runtime: ContainerRuntime,
    stale_artifacts: Vec<StoredArtifact>,
) -> Result<()> {
    if stale_artifacts.is_empty() {
        println!("No local artifacts matched the requested age cutoff.");
        return Ok(());
    }

    let prune_spinner = spinner(&format!(
        "Pruning {} local artifact(s)...",
        stale_artifacts.len()
    ));
    for artifact in stale_artifacts {
        if runtime.image_exists(&artifact.metadata.local_tag)? {
            runtime.remove_image(&artifact.metadata.local_tag)?;
        }
        fs::remove_dir_all(&artifact.dir).with_context(|| {
            format!(
                "Failed to remove local artifact directory: {}",
                artifact.dir.display()
            )
        })?;
    }
    prune_spinner.finish_with_message("Local artifact prune complete.");
    Ok(())
}

fn prune_remote_artifacts(cutoff_epoch_ms: u128) -> Result<()> {
    let config = load_global_config()?;
    if !is_publish_configured(&config) {
        bail!("Registry push is not configured. Run `arca login` first.");
    }

    let local_artifacts = load_stored_artifacts()?;
    let stale_remote_artifacts =
        load_stale_remote_artifacts(&config, &local_artifacts, cutoff_epoch_ms)?;
    prune_remote_artifact_set(&config, stale_remote_artifacts)
}

fn load_stale_remote_artifacts(
    config: &crate::config::ArcaConfig,
    local_artifacts: &[StoredArtifact],
    cutoff_epoch_ms: u128,
) -> Result<Vec<RemoteArtifact>> {
    let inventory_spinner = spinner("Loading remote artifact inventory...");
    let remote_artifacts = load_remote_artifacts(config, local_artifacts, |_| {});
    let remote_artifacts = match remote_artifacts {
        Ok(remote_artifacts) => remote_artifacts,
        Err(err) => {
            inventory_spinner.finish_with_message("Remote artifact inventory failed.");
            return Err(err);
        }
    };
    inventory_spinner.finish_and_clear();
    Ok(stale_remote_artifacts(&remote_artifacts, cutoff_epoch_ms))
}

fn stale_remote_artifacts(
    remote_artifacts: &[RemoteArtifact],
    cutoff_epoch_ms: u128,
) -> Vec<RemoteArtifact> {
    remote_artifacts
        .iter()
        .filter(|artifact| remote_created_at_epoch_ms(artifact) < cutoff_epoch_ms)
        .cloned()
        .collect()
}

fn prune_remote_artifact_set(
    config: &crate::config::ArcaConfig,
    stale_remote_artifacts: Vec<RemoteArtifact>,
) -> Result<()> {
    if stale_remote_artifacts.is_empty() {
        println!("No remote artifacts matched the requested age cutoff.");
        return Ok(());
    }

    let total = stale_remote_artifacts.len();
    let prune_spinner = spinner(&format!("Pruning {total} remote artifact(s)... 0/{total}"));
    let worker_count = thread::available_parallelism()
        .map(|count| count.get().min(8))
        .unwrap_or(4)
        .min(total)
        .max(1);
    let config = Arc::new(config.clone());
    let jobs = Arc::new(Mutex::new(stale_remote_artifacts));
    let (result_tx, result_rx) = mpsc::channel();

    for _ in 0..worker_count {
        let config = Arc::clone(&config);
        let jobs = Arc::clone(&jobs);
        let result_tx = result_tx.clone();
        thread::spawn(move || {
            loop {
                let Some(artifact) = jobs.lock().ok().and_then(|mut jobs| jobs.pop()) else {
                    break;
                };
                let remote_ref = artifact.remote_ref;
                let outcome = delete_remote_artifact(&config, &remote_ref);
                if result_tx.send((remote_ref, outcome)).is_err() {
                    break;
                }
            }
        });
    }
    drop(result_tx);

    let mut completed = 0usize;
    let mut deleted_refs = HashSet::new();
    let mut failures = Vec::new();
    for (remote_ref, outcome) in result_rx {
        completed += 1;
        match outcome {
            Ok(()) => {
                deleted_refs.insert(remote_ref);
            }
            Err(err) => failures.push(format!("{remote_ref}: {err:#}")),
        }
        prune_spinner.set_message(format!(
            "Pruning {total} remote artifact(s)... {completed}/{total}"
        ));
    }

    clear_deleted_uploaded_refs(&deleted_refs)?;
    if !failures.is_empty() {
        prune_spinner.finish_with_message(format!(
            "Remote artifact prune incomplete: {} failed.",
            failures.len()
        ));
        bail!(
            "Failed to delete {} remote artifact(s):\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    prune_spinner.finish_with_message("Remote artifact prune complete.");
    Ok(())
}

fn remote_created_at_epoch_ms(artifact: &RemoteArtifact) -> u128 {
    artifact
        .summary
        .as_ref()
        .map(|summary| summary.created_at_epoch_ms)
        .unwrap_or(artifact.uploaded_at_epoch_ms)
}

fn clear_deleted_uploaded_refs(deleted_refs: &HashSet<String>) -> Result<()> {
    if deleted_refs.is_empty() {
        return Ok(());
    }
    for mut artifact in load_stored_artifacts()? {
        if artifact
            .metadata
            .uploaded_ref
            .as_ref()
            .is_some_and(|uploaded_ref| deleted_refs.contains(uploaded_ref))
        {
            artifact.metadata.uploaded_ref = None;
            artifact.metadata.uploaded_at_epoch_ms = None;
            save_metadata(&artifact.dir, &artifact.metadata)?;
        }
    }
    Ok(())
}

fn now_epoch_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before the Unix epoch")?
        .as_millis())
}
