use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::{Deserialize, Serialize};

use crate::cache::{load_cached_list_rows_for, persist_instances, persist_instances_with_context};
use crate::cli::CloudArgs;
use crate::model::{Cloud, IceConfig};
use crate::providers::{CloudInstance, CloudProvider, RemoteCloudProvider, aws, gcp, local, vast};
use crate::support::{
    ensure_provider_cli_installed, now_unix_secs, resolve_cloud, visible_instance_name,
};
use crate::ui::{
    Color, RenderTarget, StderrRenderTarget, StdoutRenderTarget, TextEffect, stderr_is_interactive,
};

const FIELD_SEPARATOR: &str = " · ";
const LIST_INDENT: &str = "    ";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ListNote {
    pub(crate) text: String,
    pub(crate) color: Color,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ListedInstance {
    pub(crate) display_name: String,
    pub(crate) state: String,
    pub(crate) color: Color,
    pub(crate) fields: Vec<String>,
    pub(crate) detail_fields: Vec<String>,
    #[serde(default)]
    pub(crate) status_note: Option<ListNote>,
    #[serde(default)]
    pub(crate) text_effect: TextEffect,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedListRow {
    pub(crate) key: String,
    pub(crate) instance: ListedInstance,
    pub(crate) observed_at_unix: u64,
}

pub(crate) struct InteractiveList {
    progress: MultiProgress,
    spacer: Option<ProgressBar>,
    footer: ProgressBar,
    target: StderrRenderTarget,
}

impl InteractiveList {
    pub(crate) fn new(footer_message: impl Into<String>, include_spacer: bool) -> Self {
        let progress = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
        let footer = progress.add(new_list_row_spinner());
        footer.set_message(footer_message.into());
        activate_list_row_spinner(&footer);
        let spacer = include_spacer.then(|| {
            let spacer = progress.insert_before(&footer, new_list_spacer());
            finish_list_spacer(&spacer);
            spacer
        });
        Self {
            progress,
            spacer,
            footer,
            target: StderrRenderTarget,
        }
    }

    pub(crate) fn set_footer_message(&self, message: impl Into<String>) {
        self.footer.set_message(message.into());
    }

    pub(crate) fn render(&self, instance: &ListedInstance) -> String {
        render_listed_instance(instance, &self.target)
    }

    pub(crate) fn insert_pending_instance(&self, instance: &ListedInstance) -> ProgressBar {
        let row = self
            .progress
            .insert_before(self.anchor(), new_list_row_spinner());
        row.set_message(self.render(instance));
        activate_list_row_spinner(&row);
        row
    }

    pub(crate) fn insert_finished_instance(&self, instance: &ListedInstance) -> ProgressBar {
        let row = self
            .progress
            .insert_before(self.anchor(), new_list_row_spinner());
        finish_list_row(&row, &self.render(instance));
        row
    }

    pub(crate) fn set_pending_instance(&self, row: &ProgressBar, instance: &ListedInstance) {
        row.set_message(self.render(instance));
    }

    pub(crate) fn finish_instance(&self, row: &ProgressBar, instance: &ListedInstance) {
        finish_list_row(row, &self.render(instance));
    }

    pub(crate) fn seed_cached_rows(
        &self,
        cached_rows: &[CachedListRow],
    ) -> HashMap<String, ProgressBar> {
        let mut rows = HashMap::with_capacity(cached_rows.len());
        for cached in cached_rows {
            let row = self.insert_pending_instance(&cached_listed_instance(
                &cached.instance,
                cached.observed_at_unix,
            ));
            rows.insert(cached.key.clone(), row);
        }
        rows
    }

    pub(crate) fn finish_cached_rows_after_remote_error(
        &self,
        rows: &HashMap<String, ProgressBar>,
        cached_rows: &[CachedListRow],
    ) {
        for cached in cached_rows {
            if let Some(row) = rows.get(&cached.key) {
                self.finish_instance(
                    row,
                    &remote_error_cached_instance(&cached.instance, cached.observed_at_unix),
                );
            }
        }
    }

    pub(crate) fn finish_missing_cached_rows(
        &self,
        rows: &HashMap<String, ProgressBar>,
        cached_rows: &[CachedListRow],
        seen: &HashSet<String>,
    ) {
        for cached in cached_rows {
            if seen.contains(&cached.key) {
                continue;
            }
            if let Some(row) = rows.get(&cached.key) {
                self.finish_instance(
                    row,
                    &missing_remote_cached_instance(&cached.instance, cached.observed_at_unix),
                );
            }
        }
    }

    pub(crate) fn finish_empty(self, cloud: Cloud) {
        if let Some(spacer) = &self.spacer {
            clear_list_spacer(&self.progress, spacer);
        }
        self.footer.finish_with_message(no_instances_message(cloud));
    }

    pub(crate) fn clear_footer(&self) {
        clear_list_footer(&self.progress, self.spacer.as_ref(), &self.footer);
    }

    fn anchor(&self) -> &ProgressBar {
        self.spacer.as_ref().unwrap_or(&self.footer)
    }
}

pub(crate) fn cmd_list(args: CloudArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    if stderr_is_interactive() {
        return match cloud {
            Cloud::VastAi => run_interactive_remote_list::<vast::Provider>(config),
            Cloud::Gcp => run_interactive_remote_list::<gcp::Provider>(config),
            Cloud::Aws => run_interactive_remote_list::<aws::Provider>(config),
            Cloud::Local => run_interactive_local_list(config),
        };
    }

    match cloud {
        Cloud::VastAi => print_listed_instances(
            cloud,
            &load_remote_listed_instances::<vast::Provider>(config)?,
        ),
        Cloud::Gcp => print_listed_instances(
            cloud,
            &load_remote_listed_instances::<gcp::Provider>(config)?,
        ),
        Cloud::Aws => print_listed_instances(
            cloud,
            &load_remote_listed_instances::<aws::Provider>(config)?,
        ),
        Cloud::Local => print_listed_instances(cloud, &load_local_listed_instances(config)?),
    }
    Ok(())
}

fn run_interactive_local_list(config: &IceConfig) -> Result<()> {
    let list = InteractiveList::new(local::Provider::initial_loading_message(), false);
    let instances = match load_local_listed_instances(config) {
        Ok(instances) => instances,
        Err(err) => {
            list.clear_footer();
            return Err(err);
        }
    };

    if instances.is_empty() {
        list.finish_empty(local::Provider::CLOUD);
        return Ok(());
    }

    for instance in &instances {
        let _ = list.insert_finished_instance(instance);
    }
    list.clear_footer();
    Ok(())
}

pub(crate) fn run_interactive_remote_list<P>(config: &IceConfig) -> Result<()>
where
    P: RemoteCloudProvider,
    <P::Instance as CloudInstance>::ListContext: Default,
{
    let cached_rows = load_cached_list_rows_for::<P::CacheModel>();
    let list = InteractiveList::new(P::initial_loading_message(), !cached_rows.is_empty());
    let mut rows = list.seed_cached_rows(&cached_rows);

    let context = match P::context(config) {
        Ok(context) => context,
        Err(err) => {
            if cached_rows.is_empty() {
                list.clear_footer();
                return Err(err);
            }
            list.finish_cached_rows_after_remote_error(&rows, &cached_rows);
            list.clear_footer();
            eprintln!(
                "Warning: failed to initialize {} provider: {err:#}",
                P::CLOUD
            );
            return Ok(());
        }
    };

    let mut instances =
        match P::list_instances(&context, &mut |message| list.set_footer_message(message)) {
            Ok(instances) => instances,
            Err(err) => {
                if cached_rows.is_empty() {
                    list.clear_footer();
                    return Err(err);
                }
                list.finish_cached_rows_after_remote_error(&rows, &cached_rows);
                list.clear_footer();
                eprintln!("Warning: failed to load {} instances: {err:#}", P::CLOUD);
                return Ok(());
            }
        };
    P::sort_instances(&mut instances);

    if cached_rows.is_empty() && instances.is_empty() {
        list.finish_empty(P::CLOUD);
        return Ok(());
    }

    let pending_list_context = P::list_context_loading_message().is_some();
    let empty_list_context: <P::Instance as CloudInstance>::ListContext = Default::default();
    let mut seen = HashSet::new();
    for instance in &instances {
        let key = instance.cache_key();
        seen.insert(key.clone());
        let listed = instance.render(&empty_list_context, pending_list_context);
        if let Some(row) = rows.get(&key) {
            if pending_list_context {
                list.set_pending_instance(row, &listed);
            } else {
                list.finish_instance(row, &listed);
            }
            continue;
        }
        let row = if pending_list_context {
            list.insert_pending_instance(&listed)
        } else {
            list.insert_finished_instance(&listed)
        };
        rows.insert(key, row);
    }

    persist_instances::<P::CacheModel>(&instances);

    if pending_list_context {
        list.set_footer_message(P::list_context_loading_message().unwrap_or_default());
        let list_context = match P::resolve_list_context(&context, &instances, &mut |message| {
            list.set_footer_message(message)
        }) {
            Ok(list_context) => list_context,
            Err(err) => {
                finish_remote_rows(&list, &rows, &instances, &empty_list_context);
                list.finish_missing_cached_rows(&rows, &cached_rows, &seen);
                list.clear_footer();
                eprintln!("Warning: failed to load {} detail state: {err:#}", P::CLOUD);
                return Ok(());
            }
        };
        persist_instances_with_context::<P::CacheModel>(&instances, &list_context);
        finish_remote_rows(&list, &rows, &instances, &list_context);
    }

    list.finish_missing_cached_rows(&rows, &cached_rows, &seen);
    list.clear_footer();
    Ok(())
}

pub(crate) fn load_remote_listed_instances<P>(config: &IceConfig) -> Result<Vec<ListedInstance>>
where
    P: RemoteCloudProvider,
    <P::Instance as CloudInstance>::ListContext: Default,
{
    let context = P::context(config)?;
    let mut instances = P::list_instances(&context, &mut |_| {})?;
    P::sort_instances(&mut instances);
    persist_instances::<P::CacheModel>(&instances);

    let mut list_context: <P::Instance as CloudInstance>::ListContext = Default::default();
    if P::list_context_loading_message().is_some() {
        match P::resolve_list_context(&context, &instances, &mut |_| {}) {
            Ok(resolved) => {
                persist_instances_with_context::<P::CacheModel>(&instances, &resolved);
                list_context = resolved;
            }
            Err(err) => eprintln!("Warning: failed to load {} detail state: {err:#}", P::CLOUD),
        }
    }

    Ok(instances
        .iter()
        .map(|instance| instance.render(&list_context, false))
        .collect())
}

fn load_local_listed_instances(config: &IceConfig) -> Result<Vec<ListedInstance>> {
    let runtime = local::Provider::context(config)?;
    let mut instances = local::Provider::list_instances(&runtime, &mut |_| {})?;
    local::Provider::sort_instances(&mut instances);
    Ok(instances
        .iter()
        .map(|instance| instance.render(&runtime, false))
        .collect())
}

pub(crate) fn listed_instance(
    display_name: String,
    state: String,
    color: Color,
    fields: Vec<String>,
    detail_fields: Vec<String>,
) -> ListedInstance {
    ListedInstance {
        display_name,
        state,
        color,
        fields,
        detail_fields,
        status_note: None,
        text_effect: TextEffect::None,
    }
}

pub(crate) fn print_listed_instances(cloud: Cloud, instances: &[ListedInstance]) {
    if instances.is_empty() {
        println!("{}", no_instances_message(cloud));
        return;
    }
    let target = StdoutRenderTarget;
    for instance in instances {
        println!("{}", render_listed_instance(instance, &target));
    }
}

pub(crate) fn render_listed_instance(
    instance: &ListedInstance,
    target: &impl RenderTarget,
) -> String {
    if instance.text_effect == TextEffect::Strikethrough {
        let mut parts = Vec::with_capacity(
            2 + instance.fields.len() + usize::from(instance.status_note.is_some()),
        );
        parts.push(instance.display_name.clone());
        parts.push(instance.state.clone());
        parts.extend(instance.fields.clone());
        if let Some(note) = &instance.status_note {
            parts.push(note.text.clone());
        }

        let mut row = target.effect(
            &format!("● {}", parts.join(FIELD_SEPARATOR)),
            TextEffect::Strikethrough,
        );
        if !instance.detail_fields.is_empty() {
            row.push('\n');
            row.push_str(LIST_INDENT);
            row.push_str(&target.effect(
                &instance.detail_fields.join(FIELD_SEPARATOR),
                TextEffect::Strikethrough,
            ));
        }
        return row;
    }

    let bullet = target.paint("●", instance.color);
    let state = target.paint(&instance.state, instance.color);
    let mut parts =
        Vec::with_capacity(2 + instance.fields.len() + usize::from(instance.status_note.is_some()));
    parts.push(instance.display_name.clone());
    parts.push(state);
    parts.extend(instance.fields.clone());
    if let Some(note) = &instance.status_note {
        parts.push(target.paint(&note.text, note.color));
    }

    let mut row = format!("{bullet} {}", parts.join(FIELD_SEPARATOR));
    if !instance.detail_fields.is_empty() {
        row.push('\n');
        row.push_str(LIST_INDENT);
        row.push_str(&target.paint(&instance.detail_fields.join(FIELD_SEPARATOR), Color::Cyan));
    }
    row
}

pub(crate) fn missing_remote_cached_instance(
    instance: &ListedInstance,
    observed_at_unix: u64,
) -> ListedInstance {
    let mut instance = instance.clone();
    instance.status_note = Some(ListNote {
        text: format!(
            "missing remotely (cached {})",
            cache_age_text(observed_at_unix)
        ),
        color: Color::Red,
    });
    instance.text_effect = TextEffect::Strikethrough;
    instance
}

fn no_instances_message(cloud: Cloud) -> String {
    format!("No `ice`-managed instances found on `{cloud}`.")
}

fn finish_remote_rows<I>(
    list: &InteractiveList,
    rows: &HashMap<String, ProgressBar>,
    instances: &[I],
    list_context: &I::ListContext,
) where
    I: CloudInstance,
{
    for instance in instances {
        if let Some(row) = rows.get(&instance.cache_key()) {
            list.finish_instance(row, &instance.render(list_context, false));
        }
    }
}

fn cached_listed_instance(instance: &ListedInstance, observed_at_unix: u64) -> ListedInstance {
    let mut instance = instance.clone();
    instance.status_note = Some(ListNote {
        text: format!("cached {}", cache_age_text(observed_at_unix)),
        color: Color::Yellow,
    });
    instance.text_effect = TextEffect::None;
    instance
}

fn remote_error_cached_instance(
    instance: &ListedInstance,
    observed_at_unix: u64,
) -> ListedInstance {
    let mut instance = instance.clone();
    instance.status_note = Some(ListNote {
        text: format!(
            "remote poll failed (cached {})",
            cache_age_text(observed_at_unix)
        ),
        color: Color::Yellow,
    });
    instance.text_effect = TextEffect::None;
    instance
}

fn cache_age_text(observed_at_unix: u64) -> String {
    if observed_at_unix == 0 {
        return "previously".to_owned();
    }
    let age = now_unix_secs().saturating_sub(observed_at_unix);
    match age {
        0..=89 => "moments ago".to_owned(),
        90..=3_599 => format!("{}m ago", age / 60),
        3_600..=86_399 => format!("{}h ago", age / 3_600),
        _ => format!("{}d ago", age / 86_400),
    }
}

fn new_list_row_spinner() -> ProgressBar {
    let row = ProgressBar::new_spinner();
    let style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    row.set_style(style);
    row
}

fn new_list_spacer() -> ProgressBar {
    let spacer = ProgressBar::new_spinner();
    let style =
        ProgressStyle::with_template("{msg}").unwrap_or_else(|_| ProgressStyle::default_spinner());
    spacer.set_style(style);
    spacer
}

fn activate_list_row_spinner(row: &ProgressBar) {
    row.enable_steady_tick(Duration::from_millis(90));
}

fn finish_list_spacer(spacer: &ProgressBar) {
    spacer.finish_with_message(" ".to_owned());
}

fn finish_list_row(row: &ProgressBar, message: &str) {
    let style =
        ProgressStyle::with_template("{msg}").unwrap_or_else(|_| ProgressStyle::default_spinner());
    row.set_style(style);
    row.finish_with_message(message.to_owned());
}

fn clear_list_spacer(progress: &MultiProgress, spacer: &ProgressBar) {
    spacer.finish_and_clear();
    progress.remove(spacer);
}

fn clear_list_footer(progress: &MultiProgress, spacer: Option<&ProgressBar>, footer: &ProgressBar) {
    if let Some(spacer) = spacer {
        spacer.finish_and_clear();
        progress.remove(spacer);
    }
    footer.finish_and_clear();
    progress.remove(footer);
}

pub(crate) fn display_name_or_fallback(name: &str, fallback: String) -> String {
    let visible = visible_instance_name(name).trim();
    if visible.is_empty() {
        fallback
    } else {
        visible.to_owned()
    }
}

pub(crate) fn display_state(state: &str) -> String {
    let state = state.trim();
    if state.is_empty() {
        "unknown".to_owned()
    } else {
        state.to_ascii_lowercase()
    }
}

pub(crate) fn show_health_field(health: &str) -> Option<String> {
    present_field(health).filter(|value| value != "-")
}

pub(crate) fn present_field(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value == "-" {
        None
    } else {
        Some(value.to_owned())
    }
}

pub(crate) fn push_field(fields: &mut Vec<String>, value: Option<String>) {
    if let Some(value) = value {
        fields.push(value);
    }
}

pub(crate) fn list_state_color(state: &str, health: Option<&str>) -> Color {
    let health = health.unwrap_or("").trim().to_ascii_lowercase();
    if matches!(
        health.as_str(),
        "unhealthy" | "error" | "failed" | "failure" | "dead"
    ) {
        return Color::Red;
    }
    match state.trim().to_ascii_lowercase().as_str() {
        "running" => Color::Green,
        "stopped" | "terminated" | "suspended" | "unknown" => Color::Yellow,
        _ => Color::Blue,
    }
}
