use super::*;

pub(super) struct StatusWidths {
    role: usize,
    role_status: usize,
    role_mode: usize,
    parallel: usize,
    agent: usize,
    agent_summary: usize,
    agent_status: usize,
    queue_name: usize,
}

impl StatusWidths {
    pub(super) fn from_rows(roles: &[StatusRole], queue_rows: &[StatusQueueRow]) -> Self {
        Self {
            role: roles
                .iter()
                .map(|role| role.name.chars().count())
                .max()
                .unwrap_or(1),
            role_status: roles
                .iter()
                .map(|role| role.status.to_string().chars().count())
                .max()
                .unwrap_or(1),
            role_mode: roles
                .iter()
                .map(|role| role.mode.to_string().chars().count())
                .max()
                .unwrap_or(1),
            parallel: roles
                .iter()
                .map(|role| role.parallel.chars().count())
                .max()
                .unwrap_or(1),
            agent: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.name.chars().count())
                .max()
                .unwrap_or(1),
            agent_summary: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.summary.chars().count())
                .max()
                .unwrap_or(1),
            agent_status: roles
                .iter()
                .flat_map(|role| &role.agents)
                .map(|agent| agent.status.to_string().chars().count())
                .max()
                .unwrap_or(1),
            queue_name: queue_rows
                .iter()
                .map(|row| row.name.chars().count())
                .max()
                .unwrap_or(1),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum StatusQueueKind {
    Trigger,
}

impl StatusQueueKind {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Trigger => "queue",
        }
    }
}

pub(super) struct StatusQueueRow {
    pub(super) kind: StatusQueueKind,
    pub(super) name: String,
    pub(super) count: usize,
    pub(super) locked: bool,
    pub(super) active: Option<StatusQueueActive>,
    pub(super) locked_at: Option<u64>,
}

#[derive(Clone)]
pub(super) struct StatusQueueActive {
    pub(super) label: String,
}

pub(super) struct StatusChannelRow {
    pub(super) name: String,
    pub(super) artifacts: usize,
    pub(super) latest: Option<String>,
}

pub(super) struct ChannelArtifactEntry {
    pub(super) name: String,
    pub(super) modified: u64,
    pub(super) is_dir: bool,
}

impl QueryMatch for StatusChannelRow {
    fn matches_query(&self, query: &str) -> bool {
        query.trim().is_empty()
            || text_matches_query(
                [
                    self.name.clone(),
                    self.artifacts.to_string(),
                    self.latest.clone().unwrap_or_default(),
                ],
                query,
            )
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StatusQueueKey {
    pub(super) kind: StatusQueueKind,
    pub(super) name: String,
}

impl From<&StatusQueueRow> for StatusQueueKey {
    fn from(row: &StatusQueueRow) -> Self {
        Self {
            kind: row.kind,
            name: row.name.clone(),
        }
    }
}

pub(super) fn queue_key_string(queue: &StatusQueueRow) -> String {
    format!("{}:{}", queue.kind.label(), queue.name)
}

pub(super) fn load_status_channel_rows(project: &ProjectPaths) -> Result<Vec<StatusChannelRow>> {
    list_channels(project)?
        .into_iter()
        .map(|channel| {
            let channel_dir = project.channel_dir(&channel);
            Ok(StatusChannelRow {
                name: channel.to_string(),
                artifacts: count_channel_artifacts(&channel_dir)?,
                latest: latest_channel_artifact(&channel_dir)?,
            })
        })
        .collect()
}

pub(super) fn count_channel_artifacts(channel_dir: &Path) -> Result<usize> {
    Ok(channel_artifact_entries(channel_dir)?.len())
}

pub(super) fn latest_channel_artifact(channel_dir: &Path) -> Result<Option<String>> {
    Ok(channel_artifact_entries(channel_dir)?
        .into_iter()
        .next()
        .map(|entry| entry.name))
}

pub(super) fn channel_artifact_entries(channel_dir: &Path) -> Result<Vec<ChannelArtifactEntry>> {
    let mut entries = io::collect_existing_dir(channel_dir, |entry| {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            return Ok(None);
        }
        let metadata = entry
            .metadata()
            .with_context(|| format!("Failed to stat `{}`", entry.path().display()))?;
        Ok(Some(ChannelArtifactEntry {
            name: name.to_string_lossy().to_string(),
            modified: metadata
                .modified()
                .ok()
                .and_then(system_time_to_unix)
                .unwrap_or_default(),
            is_dir: metadata.is_dir(),
        }))
    })?;
    entries.sort_by(|left, right| {
        right
            .modified
            .cmp(&left.modified)
            .then_with(|| right.name.cmp(&left.name))
    });
    Ok(entries)
}

pub(super) fn load_status_queue_rows(project: &ProjectPaths) -> Result<Vec<StatusQueueRow>> {
    load_status_queue_rows_inner(project, false)
}

pub(super) fn load_all_status_queue_rows(project: &ProjectPaths) -> Result<Vec<StatusQueueRow>> {
    load_status_queue_rows_inner(project, true)
}

pub(super) fn load_status_queue_rows_inner(
    project: &ProjectPaths,
    include_empty: bool,
) -> Result<Vec<StatusQueueRow>> {
    let mut rows = Vec::new();
    for name in trigger_queue_names(project)? {
        validate_trigger_queue(&name)?;
        let state = load_trigger_queue(project, &name)?;
        let lock_path = trigger_queue_lock_path(project, &name);
        let locked = lock::is_active(&lock_path)?;
        if !include_empty && state.items.is_empty() && !locked {
            continue;
        }
        rows.push(StatusQueueRow {
            kind: StatusQueueKind::Trigger,
            name,
            count: state.items.len(),
            locked,
            active: None,
            locked_at: locked.then(|| file_modified_unix(&lock_path)).flatten(),
        });
    }
    Ok(rows)
}

pub(super) fn trigger_queue_names(project: &ProjectPaths) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for trigger in load_role_config(&role_paths)?.triggers {
            if let TriggerConfig::QueueIdle { idle_queue, .. } = &trigger {
                names.insert(idle_queue.clone());
            }
            if let TriggerLaunch::Queued { queue } = trigger_launch(&trigger) {
                names.insert(queue.to_owned());
            }
        }
    }
    collect_queue_names_from_dir(
        &project.runtime_dir().join("trigger-queues"),
        "toml",
        &mut names,
    )?;
    collect_queue_names_from_dir(
        &project.runtime_dir().join("locks").join("trigger-queues"),
        "lock",
        &mut names,
    )?;
    Ok(names)
}

pub(super) fn trigger_launch(trigger: &TriggerConfig) -> &TriggerLaunch {
    match trigger {
        TriggerConfig::RoleStepFinished { launch, .. }
        | TriggerConfig::RoleAgentFinished { launch, .. }
        | TriggerConfig::QueueIdle { launch, .. }
        | TriggerConfig::Elapsed { launch, .. } => launch,
    }
}

pub(super) fn collect_queue_names_from_dir(
    dir: &Path,
    extension: &str,
    names: &mut BTreeSet<String>,
) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("Failed to read `{}`", dir.display()))? {
        let entry = entry.context("Failed to read queue entry")?;
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            == Some(extension)
            && let Some(name) = entry.path().file_stem().and_then(|stem| stem.to_str())
        {
            names.insert(name.to_owned());
        }
    }
    Ok(())
}

pub(super) struct StatusRole {
    pub(super) name: String,
    pub(super) status: RoleStatus,
    pub(super) mode: RoleMode,
    pub(super) parallel: String,
    pub(super) expose: String,
    pub(super) agents: Vec<StatusAgent>,
}

pub(super) struct StatusAgent {
    pub(super) name: String,
    pub(super) status: AgentStatus,
    pub(super) summary: String,
    pub(super) detail: String,
}

pub(super) fn load_status_roles(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
) -> Result<Vec<StatusRole>> {
    let roles = if let Some(role) = role_filter {
        vec![role.clone()]
    } else {
        list_roles(project)?
    };
    roles
        .into_iter()
        .map(|role| load_status_role(project, &role, include_archived))
        .collect()
}

pub(super) fn load_status_role(
    project: &ProjectPaths,
    role: &RoleSlug,
    include_archived: bool,
) -> Result<StatusRole> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let config = load_role_config(&role_paths)?;
    let active = active_agent_count(&role_paths)?;
    let mut agents = Vec::new();
    for agent in list_agents(&role_paths)? {
        let state = load_agent(&role_paths.agent(agent.clone()))?;
        if state.archived && !include_archived {
            continue;
        }
        let agent_paths = role_paths.agent(agent.clone());
        let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
        agents.push(StatusAgent {
            name: agent.to_string(),
            status: state.status,
            summary,
            detail,
        });
    }
    Ok(StatusRole {
        name: role.to_string(),
        status: config.status,
        mode: config.mode,
        parallel: format!("{active}/{}", config.parallel),
        expose: config
            .expose
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
        agents,
    })
}

pub(super) fn status_agent_summary_and_detail(
    config: &RoleConfig,
    state: &AgentState,
    agent_paths: &crate::state::AgentPaths,
) -> Result<(String, String)> {
    let (manifest, manifest_error) = load_agent_manifest_for_display(agent_paths);
    let supervisor = load_supervisor_state(agent_paths)?;
    let step = config
        .steps
        .get(state.current_step % config.steps.len().max(MIN_ROLE_STEP_COUNT))
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let supervisor_detail = supervisor_list_detail(&supervisor, state.status);
    let note_detail = state
        .note
        .clone()
        .or_else(|| {
            state
                .paused_by_user
                .then(|| "agent paused by user".to_owned())
        })
        .filter(|note| {
            !((supervisor.status == SupervisorStatus::WaitingForQuota
                && note.to_ascii_lowercase().contains("quota"))
                || (supervisor.status == SupervisorStatus::WaitingForProvider
                    && note.to_ascii_lowercase().contains("provider")))
        })
        .filter(|note| {
            !supervisor_detail
                .as_deref()
                .is_some_and(|detail| detail.contains(note.as_str()))
        });
    let disposition_detail = manifest
        .disposition
        .or_else(|| state.last_exit.as_ref().and_then(|exit| exit.disposition))
        .map(|value| format!("disposition: {value}"));
    let manifest_error_detail = manifest_error
        .as_deref()
        .map(|error| format!("manifest error: {error}"));
    let steer_detail = steer_status_detail(
        &crate::backend::steer_status(&agent_paths.steer_dir())?,
        matches!(state.status, AgentStatus::Starting | AgentStatus::Running),
    );
    let primary_detail = combine_optional_details(
        combine_optional_details(
            combine_optional_details(note_detail, disposition_detail),
            manifest_error_detail,
        ),
        steer_detail,
    );
    let detail = combine_optional_details(
        combine_optional_details(Some(format!("step: {step}")), primary_detail),
        supervisor_detail,
    );
    let summary = manifest_error
        .as_ref()
        .map(|_| "*manifest error*".to_owned())
        .or_else(|| {
            manifest
                .role_summary
                .map(|summary| summary.trim().to_owned())
                .filter(|summary| !summary.is_empty())
        })
        .unwrap_or_else(|| "*name loading*".to_owned());
    Ok((
        summary,
        match (state.archived, detail) {
            (true, Some(detail)) => format!("archived{}{}", ui::FIELD_SEPARATOR, detail),
            (true, None) => "archived".to_owned(),
            (false, Some(detail)) => detail,
            (false, None) => String::new(),
        },
    ))
}

pub(super) fn print_role_row(role: &StatusRole, widths: &StatusWidths) {
    println!(
        "{} {:<role_width$}{}{}{}{:<role_mode_width$}{}{:<parallel_width$}{}expose: {}",
        ui::role_bullet(role.status),
        role.name,
        ui::FIELD_SEPARATOR,
        ui::status_role_padded(role.status, widths.role_status),
        ui::FIELD_SEPARATOR,
        role.mode,
        ui::FIELD_SEPARATOR,
        role.parallel,
        ui::FIELD_SEPARATOR,
        empty_dash(&role.expose),
        role_width = widths.role,
        role_mode_width = widths.role_mode,
        parallel_width = widths.parallel
    );
}

pub(super) fn print_agent_row(agent: &StatusAgent, widths: &StatusWidths) {
    let detail = if agent.detail.is_empty() {
        String::new()
    } else {
        format!("{}{}", ui::FIELD_SEPARATOR, agent.detail)
    };

    println!(
        "  {} {:<agent_width$}{}{:<summary_width$}{}{}{}",
        ui::agent_bullet(agent.status),
        agent.name,
        ui::FIELD_SEPARATOR,
        ui::agent_summary(&agent.summary),
        ui::FIELD_SEPARATOR,
        ui::status_agent_padded(agent.status, widths.agent_status),
        detail,
        agent_width = widths.agent,
        summary_width = widths.agent_summary
    );
}

pub(super) fn combine_optional_details(
    first: Option<String>,
    second: Option<String>,
) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) if first == second => Some(first),
        (Some(first), Some(second)) => Some(format!("{first}{}{second}", ui::FIELD_SEPARATOR)),
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(second),
        (None, None) => None,
    }
}

pub(super) fn steer_status_detail(status: &SteerStatus, active: bool) -> Option<String> {
    if let Some(error) = status.last_error.as_deref() {
        return Some(format!("steer error: {}", compact_single_line(error, 96)));
    }
    if status.pending_count > 0 {
        let label = if active {
            "steer pending"
        } else {
            "stale steer pending"
        };
        return Some(match status.latest_pending_text.as_deref() {
            Some(text) if !text.trim().is_empty() => {
                format!(
                    "{label}: {} · {}",
                    status.pending_count,
                    compact_single_line(text, 80)
                )
            }
            _ => format!("{label}: {}", status.pending_count),
        });
    }
    status.last_sent_at.map(|sent_at| {
        let text = status
            .last_sent_text
            .as_deref()
            .map(|text| compact_single_line(text, 80))
            .filter(|text| !text.is_empty())
            .unwrap_or_else(|| "sent".to_owned());
        format!("last steer: {} · {text}", event_age(sent_at))
    })
}

pub(super) fn steer_status_line(status: &SteerStatus, active: bool) -> Option<Line<'static>> {
    let detail = steer_status_detail(status, active)?;
    let style = if status.last_error.is_some() {
        Style::default().fg(Color::Red)
    } else if status.pending_count > 0 && !active {
        Style::default().fg(Color::Yellow)
    } else if status.pending_count > 0 {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    Some(Line::from(vec![
        Span::styled("steer ", Style::default().fg(Color::DarkGray)),
        Span::styled(detail, style),
    ]))
}

pub(super) fn supervisor_list_detail(
    state: &SupervisorState,
    agent_status: AgentStatus,
) -> Option<String> {
    if !matches!(
        agent_status,
        AgentStatus::Starting | AgentStatus::Running | AgentStatus::NeedsAttention
    ) {
        return None;
    }
    let presentation = state.status.list_presentation()?;
    Some(format!(
        "{}{}{}",
        presentation.label,
        if presentation.include_retry {
            retry_detail(state.next_retry_at)
        } else {
            String::new()
        },
        supervisor_event_suffix(
            state.last_event.as_deref(),
            presentation.clean_event_timestamps
        )
    ))
}

pub(super) fn supervisor_event_suffix(event: Option<&str>, clean_timestamps: bool) -> String {
    event
        .map(|event| {
            if clean_timestamps {
                clean_runtime_event(event)
            } else {
                event.to_owned()
            }
        })
        .filter(|event| !event.is_empty())
        .map(|event| format!(" ({event})"))
        .unwrap_or_default()
}

pub(super) fn clean_runtime_event(event: &str) -> String {
    let Some((head, timestamp)) = event.rsplit_once(" at ") else {
        return event.to_owned();
    };
    let Ok(timestamp) = timestamp.parse::<u64>() else {
        return event.to_owned();
    };
    format!("{head} at {}", format_unix_time(timestamp))
}

pub(super) fn retry_detail(next_retry_at: Option<u64>) -> String {
    next_retry_at
        .map(|timestamp| {
            format!(
                " retry in {}; next {}",
                human_duration(timestamp.saturating_sub(unix_timestamp())),
                format_unix_time(timestamp)
            )
        })
        .unwrap_or_default()
}

pub(super) fn print_queue_row(row: &StatusQueueRow, widths: &StatusWidths) {
    let detail = format!(
        "{}{}",
        ui::queue_count(row.count),
        if row.locked { " locked" } else { "" }
    );
    println!(
        "{} {:<queue_name_width$}{}{}",
        row.kind.label(),
        row.name,
        ui::FIELD_SEPARATOR,
        detail,
        queue_name_width = widths.queue_name
    );
}

pub(super) fn print_quota_footer(project: &ProjectPaths) -> Result<()> {
    let cached = load_cached_quota_footer(project)?;
    if !std::io::stdout().is_terminal() || cached.waiting {
        println!("{}", cached.line);
        return Ok(());
    }

    let mut stdout = std::io::stdout().lock();
    let codex_config = project_config(project)?.providers.codex;
    let handle = thread::spawn(move || crate::provider::codex::probe_health(&codex_config));
    let started = Instant::now();
    let mut frame = 0;
    while !handle.is_finished() {
        write!(
            stdout,
            "\r\x1b[2Kquota{}checking {}",
            ui::FIELD_SEPARATOR,
            ui::spinner_frame(frame)
        )?;
        stdout.flush()?;
        frame += 1;
        thread::sleep(QUOTA_PROBE_SPINNER_INTERVAL);
    }
    let probe = handle
        .join()
        .unwrap_or_else(|_| CodexHealth::Unavailable("health thread panicked".to_owned()));
    let line = match probe {
        CodexHealth::Ok => cached.line,
        CodexHealth::Unavailable(reason) => format!(
            "{}{}codex health: {}",
            cached.line,
            ui::FIELD_SEPARATOR,
            reason
        ),
    };
    let elapsed_padding = " ".repeat(
        ((started.elapsed().as_millis() / QUOTA_PROBE_SPINNER_INTERVAL.as_millis()) as usize)
            .min(QUOTA_STATUS_CLEAR_PADDING_MAX),
    );
    writeln!(stdout, "\r\x1b[2K{line}{elapsed_padding}")?;
    Ok(())
}

pub(super) struct CachedQuotaFooter {
    line: String,
    waiting: bool,
}

pub(super) fn load_cached_quota_footer(project: &ProjectPaths) -> Result<CachedQuotaFooter> {
    let codex_config = project_config(project)?.providers.codex;
    let mut waiting = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived
                || !matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
            {
                continue;
            }
            let supervisor = load_supervisor_state(&agent_paths)?;
            if supervisor.status == SupervisorStatus::WaitingForQuota {
                waiting.push((
                    format!("{role}/{agent}"),
                    supervisor.next_retry_at,
                    supervisor
                        .last_event
                        .unwrap_or_else(|| "quota backoff".to_owned()),
                ));
            }
        }
    }
    if waiting.is_empty() {
        return Ok(CachedQuotaFooter {
            line: match load_codex_rate_limits_if_interactive(&codex_config) {
                Some(limits) => limits.to_string(),
                None => "usage unavailable".to_owned(),
            },
            waiting: false,
        });
    }
    waiting.sort_by_key(|(_, retry_at, _)| *retry_at);
    let (agent, retry_at, event) = &waiting[0];
    let extra = if waiting.len() > 1 {
        format!("{}{} more", ui::FIELD_SEPARATOR, waiting.len() - 1)
    } else {
        String::new()
    };
    Ok(CachedQuotaFooter {
        line: format!(
            "waiting{}{}{}{agent}: {}{extra}{}{}",
            ui::FIELD_SEPARATOR,
            retry_detail(*retry_at).trim(),
            ui::FIELD_SEPARATOR,
            clean_runtime_event(event),
            ui::FIELD_SEPARATOR,
            load_codex_rate_limits_if_interactive(&codex_config)
                .map(|limits| limits.to_string())
                .unwrap_or_else(|| "usage unavailable".to_owned())
        ),
        waiting: true,
    })
}

pub(super) fn load_codex_rate_limits_if_interactive(
    config: &crate::config::CodexProviderConfig,
) -> Option<CodexRateLimits> {
    std::io::stdout()
        .is_terminal()
        .then(|| crate::provider::codex::load_active_rate_limits(config))
        .flatten()
}

pub(super) fn list_channels_only(project: &ProjectPaths) -> Result<()> {
    for channel in list_channels(project)? {
        let path = project.channel_dir(&channel);
        println!(
            "{:<32} artifacts: {}{}",
            channel,
            count_channel_artifacts(&path)?,
            latest_channel_artifact(&path)?
                .map(|artifact| format!(" latest: {artifact}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}
