use super::*;

#[derive(Default)]
pub(in crate::app) struct DashboardSnapshot {
    pub(in crate::app) schema: DashboardSchema,
    pub(in crate::app) roles: Vec<DashboardRole>,
    pub(in crate::app) channels: Vec<StatusChannelRow>,
    pub(in crate::app) queues: Vec<StatusQueueRow>,
    pub(in crate::app) sessions: Vec<DashboardSession>,
    pub(in crate::app) notices: Vec<DashboardNotice>,
    pub(in crate::app) notices_loading: bool,
    pub(in crate::app) notices_updated_at: Option<u64>,
}

#[derive(Default)]
pub(in crate::app) struct DashboardSchema {
    pub(in crate::app) config: ProjectConfig,
    pub(in crate::app) project_md_exists: bool,
    pub(in crate::app) roles: Vec<DashboardSchemaRole>,
}

pub(in crate::app) struct DashboardSchemaRole {
    pub(in crate::app) slug: RoleSlug,
    pub(in crate::app) config: RoleConfig,
    pub(in crate::app) role_md_exists: bool,
    pub(in crate::app) step_files: Vec<String>,
    pub(in crate::app) missing_steps: Vec<String>,
    pub(in crate::app) extra_step_files: Vec<String>,
}

#[derive(Clone)]
pub(in crate::app) struct DashboardNotice {
    pub(in crate::app) severity: NoticeSeverity,
    pub(in crate::app) text: String,
}

impl DashboardNotice {
    pub(in crate::app) fn severity(&self) -> NoticeSeverity {
        self.severity
    }

    pub(in crate::app) fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Clone)]
pub(in crate::app) struct DashboardSession {
    pub(in crate::app) id: String,
    pub(in crate::app) kind: String,
    pub(in crate::app) command: String,
    pub(in crate::app) created_at: u64,
    pub(in crate::app) title: String,
    pub(in crate::app) preview: String,
    pub(in crate::app) root: PathBuf,
    pub(in crate::app) transcript: Option<PathBuf>,
    pub(in crate::app) reply: Option<PathBuf>,
}

impl QueryMatch for DashboardSession {
    fn matches_query(&self, query: &str) -> bool {
        query.trim().is_empty()
            || text_matches_query(
                [
                    self.id.clone(),
                    self.kind.clone(),
                    self.command.clone(),
                    self.title.clone(),
                    self.preview.clone(),
                ],
                query,
            )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::app) enum NoticeSeverity {
    Info,
    Action,
    Warn,
    Error,
}

impl DashboardSnapshot {
    pub(in crate::app) fn state_signature(&self) -> String {
        let mut signature = String::new();
        for role in &self.roles {
            let _ = write!(
                signature,
                "role\0{}\0{}\0{}\0{}\0{}\0{};",
                role.slug,
                role.status,
                role.mode,
                role.parallel,
                role.auto_archive,
                role.trigger_count
            );
            for agent in &role.agents {
                let _ = write!(
                    signature,
                    "agent\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{};",
                    agent.role,
                    agent.agent,
                    agent.status,
                    agent.summary,
                    agent.detail,
                    agent.current_step,
                    agent.run_count,
                    agent.quota_waiting,
                    agent.supervisor_status,
                    agent.next_retry_at.unwrap_or_default(),
                    agent.latest_output_at.unwrap_or_default(),
                    agent.paused_by_user,
                    agent.created_at,
                    agent.updated_at,
                    agent.steer.pending_count,
                    agent.steer.last_sent_at.unwrap_or_default(),
                    agent.steer.last_error_at.unwrap_or_default()
                );
            }
        }
        for queue in &self.queues {
            let _ = write!(
                signature,
                "queue\0{}\0{}\0{}\0{}\0{}\0{};",
                queue.kind.label(),
                queue.name,
                queue.count,
                queue.locked,
                queue
                    .active
                    .as_ref()
                    .map(|active| active.label.as_str())
                    .unwrap_or(""),
                queue.locked_at.unwrap_or_default()
            );
        }
        for channel in &self.channels {
            let _ = write!(
                signature,
                "channel\0{}\0{}\0{};",
                channel.name,
                channel.artifacts,
                channel.latest.as_deref().unwrap_or("")
            );
        }
        signature
    }

    pub(in crate::app) fn rows(&self) -> Vec<DashboardSelection> {
        let mut rows = Vec::new();
        for (role_index, role) in self.roles.iter().enumerate() {
            if role_index > 0 {
                rows.push(DashboardSelection::Spacer);
            }
            rows.push(DashboardSelection::Role(role_index));
            for agent_index in 0..role.agents.len() {
                rows.push(DashboardSelection::Agent(role_index, agent_index));
            }
        }
        rows
    }

    pub(in crate::app) fn rows_matching(&self, query: &str) -> Vec<DashboardSelection> {
        let query = query.trim();
        if query.is_empty() {
            return self.rows();
        }
        let mut rows = Vec::new();
        for (role_index, role) in self.roles.iter().enumerate() {
            let role_matches = role.matches_query(query);
            let matching_agents = role
                .agents
                .iter()
                .enumerate()
                .filter_map(|(agent_index, agent)| {
                    (role_matches || agent.matches_query(query)).then_some(agent_index)
                })
                .collect::<Vec<_>>();
            if !role_matches && matching_agents.is_empty() {
                continue;
            }
            if !rows.is_empty() {
                rows.push(DashboardSelection::Spacer);
            }
            rows.push(DashboardSelection::Role(role_index));
            rows.extend(
                matching_agents
                    .into_iter()
                    .map(|agent_index| DashboardSelection::Agent(role_index, agent_index)),
            );
        }
        rows
    }

    pub(in crate::app) fn summary_width_for_rows(&self, rows: &[DashboardSelection]) -> usize {
        rows.iter()
            .filter_map(|row| match row {
                DashboardSelection::Agent(role_index, agent_index) => {
                    Some(&self.roles[*role_index].agents[*agent_index].summary)
                }
                DashboardSelection::Spacer | DashboardSelection::Role(_) => None,
            })
            .map(|summary| summary.chars().count())
            .max()
            .unwrap_or(0)
    }

    pub(in crate::app) fn agent_width_for_rows(&self, rows: &[DashboardSelection]) -> usize {
        rows.iter()
            .filter_map(|row| match row {
                DashboardSelection::Agent(role_index, agent_index) => {
                    Some(self.roles[*role_index].agents[*agent_index].agent.as_str())
                }
                DashboardSelection::Spacer | DashboardSelection::Role(_) => None,
            })
            .map(str::chars)
            .map(Iterator::count)
            .max()
            .unwrap_or(0)
    }
}

pub(in crate::app) struct DashboardRole {
    pub(in crate::app) slug: RoleSlug,
    pub(in crate::app) status: RoleStatus,
    pub(in crate::app) mode: RoleMode,
    pub(in crate::app) parallel: String,
    pub(in crate::app) expose: String,
    pub(in crate::app) auto_archive: bool,
    pub(in crate::app) trigger_count: usize,
    pub(in crate::app) agents: Vec<DashboardAgent>,
}

impl QueryMatch for DashboardRole {
    fn matches_query(&self, query: &str) -> bool {
        text_matches_query(
            [
                self.slug.to_string(),
                self.status.to_string(),
                self.mode.to_string(),
                self.parallel.clone(),
                self.expose.clone(),
            ],
            query,
        )
    }
}

impl DashboardRole {
    pub(in crate::app) fn state_lines(&self, width: usize) -> Vec<Line<'static>> {
        let mut lines = vec![
            state_heading_line(
                "role",
                &self.slug.to_string(),
                &self.status.to_string(),
                dashboard_role_style(self.status),
                width,
            ),
            Line::from(vec![
                Span::styled("mode ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.mode.to_string(), Style::default().fg(Color::White)),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("parallel ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.parallel.clone(), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(vec![
                Span::styled("expose ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.expose).to_owned(),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::styled("auto archive ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.auto_archive.to_string(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("triggers ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.trigger_count.to_string(),
                    Style::default().fg(Color::White),
                ),
            ]),
        ];
        lines.push(self.action_hint_line());
        lines
    }

    pub(in crate::app) fn action_hint_line(&self) -> Line<'static> {
        let mut spans = vec![Span::styled(
            "actions ",
            Style::default().fg(Color::DarkGray),
        )];
        spans.extend([
            footer_key("n"),
            Span::raw(" new agent  "),
            footer_key("Enter"),
        ]);
        spans.push(Span::raw(" detail"));
        match self.status {
            RoleStatus::Active => {
                spans.extend([Span::raw("  "), footer_key("Space"), Span::raw(" pause")])
            }
            RoleStatus::Paused => {
                spans.extend([Span::raw("  "), footer_key("Space"), Span::raw(" unpause")]);
            }
            RoleStatus::Draft => {}
        }
        Line::from(spans)
    }

    pub(in crate::app) fn detail_lines(&self) -> Vec<Line<'static>> {
        vec![
            Line::from(vec![
                Span::styled("role ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.slug.to_string(), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(format!("status: {}", self.status)),
            Line::from(format!("mode: {}", self.mode)),
            Line::from(format!("parallel: {}", self.parallel)),
            Line::from(format!("expose: {}", empty_dash(&self.expose))),
            Line::from(format!("auto archive: {}", self.auto_archive)),
            Line::from(format!("triggers: {}", self.trigger_count)),
        ]
    }
}

pub(in crate::app) struct DashboardAgent {
    pub(in crate::app) role: RoleSlug,
    pub(in crate::app) agent: AgentId,
    pub(in crate::app) status: AgentStatus,
    pub(in crate::app) summary: String,
    pub(in crate::app) detail: String,
    pub(in crate::app) channels: String,
    pub(in crate::app) current_step: String,
    pub(in crate::app) run_count: u64,
    pub(in crate::app) pane_id: Option<String>,
    pub(in crate::app) quota_waiting: bool,
    pub(in crate::app) supervisor_status: SupervisorStatus,
    pub(in crate::app) supervisor_updated_at: u64,
    pub(in crate::app) child_pid: Option<u32>,
    pub(in crate::app) next_retry_at: Option<u64>,
    pub(in crate::app) latest_output_at: Option<u64>,
    pub(in crate::app) running_cause: Option<String>,
    pub(in crate::app) steer: SteerStatus,
    pub(in crate::app) paused_by_user: bool,
    pub(in crate::app) created_at: u64,
    pub(in crate::app) updated_at: u64,
}

impl QueryMatch for DashboardAgent {
    fn matches_query(&self, query: &str) -> bool {
        text_matches_query(
            [
                self.role.to_string(),
                self.agent.to_string(),
                self.status.to_string(),
                self.summary.clone(),
                self.detail.clone(),
                self.channels.clone(),
                self.current_step.clone(),
                self.supervisor_status.to_string(),
                self.running_cause.clone().unwrap_or_default(),
                steer_status_detail(
                    &self.steer,
                    matches!(self.status, AgentStatus::Starting | AgentStatus::Running),
                )
                .unwrap_or_default(),
            ],
            query,
        )
    }
}

impl DashboardAgent {
    pub(in crate::app) fn state_lines(&self, width: usize) -> Vec<Line<'static>> {
        let summary_style = if self.summary == "*name loading*" {
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC)
        } else {
            Style::default().fg(Color::White)
        };
        let status_style = dashboard_agent_style(self.status, self.quota_waiting);
        let mut lines = vec![
            state_heading_line(
                "agent",
                &format!("{}/{}", self.role, self.agent),
                &self.summary,
                summary_style,
                width,
            ),
            Line::from(vec![
                Span::styled("status ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.status.to_string(), status_style),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("detail ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.detail).to_owned(),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::styled("step ", Style::default().fg(Color::DarkGray)),
                Span::styled(self.current_step.clone(), Style::default().fg(Color::Cyan)),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    event_age(self.updated_at),
                    Style::default().fg(Color::White),
                ),
            ]),
            self.runtime_progress_line(),
            Line::from(vec![
                Span::styled("channels ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&self.channels).to_owned(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("runs ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.run_count.to_string(),
                    Style::default().fg(Color::White),
                ),
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("session ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    self.pane_id.clone().unwrap_or_else(|| "-".to_owned()),
                    Style::default().fg(Color::White),
                ),
            ]),
        ];
        if let Some(cause) = self.running_cause.as_deref() {
            lines.push(Line::from(vec![
                Span::styled("why ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    ellipsize_display(cause, width.saturating_sub(4)),
                    Style::default().fg(Color::Cyan),
                ),
            ]));
        }
        if let Some(line) = steer_status_line(
            &self.steer,
            matches!(self.status, AgentStatus::Starting | AgentStatus::Running),
        ) {
            lines.push(line);
        }
        lines.push(self.action_hint_line());
        lines
    }

    pub(in crate::app) fn runtime_progress_line(&self) -> Line<'static> {
        let mut spans = vec![
            Span::styled("runtime ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.supervisor_status.to_string(),
                supervisor_status_style(self.supervisor_status),
            ),
            Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
            Span::styled("seen ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.latest_output_at
                    .map(event_age)
                    .unwrap_or_else(|| "-".to_owned()),
                Style::default().fg(Color::White),
            ),
            Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
            Span::styled("supervisor ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                event_age(self.supervisor_updated_at),
                Style::default().fg(Color::White),
            ),
        ];
        if let Some(pid) = self.child_pid {
            spans.extend([
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("pid ", Style::default().fg(Color::DarkGray)),
                Span::styled(pid.to_string(), Style::default().fg(Color::White)),
            ]);
        }
        if let Some(retry_at) = self.next_retry_at {
            spans.extend([
                Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
                Span::styled("retry ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format_unix_time_compact(retry_at),
                    Style::default().fg(Color::Yellow),
                ),
            ]);
        }
        Line::from(spans)
    }

    pub(in crate::app) fn action_hint_line(&self) -> Line<'static> {
        let mut spans = vec![Span::styled(
            "actions ",
            Style::default().fg(Color::DarkGray),
        )];
        match self.status {
            AgentStatus::Starting | AgentStatus::Running => {
                spans.extend([
                    footer_key("a"),
                    Span::raw(" attach  "),
                    footer_key("m"),
                    Span::raw(" steer  "),
                    footer_key("Space"),
                    Span::raw(" pause  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                ]);
            }
            AgentStatus::Paused => {
                spans.extend([
                    footer_key("Space"),
                    Span::raw(" unpause  "),
                    footer_key("m"),
                    Span::raw(" more  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                ]);
            }
            AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention => {
                spans.extend([
                    footer_key("m"),
                    Span::raw(" more  "),
                    footer_key("Enter"),
                    Span::raw(" detail"),
                    Span::raw("  "),
                    footer_key(":"),
                    Span::raw(" commands"),
                ]);
            }
        }
        Line::from(spans)
    }
}

#[derive(Clone, Copy)]
pub(in crate::app) enum DashboardSelection {
    Spacer,
    Role(usize),
    Agent(usize, usize),
}

impl DashboardSelection {
    pub(in crate::app) fn is_selectable(self) -> bool {
        !matches!(self, Self::Spacer)
    }
}

pub(in crate::app) fn agent_count_in_rows(rows: &[DashboardSelection]) -> usize {
    rows.iter()
        .filter(|row| matches!(row, DashboardSelection::Agent(_, _)))
        .count()
}

pub(in crate::app) fn selected_agent_position_in_rows(
    rows: &[DashboardSelection],
    selected: usize,
) -> Option<(usize, usize)> {
    let total = agent_count_in_rows(rows);
    if total == 0 || !matches!(rows.get(selected), Some(DashboardSelection::Agent(_, _))) {
        return None;
    }
    let current = rows
        .iter()
        .take(selected + 1)
        .filter(|row| matches!(row, DashboardSelection::Agent(_, _)))
        .count();
    Some((current, total))
}

#[derive(Clone, Copy)]
pub(in crate::app) enum QueueSelection {
    Header(usize),
    Item {
        queue_index: usize,
        item_index: usize,
    },
}

pub(in crate::app) fn load_dashboard_schema(project: &ProjectPaths) -> Result<DashboardSchema> {
    let mut roles = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        let step_files = role_step_prompt_files(&role_paths)?;
        let configured_steps = config
            .steps
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let missing_steps = config
            .steps
            .iter()
            .filter_map(|step| (!role_paths.step_path(step).exists()).then_some(step.to_string()))
            .collect();
        let extra_step_files = step_files
            .iter()
            .filter(|step| !configured_steps.contains(*step))
            .cloned()
            .collect();
        roles.push(DashboardSchemaRole {
            slug: role,
            config,
            role_md_exists: role_paths.role_md().exists(),
            step_files,
            missing_steps,
            extra_step_files,
        });
    }
    Ok(DashboardSchema {
        config: project_config(project)?,
        project_md_exists: project.project_md().exists(),
        roles,
    })
}

pub(in crate::app) fn role_step_prompt_files(role_paths: &RolePaths) -> Result<Vec<String>> {
    let mut files = io::collect_existing_dir(&role_paths.steps_dir(), |entry| {
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            == Some("md")
            && let Some(stem) = entry.path().file_stem().and_then(|stem| stem.to_str())
        {
            return Ok(Some(stem.to_owned()));
        }
        Ok(None)
    })?;
    files.sort();
    Ok(files)
}

pub(in crate::app) fn load_dashboard_snapshot(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
) -> Result<DashboardSnapshot> {
    let roles = if let Some(role) = role_filter {
        vec![role.clone()]
    } else {
        list_roles(project)?
    };
    let (notices, notices_loading, notices_updated_at) = load_notice_lines(project)?;
    let mut snapshot = DashboardSnapshot {
        schema: load_dashboard_schema(project)?,
        channels: load_status_channel_rows(project)?,
        queues: load_all_status_queue_rows(project)?,
        sessions: load_dashboard_sessions(project)?,
        notices,
        notices_loading,
        notices_updated_at,
        ..DashboardSnapshot::default()
    };
    for role in roles {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        let active = active_agent_count(&role_paths)?;
        let mut agents = Vec::new();
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived && !include_archived {
                continue;
            }
            let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
            let supervisor = load_supervisor_state(&agent_paths)?;
            let quota_waiting = matches!(
                supervisor.status,
                SupervisorStatus::WaitingForQuota | SupervisorStatus::WaitingForProvider
            );
            let running_cause = dashboard_agent_running_cause(&agent_paths, &state, &supervisor)?;
            let steer = crate::backend::steer_status(&agent_paths.steer_dir())?;
            let current_step = config
                .steps
                .get(state.current_step % config.steps.len().max(MIN_ROLE_STEP_COUNT))
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_owned());
            agents.push(DashboardAgent {
                role: role.clone(),
                agent: agent.clone(),
                status: state.status,
                summary,
                detail,
                channels: state
                    .channels
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                current_step,
                run_count: state.run_count,
                pane_id: state.pane_id.clone(),
                quota_waiting,
                supervisor_status: supervisor.status,
                supervisor_updated_at: supervisor.updated_at,
                child_pid: supervisor.child_pid,
                next_retry_at: supervisor.next_retry_at,
                latest_output_at: latest_agent_output_at(&agent_paths, &state, &supervisor),
                running_cause,
                steer,
                paused_by_user: state.paused_by_user,
                created_at: state.created_at,
                updated_at: state.updated_at,
            });
        }
        snapshot.roles.push(DashboardRole {
            slug: role,
            status: config.status,
            mode: config.mode,
            parallel: format!("{active}/{}", config.parallel),
            expose: config
                .expose
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            auto_archive: config.auto_archive,
            trigger_count: config.triggers.len(),
            agents,
        });
    }
    Ok(snapshot)
}

pub(in crate::app) fn dashboard_agent_running_cause(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    supervisor: &SupervisorState,
) -> Result<Option<String>> {
    if !matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        return Ok(None);
    }
    if supervisor.status == SupervisorStatus::WaitingForQuota {
        return Ok(Some(
            supervisor
                .last_event
                .as_ref()
                .map(|event| format!("quota wait · {event}"))
                .unwrap_or_else(|| "quota wait".to_owned()),
        ));
    }
    if supervisor.status == SupervisorStatus::WaitingForProvider {
        return Ok(Some(
            supervisor
                .last_event
                .as_ref()
                .map(|event| format!("provider wait · {event}"))
                .unwrap_or_else(|| "provider wait".to_owned()),
        ));
    }
    if let Some(trigger) = trigger_context_markdown_summary(&agent_paths.trigger_context())? {
        return Ok(Some(format!("triggered · {trigger}")));
    }
    if let Some(note) = state.note.as_deref()
        && !note.trim().is_empty()
    {
        return Ok(Some(note.trim().to_owned()));
    }
    if supervisor.status == SupervisorStatus::Restarting {
        return Ok(Some("runtime restart".to_owned()));
    }
    Ok(Some("user-started or resumed manually".to_owned()))
}

pub(in crate::app) fn latest_agent_output_at(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    supervisor: &SupervisorState,
) -> Option<u64> {
    supervisor
        .last_run_id
        .or_else(|| {
            matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
                .then_some(state.run_count + 1)
        })
        .and_then(|run_id| file_modified_unix(&agent_paths.run(run_id).transcript_text()))
        .or_else(|| {
            (1..=state.run_count)
                .rev()
                .find_map(|run_id| file_modified_unix(&agent_paths.run(run_id).transcript_text()))
        })
}

pub(in crate::app) fn trigger_context_markdown_summary(path: &Path) -> Result<Option<String>> {
    let Some(text) = io::read_optional_text(path)? else {
        return Ok(None);
    };
    let mut kind = None;
    let mut details = Vec::new();
    for line in text.lines() {
        let Some((key, value)) = line
            .trim()
            .strip_prefix("- ")
            .and_then(|line| line.split_once(':'))
        else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('`');
        match key {
            "trigger kind" => kind = Some(value.to_owned()),
            "source role" | "source agent" | "queue" | "reason" => {
                details.push(value.to_owned());
            }
            _ => {}
        }
    }
    Ok(kind.map(|kind| {
        if details.is_empty() {
            kind
        } else {
            format!("{kind} · {}", details.join("/"))
        }
    }))
}

pub(in crate::app) fn load_dashboard_ui_state(project: &ProjectPaths) -> Result<DashboardUiState> {
    let path = dashboard_ui_state_path(project);
    let Some(state) = io::read_optional_text(&path)? else {
        return Ok(DashboardUiState::default());
    };
    let parsed = toml::from_str::<DashboardUiState>(&state)
        .with_context(|| format!("Failed to parse `{}`", path.display()))?;
    if parsed.version == DASHBOARD_UI_STATE_VERSION {
        Ok(parsed)
    } else {
        Ok(DashboardUiState::default())
    }
}

pub(in crate::app) fn dashboard_ui_state_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("dashboard.toml")
}
