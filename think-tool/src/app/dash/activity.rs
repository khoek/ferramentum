use super::*;

#[derive(Clone)]
pub(in crate::app) struct ProjectEvent {
    pub(in crate::app) timestamp: u64,
    pub(in crate::app) lane: EventLane,
    pub(in crate::app) title: String,
    pub(in crate::app) detail: String,
    pub(in crate::app) target: EventTarget,
}

#[derive(Clone, PartialEq, Eq)]
pub(in crate::app) struct ProjectEventKey {
    pub(in crate::app) timestamp: u64,
    pub(in crate::app) lane: EventLane,
    pub(in crate::app) title: String,
    pub(in crate::app) detail: String,
}

#[derive(Clone)]
pub(in crate::app) enum EventTarget {
    Agent { role: RoleSlug, agent: AgentId },
    Queue(StatusQueueKey),
    Notice,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::app) enum EventFilter {
    All,
    Agents,
    Runs,
    Triggers,
    Notices,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::app) enum EventLane {
    Agent,
    Run,
    Trigger,
    Notice,
}

impl EventLane {
    pub(in crate::app) fn label(self) -> &'static str {
        match self {
            Self::Agent => "agents",
            Self::Run => "runs",
            Self::Trigger => "triggers",
            Self::Notice => "notices",
        }
    }

    pub(in crate::app) fn symbol(self) -> &'static str {
        match self {
            Self::Agent => "●",
            Self::Run => "◆",
            Self::Trigger => "▲",
            Self::Notice => "!",
        }
    }

    pub(in crate::app) fn style(self) -> Style {
        match self {
            Self::Agent => Style::default().fg(Color::Blue),
            Self::Run => Style::default().fg(Color::Green),
            Self::Trigger => Style::default().fg(Color::Magenta),
            Self::Notice => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        }
    }

    pub(in crate::app) fn all() -> &'static [Self] {
        &[Self::Agent, Self::Run, Self::Trigger, Self::Notice]
    }
}

impl EventFilter {
    pub(in crate::app) fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Agents => "agents",
            Self::Runs => "runs",
            Self::Triggers => "triggers",
            Self::Notices => "notices",
        }
    }

    pub(in crate::app) fn matches(self, lane: EventLane) -> bool {
        match self {
            Self::All => true,
            Self::Agents => lane == EventLane::Agent,
            Self::Runs => lane == EventLane::Run,
            Self::Triggers => lane == EventLane::Trigger,
            Self::Notices => lane == EventLane::Notice,
        }
    }
}

impl ProjectEvent {
    pub(in crate::app) fn key(&self) -> ProjectEventKey {
        ProjectEventKey {
            timestamp: self.timestamp,
            lane: self.lane,
            title: self.title.clone(),
            detail: self.detail.clone(),
        }
    }
}

pub(in crate::app) fn restore_event_selection(
    events: &[ProjectEvent],
    selected: &mut usize,
    key: Option<&ProjectEventKey>,
) {
    if let Some(key) = key
        && let Some(index) = events.iter().position(|event| event.key() == *key)
    {
        *selected = index;
        return;
    }
    *selected = (*selected).min(events.len().saturating_sub(1));
}

pub(in crate::app) fn project_event_lines(
    events: &[ProjectEvent],
    width: usize,
    selected: usize,
    filter: EventFilter,
    query: &str,
    search_active: bool,
) -> Vec<Line<'static>> {
    if events.is_empty() {
        return vec![
            event_filter_line(filter, query, search_active),
            Line::from(Span::styled(
                "No matching project events.",
                Style::default().fg(Color::DarkGray),
            )),
        ];
    }
    let mut lines = vec![event_filter_line(filter, query, search_active)];
    lines.extend(event_graph_lines(events, width));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "recent events",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )));
    for (index, event) in events
        .iter()
        .take(DASHBOARD_EVENT_LOG_MAX_EVENTS)
        .enumerate()
    {
        lines.push(event_line(event, index == selected));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        footer_key("Enter"),
        Span::raw(" jump  "),
        footer_key("a/r/t/n/0"),
        Span::raw(" filter  "),
        footer_key("/"),
        Span::raw(" search  "),
        footer_key("↑↓"),
        Span::raw(" scroll  "),
        footer_key("Esc"),
        Span::raw(" dashboard"),
    ]));
    lines
}

pub(in crate::app) fn event_filter_line(
    filter: EventFilter,
    query: &str,
    search_active: bool,
) -> Line<'static> {
    Line::from(vec![
        Span::styled("filter ", Style::default().fg(Color::DarkGray)),
        Span::styled(filter.label(), Style::default().fg(Color::Cyan)),
        Span::styled(" · search ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if query.trim().is_empty() {
                if search_active { "" } else { "-" }
            } else {
                query.trim()
            }
            .to_owned(),
            if search_active {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            },
        ),
    ])
}

pub(in crate::app) fn load_project_events(project: &ProjectPaths) -> Result<Vec<ProjectEvent>> {
    let mut events = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            events.push(ProjectEvent {
                timestamp: state.created_at,
                lane: EventLane::Agent,
                title: format!("{role}/{agent} created"),
                detail: String::new(),
                target: EventTarget::Agent {
                    role: role.clone(),
                    agent: agent.clone(),
                },
            });
            events.push(ProjectEvent {
                timestamp: state.updated_at,
                lane: EventLane::Agent,
                title: format!("{role}/{agent} {}", state.status),
                detail: state.note.clone().unwrap_or_default(),
                target: EventTarget::Agent {
                    role: role.clone(),
                    agent: agent.clone(),
                },
            });
            for run_id in 1..=state.run_count {
                let exit_path = agent_paths.run(run_id).exit();
                match read_run_exit(&agent_paths.run(run_id), &state) {
                    Ok(Some(exit)) => events.push(ProjectEvent {
                        timestamp: exit.finished_at,
                        lane: EventLane::Run,
                        title: format!("{role}/{agent} run {run_id} {}", exit.step),
                        detail: run_event_detail(&exit),
                        target: EventTarget::Agent {
                            role: role.clone(),
                            agent: agent.clone(),
                        },
                    }),
                    Ok(None) => {}
                    Err(err) => events.push(ProjectEvent {
                        timestamp: fs::metadata(&exit_path)
                            .and_then(|metadata| metadata.modified())
                            .ok()
                            .and_then(system_time_to_unix)
                            .unwrap_or(state.updated_at),
                        lane: EventLane::Run,
                        title: format!("{role}/{agent} run {run_id} exit unreadable"),
                        detail: err.to_string(),
                        target: EventTarget::Agent {
                            role: role.clone(),
                            agent: agent.clone(),
                        },
                    }),
                }
            }
        }
    }
    load_queue_events(project, &mut events)?;
    load_notice_events(project, &mut events)?;
    events.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| left.title.cmp(&right.title))
    });
    events.dedup_by(|left, right| {
        left.timestamp == right.timestamp
            && left.lane == right.lane
            && left.title == right.title
            && left.detail == right.detail
    });
    Ok(events)
}

pub(in crate::app) fn run_event_detail(exit: &RunExitState) -> String {
    let mut detail = format!(
        "{}{}",
        if exit.success { "success" } else { "failed" },
        exit.disposition
            .map(|disposition| format!(" · disposition: {disposition}"))
            .unwrap_or_default()
    );
    if let Some(message) = &exit.message
        && !message.trim().is_empty()
    {
        detail.push_str(ui::FIELD_SEPARATOR);
        detail.push_str(message.trim());
    }
    detail
}

pub(in crate::app) fn load_queue_events(
    project: &ProjectPaths,
    events: &mut Vec<ProjectEvent>,
) -> Result<()> {
    let queue_dir = project.runtime_dir().join("trigger-queues");
    if queue_dir.exists() {
        for entry in fs::read_dir(&queue_dir)
            .with_context(|| format!("Failed to read `{}`", queue_dir.display()))?
        {
            let entry = entry.context("Failed to read trigger queue entry")?;
            if entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                != Some("toml")
            {
                continue;
            }
            let path = entry.path();
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            for item in load_trigger_queue(project, name)?.items {
                events.push(ProjectEvent {
                    timestamp: item.enqueued_at,
                    lane: EventLane::Trigger,
                    title: format!("{} trigger queued", item.role),
                    detail: trigger_cause_summary(&item.cause),
                    target: EventTarget::Queue(StatusQueueKey {
                        kind: StatusQueueKind::Trigger,
                        name: name.to_owned(),
                    }),
                });
            }
        }
    }
    Ok(())
}

pub(in crate::app) fn load_notice_events(
    project: &ProjectPaths,
    events: &mut Vec<ProjectEvent>,
) -> Result<()> {
    let Some(text) = io::read_optional_text(&notice_current_path(project))? else {
        return Ok(());
    };
    let timestamp = fs::metadata(notice_current_path(project))
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(system_time_to_unix)
        .unwrap_or_else(unix_timestamp);
    for notice in text.lines().filter_map(parse_notice_line) {
        events.push(ProjectEvent {
            timestamp,
            lane: EventLane::Notice,
            title: notice.text,
            detail: notice_severity_label(notice.severity).to_owned(),
            target: EventTarget::Notice,
        });
    }
    Ok(())
}

pub(in crate::app) fn notice_severity_label(severity: NoticeSeverity) -> &'static str {
    match severity {
        NoticeSeverity::Info => "info",
        NoticeSeverity::Action => "action",
        NoticeSeverity::Warn => "warn",
        NoticeSeverity::Error => "error",
    }
}

pub(in crate::app) fn event_graph_lines(
    events: &[ProjectEvent],
    width: usize,
) -> Vec<Line<'static>> {
    let graph_width = width
        .saturating_sub(DASHBOARD_EVENT_TIMELINE_WIDTH)
        .max(DASHBOARD_EVENT_TIMELINE_WIDTH)
        .min(width.saturating_sub(12).max(DASHBOARD_EVENT_TIMELINE_WIDTH));
    let newest = events
        .iter()
        .map(|event| event.timestamp)
        .max()
        .unwrap_or(0);
    let oldest = events
        .iter()
        .map(|event| event.timestamp)
        .min()
        .unwrap_or(newest);
    let span = newest.saturating_sub(oldest).max(1);
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                "activity map",
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(
                    "  oldest {} · newest {}",
                    format_unix_time(oldest),
                    format_unix_time(newest)
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        Line::from(Span::styled(
            format!("        {}▶", "─".repeat(graph_width)),
            Style::default().fg(Color::DarkGray),
        )),
    ];
    for lane in EventLane::all() {
        let mut cells = vec![None; graph_width.max(1)];
        for event in events.iter().filter(|event| event.lane == *lane) {
            let offset = event.timestamp.saturating_sub(oldest);
            let index = ((offset as f64 / span as f64) * (cells.len().saturating_sub(1) as f64))
                .round() as usize;
            let last = cells.len() - 1;
            cells[index.min(last)] = Some(*lane);
        }
        let mut spans = vec![Span::styled(
            format!("{:<8}", lane.label()),
            Style::default().fg(Color::DarkGray),
        )];
        for cell in cells {
            if let Some(lane) = cell {
                spans.push(Span::styled(lane.symbol(), lane.style()));
            } else {
                spans.push(Span::styled("·", Style::default().fg(Color::DarkGray)));
            }
        }
        lines.push(Line::from(spans));
    }
    lines
}

pub(in crate::app) fn event_line(event: &ProjectEvent, selected: bool) -> Line<'static> {
    let detail = if event.detail.trim().is_empty() {
        String::new()
    } else {
        format!("{}{}", ui::FIELD_SEPARATOR, event.detail.trim())
    };
    Line::from(vec![
        dashboard_span(
            if selected { "▸ " } else { "  " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(event.lane.symbol(), event.lane.style(), selected),
        dashboard_span(" ", Style::default(), selected),
        dashboard_span(
            format!("{:<16}", event_age(event.timestamp)),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            event.title.clone(),
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(detail, Style::default().fg(Color::DarkGray), selected),
    ])
}

pub(in crate::app) fn status_project(
    project: &ProjectPaths,
    role_filter: Option<&RoleSlug>,
    include_archived: bool,
    offer_report: bool,
) -> Result<()> {
    if offer_report && std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        return run_dashboard(
            project.clone(),
            role_filter.cloned(),
            include_archived,
            DASHBOARD_REFRESH_INTERVAL,
        );
    }
    fire_queue_idle_triggers(project)?;
    let roles = load_status_roles(project, role_filter, include_archived)?;
    let queue_rows = load_status_queue_rows(project)?;
    let widths = StatusWidths::from_rows(&roles, &queue_rows);

    println!("{}", ui::section_heading("roles"));
    for (index, role) in roles.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_role_row(role, &widths);
        for agent in &role.agents {
            print_agent_row(agent, &widths);
        }
    }

    if !queue_rows.is_empty() {
        println!("\n{}", ui::section_heading("queues"));
        for row in &queue_rows {
            print_queue_row(row, &widths);
        }
    }

    println!("\n{}", ui::section_heading("quota"));
    print_quota_footer(project)?;
    Ok(())
}

pub(in crate::app) fn load_dashboard_sessions(
    project: &ProjectPaths,
) -> Result<Vec<DashboardSession>> {
    let mut sessions = load_command_sessions(project)?;
    sessions.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(sessions)
}

pub(in crate::app) fn load_command_sessions(
    project: &ProjectPaths,
) -> Result<Vec<DashboardSession>> {
    let commands_dir = project.runtime_dir().join("commands");
    let mut sessions = Vec::new();
    for (command, command_path) in io::collect_existing_dir(&commands_dir, |entry| {
        if !entry.file_type()?.is_dir() {
            return Ok(None);
        }
        Ok(Some((
            entry.file_name().to_string_lossy().into_owned(),
            entry.path(),
        )))
    })? {
        for (session_id, session_path) in io::collect_existing_dir(&command_path, |entry| {
            if !entry.file_type()?.is_dir() {
                return Ok(None);
            }
            Ok(Some((
                entry.file_name().to_string_lossy().into_owned(),
                entry.path(),
            )))
        })? {
            if let Some(session) = load_command_session(&command, &session_id, session_path)? {
                sessions.push(session);
            }
        }
    }
    Ok(sessions)
}

pub(in crate::app) fn load_command_session(
    command: &str,
    session_id: &str,
    root: PathBuf,
) -> Result<Option<DashboardSession>> {
    let Some((_turn, turn_root)) = latest_command_turn(&root)? else {
        return Ok(None);
    };
    let prompt = turn_root.join("PROMPT.md");
    let reply = turn_root.join("REPLY.md");
    let transcript = turn_root.join("TRANSCRIPT.txt");
    let text = io::read_optional_text(&reply)?
        .filter(|text| !text.trim().is_empty())
        .or_else(|| io::read_optional_text(&prompt).ok().flatten())
        .unwrap_or_default();
    let (title, preview) =
        session_title_and_preview(&text, &format!("think {command} conversation {session_id}"));
    Ok(Some(DashboardSession {
        id: format!("{command}/{session_id}"),
        kind: "command".to_owned(),
        command: command.to_owned(),
        created_at: session_id
            .parse::<u64>()
            .ok()
            .or_else(|| file_modified_unix(&root))
            .unwrap_or_default(),
        title,
        preview,
        root,
        transcript: Some(transcript),
        reply: Some(reply),
    }))
}

pub(in crate::app) fn latest_command_turn(root: &Path) -> Result<Option<(u64, PathBuf)>> {
    let mut latest = None;
    for entry in
        fs::read_dir(root).with_context(|| format!("Failed to read `{}`", root.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(turn) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
        else {
            continue;
        };
        if latest
            .as_ref()
            .is_none_or(|(latest_turn, _): &(u64, PathBuf)| turn > *latest_turn)
        {
            latest = Some((turn, entry.path()));
        }
    }
    Ok(latest)
}

pub(in crate::app) fn session_title_and_preview(
    text: &str,
    fallback_title: &str,
) -> (String, String) {
    let mut nonempty = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let title = nonempty
        .next()
        .map(|line| line.trim_start_matches('#').trim())
        .filter(|line| !line.is_empty())
        .map(|line| compact_single_line(line, 180))
        .unwrap_or_else(|| fallback_title.to_owned());
    let preview = nonempty
        .next()
        .map(|line| compact_single_line(line.trim_start_matches('#').trim(), 220))
        .unwrap_or_default();
    (title, preview)
}

pub(in crate::app) fn conversation_preview_lines(session: &DashboardSession) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("conversation ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                session.id.clone(),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        schema_kv_line("kind", session.kind.clone(), Color::White),
        schema_kv_line("command", session.command.clone(), Color::White),
        schema_kv_line(
            "created",
            format_unix_time(session.created_at),
            Color::White,
        ),
        schema_kv_line("root", session.root.display().to_string(), Color::Gray),
        Line::from(""),
        section_line("summary"),
        Line::from(session.title.clone()),
    ];
    if !session.preview.is_empty() {
        lines.push(Line::from(Span::styled(
            session.preview.clone(),
            Style::default().fg(Color::DarkGray),
        )));
    }
    lines.extend([
        Line::from(""),
        Line::from(vec![
            footer_key("Enter"),
            Span::raw(" detail  "),
            footer_key("o"),
            Span::raw(" open  "),
            footer_key("y/Y"),
            Span::raw(" copy"),
        ]),
    ]);
    lines
}

pub(in crate::app) fn load_session_detail_lines(
    session: Option<&DashboardSession>,
) -> Result<Vec<Line<'static>>> {
    let Some(session) = session else {
        return Ok(vec![Line::from("Conversation no longer exists.")]);
    };
    let mut lines = vec![
        Line::from(vec![
            Span::styled("conversation ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                session.id.clone(),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("kind: {}", session.kind)),
        Line::from(format!("command: {}", session.command)),
        Line::from(format!("created: {}", format_unix_time(session.created_at))),
        Line::from(format!("root: {}", session.root.display())),
        Line::from(format!(
            "transcript: {}",
            session
                .transcript
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!(
            "reply: {}",
            session
                .reply
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(""),
        Line::from(vec![
            footer_key("o"),
            Span::raw(" open  "),
            footer_key("y"),
            Span::raw(" transcript  "),
            footer_key("Y"),
            Span::raw(" reply  "),
            footer_key("Esc"),
            Span::raw(" close"),
        ]),
    ];
    if !session.title.is_empty() {
        lines.extend([
            Line::from(""),
            section_line("summary"),
            Line::from(session.title.clone()),
        ]);
        if !session.preview.is_empty() {
            lines.push(Line::from(Span::styled(
                session.preview.clone(),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }
    if let Some(path) = &session.reply
        && let Some(text) = io::read_optional_text(path)?
        && !text.trim().is_empty()
    {
        lines.extend([Line::from(""), section_line("reply")]);
        lines.extend(
            text.lines()
                .map(|line| attach_markdown_line(line, Color::White)),
        );
    }
    if let Some(path) = &session.transcript
        && let Some(text) = io::read_optional_text(path)?
        && !text.trim().is_empty()
    {
        lines.extend([Line::from(""), section_line("transcript tail")]);
        lines.extend(
            transcript_tail(&text, AGENT_HISTORY_TRANSCRIPT_TAIL_LINES)
                .lines()
                .map(|line| Line::from(line.to_owned())),
        );
    }
    Ok(lines)
}

pub(in crate::app) fn notice_dir(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("notices")
}

pub(in crate::app) fn notice_current_path(project: &ProjectPaths) -> PathBuf {
    notice_dir(project).join("current.md")
}

pub(in crate::app) fn notice_journal_path(project: &ProjectPaths) -> PathBuf {
    notice_dir(project).join("journal.md")
}

pub(in crate::app) fn notice_lock_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("locks").join("notices.lock")
}

pub(in crate::app) fn ensure_notice_task_started(project: &ProjectPaths) -> Result<()> {
    if let Err(err) = start_notice_task(project, false) {
        record_notice_generator_start_failure(project, &err);
    }
    Ok(())
}

pub(in crate::app) fn force_notice_task_started(project: &ProjectPaths) -> Result<()> {
    if let Err(err) = start_notice_task(project, true) {
        record_notice_generator_start_failure(project, &err);
    }
    Ok(())
}

pub(in crate::app) fn start_notice_task(project: &ProjectPaths, force: bool) -> Result<()> {
    if lock::is_active(&notice_lock_path(project))? || (!force && notice_is_fresh(project)?) {
        return Ok(());
    }
    Command::new(think_child_executable()?)
        .arg("run-child")
        .arg("notices")
        .arg("--project")
        .arg(&project.root)
        .current_dir(&project.root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("Failed to start notice generator")?;
    Ok(())
}

pub(in crate::app) fn think_child_executable() -> Result<PathBuf> {
    let current = std::env::current_exe().context("Failed to locate current executable")?;
    if current.exists() {
        return Ok(current);
    }
    which::which("think").context("Failed to locate `think` on PATH")
}

pub(in crate::app) fn record_notice_generator_start_failure(
    project: &ProjectPaths,
    error: &anyhow::Error,
) {
    let line = format!("warn: notice generator failed to start: {error:#}");
    let path = notice_current_path(project);
    let write_result = (|| -> Result<()> {
        io::ensure_dir(&notice_dir(project))?;
        let current = io::read_optional_text(&path)?.unwrap_or_default();
        if current.lines().any(|existing| existing == line) {
            return Ok(());
        }
        let mut next = current
            .lines()
            .filter(|existing| !existing.contains("notice generator failed to start"))
            .collect::<Vec<_>>()
            .join("\n");
        if !next.is_empty() {
            next.push('\n');
        }
        next.push_str(&line);
        next.push('\n');
        io::write_text(&path, &next)
    })();
    let _ = write_result;
}

pub(in crate::app) fn notice_is_fresh(project: &ProjectPaths) -> Result<bool> {
    let Ok(metadata) = fs::metadata(notice_current_path(project)) else {
        return Ok(false);
    };
    let Ok(modified) = metadata.modified() else {
        return Ok(false);
    };
    Ok(SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default()
        < NOTICE_REFRESH_INTERVAL)
}

pub(in crate::app) fn load_notice_lines(
    project: &ProjectPaths,
) -> Result<(Vec<DashboardNotice>, bool, Option<u64>)> {
    let loading = lock::is_active(&notice_lock_path(project))?;
    let path = notice_current_path(project);
    let updated_at = file_modified_unix(&path);
    let lines = io::read_optional_text(&path)?
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter_map(parse_notice_line)
        .take(NOTICE_LINE_LIMIT)
        .collect::<Vec<_>>();
    Ok((lines, loading, updated_at))
}

pub(in crate::app) fn notice_line_is_actionable(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    let lower = strip_notice_prefix(line).to_ascii_lowercase();
    !matches!(
        lower.as_str(),
        "notice scan active"
            | "scan active"
            | "scanning"
            | "checking"
            | "loading"
            | "loading..."
            | "all clear"
            | "queues empty"
            | "idle"
            | "waiting"
            | "no notices"
            | "no operator notices"
    ) && !lower.starts_with("scanning ")
        && !lower.starts_with("checking ")
        && !lower.starts_with("notice scan ")
}

pub(in crate::app) fn strip_notice_prefix(line: &str) -> &str {
    let Some((prefix, text)) = line.split_once(':') else {
        return line.trim();
    };
    match prefix.trim().to_ascii_lowercase().as_str() {
        "error" | "warn" | "warning" | "action" | "info" | "note" => text.trim(),
        _ => line.trim(),
    }
}

pub(in crate::app) fn parse_notice_line(line: &str) -> Option<DashboardNotice> {
    if !notice_line_is_actionable(line) {
        return None;
    }
    let (severity, text) = if let Some((prefix, text)) = line.split_once(':') {
        match prefix.trim().to_ascii_lowercase().as_str() {
            "error" => (NoticeSeverity::Error, text.trim()),
            "warn" | "warning" => (NoticeSeverity::Warn, text.trim()),
            "action" => (NoticeSeverity::Action, text.trim()),
            "info" | "note" => (NoticeSeverity::Info, text.trim()),
            _ => (NoticeSeverity::Action, line),
        }
    } else {
        (NoticeSeverity::Action, line)
    };
    (!text.is_empty()).then(|| DashboardNotice {
        severity,
        text: text.to_owned(),
    })
}

pub(in crate::app) fn retry_waits_now(project: &ProjectPaths) -> Result<()> {
    let updated = retry_waits_now_inner(project)?;
    println!(
        "retry errored requested; updated {updated} active runtime orchestrator{}",
        if updated == 1 { "" } else { "s" }
    );
    Ok(())
}

pub(in crate::app) fn retry_waits_now_inner(project: &ProjectPaths) -> Result<usize> {
    let now = unix_timestamp();
    io::write_toml(
        &retry_request_path(project),
        &RetryRequestState {
            version: RETRY_REQUEST_VERSION,
            requested_at: now,
        },
    )?;

    let mut updated = 0;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent);
            let state = load_agent(&agent_paths)?;
            if state.archived
                || !matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
            {
                continue;
            }
            let mut supervisor = load_supervisor_state(&agent_paths)?;
            if !matches!(
                supervisor.status,
                SupervisorStatus::WaitingForQuota
                    | SupervisorStatus::WaitingForProvider
                    | SupervisorStatus::Restarting
            ) {
                continue;
            }
            let event = format!("retry requested by user at {}", format_unix_time(now));
            supervisor.next_retry_at = Some(now);
            supervisor.last_event = Some(event.clone());
            save_supervisor_state(&agent_paths, &supervisor)?;
            update_agent_note(&agent_paths, &event)?;
            updated += 1;
        }
    }
    Ok(updated)
}
