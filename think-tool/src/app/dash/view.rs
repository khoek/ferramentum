use super::*;

pub(in crate::app) fn pad_display(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        value.to_owned()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

pub(in crate::app) fn state_heading_line(
    label: &'static str,
    slug: &str,
    value: &str,
    value_style: Style,
    width: usize,
) -> Line<'static> {
    let prefix = format!("{label} {slug}: ");
    let value = ellipsize_display(value, width.saturating_sub(prefix.chars().count()));
    Line::from(vec![
        Span::styled(format!("{label} "), Style::default().fg(Color::DarkGray)),
        Span::styled(
            slug.to_owned(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(": ", Style::default().fg(Color::DarkGray)),
        Span::styled(value, value_style),
    ])
}

pub(in crate::app) fn notice_style(severity: NoticeSeverity) -> Style {
    match severity {
        NoticeSeverity::Info => Style::default().fg(Color::Cyan),
        NoticeSeverity::Action => Style::default().fg(Color::Blue),
        NoticeSeverity::Warn => Style::default().fg(Color::Yellow),
        NoticeSeverity::Error => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

pub(in crate::app) fn centered_popup(area: Rect, max_width: u16, max_height: u16) -> Rect {
    let width = area
        .width
        .saturating_sub(DASHBOARD_MODAL_HORIZONTAL_MARGIN)
        .min(max_width);
    let height = area
        .height
        .saturating_sub(DASHBOARD_MODAL_VERTICAL_MARGIN)
        .min(max_height);
    Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    }
}

pub(in crate::app) fn inset_rect(area: Rect, horizontal_margin: u16, vertical_margin: u16) -> Rect {
    let width = area.width.saturating_sub(horizontal_margin);
    let height = area.height.saturating_sub(vertical_margin);
    Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    }
}

pub(in crate::app) fn draw_search_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    target: SearchTarget,
    query: &str,
    visible_rows: usize,
) {
    let popup = Rect {
        x: area.x
            + area
                .width
                .saturating_sub(area.width.min(DASHBOARD_SEARCH_MAX_WIDTH))
                / DASHBOARD_CENTERING_DIVISOR,
        y: area.y
            + area
                .height
                .saturating_sub(DASHBOARD_SEARCH_HEIGHT + DASHBOARD_FOOTER_HEIGHT),
        width: area.width.min(DASHBOARD_SEARCH_MAX_WIDTH),
        height: DASHBOARD_SEARCH_HEIGHT.min(area.height),
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "/",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(query.to_owned()),
            Span::styled(
                format!("  {visible_rows} {}", target.visible_label()),
                Style::default().fg(Color::DarkGray),
            ),
        ]))
        .block(dashboard_block(target.title()).border_style(Style::default().fg(Color::Cyan))),
        popup,
    );
}

pub(in crate::app) fn draw_help_overlay(app: &DashboardApp, frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_popup(area, DASHBOARD_HELP_MAX_WIDTH, DASHBOARD_HELP_MAX_HEIGHT);
    let mut lines = vec![Line::from(Span::styled(
        "dashboard commands",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ))];
    let ctx = app.command_ctx();
    let commands = app
        .commands(CommandSurface::Help)
        .filter_map(|command| command.key(ctx).map(|key| (key, command.label(ctx))))
        .collect::<Vec<_>>();
    for chunk in commands.chunks(3) {
        let mut spans = Vec::new();
        for (key, label) in chunk {
            spans.push(footer_key(key));
            spans.push(Span::raw(" "));
            spans.push(Span::styled(
                label.clone(),
                Style::default().fg(Color::White),
            ));
            spans.push(Span::styled("   ", Style::default().fg(Color::DarkGray)));
        }
        lines.push(Line::from(spans));
    }
    lines.extend([
        Line::from(""),
        Line::from(vec![
            footer_key(":"),
            Span::raw(" opens the command palette."),
        ]),
        Line::from(vec![
            footer_key("x"),
            Span::raw(" toggles the extended run timeline only inside the detail screen."),
        ]),
        Line::from(vec![
            footer_key("a/r/t/n/0"),
            Span::raw(" filter the timeline by lane while it is active."),
        ]),
    ]);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(dashboard_block("Help").border_style(Style::default().fg(Color::Cyan)))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub(in crate::app) fn draw_advanced_menu(frame: &mut Frame<'_>, area: Rect) {
    let width = area.width.min(DASHBOARD_ADVANCED_MAX_WIDTH);
    let height = area.height.min(DASHBOARD_ADVANCED_MAX_HEIGHT);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / DASHBOARD_CENTERING_DIVISOR,
        y: area.y + area.height.saturating_sub(height) / DASHBOARD_CENTERING_DIVISOR,
        width,
        height,
    };
    let block = dashboard_block("Advanced").border_style(Style::default().fg(Color::Magenta));
    let inner = block.inner(popup);
    frame.render_widget(Clear, popup);
    frame.render_widget(block, popup);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from(vec![
                footer_key("n"),
                Span::raw(" "),
                Span::styled("new role", Style::default().fg(Color::White)),
                Span::styled(
                    "  draft a role through the standard review workflow",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("r"),
                Span::raw(" "),
                Span::styled("retry errored", Style::default().fg(Color::White)),
                Span::styled(
                    "  wake quota/rate-limit/OOM retry backoffs",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("o"),
                Span::raw(" "),
                Span::styled("open project", Style::default().fg(Color::White)),
                Span::styled(
                    "  open this project directory",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("p"),
                Span::raw(" "),
                Span::styled("provider settings", Style::default().fg(Color::White)),
                Span::styled(
                    "  Codex accounts, model, and thinking level",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("x"),
                Span::raw(" "),
                Span::styled("archived", Style::default().fg(Color::White)),
                Span::styled(
                    "  toggle archived agents",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                footer_key("Esc"),
                Span::raw(" "),
                Span::styled("close", Style::default().fg(Color::White)),
            ]),
        ])),
        inner,
    );
}

pub(in crate::app) fn provider_settings_action_line() -> Line<'static> {
    Line::from(vec![
        footer_key("Enter"),
        Span::raw(" switch  "),
        footer_key("a"),
        Span::raw(" add  "),
        footer_key("d"),
        Span::raw(" delete  "),
        footer_key("m"),
        Span::raw(" model/thinking  "),
        footer_key("r"),
        Span::raw(" active  "),
        footer_key("Esc"),
        Span::raw(" close"),
    ])
}

pub(in crate::app) fn provider_accounts_heading(count: usize, selected: usize) -> Line<'static> {
    let position = if count == 0 {
        0
    } else {
        selected.saturating_add(1).min(count)
    };
    Line::from(vec![
        Span::styled(
            "accounts",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" {position}/{count}"),
            Style::default().fg(Color::DarkGray),
        ),
    ])
}

pub(in crate::app) fn provider_account_name_width(rows: &[ProviderAccountRow]) -> usize {
    rows.iter()
        .map(|row| row.name.chars().count())
        .max()
        .unwrap_or(7)
        .clamp(7, 24)
}

pub(in crate::app) fn provider_account_line(
    account: &ProviderAccountRow,
    selected: bool,
    width: usize,
    name_width: usize,
) -> Line<'static> {
    let (status, status_style) = provider_account_status(account);
    let prefix_style = if account.active {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let fixed_width = name_width + status.chars().count() + 13;
    let detail = provider_account_detail(account, width.saturating_sub(fixed_width).max(12));
    Line::from(vec![
        dashboard_span(
            if account.active { "● " } else { "• " },
            prefix_style,
            selected,
        ),
        dashboard_span(
            format!("{:<name_width$}", account.name),
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(status, status_style, selected),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(detail, Style::default().fg(Color::Gray), selected),
    ])
}

pub(in crate::app) fn provider_account_status(account: &ProviderAccountRow) -> (String, Style) {
    if let Some(wait_until) = account.quota_wait_until {
        (
            format!("quota until {}", format_unix_time_compact(wait_until)),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else if account.active {
        (
            "active".to_owned(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        ("ready".to_owned(), Style::default().fg(Color::Cyan))
    }
}

pub(in crate::app) fn provider_account_detail(
    account: &ProviderAccountRow,
    width: usize,
) -> String {
    let mut detail = account.codex_home.display().to_string();
    if let Some(last_used_at) = account.last_used_at {
        let _ = write!(detail, " · used {}", format_unix_time_compact(last_used_at));
    }
    if let Some(event) = &account.last_quota_event {
        let _ = write!(detail, " · {}", compact_single_line(event, 80));
    }
    ellipsize_display(&detail, width)
}

pub(in crate::app) fn provider_settings_project_config_line(
    project: &ProjectPaths,
) -> Line<'static> {
    match project_config(project) {
        Ok(config) => {
            let model = config
                .providers
                .codex
                .model
                .unwrap_or_else(|| "Codex default model".to_owned());
            let thinking = config
                .providers
                .codex
                .thinking_level
                .map(|level| level.to_string())
                .unwrap_or_else(|| "Codex default".to_owned());
            Line::from(vec![
                Span::styled("project: ", Style::default().fg(Color::DarkGray)),
                Span::styled(model, Style::default().fg(Color::White)),
                Span::styled(" · thinking ", Style::default().fg(Color::DarkGray)),
                Span::styled(thinking, Style::default().fg(Color::White)),
            ])
        }
        Err(err) => Line::from(vec![
            Span::styled("project: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("unavailable ({err:#})"),
                Style::default().fg(Color::Red),
            ),
        ]),
    }
}

pub(in crate::app) struct DashboardLayoutAreas {
    pub(in crate::app) tabs: Rect,
    pub(in crate::app) health: Rect,
    pub(in crate::app) notices: Rect,
    pub(in crate::app) state: Rect,
    pub(in crate::app) agents: Rect,
    pub(in crate::app) runtime: Rect,
    pub(in crate::app) main: Rect,
    pub(in crate::app) footer: Rect,
}

pub(in crate::app) fn dashboard_layout(area: Rect, narrow: bool) -> DashboardLayoutAreas {
    if narrow {
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(DASHBOARD_TAB_BAR_HEIGHT),
                Constraint::Length(DASHBOARD_HEALTH_STRIP_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_NOTICE_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_STATE_HEIGHT),
                Constraint::Min(DASHBOARD_NARROW_AGENTS_MIN_HEIGHT),
                Constraint::Length(DASHBOARD_NARROW_RUNTIME_HEIGHT),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        DashboardLayoutAreas {
            tabs: root[0],
            health: root[1],
            notices: root[2],
            state: root[3],
            agents: root[4],
            runtime: root[5],
            main: root[2].union(root[3]).union(root[4]).union(root[5]),
            footer: root[6],
        }
    } else {
        let root = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(DASHBOARD_TAB_BAR_HEIGHT),
                Constraint::Length(DASHBOARD_HEALTH_STRIP_HEIGHT),
                Constraint::Length(DASHBOARD_STATE_BAND_HEIGHT),
                Constraint::Min(DASHBOARD_AGENTS_MIN_HEIGHT),
                Constraint::Length(DASHBOARD_RUNTIME_BAND_HEIGHT),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let state_band = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(root[2]);
        DashboardLayoutAreas {
            tabs: root[0],
            health: root[1],
            notices: state_band[0],
            state: state_band[1],
            agents: root[3],
            runtime: root[4],
            main: root[2].union(root[3]).union(root[4]),
            footer: root[5],
        }
    }
}

pub(in crate::app) fn dashboard_block(title: &'static str) -> Block<'static> {
    dashboard_block_with_title(Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )))
}

pub(in crate::app) fn dashboard_block_with_title(title: Line<'static>) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

pub(in crate::app) fn panel_title(
    label: &'static str,
    position: Option<(usize, usize)>,
) -> Line<'static> {
    let mut spans = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )];
    if let Some((current, total)) = position
        && total > 0
    {
        spans.push(Span::styled(
            format!(" {current}/{total}"),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(spans)
}

pub(in crate::app) fn dynamic_panel_title(
    label: String,
    position: Option<(usize, usize)>,
) -> Line<'static> {
    let mut spans = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )];
    if let Some((current, total)) = position
        && total > 0
    {
        spans.push(Span::styled(
            format!(" {current}/{total}"),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(spans)
}

pub(in crate::app) fn visible_panel_rows(area: Rect) -> usize {
    usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
        .max(DASHBOARD_MIN_VISIBLE_ROWS)
}

pub(in crate::app) fn max_scroll_offset(content_len: usize, visible_rows: usize) -> usize {
    content_len.saturating_sub(visible_rows.max(DASHBOARD_MIN_VISIBLE_ROWS))
}

pub(in crate::app) fn clamped_scroll_offset(
    scroll: usize,
    content_len: usize,
    visible_rows: usize,
) -> usize {
    scroll.min(max_scroll_offset(content_len, visible_rows))
}

pub(in crate::app) fn clamped_scroll_offset_u16(
    scroll: u16,
    content_len: usize,
    visible_rows: usize,
) -> u16 {
    clamped_scroll_offset(usize::from(scroll), content_len, visible_rows).min(usize::from(u16::MAX))
        as u16
}

pub(in crate::app) fn render_scrollbar(
    frame: &mut Frame<'_>,
    area: Rect,
    content_len: usize,
    scroll: usize,
) {
    let viewport = visible_panel_rows(area);
    if content_len <= viewport || area.width < 3 || area.height <= DASHBOARD_PANEL_BORDER_ROWS {
        return;
    }
    let mut state = ScrollbarState::new(content_len)
        .position(clamped_scroll_offset(scroll, content_len, viewport))
        .viewport_content_length(viewport);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .thumb_style(Style::default().fg(Color::Cyan)),
        area,
        &mut state,
    );
}

pub(in crate::app) trait ScrollPanelBody {
    fn lines(self, width: usize, visible_rows: usize) -> Vec<Line<'static>>;
}

impl<F> ScrollPanelBody for F
where
    F: FnOnce(usize, usize) -> Vec<Line<'static>>,
{
    fn lines(self, width: usize, visible_rows: usize) -> Vec<Line<'static>> {
        self(width, visible_rows)
    }
}

pub(in crate::app) fn render_scroll_panel<B: ScrollPanelBody>(
    frame: &mut Frame<'_>,
    area: Rect,
    title: Line<'static>,
    border_color: Color,
    scroll: &mut u16,
    body: B,
) {
    let block = dashboard_block_with_title(title).border_style(Style::default().fg(border_color));
    let inner = block.inner(area);
    let visible = visible_panel_rows(area);
    let lines = body.lines(usize::from(inner.width), visible);
    let line_count = lines.len();
    *scroll = clamped_scroll_offset_u16(*scroll, line_count, visible);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((*scroll, 0))
            .wrap(Wrap { trim: false }),
        inner,
    );
    render_scrollbar(frame, area, line_count, usize::from(*scroll));
}

pub(in crate::app) trait DetailPanel {
    fn title(&self) -> &'static str;
    fn border_color(&self) -> Color;
    fn lines(self) -> Vec<Line<'static>>;
}

pub(in crate::app) struct StaticDetailPanel {
    title: &'static str,
    border_color: Color,
    lines: Vec<Line<'static>>,
}

impl DetailPanel for StaticDetailPanel {
    fn title(&self) -> &'static str {
        self.title
    }

    fn border_color(&self) -> Color {
        self.border_color
    }

    fn lines(self) -> Vec<Line<'static>> {
        self.lines
    }
}

pub(in crate::app) fn detail_panel(
    title: &'static str,
    border_color: Color,
    lines: Vec<Line<'static>>,
) -> StaticDetailPanel {
    StaticDetailPanel {
        title,
        border_color,
        lines,
    }
}

pub(in crate::app) fn render_detail_panel<P: DetailPanel>(
    frame: &mut Frame<'_>,
    area: Rect,
    scroll: u16,
    panel: P,
) {
    let popup = inset_rect(
        area,
        DASHBOARD_MODAL_HORIZONTAL_MARGIN,
        DASHBOARD_MODAL_VERTICAL_MARGIN,
    );
    let title_label = panel.title();
    let border_color = panel.border_color();
    let lines = panel.lines();
    let line_count = lines.len();
    let visible = visible_panel_rows(popup);
    let scroll = clamped_scroll_offset_u16(scroll, line_count, visible);
    let title = panel_title(
        title_label,
        (line_count > 0).then_some(((usize::from(scroll) + 1).min(line_count), line_count)),
    );
    let block = dashboard_block_with_title(title).border_style(Style::default().fg(border_color));
    let inner = block.inner(popup);
    frame.render_widget(Clear, popup);
    frame.render_widget(block, popup);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false }),
        inner,
    );
    render_scrollbar(frame, popup, line_count, usize::from(scroll));
}

pub(in crate::app) fn tab_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray).bg(Color::Black)
    }
}

pub(in crate::app) fn push_health_metric(
    spans: &mut Vec<Span<'static>>,
    label: &'static str,
    value: usize,
    color: Color,
) {
    spans.extend([
        Span::styled("  ", Style::default().bg(Color::Black)),
        Span::styled(label, Style::default().fg(Color::DarkGray).bg(Color::Black)),
        Span::styled(" ", Style::default().fg(Color::DarkGray).bg(Color::Black)),
        Span::styled(
            value.to_string(),
            Style::default()
                .fg(if value == 0 { Color::DarkGray } else { color })
                .bg(Color::Black)
                .add_modifier(if value == 0 {
                    Modifier::empty()
                } else {
                    Modifier::BOLD
                }),
        ),
    ]);
}

pub(in crate::app) fn queue_count_style(value: usize) -> Style {
    match value {
        0 => Style::default().fg(Color::DarkGray),
        1..=QUEUE_WARNING_MAX_COUNT => Style::default().fg(Color::Yellow),
        _ => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}

pub(in crate::app) fn channel_lines(channel: &StatusChannelRow) -> Vec<Line<'static>> {
    let spans = vec![
        Span::styled("● ", Style::default().fg(Color::Green)),
        Span::styled(
            channel.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(ui::FIELD_SEPARATOR, Style::default().fg(Color::DarkGray)),
        Span::styled(
            channel.artifacts.to_string(),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(" artifacts", Style::default().fg(Color::DarkGray)),
    ];
    let mut lines = vec![Line::from(spans)];
    if let Some(latest) = channel.latest.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default().fg(Color::DarkGray)),
            Span::styled("latest ", Style::default().fg(Color::DarkGray)),
            Span::styled(latest.to_owned(), Style::default().fg(Color::White)),
        ]));
    }
    lines
}

pub(in crate::app) fn channel_tab_line(
    channel: &StatusChannelRow,
    selected: bool,
    width: usize,
    unseen_alert: bool,
) -> Line<'static> {
    let marker_style = if unseen_alert {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Green)
    };
    let marker = if unseen_alert { " ! " } else { " ● " };
    let latest = channel.latest.as_deref().unwrap_or("-");
    let fixed_width = 1
        + marker.chars().count()
        + channel.name.chars().count()
        + ui::FIELD_SEPARATOR.chars().count()
        + channel.artifacts.to_string().chars().count()
        + " artifacts".chars().count()
        + ui::FIELD_SEPARATOR.chars().count()
        + "latest ".chars().count();
    Line::from(vec![
        dashboard_span(
            if selected { "▸" } else { " " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(marker, marker_style, selected),
        dashboard_span(
            channel.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            channel.artifacts.to_string(),
            Style::default().fg(Color::Cyan),
            selected,
        ),
        dashboard_span(" artifacts", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span("latest ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            ellipsize_display(latest, width.saturating_sub(fixed_width).max(1)),
            Style::default().fg(Color::White),
            selected,
        ),
    ])
}

pub(in crate::app) fn queue_header_line(
    queue: &StatusQueueRow,
    selected: bool,
    collapsed: bool,
) -> Line<'static> {
    let mut spans = vec![
        dashboard_span(
            if selected { "▸ " } else { "  " },
            Style::default().fg(Color::White),
            selected,
        ),
        dashboard_span(
            queue.kind.label(),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(" ", Style::default(), selected),
        dashboard_span(
            queue.name.clone(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            selected,
        ),
        dashboard_span(
            ui::FIELD_SEPARATOR,
            Style::default().fg(Color::DarkGray),
            selected,
        ),
        dashboard_span(
            queue.count.to_string(),
            queue_count_style(queue.count),
            selected,
        ),
        dashboard_span(" pending", Style::default().fg(Color::DarkGray), selected),
    ];
    if queue.locked {
        spans.extend([
            dashboard_span(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray),
                selected,
            ),
            dashboard_span(
                queue
                    .active
                    .as_ref()
                    .map(|active| format!("merging {}", active.label))
                    .unwrap_or_else(|| "locked".to_owned()),
                Style::default().fg(Color::Yellow),
                selected,
            ),
        ]);
    }
    if collapsed {
        spans.extend([
            dashboard_span(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray),
                selected,
            ),
            dashboard_span("collapsed", Style::default().fg(Color::DarkGray), selected),
        ]);
    }
    Line::from(spans)
}

pub(in crate::app) fn active_queue_item_line(
    queues: &[StatusQueueRow],
    width: usize,
) -> Option<Line<'static>> {
    queues.iter().find(|queue| queue.locked).and_then(|queue| {
        let active = queue.active.as_ref()?;
        let mut label = format!("currently running queued trigger {}", active.label);
        if let Some(locked_at) = queue.locked_at {
            label.push_str(&format!(" · {} elapsed", event_age(locked_at)));
        }
        if queue.count > 0 {
            label.push_str(&format!(" · {} pending", queue.count));
        }
        Some(Line::from(vec![
            Span::styled(
                "● ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                ellipsize_display(&label, width.saturating_sub(2)),
                Style::default().fg(Color::Yellow),
            ),
        ]))
    })
}

pub(in crate::app) fn queue_item_lines(
    project: &ProjectPaths,
    queue: &StatusQueueRow,
    width: usize,
    selected_item: Option<usize>,
) -> Vec<Line<'static>> {
    match queue.kind {
        StatusQueueKind::Trigger => queue_trigger_item_lines(project, queue, width, selected_item),
    }
}

pub(in crate::app) fn queue_trigger_item_lines(
    project: &ProjectPaths,
    queue: &StatusQueueRow,
    width: usize,
    selected_item: Option<usize>,
) -> Vec<Line<'static>> {
    let state = match load_trigger_queue(project, &queue.name) {
        Ok(state) => state,
        Err(err) => return queue_error_lines(err),
    };
    if state.items.is_empty() {
        return vec![queue_empty_line(
            "no queued trigger items",
            selected_item.is_none(),
        )];
    }
    state
        .items
        .iter()
        .enumerate()
        .flat_map(|(index, item)| {
            let selected = selected_item == Some(index);
            [
                queue_child_line(
                    &ellipsize_display(
                        &format!(
                            "{}. {} · enqueued {}",
                            index + 1,
                            item.role,
                            format_unix_time(item.enqueued_at)
                        ),
                        width.saturating_sub(3),
                    ),
                    selected,
                ),
                queue_child_meta_line(
                    &ellipsize_display(
                        &trigger_cause_summary(&item.cause),
                        width.saturating_sub(5),
                    ),
                    selected,
                ),
            ]
        })
        .collect()
}

pub(in crate::app) fn queue_child_line(value: &str, selected: bool) -> Line<'static> {
    Line::from(vec![
        dashboard_span("   • ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            value.to_owned(),
            Style::default().fg(Color::White),
            selected,
        ),
    ])
}

pub(in crate::app) fn queue_child_meta_line(value: &str, selected: bool) -> Line<'static> {
    Line::from(vec![
        dashboard_span("     ", Style::default().fg(Color::DarkGray), selected),
        dashboard_span(
            value.to_owned(),
            Style::default().fg(Color::DarkGray),
            selected,
        ),
    ])
}

pub(in crate::app) fn queue_empty_line(value: &'static str, _selected: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled("   ", Style::default().fg(Color::DarkGray)),
        Span::styled(value, Style::default().fg(Color::DarkGray)),
    ])
}

pub(in crate::app) fn queue_error_lines(error: anyhow::Error) -> Vec<Line<'static>> {
    vec![Line::from(Span::styled(
        format!("   failed to load queue: {error:#}"),
        Style::default().fg(Color::Red),
    ))]
}

pub(in crate::app) fn load_queue_detail_lines(
    project: &ProjectPaths,
    queues: &[StatusQueueRow],
    selection: QueueSelection,
) -> Result<Vec<Line<'static>>> {
    let Some(queue) = selection.queue_index().and_then(|index| queues.get(index)) else {
        return Ok(vec![Line::from("No queue selected.")]);
    };
    let mut lines = vec![
        section_line("queue"),
        Line::from(format!("name: {}", queue.name)),
        Line::from(format!("kind: {}", queue.kind.label())),
        Line::from(format!("items: {}", queue.count)),
        Line::from(format!("locked: {}", queue.locked)),
    ];
    if let Some(active) = &queue.active {
        lines.push(Line::from(format!("active: {}", active.label)));
    }
    if let Some(locked_at) = queue.locked_at {
        lines.push(Line::from(format!(
            "locked at: {} · {} elapsed",
            format_unix_time(locked_at),
            event_age(locked_at)
        )));
    }
    lines.push(Line::from(""));
    match selection {
        QueueSelection::Header(_) => {
            lines.push(section_line("overview"));
            lines.extend(queue_item_lines(project, queue, usize::MAX, None));
        }
        QueueSelection::Item { item_index, .. } => {
            let state = load_trigger_queue(project, &queue.name)?;
            let Some(item) = state.items.get(item_index) else {
                lines.push(Line::from("queue item is no longer present"));
                return Ok(lines);
            };
            lines.push(section_line("trigger item"));
            lines.push(Line::from(format!("position: {}", item_index + 1)));
            lines.push(Line::from(format!("role: {}", item.role)));
            lines.push(Line::from(format!(
                "enqueued: {}",
                format_unix_time(item.enqueued_at)
            )));
            lines.push(Line::from(format!(
                "cause: {}",
                trigger_cause_summary(&item.cause)
            )));
        }
    }
    Ok(lines)
}

pub(in crate::app) fn load_channel_detail_lines(
    project: &ProjectPaths,
    channel: &StatusChannelRow,
) -> Result<Vec<Line<'static>>> {
    let path = project.channel_dir(&ChannelSlug::parse(&channel.name)?);
    let entries = channel_artifact_entries(&path)?;
    let mut lines = vec![
        section_line("channel"),
        Line::from(format!("name: {}", channel.name)),
        Line::from(format!("path: {}", path.display())),
        Line::from(format!("artifacts: {}", entries.len())),
    ];
    if let Some(latest) = channel.latest.as_deref() {
        lines.push(Line::from(format!("latest: {latest}")));
    }
    lines.push(Line::from(""));
    lines.push(section_line("recent artifacts"));
    if entries.is_empty() {
        lines.push(Line::from(Span::styled(
            "no published artifacts",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(
            entries
                .into_iter()
                .take(CHANNEL_DETAIL_ARTIFACT_LIMIT)
                .map(|entry| {
                    Line::from(vec![
                        Span::styled(
                            if entry.is_dir { "dir " } else { "file" },
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(entry.name, Style::default().fg(Color::White)),
                        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(event_age(entry.modified), Style::default().fg(Color::Cyan)),
                    ])
                }),
        );
    }
    Ok(lines)
}

impl QueueSelection {
    fn queue_index(self) -> Option<usize> {
        match self {
            Self::Header(index) => Some(index),
            Self::Item { queue_index, .. } => Some(queue_index),
        }
    }
}

pub(in crate::app) fn load_notice_detail_lines(
    project: &ProjectPaths,
    snapshot: &DashboardSnapshot,
) -> Result<Vec<Line<'static>>> {
    let mut lines = vec![
        section_line("current notices"),
        Line::from(format!(
            "current file: {}",
            notice_current_path(project).display()
        )),
        Line::from(format!(
            "journal: {}",
            notice_journal_path(project).display()
        )),
    ];
    if let Some(updated_at) = snapshot.notices_updated_at {
        lines.push(Line::from(format!(
            "updated: {}",
            format_unix_time(updated_at)
        )));
    }
    lines.push(Line::from(format!("loading: {}", snapshot.notices_loading)));
    lines.push(Line::from(""));
    if snapshot.notices.is_empty() {
        lines.push(Line::from(Span::styled(
            "no current operator notices",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(snapshot.notices.iter().map(|notice| {
            Line::from(vec![
                Span::styled("● ", notice_style(notice.severity)),
                Span::styled(
                    notice_severity_label(notice.severity),
                    notice_style(notice.severity),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(notice.text.clone(), Style::default().fg(Color::White)),
            ])
        }));
    }
    lines.push(Line::from(""));
    lines.push(section_line("journal tail"));
    let journal = io::read_optional_text(&notice_journal_path(project))?.unwrap_or_default();
    let tail = journal
        .lines()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    if tail.is_empty() {
        lines.push(Line::from(Span::styled(
            "no journal entries",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(tail.into_iter().map(|line| {
            Line::from(Span::styled(
                line.to_owned(),
                Style::default().fg(Color::Gray),
            ))
        }));
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest scan transcripts"));
    let transcripts = latest_notice_transcripts(project)?;
    if transcripts.is_empty() {
        lines.push(Line::from(Span::styled(
            "no notice generator transcripts",
            Style::default().fg(Color::DarkGray),
        )));
    }
    for transcript in transcripts {
        let modified = file_modified_unix(&transcript)
            .map(format_unix_time)
            .unwrap_or_else(|| "unknown time".to_owned());
        lines.push(Line::from(vec![
            Span::styled("transcript ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                transcript.display().to_string(),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(" · ", Style::default().fg(Color::DarkGray)),
            Span::styled(modified, Style::default().fg(Color::Gray)),
        ]));
        let text = io::read_optional_text(&transcript)?.unwrap_or_default();
        for line in text
            .lines()
            .rev()
            .take(6)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            lines.push(Line::from(Span::styled(
                compact_single_line(line, 180),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }
    Ok(lines)
}

pub(in crate::app) fn latest_notice_transcripts(project: &ProjectPaths) -> Result<Vec<PathBuf>> {
    let dir = notice_dir(project);
    let Ok(entries) = fs::read_dir(&dir) else {
        return Ok(Vec::new());
    };
    let mut transcripts = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("Failed to read `{}`", dir.display()))?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("app-server-") {
            let transcript = path.join("TRANSCRIPT.txt");
            if transcript.exists() {
                transcripts.push(transcript);
            }
        }
    }
    transcripts.sort_by_key(|path| std::cmp::Reverse(file_modified_unix(path).unwrap_or_default()));
    transcripts.truncate(2);
    Ok(transcripts)
}

pub(in crate::app) fn quota_gauge(label: &'static str, limit: &CodexRateLimit) -> Gauge<'static> {
    Gauge::default()
        .gauge_style(
            Style::default()
                .fg(quota_color(limit.used_percent))
                .bg(Color::Black),
        )
        .ratio((limit.used_percent / PERCENT_FULL).clamp(RATIO_EMPTY, RATIO_FULL))
        .label(format!(
            "{label} {:>4.1}% reset {} ({})",
            limit.used_percent,
            human_duration(limit.resets_in_seconds),
            format_unix_time(limit.resets_at)
        ))
}

pub(in crate::app) fn dashboard_role_style(status: RoleStatus) -> Style {
    match status {
        RoleStatus::Draft => Style::default().fg(Color::DarkGray),
        RoleStatus::Active => Style::default().fg(Color::Green),
        RoleStatus::Paused => Style::default().fg(Color::Yellow),
    }
}

pub(in crate::app) fn dashboard_agent_style(status: AgentStatus, quota_waiting: bool) -> Style {
    match status {
        AgentStatus::Starting => Style::default().fg(Color::Cyan),
        AgentStatus::Running if quota_waiting => Style::default().fg(Color::Yellow),
        AgentStatus::Running => Style::default().fg(Color::Green),
        AgentStatus::Paused => Style::default().fg(Color::Yellow),
        AgentStatus::Done => Style::default().fg(Color::Blue),
        AgentStatus::Stopped => Style::default().fg(Color::DarkGray),
        AgentStatus::NeedsAttention => Style::default().fg(Color::Red),
    }
}

pub(in crate::app) fn supervisor_status_style(status: SupervisorStatus) -> Style {
    match status {
        SupervisorStatus::Idle => Style::default().fg(Color::DarkGray),
        SupervisorStatus::Running => Style::default().fg(Color::Green),
        SupervisorStatus::Restarting => Style::default().fg(Color::Yellow),
        SupervisorStatus::WaitingForQuota => Style::default().fg(Color::Yellow),
        SupervisorStatus::WaitingForProvider => Style::default().fg(Color::Yellow),
        SupervisorStatus::NeedsAttention => Style::default().fg(Color::Red),
    }
}

pub(in crate::app) fn quota_color(used_percent: f64) -> Color {
    if used_percent >= 90.0 {
        Color::Red
    } else if used_percent >= 70.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}

pub(in crate::app) fn dashboard_span(
    text: impl Into<String>,
    style: Style,
    selected: bool,
) -> Span<'static> {
    let style = if selected {
        style
            .add_modifier(Modifier::REVERSED)
            .add_modifier(Modifier::BOLD)
    } else {
        style
    };
    Span::styled(text.into(), style)
}

pub(in crate::app) fn empty_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

pub(in crate::app) fn load_agent_detail_lines(
    project: &ProjectPaths,
    agent: &DashboardAgent,
    extended: bool,
    more_label: Option<&'static str>,
) -> Result<Vec<Line<'static>>> {
    let role_paths = RolePaths::new(project.clone(), agent.role.clone());
    let agent_paths = role_paths.agent(agent.agent.clone());
    let state = load_agent(&agent_paths)?;
    let (manifest, manifest_error) = load_agent_manifest_for_display(&agent_paths);
    let supervisor = load_supervisor_state(&agent_paths)?;
    let mut lines = vec![
        Line::from(vec![
            Span::styled("agent ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}/{}", agent.role, agent.agent),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("status: {}", state.status)),
        Line::from(format!("summary: {}", agent.summary)),
        Line::from(format!("detail: {}", empty_dash(&agent.detail))),
        Line::from(format!("runs: {}", state.run_count)),
        Line::from(format!(
            "session: {}",
            state.pane_id.as_deref().unwrap_or("-")
        )),
        Line::from(format!(
            "data: {}",
            project.agent_data_root(&agent.role, &agent.agent).display()
        )),
        Line::from(format!("paused by user: {}", state.paused_by_user)),
        Line::from(format!("runtime: {}", supervisor.status)),
        Line::from(format!(
            "runtime updated: {}",
            event_age(supervisor.updated_at)
        )),
        Line::from(format!(
            "latest output: {}",
            latest_agent_output_at(&agent_paths, &state, &supervisor)
                .map(event_age)
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!("created: {}", format_unix_time(state.created_at))),
        Line::from(format!("updated: {}", format_unix_time(state.updated_at))),
    ];
    if let Some(line) = steer_status_line(
        &crate::backend::steer_status(&agent_paths.steer_dir())?,
        matches!(state.status, AgentStatus::Starting | AgentStatus::Running),
    ) {
        lines.push(line);
    }
    if let Some(pid) = supervisor.child_pid {
        lines.push(Line::from(format!("child pid: {pid}")));
    }
    if let Some(retry_at) = supervisor.next_retry_at {
        lines.push(Line::from(format!(
            "next retry: {} ({})",
            format_unix_time(retry_at),
            event_age(retry_at)
        )));
    }
    if let Some(summary) = manifest.role_summary.as_deref() {
        lines.push(Line::from(format!("manifest summary: {}", summary.trim())));
    }
    if let Some(error) = manifest_error.as_deref() {
        lines.push(Line::from(Span::styled(
            format!("manifest error: {error}"),
            Style::default().fg(Color::Red),
        )));
    }
    if let Some(disposition) = manifest
        .disposition
        .or_else(|| state.last_exit.as_ref().and_then(|exit| exit.disposition))
    {
        lines.push(Line::from(format!("disposition: {disposition}")));
    }
    if let Some(note) = state.note.as_deref() {
        lines.push(Line::from(format!("note: {note}")));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "channels",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    )));
    if state.channels.is_empty() {
        lines.push(Line::from(Span::styled(
            "-",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for channel in &state.channels {
            lines.push(Line::from(format!(
                "{} · outbox {}",
                channel,
                agent_paths.channel_dir(channel).display()
            )));
        }
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest run"));
    if let Some(exit) = state.last_exit.as_ref() {
        lines.push(Line::from(format!(
            "{} · {} · {} to {}{}",
            exit.step,
            if exit.success { "success" } else { "failed" },
            format_unix_time(exit.started_at),
            format_unix_time(exit.finished_at),
            exit.disposition
                .map(|disposition| format!(" · disposition: {disposition}"))
                .unwrap_or_default()
        )));
        if let Some(message) = exit.message.as_deref()
            && !message.trim().is_empty()
        {
            lines.push(Line::from(Span::styled(
                message.trim().to_owned(),
                Style::default().fg(Color::DarkGray),
            )));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "no completed run yet",
            Style::default().fg(Color::DarkGray),
        )));
    }
    lines.push(Line::from(""));
    lines.push(section_line("latest reply"));
    match latest_agent_reply(&agent_paths, state.run_count)? {
        Some(reply) => {
            let line_count = reply.lines().count();
            let preview = reply
                .lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| compact_single_line(line, 140))
                .unwrap_or_else(|| "recorded".to_owned());
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{line_count} lines"),
                    Style::default().fg(Color::Green),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(preview, Style::default().fg(Color::Gray)),
            ]));
        }
        None => lines.push(Line::from(Span::styled(
            "No REPLY.md has been recorded yet.",
            Style::default().fg(Color::DarkGray),
        ))),
    }
    lines.push(Line::from(""));
    lines.push(section_line("recent events"));
    let recent_events = load_project_events(project)?
        .into_iter()
        .filter(|event| {
            matches!(
                &event.target,
                EventTarget::Agent { role, agent: event_agent }
                    if *role == agent.role && *event_agent == agent.agent
            )
        })
        .take(8)
        .collect::<Vec<_>>();
    if recent_events.is_empty() {
        lines.push(Line::from(Span::styled(
            "none",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        lines.extend(recent_events.iter().map(|event| event_line(event, false)));
    }
    lines.push(Line::from(""));
    let mut action_spans = vec![footer_key("a"), Span::raw(" attach  ")];
    if let Some(label) = more_label {
        action_spans.extend([footer_key("m"), Span::raw(format!(" {label}  "))]);
    }
    action_spans.extend([
        footer_key("x"),
        Span::raw(if extended {
            " hide timeline  "
        } else {
            " show timeline  "
        }),
        footer_key("Esc"),
        Span::raw(" close"),
    ]);
    lines.push(Line::from(action_spans));
    if extended {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "run timeline",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )));
        lines.extend(load_run_timeline_lines(&agent_paths, &state)?);
    }
    Ok(lines)
}

pub(in crate::app) fn section_line(title: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ))
}

pub(in crate::app) fn schema_kv_line(
    label: impl Into<String>,
    value: impl Into<String>,
    value_color: Color,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{:<14}", label.into()),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(value.into(), Style::default().fg(value_color)),
    ])
}

pub(in crate::app) fn schema_status_line(label: &'static str, exists: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<14}"), Style::default().fg(Color::DarkGray)),
        Span::styled(
            if exists { "present" } else { "missing" },
            if exists {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Yellow)
            },
        ),
    ])
}

pub(in crate::app) fn schema_join(values: impl IntoIterator<Item = String>) -> String {
    let values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(", ")
    }
}

pub(in crate::app) fn schema_trigger_summary(trigger: &TriggerConfig) -> String {
    match trigger {
        TriggerConfig::RoleStepFinished { role, step, launch } => {
            format!(
                "when {role}/{step} step finishes: {}",
                schema_trigger_launch_summary(launch)
            )
        }
        TriggerConfig::RoleAgentFinished { role, launch } => {
            format!(
                "when {role} agent finishes: {}",
                schema_trigger_launch_summary(launch)
            )
        }
        TriggerConfig::QueueIdle {
            idle_queue,
            idle_seconds,
            launch,
        } => {
            format!(
                "when queue {idle_queue} is idle for {idle_seconds}s: {}",
                schema_trigger_launch_summary(launch)
            )
        }
        TriggerConfig::Elapsed {
            role,
            interval_seconds,
            launch,
        } => {
            format!(
                "every {interval_seconds}s while {role} is active: {}",
                schema_trigger_launch_summary(launch)
            )
        }
    }
}

pub(in crate::app) fn schema_trigger_launch_summary(launch: &TriggerLaunch) -> String {
    match launch {
        TriggerLaunch::Async => "launch async".to_owned(),
        TriggerLaunch::Queued { queue } => format!("enqueue on {queue}"),
    }
}

pub(in crate::app) fn latest_agent_reply(
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<Option<String>> {
    for run_id in (1..=run_count).rev() {
        let Some(reply) = io::read_optional_text(&agent_paths.run(run_id).reply())? else {
            continue;
        };
        let reply = reply.trim();
        if !reply.is_empty() {
            return Ok(Some(reply.to_owned()));
        }
    }
    Ok(None)
}

pub(in crate::app) fn load_run_timeline_lines(
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
) -> Result<Vec<Line<'static>>> {
    let mut lines = Vec::new();
    for run_id in 1..=state.run_count {
        let run_paths = agent_paths.run(run_id);
        let exit = read_run_exit(&run_paths, state)?;
        let reply_state = if run_paths.reply().exists() {
            "reply"
        } else {
            "no reply"
        };
        if let Some(exit) = exit {
            lines.push(Line::from(format!(
                "#{run_id} · {} · {} · {} to {} · {}{}",
                exit.step,
                if exit.success { "success" } else { "failed" },
                format_unix_time(exit.started_at),
                format_unix_time(exit.finished_at),
                reply_state,
                exit.disposition
                    .map(|disposition| format!(" · disposition: {disposition}"))
                    .unwrap_or_default()
            )));
            if let Some(message) = exit.message {
                lines.push(Line::from(Span::styled(
                    format!("  {message}"),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        } else {
            lines.push(Line::from(format!(
                "#{run_id} · exit state missing · {reply_state}"
            )));
        }
    }
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
        && agent_paths.run(state.run_count + 1).root().exists()
    {
        lines.push(Line::from(format!(
            "#{} · active or interrupted run",
            state.run_count + 1
        )));
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "No runs have been recorded yet.",
            Style::default().fg(Color::DarkGray),
        )));
    }
    Ok(lines)
}

#[derive(Deserialize)]
pub(in crate::app) struct LegacyPtyRunExit {
    success: bool,
    code: u32,
    signal: Option<String>,
}

pub(in crate::app) fn read_run_exit(
    run_paths: &crate::state::RunPaths,
    state: &AgentState,
) -> Result<Option<RunExitState>> {
    let path = run_paths.exit();
    if !path.exists() {
        return Ok(None);
    }
    let text = io::read_text(&path)?;
    match toml::from_str::<RunExitState>(&text) {
        Ok(exit) => Ok(Some(exit)),
        Err(full_error) => match toml::from_str::<LegacyPtyRunExit>(&text) {
            Ok(exit) => Ok(Some(recover_legacy_run_exit(run_paths, state, exit))),
            Err(_) => Err(anyhow!(full_error))
                .with_context(|| format!("Failed to parse `{}`", path.display())),
        },
    }
}

pub(in crate::app) fn recover_legacy_run_exit(
    run_paths: &crate::state::RunPaths,
    state: &AgentState,
    exit: LegacyPtyRunExit,
) -> RunExitState {
    if let Some(last_exit) = state
        .last_exit
        .as_ref()
        .filter(|last_exit| last_exit.run_id == run_paths.run_id)
    {
        return last_exit.clone();
    }
    let finished_at = file_modified_unix(&run_paths.exit()).unwrap_or_else(unix_timestamp);
    RunExitState {
        run_id: run_paths.run_id,
        step: StepSlug::parse("unknown").expect("static fallback step slug is valid"),
        started_at: file_modified_unix(&run_paths.prompt())
            .or_else(|| file_modified_unix(&run_paths.step()))
            .unwrap_or(finished_at),
        finished_at,
        success: exit.success,
        code: exit.code,
        signal: exit.signal,
        disposition: None,
        message: Some("recovered from a raw PTY exit file".to_owned()),
    }
}

pub(in crate::app) fn trigger_cause_summary(cause: &TriggerCause) -> String {
    match cause {
        TriggerCause::Manual { reason } => format!(
            "manual{}",
            reason
                .as_deref()
                .map(|reason| format!(" · {reason}"))
                .unwrap_or_default()
        ),
        TriggerCause::RoleStepFinished {
            source_role,
            source_step,
        } => format!("role step finished · {source_role}/{source_step}"),
        TriggerCause::RoleAgentFinished {
            source_role,
            source_agent,
            run_id,
            step,
        } => format!("agent finished · {source_role}/{source_agent} run {run_id} step {step}"),
        TriggerCause::QueueIdle {
            queue,
            idle_seconds,
            ..
        } => format!("queue idle · {queue} · {}", human_duration(*idle_seconds)),
        TriggerCause::Elapsed {
            source_role,
            interval_seconds,
            event_index,
            ..
        } => format!(
            "elapsed · {source_role} · {} · event {event_index}",
            human_duration(*interval_seconds)
        ),
    }
}
