//! Session-picker conventions used by `kai r`: compact chrome, blue selection,
//! type-to-search, arrow navigation, Ctrl-O density, and a ruled shortcut footer.
use super::attention::{Attention, Target, now_ms};
use super::observer::{DISCOVERY_TIMEOUT, Observer, Row, Snapshot, age, elapsed, now, safe_text};
use super::process::ProcessIdentity;
use super::transcript::TurnState;
use super::worker::Worker;
use anyhow::Result;
use crossterm::{
    cursor,
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    execute,
    style::{Attribute, ResetColor, SetAttribute},
    terminal,
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{List, ListItem, ListState, Paragraph},
};
use std::{
    io,
    time::{Duration, Instant},
};
use unicode_width::UnicodeWidthStr;

pub(super) fn watch(interval: Duration) -> Result<()> {
    let mut observer = Observer::default();
    let mut observation = Worker::new(move |(), cancel| observer.snapshot(now(), cancel));
    let mut focusing = Worker::new(|identity, cancel: &super::worker::Cancellation| {
        super::focus::focus(identity, cancel)?;
        Ok(now_ms())
    });
    let mut interactions = Worker::new(super::focus::focused);
    // Restore the terminal before bounded worker/helper cleanup during shutdown.
    let _screen = Screen::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut snapshot = Snapshot {
        version: 1,
        observed_at: now(),
        windows: Vec::new(),
        warnings: Vec::new(),
    };
    let mut view = View::new(Palette::from_env());
    view.loading = true;
    view.reconcile(&snapshot);
    let mut next_refresh = Instant::now();
    let mut next_interaction_check = Instant::now();
    let mut dirty = true;
    loop {
        if let Some(result) = observation.poll() {
            view.loading = false;
            match result {
                Ok(update) => {
                    snapshot = update;
                    view.attention.observe(&snapshot.windows, now_ms());
                    view.refresh_error = None;
                    view.reconcile(&snapshot);
                }
                Err(error) => view.refresh_error = Some(format!("Refresh failed: {error:#}")),
            }
            next_refresh = Instant::now() + interval;
            dirty = true;
        }
        if !observation.busy() && Instant::now() >= next_refresh {
            observation.start((), DISCOVERY_TIMEOUT)?;
        }
        if let Some(result) = focusing.poll() {
            view.finish_focus(result);
            dirty = true;
        }
        if let Some(result) = interactions.poll() {
            let delay = if result.is_ok() { 500 } else { 5000 };
            match result {
                Ok(update) => {
                    view.attention.focused(update);
                    view.interaction_error = None;
                }
                Err(error) => view.interaction_error = Some(format!("{error:#}")),
            }
            next_interaction_check = Instant::now() + Duration::from_millis(delay);
            dirty = true;
        }
        if !interactions.busy()
            && !snapshot.windows.is_empty()
            && Instant::now() >= next_interaction_check
        {
            interactions.start(
                snapshot.windows.iter().map(Target::from).collect(),
                Duration::from_secs(3),
            )?;
        }
        if dirty {
            terminal.draw(|frame| view.draw(frame, &snapshot, interval))?;
            dirty = false;
        }
        let tick = Duration::from_millis(50);
        let wait = if observation.busy() {
            tick
        } else {
            tick.min(next_refresh.saturating_duration_since(Instant::now()))
        };
        if event::poll(wait)? {
            match event::read()? {
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    if key.code == KeyCode::Esc && focusing.busy() {
                        focusing.cancel();
                        view.focusing = None;
                        view.notice = Some("Window switching cancelled.".into());
                    } else if key.code == KeyCode::Enter && key.modifiers.is_empty() {
                        if !focusing.busy() {
                            let mut target = None;
                            view.focus_selected(&snapshot, |identity| {
                                focusing.start(identity, Duration::from_secs(15))?;
                                target = Some(identity);
                                Ok(())
                            });
                            view.focusing = target;
                        }
                    } else if view.key(key, &snapshot) {
                        break;
                    }
                    dirty = true;
                }
                Event::Paste(text) => {
                    view.add_query(&text);
                    view.reconcile(&snapshot);
                    dirty = true;
                }
                Event::Resize(..) => dirty = true,
                _ => (),
            }
        }
    }
    Ok(())
}

struct Screen;
impl Screen {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let screen = Self;
        execute!(
            io::stdout(),
            terminal::EnterAlternateScreen,
            EnableBracketedPaste,
            cursor::Hide
        )?;
        Ok(screen)
    }
}
impl Drop for Screen {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            DisableBracketedPaste,
            SetAttribute(Attribute::Reset),
            ResetColor,
            cursor::Show,
            terminal::LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Filter {
    #[default]
    All,
    Active,
}
impl Filter {
    const ALL: [Self; 2] = [Self::All, Self::Active];
    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Active => "Active",
        }
    }
    fn includes(self, row: &Row) -> bool {
        match self {
            Self::All => true,
            Self::Active => {
                matches!(row.state, TurnState::Working | TurnState::NeedsInput)
                    || row.agents.as_ref().is_some_and(|agents| agents.running > 0)
            }
        }
    }
    fn cycle(self) -> Self {
        match self {
            Self::All => Self::Active,
            Self::Active => Self::All,
        }
    }
}

#[derive(Clone, Copy)]
struct Palette {
    selection: Style,
    secondary: Style,
    unread: Style,
    colors: bool,
}
impl Palette {
    fn from_env() -> Self {
        let term = std::env::var("TERM").unwrap_or_default();
        let colors =
            !std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()) && term != "dumb";
        let truecolor = matches!(
            std::env::var("COLORTERM").as_deref(),
            Ok("truecolor" | "24bit")
        );
        let selection = if !colors {
            Style::new().reversed().bold()
        } else if truecolor {
            Style::new()
                .fg(Color::Rgb(0, 0, 46))
                .bg(Color::Rgb(99, 168, 248))
                .bold()
        } else if term.contains("256color") {
            Style::new()
                .fg(Color::Indexed(17))
                .bg(Color::Indexed(75))
                .bold()
        } else {
            Style::new().black().on_cyan().bold()
        };
        Self {
            selection,
            secondary: Style::new().dim(),
            unread: if !colors {
                Style::new()
            } else if truecolor {
                Style::new()
                    .fg(Color::Rgb(240, 228, 192))
                    .bg(Color::Rgb(48, 44, 26))
            } else if term.contains("256color") {
                Style::new().fg(Color::Indexed(230)).bg(Color::Indexed(235))
            } else {
                Style::new().white().on_dark_gray()
            },
            colors,
        }
    }
    fn state(self, state: TurnState) -> Style {
        if !self.colors {
            return Style::new().bold();
        }
        let color = match state {
            TurnState::Working => Color::Cyan,
            TurnState::Ready => Color::Green,
            TurnState::NeedsInput | TurnState::Interrupted => Color::Yellow,
            TurnState::Error => Color::Red,
            TurnState::Unknown => Color::Reset,
        };
        Style::new().fg(color).bold()
    }
}

struct View {
    query: String,
    filter: Filter,
    dense: bool,
    expanded: bool,
    selected: Option<ProcessIdentity>,
    visible: Vec<usize>,
    list: ListState,
    page_size: usize,
    palette: Palette,
    notice: Option<String>,
    focus_error: Option<String>,
    interaction_error: Option<String>,
    attention: Attention,
    loading: bool,
    focusing: Option<ProcessIdentity>,
    refresh_error: Option<String>,
    detail_scroll: usize,
    detail_max: usize,
    detail_page: usize,
}

impl View {
    fn finish_focus(&mut self, result: Result<u64>) {
        let target = self.focusing.take();
        if let (Ok(at_ms), Some(identity)) = (&result, target) {
            self.attention.acknowledge(identity, *at_ms);
        }
        self.notice = result.err().map(|error| {
            safe_text(
                &format!(
                    "PID {}: {error:#}",
                    target.map_or(0, |identity| identity.pid)
                ),
                2048,
            )
        });
        self.focus_error.clone_from(&self.notice);
    }

    fn new(palette: Palette) -> Self {
        Self {
            query: String::new(),
            filter: Filter::All,
            dense: true,
            expanded: false,
            selected: None,
            visible: Vec::new(),
            list: ListState::default(),
            page_size: 1,
            palette,
            notice: None,
            focus_error: None,
            interaction_error: None,
            attention: Attention::default(),
            loading: false,
            focusing: None,
            refresh_error: None,
            detail_scroll: 0,
            detail_max: 0,
            detail_page: 1,
        }
    }

    fn reconcile(&mut self, snapshot: &Snapshot) {
        let query = self.query.to_lowercase();
        self.visible = snapshot
            .windows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                self.filter.includes(row)
                    && (query.is_empty()
                        || [
                            row.thread_name.as_deref().unwrap_or("Unnamed thread"),
                            row.cwd.as_deref().unwrap_or_default(),
                            row.thread_id.as_deref().unwrap_or_default(),
                            row.tty.as_deref().unwrap_or_default(),
                            row.state.label(),
                            &row.pid.to_string(),
                        ]
                        .iter()
                        .any(|value| value.to_lowercase().contains(&query)))
            })
            .map(|(index, _)| index)
            .collect();
        let index = self
            .visible
            .iter()
            .position(|index| Some(snapshot.windows[*index].identity) == self.selected)
            .unwrap_or_else(|| {
                self.list
                    .selected()
                    .unwrap_or_default()
                    .min(self.visible.len().saturating_sub(1))
            });
        self.select(index, snapshot);
    }

    fn select(&mut self, index: usize, snapshot: &Snapshot) {
        let previous = self.selected;
        self.selected = self
            .visible
            .get(index)
            .map(|index| snapshot.windows[*index].identity);
        self.list.select(self.selected.map(|_| index));
        if self.selected != previous {
            self.detail_scroll = 0;
        }
    }

    fn add_query(&mut self, text: &str) {
        self.notice = None;
        let clean = safe_text(text, 1024);
        self.query.extend(
            clean
                .chars()
                .take(1024_usize.saturating_sub(self.query.chars().count())),
        );
    }

    fn key(&mut self, key: KeyEvent, snapshot: &Snapshot) -> bool {
        let had_notice = self.notice.take().is_some();
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        let index = self.list.selected().unwrap_or_default();
        let last = self.visible.len().saturating_sub(1);
        match key.code {
            KeyCode::Char('c') if control => return true,
            KeyCode::Esc if had_notice => (),
            KeyCode::Esc if self.query.is_empty() => return true,
            KeyCode::Esc => self.query.clear(),
            KeyCode::Char('o') if control => self.dense = !self.dense,
            KeyCode::Char('e') if control => {
                self.expanded = !self.expanded;
                self.detail_scroll = 0;
            }
            KeyCode::Char('u') if control => self.query.clear(),
            KeyCode::Char('r') if control => {
                if let Some(identity) = self.selected {
                    self.attention.mark_read(identity);
                }
            }
            KeyCode::Up => self.select(index.saturating_sub(1), snapshot),
            KeyCode::Down => self.select(index.saturating_add(1).min(last), snapshot),
            KeyCode::PageUp if self.expanded => {
                self.detail_scroll = self.detail_scroll.saturating_sub(self.detail_page)
            }
            KeyCode::PageDown if self.expanded => {
                self.detail_scroll = self
                    .detail_scroll
                    .saturating_add(self.detail_page)
                    .min(self.detail_max)
            }
            KeyCode::PageUp => self.select(index.saturating_sub(self.page_size), snapshot),
            KeyCode::PageDown => {
                self.select(index.saturating_add(self.page_size).min(last), snapshot)
            }
            KeyCode::Home => self.select(0, snapshot),
            KeyCode::End => self.select(last, snapshot),
            KeyCode::Left | KeyCode::Right => self.filter = self.filter.cycle(),
            KeyCode::Backspace => {
                self.query.pop();
            }
            KeyCode::Char(ch) if !control && !key.modifiers.contains(KeyModifiers::ALT) => {
                self.add_query(&ch.to_string())
            }
            _ => (),
        }
        self.reconcile(snapshot);
        false
    }

    fn focus_selected(
        &mut self,
        snapshot: &Snapshot,
        focus: impl FnOnce(ProcessIdentity) -> Result<()>,
    ) {
        self.notice = None;
        self.focus_error = None;
        let Some(row) = snapshot
            .windows
            .iter()
            .find(|row| Some(row.identity) == self.selected)
        else {
            return;
        };
        self.notice = focus(row.identity)
            .err()
            .map(|error| safe_text(&format!("{error:#}"), 2048));
        self.focus_error.clone_from(&self.notice);
    }

    fn draw(&mut self, frame: &mut Frame, snapshot: &Snapshot, interval: Duration) {
        let area = frame.area();
        // Match the resume picker's collapsing vertical gaps and one-column chrome inset.
        let gap = u16::from(area.height >= 18);
        let focus_message = self
            .focusing
            .map(|identity| format!("Focusing PID {}…  Esc cancels", identity.pid));
        let notice_lines = wrap_notice(
            focus_message
                .as_deref()
                .or(self.notice.as_deref())
                .unwrap_or_default(),
            area.width.saturating_sub(2).into(),
        );
        let notice_height = notice_lines.len().min(usize::from(area.height / 3)) as u16;
        let notice = Paragraph::new(notice_lines.into_iter().map(Line::from).collect::<Vec<_>>());
        let details = if self.expanded {
            self.details(snapshot, area.width.saturating_sub(2).into())
        } else {
            Vec::new()
        };
        let detail_height = details
            .len()
            .min(usize::from(area.height.saturating_sub(8) / 2)) as u16;
        let [
            header,
            _,
            toolbar,
            _,
            search,
            message,
            columns,
            list,
            detail_area,
            footer,
        ] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(gap),
            Constraint::Length(1),
            Constraint::Length(gap),
            Constraint::Length(1),
            Constraint::Length(notice_height),
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(detail_height),
            Constraint::Length(3),
        ])
        .areas(area);
        let inset = |rect: Rect| {
            Rect::new(
                rect.x.saturating_add(1),
                rect.y,
                rect.width.saturating_sub(2),
                rect.height,
            )
        };
        let mut header_text = format!("Kai windows  ·  {} open", snapshot.windows.len());
        let unread = snapshot
            .windows
            .iter()
            .filter(|row| self.attention.unseen(row))
            .count();
        if self.loading {
            header_text = "Kai windows  ·  Loading…".into();
        } else if self.refresh_error.is_some() {
            header_text.push_str("  ·  Stale · Ctrl+E details");
        } else if !snapshot.warnings.is_empty() {
            header_text.push_str("  ·  Discovery warnings · Ctrl+E details");
        }
        if unread > 0 {
            header_text.push_str(&format!("  ·  ● {unread} unread"));
        }
        frame.render_widget(Line::from(header_text).bold(), inset(header));
        frame.render_widget(notice, inset(message));
        self.detail_page = usize::from(detail_height).max(1);
        self.detail_max = details.len().saturating_sub(self.detail_page);
        self.detail_scroll = self.detail_scroll.min(self.detail_max);
        frame.render_widget(
            Paragraph::new(
                details
                    .into_iter()
                    .skip(self.detail_scroll)
                    .take(self.detail_page)
                    .map(Line::from)
                    .collect::<Vec<_>>(),
            )
            .style(self.palette.secondary),
            inset(detail_area),
        );
        let mut tabs = vec![Span::styled("Filter: ", self.palette.secondary)];
        for filter in Filter::ALL {
            let style = if self.filter == filter {
                self.palette.selection
            } else {
                self.palette.secondary
            };
            tabs.push(Span::styled(format!(" {} ", filter.label()), style));
        }
        if toolbar.width >= 62 {
            tabs.push(Span::styled(
                format!("   Refresh {}s", interval.as_secs()),
                self.palette.secondary,
            ));
        }
        frame.render_widget(Line::from(tabs), inset(toolbar));
        let search_text = if self.query.is_empty() {
            "Type to search".to_owned()
        } else {
            format!("Search: {}", self.query)
        };
        frame.render_widget(
            Line::from(clip(&search_text, inset(search).width.into()))
                .style(self.palette.secondary),
            inset(search),
        );
        let compact = area.width < 62;
        let cols = Columns::new(area.width, &snapshot.windows);
        let agents = if cols.agents > 0 {
            format!("{:width$}", "Subagents", width = cols.agents)
        } else {
            String::new()
        };
        let tokens = if cols.tokens > 0 {
            format!("{:width$}", "Tokens", width = cols.tokens)
        } else {
            String::new()
        };
        frame.render_widget(
            Line::from(format!(
                "  {:state_width$}{:time_width$}{agents}{tokens}Thread",
                "State",
                "Elapsed",
                state_width = cols.state,
                time_width = cols.time
            ))
            .style(self.palette.secondary),
            inset(columns),
        );

        let list = inset(list);
        self.page_size = (usize::from(list.height) / if self.dense { 1 } else { 3 }).max(1);
        if self.visible.is_empty() {
            let message = if self.loading {
                "Looking for Kai windows…"
            } else if self.refresh_error.is_some() && snapshot.windows.is_empty() {
                "Discovery failed · Ctrl+E details"
            } else if snapshot.windows.is_empty() {
                "No Kai windows"
            } else {
                "No matching windows"
            };
            frame.render_widget(Line::from(message).style(self.palette.secondary), list);
        } else {
            let items = self
                .visible
                .iter()
                .enumerate()
                .map(|(visible_index, index)| {
                    let row = &snapshot.windows[*index];
                    self.item(
                        row,
                        snapshot.observed_at,
                        list.width.saturating_sub(2),
                        cols,
                        visible_index,
                    )
                })
                .collect::<Vec<_>>();
            frame.render_stateful_widget(
                List::new(items)
                    .highlight_symbol("› ")
                    .highlight_style(self.palette.selection),
                list,
                &mut self.list,
            );
        }
        if footer.height > 0 {
            let rule = Rect::new(footer.x, footer.y, footer.width, 1);
            frame.render_widget(
                Line::from("─".repeat(footer.width.into())).style(self.palette.secondary),
                rule,
            );
            let position = self.list.selected().map_or(0, |index| index + 1);
            let progress = format!(" {position} / {} ", self.visible.len());
            let width = progress.width() as u16;
            if width < rule.width {
                frame.render_widget(
                    Line::from(progress).style(self.palette.secondary),
                    Rect::new(rule.right() - width - 1, rule.y, width, 1),
                );
            }
            let quit = if self.focusing.is_some() {
                "cancel focus"
            } else if self.notice.is_some() {
                "dismiss"
            } else if self.query.is_empty() {
                "quit"
            } else {
                "clear search"
            };
            let density = if self.dense { "comfortable" } else { "dense" };
            let rows = if compact {
                vec![
                    hints(
                        &[("enter", "focus"), ("esc", quit), ("↑/↓", "browse")],
                        self.palette.secondary,
                        inset(footer).width,
                    ),
                    hints(
                        &[
                            ("ctrl+o", density),
                            ("ctrl+e", "details"),
                            ("ctrl+r", "mark read"),
                        ],
                        self.palette.secondary,
                        inset(footer).width,
                    ),
                ]
            } else {
                vec![
                    hints(
                        &[
                            ("enter", "focus"),
                            ("↑/↓", "browse"),
                            ("esc", quit),
                            ("←/→", "filter"),
                            ("ctrl+c", "quit"),
                        ],
                        self.palette.secondary,
                        inset(footer).width,
                    ),
                    hints(
                        &[
                            ("ctrl+o", density),
                            ("ctrl+e", "details"),
                            ("ctrl+r", "mark read"),
                            ("pgup/pgdn", if self.expanded { "details" } else { "page" }),
                        ],
                        self.palette.secondary,
                        inset(footer).width,
                    ),
                ]
            };
            frame.render_widget(
                Paragraph::new(rows),
                inset(Rect::new(
                    footer.x,
                    footer.y.saturating_add(1),
                    footer.width,
                    footer.height.saturating_sub(1),
                )),
            );
        }
    }

    fn item(
        &self,
        row: &Row,
        observed_at: u64,
        width: u16,
        cols: Columns,
        index: usize,
    ) -> ListItem<'static> {
        let selected = self.list.selected() == Some(index);
        let unread = self.attention.unseen(row);
        let normal = if selected {
            Style::new()
        } else {
            self.palette.secondary
        };
        let state_style = if selected {
            Style::new().bold()
        } else {
            self.palette.state(row.state)
        };
        let state = if cols.state == 6 {
            short_state(row.state)
        } else {
            row.state.label()
        };
        let emphasis = if selected || !self.palette.colors {
            Style::new().bold()
        } else {
            Style::new().yellow().bold()
        };
        let title_width =
            usize::from(width).saturating_sub(cols.state + cols.time + cols.agents + cols.tokens);
        let mut summary = vec![Span::styled(state.to_owned(), state_style)];
        let mut state_width = state.width();
        if cols.state >= STATE_WITH_AGE_WIDTH
            && let Some(elapsed) = state_age(row, observed_at)
        {
            let elapsed = clip(
                &format!(" · {elapsed}"),
                cols.state.saturating_sub(state_width + 1),
            );
            state_width += elapsed.width();
            summary.push(Span::styled(
                elapsed,
                if unread { emphasis } else { normal },
            ));
        }
        summary.push(Span::raw(
            " ".repeat(cols.state.saturating_sub(state_width)),
        ));
        summary.push(Span::styled(
            format!(
                "{:width$}",
                clip(&run_time(row.run_time_ms), cols.time.saturating_sub(1)),
                width = cols.time
            ),
            normal,
        ));
        if cols.agents > 0 {
            let agents = row
                .agents
                .as_ref()
                .map_or_else(|| "?".into(), super::agents::Summary::label);
            summary.push(Span::styled(
                format!(
                    "{:width$}",
                    clip(&agents, cols.agents - 1),
                    width = cols.agents
                ),
                normal,
            ));
        }
        if cols.tokens > 0 {
            let tokens = row
                .token_usage
                .as_ref()
                .map_or_else(|| "—".into(), super::tokens::Usage::label);
            summary.push(Span::styled(
                format!(
                    "{:width$}",
                    clip(&tokens, cols.tokens - 1),
                    width = cols.tokens
                ),
                normal,
            ));
        }
        let title = format!(
            "{}{}",
            if unread { "● " } else { "" },
            row.thread_name.as_deref().unwrap_or("Unnamed thread")
        );
        summary.push(Span::styled(
            clip(&title, title_width),
            if selected || unread {
                emphasis
            } else {
                Style::new()
            },
        ));
        let mut lines = vec![Line::from(summary)];
        if !self.dense {
            lines.push(
                Line::from(clip(
                    &format!(
                        "{}  ·  {}",
                        row.tty.as_deref().unwrap_or("no tty"),
                        row.cwd.as_deref().unwrap_or("—")
                    ),
                    width.into(),
                ))
                .style(normal),
            );
            let timing = if cols.state < STATE_WITH_AGE_WIDTH {
                state_age(row, observed_at)
                    .map(|elapsed| format!("{} · {elapsed}  ·  ", row.state.label()))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let timing = clip(&timing, width.into());
            let pid_width = usize::from(width).saturating_sub(timing.width());
            lines.push(Line::from(vec![
                Span::styled(timing, if unread { emphasis } else { normal }),
                Span::styled(clip(&format!("PID {}", row.pid), pid_width), normal),
            ]));
        }
        ListItem::new(lines).style(if unread && !selected && self.palette.colors {
            self.palette.unread
        } else {
            Style::new()
        })
    }

    fn details(&self, snapshot: &Snapshot, width: usize) -> Vec<String> {
        let mut messages = vec!["Details · PgUp/PgDn scroll".to_owned()];
        if let Some(error) = &self.focus_error {
            messages.push(format!("Window switching: {error}"));
        }
        if let Some(error) = &self.refresh_error {
            messages.push(error.clone());
        }
        if let Some(error) = &self.interaction_error {
            messages.push(format!(
                "Focus tracking: {error}. Ctrl+R marks the selected completion read."
            ));
        }
        for warning in &snapshot.warnings {
            messages.push(format!("Discovery: {warning}"));
        }
        if let Some(row) = snapshot
            .windows
            .iter()
            .find(|row| Some(row.identity) == self.selected)
        {
            if self.attention.unseen(row) {
                messages.push(
                    "● Unread completion · focus this session or press Ctrl+R to mark read".into(),
                );
            }
            if let Some(detail) = &row.detail {
                messages.push(format!("{}: {detail}", row.state.label()));
            }
            messages.push(format!(
                "{}  ·  {}",
                row.tty.as_deref().unwrap_or("no tty"),
                row.cwd.as_deref().unwrap_or("—")
            ));
            if row.state == TurnState::NeedsInput {
                messages.push(format!(
                    "Input requested {}",
                    state_age(row, snapshot.observed_at).unwrap_or_else(|| "—".into())
                ));
            }
            let ending = if matches!(
                row.state,
                TurnState::Ready | TurnState::Interrupted | TurnState::Error
            ) {
                "Last ended"
            } else {
                "Previous turn ended"
            };
            messages.push(format!(
                "{ending} {}  ·  PID {}",
                row.last_finished_at
                    .map_or_else(|| "—".into(), |at| age(snapshot.observed_at, at)),
                row.pid
            ));
            messages.push(format!(
                "Thread {}",
                row.thread_id.as_deref().unwrap_or("—")
            ));
            messages.push(format!(
                "Subagents: {}",
                row.agents.as_ref().map_or_else(
                    || "Unknown; session discovery is incomplete".into(),
                    super::agents::Summary::description
                )
            ));
            if let Some(usage) = &row.token_usage {
                messages.extend(usage.details());
            } else {
                messages.push("Tokens: unavailable".into());
            }
        }
        messages
            .into_iter()
            .flat_map(|message| wrap_notice(&safe_text(&message, 4096), width))
            .collect()
    }
}

fn wrap_notice(message: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return Vec::new();
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in message.split_whitespace() {
        if !line.is_empty() && line.width() + 1 + word.width() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        for ch in word.chars() {
            let ch_width = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
            if ch_width > width {
                continue;
            }
            if line.width() + ch_width > width {
                lines.push(std::mem::take(&mut line));
            }
            line.push(ch);
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

fn short_state(state: TurnState) -> &'static str {
    match state {
        TurnState::Working => "Act",
        TurnState::NeedsInput => "Input",
        TurnState::Ready => "Ready",
        TurnState::Interrupted => "Stop",
        TurnState::Error => "Err",
        TurnState::Unknown => "?",
    }
}

fn state_age(row: &Row, observed_at: u64) -> Option<String> {
    match row.state {
        TurnState::NeedsInput => row.input_requested_at,
        TurnState::Ready | TurnState::Interrupted | TurnState::Error => row.last_finished_at,
        TurnState::Working | TurnState::Unknown => None,
    }
    .map(|at| age(observed_at, at))
}

const STATE_WITH_AGE_WIDTH: usize = 22;

#[derive(Clone, Copy)]
struct Columns {
    state: usize,
    time: usize,
    agents: usize,
    tokens: usize,
}
impl Columns {
    fn new(width: u16, rows: &[Row]) -> Self {
        // Keep widths stable while browsing/filtering; grow only for longer live counts.
        let agents_width = rows
            .iter()
            .filter_map(|row| row.agents.as_ref())
            .map(|agents| agents.label().width() + 1)
            .max()
            .unwrap_or(11)
            .clamp(11, 15);
        Self {
            state: if width >= 90 {
                STATE_WITH_AGE_WIDTH
            } else if width >= 62 {
                12
            } else {
                6
            },
            time: 8,
            agents: if width >= 50 { agents_width } else { 0 },
            tokens: if width >= 70 { 10 } else { 0 },
        }
    }
}

fn hints(values: &[(&str, &str)], secondary: Style, width: u16) -> Line<'static> {
    let mut spans = Vec::new();
    let mut used = 0;
    for (index, (key, label)) in values.iter().enumerate() {
        let added = key.width() + label.width() + 1 + if index > 0 { 3 } else { 0 };
        if used + added > usize::from(width) {
            break;
        }
        used += added;
        if index > 0 {
            spans.push(Span::raw("   "));
        }
        spans.push(Span::styled((*key).to_owned(), Style::new().bold()));
        spans.push(Span::styled(format!(" {label}"), secondary));
    }
    Line::from(spans)
}

fn clip(text: &str, width: usize) -> String {
    let clean = safe_text(text, usize::MAX);
    if clean.width() <= width {
        clean
    } else if width == 0 {
        String::new()
    } else {
        format!("{}…", safe_text(&clean, width - 1))
    }
}

pub(super) fn run_time(ms: Option<u64>) -> String {
    ms.map_or_else(|| "—".into(), |value| elapsed(value / 1000))
}

#[cfg(test)]
#[path = "ui_tests.rs"]
mod tests;
