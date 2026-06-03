use std::error::Error;
use std::fmt::{self, Display};
use std::io::{self, IsTerminal};

use anyhow::{Context, Result, bail};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, Padding, Paragraph, Wrap};
use ratatui::{Frame, Terminal};

const FOOTER_HEIGHT: u16 = 1;
const MIN_VISIBLE_ROWS: u16 = 1;
const CHOICE_FRAME_EXTRA_ROWS: u16 = 5;
const CHOICE_MIN_HEIGHT: u16 = 8;
const CHOICE_OUTER_MARGIN_ROWS: u16 = 2;
const CHOICE_BODY_MIN_ROWS: u16 = 3;
const CHOICE_PAGE_STEP_ROWS: usize = 8;
const EDITOR_HELP_CHROME_ROWS: u16 = 3;
const EDITOR_RESERVED_ROWS_BELOW_HELP: u16 = 4;
const EDITOR_BODY_MIN_ROWS: u16 = 3;
const EDITOR_BODY_RESERVED_ROWS: u16 = 2;
const EDITOR_CONTEXT_MIN_ROWS: u16 = 6;
const EDITOR_CONTEXT_HEIGHT_NUMERATOR: u16 = 2;
const EDITOR_CONTEXT_HEIGHT_DENOMINATOR: u16 = 3;
const CONFIRM_MESSAGE_MIN_ROWS: u16 = 3;
const CONFIRM_BUTTON_ROWS: u16 = 3;
const CONFIRM_PANEL_HEIGHT: u16 = 11;
const CENTERED_MIN_WIDTH: u16 = 36;
const CENTERING_DIVISOR: u16 = 2;
const PERCENT_DENOMINATOR: u16 = 100;
const FLEX_SPACER_MIN: u16 = 0;
const DEFAULT_TERMINAL_COLS: u16 = 100;
const DEFAULT_TERMINAL_ROWS: u16 = 30;
const PANEL_HORIZONTAL_PADDING: u16 = 1;

#[derive(Debug, Clone)]
pub struct UserCancelled {
    message: String,
}

impl UserCancelled {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for UserCancelled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for UserCancelled {}

pub fn is_cancelled(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<UserCancelled>().is_some())
}

pub fn cancellation_message(error: &anyhow::Error) -> Option<String> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<UserCancelled>())
        .map(|cancelled| cancelled.message().to_owned())
}

pub struct PromptEditor {
    title: String,
    help: Vec<String>,
    context_title: Option<String>,
    context_lines: Vec<String>,
    history: Vec<String>,
}

impl PromptEditor {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            help: Vec::new(),
            context_title: None,
            context_lines: Vec::new(),
            history: Vec::new(),
        }
    }

    pub fn help(mut self, line: impl Into<String>) -> Self {
        self.help.push(line.into());
        self
    }

    pub fn history(mut self, history: Vec<String>) -> Self {
        self.history = history
            .into_iter()
            .map(|entry| entry.trim().to_owned())
            .filter(|entry| !entry.is_empty())
            .collect();
        self
    }

    pub fn context_text(mut self, title: impl Into<String>, text: &str) -> Self {
        self.context_title = Some(title.into());
        self.context_lines = strip_ansi(text)
            .lines()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if self.context_lines.is_empty() {
            self.context_lines.push("(no transcript output)".to_owned());
        }
        self
    }

    pub fn edit(self) -> Result<Option<String>> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("interactive prompt editor requires a terminal");
        }
        let mut terminal = TerminalSession::enter()?;
        EditorState::new(
            self.title,
            self.help,
            self.context_title,
            self.context_lines,
            self.history,
        )
        .run(&mut terminal)
    }
}

pub struct ConfirmPrompt {
    title: String,
    message: String,
    default: bool,
}

pub struct ChoicePrompt {
    title: String,
    items: Vec<String>,
    default: usize,
    shortcut: Option<ChoiceShortcut>,
}

#[derive(Clone)]
struct ChoiceShortcut {
    key: char,
    index: usize,
    label: String,
}

impl ChoicePrompt {
    pub fn new(
        title: impl Into<String>,
        items: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            title: title.into(),
            items: items.into_iter().map(Into::into).collect(),
            default: 0,
            shortcut: None,
        }
    }

    pub fn default(mut self, value: usize) -> Self {
        self.default = value;
        self
    }

    pub fn shortcut(mut self, key: char, index: usize, label: impl Into<String>) -> Self {
        self.shortcut = Some(ChoiceShortcut {
            key,
            index,
            label: label.into(),
        });
        self
    }

    pub fn select(self) -> Result<usize> {
        if self.items.is_empty() {
            bail!("choice prompt requires at least one item");
        }
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("interactive choice prompt requires a terminal");
        }
        let mut terminal = TerminalSession::enter()?;
        ChoiceState {
            title: self.title,
            items: self.items,
            selected: self.default,
            scroll: 0,
            shortcut: self.shortcut,
        }
        .run(&mut terminal)
    }
}

impl ConfirmPrompt {
    pub fn new(title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            message: message.into(),
            default: false,
        }
    }

    pub fn default(mut self, value: bool) -> Self {
        self.default = value;
        self
    }

    pub fn confirm(self) -> Result<bool> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            bail!("interactive confirmation requires a terminal");
        }
        let mut terminal = TerminalSession::enter()?;
        ConfirmState {
            title: self.title,
            message: self.message,
            selected: self.default,
        }
        .run(&mut terminal)
    }
}

struct ChoiceState {
    title: String,
    items: Vec<String>,
    selected: usize,
    scroll: usize,
    shortcut: Option<ChoiceShortcut>,
}

impl ChoiceState {
    fn run(&mut self, terminal: &mut TerminalSession) -> Result<usize> {
        self.selected = self.selected.min(self.items.len().saturating_sub(1));
        loop {
            terminal.draw(|frame| self.draw(frame))?;
            let Event::Key(key) = event::read().context("Failed to read terminal event")? else {
                continue;
            };
            match key.code {
                KeyCode::Enter => return Ok(self.selected),
                KeyCode::Esc => return Err(UserCancelled::new("selection cancelled").into()),
                KeyCode::Char(ch)
                    if self
                        .shortcut
                        .as_ref()
                        .is_some_and(|shortcut| shortcut.key == ch) =>
                {
                    return Ok(self
                        .shortcut
                        .as_ref()
                        .expect("shortcut was matched")
                        .index
                        .min(self.items.len() - 1));
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.selected = self.selected.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => {
                    self.selected = (self.selected + 1).min(self.items.len() - 1);
                }
                KeyCode::Home => self.selected = 0,
                KeyCode::End => self.selected = self.items.len() - 1,
                KeyCode::PageUp => {
                    self.selected = self.selected.saturating_sub(CHOICE_PAGE_STEP_ROWS);
                }
                KeyCode::PageDown => {
                    self.selected =
                        (self.selected + CHOICE_PAGE_STEP_ROWS).min(self.items.len() - 1);
                }
                _ => {}
            }
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let content_rows = self
            .items
            .iter()
            .map(|item| choice_item_height(item))
            .sum::<usize>()
            .min(u16::MAX as usize) as u16;
        let height = choice_panel_height(
            content_rows.saturating_add(CHOICE_FRAME_EXTRA_ROWS),
            frame.area().height,
        );
        let area = centered_rect(frame.area(), 70, height);
        frame.render_widget(Clear, area);
        let block = panel(self.title.as_str())
            .border_style(Style::default().fg(Color::Cyan))
            .padding(Padding::horizontal(PANEL_HORIZONTAL_PADDING));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(CHOICE_BODY_MIN_ROWS),
                Constraint::Length(FOOTER_HEIGHT),
            ])
            .split(inner);
        let visible = chunks[0].height.max(MIN_VISIBLE_ROWS) as usize;
        self.ensure_selected_visible(visible);
        let mut lines = Vec::new();
        for (index, item) in self.items.iter().enumerate().skip(self.scroll) {
            for line in choice_lines(item, index == self.selected) {
                if lines.len() >= visible {
                    break;
                }
                lines.push(line);
            }
            if lines.len() >= visible {
                break;
            }
        }
        frame.render_widget(Paragraph::new(Text::from(lines)), chunks[0]);
        let footer = self
            .shortcut
            .as_ref()
            .map(|shortcut| {
                format!(
                    "Enter select  {} {}  ↑/↓ move  j/k move  Esc cancel",
                    shortcut.key, shortcut.label
                )
            })
            .unwrap_or_else(|| "Enter select  ↑/↓ move  j/k move  Esc cancel".to_owned());
        draw_footer(frame, chunks[1], &footer);
    }

    fn ensure_selected_visible(&mut self, visible: usize) {
        if self.selected < self.scroll {
            self.scroll = self.selected;
        }
        while self.scroll < self.selected && self.rows_from_scroll_to_selected() > visible {
            self.scroll += 1;
        }
    }

    fn rows_from_scroll_to_selected(&self) -> usize {
        self.items
            .iter()
            .enumerate()
            .skip(self.scroll)
            .take(self.selected.saturating_sub(self.scroll) + 1)
            .map(|(_, item)| choice_item_height(item))
            .sum()
    }
}

struct EditorState {
    title: String,
    help: Vec<String>,
    context_title: Option<String>,
    context_lines: Vec<String>,
    context_scroll: usize,
    lines: Vec<String>,
    cursor_row: usize,
    cursor_col: usize,
    scroll: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    draft_before_history: Option<Vec<String>>,
}

impl EditorState {
    fn new(
        title: String,
        help: Vec<String>,
        context_title: Option<String>,
        context_lines: Vec<String>,
        history: Vec<String>,
    ) -> Self {
        Self {
            title,
            help,
            context_title,
            context_lines,
            context_scroll: usize::MAX,
            lines: vec![String::new()],
            cursor_row: 0,
            cursor_col: 0,
            scroll: 0,
            history,
            history_index: None,
            draft_before_history: None,
        }
    }

    fn run(&mut self, terminal: &mut TerminalSession) -> Result<Option<String>> {
        loop {
            terminal.draw(|frame| self.draw(frame))?;
            let Event::Key(key) = event::read().context("Failed to read terminal event")? else {
                continue;
            };
            match self.handle_key(key)? {
                EditorAction::Continue => {}
                EditorAction::Submit => {
                    let text = self.text();
                    return Ok((!text.trim().is_empty()).then_some(text.trim().to_owned()));
                }
                EditorAction::Cancel => return Ok(None),
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<EditorAction> {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('c') => return Ok(EditorAction::Cancel),
                KeyCode::Char('d') => return Ok(EditorAction::Submit),
                KeyCode::Char('a') => {
                    self.cursor_col = 0;
                    return Ok(EditorAction::Continue);
                }
                KeyCode::Char('e') => {
                    self.cursor_col = self.current_line().chars().count();
                    return Ok(EditorAction::Continue);
                }
                _ => {}
            }
        }
        match key.code {
            KeyCode::Esc => Ok(EditorAction::Cancel),
            KeyCode::Enter => {
                self.begin_edit();
                let byte_col = self.byte_col();
                let suffix = self.current_line_mut().split_off(byte_col);
                self.cursor_row += 1;
                self.cursor_col = 0;
                self.lines.insert(self.cursor_row, suffix);
                Ok(EditorAction::Continue)
            }
            KeyCode::Backspace => {
                self.begin_edit();
                self.backspace();
                Ok(EditorAction::Continue)
            }
            KeyCode::Delete => {
                self.begin_edit();
                self.delete();
                Ok(EditorAction::Continue)
            }
            KeyCode::Left => {
                self.move_left();
                Ok(EditorAction::Continue)
            }
            KeyCode::Right => {
                self.move_right();
                Ok(EditorAction::Continue)
            }
            KeyCode::Up => {
                if self.cursor_row == 0 {
                    self.history_previous();
                } else {
                    self.move_up();
                }
                Ok(EditorAction::Continue)
            }
            KeyCode::Down => {
                if self.history_index.is_some() {
                    self.history_next();
                } else {
                    self.move_down();
                }
                Ok(EditorAction::Continue)
            }
            KeyCode::PageUp => {
                if self.has_context() {
                    self.context_scroll = self.context_scroll.saturating_sub(self.context_rows());
                } else {
                    self.cursor_row = self.cursor_row.saturating_sub(self.visible_body_rows());
                    self.clamp_cursor_col();
                }
                Ok(EditorAction::Continue)
            }
            KeyCode::PageDown => {
                if self.has_context() {
                    self.context_scroll =
                        (self.context_scroll + self.context_rows()).min(self.max_context_scroll());
                } else {
                    self.cursor_row =
                        (self.cursor_row + self.visible_body_rows()).min(self.lines.len() - 1);
                    self.clamp_cursor_col();
                }
                Ok(EditorAction::Continue)
            }
            KeyCode::Home => {
                self.cursor_col = 0;
                Ok(EditorAction::Continue)
            }
            KeyCode::End => {
                self.cursor_col = self.current_line().chars().count();
                Ok(EditorAction::Continue)
            }
            KeyCode::Char(ch) => {
                self.begin_edit();
                let byte_col = self.byte_col();
                self.current_line_mut().insert(byte_col, ch);
                self.cursor_col += 1;
                Ok(EditorAction::Continue)
            }
            _ => Ok(EditorAction::Continue),
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let help_height = self.help_height(area.height);
        let context_height = if self.has_context() {
            editor_context_height(
                area.height
                    .saturating_sub(help_height + EDITOR_RESERVED_ROWS_BELOW_HELP),
            )
        } else {
            0
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(help_height),
                Constraint::Length(context_height),
                Constraint::Min(EDITOR_BODY_MIN_ROWS),
                Constraint::Length(FOOTER_HEIGHT),
            ])
            .split(area);
        self.draw_header(frame, chunks[0]);
        if self.has_context() {
            self.draw_context(frame, chunks[1]);
        }
        self.draw_editor(frame, chunks[2]);
        draw_footer(
            frame,
            chunks[3],
            "Ctrl-D submit  Esc cancel  ↑ history  PgUp/PgDn scroll",
        );
    }

    fn draw_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = vec![Line::from(vec![Span::styled(
            self.title.as_str(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )])];
        lines.extend(self.help.iter().map(|line| {
            Line::from(Span::styled(
                line.as_str(),
                Style::default().fg(Color::DarkGray),
            ))
        }));
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(panel("think").border_style(Style::default().fg(Color::Cyan)))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn draw_context(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let block = panel(self.context_title.as_deref().unwrap_or("Context"))
            .border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(area);
        let visible = inner.height as usize;
        self.context_scroll = if self.context_scroll == usize::MAX {
            self.max_context_scroll_for(visible)
        } else {
            self.context_scroll
                .min(self.max_context_scroll_for(visible))
        };
        let lines = self
            .context_lines
            .iter()
            .skip(self.context_scroll)
            .take(visible)
            .map(|line| {
                Line::from(Span::styled(
                    line.as_str(),
                    Style::default().fg(Color::Gray),
                ))
            })
            .collect::<Vec<_>>();
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_editor(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let block = panel("Prompt").border_style(Style::default().fg(Color::Blue));
        let inner = block.inner(area);
        let body_rows = inner.height.max(MIN_VISIBLE_ROWS) as usize;
        if self.cursor_row < self.scroll {
            self.scroll = self.cursor_row;
        } else if self.cursor_row >= self.scroll + body_rows {
            self.scroll = self.cursor_row + 1 - body_rows;
        }
        let lines = (0..body_rows)
            .map(|row| {
                self.lines
                    .get(self.scroll + row)
                    .map(|line| Line::from(line.as_str()))
                    .unwrap_or_else(|| {
                        Line::from(Span::styled("~", Style::default().fg(Color::DarkGray)))
                    })
            })
            .collect::<Vec<_>>();
        frame.render_widget(block, area);
        frame.render_widget(Paragraph::new(Text::from(lines)), inner);
        let cursor_y = inner.y + self.cursor_row.saturating_sub(self.scroll) as u16;
        let cursor_x = inner.x + self.cursor_col.min(inner.width.saturating_sub(1) as usize) as u16;
        if cursor_y < inner.y + inner.height {
            frame.set_cursor_position(Position::new(cursor_x, cursor_y));
        }
    }

    fn header_rows(&self) -> u16 {
        self.help.len() as u16 + EDITOR_HELP_CHROME_ROWS
    }

    fn has_context(&self) -> bool {
        !self.context_lines.is_empty()
    }

    fn context_rows(&self) -> usize {
        if !self.has_context() {
            return 0;
        }
        let (_, rows) =
            crossterm::terminal::size().unwrap_or((DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS));
        editor_context_height(rows.saturating_sub(self.header_rows() + EDITOR_HELP_CHROME_ROWS))
            as usize
    }

    fn max_context_scroll(&self) -> usize {
        self.max_context_scroll_for(self.context_rows())
    }

    fn max_context_scroll_for(&self, visible: usize) -> usize {
        self.context_lines
            .len()
            .saturating_sub(visible.max(MIN_VISIBLE_ROWS as usize))
    }

    fn visible_body_rows(&self) -> usize {
        let (_, rows) =
            crossterm::terminal::size().unwrap_or((DEFAULT_TERMINAL_COLS, DEFAULT_TERMINAL_ROWS));
        rows.saturating_sub(
            self.header_rows() + self.context_rows() as u16 + EDITOR_BODY_RESERVED_ROWS,
        )
        .max(MIN_VISIBLE_ROWS) as usize
    }

    fn help_height(&self, area_height: u16) -> u16 {
        self.header_rows().min(
            area_height
                .saturating_sub(EDITOR_RESERVED_ROWS_BELOW_HELP)
                .max(MIN_VISIBLE_ROWS),
        )
    }

    fn text(&self) -> String {
        self.lines.join("\n")
    }

    fn current_line(&self) -> &str {
        &self.lines[self.cursor_row]
    }

    fn current_line_mut(&mut self) -> &mut String {
        &mut self.lines[self.cursor_row]
    }

    fn byte_col(&self) -> usize {
        char_to_byte(self.current_line(), self.cursor_col)
    }

    fn clamp_cursor_col(&mut self) {
        self.cursor_col = self.cursor_col.min(self.current_line().chars().count());
    }

    fn backspace(&mut self) {
        if self.cursor_col > 0 {
            let byte_col = self.byte_col();
            let previous = char_to_byte(self.current_line(), self.cursor_col - 1);
            self.current_line_mut().drain(previous..byte_col);
            self.cursor_col -= 1;
        } else if self.cursor_row > 0 {
            let removed = self.lines.remove(self.cursor_row);
            self.cursor_row -= 1;
            self.cursor_col = self.current_line().chars().count();
            self.current_line_mut().push_str(&removed);
        }
    }

    fn delete(&mut self) {
        if self.cursor_col < self.current_line().chars().count() {
            let start = self.byte_col();
            let end = char_to_byte(self.current_line(), self.cursor_col + 1);
            self.current_line_mut().drain(start..end);
        } else if self.cursor_row + 1 < self.lines.len() {
            let next = self.lines.remove(self.cursor_row + 1);
            self.current_line_mut().push_str(&next);
        }
    }

    fn move_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor_row > 0 {
            self.cursor_row -= 1;
            self.cursor_col = self.current_line().chars().count();
        }
    }

    fn move_right(&mut self) {
        if self.cursor_col < self.current_line().chars().count() {
            self.cursor_col += 1;
        } else if self.cursor_row + 1 < self.lines.len() {
            self.cursor_row += 1;
            self.cursor_col = 0;
        }
    }

    fn move_up(&mut self) {
        if self.cursor_row > 0 {
            self.cursor_row -= 1;
            self.clamp_cursor_col();
        }
    }

    fn move_down(&mut self) {
        if self.cursor_row + 1 < self.lines.len() {
            self.cursor_row += 1;
            self.clamp_cursor_col();
        }
    }

    fn history_previous(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let index = match self.history_index {
            Some(0) => 0,
            Some(index) => index - 1,
            None => {
                self.draft_before_history = Some(self.lines.clone());
                self.history.len() - 1
            }
        };
        self.history_index = Some(index);
        self.replace_text(self.history[index].clone());
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            self.history_index = Some(index + 1);
            self.replace_text(self.history[index + 1].clone());
        } else {
            self.lines = self
                .draft_before_history
                .take()
                .unwrap_or_else(|| vec![String::new()]);
            self.history_index = None;
            self.cursor_row = self.lines.len().saturating_sub(1);
            self.cursor_col = self.current_line().chars().count();
            self.scroll = 0;
        }
    }

    fn replace_text(&mut self, text: String) {
        self.lines = text.lines().map(ToOwned::to_owned).collect();
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        self.cursor_row = self.lines.len() - 1;
        self.cursor_col = self.current_line().chars().count();
        self.scroll = 0;
    }

    fn begin_edit(&mut self) {
        self.history_index = None;
        self.draft_before_history = None;
    }
}

fn editor_context_height(available: u16) -> u16 {
    if available < EDITOR_CONTEXT_MIN_ROWS {
        available
    } else {
        available
            .saturating_mul(EDITOR_CONTEXT_HEIGHT_NUMERATOR)
            .saturating_div(EDITOR_CONTEXT_HEIGHT_DENOMINATOR)
            .clamp(EDITOR_CONTEXT_MIN_ROWS, available)
    }
}

struct ConfirmState {
    title: String,
    message: String,
    selected: bool,
}

impl ConfirmState {
    fn run(&mut self, terminal: &mut TerminalSession) -> Result<bool> {
        loop {
            terminal.draw(|frame| self.draw(frame))?;
            let Event::Key(key) = event::read().context("Failed to read terminal event")? else {
                continue;
            };
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => return Ok(true),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => return Ok(false),
                KeyCode::Enter => return Ok(self.selected),
                KeyCode::Left | KeyCode::Right | KeyCode::Tab => self.selected = !self.selected,
                _ => {}
            }
        }
    }

    fn draw(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 68, CONFIRM_PANEL_HEIGHT);
        frame.render_widget(Clear, area);
        let block = panel(self.title.as_str())
            .border_style(Style::default().fg(Color::Cyan))
            .padding(Padding::horizontal(PANEL_HORIZONTAL_PADDING));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(CONFIRM_MESSAGE_MIN_ROWS),
                Constraint::Length(CONFIRM_BUTTON_ROWS),
                Constraint::Length(FOOTER_HEIGHT),
            ])
            .split(inner);
        frame.render_widget(
            Paragraph::new(self.message.as_str())
                .style(Style::default().fg(Color::White))
                .wrap(Wrap { trim: false }),
            chunks[0],
        );
        frame.render_widget(Paragraph::new(confirm_buttons(self.selected)), chunks[1]);
        draw_footer(
            frame,
            chunks[2],
            "Enter accept  ←/→ toggle  y yes  n no  Esc no",
        );
    }
}

enum EditorAction {
    Continue,
    Submit,
    Cancel,
}

pub(crate) struct TerminalSession {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalSession {
    pub(crate) fn enter() -> Result<Self> {
        enable_raw_mode().context("Failed to enable raw terminal mode")?;
        execute!(io::stdout(), EnterAlternateScreen)
            .context("Failed to enter alternate terminal screen")?;
        let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))
            .context("Failed to initialize terminal UI")?;
        terminal.clear().context("Failed to clear terminal UI")?;
        Ok(Self { terminal })
    }

    pub(crate) fn draw(&mut self, render: impl FnOnce(&mut Frame<'_>)) -> Result<()> {
        self.terminal
            .draw(render)
            .context("Failed to render terminal UI")?;
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

fn panel(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(Line::from(Span::styled(
            title.to_owned(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )))
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect, text: &str) {
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(Color::Black).bg(Color::Cyan))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn centered_rect(area: Rect, percent_x: u16, height: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(area.height.saturating_sub(height) / CENTERING_DIVISOR),
            Constraint::Length(height.min(area.height)),
            Constraint::Min(FLEX_SPACER_MIN),
        ])
        .split(area);
    let width = area
        .width
        .saturating_mul(percent_x)
        .saturating_div(PERCENT_DENOMINATOR);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(area.width.saturating_sub(width) / CENTERING_DIVISOR),
            Constraint::Length(width.max(CENTERED_MIN_WIDTH).min(area.width)),
            Constraint::Min(FLEX_SPACER_MIN),
        ])
        .split(vertical[1])[1]
}

fn choice_panel_height(desired: u16, terminal_height: u16) -> u16 {
    let max_height = terminal_height.saturating_sub(CHOICE_OUTER_MARGIN_ROWS);
    if max_height >= CHOICE_MIN_HEIGHT {
        desired.clamp(CHOICE_MIN_HEIGHT, max_height)
    } else {
        terminal_height
    }
}

fn confirm_buttons(selected: bool) -> Line<'static> {
    Line::from(vec![
        Span::raw("  "),
        button("Yes", selected),
        Span::raw("  "),
        button("No", !selected),
    ])
}

fn button(label: &'static str, selected: bool) -> Span<'static> {
    if selected {
        Span::styled(
            format!(" {label} "),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(
            format!(" {label} "),
            Style::default().fg(Color::Gray).bg(Color::DarkGray),
        )
    }
}

fn choice_item_height(item: &str) -> usize {
    item.lines().count().max(1)
}

fn choice_lines(item: &str, selected: bool) -> Vec<Line<'_>> {
    let item_lines = if item.is_empty() {
        vec![""]
    } else {
        item.lines().collect::<Vec<_>>()
    };
    item_lines
        .into_iter()
        .enumerate()
        .map(|(line_index, line)| {
            let marker = if line_index == 0 && selected {
                "▸ "
            } else {
                "  "
            };
            let text_style = if selected && line_index == 0 {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else if selected {
                Style::default().fg(Color::Green)
            } else if line_index == 0 {
                Style::default().fg(Color::Gray)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Line::from(vec![
                Span::styled(
                    marker,
                    Style::default().fg(if selected {
                        Color::Green
                    } else {
                        Color::DarkGray
                    }),
                ),
                Span::styled(line, text_style),
            ])
        })
        .collect()
}

fn char_to_byte(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(index, _)| index)
        .unwrap_or(text.len())
}

fn strip_ansi(text: &str) -> String {
    let mut stripped = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\x1b' {
            stripped.push(ch);
            continue;
        }
        if chars.next_if_eq(&'[').is_some() {
            for ch in chars.by_ref() {
                if ('@'..='~').contains(&ch) {
                    break;
                }
            }
        }
    }
    stripped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editor_history_recalls_last_prompt_and_restores_draft() {
        let mut state = EditorState::new(
            "Prompt".to_owned(),
            Vec::new(),
            None,
            Vec::new(),
            vec!["first".to_owned(), "second".to_owned()],
        );
        state.lines = vec!["draft".to_owned()];
        state.cursor_col = 5;

        state.history_previous();
        assert_eq!(state.text(), "second");
        state.history_previous();
        assert_eq!(state.text(), "first");
        state.history_next();
        assert_eq!(state.text(), "second");
        state.history_next();
        assert_eq!(state.text(), "draft");
    }

    #[test]
    fn cancellation_errors_are_detectable_through_context() {
        let error = anyhow::Error::from(UserCancelled::new("account selection cancelled"))
            .context("provider setup");

        assert!(is_cancelled(&error));
        assert_eq!(
            cancellation_message(&error).as_deref(),
            Some("account selection cancelled")
        );
    }
}
