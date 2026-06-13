use super::*;

pub(super) fn attach(args: AttachArgs) -> Result<()> {
    let project = current_project_awake()?;
    run_attach_viewer(&project, selection::resolve_attach(&project, args.target)?)
}

#[derive(Clone)]
pub(super) enum AttachScope {
    Project,
    Role(RoleSlug),
    Agent(ResolvedAgent),
}

impl From<AttachTarget> for AttachScope {
    fn from(value: AttachTarget) -> Self {
        match value {
            AttachTarget::Project => Self::Project,
            AttachTarget::Role(role) => Self::Role(role),
            AttachTarget::Agent(agent) => Self::Agent(agent),
        }
    }
}

pub(super) struct AttachViewerApp {
    project: ProjectPaths,
    scope: AttachScope,
    agent: Option<AttachViewerAgent>,
    scroll: usize,
    follow: bool,
    last_transcript_rows: usize,
    collapse_thinking: bool,
    composer: Option<AttachComposer>,
    search: AttachSearch,
    switcher: Option<AttachSwitcher>,
    message: Option<String>,
}

#[derive(Clone)]
pub(super) struct AttachViewerAgent {
    resolved: ResolvedAgent,
    status: AgentStatus,
    summary: String,
    detail: String,
    steer: SteerStatus,
    supervisor_status: SupervisorStatus,
    next_retry_at: Option<u64>,
    latest_output_at: Option<u64>,
    child_pid: Option<u32>,
    run_count: u64,
    updated_at: u64,
}

pub(super) struct AttachStatusRail {
    subject: String,
    status: String,
    status_style: Style,
    steer: String,
    steer_style: Style,
    output: String,
    quota: String,
    quota_style: Style,
    directory: String,
}

pub(super) struct AttachComposer {
    buffer: crate::input::buffer::TextBuffer,
}

#[derive(Default)]
pub(super) struct AttachSearch {
    active: bool,
    query: String,
}

pub(super) struct AttachSwitcher {
    query: String,
    selected: usize,
}

pub(super) enum AttachViewerEvent {
    Continue,
    FollowUp(ResolvedAgent, String),
    Quit,
}

pub(super) struct CommandConversationApp {
    command_name: String,
    cwd: PathBuf,
    root: PathBuf,
    turn: u64,
    turn_root: PathBuf,
    scroll: usize,
    follow: bool,
    last_transcript_rows: usize,
    collapse_thinking: bool,
    composer: Option<AttachComposer>,
    search: AttachSearch,
    steer_dir: Option<PathBuf>,
    message: Option<String>,
    exit: Option<AppServerTurnExit>,
    error: Option<String>,
    follow_up_history: Vec<String>,
}

pub(super) enum CommandConversationOutcome {
    Finish,
    Detach,
    FollowUp(String),
}

trait TranscriptController {
    fn scroll(&self) -> usize;
    fn set_scroll(&mut self, scroll: usize);
    fn follow(&self) -> bool;
    fn set_follow(&mut self, follow: bool);
    fn last_transcript_rows(&self) -> usize;
    fn collapse_thinking(&self) -> bool;
    fn set_collapse_thinking(&mut self, collapse: bool);
    fn search(&self) -> &AttachSearch;
    fn search_mut(&mut self) -> &mut AttachSearch;
    fn message_mut(&mut self) -> &mut Option<String>;
    fn load_transcript_document(&self) -> Result<AttachDocument>;

    fn handle_transcript_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        document: &AttachDocument,
    ) -> Result<bool> {
        match key.code {
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(document, -1);
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.jump_reply(document, 1);
            }
            KeyCode::Up | KeyCode::Char('k') => self.scroll_up(ATTACH_SCROLL_STEP),
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_down(ATTACH_SCROLL_STEP, document.lines.len());
            }
            KeyCode::PageUp => self.scroll_up(ATTACH_PAGE_STEP),
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.scroll_down(ATTACH_PAGE_STEP, document.lines.len());
            }
            KeyCode::Home => {
                self.set_scroll(0);
                self.set_follow(false);
            }
            KeyCode::End => self.scroll_to_bottom(document.lines.len()),
            KeyCode::Char('F') => self.toggle_follow(document.lines.len()),
            KeyCode::Char('t') => self.toggle_thinking(document)?,
            KeyCode::Char('/') => {
                self.search_mut().active = true;
                self.clear_message();
            }
            KeyCode::Char('n') => self.navigate_search(document, 1),
            KeyCode::Char('N') => self.navigate_search(document, -1),
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn handle_search_key(&mut self, key: crossterm::event::KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => self.search_mut().active = false,
            KeyCode::Enter => {
                let document = self.load_transcript_document()?;
                self.search_mut().active = false;
                self.navigate_search(&document, 1);
            }
            KeyCode::Backspace => {
                self.search_mut().query.pop();
                self.scroll_to_first_search_match()?;
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.search_mut().query.clear();
            }
            KeyCode::Char(ch) => {
                self.search_mut().query.push(ch);
                self.scroll_to_first_search_match()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn scroll_to_first_search_match(&mut self) -> Result<()> {
        if self.search().query.trim().is_empty() {
            return Ok(());
        }
        let document = self.load_transcript_document()?;
        let matches = attach_search_matches(&document, &self.search().query);
        if let Some(offset) = matches.first().copied() {
            self.set_scroll(offset);
            self.clamp_manual_scroll(document.lines.len());
        }
        Ok(())
    }

    fn navigate_search(&mut self, document: &AttachDocument, direction: i8) {
        let query = self.search().query.clone();
        if query.trim().is_empty() {
            self.set_message("no transcript search query");
            return;
        }
        let matches = attach_search_matches(document, &query);
        if matches.is_empty() {
            self.set_message(format!("no matches for `{query}`"));
            return;
        }
        let scroll = self.scroll();
        let next_scroll = if direction < 0 {
            matches
                .iter()
                .rev()
                .copied()
                .find(|offset| *offset < scroll)
                .unwrap_or_else(|| *matches.last().expect("matches is nonempty"))
        } else {
            matches
                .iter()
                .copied()
                .find(|offset| *offset > scroll)
                .unwrap_or(matches[0])
        };
        self.set_scroll(next_scroll);
        self.clamp_manual_scroll(document.lines.len());
        self.set_message(format!(
            "match {}/{} for `{query}`",
            attach_match_position(&matches, self.scroll()),
            matches.len()
        ));
    }

    fn toggle_thinking(&mut self, document: &AttachDocument) -> Result<()> {
        let was_following = self.follow();
        let marker_index = current_attach_marker_index(&document.markers, self.scroll());
        let marker_offset = marker_index
            .and_then(|index| document.markers.get(index))
            .map(|marker| marker.offset)
            .unwrap_or(0);
        let marker_delta = self.scroll().saturating_sub(marker_offset);
        self.set_collapse_thinking(!self.collapse_thinking());
        let next_document = self.load_transcript_document()?;
        if was_following {
            self.scroll_to_bottom(next_document.lines.len());
        } else {
            let next_scroll = marker_index
                .and_then(|index| next_document.markers.get(index))
                .map(|marker| marker.offset.saturating_add(marker_delta))
                .unwrap_or_else(|| self.scroll());
            self.set_scroll(next_scroll);
            self.clamp_manual_scroll(next_document.lines.len());
        }
        Ok(())
    }

    fn jump_reply(&mut self, document: &AttachDocument, direction: i8) {
        if document.markers.is_empty() {
            return;
        }
        let current = current_attach_marker_index(&document.markers, self.scroll()).unwrap_or(0);
        let next = if direction < 0 {
            current.saturating_sub(1)
        } else {
            (current + 1).min(document.markers.len() - 1)
        };
        self.set_scroll(document.markers[next].offset);
        self.clamp_manual_scroll(document.lines.len());
    }

    fn scroll_up(&mut self, amount: usize) {
        self.set_scroll(self.scroll().saturating_sub(amount));
        self.set_follow(false);
        self.clear_message();
    }

    fn scroll_down(&mut self, amount: usize, line_count: usize) {
        self.set_scroll(self.scroll().saturating_add(amount));
        self.set_follow(false);
        self.clamp_manual_scroll(line_count);
        self.clear_message();
    }

    fn toggle_follow(&mut self, line_count: usize) {
        if self.follow() {
            self.set_follow(false);
            self.set_message("follow disabled");
        } else {
            self.scroll_to_bottom(line_count);
            self.set_message("follow enabled");
        }
    }

    fn scroll_to_bottom(&mut self, line_count: usize) {
        self.set_follow(true);
        self.set_scroll(self.max_transcript_scroll(line_count));
    }

    fn sync_transcript_scroll(&mut self, line_count: usize) {
        if self.follow() {
            self.set_scroll(self.max_transcript_scroll(line_count));
        } else {
            self.clamp_manual_scroll(line_count);
        }
    }

    fn clamp_manual_scroll(&mut self, line_count: usize) {
        let max_scroll = self.max_transcript_scroll(line_count);
        self.set_scroll(self.scroll().min(max_scroll));
        self.set_follow(self.scroll() == max_scroll);
    }

    fn max_transcript_scroll(&self, line_count: usize) -> usize {
        max_scroll_offset(line_count, self.last_transcript_rows())
    }

    fn set_message(&mut self, message: impl Into<String>) {
        *self.message_mut() = Some(message.into());
    }

    fn clear_message(&mut self) {
        *self.message_mut() = None;
    }
}

impl TranscriptController for CommandConversationApp {
    fn scroll(&self) -> usize {
        self.scroll
    }

    fn set_scroll(&mut self, scroll: usize) {
        self.scroll = scroll;
    }

    fn follow(&self) -> bool {
        self.follow
    }

    fn set_follow(&mut self, follow: bool) {
        self.follow = follow;
    }

    fn last_transcript_rows(&self) -> usize {
        self.last_transcript_rows
    }

    fn collapse_thinking(&self) -> bool {
        self.collapse_thinking
    }

    fn set_collapse_thinking(&mut self, collapse: bool) {
        self.collapse_thinking = collapse;
    }

    fn search(&self) -> &AttachSearch {
        &self.search
    }

    fn search_mut(&mut self) -> &mut AttachSearch {
        &mut self.search
    }

    fn message_mut(&mut self) -> &mut Option<String> {
        &mut self.message
    }

    fn load_transcript_document(&self) -> Result<AttachDocument> {
        self.document()
    }
}

impl TranscriptController for AttachViewerApp {
    fn scroll(&self) -> usize {
        self.scroll
    }

    fn set_scroll(&mut self, scroll: usize) {
        self.scroll = scroll;
    }

    fn follow(&self) -> bool {
        self.follow
    }

    fn set_follow(&mut self, follow: bool) {
        self.follow = follow;
    }

    fn last_transcript_rows(&self) -> usize {
        self.last_transcript_rows
    }

    fn collapse_thinking(&self) -> bool {
        self.collapse_thinking
    }

    fn set_collapse_thinking(&mut self, collapse: bool) {
        self.collapse_thinking = collapse;
    }

    fn search(&self) -> &AttachSearch {
        &self.search
    }

    fn search_mut(&mut self) -> &mut AttachSearch {
        &mut self.search
    }

    fn message_mut(&mut self) -> &mut Option<String> {
        &mut self.message
    }

    fn load_transcript_document(&self) -> Result<AttachDocument> {
        self.document()
    }
}

pub(super) fn run_attach_viewer(project: &ProjectPaths, target: AttachTarget) -> Result<()> {
    let mut app = AttachViewerApp {
        project: project.clone(),
        scope: target.into(),
        agent: None,
        scroll: 0,
        follow: true,
        last_transcript_rows: DASHBOARD_MIN_VISIBLE_ROWS,
        collapse_thinking: true,
        composer: None,
        search: AttachSearch::default(),
        switcher: None,
        message: None,
    };
    let mut terminal = Some(TerminalSession::enter()?);
    loop {
        app.refresh()?;
        terminal
            .as_mut()
            .expect("attach terminal is active")
            .draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
        {
            match app.handle_key(key)? {
                AttachViewerEvent::Continue => {}
                AttachViewerEvent::Quit => return Ok(()),
                AttachViewerEvent::FollowUp(agent, query) => {
                    drop(terminal.take());
                    more_agent(MoreArgs {
                        agent: Some(AgentSpec {
                            role: Some(agent.role),
                            agent: agent.agent,
                        }),
                        query: Some(query),
                        new: false,
                    })?;
                    terminal = Some(TerminalSession::enter()?);
                    app.message = Some("follow-up completed".to_owned());
                    app.scroll_to_latest_reply()?;
                }
            }
        }
    }
}

pub(super) fn run_command_conversation_tui(
    cwd: &Path,
    command_name: &str,
    initial_prompt: String,
    policy: AppServerPolicy,
) -> Result<()> {
    let root = command_run_root(cwd, command_name)?;
    io::ensure_dir(&root)?;
    let mut terminal = TerminalSession::enter()?;
    let mut turn = 1;
    let mut prompt = initial_prompt;
    let mut follow_up_history = Vec::new();
    loop {
        let turn_root = root.join(turn.to_string());
        io::ensure_dir(&turn_root)?;
        let prompt_path = turn_root.join("PROMPT.md");
        let reply_path = turn_root.join("REPLY.md");
        let steer_dir = turn_root.join("steer");
        io::write_text(&prompt_path, &prompt)?;
        let run_cwd = cwd.to_owned();
        let command_root = root.clone();
        let runner_root = turn_root.clone();
        let runner_steer_dir = steer_dir.clone();
        let handle = thread::spawn(move || {
            run_app_server_file_turn(FileAppServerTurn {
                cwd: &run_cwd,
                command_root: &command_root,
                turn_root: &runner_root,
                prompt_path: &prompt_path,
                reply_path: &reply_path,
                steer_dir: Some(&runner_steer_dir),
                policy,
            })
        });
        let mut app = CommandConversationApp::new(
            command_name,
            cwd.to_owned(),
            root.clone(),
            turn,
            turn_root.clone(),
            Some(steer_dir),
            follow_up_history.clone(),
        );
        let outcome = run_command_conversation_turn(&mut terminal, &mut app, handle)?;
        match outcome {
            CommandConversationOutcome::Finish | CommandConversationOutcome::Detach => {
                return Ok(());
            }
            CommandConversationOutcome::FollowUp(follow_up) => {
                follow_up_history.push(follow_up.clone());
                turn += 1;
                prompt = format!(
                    "# think {command_name} follow-up\n\nContinue the same `think {command_name}` session. The user replied:\n\n{follow_up}\n"
                );
            }
        }
    }
}

pub(super) fn run_command_conversation_turn(
    terminal: &mut TerminalSession,
    app: &mut CommandConversationApp,
    handle: thread::JoinHandle<Result<AppServerTurnExit>>,
) -> Result<CommandConversationOutcome> {
    loop {
        if handle.is_finished() {
            match handle
                .join()
                .unwrap_or_else(|_| Err(anyhow!("app-server runner thread panicked")))
            {
                Ok(exit) => app.set_exit(exit),
                Err(err) => app.set_error(err.to_string()),
            }
            break;
        }
        terminal.draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(outcome) = app.handle_key(key)?
        {
            return Ok(outcome);
        }
    }
    loop {
        terminal.draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_REFRESH_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(outcome) = app.handle_key(key)?
        {
            return match outcome {
                CommandConversationOutcome::FollowUp(_) if app.turn_failed() => {
                    app.message =
                        Some("turn failed; quit after reviewing the transcript".to_owned());
                    Ok(CommandConversationOutcome::Finish)
                }
                outcome => Ok(outcome),
            };
        }
    }
}

impl CommandConversationApp {
    fn new(
        command_name: &str,
        cwd: PathBuf,
        root: PathBuf,
        turn: u64,
        turn_root: PathBuf,
        steer_dir: Option<PathBuf>,
        follow_up_history: Vec<String>,
    ) -> Self {
        Self {
            command_name: command_name.to_owned(),
            cwd,
            root,
            turn,
            turn_root,
            scroll: 0,
            follow: true,
            last_transcript_rows: DASHBOARD_MIN_VISIBLE_ROWS,
            collapse_thinking: true,
            composer: None,
            search: AttachSearch::default(),
            steer_dir,
            message: None,
            exit: None,
            error: None,
            follow_up_history,
        }
    }

    fn set_exit(&mut self, exit: AppServerTurnExit) {
        self.message = Some(if exit.success {
            "turn completed; press r to reply or q to finish".to_owned()
        } else {
            format!(
                "app-server exited with status {}; press q after reviewing",
                exit.code
            )
        });
        self.exit = Some(exit);
    }

    fn set_error(&mut self, error: String) {
        self.message = Some(format!("app-server runner failed: {error}"));
        self.error = Some(error);
    }

    fn turn_running(&self) -> bool {
        self.exit.is_none() && self.error.is_none()
    }

    fn turn_failed(&self) -> bool {
        self.error.is_some() || self.exit.as_ref().is_some_and(|exit| !exit.success)
    }

    fn handle_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<CommandConversationOutcome>> {
        if self.composer.is_some() {
            return self.handle_composer_key(key);
        }
        if self.search.active {
            self.handle_search_key(key)?;
            return Ok(None);
        }
        let document = self.document()?;
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if self.turn_running() {
                    Ok(Some(CommandConversationOutcome::Detach))
                } else if self.turn_failed() {
                    self.raise_turn_failure()
                } else {
                    Ok(Some(CommandConversationOutcome::Finish))
                }
            }
            KeyCode::Char('o') => {
                open_path(&self.turn_root)?;
                self.message = Some("opened turn directory".to_owned());
                Ok(None)
            }
            KeyCode::Char('y') => {
                copy_to_clipboard(&self.turn_root.join("TRANSCRIPT.txt").display().to_string())?;
                self.message = Some("copied transcript path".to_owned());
                Ok(None)
            }
            KeyCode::Char('Y') => {
                copy_to_clipboard(&self.turn_root.join("REPLY.md").display().to_string())?;
                self.message = Some("copied reply path".to_owned());
                Ok(None)
            }
            KeyCode::Char('r') | KeyCode::Char('m') => {
                if self.turn_running() {
                    self.open_composer();
                } else if self.turn_failed() {
                    self.message =
                        Some("turn failed; quit after reviewing the transcript".to_owned());
                } else {
                    self.open_composer();
                }
                Ok(None)
            }
            _ if self.handle_transcript_key(key, &document)? => Ok(None),
            _ => Ok(None),
        }
    }

    fn handle_composer_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<CommandConversationOutcome>> {
        match key.code {
            KeyCode::Esc => {
                self.composer = None;
                self.message = Some("reply cancelled".to_owned());
                Ok(None)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let Some(composer) = self.composer.take() else {
                    return Ok(None);
                };
                let query = composer.text();
                if query.trim().is_empty() {
                    self.message = Some("blank reply cancelled".to_owned());
                    return Ok(None);
                }
                if self.turn_running() {
                    let Some(steer_dir) = &self.steer_dir else {
                        self.message = Some("this turn cannot accept live steering".to_owned());
                        return Ok(None);
                    };
                    crate::backend::enqueue_steer(steer_dir, &query)?;
                    self.follow_up_history.push(query);
                    self.message = Some("sent live steer to running turn".to_owned());
                    return Ok(None);
                }
                Ok(Some(CommandConversationOutcome::FollowUp(query)))
            }
            _ => {
                if let Some(composer) = &mut self.composer {
                    composer.apply_edit_key(key);
                }
                Ok(None)
            }
        }
    }

    fn raise_turn_failure(&self) -> Result<Option<CommandConversationOutcome>> {
        if let Some(error) = &self.error {
            bail!(
                "app-server failed while running `think {}`: {error}",
                self.command_name
            );
        }
        if let Some(exit) = &self.exit
            && !exit.success
        {
            bail!(
                "app-server for `think {}` exited with status code {}",
                self.command_name,
                exit.code
            );
        }
        Ok(Some(CommandConversationOutcome::Finish))
    }

    fn open_composer(&mut self) {
        self.composer = Some(AttachComposer::new(self.follow_up_history.clone()));
        self.message = None;
    }

    fn document(&self) -> Result<AttachDocument> {
        let mut document = AttachDocument {
            lines: vec![
                Line::from(vec![
                    Span::styled("command ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("think {}", self.command_name),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(" · turn ", Style::default().fg(Color::DarkGray)),
                    Span::styled(self.turn.to_string(), Style::default().fg(Color::White)),
                    Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                    Span::styled(self.turn_status_label(), self.turn_status_style()),
                ]),
                Line::from(vec![
                    Span::styled("root ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        self.root.display().to_string(),
                        Style::default().fg(Color::Gray),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("files ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        self.turn_root.display().to_string(),
                        Style::default().fg(Color::Gray),
                    ),
                ]),
            ],
            markers: Vec::new(),
        };
        if let Some(steer_dir) = &self.steer_dir
            && let Some(line) = steer_status_line(
                &crate::backend::steer_status(steer_dir)?,
                self.turn_running(),
            )
        {
            document.lines.push(line);
        }
        if self.command_name == "project-setup" {
            document.lines.push(Line::from(""));
            document.lines.push(section_line("setup checkpoints"));
            for checkpoint in project_setup_checkpoints(&self.cwd, &self.turn_root)? {
                document.lines.push(Line::from(vec![
                    Span::styled(
                        if checkpoint.complete {
                            "done "
                        } else {
                            "todo "
                        },
                        if checkpoint.complete {
                            Style::default().fg(Color::Green)
                        } else {
                            Style::default().fg(Color::Yellow)
                        },
                    ),
                    Span::styled(
                        format!("{:<8}", checkpoint.label),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(checkpoint.detail, Style::default().fg(Color::DarkGray)),
                ]));
            }
        }
        document.lines.push(Line::from(""));
        if let Some(reply) = io::read_optional_text(&self.turn_root.join("REPLY.md"))?
            && !reply.trim().is_empty()
        {
            document.markers.push(AttachReplyMarker {
                offset: document.lines.len(),
                label: format!("turn {} reply", self.turn),
            });
            document.lines.push(section_line("latest reply"));
            document.lines.extend(
                reply
                    .lines()
                    .map(|line| attach_markdown_line(line, Color::White)),
            );
            document.lines.push(Line::from(""));
        }
        let transcript =
            io::read_optional_text(&self.turn_root.join("TRANSCRIPT.txt"))?.unwrap_or_default();
        document.lines.push(section_line("live transcript"));
        if transcript.trim().is_empty() {
            document.lines.push(Line::from(Span::styled(
                if self.turn_running() {
                    "waiting for app-server output..."
                } else {
                    "app-server produced no transcript output."
                },
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for block in crate::transcript::parse(&transcript) {
                push_attach_transcript_block(
                    &mut document,
                    self.turn,
                    block,
                    self.collapse_thinking,
                );
            }
        }
        Ok(document)
    }

    fn turn_status_label(&self) -> String {
        if let Some(error) = &self.error {
            return format!("failed: {error}");
        }
        match &self.exit {
            None => "running".to_owned(),
            Some(exit) if exit.success => "done".to_owned(),
            Some(exit) => format!("failed {}", exit.code),
        }
    }

    fn turn_status_style(&self) -> Style {
        if self.turn_running() {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else if self.turn_failed() {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        }
    }

    fn status_rail(&self) -> AttachStatusRail {
        let steer = self
            .steer_dir
            .as_ref()
            .and_then(|dir| crate::backend::steer_status(dir).ok())
            .map(|status| attach_steer_status(&status, self.turn_running()))
            .unwrap_or_else(|| ("-".to_owned(), Style::default().fg(Color::DarkGray)));
        let output = file_modified_unix(&self.turn_root.join("TRANSCRIPT.txt"))
            .map(event_age)
            .unwrap_or_else(|| "-".to_owned());
        let mut status = self.turn_status_label();
        if let Some(progress) = self.setup_progress_summary() {
            status = format!("{status} · {progress}");
        }
        AttachStatusRail {
            subject: format!("think {} turn {}", self.command_name, self.turn),
            status,
            status_style: self.turn_status_style(),
            steer: steer.0,
            steer_style: steer.1,
            output,
            quota: "-".to_owned(),
            quota_style: Style::default().fg(Color::DarkGray),
            directory: self.turn_root.display().to_string(),
        }
    }

    fn setup_progress_summary(&self) -> Option<String> {
        (self.command_name == "project-setup")
            .then(|| project_setup_checkpoints(&self.cwd, &self.turn_root))
            .and_then(Result::ok)
            .map(|checkpoints| {
                let complete = checkpoints
                    .iter()
                    .filter(|checkpoint| checkpoint.complete)
                    .count();
                let active = checkpoints
                    .iter()
                    .find(|checkpoint| !checkpoint.complete)
                    .map(|checkpoint| checkpoint.label)
                    .unwrap_or("done");
                format!("setup {complete}/{} {active}", checkpoints.len())
            })
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let bottom_height = if self.composer.is_some() {
            ATTACH_COMPOSER_HEIGHT
        } else if self.search.active {
            DASHBOARD_SEARCH_HEIGHT
        } else {
            0
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(bottom_height),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let main = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(chunks[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(ATTACH_REPLY_RAIL_WIDTH),
                Constraint::Min(1),
            ])
            .split(main[1]);
        let document = self.document().unwrap_or_else(|err| AttachDocument {
            lines: vec![Line::from(format!("Failed to load transcript: {err:#}"))],
            markers: Vec::new(),
        });
        draw_attach_status_rail(frame, main[0], self.status_rail());
        self.last_transcript_rows = visible_panel_rows(body[1]);
        self.sync_transcript_scroll(document.lines.len());
        draw_attach_reply_rail(frame, body[0], &document, self.scroll);
        draw_attach_transcript_panel(
            frame,
            body[1],
            attach_transcript_title(
                format!(
                    "Attached Turn · think {} · turn {} · follow {} · thinking {}",
                    self.command_name,
                    self.turn,
                    if self.follow { "on" } else { "off" },
                    if self.collapse_thinking {
                        "collapsed"
                    } else {
                        "shown"
                    }
                ),
                &document,
                self.scroll,
            ),
            &document,
            &self.search,
            self.scroll,
            self.message.as_deref(),
        );
        if let Some(composer) = &self.composer {
            draw_attach_composer(
                frame,
                chunks[1],
                if self.turn_running() {
                    "Live Steer"
                } else {
                    "Follow-Up"
                },
                composer,
            );
        } else if self.search.active {
            draw_attach_search_bar(frame, chunks[1], &self.search, self.scroll, &document);
        }
        let footer_pairs = if self.composer.is_some() && self.turn_running() {
            COMMAND_CONVERSATION_FOOTER_STEER_COMPOSER
        } else if self.composer.is_some() {
            ATTACH_FOOTER_COMPOSER
        } else if self.search.active {
            ATTACH_FOOTER_SEARCH
        } else if self.turn_running() {
            COMMAND_CONVERSATION_FOOTER_RUNNING
        } else if self.follow {
            COMMAND_CONVERSATION_FOOTER_DONE_FOLLOW_ON
        } else {
            COMMAND_CONVERSATION_FOOTER_DONE_FOLLOW_OFF
        };
        draw_dashboard_footer(
            frame,
            chunks[2],
            footer_line_from_pairs(footer_pairs, usize::from(chunks[2].width)),
        );
    }
}

impl AttachViewerApp {
    fn refresh(&mut self) -> Result<()> {
        let current = self.agent.as_ref().map(|agent| agent.resolved.label());
        let agents = load_attach_viewer_agents(&self.project, &self.scope)?;
        self.agent = current
            .and_then(|label| {
                agents
                    .iter()
                    .find(|agent| agent.resolved.label() == label)
                    .cloned()
            })
            .or_else(|| agents.into_iter().next());
        let document = self.document()?;
        self.sync_transcript_scroll(document.lines.len());
        Ok(())
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Result<AttachViewerEvent> {
        if self.switcher.is_some() {
            return self.handle_switcher_key(key);
        }
        if self.composer.is_some() {
            return self.handle_composer_key(key);
        }
        if self.search.active {
            self.handle_search_key(key)?;
            return Ok(AttachViewerEvent::Continue);
        }
        let document = self.document()?;
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(AttachViewerEvent::Quit),
            KeyCode::Char('m') | KeyCode::Char('f') => {
                self.open_composer()?;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('o') => {
                if let Some(path) = self.selected_run_root() {
                    open_path(&path)?;
                    self.message = Some("opened run directory".to_owned());
                } else {
                    self.message = Some("no agent selected".to_owned());
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('y') => {
                if let Some(path) = self.selected_transcript_path() {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.message = Some("copied transcript path".to_owned());
                } else {
                    self.message = Some("no transcript path available".to_owned());
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('Y') => {
                if let Some(path) = self.selected_reply_path() {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.message = Some("copied reply path".to_owned());
                } else {
                    self.message = Some("no reply path available".to_owned());
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('s') => {
                self.switcher = Some(AttachSwitcher {
                    query: String::new(),
                    selected: 0,
                });
                Ok(AttachViewerEvent::Continue)
            }
            _ if self.handle_transcript_key(key, &document)? => Ok(AttachViewerEvent::Continue),
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn handle_composer_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<AttachViewerEvent> {
        match key.code {
            KeyCode::Esc => {
                self.composer = None;
                self.message = Some("follow-up cancelled".to_owned());
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                let Some(agent) = self.agent.clone() else {
                    self.composer = None;
                    self.message = Some("no agent selected".to_owned());
                    return Ok(AttachViewerEvent::Continue);
                };
                let Some(composer) = self.composer.take() else {
                    return Ok(AttachViewerEvent::Continue);
                };
                let query = composer.text();
                if query.trim().is_empty() {
                    self.message = Some("blank follow-up cancelled".to_owned());
                    return Ok(AttachViewerEvent::Continue);
                }
                crate::input::history::append(&self.project, "followups", &query)?;
                if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
                    let agent_paths =
                        RolePaths::new(self.project.clone(), agent.resolved.role.clone())
                            .agent(agent.resolved.agent.clone());
                    crate::backend::enqueue_steer(&agent_paths.steer_dir(), &query)?;
                    self.message = Some(format!("sent live steer to {}", agent.resolved.label()));
                    return Ok(AttachViewerEvent::Continue);
                }
                Ok(AttachViewerEvent::FollowUp(agent.resolved, query))
            }
            _ => {
                if let Some(composer) = &mut self.composer {
                    composer.apply_edit_key(key);
                }
                Ok(AttachViewerEvent::Continue)
            }
        }
    }

    fn handle_switcher_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<AttachViewerEvent> {
        match key.code {
            KeyCode::Esc => {
                self.switcher = None;
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Enter => {
                let agents = self.switcher_agents()?;
                let Some(switcher) = self.switcher.take() else {
                    return Ok(AttachViewerEvent::Continue);
                };
                if let Some(agent) =
                    agents.get(switcher.selected.min(agents.len().saturating_sub(1)))
                {
                    self.scope = AttachScope::Agent(agent.resolved.clone());
                    self.agent = Some(agent.clone());
                    self.follow = true;
                    let document = self.document()?;
                    self.scroll_to_bottom(document.lines.len());
                    self.message = Some(format!("attached to {}", agent.resolved.label()));
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Backspace => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.query.pop();
                    switcher.selected = 0;
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Up => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.selected = switcher.selected.saturating_sub(1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Down => {
                let agents = self.switcher_agents()?;
                if let Some(switcher) = &mut self.switcher
                    && !agents.is_empty()
                {
                    switcher.selected = (switcher.selected + 1).min(agents.len() - 1);
                }
                Ok(AttachViewerEvent::Continue)
            }
            KeyCode::Char(ch) => {
                if let Some(switcher) = &mut self.switcher {
                    switcher.query.push(ch);
                    switcher.selected = 0;
                }
                Ok(AttachViewerEvent::Continue)
            }
            _ => Ok(AttachViewerEvent::Continue),
        }
    }

    fn open_composer(&mut self) -> Result<()> {
        self.composer = Some(AttachComposer::new(crate::input::history::load(
            &self.project,
            "followups",
        )?));
        self.message = None;
        Ok(())
    }

    fn selected_run_id(&self) -> Option<u64> {
        let agent = self.agent.as_ref()?;
        Some(
            if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
                agent.run_count + 1
            } else {
                agent.run_count
            },
        )
    }

    fn selected_agent_paths(&self) -> Option<crate::state::AgentPaths> {
        let agent = self.agent.as_ref()?;
        Some(
            RolePaths::new(self.project.clone(), agent.resolved.role.clone())
                .agent(agent.resolved.agent.clone()),
        )
    }

    fn selected_run_root(&self) -> Option<PathBuf> {
        let agent_paths = self.selected_agent_paths()?;
        let run_id = self.selected_run_id()?;
        Some(if run_id == 0 {
            agent_paths.root()
        } else {
            agent_paths.run(run_id).root()
        })
    }

    fn selected_transcript_path(&self) -> Option<PathBuf> {
        let run_id = self.selected_run_id()?;
        if run_id == 0 {
            return None;
        }
        Some(self.selected_agent_paths()?.run(run_id).transcript_text())
    }

    fn selected_reply_path(&self) -> Option<PathBuf> {
        let run_id = self.selected_run_id()?;
        if run_id == 0 {
            return None;
        }
        Some(self.selected_agent_paths()?.run(run_id).reply())
    }

    fn scroll_to_latest_reply(&mut self) -> Result<()> {
        let document = self.document()?;
        if let Some(marker) = document.markers.last() {
            self.scroll = marker.offset;
            self.clamp_manual_scroll(document.lines.len());
        }
        Ok(())
    }

    fn document(&self) -> Result<AttachDocument> {
        match &self.agent {
            Some(agent) => attach_document(&self.project, agent, self.collapse_thinking),
            None => Ok(AttachDocument {
                lines: vec![Line::from(Span::styled(
                    "No agent matches this attach target.",
                    Style::default().fg(Color::DarkGray),
                ))],
                markers: Vec::new(),
            }),
        }
    }

    fn switcher_agents(&self) -> Result<Vec<AttachViewerAgent>> {
        let query = self
            .switcher
            .as_ref()
            .map(|switcher| switcher.query.trim().to_owned())
            .unwrap_or_default();
        let mut agents = load_attach_viewer_agents(&self.project, &AttachScope::Project)?;
        if !query.is_empty() {
            agents.retain(|agent| {
                text_matches_query(
                    [
                        agent.resolved.label(),
                        agent.status.to_string(),
                        agent.summary.clone(),
                        agent.detail.clone(),
                    ],
                    &query,
                )
            });
        }
        Ok(agents)
    }

    fn status_rail(&self) -> AttachStatusRail {
        let Some(agent) = &self.agent else {
            return AttachStatusRail {
                subject: "no agent".to_owned(),
                status: "-".to_owned(),
                status_style: Style::default().fg(Color::DarkGray),
                steer: "-".to_owned(),
                steer_style: Style::default().fg(Color::DarkGray),
                output: "-".to_owned(),
                quota: "-".to_owned(),
                quota_style: Style::default().fg(Color::DarkGray),
                directory: self.project.root.display().to_string(),
            };
        };
        let active = matches!(agent.status, AgentStatus::Starting | AgentStatus::Running);
        let (steer, steer_style) = attach_steer_status(&agent.steer, active);
        let (quota, quota_style) = attach_quota_status(agent);
        let status = agent
            .child_pid
            .map(|pid| format!("{} pid {pid}", agent.status))
            .unwrap_or_else(|| agent.status.to_string());
        AttachStatusRail {
            subject: agent.resolved.label(),
            status,
            status_style: dashboard_agent_style(agent.status, false),
            steer,
            steer_style,
            output: agent
                .latest_output_at
                .map(event_age)
                .unwrap_or_else(|| "-".to_owned()),
            quota,
            quota_style,
            directory: self
                .selected_run_root()
                .unwrap_or_else(|| self.project.root.clone())
                .display()
                .to_string(),
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let bottom_height = if self.composer.is_some() {
            ATTACH_COMPOSER_HEIGHT
        } else if self.search.active {
            DASHBOARD_SEARCH_HEIGHT
        } else {
            0
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(bottom_height),
                Constraint::Length(DASHBOARD_FOOTER_HEIGHT),
            ])
            .split(area);
        let main = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(chunks[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(ATTACH_REPLY_RAIL_WIDTH),
                Constraint::Min(1),
            ])
            .split(main[1]);
        let document = self.document().unwrap_or_else(|err| AttachDocument {
            lines: vec![Line::from(format!("Failed to load transcript: {err:#}"))],
            markers: Vec::new(),
        });
        draw_attach_status_rail(frame, main[0], self.status_rail());
        self.last_transcript_rows = visible_panel_rows(body[1]);
        self.sync_transcript_scroll(document.lines.len());
        draw_attach_reply_rail(frame, body[0], &document, self.scroll);
        draw_attach_transcript_panel(
            frame,
            body[1],
            attach_transcript_title(
                format!(
                    "Transcript · {} · follow {} · thinking {}",
                    self.agent
                        .as_ref()
                        .map(|agent| agent.resolved.label())
                        .unwrap_or_else(|| "no agent".to_owned()),
                    if self.follow { "on" } else { "off" },
                    if self.collapse_thinking {
                        "collapsed"
                    } else {
                        "shown"
                    }
                ),
                &document,
                self.scroll,
            ),
            &document,
            &self.search,
            self.scroll,
            self.message.as_deref(),
        );
        if let Some(composer) = &self.composer {
            draw_attach_composer(frame, chunks[1], "Follow-Up", composer);
        } else if self.search.active {
            draw_attach_search_bar(frame, chunks[1], &self.search, self.scroll, &document);
        }
        if self.switcher.is_some() {
            self.draw_switcher(frame, main[1]);
        }
        let footer_pairs = if self.composer.is_some() {
            ATTACH_FOOTER_COMPOSER
        } else if self.search.active {
            ATTACH_FOOTER_SEARCH
        } else if self.follow {
            ATTACH_FOOTER_FOLLOW_ON
        } else {
            ATTACH_FOOTER_FOLLOW_OFF
        };
        draw_dashboard_footer(
            frame,
            chunks[2],
            footer_line_from_pairs(footer_pairs, usize::from(chunks[2].width)),
        );
    }

    fn draw_switcher(&self, frame: &mut Frame<'_>, area: Rect) {
        let agents = self.switcher_agents().unwrap_or_default();
        let switcher = self.switcher.as_ref().expect("switcher is active");
        let popup = centered_popup(
            area,
            DASHBOARD_PALETTE_MAX_WIDTH.min(area.width.saturating_sub(4)),
            DASHBOARD_PALETTE_MAX_HEIGHT.min(area.height.saturating_sub(4)),
        );
        let block =
            dashboard_block("Switch Agent").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        let mut lines = vec![Line::from(vec![
            Span::styled("query ", Style::default().fg(Color::DarkGray)),
            Span::styled(switcher.query.clone(), Style::default().fg(Color::White)),
        ])];
        lines.push(Line::from(""));
        lines.extend(
            agents
                .iter()
                .enumerate()
                .take(usize::from(inner.height.saturating_sub(2)))
                .map(|(index, agent)| {
                    let selected = index == switcher.selected;
                    Line::from(vec![
                        dashboard_span(
                            if selected { "▸ " } else { "  " },
                            Style::default().fg(Color::White),
                            selected,
                        ),
                        dashboard_span(
                            agent.resolved.label(),
                            dashboard_agent_style(agent.status, false),
                            selected,
                        ),
                        dashboard_span(" · ", Style::default().fg(Color::DarkGray), selected),
                        dashboard_span(
                            agent.summary.clone(),
                            Style::default().fg(Color::Gray),
                            selected,
                        ),
                    ])
                }),
        );
        frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    }
}

fn attach_transcript_title(
    label: String,
    document: &AttachDocument,
    scroll: usize,
) -> Line<'static> {
    dynamic_panel_title(
        label,
        (!document.lines.is_empty())
            .then_some(((scroll + 1).min(document.lines.len()), document.lines.len())),
    )
}

fn draw_attach_reply_rail(
    frame: &mut Frame<'_>,
    area: Rect,
    document: &AttachDocument,
    scroll: usize,
) {
    let current = current_attach_marker_index(&document.markers, scroll);
    let height = usize::from(area.height);
    let start = current
        .map(|index| index.saturating_sub(height / 2))
        .unwrap_or(0)
        .min(document.markers.len().saturating_sub(height));
    let lines = document
        .markers
        .iter()
        .enumerate()
        .skip(start)
        .take(height)
        .map(|(index, marker)| {
            let selected = Some(index) == current;
            let assistant_marker = attach_marker_is_assistant(&marker.label);
            Line::from(Span::styled(
                if selected { "●" } else { "•" },
                Style::default()
                    .fg(if assistant_marker || selected {
                        Color::Green
                    } else {
                        Color::DarkGray
                    })
                    .add_modifier(if selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
            ))
            .alignment(ratatui::layout::Alignment::Center)
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), area);
}

fn draw_attach_transcript_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    title: Line<'static>,
    document: &AttachDocument,
    search: &AttachSearch,
    scroll: usize,
    message: Option<&str>,
) {
    let block = dashboard_block_with_title(title).border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let matches = attach_search_matches(document, &search.query);
    let mut visible = document
        .lines
        .iter()
        .enumerate()
        .skip(scroll)
        .map(|(index, line)| {
            let mut line = line.clone();
            if matches.binary_search(&index).is_ok() {
                let selected = index == scroll;
                line = line.patch_style(
                    Style::default()
                        .bg(if selected {
                            Color::Blue
                        } else {
                            Color::DarkGray
                        })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                );
            }
            line
        })
        .collect::<Vec<_>>();
    if let Some(message) = message {
        visible.insert(
            0,
            Line::from(vec![
                Span::styled("notice ", Style::default().fg(Color::Yellow)),
                Span::styled(message.to_owned(), Style::default().fg(Color::White)),
            ]),
        );
    }
    frame.render_widget(
        Paragraph::new(Text::from(crate::tui::wrap_lines(
            &visible,
            usize::from(inner.width),
        ))),
        inner,
    );
    render_scrollbar(frame, area, document.lines.len(), scroll);
}

fn draw_attach_composer(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &'static str,
    composer: &AttachComposer,
) {
    let block = dashboard_block(title).border_style(Style::default().fg(Color::Magenta));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(Text::from(
            composer.display_lines(usize::from(inner.width), usize::from(inner.height)),
        )),
        inner,
    );
}

fn draw_attach_search_bar(
    frame: &mut Frame<'_>,
    area: Rect,
    search: &AttachSearch,
    scroll: usize,
    document: &AttachDocument,
) {
    let matches = attach_search_matches(document, &search.query);
    let status = if search.query.trim().is_empty() {
        "type to search".to_owned()
    } else if matches.is_empty() {
        "no matches".to_owned()
    } else {
        format!(
            "{} matches · current {}",
            matches.len(),
            attach_match_position(&matches, scroll)
        )
    };
    let block =
        dashboard_block("Transcript Search").border_style(Style::default().fg(Color::Magenta));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("/", Style::default().fg(Color::Magenta)),
            Span::styled(search.query.clone(), Style::default().fg(Color::White)),
            Span::styled("  ", Style::default()),
            Span::styled(status, Style::default().fg(Color::DarkGray)),
        ])),
        inner,
    );
}

impl AttachComposer {
    pub(super) fn new(history: Vec<String>) -> Self {
        Self {
            buffer: crate::input::buffer::TextBuffer::new(history),
        }
    }

    pub(super) fn text(&self) -> String {
        self.buffer.text()
    }

    pub(super) fn display_lines(&self, width: usize, height: usize) -> Vec<Line<'static>> {
        let layout = crate::input::view::WrappedInput::new(&self.buffer, width)
            .style(Style::default().fg(Color::White))
            .cursor(crate::input::view::CursorRender::InlineMarker {
                marker: '▏',
                style: Style::default().fg(Color::Magenta),
            })
            .layout();
        layout.visible_lines(layout.scroll_for_cursor(0, height), height, Line::from(""))
    }

    pub(super) fn apply_edit_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        match key.code {
            KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.buffer.history_previous();
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.buffer.history_next();
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.buffer.clear();
            }
            KeyCode::Enter => self.buffer.insert_newline(),
            KeyCode::Backspace => self.buffer.backspace(),
            KeyCode::Left => self.buffer.move_left(),
            KeyCode::Right => self.buffer.move_right(),
            KeyCode::Up => self.buffer.move_vertical(-1),
            KeyCode::Down => self.buffer.move_vertical(1),
            KeyCode::Char(ch) => self.buffer.insert(ch),
            _ => return false,
        }
        true
    }
}

pub(super) fn draw_attach_status_rail(frame: &mut Frame<'_>, area: Rect, rail: AttachStatusRail) {
    if area.height == 0 {
        return;
    }
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " attached ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                rail.subject,
                Style::default()
                    .fg(Color::White)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                "status ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(rail.status, rail.status_style.bg(Color::Black)),
            Span::styled(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                "steer ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(rail.steer, rail.steer_style.bg(Color::Black)),
            Span::styled(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                "output ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                rail.output,
                Style::default().fg(Color::White).bg(Color::Black),
            ),
            Span::styled(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                "quota ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(rail.quota, rail.quota_style.bg(Color::Black)),
            Span::styled(
                ui::FIELD_SEPARATOR,
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                "dir ",
                Style::default().fg(Color::DarkGray).bg(Color::Black),
            ),
            Span::styled(
                rail.directory,
                Style::default().fg(Color::Gray).bg(Color::Black),
            ),
        ]))
        .style(Style::default().fg(Color::White).bg(Color::Black)),
        area,
    );
}

pub(super) fn attach_steer_status(status: &SteerStatus, active: bool) -> (String, Style) {
    if status.last_error.is_some() {
        return ("error".to_owned(), Style::default().fg(Color::Red));
    }
    if status.pending_count > 0 {
        return (
            format!("{} pending", status.pending_count),
            if active {
                Style::default().fg(Color::Magenta)
            } else {
                Style::default().fg(Color::Yellow)
            },
        );
    }
    if let Some(sent_at) = status.last_sent_at {
        return (event_age(sent_at), Style::default().fg(Color::Cyan));
    }
    ("-".to_owned(), Style::default().fg(Color::DarkGray))
}

pub(super) fn attach_quota_status(agent: &AttachViewerAgent) -> (String, Style) {
    match agent.supervisor_status {
        SupervisorStatus::WaitingForQuota | SupervisorStatus::WaitingForProvider => (
            agent
                .next_retry_at
                .map(|retry| format!("wait {}", format_unix_time_compact(retry)))
                .unwrap_or_else(|| "wait".to_owned()),
            Style::default().fg(Color::Yellow),
        ),
        _ => ("ok".to_owned(), Style::default().fg(Color::Green)),
    }
}

pub(super) struct SetupCheckpoint {
    label: &'static str,
    complete: bool,
    detail: String,
}

pub(super) fn project_setup_checkpoints(
    project: &Path,
    turn_root: &Path,
) -> Result<Vec<SetupCheckpoint>> {
    let started_at = file_modified_unix(&turn_root.join("PROMPT.md")).unwrap_or_default();
    let project_md = project.join("PROJECT.md");
    let config = project.join("think.toml");
    let roles_dir = project.join("roles");
    let reply = io::read_optional_text(&turn_root.join("REPLY.md"))?.unwrap_or_default();
    let role_config_updates = role_setup_update_count(&roles_dir, started_at)?;
    Ok(vec![
        SetupCheckpoint {
            label: "scaffold",
            complete: project_md.exists() && config.exists(),
            detail: "PROJECT.md and think.toml present".to_owned(),
        },
        SetupCheckpoint {
            label: "config",
            complete: file_modified_unix(&config).is_some_and(|modified| modified >= started_at),
            detail: "think.toml reviewed or updated".to_owned(),
        },
        SetupCheckpoint {
            label: "roles",
            complete: role_config_updates > 0,
            detail: format!("{role_config_updates} role files updated"),
        },
        SetupCheckpoint {
            label: "reply",
            complete: !reply.trim().is_empty(),
            detail: "setup summary written".to_owned(),
        },
    ])
}

pub(super) fn role_setup_update_count(roles_dir: &Path, started_at: u64) -> Result<usize> {
    if !roles_dir.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(roles_dir)
        .with_context(|| format!("Failed to read `{}`", roles_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for path in [
            entry.path().join("config.toml"),
            entry.path().join("ROLE.md"),
        ] {
            if file_modified_unix(&path).is_some_and(|modified| modified >= started_at) {
                count += 1;
            }
        }
    }
    Ok(count)
}

pub(super) fn load_attach_viewer_agents(
    project: &ProjectPaths,
    scope: &AttachScope,
) -> Result<Vec<AttachViewerAgent>> {
    let roles = match scope {
        AttachScope::Project => list_roles_by_display_order(project)?,
        AttachScope::Role(role) => vec![role.clone()],
        AttachScope::Agent(agent) => vec![agent.role.clone()],
    };
    let mut agents = Vec::new();
    for role in roles {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        for agent in list_agents(&role_paths)? {
            if let AttachScope::Agent(target) = scope
                && (target.role != role || target.agent != agent)
            {
                continue;
            }
            let agent_paths = role_paths.agent(agent.clone());
            let state = load_agent(&agent_paths)?;
            if state.archived {
                continue;
            }
            let (summary, detail) = status_agent_summary_and_detail(&config, &state, &agent_paths)?;
            let supervisor = load_supervisor_state(&agent_paths)?;
            let steer = crate::backend::steer_status(&agent_paths.steer_dir())?;
            let latest_output_at = latest_agent_output_at(&agent_paths, &state, &supervisor);
            agents.push(AttachViewerAgent {
                resolved: ResolvedAgent {
                    role: role.clone(),
                    agent,
                },
                status: state.status,
                summary,
                detail,
                steer,
                supervisor_status: supervisor.status,
                next_retry_at: supervisor.next_retry_at,
                latest_output_at,
                child_pid: supervisor.child_pid,
                run_count: state.run_count,
                updated_at: state.updated_at,
            });
        }
    }
    agents.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.resolved.label().cmp(&right.resolved.label()))
    });
    Ok(agents)
}

pub(super) fn attach_document(
    project: &ProjectPaths,
    agent: &AttachViewerAgent,
    collapse_thinking: bool,
) -> Result<AttachDocument> {
    let role_paths = RolePaths::new(project.clone(), agent.resolved.role.clone());
    let agent_paths = role_paths.agent(agent.resolved.agent.clone());
    let mut document = AttachDocument {
        lines: vec![
            Line::from(vec![
                Span::styled("agent ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    agent.resolved.label(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    agent.status.to_string(),
                    dashboard_agent_style(agent.status, false),
                ),
                Span::styled(" · ", Style::default().fg(Color::DarkGray)),
                Span::styled(agent.summary.clone(), Style::default().fg(Color::Gray)),
            ]),
            Line::from(vec![
                Span::styled("detail ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    empty_dash(&agent.detail).to_owned(),
                    Style::default().fg(Color::Gray),
                ),
                Span::styled(" · updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    event_age(agent.updated_at),
                    Style::default().fg(Color::Gray),
                ),
            ]),
        ],
        markers: Vec::new(),
    };
    if let Some(line) = steer_status_line(
        &agent.steer,
        matches!(agent.status, AgentStatus::Starting | AgentStatus::Running),
    ) {
        document.lines.push(line);
    }
    document.lines.push(Line::from(""));
    let mut any_run = false;
    for run_id in attach_run_ids(agent) {
        let run_paths = agent_paths.run(run_id);
        if !run_paths.root().exists() {
            continue;
        }
        any_run = true;
        push_attach_run(&mut document, &run_paths, run_id, agent, collapse_thinking)?;
    }
    if !any_run {
        document.lines.push(Line::from(Span::styled(
            "No run history has been recorded yet.",
            Style::default().fg(Color::DarkGray),
        )));
    }
    Ok(document)
}

pub(super) fn attach_run_ids(agent: &AttachViewerAgent) -> Vec<u64> {
    let end = if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
        agent.run_count + 1
    } else {
        agent.run_count
    };
    (1..=end).collect()
}

pub(super) fn push_attach_run(
    document: &mut AttachDocument,
    run_paths: &crate::state::RunPaths,
    run_id: u64,
    agent: &AttachViewerAgent,
    collapse_thinking: bool,
) -> Result<()> {
    let state = load_agent(&run_paths.agent)?;
    let exit = read_run_exit(run_paths, &state)?;
    let status = exit
        .as_ref()
        .map(|exit| {
            if exit.success {
                "success".to_owned()
            } else {
                format!("failed {}", exit.code)
            }
        })
        .unwrap_or_else(|| {
            if run_id == agent.run_count + 1 {
                "active".to_owned()
            } else {
                "unfinished".to_owned()
            }
        });
    document.markers.push(AttachReplyMarker {
        offset: document.lines.len(),
        label: format!("run {run_id}"),
    });
    document.lines.push(Line::from(vec![
        Span::styled("run ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            run_id.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" · ", Style::default().fg(Color::DarkGray)),
        Span::styled(status, Style::default().fg(Color::White)),
        Span::styled(
            exit.as_ref()
                .map(|exit| {
                    format!(
                        " · {} to {}",
                        format_unix_time(exit.started_at),
                        format_unix_time(exit.finished_at)
                    )
                })
                .unwrap_or_default(),
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    if let Some(prompt) = io::read_optional_text(&run_paths.prompt())?
        && let Some(summary) = attach_prompt_summary(&prompt)
    {
        document.lines.push(Line::from(vec![
            Span::styled("prompt ", Style::default().fg(Color::DarkGray)),
            Span::styled(summary, Style::default().fg(Color::Gray)),
        ]));
    }
    if let Some(reply) = io::read_optional_text(&run_paths.reply())?
        && !reply.trim().is_empty()
    {
        document.markers.push(AttachReplyMarker {
            offset: document.lines.len(),
            label: format!("run {run_id} reply"),
        });
        document.lines.push(section_line("reply"));
        document.lines.extend(
            reply
                .lines()
                .map(|line| attach_markdown_line(line, Color::White)),
        );
    }
    let transcript = io::read_optional_text(&run_paths.transcript_text())?.unwrap_or_default();
    if transcript.trim().is_empty() {
        document.lines.push(Line::from(Span::styled(
            "No transcript has been recorded for this run.",
            Style::default().fg(Color::DarkGray),
        )));
        document.lines.push(Line::from(""));
        return Ok(());
    }
    document.lines.push(section_line("transcript"));
    for block in crate::transcript::parse(&transcript) {
        push_attach_transcript_block(document, run_id, block, collapse_thinking);
    }
    document.lines.push(Line::from(""));
    Ok(())
}

pub(super) fn attach_prompt_summary(prompt: &str) -> Option<String> {
    extract_user_follow_up(prompt)
        .or_else(|| {
            let (_, prompt) = prompt.split_once("# Agent-specific prompt")?;
            Some(prompt.trim().to_owned())
        })
        .or_else(|| {
            prompt
                .lines()
                .find(|line| {
                    let line = line.trim();
                    !line.is_empty() && !line.starts_with('#')
                })
                .map(str::to_owned)
        })
        .map(|summary| compact_single_line(&summary, 180))
}

pub(super) fn compact_single_line(text: &str, limit: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        return compact;
    }
    let mut shortened = compact
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    shortened.push('…');
    shortened
}
