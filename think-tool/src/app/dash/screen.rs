use super::*;

use std::borrow::Cow;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectionMove {
    Home,
    End,
    Previous,
    Next,
    PagePrevious,
    PageNext,
}

impl SelectionMove {
    fn from_key(code: &KeyCode) -> Option<Self> {
        match code {
            KeyCode::Home => Some(Self::Home),
            KeyCode::End => Some(Self::End),
            KeyCode::Up | KeyCode::Char('k') => Some(Self::Previous),
            KeyCode::Down | KeyCode::Char('j') => Some(Self::Next),
            KeyCode::PageUp => Some(Self::PagePrevious),
            KeyCode::PageDown => Some(Self::PageNext),
            _ => None,
        }
    }

    fn apply_to_index(self, selected: &mut usize, count: usize, page_step: usize) {
        if count == 0 {
            *selected = 0;
            return;
        }
        match self {
            Self::Home => *selected = 0,
            Self::End => *selected = count - 1,
            Self::Previous => *selected = selected.saturating_sub(1),
            Self::Next => *selected = (*selected + 1).min(count - 1),
            Self::PagePrevious => *selected = selected.saturating_sub(page_step),
            Self::PageNext => *selected = (*selected + page_step).min(count - 1),
        }
    }

    fn apply_to_scrolled_index(
        self,
        selected: &mut usize,
        scroll: &mut u16,
        count: usize,
        page_step: usize,
    ) {
        match self {
            Self::Home => *scroll = 0,
            Self::PagePrevious => *scroll = scroll.saturating_sub(usize_to_u16(page_step)),
            Self::PageNext => *scroll = scroll.saturating_add(usize_to_u16(page_step)),
            Self::End | Self::Previous | Self::Next => {}
        }
        self.apply_to_index(selected, count, page_step);
    }

    fn should_center_after_move(self) -> bool {
        matches!(self, Self::End | Self::Previous | Self::Next)
    }
}

#[derive(Clone, Copy)]
enum ScrollMove {
    Previous,
    Next,
    PagePrevious,
    PageNext,
    Home,
}

impl ScrollMove {
    fn from_key(code: &KeyCode, home_enabled: bool) -> Option<Self> {
        match code {
            KeyCode::Up | KeyCode::Char('k') => Some(Self::Previous),
            KeyCode::Down | KeyCode::Char('j') => Some(Self::Next),
            KeyCode::PageUp => Some(Self::PagePrevious),
            KeyCode::PageDown => Some(Self::PageNext),
            KeyCode::Home if home_enabled => Some(Self::Home),
            _ => None,
        }
    }

    fn apply(self, scroll: &mut u16, line_step: u16, page_step: usize) {
        match self {
            Self::Previous => *scroll = scroll.saturating_sub(line_step),
            Self::Next => *scroll = scroll.saturating_add(line_step),
            Self::PagePrevious => *scroll = scroll.saturating_sub(usize_to_u16(page_step)),
            Self::PageNext => *scroll = scroll.saturating_add(usize_to_u16(page_step)),
            Self::Home => *scroll = 0,
        }
    }
}

#[derive(Clone, Copy)]
enum TextEdit {
    Clear,
    Backspace,
    Insert(char),
}

impl TextEdit {
    fn from_key(key: &crossterm::event::KeyEvent) -> Option<Self> {
        match key.code {
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Self::Clear)
            }
            KeyCode::Backspace => Some(Self::Backspace),
            KeyCode::Char(ch) => Some(Self::Insert(ch)),
            _ => None,
        }
    }

    fn apply(self, text: &mut String) {
        match self {
            Self::Clear => text.clear(),
            Self::Backspace => {
                text.pop();
            }
            Self::Insert(ch) => text.push(ch),
        }
    }
}

trait DetailOverlayTarget {
    const HOME_ENABLED: bool;
    const TITLE: &'static str;
    const BORDER: Color;

    fn overlay(&self, scroll: u16) -> DashboardOverlay;
    fn lines(&self, app: &DashboardApp) -> Vec<Line<'static>>;
}

struct QueueDetailTarget {
    selection: QueueSelection,
}

impl DetailOverlayTarget for QueueDetailTarget {
    const HOME_ENABLED: bool = true;
    const TITLE: &'static str = "Queue Detail";
    const BORDER: Color = Color::Yellow;

    fn overlay(&self, scroll: u16) -> DashboardOverlay {
        DashboardOverlay::QueueDetail {
            selection: self.selection,
            scroll,
        }
    }

    fn lines(&self, app: &DashboardApp) -> Vec<Line<'static>> {
        load_queue_detail_lines(&app.project, &app.snapshot.queues, self.selection)
            .unwrap_or_else(|err| detail_error_lines("queue detail", err))
    }
}

struct ChannelDetailTarget {
    channel_index: usize,
}

impl DetailOverlayTarget for ChannelDetailTarget {
    const HOME_ENABLED: bool = false;
    const TITLE: &'static str = "Channel Detail";
    const BORDER: Color = Color::Yellow;

    fn overlay(&self, scroll: u16) -> DashboardOverlay {
        DashboardOverlay::ChannelDetail {
            channel_index: self.channel_index,
            scroll,
        }
    }

    fn lines(&self, app: &DashboardApp) -> Vec<Line<'static>> {
        app.snapshot
            .channels
            .get(self.channel_index)
            .map(|channel| load_channel_detail_lines(&app.project, channel))
            .transpose()
            .unwrap_or_else(|err| Some(detail_error_lines("channel detail", err)))
            .unwrap_or_else(|| no_selection_lines("channel"))
    }
}

struct NoticeDetailTarget;

impl DetailOverlayTarget for NoticeDetailTarget {
    const HOME_ENABLED: bool = true;
    const TITLE: &'static str = "Notice Provenance";
    const BORDER: Color = Color::Yellow;

    fn overlay(&self, scroll: u16) -> DashboardOverlay {
        DashboardOverlay::NoticeDetail { scroll }
    }

    fn lines(&self, app: &DashboardApp) -> Vec<Line<'static>> {
        load_notice_detail_lines(&app.project, &app.snapshot)
            .unwrap_or_else(|err| detail_error_lines("notice detail", err))
    }
}

struct ConversationDetailTarget {
    session: String,
}

impl DetailOverlayTarget for ConversationDetailTarget {
    const HOME_ENABLED: bool = true;
    const TITLE: &'static str = "Conversation";
    const BORDER: Color = Color::Magenta;

    fn overlay(&self, scroll: u16) -> DashboardOverlay {
        DashboardOverlay::ConversationDetail {
            session: self.session.clone(),
            scroll,
        }
    }

    fn lines(&self, app: &DashboardApp) -> Vec<Line<'static>> {
        load_session_detail_lines(app.session_by_id(&self.session))
            .unwrap_or_else(|err| detail_error_lines("conversation", err))
    }
}

fn detail_error_lines(label: &str, err: impl std::fmt::Display) -> Vec<Line<'static>> {
    vec![Line::from(format!("Failed to load {label}: {err:#}"))]
}

fn no_selection_lines(label: &str) -> Vec<Line<'static>> {
    vec![Line::from(format!("No {label} selected."))]
}

trait DashboardListTab {
    const CENTER_SELECTED: bool;

    fn restore(app: &mut DashboardApp);
    fn count(app: &DashboardApp) -> usize;
    fn selection(app: &mut DashboardApp) -> (&mut usize, &mut u16);
    fn remember(app: &mut DashboardApp);
}

struct SchemaListTab;

impl DashboardListTab for SchemaListTab {
    const CENTER_SELECTED: bool = true;

    fn restore(app: &mut DashboardApp) {
        app.restore_schema_selection();
    }

    fn count(app: &DashboardApp) -> usize {
        app.snapshot.schema.roles.len()
    }

    fn selection(app: &mut DashboardApp) -> (&mut usize, &mut u16) {
        (&mut app.schema_selected, &mut app.schema_scroll)
    }

    fn remember(_app: &mut DashboardApp) {}
}

struct ChannelListTab;

impl DashboardListTab for ChannelListTab {
    const CENTER_SELECTED: bool = false;

    fn restore(_app: &mut DashboardApp) {}

    fn count(app: &DashboardApp) -> usize {
        app.filtered_channel_indices().len()
    }

    fn selection(app: &mut DashboardApp) -> (&mut usize, &mut u16) {
        (&mut app.channel_selected, &mut app.channel_scroll)
    }

    fn remember(_app: &mut DashboardApp) {}
}

struct SessionListTab;

impl DashboardListTab for SessionListTab {
    const CENTER_SELECTED: bool = true;

    fn restore(app: &mut DashboardApp) {
        app.restore_session_selection();
    }

    fn count(app: &DashboardApp) -> usize {
        app.filtered_session_indices().len()
    }

    fn selection(app: &mut DashboardApp) -> (&mut usize, &mut u16) {
        (&mut app.session_selected, &mut app.session_scroll)
    }

    fn remember(app: &mut DashboardApp) {
        app.remember_session_selection();
    }
}

fn usize_to_u16(value: usize) -> u16 {
    value.min(usize::from(u16::MAX)) as u16
}

fn center_selected_row(scroll: &mut u16, selected: usize, height: usize) {
    let height = height.max(DASHBOARD_MIN_VISIBLE_ROWS);
    let current = usize::from(*scroll);
    if selected < current {
        *scroll = usize_to_u16(selected);
    } else if selected >= current + height {
        *scroll = usize_to_u16(selected.saturating_add(1).saturating_sub(height));
    }
}

struct SearchSpec {
    target: SearchTarget,
    query: String,
}

pub(in crate::app) enum DashboardIntent {
    None,
    Navigate(DashboardRoute),
    Overlay(DashboardOverlay),
    Action(DashboardAction),
    Toast(DashboardToast),
}

impl DashboardIntent {
    fn from_action(action: Option<DashboardAction>) -> Self {
        action.map(Self::Action).unwrap_or(Self::None)
    }
}

struct PaletteEntry {
    command: &'static CommandDef,
    group: PaletteGroup,
    label: String,
    detail: String,
    key: Option<&'static str>,
}

struct CommandText {
    label: Cow<'static, str>,
    detail: Cow<'static, str>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CommandId {
    Detail,
    New,
    More,
    Attach,
    Search,
    Channels,
    Queues,
    ToggleQueueCollapsed,
    JumpEvent,
    Timeline,
    Schema,
    OpenProjectMd,
    OpenConfig,
    Conversations,
    Advanced,
    Check,
    Assist,
    OpenProject,
    OpenSelectedDir,
    NewChannel,
    ToggleRole,
    ToggleAllRoles,
    TriggerRole,
    ArchiveAgent,
    ToggleArchived,
    RetryErrored,
    ProviderSettings,
    Help,
    Quit,
}

impl CommandId {
    fn as_str(self) -> &'static str {
        match self {
            Self::Detail => "detail",
            Self::New => "new",
            Self::More => "more",
            Self::Attach => "attach",
            Self::Search => "search",
            Self::Channels => "channels",
            Self::Queues => "queues",
            Self::ToggleQueueCollapsed => "toggle-queue-collapsed",
            Self::JumpEvent => "jump-event",
            Self::Timeline => "timeline",
            Self::Schema => "schema",
            Self::OpenProjectMd => "open-project-md",
            Self::OpenConfig => "open-config",
            Self::Conversations => "conversations",
            Self::Advanced => "advanced",
            Self::Check => "check",
            Self::Assist => "assist",
            Self::OpenProject => "open-project",
            Self::OpenSelectedDir => "open-selected-dir",
            Self::NewChannel => "new-channel",
            Self::ToggleRole => "toggle-role",
            Self::ToggleAllRoles => "toggle-all-roles",
            Self::TriggerRole => "trigger-role",
            Self::ArchiveAgent => "archive-agent",
            Self::ToggleArchived => "toggle-archived",
            Self::RetryErrored => "retry-errored",
            Self::ProviderSettings => "provider-settings",
            Self::Help => "help",
            Self::Quit => "quit",
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::app) enum CommandSurface {
    Palette,
    Footer,
    Help,
}

impl CommandSurface {
    const fn bit(self) -> u8 {
        match self {
            Self::Palette => 1,
            Self::Footer => 2,
            Self::Help => 4,
        }
    }
}

#[derive(Clone, Copy)]
struct CommandSurfaces(u8);

impl CommandSurfaces {
    const ALL: Self = Self(
        CommandSurface::Palette.bit() | CommandSurface::Footer.bit() | CommandSurface::Help.bit(),
    );
    const PALETTE_HELP: Self = Self(CommandSurface::Palette.bit() | CommandSurface::Help.bit());

    const fn contains(self, surface: CommandSurface) -> bool {
        self.0 & surface.bit() != 0
    }
}

#[derive(Clone, Copy)]
pub(in crate::app) struct CommandCtx<'a> {
    app: &'a DashboardApp,
}

impl<'a> CommandCtx<'a> {
    fn new(app: &'a DashboardApp) -> Self {
        Self { app }
    }
}

type CommandTextFn = for<'a> fn(CommandCtx<'a>) -> Option<CommandText>;
type CommandVisibleFn = for<'a> fn(CommandCtx<'a>) -> bool;
type CommandRunFn = fn(&mut DashboardApp) -> Result<DashboardIntent>;

#[derive(Clone, Copy)]
struct CommandRoutes(u8);

impl CommandRoutes {
    const DASHBOARD: Self = Self(1);
    const SCHEMA: Self = Self(2);
    const CHANNELS: Self = Self(4);
    const QUEUES: Self = Self(8);
    const TIMELINE: Self = Self(16);
    const CONVERSATIONS: Self = Self(32);

    const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    fn contains(self, route: &DashboardRoute) -> bool {
        let bit = match route {
            DashboardRoute::Dashboard => Self::DASHBOARD.0,
            DashboardRoute::Schema { .. } => Self::SCHEMA.0,
            DashboardRoute::Channels { .. } => Self::CHANNELS.0,
            DashboardRoute::Queues { .. } => Self::QUEUES.0,
            DashboardRoute::Timeline { .. } => Self::TIMELINE.0,
            DashboardRoute::Conversations { .. } => Self::CONVERSATIONS.0,
        };
        self.0 & bit != 0
    }
}

#[derive(Clone, Copy)]
enum CommandKey {
    None,
    Static(&'static str),
    Route(CommandRoutes, &'static str),
}

impl CommandKey {
    fn resolve(self, ctx: CommandCtx<'_>) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Static(key) => Some(key),
            Self::Route(routes, key) => routes.contains(&ctx.app.route).then_some(key),
        }
    }
}

pub(in crate::app) struct CommandDef {
    id: CommandId,
    group: PaletteGroup,
    key: CommandKey,
    label: &'static str,
    detail: &'static str,
    aliases: &'static [&'static str],
    surfaces: CommandSurfaces,
    text: Option<CommandTextFn>,
    visible: CommandVisibleFn,
    run: CommandRunFn,
}

impl CommandDef {
    fn id(&self) -> &'static str {
        self.id.as_str()
    }

    fn group(&self) -> PaletteGroup {
        self.group
    }

    pub(in crate::app) fn key(&self, ctx: CommandCtx<'_>) -> Option<&'static str> {
        self.key.resolve(ctx)
    }

    pub(in crate::app) fn label(&self, ctx: CommandCtx<'_>) -> Cow<'static, str> {
        self.text
            .and_then(|text| text(ctx))
            .map(|text| text.label)
            .unwrap_or(Cow::Borrowed(self.label))
    }

    fn detail(&self, ctx: CommandCtx<'_>) -> Cow<'static, str> {
        self.text
            .and_then(|text| text(ctx))
            .map(|text| text.detail)
            .unwrap_or(Cow::Borrowed(self.detail))
    }

    fn aliases(&self) -> &'static [&'static str] {
        self.aliases
    }

    fn visible(&self, ctx: CommandCtx<'_>, surface: CommandSurface) -> bool {
        self.surfaces.contains(surface) && (self.visible)(ctx)
    }

    fn run(&self, app: &mut DashboardApp) -> Result<DashboardIntent> {
        (self.run)(app)
    }
}

pub(in crate::app) struct CommandRegistry {
    commands: &'static [CommandDef],
}

impl CommandRegistry {
    const fn new(commands: &'static [CommandDef]) -> Self {
        Self { commands }
    }

    pub(in crate::app) fn visible<'a>(
        &'static self,
        ctx: CommandCtx<'a>,
        surface: CommandSurface,
    ) -> impl Iterator<Item = &'static CommandDef> + 'a {
        self.commands
            .iter()
            .filter(move |command| command.visible(ctx, surface))
    }
}

trait DashboardTab: Sync {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()>;
    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent>;
    fn search(&self, app: &DashboardApp) -> Option<SearchSpec>;
    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        narrow: bool,
    );
}

struct OverviewTab;
struct SchemaTab;
struct ChannelsTab;
struct QueuesTab;
struct TimelineTab;
struct ConversationsTab;

static OVERVIEW_TAB: OverviewTab = OverviewTab;
static SCHEMA_TAB: SchemaTab = SchemaTab;
static CHANNELS_TAB: ChannelsTab = ChannelsTab;
static QUEUES_TAB: QueuesTab = QueuesTab;
static TIMELINE_TAB: TimelineTab = TimelineTab;
static CONVERSATIONS_TAB: ConversationsTab = ConversationsTab;

impl DashboardRoute {
    fn tab(&self) -> &'static dyn DashboardTab {
        match self {
            Self::Dashboard => &OVERVIEW_TAB,
            Self::Schema { .. } => &SCHEMA_TAB,
            Self::Channels { .. } => &CHANNELS_TAB,
            Self::Queues { .. } => &QUEUES_TAB,
            Self::Timeline { .. } => &TIMELINE_TAB,
            Self::Conversations { .. } => &CONVERSATIONS_TAB,
        }
    }

    fn next(&self) -> Self {
        match self {
            Self::Dashboard => Self::Schema { role: None },
            Self::Schema { .. } => Self::Channels { channel: None },
            Self::Channels { .. } => Self::Queues { queue: None },
            Self::Queues { .. } => Self::Timeline { event: None },
            Self::Timeline { .. } => Self::Conversations { session: None },
            Self::Conversations { .. } => Self::Dashboard,
        }
    }

    fn previous(&self) -> Self {
        match self {
            Self::Dashboard => Self::Conversations { session: None },
            Self::Schema { .. } => Self::Dashboard,
            Self::Channels { .. } => Self::Schema { role: None },
            Self::Queues { .. } => Self::Channels { channel: None },
            Self::Timeline { .. } => Self::Queues { queue: None },
            Self::Conversations { .. } => Self::Timeline { event: None },
        }
    }
}

pub(in crate::app) fn run_dashboard(
    project: ProjectPaths,
    role_filter: Option<RoleSlug>,
    include_archived: bool,
    refresh: Duration,
) -> Result<()> {
    run_dashboard_with_initial_tab(
        project,
        role_filter,
        include_archived,
        refresh,
        DashboardRoute::Dashboard,
    )
}

pub(in crate::app) fn run_dashboard_with_initial_tab(
    project: ProjectPaths,
    role_filter: Option<RoleSlug>,
    include_archived: bool,
    refresh: Duration,
    initial_route: DashboardRoute,
) -> Result<()> {
    let mut app = DashboardApp::new(
        project.clone(),
        role_filter,
        include_archived,
        initial_route.clone(),
    );
    app.navigate(initial_route)?;
    let mut terminal = Some(TerminalSession::enter()?);
    let mut next_refresh = Instant::now();
    loop {
        app.tick_frame();
        if Instant::now() >= next_refresh {
            app.refresh()?;
            next_refresh = Instant::now() + refresh;
        }
        terminal
            .as_mut()
            .expect("dashboard terminal is active")
            .draw(|frame| app.draw(frame))?;
        if event::poll(DASHBOARD_FRAME_INTERVAL).context("Failed to poll terminal events")?
            && let Event::Key(key) = event::read().context("Failed to read terminal event")?
            && let Some(action) = app.handle_key(key)?
        {
            let suspend_terminal = action.suspends_terminal();
            if suspend_terminal {
                drop(terminal.take());
            }
            match run_dashboard_action(&project, action)? {
                DashboardActionOutcome::Continue(toast) => {
                    app.toast = toast;
                    if suspend_terminal {
                        terminal = Some(TerminalSession::enter()?);
                    }
                }
                DashboardActionOutcome::Exit => return Ok(()),
            }
        }
    }
}

pub(in crate::app) enum DashboardActionOutcome {
    Continue(Option<DashboardToast>),
    Exit,
}

pub(in crate::app) fn run_dashboard_action(
    project: &ProjectPaths,
    action: DashboardAction,
) -> Result<DashboardActionOutcome> {
    let outcome = action.execute(project);
    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(err) if crate::input::editor::is_cancelled(&err) => Ok(
            DashboardActionOutcome::Continue(Some(DashboardToast::info(
                crate::input::editor::cancellation_message(&err)
                    .unwrap_or_else(|| "cancelled".to_owned()),
            ))),
        ),
        Err(err) => Err(err),
    }
}

impl DashboardAction {
    fn execute(self, project: &ProjectPaths) -> Result<DashboardActionOutcome> {
        Ok(match self {
            Self::Quit => DashboardActionOutcome::Exit,
            Self::AttachRole(role) => {
                run_attach_viewer(project, AttachTarget::Role(role))?;
                DashboardActionOutcome::Continue(Some(DashboardToast::info(
                    "attach session returned",
                )))
            }
            Self::AttachAgent(agent) => {
                run_attach_viewer(project, AttachTarget::Agent(agent))?;
                DashboardActionOutcome::Continue(Some(DashboardToast::info(
                    "attach session returned",
                )))
            }
            Self::MoreWithQuery(agent, query) => {
                more_agent(MoreArgs {
                    agent: Some(AgentSpec {
                        role: Some(agent.role),
                        agent: agent.agent,
                    }),
                    query: Some(query),
                    new: false,
                })?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "agent follow-up completed",
                )))
            }
            Self::New(role) => {
                let label = role.to_string();
                new_agent(
                    project,
                    NewAgentArgs {
                        role: Some(role),
                        prompt: None,
                        no_prompt: false,
                        attach: false,
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "new `{label}` agent started"
                ))))
            }
            Self::NewRole => {
                draft_role(
                    project,
                    RoleDraftArgs {
                        role: None,
                        request: None,
                        feedback: Vec::new(),
                        no_review: false,
                        active: false,
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "role draft workflow completed",
                )))
            }
            Self::Archive(agent) => {
                let label = agent.label();
                archive_agent(
                    project,
                    AgentSelectorArgs {
                        agent: Some(AgentSpec {
                            role: Some(agent.role),
                            agent: agent.agent,
                        }),
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "archived {label}"
                ))))
            }
            Self::TriggerRole(role) => {
                launch_triggered_role(
                    project,
                    &role,
                    &TriggerLaunch::Queued {
                        queue: role.to_string(),
                    },
                    TriggerCause::Manual {
                        reason: Some("triggered from think status".to_owned()),
                    },
                )?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "trigger queued for `{role}`"
                ))))
            }
            Self::Check => {
                check_project(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project check completed",
                )))
            }
            Self::Assist => {
                assist_project_interactive(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project assist completed",
                )))
            }
            Self::OpenProject => {
                open_project_directory_at(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "project directory opened",
                )))
            }
            Self::OpenPath { path, label } => {
                open_path(&path)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "opened {label}"
                ))))
            }
            Self::NewChannel => {
                let channel = selection::resolve_or_prompt_new_slug(
                    None::<ChannelSlug>,
                    "Channel to create",
                )?;
                create_channel(project, &channel)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "channel `{channel}` ready"
                ))))
            }
            Self::CodexLogin => {
                codex_provider_login(CodexLoginArgs {
                    account: None,
                    home: None,
                })?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "Codex account login completed",
                )))
            }
            Self::CodexConfig => {
                codex_provider_config_interactive(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "Codex provider config updated",
                )))
            }
            Self::ToggleRole(role) => {
                toggle_role(project, &role)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "toggled role `{role}`"
                ))))
            }
            Self::ToggleAgent(agent) => {
                let label = agent.label();
                toggle_agent(project, &agent)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(format!(
                    "toggled agent `{label}`"
                ))))
            }
            Self::ToggleAllRoles => {
                toggle_all_roles(project)?;
                DashboardActionOutcome::Continue(Some(DashboardToast::success(
                    "role toggle completed",
                )))
            }
        })
    }
}

impl DashboardOverlay {
    fn draw(&self, app: &DashboardApp, frame: &mut Frame<'_>, area: Rect) {
        match self {
            Self::None => {}
            Self::Advanced => draw_advanced_menu(frame, area),
            Self::Help => draw_help_overlay(app, frame, area),
            Self::Search { target, query } => draw_search_overlay(
                frame,
                area,
                *target,
                query,
                app.search_visible_count(*target),
            ),
            Self::Palette { query, selected } => app.draw_palette(frame, area, query, *selected),
            Self::Composer {
                agent,
                running,
                composer,
            } => app.draw_dashboard_composer(frame, area, agent, *running, composer),
            Self::Detail { extended, scroll } => {
                app.draw_focused_detail(frame, area, *extended, *scroll);
            }
            Self::QueueDetail { selection, scroll } => app.draw_detail_target(
                frame,
                area,
                &QueueDetailTarget {
                    selection: *selection,
                },
                *scroll,
            ),
            Self::ChannelDetail {
                channel_index,
                scroll,
            } => app.draw_detail_target(
                frame,
                area,
                &ChannelDetailTarget {
                    channel_index: *channel_index,
                },
                *scroll,
            ),
            Self::NoticeDetail { scroll } => {
                app.draw_detail_target(frame, area, &NoticeDetailTarget, *scroll);
            }
            Self::ConversationDetail { session, scroll } => app.draw_detail_target(
                frame,
                area,
                &ConversationDetailTarget {
                    session: session.clone(),
                },
                *scroll,
            ),
            Self::ProviderSettings { selected } => {
                app.draw_provider_settings(frame, area, *selected);
            }
        }
    }

    fn footer_line(&self, app: &DashboardApp, width: usize) -> Line<'static> {
        match self {
            Self::None => app.route_footer_line(width),
            Self::Advanced => footer_line_from_pairs(
                &[
                    ("n", "new role"),
                    ("r", "retry errored"),
                    ("o", "open project"),
                    ("p", "provider settings"),
                    ("x", "archived"),
                    ("Esc", "close"),
                ],
                width,
            ),
            Self::ProviderSettings { .. } => footer_line_from_pairs(
                &[
                    ("↑↓", "select"),
                    ("Enter/s", "switch"),
                    ("a", "add"),
                    ("d", "delete"),
                    ("m", "model"),
                    ("Esc", "close"),
                ],
                width,
            ),
            Self::Help => footer_line_from_pairs(&[("Esc", "close"), ("q", "close")], width),
            Self::Search { .. } => footer_line_from_pairs(
                &[
                    ("type", "filter"),
                    ("Ctrl-u", "clear"),
                    ("Enter", "apply"),
                    ("Esc", "close"),
                ],
                width,
            ),
            Self::Palette { .. } => footer_line_from_pairs(
                &[
                    ("type", "search"),
                    ("↑↓", "select"),
                    ("Enter", "run"),
                    ("Esc", "close"),
                ],
                width,
            ),
            Self::Composer { running, .. } => {
                let send_label = if *running { "steer" } else { "send" };
                let mut pairs = vec![("Ctrl-D", send_label)];
                if *running {
                    pairs.push(("Ctrl-A", "steer+attach"));
                }
                pairs.extend([
                    ("Esc", "cancel"),
                    ("Enter", "newline"),
                    ("↑↓", "cursor"),
                    ("Ctrl-↑↓", "history"),
                ]);
                footer_line_from_pairs(&pairs, width)
            }
            Self::Detail { extended, .. } => {
                let mut pairs = vec![("↑↓", "scroll"), ("a", "attach")];
                if let Some(agent) = app.selected_agent() {
                    pairs.push(
                        if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
                            ("m", "steer")
                        } else {
                            ("m", "more")
                        },
                    );
                }
                pairs.extend([
                    ("n", "new"),
                    ("Space", "pause/unpause"),
                    (
                        "x",
                        if *extended {
                            "hide timeline"
                        } else {
                            "timeline"
                        },
                    ),
                    ("Esc", "close"),
                ]);
                footer_line_from_pairs(&pairs, width)
            }
            Self::QueueDetail { .. } | Self::NoticeDetail { .. } => footer_line_from_pairs(
                &[("↑↓", "scroll"), ("PgUp/PgDn", "scroll"), ("Esc", "close")],
                width,
            ),
            Self::ChannelDetail { .. } => footer_line_from_pairs(
                &[
                    ("↑↓", "scroll"),
                    ("PgUp/PgDn", "scroll"),
                    ("o", "open dir"),
                    ("Esc", "close"),
                ],
                width,
            ),
            Self::ConversationDetail { .. } => footer_line_from_pairs(
                &[
                    ("↑↓", "scroll"),
                    ("PgUp/PgDn", "scroll"),
                    ("o", "open"),
                    ("y/Y", "copy"),
                    ("Esc", "close"),
                ],
                width,
            ),
        }
    }
}

impl DashboardTab for OverviewTab {
    fn enter(&self, app: &mut DashboardApp, _route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_overview_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, app: &DashboardApp) -> Option<SearchSpec> {
        Some(SearchSpec {
            target: SearchTarget::Agents,
            query: app.filter.clone(),
        })
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        narrow: bool,
    ) {
        app.draw_notices(frame, root.notices);
        app.draw_detail(frame, root.state);
        app.draw_tree(frame, root.agents);
        app.draw_runtime(frame, root.runtime, narrow);
    }
}

impl DashboardTab for SchemaTab {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        if let DashboardRoute::Schema { role: Some(role) } = route
            && let Some(index) = app
                .snapshot
                .schema
                .roles
                .iter()
                .position(|schema_role| schema_role.slug == *role)
        {
            app.schema_selected = index;
        }
        app.restore_schema_selection();
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_schema_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, _app: &DashboardApp) -> Option<SearchSpec> {
        None
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        narrow: bool,
    ) {
        app.draw_schema_tab(frame, root.main, narrow);
    }
}

impl DashboardTab for ChannelsTab {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        if let DashboardRoute::Channels {
            channel: Some(channel),
        } = route
        {
            let filtered = app.filtered_channel_indices();
            if let Some(index) = filtered.iter().position(|snapshot_index| {
                app.snapshot.channels[*snapshot_index].name == channel.as_str()
            }) {
                app.channel_selected = index;
            }
        }
        app.restore_channel_selection();
        app.mark_alerts_seen();
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_channel_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, app: &DashboardApp) -> Option<SearchSpec> {
        Some(SearchSpec {
            target: SearchTarget::Channels,
            query: app.channel_query.clone(),
        })
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        _narrow: bool,
    ) {
        app.draw_channel_tab(frame, root.main);
    }
}

impl DashboardTab for QueuesTab {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        let key = match route {
            DashboardRoute::Queues { queue } => queue.clone(),
            _ => None,
        };
        app.restore_queue_selection(key.or_else(|| app.selected_queue_key()));
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_queue_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, app: &DashboardApp) -> Option<SearchSpec> {
        Some(SearchSpec {
            target: SearchTarget::Queues,
            query: app.queue_query.clone(),
        })
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        _narrow: bool,
    ) {
        app.draw_queue_tab(frame, root.main);
    }
}

impl DashboardTab for TimelineTab {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        if let DashboardRoute::Timeline { event: Some(event) } = route {
            app.event_selected_key = Some(event.clone());
        }
        app.restore_event_selection();
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_event_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, app: &DashboardApp) -> Option<SearchSpec> {
        Some(SearchSpec {
            target: SearchTarget::Events,
            query: app.event_query.clone(),
        })
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        _narrow: bool,
    ) {
        app.draw_event_log(frame, root.main);
    }
}

impl DashboardTab for ConversationsTab {
    fn enter(&self, app: &mut DashboardApp, route: &DashboardRoute) -> Result<()> {
        app.overlay = DashboardOverlay::None;
        if let DashboardRoute::Conversations {
            session: Some(session),
        } = route
        {
            app.session_selected_key = Some(session.clone());
        }
        app.restore_session_selection();
        Ok(())
    }

    fn handle_key(
        &self,
        app: &mut DashboardApp,
        key: crossterm::event::KeyEvent,
    ) -> Result<DashboardIntent> {
        app.handle_session_tab_key(key)
            .map(DashboardIntent::from_action)
    }

    fn search(&self, app: &DashboardApp) -> Option<SearchSpec> {
        Some(SearchSpec {
            target: SearchTarget::Conversations,
            query: app.session_query.clone(),
        })
    }

    fn draw(
        &self,
        app: &mut DashboardApp,
        frame: &mut Frame<'_>,
        root: &DashboardLayoutAreas,
        _narrow: bool,
    ) {
        app.draw_conversations_tab(frame, root.main);
    }
}

macro_rules! dashboard_command {
    (
        $id:expr,
        $group:expr,
        $key:expr,
        $label:literal,
        $detail:literal,
        [$($alias:literal),* $(,)?],
        $surfaces:expr,
        $available:ident,
        $run:ident
        $(, text = $text:ident)?
    ) => {
        CommandDef {
            id: $id,
            group: $group,
            key: $key,
            label: $label,
            detail: $detail,
            aliases: &[$($alias),*],
            surfaces: $surfaces,
            text: dashboard_command!(@text $($text)?),
            visible: $available,
            run: $run,
        }
    };
    (@text $text:ident) => {
        Some($text)
    };
    (@text) => {
        None
    };
}

pub(in crate::app) static DASHBOARD_COMMANDS: &[CommandDef] = &[
    dashboard_command!(
        CommandId::Detail,
        PaletteGroup::Current,
        CommandKey::Static("Enter"),
        "detail",
        "open focused detail for the selected item",
        ["open", "inspect", "enter"],
        CommandSurfaces::ALL,
        command_visible_detail,
        command_run_detail,
        text = command_text_detail
    ),
    dashboard_command!(
        CommandId::More,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::DASHBOARD, "m"),
        "more",
        "continue the selected inactive agent with a follow-up query",
        ["continue", "query", "reply", "steer", "live", "m"],
        CommandSurfaces::ALL,
        command_visible_more,
        command_run_more,
        text = command_text_more
    ),
    dashboard_command!(
        CommandId::Attach,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::DASHBOARD, "a"),
        "attach",
        "open the transcript viewer for the selected role or agent",
        ["attach", "terminal", "session", "transcript", "a"],
        CommandSurfaces::ALL,
        command_visible_attach,
        command_run_attach
    ),
    dashboard_command!(
        CommandId::ToggleRole,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::DASHBOARD, "Space"),
        "pause/unpause",
        "pause or unpause the selected role or agent",
        ["pause", "unpause", "resume", "space"],
        CommandSurfaces::ALL,
        command_visible_toggle_role,
        command_run_toggle_role
    ),
    dashboard_command!(
        CommandId::New,
        PaletteGroup::Operate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "n"),
        "new agent",
        "create an agent for the selected or default role",
        ["create", "agent", "n"],
        CommandSurfaces::ALL,
        command_visible_new,
        command_run_new
    ),
    dashboard_command!(
        CommandId::OpenSelectedDir,
        PaletteGroup::Current,
        CommandKey::Route(
            CommandRoutes::SCHEMA
                .union(CommandRoutes::CHANNELS)
                .union(CommandRoutes::CONVERSATIONS),
            "o",
        ),
        "open selected dir",
        "open the selected role, agent, channel, or conversation directory",
        [
            "open",
            "directory",
            "folder",
            "agent",
            "channel",
            "conversation"
        ],
        CommandSurfaces::ALL,
        command_visible_open_selected_dir,
        command_run_open_selected_dir
    ),
    dashboard_command!(
        CommandId::TriggerRole,
        PaletteGroup::Current,
        CommandKey::None,
        "trigger role",
        "enqueue a manual trigger for the selected role",
        ["manual", "launch"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_trigger_role,
        command_run_trigger_role
    ),
    dashboard_command!(
        CommandId::ArchiveAgent,
        PaletteGroup::Current,
        CommandKey::None,
        "archive agent",
        "hide an inactive selected agent while keeping its files",
        ["hide", "archive"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_archive_agent,
        command_run_archive_agent
    ),
    dashboard_command!(
        CommandId::ToggleQueueCollapsed,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::QUEUES, "Space"),
        "collapse queue",
        "collapse or expand the selected queue",
        ["collapse", "expand", "space"],
        CommandSurfaces::ALL,
        command_visible_toggle_queue_collapsed,
        command_run_toggle_queue_collapsed
    ),
    dashboard_command!(
        CommandId::JumpEvent,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::TIMELINE, "Enter"),
        "jump",
        "jump from the selected event to its target",
        ["jump", "event", "enter"],
        CommandSurfaces::ALL,
        command_visible_jump_event,
        command_run_jump_event
    ),
    dashboard_command!(
        CommandId::Search,
        PaletteGroup::Navigate,
        CommandKey::Static("/"),
        "search",
        "filter the current dashboard tab",
        ["filter", "find", "/", "slash"],
        CommandSurfaces::ALL,
        command_visible_search,
        command_run_search
    ),
    dashboard_command!(
        CommandId::NewChannel,
        PaletteGroup::Operate,
        CommandKey::Route(CommandRoutes::CHANNELS, "n"),
        "new channel",
        "create a publish channel",
        ["create", "channel", "artifact"],
        CommandSurfaces::ALL,
        command_visible_new_channel,
        command_run_new_channel
    ),
    dashboard_command!(
        CommandId::OpenProjectMd,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::SCHEMA, "p"),
        "open PROJECT.md",
        "open the project instruction file",
        ["project.md", "instructions", "project"],
        CommandSurfaces::ALL,
        command_visible_schema,
        command_run_open_project_md
    ),
    dashboard_command!(
        CommandId::OpenConfig,
        PaletteGroup::Current,
        CommandKey::Route(CommandRoutes::SCHEMA, "t"),
        "open think.toml",
        "open the project schema config file",
        ["think.toml", "config", "schema"],
        CommandSurfaces::ALL,
        command_visible_schema,
        command_run_open_config
    ),
    dashboard_command!(
        CommandId::Channels,
        PaletteGroup::Navigate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "l"),
        "channels",
        "show publish channels and alert artifacts",
        ["channel", "channels", "artifact", "artifacts", "alert"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_channels
    ),
    dashboard_command!(
        CommandId::Queues,
        PaletteGroup::Navigate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "o"),
        "queues",
        "show ordered trigger queues",
        ["queue", "trigger", "o"],
        CommandSurfaces::ALL,
        command_visible_queues,
        command_run_queues
    ),
    dashboard_command!(
        CommandId::Timeline,
        PaletteGroup::Navigate,
        CommandKey::None,
        "timeline",
        "switch to the full-width timeline of recent project events",
        ["events", "timeline", "history"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_always,
        command_run_timeline
    ),
    dashboard_command!(
        CommandId::Schema,
        PaletteGroup::Navigate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "s"),
        "schema",
        "review project config, provider settings, roles, steps, and triggers",
        ["schema", "project", "config", "think.toml", "roles"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_schema
    ),
    dashboard_command!(
        CommandId::Conversations,
        PaletteGroup::Navigate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "C"),
        "conversations",
        "inspect non-agent command conversations",
        ["conversations", "history", "sessions", "debug", "commands"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_conversations
    ),
    dashboard_command!(
        CommandId::Check,
        PaletteGroup::Operate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "c"),
        "technical check",
        "ask the configured backend to inspect project health without permanent changes",
        ["health", "audit", "c"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_check
    ),
    dashboard_command!(
        CommandId::Assist,
        PaletteGroup::Operate,
        CommandKey::Route(CommandRoutes::DASHBOARD, "i"),
        "assist",
        "ask the configured backend to operate this think project with full context",
        ["operator", "configure", "manage", "i"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_assist
    ),
    dashboard_command!(
        CommandId::Advanced,
        PaletteGroup::Maintenance,
        CommandKey::Route(CommandRoutes::DASHBOARD, "A"),
        "advanced",
        "open advanced maintenance actions",
        ["maintenance", "technical"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_advanced
    ),
    dashboard_command!(
        CommandId::OpenProject,
        PaletteGroup::Navigate,
        CommandKey::Static("A o"),
        "open project",
        "open the current project directory",
        ["open", "project", "directory", "folder", "finder"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_always,
        command_run_open_project
    ),
    dashboard_command!(
        CommandId::ToggleAllRoles,
        PaletteGroup::Maintenance,
        CommandKey::Route(CommandRoutes::DASHBOARD, "U"),
        "toggle all roles",
        "pause all active roles or unpause all paused roles",
        ["pause all", "unpause all", "shift u"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_toggle_all_roles
    ),
    dashboard_command!(
        CommandId::ToggleArchived,
        PaletteGroup::Maintenance,
        CommandKey::Static("A x"),
        "toggle archived agents",
        "show or hide archived agents in the table",
        ["archived", "show archived"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_always,
        command_run_toggle_archived
    ),
    dashboard_command!(
        CommandId::RetryErrored,
        PaletteGroup::Maintenance,
        CommandKey::Static("A r"),
        "retry errored waits",
        "wake quota, rate-limit, and OOM retry backoffs now",
        ["retry", "quota", "rate limit", "error"],
        CommandSurfaces::PALETTE_HELP,
        command_visible_always,
        command_run_retry_errored
    ),
    dashboard_command!(
        CommandId::ProviderSettings,
        PaletteGroup::Maintenance,
        CommandKey::Static("A p"),
        "provider settings",
        "open Codex account, model, and thinking settings",
        [
            "codex", "provider", "settings", "login", "account", "model", "thinking"
        ],
        CommandSurfaces::PALETTE_HELP,
        command_visible_always,
        command_run_provider_settings
    ),
    dashboard_command!(
        CommandId::Help,
        PaletteGroup::Help,
        CommandKey::Static("?"),
        "help",
        "show dashboard keys",
        ["?", "keys"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_help
    ),
    dashboard_command!(
        CommandId::Quit,
        PaletteGroup::Help,
        CommandKey::Static("q"),
        "quit",
        "leave the dashboard",
        ["exit", "close", "q"],
        CommandSurfaces::ALL,
        command_visible_always,
        command_run_quit
    ),
];

pub(in crate::app) static COMMAND_REGISTRY: CommandRegistry =
    CommandRegistry::new(DASHBOARD_COMMANDS);

fn command_visible_always(_ctx: CommandCtx<'_>) -> bool {
    true
}

fn command_visible_detail(ctx: CommandCtx<'_>) -> bool {
    match &ctx.app.route {
        DashboardRoute::Dashboard => ctx.app.visible_rows().get(ctx.app.selected).is_some(),
        DashboardRoute::Channels { .. } => ctx.app.selected_channel_index().is_some(),
        DashboardRoute::Queues { .. } => ctx
            .app
            .queue_selection_rows()
            .get(ctx.app.queue_selected)
            .is_some(),
        DashboardRoute::Conversations { .. } => ctx.app.selected_session().is_some(),
        DashboardRoute::Schema { .. } | DashboardRoute::Timeline { .. } => false,
    }
}

fn command_visible_new(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard) && ctx.app.selected_role_slug().is_some()
}

fn command_visible_more(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard) && ctx.app.selected_agent().is_some()
}

fn command_visible_attach(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard)
        && ctx.app.selected_attach_action().is_some()
}

fn command_visible_search(ctx: CommandCtx<'_>) -> bool {
    ctx.app.current_tab().search(ctx.app).is_some()
}

fn command_visible_queues(ctx: CommandCtx<'_>) -> bool {
    !ctx.app.snapshot.queues.is_empty()
}

fn command_visible_open_selected_dir(ctx: CommandCtx<'_>) -> bool {
    ctx.app.selected_open_dir_action().is_some()
}

fn command_visible_toggle_queue_collapsed(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Queues { .. }) && !ctx.app.snapshot.queues.is_empty()
}

fn command_visible_jump_event(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Timeline { .. })
}

fn command_visible_schema(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Schema { .. })
}

fn command_visible_new_channel(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Channels { .. })
}

fn command_visible_toggle_role(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard) && ctx.app.selected_pause_action().is_some()
}

fn command_visible_trigger_role(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard) && ctx.app.selected_role_slug().is_some()
}

fn command_visible_archive_agent(ctx: CommandCtx<'_>) -> bool {
    matches!(&ctx.app.route, DashboardRoute::Dashboard)
        && ctx.app.selected_archivable_agent().is_some()
}

fn command_text_detail(_ctx: CommandCtx<'_>) -> Option<CommandText> {
    None
}

fn command_text_more(ctx: CommandCtx<'_>) -> Option<CommandText> {
    ctx.app
        .selected_agent()
        .filter(|agent| matches!(agent.status, AgentStatus::Starting | AgentStatus::Running))
        .map(|_| CommandText {
            label: Cow::Borrowed("steer live agent"),
            detail: Cow::Borrowed("send a live query to the selected running agent"),
        })
}

fn command_run_detail(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(match &app.route {
        DashboardRoute::Dashboard => DashboardIntent::Overlay(DashboardOverlay::Detail {
            extended: false,
            scroll: app.remembered_detail_scroll(),
        }),
        DashboardRoute::Channels { .. } => app
            .selected_channel_index()
            .map(|channel_index| {
                DashboardIntent::Overlay(DashboardOverlay::ChannelDetail {
                    channel_index,
                    scroll: 0,
                })
            })
            .unwrap_or(DashboardIntent::None),
        DashboardRoute::Queues { .. } => app
            .queue_selection_rows()
            .get(app.queue_selected)
            .copied()
            .map(|selection| {
                DashboardIntent::Overlay(DashboardOverlay::QueueDetail {
                    selection,
                    scroll: 0,
                })
            })
            .unwrap_or(DashboardIntent::None),
        DashboardRoute::Conversations { .. } => app
            .selected_session()
            .map(|session| {
                DashboardIntent::Overlay(DashboardOverlay::ConversationDetail {
                    session: session.id.clone(),
                    scroll: 0,
                })
            })
            .unwrap_or(DashboardIntent::None),
        DashboardRoute::Schema { .. } | DashboardRoute::Timeline { .. } => DashboardIntent::None,
    })
}

fn command_run_new(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(app.selected_new_action()))
}

fn command_run_more(app: &mut DashboardApp) -> Result<DashboardIntent> {
    app.open_selected_more_composer()?;
    Ok(DashboardIntent::None)
}

fn command_run_attach(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(app.selected_attach_action()))
}

fn command_run_search(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(app
        .current_tab()
        .search(app)
        .map(|search| {
            DashboardIntent::Overlay(DashboardOverlay::Search {
                target: search.target,
                query: search.query,
            })
        })
        .unwrap_or_else(|| {
            DashboardIntent::Toast(DashboardToast::info("current tab has no search"))
        }))
}

fn command_run_channels(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Navigate(DashboardRoute::Channels {
        channel: None,
    }))
}

fn command_run_queues(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Navigate(DashboardRoute::Queues {
        queue: None,
    }))
}

fn command_run_toggle_queue_collapsed(app: &mut DashboardApp) -> Result<DashboardIntent> {
    app.toggle_selected_queue_collapsed();
    Ok(DashboardIntent::None)
}

fn command_run_jump_event(app: &mut DashboardApp) -> Result<DashboardIntent> {
    let events = app.filtered_project_events(app.event_filter, &app.event_query)?;
    if let Some(event) = events.get(app.event_selected) {
        app.jump_to_event(event)?;
    } else {
        app.set_toast(DashboardToast::warn("no event selected"));
    }
    Ok(DashboardIntent::None)
}

fn command_run_timeline(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Navigate(DashboardRoute::Timeline {
        event: None,
    }))
}

fn command_run_schema(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Navigate(DashboardRoute::Schema {
        role: None,
    }))
}

fn command_run_open_project_md(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::OpenPath {
        path: app.project.project_md(),
        label: "PROJECT.md".to_owned(),
    }))
}

fn command_run_open_config(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::OpenPath {
        path: app.project.config(),
        label: "think.toml".to_owned(),
    }))
}

fn command_run_conversations(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Navigate(DashboardRoute::Conversations {
        session: None,
    }))
}

fn command_run_advanced(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Overlay(DashboardOverlay::Advanced))
}

fn command_run_check(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::Check))
}

fn command_run_assist(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::Assist))
}

fn command_run_open_project(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::OpenProject))
}

fn command_run_open_selected_dir(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(app.selected_open_dir_action()))
}

fn command_run_new_channel(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::NewChannel))
}

fn command_run_toggle_role(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(app.selected_pause_action()))
}

fn command_run_toggle_all_roles(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Action(DashboardAction::ToggleAllRoles))
}

fn command_run_trigger_role(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(
        app.selected_role_slug().map(DashboardAction::TriggerRole),
    ))
}

fn command_run_archive_agent(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::from_action(
        app.selected_agent_action(DashboardAction::Archive),
    ))
}

fn command_run_toggle_archived(app: &mut DashboardApp) -> Result<DashboardIntent> {
    app.toggle_archived()?;
    Ok(DashboardIntent::None)
}

fn command_run_retry_errored(app: &mut DashboardApp) -> Result<DashboardIntent> {
    let updated = retry_waits_now_inner(&app.project)?;
    Ok(DashboardIntent::Toast(DashboardToast::success(format!(
        "retry errored requested; updated {updated} agents"
    ))))
}

fn command_run_provider_settings(app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Overlay(
        DashboardOverlay::ProviderSettings {
            selected: app.provider_active_account_index(),
        },
    ))
}

fn command_run_help(_app: &mut DashboardApp) -> Result<DashboardIntent> {
    Ok(DashboardIntent::Overlay(DashboardOverlay::Help))
}

fn command_run_quit(app: &mut DashboardApp) -> Result<DashboardIntent> {
    app.persist_ui_state()?;
    Ok(DashboardIntent::Action(DashboardAction::Quit))
}

pub(in crate::app) struct DashboardApp {
    project: ProjectPaths,
    role_filter: Option<RoleSlug>,
    include_archived: bool,
    selected: usize,
    agents_scroll: usize,
    last_agents_height: usize,
    restore_selection: Option<DashboardSelectionKey>,
    filter: String,
    route: DashboardRoute,
    overlay: DashboardOverlay,
    toast: Option<DashboardToast>,
    snapshot: DashboardSnapshot,
    quota: QuotaLoadState,
    quota_probe: Option<thread::JoinHandle<Option<CodexRateLimits>>>,
    schema_scroll: u16,
    schema_selected: usize,
    event_scroll: u16,
    event_selected: usize,
    event_filter: EventFilter,
    event_query: String,
    event_selected_key: Option<ProjectEventKey>,
    session_query: String,
    session_scroll: u16,
    session_selected: usize,
    session_selected_key: Option<String>,
    channel_query: String,
    channel_scroll: u16,
    channel_selected: usize,
    queue_query: String,
    queue_scroll: u16,
    queue_selected: usize,
    queue_selected_key: Option<StatusQueueKey>,
    collapsed_queues: BTreeSet<String>,
    detail_scrolls: BTreeMap<String, u16>,
    last_queue_height: usize,
    last_state_signature: Option<String>,
    frame_tick: usize,
    seen_alert_latest: Option<String>,
}

impl DashboardApp {
    fn new(
        project: ProjectPaths,
        role_filter: Option<RoleSlug>,
        include_archived: bool,
        initial_tab: DashboardRoute,
    ) -> Self {
        let ui_state = load_dashboard_ui_state(&project).unwrap_or_default();
        let DashboardUiState {
            selected_role,
            selected_agent,
            agents_scroll,
            selected_offset,
            filter,
            seen_alert_latest,
            ..
        } = ui_state;
        let restore_selection = selected_role.map(|role| DashboardSelectionKey {
            role,
            agent: selected_agent,
            selected_offset,
        });
        let codex_config = project_config(&project)
            .map(|config| config.providers.codex)
            .unwrap_or_default();
        Self {
            project,
            role_filter,
            include_archived,
            selected: 0,
            agents_scroll,
            last_agents_height: DASHBOARD_AGENTS_MIN_HEIGHT as usize,
            restore_selection,
            filter,
            route: initial_tab,
            overlay: DashboardOverlay::None,
            toast: None,
            snapshot: DashboardSnapshot::default(),
            quota: QuotaLoadState::Loading,
            quota_probe: Some(thread::spawn(move || {
                crate::provider::codex::load_active_rate_limits(&codex_config)
            })),
            schema_scroll: 0,
            schema_selected: 0,
            event_scroll: 0,
            event_selected: 0,
            event_filter: EventFilter::All,
            event_query: String::new(),
            event_selected_key: None,
            session_query: String::new(),
            session_scroll: 0,
            session_selected: 0,
            session_selected_key: None,
            channel_query: String::new(),
            channel_scroll: 0,
            channel_selected: 0,
            queue_query: String::new(),
            queue_scroll: 0,
            queue_selected: 0,
            queue_selected_key: None,
            collapsed_queues: BTreeSet::new(),
            detail_scrolls: BTreeMap::new(),
            last_queue_height: DASHBOARD_AGENTS_MIN_HEIGHT as usize,
            last_state_signature: None,
            frame_tick: 0,
            seen_alert_latest,
        }
    }

    fn tick_frame(&mut self) {
        self.frame_tick = self.frame_tick.wrapping_add(1);
        self.expire_toast();
    }

    fn refresh(&mut self) -> Result<()> {
        self.expire_toast();
        let selected = self.selected_key();
        let queue_key = self.selected_queue_key();
        wake_project(&self.project)?;
        fire_queue_idle_triggers(&self.project)?;
        ensure_notice_task_started(&self.project)?;
        self.refresh_quota();
        self.snapshot = load_dashboard_snapshot(
            &self.project,
            self.role_filter.as_ref(),
            self.include_archived,
        )?;
        let state_signature = self.snapshot.state_signature();
        if self
            .last_state_signature
            .as_ref()
            .is_some_and(|last| *last != state_signature)
        {
            force_notice_task_started(&self.project)?;
            self.snapshot.notices_loading = true;
        }
        self.last_state_signature = Some(state_signature);
        if self.restore_selection.is_none() {
            self.restore_selection = selected;
        }
        self.restore_selection_if_pending();
        self.restore_queue_selection(queue_key);
        self.ensure_selectable_selection();
        Ok(())
    }

    fn expire_toast(&mut self) {
        if self.toast.as_ref().is_some_and(|toast| !toast.is_visible()) {
            self.toast = None;
        }
    }

    fn refresh_quota(&mut self) {
        if self
            .quota_probe
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            let handle = self.quota_probe.take().expect("quota probe was checked");
            self.quota = QuotaLoadState::Ready(handle.join().unwrap_or(None));
        }
    }

    fn set_toast(&mut self, toast: DashboardToast) {
        self.toast = Some(toast);
    }

    fn current_tab(&self) -> &'static dyn DashboardTab {
        self.route.tab()
    }

    pub(in crate::app) fn command_ctx(&self) -> CommandCtx<'_> {
        CommandCtx::new(self)
    }

    fn navigate(&mut self, route: DashboardRoute) -> Result<()> {
        route.tab().enter(self, &route)?;
        self.route = route;
        Ok(())
    }

    fn apply_intent(&mut self, intent: DashboardIntent) -> Result<Option<DashboardAction>> {
        match intent {
            DashboardIntent::None => Ok(None),
            DashboardIntent::Navigate(route) => {
                self.navigate(route)?;
                Ok(None)
            }
            DashboardIntent::Overlay(overlay) => {
                self.overlay = overlay;
                Ok(None)
            }
            DashboardIntent::Action(action) => Ok(Some(action)),
            DashboardIntent::Toast(toast) => {
                self.set_toast(toast);
                Ok(None)
            }
        }
    }

    fn open_queues(&mut self) -> Result<()> {
        self.navigate(DashboardRoute::Queues {
            queue: self.selected_queue_key(),
        })
    }

    fn open_channels(&mut self) -> Result<()> {
        self.navigate(DashboardRoute::Channels { channel: None })
    }

    fn open_dashboard(&mut self) -> Result<()> {
        self.navigate(DashboardRoute::Dashboard)
    }

    fn open_schema(&mut self) -> Result<()> {
        self.navigate(DashboardRoute::Schema { role: None })
    }

    fn open_conversations(&mut self) -> Result<()> {
        self.navigate(DashboardRoute::Conversations { session: None })
    }

    fn open_provider_settings(&mut self) {
        self.overlay = DashboardOverlay::ProviderSettings {
            selected: self.provider_active_account_index(),
        };
    }

    fn provider_account_rows(&self) -> Result<Vec<ProviderAccountRow>> {
        let state = crate::provider::codex::list_accounts()?;
        Ok(state
            .accounts
            .into_iter()
            .map(|(name, account)| ProviderAccountRow {
                active: name == state.active_account,
                name,
                codex_home: account.codex_home,
                quota_wait_until: account.quota_wait_until,
                last_quota_event: account.last_quota_event,
                last_used_at: account.last_used_at,
            })
            .collect())
    }

    fn provider_active_account_index(&self) -> usize {
        self.provider_account_rows()
            .ok()
            .and_then(|rows| rows.iter().position(|row| row.active))
            .unwrap_or(0)
    }

    fn open_detail_overlay(&mut self, extended: bool) {
        self.overlay = DashboardOverlay::Detail {
            extended,
            scroll: self.remembered_detail_scroll(),
        };
    }

    fn keep_detail_overlay(&mut self, extended: bool, scroll: u16) {
        self.remember_detail_scroll(scroll);
        self.overlay = DashboardOverlay::Detail { extended, scroll };
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Result<Option<DashboardAction>> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            self.persist_ui_state()?;
            return Ok(Some(DashboardAction::Quit));
        }
        if !matches!(self.overlay, DashboardOverlay::None) {
            return self.handle_overlay_key(key);
        }
        if matches!(key.code, KeyCode::Tab | KeyCode::BackTab) {
            let route = if matches!(key.code, KeyCode::BackTab) {
                self.route.previous()
            } else {
                self.route.next()
            };
            return self.apply_intent(DashboardIntent::Navigate(route));
        }
        let tab = self.current_tab();
        let intent = tab.handle_key(self, key)?;
        self.apply_intent(intent)
    }

    fn handle_overview_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            match movement {
                SelectionMove::Home => self.select_first(),
                SelectionMove::End => self.select_last(),
                SelectionMove::Previous => self.move_selection_by(-1),
                SelectionMove::Next => self.move_selection_by(1),
                SelectionMove::PagePrevious => self.move_selection_by(-self.page_step()),
                SelectionMove::PageNext => self.move_selection_by(self.page_step()),
            }
            self.persist_ui_state()?;
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Char('?') => {
                self.overlay = DashboardOverlay::Help;
                Ok(None)
            }
            KeyCode::Char(':') => {
                self.overlay = DashboardOverlay::Palette {
                    query: String::new(),
                    selected: 0,
                };
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Agents,
                    query: self.filter.clone(),
                };
                Ok(None)
            }
            KeyCode::Char('o') => {
                self.open_queues()?;
                Ok(None)
            }
            KeyCode::Char('l') => {
                self.open_channels()?;
                Ok(None)
            }
            KeyCode::Char('s') => {
                self.open_schema()?;
                Ok(None)
            }
            KeyCode::Char('C') => {
                self.open_conversations()?;
                Ok(None)
            }
            KeyCode::Char('!') => {
                if self.snapshot.notices.is_empty() && self.has_unseen_alerts() {
                    self.open_channels()?;
                } else if self.snapshot.notices.is_empty() {
                    self.set_toast(DashboardToast::info("no operator notices"));
                } else {
                    self.overlay = DashboardOverlay::NoticeDetail { scroll: 0 };
                }
                Ok(None)
            }
            KeyCode::Enter => {
                self.open_detail_overlay(false);
                Ok(None)
            }
            KeyCode::Char('a') => Ok(self.selected_attach_action()),
            KeyCode::Char('m') => {
                self.open_selected_more_composer()?;
                Ok(None)
            }
            KeyCode::Char('n') => Ok(self.selected_new_action()),
            KeyCode::Char('c') => Ok(Some(DashboardAction::Check)),
            KeyCode::Char('i') => Ok(Some(DashboardAction::Assist)),
            KeyCode::Char(' ') => Ok(self.selected_pause_action()),
            KeyCode::Char('U') => Ok(Some(DashboardAction::ToggleAllRoles)),
            KeyCode::Char('A') => {
                self.overlay = DashboardOverlay::Advanced;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_overlay_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        match std::mem::take(&mut self.overlay) {
            DashboardOverlay::None => Ok(None),
            DashboardOverlay::Advanced => self.handle_advanced_key(key),
            DashboardOverlay::Help => self.handle_help_key(key),
            DashboardOverlay::Search { target, mut query } => {
                self.handle_search_key(key, target, &mut query)
            }
            DashboardOverlay::Palette {
                mut query,
                mut selected,
            } => self.handle_palette_key(key, &mut query, &mut selected),
            DashboardOverlay::Composer {
                agent,
                running,
                composer,
            } => self.handle_dashboard_composer_key(key, agent, running, composer),
            DashboardOverlay::Detail {
                mut extended,
                mut scroll,
            } => self.handle_detail_key(key, &mut extended, &mut scroll),
            DashboardOverlay::QueueDetail {
                selection,
                mut scroll,
            } => self.handle_queue_detail_key(key, selection, &mut scroll),
            DashboardOverlay::ChannelDetail {
                channel_index,
                mut scroll,
            } => self.handle_channel_detail_key(key, channel_index, &mut scroll),
            DashboardOverlay::NoticeDetail { mut scroll } => {
                self.handle_notice_detail_key(key, &mut scroll)
            }
            DashboardOverlay::ConversationDetail {
                session,
                mut scroll,
            } => self.handle_session_detail_key(key, session, &mut scroll),
            DashboardOverlay::ProviderSettings { mut selected } => {
                self.handle_provider_settings_key(key, &mut selected)
            }
        }
    }

    fn handle_advanced_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('A') => Ok(None),
            KeyCode::Char('r') => {
                let updated = retry_waits_now_inner(&self.project)?;
                self.set_toast(DashboardToast::success(format!(
                    "retry errored requested; updated {updated} agents"
                )));
                Ok(None)
            }
            KeyCode::Char('x') => {
                self.toggle_archived()?;
                Ok(None)
            }
            KeyCode::Char('n') => Ok(Some(DashboardAction::NewRole)),
            KeyCode::Char('o') => Ok(Some(DashboardAction::OpenProject)),
            KeyCode::Char('p') => {
                self.open_provider_settings();
                Ok(None)
            }
            _ => {
                self.overlay = DashboardOverlay::Advanced;
                Ok(None)
            }
        }
    }

    fn handle_provider_settings_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        selected: &mut usize,
    ) -> Result<Option<DashboardAction>> {
        let rows = self.provider_account_rows().unwrap_or_default();
        *selected = (*selected).min(rows.len().saturating_sub(1));
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            movement.apply_to_index(selected, rows.len(), self.last_agents_height);
            self.keep_provider_settings_overlay(*selected);
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Enter | KeyCode::Char('s') => {
                if let Some(account) = rows.get(*selected) {
                    crate::provider::codex::set_active_account(&account.name)?;
                    self.set_toast(DashboardToast::success(format!(
                        "active Codex account: `{}`",
                        account.name
                    )));
                    self.keep_provider_settings_overlay(self.provider_active_account_index());
                } else {
                    self.set_toast(DashboardToast::warn("no Codex account selected"));
                    self.keep_provider_settings_overlay(0);
                }
                Ok(None)
            }
            KeyCode::Char('a') => Ok(Some(DashboardAction::CodexLogin)),
            KeyCode::Char('d') | KeyCode::Delete => {
                if let Some(account) = rows.get(*selected) {
                    match crate::provider::codex::remove_account(&account.name) {
                        Ok(()) => {
                            self.set_toast(DashboardToast::success(format!(
                                "removed Codex account `{}`",
                                account.name
                            )));
                            *selected = (*selected).min(
                                self.provider_account_rows()
                                    .map_or(0, |rows| rows.len())
                                    .saturating_sub(1),
                            );
                        }
                        Err(err) => self.set_toast(DashboardToast::warn(format!("{err:#}"))),
                    }
                } else {
                    self.set_toast(DashboardToast::warn("no Codex account selected"));
                    *selected = 0;
                }
                self.keep_provider_settings_overlay(*selected);
                Ok(None)
            }
            KeyCode::Char('m') | KeyCode::Char('c') => Ok(Some(DashboardAction::CodexConfig)),
            KeyCode::Char('r') => {
                *selected = self.provider_active_account_index();
                self.keep_provider_settings_overlay(*selected);
                Ok(None)
            }
            _ => {
                self.keep_provider_settings_overlay(*selected);
                Ok(None)
            }
        }
    }

    fn handle_help_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if matches!(
            key.code,
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('?')
        ) {
            Ok(None)
        } else {
            self.overlay = DashboardOverlay::Help;
            Ok(None)
        }
    }

    fn keep_search_overlay(&mut self, target: SearchTarget, query: &str) {
        self.overlay = DashboardOverlay::Search {
            target,
            query: query.to_owned(),
        };
    }

    fn keep_palette_overlay(&mut self, query: &str, selected: usize) {
        self.overlay = DashboardOverlay::Palette {
            query: query.to_owned(),
            selected,
        };
    }

    fn keep_provider_settings_overlay(&mut self, selected: usize) {
        self.overlay = DashboardOverlay::ProviderSettings { selected };
    }

    fn handle_search_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        target: SearchTarget,
        query: &mut String,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.apply_search(target, query)?;
                Ok(None)
            }
            _ => {
                if let Some(edit) = TextEdit::from_key(&key) {
                    edit.apply(query);
                    self.apply_search(target, query)?;
                }
                self.keep_search_overlay(target, query);
                Ok(None)
            }
        }
    }

    fn handle_palette_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        query: &mut String,
        selected: &mut usize,
    ) -> Result<Option<DashboardAction>> {
        let entries = self.palette_entries(query);
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            movement.apply_to_index(selected, entries.len(), self.last_agents_height);
            self.keep_palette_overlay(query, *selected);
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Enter => {
                let Some(entry) = entries.get((*selected).min(entries.len().saturating_sub(1)))
                else {
                    return Ok(None);
                };
                let command = entry.command;
                let intent = command.run(self)?;
                self.apply_intent(intent)
            }
            _ => {
                if let Some(edit) = TextEdit::from_key(&key) {
                    edit.apply(query);
                    *selected = if matches!(edit, TextEdit::Backspace) {
                        (*selected).min(self.palette_entries(query).len().saturating_sub(1))
                    } else {
                        0
                    };
                }
                self.keep_palette_overlay(query, *selected);
                Ok(None)
            }
        }
    }

    fn handle_dashboard_composer_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        agent: ResolvedAgent,
        running: bool,
        mut composer: AttachComposer,
    ) -> Result<Option<DashboardAction>> {
        match key.code {
            KeyCode::Esc => {
                self.set_toast(DashboardToast::info("follow-up cancelled"));
                Ok(None)
            }
            KeyCode::Char(ch)
                if key.modifiers.contains(KeyModifiers::CONTROL) && (ch == 'd' || ch == 'a') =>
            {
                let query = composer.text();
                if query.trim().is_empty() {
                    self.set_toast(DashboardToast::info("blank follow-up cancelled"));
                    if ch == 'a' {
                        self.keep_dashboard_composer(agent, running, composer);
                    }
                    return Ok(None);
                }
                if ch == 'a' && !running {
                    self.set_toast(DashboardToast::warn(
                        "steer and attach is only available for running agents",
                    ));
                    self.keep_dashboard_composer(agent, running, composer);
                    return Ok(None);
                }
                crate::input::history::append(&self.project, "followups", &query)?;
                let agent_paths = RolePaths::new(self.project.clone(), agent.role.clone())
                    .agent(agent.agent.clone());
                if matches!(
                    load_agent(&agent_paths)?.status,
                    AgentStatus::Starting | AgentStatus::Running
                ) {
                    crate::backend::enqueue_steer(&agent_paths.steer_dir(), &query)?;
                    if ch == 'a' {
                        return Ok(Some(DashboardAction::AttachAgent(agent)));
                    }
                    self.set_toast(DashboardToast::success(format!(
                        "sent live steer to {}",
                        agent.label()
                    )));
                    return Ok(None);
                }
                if ch == 'a' {
                    self.set_toast(DashboardToast::warn("agent is no longer running"));
                    self.keep_dashboard_composer(agent, running, composer);
                    return Ok(None);
                }
                Ok(Some(DashboardAction::MoreWithQuery(agent, query)))
            }
            _ => {
                composer.apply_edit_key(key);
                self.keep_dashboard_composer(agent, running, composer);
                Ok(None)
            }
        }
    }

    fn keep_dashboard_composer(
        &mut self,
        agent: ResolvedAgent,
        running: bool,
        composer: AttachComposer,
    ) {
        self.overlay = DashboardOverlay::Composer {
            agent,
            running,
            composer,
        };
    }

    fn apply_detail_scroll<T: DetailOverlayTarget>(
        &mut self,
        key: &crossterm::event::KeyEvent,
        target: &T,
        scroll: &mut u16,
    ) -> bool {
        let Some(movement) = ScrollMove::from_key(&key.code, T::HOME_ENABLED) else {
            return false;
        };
        movement.apply(
            scroll,
            DASHBOARD_DETAIL_SCROLL_STEP,
            self.last_agents_height,
        );
        self.retain_detail_overlay(target, *scroll);
        true
    }

    fn retain_detail_overlay<T: DetailOverlayTarget>(&mut self, target: &T, scroll: u16) {
        self.overlay = target.overlay(scroll);
    }

    fn continue_detail_overlay<T: DetailOverlayTarget>(
        &mut self,
        target: &T,
        scroll: u16,
    ) -> Result<Option<DashboardAction>> {
        self.retain_detail_overlay(target, scroll);
        Ok(None)
    }

    fn apply_list_tab_move<T: DashboardListTab>(&mut self, movement: SelectionMove) {
        T::restore(self);
        let count = T::count(self);
        let height = self.last_agents_height;
        {
            let (selected, scroll) = T::selection(self);
            movement.apply_to_scrolled_index(selected, scroll, count, height);
            if T::CENTER_SELECTED && movement.should_center_after_move() {
                center_selected_row(scroll, *selected, height);
            }
        }
        T::remember(self);
    }

    fn handle_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        extended: &mut bool,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = ScrollMove::from_key(&key.code, true) {
            movement.apply(
                scroll,
                DASHBOARD_DETAIL_SCROLL_STEP,
                self.last_agents_height,
            );
            self.keep_detail_overlay(*extended, *scroll);
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => Ok(None),
            KeyCode::Char('x') => {
                *extended = !*extended;
                *scroll = 0;
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
            KeyCode::Char('a') => Ok(self.selected_attach_action()),
            KeyCode::Char('m') => {
                self.open_selected_more_composer()?;
                Ok(None)
            }
            KeyCode::Char('n') => Ok(self.selected_new_action()),
            KeyCode::Char(' ') => Ok(self.selected_pause_action()),
            _ => {
                self.keep_detail_overlay(*extended, *scroll);
                Ok(None)
            }
        }
    }

    fn handle_queue_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        selection: QueueSelection,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        let target = QueueDetailTarget { selection };
        if self.apply_detail_scroll(&key, &target, scroll) {
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            _ => self.continue_detail_overlay(&target, *scroll),
        }
    }

    fn handle_channel_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        channel_index: usize,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        let target = ChannelDetailTarget { channel_index };
        if self.apply_detail_scroll(&key, &target, scroll) {
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Char('o') => {
                let channel = self.snapshot.channels.get(channel_index);
                Ok(channel.map(|channel| DashboardAction::OpenPath {
                    path: self.project.channels_dir().join(&channel.name),
                    label: format!("channel `{}`", channel.name),
                }))
            }
            _ => self.continue_detail_overlay(&target, *scroll),
        }
    }

    fn handle_notice_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        let target = NoticeDetailTarget;
        if self.apply_detail_scroll(&key, &target, scroll) {
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            _ => self.continue_detail_overlay(&target, *scroll),
        }
    }

    fn handle_schema_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            self.apply_list_tab_move::<SchemaListTab>(movement);
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Esc | KeyCode::Char('s') => {
                self.open_dashboard()?;
                Ok(None)
            }
            KeyCode::Char('p') => Ok(Some(DashboardAction::OpenPath {
                path: self.project.project_md(),
                label: "PROJECT.md".to_owned(),
            })),
            KeyCode::Char('t') => Ok(Some(DashboardAction::OpenPath {
                path: self.project.config(),
                label: "think.toml".to_owned(),
            })),
            KeyCode::Char('o') | KeyCode::Enter => {
                Ok(self
                    .selected_schema_role()
                    .map(|role| DashboardAction::OpenPath {
                        path: self.project.role_dir(&role.slug),
                        label: format!("role `{}`", role.slug),
                    }))
            }
            _ => Ok(None),
        }
    }

    fn handle_queue_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            match movement {
                SelectionMove::PagePrevious => {
                    self.queue_scroll = self
                        .queue_scroll
                        .saturating_sub(usize_to_u16(self.last_agents_height));
                }
                SelectionMove::PageNext => {
                    self.queue_scroll = self
                        .queue_scroll
                        .saturating_add(usize_to_u16(self.last_agents_height));
                }
                SelectionMove::Home
                | SelectionMove::End
                | SelectionMove::Previous
                | SelectionMove::Next => {
                    let row_count = self.queue_selection_rows().len();
                    movement.apply_to_index(
                        &mut self.queue_selected,
                        row_count,
                        self.last_agents_height,
                    );
                    self.remember_queue_selection();
                }
            }
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Enter => {
                if let Some(selection) = self.queue_selection_rows().get(self.queue_selected) {
                    self.overlay = DashboardOverlay::QueueDetail {
                        selection: *selection,
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Char(' ') => {
                self.toggle_selected_queue_collapsed();
                self.center_queue_selection();
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Queues,
                    query: self.queue_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Esc | KeyCode::Char('o') => {
                self.open_dashboard()?;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_channel_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            self.apply_list_tab_move::<ChannelListTab>(movement);
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Enter => {
                if let Some(channel_index) = self.selected_channel_index() {
                    self.overlay = DashboardOverlay::ChannelDetail {
                        channel_index,
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Char('o') => Ok(self.selected_open_dir_action()),
            KeyCode::Char('n') => Ok(Some(DashboardAction::NewChannel)),
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Channels,
                    query: self.channel_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Esc | KeyCode::Char('l') => {
                self.open_dashboard()?;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_event_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        let events = self.filtered_project_events(self.event_filter, &self.event_query)?;
        restore_event_selection(
            &events,
            &mut self.event_selected,
            self.event_selected_key.as_ref(),
        );
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            match movement {
                SelectionMove::Previous => {
                    movement.apply_to_index(
                        &mut self.event_selected,
                        events.len(),
                        self.last_agents_height,
                    );
                    self.event_scroll = self
                        .event_scroll
                        .saturating_sub(DASHBOARD_EVENT_SCROLL_STEP);
                }
                SelectionMove::Next => {
                    movement.apply_to_index(
                        &mut self.event_selected,
                        events.len(),
                        self.last_agents_height,
                    );
                    self.event_scroll = self
                        .event_scroll
                        .saturating_add(DASHBOARD_EVENT_SCROLL_STEP);
                }
                SelectionMove::Home
                | SelectionMove::End
                | SelectionMove::PagePrevious
                | SelectionMove::PageNext => {
                    movement.apply_to_scrolled_index(
                        &mut self.event_selected,
                        &mut self.event_scroll,
                        events.len(),
                        self.last_agents_height,
                    );
                    if movement.should_center_after_move() {
                        center_selected_row(
                            &mut self.event_scroll,
                            self.event_selected,
                            self.last_agents_height,
                        );
                    }
                }
            }
            self.remember_event_selection(&events);
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Esc => {
                self.open_dashboard()?;
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Events,
                    query: self.event_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Char('0') => {
                self.set_event_filter(EventFilter::All)?;
                Ok(None)
            }
            KeyCode::Char('a') => {
                self.set_event_filter(EventFilter::Agents)?;
                Ok(None)
            }
            KeyCode::Char('r') => {
                self.set_event_filter(EventFilter::Runs)?;
                Ok(None)
            }
            KeyCode::Char('t') => {
                self.set_event_filter(EventFilter::Triggers)?;
                Ok(None)
            }
            KeyCode::Char('n') => {
                self.set_event_filter(EventFilter::Notices)?;
                Ok(None)
            }
            KeyCode::Enter => {
                if let Some(event) = events.get(self.event_selected).cloned() {
                    self.jump_to_event(&event)?;
                } else {
                    self.set_toast(DashboardToast::warn("no event selected"));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_session_tab_key(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> Result<Option<DashboardAction>> {
        if let Some(movement) = SelectionMove::from_key(&key.code) {
            self.apply_list_tab_move::<SessionListTab>(movement);
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => {
                self.persist_ui_state()?;
                Ok(Some(DashboardAction::Quit))
            }
            KeyCode::Esc | KeyCode::Char('C') => {
                self.open_dashboard()?;
                Ok(None)
            }
            KeyCode::Char('/') => {
                self.overlay = DashboardOverlay::Search {
                    target: SearchTarget::Conversations,
                    query: self.session_query.clone(),
                };
                Ok(None)
            }
            KeyCode::Enter => {
                if let Some(session) = self.selected_session() {
                    self.overlay = DashboardOverlay::ConversationDetail {
                        session: session.id.clone(),
                        scroll: 0,
                    };
                }
                Ok(None)
            }
            KeyCode::Char('o') => {
                Ok(self
                    .selected_session()
                    .map(|session| DashboardAction::OpenPath {
                        path: session.root.clone(),
                        label: format!("conversation `{}`", session.id),
                    }))
            }
            KeyCode::Char('y') => {
                if let Some(path) = self
                    .selected_session()
                    .and_then(|session| session.transcript.clone())
                {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.set_toast(DashboardToast::success("copied transcript path"));
                } else {
                    self.set_toast(DashboardToast::warn("no transcript path available"));
                }
                Ok(None)
            }
            KeyCode::Char('Y') => {
                if let Some(path) = self
                    .selected_session()
                    .and_then(|session| session.reply.clone())
                {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.set_toast(DashboardToast::success("copied reply path"));
                } else {
                    self.set_toast(DashboardToast::warn("no reply path available"));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn handle_session_detail_key(
        &mut self,
        key: crossterm::event::KeyEvent,
        session: String,
        scroll: &mut u16,
    ) -> Result<Option<DashboardAction>> {
        let target = ConversationDetailTarget { session };
        if self.apply_detail_scroll(&key, &target, scroll) {
            return Ok(None);
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => Ok(None),
            KeyCode::Char('o') => {
                if let Some(record) = self.session_by_id(&target.session) {
                    open_path(&record.root)?;
                    self.set_toast(DashboardToast::success("opened conversation directory"));
                } else {
                    self.set_toast(DashboardToast::warn("conversation no longer exists"));
                }
                self.continue_detail_overlay(&target, *scroll)
            }
            KeyCode::Char('y') => {
                if let Some(path) = self
                    .session_by_id(&target.session)
                    .and_then(|record| record.transcript.clone())
                {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.set_toast(DashboardToast::success("copied transcript path"));
                } else {
                    self.set_toast(DashboardToast::warn("no transcript path available"));
                }
                self.continue_detail_overlay(&target, *scroll)
            }
            KeyCode::Char('Y') => {
                if let Some(path) = self
                    .session_by_id(&target.session)
                    .and_then(|record| record.reply.clone())
                {
                    copy_to_clipboard(&path.display().to_string())?;
                    self.set_toast(DashboardToast::success("copied reply path"));
                } else {
                    self.set_toast(DashboardToast::warn("no reply path available"));
                }
                self.continue_detail_overlay(&target, *scroll)
            }
            _ => self.continue_detail_overlay(&target, *scroll),
        }
    }

    fn restore_session_selection(&mut self) {
        let indices = self.filtered_session_indices();
        if let Some(key) = self.session_selected_key.as_deref()
            && let Some(index) = indices
                .iter()
                .position(|snapshot_index| self.snapshot.sessions[*snapshot_index].id == key)
        {
            self.session_selected = index;
        }
        self.session_selected = self.session_selected.min(indices.len().saturating_sub(1));
        self.center_session_selection();
        self.remember_session_selection();
    }

    fn remember_session_selection(&mut self) {
        self.session_selected_key = self.selected_session().map(|session| session.id.clone());
    }

    fn center_session_selection(&mut self) {
        center_selected_row(
            &mut self.session_scroll,
            self.session_selected,
            self.last_agents_height,
        );
    }

    fn set_event_filter(&mut self, filter: EventFilter) -> Result<()> {
        self.event_filter = filter;
        self.event_selected = 0;
        self.event_scroll = 0;
        self.restore_event_selection();
        Ok(())
    }

    fn restore_event_selection(&mut self) {
        if let Ok(events) = self.filtered_project_events(self.event_filter, &self.event_query) {
            restore_event_selection(
                &events,
                &mut self.event_selected,
                self.event_selected_key.as_ref(),
            );
            self.remember_event_selection(&events);
        }
    }

    fn remember_event_selection(&mut self, events: &[ProjectEvent]) {
        self.event_selected_key = events.get(self.event_selected).map(ProjectEvent::key);
    }

    fn apply_filter(&mut self, query: &str) {
        let selected = self.selected_key();
        self.filter = query.trim().to_owned();
        if let Some(key) = selected {
            self.restore_selection = Some(key);
            self.restore_selection_if_pending();
        }
        self.ensure_selectable_selection();
        self.ensure_selected_visible();
    }

    fn apply_search(&mut self, target: SearchTarget, query: &str) -> Result<()> {
        match target {
            SearchTarget::Agents => {
                self.apply_filter(query);
                self.persist_ui_state()?;
            }
            SearchTarget::Channels => {
                self.channel_query = query.trim().to_owned();
                self.restore_channel_selection();
            }
            SearchTarget::Queues => {
                self.queue_query = query.trim().to_owned();
                self.restore_queue_selection(self.queue_selected_key.clone());
                self.center_queue_selection();
            }
            SearchTarget::Events => {
                self.event_query = query.trim().to_owned();
                self.event_selected = 0;
                self.event_scroll = 0;
                self.restore_event_selection();
            }
            SearchTarget::Conversations => {
                self.session_query = query.trim().to_owned();
                self.session_selected = 0;
                self.session_scroll = 0;
                self.restore_session_selection();
            }
        }
        Ok(())
    }

    fn search_visible_count(&self, target: SearchTarget) -> usize {
        match target {
            SearchTarget::Agents => self.visible_agent_count(),
            SearchTarget::Channels => self.filtered_channel_indices().len(),
            SearchTarget::Queues => self.queue_selection_rows().len(),
            SearchTarget::Events => self
                .filtered_project_events(self.event_filter, &self.event_query)
                .map(|events| events.len())
                .unwrap_or_default(),
            SearchTarget::Conversations => self.filtered_session_indices().len(),
        }
    }

    fn toggle_archived(&mut self) -> Result<()> {
        self.include_archived = !self.include_archived;
        self.set_toast(DashboardToast::info(if self.include_archived {
            "showing archived agents"
        } else {
            "hiding archived agents"
        }));
        self.refresh()?;
        self.persist_ui_state()
    }

    fn palette_entries(&self, query: &str) -> Vec<PaletteEntry> {
        let ctx = self.command_ctx();
        let mut entries = self
            .commands(CommandSurface::Palette)
            .map(|command| PaletteEntry {
                command,
                group: command.group(),
                label: command.label(ctx).into_owned(),
                detail: command.detail(ctx).into_owned(),
                key: command.key(ctx),
            })
            .collect::<Vec<_>>();
        let query = query.trim().to_ascii_lowercase();
        if !query.is_empty() {
            entries.retain(|entry| {
                query.split_whitespace().all(|term| {
                    entry.label.to_ascii_lowercase().contains(term)
                        || entry.detail.to_ascii_lowercase().contains(term)
                        || entry.command.id().contains(term)
                        || entry
                            .key
                            .is_some_and(|key| key.to_ascii_lowercase().contains(term))
                        || entry
                            .command
                            .aliases()
                            .iter()
                            .any(|alias| alias.contains(term))
                })
            });
        }
        entries
    }

    pub(in crate::app) fn commands(
        &self,
        surface: CommandSurface,
    ) -> impl Iterator<Item = &'static CommandDef> + '_ {
        COMMAND_REGISTRY.visible(self.command_ctx(), surface)
    }

    fn restore_selection_if_pending(&mut self) {
        let Some(key) = self.restore_selection.take() else {
            return;
        };
        let rows = self.visible_rows();
        if let Some(index) = rows
            .iter()
            .position(|row| self.selection_matches_key(*row, &key))
        {
            self.selected = index;
            self.agents_scroll = self
                .selected
                .saturating_sub(self.restore_anchor_offset(&key));
            return;
        }
        if key.agent.is_some()
            && let Some(index) = rows.iter().position(|row| {
                matches!(
                    row,
                    DashboardSelection::Role(role_index)
                        if self.snapshot.roles[*role_index].slug == key.role
                )
            })
        {
            self.selected = index;
            self.agents_scroll = self
                .selected
                .saturating_sub(self.restore_anchor_offset(&key));
        }
    }

    fn restore_anchor_offset(&self, key: &DashboardSelectionKey) -> usize {
        let contextual_offset = self
            .last_agents_height
            .saturating_sub(1)
            .min(self.last_agents_height / 3)
            .max(3);
        key.selected_offset
            .max(contextual_offset)
            .min(self.selected)
    }

    fn selection_matches_key(&self, row: DashboardSelection, key: &DashboardSelectionKey) -> bool {
        match row {
            DashboardSelection::Role(role_index) => {
                key.agent.is_none() && self.snapshot.roles[role_index].slug == key.role
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                key.agent.as_ref().is_some_and(|agent| {
                    let selected = &self.snapshot.roles[role_index].agents[agent_index];
                    selected.role == key.role && selected.agent == *agent
                })
            }
            DashboardSelection::Spacer => false,
        }
    }

    fn ensure_selectable_selection(&mut self) {
        let rows = self.visible_rows();
        if rows.is_empty() {
            self.selected = 0;
            self.agents_scroll = 0;
            return;
        }
        if self.selected < rows.len() && rows[self.selected].is_selectable() {
            return;
        }
        self.selected = rows.iter().position(|row| row.is_selectable()).unwrap_or(0);
    }

    fn selectable_indices(&self) -> Vec<usize> {
        self.visible_rows()
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.is_selectable().then_some(index))
            .collect()
    }

    fn select_first(&mut self) {
        if let Some(index) = self.selectable_indices().first() {
            self.selected = *index;
            self.ensure_selected_visible();
        }
    }

    fn select_last(&mut self) {
        if let Some(index) = self.selectable_indices().last() {
            self.selected = *index;
            self.ensure_selected_visible();
        }
    }

    fn move_selection_by(&mut self, delta: isize) {
        let indices = self.selectable_indices();
        if indices.is_empty() {
            self.selected = 0;
            self.agents_scroll = 0;
            return;
        }
        let position = indices
            .iter()
            .position(|index| *index == self.selected)
            .unwrap_or(0);
        let next = if delta.is_negative() {
            position.saturating_sub(delta.unsigned_abs())
        } else {
            (position + delta as usize).min(indices.len() - 1)
        };
        self.selected = indices[next];
        self.ensure_selected_visible();
    }

    fn page_step(&self) -> isize {
        self.last_agents_height
            .saturating_sub(DASHBOARD_PAGE_OVERLAP_ROWS)
            .max(DASHBOARD_MIN_VISIBLE_ROWS) as isize
    }

    fn ensure_selected_visible(&mut self) {
        let rows_len = self.visible_rows().len();
        if rows_len == 0 {
            self.agents_scroll = 0;
            return;
        }
        self.agents_scroll = self.agents_scroll.min(rows_len - 1);
        let height = self.last_agents_height.max(DASHBOARD_MIN_VISIBLE_ROWS);
        if self.selected < self.agents_scroll {
            self.agents_scroll = self.selected;
        } else if self.selected >= self.agents_scroll + height {
            self.agents_scroll = self.selected + 1 - height;
        }
    }

    fn selected_key(&self) -> Option<DashboardSelectionKey> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardSelectionKey {
                role: self.snapshot.roles[*role_index].slug.clone(),
                agent: None,
                selected_offset: self.selected.saturating_sub(self.agents_scroll),
            }),
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                Some(DashboardSelectionKey {
                    role: agent.role.clone(),
                    agent: Some(agent.agent.clone()),
                    selected_offset: self.selected.saturating_sub(self.agents_scroll),
                })
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn remembered_detail_scroll(&self) -> u16 {
        self.detail_scroll_key()
            .and_then(|key| self.detail_scrolls.get(&key).copied())
            .unwrap_or(0)
    }

    fn remember_detail_scroll(&mut self, scroll: u16) {
        if let Some(key) = self.detail_scroll_key() {
            self.detail_scrolls.insert(key, scroll);
        }
    }

    fn detail_scroll_key(&self) -> Option<String> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => {
                Some(format!("role/{}", self.snapshot.roles[*role_index].slug))
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                Some(format!("agent/{}/{}", agent.role, agent.agent))
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_role_slug(&self) -> Option<RoleSlug> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => {
                Some(self.snapshot.roles[*role_index].slug.clone())
            }
            DashboardSelection::Agent(role_index, _) => {
                Some(self.snapshot.roles[*role_index].slug.clone())
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn persist_ui_state(&self) -> Result<()> {
        let selected = self.selected_key();
        let selected_role = selected.as_ref().map(|key| key.role.clone());
        let selected_agent = selected.and_then(|key| key.agent);
        io::write_toml(
            &dashboard_ui_state_path(&self.project),
            &DashboardUiState {
                version: DASHBOARD_UI_STATE_VERSION,
                selected_role,
                selected_agent,
                agents_scroll: self.agents_scroll,
                selected_offset: self.selected.saturating_sub(self.agents_scroll),
                filter: self.filter.clone(),
                seen_alert_latest: self.seen_alert_latest.clone(),
            },
        )
    }

    fn selected_attach_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::AttachRole(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, agent_index) => Some(
                DashboardAction::AttachAgent(self.resolved_agent(*role_index, *agent_index)),
            ),
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_more_target(&self) -> Option<(ResolvedAgent, bool)> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(_) => None,
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                Some((
                    self.resolved_agent(*role_index, *agent_index),
                    matches!(agent.status, AgentStatus::Starting | AgentStatus::Running),
                ))
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn open_selected_more_composer(&mut self) -> Result<()> {
        let Some((agent, running)) = self.selected_more_target() else {
            self.set_toast(DashboardToast::warn("select an agent first"));
            return Ok(());
        };
        self.overlay = DashboardOverlay::Composer {
            agent,
            running,
            composer: AttachComposer::new(crate::input::history::load(&self.project, "followups")?),
        };
        Ok(())
    }

    fn selected_new_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::New(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, _) => Some(DashboardAction::New(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Spacer => None,
        }
    }

    fn selected_pause_action(&self) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Role(role_index) => Some(DashboardAction::ToggleRole(
                self.snapshot.roles[*role_index].slug.clone(),
            )),
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                (agent.paused_by_user
                    || matches!(
                        agent.status,
                        AgentStatus::Starting | AgentStatus::Running | AgentStatus::Paused
                    ))
                .then(|| {
                    DashboardAction::ToggleAgent(self.resolved_agent(*role_index, *agent_index))
                })
            }
            DashboardSelection::Spacer => None,
        }
    }

    fn resolved_agent(&self, role_index: usize, agent_index: usize) -> ResolvedAgent {
        let agent = &self.snapshot.roles[role_index].agents[agent_index];
        ResolvedAgent {
            role: agent.role.clone(),
            agent: agent.agent.clone(),
        }
    }

    fn selected_agent_action(
        &self,
        action: impl FnOnce(ResolvedAgent) -> DashboardAction,
    ) -> Option<DashboardAction> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Agent(role_index, agent_index) => {
                Some(action(self.resolved_agent(*role_index, *agent_index)))
            }
            DashboardSelection::Role(_) | DashboardSelection::Spacer => None,
        }
    }

    fn selected_open_dir_action(&self) -> Option<DashboardAction> {
        match &self.route {
            DashboardRoute::Channels { .. } => {
                let index = *self.filtered_channel_indices().get(self.channel_selected)?;
                let channel = self.snapshot.channels.get(index)?;
                Some(DashboardAction::OpenPath {
                    path: self.project.channels_dir().join(&channel.name),
                    label: format!("channel `{}`", channel.name),
                })
            }
            DashboardRoute::Dashboard => match self.visible_rows().get(self.selected)? {
                DashboardSelection::Role(role_index) => {
                    let role = &self.snapshot.roles[*role_index];
                    Some(DashboardAction::OpenPath {
                        path: self.project.role_dir(&role.slug),
                        label: format!("role `{}`", role.slug),
                    })
                }
                DashboardSelection::Agent(role_index, agent_index) => {
                    let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                    Some(DashboardAction::OpenPath {
                        path: RolePaths::new(self.project.clone(), agent.role.clone())
                            .agent(agent.agent.clone())
                            .root(),
                        label: format!("agent `{}/{}`", agent.role, agent.agent),
                    })
                }
                DashboardSelection::Spacer => None,
            },
            DashboardRoute::Conversations { .. } => {
                self.selected_session()
                    .map(|session| DashboardAction::OpenPath {
                        path: session.root.clone(),
                        label: format!("conversation `{}`", session.id),
                    })
            }
            DashboardRoute::Schema { .. } => {
                self.selected_schema_role()
                    .map(|role| DashboardAction::OpenPath {
                        path: self.project.role_dir(&role.slug),
                        label: format!("role `{}`", role.slug),
                    })
            }
            DashboardRoute::Queues { .. } | DashboardRoute::Timeline { .. } => None,
        }
    }

    fn selected_agent(&self) -> Option<&DashboardAgent> {
        match self.visible_rows().get(self.selected)? {
            DashboardSelection::Agent(role_index, agent_index) => {
                Some(&self.snapshot.roles[*role_index].agents[*agent_index])
            }
            DashboardSelection::Role(_) | DashboardSelection::Spacer => None,
        }
    }

    fn selected_inactive_agent(&self) -> Option<&DashboardAgent> {
        self.selected_agent()
            .filter(|agent| !matches!(agent.status, AgentStatus::Starting | AgentStatus::Running))
    }

    fn selected_archivable_agent(&self) -> Option<&DashboardAgent> {
        self.selected_inactive_agent()
    }

    fn visible_rows(&self) -> Vec<DashboardSelection> {
        self.snapshot.rows_matching(&self.filter)
    }

    fn visible_agent_count(&self) -> usize {
        agent_count_in_rows(&self.visible_rows())
    }

    fn selected_schema_role(&self) -> Option<&DashboardSchemaRole> {
        self.snapshot.schema.roles.get(self.schema_selected)
    }

    fn restore_schema_selection(&mut self) {
        self.schema_selected = self
            .schema_selected
            .min(self.snapshot.schema.roles.len().saturating_sub(1));
        self.center_schema_selection();
    }

    fn center_schema_selection(&mut self) {
        center_selected_row(
            &mut self.schema_scroll,
            self.schema_selected,
            self.last_agents_height,
        );
    }

    fn filtered_channel_indices(&self) -> Vec<usize> {
        matching_indices(&self.snapshot.channels, &self.channel_query)
    }

    fn filtered_session_indices(&self) -> Vec<usize> {
        matching_indices(&self.snapshot.sessions, &self.session_query)
    }

    fn selected_session(&self) -> Option<&DashboardSession> {
        self.filtered_session_indices()
            .get(self.session_selected)
            .and_then(|index| self.snapshot.sessions.get(*index))
    }

    fn session_by_id(&self, session: &str) -> Option<&DashboardSession> {
        self.snapshot
            .sessions
            .iter()
            .find(|record| record.id == session)
    }

    fn restore_channel_selection(&mut self) {
        let indices = self.filtered_channel_indices();
        self.channel_selected = self.channel_selected.min(indices.len().saturating_sub(1));
        self.channel_scroll = self.channel_scroll.min(usize_to_u16(self.channel_selected));
    }

    fn selected_channel_index(&self) -> Option<usize> {
        self.filtered_channel_indices()
            .get(self.channel_selected)
            .copied()
    }

    fn alert_latest(&self) -> Option<String> {
        self.snapshot
            .channels
            .iter()
            .find(|channel| channel.name == ALERTS_CHANNEL)
            .and_then(|channel| channel.latest.clone())
    }

    fn has_unseen_alerts(&self) -> bool {
        self.alert_latest()
            .is_some_and(|latest| self.seen_alert_latest.as_ref() != Some(&latest))
    }

    fn mark_alerts_seen(&mut self) {
        if let Some(latest) = self.alert_latest() {
            self.seen_alert_latest = Some(latest);
            let _ = self.persist_ui_state();
        }
    }

    fn selected_agent_position(&self, rows: &[DashboardSelection]) -> Option<(usize, usize)> {
        selected_agent_position_in_rows(rows, self.selected)
    }

    fn selected_queue_key(&self) -> Option<StatusQueueKey> {
        self.selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
            .map(StatusQueueKey::from)
            .or_else(|| self.queue_selected_key.clone())
    }

    fn restore_queue_selection(&mut self, key: Option<StatusQueueKey>) {
        let rows = self.queue_selection_rows();
        if let Some(key) = key {
            self.queue_selected_key = Some(key.clone());
            if let Some(index) = rows.iter().position(|row| {
                self.queue_index_for_selection(*row)
                    .and_then(|queue_index| self.snapshot.queues.get(queue_index))
                    .map(StatusQueueKey::from)
                    == Some(key.clone())
            }) {
                self.queue_selected = index;
                self.center_queue_selection();
                return;
            }
        }
        self.queue_selected = self.queue_selected.min(rows.len().saturating_sub(1));
        self.queue_selected_key = self
            .selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
            .map(StatusQueueKey::from);
        self.center_queue_selection();
    }

    fn remember_queue_selection(&mut self) {
        self.queue_selected_key = self.selected_queue_key();
        self.center_queue_selection();
    }

    fn queue_selection_rows(&self) -> Vec<QueueSelection> {
        let mut rows = Vec::new();
        for queue_index in self.filtered_queue_indices() {
            let queue = &self.snapshot.queues[queue_index];
            rows.push(QueueSelection::Header(queue_index));
            if !self.queue_collapsed(queue) {
                rows.extend((0..queue.count).map(|item_index| QueueSelection::Item {
                    queue_index,
                    item_index,
                }));
            }
        }
        rows
    }

    fn filtered_queue_indices(&self) -> Vec<usize> {
        let query = self.queue_query.trim();
        self.snapshot
            .queues
            .iter()
            .enumerate()
            .filter_map(|(index, _)| self.queue_matches_query(index, query).then_some(index))
            .collect()
    }

    fn queue_matches_query(&self, queue_index: usize, query: &str) -> bool {
        let Some(queue) = self.snapshot.queues.get(queue_index) else {
            return false;
        };
        if query.is_empty()
            || text_matches_query(
                [
                    queue.kind.label().to_owned(),
                    queue.name.clone(),
                    queue.count.to_string(),
                    if queue.locked { "locked" } else { "unlocked" }.to_owned(),
                    queue
                        .active
                        .as_ref()
                        .map(|active| active.label.clone())
                        .unwrap_or_default(),
                ],
                query,
            )
        {
            return true;
        }
        load_trigger_queue(&self.project, &queue.name)
            .ok()
            .is_some_and(|state| {
                state.items.into_iter().any(|item| {
                    text_matches_query(
                        [
                            item.role.to_string(),
                            format_unix_time(item.enqueued_at),
                            trigger_cause_summary(&item.cause),
                        ],
                        query,
                    )
                })
            })
    }

    fn selected_queue_index(&self) -> Option<usize> {
        self.queue_index_for_selection(*self.queue_selection_rows().get(self.queue_selected)?)
    }

    fn queue_index_for_selection(&self, selection: QueueSelection) -> Option<usize> {
        match selection {
            QueueSelection::Header(index) => Some(index),
            QueueSelection::Item { queue_index, .. } => Some(queue_index),
        }
    }

    fn toggle_selected_queue_collapsed(&mut self) {
        let Some(queue) = self
            .selected_queue_index()
            .and_then(|index| self.snapshot.queues.get(index))
        else {
            return;
        };
        let key = queue_key_string(queue);
        if !self.collapsed_queues.remove(&key) {
            self.collapsed_queues.insert(key);
        }
        self.queue_selected = self
            .queue_selected
            .min(self.queue_selection_rows().len().saturating_sub(1));
    }

    fn queue_collapsed(&self, queue: &StatusQueueRow) -> bool {
        self.collapsed_queues.contains(&queue_key_string(queue))
    }

    fn center_queue_selection(&mut self) {
        let offset = self.queue_selection_line_offset(self.queue_selected);
        self.queue_scroll = usize_to_u16(offset.saturating_sub(self.last_queue_height / 2));
    }

    fn queue_selection_line_offset(&self, selected_row: usize) -> usize {
        let mut row_index = 0;
        let mut line_offset = self.active_queue_line_count();
        for (visible_index, queue_index) in self.filtered_queue_indices().into_iter().enumerate() {
            let queue = &self.snapshot.queues[queue_index];
            if visible_index > 0 {
                line_offset += 1;
            }
            if row_index == selected_row {
                return line_offset;
            }
            row_index += 1;
            line_offset += 1;
            if self.queue_collapsed(queue) {
                line_offset += 1;
                continue;
            }
            if queue.count == 0 {
                line_offset += 1;
                continue;
            }
            for item_index in 0..queue.count {
                if row_index == selected_row {
                    return line_offset;
                }
                row_index += 1;
                line_offset += self.queue_item_line_count(queue, item_index);
            }
        }
        line_offset
    }

    fn active_queue_line_count(&self) -> usize {
        active_queue_item_line(&self.snapshot.queues, usize::MAX)
            .map(|_| 2)
            .unwrap_or_default()
    }

    fn queue_item_line_count(&self, _queue: &StatusQueueRow, _item_index: usize) -> usize {
        2
    }

    fn filtered_project_events(
        &self,
        filter: EventFilter,
        query: &str,
    ) -> Result<Vec<ProjectEvent>> {
        let query = query.trim().to_ascii_lowercase();
        Ok(load_project_events(&self.project)?
            .into_iter()
            .filter(|event| filter.matches(event.lane))
            .filter(|event| {
                query.is_empty()
                    || [
                        event.title.as_str(),
                        event.detail.as_str(),
                        event.lane.label(),
                    ]
                    .join(" ")
                    .to_ascii_lowercase()
                    .contains(&query)
            })
            .collect())
    }

    fn jump_to_event(&mut self, event: &ProjectEvent) -> Result<()> {
        match &event.target {
            EventTarget::Agent { role, agent } => {
                self.filter.clear();
                let key = DashboardSelectionKey {
                    role: role.clone(),
                    agent: Some(agent.clone()),
                    selected_offset: 0,
                };
                self.restore_selection = Some(key);
                self.navigate(DashboardRoute::Dashboard)?;
                self.restore_selection_if_pending();
                self.ensure_selectable_selection();
                self.ensure_selected_visible();
                self.open_detail_overlay(false);
                self.set_toast(DashboardToast::info(format!("opened {role}/{agent}")));
                self.persist_ui_state()?;
            }
            EventTarget::Queue(key) => {
                self.navigate(DashboardRoute::Queues {
                    queue: Some(key.clone()),
                })?;
                self.set_toast(DashboardToast::info(format!("opened queue `{}`", key.name)));
            }
            EventTarget::Notice => {
                self.navigate(DashboardRoute::Dashboard)?;
                self.set_toast(DashboardToast::info("opened notices"));
            }
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        self.expire_toast();
        let area = frame.area();
        let narrow = area.width < DASHBOARD_NARROW_WIDTH;
        let root = dashboard_layout(area, narrow);
        self.draw_tabs(frame, root.tabs);
        self.draw_health_strip(frame, root.health);
        let tab = self.current_tab();
        tab.draw(self, frame, &root, narrow);
        draw_dashboard_footer(
            frame,
            root.footer,
            self.footer_line(usize::from(root.footer.width)),
        );
        self.overlay.draw(self, frame, area);
    }

    fn draw_tabs(&self, frame: &mut Frame<'_>, area: Rect) {
        let dashboard_style = tab_style(matches!(&self.route, DashboardRoute::Dashboard));
        let schema_style = tab_style(matches!(&self.route, DashboardRoute::Schema { .. }));
        let channels_style = if self.has_unseen_alerts()
            && !matches!(&self.route, DashboardRoute::Channels { .. })
        {
            Style::default()
                .fg(Color::Yellow)
                .bg(Color::Black)
                .add_modifier(Modifier::BOLD)
        } else {
            tab_style(matches!(&self.route, DashboardRoute::Channels { .. }))
        };
        let queues_style = tab_style(matches!(&self.route, DashboardRoute::Queues { .. }));
        let events_style = tab_style(matches!(&self.route, DashboardRoute::Timeline { .. }));
        let conversations_style =
            tab_style(matches!(&self.route, DashboardRoute::Conversations { .. }));
        let channels_label = if self.has_unseen_alerts() {
            " Channels! "
        } else {
            " Channels "
        };
        let mut spans = vec![
            Span::raw(" "),
            Span::styled(" Dashboard ", dashboard_style),
            Span::raw(" "),
            Span::styled(" Schema ", schema_style),
            Span::raw(" "),
            Span::styled(channels_label, channels_style),
            Span::raw(" "),
            Span::styled(" Queues ", queues_style),
            Span::raw(" "),
            Span::styled(" Timeline ", events_style),
            Span::raw(" "),
            Span::styled(" Conversations ", conversations_style),
            Span::styled("  ", Style::default().fg(Color::DarkGray)),
            footer_key("Tab"),
            Span::styled(" switch", Style::default().fg(Color::DarkGray)),
        ];
        if let Some(toast) = self.toast.as_ref().filter(|toast| toast.is_visible()) {
            spans.extend([
                Span::styled("  ", Style::default().fg(Color::DarkGray)),
                Span::styled("● ", toast.level.style()),
                Span::styled(
                    toast.text.clone(),
                    toast.level.style().add_modifier(Modifier::BOLD),
                ),
            ]);
        }
        frame.render_widget(
            Paragraph::new(Line::from(spans))
                .style(Style::default().fg(Color::White).bg(Color::Black)),
            area,
        );
    }

    fn draw_health_strip(&self, frame: &mut Frame<'_>, area: Rect) {
        if area.height == 0 {
            return;
        }
        let mut running = 0;
        let mut attention = 0;
        let mut quota_waits = 0;
        let mut pending_steers = 0;
        let mut stale_steers = 0;
        for role in &self.snapshot.roles {
            for agent in &role.agents {
                let active = matches!(agent.status, AgentStatus::Starting | AgentStatus::Running);
                if active {
                    running += 1;
                }
                if matches!(agent.status, AgentStatus::NeedsAttention)
                    || matches!(agent.supervisor_status, SupervisorStatus::NeedsAttention)
                {
                    attention += 1;
                }
                if agent.quota_waiting {
                    quota_waits += 1;
                }
                pending_steers += agent.steer.pending_count;
                if agent.steer.pending_count > 0 && !active {
                    stale_steers += agent.steer.pending_count;
                }
            }
        }
        let queued = self
            .snapshot
            .queues
            .iter()
            .filter(|queue| queue.count > 0 || queue.locked)
            .count();
        let mut spans = vec![Span::styled(
            " health ",
            Style::default().fg(Color::DarkGray).bg(Color::Black),
        )];
        push_health_metric(&mut spans, "running", running, Color::Cyan);
        push_health_metric(&mut spans, "attention", attention, Color::Red);
        push_health_metric(&mut spans, "quota", quota_waits, Color::Yellow);
        push_health_metric(&mut spans, "steers", pending_steers, Color::Magenta);
        push_health_metric(&mut spans, "stale", stale_steers, Color::Red);
        push_health_metric(&mut spans, "queues", queued, Color::Yellow);
        if self.has_unseen_alerts() {
            spans.extend([
                Span::styled(
                    "  alerts ",
                    Style::default().fg(Color::Yellow).bg(Color::Black),
                ),
                Span::styled(
                    self.alert_latest().unwrap_or_else(|| "unseen".to_owned()),
                    Style::default()
                        .fg(Color::White)
                        .bg(Color::Black)
                        .add_modifier(Modifier::BOLD),
                ),
            ]);
        } else if attention == 0 && quota_waits == 0 && stale_steers == 0 {
            spans.push(Span::styled(
                "  ok",
                Style::default()
                    .fg(Color::Green)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            ));
        }
        frame.render_widget(
            Paragraph::new(Line::from(spans))
                .style(Style::default().fg(Color::White).bg(Color::Black)),
            area,
        );
    }

    fn draw_notices(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut title = vec![Span::styled(
            "Notices",
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )];
        if self.snapshot.notices_loading {
            title.extend([
                Span::styled(" refreshing ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    ui::spinner_frame(self.frame_tick),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]);
        }
        if let Some(updated_at) = self.snapshot.notices_updated_at {
            title.extend([
                Span::styled(" · updated ", Style::default().fg(Color::DarkGray)),
                Span::styled(event_age(updated_at), Style::default().fg(Color::DarkGray)),
            ]);
        }
        let title = Line::from(title);
        let mut lines = Vec::new();
        if let Some(latest) = self.alert_latest().filter(|_| self.has_unseen_alerts()) {
            lines.push(Line::from(vec![
                Span::styled("! ", Style::default().fg(Color::Yellow)),
                Span::styled("unopened alert ", Style::default().fg(Color::Yellow)),
                Span::styled(latest, Style::default().fg(Color::White)),
            ]));
        }
        if self.snapshot.notices.is_empty() {
            if lines.is_empty() {
                lines.push(Line::from(Span::styled(
                    if self.snapshot.notices_loading {
                        "refreshing..."
                    } else {
                        "no operator notices"
                    },
                    Style::default().fg(Color::DarkGray),
                )));
            }
        } else {
            lines.extend(self.snapshot.notices.iter().map(|notice| {
                Line::from(vec![
                    Span::styled("● ", notice_style(notice.severity)),
                    Span::styled(notice.text.clone(), Style::default().fg(Color::White)),
                ])
            }));
        }
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(title)
                        .border_style(Style::default().fg(Color::Yellow)),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn draw_tree(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.ensure_selected_visible();
        let rows = self.visible_rows();
        let rows_len = rows.len();
        let position = self.selected_agent_position(&rows);
        let content_width = usize::from(area.width.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS));
        let agent_width = self.snapshot.agent_width_for_rows(&rows);
        let summary_width = self.snapshot.summary_width_for_rows(&rows);
        let items = if rows.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                if self.filter.is_empty() {
                    "No roles found. Run `think role new` or create a templated project."
                } else {
                    "No matching agents. Press / to clear or refine the filter."
                },
                Style::default().fg(Color::DarkGray),
            )))]
        } else {
            rows.into_iter()
                .enumerate()
                .skip(self.agents_scroll)
                .take(self.last_agents_height)
                .map(|(index, row)| {
                    ListItem::new(self.row_line(
                        row,
                        index == self.selected,
                        agent_width,
                        summary_width,
                        content_width,
                    ))
                })
                .collect::<Vec<_>>()
        };
        frame.render_widget(
            List::new(items).block(
                dashboard_block_with_title(panel_title("Agents", position))
                    .border_style(Style::default().fg(Color::Blue)),
            ),
            area,
        );
        render_scrollbar(frame, area, rows_len, self.agents_scroll);
    }

    fn row_line(
        &self,
        row: DashboardSelection,
        selected: bool,
        agent_width: usize,
        summary_width: usize,
        content_width: usize,
    ) -> Line<'static> {
        match row {
            DashboardSelection::Spacer => Line::from(""),
            DashboardSelection::Role(role_index) => {
                let role = &self.snapshot.roles[role_index];
                let style = dashboard_role_style(role.status);
                let label = if content_width < 64 {
                    role.slug.to_string()
                } else if role.agents.is_empty() {
                    format!(
                        "{}{}{} · (n new agent)",
                        role.slug,
                        ui::FIELD_SEPARATOR,
                        role.parallel
                    )
                } else {
                    format!("{}{}{}", role.slug, ui::FIELD_SEPARATOR, role.parallel)
                };
                Line::from(vec![
                    dashboard_span(
                        if selected { "▸" } else { " " },
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(" ", style, selected),
                    dashboard_span(
                        ellipsize_display(&label, content_width.saturating_sub(2)),
                        style.add_modifier(Modifier::BOLD),
                        selected,
                    ),
                ])
            }
            DashboardSelection::Agent(role_index, agent_index) => {
                let agent = &self.snapshot.roles[role_index].agents[agent_index];
                let status_style = dashboard_agent_style(agent.status, agent.quota_waiting);
                let summary_style = if agent.summary == "*name loading*" {
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC)
                } else {
                    Style::default().fg(Color::White)
                };
                let detail = if content_width < 96 || agent.detail.is_empty() {
                    String::new()
                } else {
                    format!("{}{}", ui::FIELD_SEPARATOR, agent.detail)
                };
                let fixed_width = 1
                    + 3
                    + agent_width
                    + ui::FIELD_SEPARATOR.chars().count()
                    + ui::FIELD_SEPARATOR.chars().count()
                    + agent.status.to_string().chars().count()
                    + detail.chars().count();
                let usable_summary_width =
                    summary_width.min(content_width.saturating_sub(fixed_width));
                let summary = pad_display(
                    &ellipsize_display(&agent.summary, usable_summary_width),
                    usable_summary_width,
                );
                let status_glyph = match agent.status {
                    AgentStatus::Starting | AgentStatus::Running => {
                        format!(" {} ", ui::spinner_frame(self.frame_tick))
                    }
                    AgentStatus::Paused => " ‖ ".to_owned(),
                    AgentStatus::Done => " ✓ ".to_owned(),
                    AgentStatus::Stopped => " × ".to_owned(),
                    AgentStatus::NeedsAttention => " ! ".to_owned(),
                };
                Line::from(vec![
                    dashboard_span(
                        if selected { "▸" } else { " " },
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(status_glyph, status_style, selected),
                    dashboard_span(
                        pad_display(agent.agent.as_str(), agent_width),
                        Style::default().fg(Color::White),
                        selected,
                    ),
                    dashboard_span(
                        ui::FIELD_SEPARATOR,
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                    dashboard_span(summary, summary_style, selected),
                    dashboard_span(
                        ui::FIELD_SEPARATOR,
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                    dashboard_span(agent.status.to_string(), status_style, selected),
                    dashboard_span(detail, Style::default().fg(Color::DarkGray), selected),
                ])
            }
        }
    }

    fn draw_detail(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = dashboard_block("State").border_style(Style::default().fg(Color::Green));
        let inner = block.inner(area);
        let lines = match self.visible_rows().get(self.selected) {
            Some(DashboardSelection::Role(role_index)) => {
                self.snapshot.roles[*role_index].state_lines(usize::from(inner.width))
            }
            Some(DashboardSelection::Agent(role_index, agent_index)) => {
                self.snapshot.roles[*role_index].agents[*agent_index]
                    .state_lines(usize::from(inner.width))
            }
            Some(DashboardSelection::Spacer) => vec![Line::from("")],
            None => vec![Line::from("No roles or agents.")],
        };
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_runtime(&self, frame: &mut Frame<'_>, area: Rect, narrow: bool) {
        let chunks = Layout::default()
            .direction(if narrow {
                Direction::Vertical
            } else {
                Direction::Horizontal
            })
            .constraints(if narrow {
                [Constraint::Percentage(50), Constraint::Percentage(50)]
            } else {
                [Constraint::Percentage(45), Constraint::Percentage(55)]
            })
            .split(area);
        self.draw_channels(frame, chunks[0]);
        self.draw_quota(frame, chunks[1]);
    }

    fn draw_channel_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.restore_channel_selection();
        let filtered_indices = self.filtered_channel_indices();
        let position = (!filtered_indices.is_empty()).then_some((
            self.channel_selected
                .min(filtered_indices.len().saturating_sub(1))
                + 1,
            filtered_indices.len(),
        ));
        let visible = visible_panel_rows(area);
        if self.channel_selected < usize::from(self.channel_scroll) {
            self.channel_scroll = usize_to_u16(self.channel_selected);
        }
        if self.channel_selected >= usize::from(self.channel_scroll) + visible {
            self.channel_scroll = usize_to_u16(
                self.channel_selected
                    .saturating_add(1)
                    .saturating_sub(visible),
            );
        }
        let title = panel_title("Channels", position);
        let mut scroll = self.channel_scroll;
        render_scroll_panel(
            frame,
            area,
            title,
            Color::Yellow,
            &mut scroll,
            |width, _visible| self.channel_tab_lines(width, &filtered_indices),
        );
        self.channel_scroll = scroll;
    }

    fn channel_tab_lines(&self, width: usize, filtered_indices: &[usize]) -> Vec<Line<'static>> {
        if self.snapshot.channels.is_empty() {
            return vec![Line::from(Span::styled(
                "No channels configured. Use the palette to create one.",
                Style::default().fg(Color::DarkGray),
            ))];
        }
        if filtered_indices.is_empty() {
            return vec![Line::from(Span::styled(
                format!("No channels match `{}`.", self.channel_query),
                Style::default().fg(Color::DarkGray),
            ))];
        }
        filtered_indices
            .iter()
            .enumerate()
            .filter_map(|(visible_index, channel_index)| {
                self.snapshot.channels.get(*channel_index).map(|channel| {
                    channel_tab_line(
                        channel,
                        visible_index == self.channel_selected,
                        width,
                        channel.name == ALERTS_CHANNEL && self.has_unseen_alerts(),
                    )
                })
            })
            .collect()
    }

    fn draw_channels(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = if self.snapshot.channels.is_empty() {
            vec![Line::from(Span::styled(
                "no channels",
                Style::default().fg(Color::DarkGray),
            ))]
        } else {
            self.snapshot
                .channels
                .iter()
                .flat_map(channel_lines)
                .collect::<Vec<_>>()
        };
        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(
                dashboard_block("Channels").border_style(Style::default().fg(Color::Yellow)),
            ),
            area,
        );
    }

    fn draw_quota(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = dashboard_block("Quota").border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        match &self.quota {
            QuotaLoadState::Loading => {
                frame.render_widget(
                    Paragraph::new("loading...").style(Style::default().fg(Color::DarkGray)),
                    inner,
                );
            }
            QuotaLoadState::Ready(Some(limits)) => {
                let rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(DASHBOARD_QUOTA_GAUGE_ROWS),
                        Constraint::Length(DASHBOARD_QUOTA_GAUGE_ROWS),
                    ])
                    .split(inner);
                frame.render_widget(quota_gauge("primary", &limits.primary), rows[0]);
                frame.render_widget(quota_gauge("secondary", &limits.secondary), rows[1]);
            }
            QuotaLoadState::Ready(None) => {
                frame.render_widget(
                    Paragraph::new("usage unavailable").style(Style::default().fg(Color::DarkGray)),
                    inner,
                );
            }
        }
    }

    fn draw_palette(&self, frame: &mut Frame<'_>, area: Rect, query: &str, selected: usize) {
        let popup = centered_popup(
            area,
            DASHBOARD_PALETTE_MAX_WIDTH,
            DASHBOARD_PALETTE_MAX_HEIGHT,
        );
        let block =
            dashboard_block("Command Palette").border_style(Style::default().fg(Color::Magenta));
        let inner_width = usize::from(block.inner(popup).width);
        let entries = self.palette_entries(query);
        let query_line = Line::from(vec![
            Span::styled(
                ":",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(query.to_owned()),
        ]);
        let mut lines = vec![query_line, Line::from("")];
        if entries.is_empty() {
            lines.push(Line::from(Span::styled(
                "No matching commands.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            let selected = selected.min(entries.len().saturating_sub(1));
            for group in PaletteGroup::ALL {
                let group_entries = entries
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| entry.group == group)
                    .collect::<Vec<_>>();
                if group_entries.is_empty() {
                    continue;
                }
                lines.push(Line::from(Span::styled(
                    group.label(),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )));
                for (index, entry) in group_entries {
                    let selected = index == selected;
                    lines.push(Line::from(vec![
                        dashboard_span(
                            if selected { "▸ " } else { "  " },
                            Style::default().fg(Color::White),
                            selected,
                        ),
                        dashboard_span(
                            entry.label.clone(),
                            Style::default().fg(Color::White),
                            selected,
                        ),
                        dashboard_span(
                            entry
                                .key
                                .map(|key| format!("  [{key}]"))
                                .unwrap_or_default(),
                            Style::default().fg(Color::Cyan),
                            selected,
                        ),
                        dashboard_span(
                            palette_detail(&entry.label, entry.key, &entry.detail, inner_width),
                            Style::default().fg(Color::DarkGray),
                            selected,
                        ),
                    ]));
                }
            }
        }
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(block)
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_dashboard_composer(
        &self,
        frame: &mut Frame<'_>,
        area: Rect,
        agent: &ResolvedAgent,
        running: bool,
        composer: &AttachComposer,
    ) {
        let available = area.height.saturating_sub(DASHBOARD_FOOTER_HEIGHT);
        if available == 0 {
            return;
        }
        let height = ATTACH_COMPOSER_HEIGHT.min(available);
        let popup = Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(DASHBOARD_FOOTER_HEIGHT + height),
            width: area.width,
            height,
        };
        let title = if running {
            format!("Live Steer · {}", agent.label())
        } else {
            format!("Follow-Up · {}", agent.label())
        };
        let block = dashboard_block_with_title(Line::from(Span::styled(
            title,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )))
        .border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(popup);
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(composer.display_lines())).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_focused_detail(&self, frame: &mut Frame<'_>, area: Rect, extended: bool, scroll: u16) {
        render_detail_panel(
            frame,
            area,
            scroll,
            detail_panel("Detail", Color::Cyan, self.focused_detail_lines(extended)),
        );
    }

    fn draw_detail_target<T: DetailOverlayTarget>(
        &self,
        frame: &mut Frame<'_>,
        area: Rect,
        target: &T,
        scroll: u16,
    ) {
        render_detail_panel(
            frame,
            area,
            scroll,
            detail_panel(T::TITLE, T::BORDER, target.lines(self)),
        );
    }

    fn focused_detail_lines(&self, extended: bool) -> Vec<Line<'static>> {
        match self.visible_rows().get(self.selected) {
            Some(DashboardSelection::Role(role_index)) => {
                let role = &self.snapshot.roles[*role_index];
                let mut lines = role.detail_lines();
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    footer_key("n"),
                    Span::raw(" new agent  "),
                    footer_key("Space"),
                    Span::raw(" pause/unpause  "),
                    footer_key("Esc"),
                    Span::raw(" close"),
                ]));
                lines
            }
            Some(DashboardSelection::Agent(role_index, agent_index)) => {
                let agent = &self.snapshot.roles[*role_index].agents[*agent_index];
                load_agent_detail_lines(
                    &self.project,
                    agent,
                    extended,
                    Some(
                        if matches!(agent.status, AgentStatus::Starting | AgentStatus::Running) {
                            "steer"
                        } else {
                            "more"
                        },
                    ),
                )
                .unwrap_or_else(|err| {
                    vec![Line::from(format!("Failed to load agent detail: {err:#}"))]
                })
            }
            Some(DashboardSelection::Spacer) | None => vec![Line::from("No selection.")],
        }
    }

    fn draw_provider_settings(&self, frame: &mut Frame<'_>, area: Rect, selected: usize) {
        let popup = inset_rect(area, 4, 2);
        let block =
            dashboard_block("Provider Accounts").border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(popup);
        let width = usize::from(inner.width);
        let mut lines = vec![
            provider_settings_project_config_line(&self.project),
            provider_settings_action_line(),
            Line::from(""),
        ];
        match self.provider_account_rows() {
            Ok(rows) => {
                let selected = selected.min(rows.len().saturating_sub(1));
                lines.push(provider_accounts_heading(rows.len(), selected));
                let visible = usize::from(inner.height).saturating_sub(lines.len()).max(1);
                let start = selected
                    .saturating_sub(visible / 2)
                    .min(rows.len().saturating_sub(visible));
                let name_width = provider_account_name_width(&rows);
                lines.extend(rows.iter().enumerate().skip(start).take(visible).map(
                    |(index, account)| {
                        provider_account_line(account, index == selected, width, name_width)
                    },
                ));
            }
            Err(err) => {
                lines.push(Line::from(vec![
                    Span::styled("accounts unavailable: ", Style::default().fg(Color::Red)),
                    Span::styled(format!("{err:#}"), Style::default().fg(Color::White)),
                ]));
            }
        }
        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn draw_schema_tab(&mut self, frame: &mut Frame<'_>, area: Rect, narrow: bool) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.restore_schema_selection();
        if !narrow && area.width >= 112 {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
                .split(area);
            self.draw_schema_overview(frame, chunks[0]);
            self.draw_schema_role_preview(frame, chunks[1]);
        } else {
            self.draw_schema_overview(frame, area);
        }
    }

    fn draw_schema_overview(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let role_count = self.snapshot.schema.roles.len();
        let title = panel_title(
            "Schema",
            (role_count > 0).then_some((self.schema_selected.min(role_count - 1) + 1, role_count)),
        );
        let mut scroll = self.schema_scroll;
        render_scroll_panel(
            frame,
            area,
            title,
            Color::Blue,
            &mut scroll,
            |width, _visible| self.schema_tab_lines(width),
        );
        self.schema_scroll = scroll;
    }

    fn draw_schema_role_preview(&self, frame: &mut Frame<'_>, area: Rect) {
        let title = self
            .selected_schema_role()
            .map(|role| format!("Role · {}", role.slug))
            .unwrap_or_else(|| "Role".to_owned());
        let block = dashboard_block_with_title(dynamic_panel_title(title, None))
            .border_style(Style::default().fg(Color::Green));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(
            Paragraph::new(Text::from(self.schema_role_detail_lines(
                self.selected_schema_role(),
                usize::from(inner.width),
            )))
            .wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn schema_tab_lines(&self, width: usize) -> Vec<Line<'static>> {
        let config = &self.snapshot.schema.config;
        let codex = &config.providers.codex;
        let mut lines = vec![
            Line::from(vec![
                footer_key("p"),
                Span::raw(" PROJECT.md  "),
                footer_key("t"),
                Span::raw(" think.toml  "),
                footer_key("o"),
                Span::raw(" role dir  "),
                footer_key("Esc"),
                Span::raw(" dashboard"),
            ]),
            Line::from(""),
            section_line("project"),
            schema_kv_line("path", self.project.root.display().to_string(), Color::Gray),
            schema_kv_line(
                "config",
                self.project.config().display().to_string(),
                Color::Gray,
            ),
            schema_kv_line("version", config.version.to_string(), Color::White),
            schema_kv_line(
                "template",
                config
                    .template
                    .map(|template| template.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                Color::Cyan,
            ),
            schema_kv_line(
                "default role",
                config
                    .default_role
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_owned()),
                Color::Cyan,
            ),
            schema_kv_line("backend", config.default_backend.to_string(), Color::White),
            schema_kv_line(
                "channels",
                schema_join(config.channels.iter().map(ToString::to_string)),
                Color::Yellow,
            ),
            schema_kv_line(
                "transcripts",
                format!(
                    "raw={} text={}",
                    config.transcripts.record_raw, config.transcripts.record_text
                ),
                Color::White,
            ),
            schema_kv_line(
                "theme",
                format!("{:?}", config.ui.theme).to_ascii_lowercase(),
                Color::White,
            ),
            Line::from(""),
            section_line("codex provider"),
            schema_kv_line(
                "model",
                codex.model.clone().unwrap_or_else(|| "-".to_owned()),
                Color::Cyan,
            ),
            schema_kv_line(
                "thinking",
                codex
                    .thinking_level
                    .map(|level| level.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                Color::Cyan,
            ),
            Line::from(""),
            section_line("files"),
            schema_status_line("PROJECT.md", self.snapshot.schema.project_md_exists),
            schema_status_line("think.toml", self.project.config().exists()),
            Line::from(""),
            section_line("roles"),
        ];
        if self.snapshot.schema.roles.is_empty() {
            lines.push(Line::from(Span::styled(
                "No roles are configured.",
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        for (index, role) in self.snapshot.schema.roles.iter().enumerate() {
            let selected = index == self.schema_selected;
            let steps = format!("{}/{}", role.step_files.len(), role.config.steps.len());
            let issue = if role.missing_steps.is_empty() && role.extra_step_files.is_empty() {
                "ok".to_owned()
            } else {
                format!(
                    "{} missing, {} extra",
                    role.missing_steps.len(),
                    role.extra_step_files.len()
                )
            };
            let issue_style = if issue == "ok" {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Yellow)
            };
            let role_width = width.saturating_sub(72).max(10);
            lines.push(Line::from(vec![
                dashboard_span(
                    if selected { "> " } else { "  " },
                    Style::default().fg(Color::White),
                    selected,
                ),
                dashboard_span(
                    pad_display(
                        &ellipsize_display(role.slug.as_str(), role_width),
                        role_width,
                    ),
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
                    format!("{:<6}", role.config.status),
                    dashboard_role_style(role.config.status),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    format!("{:<10}", role.config.mode),
                    Style::default().fg(Color::Cyan),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    format!("parallel {:<8}", role.config.parallel),
                    Style::default().fg(Color::White),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    format!("steps {steps:<5}"),
                    Style::default().fg(Color::White),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(issue, issue_style, selected),
            ]));
        }
        lines
    }

    fn schema_role_detail_lines(
        &self,
        role: Option<&DashboardSchemaRole>,
        width: usize,
    ) -> Vec<Line<'static>> {
        let Some(role) = role else {
            return vec![Line::from(Span::styled(
                "No role selected.",
                Style::default().fg(Color::DarkGray),
            ))];
        };
        let mut lines = vec![
            section_line("contract"),
            schema_kv_line("role", role.slug.to_string(), Color::Cyan),
            schema_kv_line("status", role.config.status.to_string(), Color::White),
            schema_kv_line("backend", role.config.backend.to_string(), Color::White),
            schema_kv_line("mode", role.config.mode.to_string(), Color::Cyan),
            schema_kv_line("parallel", role.config.parallel.to_string(), Color::Cyan),
            schema_kv_line(
                "agent names",
                role.config.agent_names.to_string(),
                Color::White,
            ),
            schema_kv_line(
                "agent prefix",
                role.config
                    .agent_prefix
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
                Color::White,
            ),
            schema_kv_line(
                "auto archive",
                role.config.auto_archive.to_string(),
                Color::White,
            ),
            schema_kv_line(
                "expose",
                schema_join(role.config.expose.iter().map(ToString::to_string)),
                Color::White,
            ),
            Line::from(""),
            section_line("files"),
            schema_status_line("config.toml", true),
            schema_status_line("ROLE.md", role.role_md_exists),
            schema_kv_line(
                "step prompts",
                format!("{}/{}", role.step_files.len(), role.config.steps.len()),
                Color::White,
            ),
        ];
        if !role.missing_steps.is_empty() {
            lines.push(schema_kv_line(
                "missing steps",
                schema_join(role.missing_steps.iter().cloned()),
                Color::Yellow,
            ));
        }
        if !role.extra_step_files.is_empty() {
            lines.push(schema_kv_line(
                "extra step files",
                schema_join(role.extra_step_files.iter().cloned()),
                Color::Yellow,
            ));
        }
        lines.extend([Line::from(""), section_line("steps")]);
        if role.config.steps.is_empty() {
            lines.push(Line::from(Span::styled(
                "No steps configured.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for step in &role.config.steps {
                let step = step.to_string();
                let exists = role.step_files.iter().any(|file| file == &step);
                lines.push(Line::from(vec![
                    Span::styled(
                        if exists { "present " } else { "missing " },
                        if exists {
                            Style::default().fg(Color::Green)
                        } else {
                            Style::default().fg(Color::Yellow)
                        },
                    ),
                    Span::styled(
                        ellipsize_display(&step, width.saturating_sub(8)),
                        Style::default().fg(Color::White),
                    ),
                ]));
            }
        }
        lines.extend([Line::from(""), section_line("triggers")]);
        if role.config.triggers.is_empty() {
            lines.push(Line::from(Span::styled(
                "No triggers configured.",
                Style::default().fg(Color::DarkGray),
            )));
        } else {
            for trigger in &role.config.triggers {
                lines.push(Line::from(vec![
                    Span::styled("- ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        ellipsize_display(
                            &schema_trigger_summary(trigger),
                            width.saturating_sub(2),
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
            }
        }
        lines
    }

    fn draw_event_log(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let event_result = self.filtered_project_events(self.event_filter, &self.event_query);
        let event_count = event_result.as_ref().map_or(0, Vec::len);
        let title = panel_title(
            "Timeline",
            (event_count > 0).then_some((
                self.event_selected.min(event_count.saturating_sub(1)) + 1,
                event_count,
            )),
        );
        let mut scroll = self.event_scroll;
        render_scroll_panel(
            frame,
            area,
            title,
            Color::Magenta,
            &mut scroll,
            |width, _visible| match event_result {
                Ok(events) => project_event_lines(
                    &events,
                    width,
                    self.event_selected.min(events.len().saturating_sub(1)),
                    self.event_filter,
                    &self.event_query,
                    false,
                ),
                Err(err) => vec![Line::from(format!("Failed to load timeline: {err:#}"))],
            },
        );
        self.event_scroll = scroll;
    }

    fn draw_conversations_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        if area.width >= 112 {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
                .split(area);
            self.draw_conversations_list(frame, chunks[0]);
            self.draw_conversation_preview(frame, chunks[1]);
        } else {
            self.draw_conversations_list(frame, area);
        }
    }

    fn draw_conversations_list(&mut self, frame: &mut Frame<'_>, area: Rect) {
        self.last_agents_height =
            usize::from(area.height.saturating_sub(DASHBOARD_PANEL_BORDER_ROWS))
                .max(DASHBOARD_MIN_VISIBLE_ROWS);
        self.restore_session_selection();
        let indices = self.filtered_session_indices();
        let title = panel_title(
            "Conversations",
            (!indices.is_empty()).then_some((
                self.session_selected.min(indices.len().saturating_sub(1)) + 1,
                indices.len(),
            )),
        );
        let mut scroll = self.session_scroll;
        render_scroll_panel(
            frame,
            area,
            title,
            Color::Magenta,
            &mut scroll,
            |width, _visible| self.session_tab_lines(width, &indices),
        );
        self.session_scroll = scroll;
    }

    fn draw_conversation_preview(&self, frame: &mut Frame<'_>, area: Rect) {
        let title = self
            .selected_session()
            .map(|session| format!("Preview · {}", session.id))
            .unwrap_or_else(|| "Preview".to_owned());
        let block = dashboard_block_with_title(dynamic_panel_title(title, None))
            .border_style(Style::default().fg(Color::Magenta));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let lines = self
            .selected_session()
            .map(conversation_preview_lines)
            .unwrap_or_else(|| {
                vec![Line::from(Span::styled(
                    "No conversation selected.",
                    Style::default().fg(Color::DarkGray),
                ))]
            });
        frame.render_widget(
            Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
            inner,
        );
    }

    fn session_tab_lines(&self, width: usize, indices: &[usize]) -> Vec<Line<'static>> {
        let mut lines = vec![Line::from(vec![
            footer_key("Enter"),
            Span::raw(" detail  "),
            footer_key("o"),
            Span::raw(" open  "),
            footer_key("y/Y"),
            Span::raw(" copy  "),
            footer_key("/"),
            Span::raw(" search  "),
            footer_key("Esc"),
            Span::raw(" dashboard"),
        ])];
        if self.snapshot.sessions.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "No command conversations have been recorded yet.",
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        if indices.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("No conversations match `{}`.", self.session_query),
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        lines.push(Line::from(""));
        for (visible_index, snapshot_index) in indices.iter().enumerate() {
            let Some(session) = self.snapshot.sessions.get(*snapshot_index) else {
                continue;
            };
            let selected = visible_index == self.session_selected;
            let title_width = width.saturating_sub(44).max(16);
            lines.push(Line::from(vec![
                dashboard_span(
                    if selected { "▸ " } else { "  " },
                    Style::default().fg(Color::White),
                    selected,
                ),
                dashboard_span(
                    format_unix_time_compact(session.created_at),
                    Style::default().fg(Color::Cyan),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    format!("{:<9}", session.kind),
                    Style::default().fg(Color::Magenta),
                    selected,
                ),
                dashboard_span(
                    ui::FIELD_SEPARATOR,
                    Style::default().fg(Color::DarkGray),
                    selected,
                ),
                dashboard_span(
                    ellipsize_display(&session.title, title_width),
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                    selected,
                ),
            ]));
            if !session.preview.is_empty() {
                lines.push(Line::from(vec![
                    dashboard_span("    ", Style::default().fg(Color::DarkGray), selected),
                    dashboard_span(
                        ellipsize_display(&session.preview, width.saturating_sub(4)),
                        Style::default().fg(Color::DarkGray),
                        selected,
                    ),
                ]));
            }
        }
        lines
    }

    fn draw_queue_tab(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let rows = self.queue_selection_rows();
        let title = panel_title(
            "Queues",
            (!rows.is_empty()).then_some((self.queue_selected.min(rows.len() - 1) + 1, rows.len())),
        );
        self.last_queue_height = visible_panel_rows(area);
        self.center_queue_selection();
        let mut scroll = self.queue_scroll;
        render_scroll_panel(
            frame,
            area,
            title,
            Color::Yellow,
            &mut scroll,
            |width, _visible| self.queue_tab_lines(width),
        );
        self.queue_scroll = scroll;
    }

    fn queue_tab_lines(&self, width: usize) -> Vec<Line<'static>> {
        if self.snapshot.queues.is_empty() {
            return vec![Line::from(Span::styled(
                "No queues are configured.",
                Style::default().fg(Color::DarkGray),
            ))];
        }
        let mut lines = Vec::new();
        if let Some(active) = active_queue_item_line(&self.snapshot.queues, width) {
            lines.push(active);
            lines.push(Line::from(""));
        }
        let filtered_indices = self.filtered_queue_indices();
        if filtered_indices.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("No queues match `{}`.", self.queue_query),
                Style::default().fg(Color::DarkGray),
            )));
            return lines;
        }
        for (visible_index, index) in filtered_indices.into_iter().enumerate() {
            let queue = &self.snapshot.queues[index];
            if visible_index > 0 {
                lines.push(Line::from(""));
            }
            let header_selected = matches!(
                self.queue_selection_rows().get(self.queue_selected),
                Some(QueueSelection::Header(queue_index)) if *queue_index == index
            );
            let collapsed = self.queue_collapsed(queue);
            lines.push(queue_header_line(queue, header_selected, collapsed));
            if collapsed {
                lines.push(queue_empty_line("collapsed", header_selected));
            } else {
                let selected_item = match self.queue_selection_rows().get(self.queue_selected) {
                    Some(QueueSelection::Item {
                        queue_index,
                        item_index,
                    }) if *queue_index == index => Some(*item_index),
                    _ => None,
                };
                lines.extend(queue_item_lines(&self.project, queue, width, selected_item));
            }
        }
        lines
    }

    fn footer_line(&self, width: usize) -> Line<'static> {
        self.overlay.footer_line(self, width)
    }

    fn route_footer_line(&self, width: usize) -> Line<'static> {
        match &self.route {
            DashboardRoute::Dashboard => self.main_footer_line(width),
            DashboardRoute::Schema { .. } => self.schema_footer_line(width),
            DashboardRoute::Channels { .. } => self.channel_footer_line(width),
            DashboardRoute::Queues { .. } => self.queue_footer_line(width),
            DashboardRoute::Timeline { .. } => self.event_footer_line(width),
            DashboardRoute::Conversations { .. } => self.session_footer_line(width),
        }
    }

    fn footer_command_pairs(&self) -> impl Iterator<Item = FooterPair> + '_ {
        let ctx = self.command_ctx();
        self.commands(CommandSurface::Footer)
            .filter_map(move |command| {
                command
                    .key(ctx)
                    .map(|key| FooterPair::new(key, command.label(ctx)))
            })
    }

    fn channel_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_items(
            self.footer_command_pairs().chain(
                [
                    ("↑↓", "select"),
                    ("PgUp/PgDn", "scroll"),
                    ("Esc", "dashboard"),
                ]
                .map(FooterPair::from),
            ),
            width,
        )
    }

    fn schema_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_items(
            self.footer_command_pairs().chain(
                [
                    ("↑↓", "select role"),
                    ("PgUp/PgDn", "scroll"),
                    ("Esc", "dashboard"),
                ]
                .map(FooterPair::from),
            ),
            width,
        )
    }

    fn queue_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_items(
            self.footer_command_pairs().chain(
                [
                    ("↑↓", "select queue"),
                    ("PgUp/PgDn", "scroll"),
                    ("Esc", "dashboard"),
                ]
                .map(FooterPair::from),
            ),
            width,
        )
    }

    fn event_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_items(
            self.footer_command_pairs().chain(
                [
                    ("a/r/t/n/0", "filter"),
                    ("↑↓", "select"),
                    ("Esc", "dashboard"),
                ]
                .map(FooterPair::from),
            ),
            width,
        )
    }

    fn session_footer_line(&self, width: usize) -> Line<'static> {
        footer_line_from_items(
            self.footer_command_pairs().chain(
                [
                    ("y/Y", "copy"),
                    ("↑↓", "select"),
                    ("PgUp/PgDn", "scroll"),
                    ("Esc", "dashboard"),
                ]
                .map(FooterPair::from),
            ),
            width,
        )
    }

    fn main_footer_line(&self, width: usize) -> Line<'static> {
        let fixed_pairs = [
            FooterPair::from(("↑↓", "select")),
            FooterPair::from((":", "palette")),
        ];
        let notice_pair = (!self.snapshot.notices.is_empty() || self.has_unseen_alerts())
            .then(|| FooterPair::from(("!", "notices")));
        footer_line_from_items(
            fixed_pairs
                .into_iter()
                .chain(notice_pair)
                .chain(self.footer_command_pairs()),
            width,
        )
    }
}
