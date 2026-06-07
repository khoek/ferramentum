use super::*;

mod activity;
mod model;
mod screen;
mod view;

pub(super) use self::activity::*;
pub(super) use self::model::*;
pub(super) use self::screen::*;
pub(super) use self::view::*;

pub(super) enum DashboardAction {
    Quit,
    AttachRole(RoleSlug),
    AttachAgent(ResolvedAgent),
    MoreWithQuery(ResolvedAgent, String),
    New(RoleSlug),
    NewRole,
    Archive(ResolvedAgent),
    TriggerRole(RoleSlug),
    Check,
    Assist,
    OpenProject,
    OpenPath { path: PathBuf, label: String },
    NewChannel,
    CodexLogin,
    CodexConfig,
    ToggleRole(RoleSlug),
    ToggleAgent(ResolvedAgent),
    ToggleAllRoles,
}

impl DashboardAction {
    fn suspends_terminal(&self) -> bool {
        matches!(
            self,
            Self::AttachRole(_)
                | Self::AttachAgent(_)
                | Self::MoreWithQuery(_, _)
                | Self::New(_)
                | Self::NewRole
                | Self::Check
                | Self::Assist
                | Self::NewChannel
                | Self::CodexLogin
                | Self::CodexConfig
        )
    }
}

#[derive(Default)]
pub(super) enum DashboardOverlay {
    #[default]
    None,
    Advanced,
    Help,
    Search {
        target: SearchTarget,
        query: String,
    },
    Palette {
        query: String,
        selected: usize,
    },
    Composer {
        agent: ResolvedAgent,
        running: bool,
        composer: AttachComposer,
    },
    Detail {
        extended: bool,
        scroll: u16,
    },
    QueueDetail {
        selection: QueueSelection,
        scroll: u16,
    },
    ChannelDetail {
        channel_index: usize,
        scroll: u16,
    },
    NoticeDetail {
        scroll: u16,
    },
    ConversationDetail {
        session: String,
        scroll: u16,
    },
    ProviderSettings {
        selected: usize,
    },
}

#[derive(Clone)]
pub(super) enum DashboardRoute {
    Dashboard,
    Schema { role: Option<RoleSlug> },
    Channels { channel: Option<ChannelSlug> },
    Queues { queue: Option<StatusQueueKey> },
    Timeline { event: Option<ProjectEventKey> },
    Conversations { session: Option<String> },
}

#[derive(Clone, Copy)]
pub(super) enum SearchTarget {
    Agents,
    Channels,
    Queues,
    Events,
    Conversations,
}

impl SearchTarget {
    fn title(self) -> &'static str {
        match self {
            Self::Agents => "Filter Agents",
            Self::Channels => "Filter Channels",
            Self::Queues => "Filter Queues",
            Self::Events => "Filter Events",
            Self::Conversations => "Filter Conversations",
        }
    }

    fn visible_label(self) -> &'static str {
        match self {
            Self::Agents => "visible agents",
            Self::Channels => "visible channels",
            Self::Queues => "visible rows",
            Self::Events => "visible events",
            Self::Conversations => "visible conversations",
        }
    }
}

#[derive(Clone)]
pub(super) struct ProviderAccountRow {
    name: String,
    active: bool,
    codex_home: PathBuf,
    quota_wait_until: Option<u64>,
    last_quota_event: Option<String>,
    last_used_at: Option<u64>,
}

pub(super) enum QuotaLoadState {
    Loading,
    Ready(Option<CodexRateLimits>),
}

#[derive(Clone)]
pub(super) struct DashboardToast {
    level: ToastLevel,
    text: String,
    created_at: Instant,
}

#[derive(Clone, Copy)]
pub(super) enum ToastLevel {
    Info,
    Success,
    Warn,
}

impl DashboardToast {
    fn info(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Info,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn success(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Success,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn warn(text: impl Into<String>) -> Self {
        Self {
            level: ToastLevel::Warn,
            text: text.into(),
            created_at: Instant::now(),
        }
    }

    fn is_visible(&self) -> bool {
        self.created_at.elapsed() < DASHBOARD_TOAST_TTL
    }
}

impl ToastLevel {
    fn style(self) -> Style {
        match self {
            Self::Info => Style::default().fg(Color::Cyan).bg(Color::Black),
            Self::Success => Style::default().fg(Color::Green).bg(Color::Black),
            Self::Warn => Style::default().fg(Color::Yellow).bg(Color::Black),
        }
    }
}

const DASHBOARD_UI_STATE_VERSION: u32 = 2;

#[derive(Debug, Default, Serialize, Deserialize)]
pub(super) struct DashboardUiState {
    version: u32,
    selected_role: Option<RoleSlug>,
    selected_agent: Option<AgentId>,
    agents_scroll: usize,
    selected_offset: usize,
    #[serde(default)]
    filter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seen_alert_latest: Option<String>,
}

#[derive(Clone)]
pub(super) struct DashboardSelectionKey {
    role: RoleSlug,
    agent: Option<AgentId>,
    selected_offset: usize,
}
