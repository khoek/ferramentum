use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::{BackendName, RoleMode};
use crate::ids::{AgentId, ChannelSlug, RoleSlug, StepSlug};
use crate::io;

pub const AGENT_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct ProjectPaths {
    pub root: PathBuf,
}

impl ProjectPaths {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn find_from(start: &Path) -> Result<Self> {
        let mut current = start
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize `{}`", start.display()))?;
        if current.is_file() {
            current.pop();
        }
        loop {
            if current.join("think.toml").exists() {
                return Ok(Self::new(current));
            }
            if !current.pop() {
                bail!(
                    "No think project found. Run `think project init` or `think project new <path>` first."
                );
            }
        }
    }

    pub fn project_md(&self) -> PathBuf {
        self.root.join("PROJECT.md")
    }

    pub fn config(&self) -> PathBuf {
        self.root.join("think.toml")
    }

    pub fn roles_dir(&self) -> PathBuf {
        self.root.join("roles")
    }

    pub fn role_dir(&self, role: &RoleSlug) -> PathBuf {
        self.roles_dir().join(role.as_str())
    }

    pub fn channels_dir(&self) -> PathBuf {
        self.root.join("channels")
    }

    pub fn channel_dir(&self, channel: &ChannelSlug) -> PathBuf {
        self.channels_dir().join(channel.as_str())
    }

    pub fn runtime_dir(&self) -> PathBuf {
        self.root.join("runtime")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    pub fn role_data_dir(&self, role: &RoleSlug) -> PathBuf {
        self.data_dir().join("roles").join(role.as_str())
    }

    pub fn role_agent_data_dir(&self, role: &RoleSlug) -> PathBuf {
        self.role_data_dir(role).join("agents")
    }

    pub fn agent_data_root(&self, role: &RoleSlug, agent: &AgentId) -> PathBuf {
        self.role_agent_data_dir(role).join(agent.as_str())
    }

    pub fn channel_lock_path(&self, channel: &ChannelSlug) -> PathBuf {
        self.runtime_dir()
            .join("locks")
            .join("channels")
            .join(format!("{}.lock", channel.as_str()))
    }

    pub fn agent_lock_path(&self) -> PathBuf {
        self.runtime_dir().join("locks").join("agents.lock")
    }
}

#[derive(Debug, Clone)]
pub struct RolePaths {
    pub project: ProjectPaths,
    pub role: RoleSlug,
}

impl RolePaths {
    pub fn new(project: ProjectPaths, role: RoleSlug) -> Self {
        Self { project, role }
    }

    pub fn root(&self) -> PathBuf {
        self.project.role_dir(&self.role)
    }

    pub fn role_md(&self) -> PathBuf {
        self.root().join("ROLE.md")
    }

    pub fn config(&self) -> PathBuf {
        self.root().join("config.toml")
    }

    pub fn steps_dir(&self) -> PathBuf {
        self.root().join("steps")
    }

    pub fn step_path(&self, step: &StepSlug) -> PathBuf {
        self.steps_dir().join(format!("{}.md", step.as_str()))
    }

    pub fn agents_dir(&self) -> PathBuf {
        self.root().join("agents")
    }

    pub fn agent(&self, agent: AgentId) -> AgentPaths {
        AgentPaths::new(self.clone(), agent)
    }
}

#[derive(Debug, Clone)]
pub struct AgentPaths {
    pub role: RolePaths,
    pub agent: AgentId,
}

impl AgentPaths {
    pub fn new(role: RolePaths, agent: AgentId) -> Self {
        Self { role, agent }
    }

    pub fn root(&self) -> PathBuf {
        self.role.agents_dir().join(self.agent.as_str())
    }

    pub fn state(&self) -> PathBuf {
        self.root().join("agent.toml")
    }

    pub fn prompt(&self) -> PathBuf {
        self.root().join("PROMPT.md")
    }

    pub fn agent_prompt(&self) -> PathBuf {
        self.root().join("AGENT_PROMPT.md")
    }

    pub fn trigger_context(&self) -> PathBuf {
        self.root().join("TRIGGER.md")
    }

    pub fn exposure_context(&self) -> PathBuf {
        self.root().join("EXPOSED.md")
    }

    pub fn manifest(&self) -> PathBuf {
        self.root().join("manifest.toml")
    }

    pub fn backend_thread_state(&self) -> PathBuf {
        self.root().join("backend-thread.toml")
    }

    pub fn steer_dir(&self) -> PathBuf {
        self.role
            .project
            .runtime_dir()
            .join("steer")
            .join(self.role.role.as_str())
            .join(self.agent.as_str())
    }

    pub fn data_dir(&self) -> PathBuf {
        self.root().join("data")
    }

    pub fn data_own(&self) -> PathBuf {
        self.data_dir().join("own")
    }

    pub fn data_all(&self) -> PathBuf {
        self.data_dir().join("all")
    }

    pub fn work_dir(&self) -> PathBuf {
        self.root().join("work")
    }

    pub fn work_own(&self) -> PathBuf {
        self.work_dir().join("own")
    }

    pub fn work_all(&self) -> PathBuf {
        self.work_dir().join("all")
    }

    pub fn channels_dir(&self) -> PathBuf {
        self.root().join("channels")
    }

    pub fn channel_dir(&self, channel: &ChannelSlug) -> PathBuf {
        self.channels_dir().join(channel.as_str())
    }

    pub fn runs_dir(&self) -> PathBuf {
        self.root().join("runs")
    }

    pub fn run(&self, run_id: u64) -> RunPaths {
        RunPaths {
            agent: self.clone(),
            run_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunPaths {
    pub agent: AgentPaths,
    pub run_id: u64,
}

impl RunPaths {
    pub fn root(&self) -> PathBuf {
        self.agent.runs_dir().join(self.run_id.to_string())
    }

    pub fn step(&self) -> PathBuf {
        self.root().join("STEP.md")
    }

    pub fn prompt(&self) -> PathBuf {
        self.root().join("PROMPT.md")
    }

    pub fn transcript_text(&self) -> PathBuf {
        self.root().join("TRANSCRIPT.txt")
    }

    pub fn reply(&self) -> PathBuf {
        self.root().join("REPLY.md")
    }

    pub fn exit(&self) -> PathBuf {
        self.root().join("exit.toml")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentState {
    pub version: u32,
    pub role: RoleSlug,
    pub agent: AgentId,
    pub backend: BackendName,
    pub mode: RoleMode,
    pub status: AgentStatus,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub paused_by_user: bool,
    pub current_step: usize,
    pub run_count: u64,
    pub pane_id: Option<String>,
    pub channels: Vec<ChannelSlug>,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_exit: Option<RunExitState>,
    pub note: Option<String>,
}

impl AgentState {
    pub fn new(
        role: RoleSlug,
        agent: AgentId,
        backend: BackendName,
        mode: RoleMode,
        channels: Vec<ChannelSlug>,
    ) -> Self {
        let now = unix_timestamp();
        Self {
            version: AGENT_STATE_VERSION,
            role,
            agent,
            backend,
            mode,
            status: AgentStatus::Starting,
            archived: false,
            paused_by_user: false,
            current_step: 0,
            run_count: 0,
            pane_id: None,
            channels,
            created_at: now,
            updated_at: now,
            last_exit: None,
            note: None,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = unix_timestamp();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStatus {
    Starting,
    Running,
    Paused,
    Done,
    Stopped,
    NeedsAttention,
}

impl std::fmt::Display for AgentStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => formatter.write_str("starting"),
            Self::Running => formatter.write_str("running"),
            Self::Paused => formatter.write_str("paused"),
            Self::Done => formatter.write_str("done"),
            Self::Stopped => formatter.write_str("stopped"),
            Self::NeedsAttention => formatter.write_str("needs-attention"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunExitState {
    pub run_id: u64,
    pub step: StepSlug,
    pub started_at: u64,
    pub finished_at: u64,
    pub success: bool,
    pub code: u32,
    pub signal: Option<String>,
    pub disposition: Option<Disposition>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentManifest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<Disposition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Disposition {
    Continue,
    Stop,
}

impl std::fmt::Display for Disposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue => formatter.write_str("continue"),
            Self::Stop => formatter.write_str("stop"),
        }
    }
}

pub fn load_agent(paths: &AgentPaths) -> Result<AgentState> {
    io::read_toml(&paths.state())
}

pub fn save_agent(paths: &AgentPaths, state: &mut AgentState) -> Result<()> {
    state.touch();
    io::write_toml(&paths.state(), state)
}

pub fn list_roles(project: &ProjectPaths) -> Result<Vec<RoleSlug>> {
    list_slug_dirs(&project.roles_dir())
}

pub fn list_channels(project: &ProjectPaths) -> Result<Vec<ChannelSlug>> {
    list_slug_dirs(&project.channels_dir())
}

pub fn list_agents(role: &RolePaths) -> Result<Vec<AgentId>> {
    list_slug_dirs(&role.agents_dir())
}

fn list_slug_dirs<S>(dir: &Path) -> Result<Vec<S>>
where
    S: std::str::FromStr<Err = anyhow::Error> + Ord + ToString,
{
    let mut values = io::collect_existing_dir(dir, |entry| {
        if !entry
            .file_type()
            .with_context(|| format!("Failed to inspect `{}`", entry.path().display()))?
            .is_dir()
        {
            return Ok(None);
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Ok(None);
        };
        Ok(Some(name.parse()?))
    })?;
    values.sort_by(|left: &S, right: &S| {
        natural_cmp(&left.to_string(), &right.to_string()).then_with(|| left.cmp(right))
    });
    Ok(values)
}

fn natural_cmp(left: &str, right: &str) -> Ordering {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        let left_digit = left[left_index].is_ascii_digit();
        let right_digit = right[right_index].is_ascii_digit();
        if left_digit && right_digit {
            let left_start = left_index;
            let right_start = right_index;
            while left_index < left.len() && left[left_index].is_ascii_digit() {
                left_index += 1;
            }
            while right_index < right.len() && right[right_index].is_ascii_digit() {
                right_index += 1;
            }
            let left_digits = std::str::from_utf8(&left[left_start..left_index]).unwrap_or("");
            let right_digits = std::str::from_utf8(&right[right_start..right_index]).unwrap_or("");
            let left_number = left_digits.trim_start_matches('0');
            let right_number = right_digits.trim_start_matches('0');
            let left_number = if left_number.is_empty() {
                "0"
            } else {
                left_number
            };
            let right_number = if right_number.is_empty() {
                "0"
            } else {
                right_number
            };
            let ordering = left_number
                .len()
                .cmp(&right_number.len())
                .then_with(|| left_number.cmp(right_number))
                .then_with(|| left_digits.len().cmp(&right_digits.len()));
            if ordering != Ordering::Equal {
                return ordering;
            }
        } else {
            let left_start = left_index;
            let right_start = right_index;
            while left_index < left.len() && !left[left_index].is_ascii_digit() {
                left_index += 1;
            }
            while right_index < right.len() && !right[right_index].is_ascii_digit() {
                right_index += 1;
            }
            let ordering = left[left_start..left_index].cmp(&right[right_start..right_index]);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
    }
    left.len().cmp(&right.len())
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::natural_cmp;

    #[test]
    fn natural_sort_orders_embedded_numbers_by_value() {
        let mut values = vec!["e11", "e2", "e1", "e02", "e10"];
        values.sort_by(|left, right| natural_cmp(left, right));
        assert_eq!(values, ["e1", "e2", "e02", "e10", "e11"]);
    }
}
