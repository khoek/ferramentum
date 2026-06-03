use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, RoleSlug};
use crate::state::ProjectPaths;

const NATIVE_SESSION_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct PaneSpawnRequest<'a> {
    pub project: &'a ProjectPaths,
    pub role: &'a RoleSlug,
    pub agent: &'a AgentId,
}

#[derive(Debug, Clone)]
pub struct PaneHandle {
    pub pane_id: String,
}

pub trait SessionHost {
    fn ensure_project_session(&self, project: &ProjectPaths) -> Result<()>;
    fn spawn_pane(&self, request: PaneSpawnRequest<'_>) -> Result<PaneHandle>;
}

#[derive(Debug, Clone, Copy)]
pub struct NativeSessionHost;

#[derive(Debug, Serialize, Deserialize)]
struct NativeSessionState {
    version: u32,
    pid: Option<u32>,
    started_at: u64,
    log_path: PathBuf,
}

impl NativeSessionHost {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    fn agent_session_dir(project: &ProjectPaths, role: &RoleSlug, agent: &AgentId) -> PathBuf {
        project
            .runtime_dir()
            .join("sessions")
            .join(role.as_str())
            .join(agent.as_str())
    }

    fn agent_session_state_path(
        project: &ProjectPaths,
        role: &RoleSlug,
        agent: &AgentId,
    ) -> PathBuf {
        Self::agent_session_dir(project, role, agent).join("session.toml")
    }

    fn agent_session_log_path(project: &ProjectPaths, role: &RoleSlug, agent: &AgentId) -> PathBuf {
        Self::agent_session_dir(project, role, agent).join("orchestrator.log")
    }

    fn orchestrator_lock_path(project: &ProjectPaths, role: &RoleSlug, agent: &AgentId) -> PathBuf {
        project
            .runtime_dir()
            .join("locks")
            .join("orchestrators")
            .join(role.as_str())
            .join(format!("{}.lock", agent.as_str()))
    }

    fn handle_for(role: &RoleSlug, agent: &AgentId) -> PaneHandle {
        PaneHandle {
            pane_id: format!("native:{role}/{agent}"),
        }
    }
}

impl SessionHost for NativeSessionHost {
    fn ensure_project_session(&self, project: &ProjectPaths) -> Result<()> {
        crate::io::ensure_dir(&project.runtime_dir().join("sessions"))
    }

    fn spawn_pane(&self, request: PaneSpawnRequest<'_>) -> Result<PaneHandle> {
        self.ensure_project_session(request.project)?;
        let session_dir = Self::agent_session_dir(request.project, request.role, request.agent);
        crate::io::ensure_dir(&session_dir)?;
        if crate::lock::is_active(&Self::orchestrator_lock_path(
            request.project,
            request.role,
            request.agent,
        ))? {
            return Ok(Self::handle_for(request.role, request.agent));
        }

        let log_path = Self::agent_session_log_path(request.project, request.role, request.agent);
        let log = append_log_file(&log_path)?;
        let stderr = log
            .try_clone()
            .with_context(|| format!("Failed to clone session log `{}`", log_path.display()))?;
        let current_exe =
            std::env::current_exe().context("Failed to resolve current executable")?;
        let mut command = Command::new(current_exe);
        command
            .arg("run-child")
            .arg("orchestrator")
            .arg("--project")
            .arg(&request.project.root)
            .arg("--role")
            .arg(request.role.as_str())
            .arg("--agent")
            .arg(request.agent.as_str())
            .current_dir(&request.project.root)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        detach_orchestrator(&mut command);
        let child = command.spawn().with_context(|| {
            format!(
                "Failed to spawn orchestrator for `{}/{}`",
                request.role, request.agent
            )
        })?;
        crate::io::write_toml(
            &Self::agent_session_state_path(request.project, request.role, request.agent),
            &NativeSessionState {
                version: NATIVE_SESSION_STATE_VERSION,
                pid: Some(child.id()),
                started_at: crate::state::unix_timestamp(),
                log_path,
            },
        )?;
        Ok(Self::handle_for(request.role, request.agent))
    }
}

#[cfg(unix)]
fn detach_orchestrator(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
}

#[cfg(not(unix))]
fn detach_orchestrator(_command: &mut Command) {}

fn append_log_file(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        crate::io::ensure_dir(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open session log `{}`", path.display()))
}
