use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::config::CodexProviderConfig;
use crate::provider::{Invocation, QuotaDecision};

#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub provider: Option<Invocation>,
}

pub trait AgentBackend {
    fn name(&self) -> &'static str;
    fn command(&self, request: AgentCommandRequest<'_>) -> Result<CommandSpec>;
    fn quota_decision(
        &self,
        _invocation: Option<&Invocation>,
        _transcript: &str,
        _attempt: u32,
    ) -> Result<Option<QuotaDecision>> {
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AgentCommandRequest<'a> {
    pub agent_dir: &'a Path,
    pub reply_path: &'a Path,
    pub resume_session: Option<&'a str>,
    pub restart_notice: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct CodexAgent {
    config: CodexProviderConfig,
}

impl CodexAgent {
    pub fn new(config: CodexProviderConfig) -> Self {
        Self { config }
    }
}

impl AgentBackend for CodexAgent {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn command(&self, request: AgentCommandRequest<'_>) -> Result<CommandSpec> {
        crate::provider::codex::agent_command(
            request.agent_dir,
            request.reply_path,
            request.resume_session,
            request.restart_notice,
            &self.config,
        )
    }

    fn quota_decision(
        &self,
        invocation: Option<&Invocation>,
        transcript: &str,
        attempt: u32,
    ) -> Result<Option<QuotaDecision>> {
        if !crate::provider::codex::transcript_indicates_quota(transcript) {
            return Ok(None);
        }
        Ok(Some(
            crate::provider::codex::record_quota_failure_and_select_next(
                invocation, transcript, attempt,
            )?,
        ))
    }
}
