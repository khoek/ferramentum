use std::io::IsTerminal;

use crossterm::style::Stylize;

use crate::config::RoleStatus;
use crate::state::AgentStatus;

const BULLET: &str = "●";
const AGENT_BULLET: &str = "●";
pub const FIELD_SEPARATOR: &str = " · ";

pub fn section_heading(value: &str) -> String {
    if !std::io::stdout().is_terminal() {
        return value.to_owned();
    }
    value.bold().to_string()
}

pub fn agent_summary(value: &str) -> String {
    if !std::io::stdout().is_terminal() {
        return value.to_owned();
    }
    if value == "*name loading*" {
        value.dark_grey().italic().to_string()
    } else {
        value.to_owned()
    }
}

pub fn spinner_frame(index: usize) -> &'static str {
    const FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    FRAMES[index % FRAMES.len()]
}

pub fn status_role(value: RoleStatus) -> String {
    if !std::io::stdout().is_terminal() {
        return value.to_string();
    }
    match value {
        RoleStatus::Draft => value.to_string().dark_grey().to_string(),
        RoleStatus::Active => value.to_string().green().to_string(),
        RoleStatus::Paused => value.to_string().yellow().to_string(),
    }
}

pub fn status_role_padded(value: RoleStatus, width: usize) -> String {
    pad_styled(&value.to_string(), status_role(value), width)
}

pub fn role_bullet(value: RoleStatus) -> String {
    if !std::io::stdout().is_terminal() {
        return BULLET.to_owned();
    }
    match value {
        RoleStatus::Draft => BULLET.dark_grey().to_string(),
        RoleStatus::Active => BULLET.green().to_string(),
        RoleStatus::Paused => BULLET.yellow().to_string(),
    }
}

pub fn status_agent(value: AgentStatus) -> String {
    if !std::io::stdout().is_terminal() {
        return value.to_string();
    }
    match value {
        AgentStatus::Starting => value.to_string().cyan().to_string(),
        AgentStatus::Running => value.to_string().green().to_string(),
        AgentStatus::Paused => value.to_string().yellow().to_string(),
        AgentStatus::Done => value.to_string().blue().to_string(),
        AgentStatus::Stopped => value.to_string().dark_grey().to_string(),
        AgentStatus::NeedsAttention => value.to_string().red().to_string(),
    }
}

pub fn status_agent_padded(value: AgentStatus, width: usize) -> String {
    pad_styled(&value.to_string(), status_agent(value), width)
}

pub fn agent_bullet(value: AgentStatus) -> String {
    if !std::io::stdout().is_terminal() {
        return AGENT_BULLET.to_owned();
    }
    match value {
        AgentStatus::Starting => AGENT_BULLET.cyan().to_string(),
        AgentStatus::Running => AGENT_BULLET.green().to_string(),
        AgentStatus::Paused => AGENT_BULLET.yellow().to_string(),
        AgentStatus::Done => AGENT_BULLET.blue().to_string(),
        AgentStatus::Stopped => AGENT_BULLET.dark_grey().to_string(),
        AgentStatus::NeedsAttention => AGENT_BULLET.red().to_string(),
    }
}

pub fn queue_count(value: usize) -> String {
    if !std::io::stdout().is_terminal() {
        return value.to_string();
    }
    match value {
        0 => value.to_string().dark_grey().to_string(),
        1..=2 => value.to_string().yellow().to_string(),
        _ => value.to_string().red().bold().to_string(),
    }
}

fn pad_styled(raw: &str, styled: String, width: usize) -> String {
    let padding = width.saturating_sub(raw.chars().count());
    if padding == 0 {
        styled
    } else {
        format!("{styled}{}", " ".repeat(padding))
    }
}
