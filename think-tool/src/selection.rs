use std::io::IsTerminal;
use std::str::FromStr;

use anyhow::{Context, Result, bail};

use crate::ids::{AgentId, RoleSlug};
use crate::state::{
    AgentState, AgentStatus, ProjectPaths, RolePaths, list_agents, list_roles, load_agent,
};
use crate::terminal_editor::{ChoicePrompt, PromptEditor, UserCancelled};

#[derive(Debug, Clone)]
pub struct AgentSpec {
    pub role: Option<RoleSlug>,
    pub agent: AgentId,
}

impl FromStr for AgentSpec {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        if let Some((role, agent)) = value.split_once('/') {
            if agent.contains('/') {
                bail!("agent selector `{value}` must have the form ROLE/AGENT");
            }
            return Ok(Self {
                role: Some(RoleSlug::parse(role)?),
                agent: AgentId::parse(agent)?,
            });
        }
        Ok(Self {
            role: None,
            agent: AgentId::parse(value)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AttachSpec {
    raw: String,
}

impl AttachSpec {
    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

impl FromStr for AttachSpec {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        if let Some((role, agent)) = value.split_once('/') {
            if agent.contains('/') {
                bail!("attach selector `{value}` must have the form ROLE/AGENT, ROLE, or AGENT");
            }
            RoleSlug::parse(role)?;
            AgentId::parse(agent)?;
        } else {
            AgentId::parse(value)?;
        }
        Ok(Self {
            raw: value.to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedAgent {
    pub role: RoleSlug,
    pub agent: AgentId,
}

impl ResolvedAgent {
    pub fn label(&self) -> String {
        format!("{}/{}", self.role, self.agent)
    }
}

pub enum AgentChoice {
    Existing(ResolvedAgent),
    New,
}

pub fn resolve_agent(project: &ProjectPaths, spec: AgentSpec) -> Result<ResolvedAgent> {
    if let Some(role) = spec.role {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        if role_paths.agent(spec.agent.clone()).state().exists() {
            return Ok(ResolvedAgent {
                role,
                agent: spec.agent,
            });
        }
        bail!("No agent `{}/{}` exists.", role, spec.agent);
    }

    let mut matches = find_agents_by_id(project, &spec.agent)?;
    match matches.len() {
        0 => bail!("No agent `{}` exists in this project.", spec.agent),
        1 => Ok(matches.remove(0)),
        _ => bail!(
            "Agent `{}` is ambiguous; use ROLE/AGENT. Matches: {}",
            spec.agent,
            matches
                .iter()
                .map(ResolvedAgent::label)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub fn choose_agent(project: &ProjectPaths, prompt: &str) -> Result<ResolvedAgent> {
    let agents = list_all_agents(project)?;
    match agents.len() {
        0 => bail!("No agents exist in this project."),
        1 => Ok(agents.into_iter().next().expect("one agent exists")),
        _ if interactive() => {
            let labels = agents
                .iter()
                .map(|agent| agent_label(project, agent))
                .collect::<Result<Vec<_>>>()?;
            let selection = ChoicePrompt::new(prompt, labels)
                .default(0)
                .select()
                .context("Failed to read agent selection")?;
            Ok(agents[selection].clone())
        }
        _ => bail!("Pass an agent selector when running noninteractively."),
    }
}

pub fn choose_agent_or_new(project: &ProjectPaths, prompt: &str) -> Result<AgentChoice> {
    let mut rows = list_agents_for_choice(project, false)?
        .into_iter()
        .map(|agent| agent_choice_row(project, agent))
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.agent.label().cmp(&right.agent.label()))
    });
    if !interactive() {
        bail!("Pass an agent selector or `--new` when running noninteractively.");
    }
    let mut labels = vec!["+ new agent".to_owned()];
    labels.extend(rows.iter().map(|row| row.label.clone()));
    let selection = ChoicePrompt::new(prompt, labels)
        .default(usize::from(!rows.is_empty()))
        .shortcut('n', 0, "new")
        .select()
        .context("Failed to read agent selection")?;
    if selection == 0 {
        Ok(AgentChoice::New)
    } else {
        Ok(AgentChoice::Existing(
            rows.get(selection - 1)
                .expect("agent selection is shifted by one")
                .agent
                .clone(),
        ))
    }
}

pub fn resolve_or_choose_agent(
    project: &ProjectPaths,
    spec: Option<AgentSpec>,
    prompt: &str,
) -> Result<ResolvedAgent> {
    match spec {
        Some(spec) => resolve_agent(project, spec),
        None => choose_agent(project, prompt),
    }
}

pub fn resolve_or_choose_agent_or_new(
    project: &ProjectPaths,
    spec: Option<AgentSpec>,
    prompt: &str,
) -> Result<AgentChoice> {
    match spec {
        Some(spec) => Ok(AgentChoice::Existing(resolve_agent(project, spec)?)),
        None => choose_agent_or_new(project, prompt),
    }
}

pub fn choose_role(project: &ProjectPaths, prompt: &str) -> Result<RoleSlug> {
    let roles = list_roles(project)?;
    choose_slug(roles, prompt, "role")
}

pub fn resolve_or_choose_role(
    project: &ProjectPaths,
    role: Option<RoleSlug>,
    prompt: &str,
) -> Result<RoleSlug> {
    match role {
        Some(role) => Ok(role),
        None => choose_role(project, prompt),
    }
}

pub fn resolve_or_prompt_new_slug<S>(slug: Option<S>, prompt: &str) -> Result<S>
where
    S: FromStr<Err = anyhow::Error>,
{
    match slug {
        Some(slug) => Ok(slug),
        None => prompt_new_slug(prompt),
    }
}

pub fn prompt_new_slug<S>(prompt: &str) -> Result<S>
where
    S: FromStr<Err = anyhow::Error>,
{
    if !interactive() {
        bail!("Pass a slug when running noninteractively.");
    }
    let Some(value) = PromptEditor::new(prompt)
        .help("Enter a slug. Blank submit cancels.")
        .edit()
        .context("Failed to read slug")?
    else {
        return Err(UserCancelled::new("slug entry cancelled").into());
    };
    S::from_str(value.trim())
}

pub fn resolve_attach(project: &ProjectPaths, spec: Option<AttachSpec>) -> Result<AttachTarget> {
    let Some(spec) = spec else {
        return Ok(AttachTarget::Project);
    };
    if spec.as_str().contains('/') {
        return Ok(AttachTarget::Agent(resolve_agent(
            project,
            AgentSpec::from_str(spec.as_str())?,
        )?));
    }

    let role = RoleSlug::parse(spec.as_str())?;
    let role_exists = project.role_dir(&role).exists();
    let agent = AgentId::parse(spec.as_str())?;
    let agent_matches = find_agents_by_id(project, &agent)?;
    match (role_exists, agent_matches.len()) {
        (true, 0) => Ok(AttachTarget::Role(role)),
        (false, 1) => Ok(AttachTarget::Agent(
            agent_matches.into_iter().next().expect("one agent exists"),
        )),
        (false, 0) => bail!("No role or agent `{}` exists.", spec.as_str()),
        _ => bail!("Selector `{}` is ambiguous; use ROLE/AGENT.", spec.as_str()),
    }
}

pub enum AttachTarget {
    Project,
    Role(RoleSlug),
    Agent(ResolvedAgent),
}

fn choose_slug<S>(values: Vec<S>, prompt: &str, kind: &str) -> Result<S>
where
    S: Clone + ToString,
{
    match values.len() {
        0 => bail!("No {kind}s exist."),
        1 => Ok(values.into_iter().next().expect("one value exists")),
        _ if interactive() => {
            let labels = values.iter().map(ToString::to_string).collect::<Vec<_>>();
            let selection = ChoicePrompt::new(prompt, labels)
                .default(0)
                .select()
                .with_context(|| format!("Failed to read {kind} selection"))?;
            Ok(values[selection].clone())
        }
        _ => bail!("Pass a {kind} when running noninteractively."),
    }
}

fn list_all_agents(project: &ProjectPaths) -> Result<Vec<ResolvedAgent>> {
    list_agents_for_choice(project, true)
}

fn list_agents_for_choice(
    project: &ProjectPaths,
    include_archived: bool,
) -> Result<Vec<ResolvedAgent>> {
    let mut agents = Vec::new();
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        for agent in list_agents(&role_paths)? {
            if !include_archived && load_agent(&role_paths.agent(agent.clone()))?.archived {
                continue;
            }
            agents.push(ResolvedAgent {
                role: role.clone(),
                agent,
            });
        }
    }
    Ok(agents)
}

fn find_agents_by_id(project: &ProjectPaths, agent: &AgentId) -> Result<Vec<ResolvedAgent>> {
    let mut matches = Vec::new();
    for role in list_roles(project)? {
        if RolePaths::new(project.clone(), role.clone())
            .agent(agent.clone())
            .state()
            .exists()
        {
            matches.push(ResolvedAgent {
                role,
                agent: agent.clone(),
            });
        }
    }
    Ok(matches)
}

struct AgentChoiceRow {
    agent: ResolvedAgent,
    label: String,
    created_at: u64,
    updated_at: u64,
}

fn agent_choice_row(project: &ProjectPaths, agent: ResolvedAgent) -> Result<AgentChoiceRow> {
    let state = load_agent(
        &RolePaths::new(project.clone(), agent.role.clone()).agent(agent.agent.clone()),
    )?;
    Ok(AgentChoiceRow {
        label: agent_label_from_state(&agent, &state),
        created_at: state.created_at,
        updated_at: state.updated_at,
        agent,
    })
}

fn agent_label(project: &ProjectPaths, agent: &ResolvedAgent) -> Result<String> {
    let state = load_agent(
        &RolePaths::new(project.clone(), agent.role.clone()).agent(agent.agent.clone()),
    )?;
    Ok(agent_label_from_state(agent, &state))
}

fn agent_label_from_state(agent: &ResolvedAgent, state: &AgentState) -> String {
    let archived = if state.archived { " archived" } else { "" };
    format!(
        "{}/{} · {}{}",
        agent.role,
        agent.agent,
        agent_status(state.status),
        archived
    )
}

fn agent_status(status: AgentStatus) -> &'static str {
    match status {
        AgentStatus::Starting => "starting",
        AgentStatus::Running => "running",
        AgentStatus::Paused => "paused",
        AgentStatus::Done => "done",
        AgentStatus::Stopped => "stopped",
        AgentStatus::NeedsAttention => "needs-attention",
    }
}

fn interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::AgentSpec;

    #[test]
    fn agent_spec_accepts_bare_agent_or_role_qualified_agent() {
        let bare = AgentSpec::from_str("1").expect("bare agent selector parses");
        assert_eq!(bare.role.map(|role| role.to_string()), None);
        assert_eq!(bare.agent.to_string(), "1");

        let qualified = AgentSpec::from_str("episode/1").expect("qualified agent selector parses");
        assert_eq!(
            qualified.role.map(|role| role.to_string()),
            Some("episode".to_owned())
        );
        assert_eq!(qualified.agent.to_string(), "1");
    }

    #[test]
    fn agent_spec_rejects_multiple_slashes() {
        assert!(AgentSpec::from_str("episode/1/again").is_err());
    }
}
