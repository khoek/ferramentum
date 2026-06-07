use super::*;

pub(super) fn current_project() -> Result<ProjectPaths> {
    let cwd = std::env::current_dir().context("Failed to read current directory")?;
    match ProjectPaths::find_from(&cwd) {
        Ok(project) => {
            let _ = remember_project(&project);
            Ok(project)
        }
        Err(err) => choose_remembered_project(err),
    }
}

pub(super) fn current_project_awake() -> Result<ProjectPaths> {
    let project = current_project()?;
    wake_project(&project)?;
    Ok(project)
}

pub(super) fn wake_project(project: &ProjectPaths) -> Result<()> {
    let Some(_lock) =
        lock::FileLock::try_acquire(project_wake_lock_path(project), "project wake lock")?
    else {
        return Ok(());
    };
    clean_stale_orchestrator_locks(project)?;
    wake_missing_orchestrators(project)
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(super) struct ProjectRegistry {
    version: u32,
    #[serde(default)]
    projects: Vec<ProjectRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ProjectRegistryEntry {
    path: PathBuf,
    last_used: u64,
}

pub(super) fn choose_remembered_project(original_error: anyhow::Error) -> Result<ProjectPaths> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(original_error);
    }
    let projects = load_remembered_projects()?;
    if projects.is_empty() {
        bail!(
            "{original_error:#}\nNo remembered projects are available in `{}`. \
             Create one with `think project new <path>`.",
            project_registry_path()?.display()
        );
    }
    let labels = projects
        .iter()
        .map(project_choice_label)
        .collect::<Vec<_>>();
    let selected = ChoicePrompt::new("Think Project", labels)
        .select()
        .context("Failed to select think project")?;
    let project = projects
        .get(selected)
        .map(|entry| ProjectPaths::new(entry.path.clone()))
        .context("Selected project disappeared")?;
    remember_project(&project)?;
    Ok(project)
}

pub(super) fn load_remembered_projects() -> Result<Vec<ProjectRegistryEntry>> {
    let mut registry = read_project_registry()?;
    registry
        .projects
        .retain(|entry| entry.path.join("think.toml").exists());
    registry
        .projects
        .sort_by(|left, right| right.last_used.cmp(&left.last_used));
    write_project_registry(&registry)?;
    Ok(registry.projects)
}

pub(super) fn remember_project(project: &ProjectPaths) -> Result<()> {
    let path = project
        .root
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize `{}`", project.root.display()))?;
    let mut registry = read_project_registry()?;
    registry.projects.retain(|entry| entry.path != path);
    registry.projects.push(ProjectRegistryEntry {
        path,
        last_used: unix_timestamp(),
    });
    registry
        .projects
        .sort_by(|left, right| right.last_used.cmp(&left.last_used));
    write_project_registry(&registry)
}

pub(super) fn read_project_registry() -> Result<ProjectRegistry> {
    let path = project_registry_path()?;
    if !path.exists() {
        return Ok(ProjectRegistry {
            version: PROJECT_REGISTRY_VERSION,
            projects: Vec::new(),
        });
    }
    let mut registry = io::read_toml::<ProjectRegistry>(&path)?;
    if registry.version != PROJECT_REGISTRY_VERSION {
        registry = ProjectRegistry {
            version: PROJECT_REGISTRY_VERSION,
            projects: Vec::new(),
        };
    }
    Ok(registry)
}

pub(super) fn write_project_registry(registry: &ProjectRegistry) -> Result<()> {
    let path = project_registry_path()?;
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    io::write_toml(&path, registry)
}

pub(super) fn project_registry_path() -> Result<PathBuf> {
    Ok(crate::maintenance::think_home()?.join("projects.toml"))
}

pub(super) fn project_choice_label(entry: &ProjectRegistryEntry) -> String {
    let project = ProjectPaths::new(entry.path.clone());
    let name = project
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project");
    let summary = project_picker_summary(&project).unwrap_or_else(|_| ProjectPickerSummary {
        badge: '!',
        parts: vec!["state unavailable".to_owned()],
    });
    let mut parts = vec![
        format!("{} {name}", summary.badge),
        format!("opened {}", event_age(entry.last_used)),
    ];
    parts.extend(summary.parts);
    format!("{}\n  {}", parts.join(" · "), project.root.display())
}

pub(super) struct ProjectPickerSummary {
    badge: char,
    parts: Vec<String>,
}

pub(super) fn project_picker_summary(project: &ProjectPaths) -> Result<ProjectPickerSummary> {
    let mut running = 0;
    let mut needs_attention = 0;
    let mut paused = 0;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role);
        for agent in list_agents(&role_paths)? {
            let state = load_agent(&role_paths.agent(agent))?;
            if state.archived {
                continue;
            }
            match state.status {
                AgentStatus::Starting | AgentStatus::Running => running += 1,
                AgentStatus::NeedsAttention => needs_attention += 1,
                AgentStatus::Paused => paused += 1,
                AgentStatus::Done | AgentStatus::Stopped => {}
            }
        }
    }

    let queued = load_status_queue_rows(project)?
        .into_iter()
        .map(|queue| queue.count)
        .sum::<usize>();

    let badge = if needs_attention > 0 {
        '!'
    } else if running > 0 {
        '●'
    } else if paused > 0 {
        '◐'
    } else {
        '○'
    };
    let mut parts = Vec::new();
    if running > 0 {
        parts.push(format!("{running} running"));
    }
    if needs_attention > 0 {
        parts.push(format!("{needs_attention} attention"));
    }
    if paused > 0 {
        parts.push(format!("{paused} paused"));
    }
    if queued > 0 {
        parts.push(format!("{queued} queued"));
    }
    if parts.is_empty() {
        parts.push("idle".to_owned());
    }
    Ok(ProjectPickerSummary { badge, parts })
}

pub(super) fn wake_missing_orchestrators(project: &ProjectPaths) -> Result<()> {
    let mut host = None;
    for role in list_roles(project)? {
        let role_paths = RolePaths::new(project.clone(), role.clone());
        let config = load_role_config(&role_paths)?;
        for agent in list_agents(&role_paths)? {
            let agent_paths = role_paths.agent(agent.clone());
            let mut state = load_agent(&agent_paths)?;
            repair_paused_agent_state(&agent_paths, &mut state)?;
            if !orchestrator_should_be_running(&config, &state)
                || lock::is_active(&orchestrator_lock_path(project, &role, &agent))?
                || orchestrator_wake_is_recent(&agent_paths)?
            {
                continue;
            }
            let host = match &host {
                Some(host) => host,
                None => {
                    host = Some(NativeSessionHost::new()?);
                    host.as_ref()
                        .expect("native session host was just initialized")
                }
            };
            let pane = host.spawn_pane(PaneSpawnRequest {
                project,
                role: &role,
                agent: &agent,
            })?;
            state.pane_id = Some(pane.pane_id);
            state.status = AgentStatus::Running;
            state.paused_by_user = false;
            state.note = Some("runtime orchestrator restarted by project wake".to_owned());
            save_agent(&agent_paths, &mut state)?;
            let mut supervisor = load_supervisor_state(&agent_paths)?;
            supervisor.last_event = Some(ORCHESTRATOR_WAKE_EVENT.to_owned());
            save_supervisor_state(&agent_paths, &supervisor)?;
            ensure_role_runtime_started(&role_paths)?;
        }
    }
    Ok(())
}

pub(super) fn clean_stale_orchestrator_locks(project: &ProjectPaths) -> Result<()> {
    let root = project.runtime_dir().join("locks").join("orchestrators");
    let Ok(role_entries) = fs::read_dir(&root) else {
        return Ok(());
    };
    for role_entry in role_entries {
        let role_entry = role_entry
            .with_context(|| format!("Failed to read lock directory `{}`", root.display()))?;
        if !role_entry
            .file_type()
            .with_context(|| format!("Failed to inspect `{}`", role_entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        for lock_entry in fs::read_dir(role_entry.path()).with_context(|| {
            format!(
                "Failed to read lock directory `{}`",
                role_entry.path().display()
            )
        })? {
            let lock_entry = lock_entry.with_context(|| {
                format!(
                    "Failed to read lock under `{}`",
                    role_entry.path().display()
                )
            })?;
            if lock_entry
                .file_type()
                .with_context(|| format!("Failed to inspect `{}`", lock_entry.path().display()))?
                .is_file()
            {
                lock::is_active(&lock_entry.path())?;
            }
        }
    }
    Ok(())
}

pub(super) fn repair_paused_agent_state(
    agent_paths: &crate::state::AgentPaths,
    state: &mut AgentState,
) -> Result<()> {
    if state.status == AgentStatus::Done
        && state
            .note
            .as_deref()
            .is_some_and(|note| note.starts_with("role is paused"))
    {
        state.status = AgentStatus::Paused;
        save_agent(agent_paths, state)?;
    }
    Ok(())
}

pub(super) fn orchestrator_should_be_running(config: &RoleConfig, state: &AgentState) -> bool {
    config.status == RoleStatus::Active
        && !state.archived
        && !state.paused_by_user
        && matches!(state.status, AgentStatus::Starting | AgentStatus::Running)
}

pub(super) fn orchestrator_wake_is_recent(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let supervisor = load_supervisor_state(agent_paths)?;
    Ok(
        supervisor.last_event.as_deref() == Some(ORCHESTRATOR_WAKE_EVENT)
            && unix_timestamp().saturating_sub(supervisor.updated_at)
                < ORCHESTRATOR_WAKE_GRACE_SECONDS,
    )
}

pub(super) fn project_wake_lock_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("locks").join("wake.lock")
}

pub(super) fn run_project_command(command: ProjectCommand) -> Result<()> {
    match command {
        ProjectCommand::New(args) => create_project(args),
        ProjectCommand::Init(args) => init_project(
            &std::env::current_dir().context("Failed to read current directory")?,
            choose_template(args.template, args.no_template)?,
        ),
    }
}

pub(super) fn create_project(args: ProjectNewArgs) -> Result<()> {
    let template = choose_template(args.template, args.no_template)?;
    io::require_empty_or_missing_dir(&args.path)?;
    io::ensure_dir(&args.path)?;
    init_project(&args.path, template)
}

pub(super) fn init_project(path: &Path, template_choice: Option<ProjectTemplate>) -> Result<()> {
    io::ensure_dir(path)?;
    let root = io::canonicalize_existing(path)?;
    let project = ProjectPaths::new(root);
    if project.config().exists() {
        bail!("`{}` is already a think project.", project.root.display());
    }
    io::write_text_if_missing(&project.project_md(), prompt::DEFAULT_PROJECT_MD)?;
    let config = ProjectConfig::with_template(template_choice);
    io::write_toml(&project.config(), &config)?;
    io::ensure_dir(&project.roles_dir())?;
    io::ensure_dir(&project.channels_dir())?;
    io::ensure_dir(&project.data_dir())?;
    io::ensure_dir(&project.runtime_dir().join("locks"))?;
    io::ensure_dir(&project.runtime_dir().join("locks").join("channels"))?;
    io::ensure_dir(&project.runtime_dir().join("locks").join("trigger-queues"))?;
    io::ensure_dir(&project.runtime_dir().join("trigger-queues"))?;
    io::ensure_dir(&project.runtime_dir().join("trigger-events"))?;
    io::ensure_dir(&project.runtime_dir().join("role-runtime"))?;
    io::ensure_dir(&project.runtime_dir().join("queue-runtime"))?;
    io::ensure_dir(&project.runtime_dir().join("sessions"))?;
    for channel in &config.channels {
        git::init_channel(&project.channel_dir(channel))?;
    }
    if let Some(template_choice) = template_choice {
        template::apply(&project, template_choice)?;
        println!("applied project template `{template_choice}`");
        maybe_draft_project_setup(&project, template_choice)?;
    }
    remember_project(&project)?;
    println!("initialized think project at {}", project.root.display());
    offer_project_next_action(&project)?;
    Ok(())
}

pub(super) fn offer_project_next_action(project: &ProjectPaths) -> Result<()> {
    if !interactive_terminal() {
        return Ok(());
    }
    match ChoicePrompt::new(
        "Next Action",
        ["open dashboard (recommended)", "review schema", "finish"],
    )
    .default(0)
    .select()
    .context("Failed to read next action")?
    {
        0 => run_dashboard(project.clone(), None, false, DASHBOARD_REFRESH_INTERVAL),
        1 => run_schema_review(project.clone()),
        2 => Ok(()),
        _ => unreachable!("choice prompt returned an invalid selection"),
    }
}

pub(super) fn run_schema_review(project: ProjectPaths) -> Result<()> {
    run_dashboard_with_initial_tab(
        project,
        None,
        false,
        DASHBOARD_REFRESH_INTERVAL,
        DashboardRoute::Schema { role: None },
    )
}

pub(super) fn maybe_draft_project_setup(
    project: &ProjectPaths,
    template_choice: ProjectTemplate,
) -> Result<()> {
    if !interactive_terminal() {
        return Ok(());
    }
    let Some(brief) = PromptEditor::new("Project Setup")
        .help(format!("Template: {template_choice}"))
        .help("Describe the actual project so the setup turn can tailor PROJECT.md and prompts.")
        .help("Leave blank and submit to keep the generated scaffold unchanged.")
        .edit()
        .context("Failed to read project setup brief")?
    else {
        return Ok(());
    };
    run_command_conversation_attached(
        &project.root,
        "project-setup",
        assemble_project_setup_prompt(project, template_choice, &brief)?,
        AppServerPolicy::WorkspaceWrite,
    )
}

pub(super) fn assemble_project_setup_prompt(
    project: &ProjectPaths,
    template_choice: ProjectTemplate,
    brief: &str,
) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think project setup")?;
    writeln!(
        prompt,
        "\nYou are running inside the optional app-server project setup pass immediately \
         after `think project new` or `think project init`."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(prompt, "Template: `{template_choice}`")?;
    writeln!(
        prompt,
        "\nUse the user's brief to tailor the generated project files. Preserve the template's \
         intended workflow and avoid deleting scaffolded structure unless the brief clearly \
         makes it irrelevant."
    )?;
    writeln!(
        prompt,
        "You may edit `PROJECT.md`, role prompt files, role configs, channel lists in \
         `think.toml`, and template scaffold files when doing so improves the project setup."
    )?;
    writeln!(
        prompt,
        "Do not start agents, modify runtime state, publish channel artifacts, or perform long \
         computations. Leave the project in a coherent initialized state."
    )?;
    writeln!(
        prompt,
        "\nBefore editing, read the generated `PROJECT.md`, `think.toml`, roles, and channel \
         configuration so your changes fit the scaffold."
    )?;
    if matches!(template_choice, ProjectTemplate::EpisodesCode) {
        writeln!(
            prompt,
            "\nFor the `episodes-code` template, setup must leave the target source repository as a \
             git checkout at `{}/repo`. If the user's brief names a repository URL, run `git clone` \
             into that directory. If it names a local filesystem directory, copy that directory in \
             its entirety into that directory. These are the only source-repository setup options. \
             If the source repository is ambiguous, do not invent one; report that setup still \
             needs the source repo. Do not push or start agents.",
            project.root.display()
        )?;
    }
    writeln!(
        prompt,
        "End with a compact operator-facing summary: what you changed, important files touched, \
         checks run or skipped, and any setup decisions still needed."
    )?;
    writeln!(prompt, "\n# User project brief\n\n{}", brief.trim())?;
    Ok(prompt)
}

pub(super) fn choose_template(
    explicit: Option<ProjectTemplate>,
    no_template: bool,
) -> Result<Option<ProjectTemplate>> {
    if explicit.is_some() || no_template {
        return Ok(explicit);
    }
    if !interactive_terminal() {
        return Ok(None);
    }
    let selection = ChoicePrompt::new(
        "Project Template",
        ["none", "episodes-math", "episodes-code"],
    )
    .default(0)
    .select()
    .context("Failed to read project template selection")?;
    Ok(match selection {
        0 => None,
        1 => Some(ProjectTemplate::EpisodesMath),
        2 => Some(ProjectTemplate::EpisodesCode),
        _ => unreachable!("choice prompt returned an invalid selection"),
    })
}

pub(super) fn interactive_terminal() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}
