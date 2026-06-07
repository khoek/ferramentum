use super::*;

pub(super) fn run_role_command(command: RoleCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        RoleCommand::New(args) => create_role(&project, args),
        RoleCommand::Draft(args) => draft_role(&project, args),
        RoleCommand::Edit(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to edit")?;
            edit_role(&project, &role)
        }
        RoleCommand::Activate(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to activate")?;
            activate_role(&project, &role)
        }
        RoleCommand::Pause(args) => {
            let role = selection::resolve_or_choose_role(&project, args.role, "Role to pause")?;
            set_role_status(&project, &role, RoleStatus::Paused)
        }
    }
}

pub(super) fn run_agent_command(command: AgentCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        AgentCommand::New(args) => new_agent(&project, args),
        AgentCommand::Attach(args) => attach(args),
        AgentCommand::Archive(args) => archive_agent(&project, args),
        AgentCommand::Pause(args) => pause_agent(&project, args),
        AgentCommand::Stop(args) => stop_agent(&project, args),
        AgentCommand::Resume(args) => resume_agent(&project, args),
    }
}

pub(super) fn run_channel_command(command: ChannelCommand) -> Result<()> {
    let project = current_project_awake()?;
    match command {
        ChannelCommand::New(args) => {
            let channel = selection::resolve_or_prompt_new_slug(args.channel, "Channel to create")?;
            create_channel(&project, &channel)
        }
        ChannelCommand::List => list_channels_only(&project),
    }
}

pub(super) fn create_channel(project: &ProjectPaths, channel: &ChannelSlug) -> Result<()> {
    git::init_channel(&project.channel_dir(channel))?;
    let mut config = project_config(project)?;
    if !config.channels.iter().any(|existing| existing == channel) {
        config.channels.push(channel.clone());
        config.channels.sort();
        io::write_toml(&project.config(), &config)?;
    }
    println!("channel `{channel}` ready");
    Ok(())
}

pub(super) fn create_role(project: &ProjectPaths, args: RoleNewArgs) -> Result<()> {
    let role = selection::resolve_or_prompt_new_slug(args.role, "Role to create")?;
    let project_config = project_config(project)?;
    let steps = if args.steps.is_empty() {
        vec![template::default_step_slug(project_config.template)?]
    } else {
        args.steps
    };
    let agent_names = args
        .agent_names
        .unwrap_or_else(|| template::default_agent_names(project_config.template));
    let parallel = args
        .parallel
        .unwrap_or_else(|| template::default_parallel(project_config.template));

    let role_paths = RolePaths::new(project.clone(), role.clone());
    if role_paths.root().exists() {
        bail!("Role `{role}` already exists.");
    }
    io::ensure_dir(&role_paths.steps_dir())?;
    io::ensure_dir(&role_paths.agents_dir())?;
    io::write_text(
        &role_paths.role_md(),
        template::default_role_md(project_config.template),
    )?;
    for step in &steps {
        io::write_text_if_missing(
            &role_paths.step_path(step),
            template::default_step_md(project_config.template, step),
        )?;
    }
    let config = RoleConfig {
        version: ROLE_CONFIG_VERSION,
        status: if args.active {
            RoleStatus::Active
        } else {
            RoleStatus::Draft
        },
        backend: project_config.default_backend,
        mode: args.mode,
        parallel,
        agent_names,
        agent_prefix: args.agent_prefix,
        auto_archive: args.auto_archive,
        expose: args.expose,
        steps,
        triggers: Vec::new(),
    };
    validate_role_config(&config)?;
    save_role_config(&role_paths, &config)?;
    println!("role `{role}` created");
    if args.active {
        activate_role(project, &role)?;
    }
    Ok(())
}

pub(super) fn draft_role(project: &ProjectPaths, args: RoleDraftArgs) -> Result<()> {
    let role = selection::resolve_or_prompt_new_slug(args.role, "Role to draft")?;
    ensure_draft_role(project, &role)?;

    if let Some(request) = args.request.as_deref() {
        revise_role_draft(project, &role, "initial request", request)?;
    } else if should_use_interactive_review(args.no_review)
        && let Some(request) = read_multiline_request(PromptRequest::draft("Draft Role"))?
    {
        revise_role_draft(project, &role, "initial request", &request)?;
    }

    for feedback in &args.feedback {
        revise_role_draft(project, &role, "revision feedback", feedback)?;
    }

    if should_use_interactive_review(args.no_review) && !args.active {
        review_role_draft(project, &role)?;
        return Ok(());
    }

    if args.active {
        set_role_status(project, &role, RoleStatus::Active)?;
        activate_role(project, &role)?;
    }
    Ok(())
}

pub(super) fn ensure_draft_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    if role_paths.root().exists() {
        let project_config = project_config(project)?;
        io::ensure_dir(&role_paths.steps_dir())?;
        io::ensure_dir(&role_paths.agents_dir())?;
        io::write_text_if_missing(
            &role_paths.role_md(),
            template::default_role_md(project_config.template),
        )?;
        let step = template::default_step_slug(project_config.template)?;
        io::write_text_if_missing(
            &role_paths.step_path(&step),
            template::default_step_md(project_config.template, &step),
        )?;
        if !role_paths.config().exists() {
            save_role_config(
                &role_paths,
                &RoleConfig {
                    version: ROLE_CONFIG_VERSION,
                    status: RoleStatus::Draft,
                    backend: project_config.default_backend,
                    mode: RoleMode::Repeatable,
                    parallel: template::default_parallel(project_config.template),
                    agent_names: template::default_agent_names(project_config.template),
                    agent_prefix: None,
                    auto_archive: false,
                    expose: Vec::new(),
                    steps: vec![step],
                    triggers: Vec::new(),
                },
            )?;
        }
        println!("role `{role}` ready for draft revision");
        return Ok(());
    }

    create_role(
        project,
        RoleNewArgs {
            role: Some(role.clone()),
            mode: RoleMode::Repeatable,
            parallel: None,
            expose: Vec::new(),
            steps: Vec::new(),
            agent_names: None,
            agent_prefix: None,
            auto_archive: false,
            active: false,
        },
    )
}

pub(super) fn revise_role_draft(
    project: &ProjectPaths,
    role: &RoleSlug,
    label: &str,
    request: &str,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    append_draft_request(&role_paths, label, request)?;
    run_backend_role_draft(project, role, label, request)
}

pub(super) fn append_draft_request(
    role_paths: &RolePaths,
    label: &str,
    request: &str,
) -> Result<()> {
    let path = role_paths.root().join("DRAFT_REQUEST.md");
    let mut text = io::read_optional_text(&path)?.unwrap_or_default();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    writeln!(text, "## {label} ({})\n", unix_timestamp())?;
    writeln!(text, "{}\n", request.trim())?;
    io::write_text(&path, &text)
}

pub(super) fn should_use_interactive_review(no_review: bool) -> bool {
    !no_review && std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

pub(super) fn editor_command() -> String {
    std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "nano".to_owned())
}

pub(super) fn review_role_draft(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    loop {
        let choices = [
            "keep draft",
            "revise with backend",
            "show generated files",
            "edit ROLE.md",
            "start role now",
            "delete draft",
        ];
        match ChoicePrompt::new(format!("Review draft role `{role}`"), choices)
            .default(0)
            .select()
            .context("Failed to read draft review selection")?
        {
            0 => return Ok(()),
            1 => {
                if let Some(feedback) =
                    read_multiline_request(PromptRequest::draft("Revise Draft Role"))?
                {
                    revise_role_draft(project, role, "revision feedback", &feedback)?;
                }
            }
            2 => show_draft_files(&RolePaths::new(project.clone(), role.clone()))?,
            3 => edit_role(project, role)?,
            4 => {
                set_role_status(project, role, RoleStatus::Active)?;
                activate_role(project, role)?;
                return Ok(());
            }
            5 => {
                delete_draft_role(project, role)?;
                return Ok(());
            }
            _ => unreachable!("choice prompt returned an invalid selection"),
        }
    }
}

pub(super) fn show_draft_files(role_paths: &RolePaths) -> Result<()> {
    print_marked_file(&role_paths.role_md())?;
    print_marked_file(&role_paths.config())?;
    let mut steps = io::collect_existing_dir(&role_paths.steps_dir(), |entry| {
        Ok((entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            == Some("md"))
        .then(|| entry.path()))
    })?;
    steps.sort();
    for step in steps {
        print_marked_file(&step)?;
    }
    Ok(())
}

pub(super) fn print_marked_file(path: &Path) -> Result<()> {
    println!("\n--- {} ---\n{}", path.display(), io::read_text(path)?);
    Ok(())
}

pub(super) fn delete_draft_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let agents = list_agents(&role_paths)?;
    if !agents.is_empty() {
        bail!("Refusing to delete role `{role}` because it already has agents.");
    }
    fs::remove_dir_all(role_paths.root())
        .with_context(|| format!("Failed to delete draft role `{role}`"))?;
    println!("deleted draft role `{role}`");
    Ok(())
}

pub(super) fn run_backend_role_draft(
    project: &ProjectPaths,
    role: &RoleSlug,
    label: &str,
    request: &str,
) -> Result<()> {
    let project_config = project_config(project)?;
    let template = project_config
        .template
        .map(|template| template.to_string())
        .unwrap_or_else(|| "none".to_owned());
    let backend = project_config.default_backend;
    let prompt = format!(
        "You are drafting or revising a think role under roles/{role}.\n\n\
         Read PROJECT.md, think.toml, and the existing files in roles/{role}/. Apply the {label} \
         below by editing only these role files:\n\
         - roles/{role}/ROLE.md\n\
         - roles/{role}/config.toml\n\
         - roles/{role}/steps/*.md\n\
         - roles/{role}/DRAFT_NOTES.md if useful\n\n\
         Do not edit PROJECT.md, channels/, runtime/, data/, or any agent directory. Do not start \
         any agents. Leave status = \"draft\" unless the request explicitly asks for a \
         different status.\n\n\
         The role config schema is:\n\
         version = {ROLE_CONFIG_VERSION}\n\
         status = \"draft\" | \"active\" | \"paused\"\n\
         backend = \"{backend}\"\n\
         mode = \"oneshot\" | \"repeatable\" | \"infinite\"\n\
         parallel = positive integer, or parallel = \"infinite\"\n\
         agent_names = \"sequential\" | \"random-8\" | \"adjective-noun\"\n\
         expose = [\"last-agent-finished\" | \"last-agent-started\"] # optional\n\
         steps = [step slugs whose files exist in steps/<slug>.md]\n\
         triggers are optional [[triggers]] tables. A triggered role declares when it should be \
         launched. Supported trigger tables are:\n\
         kind = \"role-step-finished\", role = \"source-role\", step = \"source-step\", \
         launch = \"async\"\n\
         or kind = \"role-agent-finished\", role = \"source-role\", launch = \"queued\", \
         queue = \"queue-name\"\n\
         or kind = \"elapsed\", role = \"source-role\", interval_seconds = 3600, \
         launch = \"queued\", queue = \"queue-name\"\n\n\
         Project template: {template}. For episodes-math episode roles, normally use \
         steps = [\"work\"], mode = \"repeatable\", parallel = \"infinite\", and a concise work \
         step that treats each agent as one episode, writes durable material in work/, and \
         publishes selected artifacts through channels/. For episodes-code episode roles, use \
         the same repeatable/infinite shape, but require repo/, a private git worktree, one \
         focused local branch, a committed implementation, relevant checks, and a structured \
         branch handoff in channels/branches/. For episodes-code merge backlog work, use the \
         merger role with a tranche prompt naming the exact branches to integrate.\n\n\
         {label}:\n{request}\n"
    );
    run_command_conversation_attached(
        &project.root,
        "role-draft",
        prompt,
        AppServerPolicy::WorkspaceWrite,
    )
}

pub(super) fn edit_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let editor = editor_command();
    let mut parts = shell_words::split(&editor).context("Failed to parse editor command")?;
    if parts.is_empty() {
        parts.push("nano".to_owned());
    }
    let program = parts.remove(0);
    let status = Command::new(program)
        .args(parts)
        .arg(role_paths.role_md())
        .status()
        .context("Failed to start editor")?;
    if status.success() {
        Ok(())
    } else {
        bail!("Editor exited with status {status}")
    }
}

pub(super) fn set_role_status(
    project: &ProjectPaths,
    role: &RoleSlug,
    status: RoleStatus,
) -> Result<()> {
    set_role_status_inner(project, role, status)?;
    println!("role `{role}` is now {status}");
    Ok(())
}

pub(super) fn set_role_status_inner(
    project: &ProjectPaths,
    role: &RoleSlug,
    status: RoleStatus,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    config.status = status;
    save_role_config(&role_paths, &config)?;
    if status == RoleStatus::Paused {
        pause_agents_for_role_pause(&role_paths)?;
        refresh_role_runtime_started(&role_paths)?;
    }
    Ok(())
}

pub(super) fn pause_agents_for_role_pause(role_paths: &RolePaths) -> Result<()> {
    for agent in list_agents(role_paths)? {
        let agent_paths = role_paths.agent(agent.clone());
        let mut state = load_agent(&agent_paths)?;
        if state.archived
            || matches!(
                state.status,
                AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention
            )
        {
            continue;
        }
        state.status = AgentStatus::Paused;
        state.note = Some(role_pause_note(RoleStatus::Paused, state.paused_by_user));
        save_agent(&agent_paths, &mut state)?;
        terminate_live_agent_child(&agent_paths)?;
    }
    Ok(())
}

pub(super) fn role_pause_note(status: RoleStatus, paused_by_user: bool) -> String {
    if paused_by_user {
        format!("role is {status}; agent is paused by user")
    } else {
        format!("role is {status}")
    }
}

pub(super) fn activate_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    activate_role_inner(project, role, true)
}

pub(super) fn activate_role_inner(
    project: &ProjectPaths,
    role: &RoleSlug,
    verbose: bool,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    validate_role_config(&config)?;
    config.status = RoleStatus::Active;
    save_role_config(&role_paths, &config)?;
    resume_paused_agents(&role_paths)?;

    if let Some(target) = config.parallel.target_count() {
        let active = active_agent_count(&role_paths)?;
        let missing = target.saturating_sub(active);
        if missing == 0 {
            if verbose {
                println!("role `{role}` already has {active}/{target} active agents");
            }
            return Ok(());
        }
        for _ in 0..missing {
            start_one_agent_inner(&role_paths, &config, None, None, verbose)?;
        }
    } else if verbose {
        println!("role `{role}` is active with infinite parallelism");
    }
    Ok(())
}

pub(super) fn resume_paused_agents(role_paths: &RolePaths) -> Result<usize> {
    let mut host = None;
    let mut resumed = 0;
    for agent in list_agents(role_paths)? {
        let agent_paths = role_paths.agent(agent.clone());
        let mut state = load_agent(&agent_paths)?;
        if state.archived || state.status != AgentStatus::Paused || state.paused_by_user {
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
            project: &role_paths.project,
            role: &role_paths.role,
            agent: &agent,
        })?;
        state.status = AgentStatus::Running;
        state.paused_by_user = false;
        state.note = Some("resumed after role unpause".to_owned());
        state.pane_id = Some(pane.pane_id);
        save_agent(&agent_paths, &mut state)?;
        resumed += 1;
    }
    if resumed > 0 {
        ensure_role_runtime_started(role_paths)?;
    }
    Ok(resumed)
}

pub(super) fn toggle_role(project: &ProjectPaths, role: &RoleSlug) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    match load_role_config(&role_paths)?.status {
        RoleStatus::Active => set_role_status_inner(project, role, RoleStatus::Paused),
        RoleStatus::Paused => activate_role_inner(project, role, false),
        RoleStatus::Draft => Ok(()),
    }
}

pub(super) fn toggle_all_roles(project: &ProjectPaths) -> Result<()> {
    let roles = list_roles(project)?;
    let mut active = Vec::new();
    let mut paused = Vec::new();
    for role in roles {
        match load_role_config(&RolePaths::new(project.clone(), role.clone()))?.status {
            RoleStatus::Active => active.push(role),
            RoleStatus::Paused => paused.push(role),
            RoleStatus::Draft => {}
        }
    }
    if !active.is_empty() {
        if ConfirmPrompt::new(
            "Pause Roles",
            format!(
                "Pause {} active role{}?",
                active.len(),
                plural(active.len())
            ),
        )
        .default(false)
        .confirm()?
        {
            for role in active {
                set_role_status_inner(project, &role, RoleStatus::Paused)?;
            }
        }
        return Ok(());
    }
    if paused.is_empty() {
        return Ok(());
    }
    if ConfirmPrompt::new(
        "Unpause Roles",
        format!(
            "Unpause {} paused role{}?",
            paused.len(),
            plural(paused.len())
        ),
    )
    .default(false)
    .confirm()?
    {
        for role in paused {
            activate_role_inner(project, &role, false)?;
        }
    }
    Ok(())
}

pub(super) fn plural(value: usize) -> &'static str {
    if value == 1 { "" } else { "s" }
}

pub(super) fn start_one_agent(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
) -> Result<AgentId> {
    start_one_agent_inner(role_paths, config, custom_prompt, None, true)
}

pub(super) fn start_one_agent_with_trigger(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
    trigger_cause: Option<&TriggerCause>,
) -> Result<AgentId> {
    start_one_agent_inner(role_paths, config, custom_prompt, trigger_cause, true)
}

pub(super) fn start_one_agent_inner(
    role_paths: &RolePaths,
    config: &RoleConfig,
    custom_prompt: Option<&str>,
    trigger_cause: Option<&TriggerCause>,
    verbose: bool,
) -> Result<AgentId> {
    let (agent, agent_paths) = create_agent(role_paths, config)?;
    if let Some(custom_prompt) = custom_prompt {
        io::write_text(&agent_paths.agent_prompt(), custom_prompt)?;
    }
    if let Some(trigger_cause) = trigger_cause {
        io::write_text(
            &agent_paths.trigger_context(),
            &trigger_cause.render(&role_paths.role),
        )?;
    }
    let pane = NativeSessionHost::new()?.spawn_pane(PaneSpawnRequest {
        project: &role_paths.project,
        role: &role_paths.role,
        agent: &agent,
    })?;
    let mut state = load_agent(&agent_paths)?;
    state.pane_id = Some(pane.pane_id);
    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;
    if verbose {
        println!("started agent `{}` for role `{}`", agent, role_paths.role);
    }
    Ok(agent)
}

pub(super) fn start_one_agent_sync_with_trigger(
    role_paths: &RolePaths,
    config: &RoleConfig,
    trigger_cause: Option<&TriggerCause>,
) -> Result<AgentId> {
    let (agent, agent_paths) = create_agent(role_paths, config)?;
    if let Some(trigger_cause) = trigger_cause {
        io::write_text(
            &agent_paths.trigger_context(),
            &trigger_cause.render(&role_paths.role),
        )?;
    }
    let mut state = load_agent(&agent_paths)?;
    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.note = Some("started by queued trigger".to_owned());
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;
    run_orchestrator(RunOrchestratorArgs {
        project: role_paths.project.root.clone(),
        role: role_paths.role.clone(),
        agent: agent.clone(),
    })?;
    Ok(agent)
}

pub(super) fn create_agent(
    role_paths: &RolePaths,
    config: &RoleConfig,
) -> Result<(AgentId, crate::state::AgentPaths)> {
    let _lock = lock::FileLock::acquire(
        role_paths.project.agent_lock_path(),
        "agent allocation lock",
    )?;
    let agent = allocate_agent_id(
        role_paths,
        config.agent_names,
        config.agent_prefix.as_deref(),
    )?;
    let agent_paths = role_paths.agent(agent.clone());
    prepare_agent_work(&agent_paths)?;
    io::ensure_dir(&agent_paths.channels_dir())?;
    io::ensure_dir(&agent_paths.runs_dir())?;
    prepare_agent_data(&agent_paths)?;
    io::write_toml(&agent_paths.manifest(), &AgentManifest::default())?;

    let channels = project_config(&role_paths.project)?.channels;
    for channel in &channels {
        git::init_channel(&role_paths.project.channel_dir(channel))?;
        io::ensure_dir(&agent_paths.channel_dir(channel))?;
    }

    let mut state = AgentState::new(
        role_paths.role.clone(),
        agent.clone(),
        config.backend,
        config.mode,
        channels,
    );
    save_agent(&agent_paths, &mut state)?;
    write_exposure_context(&role_paths.project, &agent_paths, config)?;
    Ok((agent, agent_paths))
}

pub(super) fn write_exposure_context(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    config: &RoleConfig,
) -> Result<()> {
    if config.expose.is_empty() {
        return Ok(());
    }
    let mut text = String::new();
    for exposed in &config.expose {
        match exposed {
            ExposedContext::LastAgentFinished => {
                writeln!(text, "## last-agent-finished")?;
                match last_role_agent_by(project, agent_paths, LastAgentSelector::Finished)? {
                    Some(summary) => write_agent_exposure_summary(&mut text, &summary)?,
                    None => writeln!(
                        text,
                        "No previous finished agent exists for role `{}`.",
                        agent_paths.role.role
                    )?,
                }
            }
            ExposedContext::LastAgentStarted => {
                writeln!(text, "## last-agent-started")?;
                match last_role_agent_by(project, agent_paths, LastAgentSelector::Started)? {
                    Some(summary) => write_agent_exposure_summary(&mut text, &summary)?,
                    None => writeln!(
                        text,
                        "No previous started agent exists for role `{}`.",
                        agent_paths.role.role
                    )?,
                }
            }
        }
        writeln!(text)?;
    }
    io::write_text(&agent_paths.exposure_context(), text.trim_end())?;
    Ok(())
}

pub(super) enum LastAgentSelector {
    Finished,
    Started,
}

pub(super) struct AgentExposureSummary {
    role: RoleSlug,
    agent: AgentId,
    status: AgentStatus,
    run_count: u64,
    timestamp: u64,
    work_dir: PathBuf,
    root: PathBuf,
}

pub(super) fn last_role_agent_by(
    project: &ProjectPaths,
    current: &crate::state::AgentPaths,
    selector: LastAgentSelector,
) -> Result<Option<AgentExposureSummary>> {
    let mut best = None;
    for agent in list_agents(&current.role)? {
        if agent == current.agent {
            continue;
        }
        let paths = current.role.agent(agent.clone());
        let state = load_agent(&paths)?;
        let timestamp = match selector {
            LastAgentSelector::Started => state.created_at,
            LastAgentSelector::Finished => {
                let Some(exit) = state.last_exit.as_ref().filter(|exit| exit.success) else {
                    continue;
                };
                if state.status != AgentStatus::Done {
                    continue;
                }
                exit.finished_at
            }
        };
        let summary = AgentExposureSummary {
            role: current.role.role.clone(),
            agent,
            status: state.status,
            run_count: state.run_count,
            timestamp,
            work_dir: paths.work_own(),
            root: paths.root(),
        };
        if best
            .as_ref()
            .is_none_or(|best: &AgentExposureSummary| summary.timestamp > best.timestamp)
        {
            best = Some(summary);
        }
    }
    let _ = project;
    Ok(best)
}

pub(super) fn write_agent_exposure_summary(
    text: &mut String,
    summary: &AgentExposureSummary,
) -> std::fmt::Result {
    writeln!(text, "- role: `{}`", summary.role)?;
    writeln!(text, "- agent: `{}`", summary.agent)?;
    writeln!(text, "- status: `{}`", summary.status)?;
    writeln!(text, "- run count: `{}`", summary.run_count)?;
    writeln!(text, "- timestamp: `{}`", summary.timestamp)?;
    writeln!(text, "- agent root: `{}`", summary.root.display())?;
    writeln!(
        text,
        "- work/own directory: `{}`",
        summary.work_dir.display()
    )
}

pub(super) fn stop_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to stop")?;
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    state.status = AgentStatus::Stopped;
    state.paused_by_user = false;
    state.note = Some("stopped by user".to_owned());
    save_agent(&paths, &mut state)?;
    terminate_live_agent_child(&paths)?;
    refresh_role_runtime_started(&role_paths)?;
    println!("agent `{}` marked stopped", resolved.label());
    Ok(())
}

pub(super) fn pause_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to pause")?;
    pause_agent_inner(project, &resolved, true)
}

pub(super) fn pause_agent_inner(
    project: &ProjectPaths,
    resolved: &ResolvedAgent,
    verbose: bool,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    if matches!(
        state.status,
        AgentStatus::Done | AgentStatus::Stopped | AgentStatus::NeedsAttention
    ) {
        bail!(
            "Agent `{}` is {}; there is no active execution to pause.",
            resolved.label(),
            state.status
        );
    }
    state.status = AgentStatus::Paused;
    state.paused_by_user = true;
    state.note = Some("agent paused by user".to_owned());
    save_agent(&paths, &mut state)?;
    let terminated = terminate_live_agent_child(&paths)?;
    refresh_role_runtime_started(&role_paths)?;
    if verbose {
        if terminated {
            println!(
                "agent `{}` paused; live child was signalled",
                resolved.label()
            );
        } else {
            println!("agent `{}` paused", resolved.label());
        }
    }
    Ok(())
}

pub(super) fn archive_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to archive")?;
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&paths)?;
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        bail!(
            "Refusing to archive active agent `{}`. Stop it or wait until it is done first.",
            resolved.label()
        );
    }
    state.archived = true;
    state.paused_by_user = false;
    state.note = Some("archived by user".to_owned());
    save_agent(&paths, &mut state)?;
    refresh_role_runtime_started(&role_paths)?;
    println!(
        "agent `{}/{}` archived; files remain at {}",
        resolved.role,
        resolved.agent,
        paths.root().display()
    );
    Ok(())
}

pub(super) fn resume_agent(project: &ProjectPaths, args: AgentSelectorArgs) -> Result<()> {
    let resolved = selection::resolve_or_choose_agent(project, args.agent, "Agent to resume")?;
    resume_agent_inner(project, &resolved, true)
}

pub(super) fn resume_agent_inner(
    project: &ProjectPaths,
    resolved: &ResolvedAgent,
    verbose: bool,
) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let config = load_role_config(&role_paths)?;
    let agent_paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&agent_paths)?;
    state.paused_by_user = false;
    state.archived = false;
    if config.status != RoleStatus::Active {
        state.status = AgentStatus::Paused;
        state.note = Some(role_pause_note(config.status, false));
        save_agent(&agent_paths, &mut state)?;
        refresh_role_runtime_started(&role_paths)?;
        if verbose {
            println!(
                "agent `{}` unpaused but role `{}` is {}",
                resolved.label(),
                resolved.role,
                config.status
            );
        }
        return Ok(());
    }
    state.status = AgentStatus::Running;
    state.note = None;
    let pane = NativeSessionHost::new()?.spawn_pane(PaneSpawnRequest {
        project,
        role: &resolved.role,
        agent: &resolved.agent,
    })?;
    state.pane_id = Some(pane.pane_id);
    save_agent(&agent_paths, &mut state)?;
    ensure_role_runtime_started(&role_paths)?;
    if verbose {
        println!("resumed agent `{}`", resolved.label());
    }
    Ok(())
}

pub(super) fn toggle_agent(project: &ProjectPaths, resolved: &ResolvedAgent) -> Result<()> {
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let state = load_agent(&role_paths.agent(resolved.agent.clone()))?;
    if state.paused_by_user {
        resume_agent_inner(project, resolved, false)
    } else {
        pause_agent_inner(project, resolved, false)
    }
}

pub(super) fn terminate_live_agent_child(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let supervisor = load_supervisor_state(agent_paths)?;
    if supervisor.status != SupervisorStatus::Running {
        return Ok(false);
    }
    let Some(pid) = supervisor.child_pid else {
        return Ok(false);
    };
    terminate_process(pid)
}

pub(super) fn terminate_process(pid: u32) -> Result<bool> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if result == 0 {
            return Ok(true);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(err).with_context(|| format!("Failed to signal child process {pid}"))
    }

    #[cfg(not(unix))]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status()
            .with_context(|| format!("Failed to signal child process {pid}"))?;
        Ok(status.success())
    }
}
