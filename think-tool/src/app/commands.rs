use super::*;

pub(super) fn run_child_command(command: RunChildCommand) -> Result<()> {
    match command {
        RunChildCommand::Orchestrator(args) => run_orchestrator(args),
        RunChildCommand::Notices(args) => run_notices(args),
    }
}

pub(super) fn run_advanced_command(command: AdvancedCommand) -> Result<()> {
    match command {
        AdvancedCommand::RetryErrored => {
            let project = current_project()?;
            retry_waits_now(&project)?;
            wake_project(&project)
        }
        AdvancedCommand::Trigger(args) => trigger_role_manually(args),
        AdvancedCommand::Provider(command) => run_provider_command(command),
    }
}

pub(super) fn open_project_directory() -> Result<()> {
    let project = current_project()?;
    open_project_directory_at(&project)
}

pub(super) fn open_project_directory_at(project: &ProjectPaths) -> Result<()> {
    open_path(&project.root)
}

pub(super) fn open_path(path: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = Command::new("open");
    #[cfg(target_os = "linux")]
    let mut command = Command::new("xdg-open");
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command.arg("/C").arg("start").arg("");
        command
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    compile_error!("Unsupported platform for opening directories");

    let status = command.arg(path).status().with_context(|| {
        format!(
            "Failed to open `{}` with the platform opener",
            path.display()
        )
    })?;
    if !status.success() {
        bail!(
            "platform opener for `{}` exited with {status}",
            path.display()
        );
    }
    Ok(())
}

pub(super) fn copy_to_clipboard(text: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let candidates: Vec<(&str, Vec<&str>)> = vec![("pbcopy", vec![])];
    #[cfg(target_os = "linux")]
    let candidates: Vec<(&str, Vec<&str>)> = vec![
        ("wl-copy", vec![]),
        ("xclip", vec!["-selection", "clipboard"]),
        ("xsel", vec!["--clipboard", "--input"]),
    ];
    #[cfg(target_os = "windows")]
    let candidates: Vec<(&str, Vec<&str>)> = vec![("clip", vec![])];
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    compile_error!("Unsupported platform for copying to clipboard");

    let mut errors = Vec::new();
    for (program, args) in candidates {
        let mut child = match Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(err) => {
                errors.push(format!("{program}: {err}"));
                continue;
            }
        };
        child
            .stdin
            .as_mut()
            .context("clipboard command stdin was unavailable")?
            .write_all(text.as_bytes())?;
        let status = child.wait()?;
        if status.success() {
            return Ok(());
        }
        errors.push(format!("{program}: exited with {status}"));
    }
    bail!("failed to copy to clipboard: {}", errors.join("; "))
}

pub(super) fn run_provider_command(command: ProviderCommand) -> Result<()> {
    match command {
        ProviderCommand::Codex(command) => run_codex_provider_command(command),
    }
}

pub(super) fn run_codex_provider_command(command: CodexProviderCommand) -> Result<()> {
    match command {
        CodexProviderCommand::Login(args) => codex_provider_login(args),
        CodexProviderCommand::List => codex_provider_list(),
        CodexProviderCommand::Use(args) => codex_provider_use(args),
        CodexProviderCommand::Config(args) => codex_provider_config(args),
    }
}

pub(super) fn codex_provider_login(args: CodexLoginArgs) -> Result<()> {
    let account = read_codex_account_name(args.account, "Codex Account")?;
    crate::provider::codex::authenticate_account(&account, args.home)?;
    println!("authenticated Codex account `{account}`");
    Ok(())
}

pub(super) fn codex_provider_list() -> Result<()> {
    let state = crate::provider::codex::list_accounts()?;
    for (account, data) in &state.accounts {
        let marker = if *account == state.active_account {
            "*"
        } else {
            " "
        };
        let quota = crate::provider::codex::load_account_rate_limits(account, &data.codex_home)
            .map(|limits| limits.to_string())
            .unwrap_or_else(|| "usage unavailable".to_owned());
        let wait = data
            .quota_wait_until
            .map(|timestamp| {
                format!(
                    "{}quota wait until {}",
                    ui::FIELD_SEPARATOR,
                    format_unix_time(timestamp)
                )
            })
            .unwrap_or_default();
        println!(
            "{marker} {account:<16} {}{}{}",
            data.codex_home.display(),
            ui::FIELD_SEPARATOR,
            quota
        );
        if !wait.is_empty() {
            println!("  {wait}");
        }
    }
    Ok(())
}

pub(super) fn codex_provider_use(args: CodexUseArgs) -> Result<()> {
    let account = match args.account {
        Some(account) => account,
        None => choose_codex_account()?,
    };
    crate::provider::codex::set_active_account(&account)?;
    println!("active Codex account: `{account}`");
    Ok(())
}

pub(super) fn codex_provider_config(args: CodexConfigArgs) -> Result<()> {
    let project = current_project()?;
    let mut config = project_config(&project)?;
    if args.model.is_none() && args.thinking.is_none() {
        println!(
            "model: {}",
            config
                .providers
                .codex
                .model
                .as_deref()
                .unwrap_or("(Codex default)")
        );
        println!(
            "thinking: {}",
            config
                .providers
                .codex
                .thinking_level
                .map(|level| level.to_string())
                .unwrap_or_else(|| "(Codex default)".to_owned())
        );
        return Ok(());
    }
    if let Some(model) = args.model {
        config.providers.codex.model = Some(model);
    }
    if let Some(thinking_level) = args.thinking {
        config.providers.codex.thinking_level = Some(thinking_level);
    }
    io::write_toml(&project.config(), &config)?;
    println!(
        "updated Codex provider config in {}",
        project.config().display()
    );
    Ok(())
}

pub(super) fn codex_provider_config_interactive(project: &ProjectPaths) -> Result<()> {
    let mut config = project_config(project)?;
    let catalog = crate::provider::codex::load_model_catalog(&config.providers.codex)
        .context("Failed to load Codex model catalog")?;
    let (model_labels, model_choices, model_default) =
        codex_model_choices(config.providers.codex.model.as_deref(), &catalog);
    let model_selection = ChoicePrompt::new("Codex Model", model_labels)
        .default(model_default)
        .select()
        .context("Failed to choose Codex model")?;
    let selected_model = match model_choices[model_selection] {
        CodexModelChoice::KeepCurrent => config.providers.codex.model.clone(),
        CodexModelChoice::Default => None,
        CodexModelChoice::Catalog(index) => Some(catalog[index].slug.clone()),
    };
    config.providers.codex.model = selected_model;

    let current_thinking = config
        .providers
        .codex
        .thinking_level
        .map(|level| level.to_string())
        .unwrap_or_else(|| "Codex default".to_owned());
    let supported_levels =
        codex_supported_thinking_levels(config.providers.codex.model.as_deref(), &catalog);
    let mut choices = vec![format!("keep current ({current_thinking})")];
    choices.extend(supported_levels.iter().map(ToString::to_string));
    choices.push("unset to Codex default".to_owned());
    let selection = ChoicePrompt::new("Codex Thinking", choices)
        .default(0)
        .select()
        .context("Failed to read Codex thinking level")?;
    config.providers.codex.thinking_level = if selection == 0 {
        config.providers.codex.thinking_level
    } else if selection == supported_levels.len() + 1 {
        None
    } else {
        Some(supported_levels[selection - 1])
    };
    io::write_toml(&project.config(), &config)
}

#[derive(Clone, Copy)]
pub(super) enum CodexModelChoice {
    KeepCurrent,
    Default,
    Catalog(usize),
}

pub(super) fn codex_model_choices(
    current: Option<&str>,
    catalog: &[crate::provider::codex::ModelCatalogEntry],
) -> (Vec<String>, Vec<CodexModelChoice>, usize) {
    let current_index =
        current.and_then(|current| catalog.iter().position(|model| model.slug == current));
    let mut labels = Vec::new();
    let mut choices = Vec::new();
    if let Some(current) = current.filter(|_| current_index.is_none()) {
        labels.push(format!("keep current ({current})"));
        choices.push(CodexModelChoice::KeepCurrent);
    }
    let default_index = labels.len();
    labels.push("Codex default".to_owned());
    choices.push(CodexModelChoice::Default);
    for (index, model) in catalog.iter().enumerate() {
        labels.push(codex_model_label(model));
        choices.push(CodexModelChoice::Catalog(index));
    }
    let selected = current_index
        .map(|index| default_index + 1 + index)
        .unwrap_or(default_index.min(labels.len().saturating_sub(1)));
    (labels, choices, selected)
}

pub(super) fn codex_model_label(model: &crate::provider::codex::ModelCatalogEntry) -> String {
    let name = if model.display_name == model.slug {
        model.slug.clone()
    } else {
        format!("{} ({})", model.display_name, model.slug)
    };
    match &model.description {
        Some(description) => format!("{name} · {}", ellipsize_display(description, 80)),
        None => name,
    }
}

pub(super) fn codex_supported_thinking_levels(
    model: Option<&str>,
    catalog: &[crate::provider::codex::ModelCatalogEntry],
) -> Vec<CodexThinkingLevel> {
    let levels = model
        .and_then(|model| catalog.iter().find(|entry| entry.slug == model))
        .map(|entry| entry.supported_reasoning_levels.clone())
        .unwrap_or_else(|| {
            catalog
                .iter()
                .flat_map(|entry| entry.supported_reasoning_levels.iter().copied())
                .collect()
        });
    let mut unique = Vec::new();
    for level in levels {
        if !unique.contains(&level) {
            unique.push(level);
        }
    }
    unique
}

pub(super) fn read_codex_account_name(account: Option<String>, title: &str) -> Result<String> {
    if let Some(account) = account {
        let account = account.trim().to_owned();
        if !account.is_empty() {
            return Ok(account);
        }
    }
    let Some(account) = PromptEditor::new(title)
        .help("Enter a short account name, for example `work` or `personal`.")
        .help("Leave blank and submit to cancel.")
        .edit()
        .context("Failed to read Codex account name")?
    else {
        return Err(UserCancelled::new("Codex account selection cancelled").into());
    };
    let account = account.trim().to_owned();
    if account.is_empty() {
        return Err(UserCancelled::new("Codex account selection cancelled").into());
    }
    Ok(account)
}

pub(super) fn choose_codex_account() -> Result<String> {
    let state = crate::provider::codex::list_accounts()?;
    let accounts = state.accounts.keys().cloned().collect::<Vec<_>>();
    let default = accounts
        .iter()
        .position(|account| *account == state.active_account)
        .unwrap_or(0);
    let selected = ChoicePrompt::new("Codex Account", accounts.clone())
        .default(default)
        .select()
        .context("Failed to choose Codex account")?;
    Ok(accounts[selected].clone())
}

pub(super) fn print_help(args: HelpArgs) -> Result<()> {
    let mut command = if args.all {
        Cli::command().mut_subcommands(|subcommand| match subcommand.get_name() {
            name if name.starts_with("__") => subcommand,
            "run-child" => subcommand,
            _ => subcommand.hide(false),
        })
    } else {
        Cli::command()
    };
    command.print_long_help()?;
    Ok(())
}

pub(super) fn new_agent(project: &ProjectPaths, args: NewAgentArgs) -> Result<()> {
    let role = match args.role {
        Some(role) => role,
        None => choose_role_for_agent(project)?,
    };
    let custom_prompt = match custom_agent_prompt(&role, args.prompt, args.no_prompt)? {
        CustomAgentPrompt::Prompt(prompt) => Some(prompt),
        CustomAgentPrompt::Default => None,
        CustomAgentPrompt::Cancel => {
            println!("agent creation cancelled");
            return Ok(());
        }
    };
    let role_paths = RolePaths::new(project.clone(), role.clone());
    let mut config = load_role_config(&role_paths)?;
    config.status = RoleStatus::Active;
    save_role_config(&role_paths, &config)?;
    let agent = start_one_agent(&role_paths, &config, custom_prompt.as_deref())?;
    if args.attach {
        run_attach_viewer(project, AttachTarget::Agent(ResolvedAgent { role, agent }))?;
    }
    Ok(())
}

pub(super) fn choose_role_for_agent(project: &ProjectPaths) -> Result<RoleSlug> {
    if let Some(default_role) = project_config(project)?.default_role {
        let paths = RolePaths::new(project.clone(), default_role.clone());
        if paths.config().exists() && load_role_config(&paths)?.status != RoleStatus::Paused {
            return Ok(default_role);
        }
    }
    let mut roles = list_roles(project)?;
    roles.retain(|role| {
        let paths = RolePaths::new(project.clone(), role.clone());
        load_role_config(&paths)
            .map(|config| config.status != RoleStatus::Paused)
            .unwrap_or(false)
    });
    match roles.len() {
        0 => bail!("No runnable roles found. Create one with `think role new <slug>` first."),
        1 => Ok(roles.remove(0)),
        _ if std::io::stdin().is_terminal() && std::io::stderr().is_terminal() => {
            let labels = roles.iter().map(ToString::to_string).collect::<Vec<_>>();
            let selection = ChoicePrompt::new("Role", labels)
                .default(0)
                .select()
                .context("Failed to read role selection")?;
            Ok(roles.remove(selection))
        }
        _ => bail!("Pass a role slug when running `think agent new` noninteractively."),
    }
}

pub(super) enum CustomAgentPrompt {
    Prompt(String),
    Default,
    Cancel,
}

pub(super) fn custom_agent_prompt(
    role: &RoleSlug,
    prompt: Option<String>,
    no_prompt: bool,
) -> Result<CustomAgentPrompt> {
    if let Some(prompt) = prompt {
        let prompt = prompt.trim().to_owned();
        return Ok(if prompt.is_empty() {
            CustomAgentPrompt::Cancel
        } else {
            CustomAgentPrompt::Prompt(prompt)
        });
    }
    if no_prompt || !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(CustomAgentPrompt::Default);
    }
    let prompt = PromptEditor::new("Custom Agent Prompt")
        .help(format!("New agent role: {role}"))
        .help("Write instructions only for this agent. Leave blank and submit to cancel.")
        .edit()
        .context("Failed to read custom agent prompt")?;
    Ok(if let Some(prompt) = prompt {
        CustomAgentPrompt::Prompt(prompt)
    } else {
        CustomAgentPrompt::Cancel
    })
}

pub(super) fn check_project(project: &ProjectPaths) -> Result<()> {
    run_command_conversation_attached(
        &project.root,
        "check",
        assemble_check_prompt(project)?,
        AppServerPolicy::ReadOnly,
    )
}

pub(super) fn assist_project(args: AssistArgs) -> Result<()> {
    let project = current_project_awake()?;
    let Some(query) = read_query(PromptRequest::assist(), args.query)? else {
        return Err(UserCancelled::new("assist cancelled").into());
    };
    assist_project_with_query(&project, &query)
}

pub(super) fn assist_project_interactive(project: &ProjectPaths) -> Result<()> {
    let Some(query) = read_query(PromptRequest::assist(), None)? else {
        return Err(UserCancelled::new("assist cancelled").into());
    };
    assist_project_with_query(project, &query)
}

pub(super) fn assist_project_with_query(project: &ProjectPaths, query: &str) -> Result<()> {
    let prompt = assemble_assist_prompt(project, query)?;
    run_command_conversation_attached(
        &project.root,
        "assist",
        prompt,
        AppServerPolicy::WorkspaceWrite,
    )
}

pub(super) fn run_notices(args: RunNoticesArgs) -> Result<()> {
    let project = ProjectPaths::new(
        args.project
            .canonicalize()
            .context("Invalid project path")?,
    );
    let Some(_lock) = lock::FileLock::try_acquire(notice_lock_path(&project), "notice lock")?
    else {
        return Ok(());
    };
    let notice_root = notice_dir(&project);
    io::ensure_dir(&notice_root)?;
    io::write_text(&notice_current_path(&project), "")?;
    let run_root = notice_root.join(format!("app-server-{}", unix_timestamp()));
    io::ensure_dir(&run_root)?;
    let prompt_path = run_root.join("PROMPT.md");
    let reply_path = run_root.join("REPLY.md");
    io::write_text(&prompt_path, &assemble_notices_prompt(&project)?)?;
    let exit = run_app_server_file_turn(FileAppServerTurn {
        cwd: &project.root,
        command_root: &notice_root,
        turn_root: &run_root,
        prompt_path: &prompt_path,
        reply_path: &reply_path,
        steer_dir: None,
        policy: AppServerPolicy::WorkspaceWrite,
    })?;
    if !exit.success {
        io::write_text(
            &notice_current_path(&project),
            &format!(
                "notice generator failed: app-server exited with {}\n",
                exit.code
            ),
        )?;
    }
    Ok(())
}

pub(super) fn assemble_notices_prompt(project: &ProjectPaths) -> Result<String> {
    let current = notice_current_path(project);
    let journal = notice_journal_path(project);
    let mut prompt = String::new();
    writeln!(prompt, "# think dashboard notices")?;
    writeln!(
        prompt,
        "\nYou are running as the lightweight notice generator for the `think status` dashboard."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(
        prompt,
        "\nYou may only edit these files:\n- `{}`\n- `{}`",
        current.display(),
        journal.display()
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nThe current notice file must contain zero or more complete operator notice lines. ",
            "Empty means there are no abnormal, operator-actionable notices. If you identify a ",
            "complete notice while inspecting, immediately rewrite the current notice file with ",
            "the complete set of notices known so far. Each notice line must begin with exactly ",
            "`error:`, `warn:`, `action:`, or `info:`, be evidence-based, under 110 characters, ",
            "and be useful to an operator deciding what to inspect."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Only report abnormal or actionable conditions: paused roles, agents needing attention, ",
            "quota waits with retry time, locks blocking active work, failed channel publishing, ",
            "that block automation, missing orchestrators for active agents, or other technical ",
            "conditions the operator should inspect."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Never write progress, placeholder, meta, or business-as-usual lines to the current ",
            "notice file. Forbidden examples include `notice scan active`, `scanning`, `checking`, ",
            "`all clear`, `queues empty`, `idle`, `waiting`, and ordinary empty-queue or idle-orchestrator ",
            "status. If there are no abnormal notices, leave the current notice file empty."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Do not report inert stale lock files for done, stopped, paused, archived, or inactive-role ",
            "agents. Lock files are actionable only when they block work that durable state says should ",
            "currently be running."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Append a timestamped compact entry to the journal file after the scan. Check agent ",
            "health, runtime orchestrator state, trigger queues, channel publishing, quota waits, ",
            "and locks blocking active work. ",
            "Do less thinking than a full project review and finish quickly."
        )
    )?;
    writeln!(
        prompt,
        "Do not start or stop agents, publish channel artifacts, edit role files, or change project configuration."
    )?;
    append_current_project_context(&mut prompt, &project.root)?;
    Ok(prompt)
}

pub(super) fn trigger_role_manually(args: TriggerArgs) -> Result<()> {
    let project = current_project_awake()?;
    let role = selection::resolve_or_choose_role(&project, args.role, "Role to trigger")?;
    let launch = if args.async_launch {
        TriggerLaunch::Async
    } else {
        TriggerLaunch::Queued {
            queue: role.to_string(),
        }
    };
    launch_triggered_role(
        &project,
        &role,
        &launch,
        TriggerCause::Manual {
            reason: args.reason,
        },
    )?;
    println!("triggered role `{role}`");
    Ok(())
}
