use std::borrow::Cow;

use super::*;

pub(super) fn more_agent(args: MoreArgs) -> Result<()> {
    let project = current_project_awake()?;
    if args.new {
        return new_agent(
            &project,
            NewAgentArgs {
                role: None,
                prompt: args.query,
                no_prompt: false,
                attach: false,
            },
        );
    }
    let choice = selection::resolve_or_choose_agent_or_new(&project, args.agent, "Agent")?;
    let selection::AgentChoice::Existing(resolved) = choice else {
        return new_agent(
            &project,
            NewAgentArgs {
                role: None,
                prompt: args.query,
                no_prompt: false,
                attach: false,
            },
        );
    };
    let label = resolved.label();
    let role_paths = RolePaths::new(project.clone(), resolved.role.clone());
    let agent_paths = role_paths.agent(resolved.agent.clone());
    let mut state = load_agent(&agent_paths)?;
    let Some(query) = read_query(
        more_prompt_request(&label, &agent_paths, state.run_count)?,
        args.query,
    )?
    else {
        println!("more cancelled");
        return Ok(());
    };
    if matches!(state.status, AgentStatus::Starting | AgentStatus::Running) {
        crate::backend::enqueue_steer(&agent_paths.steer_dir(), &query)?;
        println!("sent live steer to active agent `{label}`");
        return Ok(());
    }

    let mut query = query;
    let mut query_history = agent_prompt_history(&agent_paths, state.run_count)?;
    loop {
        let transcript_path = run_more_turn(
            &project,
            &role_paths,
            &agent_paths,
            &mut state,
            &label,
            &query,
        )?;
        query_history.push(query.clone());
        if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
            break;
        }
        let Some(follow_up) = read_multiline_request(
            PromptRequest::conversation_reply("more", &transcript_path)
                .with_context(
                    "Agent Reply History",
                    agent_history_context(&agent_paths, state.run_count)?,
                )
                .with_history(query_history.clone()),
        )?
        else {
            break;
        };
        query = follow_up;
    }

    Ok(())
}

pub(super) fn more_prompt_request(
    label: &str,
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<PromptRequest> {
    Ok(PromptRequest::more(label)
        .with_context(
            "Agent Reply History",
            agent_history_context(agent_paths, run_count)?,
        )
        .with_history(agent_prompt_history(agent_paths, run_count)?))
}

pub(super) fn agent_history_context(
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<String> {
    let mut context = String::new();
    if let Some(agent_prompt) = io::read_optional_text(&agent_paths.root().join("AGENT_PROMPT.md"))?
        && !agent_prompt.trim().is_empty()
    {
        writeln!(
            context,
            "# Agent-specific prompt\n\n{}",
            agent_prompt.trim()
        )?;
    }
    if run_count == 0
        && let Some(root_prompt) = io::read_optional_text(&agent_paths.root().join("PROMPT.md"))?
        && !root_prompt.trim().is_empty()
    {
        writeln!(context, "\n# Initial prompt\n\n{}", root_prompt.trim())?;
    }
    for run_id in 1..=run_count {
        let run_paths = agent_paths.run(run_id);
        if let Some(prompt) = io::read_optional_text(&run_paths.prompt())?
            && !prompt.trim().is_empty()
        {
            if let Some(follow_up) = extract_user_follow_up(&prompt) {
                writeln!(context, "\n# Run {run_id} user follow-up\n\n{follow_up}")?;
            } else {
                writeln!(
                    context,
                    "\n# Run {run_id} prompt\n\n{}",
                    summarize_prompt_for_history(&prompt, &run_paths.prompt())
                )?;
            }
        }
        if let Some(reply) = io::read_optional_text(&run_paths.reply())?
            && !reply.trim().is_empty()
        {
            writeln!(context, "\n# Run {run_id} agent reply\n\n{}", reply.trim())?;
        } else if let Some(transcript) = io::read_optional_text(&run_paths.transcript_text())?
            && !transcript.trim().is_empty()
        {
            writeln!(
                context,
                "\n# Run {run_id} transcript tail (no REPLY.md was recorded)\n\n{}",
                transcript_tail(&transcript, AGENT_HISTORY_TRANSCRIPT_TAIL_LINES)
            )?;
        }
    }
    if context.trim().is_empty() {
        Ok("No prior prompt or transcript output has been recorded for this agent yet.".to_owned())
    } else {
        Ok(context)
    }
}

pub(super) fn summarize_prompt_for_history(prompt: &str, path: &Path) -> String {
    let title = prompt
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with('#') && !line.trim_start_matches('#').trim().is_empty())
        .map(|line| line.trim_start_matches('#').trim().to_owned())
        .unwrap_or_else(|| "managed think prompt".to_owned());
    format!("{title}\nPrompt file: {}", path.display())
}

pub(super) fn agent_prompt_history(
    agent_paths: &crate::state::AgentPaths,
    run_count: u64,
) -> Result<Vec<String>> {
    let mut history = Vec::new();
    if let Some(agent_prompt) = io::read_optional_text(&agent_paths.root().join("AGENT_PROMPT.md"))?
        && !agent_prompt.trim().is_empty()
    {
        history.push(agent_prompt.trim().to_owned());
    }
    for run_id in 1..=run_count {
        let Some(prompt) = io::read_optional_text(&agent_paths.run(run_id).prompt())? else {
            continue;
        };
        if let Some(follow_up) = extract_user_follow_up(&prompt) {
            history.push(follow_up);
        }
    }
    Ok(history)
}

pub(super) fn extract_user_follow_up(prompt: &str) -> Option<String> {
    let (_, follow_up) = prompt.split_once("# User follow-up")?;
    let follow_up = follow_up.trim();
    (!follow_up.is_empty()).then(|| follow_up.to_owned())
}

pub(super) fn transcript_tail(transcript: &str, max_lines: usize) -> String {
    let lines = transcript
        .lines()
        .map(strip_ansi_for_context)
        .map(|line| line.trim_end().to_owned())
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let start = lines
        .len()
        .saturating_sub(max_lines.max(MIN_AGENT_HISTORY_TRANSCRIPT_TAIL_LINES));
    lines[start..].join("\n")
}

pub(super) fn strip_ansi_for_context(line: &str) -> String {
    let mut output = String::new();
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else if ch != '\r' {
            output.push(ch);
        }
    }
    output
}

pub(super) fn run_more_turn(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    state: &mut AgentState,
    label: &str,
    query: &str,
) -> Result<PathBuf> {
    let run_id = state.run_count + 1;
    let run_paths = agent_paths.run(run_id);
    io::ensure_dir(&run_paths.root())?;
    let prompt = assemble_more_prompt(project, role_paths, agent_paths, state, query)?;
    io::write_text(&run_paths.prompt(), &prompt)?;

    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.archived = false;
    state.note = Some("think more follow-up".to_owned());
    save_agent(agent_paths, state)?;
    ensure_role_runtime_started(role_paths)?;

    println!("continuing agent `{label}` with app-server");
    let started_at = unix_timestamp();
    let more_run = run_more_app_server_turn(agent_paths, &run_paths);
    let finished_at = unix_timestamp();
    let more_run = match more_run {
        Ok(more_run) if more_run.exit.success => more_run,
        Ok(more_run) => {
            let exit = run_exit_from_app_server(
                run_id,
                StepSlug::parse("more")?,
                started_at,
                finished_at,
                more_run.exit,
                None,
                more_run.message,
            );
            io::write_toml(&run_paths.exit(), &exit)?;
            let mut state = load_agent(agent_paths)?;
            let code = exit.code;
            state.status = AgentStatus::NeedsAttention;
            state.note = Some(format!("think more exited with status code {}", exit.code));
            state.last_exit = Some(exit);
            save_agent(agent_paths, &mut state)?;
            refresh_role_runtime_started(role_paths)?;
            bail!("think more exited with status code {code}");
        }
        Err(err) => {
            let mut state = load_agent(agent_paths)?;
            state.status = AgentStatus::NeedsAttention;
            state.note = Some(err.to_string());
            save_agent(agent_paths, &mut state)?;
            refresh_role_runtime_started(role_paths)?;
            return Err(err);
        }
    };

    let mut updated_state = load_agent(agent_paths)?;
    finalize_channels(project, agent_paths, &updated_state, run_id)?;
    let exit = run_exit_from_app_server(
        run_id,
        StepSlug::parse("more")?,
        started_at,
        finished_at,
        more_run.exit,
        None,
        more_run.message,
    );
    io::write_toml(&run_paths.exit(), &exit)?;
    updated_state.run_count = run_id;
    updated_state.status = AgentStatus::Done;
    updated_state.paused_by_user = false;
    updated_state.note = None;
    updated_state.last_exit = Some(exit);
    save_agent(agent_paths, &mut updated_state)?;
    refresh_role_runtime_started(role_paths)?;
    *state = updated_state;
    Ok(run_paths.transcript_text())
}

pub(super) struct PromptRequest {
    title: String,
    help: Vec<String>,
    context: Option<(String, String)>,
    history: Vec<String>,
}

impl PromptRequest {
    pub(super) fn assist() -> Self {
        Self {
            title: "Project Assist".to_owned(),
            help: vec![
                "Describe what you want the project assistant to configure or inspect.".to_owned(),
                "It may run think CLI commands and edit project configuration as needed."
                    .to_owned(),
                "Blank submit cancels.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn more(agent: &str) -> Self {
        Self {
            title: format!("More For {agent}"),
            help: vec![
                "Write the follow-up query for this agent.".to_owned(),
                "The query continues the app-server thread, or steers the live turn if the agent is active."
                    .to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    pub(super) fn draft(label: &str) -> Self {
        Self {
            title: label.to_owned(),
            help: vec![
                "Describe the requested role draft or revision.".to_owned(),
                "Blank submit cancels this step.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn conversation_reply(command: &str, transcript: &Path) -> Self {
        Self {
            title: format!("Reply To think {command}"),
            help: vec![
                "The latest app-server output is shown above the editor buffer; PageUp/PageDown scrolls it."
                    .to_owned(),
                format!("Transcript: {}", transcript.display()),
                "Write a follow-up reply, or submit blank to finish.".to_owned(),
            ],
            context: None,
            history: Vec::new(),
        }
    }

    fn with_context(mut self, title: impl Into<String>, text: String) -> Self {
        self.context = Some((title.into(), text));
        self
    }

    fn with_history(mut self, history: Vec<String>) -> Self {
        self.history = history;
        self
    }
}

pub(super) fn read_multiline_request(request: PromptRequest) -> Result<Option<String>> {
    let mut editor = PromptEditor::new(request.title);
    for line in request.help {
        editor = editor.help(line);
    }
    if let Some((title, text)) = request.context {
        editor = editor.context_text(title, &text);
    }
    editor = editor.history(request.history);
    editor.edit()
}

pub(super) fn read_query(request: PromptRequest, query: Option<String>) -> Result<Option<String>> {
    if let Some(query) = query {
        let query = query.trim().to_owned();
        return Ok((!query.is_empty()).then_some(query));
    }
    if std::io::stdin().is_terminal() && std::io::stderr().is_terminal() {
        return read_multiline_request(request);
    }
    bail!("Pass a query when running noninteractively.")
}

pub(super) fn assemble_more_prompt(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    query: &str,
) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think more follow-up")?;
    writeln!(
        prompt,
        "\nYou are continuing via `think more` for agent `{}/{}`.",
        role_paths.role, agent_paths.agent
    )?;
    writeln!(
        prompt,
        "You may be in a resumed app-server thread or a new app-server thread if thread state was unavailable."
    )?;
    writeln!(
        prompt,
        "Continue in the same project context, workspace, data directories, channels, and conventions as the original run."
    )?;
    writeln!(prompt, "\nImportant files in this agent root:")?;
    writeln!(prompt, "- `PROMPT.md`: original assembled think prompt")?;
    writeln!(
        prompt,
        "- `AGENT_PROMPT.md`: original agent-specific prompt, if present"
    )?;
    writeln!(
        prompt,
        "- `agent.toml` and `manifest.toml`: current agent state and manifest"
    )?;
    writeln!(prompt, "- `runs/`: prior prompts and transcripts")?;
    writeln!(
        prompt,
        "\nDo not start over. Read enough of the prior prompts, transcripts, manifests, work directory, and channel outboxes to answer the user's follow-up in continuity."
    )?;
    writeln!(
        prompt,
        "Publish any newly finished artifacts through `channels/` before exiting, as in the original think prompt."
    )?;
    writeln!(prompt, "\n# Runtime Summary")?;
    writeln!(prompt, "- project: `{}`", project.root.display())?;
    writeln!(prompt, "- role: `{}`", role_paths.role)?;
    writeln!(prompt, "- agent: `{}`", agent_paths.agent)?;
    writeln!(prompt, "- prior run count: `{}`", state.run_count)?;
    writeln!(
        prompt,
        "- run reply file: `{}`",
        agent_paths.run(state.run_count + 1).reply().display()
    )?;
    writeln!(
        prompt,
        "\nBefore exiting, write a compact final reply to the run reply file. It should read like \
         the final answer you would give the operator for this follow-up."
    )?;
    writeln!(prompt, "\n# User follow-up\n\n{}", query.trim())?;
    Ok(prompt)
}

pub(super) fn run_more_app_server_turn(
    agent_paths: &crate::state::AgentPaths,
    run_paths: &crate::state::RunPaths,
) -> Result<MoreAppServerRun> {
    Ok(MoreAppServerRun {
        exit: run_agent_app_server_turn(AgentAppServerTurn {
            agent_paths,
            run_paths,
            prompt: &more_prompt_instruction(run_paths.run_id),
            policy: AppServerPolicy::WorkspaceWrite,
        })?,
        message: Some("continued app-server thread".to_owned()),
    })
}

pub(super) struct AgentAppServerTurn<'a> {
    pub(super) agent_paths: &'a crate::state::AgentPaths,
    pub(super) run_paths: &'a crate::state::RunPaths,
    pub(super) prompt: &'a str,
    pub(super) policy: AppServerPolicy,
}

pub(super) fn run_agent_app_server_turn(
    request: AgentAppServerTurn<'_>,
) -> Result<AppServerTurnExit> {
    run_configured_app_server_turn(request)
}

pub(super) struct MoreAppServerRun {
    exit: AppServerTurnExit,
    message: Option<String>,
}

pub(super) fn run_exit_from_app_server(
    run_id: u64,
    step: StepSlug,
    started_at: u64,
    finished_at: u64,
    exit: AppServerTurnExit,
    disposition: Option<Disposition>,
    message: Option<String>,
) -> RunExitState {
    RunExitState {
        run_id,
        step,
        started_at,
        finished_at,
        success: exit.success,
        code: exit.code,
        signal: exit.signal,
        disposition,
        message: message.or(exit.message),
    }
}

pub(super) fn more_prompt_instruction(run_id: u64) -> String {
    format!("Read runs/{run_id}/PROMPT.md in the current directory and follow it exactly.")
}

pub(super) fn assemble_check_prompt(project: &ProjectPaths) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think check")?;
    writeln!(
        prompt,
        "\nYou are running from the `think status` dashboard check action. Audit the current think project for technical health."
    )?;
    writeln!(
        prompt,
        "\nProject root: `{}`\n\nTake no permanent actions: do not edit files, do not start or stop agents, publish channel artifacts, or kill processes.",
        project.root.display()
    )?;
    writeln!(
        prompt,
        "Inspect project state, agent manifests, run transcripts, channel logs, trigger queues, and obvious runtime problems. If the project is technically healthy, summarize the nontechnical research/progress state."
    )?;
    writeln!(
        prompt,
        "If anything looks unhealthy, report the issue, evidence, severity, and the next command or fix that would address it."
    )?;
    append_think_doc_context(&mut prompt, &project.root)?;
    append_current_project_context(&mut prompt, &project.root)?;
    Ok(prompt)
}

pub(super) fn assemble_assist_prompt(project: &ProjectPaths, query: &str) -> Result<String> {
    let mut prompt = String::new();
    writeln!(prompt, "# think assist")?;
    writeln!(
        prompt,
        "\nYou are running from the `think assist` operator workflow. The user wants help operating this think project."
    )?;
    writeln!(prompt, "\nProject root: `{}`", project.root.display())?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "\nYou may take permanent project-management actions when they directly serve the user's request: ",
            "run `think` CLI commands, create or configure roles, create or continue agents, trigger roles, ",
            "change provider settings, and inspect runtime state. Prefer the public `think` CLI for operational ",
            "changes. Edit project files directly only when the CLI does not expose the needed operation or the ",
            "docs make direct editing the intended interface."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Use the same operational awareness expected of the dashboard notices agent: inspect agent health, ",
            "runtime orchestrator state, trigger queues, channel logs, quota waits, locks, manifests, transcripts, ",
            "and project docs as needed. Unlike notices/check/update, you are allowed to act."
        )
    )?;
    writeln!(
        prompt,
        "{}",
        concat!(
            "Do not invent hidden state. Read files and run commands to verify assumptions. Keep changes scoped, ",
            "preserve unrelated work, and avoid doing the mathematical research yourself unless the user's request ",
            "explicitly asks for a small direct inspection. When launching or modifying agents, make the prompt and ",
            "role choices explicit and coherent."
        )
    )?;
    writeln!(
        prompt,
        "\nBefore exiting, report exactly what you changed or launched, which commands you ran, and any remaining operator decisions."
    )?;
    append_think_doc_context(&mut prompt, &project.root)?;
    append_current_project_context(&mut prompt, &project.root)?;
    append_project_operation_snapshot(&mut prompt, project)?;
    writeln!(prompt, "\n# User assist request\n\n{}", query.trim())?;
    Ok(prompt)
}

pub(super) fn append_think_doc_context(prompt: &mut String, cwd: &Path) -> Result<()> {
    let Some(source_dir) = find_think_source_dir(cwd)? else {
        writeln!(
            prompt,
            "\n# think-tool docs\n\nNo local think-tool source directory was found; inspect the workspace directly."
        )?;
        return Ok(());
    };
    writeln!(
        prompt,
        "\n# think-tool source\n\n`{}`",
        source_dir.display()
    )?;
    append_file_section(prompt, &source_dir.join("README.md"))?;
    append_file_section(prompt, &source_dir.join("DESIGN.md"))?;
    Ok(())
}

pub(super) fn append_current_project_context(prompt: &mut String, cwd: &Path) -> Result<()> {
    let Ok(project) = ProjectPaths::find_from(cwd) else {
        writeln!(
            prompt,
            "\n# Current think project\n\nNo containing think project was found from `{}`.",
            cwd.display()
        )?;
        return Ok(());
    };
    writeln!(
        prompt,
        "\n# Current think project\n\nRoot: `{}`",
        project.root.display()
    )?;
    append_file_section(prompt, &project.project_md())?;
    append_file_section(prompt, &project.config())?;
    writeln!(
        prompt,
        "\nAgents, channels, runtime files, and transcripts live under this project root; inspect them directly when needed."
    )?;
    Ok(())
}

pub(super) fn append_project_operation_snapshot(
    prompt: &mut String,
    project: &ProjectPaths,
) -> Result<()> {
    writeln!(prompt, "\n# Current operational snapshot")?;
    writeln!(prompt, "\n## Roles and agents")?;
    for role in load_status_roles(project, None, true)? {
        writeln!(
            prompt,
            "- role `{}`: status {}, mode {}, parallel {}, expose {}",
            role.name, role.status, role.mode, role.parallel, role.expose
        )?;
        for agent in role.agents {
            let detail = if agent.detail.trim().is_empty() {
                String::new()
            } else {
                format!("; {}", agent.detail)
            };
            writeln!(
                prompt,
                "  - agent `{}`: status {}; summary `{}`{}",
                agent.name, agent.status, agent.summary, detail
            )?;
        }
    }
    writeln!(prompt, "\n## Channels")?;
    let channels = load_status_channel_rows(project)?;
    if channels.is_empty() {
        writeln!(prompt, "No channels.")?;
    } else {
        for channel in channels {
            writeln!(
                prompt,
                "- `{}`: {} artifacts, latest {}",
                channel.name,
                channel.artifacts,
                channel.latest.as_deref().unwrap_or("-")
            )?;
        }
    }
    writeln!(prompt, "\n## Queues")?;
    let queues = load_all_status_queue_rows(project)?;
    if queues.is_empty() {
        writeln!(prompt, "No nonempty or locked queues.")?;
    } else {
        for queue in queues {
            let active = queue
                .active
                .as_ref()
                .map(|active| format!(", active {}", active.label))
                .unwrap_or_default();
            writeln!(
                prompt,
                "- {} `{}`: pending {}, locked {}{}",
                queue.kind.label(),
                queue.name,
                queue.count,
                queue.locked,
                active
            )?;
        }
    }
    let notices = load_notice_lines(project)?.0;
    if !notices.is_empty() {
        writeln!(prompt, "\n## Dashboard notices")?;
        for notice in notices {
            writeln!(
                prompt,
                "- {}: {}",
                notice_severity_label(notice.severity()),
                notice.text()
            )?;
        }
    }
    Ok(())
}

pub(super) fn append_file_section(prompt: &mut String, path: &Path) -> Result<()> {
    if path.exists() {
        writeln!(
            prompt,
            "\n## `{}`\n\n{}",
            path.display(),
            io::read_text(path)?
        )?;
    }
    Ok(())
}

pub(super) fn find_think_source_dir(cwd: &Path) -> Result<Option<PathBuf>> {
    for ancestor in cwd.ancestors() {
        let candidate = ancestor.join("think-tool");
        if candidate.join("DESIGN.md").exists() && candidate.join("README.md").exists() {
            return Ok(Some(candidate));
        }
        if ancestor.join("DESIGN.md").exists()
            && ancestor.join("README.md").exists()
            && ancestor.join("Cargo.toml").exists()
            && ancestor.file_name().and_then(|name| name.to_str()) == Some("think-tool")
        {
            return Ok(Some(ancestor.to_owned()));
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if manifest_dir.join("DESIGN.md").exists() && manifest_dir.join("README.md").exists() {
        return Ok(Some(manifest_dir));
    }
    Ok(None)
}

pub(super) fn run_command_conversation(
    cwd: &Path,
    command_name: &str,
    initial_prompt: String,
    policy: AppServerPolicy,
) -> Result<()> {
    let root = command_run_root(cwd, command_name)?;
    io::ensure_dir(&root)?;
    let mut turn = 1;
    let mut prompt = initial_prompt;
    let mut follow_up_history = Vec::new();
    loop {
        let turn_root = root.join(turn.to_string());
        io::ensure_dir(&turn_root)?;
        let prompt_path = turn_root.join("PROMPT.md");
        let reply_path = turn_root.join("REPLY.md");
        io::write_text(&prompt_path, &prompt)?;
        print_command_conversation_progress(command_name, turn)?;
        let exit = run_app_server_file_turn(FileAppServerTurn {
            cwd,
            command_root: &root,
            turn_root: &turn_root,
            prompt_path: &prompt_path,
            reply_path: &reply_path,
            steer_dir: None,
            policy,
        })?;
        let transcript =
            io::read_optional_text(&turn_root.join("TRANSCRIPT.txt"))?.unwrap_or_default();
        if !exit.success {
            bail!("app-server turn failed with status code {}", exit.code);
        }
        if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
            return Ok(());
        }
        clear_command_conversation_progress()?;
        let follow_up = read_multiline_request(
            PromptRequest::conversation_reply(command_name, &turn_root.join("TRANSCRIPT.txt"))
                .with_context(
                    "App-Server Reply",
                    command_conversation_context(&reply_path, &transcript)?,
                )
                .with_history(follow_up_history.clone()),
        )?;
        let Some(follow_up) = follow_up else {
            return Ok(());
        };
        follow_up_history.push(follow_up.clone());
        turn += 1;
        prompt = format!(
            "# think {command_name} follow-up\n\nContinue the same `think {command_name}` session. The user replied:\n\n{follow_up}\n"
        );
    }
}

pub(super) fn run_command_conversation_attached(
    cwd: &Path,
    command_name: &str,
    initial_prompt: String,
    policy: AppServerPolicy,
) -> Result<()> {
    if interactive_terminal() {
        run_command_conversation_tui(cwd, command_name, initial_prompt, policy)
    } else {
        run_command_conversation(cwd, command_name, initial_prompt, policy)
    }
}

pub(super) struct FileAppServerTurn<'a> {
    pub(super) cwd: &'a Path,
    pub(super) command_root: &'a Path,
    pub(super) turn_root: &'a Path,
    pub(super) prompt_path: &'a Path,
    pub(super) reply_path: &'a Path,
    pub(super) steer_dir: Option<&'a Path>,
    pub(super) policy: AppServerPolicy,
}

pub(super) fn run_app_server_file_turn(
    request: FileAppServerTurn<'_>,
) -> Result<AppServerTurnExit> {
    run_configured_app_server_turn(request)
}

trait AppServerTurnSpec {
    fn cwd(&self) -> Cow<'_, Path>;
    fn prompt(&self) -> Cow<'_, str>;
    fn run_root(&self) -> Cow<'_, Path>;
    fn transcript_path(&self) -> Cow<'_, Path>;
    fn reply_path(&self) -> Cow<'_, Path>;
    fn state_path(&self) -> Cow<'_, Path>;
    fn steer_dir(&self) -> Option<Cow<'_, Path>>;
    fn policy(&self) -> AppServerPolicy;
    fn config(&self) -> Result<DefaultAppServerConfig>;
}

impl AppServerTurnSpec for AgentAppServerTurn<'_> {
    fn cwd(&self) -> Cow<'_, Path> {
        Cow::Owned(self.agent_paths.root())
    }

    fn prompt(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.prompt)
    }

    fn run_root(&self) -> Cow<'_, Path> {
        Cow::Owned(self.run_paths.root())
    }

    fn transcript_path(&self) -> Cow<'_, Path> {
        Cow::Owned(self.run_paths.transcript_text())
    }

    fn reply_path(&self) -> Cow<'_, Path> {
        Cow::Owned(self.run_paths.reply())
    }

    fn state_path(&self) -> Cow<'_, Path> {
        Cow::Owned(self.agent_paths.backend_thread_state())
    }

    fn steer_dir(&self) -> Option<Cow<'_, Path>> {
        Some(Cow::Owned(self.agent_paths.steer_dir()))
    }

    fn policy(&self) -> AppServerPolicy {
        self.policy
    }

    fn config(&self) -> Result<DefaultAppServerConfig> {
        app_server_config_for_project(&self.agent_paths.role.project)
    }
}

impl AppServerTurnSpec for FileAppServerTurn<'_> {
    fn cwd(&self) -> Cow<'_, Path> {
        Cow::Borrowed(self.cwd)
    }

    fn prompt(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "Read `{}` and follow it exactly.",
            self.prompt_path.display()
        ))
    }

    fn run_root(&self) -> Cow<'_, Path> {
        Cow::Borrowed(self.turn_root)
    }

    fn transcript_path(&self) -> Cow<'_, Path> {
        Cow::Owned(self.turn_root.join("TRANSCRIPT.txt"))
    }

    fn reply_path(&self) -> Cow<'_, Path> {
        Cow::Borrowed(self.reply_path)
    }

    fn state_path(&self) -> Cow<'_, Path> {
        Cow::Owned(self.command_root.join("backend-thread.toml"))
    }

    fn steer_dir(&self) -> Option<Cow<'_, Path>> {
        self.steer_dir.map(Cow::Borrowed)
    }

    fn policy(&self) -> AppServerPolicy {
        self.policy
    }

    fn config(&self) -> Result<DefaultAppServerConfig> {
        app_server_config_for_cwd(self.cwd)
    }
}

fn run_configured_app_server_turn<T: AppServerTurnSpec>(turn: T) -> Result<AppServerTurnExit> {
    let cwd = turn.cwd();
    let prompt = turn.prompt();
    let run_root = turn.run_root();
    let transcript_path = turn.transcript_path();
    let reply_path = turn.reply_path();
    let state_path = turn.state_path();
    let steer_dir = turn.steer_dir();
    let config = turn.config()?;
    crate::backend::run_turn(AppServerTurnRequest {
        backend: app_server_backend(),
        cwd: cwd.as_ref(),
        prompt: prompt.as_ref(),
        run_root: run_root.as_ref(),
        transcript_path: transcript_path.as_ref(),
        reply_path: reply_path.as_ref(),
        state_path: state_path.as_ref(),
        steer_dir: steer_dir.as_deref(),
        policy: turn.policy(),
        config: &config,
    })
}

pub(super) fn print_command_conversation_progress(command_name: &str, turn: u64) -> Result<()> {
    if std::io::stdout().is_terminal() {
        let mut stdout = std::io::stdout().lock();
        write!(
            stdout,
            "\r\x1b[2K{} running app-server for `think {command_name}` turn {turn}...",
            ui::spinner_frame(turn as usize)
        )?;
        stdout.flush()?;
    }
    Ok(())
}

pub(super) fn clear_command_conversation_progress() -> Result<()> {
    if std::io::stdout().is_terminal() {
        let mut stdout = std::io::stdout().lock();
        write!(stdout, "\r\x1b[2K")?;
        stdout.flush()?;
    }
    Ok(())
}

pub(super) fn command_conversation_context(reply_path: &Path, transcript: &str) -> Result<String> {
    if let Some(reply) = io::read_optional_text(reply_path)?
        && !reply.trim().is_empty()
    {
        return Ok(format!(
            "{}\n\nTranscript: {}",
            reply.trim(),
            reply_path
                .parent()
                .map(|path| path.join("TRANSCRIPT.txt").display().to_string())
                .unwrap_or_else(|| "(unavailable)".to_owned())
        ));
    }
    let tail = transcript_tail(transcript, AGENT_HISTORY_TRANSCRIPT_TAIL_LINES);
    Ok(if tail.is_empty() {
        "The app-server turn produced no final reply. See TRANSCRIPT.txt for raw output.".to_owned()
    } else {
        format!("The app-server final reply was unavailable; transcript tail:\n\n{tail}")
    })
}

pub(super) fn command_run_root(cwd: &Path, command_name: &str) -> Result<PathBuf> {
    let timestamp = unix_timestamp();
    if let Ok(project) = ProjectPaths::find_from(cwd) {
        Ok(project
            .runtime_dir()
            .join("commands")
            .join(command_name)
            .join(timestamp.to_string()))
    } else {
        Ok(crate::maintenance::think_home()?
            .join("commands")
            .join(command_name)
            .join(timestamp.to_string()))
    }
}

type DefaultAppServerBackend = crate::provider::codex::CodexAppServerBackend;
type DefaultAppServerConfig = crate::config::CodexProviderConfig;

pub(super) fn app_server_backend() -> &'static DefaultAppServerBackend {
    &crate::provider::codex::APP_SERVER_BACKEND
}

pub(super) fn app_server_config_for_cwd(cwd: &Path) -> Result<DefaultAppServerConfig> {
    Ok(ProjectPaths::find_from(cwd)
        .ok()
        .and_then(|project| project_config(&project).ok())
        .map(|config| config.providers.codex)
        .unwrap_or_default())
}

pub(super) fn app_server_config_for_project(
    project: &ProjectPaths,
) -> Result<DefaultAppServerConfig> {
    Ok(project_config(project)?.providers.codex)
}
