use super::*;

const TRIGGER_QUEUE_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn run_orchestrator(args: RunOrchestratorArgs) -> Result<()> {
    let project = ProjectPaths::new(
        args.project
            .canonicalize()
            .context("Invalid project path")?,
    );
    let role_paths = RolePaths::new(project.clone(), args.role.clone());
    let agent_paths = role_paths.agent(args.agent.clone());
    let Some(_orchestrator_lock) = lock::FileLock::try_acquire(
        orchestrator_lock_path(&project, &args.role, &args.agent),
        "runtime orchestrator lock",
    )?
    else {
        return Ok(());
    };
    loop {
        let config = load_role_config(&role_paths)?;
        if config.status != RoleStatus::Active {
            let mut state = load_agent(&agent_paths)?;
            state.status = AgentStatus::Paused;
            state.note = Some(role_pause_note(config.status, state.paused_by_user));
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        let mut state = load_agent(&agent_paths)?;
        if state.archived {
            break;
        }
        if state.paused_by_user || state.status == AgentStatus::Paused {
            state.status = AgentStatus::Paused;
            state.note = Some(if state.paused_by_user {
                "agent paused by user".to_owned()
            } else {
                "agent paused".to_owned()
            });
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        if matches!(
            state.status,
            AgentStatus::Stopped | AgentStatus::NeedsAttention
        ) {
            break;
        }
        if config.steps.is_empty() {
            state.status = AgentStatus::NeedsAttention;
            state.note = Some("role has no steps".to_owned());
            save_agent(&agent_paths, &mut state)?;
            refresh_role_runtime_started(&role_paths)?;
            break;
        }
        match run_agent_step(&project, &role_paths, &agent_paths, &config, state) {
            Ok(ContinueDecision::Continue) => continue,
            Ok(ContinueDecision::Stop) => break,
            Err(err) => {
                let mut state = load_agent(&agent_paths)?;
                state.status = AgentStatus::NeedsAttention;
                state.note = Some(err.to_string());
                save_agent(&agent_paths, &mut state)?;
                refresh_role_runtime_started(&role_paths)?;
                return Err(err);
            }
        }
    }
    println!(
        "\nthink agent `{}/{}` is done. Attach later with `think agent attach {}/{}`.",
        args.role, args.agent, args.role, args.agent
    );
    Ok(())
}

pub(super) enum ContinueDecision {
    Continue,
    Stop,
}

pub(super) enum SupervisedCommandOutcome {
    Exit(AppServerTurnExit),
    Stopped,
}

pub(super) struct SupervisedCommandResult {
    outcome: SupervisedCommandOutcome,
}

enum SupervisorTransition {
    Running {
        run_id: u64,
        event: Option<String>,
    },
    Idle {
        run_id: u64,
        event: String,
    },
    NeedsAttention {
        run_id: u64,
        event: String,
        clear_child_pid: bool,
    },
}

impl SupervisorTransition {
    fn apply(self, state: &mut SupervisorState) {
        match self {
            Self::Running { run_id, event } => {
                state.status = SupervisorStatus::Running;
                state.last_run_id = Some(run_id);
                state.child_pid = None;
                state.next_retry_at = None;
                state.last_event = event;
            }
            Self::Idle { run_id, event } => {
                state.status = SupervisorStatus::Idle;
                state.last_run_id = Some(run_id);
                state.child_pid = None;
                state.next_retry_at = None;
                state.last_event = Some(event);
            }
            Self::NeedsAttention {
                run_id,
                event,
                clear_child_pid,
            } => {
                state.status = SupervisorStatus::NeedsAttention;
                state.last_run_id = Some(run_id);
                if clear_child_pid {
                    state.child_pid = None;
                }
                state.next_retry_at = None;
                state.last_event = Some(event);
            }
        }
    }
}

pub(super) struct FinalizationInput {
    exit: AppServerTurnExit,
    disposition: Option<Disposition>,
    state: AgentState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AgentRepairKind {
    AgentState,
    ChannelOutbox,
    Manifest,
    Reply,
}

impl std::fmt::Display for AgentRepairKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AgentState => formatter.write_str("agent state"),
            Self::ChannelOutbox => formatter.write_str("channel outbox"),
            Self::Manifest => formatter.write_str("manifest"),
            Self::Reply => formatter.write_str("reply"),
        }
    }
}

#[derive(Debug)]
pub(super) struct AgentRepairError {
    kind: AgentRepairKind,
    message: String,
}

impl std::fmt::Display for AgentRepairError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentRepairError {}

pub(super) fn run_agent_step(
    project: &ProjectPaths,
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    config: &RoleConfig,
    mut state: AgentState,
) -> Result<ContinueDecision> {
    let run_id = state.run_count + 1;
    let step = config.steps[state.current_step % config.steps.len()].clone();
    let run_paths = agent_paths.run(run_id);
    prepare_agent_work(agent_paths)?;
    prepare_agent_data(agent_paths)?;
    io::ensure_dir(&run_paths.root())?;
    prepare_agent_manifest(agent_paths)?;
    io::write_text(
        &run_paths.step(),
        &io::read_text(&role_paths.step_path(&step))?,
    )?;
    let assembled = prompt::assemble(project, role_paths, config, &state, &step)?;
    io::write_text(&agent_paths.prompt(), &assembled)?;
    io::write_text(&run_paths.prompt(), &assembled)?;

    state.status = AgentStatus::Running;
    state.paused_by_user = false;
    state.note = None;
    save_agent(agent_paths, &mut state)?;
    ensure_role_runtime_started(role_paths)?;

    let started_at = unix_timestamp();
    let elapsed_triggers_done = Arc::new(AtomicBool::new(false));
    let elapsed_trigger_thread = start_elapsed_trigger_thread(
        project.clone(),
        role_paths.role.clone(),
        elapsed_triggers_done.clone(),
    )?;
    println!(
        "starting app-server run {} for `{}/{}`",
        run_id, role_paths.role, agent_paths.agent
    );
    let mut repair_attempts = 0;
    let mut restart_notice = None;
    let step_result: Result<Option<FinalizationInput>> = loop {
        let result = run_supervised_agent_command(
            role_paths,
            agent_paths,
            &run_paths,
            restart_notice.take(),
        );
        let result = match result {
            Ok(result) => result,
            Err(err) => break Err(err),
        };
        let SupervisedCommandOutcome::Exit(exit) = result.outcome else {
            break Ok(None);
        };
        if !exit.success {
            match validate_agent_state_after_run(agent_paths, &state) {
                Ok(state) => {
                    break Ok(Some(FinalizationInput {
                        exit,
                        disposition: None,
                        state,
                    }));
                }
                Err(err) => break Err(err),
            }
        }
        match validate_agent_finalization(
            project,
            agent_paths,
            config.mode,
            &state,
            run_id,
            &run_paths,
        ) {
            Ok((state, disposition)) => {
                break Ok(Some(FinalizationInput {
                    exit,
                    disposition,
                    state,
                }));
            }
            Err(err)
                if is_agent_repair_error(&err) && repair_attempts < MAX_AGENT_REPAIR_RETRIES =>
            {
                repair_attempts += 1;
                let retry_at = unix_timestamp() + agent_repair_retry_delay_seconds();
                let event = agent_repair_event(&err, repair_attempts, retry_at);
                let kind = agent_repair_kind(&err).expect("repair errors have a kind");
                if let Err(err) =
                    record_agent_repair_retry(agent_paths, run_id, retry_at, &event, &state, kind)
                {
                    break Err(err);
                }
                match wait_until_retry(agent_paths, role_paths, retry_at) {
                    Ok(true) => {}
                    Ok(false) => break Ok(None),
                    Err(err) => break Err(err),
                }
                restart_notice = Some(agent_repair_notice(
                    &err,
                    kind,
                    config.mode,
                    repair_attempts,
                ));
            }
            Err(err) => {
                let event = if is_agent_repair_error(&err) {
                    agent_repair_exhausted_event(&err)
                } else {
                    err.to_string()
                };
                if let Err(err) = save_supervisor_needs_attention(agent_paths, run_id, &event) {
                    break Err(err);
                }
                if let Err(err) = update_agent_note(agent_paths, &event) {
                    break Err(err);
                }
                break Err(err);
            }
        }
    };
    elapsed_triggers_done.store(true, Ordering::Relaxed);
    if let Some(handle) = elapsed_trigger_thread {
        handle
            .join()
            .unwrap_or_else(|_| Err(anyhow!("elapsed trigger thread panicked")))?;
    }
    let Some(finalization) = step_result? else {
        return Ok(ContinueDecision::Stop);
    };
    let FinalizationInput {
        exit,
        disposition,
        mut state,
    } = finalization;
    let finished_at = unix_timestamp();
    if !exit.success {
        let exit = run_exit_from_app_server(
            run_id,
            step.clone(),
            started_at,
            finished_at,
            exit,
            None,
            None,
        );
        io::write_toml(&run_paths.exit(), &exit)?;
        bail!("agent exited unsuccessfully with status code {}", exit.code);
    }

    let pause_requested = state.paused_by_user || state.status == AgentStatus::Paused;
    let pause_note = state.note.clone().unwrap_or_else(|| {
        if state.paused_by_user {
            "agent paused by user".to_owned()
        } else {
            "agent paused".to_owned()
        }
    });
    finalize_channels(project, agent_paths, &state, run_id)?;
    state.run_count = run_id;
    state.note = None;
    let exit = run_exit_from_app_server(
        run_id,
        step.clone(),
        started_at,
        finished_at,
        exit,
        disposition,
        None,
    );
    io::write_toml(&run_paths.exit(), &exit)?;
    state.last_exit = Some(exit);

    let decision = match config.mode {
        RoleMode::Oneshot => {
            state.status = AgentStatus::Done;
            state.paused_by_user = false;
            ContinueDecision::Stop
        }
        RoleMode::Repeatable => match disposition {
            Some(Disposition::Continue) => {
                state.current_step = (state.current_step + 1) % config.steps.len();
                if pause_requested {
                    state.status = AgentStatus::Paused;
                    state.note = Some(pause_note.clone());
                    ContinueDecision::Stop
                } else {
                    state.status = AgentStatus::Running;
                    ContinueDecision::Continue
                }
            }
            Some(Disposition::Stop) => {
                state.status = AgentStatus::Done;
                state.paused_by_user = false;
                ContinueDecision::Stop
            }
            None => unreachable!("repeatable disposition was validated above"),
        },
        RoleMode::Infinite => {
            state.current_step = (state.current_step + 1) % config.steps.len();
            if pause_requested {
                state.status = AgentStatus::Paused;
                state.note = Some(pause_note);
                ContinueDecision::Stop
            } else {
                state.status = AgentStatus::Running;
                ContinueDecision::Continue
            }
        }
    };
    if config.auto_archive && matches!(decision, ContinueDecision::Stop) {
        state.archived = true;
    }
    save_agent(agent_paths, &mut state)?;
    if matches!(decision, ContinueDecision::Stop) {
        refresh_role_runtime_started(role_paths)?;
    }
    fire_role_step_finished_triggers(project, &role_paths.role, &step)?;
    if state.status == AgentStatus::Done {
        fire_role_agent_finished_triggers(project, &role_paths.role, &state.agent, run_id, &step)?;
    }
    Ok(decision)
}

pub(super) fn run_supervised_agent_command(
    role_paths: &RolePaths,
    agent_paths: &crate::state::AgentPaths,
    run_paths: &crate::state::RunPaths,
    initial_restart_notice: Option<String>,
) -> Result<SupervisedCommandResult> {
    let mut restart_notice = initial_restart_notice;
    let mut oom_restarts = 0;
    loop {
        save_supervisor_transition(
            agent_paths,
            SupervisorTransition::Running {
                run_id: run_paths.run_id,
                event: restart_notice.clone(),
            },
        )?;

        let prompt = match restart_notice.as_deref() {
            Some(notice) => {
                format!(
                    "Read PROMPT.md in the current directory and follow it exactly.\n\nSupervisor notice: {notice}"
                )
            }
            None => "Read PROMPT.md in the current directory and follow it exactly.".to_owned(),
        };
        let exit = run_agent_app_server_turn(AgentAppServerTurn {
            agent_paths,
            run_paths,
            prompt: &prompt,
            policy: AppServerPolicy::WorkspaceWrite,
        });
        match exit {
            Ok(exit) if exit.success => {
                save_supervisor_transition(
                    agent_paths,
                    SupervisorTransition::Idle {
                        run_id: run_paths.run_id,
                        event: "app-server turn completed successfully".to_owned(),
                    },
                )?;
                return Ok(SupervisedCommandResult {
                    outcome: SupervisedCommandOutcome::Exit(exit),
                });
            }
            Ok(_exit) if agent_execution_was_paused(agent_paths)? => {
                save_supervisor_transition(
                    agent_paths,
                    SupervisorTransition::Idle {
                        run_id: run_paths.run_id,
                        event: "app-server turn stopped after agent pause".to_owned(),
                    },
                )?;
                return Ok(SupervisedCommandResult {
                    outcome: SupervisedCommandOutcome::Stopped,
                });
            }
            Ok(exit) if exit_looks_like_oom(&exit) && oom_restarts < MAX_OOM_RESTARTS => {
                oom_restarts += 1;
                let retry_at = unix_timestamp() + oom_restart_delay_seconds();
                let event = format!(
                    "app-server exited with possible OOM status {}; restarting at {}",
                    exit.code,
                    format_unix_time(retry_at)
                );
                save_supervisor_wait(
                    agent_paths,
                    SupervisorStatus::Restarting,
                    run_paths.run_id,
                    exit.pid,
                    retry_at,
                    &event,
                )?;
                update_agent_note(agent_paths, &event)?;
                if !wait_until_retry(agent_paths, role_paths, retry_at)? {
                    return Ok(SupervisedCommandResult {
                        outcome: SupervisedCommandOutcome::Stopped,
                    });
                }
                restart_notice = Some(
                    "The previous app-server turn appeared to be killed by memory pressure. Avoid repeating the memory-heavy operation; use smaller batches, streaming, checkpoints, or external artifacts in data/own/."
                        .to_owned(),
                );
            }
            Ok(exit) => {
                save_supervisor_transition(
                    agent_paths,
                    SupervisorTransition::NeedsAttention {
                        run_id: run_paths.run_id,
                        event: format!("app-server turn failed with status code {}", exit.code),
                        clear_child_pid: true,
                    },
                )?;
                return Ok(SupervisedCommandResult {
                    outcome: SupervisedCommandOutcome::Exit(exit),
                });
            }
            Err(err) => {
                if agent_execution_was_paused(agent_paths)? {
                    save_supervisor_transition(
                        agent_paths,
                        SupervisorTransition::Idle {
                            run_id: run_paths.run_id,
                            event: "app-server turn stopped after agent pause".to_owned(),
                        },
                    )?;
                    return Ok(SupervisedCommandResult {
                        outcome: SupervisedCommandOutcome::Stopped,
                    });
                }
                save_supervisor_transition(
                    agent_paths,
                    SupervisorTransition::NeedsAttention {
                        run_id: run_paths.run_id,
                        event: err.to_string(),
                        clear_child_pid: false,
                    },
                )?;
                return Err(err);
            }
        }
    }
}

pub(super) fn supervisor_path(agent_paths: &crate::state::AgentPaths) -> PathBuf {
    agent_paths.root().join("orchestrator.toml")
}

pub(super) fn agent_execution_was_paused(agent_paths: &crate::state::AgentPaths) -> Result<bool> {
    let state = load_agent(agent_paths)?;
    Ok(state.archived || state.paused_by_user || state.status == AgentStatus::Paused)
}

pub(super) fn orchestrator_lock_path(
    project: &ProjectPaths,
    role: &RoleSlug,
    agent: &AgentId,
) -> PathBuf {
    project
        .runtime_dir()
        .join("locks")
        .join("orchestrators")
        .join(role.as_str())
        .join(format!("{}.lock", agent.as_str()))
}

pub(super) fn load_supervisor_state(
    agent_paths: &crate::state::AgentPaths,
) -> Result<SupervisorState> {
    let path = supervisor_path(agent_paths);
    if path.exists() {
        io::read_toml(&path)
    } else {
        Ok(SupervisorState::default())
    }
}

pub(super) fn save_supervisor_state(
    agent_paths: &crate::state::AgentPaths,
    state: &SupervisorState,
) -> Result<()> {
    let mut state = state.clone();
    state.version = SUPERVISOR_STATE_VERSION;
    state.updated_at = unix_timestamp();
    io::write_toml(&supervisor_path(agent_paths), &state)
}

fn update_supervisor_state(
    agent_paths: &crate::state::AgentPaths,
    update: impl FnOnce(&mut SupervisorState),
) -> Result<()> {
    let mut state = load_supervisor_state(agent_paths)?;
    update(&mut state);
    save_supervisor_state(agent_paths, &state)
}

fn save_supervisor_transition(
    agent_paths: &crate::state::AgentPaths,
    transition: SupervisorTransition,
) -> Result<()> {
    update_supervisor_state(agent_paths, |state| transition.apply(state))
}

pub(super) fn save_supervisor_wait(
    agent_paths: &crate::state::AgentPaths,
    status: SupervisorStatus,
    run_id: u64,
    child_pid: Option<u32>,
    retry_at: u64,
    event: &str,
) -> Result<()> {
    update_supervisor_state(agent_paths, |state| {
        state.status = status;
        state.last_run_id = Some(run_id);
        state.child_pid = child_pid;
        state.next_retry_at = Some(retry_at);
        state.last_event = Some(event.to_owned());
        match status {
            SupervisorStatus::Restarting => state.oom_restarts += 1,
            SupervisorStatus::WaitingForQuota => state.quota_retries += 1,
            SupervisorStatus::WaitingForProvider => state.provider_retries += 1,
            SupervisorStatus::Idle
            | SupervisorStatus::Running
            | SupervisorStatus::NeedsAttention => {}
        }
    })
}

pub(super) fn save_supervisor_repair_retry(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    retry_at: u64,
    event: &str,
) -> Result<()> {
    update_supervisor_state(agent_paths, |state| {
        state.status = SupervisorStatus::Restarting;
        state.last_run_id = Some(run_id);
        state.child_pid = None;
        state.next_retry_at = Some(retry_at);
        state.last_event = Some(event.to_owned());
        state.repair_retries += 1;
    })
}

pub(super) fn save_supervisor_needs_attention(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    event: &str,
) -> Result<()> {
    save_supervisor_transition(
        agent_paths,
        SupervisorTransition::NeedsAttention {
            run_id,
            event: event.to_owned(),
            clear_child_pid: true,
        },
    )
}

pub(super) fn update_agent_note(agent_paths: &crate::state::AgentPaths, note: &str) -> Result<()> {
    let mut state = load_agent(agent_paths)?;
    state.note = Some(note.to_owned());
    save_agent(agent_paths, &mut state)
}

pub(super) fn record_agent_repair_retry(
    agent_paths: &crate::state::AgentPaths,
    run_id: u64,
    retry_at: u64,
    event: &str,
    expected_state: &AgentState,
    kind: AgentRepairKind,
) -> Result<()> {
    save_supervisor_repair_retry(agent_paths, run_id, retry_at, event)?;
    if update_agent_note(agent_paths, event).is_ok() {
        return Ok(());
    }
    if kind != AgentRepairKind::AgentState {
        return update_agent_note(agent_paths, event);
    }
    let mut restored = expected_state.clone();
    restored.status = AgentStatus::Running;
    restored.paused_by_user = false;
    restored.note = Some(event.to_owned());
    save_agent(agent_paths, &mut restored)
}

pub(super) fn wait_until_retry(
    agent_paths: &crate::state::AgentPaths,
    role_paths: &RolePaths,
    retry_at: u64,
) -> Result<bool> {
    let wait_started_at = unix_timestamp();
    loop {
        let state = load_agent(agent_paths)?;
        if state.archived
            || matches!(
                state.status,
                AgentStatus::Stopped | AgentStatus::Paused | AgentStatus::Done
            )
        {
            return Ok(false);
        }
        if load_role_config(role_paths)?.status != RoleStatus::Active {
            return Ok(false);
        }
        let now = unix_timestamp();
        if now >= retry_at {
            return Ok(true);
        }
        let supervisor = load_supervisor_state(agent_paths)?;
        if supervisor
            .next_retry_at
            .is_some_and(|timestamp| timestamp <= now)
            || retry_requested_after(&role_paths.project, wait_started_at)?
        {
            return Ok(true);
        }
        thread::sleep(Duration::from_secs(
            (retry_at - now).clamp(RETRY_POLL_MIN_SECONDS, RETRY_POLL_MAX_SECONDS),
        ));
    }
}

pub(super) fn retry_requested_after(project: &ProjectPaths, timestamp: u64) -> Result<bool> {
    let path = retry_request_path(project);
    if !path.exists() {
        return Ok(false);
    }
    Ok(io::read_toml::<RetryRequestState>(&path)?.requested_at > timestamp)
}

pub(super) fn retry_request_path(project: &ProjectPaths) -> PathBuf {
    project.runtime_dir().join("retry.toml")
}

pub(super) fn exit_looks_like_oom(exit: &AppServerTurnExit) -> bool {
    exit.code == 137
        || exit
            .signal
            .as_deref()
            .is_some_and(|signal| matches!(signal, "KILL" | "SIGKILL" | "9"))
}

pub(super) fn oom_restart_delay_seconds() -> u64 {
    env_retry_delay_seconds(
        "THINK_ORCHESTRATOR_OOM_RESTART_DELAY_SECONDS",
        DEFAULT_OOM_RESTART_DELAY_SECONDS,
    )
}

pub(super) fn agent_repair_retry_delay_seconds() -> u64 {
    env_retry_delay_seconds(
        "THINK_ORCHESTRATOR_AGENT_REPAIR_RETRY_SECONDS",
        DEFAULT_AGENT_REPAIR_RETRY_SECONDS,
    )
}

pub(super) fn env_retry_delay_seconds(name: &str, default: u64) -> u64 {
    if let Ok(value) = std::env::var(name)
        && let Ok(value) = value.parse::<u64>()
    {
        return value.max(MIN_RETRY_DELAY_SECONDS);
    }
    default
}

pub(super) fn finalize_channels(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
) -> Result<()> {
    for channel in &state.channels {
        publish_channel_outbox(project, agent_paths, state, run_id, channel)?;
    }
    Ok(())
}

pub(super) fn validate_channel_outboxes(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
) -> Result<()> {
    for channel in &state.channels {
        let outbox = agent_paths.channel_dir(channel);
        if !outbox.exists() || directory_is_empty(&outbox)? {
            continue;
        }
        let channel_dir = project.channel_dir(channel);
        for entry in fs::read_dir(&outbox)
            .with_context(|| format!("Failed to read channel outbox `{}`", outbox.display()))?
        {
            let entry = entry.with_context(|| format!("Failed to read `{}`", outbox.display()))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(agent_repair_error(
                    AgentRepairKind::ChannelOutbox,
                    format!(
                        "Channel outbox `{}` contains a non-UTF-8 top-level name.",
                        outbox.display()
                    ),
                ));
            };
            if name == "." || name == ".." {
                continue;
            }
            validate_publish_entry(
                &entry.path(),
                &channel_dir.join(format!(
                    "{}-{}-{}-{}",
                    state.role, state.agent, run_id, name
                )),
            )?;
        }
    }
    Ok(())
}

pub(super) fn publish_channel_outbox(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    state: &AgentState,
    run_id: u64,
    channel: &ChannelSlug,
) -> Result<()> {
    let outbox = agent_paths.channel_dir(channel);
    if !outbox.exists() || directory_is_empty(&outbox)? {
        return Ok(());
    }
    let _lock = lock::FileLock::acquire(
        project.channel_lock_path(channel),
        &format!("channel `{channel}` publish lock"),
    )?;
    let channel_dir = project.channel_dir(channel);
    git::init_channel(&channel_dir)?;
    let mut published = false;
    for entry in fs::read_dir(&outbox)
        .with_context(|| format!("Failed to read channel outbox `{}`", outbox.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", outbox.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!(
                    "Channel outbox `{}` contains a non-UTF-8 top-level name.",
                    outbox.display()
                ),
            ));
        };
        if name == "." || name == ".." {
            continue;
        }
        let target = channel_dir.join(format!(
            "{}-{}-{}-{}",
            state.role, state.agent, run_id, name
        ));
        published |= copy_publish_entry(&entry.path(), &target)?;
    }
    if published {
        git::commit_all(
            &channel_dir,
            &format!(
                "think: publish {}/{} run {} to {}",
                state.role, state.agent, run_id, channel
            ),
        )?;
    }
    clear_directory_contents(&outbox)?;
    Ok(())
}

pub(super) fn directory_is_empty(path: &Path) -> Result<bool> {
    Ok(fs::read_dir(path)
        .with_context(|| format!("Failed to read `{}`", path.display()))?
        .next()
        .is_none())
}

pub(super) fn validate_publish_entry(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect `{}`", source.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish symlink `{}`.", source.display()),
        ));
    }
    if metadata.is_dir() {
        if target.exists() {
            return ensure_directories_identical(source, target);
        }
        return validate_publish_directory(source, target);
    }
    if metadata.is_file() {
        if target.exists() {
            ensure_files_identical(source, target)?;
        }
        return Ok(());
    }
    Err(agent_repair_error(
        AgentRepairKind::ChannelOutbox,
        format!("Refusing to publish non-file `{}`.", source.display()),
    ))
}

pub(super) fn validate_publish_directory(source: &Path, target: &Path) -> Result<()> {
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        validate_publish_entry(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

pub(super) fn copy_publish_entry(source: &Path, target: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect `{}`", source.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish symlink `{}`.", source.display()),
        ));
    }
    if metadata.is_dir() {
        if target.exists() {
            ensure_directories_identical(source, target)?;
            return Ok(false);
        }
        copy_directory(source, target)?;
        return Ok(true);
    }
    if !metadata.is_file() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!("Refusing to publish non-file `{}`.", source.display()),
        ));
    }
    if target.exists() {
        ensure_files_identical(source, target)?;
        return Ok(false);
    }
    if let Some(parent) = target.parent() {
        io::ensure_dir(parent)?;
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "Failed to publish `{}` to `{}`",
            source.display(),
            target.display()
        )
    })?;
    Ok(true)
}

pub(super) fn copy_directory(source: &Path, target: &Path) -> Result<()> {
    io::ensure_dir(target)?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        copy_publish_entry(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

pub(super) fn ensure_directories_identical(source: &Path, target: &Path) -> Result<()> {
    if !target.is_dir() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` exists but is not a directory matching `{}`.",
                target.display(),
                source.display()
            ),
        ));
    }
    let mut source_entries = BTreeSet::new();
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read directory `{}`", source.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", source.display()))?;
        source_entries.insert(entry.file_name());
        let target_entry = target.join(entry.file_name());
        let source_path = entry.path();
        let source_type = fs::symlink_metadata(&source_path)
            .with_context(|| format!("Failed to inspect `{}`", source_path.display()))?;
        if source_type.file_type().is_symlink() {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!("Refusing to publish symlink `{}`.", source_path.display()),
            ));
        }
        if source_type.is_dir() {
            ensure_directories_identical(&source_path, &target_entry)?;
        } else if source_type.is_file() {
            ensure_files_identical(&source_path, &target_entry)?;
        } else {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!("Refusing to publish non-file `{}`.", source_path.display()),
            ));
        }
    }
    for entry in fs::read_dir(target)
        .with_context(|| format!("Failed to read directory `{}`", target.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", target.display()))?;
        if !source_entries.contains(&entry.file_name()) {
            return Err(agent_repair_error(
                AgentRepairKind::ChannelOutbox,
                format!(
                    "Publish target `{}` contains extra entry `{}`.",
                    target.display(),
                    entry.file_name().to_string_lossy()
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn ensure_files_identical(source: &Path, target: &Path) -> Result<()> {
    if !target.is_file() {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` exists but is not a file matching `{}`.",
                target.display(),
                source.display()
            ),
        ));
    }
    if fs::read(source).with_context(|| format!("Failed to read `{}`", source.display()))?
        != fs::read(target).with_context(|| format!("Failed to read `{}`", target.display()))?
    {
        return Err(agent_repair_error(
            AgentRepairKind::ChannelOutbox,
            format!(
                "Publish target `{}` already exists with different content.",
                target.display()
            ),
        ));
    }
    Ok(())
}

pub(super) fn clear_directory_contents(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read directory `{}`", path.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read `{}`", path.display()))?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path)
            .with_context(|| format!("Failed to inspect `{}`", entry_path.display()))?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&entry_path)
                .with_context(|| format!("Failed to remove `{}`", entry_path.display()))?;
        } else {
            fs::remove_file(&entry_path)
                .with_context(|| format!("Failed to remove `{}`", entry_path.display()))?;
        }
    }
    Ok(())
}

pub(super) fn ensure_role_runtime_started(role_paths: &RolePaths) -> Result<u64> {
    let path = role_runtime_path(&role_paths.project, &role_paths.role);
    if path.exists() {
        return Ok(io::read_toml::<RoleRuntimeState>(&path)?.started_at);
    }
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    let state = RoleRuntimeState {
        started_at: unix_timestamp(),
    };
    io::write_toml(&path, &state)?;
    Ok(state.started_at)
}

pub(super) fn refresh_role_runtime_started(role_paths: &RolePaths) -> Result<()> {
    let path = role_runtime_path(&role_paths.project, &role_paths.role);
    let config = load_role_config(role_paths)?;
    if config.status == RoleStatus::Active && active_agent_count(role_paths)? > 0 {
        ensure_role_runtime_started(role_paths)?;
    } else if path.exists() {
        fs::remove_file(&path).with_context(|| format!("Failed to remove `{}`", path.display()))?;
    }
    Ok(())
}

pub(super) fn load_role_runtime_started(
    project: &ProjectPaths,
    role: &RoleSlug,
) -> Result<Option<u64>> {
    let role_paths = RolePaths::new(project.clone(), role.clone());
    if load_role_config(&role_paths)?.status != RoleStatus::Active {
        return Ok(None);
    }
    let path = role_runtime_path(project, role);
    if path.exists() {
        Ok(Some(io::read_toml::<RoleRuntimeState>(&path)?.started_at))
    } else {
        Ok(None)
    }
}

pub(super) fn role_runtime_path(project: &ProjectPaths, role: &RoleSlug) -> PathBuf {
    project
        .runtime_dir()
        .join("role-runtime")
        .join(format!("{role}.toml"))
}

pub(super) fn load_agent_manifest(paths: &crate::state::AgentPaths) -> Result<AgentManifest> {
    if paths.manifest().exists() {
        io::read_toml(&paths.manifest())
    } else {
        Ok(AgentManifest::default())
    }
}

pub(super) fn load_agent_manifest_for_display(
    paths: &crate::state::AgentPaths,
) -> (AgentManifest, Option<String>) {
    match load_agent_manifest(paths) {
        Ok(manifest) => (manifest, None),
        Err(err) => (
            AgentManifest::default(),
            Some(compact_single_line(&format!("{err:#}"), 180)),
        ),
    }
}

pub(super) fn save_agent_manifest(
    paths: &crate::state::AgentPaths,
    manifest: &AgentManifest,
) -> Result<()> {
    io::write_toml(&paths.manifest(), manifest)
}

pub(super) fn prepare_agent_manifest(paths: &crate::state::AgentPaths) -> Result<()> {
    let mut manifest = match load_agent_manifest(paths) {
        Ok(manifest) => manifest,
        Err(err) if is_toml_parse_error(&err) => AgentManifest::default(),
        Err(err) => return Err(err),
    };
    manifest.disposition = None;
    save_agent_manifest(paths, &manifest)
}

pub(super) fn validate_agent_finalization(
    project: &ProjectPaths,
    agent_paths: &crate::state::AgentPaths,
    mode: RoleMode,
    expected_state: &AgentState,
    run_id: u64,
    run_paths: &crate::state::RunPaths,
) -> Result<(AgentState, Option<Disposition>)> {
    let state = validate_agent_state_after_run(agent_paths, expected_state)?;
    let disposition = validate_agent_manifest_for_mode(agent_paths, mode)?;
    validate_run_reply(run_paths)?;
    validate_channel_outboxes(project, agent_paths, &state, run_id)?;
    Ok((state, disposition))
}

pub(super) fn validate_agent_state_after_run(
    paths: &crate::state::AgentPaths,
    expected: &AgentState,
) -> Result<AgentState> {
    let state = match load_agent(paths) {
        Ok(state) => state,
        Err(err) if is_toml_parse_error(&err) => {
            return Err(agent_repair_error(
                AgentRepairKind::AgentState,
                format!("agent.toml is invalid: {err:#}"),
            ));
        }
        Err(err) => return Err(err),
    };
    if state.role != expected.role {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed role from `{}` to `{}`",
            expected.role, state.role
        )));
    }
    if state.agent != expected.agent {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed agent from `{}` to `{}`",
            expected.agent, state.agent
        )));
    }
    if state.backend != expected.backend {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed backend from `{}` to `{}`",
            expected.backend, state.backend
        )));
    }
    if state.mode != expected.mode {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed mode from `{}` to `{}`",
            expected.mode, state.mode
        )));
    }
    if state.current_step != expected.current_step {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed current_step from `{}` to `{}`",
            expected.current_step, state.current_step
        )));
    }
    if state.run_count != expected.run_count {
        return Err(agent_state_repair_error(format!(
            "agent.toml changed run_count from `{}` to `{}`",
            expected.run_count, state.run_count
        )));
    }
    if state.channels != expected.channels {
        return Err(agent_state_repair_error(
            "agent.toml changed the channel list".to_owned(),
        ));
    }
    if !matches!(
        state.status,
        AgentStatus::Running
            | AgentStatus::Paused
            | AgentStatus::Stopped
            | AgentStatus::NeedsAttention
    ) {
        return Err(agent_state_repair_error(format!(
            "agent.toml has invalid in-run status `{}`",
            state.status
        )));
    }
    Ok(state)
}

pub(super) fn agent_state_repair_error(message: String) -> anyhow::Error {
    agent_repair_error(AgentRepairKind::AgentState, message)
}

pub(super) fn validate_agent_manifest_for_mode(
    paths: &crate::state::AgentPaths,
    mode: RoleMode,
) -> Result<Option<Disposition>> {
    let manifest = match load_agent_manifest(paths) {
        Ok(manifest) => manifest,
        Err(err) if is_toml_parse_error(&err) => {
            return Err(agent_repair_error(
                AgentRepairKind::Manifest,
                format!("manifest.toml is invalid: {err:#}"),
            ));
        }
        Err(err) => return Err(err),
    };
    match mode {
        RoleMode::Repeatable => manifest.disposition.map(Some).ok_or_else(|| {
            agent_repair_error(
                AgentRepairKind::Manifest,
                "repeatable role run ended without `disposition = \"continue\"` or \
                 `disposition = \"stop\"` in manifest.toml"
                    .to_owned(),
            )
        }),
        RoleMode::Oneshot | RoleMode::Infinite => Ok(None),
    }
}

pub(super) fn validate_run_reply(run_paths: &crate::state::RunPaths) -> Result<()> {
    let Some(reply) = io::read_optional_text(&run_paths.reply())? else {
        return Err(agent_repair_error(
            AgentRepairKind::Reply,
            format!(
                "run reply file `{}` is missing",
                run_paths.reply().display()
            ),
        ));
    };
    if reply.trim().is_empty() {
        return Err(agent_repair_error(
            AgentRepairKind::Reply,
            format!("run reply file `{}` is empty", run_paths.reply().display()),
        ));
    }
    Ok(())
}

pub(super) fn agent_repair_event(err: &anyhow::Error, attempt: u32, retry_at: u64) -> String {
    let kind = agent_repair_kind(err)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "agent output".to_owned());
    format!(
        "{kind} is invalid; repair {attempt}/{MAX_AGENT_REPAIR_RETRIES} at {}: {}",
        format_unix_time(retry_at),
        compact_single_line(&format!("{err:#}"), 160)
    )
}

pub(super) fn agent_repair_exhausted_event(err: &anyhow::Error) -> String {
    let kind = agent_repair_kind(err)
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "agent output".to_owned());
    format!(
        "{kind} is still invalid after {MAX_AGENT_REPAIR_RETRIES} repair attempts: {}",
        compact_single_line(&format!("{err:#}"), 160)
    )
}

pub(super) fn agent_repair_notice(
    err: &anyhow::Error,
    kind: AgentRepairKind,
    mode: RoleMode,
    attempt: u32,
) -> String {
    let instruction = match kind {
        AgentRepairKind::AgentState => {
            "The runtime restored `agent.toml` to a valid state. Do not edit `agent.toml`, \
             `orchestrator.toml`, runtime files, or project channel directories."
        }
        AgentRepairKind::ChannelOutbox => {
            "Fix only your local `channels/<channel>/` outbox. Remove symlinks and special files, \
             rename or remove colliding artifacts, and leave project channel directories alone."
        }
        AgentRepairKind::Manifest => manifest_mode_instruction(mode),
        AgentRepairKind::Reply => {
            "Provide a compact final reply. It must not be empty; summarize what you did, what \
             changed, what is known now, and the most important next step if one exists."
        }
    };
    format!(
        concat!(
            "The previous run finished, but think could not finalize it because the agent's ",
            "{kind} output is invalid.\n\n",
            "# Runtime error\n\n",
            "{err:#}\n\n",
            "{instruction}\n\n",
            "Do not redo unrelated work. Repair the issue and exit again.\n\n",
            "This is repair attempt {attempt}/{max_attempts}."
        ),
        kind = kind,
        err = err,
        instruction = instruction,
        attempt = attempt,
        max_attempts = MAX_AGENT_REPAIR_RETRIES
    )
}

pub(super) fn manifest_mode_instruction(mode: RoleMode) -> &'static str {
    match mode {
        RoleMode::Repeatable => {
            "Because this is a repeatable role, choose exactly one of \
             `disposition = \"continue\"` or `disposition = \"stop\"` before exiting."
        }
        RoleMode::Oneshot => {
            "Because this is a one-shot role, remove the `disposition` key. Completion is recorded \
             by the think runtime; do not write `disposition = \"done\"`."
        }
        RoleMode::Infinite => {
            "Because this is an infinite role, remove the `disposition` key. The think runtime \
             will start the next run automatically."
        }
    }
}

pub(super) fn agent_repair_error(kind: AgentRepairKind, message: String) -> anyhow::Error {
    anyhow!(AgentRepairError { kind, message })
}

pub(super) fn is_agent_repair_error(err: &anyhow::Error) -> bool {
    agent_repair_kind(err).is_some()
}

pub(super) fn agent_repair_kind(err: &anyhow::Error) -> Option<AgentRepairKind> {
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<AgentRepairError>()
            .map(|error| error.kind)
    })
}

pub(super) fn is_toml_parse_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<toml::de::Error>().is_some())
}

pub(super) fn fire_role_step_finished_triggers(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_step: &crate::ids::StepSlug,
) -> Result<()> {
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::RoleStepFinished { role, step, launch } = trigger else {
                continue;
            };
            if role == *source_role && step == *source_step {
                launch_triggered_role(
                    project,
                    &target,
                    &launch,
                    TriggerCause::RoleStepFinished {
                        source_role: source_role.clone(),
                        source_step: source_step.clone(),
                    },
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn fire_role_agent_finished_triggers(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_agent: &AgentId,
    run_id: u64,
    step: &StepSlug,
) -> Result<()> {
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::RoleAgentFinished { role, launch } = trigger else {
                continue;
            };
            if role == *source_role
                && claim_role_agent_finished_trigger(
                    project,
                    source_role,
                    source_agent,
                    run_id,
                    &target,
                )?
            {
                launch_triggered_role(
                    project,
                    &target,
                    &launch,
                    TriggerCause::RoleAgentFinished {
                        source_role: source_role.clone(),
                        source_agent: source_agent.clone(),
                        run_id,
                        step: step.clone(),
                    },
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn fire_queue_idle_triggers(project: &ProjectPaths) -> Result<()> {
    let mut trigger_specs = Vec::new();
    for target in list_roles(project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::QueueIdle {
                idle_queue,
                idle_seconds,
                launch,
            } = trigger
            else {
                continue;
            };
            trigger_specs.push((target.clone(), idle_queue, idle_seconds, launch));
        }
    }

    for (target, queue, idle_seconds, launch) in trigger_specs {
        validate_trigger_queue(&queue)?;
        let Some(empty_since) = refresh_queue_empty_since(project, &queue)? else {
            continue;
        };
        if unix_timestamp().saturating_sub(empty_since) < idle_seconds {
            continue;
        }
        if claim_queue_idle_trigger(project, &queue, empty_since, &target, idle_seconds)? {
            launch_triggered_role(
                project,
                &target,
                &launch,
                TriggerCause::QueueIdle {
                    queue,
                    idle_seconds,
                    empty_since,
                },
            )?;
        }
    }
    Ok(())
}

pub(super) fn start_elapsed_trigger_thread(
    project: ProjectPaths,
    source_role: RoleSlug,
    done: Arc<AtomicBool>,
) -> Result<Option<thread::JoinHandle<Result<()>>>> {
    let mut triggers = Vec::new();
    for target in list_roles(&project)? {
        let target_paths = RolePaths::new(project.clone(), target.clone());
        let config = load_role_config(&target_paths)?;
        for trigger in config.triggers {
            let TriggerConfig::Elapsed {
                role,
                interval_seconds,
                launch,
            } = trigger
            else {
                continue;
            };
            if role == source_role && interval_seconds > 0 {
                triggers.push((target.clone(), interval_seconds, launch));
            }
        }
    }
    if triggers.is_empty() {
        return Ok(None);
    }
    Ok(Some(thread::spawn(move || {
        while !done.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            let Some(started_at) = load_role_runtime_started(&project, &source_role)? else {
                return Ok(());
            };
            let elapsed = unix_timestamp().saturating_sub(started_at);
            for (target, interval_seconds, launch) in &triggers {
                let count = elapsed / *interval_seconds;
                for event_index in 1..=count {
                    if claim_elapsed_trigger(
                        &project,
                        &source_role,
                        started_at,
                        target,
                        *interval_seconds,
                        event_index,
                    )? {
                        launch_triggered_role(
                            &project,
                            target,
                            launch,
                            TriggerCause::Elapsed {
                                source_role: source_role.clone(),
                                source_started_at: started_at,
                                interval_seconds: *interval_seconds,
                                event_index,
                            },
                        )?;
                    }
                }
            }
        }
        Ok(())
    })))
}

pub(super) fn claim_elapsed_trigger(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_started_at: u64,
    target_role: &RoleSlug,
    interval_seconds: u64,
    event_index: u64,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("elapsed")
        .join(source_role.as_str())
        .join(source_started_at.to_string())
        .join(target_role.as_str())
        .join(interval_seconds.to_string())
        .join(format!("{event_index}.lock"));
    lock::claim_once(&path, "elapsed trigger event")
}

pub(super) fn claim_role_agent_finished_trigger(
    project: &ProjectPaths,
    source_role: &RoleSlug,
    source_agent: &AgentId,
    run_id: u64,
    target_role: &RoleSlug,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("role-agent-finished")
        .join(source_role.as_str())
        .join(source_agent.as_str())
        .join(run_id.to_string())
        .join(target_role.as_str())
        .join("claimed.lock");
    lock::claim_once(&path, "role agent finished trigger event")
}

pub(super) fn claim_queue_idle_trigger(
    project: &ProjectPaths,
    queue: &str,
    empty_since: u64,
    target_role: &RoleSlug,
    idle_seconds: u64,
) -> Result<bool> {
    let path = project
        .runtime_dir()
        .join("trigger-events")
        .join("queue-idle")
        .join(queue)
        .join(empty_since.to_string())
        .join(target_role.as_str())
        .join(idle_seconds.to_string())
        .join("claimed.lock");
    lock::claim_once(&path, "queue idle trigger event")
}

pub(super) fn launch_triggered_role(
    project: &ProjectPaths,
    role: &RoleSlug,
    launch: &TriggerLaunch,
    cause: TriggerCause,
) -> Result<()> {
    match launch {
        TriggerLaunch::Async => {
            let role_paths = RolePaths::new(project.clone(), role.clone());
            let mut config = load_role_config(&role_paths)?;
            if config.status == RoleStatus::Paused {
                return Ok(());
            }
            config.status = RoleStatus::Active;
            save_role_config(&role_paths, &config)?;
            start_one_agent_with_trigger(&role_paths, &config, None, Some(&cause))?;
            Ok(())
        }
        TriggerLaunch::Queued { queue } => {
            enqueue_triggered_role(project, queue, role, cause)?;
            ensure_trigger_queue_worker_started(project, queue)
        }
    }
}

pub(super) fn run_trigger_queue_worker(args: RunQueueArgs) -> Result<()> {
    let project = ProjectPaths::new(
        args.project
            .canonicalize()
            .context("Invalid project path")?,
    );
    drain_trigger_queue(&project, &args.queue)
}

pub(super) fn enqueue_triggered_role(
    project: &ProjectPaths,
    queue: &str,
    role: &RoleSlug,
    cause: TriggerCause,
) -> Result<()> {
    validate_trigger_queue(queue)?;
    let _state_lock = acquire_trigger_queue_state_lock(project, queue)?;
    let mut state = load_trigger_queue(project, queue)?;
    state.items.push(TriggerQueueItem {
        role: role.clone(),
        enqueued_at: unix_timestamp(),
        cause,
    });
    io::write_toml(&trigger_queue_path(project, queue), &state)
}

pub(super) fn ensure_trigger_queue_worker_started(
    project: &ProjectPaths,
    queue: &str,
) -> Result<()> {
    validate_trigger_queue(queue)?;
    if lock::is_active(&trigger_queue_lock_path(project, queue))? {
        return Ok(());
    }
    let log_path = trigger_queue_worker_log_path(project, queue);
    let log = append_trigger_queue_worker_log(&log_path)?;
    let stderr = log.try_clone().with_context(|| {
        format!(
            "Failed to clone trigger queue worker log `{}`",
            log_path.display()
        )
    })?;
    Command::new(think_child_executable()?)
        .arg("run-child")
        .arg("queue")
        .arg("--project")
        .arg(&project.root)
        .arg("--queue")
        .arg(queue)
        .current_dir(&project.root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("Failed to start trigger queue worker `{queue}`"))?;
    Ok(())
}

pub(super) fn drain_trigger_queue(project: &ProjectPaths, queue: &str) -> Result<()> {
    validate_trigger_queue(queue)?;
    let Some(drain_lock) = lock::FileLock::try_acquire(
        trigger_queue_lock_path(project, queue),
        "trigger queue lock",
    )?
    else {
        return Ok(());
    };
    let mut drain_lock = Some(drain_lock);
    loop {
        let item = {
            let _state_lock = acquire_trigger_queue_state_lock(project, queue)?;
            let queue_state = load_trigger_queue(project, queue)?;
            let Some(item) = queue_state.items.first().cloned() else {
                // Let a concurrent enqueue observe the inactive drain lock after it
                // obtains the state lock, so it knows to start the next worker.
                drop(drain_lock.take());
                return Ok(());
            };
            item
        };
        let role_paths = RolePaths::new(project.clone(), item.role.clone());
        let mut config = load_role_config(&role_paths)?;
        if config.status != RoleStatus::Paused {
            config.status = RoleStatus::Active;
            save_role_config(&role_paths, &config)?;
            start_one_agent_sync_with_trigger(&role_paths, &config, Some(&item.cause))?;
        }
        let _state_lock = acquire_trigger_queue_state_lock(project, queue)?;
        let mut queue_state = load_trigger_queue(project, queue)?;
        let Some(position) = queue_state
            .items
            .iter()
            .position(|candidate| candidate == &item)
        else {
            bail!("trigger queue `{queue}` changed while processing its head item");
        };
        if position != 0 {
            bail!("trigger queue `{queue}` head changed while processing");
        }
        queue_state.items.remove(0);
        io::write_toml(&trigger_queue_path(project, queue), &queue_state)?;
    }
}

pub(super) fn load_trigger_queue(project: &ProjectPaths, queue: &str) -> Result<TriggerQueueState> {
    let path = trigger_queue_path(project, queue);
    if path.exists() {
        io::read_toml(&path)
    } else {
        Ok(TriggerQueueState::default())
    }
}

pub(super) fn trigger_queue_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("trigger-queues")
        .join(format!("{queue}.toml"))
}

pub(super) fn trigger_queue_lock_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("locks")
        .join("trigger-queues")
        .join(format!("{queue}.lock"))
}

pub(super) fn trigger_queue_state_lock_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("locks")
        .join("trigger-queue-state")
        .join(format!("{queue}.lock"))
}

pub(super) fn trigger_queue_worker_log_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("sessions")
        .join("trigger-queues")
        .join(format!("{queue}.log"))
}

pub(super) fn append_trigger_queue_worker_log(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| {
            format!(
                "Failed to open trigger queue worker log `{}`",
                path.display()
            )
        })
}

pub(super) fn acquire_trigger_queue_state_lock(
    project: &ProjectPaths,
    queue: &str,
) -> Result<lock::FileLock> {
    lock::FileLock::acquire_wait(
        trigger_queue_state_lock_path(project, queue),
        "trigger queue state lock",
        TRIGGER_QUEUE_STATE_LOCK_TIMEOUT,
    )
}

pub(super) fn queue_runtime_path(project: &ProjectPaths, queue: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("queue-runtime")
        .join(format!("{queue}.toml"))
}

pub(super) fn refresh_queue_empty_since(
    project: &ProjectPaths,
    queue: &str,
) -> Result<Option<u64>> {
    let path = queue_runtime_path(project, queue);
    if !queue_is_empty(project, queue)? {
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("Failed to remove `{}`", path.display()))?;
        }
        return Ok(None);
    }

    if path.exists() {
        return Ok(Some(io::read_toml::<QueueRuntimeState>(&path)?.empty_since));
    }
    let state = QueueRuntimeState {
        empty_since: unix_timestamp(),
    };
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    io::write_toml(&path, &state)?;
    Ok(Some(state.empty_since))
}

pub(super) fn queue_is_empty(project: &ProjectPaths, queue: &str) -> Result<bool> {
    if !load_trigger_queue(project, queue)?.items.is_empty() {
        return Ok(false);
    }
    Ok(!lock::is_active(&trigger_queue_lock_path(project, queue))?
        && !lock::is_active(&trigger_queue_state_lock_path(project, queue))?)
}

pub(super) fn validate_trigger_queue(queue: &str) -> Result<()> {
    if queue.is_empty()
        || queue.starts_with('-')
        || queue.ends_with('-')
        || queue.contains("--")
        || !queue
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("trigger queue `{queue}` must be a lowercase ASCII slug");
    }
    Ok(())
}
