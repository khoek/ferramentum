use std::any::Any;
use std::io::{self, Write};
use std::panic::{self, AssertUnwindSafe};
use std::process::{Command, ExitCode};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use crate::cli::{
    Cli, Commands, ConfigArgs, ConfigCommands, ConfigGetArgs, ConfigSetArgs, ConfigUnsetArgs,
    CreateArgs, LoginArgs, RefreshCatalogArgs,
};
use crate::config_store::{
    get_config_value, load_config, normalize_config_key, parse_key_value_pair, save_config,
    set_config_value, supported_config_keys, unset_config_value,
};
use crate::gpu::{canonicalize_gpu_name, ensure_runtime_gpu_data_files};
use crate::local::detect_local_container_runtime;
use crate::model::{
    Cloud, CloudMachineCandidate, CreateSearchRequirements, IceConfig, LoginMethod, LoginOutcome,
    OfferDecision, RuntimeCostEstimate,
};
use crate::providers::{
    CloudInstance, CloudProvider, CreateProvider, MarketCreateProvider, aws, gcp, local, vast,
};
use crate::provision::{
    build_accept_prompt, build_search_requirements, ensure_default_create_config,
    estimate_runtime_cost, find_cheapest_cloud_machine, load_gpu_options,
    machine_candidate_summary_lines, prompt_adjust_search_filters, prompt_create_search_filters,
    prompt_offer_decision,
};
use crate::support::{
    ensure_command_available, ensure_provider_cli_installed, maybe_open_browser, nonempty_string,
    prompt_confirm, prompt_theme, require_interactive, resolve_cloud, spinner,
};
use crate::ui::{print_big_red_error, print_notice, print_stage, print_warning};
use crate::workload::{InstanceWorkload, resolve_deploy_hours, resolve_deploy_workload};
use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use crossterm::cursor::{Hide, RestorePosition, SavePosition, Show};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode};
use dialoguer::{Input, Password};

const LIVE_OFFER_INPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LIVE_OFFER_SPINNER_INTERVAL: Duration = Duration::from_millis(250);
const LIVE_OFFER_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

enum MarketCandidateReviewOutcome {
    Accept(CloudMachineCandidate),
    Reject,
    ChangeFilter,
    DryRunComplete,
}

#[derive(Clone, Copy)]
enum LiveOfferAction {
    Accept,
    Reject,
    ChangeFilter,
}

struct RawModeGuard;

impl RawModeGuard {
    fn new() -> Result<Self> {
        enable_raw_mode().context("Failed to enable raw terminal mode")?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

#[derive(Default)]
struct LiveOfferRenderer {
    started: bool,
    last_frame: String,
}

impl LiveOfferRenderer {
    fn redraw(&mut self, lines: &[String]) -> Result<()> {
        let frame = lines.join("\n");
        if self.last_frame == frame {
            return Ok(());
        }

        let mut stderr = io::stderr().lock();
        if !self.started {
            execute!(stderr, SavePosition, Hide)
                .context("Failed to initialize live offer prompt")?;
            self.started = true;
        } else {
            execute!(stderr, RestorePosition, Clear(ClearType::FromCursorDown))
                .context("Failed to clear live offer prompt")?;
        }
        for line in lines {
            write!(stderr, "{line}\r\n").context("Failed to render live offer prompt")?;
        }
        stderr
            .flush()
            .context("Failed to flush live offer prompt")?;
        self.last_frame = frame;
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }

        let mut stderr = io::stderr().lock();
        execute!(
            stderr,
            RestorePosition,
            Clear(ClearType::FromCursorDown),
            Show
        )
        .context("Failed to clear live offer prompt")?;
        stderr
            .flush()
            .context("Failed to flush live offer prompt clear")?;
        self.started = false;
        self.last_frame.clear();
        Ok(())
    }
}

impl Drop for LiveOfferRenderer {
    fn drop(&mut self) {
        let _ = self.clear();
    }
}

pub(crate) fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            print_big_red_error(&format!("{err:#}"));
            ExitCode::from(1)
        }
    }
}

pub(crate) fn run() -> Result<ExitCode> {
    let _instance_lock = capulus::acquire("ice")?;
    let cli = Cli::parse();
    ensure_runtime_gpu_data_files()?;
    let mut config = load_config()?;

    match cli.command {
        Commands::Login(args) => cmd_login(args, &mut config)?,
        Commands::Config(args) => cmd_config(args, &mut config)?,
        Commands::List(args) => crate::listing::cmd_list(args, &config)?,
        Commands::Logs(args) => crate::commands::cmd_logs(args, &config)?,
        Commands::Shell(args) => crate::commands::cmd_shell(args, &config)?,
        Commands::Dl(args) => crate::commands::cmd_download(args, &config)?,
        Commands::Stop(args) => crate::commands::cmd_stop(args, &config)?,
        Commands::Start(args) => crate::commands::cmd_start(args, &config)?,
        Commands::Delete(args) => crate::commands::cmd_delete(args, &config)?,
        Commands::Create(args) => cmd_create(args, &mut config)?,
        Commands::RefreshCatalog(args) => cmd_refresh_catalog(args, &config)?,
    }

    Ok(ExitCode::SUCCESS)
}

fn cmd_login(args: LoginArgs, config: &mut IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    ensure_provider_cli_installed(cloud)?;
    let outcome = match cloud {
        Cloud::VastAi => login_vast(config, args.force)?,
        Cloud::Gcp => login_gcp(config, args.force)?,
        Cloud::Aws => login_aws(config, args.force)?,
        Cloud::Local => login_local()?,
    };
    print_login_outcome(cloud, &outcome);
    Ok(())
}

fn cmd_config(args: ConfigArgs, config: &mut IceConfig) -> Result<()> {
    match args.command {
        ConfigCommands::List(_) => cmd_config_list(config),
        ConfigCommands::Get(args) => cmd_config_get(args, config),
        ConfigCommands::Set(args) => cmd_config_set(args, config),
        ConfigCommands::Unset(args) => cmd_config_unset(args, config),
    }
}

fn cmd_config_list(config: &IceConfig) -> Result<()> {
    for key in supported_config_keys() {
        println!("{key} = {}", get_config_value(config, key)?);
    }
    Ok(())
}

fn cmd_config_get(args: ConfigGetArgs, config: &IceConfig) -> Result<()> {
    let key = normalize_config_key(&args.key)?;
    println!("{key} = {}", get_config_value(config, &key)?);
    Ok(())
}

fn cmd_config_set(args: ConfigSetArgs, config: &mut IceConfig) -> Result<()> {
    let (key, value) = parse_key_value_pair(&args.pair)?;
    let rendered = set_config_value(config, &key, &value)?;
    let path = save_config(config)?;
    print_notice(&format!("Set `{key}` = {rendered} ({})", path.display()));
    Ok(())
}

fn cmd_config_unset(args: ConfigUnsetArgs, config: &mut IceConfig) -> Result<()> {
    let key = normalize_config_key(&args.key)?;
    unset_config_value(config, &key)?;
    let path = save_config(config)?;
    print_notice(&format!("Unset `{key}` ({})", path.display()));
    Ok(())
}

fn print_login_outcome(cloud: Cloud, outcome: &LoginOutcome) {
    let method = match outcome.method {
        LoginMethod::Cached => "cached config",
        LoginMethod::AutoDetected => "existing local credentials",
        LoginMethod::Prompted => "interactive input",
    };
    if let Some(path) = &outcome.saved_path {
        println!("{} login ready via {} ({})", cloud, method, path.display());
    } else {
        println!("{cloud} login ready via {method}");
    }
}

fn cmd_create(args: CreateArgs, config: &mut IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    match cloud {
        Cloud::VastAi => create_for::<vast::Provider>(config, &args),
        Cloud::Local => create_for::<local::Provider>(config, &args),
        Cloud::Gcp => create_market_machine::<gcp::Provider>(config, &args),
        Cloud::Aws => create_market_machine::<aws::Provider>(config, &args),
    }
}

fn refresh_catalog_error_message(cloud: Cloud) -> Option<String> {
    match cloud {
        Cloud::Gcp => None,
        Cloud::Aws => None,
        cloud => Some(format!(
            "`ice refresh-catalog --cloud {cloud}` is not implemented."
        )),
    }
}

fn cmd_refresh_catalog(args: RefreshCatalogArgs, config: &IceConfig) -> Result<()> {
    if let Some(message) = refresh_catalog_error_message(args.cloud) {
        bail!("{message}");
    }
    match args.cloud {
        Cloud::Gcp => {
            ensure_provider_cli_installed(Cloud::Gcp)?;
            let outcome = gcp::refresh_local_catalog(config)?;
            println!(
                "Refreshed gcp catalog with {} changed entries ({} total priced machine entries, {}).",
                outcome.changed_entry_count,
                outcome.entry_count,
                outcome.path.display()
            );
            if outcome.warning_count != 0 {
                print_warning(&format!(
                    "Encountered {} GCP catalog warnings:",
                    outcome.warning_count
                ));
                for line in outcome.warning_summary {
                    print_warning(&line);
                }
            }
            Ok(())
        }
        Cloud::Aws => {
            ensure_provider_cli_installed(Cloud::Aws)?;
            let outcome = aws::refresh_local_catalog(config)?;
            println!(
                "Refreshed aws catalog with {} changed entries ({} total priced machine entries, {}).",
                outcome.changed_entry_count,
                outcome.entry_count,
                outcome.path.display()
            );
            if outcome.warning_count != 0 {
                print_warning(&format!(
                    "Encountered {} AWS catalog warnings:",
                    outcome.warning_count
                ));
                for line in outcome.warning_summary {
                    print_warning(&line);
                }
            }
            Ok(())
        }
        _ => unreachable!("non-GCP/AWS refresh-catalog availability is handled above"),
    }
}

fn create_for<P: CreateProvider>(config: &mut IceConfig, args: &CreateArgs) -> Result<()> {
    P::ensure_cli()?;
    P::create(config, args)
}

fn create_market_machine<P: MarketCreateProvider>(
    config: &mut IceConfig,
    args: &CreateArgs,
) -> Result<()>
where
    <P as CloudProvider>::Instance: CloudInstance,
    P: 'static,
{
    let mut effective_config = config.clone();
    apply_create_search_overrides(&mut effective_config, P::CLOUD, args)?;
    P::ensure_cli()?;
    let gpu_options = load_gpu_options(P::CLOUD, None);
    ensure_default_create_config(&mut effective_config, P::CLOUD, &gpu_options)?;
    let hours = resolve_deploy_hours(&effective_config, args.hours)?;
    let workload = resolve_deploy_workload(&args.target_request())?;
    let mut search = build_search_requirements(&effective_config, P::CLOUD)?;
    if args.custom {
        prompt_create_search_filters(P::CLOUD, &mut search, &gpu_options)?;
    }

    let candidate = loop {
        let candidate = find_cheapest_cloud_machine(
            P::CLOUD,
            &effective_config,
            &search,
            args.machine.as_deref(),
        )?;
        match review_market_candidate::<P>(&effective_config, args, &search, hours, candidate)? {
            MarketCandidateReviewOutcome::Accept(candidate) => break candidate,
            MarketCandidateReviewOutcome::ChangeFilter => {
                prompt_adjust_search_filters(
                    P::CLOUD,
                    &mut search,
                    &load_gpu_options(P::CLOUD, None),
                )?;
            }
            MarketCandidateReviewOutcome::Reject => {
                println!("Aborted.");
                return Ok(());
            }
            MarketCandidateReviewOutcome::DryRunComplete => return Ok(()),
        }
    };

    let instance = P::create_machine(&effective_config, &candidate, hours, &workload)?;
    if let InstanceWorkload::Unpack(source) = &workload {
        P::deploy_unpack(&effective_config, &instance, source)?;
        if prompt_confirm("Follow unpack logs now?", true)? {
            print_stage("Following unpack logs");
            P::stream_unpack_logs(&effective_config, &instance, 200, true)?;
        } else {
            println!(
                "Use `ice logs --cloud {} {} --follow` to inspect stdout/stderr.",
                P::CLOUD,
                instance.display_name()
            );
        }
    } else if prompt_confirm("Open shell in the new instance now?", true)? {
        print_stage("Opening shell");
        P::open_instance_shell(&effective_config, &instance)?;
    }
    Ok(())
}

fn review_market_candidate<P: MarketCreateProvider + 'static>(
    config: &IceConfig,
    args: &CreateArgs,
    search: &CreateSearchRequirements,
    hours: f64,
    candidate: CloudMachineCandidate,
) -> Result<MarketCandidateReviewOutcome> {
    if matches!(P::CLOUD, Cloud::Gcp | Cloud::Aws) {
        return review_live_market_candidate::<P>(config, args, search, hours, candidate);
    }

    let candidate = P::refresh_machine_offer(config, &candidate)?;
    review_verified_market_candidate(config, P::CLOUD, args, search, hours, candidate)
}

fn apply_create_search_overrides(
    config: &mut IceConfig,
    cloud: Cloud,
    args: &CreateArgs,
) -> Result<()> {
    let gpu_override = create_gpu_override(args)?;
    match cloud {
        Cloud::VastAi => apply_cloud_search_overrides(
            &mut config.default.vast_ai.min_cpus,
            &mut config.default.vast_ai.min_ram_gb,
            &mut config.default.vast_ai.allowed_gpus,
            &mut config.default.vast_ai.max_price_per_hr,
            args,
            gpu_override,
        ),
        Cloud::Gcp => apply_cloud_search_overrides(
            &mut config.default.gcp.min_cpus,
            &mut config.default.gcp.min_ram_gb,
            &mut config.default.gcp.allowed_gpus,
            &mut config.default.gcp.max_price_per_hr,
            args,
            gpu_override,
        ),
        Cloud::Aws => apply_cloud_search_overrides(
            &mut config.default.aws.min_cpus,
            &mut config.default.aws.min_ram_gb,
            &mut config.default.aws.allowed_gpus,
            &mut config.default.aws.max_price_per_hr,
            args,
            gpu_override,
        ),
        Cloud::Local => Ok(()),
    }
}

fn apply_cloud_search_overrides(
    min_cpus: &mut Option<u32>,
    min_ram_gb: &mut Option<f64>,
    allowed_gpus: &mut Option<Vec<String>>,
    max_price_per_hr: &mut Option<f64>,
    args: &CreateArgs,
    gpu_override: Option<Vec<String>>,
) -> Result<()> {
    if let Some(value) = args.min_cpus {
        if value == 0 {
            bail!("--min-cpus must be at least 1.");
        }
        *min_cpus = Some(value);
    }
    if let Some(value) = args.min_ram_gb {
        if !(value.is_finite() && value > 0.0) {
            bail!("--min-ram-gb must be a positive number.");
        }
        *min_ram_gb = Some(value);
    }
    if let Some(value) = args.max_price_per_hr {
        if !(value.is_finite() && value > 0.0) {
            bail!("--max-price-per-hr must be a positive number.");
        }
        *max_price_per_hr = Some(value);
    }
    if let Some(gpus) = gpu_override {
        *allowed_gpus = Some(gpus);
    }
    Ok(())
}

fn create_gpu_override(args: &CreateArgs) -> Result<Option<Vec<String>>> {
    if args.no_gpu {
        return Ok(Some(Vec::new()));
    }
    if args.gpus.is_empty() {
        return Ok(None);
    }
    let mut values = Vec::with_capacity(args.gpus.len());
    for value in &args.gpus {
        let token = value.trim();
        if token.is_empty() {
            bail!("--gpu does not accept empty values.");
        }
        values.push(
            canonicalize_gpu_name(token).ok_or_else(|| anyhow!("Unknown GPU model `{token}`."))?,
        );
    }
    values.sort();
    values.dedup();
    Ok(Some(values))
}

fn review_live_market_candidate<P: MarketCreateProvider + 'static>(
    config: &IceConfig,
    args: &CreateArgs,
    search: &CreateSearchRequirements,
    hours: f64,
    candidate: CloudMachineCandidate,
) -> Result<MarketCandidateReviewOutcome> {
    let cached_cost = estimate_runtime_cost(P::CLOUD, candidate.hourly_usd, hours)?;
    let receiver = spawn_machine_offer_refresh::<P>(config, &candidate);

    if args.dry_run {
        print_machine_candidate_summary_with_note(
            config,
            P::CLOUD,
            &candidate,
            &cached_cost,
            search,
            Some("(cached)"),
        )?;
        let verified = wait_for_verified_machine_offer(
            P::CLOUD,
            &candidate.machine,
            receiver,
            &format!("Verifying {} price...", P::CLOUD),
        )?;
        return review_verified_market_candidate(config, P::CLOUD, args, search, hours, verified);
    }

    match prompt_live_machine_offer(
        config,
        P::CLOUD,
        search,
        &candidate,
        &cached_cost,
        hours,
        receiver,
    )? {
        MarketCandidateReviewOutcome::Accept(verified) => {
            let verified_cost = estimate_runtime_cost(P::CLOUD, verified.hourly_usd, hours)?;
            print_machine_candidate_summary(config, P::CLOUD, &verified, &verified_cost, search)?;
            if verified_cost.hourly_usd > search.max_price_per_hr {
                bail!(
                    "No machine meets max price ${:.4}/hr. Cheapest matching machine is {} in {} at ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested).",
                    search.max_price_per_hr,
                    verified.machine,
                    verified.region,
                    verified.hourly_usd,
                    verified_cost.total_usd,
                    verified_cost.billed_hours,
                    verified_cost.requested_hours
                );
            }
            Ok(MarketCandidateReviewOutcome::Accept(verified))
        }
        outcome => Ok(outcome),
    }
}

fn review_verified_market_candidate(
    config: &IceConfig,
    cloud: Cloud,
    args: &CreateArgs,
    search: &CreateSearchRequirements,
    hours: f64,
    candidate: CloudMachineCandidate,
) -> Result<MarketCandidateReviewOutcome> {
    let cost = estimate_runtime_cost(cloud, candidate.hourly_usd, hours)?;
    print_machine_candidate_summary(config, cloud, &candidate, &cost, search)?;

    if cost.hourly_usd > search.max_price_per_hr {
        bail!(
            "No machine meets max price ${:.4}/hr. Cheapest matching machine is {} in {} at ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested).",
            search.max_price_per_hr,
            candidate.machine,
            candidate.region,
            candidate.hourly_usd,
            cost.total_usd,
            cost.billed_hours,
            cost.requested_hours
        );
    }

    if args.dry_run {
        println!(
            "Dry run: cheapest matching machine is {} in {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before create.",
            candidate.machine,
            candidate.region,
            candidate.hourly_usd,
            cost.total_usd,
            cost.billed_hours,
            cost.requested_hours
        );
        return Ok(MarketCandidateReviewOutcome::DryRunComplete);
    }

    Ok(match prompt_offer_decision(&build_accept_prompt(&cost))? {
        OfferDecision::Accept => MarketCandidateReviewOutcome::Accept(candidate),
        OfferDecision::Reject => MarketCandidateReviewOutcome::Reject,
        OfferDecision::ChangeFilter => MarketCandidateReviewOutcome::ChangeFilter,
    })
}

fn spawn_machine_offer_refresh<P: MarketCreateProvider + 'static>(
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
) -> Receiver<Result<CloudMachineCandidate>> {
    let (sender, receiver) = mpsc::channel();
    let config = config.clone();
    let candidate = candidate.clone();
    thread::spawn(move || {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            P::refresh_machine_offer(&config, &candidate)
        }))
        .unwrap_or_else(|payload| {
            Err(anyhow!(
                "Live {} offer verification worker panicked: {}",
                P::CLOUD,
                panic_payload_message(payload)
            ))
        });
        let _ = sender.send(result);
    });
    receiver
}

fn wait_for_verified_machine_offer(
    cloud: Cloud,
    machine: &str,
    receiver: Receiver<Result<CloudMachineCandidate>>,
    message: &str,
) -> Result<CloudMachineCandidate> {
    let progress = spinner(message);
    let result = receiver.recv().map_err(|_| {
        anyhow!("Live {cloud} offer verification worker exited unexpectedly for `{machine}`.")
    })?;
    progress.finish_and_clear();
    result
}

fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_owned();
    }
    "unknown panic payload".to_owned()
}

fn print_machine_candidate_summary_with_note(
    config: &IceConfig,
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
    price_note: Option<&str>,
) -> Result<()> {
    println!();
    for line in
        machine_candidate_summary_display_lines(config, cloud, candidate, cost, req, price_note)?
    {
        println!("{line}");
    }
    println!();
    Ok(())
}

fn prompt_live_machine_offer(
    config: &IceConfig,
    cloud: Cloud,
    req: &CreateSearchRequirements,
    cached_candidate: &CloudMachineCandidate,
    cached_cost: &RuntimeCostEstimate,
    hours: f64,
    receiver: Receiver<Result<CloudMachineCandidate>>,
) -> Result<MarketCandidateReviewOutcome> {
    require_interactive("Offer acceptance prompt requires interactive stdin.")?;

    let static_lines =
        live_machine_offer_static_lines(config, cloud, req, cached_candidate, cached_cost)?;
    let raw_mode = RawModeGuard::new()?;
    let mut renderer = LiveOfferRenderer::default();
    let mut selected_index = 0usize;
    let mut spinner_index = 0usize;
    let mut last_spinner_tick = Instant::now();
    let mut accepted_early = false;
    let mut verified_candidate: Option<CloudMachineCandidate> = None;
    let mut verified_cost: Option<RuntimeCostEstimate> = None;
    let mut receiver = Some(receiver);

    loop {
        if let Some(candidate) =
            try_recv_live_offer_candidate(cloud, &cached_candidate.machine, &mut receiver)?
        {
            let cost = estimate_runtime_cost(cloud, candidate.hourly_usd, hours)?;
            verified_candidate = Some(candidate);
            verified_cost = Some(cost);
        }

        let verified_over_price = verified_cost
            .map(|cost| cost.hourly_usd > req.max_price_per_hr)
            .unwrap_or(false);

        if accepted_early
            && !verified_over_price
            && let Some(candidate) = verified_candidate.clone()
        {
            renderer.clear()?;
            drop(raw_mode);
            return Ok(MarketCandidateReviewOutcome::Accept(candidate));
        }

        let options = live_offer_options(
            verified_candidate.is_some(),
            accepted_early,
            verified_over_price,
        );
        if selected_index >= options.len() {
            selected_index = options.len().saturating_sub(1);
        }
        let lines = live_machine_offer_dynamic_lines(
            req,
            cached_cost,
            verified_cost.as_ref(),
            accepted_early,
            selected_index,
            spinner_index,
            &options,
        );
        let mut frame = static_lines.clone();
        frame.push(String::new());
        frame.extend(lines);
        renderer.redraw(&frame)?;

        if event::poll(LIVE_OFFER_INPUT_POLL_INTERVAL).context("Failed to poll terminal input")? {
            match event::read().context("Failed to read terminal input")? {
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    if let Some(outcome) =
                        handle_live_offer_key(key.code, &options, &mut selected_index)?
                    {
                        match outcome {
                            LiveOfferAction::Accept => {
                                if verified_candidate.is_some() {
                                    renderer.clear()?;
                                    drop(raw_mode);
                                    return Ok(MarketCandidateReviewOutcome::Accept(
                                        verified_candidate.expect("verified candidate"),
                                    ));
                                }
                                accepted_early = true;
                                selected_index = 0;
                            }
                            LiveOfferAction::Reject => {
                                renderer.clear()?;
                                drop(raw_mode);
                                return Ok(MarketCandidateReviewOutcome::Reject);
                            }
                            LiveOfferAction::ChangeFilter => {
                                renderer.clear()?;
                                drop(raw_mode);
                                return Ok(MarketCandidateReviewOutcome::ChangeFilter);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if verified_candidate.is_none()
            && last_spinner_tick.elapsed() >= LIVE_OFFER_SPINNER_INTERVAL
        {
            spinner_index = (spinner_index + 1) % LIVE_OFFER_SPINNER_FRAMES.len();
            last_spinner_tick = Instant::now();
        }
    }
}

fn live_offer_options(
    verified: bool,
    accepted_early: bool,
    verified_over_price: bool,
) -> Vec<(LiveOfferAction, &'static str)> {
    if verified {
        if verified_over_price {
            return vec![
                (LiveOfferAction::Reject, "reject"),
                (LiveOfferAction::ChangeFilter, "change filter"),
            ];
        }
        return vec![
            (LiveOfferAction::Accept, "accept and create"),
            (LiveOfferAction::Reject, "reject"),
            (LiveOfferAction::ChangeFilter, "change filter"),
        ];
    }

    if accepted_early {
        return vec![
            (LiveOfferAction::Reject, "abort"),
            (LiveOfferAction::ChangeFilter, "change filter"),
        ];
    }

    vec![
        (LiveOfferAction::Accept, "accept once verified"),
        (LiveOfferAction::Reject, "reject"),
        (LiveOfferAction::ChangeFilter, "change filter"),
    ]
}

fn live_machine_offer_static_lines(
    config: &IceConfig,
    cloud: Cloud,
    req: &CreateSearchRequirements,
    cached_candidate: &CloudMachineCandidate,
    cached_cost: &RuntimeCostEstimate,
) -> Result<Vec<String>> {
    let mut lines = machine_candidate_summary_display_lines(
        config,
        cloud,
        cached_candidate,
        cached_cost,
        req,
        Some("(cached)"),
    )?;
    lines.retain(|line| !line.starts_with("  Price:"));
    if matches!(lines.last(), Some(line) if line.starts_with("  Your filters:")) {
        lines.pop();
    }
    Ok(lines)
}

pub(crate) fn machine_candidate_summary_display_lines(
    config: &IceConfig,
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
    price_note: Option<&str>,
) -> Result<Vec<String>> {
    let mut lines = machine_candidate_summary_lines(cloud, candidate, cost, req, price_note);
    if let Some(project) = machine_candidate_project(config, cloud)? {
        let insert_at = lines
            .iter()
            .position(|line| line.starts_with("  Region:"))
            .unwrap_or(lines.len());
        lines.insert(insert_at, format!("  Project: {project}"));
    }
    Ok(lines)
}

fn print_machine_candidate_summary(
    config: &IceConfig,
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
) -> Result<()> {
    print_machine_candidate_summary_with_note(config, cloud, candidate, cost, req, None)
}

fn machine_candidate_project(config: &IceConfig, cloud: Cloud) -> Result<Option<String>> {
    match cloud {
        Cloud::Gcp => detect_gcp_project(config, true).map(Some).ok_or_else(|| {
            anyhow!("No GCP project configured. Run `ice login --cloud gcp` first.")
        }),
        Cloud::Aws | Cloud::Local | Cloud::VastAi => Ok(None),
    }
}

fn live_machine_offer_price_line(
    req: &CreateSearchRequirements,
    cached_cost: &RuntimeCostEstimate,
    verified_cost: Option<&RuntimeCostEstimate>,
    accepted_early: bool,
    spinner_index: usize,
) -> String {
    if let Some(cost) = verified_cost {
        if cost.hourly_usd > req.max_price_per_hr {
            return format!(
                "  Price: ${:.4}/hr (verified, exceeds max ${:.4}/hr)",
                cost.hourly_usd, req.max_price_per_hr
            );
        }
        return format!("  Price: ${:.4}/hr (verified)", cost.hourly_usd);
    }
    if accepted_early {
        return format!(
            "  Price: ${:.4}/hr (cached, {} verifying after early accept)",
            cached_cost.hourly_usd, LIVE_OFFER_SPINNER_FRAMES[spinner_index]
        );
    }
    format!(
        "  Price: ${:.4}/hr (cached, {} verifying)",
        cached_cost.hourly_usd, LIVE_OFFER_SPINNER_FRAMES[spinner_index]
    )
}

fn live_machine_offer_dynamic_lines(
    req: &CreateSearchRequirements,
    cached_cost: &RuntimeCostEstimate,
    verified_cost: Option<&RuntimeCostEstimate>,
    accepted_early: bool,
    selected_index: usize,
    spinner_index: usize,
    options: &[(LiveOfferAction, &'static str)],
) -> Vec<String> {
    let mut lines = vec![live_machine_offer_price_line(
        req,
        cached_cost,
        verified_cost,
        accepted_early,
        spinner_index,
    )];

    lines.push(if verified_cost.is_some() {
        "Keys: j/k move, Enter confirm, f filters, n reject".to_owned()
    } else if accepted_early {
        "Keys: j/k move, Enter confirm, f filters, n abort".to_owned()
    } else {
        "Keys: j/k move, Enter confirm, a early, f filters, n reject".to_owned()
    });
    lines.push(String::new());

    for (index, (_, label)) in options.iter().enumerate() {
        let cursor = if index == selected_index { ">" } else { " " };
        lines.push(format!("{cursor} {label}"));
    }

    lines
}

fn try_recv_live_offer_candidate(
    cloud: Cloud,
    machine: &str,
    receiver: &mut Option<Receiver<Result<CloudMachineCandidate>>>,
) -> Result<Option<CloudMachineCandidate>> {
    let Some(active_receiver) = receiver.as_ref() else {
        return Ok(None);
    };
    match active_receiver.try_recv() {
        Ok(result) => {
            *receiver = None;
            result.map(Some)
        }
        Err(TryRecvError::Empty) => Ok(None),
        Err(TryRecvError::Disconnected) => {
            bail!("Live {cloud} offer verification worker exited unexpectedly for `{machine}`.");
        }
    }
}

fn handle_live_offer_key(
    code: KeyCode,
    options: &[(LiveOfferAction, &'static str)],
    selected_index: &mut usize,
) -> Result<Option<LiveOfferAction>> {
    match code {
        KeyCode::Up | KeyCode::Char('k') | KeyCode::Char('K') => {
            *selected_index = selected_index.saturating_sub(1);
            Ok(None)
        }
        KeyCode::Down | KeyCode::Char('j') | KeyCode::Char('J') => {
            if *selected_index + 1 < options.len() {
                *selected_index += 1;
            }
            Ok(None)
        }
        KeyCode::Enter => Ok(options.get(*selected_index).map(|(action, _)| *action)),
        KeyCode::Char('a') | KeyCode::Char('A') => Ok(options
            .iter()
            .find_map(|(action, _)| matches!(action, LiveOfferAction::Accept).then_some(*action))),
        KeyCode::Char('f') | KeyCode::Char('F') | KeyCode::Char('c') | KeyCode::Char('C') => {
            Ok(Some(LiveOfferAction::ChangeFilter))
        }
        KeyCode::Char('n')
        | KeyCode::Char('N')
        | KeyCode::Char('q')
        | KeyCode::Char('Q')
        | KeyCode::Esc => Ok(Some(LiveOfferAction::Reject)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_req() -> CreateSearchRequirements {
        CreateSearchRequirements {
            min_cpus: 1,
            min_ram_gb: 1.0,
            allowed_gpus: Vec::new(),
            max_price_per_hr: 1.0,
        }
    }

    fn test_candidate(hourly_usd: f64) -> CloudMachineCandidate {
        CloudMachineCandidate {
            machine: "e2-micro".to_owned(),
            vcpus: 2,
            ram_mb: 1_000,
            gpus: Vec::new(),
            hourly_usd,
            region: "us-central1".to_owned(),
            zone: Some("us-central1-a".to_owned()),
        }
    }

    #[test]
    fn live_offer_options_switch_to_verified_accept_label() {
        let options = live_offer_options(true, false, false);
        assert_eq!(options[0].1, "accept and create");
        assert!(matches!(options[0].0, LiveOfferAction::Accept));
    }

    #[test]
    fn live_offer_lines_show_verified_status_and_price() {
        let req = test_req();
        let verified = test_candidate(0.0084);
        let verified_cost =
            estimate_runtime_cost(Cloud::Gcp, verified.hourly_usd, 1.0).expect("verified cost");

        let lines = live_machine_offer_dynamic_lines(
            &req,
            &verified_cost,
            Some(&verified_cost),
            false,
            0,
            0,
            &live_offer_options(true, false, false),
        );

        assert!(
            lines
                .iter()
                .any(|line| line.contains("Price: $0.0084/hr (verified)"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("> accept and create"))
        );
    }

    #[test]
    fn live_offer_receiver_ignores_disconnect_after_result_is_received() {
        let (sender, receiver) = std::sync::mpsc::channel();
        sender
            .send(Ok(test_candidate(0.0084)))
            .expect("send verified candidate");
        drop(sender);

        let mut receiver = Some(receiver);
        let first = try_recv_live_offer_candidate(Cloud::Gcp, "e2-micro", &mut receiver)
            .expect("receive verified candidate");
        assert!(first.is_some());
        let second = try_recv_live_offer_candidate(Cloud::Gcp, "e2-micro", &mut receiver)
            .expect("ignore disconnect after result");
        assert!(second.is_none());
    }

    #[test]
    fn refresh_catalog_supports_aws() {
        assert_eq!(refresh_catalog_error_message(Cloud::Aws), None);
    }

    #[test]
    fn refresh_catalog_reports_vast_as_unimplemented() {
        assert_eq!(
            refresh_catalog_error_message(Cloud::VastAi).as_deref(),
            Some("`ice refresh-catalog --cloud vast.ai` is not implemented.")
        );
    }
}

fn login_local() -> Result<LoginOutcome> {
    detect_local_container_runtime()?;
    Ok(LoginOutcome {
        method: LoginMethod::AutoDetected,
        saved_path: None,
    })
}

fn login_vast(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    if !force && let Some(existing_key) = config.auth.vast_ai.api_key.as_deref() {
        match vast::VastClient::new(existing_key)?.validate_api_key() {
            Ok(()) => {
                return Ok(LoginOutcome {
                    method: LoginMethod::Cached,
                    saved_path: None,
                });
            }
            Err(err) => print_warning(&format!("Stored vast.ai API key is invalid: {err:#}")),
        }
    }

    require_interactive("`ice login --cloud vast.ai` requires interactive stdin.")?;
    let key_page = "https://cloud.vast.ai/manage-keys/";
    print_notice(&format!(
        "Open {key_page}, copy/create an API key, then paste it below."
    ));
    maybe_open_browser(key_page);

    let api_key = Password::with_theme(prompt_theme())
        .with_prompt("Paste Vast API key")
        .interact()
        .context("Failed to read API key")?;
    let api_key = api_key.trim().to_owned();
    if api_key.is_empty() {
        bail!("API key cannot be empty.");
    }

    let spinner = spinner("Validating vast.ai API key...");
    let client = vast::VastClient::new(&api_key)?;
    client.validate_api_key()?;
    spinner.finish_with_message("vast.ai API key validated.");

    config.auth.vast_ai.api_key = Some(api_key);
    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn login_gcp(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    ensure_command_available("gcloud")?;

    let detected_project = detect_gcp_project(config, !force);
    let detected_creds_path = detect_gcp_credentials_path(config, !force);
    let has_active_account = gcp_has_active_account()?;

    if has_active_account || detected_creds_path.is_some() {
        let mut changed = false;
        if force {
            if detected_project.is_none() && config.auth.gcp.project.take().is_some() {
                changed = true;
            }
            if detected_creds_path.is_none()
                && config.auth.gcp.service_account_json.take().is_some()
            {
                changed = true;
            }
        }
        if let Some(project) = detected_project
            && config.auth.gcp.project.as_deref() != Some(project.as_str())
        {
            config.auth.gcp.project = Some(project);
            changed = true;
        }
        if let Some(path) = detected_creds_path
            && config.auth.gcp.service_account_json.as_deref() != Some(path.as_str())
        {
            config.auth.gcp.service_account_json = Some(path);
            changed = true;
        }

        return Ok(LoginOutcome {
            method: LoginMethod::AutoDetected,
            saved_path: if changed {
                Some(save_config(config)?)
            } else {
                None
            },
        });
    }

    require_interactive("`ice login --cloud gcp` requires interactive stdin.")?;
    maybe_open_browser("https://console.cloud.google.com/");
    print_warning(
        "Could not auto-detect GCP credentials. Provide a service-account JSON path, or run `gcloud auth login` and retry.",
    );

    let project_seed = detect_gcp_project(config, true)
        .or_else(|| {
            if force {
                None
            } else {
                config.auth.gcp.project.clone()
            }
        })
        .unwrap_or_default();
    let service_account_seed = detect_gcp_credentials_path(config, true)
        .or_else(|| {
            if force {
                None
            } else {
                config.auth.gcp.service_account_json.clone()
            }
        })
        .unwrap_or_default();

    let project = Input::<String>::with_theme(prompt_theme())
        .with_prompt("GCP project ID (optional)")
        .with_initial_text(project_seed)
        .allow_empty(true)
        .interact_text()
        .context("Failed to read GCP project ID")?;

    let service_account_json = Input::<String>::with_theme(prompt_theme())
        .with_prompt("Service-account JSON path")
        .with_initial_text(service_account_seed)
        .allow_empty(true)
        .interact_text()
        .context("Failed to read GCP credentials path")?;

    let service_account_json = nonempty_string(service_account_json);
    if service_account_json.is_none() {
        bail!(
            "No credentials configured. Provide a service-account JSON path, or run `gcloud auth login` and retry."
        );
    }

    config.auth.gcp.project = nonempty_string(project);
    config.auth.gcp.service_account_json = service_account_json;
    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn login_aws(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    ensure_command_available("aws")?;

    let mut changed = false;
    let env_keypair = detect_aws_env_keypair();
    if let Some((access_key_id, secret_access_key)) = env_keypair.as_ref() {
        if config.auth.aws.access_key_id.as_deref() != Some(access_key_id.as_str()) {
            config.auth.aws.access_key_id = Some(access_key_id.clone());
            changed = true;
        }
        if config.auth.aws.secret_access_key.as_deref() != Some(secret_access_key.as_str()) {
            config.auth.aws.secret_access_key = Some(secret_access_key.clone());
            changed = true;
        }
    } else if force {
        if config.auth.aws.access_key_id.take().is_some() {
            changed = true;
        }
        if config.auth.aws.secret_access_key.take().is_some() {
            changed = true;
        }
    }

    if aws_identity_detected(config, !force)? {
        return Ok(LoginOutcome {
            method: LoginMethod::AutoDetected,
            saved_path: if changed {
                Some(save_config(config)?)
            } else {
                None
            },
        });
    }

    require_interactive("`ice login --cloud aws` requires interactive stdin.")?;
    maybe_open_browser("https://console.aws.amazon.com/");
    print_warning("Could not auto-detect AWS credentials. Enter an access key pair.");

    let access_key_seed = if force {
        env_keypair
            .as_ref()
            .map(|(key, _)| key.clone())
            .unwrap_or_default()
    } else {
        config.auth.aws.access_key_id.clone().unwrap_or_default()
    };
    let access_key_id = Input::<String>::with_theme(prompt_theme())
        .with_prompt("AWS access key ID")
        .with_initial_text(access_key_seed)
        .interact_text()
        .context("Failed to read AWS access key ID")?;

    let secret_access_key = Password::with_theme(prompt_theme())
        .with_prompt("AWS secret access key")
        .allow_empty_password(false)
        .interact()
        .context("Failed to read AWS secret access key")?;

    let access_key_id = nonempty_string(access_key_id)
        .ok_or_else(|| anyhow!("AWS access key ID cannot be empty."))?;
    let secret_access_key = secret_access_key.trim().to_owned();
    if secret_access_key.is_empty() {
        bail!("AWS secret access key cannot be empty.");
    }

    config.auth.aws.access_key_id = Some(access_key_id);
    config.auth.aws.secret_access_key = Some(secret_access_key);
    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn detect_gcp_project(config: &IceConfig, include_cached: bool) -> Option<String> {
    capulus::gcp::detect_project(config.auth.gcp.project.as_deref(), include_cached)
}

fn detect_gcp_credentials_path(config: &IceConfig, include_cached: bool) -> Option<String> {
    capulus::gcp::detect_credentials_path(
        config.auth.gcp.service_account_json.as_deref(),
        include_cached,
    )
}

fn gcp_has_active_account() -> Result<bool> {
    capulus::gcp::has_active_account()
}

fn detect_aws_env_keypair() -> Option<(String, String)> {
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    let access_key_id = access_key_id.trim().to_owned();
    let secret_access_key = secret_access_key.trim().to_owned();
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }
    Some((access_key_id, secret_access_key))
}

fn aws_identity_detected(config: &IceConfig, include_cached: bool) -> Result<bool> {
    let mut default_chain = Command::new("aws");
    default_chain.args([
        "sts",
        "get-caller-identity",
        "--output",
        "json",
        "--region",
        "us-east-1",
    ]);
    if default_chain.status().is_ok_and(|status| status.success()) {
        return Ok(true);
    }

    if !include_cached {
        return Ok(false);
    }

    let Some(access_key_id) = config.auth.aws.access_key_id.as_deref() else {
        return Ok(false);
    };
    let Some(secret_access_key) = config.auth.aws.secret_access_key.as_deref() else {
        return Ok(false);
    };
    if access_key_id.trim().is_empty() || secret_access_key.trim().is_empty() {
        return Ok(false);
    }

    let mut explicit_keys = Command::new("aws");
    explicit_keys
        .env("AWS_ACCESS_KEY_ID", access_key_id.trim())
        .env("AWS_SECRET_ACCESS_KEY", secret_access_key.trim())
        .args([
            "sts",
            "get-caller-identity",
            "--output",
            "json",
            "--region",
            "us-east-1",
        ]);
    Ok(explicit_keys.status().is_ok_and(|status| status.success()))
}
