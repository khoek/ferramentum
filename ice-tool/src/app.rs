use std::path::Path;
use std::process::{Command, ExitCode};

use crate::cli::{
    Cli, Commands, ConfigArgs, ConfigCommands, ConfigGetArgs, ConfigSetArgs, ConfigUnsetArgs,
    DeployArgs, LoginArgs,
};
use crate::config_store::{
    get_config_value, load_config, normalize_config_key, parse_key_value_pair, save_config,
    set_config_value, supported_config_keys, unset_config_value,
};
use crate::local::detect_local_container_runtime;
use crate::model::{Cloud, IceConfig, LoginMethod, LoginOutcome, OfferDecision};
use crate::providers::{
    CloudInstance, CloudProvider, CreateProvider, MarketCreateProvider, aws, gcp, local, vast,
};
use crate::provision::{
    build_accept_prompt, build_search_requirements, ensure_default_create_config,
    estimate_runtime_cost, find_cheapest_cloud_machine, load_gpu_options,
    print_machine_candidate_summary, prompt_adjust_search_filters, prompt_create_search_filters,
    prompt_offer_decision,
};
use crate::support::{
    ensure_command_available, ensure_provider_cli_installed, maybe_open_browser, nonempty_string,
    prompt_confirm, prompt_theme, require_interactive, resolve_cloud, spinner,
};
use crate::ui::{print_big_red_error, print_stage};
use crate::workload::{InstanceWorkload, resolve_deploy_hours, resolve_deploy_workload};
use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use dialoguer::{Input, Password};

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
    let cli = Cli::parse();
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
        Commands::Deploy(args) => cmd_deploy(args, &mut config)?,
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
    eprintln!("Set `{key}` = {rendered} ({})", path.display());
    Ok(())
}

fn cmd_config_unset(args: ConfigUnsetArgs, config: &mut IceConfig) -> Result<()> {
    let key = normalize_config_key(&args.key)?;
    unset_config_value(config, &key)?;
    let path = save_config(config)?;
    eprintln!("Unset `{key}` ({})", path.display());
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

fn cmd_deploy(args: DeployArgs, config: &mut IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    match cloud {
        Cloud::VastAi => deploy_for::<vast::Provider>(config, &args),
        Cloud::Local => deploy_for::<local::Provider>(config, &args),
        Cloud::Gcp => deploy_market_machine::<gcp::Provider>(config, &args),
        Cloud::Aws => deploy_market_machine::<aws::Provider>(config, &args),
    }
}

fn deploy_for<P: CreateProvider>(config: &mut IceConfig, args: &DeployArgs) -> Result<()> {
    P::ensure_cli()?;
    P::create(config, args)
}

fn deploy_market_machine<P: MarketCreateProvider>(
    config: &mut IceConfig,
    args: &DeployArgs,
) -> Result<()>
where
    <P as CloudProvider>::Instance: CloudInstance,
{
    P::ensure_cli()?;
    let gpu_options = load_gpu_options(P::CLOUD, None);
    ensure_default_create_config(config, P::CLOUD, &gpu_options)?;
    let hours = resolve_deploy_hours(config, args.hours)?;
    let workload = resolve_deploy_workload(&args.target_request())?;
    let mut search = build_search_requirements(config, P::CLOUD)?;
    if args.custom {
        prompt_create_search_filters(&mut search, &gpu_options)?;
    }

    let candidate = loop {
        let candidate =
            find_cheapest_cloud_machine(P::CLOUD, config, &search, args.machine.as_deref())?;
        let cost = estimate_runtime_cost(P::CLOUD, candidate.hourly_usd, hours)?;
        print_machine_candidate_summary(P::CLOUD, &candidate, &cost, &search);

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
                "Dry run: cheapest matching machine is {} in {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before deploy.",
                candidate.machine,
                candidate.region,
                candidate.hourly_usd,
                cost.total_usd,
                cost.billed_hours,
                cost.requested_hours
            );
            return Ok(());
        }

        match prompt_offer_decision(&build_accept_prompt(&cost))? {
            OfferDecision::ChangeFilter => {
                prompt_adjust_search_filters(&mut search, &load_gpu_options(P::CLOUD, None))?;
            }
            OfferDecision::Reject => {
                println!("Aborted.");
                return Ok(());
            }
            OfferDecision::Accept => break candidate,
        }
    };

    let instance = P::create_machine(config, &candidate, hours, &workload)?;
    if let InstanceWorkload::Unpack(source) = &workload {
        P::deploy_unpack(config, &instance, source)?;
        if prompt_confirm("Follow unpack logs now?", true)? {
            print_stage("Following unpack logs");
            P::stream_unpack_logs(config, &instance, 200, true)?;
        } else {
            println!(
                "Use `ice logs --cloud {} {} --follow` to inspect stdout/stderr.",
                P::CLOUD,
                instance.display_name()
            );
        }
    } else if prompt_confirm("Open shell in the new instance now?", true)? {
        print_stage("Opening shell");
        P::open_instance_shell(config, &instance)?;
    }
    Ok(())
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
            Err(err) => eprintln!("Stored vast.ai API key is invalid: {err:#}"),
        }
    }

    require_interactive("`ice login --cloud vast.ai` requires interactive stdin.")?;
    let key_page = "https://cloud.vast.ai/manage-keys/";
    eprintln!("Open {key_page}, copy/create an API key, then paste it below.");
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
    eprintln!(
        "Could not auto-detect GCP credentials. Provide a service-account JSON path, or run `gcloud auth login` and retry."
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
    eprintln!("Could not auto-detect AWS credentials. Enter an access key pair.");

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
    if include_cached
        && let Some(project) = config.auth.gcp.project.as_deref()
        && !project.trim().is_empty()
    {
        return Some(project.trim().to_owned());
    }
    for env_key in [
        "CLOUDSDK_CORE_PROJECT",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
    ] {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    let mut command = Command::new("gcloud");
    command.args(["config", "get-value", "project", "--quiet"]);
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("(unset)") {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn detect_gcp_credentials_path(config: &IceConfig, include_cached: bool) -> Option<String> {
    let mut candidates = Vec::new();
    if include_cached && let Some(path) = config.auth.gcp.service_account_json.as_deref() {
        candidates.push(path.to_owned());
    }
    if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        && !path.trim().is_empty()
    {
        candidates.push(path);
    }
    if let Some(home) = dirs::home_dir() {
        candidates.push(
            home.join(".config/gcloud/application_default_credentials.json")
                .display()
                .to_string(),
        );
    }

    candidates
        .into_iter()
        .find(|candidate| Path::new(candidate).is_file())
}

fn gcp_has_active_account() -> Result<bool> {
    let mut command = Command::new("gcloud");
    command.args([
        "auth",
        "list",
        "--filter=status:ACTIVE",
        "--format=value(account)",
    ]);
    let output = command
        .output()
        .context("Failed to run `gcloud auth list` for credential detection")?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| !line.trim().is_empty()))
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
