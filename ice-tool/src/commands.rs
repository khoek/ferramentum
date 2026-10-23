use std::time::Duration;

use anyhow::{Result, bail};
use serde_json::json;

use crate::cache::remove_instance;
use crate::cli::{InstanceArgs, LogsArgs, PullArgs, PushArgs, ShellArgs};
use crate::lifecycle::{Action, Budget};
use crate::model::{Cloud, IceConfig};
use crate::providers::{
    CloudInstance, CloudProvider, CommandProvider, RemoteCloudProvider, aws, gcp, local, vast,
    verda,
};
use crate::support::{ensure_provider_cli_installed, resolve_cloud};

pub(crate) fn cmd_logs(args: LogsArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if args.provider_logs && cloud != Cloud::VastAi {
        return Err(crate::automation::error(
            "invalid_arguments",
            "--provider-logs is currently supported only for vast.ai.",
            json!({"cloud": cloud}),
        ));
    }
    match cloud {
        Cloud::Verda => run_logs::<verda::Provider>(config, &args),
        Cloud::VastAi => run_logs::<vast::Provider>(config, &args),
        Cloud::Gcp => run_logs::<gcp::Provider>(config, &args),
        Cloud::Aws => run_logs::<aws::Provider>(config, &args),
        Cloud::Local => run_logs::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_shell(args: ShellArgs, config: &IceConfig) -> Result<()> {
    if crate::output::is_json() && !args.print_creds {
        bail!("`shell --json` requires `--print-creds`; interactive shells need a terminal.");
    }
    let cloud = resolve_cloud(args.cloud, config)?;
    if args.no_probe && !matches!(cloud, Cloud::VastAi | Cloud::Verda) {
        return Err(crate::automation::error(
            "invalid_arguments",
            "--no-probe is supported only for vast.ai and verda.",
            json!({"cloud": cloud, "flags": ["--no-probe"]}),
        ));
    }
    match cloud {
        Cloud::Verda => run_shell::<verda::Provider>(config, &args),
        Cloud::VastAi => run_shell::<vast::Provider>(config, &args),
        Cloud::Gcp => run_shell::<gcp::Provider>(config, &args),
        Cloud::Aws => run_shell::<aws::Provider>(config, &args),
        Cloud::Local => run_shell::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_pull(args: PullArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::Verda => run_pull::<verda::Provider>(config, &args),
        Cloud::VastAi => run_pull::<vast::Provider>(config, &args),
        Cloud::Gcp => run_pull::<gcp::Provider>(config, &args),
        Cloud::Aws => run_pull::<aws::Provider>(config, &args),
        Cloud::Local => run_pull::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_push(args: PushArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::Verda => run_push::<verda::Provider>(config, &args),
        Cloud::VastAi => run_push::<vast::Provider>(config, &args),
        Cloud::Gcp => run_push::<gcp::Provider>(config, &args),
        Cloud::Aws => run_push::<aws::Provider>(config, &args),
        Cloud::Local => run_push::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_stop(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let _budget = Budget::enter(Duration::from_secs(args.timeout))?;
    match resolve_cloud(args.cloud, config)? {
        Cloud::Verda => cmd_stop_cloud::<verda::Provider>(config, &args.instance),
        Cloud::VastAi => cmd_stop_cloud::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_stop_cloud::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_stop_cloud::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_stop_cloud::<local::Provider>(config, &args.instance),
    }
}

pub(crate) fn cmd_start(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let _budget = Budget::enter(Duration::from_secs(args.timeout))?;
    match resolve_cloud(args.cloud, config)? {
        Cloud::Verda => cmd_start_cloud::<verda::Provider>(config, &args.instance),
        Cloud::VastAi => cmd_start_cloud::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_start_cloud::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_start_cloud::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_start_cloud::<local::Provider>(config, &args.instance),
    }
}

pub(crate) fn cmd_delete(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let _budget = Budget::enter(Duration::from_secs(args.timeout))?;
    match resolve_cloud(args.cloud, config)? {
        Cloud::Verda => verda::delete(config, &args.instance),
        Cloud::VastAi => cmd_delete_remote::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_delete_remote::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_delete_remote::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_delete_local::<local::Provider>(config, &args.instance),
    }
}

fn run_logs<P: CommandProvider>(config: &IceConfig, args: &LogsArgs) -> Result<()> {
    P::ensure_cli()?;
    crate::output::begin_logs(P::CLOUD, &args.instance);
    P::logs(config, args)?;
    if crate::output::is_json() {
        crate::output::log_event(json!({"event": "complete"}))?;
    }
    Ok(())
}

fn run_shell<P: CommandProvider>(config: &IceConfig, args: &ShellArgs) -> Result<()> {
    P::ensure_cli()?;
    P::shell(config, args)
}

fn run_pull<P: CommandProvider>(config: &IceConfig, args: &PullArgs) -> Result<()> {
    P::ensure_cli()?;
    P::pull(config, args)?;
    if crate::output::is_json() {
        crate::output::emit(
            "pull",
            P::CLOUD,
            json!({
                "status": "completed", "instance": args.instance,
                "remote_path": args.remote_path,
                "local_path": args.local_path.as_deref().unwrap_or(std::path::Path::new(".")),
            }),
        )?;
    }
    Ok(())
}

fn run_push<P: CommandProvider>(config: &IceConfig, args: &PushArgs) -> Result<()> {
    P::ensure_cli()?;
    P::push(config, args)?;
    if crate::output::is_json() {
        crate::output::emit(
            "push",
            P::CLOUD,
            json!({
                "status": "completed", "instance": args.instance,
                "local_path": args.local_path,
                "remote_path": args.remote_path.as_deref().unwrap_or("."),
            }),
        )?;
    }
    Ok(())
}

fn cmd_stop_cloud<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    cmd_transition::<P>(config, identifier, Action::Stop).map(|_| ())
}
fn cmd_start_cloud<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    cmd_transition::<P>(config, identifier, Action::Start).map(|_| ())
}
fn cmd_delete_remote<P: RemoteCloudProvider>(config: &IceConfig, identifier: &str) -> Result<()>
where
    <P::Instance as CloudInstance>::ListContext: Default,
{
    let instance = cmd_transition::<P>(config, identifier, Action::Delete)?;
    remove_instance::<P::CacheModel>(&instance);
    Ok(())
}
fn cmd_delete_local<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    cmd_transition::<P>(config, identifier, Action::Delete).map(|_| ())
}
fn cmd_transition<P: CloudProvider>(
    config: &IceConfig,
    identifier: &str,
    action: Action,
) -> Result<P::Instance> {
    crate::automation::recovery(
        "resolving_instance",
        json!({"action":action, "instance_identifier":identifier, "request":"not_sent"}),
    );
    ensure_cli::<P>()?;
    let context = P::context(config)?;
    let instance = P::resolve_instance(&context, identifier)?;
    let result = crate::lifecycle::transition::<P>(&context, &instance, action)?;
    if crate::output::is_json() {
        crate::output::emit(action.command(), P::CLOUD, result)?;
    } else {
        println!(
            "{}: {} (verified)",
            instance.display_name(),
            result["state"].as_str().unwrap_or("unknown")
        );
        if let Some(note) = result["billing_note"].as_str() {
            eprintln!("{note}");
        }
    }
    Ok(instance)
}

fn ensure_cli<P: CloudProvider>() -> Result<()> {
    if P::CLOUD != Cloud::VastAi {
        ensure_provider_cli_installed(P::CLOUD)?;
    }
    Ok(())
}
