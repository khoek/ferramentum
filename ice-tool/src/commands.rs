use std::time::Duration;

use anyhow::Result;

use crate::cache::remove_instance;
use crate::cli::{InstanceArgs, LogsArgs, PullArgs, PushArgs, ShellArgs};
use crate::model::{Cloud, IceConfig};
use crate::providers::{
    CloudInstance, CloudProvider, CommandProvider, RemoteCloudProvider, aws, gcp, local, vast,
};
use crate::support::{
    VAST_WAIT_TIMEOUT_SECS, ensure_provider_cli_installed, resolve_cloud, spinner,
};

pub(crate) fn cmd_logs(args: LogsArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => run_logs::<vast::Provider>(config, &args),
        Cloud::Gcp => run_logs::<gcp::Provider>(config, &args),
        Cloud::Aws => run_logs::<aws::Provider>(config, &args),
        Cloud::Local => run_logs::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_shell(args: ShellArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => run_shell::<vast::Provider>(config, &args),
        Cloud::Gcp => run_shell::<gcp::Provider>(config, &args),
        Cloud::Aws => run_shell::<aws::Provider>(config, &args),
        Cloud::Local => run_shell::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_pull(args: PullArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => run_pull::<vast::Provider>(config, &args),
        Cloud::Gcp => run_pull::<gcp::Provider>(config, &args),
        Cloud::Aws => run_pull::<aws::Provider>(config, &args),
        Cloud::Local => run_pull::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_push(args: PushArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => run_push::<vast::Provider>(config, &args),
        Cloud::Gcp => run_push::<gcp::Provider>(config, &args),
        Cloud::Aws => run_push::<aws::Provider>(config, &args),
        Cloud::Local => run_push::<local::Provider>(config, &args),
    }
}

pub(crate) fn cmd_stop(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => cmd_stop_cloud::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_stop_cloud::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_stop_cloud::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_stop_cloud::<local::Provider>(config, &args.instance),
    }
}

pub(crate) fn cmd_start(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => cmd_start_cloud::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_start_cloud::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_start_cloud::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_start_cloud::<local::Provider>(config, &args.instance),
    }
}

pub(crate) fn cmd_delete(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    match resolve_cloud(args.cloud, config)? {
        Cloud::VastAi => cmd_delete_remote::<vast::Provider>(config, &args.instance),
        Cloud::Gcp => cmd_delete_remote::<gcp::Provider>(config, &args.instance),
        Cloud::Aws => cmd_delete_remote::<aws::Provider>(config, &args.instance),
        Cloud::Local => cmd_delete_local::<local::Provider>(config, &args.instance),
    }
}

fn run_logs<P: CommandProvider>(config: &IceConfig, args: &LogsArgs) -> Result<()> {
    P::ensure_cli()?;
    P::logs(config, args)
}

fn run_shell<P: CommandProvider>(config: &IceConfig, args: &ShellArgs) -> Result<()> {
    P::ensure_cli()?;
    P::shell(config, args)
}

fn run_pull<P: CommandProvider>(config: &IceConfig, args: &PullArgs) -> Result<()> {
    P::ensure_cli()?;
    P::pull(config, args)
}

fn run_push<P: CommandProvider>(config: &IceConfig, args: &PushArgs) -> Result<()> {
    P::ensure_cli()?;
    P::push(config, args)
}

fn cmd_stop_cloud<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    ensure_cli::<P>()?;
    let context = P::context(config)?;
    let instance = P::resolve_instance(&context, identifier)?;
    let name = instance.display_name();
    if instance.is_stopped() {
        println!("Instance {name} is already stopped.");
        return Ok(());
    }
    P::set_running(&context, &instance, false)?;
    let _ = P::wait_for_running_state(
        &context,
        &instance,
        false,
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )?;
    println!("Stopped instance {name}.");
    Ok(())
}

fn cmd_start_cloud<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    ensure_cli::<P>()?;
    let context = P::context(config)?;
    let instance = P::resolve_instance(&context, identifier)?;
    let name = instance.display_name();
    if instance.is_running() {
        println!("Instance {name} is already running.");
        return Ok(());
    }
    P::set_running(&context, &instance, true)?;
    let _ = P::wait_for_running_state(
        &context,
        &instance,
        true,
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )?;
    println!("Started instance {name}.");
    Ok(())
}

fn cmd_delete_remote<P: RemoteCloudProvider>(config: &IceConfig, identifier: &str) -> Result<()>
where
    <P::Instance as CloudInstance>::ListContext: Default,
{
    ensure_cli::<P>()?;
    let context = P::context(config)?;
    let mut instance = P::resolve_instance(&context, identifier)?;
    if !instance.is_stopped() {
        P::set_running(&context, &instance, false)?;
        instance = P::wait_for_running_state(
            &context,
            &instance,
            false,
            Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
        )?;
    }

    let name = instance.display_name();
    let spinner = spinner(&format!("Deleting instance {name}..."));
    P::delete_instance(&context, &instance)?;
    spinner.finish_with_message("Deleted.");
    remove_instance::<P::CacheModel>(&instance);
    println!("Deleted instance {name}.");
    Ok(())
}

fn cmd_delete_local<P: CloudProvider>(config: &IceConfig, identifier: &str) -> Result<()> {
    ensure_cli::<P>()?;
    let context = P::context(config)?;
    let mut instance = P::resolve_instance(&context, identifier)?;
    if !instance.is_stopped() {
        P::set_running(&context, &instance, false)?;
        instance = P::wait_for_running_state(
            &context,
            &instance,
            false,
            Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
        )?;
    }

    let name = instance.display_name();
    P::delete_instance(&context, &instance)?;
    println!("Deleted instance {name}.");
    Ok(())
}

fn ensure_cli<P: CloudProvider>() -> Result<()> {
    if P::CLOUD != Cloud::VastAi {
        ensure_provider_cli_installed(P::CLOUD)?;
    }
    Ok(())
}
