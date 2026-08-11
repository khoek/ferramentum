use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result, bail};
use auc_tool::application::{ApplicationServer, AucApplication, LocalSessionAuthorizer};
use auc_tool::authenticator::PresenceGate;
use auc_tool::product::{application_agent_info, managed_product};
use auc_tool::release::AucReleaseSource;
use auc_tool::transport::run_uhid;
use auc_tool::vault::Vault;
use capulus::managed::{
    ActivatedListeners, AgentInfo, JobId, ManagedAgent, ManagementServer, ManagementServerOptions,
    RedeployWorker,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "auc-agent", version, about = "Privileged auc system agent")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve,
    InstallationManifest,
    Install {
        #[arg(long)]
        operator_uid: u32,
    },
    Uninstall {
        #[arg(long)]
        operator_uid: u32,
        #[arg(long)]
        purge_vault: bool,
    },
    RedeployWorker {
        #[arg(long)]
        job: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve => {
            require_root()?;
            let listeners = ActivatedListeners::from_environment(&["application", "capulus"])
                .context("failed to adopt auc systemd sockets")?;
            runtime()?.block_on(serve(listeners))
        }
        Command::InstallationManifest => {
            println!("{}", managed_product()?.installation_manifest().to_json()?);
            Ok(())
        }
        Command::Install { operator_uid } => {
            require_root()?;
            runtime()?.block_on(auc_tool::system::install(operator_uid))
        }
        Command::Uninstall {
            operator_uid,
            purge_vault,
        } => {
            require_root()?;
            runtime()?.block_on(auc_tool::system::uninstall(operator_uid, purge_vault))
        }
        Command::RedeployWorker { job } => {
            require_root()?;
            let job = JobId::parse(&job)?;
            runtime()?.block_on(redeploy_worker(job))
        }
    }
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("auc-agent")
        .build()
        .context("failed to create the auc async runtime")
}

async fn serve(mut listeners: ActivatedListeners) -> Result<()> {
    let application_listener = listeners.take_tokio("application")?;
    let management_listener = listeners.take_tokio("capulus")?;
    if !listeners.is_empty() {
        bail!("auc-agent retained an unexpected systemd listener");
    }
    let product = Arc::new(managed_product()?);
    let vault = Vault::open()?;
    let presence = PresenceGate::new();
    let device_present = Arc::new(AtomicBool::new(false));
    let application = Arc::new(AucApplication::new(
        vault.clone(),
        presence.clone(),
        Arc::clone(&device_present),
        LocalSessionAuthorizer::connect().await?,
    ));
    let management = Arc::new(ManagedAgent::new(
        Arc::clone(&product),
        Arc::new(AucReleaseSource::new()?),
    )?);
    let application_server = ApplicationServer::new(application_listener, application);
    let management_server = ManagementServer::new(
        management_listener,
        management,
        ManagementServerOptions::default(),
    )?;
    let transport = tokio::task::spawn_blocking(move || run_uhid(vault, presence, device_present));

    tokio::select! {
        result = application_server.run() => result.context("auc application server stopped"),
        result = management_server.run() => result.context("auc management server stopped"),
        result = transport => result
            .context("auc UHID task panicked")?
            .context("auc UHID transport stopped"),
    }
}

async fn redeploy_worker(job: JobId) -> Result<()> {
    let product = Arc::new(managed_product()?);
    RedeployWorker::new(Arc::clone(&product))?
        .run(job, move || application_health(&product))
        .await
}

fn application_health(product: &capulus::managed::ManagedProduct) -> Result<AgentInfo> {
    application_agent_info()
        .with_context(|| format!("{} application health check failed", product.name()))
}

fn require_root() -> Result<()> {
    if rustix::process::geteuid().is_root() {
        Ok(())
    } else {
        bail!("auc-agent must run as root")
    }
}
