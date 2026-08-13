use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result, bail};
use capulus::managed::{
    ActivatedListeners, ManagedAgent, ManagementServer, ManagementServerOptions,
};

use crate::application::{ApplicationServer, AucApplication, LocalSessionAuthorizer};
use crate::authenticator::PresenceGate;
use crate::product::{application_agent_info, managed_product};
use crate::release::AucReleaseSource;
use crate::transport::run_uhid;
use crate::vault::Vault;

pub fn run() -> Result<()> {
    require_root()?;
    let listeners = ActivatedListeners::from_environment(&["application", "capulus"])
        .context("failed to adopt auc systemd sockets")?;
    runtime()?.block_on(serve(listeners))
}

pub fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("auc-agent")
        .build()
        .context("failed to create the auc async runtime")
}

pub fn application_health() -> Result<capulus::managed::AgentInfo> {
    let product = managed_product()?;
    application_agent_info()
        .with_context(|| format!("{} application health check failed", product.name()))
}

pub fn require_root() -> Result<()> {
    if rustix::process::geteuid().is_root() {
        Ok(())
    } else {
        bail!("auc agent operations must run as root")
    }
}

async fn serve(mut listeners: ActivatedListeners) -> Result<()> {
    let application_listener = listeners.take_tokio("application")?;
    let management_listener = listeners.take_tokio("capulus")?;
    if !listeners.is_empty() {
        bail!("auc agent retained an unexpected systemd listener");
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
