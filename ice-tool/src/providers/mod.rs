use std::fs;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};

use crate::cache::CloudCacheModel;
use crate::cli::{CreateArgs, LogsArgs, PullArgs, PushArgs, ShellArgs};
use crate::listing::ListedInstance;
use crate::model::{Cloud, CloudMachineCandidate, IceConfig};
use crate::support::{VAST_WAIT_TIMEOUT_SECS, ensure_provider_cli_installed, prompt_confirm};
use crate::ui::print_stage;
use crate::unpack::{
    materialize_unpack_bundle, unpack_logs_remote_command, unpack_start_remote_command,
};
use crate::workload::{InstanceWorkload, display_unpack_source};

pub(crate) mod aws;
pub(crate) mod catalog;
pub(crate) mod gcp;
pub(crate) mod local;
pub(crate) mod vast;

pub(crate) fn load_cached_arc<T, F>(
    cache: &LazyLock<Mutex<Option<Arc<T>>>>,
    load: F,
    cache_name: &str,
) -> Result<Arc<T>>
where
    T: Send + Sync + 'static,
    F: FnOnce() -> Result<T>,
{
    if let Some(value) = cache
        .lock()
        .map_err(|_| anyhow!("{cache_name} cache mutex is poisoned."))?
        .as_ref()
        .cloned()
    {
        return Ok(value);
    }

    let value = Arc::new(load()?);
    let mut guard = cache
        .lock()
        .map_err(|_| anyhow!("{cache_name} cache mutex is poisoned."))?;
    if let Some(existing) = guard.as_ref() {
        return Ok(existing.clone());
    }
    *guard = Some(value.clone());
    Ok(value)
}

pub(crate) fn clear_cached_arc<T>(
    cache: &LazyLock<Mutex<Option<Arc<T>>>>,
    cache_name: &str,
) -> Result<()>
where
    T: Send + Sync + 'static,
{
    let mut guard = cache
        .lock()
        .map_err(|_| anyhow!("{cache_name} cache mutex is poisoned."))?;
    *guard = None;
    Ok(())
}

pub(crate) trait CloudInstance {
    type ListContext;

    fn cache_key(&self) -> String;
    fn display_name(&self) -> String;
    fn state_value(&self) -> &str;
    fn is_running(&self) -> bool;
    fn is_stopped(&self) -> bool;
    fn workload(&self) -> Option<&InstanceWorkload>;
    fn render(&self, context: &Self::ListContext, pending_context: bool) -> ListedInstance;
}

pub(crate) trait CloudProvider: Sized {
    type Instance: CloudInstance;
    type ProviderContext<'a>
    where
        Self: 'a;

    const CLOUD: Cloud;

    fn context<'a>(config: &'a IceConfig) -> Result<Self::ProviderContext<'a>>;
    fn list_instances(
        context: &Self::ProviderContext<'_>,
        on_progress: &mut dyn FnMut(String),
    ) -> Result<Vec<Self::Instance>>;
    fn sort_instances(instances: &mut [Self::Instance]);
    fn resolve_instance(
        context: &Self::ProviderContext<'_>,
        identifier: &str,
    ) -> Result<Self::Instance>;
    fn set_running(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
    ) -> Result<()>;
    fn wait_for_running_state(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
        timeout: Duration,
    ) -> Result<Self::Instance>;
    fn delete_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<()>;

    fn initial_loading_message() -> String {
        format!("Loading {} instance state...", Self::CLOUD)
    }
}

pub(crate) trait RemoteCloudProvider: CloudProvider {
    type CacheModel: CloudCacheModel<
            Instance = Self::Instance,
            ListContext = <Self::Instance as CloudInstance>::ListContext,
        >;

    fn list_context_loading_message() -> Option<String> {
        None
    }

    fn resolve_list_context(
        _context: &Self::ProviderContext<'_>,
        _instances: &[Self::Instance],
        _on_progress: &mut dyn FnMut(String),
    ) -> Result<<Self::Instance as CloudInstance>::ListContext>
    where
        <Self::Instance as CloudInstance>::ListContext: Default,
    {
        let context: <Self::Instance as CloudInstance>::ListContext = Default::default();
        Ok(context)
    }
}

pub(crate) trait CommandProvider: CloudProvider {
    fn ensure_cli() -> Result<()> {
        if Self::CLOUD != Cloud::VastAi {
            ensure_provider_cli_installed(Self::CLOUD)?;
        }
        Ok(())
    }

    fn logs(config: &IceConfig, args: &LogsArgs) -> Result<()>;
    fn shell(config: &IceConfig, args: &ShellArgs) -> Result<()>;
    fn pull(config: &IceConfig, args: &PullArgs) -> Result<()>;
    fn push(config: &IceConfig, args: &PushArgs) -> Result<()>;
}

pub(crate) trait CreateProvider: CommandProvider {
    fn create(config: &mut IceConfig, args: &CreateArgs) -> Result<()>;
}

pub(crate) trait RemoteSshProvider: RemoteCloudProvider {
    fn create_machine(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
        hours: f64,
        workload: &InstanceWorkload,
    ) -> Result<Self::Instance>;

    fn wait_for_ssh_ready(
        config: &IceConfig,
        instance: &Self::Instance,
        timeout: Duration,
    ) -> Result<()>;
    fn shell_connect_command(config: &IceConfig, instance: &Self::Instance) -> Result<String>;
    fn open_instance_shell(config: &IceConfig, instance: &Self::Instance) -> Result<()>;
    fn pull_from_instance(
        config: &IceConfig,
        instance: &Self::Instance,
        remote_path: &str,
        local_path: Option<&Path>,
    ) -> Result<()>;
    fn push_to_instance(
        config: &IceConfig,
        instance: &Self::Instance,
        local_path: &Path,
        remote_path: Option<&str>,
    ) -> Result<()>;
    fn upload_unpack_bundle(
        config: &IceConfig,
        instance: &Self::Instance,
        bundle_root: &Path,
        remote_dir: &str,
    ) -> Result<()>;
    fn run_ssh_command(
        config: &IceConfig,
        instance: &Self::Instance,
        command: &str,
        allocate_tty: bool,
    ) -> Result<()>;
    fn remote_unpack_dir(instance: &Self::Instance) -> String;
    fn refresh_machine_offer(
        _config: &IceConfig,
        candidate: &CloudMachineCandidate,
    ) -> Result<CloudMachineCandidate> {
        Ok(candidate.clone())
    }

    fn stream_unpack_logs(
        config: &IceConfig,
        instance: &Self::Instance,
        tail: u32,
        follow: bool,
    ) -> Result<()> {
        Self::run_ssh_command(
            config,
            instance,
            &unpack_logs_remote_command(&Self::remote_unpack_dir(instance), tail, follow),
            false,
        )
    }

    fn deploy_unpack(config: &IceConfig, instance: &Self::Instance, source: &str) -> Result<()> {
        print_stage(&format!(
            "Materializing unpack bundle from {}",
            display_unpack_source(source)
        ));
        let bundle = materialize_unpack_bundle(config, source)?;
        let remote_dir = Self::remote_unpack_dir(instance);
        let result = (|| {
            print_stage("Waiting for SSH access");
            Self::wait_for_ssh_ready(
                config,
                instance,
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            print_stage("Uploading unpack bundle");
            Self::upload_unpack_bundle(config, instance, &bundle.root, &remote_dir)?;
            print_stage("Starting unpack workload");
            Self::run_ssh_command(
                config,
                instance,
                &unpack_start_remote_command(&remote_dir),
                false,
            )
        })();
        let _ = fs::remove_dir_all(&bundle.root);
        result
    }
}

impl<T: RemoteSshProvider> CommandProvider for T {
    fn logs(config: &IceConfig, args: &LogsArgs) -> Result<()> {
        let context = T::context(config)?;
        let instance = T::resolve_instance(&context, &args.instance)?;
        if !matches!(instance.workload(), Some(InstanceWorkload::Unpack(_))) {
            bail!(
                "`ice logs --cloud {}` is only implemented for `unpack` workloads.",
                T::CLOUD
            );
        }
        if args.filter.is_some() || args.daemon {
            bail!("`ice logs` filter/daemon flags are not supported for `unpack` workloads.");
        }
        T::stream_unpack_logs(config, &instance, args.tail, args.follow)
    }

    fn shell(config: &IceConfig, args: &ShellArgs) -> Result<()> {
        if args.preserve_ephemeral {
            bail!(
                "`ice shell --cloud {}` does not support `--preserve-ephemeral`.",
                T::CLOUD
            );
        }
        let context = T::context(config)?;
        let mut instance = T::resolve_instance(&context, &args.instance)?;
        if instance.is_stopped() {
            if !prompt_confirm("Instance is stopped. Start it before opening shell?", true)? {
                bail!("Aborted: instance is stopped.");
            }
            T::set_running(&context, &instance, true)?;
            instance = T::wait_for_running_state(
                &context,
                &instance,
                true,
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
        }
        T::wait_for_ssh_ready(
            config,
            &instance,
            Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
        )?;
        if args.print_creds {
            println!("{}", T::shell_connect_command(config, &instance)?);
            return Ok(());
        }
        T::open_instance_shell(config, &instance)
    }

    fn pull(config: &IceConfig, args: &PullArgs) -> Result<()> {
        let context = T::context(config)?;
        let instance = T::resolve_instance(&context, &args.instance)?;
        if !instance.is_running() {
            bail!(
                "Instance `{}` is not running (state: {}).",
                instance.display_name(),
                instance.state_value()
            );
        }
        T::pull_from_instance(
            config,
            &instance,
            &args.remote_path,
            args.local_path.as_deref(),
        )
    }

    fn push(config: &IceConfig, args: &PushArgs) -> Result<()> {
        if !args.local_path.exists() {
            bail!("Local path `{}` does not exist.", args.local_path.display());
        }
        let context = T::context(config)?;
        let instance = T::resolve_instance(&context, &args.instance)?;
        if !instance.is_running() {
            bail!(
                "Instance `{}` is not running (state: {}).",
                instance.display_name(),
                instance.state_value()
            );
        }
        T::push_to_instance(
            config,
            &instance,
            args.local_path.as_path(),
            args.remote_path.as_deref(),
        )
    }
}

pub(crate) trait MarketCreateProvider: CommandProvider {
    fn create_machine(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
        hours: f64,
        workload: &InstanceWorkload,
    ) -> Result<Self::Instance>;

    fn deploy_unpack(config: &IceConfig, instance: &Self::Instance, source: &str) -> Result<()>;
    fn stream_unpack_logs(
        config: &IceConfig,
        instance: &Self::Instance,
        tail: u32,
        follow: bool,
    ) -> Result<()>;
    fn open_instance_shell(config: &IceConfig, instance: &Self::Instance) -> Result<()>;
    fn refresh_machine_offer(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
    ) -> Result<CloudMachineCandidate>;
}

impl<T: RemoteSshProvider> MarketCreateProvider for T {
    fn create_machine(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
        hours: f64,
        workload: &InstanceWorkload,
    ) -> Result<Self::Instance> {
        <T as RemoteSshProvider>::create_machine(config, candidate, hours, workload)
    }

    fn deploy_unpack(config: &IceConfig, instance: &Self::Instance, source: &str) -> Result<()> {
        <T as RemoteSshProvider>::deploy_unpack(config, instance, source)
    }

    fn stream_unpack_logs(
        config: &IceConfig,
        instance: &Self::Instance,
        tail: u32,
        follow: bool,
    ) -> Result<()> {
        <T as RemoteSshProvider>::stream_unpack_logs(config, instance, tail, follow)
    }

    fn open_instance_shell(config: &IceConfig, instance: &Self::Instance) -> Result<()> {
        <T as RemoteSshProvider>::open_instance_shell(config, instance)
    }

    fn refresh_machine_offer(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
    ) -> Result<CloudMachineCandidate> {
        <T as RemoteSshProvider>::refresh_machine_offer(config, candidate)
    }
}
