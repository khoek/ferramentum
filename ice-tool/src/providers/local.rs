use std::time::Duration;

use anyhow::{Result, bail};

use crate::cli::{DeployArgs, DownloadArgs, LogsArgs, ShellArgs};
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color, listed_instance,
    present_field, push_field, show_health_field,
};
use crate::local::{
    LocalContext, LocalInstance, local_backend_display, local_context, local_delete_instance,
    local_describe_instance, local_download, local_list_instances, local_open_shell,
    local_start_instance, local_stop_instance, local_stream_logs, local_workload_display,
    resolve_local_instance,
};
use crate::model::{Cloud, IceConfig};
use crate::providers::{CloudInstance, CloudProvider, CommandProvider, CreateProvider};
use crate::support::{prompt_confirm, truncate_ellipsis, visible_instance_name};
use crate::workload::{
    InstanceWorkload, display_unpack_source, resolve_deploy_hours, resolve_deploy_workload,
    workload_display_value,
};

pub(crate) struct Provider;

impl CloudInstance for LocalInstance {
    type ListContext = LocalContext;

    fn cache_key(&self) -> String {
        self.name.clone()
    }

    fn display_name(&self) -> String {
        visible_instance_name(&self.name).to_owned()
    }

    fn state_value(&self) -> &str {
        &self.state
    }

    fn is_running(&self) -> bool {
        LocalInstance::is_running(self)
    }

    fn is_stopped(&self) -> bool {
        LocalInstance::is_stopped(self)
    }

    fn workload(&self) -> Option<&InstanceWorkload> {
        self.workload.as_ref()
    }

    fn render(&self, context: &Self::ListContext, _pending_context: bool) -> ListedInstance {
        let health = show_health_field(&self.health_hint());
        let state = display_state(&self.state);
        let mut fields = Vec::new();
        push_field(&mut fields, health.clone());
        fields.push(format!("{:.2}h", self.runtime_hours()));
        push_field(
            &mut fields,
            self.remaining_hours()
                .map(|value| format!("rem {value:.2}h")),
        );
        fields.push(local_backend_display(context, self));

        let mut detail_fields = vec![format!(
            "{}://{}",
            local_backend_display(context, self),
            self.name
        )];
        push_field(
            &mut detail_fields,
            present_field(&local_workload_display(self)),
        );

        listed_instance(
            display_name_or_fallback(&self.name, truncate_ellipsis(&self.id, 12)),
            state.clone(),
            list_state_color(&state, health.as_deref()),
            fields,
            detail_fields,
        )
    }
}

impl CloudProvider for Provider {
    type Instance = LocalInstance;
    type ProviderContext<'a> = LocalContext;

    const CLOUD: Cloud = Cloud::Local;

    fn context<'a>(_config: &'a IceConfig) -> Result<Self::ProviderContext<'a>> {
        Ok(local_context())
    }

    fn list_instances(
        context: &Self::ProviderContext<'_>,
        on_progress: &mut dyn FnMut(String),
    ) -> Result<Vec<Self::Instance>> {
        on_progress(Self::initial_loading_message());
        local_list_instances(context)
    }

    fn sort_instances(instances: &mut [Self::Instance]) {
        instances.sort_by(|left, right| right.name.cmp(&left.name));
    }

    fn resolve_instance(
        context: &Self::ProviderContext<'_>,
        identifier: &str,
    ) -> Result<Self::Instance> {
        resolve_local_instance(context, identifier)
    }

    fn set_running(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
    ) -> Result<()> {
        if running {
            let _ = local_start_instance(context, instance)?;
            return Ok(());
        }
        local_stop_instance(context, instance)
    }

    fn wait_for_running_state(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
        _timeout: Duration,
    ) -> Result<Self::Instance> {
        let refreshed = local_describe_instance(context, &instance.name)?;
        if refreshed.is_running() == running {
            return Ok(refreshed);
        }
        if refreshed.is_stopped() == !running {
            return Ok(refreshed);
        }
        bail!(
            "Local workload `{}` did not reach the expected running state.",
            refreshed.display_name()
        )
    }

    fn delete_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<()> {
        local_delete_instance(context, instance)
    }
}

impl CommandProvider for Provider {
    fn logs(_config: &IceConfig, args: &LogsArgs) -> Result<()> {
        if args.filter.is_some() || args.daemon {
            bail!("`ice logs --cloud local` does not support `--filter` or `--daemon`.");
        }
        let context = local_context();
        let instance = resolve_local_instance(&context, &args.instance)?;
        local_stream_logs(&context, &instance, args.tail, args.follow)
    }

    fn shell(_config: &IceConfig, args: &ShellArgs) -> Result<()> {
        let context = local_context();
        let mut instance = resolve_local_instance(&context, &args.instance)?;
        if instance.is_stopped()
            && matches!(
                instance.backend,
                crate::local::LocalInstanceBackend::Container
            )
        {
            if !prompt_confirm("Instance is stopped. Start it before opening shell?", true)? {
                bail!("Aborted: instance is stopped.");
            }
            instance = local_start_instance(&context, &instance)?;
        }
        local_open_shell(&context, &instance)
    }

    fn download(config: &IceConfig, args: &DownloadArgs) -> Result<()> {
        let _ = config;
        let context = local_context();
        let instance = resolve_local_instance(&context, &args.instance)?;
        local_download(
            &context,
            &instance,
            &args.remote_path,
            args.local_path.as_deref(),
        )
    }
}

impl CreateProvider for Provider {
    fn create(config: &mut IceConfig, args: &DeployArgs) -> Result<()> {
        if args.custom {
            bail!("`ice deploy --cloud local` does not support `--custom`.");
        }
        if let Some(machine) = args.machine.as_deref()
            && !machine.trim().is_empty()
        {
            bail!("`ice deploy --cloud local` does not support `--machine`.");
        }

        let hours = resolve_deploy_hours(config, args.hours)?;
        let workload = resolve_deploy_workload(&args.target_request())?;
        if matches!(workload, InstanceWorkload::Shell) {
            bail!(
                "`ice deploy --cloud local` requires `--container`, `--unpack`, or `--arca`; there is no host VM for `--ssh`."
            );
        }
        let context = local_context();

        println!();
        println!("Local workload:");
        match &workload {
            InstanceWorkload::Container(container) => {
                println!("  Mode: container");
                println!("  Runtime: {}", context.require_runtime()?.shell_prefix());
                println!("  Image: {}", container.container_ref());
            }
            InstanceWorkload::Unpack(source) => {
                println!("  Mode: unpack");
                println!("  Source: {}", display_unpack_source(source));
                if let Some(runtime) = context.runtime {
                    println!("  Container runtime: {}", runtime.shell_prefix());
                } else {
                    println!("  Container runtime: none");
                }
            }
            InstanceWorkload::Shell => unreachable!(),
        }
        println!("  Requested runtime: {:.3}h", hours);
        println!();

        if args.dry_run {
            println!(
                "Dry run: would start {} locally for {:.3}h.",
                workload_display_value(Some(&workload)),
                hours
            );
            return Ok(());
        }

        let instance = crate::local::local_create_instance(config, &context, hours, &workload)?;
        if !instance.is_running() {
            println!(
                "Local workload {} is not running after deploy (state: {}).",
                visible_instance_name(&instance.name),
                instance.state
            );
            return Ok(());
        }

        if prompt_confirm("Open shell in the new instance now?", true)? {
            local_open_shell(&context, &instance)?;
        }

        Ok(())
    }
}
