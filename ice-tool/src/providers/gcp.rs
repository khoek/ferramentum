use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color,
    listed_instance as base_listed_instance, present_field, push_field,
};
use crate::model::{Cloud, CloudMachineCandidate, IceConfig, RegistryAuth};
use crate::providers::{CloudInstance, CloudProvider, RemoteCloudProvider, RemoteSshProvider};
use crate::provision::{estimated_machine_hourly_price, short_gcp_zone};
use crate::support::{
    GCP_CLOUD_PLATFORM_SCOPE, GCP_CONTAINER_IMAGE_FAMILY, GCP_CONTAINER_IMAGE_PROJECT,
    ICE_LABEL_PREFIX, ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, VAST_POLL_INTERVAL_SECS,
    VAST_WAIT_TIMEOUT_SECS, build_cloud_instance_name, elapsed_hours_from_rfc3339, elapsed_since,
    prefix_lookup_indices, run_command_json, run_command_status, run_command_text, spinner,
    visible_instance_name, write_temp_file,
};
use crate::unpack::{
    pack_directory_as_tar, remote_unpack_dir_for_gcp, unpack_prepare_remote_dir_command,
    unpack_shell_remote_command,
};
use crate::workload::{
    InstanceWorkload, build_linux_startup_script, gcp_workload_metadata_arg,
    instance_shell_remote_command, parse_workload_metadata, registry_auth_for_workload,
    workload_display_value, wrap_remote_shell_script,
};

#[derive(Debug, Clone)]
pub(crate) struct GcpInstance {
    pub(crate) name: String,
    pub(crate) zone: String,
    pub(crate) status: String,
    pub(crate) machine_type: String,
    pub(crate) creation_timestamp: Option<String>,
    pub(crate) last_start_timestamp: Option<String>,
    pub(crate) workload: Option<InstanceWorkload>,
}

pub(crate) struct Provider;
pub(crate) struct CacheModel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    pub(crate) name: String,
    pub(crate) zone: String,
    #[serde(default)]
    pub(crate) listed: Option<ListedInstance>,
    #[serde(default)]
    pub(crate) observed_at_unix: Option<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct CacheStore {
    #[serde(default)]
    pub(crate) entries: Vec<CacheEntry>,
}

impl GcpInstance {
    pub(crate) fn is_running(&self) -> bool {
        self.status.eq_ignore_ascii_case("RUNNING")
    }

    pub(crate) fn is_stopped(&self) -> bool {
        matches!(
            self.status.as_str(),
            "TERMINATED" | "STOPPING" | "SUSPENDED" | "SUSPENDING"
        )
    }

    pub(crate) fn runtime_hours(&self) -> f64 {
        if !self.is_running() {
            return 0.0;
        }
        if let Some(last_start) = self.last_start_timestamp.as_deref()
            && let Some(hours) = elapsed_hours_from_rfc3339(last_start)
        {
            return hours;
        }
        if let Some(created) = self.creation_timestamp.as_deref()
            && let Some(hours) = elapsed_hours_from_rfc3339(created)
        {
            return hours;
        }
        0.0
    }
}

impl CloudInstance for GcpInstance {
    type ListContext = ();

    fn cache_key(&self) -> String {
        format!("{}/{}", self.zone, self.name)
    }

    fn display_name(&self) -> String {
        visible_instance_name(&self.name).to_owned()
    }

    fn state_value(&self) -> &str {
        &self.status
    }

    fn is_running(&self) -> bool {
        self.is_running()
    }

    fn is_stopped(&self) -> bool {
        self.is_stopped()
    }

    fn workload(&self) -> Option<&InstanceWorkload> {
        self.workload.as_ref()
    }

    fn render(&self, _context: &Self::ListContext, _pending_context: bool) -> ListedInstance {
        let health = self.is_running().then_some("ok".to_owned());
        let state = display_state(&self.status);
        let mut fields = Vec::new();
        push_field(&mut fields, health.clone());
        fields.push(format!("{:.2}h", self.runtime_hours()));
        push_field(
            &mut fields,
            estimated_machine_hourly_price(Cloud::Gcp, &self.machine_type)
                .map(|value| format!("${value:.4}/hr")),
        );
        fields.push(
            self.machine_type
                .rsplit('/')
                .next()
                .unwrap_or("-")
                .to_owned(),
        );
        fields.push(short_gcp_zone(&self.zone));

        let mut detail_fields = vec![format!(
            "gcp://{}/{}",
            short_gcp_zone(&self.zone),
            self.name
        )];
        push_field(
            &mut detail_fields,
            present_field(&workload_display_value(self.workload.as_ref())),
        );

        base_listed_instance(
            display_name_or_fallback(&self.name, self.name.clone()),
            state.clone(),
            list_state_color(&state, health.as_deref()),
            fields,
            detail_fields,
        )
    }
}

impl CloudCacheModel for CacheModel {
    type Instance = GcpInstance;
    type ListContext = ();
    type Entry = CacheEntry;
    type Store = CacheStore;

    const CLOUD: Cloud = Cloud::Gcp;

    fn entries(store: &Self::Store) -> &[Self::Entry] {
        &store.entries
    }

    fn entries_mut(store: &mut Self::Store) -> &mut Vec<Self::Entry> {
        &mut store.entries
    }

    fn key_for_entry(entry: &Self::Entry) -> String {
        format!("{}/{}", entry.zone, entry.name)
    }

    fn entry_from_instance(
        instance: &Self::Instance,
        observed_at_unix: u64,
        context: &Self::ListContext,
    ) -> Option<Self::Entry> {
        Some(CacheEntry {
            name: instance.name.clone(),
            zone: instance.zone.clone(),
            listed: Some(instance.render(context, false)),
            observed_at_unix: Some(observed_at_unix),
        })
    }

    fn listed_from_entry(entry: &Self::Entry) -> Option<&ListedInstance> {
        entry.listed.as_ref()
    }

    fn observed_at_unix(entry: &Self::Entry) -> u64 {
        entry.observed_at_unix.unwrap_or_default()
    }
}

impl CloudProvider for Provider {
    type Instance = GcpInstance;
    type ProviderContext<'a> = &'a IceConfig;
    const CLOUD: Cloud = Cloud::Gcp;

    fn context<'a>(config: &'a IceConfig) -> Result<Self::ProviderContext<'a>> {
        Ok(config)
    }

    fn list_instances(
        context: &Self::ProviderContext<'_>,
        on_progress: &mut dyn FnMut(String),
    ) -> Result<Vec<Self::Instance>> {
        on_progress(Self::initial_loading_message());
        list_instances(context)
    }

    fn sort_instances(instances: &mut [Self::Instance]) {
        instances.sort_by(|left, right| right.name.cmp(&left.name));
    }

    fn resolve_instance(
        context: &Self::ProviderContext<'_>,
        identifier: &str,
    ) -> Result<Self::Instance> {
        resolve_instance(context, identifier)
    }

    fn set_running(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
    ) -> Result<()> {
        set_instance_state(context, instance, running)
    }

    fn wait_for_running_state(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
        timeout: Duration,
    ) -> Result<Self::Instance> {
        wait_for_state(
            context,
            &instance.name,
            &instance.zone,
            if running { "RUNNING" } else { "TERMINATED" },
            timeout,
        )
    }

    fn delete_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<()> {
        delete_instance(context, instance)
    }
}

impl RemoteCloudProvider for Provider {
    type CacheModel = CacheModel;
}

impl RemoteSshProvider for Provider {
    fn create_machine(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
        hours: f64,
        workload: &InstanceWorkload,
    ) -> Result<Self::Instance> {
        create_instance(config, candidate, hours, workload)
    }

    fn open_instance_shell(config: &IceConfig, instance: &Self::Instance) -> Result<()> {
        open_shell(config, instance)
    }

    fn download_from_instance(
        config: &IceConfig,
        instance: &Self::Instance,
        remote_path: &str,
        local_path: Option<&Path>,
    ) -> Result<()> {
        download(config, instance, remote_path, local_path)
    }

    fn wait_for_ssh_ready(
        config: &IceConfig,
        instance: &Self::Instance,
        timeout: Duration,
    ) -> Result<()> {
        wait_for_ssh_ready(config, instance, timeout)
    }

    fn upload_unpack_bundle(
        config: &IceConfig,
        instance: &Self::Instance,
        bundle_root: &Path,
        remote_dir: &str,
    ) -> Result<()> {
        upload_unpack_bundle(config, instance, bundle_root, remote_dir)
    }

    fn run_ssh_command(
        config: &IceConfig,
        instance: &Self::Instance,
        command: &str,
        allocate_tty: bool,
    ) -> Result<()> {
        run_ssh_command(config, instance, command, allocate_tty)
    }

    fn remote_unpack_dir(instance: &Self::Instance) -> String {
        remote_unpack_dir_for_gcp(instance)
    }
}

pub(crate) fn registry_login(config: &IceConfig) -> Result<RegistryAuth> {
    Ok(RegistryAuth {
        username: "oauth2accesstoken",
        secret: registry_access_token(config)?,
    })
}

pub(crate) fn list_instances(config: &IceConfig) -> Result<Vec<GcpInstance>> {
    let mut command = command(config);
    command.args([
        "compute",
        "instances",
        "list",
        "--filter=labels.ice_managed=true OR name~'^ice-.*'",
        "--format=json",
    ]);
    maybe_add_project_arg(&mut command, config);
    let instances = parse_instances(run_command_json(&mut command, "list gcp instances")?)?;
    persist_instances::<CacheModel>(&instances);
    Ok(instances)
}

pub(crate) fn resolve_instance(config: &IceConfig, identifier: &str) -> Result<GcpInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }

    let cache = load_cache_store::<CacheModel>();
    if let crate::model::PrefixLookup::Unique(index) = prefix_lookup_indices(
        <CacheModel as CloudCacheModel>::entries(&cache),
        identifier,
        |entry| entry.name.as_str(),
    )? {
        let entry = &<CacheModel as CloudCacheModel>::entries(&cache)[index];
        if let Ok(instance) = describe_instance(config, &entry.name, &entry.zone)
            && instance.name.starts_with(ICE_LABEL_PREFIX)
        {
            return Ok(instance);
        }
    }

    resolve_instance_from_list(list_instances(config)?, identifier)
}

pub(crate) fn set_instance_state(
    config: &IceConfig,
    instance: &GcpInstance,
    running: bool,
) -> Result<()> {
    let action = if running { "start" } else { "stop" };
    let spinner = spinner(&format!(
        "{action}ing instance {}...",
        visible_instance_name(&instance.name)
    ));
    let mut command = command(config);
    command.args([
        "compute",
        "instances",
        action,
        &instance.name,
        "--zone",
        &instance.zone,
        "--quiet",
    ]);
    maybe_add_project_arg(&mut command, config);
    run_command_status(&mut command, &format!("{action} gcp instance"))?;
    spinner.finish_with_message(format!("{action} requested."));
    Ok(())
}

pub(crate) fn wait_for_state(
    config: &IceConfig,
    name: &str,
    zone: &str,
    desired_state: &str,
    timeout: Duration,
) -> Result<GcpInstance> {
    let start = SystemTime::now();
    loop {
        if elapsed_since(start)? > timeout {
            bail!("Timed out waiting for gcp instance `{name}` to reach `{desired_state}`");
        }
        let instance = describe_instance(config, name, zone)?;
        if instance.status.eq_ignore_ascii_case(desired_state) {
            return Ok(instance);
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn open_shell(config: &IceConfig, instance: &GcpInstance) -> Result<()> {
    let remote_command = match instance.workload.as_ref() {
        Some(InstanceWorkload::Unpack(_)) => {
            unpack_shell_remote_command(&remote_unpack_dir_for_gcp(instance))
        }
        Some(workload) => instance_shell_remote_command(workload),
        None => bail!(
            "Instance `{}` is missing workload metadata; refuse to guess its shell mode.",
            instance.name
        ),
    };
    run_ssh_command(config, instance, &remote_command, true)
}

pub(crate) fn download(
    config: &IceConfig,
    instance: &GcpInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut command = command(config);
    command.args([
        "compute",
        "scp",
        "--recurse",
        &format!("{}:{}", instance.name, remote_path),
    ]);
    command.arg(destination);
    command.args([
        "--zone",
        &instance.zone,
        "--scp-flag=-o",
        "--scp-flag=StrictHostKeyChecking=accept-new",
    ]);
    maybe_add_project_arg(&mut command, config);
    run_command_status(&mut command, "download from gcp instance")
}

pub(crate) fn delete_instance(config: &IceConfig, instance: &GcpInstance) -> Result<()> {
    let mut command = command(config);
    command.args([
        "compute",
        "instances",
        "delete",
        &instance.name,
        "--zone",
        &instance.zone,
        "--quiet",
    ]);
    maybe_add_project_arg(&mut command, config);
    run_command_status(&mut command, "delete gcp instance")
}

pub(crate) fn create_instance(
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
    hours: f64,
    workload: &InstanceWorkload,
) -> Result<GcpInstance> {
    let zone = candidate
        .zone
        .clone()
        .ok_or_else(|| anyhow!("Missing zone for selected GCP machine type."))?;
    let existing_names = list_instances(config)?
        .into_iter()
        .map(|instance| visible_instance_name(&instance.name).to_owned())
        .collect::<HashSet<_>>();
    let name = build_cloud_instance_name(&existing_names)?;
    let image_family = config
        .default
        .gcp
        .image_family
        .clone()
        .unwrap_or_else(|| "debian-12".to_owned());
    let image_project = config
        .default
        .gcp
        .image_project
        .clone()
        .unwrap_or_else(|| "debian-cloud".to_owned());
    let disk_gb = config.default.gcp.boot_disk_gb.unwrap_or(50);
    let direct_container_service_account = match workload {
        InstanceWorkload::Container(_) => service_account_email(config)?,
        InstanceWorkload::Shell | InstanceWorkload::Unpack(_) => None,
    };
    let startup_workload = if direct_container_service_account.is_some() {
        InstanceWorkload::Shell
    } else {
        workload.clone()
    };
    let registry_auth = registry_auth_for_workload(config, &startup_workload)?;
    let startup_script = build_linux_startup_script(
        "/sbin/shutdown -h now",
        hours,
        &startup_workload,
        registry_auth.as_ref(),
    )?;
    let script_path = write_temp_file("ice-gcp-startup", ".sh", &startup_script)?;
    let workload_metadata = gcp_workload_metadata_arg(workload);
    let startup_metadata = format!("startup-script={}", script_path.display());

    let spinner = spinner("Creating gcp instance...");
    let mut command = command(config);
    match (workload, direct_container_service_account.as_deref()) {
        (InstanceWorkload::Container(container), Some(service_account)) => {
            command.args([
                "compute",
                "instances",
                "create-with-container",
                &name,
                "--zone",
                &zone,
                "--machine-type",
                &candidate.machine,
                "--container-image",
                &container.container_ref(),
                "--image-family",
                GCP_CONTAINER_IMAGE_FAMILY,
                "--image-project",
                GCP_CONTAINER_IMAGE_PROJECT,
                "--boot-disk-size",
                &format!("{disk_gb}GB"),
                "--labels",
                "ice_managed=true,ice_creator=ice",
                "--metadata",
                &workload_metadata,
                "--metadata-from-file",
                &startup_metadata,
                "--service-account",
                service_account,
                "--scopes",
                GCP_CLOUD_PLATFORM_SCOPE,
            ]);
        }
        _ => {
            command.args([
                "compute",
                "instances",
                "create",
                &name,
                "--zone",
                &zone,
                "--machine-type",
                &candidate.machine,
                "--image-family",
                &image_family,
                "--image-project",
                &image_project,
                "--boot-disk-size",
                &format!("{disk_gb}GB"),
                "--labels",
                "ice_managed=true,ice_creator=ice",
                "--metadata",
                &workload_metadata,
                "--metadata-from-file",
                &startup_metadata,
            ]);
        }
    }
    maybe_add_project_arg(&mut command, config);
    let result = run_command_status(&mut command, "create gcp instance");
    let _ = fs::remove_file(&script_path);
    result?;
    spinner.finish_with_message("Creation requested.");
    wait_for_state(
        config,
        &name,
        &zone,
        "RUNNING",
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )
}

fn registry_access_token(config: &IceConfig) -> Result<String> {
    let mut adc_command = command(config);
    adc_command.args(["auth", "application-default", "print-access-token"]);
    if let Ok(token) = run_command_text(
        &mut adc_command,
        "print a GCP access token from application-default credentials",
    ) && !token.trim().is_empty()
    {
        return Ok(token);
    }

    let mut account_command = command(config);
    account_command.args(["auth", "print-access-token"]);
    let token = run_command_text(
        &mut account_command,
        "print a GCP access token from the active gcloud account",
    )?;
    if token.trim().is_empty() {
        bail!("`gcloud auth print-access-token` returned an empty token.");
    }
    Ok(token)
}

fn service_account_email(config: &IceConfig) -> Result<Option<String>> {
    let Some(path) = config.auth.gcp.service_account_json.as_deref() else {
        return Ok(None);
    };
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read GCP service account JSON: {path}"))?;
    let value = serde_json::from_str::<Value>(&contents)
        .with_context(|| format!("Failed to parse GCP service account JSON: {path}"))?;
    Ok(value
        .get("client_email")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|email| !email.is_empty())
        .map(str::to_owned))
}

fn command(config: &IceConfig) -> Command {
    let mut command = Command::new("gcloud");
    if let Some(path) = config.auth.gcp.service_account_json.as_deref()
        && !path.trim().is_empty()
    {
        command.env("GOOGLE_APPLICATION_CREDENTIALS", path.trim());
    }
    command
}

fn maybe_add_project_arg(command: &mut Command, config: &IceConfig) {
    if let Some(project) = config.auth.gcp.project.as_deref()
        && !project.trim().is_empty()
    {
        command.arg("--project").arg(project.trim());
    }
}

fn parse_instances(value: Value) -> Result<Vec<GcpInstance>> {
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("Unexpected gcp instances response shape"))?;
    Ok(rows.iter().filter_map(parse_instance_row).collect())
}

fn parse_instance_row(row: &Value) -> Option<GcpInstance> {
    let name = row.get("name")?.as_str()?.to_owned();
    let zone = short_gcp_zone(row.get("zone").and_then(Value::as_str).unwrap_or(""));
    let status = row
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned();
    let machine_type = row
        .get("machineType")
        .and_then(Value::as_str)
        .map(|value| value.rsplit('/').next().unwrap_or(value).to_owned())
        .unwrap_or_else(|| "unknown".to_owned());

    Some(GcpInstance {
        name,
        zone,
        status,
        machine_type,
        creation_timestamp: row
            .get("creationTimestamp")
            .and_then(Value::as_str)
            .map(str::to_owned),
        last_start_timestamp: row
            .get("lastStartTimestamp")
            .and_then(Value::as_str)
            .map(str::to_owned),
        workload: parse_workload_metadata(
            instance_metadata_value(row, ICE_WORKLOAD_KIND_METADATA_KEY),
            instance_metadata_value(row, ICE_WORKLOAD_REGISTRY_METADATA_KEY),
            instance_metadata_value(row, ICE_WORKLOAD_CONTAINER_METADATA_KEY),
            instance_metadata_value(row, ICE_WORKLOAD_SOURCE_METADATA_KEY),
        ),
    })
}

fn instance_metadata_value<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get("metadata")
        .and_then(|metadata| metadata.get("items"))
        .and_then(Value::as_array)?
        .iter()
        .find_map(|item| {
            (item.get("key").and_then(Value::as_str) == Some(key))
                .then(|| item.get("value").and_then(Value::as_str))
                .flatten()
        })
}

fn describe_instance(config: &IceConfig, name: &str, zone: &str) -> Result<GcpInstance> {
    let mut command = command(config);
    command.args([
        "compute",
        "instances",
        "describe",
        name,
        "--zone",
        zone,
        "--format=json",
    ]);
    maybe_add_project_arg(&mut command, config);
    let instance = parse_instance_row(&run_command_json(&mut command, "describe gcp instance")?)
        .ok_or_else(|| anyhow!("Could not parse gcp instance description for {name}"))?;
    upsert_instance::<CacheModel>(&instance);
    Ok(instance)
}

fn resolve_instance_from_list(
    instances: Vec<GcpInstance>,
    identifier: &str,
) -> Result<GcpInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.name.as_str())? {
        crate::model::PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        crate::model::PrefixLookup::Ambiguous(indices) => {
            let listing = indices
                .into_iter()
                .map(|index| {
                    let item = &instances[index];
                    format!("{} ({})", visible_instance_name(&item.name), item.zone)
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        crate::model::PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn run_ssh_command(
    config: &IceConfig,
    instance: &GcpInstance,
    remote_command: &str,
    allocate_tty: bool,
) -> Result<()> {
    let mut command = command(config);
    command.args([
        "compute",
        "ssh",
        &instance.name,
        "--zone",
        &instance.zone,
        "--command",
        remote_command,
        "--ssh-flag=-o",
        "--ssh-flag=StrictHostKeyChecking=accept-new",
    ]);
    if allocate_tty {
        command.arg("--ssh-flag=-t");
    }
    maybe_add_project_arg(&mut command, config);
    run_command_status(&mut command, "run remote command on gcp instance")
}

fn wait_for_ssh_ready(config: &IceConfig, instance: &GcpInstance, timeout: Duration) -> Result<()> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for SSH on gcp instance {}...",
        visible_instance_name(&instance.name)
    ));
    let probe = wrap_remote_shell_script("true");
    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            bail!(
                "Timed out waiting for SSH readiness on gcp instance `{}`.",
                instance.name
            );
        }
        if run_ssh_command(config, instance, &probe, false).is_ok() {
            spinner.finish_with_message(format!(
                "Gcp instance {} is SSH-ready.",
                visible_instance_name(&instance.name)
            ));
            return Ok(());
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn upload_unpack_bundle(
    config: &IceConfig,
    instance: &GcpInstance,
    bundle_root: &Path,
    remote_dir: &str,
) -> Result<()> {
    let tar_path = bundle_root.with_extension("tar");
    pack_directory_as_tar(bundle_root, &tar_path)?;
    let remote_tar = format!("{remote_dir}.tar");
    let result = (|| {
        run_ssh_command(
            config,
            instance,
            &unpack_prepare_remote_dir_command(remote_dir),
            false,
        )?;
        let mut scp = command(config);
        scp.args([
            "compute",
            "scp",
            "--scp-flag=-o",
            "--scp-flag=StrictHostKeyChecking=accept-new",
        ]);
        scp.arg(&tar_path);
        scp.arg(format!("{}:{}", instance.name, remote_tar));
        scp.args(["--zone", &instance.zone]);
        maybe_add_project_arg(&mut scp, config);
        run_command_status(&mut scp, "upload unpack bundle to gcp instance")?;
        run_ssh_command(
            config,
            instance,
            &wrap_remote_shell_script(&format!(
                "mkdir -p {dir} && tar xf {tarball} -C {dir} && rm -f {tarball}",
                dir = crate::support::shell_quote_single(remote_dir),
                tarball = crate::support::shell_quote_single(&remote_tar),
            )),
            false,
        )
    })();
    let _ = fs::remove_file(&tar_path);
    result
}
