use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color,
    listed_instance as base_listed_instance, present_field, push_field,
};
use crate::model::{Cloud, CloudMachineCandidate, IceConfig};
use crate::providers::{CloudInstance, CloudProvider, RemoteCloudProvider, RemoteSshProvider};
use crate::provision::{AWS_MACHINE_SPECS, estimated_machine_hourly_price};
use crate::remote::{RemoteAccess, run_rsync_download, run_rsync_upload};
use crate::support::{
    ICE_LABEL_PREFIX, ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, VAST_POLL_INTERVAL_SECS,
    VAST_WAIT_TIMEOUT_SECS, build_cloud_instance_name, elapsed_hours_from_rfc3339, elapsed_since,
    prefix_lookup_indices, run_command_json, run_command_output, run_command_status,
    run_command_text, spinner, truncate_ellipsis, visible_instance_name, write_temp_file,
};
use crate::unpack::{
    remote_unpack_dir_for_aws, unpack_prepare_remote_dir_command, unpack_shell_remote_command,
};
use crate::workload::{
    InstanceWorkload, aws_instance_tag_specification, build_linux_startup_script,
    instance_shell_remote_command, parse_workload_metadata, registry_auth_for_workload,
    workload_display_value, wrap_remote_shell_script,
};

#[derive(Debug, Clone)]
pub(crate) struct AwsInstance {
    pub(crate) instance_id: String,
    pub(crate) name: Option<String>,
    pub(crate) region: String,
    pub(crate) state: String,
    pub(crate) instance_type: String,
    pub(crate) launch_time: Option<String>,
    pub(crate) public_ip: Option<String>,
    pub(crate) public_dns: Option<String>,
    pub(crate) workload: Option<InstanceWorkload>,
}

pub(crate) struct Provider;
pub(crate) struct CacheModel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    pub(crate) instance_id: String,
    pub(crate) name: Option<String>,
    pub(crate) region: String,
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

impl AwsInstance {
    pub(crate) fn label_str(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }

    pub(crate) fn is_running(&self) -> bool {
        self.state.eq_ignore_ascii_case("running")
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.state.eq_ignore_ascii_case("stopped")
    }

    pub(crate) fn runtime_hours(&self) -> f64 {
        if !self.is_running() {
            return 0.0;
        }
        self.launch_time
            .as_deref()
            .and_then(elapsed_hours_from_rfc3339)
            .unwrap_or(0.0)
    }
}

impl CloudInstance for AwsInstance {
    type ListContext = ();

    fn cache_key(&self) -> String {
        format!("{}/{}", self.region, self.instance_id)
    }

    fn display_name(&self) -> String {
        self.instance_id.clone()
    }

    fn state_value(&self) -> &str {
        &self.state
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
        let state = display_state(&self.state);
        let mut fields = Vec::new();
        push_field(&mut fields, health.clone());
        fields.push(format!("{:.2}h", self.runtime_hours()));
        push_field(
            &mut fields,
            estimated_machine_hourly_price(Cloud::Aws, &self.instance_type)
                .map(|value| format!("${value:.4}/hr")),
        );
        fields.push(self.instance_type.clone());
        fields.push(self.region.clone());

        let mut detail_fields = vec![format!("aws://{}/{}", self.region, self.instance_id)];
        push_field(
            &mut detail_fields,
            self.public_dns
                .clone()
                .or_else(|| self.public_ip.clone())
                .map(|host| format!("ssh://{host}")),
        );
        push_field(
            &mut detail_fields,
            present_field(&workload_display_value(self.workload.as_ref())),
        );

        base_listed_instance(
            display_name_or_fallback(self.label_str(), truncate_ellipsis(&self.instance_id, 12)),
            state.clone(),
            list_state_color(&state, health.as_deref()),
            fields,
            detail_fields,
        )
    }
}

impl CloudCacheModel for CacheModel {
    type Instance = AwsInstance;
    type ListContext = ();
    type Entry = CacheEntry;
    type Store = CacheStore;

    const CLOUD: Cloud = Cloud::Aws;

    fn entries(store: &Self::Store) -> &[Self::Entry] {
        &store.entries
    }

    fn entries_mut(store: &mut Self::Store) -> &mut Vec<Self::Entry> {
        &mut store.entries
    }

    fn key_for_entry(entry: &Self::Entry) -> String {
        format!("{}/{}", entry.region, entry.instance_id)
    }

    fn entry_from_instance(
        instance: &Self::Instance,
        observed_at_unix: u64,
        context: &Self::ListContext,
    ) -> Option<Self::Entry> {
        Some(CacheEntry {
            instance_id: instance.instance_id.clone(),
            name: instance.name.clone(),
            region: instance.region.clone(),
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
    type Instance = AwsInstance;
    type ProviderContext<'a> = &'a IceConfig;
    const CLOUD: Cloud = Cloud::Aws;

    fn context<'a>(config: &'a IceConfig) -> Result<Self::ProviderContext<'a>> {
        Ok(config)
    }

    fn list_instances(
        context: &Self::ProviderContext<'_>,
        on_progress: &mut dyn FnMut(String),
    ) -> Result<Vec<Self::Instance>> {
        list_instances_with_progress(context, |region, index, total| {
            on_progress(format!(
                "Loading aws instance state... {region} ({index}/{total})"
            ));
        })
    }

    fn sort_instances(instances: &mut [Self::Instance]) {
        instances.sort_by(|left, right| right.instance_id.cmp(&left.instance_id));
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
            &instance.instance_id,
            &instance.region,
            if running { "running" } else { "stopped" },
            timeout,
        )
    }

    fn delete_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<()> {
        terminate_instance(context, instance)
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
        remote_unpack_dir_for_aws(instance)
    }
}

pub(crate) fn list_instances(config: &IceConfig) -> Result<Vec<AwsInstance>> {
    list_instances_with_progress(config, |_, _, _| {})
}

pub(crate) fn list_instances_with_progress<F>(
    config: &IceConfig,
    mut on_region: F,
) -> Result<Vec<AwsInstance>>
where
    F: FnMut(&str, usize, usize),
{
    let regions = regions_to_query(config);
    let total = regions.len();
    let mut instances = Vec::new();
    for (index, region) in regions.into_iter().enumerate() {
        on_region(&region, index + 1, total);
        let mut command = command(config, &region);
        command.args([
            "ec2",
            "describe-instances",
            "--filters",
            "Name=tag:ice-managed,Values=true",
            "Name=instance-state-name,Values=pending,running,stopping,stopped",
            "--output",
            "json",
            "--region",
            &region,
        ]);
        instances.extend(parse_instances(
            &run_command_json(&mut command, &format!("list aws instances in {region}"))?,
            &region,
        )?);
    }
    persist_instances::<CacheModel>(&instances);
    Ok(instances)
}

pub(crate) fn resolve_instance(config: &IceConfig, identifier: &str) -> Result<AwsInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }
    let needle = identifier.to_ascii_lowercase();
    let cache = load_cache_store::<CacheModel>();
    let cache_entries = <CacheModel as CloudCacheModel>::entries(&cache);

    if needle.starts_with("i-")
        && let Some(entry) = cache_entries
            .iter()
            .find(|entry| entry.instance_id.eq_ignore_ascii_case(identifier))
        && let Ok(instance) = describe_instance(config, &entry.instance_id, &entry.region)
    {
        return Ok(instance);
    }

    let named_cache = cache_entries
        .iter()
        .filter(|entry| {
            entry
                .name
                .as_deref()
                .map(|name| name.starts_with(ICE_LABEL_PREFIX))
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    if let crate::model::PrefixLookup::Unique(index) =
        prefix_lookup_indices(&named_cache, identifier, |entry| {
            entry.name.as_deref().unwrap_or("")
        })?
    {
        let entry = &named_cache[index];
        if let Ok(instance) = describe_instance(config, &entry.instance_id, &entry.region) {
            return Ok(instance);
        }
    }

    let instances = list_instances(config)?;
    if needle.starts_with("i-") {
        return instances
            .into_iter()
            .find(|instance| instance.instance_id.eq_ignore_ascii_case(identifier))
            .ok_or_else(|| anyhow!("No AWS instance found with ID `{identifier}`."));
    }
    resolve_instance_from_list(instances, identifier)
}

pub(crate) fn set_instance_state(
    config: &IceConfig,
    instance: &AwsInstance,
    running: bool,
) -> Result<()> {
    let action = if running {
        "start-instances"
    } else {
        "stop-instances"
    };
    let spinner = spinner(&format!(
        "{} instance {}...",
        if running { "Starting" } else { "Stopping" },
        instance.instance_id
    ));
    let mut command = command(config, &instance.region);
    command.args([
        "ec2",
        action,
        "--instance-ids",
        &instance.instance_id,
        "--region",
        &instance.region,
        "--output",
        "json",
    ]);
    run_command_output(&mut command, "set aws instance state")?;
    spinner.finish_with_message("State change requested.");
    Ok(())
}

pub(crate) fn wait_for_state(
    config: &IceConfig,
    instance_id: &str,
    region: &str,
    desired_state: &str,
    timeout: Duration,
) -> Result<AwsInstance> {
    let start = SystemTime::now();
    loop {
        if elapsed_since(start)? > timeout {
            bail!("Timed out waiting for aws instance {instance_id} to reach `{desired_state}`");
        }
        let instance = describe_instance(config, instance_id, region)?;
        if instance.state.eq_ignore_ascii_case(desired_state) {
            return Ok(instance);
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn open_shell(config: &IceConfig, instance: &AwsInstance) -> Result<()> {
    let remote_command = match instance.workload.as_ref() {
        Some(InstanceWorkload::Unpack(_)) => {
            unpack_shell_remote_command(&remote_unpack_dir_for_aws(instance))
        }
        Some(workload) => instance_shell_remote_command(workload),
        None => bail!(
            "Instance `{}` is missing workload metadata; refuse to guess its shell mode.",
            instance.instance_id
        ),
    };
    run_ssh_command(config, instance, &remote_command, true)
}

pub(crate) fn download(
    config: &IceConfig,
    instance: &AwsInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let key_path = ssh_key_path(config)?;
    let user = ssh_user(config);
    let host = ssh_host(instance)?;
    run_rsync_download(
        RemoteAccess {
            user: &user,
            host: &host,
            port: None,
            identity_file: Some(key_path.as_path()),
        },
        remote_path,
        local_path,
        "download from aws instance",
    )
}

pub(crate) fn terminate_instance(config: &IceConfig, instance: &AwsInstance) -> Result<()> {
    let mut command = command(config, &instance.region);
    command.args([
        "ec2",
        "terminate-instances",
        "--instance-ids",
        &instance.instance_id,
        "--region",
        &instance.region,
        "--output",
        "json",
    ]);
    run_command_output(&mut command, "terminate aws instance")?;
    Ok(())
}

pub(crate) fn create_instance(
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
    hours: f64,
    workload: &InstanceWorkload,
) -> Result<AwsInstance> {
    let region = candidate.region.clone();
    let ami = config
        .default
        .aws
        .ami
        .as_deref()
        .filter(|ami| !ami.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or(lookup_default_ami(config, &region)?);
    let existing_names = list_instances(config)?
        .into_iter()
        .map(|instance| visible_instance_name(instance.label_str()).to_owned())
        .filter(|name| !name.is_empty())
        .collect::<HashSet<_>>();
    let name = build_cloud_instance_name(&existing_names)?;
    let registry_auth = registry_auth_for_workload(config, workload)?;
    let startup_script =
        build_linux_startup_script("shutdown -h now", hours, workload, registry_auth.as_ref())?;
    let script_path = write_temp_file("ice-aws-startup", ".sh", &startup_script)?;
    let tag_specifications = aws_instance_tag_specification(&name, workload)?;

    let mut command = command(config, &region);
    command.args([
        "ec2",
        "run-instances",
        "--image-id",
        &ami,
        "--instance-type",
        &candidate.machine,
        "--count",
        "1",
        "--tag-specifications",
        &tag_specifications,
        "--user-data",
        &format!("file://{}", script_path.display()),
        "--region",
        &region,
        "--output",
        "json",
    ]);

    if let Some(key_name) = config.default.aws.key_name.as_deref()
        && !key_name.trim().is_empty()
    {
        command.arg("--key-name").arg(key_name.trim());
    }
    if let Some(group) = config.default.aws.security_group_id.as_deref()
        && !group.trim().is_empty()
    {
        command.arg("--security-group-ids").arg(group.trim());
    }
    if let Some(subnet) = config.default.aws.subnet_id.as_deref()
        && !subnet.trim().is_empty()
    {
        command.arg("--subnet-id").arg(subnet.trim());
    }
    if let Some(size) = config.default.aws.root_disk_gb
        && size > 0
    {
        command.arg("--block-device-mappings").arg(format!(
            "[{{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{{\"VolumeSize\":{size},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}}}]"
        ));
    }

    let spinner = spinner("Creating aws instance...");
    let value = run_command_json(&mut command, "create aws instance");
    let _ = fs::remove_file(&script_path);
    let value = value?;
    spinner.finish_with_message("Creation requested.");
    let instance_id = value
        .get("Instances")
        .and_then(Value::as_array)
        .and_then(|instances| instances.first())
        .and_then(|instance| instance.get("InstanceId"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("AWS create response missing instance ID"))?;
    wait_for_state(
        config,
        instance_id,
        &region,
        "running",
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )
}

fn command(config: &IceConfig, region: &str) -> Command {
    let mut command = Command::new("aws");
    if let Some(access_key_id) = config.auth.aws.access_key_id.as_deref()
        && !access_key_id.trim().is_empty()
    {
        command.env("AWS_ACCESS_KEY_ID", access_key_id.trim());
    }
    if let Some(secret_access_key) = config.auth.aws.secret_access_key.as_deref()
        && !secret_access_key.trim().is_empty()
    {
        command.env("AWS_SECRET_ACCESS_KEY", secret_access_key.trim());
    }
    command.env("AWS_DEFAULT_REGION", region);
    command
}

fn regions_to_query(config: &IceConfig) -> Vec<String> {
    if let Some(region) = config.default.aws.region.clone() {
        return vec![region];
    }
    let mut regions = BTreeSet::new();
    for spec in AWS_MACHINE_SPECS {
        for region in spec.regions {
            regions.insert((*region).to_owned());
        }
    }
    regions.into_iter().collect()
}

fn parse_instances(value: &Value, region: &str) -> Result<Vec<AwsInstance>> {
    let Some(reservations) = value.get("Reservations").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut instances = Vec::new();
    for reservation in reservations {
        let Some(rows) = reservation.get("Instances").and_then(Value::as_array) else {
            continue;
        };
        for row in rows {
            if let Some(instance) = parse_instance_row(row, region) {
                instances.push(instance);
            }
        }
    }
    Ok(instances)
}

fn parse_instance_row(row: &Value, region: &str) -> Option<AwsInstance> {
    let tags = row
        .get("Tags")
        .and_then(Value::as_array)
        .map(|values| extract_tags(values))
        .unwrap_or_default();
    let name = tags.get("Name").cloned();
    let ice_managed = tags
        .get("ice-managed")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !ice_managed && !name.as_deref().unwrap_or("").starts_with(ICE_LABEL_PREFIX) {
        return None;
    }

    Some(AwsInstance {
        instance_id: row.get("InstanceId")?.as_str()?.to_owned(),
        name,
        region: region.to_owned(),
        state: row
            .get("State")
            .and_then(|value| value.get("Name"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        instance_type: row
            .get("InstanceType")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        launch_time: row
            .get("LaunchTime")
            .and_then(Value::as_str)
            .map(str::to_owned),
        public_ip: row
            .get("PublicIpAddress")
            .and_then(Value::as_str)
            .map(str::to_owned),
        public_dns: row
            .get("PublicDnsName")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        workload: parse_workload_metadata(
            tags.get(ICE_WORKLOAD_KIND_METADATA_KEY).map(String::as_str),
            tags.get(ICE_WORKLOAD_REGISTRY_METADATA_KEY)
                .map(String::as_str),
            tags.get(ICE_WORKLOAD_CONTAINER_METADATA_KEY)
                .map(String::as_str),
            tags.get(ICE_WORKLOAD_SOURCE_METADATA_KEY)
                .map(String::as_str),
        ),
    })
}

fn extract_tags(values: &[Value]) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for value in values {
        if let (Some(key), Some(entry_value)) = (
            value.get("Key").and_then(Value::as_str),
            value.get("Value").and_then(Value::as_str),
        ) {
            tags.insert(key.to_owned(), entry_value.to_owned());
        }
    }
    tags
}

fn resolve_instance_from_list(
    instances: Vec<AwsInstance>,
    identifier: &str,
) -> Result<AwsInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.label_str())? {
        crate::model::PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        crate::model::PrefixLookup::Ambiguous(indices) => {
            let listing = indices
                .into_iter()
                .map(|index| {
                    let item = &instances[index];
                    format!(
                        "{} ({})",
                        item.instance_id,
                        visible_instance_name(item.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        crate::model::PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn describe_instance(config: &IceConfig, id: &str, region: &str) -> Result<AwsInstance> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-instances",
        "--instance-ids",
        id,
        "--output",
        "json",
        "--region",
        region,
    ]);
    let instance = parse_instances(
        &run_command_json(&mut command, "describe aws instance")?,
        region,
    )?
    .into_iter()
    .find(|instance| instance.instance_id == id)
    .ok_or_else(|| anyhow!("No AWS instance found with ID `{id}` in region `{region}`."))?;
    upsert_instance::<CacheModel>(&instance);
    Ok(instance)
}

fn ssh_user(config: &IceConfig) -> String {
    config
        .default
        .aws
        .ssh_user
        .clone()
        .unwrap_or_else(|| "ec2-user".to_owned())
}

fn ssh_key_path(config: &IceConfig) -> Result<PathBuf> {
    let Some(path) = config.default.aws.ssh_key_path.as_deref() else {
        bail!(
            "Missing `default.aws.ssh_key_path`. Set it with e.g. `ice config set default.aws.ssh_key_path=/path/to/key.pem`."
        );
    };
    Ok(PathBuf::from(path))
}

fn ssh_host(instance: &AwsInstance) -> Result<String> {
    if let Some(host) = instance.public_dns.as_deref()
        && !host.trim().is_empty()
    {
        return Ok(host.to_owned());
    }
    if let Some(host) = instance.public_ip.as_deref()
        && !host.trim().is_empty()
    {
        return Ok(host.to_owned());
    }
    bail!(
        "Instance {} has no public IP/DNS for SSH.",
        instance.instance_id
    )
}

fn run_ssh_command(
    config: &IceConfig,
    instance: &AwsInstance,
    remote_command: &str,
    allocate_tty: bool,
) -> Result<()> {
    let key_path = ssh_key_path(config)?;
    let user = ssh_user(config);
    let host = ssh_host(instance)?;
    let mut command = Command::new("ssh");
    command
        .arg("-i")
        .arg(key_path)
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new");
    if allocate_tty {
        command.arg("-t");
    }
    command.arg(format!("{user}@{host}")).arg(remote_command);
    run_command_status(&mut command, "run remote command on aws instance")
}

fn wait_for_ssh_ready(config: &IceConfig, instance: &AwsInstance, timeout: Duration) -> Result<()> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for SSH on aws instance {}...",
        instance.instance_id
    ));
    let probe = wrap_remote_shell_script("true");
    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            bail!(
                "Timed out waiting for SSH readiness on aws instance `{}`.",
                instance.instance_id
            );
        }
        if run_ssh_command(config, instance, &probe, false).is_ok() {
            spinner.finish_with_message(format!(
                "Aws instance {} is SSH-ready.",
                instance.instance_id
            ));
            return Ok(());
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn upload_unpack_bundle(
    config: &IceConfig,
    instance: &AwsInstance,
    bundle_root: &Path,
    remote_dir: &str,
) -> Result<()> {
    run_ssh_command(
        config,
        instance,
        &unpack_prepare_remote_dir_command(remote_dir),
        false,
    )?;
    let key_path = ssh_key_path(config)?;
    let user = ssh_user(config);
    let host = ssh_host(instance)?;
    run_rsync_upload(
        RemoteAccess {
            user: &user,
            host: &host,
            port: None,
            identity_file: Some(key_path.as_path()),
        },
        bundle_root,
        remote_dir,
        "upload unpack bundle to aws instance",
    )
}

fn lookup_default_ami(config: &IceConfig, region: &str) -> Result<String> {
    let mut command = command(config, region);
    command.args([
        "ssm",
        "get-parameter",
        "--name",
        "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
        "--query",
        "Parameter.Value",
        "--output",
        "text",
        "--region",
        region,
    ]);
    run_command_text(&mut command, "lookup default aws ami")
}
