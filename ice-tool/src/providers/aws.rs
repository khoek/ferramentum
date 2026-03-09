use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use csv::StringRecord;
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::gpu::{
    canonicalize_gpu_name_for_cloud, normalize_gpu_name_token, runtime_provider_data_path,
};
use crate::http_retry;
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color,
    listed_instance as base_listed_instance, present_field, push_field,
};
use crate::model::{Cloud, CloudMachineCandidate, CreateSearchRequirements, IceConfig};
use crate::providers::{
    CloudInstance, CloudProvider, RemoteCloudProvider, RemoteSshProvider, clear_cached_arc,
    load_cached_arc,
};
use crate::provision::estimated_machine_hourly_price;
use crate::remote::{RemoteAccess, run_rsync_download, run_rsync_upload};
use crate::support::{
    ICE_LABEL_PREFIX, ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, VAST_POLL_INTERVAL_SECS,
    VAST_WAIT_TIMEOUT_SECS, build_cloud_instance_name, elapsed_hours_from_rfc3339, elapsed_since,
    now_unix_secs, prefix_lookup_indices, progress_bar, run_command_json, run_command_output,
    run_command_status, run_command_text, spinner, truncate_ellipsis, visible_instance_name,
    write_temp_file,
};
use crate::ui::print_warning;
use crate::unpack::{
    remote_unpack_dir_for_aws, unpack_prepare_remote_dir_command, unpack_shell_remote_command,
};
use crate::workload::{
    InstanceWorkload, aws_instance_tag_specification, build_linux_startup_script,
    instance_shell_remote_command, parse_workload_metadata, registry_auth_for_workload,
    workload_display_value, wrap_remote_shell_script,
};

const AWS_DEFAULT_IMAGE_ARCHITECTURE: &str = "x86_64";
const AWS_ARM64_IMAGE_ARCHITECTURE: &str = "arm64";
const AWS_DEFAULT_VIRTUALIZATION_TYPE: &str = "hvm";
const AWS_DEFAULT_AMI_PARAMETER_X86_64: &str =
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64";
const AWS_DEFAULT_AMI_PARAMETER_ARM64: &str =
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64";
const AWS_LOCAL_CATALOG_MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;
const AWS_PRICE_LIST_BASE_URL: &str = "https://pricing.us-east-1.amazonaws.com";
const AWS_PRICE_LIST_REGION_INDEX_URL: &str =
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/region_index.json";
const AWS_PRICE_LIST_CSV_METADATA_ROWS: usize = 5;
const AWS_PRICING_MAX_RETRIES: u32 = 10;
const AWS_PRICING_MAX_ATTEMPTS: u32 = AWS_PRICING_MAX_RETRIES + 1;
const AWS_PRICING_CONNECT_TIMEOUT_SECS: u64 = 30;
const AWS_PRICING_REQUEST_TIMEOUT_SECS: u64 = 1800;
const AWS_PRICE_LIST_REGION: &str = "us-east-1";
const AWS_REGION_FILTER_LOCATION_TYPE: &str = "AWS Region";
const AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY: &str = "Compute Instance";
const AWS_BARE_METAL_PRODUCT_FAMILY: &str = "Compute Instance (bare metal)";
static AWS_LOCAL_CATALOG_STORE_CACHE: LazyLock<Mutex<Option<Arc<AwsMachineCatalogStore>>>> =
    LazyLock::new(|| Mutex::new(None));

fn aws_parallelism(task_count: usize) -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(4)
        .max(1)
        .min(task_count.max(1))
}

#[derive(Debug, Clone)]
struct AwsImageRequirements {
    architecture: String,
    virtualization_type: String,
}

#[derive(Debug, Clone)]
struct AwsMachineShape {
    machine: String,
    region: String,
    vcpus: u32,
    ram_mb: u32,
    gpus: Vec<String>,
    has_accelerators: bool,
    architecture: String,
    virtualization_types: Vec<String>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct AwsMachineCatalogEntry {
    pub(crate) machine: String,
    pub(crate) region: String,
    pub(crate) zone: String,
    pub(crate) vcpus: u32,
    pub(crate) ram_mb: u32,
    #[serde(default)]
    pub(crate) gpus: Vec<String>,
    #[serde(default)]
    pub(crate) has_accelerators: bool,
    pub(crate) architecture: String,
    #[serde(default)]
    pub(crate) virtualization_types: Vec<String>,
    pub(crate) hourly_usd: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsMachineCatalogStore {
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    entries: Vec<AwsMachineCatalogEntry>,
}

#[derive(Debug)]
pub(crate) struct RefreshCatalogOutcome {
    pub(crate) path: PathBuf,
    pub(crate) entry_count: usize,
    pub(crate) changed_entry_count: usize,
    pub(crate) warning_count: usize,
    pub(crate) warning_summary: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum AwsCatalogWarning {
    MissingInstanceOffering { machine: String, region: String },
    MissingPrice { machine: String, region: String },
    HostTenancyPriceOnly { machine: String, region: String },
}

#[derive(Debug, Deserialize)]
struct AwsPriceListRegionIndex {
    regions: HashMap<String, AwsPriceListRegionFile>,
}

#[derive(Debug, Deserialize)]
struct AwsPriceListRegionFile {
    #[serde(rename = "currentVersionUrl")]
    current_version_url: String,
}

#[derive(Debug, Clone, Copy)]
struct AwsBulkPriceCsvColumns {
    term_type: usize,
    unit: usize,
    currency: usize,
    price_per_unit: usize,
    product_family: usize,
    location_type: usize,
    instance_type: usize,
    tenancy: usize,
    operating_system: usize,
    pre_installed_software: usize,
    capacity_status: usize,
    region_code: usize,
}

#[derive(Debug, Default)]
struct AwsLivePricing {
    shared_prices: HashMap<(String, String), f64>,
    available_tenancies: HashMap<(String, String), BTreeSet<String>>,
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

    fn refresh_machine_offer(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
    ) -> Result<CloudMachineCandidate> {
        refresh_machine_offer(config, candidate)
    }
}

pub(crate) fn find_cheapest_machine_candidate(
    config: &IceConfig,
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
) -> Result<CloudMachineCandidate> {
    let store = load_local_catalog_store().with_context(
        || "AWS search uses the local catalog only. Run `ice refresh-catalog --cloud aws` first.",
    )?;
    warn_if_catalog_stale(&store);
    let preferred_region = validated_preferred_region(config)?;
    let search_zone = resolve_search_zone(config, preferred_region.as_deref())?;
    let images = resolve_search_image_requirements(config, preferred_region.as_deref())?;
    let override_name = machine_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(name) = override_name.as_deref()
        && !store
            .entries
            .iter()
            .any(|entry| entry.machine.eq_ignore_ascii_case(name))
    {
        bail!(
            "Machine type `{name}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`."
        );
    }

    select_cheapest_machine_candidate(
        &store.entries,
        req,
        machine_override,
        &images,
        preferred_region.as_deref(),
        search_zone.as_deref(),
    )
}

fn resolve_search_image_requirements(
    config: &IceConfig,
    preferred_region: Option<&str>,
) -> Result<Vec<AwsImageRequirements>> {
    if let Some(ami) = config
        .default
        .aws
        .ami
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let region = preferred_region.ok_or_else(|| {
            anyhow!(
                "Set `default.aws.region` or `AWS_REGION` when `default.aws.ami` is configured. \
                 AMI IDs are region-specific."
            )
        })?;
        return Ok(vec![describe_image_requirements(config, &region, ami)?]);
    }

    Ok(vec![
        AwsImageRequirements {
            architecture: AWS_DEFAULT_IMAGE_ARCHITECTURE.to_owned(),
            virtualization_type: AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned(),
        },
        AwsImageRequirements {
            architecture: AWS_ARM64_IMAGE_ARCHITECTURE.to_owned(),
            virtualization_type: AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned(),
        },
    ])
}

fn describe_image_requirements(
    config: &IceConfig,
    region: &str,
    ami: &str,
) -> Result<AwsImageRequirements> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-images",
        "--image-ids",
        ami,
        "--output",
        "json",
        "--region",
        region,
    ]);
    let value = run_command_json(&mut command, "describe aws ami")?;
    let image = value
        .get("Images")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(|| anyhow!("AWS image `{ami}` was not found in region `{region}`."))?;
    let architecture = image
        .get("Architecture")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("AWS image `{ami}` is missing an architecture field."))?;
    let virtualization_type = image
        .get("VirtualizationType")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("AWS image `{ami}` is missing a virtualization type."))?;

    Ok(AwsImageRequirements {
        architecture: architecture.to_owned(),
        virtualization_type: virtualization_type.to_owned(),
    })
}

fn refresh_machine_offer(
    _config: &IceConfig,
    candidate: &CloudMachineCandidate,
) -> Result<CloudMachineCandidate> {
    let catalog = load_local_catalog_store().with_context(
        || "AWS create uses the local catalog only. Run `ice refresh-catalog --cloud aws` first.",
    )?;
    let entry = catalog_entry_for_candidate(&catalog, candidate)
        .cloned()
        .ok_or_else(|| match candidate.zone.as_deref() {
            Some(zone) => anyhow!(
                "Machine type `{}` in `{}` / `{zone}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`.",
                candidate.machine,
                candidate.region
            ),
            None => anyhow!(
                "Machine type `{}` in `{}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`.",
                candidate.machine,
                candidate.region
            ),
        })?;
    let zone = candidate.zone.as_ref().map(|_| entry.zone.clone());
    Ok(CloudMachineCandidate {
        machine: entry.machine,
        vcpus: entry.vcpus,
        ram_mb: entry.ram_mb,
        gpus: entry.gpus,
        hourly_usd: entry.hourly_usd,
        region: entry.region,
        zone,
    })
}

#[cfg(test)]
fn memory_mib_requirement(min_ram_gb: f64) -> u32 {
    ((min_ram_gb * 1024.0).ceil() as u32).max(1)
}

#[cfg(test)]
fn map_gpu_filter_to_aws_accelerator(value: &str) -> Option<String> {
    canonicalize_gpu_name_for_cloud(Cloud::Aws, value).and_then(|canonical| {
        match canonical.as_str() {
            "Tesla T4" => Some("t4".to_owned()),
            "A10" => Some("a10g".to_owned()),
            "H100 SXM" => Some("h100".to_owned()),
            _ => None,
        }
    })
}

fn load_live_prices_for_regions(
    regions: &[String],
    instance_types: &[String],
) -> Result<AwsLivePricing> {
    if regions.is_empty() || instance_types.is_empty() {
        return Ok(AwsLivePricing::default());
    }

    let client = aws_pricing_http_client()?;
    let wanted_instance_types = instance_types.iter().cloned().collect::<HashSet<_>>();
    let region_price_files = aws_region_price_files(&client, regions)?;
    let progress = progress_bar(
        "Loading pricing:",
        "0 region/type pairs",
        region_price_files.len() as u64,
    );
    let mut pricing = AwsLivePricing::default();
    let queue = Mutex::new(region_price_files.into_iter().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_parallelism(
        queue
            .lock()
            .expect("AWS pricing queue mutex poisoned")
            .len(),
    );

    let result = thread::scope(|scope| -> Result<AwsLivePricing> {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let queue = &queue;
            let client = &client;
            let wanted_instance_types = &wanted_instance_types;
            scope.spawn(move || {
                loop {
                    let region_price_file = {
                        let mut guard = queue.lock().expect("AWS pricing queue mutex poisoned");
                        guard.pop_front()
                    };
                    let Some((region, csv_url)) = region_price_file else {
                        break;
                    };
                    let result = load_live_price_file_for_region(
                        client,
                        &region,
                        &csv_url,
                        wanted_instance_types,
                    );
                    let _ = sender.send((region, result));
                }
            });
        }
        drop(sender);

        let mut completed = 0_u64;
        while let Ok((region, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            let region_pricing = result
                .with_context(|| format!("Failed to load AWS bulk price list in {region}"))?;
            merge_live_pricing(&mut pricing, region_pricing);
            progress.set_message(format!("{} region/type pairs", pricing.shared_prices.len()));
        }
        Ok(pricing)
    });

    match result {
        Ok(pricing) => {
            progress.finish_with_message(format!(
                "Loaded AWS pricing for {} region/type pairs.",
                pricing.shared_prices.len()
            ));
            Ok(pricing)
        }
        Err(err) => {
            progress.finish_and_clear();
            Err(err)
        }
    }
}

fn merge_live_pricing(pricing: &mut AwsLivePricing, region_pricing: AwsLivePricing) {
    for (key, hourly_usd) in region_pricing.shared_prices {
        let entry = pricing.shared_prices.entry(key).or_insert(hourly_usd);
        *entry = (*entry).min(hourly_usd);
    }
    for (key, tenancies) in region_pricing.available_tenancies {
        pricing
            .available_tenancies
            .entry(key)
            .or_default()
            .extend(tenancies);
    }
}

impl AwsBulkPriceCsvColumns {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        Ok(Self {
            term_type: csv_header_index(headers, "TermType")?,
            unit: csv_header_index(headers, "Unit")?,
            currency: csv_header_index(headers, "Currency")?,
            price_per_unit: csv_header_index(headers, "PricePerUnit")?,
            product_family: csv_header_index(headers, "Product Family")?,
            location_type: csv_header_index(headers, "Location Type")?,
            instance_type: csv_header_index(headers, "Instance Type")?,
            tenancy: csv_header_index(headers, "Tenancy")?,
            operating_system: csv_header_index(headers, "Operating System")?,
            pre_installed_software: csv_header_index(headers, "Pre Installed S/W")?,
            capacity_status: csv_header_index(headers, "CapacityStatus")?,
            region_code: csv_header_index(headers, "Region Code")?,
        })
    }
}

fn aws_pricing_http_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(AWS_PRICING_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(AWS_PRICING_REQUEST_TIMEOUT_SECS))
        .build()
        .context("Failed to build the AWS bulk pricing HTTP client")
}

fn aws_region_price_files(client: &Client, regions: &[String]) -> Result<Vec<(String, String)>> {
    let index = send_aws_pricing_request(
        || client.get(AWS_PRICE_LIST_REGION_INDEX_URL),
        "load the AWS EC2 bulk price list region index",
    )?
    .json::<AwsPriceListRegionIndex>()
    .context("Failed to decode the AWS EC2 bulk price list region index")?;
    let missing_regions = regions
        .iter()
        .filter(|region| !index.regions.contains_key(*region))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_regions.is_empty() {
        print_warning(&format!(
            "AWS bulk price list index did not include {} regions: {}",
            missing_regions.len(),
            truncate_ellipsis(&missing_regions.join(", "), 280)
        ));
    }

    let price_files = regions
        .iter()
        .filter_map(|region| {
            index.regions.get(region).map(|entry| {
                aws_price_list_csv_url(&entry.current_version_url)
                    .map(|csv_url| (region.clone(), csv_url))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if price_files.is_empty() {
        bail!("AWS bulk price list index did not include any of the requested regions.");
    }
    Ok(price_files)
}

fn aws_price_list_csv_url(current_version_url: &str) -> Result<String> {
    let prefix = current_version_url
        .strip_suffix("/index.json")
        .ok_or_else(|| anyhow!("Unexpected AWS price list URL `{current_version_url}`."))?;
    Ok(format!("{AWS_PRICE_LIST_BASE_URL}{prefix}/index.csv"))
}

fn load_live_price_file_for_region(
    client: &Client,
    region: &str,
    csv_url: &str,
    wanted_instance_types: &HashSet<String>,
) -> Result<AwsLivePricing> {
    let response = send_aws_pricing_request(
        || client.get(csv_url),
        &format!("download the AWS EC2 bulk price list for {region}"),
    )?;
    let mut pricing = AwsLivePricing::default();
    ingest_bulk_price_csv(&mut pricing, response, wanted_instance_types)?;
    Ok(pricing)
}

fn send_aws_pricing_request<F>(mut make_request: F, context: &str) -> Result<Response>
where
    F: FnMut() -> RequestBuilder,
{
    let policy = http_retry::BackoffPolicy {
        max_attempts: AWS_PRICING_MAX_ATTEMPTS,
        ..Default::default()
    };
    let attempts = policy.max_attempts.max(1);
    let mut last_error = None;

    for attempt in 1..=attempts {
        match make_request().send() {
            Ok(response) if response.status().is_success() => return Ok(response),
            Ok(response) => {
                let status = response.status();
                let detail = response
                    .text()
                    .ok()
                    .map(|text| truncate_ellipsis(text.trim(), 280))
                    .filter(|text| !text.is_empty())
                    .unwrap_or_else(|| status.to_string());
                let is_retryable =
                    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
                last_error = Some(format!("HTTP {status}: {detail}"));
                if !is_retryable || attempt == attempts {
                    bail!(
                        "Failed to {context}: {}",
                        last_error.expect("AWS pricing error detail should exist")
                    );
                }
            }
            Err(err) => {
                last_error = Some(err.to_string());
                if attempt == attempts {
                    return Err(err)
                        .with_context(|| format!("Failed to {context} (request send error)"));
                }
            }
        }

        thread::sleep(policy.delay_for_retry(attempt.saturating_sub(1)));
    }

    bail!(
        "Failed to {context}: {}",
        last_error.unwrap_or_else(|| "request failed without an error detail".to_owned())
    )
}

fn ingest_bulk_price_csv(
    pricing: &mut AwsLivePricing,
    reader: impl Read,
    wanted_instance_types: &HashSet<String>,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    skip_bulk_price_csv_metadata(&mut reader)?;
    let mut csv_reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let columns = AwsBulkPriceCsvColumns::from_headers(
        csv_reader
            .headers()
            .context("Failed to read the AWS bulk price list CSV header")?,
    )?;
    for row in csv_reader.records() {
        let row = row.context("Failed to parse an AWS bulk price list CSV row")?;
        ingest_bulk_price_csv_row(pricing, &row, columns, wanted_instance_types);
    }
    Ok(())
}

fn skip_bulk_price_csv_metadata(reader: &mut impl BufRead) -> Result<()> {
    let mut line = String::new();
    for _ in 0..AWS_PRICE_LIST_CSV_METADATA_ROWS {
        line.clear();
        if reader
            .read_line(&mut line)
            .context("Failed to read the AWS bulk price list CSV metadata")?
            == 0
        {
            bail!("AWS bulk price list CSV ended before the header row.");
        }
    }
    Ok(())
}

fn csv_header_index(headers: &StringRecord, name: &str) -> Result<usize> {
    headers
        .iter()
        .position(|header| header == name)
        .ok_or_else(|| anyhow!("AWS bulk price list CSV is missing the `{name}` column."))
}

fn ingest_bulk_price_csv_row(
    pricing: &mut AwsLivePricing,
    row: &StringRecord,
    columns: AwsBulkPriceCsvColumns,
    wanted_instance_types: &HashSet<String>,
) {
    if csv_field(row, columns.term_type) != Some("OnDemand")
        || csv_field(row, columns.unit) != Some("Hrs")
        || csv_field(row, columns.currency) != Some("USD")
        || !matches!(
            csv_field(row, columns.product_family),
            Some(AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY | AWS_BARE_METAL_PRODUCT_FAMILY)
        )
        || csv_field(row, columns.location_type) != Some(AWS_REGION_FILTER_LOCATION_TYPE)
        || csv_field(row, columns.operating_system) != Some("Linux")
        || csv_field(row, columns.pre_installed_software) != Some("NA")
        || csv_field(row, columns.capacity_status) != Some("Used")
    {
        return;
    }

    let Some(machine) = csv_field(row, columns.instance_type)
        .filter(|value| wanted_instance_types.contains(*value))
    else {
        return;
    };
    let Some(region) = csv_field(row, columns.region_code) else {
        return;
    };
    let Some(tenancy) = csv_field(row, columns.tenancy) else {
        return;
    };
    let Some(hourly_usd) = csv_field(row, columns.price_per_unit)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
    else {
        return;
    };
    let key = (region.to_owned(), machine.to_owned());
    pricing
        .available_tenancies
        .entry(key.clone())
        .or_default()
        .insert(tenancy.to_owned());
    if tenancy != "Shared" {
        return;
    }
    let entry = pricing.shared_prices.entry(key).or_insert(hourly_usd);
    *entry = (*entry).min(hourly_usd);
}

fn csv_field<'a>(row: &'a StringRecord, index: usize) -> Option<&'a str> {
    row.get(index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn load_live_machine_shapes(config: &IceConfig) -> Result<Vec<AwsMachineShape>> {
    let regions = list_active_regions(config)?;
    let progress = progress_bar("Loading machine types:", "0 types", regions.len() as u64);
    let queue = Mutex::new(regions.clone().into_iter().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_parallelism(regions.len());

    let result = thread::scope(|scope| -> Result<Vec<AwsMachineShape>> {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let queue = &queue;
            scope.spawn(move || {
                loop {
                    let region = {
                        let mut guard = queue
                            .lock()
                            .expect("AWS machine-shape queue mutex poisoned");
                        guard.pop_front()
                    };
                    let Some(region) = region else {
                        break;
                    };
                    let result = load_live_machine_shapes_for_region(config, &region);
                    let _ = sender.send((region, result));
                }
            });
        }
        drop(sender);

        let mut shapes = Vec::new();
        let mut completed = 0_u64;
        while let Ok((region, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            let mut region_shapes =
                result.with_context(|| format!("Failed to load AWS machine shapes in {region}"))?;
            shapes.append(&mut region_shapes);
            progress.set_message(format!("{} types", shapes.len()));
        }
        Ok(shapes)
    });

    match result {
        Ok(shapes) => {
            progress.finish_with_message(format!(
                "Loaded {} AWS machine types from {} regions.",
                shapes.len(),
                regions.len(),
            ));
            Ok(shapes)
        }
        Err(err) => {
            progress.finish_and_clear();
            Err(err)
        }
    }
}

fn load_live_machine_shapes_for_region(
    config: &IceConfig,
    region: &str,
) -> Result<Vec<AwsMachineShape>> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-instance-types",
        "--output",
        "json",
        "--region",
        region,
    ]);
    let value = run_command_json(
        &mut command,
        &format!("describe aws instance types in {region}"),
    )?;
    Ok(parse_machine_shapes(&value, region))
}

#[cfg(test)]
fn parse_machine_shape(value: &Value, region: &str) -> Option<AwsMachineShape> {
    value
        .get("InstanceTypes")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| parse_machine_shape_row(row, region))
}

fn parse_machine_shapes(value: &Value, region: &str) -> Vec<AwsMachineShape> {
    value
        .get("InstanceTypes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| parse_machine_shape_row(row, region))
        .collect()
}

fn parse_machine_shape_row(row: &Value, region: &str) -> Option<AwsMachineShape> {
    let machine = row.get("InstanceType")?.as_str()?.to_owned();
    let vcpus = row
        .get("VCpuInfo")?
        .get("DefaultVCpus")?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())?;
    let ram_mib = row
        .get("MemoryInfo")?
        .get("SizeInMiB")?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())?;
    let gpus = row
        .get("GpuInfo")
        .and_then(|value| value.get("Gpus"))
        .and_then(Value::as_array)
        .map(|rows| expand_gpu_labels(rows))
        .unwrap_or_default();

    Some(AwsMachineShape {
        machine,
        region: region.to_owned(),
        vcpus,
        ram_mb: ((f64::from(ram_mib)) * 1.048_576).round() as u32,
        gpus,
        has_accelerators: machine_has_accelerators(row),
        architecture: parse_supported_architecture(row)?,
        virtualization_types: parse_supported_virtualization_types(row)?,
    })
}

fn parse_supported_architecture(row: &Value) -> Option<String> {
    let architectures = row
        .get("ProcessorInfo")?
        .get("SupportedArchitectures")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    if architectures
        .iter()
        .any(|value| value.eq_ignore_ascii_case(AWS_DEFAULT_IMAGE_ARCHITECTURE))
    {
        return Some(AWS_DEFAULT_IMAGE_ARCHITECTURE.to_owned());
    }
    if architectures
        .iter()
        .any(|value| value.eq_ignore_ascii_case(AWS_ARM64_IMAGE_ARCHITECTURE))
    {
        return Some(AWS_ARM64_IMAGE_ARCHITECTURE.to_owned());
    }
    architectures.first().map(|value| (*value).to_owned())
}

fn parse_supported_virtualization_types(row: &Value) -> Option<Vec<String>> {
    let mut types = row
        .get("SupportedVirtualizationTypes")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    types.sort();
    types.dedup();
    (!types.is_empty()).then_some(types)
}

fn machine_has_accelerators(row: &Value) -> bool {
    [
        "GpuInfo",
        "FpgaInfo",
        "InferenceAcceleratorInfo",
        "NeuronInfo",
    ]
    .into_iter()
    .any(|field| row.get(field).is_some())
}

fn expand_gpu_labels(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .flat_map(|row| {
            let label = row
                .get("Name")
                .and_then(Value::as_str)
                .map(normalize_aws_gpu_label)
                .unwrap_or_else(|| "GPU".to_owned());
            let count = row
                .get("Count")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(1)
                .max(1);
            std::iter::repeat_n(label, count)
        })
        .collect()
}

fn normalize_aws_gpu_label(raw: &str) -> String {
    canonicalize_gpu_name_for_cloud(Cloud::Aws, raw).unwrap_or_else(|| raw.trim().to_owned())
}

pub(crate) fn cached_machine_hourly_price(machine: &str) -> Option<f64> {
    load_local_catalog_store()
        .ok()?
        .entries
        .iter()
        .filter(|entry| entry.machine.eq_ignore_ascii_case(machine))
        .map(|entry| entry.hourly_usd)
        .min_by(|left, right| left.total_cmp(right))
}

fn aws_catalog_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "machine-catalog.toml")
}

fn load_local_catalog_store() -> Result<Arc<AwsMachineCatalogStore>> {
    load_cached_arc(
        &AWS_LOCAL_CATALOG_STORE_CACHE,
        || {
            let path = aws_catalog_path()?;
            let content = fs::read_to_string(&path).with_context(|| {
                format!("Failed to read local AWS catalog at {}", path.display())
            })?;
            if content.trim().is_empty() {
                bail!(
                    "Local AWS catalog at {} is empty. Run `ice refresh-catalog --cloud aws`.",
                    path.display()
                );
            }
            let store = toml::from_str::<AwsMachineCatalogStore>(&content).with_context(|| {
                format!("Failed to parse local AWS catalog at {}", path.display())
            })?;
            if store.entries.is_empty() {
                bail!(
                    "Local AWS catalog at {} contains no priced machine entries. Run `ice refresh-catalog --cloud aws`.",
                    path.display()
                );
            }
            if store.entries.iter().any(|entry| {
                entry.zone.trim().is_empty()
                    || entry.ram_mb == 0
                    || entry.architecture.trim().is_empty()
                    || entry.virtualization_types.is_empty()
            }) {
                bail!(
                    "Local AWS catalog at {} contains legacy compatibility data. Run `ice refresh-catalog --cloud aws`.",
                    path.display()
                );
            }
            Ok(store)
        },
        "AWS runtime-data",
    )
}

fn load_previous_local_catalog_store() -> Result<Option<AwsMachineCatalogStore>> {
    let path = aws_catalog_path()?;
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("Failed to read local AWS catalog at {}", path.display())
            });
        }
    };
    if content.trim().is_empty() {
        return Ok(None);
    }
    let store = toml::from_str::<AwsMachineCatalogStore>(&content)
        .with_context(|| format!("Failed to parse local AWS catalog at {}", path.display()))?;
    Ok(Some(store))
}

fn save_local_catalog_store(store: &AwsMachineCatalogStore) -> Result<PathBuf> {
    let path = aws_catalog_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS catalog path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content = toml::to_string_pretty(store).context("Failed to serialize local AWS catalog")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&AWS_LOCAL_CATALOG_STORE_CACHE, "AWS runtime-data")?;
    Ok(path)
}

fn warn_if_catalog_stale(store: &AwsMachineCatalogStore) {
    if store.refreshed_at_unix == 0 {
        print_warning(
            "AWS catalog has no refresh timestamp; search quality may be degraded. Run `ice refresh-catalog --cloud aws`.",
        );
        return;
    }
    let age_secs = now_unix_secs().saturating_sub(store.refreshed_at_unix);
    if age_secs > AWS_LOCAL_CATALOG_MAX_AGE_SECS {
        let age_days = age_secs as f64 / 86_400.0;
        print_warning(&format!(
            "AWS catalog is {:.1} days old; search quality may be degraded. Run `ice refresh-catalog --cloud aws`.",
            age_days
        ));
    }
}

fn changed_catalog_entry_count(
    previous: &[AwsMachineCatalogEntry],
    current: &[AwsMachineCatalogEntry],
) -> usize {
    let previous_by_key = previous
        .iter()
        .map(|entry| {
            (
                (
                    entry.region.clone(),
                    entry.zone.clone(),
                    entry.machine.clone(),
                ),
                entry,
            )
        })
        .collect::<HashMap<_, _>>();
    let current_by_key = current
        .iter()
        .map(|entry| {
            (
                (
                    entry.region.clone(),
                    entry.zone.clone(),
                    entry.machine.clone(),
                ),
                entry,
            )
        })
        .collect::<HashMap<_, _>>();
    let updated_or_added = current_by_key
        .iter()
        .filter(|(key, entry)| match previous_by_key.get(*key) {
            Some(previous_entry) => *previous_entry != **entry,
            None => true,
        })
        .count();
    let deleted = previous_by_key
        .keys()
        .filter(|key| !current_by_key.contains_key(*key))
        .count();
    updated_or_added + deleted
}

fn aws_catalog_warning_priority(warning: &AwsCatalogWarning) -> u8 {
    match warning {
        AwsCatalogWarning::MissingPrice { .. } => 0,
        AwsCatalogWarning::MissingInstanceOffering { .. } => 1,
        AwsCatalogWarning::HostTenancyPriceOnly { .. } => 2,
    }
}

fn sorted_catalog_warnings(
    warnings: &BTreeMap<AwsCatalogWarning, Vec<String>>,
) -> Vec<(&AwsCatalogWarning, &Vec<String>)> {
    let mut groups = warnings.iter().collect::<Vec<_>>();
    groups.sort_by(|(left_warning, left_zones), (right_warning, right_zones)| {
        aws_catalog_warning_priority(left_warning)
            .cmp(&aws_catalog_warning_priority(right_warning))
            .then_with(|| right_zones.len().max(1).cmp(&left_zones.len().max(1)))
            .then_with(|| left_warning.cmp(right_warning))
    });
    groups
}

fn render_catalog_warnings(warnings: &BTreeMap<AwsCatalogWarning, Vec<String>>) -> Vec<String> {
    sorted_catalog_warnings(warnings)
        .into_iter()
        .take(8)
        .map(|(warning, zones)| match warning {
            AwsCatalogWarning::MissingInstanceOffering { machine, region } => format!(
                "missing AWS zone offering for `{machine}` in `{region}`: returned by the live instance-type list but not by the zone-offering list."
            ),
            AwsCatalogWarning::MissingPrice { machine, region } => {
                let preview = zones.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
                format!(
                    "{} zones: missing live AWS price for `{machine}` in `{region}`. Examples: {preview}",
                    zones.len()
                )
            }
            AwsCatalogWarning::HostTenancyPriceOnly { machine, region } => {
                let preview = zones.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
                format!(
                    "{} zones: live AWS price for `{machine}` in `{region}` exists only with `Host` tenancy. Examples: {preview}",
                    zones.len()
                )
            }
        })
        .collect()
}

fn catalog_entry_for_candidate<'a>(
    catalog: &'a AwsMachineCatalogStore,
    candidate: &CloudMachineCandidate,
) -> Option<&'a AwsMachineCatalogEntry> {
    catalog
        .entries
        .iter()
        .filter(|entry| {
            entry.machine.eq_ignore_ascii_case(&candidate.machine)
                && entry.region.eq_ignore_ascii_case(&candidate.region)
                && candidate
                    .zone
                    .as_deref()
                    .is_none_or(|zone| entry.zone.eq_ignore_ascii_case(zone))
        })
        .min_by(|left, right| left.zone.cmp(&right.zone))
}

fn catalog_entry_matches_image_requirements(
    entry: &AwsMachineCatalogEntry,
    image_requirements: &[AwsImageRequirements],
) -> bool {
    image_requirements.iter().any(|image| {
        entry.architecture.eq_ignore_ascii_case(&image.architecture)
            && entry
                .virtualization_types
                .iter()
                .any(|value| value.eq_ignore_ascii_case(&image.virtualization_type))
    })
}

fn select_cheapest_machine_candidate(
    catalog: &[AwsMachineCatalogEntry],
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
    image_requirements: &[AwsImageRequirements],
    preferred_region: Option<&str>,
    search_zone: Option<&str>,
) -> Result<CloudMachineCandidate> {
    let override_name = machine_override
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let allowed_gpu_set = req
        .allowed_gpus
        .iter()
        .map(|gpu| {
            canonicalize_gpu_name_for_cloud(Cloud::Aws, gpu)
                .unwrap_or_else(|| gpu.trim().to_owned())
        })
        .map(|gpu| normalize_gpu_name_token(&gpu))
        .collect::<HashSet<_>>();
    let min_ram_mb = req.min_ram_gb * 1000.0;
    let mut unique_region_matches = BTreeMap::new();
    let mut zonal_matches = Vec::new();

    for entry in catalog {
        if let Some(region) = preferred_region
            && !entry.region.eq_ignore_ascii_case(region)
        {
            continue;
        }
        if let Some(zone) = search_zone
            && !entry.zone.eq_ignore_ascii_case(zone)
        {
            continue;
        }
        if let Some(name) = override_name
            && !entry.machine.eq_ignore_ascii_case(name)
        {
            continue;
        }
        if entry.vcpus < req.min_cpus || f64::from(entry.ram_mb) + 0.000_001 < min_ram_mb {
            continue;
        }
        if !catalog_entry_matches_image_requirements(entry, image_requirements) {
            continue;
        }
        if allowed_gpu_set.is_empty() {
            if entry.has_accelerators {
                continue;
            }
        } else {
            let gpu_match = entry.gpus.iter().any(|gpu| {
                let canonical = canonicalize_gpu_name_for_cloud(Cloud::Aws, gpu)
                    .unwrap_or_else(|| gpu.trim().to_owned());
                allowed_gpu_set.contains(&normalize_gpu_name_token(&canonical))
            });
            if !gpu_match {
                continue;
            }
        }
        if search_zone.is_some() {
            zonal_matches.push(entry.clone());
        } else {
            unique_region_matches
                .entry((entry.region.clone(), entry.machine.clone()))
                .or_insert_with(|| entry.clone());
        }
    }

    let mut candidates = if search_zone.is_some() {
        zonal_matches
    } else {
        unique_region_matches.into_values().collect::<Vec<_>>()
    };

    if candidates.is_empty() {
        bail!(
            "No aws machine type matches filters (min_cpus={}, min_ram_gb={}, allowed_gpus=[{}]){}.",
            req.min_cpus,
            req.min_ram_gb,
            req.allowed_gpus.join(", "),
            override_name
                .map(|name| format!(", machine={name}"))
                .unwrap_or_default()
        );
    }

    candidates.sort_by(|left, right| {
        left.hourly_usd
            .total_cmp(&right.hourly_usd)
            .then_with(|| {
                let left_pref = preferred_region
                    .map(|region| left.region.eq_ignore_ascii_case(region))
                    .unwrap_or(false);
                let right_pref = preferred_region
                    .map(|region| right.region.eq_ignore_ascii_case(region))
                    .unwrap_or(false);
                right_pref.cmp(&left_pref)
            })
            .then_with(|| {
                let left_pref = search_zone
                    .map(|zone| left.zone.eq_ignore_ascii_case(zone))
                    .unwrap_or(false);
                let right_pref = search_zone
                    .map(|zone| right.zone.eq_ignore_ascii_case(zone))
                    .unwrap_or(false);
                right_pref.cmp(&left_pref)
            })
            .then_with(|| left.region.cmp(&right.region))
            .then_with(|| left.machine.cmp(&right.machine))
            .then_with(|| left.zone.cmp(&right.zone))
    });

    let winner = candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No priced AWS candidate after sort"))?;
    Ok(CloudMachineCandidate {
        machine: winner.machine,
        vcpus: winner.vcpus,
        ram_mb: winner.ram_mb,
        gpus: winner.gpus,
        hourly_usd: winner.hourly_usd,
        region: winner.region,
        zone: search_zone.map(|_| winner.zone),
    })
}

fn load_live_instance_offerings(
    config: &IceConfig,
    regions: &[String],
) -> Result<HashMap<(String, String), Vec<String>>> {
    let progress = progress_bar(
        "Loading offerings:",
        "0 region/type pairs",
        regions.len() as u64,
    );
    let mut offerings = HashMap::<(String, String), BTreeSet<String>>::new();
    let queue = Mutex::new(regions.iter().cloned().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_parallelism(regions.len());

    let result = thread::scope(|scope| -> Result<HashMap<(String, String), Vec<String>>> {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let queue = &queue;
            scope.spawn(move || {
                loop {
                    let region = {
                        let mut guard = queue.lock().expect("AWS offering queue mutex poisoned");
                        guard.pop_front()
                    };
                    let Some(region) = region else {
                        break;
                    };
                    let result = load_live_instance_offerings_for_region(config, &region);
                    let _ = sender.send((region, result));
                }
            });
        }
        drop(sender);

        let mut completed = 0_u64;
        while let Ok((region, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            for (key, zones) in result
                .with_context(|| format!("Failed to load AWS instance offerings in {region}"))?
            {
                offerings.entry(key).or_default().extend(zones);
            }
            progress.set_message(format!("{} region/type pairs", offerings.len()));
        }
        Ok(offerings
            .into_iter()
            .map(|(key, zones)| (key, zones.into_iter().collect()))
            .collect())
    });

    match result {
        Ok(offerings) => {
            progress.finish_with_message(format!(
                "Loaded AWS instance offerings for {} region/type pairs.",
                offerings.len(),
            ));
            Ok(offerings)
        }
        Err(err) => {
            progress.finish_and_clear();
            Err(err)
        }
    }
}

fn load_live_instance_offerings_for_region(
    config: &IceConfig,
    region: &str,
) -> Result<HashMap<(String, String), BTreeSet<String>>> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-instance-type-offerings",
        "--location-type",
        "availability-zone",
        "--output",
        "json",
        "--region",
        region,
    ]);
    let value = run_command_json(
        &mut command,
        &format!("describe aws instance type offerings in {region}"),
    )?;
    let mut offerings = HashMap::<(String, String), BTreeSet<String>>::new();
    for row in value
        .get("InstanceTypeOfferings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(machine) = row
            .get("InstanceType")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        let Some(zone) = row
            .get("Location")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        offerings
            .entry((region.to_owned(), machine))
            .or_default()
            .insert(zone);
    }
    Ok(offerings)
}

pub(crate) fn refresh_local_catalog(config: &IceConfig) -> Result<RefreshCatalogOutcome> {
    let previous_store = match load_previous_local_catalog_store() {
        Ok(store) => store,
        Err(err) => {
            print_warning(&format!(
                "Failed to compare refreshed AWS catalog against the previous cache: \
                 {err:#}. Reporting all entries as changed."
            ));
            None
        }
    };
    let shapes = load_live_machine_shapes(config)?;
    let regions = shapes
        .iter()
        .map(|shape| shape.region.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let offerings = load_live_instance_offerings(config, &regions)?;
    let unique_types = shapes
        .iter()
        .map(|shape| shape.machine.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let prices = load_live_prices_for_regions(&regions, &unique_types)?;
    let mut warnings = BTreeMap::<AwsCatalogWarning, Vec<String>>::new();
    let mut entries = Vec::new();

    for shape in shapes {
        let key = (shape.region.clone(), shape.machine.clone());
        let Some(zones) = offerings.get(&key) else {
            warnings
                .entry(AwsCatalogWarning::MissingInstanceOffering {
                    machine: shape.machine,
                    region: shape.region,
                })
                .or_default();
            continue;
        };
        let Some(hourly_usd) = prices.shared_prices.get(&key).copied() else {
            let warning = if prices
                .available_tenancies
                .get(&key)
                .is_some_and(|tenancies| tenancies.contains("Host"))
            {
                AwsCatalogWarning::HostTenancyPriceOnly {
                    machine: shape.machine,
                    region: shape.region,
                }
            } else {
                AwsCatalogWarning::MissingPrice {
                    machine: shape.machine,
                    region: shape.region,
                }
            };
            warnings
                .entry(warning)
                .or_default()
                .extend(zones.iter().cloned());
            continue;
        };
        for zone in zones {
            entries.push(AwsMachineCatalogEntry {
                machine: shape.machine.clone(),
                region: shape.region.clone(),
                zone: zone.clone(),
                vcpus: shape.vcpus,
                ram_mb: shape.ram_mb,
                gpus: shape.gpus.clone(),
                has_accelerators: shape.has_accelerators,
                architecture: shape.architecture.clone(),
                virtualization_types: shape.virtualization_types.clone(),
                hourly_usd,
            });
        }
    }

    entries.sort_by(|left, right| {
        left.machine
            .cmp(&right.machine)
            .then_with(|| left.region.cmp(&right.region))
            .then_with(|| left.zone.cmp(&right.zone))
    });
    if entries.is_empty() {
        bail!(
            "No AWS machine types were priced and offered. Check AWS bulk pricing downloads and rerun `ice refresh-catalog --cloud aws`."
        );
    }
    let store = AwsMachineCatalogStore {
        refreshed_at_unix: now_unix_secs(),
        entries,
    };
    let changed_entry_count = previous_store
        .as_ref()
        .map_or(store.entries.len(), |previous| {
            changed_catalog_entry_count(&previous.entries, &store.entries)
        });
    let path = save_local_catalog_store(&store)?;
    let warning_count = warnings.values().map(|zones| zones.len().max(1)).sum();
    let warning_summary = render_catalog_warnings(&warnings);
    Ok(RefreshCatalogOutcome {
        path,
        entry_count: store.entries.len(),
        changed_entry_count,
        warning_count,
        warning_summary,
    })
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
    let regions = regions_to_query(config)?;
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
    let ami = if let Some(ami) = config
        .default
        .aws
        .ami
        .as_deref()
        .filter(|ami| !ami.trim().is_empty())
    {
        ami.to_owned()
    } else {
        let catalog = load_local_catalog_store().with_context(
            || "AWS create uses the local catalog only. Run `ice refresh-catalog --cloud aws` first.",
        )?;
        let entry = catalog_entry_for_candidate(&catalog, candidate).ok_or_else(|| {
            anyhow!(
                "Machine type `{}` in `{}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`.",
                candidate.machine,
                candidate.region
            )
        })?;
        lookup_default_ami(config, &region, &entry.architecture)?
    };
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

fn preferred_region(config: &IceConfig) -> Result<Option<String>> {
    if let Some(region) = config
        .default
        .aws
        .region
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(Some(region.to_owned()));
    }
    for key in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(Some(value.to_owned()));
            }
        }
    }
    Ok(None)
}

fn regions_to_query(config: &IceConfig) -> Result<Vec<String>> {
    if let Some(region) = preferred_region(config)? {
        return Ok(vec![region.to_owned()]);
    }
    list_active_regions(config)
}

fn validated_preferred_region(config: &IceConfig) -> Result<Option<String>> {
    if preferred_region(config)?.is_none() {
        if config
            .default
            .aws
            .subnet_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        {
            bail!(
                "Set `default.aws.region` or `AWS_REGION` when `default.aws.subnet_id` is configured."
            );
        }
        if config
            .default
            .aws
            .security_group_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
        {
            bail!(
                "Set `default.aws.region` or `AWS_REGION` when `default.aws.security_group_id` is configured."
            );
        }
    }
    preferred_region(config)
}

#[cfg(test)]
fn search_regions_to_query(config: &IceConfig) -> Result<Vec<String>> {
    if let Some(region) = validated_preferred_region(config)? {
        return Ok(vec![region]);
    }
    regions_to_query(config)
}

fn resolve_search_zone(config: &IceConfig, region: Option<&str>) -> Result<Option<String>> {
    let Some(subnet_id) = config
        .default
        .aws
        .subnet_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let region = region.ok_or_else(|| {
        anyhow!(
            "Set `default.aws.region` or `AWS_REGION` when `default.aws.subnet_id` is configured."
        )
    })?;
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-subnets",
        "--subnet-ids",
        subnet_id,
        "--query",
        "Subnets[0].AvailabilityZone",
        "--output",
        "text",
        "--region",
        region,
    ]);
    let zone = run_command_text(&mut command, "resolve aws subnet availability zone")?;
    let zone = zone.trim();
    if zone.is_empty() || zone.eq_ignore_ascii_case("None") {
        bail!("AWS subnet `{subnet_id}` has no availability zone.");
    }
    Ok(Some(zone.to_owned()))
}

fn list_active_regions(config: &IceConfig) -> Result<Vec<String>> {
    let mut command = command(config, AWS_PRICE_LIST_REGION);
    command.args([
        "ec2",
        "describe-regions",
        "--all-regions",
        "--filters",
        "Name=opt-in-status,Values=opt-in-not-required,opted-in",
        "--query",
        "Regions[].RegionName",
        "--output",
        "json",
        "--region",
        AWS_PRICE_LIST_REGION,
    ]);
    let value = run_command_json(&mut command, "list AWS regions")?;
    let mut regions = value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    regions.sort();
    regions.dedup();
    if regions.is_empty() {
        bail!("AWS region list is empty.");
    }
    Ok(regions)
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

fn lookup_default_ami(config: &IceConfig, region: &str, architecture: &str) -> Result<String> {
    let parameter = match architecture {
        AWS_DEFAULT_IMAGE_ARCHITECTURE => AWS_DEFAULT_AMI_PARAMETER_X86_64,
        AWS_ARM64_IMAGE_ARCHITECTURE => AWS_DEFAULT_AMI_PARAMETER_ARM64,
        other => bail!("Unsupported AWS architecture `{other}` for default AMI lookup."),
    };
    let mut command = command(config, region);
    command.args([
        "ssm",
        "get-parameter",
        "--name",
        parameter,
        "--query",
        "Parameter.Value",
        "--output",
        "text",
        "--region",
        region,
    ]);
    run_command_text(&mut command, "lookup default aws ami")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CreateSearchRequirements;
    use serde_json::json;

    fn test_catalog_entry(
        machine: &str,
        region: &str,
        zone: &str,
        gpus: &[&str],
        has_accelerators: bool,
        architecture: &str,
        hourly_usd: f64,
    ) -> AwsMachineCatalogEntry {
        AwsMachineCatalogEntry {
            machine: machine.to_owned(),
            region: region.to_owned(),
            zone: zone.to_owned(),
            vcpus: 4,
            ram_mb: 16_384,
            gpus: gpus.iter().map(|gpu| (*gpu).to_owned()).collect(),
            has_accelerators,
            architecture: architecture.to_owned(),
            virtualization_types: vec![AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned()],
            hourly_usd,
        }
    }

    #[test]
    fn machine_shape_parser_expands_gpu_counts() {
        let value = json!({
            "InstanceTypes": [{
                "InstanceType": "g5.12xlarge",
                "VCpuInfo": {"DefaultVCpus": 48},
                "MemoryInfo": {"SizeInMiB": 196608},
                "ProcessorInfo": {"SupportedArchitectures": ["x86_64"]},
                "SupportedVirtualizationTypes": ["hvm"],
                "GpuInfo": {
                    "Gpus": [{
                        "Name": "NVIDIA A10G Tensor Core",
                        "Count": 4
                    }]
                }
            }]
        });

        let shape = parse_machine_shape(&value, "us-east-1").expect("shape should parse");
        assert_eq!(shape.machine, "g5.12xlarge");
        assert_eq!(shape.region, "us-east-1");
        assert_eq!(shape.vcpus, 48);
        assert_eq!(shape.ram_mb, 206_158);
        assert_eq!(shape.gpus, vec!["A10", "A10", "A10", "A10"]);
        assert!(shape.has_accelerators);
        assert_eq!(shape.architecture, AWS_DEFAULT_IMAGE_ARCHITECTURE);
        assert_eq!(
            shape.virtualization_types,
            vec![AWS_DEFAULT_VIRTUALIZATION_TYPE]
        );
    }

    #[test]
    fn aws_gpu_filters_map_to_accelerator_names() {
        assert_eq!(
            map_gpu_filter_to_aws_accelerator("Tesla T4"),
            Some("t4".to_owned())
        );
        assert_eq!(
            map_gpu_filter_to_aws_accelerator("A10"),
            Some("a10g".to_owned())
        );
        assert_eq!(
            map_gpu_filter_to_aws_accelerator("H100 SXM"),
            Some("h100".to_owned())
        );
    }

    #[test]
    fn search_regions_require_region_when_subnet_is_pinned() {
        let mut config = IceConfig::default();
        config.default.aws.subnet_id = Some("subnet-123".to_owned());
        let error =
            search_regions_to_query(&config).expect_err("subnet without region should fail");
        assert!(error.to_string().contains("default.aws.region"));
    }

    #[test]
    fn default_search_image_requirements_include_x86_and_arm() {
        let images = resolve_search_image_requirements(&IceConfig::default(), None)
            .expect("default search images");
        assert_eq!(images.len(), 2);
        assert!(
            images
                .iter()
                .any(|image| image.architecture == AWS_DEFAULT_IMAGE_ARCHITECTURE)
        );
        assert!(
            images
                .iter()
                .any(|image| image.architecture == AWS_ARM64_IMAGE_ARCHITECTURE)
        );
    }

    #[test]
    fn memory_requirement_rounds_fractional_gib_up() {
        assert_eq!(memory_mib_requirement(0.5), 512);
        assert_eq!(memory_mib_requirement(1.25), 1280);
    }

    #[test]
    fn aws_catalog_selection_prefers_cpu_only_when_gpu_filter_is_empty() {
        let req = CreateSearchRequirements {
            min_cpus: 1,
            min_ram_gb: 1.0,
            allowed_gpus: Vec::new(),
            max_price_per_hr: 10.0,
        };
        let catalog = vec![
            test_catalog_entry(
                "g5.xlarge",
                "us-east-1",
                "us-east-1a",
                &["A10"],
                true,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                1.10,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                "us-east-1a",
                &[],
                false,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                0.09,
            ),
        ];

        let candidate = select_cheapest_machine_candidate(
            &catalog,
            &req,
            None,
            &[AwsImageRequirements {
                architecture: AWS_DEFAULT_IMAGE_ARCHITECTURE.to_owned(),
                virtualization_type: AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned(),
            }],
            Some("us-east-1"),
            None,
        )
        .expect("candidate");
        assert_eq!(candidate.machine, "c7i.large");
        assert!(candidate.zone.is_none());
    }

    #[test]
    fn aws_catalog_selection_honors_gpu_filter() {
        let req = CreateSearchRequirements {
            min_cpus: 1,
            min_ram_gb: 1.0,
            allowed_gpus: vec!["A10".to_owned()],
            max_price_per_hr: 10.0,
        };
        let catalog = vec![
            test_catalog_entry(
                "g5.xlarge",
                "us-east-1",
                "us-east-1a",
                &["NVIDIA A10G Tensor Core"],
                true,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                1.10,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                "us-east-1a",
                &[],
                false,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                0.09,
            ),
        ];

        let candidate = select_cheapest_machine_candidate(
            &catalog,
            &req,
            None,
            &[AwsImageRequirements {
                architecture: AWS_DEFAULT_IMAGE_ARCHITECTURE.to_owned(),
                virtualization_type: AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned(),
            }],
            Some("us-east-1"),
            None,
        )
        .expect("candidate");
        assert_eq!(candidate.machine, "g5.xlarge");
    }

    #[test]
    fn aws_catalog_selection_honors_image_architecture_and_zone() {
        let req = CreateSearchRequirements {
            min_cpus: 1,
            min_ram_gb: 1.0,
            allowed_gpus: Vec::new(),
            max_price_per_hr: 10.0,
        };
        let catalog = vec![
            test_catalog_entry(
                "c7g.large",
                "us-east-1",
                "us-east-1a",
                &[],
                false,
                AWS_ARM64_IMAGE_ARCHITECTURE,
                0.05,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                "us-east-1b",
                &[],
                false,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                0.08,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                "us-east-1a",
                &[],
                false,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                0.08,
            ),
        ];

        let candidate = select_cheapest_machine_candidate(
            &catalog,
            &req,
            None,
            &[AwsImageRequirements {
                architecture: AWS_DEFAULT_IMAGE_ARCHITECTURE.to_owned(),
                virtualization_type: AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned(),
            }],
            Some("us-east-1"),
            Some("us-east-1a"),
        )
        .expect("candidate");
        assert_eq!(candidate.machine, "c7i.large");
        assert_eq!(candidate.zone.as_deref(), Some("us-east-1a"));
    }

    #[test]
    fn bulk_price_csv_ingest_accepts_standard_and_bare_metal_linux_rows() {
        let csv = r#""FormatVersion","v1.0"
"Disclaimer","test"
"Publication Date","2026-03-05T20:59:55Z"
"Version","20260305205955"
"OfferCode","AmazonEC2"
"SKU","OfferTermCode","RateCode","TermType","PriceDescription","EffectiveDate","StartingRange","EndingRange","Unit","PricePerUnit","Currency","Product Family","Location Type","Instance Type","Tenancy","Operating System","Pre Installed S/W","CapacityStatus","Region Code"
"sku-1","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","0.1234000000","USD","Compute Instance","AWS Region","c7i.large","Shared","Linux","NA","Used","us-east-1"
"sku-2","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","0.4567000000","USD","Compute Instance","AWS Region","c7i.large","Dedicated","Linux","NA","Used","us-east-1"
"sku-3","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","0.7890000000","USD","Compute Instance","AWS Region","m7i.large","Shared","Windows","NA","Used","us-east-1"
"sku-4","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","0.1111000000","USD","Compute Instance","AWS Region","c7i.large","Shared","Linux","NA","Used","us-east-1"
"sku-5","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","4.0800000000","USD","Compute Instance (bare metal)","AWS Region","c5.metal","Shared","Linux","NA","Used","us-east-1"
"#;
        let wanted_instance_types = HashSet::from(["c5.metal".to_owned(), "c7i.large".to_owned()]);
        let mut pricing = AwsLivePricing::default();

        ingest_bulk_price_csv(
            &mut pricing,
            std::io::Cursor::new(csv.as_bytes()),
            &wanted_instance_types,
        )
        .expect("AWS bulk price CSV should parse");

        assert_eq!(
            pricing
                .shared_prices
                .get(&(String::from("us-east-1"), String::from("c5.metal"))),
            Some(&4.08)
        );
        assert_eq!(
            pricing
                .shared_prices
                .get(&(String::from("us-east-1"), String::from("c7i.large"))),
            Some(&0.1111)
        );
        assert_eq!(pricing.shared_prices.len(), 2);
    }

    #[test]
    fn bulk_price_csv_ingest_tracks_host_only_rows() {
        let csv = r#""FormatVersion","v1.0"
"Disclaimer","test"
"Publication Date","2026-03-05T20:59:55Z"
"Version","20260305205955"
"OfferCode","AmazonEC2"
"SKU","OfferTermCode","RateCode","TermType","PriceDescription","EffectiveDate","StartingRange","EndingRange","Unit","PricePerUnit","Currency","Product Family","Location Type","Instance Type","Tenancy","Operating System","Pre Installed S/W","CapacityStatus","Region Code"
"sku-1","term","rate","OnDemand","desc","2026-03-01","0","Inf","Hrs","14.0000000000","USD","Compute Instance (bare metal)","AWS Region","mac2.metal","Host","Linux","NA","Used","us-east-1"
"#;
        let wanted_instance_types = HashSet::from(["mac2.metal".to_owned()]);
        let mut pricing = AwsLivePricing::default();

        ingest_bulk_price_csv(
            &mut pricing,
            std::io::Cursor::new(csv.as_bytes()),
            &wanted_instance_types,
        )
        .expect("AWS bulk price CSV should parse");

        assert!(pricing.shared_prices.is_empty());
        assert!(
            pricing
                .available_tenancies
                .get(&(String::from("us-east-1"), String::from("mac2.metal")))
                .is_some_and(|tenancies| tenancies.contains("Host"))
        );
    }

    #[test]
    fn sorted_catalog_warnings_prioritizes_true_missing_prices() {
        let mut warnings = BTreeMap::new();
        warnings.insert(
            AwsCatalogWarning::HostTenancyPriceOnly {
                machine: "mac2.metal".to_owned(),
                region: "us-east-1".to_owned(),
            },
            vec!["us-east-1a".to_owned(), "us-east-1b".to_owned()],
        );
        warnings.insert(
            AwsCatalogWarning::MissingPrice {
                machine: "trn2.3xlarge".to_owned(),
                region: "sa-east-1".to_owned(),
            },
            vec!["sa-east-1a".to_owned()],
        );

        let sorted = sorted_catalog_warnings(&warnings);
        assert!(matches!(
            sorted.first(),
            Some((AwsCatalogWarning::MissingPrice { .. }, _))
        ));
    }

    #[test]
    fn price_list_csv_url_uses_versioned_region_file_path() {
        assert_eq!(
            aws_price_list_csv_url(
                "/offers/v1.0/aws/AmazonEC2/20260305205955/us-east-1/index.json"
            )
            .expect("CSV URL should parse"),
            "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/20260305205955/us-east-1/index.csv"
        );
    }
}
