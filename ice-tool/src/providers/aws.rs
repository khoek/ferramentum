use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
use crate::providers::catalog::{
    MachineRegionEntry, MachineRegionKey, MachineRegionSkipEntry, RefreshCatalogOutcome,
    build_machine_region_index, changed_entry_count_by_key, machine_region_skip_reason,
    skipped_machine_region_key_set,
};
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
    now_unix_secs, prefix_lookup_indices, progress_bar, render_command_line, run_command_json,
    run_command_output, run_command_status, run_command_text, spinner, truncate_ellipsis,
    visible_instance_name, write_temp_file,
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
const AWS_MACHINE_SHAPE_CACHE_MAX_AGE_SECS: u64 = 30 * 24 * 60 * 60;
const AWS_REGION_OFFERINGS_CACHE_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const AWS_ZONE_OFFERINGS_CACHE_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const AWS_PRICE_LIST_REGION_INDEX_URL: &str =
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/region_index.json";
const AWS_PRICING_MAX_RETRIES: u32 = 10;
const AWS_PRICING_MAX_ATTEMPTS: u32 = AWS_PRICING_MAX_RETRIES + 1;
const AWS_PRICING_CONNECT_TIMEOUT_SECS: u64 = 30;
const AWS_PRICING_REQUEST_TIMEOUT_SECS: u64 = 1800;
const AWS_PRICING_MAX_PARALLEL_REQUESTS: usize = 4;
const AWS_INSTANCE_TYPES_PER_REQUEST: usize = 100;
const AWS_PRICING_QUERY_PAGE_SIZE: &str = "100";
const AWS_PRICING_FILTER_VALUE_MAX_CHARS: usize = 1024;
const AWS_PRICE_LIST_REGION: &str = "us-east-1";
const AWS_REGION_FILTER_LOCATION_TYPE: &str = "AWS Region";
const AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY: &str = "Compute Instance";
const AWS_BARE_METAL_PRODUCT_FAMILY: &str = "Compute Instance (bare metal)";
static AWS_LOCAL_CATALOG_STORE_CACHE: LazyLock<Mutex<Option<Arc<AwsMachineCatalogStore>>>> =
    LazyLock::new(|| Mutex::new(None));
static AWS_MACHINE_PRICING_MAP_STORE_CACHE: LazyLock<
    Mutex<Option<Arc<AwsMachinePricingMapStore>>>,
> = LazyLock::new(|| Mutex::new(None));
static AWS_MACHINE_SHAPE_CACHE_STORE_CACHE: LazyLock<
    Mutex<Option<Arc<AwsMachineShapeCacheStore>>>,
> = LazyLock::new(|| Mutex::new(None));
static AWS_REGION_OFFERINGS_CACHE_STORE_CACHE: LazyLock<
    Mutex<Option<Arc<AwsRegionOfferingsCacheStore>>>,
> = LazyLock::new(|| Mutex::new(None));
static AWS_ZONE_OFFERINGS_CACHE_STORE_CACHE: LazyLock<
    Mutex<Option<Arc<AwsZoneOfferingsCacheStore>>>,
> = LazyLock::new(|| Mutex::new(None));

fn aws_parallelism(task_count: usize) -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(4)
        .max(1)
        .min(task_count.max(1))
}

fn aws_pricing_parallelism(task_count: usize) -> usize {
    aws_parallelism(task_count).min(AWS_PRICING_MAX_PARALLEL_REQUESTS)
}

#[derive(Debug, Clone)]
struct AwsImageRequirements {
    architecture: String,
    virtualization_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsMachineShape {
    machine: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) zone: Option<String>,
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsMachineShapeCacheStore {
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    entries: Vec<AwsMachineShape>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsRegionOfferingsCacheStore {
    #[serde(default)]
    entries: Vec<AwsRegionOfferingsCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsRegionOfferingsCacheEntry {
    region: String,
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    machines: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AwsMachinePricingMapEntry {
    machine: String,
    region: String,
    skip_reason: String,
}

impl MachineRegionEntry for AwsMachinePricingMapEntry {
    fn machine(&self) -> &str {
        &self.machine
    }

    fn region(&self) -> &str {
        &self.region
    }
}

impl MachineRegionSkipEntry for AwsMachinePricingMapEntry {
    fn skip_reason(&self) -> Option<&str> {
        Some(self.skip_reason.as_str())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsMachinePricingMapStore {
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    entries: Vec<AwsMachinePricingMapEntry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsZoneOfferingsCacheStore {
    #[serde(default)]
    entries: Vec<AwsZoneOfferingsCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsZoneOfferingsCacheEntry {
    region: String,
    zone: String,
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    machines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum AwsCatalogWarning {
    PricingMapEntryNowPriced { machine: String, region: String },
    StaleMachinePricingMap { machine: String, region: String },
    MissingPrice { machine: String, region: String },
    HostTenancyPriceOnly { machine: String, region: String },
}

impl MachineRegionEntry for AwsCatalogWarning {
    fn machine(&self) -> &str {
        match self {
            AwsCatalogWarning::PricingMapEntryNowPriced { machine, .. }
            | AwsCatalogWarning::StaleMachinePricingMap { machine, .. }
            | AwsCatalogWarning::MissingPrice { machine, .. }
            | AwsCatalogWarning::HostTenancyPriceOnly { machine, .. } => machine,
        }
    }

    fn region(&self) -> &str {
        match self {
            AwsCatalogWarning::PricingMapEntryNowPriced { region, .. }
            | AwsCatalogWarning::StaleMachinePricingMap { region, .. }
            | AwsCatalogWarning::MissingPrice { region, .. }
            | AwsCatalogWarning::HostTenancyPriceOnly { region, .. } => region,
        }
    }
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

#[derive(Debug, Default)]
struct AwsLivePricing {
    shared_prices: HashMap<(String, String), f64>,
    available_tenancies: HashMap<(String, String), BTreeSet<String>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsPricingCacheStore {
    #[serde(default)]
    regions: Vec<AwsPricingCacheRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsPricingCacheRegion {
    region: String,
    current_version_url: String,
    #[serde(default)]
    entries: Vec<AwsPricingCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsPricingCacheEntry {
    machine: String,
    #[serde(default)]
    shared_hourly_usd: Option<f64>,
    #[serde(default)]
    available_tenancies: Vec<String>,
}

#[derive(Debug, Clone)]
struct AwsRegionPricingVersion {
    region: String,
    current_version_url: String,
}

#[derive(Debug, Clone)]
struct AwsPricingQueryBatch {
    region_codes: Vec<String>,
    instance_types: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AwsPricingQueryProduct {
    #[serde(default, rename = "version")]
    _version: Option<String>,
    product: AwsPricingQueryProductMetadata,
    #[serde(default)]
    terms: AwsPricingQueryTerms,
}

#[derive(Debug, Deserialize)]
struct AwsPricingQueryProductMetadata {
    #[serde(rename = "productFamily")]
    product_family: String,
    attributes: HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct AwsPricingQueryTerms {
    #[serde(default, rename = "OnDemand")]
    on_demand: HashMap<String, AwsPricingQueryTerm>,
}

#[derive(Debug, Default, Deserialize)]
struct AwsPricingQueryTerm {
    #[serde(default, rename = "priceDimensions")]
    price_dimensions: HashMap<String, AwsPricingQueryPriceDimension>,
}

#[derive(Debug, Default, Deserialize)]
struct AwsPricingQueryPriceDimension {
    #[serde(default)]
    unit: String,
    #[serde(default, rename = "pricePerUnit")]
    price_per_unit: HashMap<String, String>,
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

    fn shell_connect_command(config: &IceConfig, instance: &Self::Instance) -> Result<String> {
        build_shell_connect_command(config, instance)
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
    let zone_offered_machines = match (preferred_region.as_deref(), search_zone.as_deref()) {
        (Some(region), Some(zone)) => Some(load_zone_instance_offerings(config, region, zone)?),
        _ => None,
    };
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
        if let Some(region) = preferred_region.as_deref() {
            return Err(missing_catalog_entry_error(
                name,
                region,
                search_zone.as_deref(),
            ));
        }
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
        zone_offered_machines.as_ref(),
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
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
) -> Result<CloudMachineCandidate> {
    let catalog = load_local_catalog_store().with_context(
        || "AWS create uses the local catalog only. Run `ice refresh-catalog --cloud aws` first.",
    )?;
    let entry = catalog_entry_for_candidate(&catalog, candidate)
        .cloned()
        .ok_or_else(|| {
            missing_catalog_entry_error(
                &candidate.machine,
                &candidate.region,
                candidate.zone.as_deref(),
            )
        })?;
    if let Some(zone) = candidate.zone.as_deref() {
        ensure_machine_offered_in_zone(config, &entry.region, zone, &entry.machine)?;
    }
    Ok(CloudMachineCandidate {
        machine: entry.machine,
        vcpus: entry.vcpus,
        ram_mb: entry.ram_mb,
        gpus: entry.gpus,
        hourly_usd: entry.hourly_usd,
        region: entry.region,
        zone: candidate.zone.clone(),
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

fn merge_cached_pricing_region(
    pricing: &mut AwsLivePricing,
    cached_region: &AwsPricingCacheRegion,
) {
    for entry in &cached_region.entries {
        let key = (cached_region.region.clone(), entry.machine.clone());
        if let Some(hourly_usd) = entry.shared_hourly_usd {
            let current = pricing
                .shared_prices
                .entry(key.clone())
                .or_insert(hourly_usd);
            *current = (*current).min(hourly_usd);
        }
        if !entry.available_tenancies.is_empty() {
            pricing
                .available_tenancies
                .entry(key)
                .or_default()
                .extend(entry.available_tenancies.iter().cloned());
        }
    }
}

fn cached_pricing_region_from_live_pricing(
    region: &str,
    current_version_url: &str,
    pricing: &AwsLivePricing,
    wanted_instance_types: &HashSet<String>,
) -> AwsPricingCacheRegion {
    let region_key = region.to_owned();
    let mut machines = wanted_instance_types
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    machines.extend(
        pricing
            .shared_prices
            .keys()
            .filter(|(entry_region, _)| entry_region == &region_key)
            .map(|(_, machine)| machine.clone()),
    );
    machines.extend(
        pricing
            .available_tenancies
            .keys()
            .filter(|(entry_region, _)| entry_region == &region_key)
            .map(|(_, machine)| machine.clone()),
    );

    let entries = machines
        .into_iter()
        .map(|machine| {
            let key = (region_key.clone(), machine.clone());
            let mut available_tenancies = pricing
                .available_tenancies
                .get(&key)
                .map(|tenancies| tenancies.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            available_tenancies.sort();
            available_tenancies.dedup();
            AwsPricingCacheEntry {
                machine,
                shared_hourly_usd: pricing.shared_prices.get(&key).copied(),
                available_tenancies,
            }
        })
        .collect::<Vec<_>>();

    AwsPricingCacheRegion {
        region: region.to_owned(),
        current_version_url: current_version_url.to_owned(),
        entries,
    }
}

fn merge_live_pricing(pricing: &mut AwsLivePricing, additional: AwsLivePricing) {
    for (key, hourly_usd) in additional.shared_prices {
        let current = pricing.shared_prices.entry(key).or_insert(hourly_usd);
        *current = (*current).min(hourly_usd);
    }
    for (key, tenancies) in additional.available_tenancies {
        pricing
            .available_tenancies
            .entry(key)
            .or_default()
            .extend(tenancies);
    }
}

fn cached_pricing_region_is_complete(
    cached_region: &AwsPricingCacheRegion,
    wanted_instance_types: &HashSet<String>,
) -> bool {
    let cached_machines = cached_region
        .entries
        .iter()
        .map(|entry| entry.machine.as_str())
        .collect::<HashSet<_>>();
    wanted_instance_types
        .iter()
        .all(|machine| cached_machines.contains(machine.as_str()))
}

fn load_live_prices_for_regions(
    regions: &[String],
    instance_types: &[String],
) -> Result<AwsLivePricing> {
    if regions.is_empty() || instance_types.is_empty() {
        return Ok(AwsLivePricing::default());
    }

    let client = aws_pricing_http_client()?;
    let region_versions = aws_region_pricing_versions(&client, regions)?;
    let wanted_instance_types = instance_types.iter().cloned().collect::<HashSet<_>>();
    let mut pricing = AwsLivePricing::default();
    let previous_cache = load_pricing_cache_store()?.unwrap_or_default();
    let requested_regions = regions.iter().map(String::as_str).collect::<HashSet<_>>();
    let retained_cache_regions = previous_cache
        .regions
        .iter()
        .filter(|cached| !requested_regions.contains(cached.region.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let cached_by_region = previous_cache
        .regions
        .iter()
        .map(|entry| (entry.region.as_str(), entry))
        .collect::<HashMap<_, _>>();
    let mut next_cache_regions = Vec::new();
    let mut stale_region_versions = Vec::new();

    for region_version in region_versions {
        let cached_region = cached_by_region
            .get(region_version.region.as_str())
            .filter(|cached| {
                cached.current_version_url == region_version.current_version_url
                    && cached_pricing_region_is_complete(cached, &wanted_instance_types)
            });
        if let Some(cached_region) = cached_region {
            merge_cached_pricing_region(&mut pricing, cached_region);
            next_cache_regions.push((*cached_region).clone());
        } else {
            stale_region_versions.push(region_version);
        }
    }

    let reused_region_count = next_cache_regions.len();
    let stale_region_count = stale_region_versions.len();
    if stale_region_versions.is_empty() {
        let spinner = spinner("Loading pricing: reusing cached AWS pricing...");
        spinner.finish_with_message(format!(
            "Loaded AWS pricing for {} region/type pairs ({} cached regions reused, 0 regions refreshed).",
            pricing.shared_prices.len(),
            reused_region_count
        ));
        return Ok(pricing);
    }

    let batches = aws_pricing_query_batches(&stale_region_versions, instance_types)?;
    let batch_count = batches.len();
    let progress = progress_bar(
        "Loading pricing:",
        &format!(
            "{} cached regions reused, 0 region/type pairs",
            reused_region_count
        ),
        batch_count as u64,
    );
    let queue = Mutex::new(batches.into_iter().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_pricing_parallelism(
        queue
            .lock()
            .expect("AWS pricing queue mutex poisoned")
            .len(),
    );

    let result = thread::scope(|scope| -> Result<AwsLivePricing> {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let queue = &queue;
            scope.spawn(move || {
                loop {
                    let batch = {
                        let mut guard = queue.lock().expect("AWS pricing queue mutex poisoned");
                        guard.pop_front()
                    };
                    let Some(batch) = batch else {
                        break;
                    };
                    let label = format!(
                        "{} regions x {} instance types",
                        batch.region_codes.len(),
                        batch.instance_types.len()
                    );
                    let result = load_live_price_query_batch(&batch);
                    let _ = sender.send((label, result));
                }
            });
        }
        drop(sender);

        let mut stale_pricing = AwsLivePricing::default();
        let mut completed = 0_u64;
        while let Ok((label, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            let batch_pricing =
                result.with_context(|| format!("Failed to query AWS pricing for {label}"))?;
            merge_live_pricing(&mut stale_pricing, batch_pricing);
            progress.set_message(format!(
                "{} cached regions reused, {} region/type pairs",
                reused_region_count,
                pricing.shared_prices.len() + stale_pricing.shared_prices.len()
            ));
        }
        Ok(stale_pricing)
    });

    match result {
        Ok(stale_pricing) => {
            for region_version in &stale_region_versions {
                let cached_region = cached_pricing_region_from_live_pricing(
                    &region_version.region,
                    &region_version.current_version_url,
                    &stale_pricing,
                    &wanted_instance_types,
                );
                merge_cached_pricing_region(&mut pricing, &cached_region);
                next_cache_regions.push(cached_region);
            }
            next_cache_regions.extend(retained_cache_regions);
            next_cache_regions.sort_by(|left, right| left.region.cmp(&right.region));
            save_pricing_cache_store(&AwsPricingCacheStore {
                regions: next_cache_regions,
            })?;
            progress.finish_with_message(format!(
                "Loaded AWS pricing for {} region/type pairs ({} cached regions reused, {} query batches, {} regions refreshed).",
                pricing.shared_prices.len(),
                reused_region_count,
                batch_count,
                stale_region_count
            ));
            Ok(pricing)
        }
        Err(err) => {
            progress.finish_and_clear();
            Err(err)
        }
    }
}

fn aws_pricing_http_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(AWS_PRICING_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(AWS_PRICING_REQUEST_TIMEOUT_SECS))
        .build()
        .context("Failed to build the AWS pricing HTTP client")
}

fn aws_region_pricing_versions(
    client: &Client,
    regions: &[String],
) -> Result<Vec<AwsRegionPricingVersion>> {
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

    let region_versions = regions
        .iter()
        .filter_map(|region| {
            index
                .regions
                .get(region)
                .map(|entry| AwsRegionPricingVersion {
                    region: region.clone(),
                    current_version_url: entry.current_version_url.clone(),
                })
        })
        .collect::<Vec<_>>();
    if region_versions.is_empty() {
        bail!("AWS bulk price list index did not include any of the requested regions.");
    }
    Ok(region_versions)
}

fn aws_pricing_query_batches(
    stale_region_versions: &[AwsRegionPricingVersion],
    instance_types: &[String],
) -> Result<Vec<AwsPricingQueryBatch>> {
    let region_codes = stale_region_versions
        .iter()
        .map(|entry| entry.region.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let region_batches = chunk_aws_any_of_filter_values(&region_codes)?;
    let instance_type_batches = chunk_aws_any_of_filter_values(instance_types)?;
    let mut batches = Vec::with_capacity(region_batches.len() * instance_type_batches.len());

    for region_codes in &region_batches {
        for instance_types in &instance_type_batches {
            batches.push(AwsPricingQueryBatch {
                region_codes: region_codes.clone(),
                instance_types: instance_types.clone(),
            });
        }
    }

    Ok(batches)
}

fn chunk_aws_any_of_filter_values(values: &[String]) -> Result<Vec<Vec<String>>> {
    let ordered_values = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_len = 0_usize;

    for value in ordered_values {
        if value.len() > AWS_PRICING_FILTER_VALUE_MAX_CHARS {
            bail!(
                "AWS pricing filter value `{value}` exceeds the {} character filter limit.",
                AWS_PRICING_FILTER_VALUE_MAX_CHARS
            );
        }
        let next_len = if current_batch.is_empty() {
            value.len()
        } else {
            current_len + 1 + value.len()
        };
        if !current_batch.is_empty() && next_len > AWS_PRICING_FILTER_VALUE_MAX_CHARS {
            batches.push(current_batch);
            current_batch = Vec::new();
            current_len = 0;
        }
        if current_batch.is_empty() {
            current_len = value.len();
        } else {
            current_len += 1 + value.len();
        }
        current_batch.push(value);
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }
    Ok(batches)
}

fn load_live_price_query_batch(batch: &AwsPricingQueryBatch) -> Result<AwsLivePricing> {
    let mut command = Command::new("aws");
    command.args(["pricing", "get-products"]);
    command.args(["--service-code", "AmazonEC2"]);
    command.args(["--region", AWS_PRICE_LIST_REGION]);
    command.args(["--format-version", "aws_v1"]);
    command.args(["--page-size", AWS_PRICING_QUERY_PAGE_SIZE]);
    command.args([
        "--cli-connect-timeout",
        &AWS_PRICING_CONNECT_TIMEOUT_SECS.to_string(),
    ]);
    command.args([
        "--cli-read-timeout",
        &AWS_PRICING_REQUEST_TIMEOUT_SECS.to_string(),
    ]);
    command.args(["--no-cli-pager", "--output", "json"]);
    let filters = json!([
        {
            "Type": "ANY_OF",
            "Field": "instanceType",
            "Value": batch.instance_types.join(","),
        },
        {
            "Type": "ANY_OF",
            "Field": "regionCode",
            "Value": batch.region_codes.join(","),
        },
        {
            "Type": "TERM_MATCH",
            "Field": "operatingSystem",
            "Value": "Linux",
        },
        {
            "Type": "TERM_MATCH",
            "Field": "preInstalledSw",
            "Value": "NA",
        },
        {
            "Type": "TERM_MATCH",
            "Field": "capacitystatus",
            "Value": "Used",
        },
        {
            "Type": "TERM_MATCH",
            "Field": "locationType",
            "Value": AWS_REGION_FILTER_LOCATION_TYPE,
        }
    ]);
    command.args([
        "--filters",
        &serde_json::to_string(&filters).context("Failed to serialize AWS pricing filters")?,
    ]);
    let value = run_command_json(
        &mut command,
        &format!(
            "query AWS pricing for {} regions and {} instance types",
            batch.region_codes.len(),
            batch.instance_types.len()
        ),
    )?;
    let wanted_regions = batch.region_codes.iter().cloned().collect::<HashSet<_>>();
    let wanted_instance_types = batch.instance_types.iter().cloned().collect::<HashSet<_>>();
    let mut pricing = AwsLivePricing::default();
    ingest_pricing_query_results(
        &mut pricing,
        &value,
        &wanted_regions,
        &wanted_instance_types,
    )?;
    Ok(pricing)
}

fn ingest_pricing_query_results(
    pricing: &mut AwsLivePricing,
    value: &Value,
    wanted_regions: &HashSet<String>,
    wanted_instance_types: &HashSet<String>,
) -> Result<()> {
    let price_list = value
        .get("PriceList")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("AWS pricing query response did not contain a PriceList array."))?;
    for item in price_list {
        let item = item.as_str().ok_or_else(|| {
            anyhow!("AWS pricing query response contained a non-string PriceList item.")
        })?;
        ingest_pricing_query_product(pricing, item, wanted_regions, wanted_instance_types)?;
    }
    Ok(())
}

fn ingest_pricing_query_product(
    pricing: &mut AwsLivePricing,
    item: &str,
    wanted_regions: &HashSet<String>,
    wanted_instance_types: &HashSet<String>,
) -> Result<()> {
    let product = serde_json::from_str::<AwsPricingQueryProduct>(item).with_context(|| {
        format!(
            "Failed to parse AWS pricing product {}",
            truncate_ellipsis(item, 240)
        )
    })?;
    if !matches!(
        product.product.product_family.as_str(),
        AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY | AWS_BARE_METAL_PRODUCT_FAMILY
    ) {
        return Ok(());
    }

    let attributes = &product.product.attributes;
    if query_product_attr(attributes, "operatingSystem") != Some("Linux")
        || query_product_attr(attributes, "preInstalledSw") != Some("NA")
        || query_product_attr(attributes, "capacitystatus") != Some("Used")
        || query_product_attr(attributes, "locationType") != Some(AWS_REGION_FILTER_LOCATION_TYPE)
    {
        return Ok(());
    }

    let Some(region) = query_product_attr(attributes, "regionCode")
        .filter(|value| wanted_regions.contains(*value))
    else {
        return Ok(());
    };
    let Some(machine) = query_product_attr(attributes, "instanceType")
        .filter(|value| wanted_instance_types.contains(*value))
    else {
        return Ok(());
    };
    let Some(tenancy) = query_product_attr(attributes, "tenancy") else {
        return Ok(());
    };
    let key = (region.to_owned(), machine.to_owned());
    pricing
        .available_tenancies
        .entry(key.clone())
        .or_default()
        .insert(tenancy.to_owned());
    if tenancy != "Shared" {
        return Ok(());
    }
    let Some(hourly_usd) = aws_ondemand_hourly_usd(&product.terms) else {
        return Ok(());
    };
    let current = pricing.shared_prices.entry(key).or_insert(hourly_usd);
    *current = (*current).min(hourly_usd);
    Ok(())
}

fn query_product_attr<'a>(attributes: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    attributes
        .get(name)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn aws_ondemand_hourly_usd(terms: &AwsPricingQueryTerms) -> Option<f64> {
    terms
        .on_demand
        .values()
        .flat_map(|term| term.price_dimensions.values())
        .filter(|dimension| dimension.unit == "Hrs")
        .filter_map(|dimension| dimension.price_per_unit.get("USD"))
        .filter_map(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .min_by(|left, right| left.total_cmp(right))
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

fn load_machine_shapes_for_offerings(
    config: &IceConfig,
    region_offerings: &HashMap<String, Vec<String>>,
) -> Result<Vec<AwsMachineShape>> {
    let mut representative_region_by_machine = BTreeMap::new();
    for (region, machines) in region_offerings {
        for machine in machines {
            representative_region_by_machine
                .entry(machine.clone())
                .or_insert_with(|| region.clone());
        }
    }
    if representative_region_by_machine.is_empty() {
        bail!("AWS region offerings returned no machine types.");
    }

    let cache_store = load_machine_shape_cache_store()?;
    let cache_is_fresh = aws_cache_is_fresh(
        cache_store.refreshed_at_unix,
        AWS_MACHINE_SHAPE_CACHE_MAX_AGE_SECS,
    );
    let cached_by_machine = cache_store
        .entries
        .iter()
        .map(|shape| (shape.machine.as_str(), shape))
        .collect::<HashMap<_, _>>();
    if cache_is_fresh
        && representative_region_by_machine
            .keys()
            .all(|machine| cached_by_machine.contains_key(machine.as_str()))
    {
        let shapes = representative_region_by_machine
            .keys()
            .filter_map(|machine| cached_by_machine.get(machine.as_str()).copied().cloned())
            .collect::<Vec<_>>();
        let spinner = spinner("Loading machine types: reusing cached AWS machine shapes...");
        spinner.finish_with_message(format!(
            "Loaded {} AWS machine types from the machine-shape cache.",
            shapes.len(),
        ));
        return Ok(shapes);
    }

    let mut region_batches = Vec::new();
    for (region, machines) in representative_region_by_machine.iter().fold(
        BTreeMap::<String, Vec<String>>::new(),
        |mut grouped, (machine, region)| {
            grouped
                .entry(region.clone())
                .or_default()
                .push(machine.clone());
            grouped
        },
    ) {
        for batch in machines.chunks(AWS_INSTANCE_TYPES_PER_REQUEST) {
            region_batches.push((region.clone(), batch.to_vec()));
        }
    }

    let progress = progress_bar(
        "Loading machine types:",
        "0 machine shapes",
        region_batches.len() as u64,
    );
    let queue = Mutex::new(region_batches.into_iter().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_parallelism(
        queue
            .lock()
            .expect("AWS machine-shape queue mutex poisoned")
            .len(),
    );

    let result = thread::scope(|scope| -> Result<Vec<AwsMachineShape>> {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let queue = &queue;
            scope.spawn(move || {
                loop {
                    let batch = {
                        let mut guard = queue
                            .lock()
                            .expect("AWS machine-shape queue mutex poisoned");
                        guard.pop_front()
                    };
                    let Some((region, instance_types)) = batch else {
                        break;
                    };
                    let label = format!("{region} ({} machine types)", instance_types.len());
                    let result =
                        load_live_machine_shapes_for_region(config, &region, &instance_types);
                    let _ = sender.send((label, result));
                }
            });
        }
        drop(sender);

        let mut shapes = Vec::new();
        let mut completed = 0_u64;
        while let Ok((label, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            let mut batch_shapes =
                result.with_context(|| format!("Failed to load AWS machine shapes for {label}"))?;
            shapes.append(&mut batch_shapes);
            progress.set_message(format!("{} machine shapes", shapes.len()));
        }
        Ok(shapes)
    });

    match result {
        Ok(mut shapes) => {
            let missing_machines = representative_region_by_machine
                .keys()
                .filter(|machine| !shapes.iter().any(|shape| shape.machine == machine.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !missing_machines.is_empty() {
                progress.finish_and_clear();
                bail!(
                    "AWS machine-shape refresh did not return {} requested machine types. Examples: {}",
                    missing_machines.len(),
                    truncate_ellipsis(
                        &missing_machines
                            .iter()
                            .take(8)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", "),
                        280
                    )
                );
            }
            shapes.sort_by(|left, right| left.machine.cmp(&right.machine));
            save_machine_shape_cache_store(&AwsMachineShapeCacheStore {
                refreshed_at_unix: now_unix_secs(),
                entries: shapes.clone(),
            })?;
            progress.finish_with_message(format!(
                "Loaded {} AWS machine types from {} representative regions.",
                shapes.len(),
                representative_region_by_machine
                    .values()
                    .collect::<BTreeSet<_>>()
                    .len(),
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
    instance_types: &[String],
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
    if !instance_types.is_empty() {
        command.arg("--instance-types");
        for machine in instance_types {
            command.arg(machine);
        }
    }
    let value = run_command_json(
        &mut command,
        &format!(
            "describe {} aws instance types in {region}",
            instance_types.len()
        ),
    )?;
    let shapes = parse_machine_shapes(&value);
    let parsed_machines = shapes
        .iter()
        .map(|shape| shape.machine.as_str())
        .collect::<HashSet<_>>();
    let missing = instance_types
        .iter()
        .filter(|machine| !parsed_machines.contains(machine.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "AWS describe-instance-types in {region} did not return {} requested machine types. Examples: {}",
            missing.len(),
            truncate_ellipsis(
                &missing
                    .iter()
                    .take(8)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
                280
            )
        );
    }
    Ok(shapes)
}

#[cfg(test)]
fn parse_machine_shape(value: &Value) -> Option<AwsMachineShape> {
    value
        .get("InstanceTypes")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(parse_machine_shape_row)
}

fn parse_machine_shapes(value: &Value) -> Vec<AwsMachineShape> {
    value
        .get("InstanceTypes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_machine_shape_row)
        .collect()
}

fn parse_machine_shape_row(row: &Value) -> Option<AwsMachineShape> {
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

fn aws_machine_pricing_map_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "machine-pricing-map.toml")
}

fn aws_machine_shape_cache_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "machine-shapes.toml")
}

fn aws_region_offerings_cache_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "region-offerings.toml")
}

fn aws_pricing_cache_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "pricing-cache.toml")
}

fn aws_zone_offerings_cache_path() -> Result<PathBuf> {
    runtime_provider_data_path(Cloud::Aws, "zone-offerings.toml")
}

fn load_pricing_cache_store() -> Result<Option<AwsPricingCacheStore>> {
    let path = aws_pricing_cache_path()?;
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("Failed to read AWS pricing cache at {}", path.display())
            });
        }
    };
    if content.trim().is_empty() {
        return Ok(None);
    }
    let store = toml::from_str::<AwsPricingCacheStore>(&content)
        .with_context(|| format!("Failed to parse AWS pricing cache at {}", path.display()))?;
    Ok(Some(store))
}

fn save_pricing_cache_store(store: &AwsPricingCacheStore) -> Result<PathBuf> {
    let path = aws_pricing_cache_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS pricing cache path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content = toml::to_string_pretty(store).context("Failed to serialize AWS pricing cache")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(path)
}

fn load_machine_shape_cache_store() -> Result<Arc<AwsMachineShapeCacheStore>> {
    load_cached_arc(
        &AWS_MACHINE_SHAPE_CACHE_STORE_CACHE,
        || {
            let path = aws_machine_shape_cache_path()?;
            let content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Ok(AwsMachineShapeCacheStore::default());
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "Failed to read AWS machine-shape cache at {}",
                            path.display()
                        )
                    });
                }
            };
            if content.trim().is_empty() {
                return Ok(AwsMachineShapeCacheStore::default());
            }
            toml::from_str::<AwsMachineShapeCacheStore>(&content).with_context(|| {
                format!(
                    "Failed to parse AWS machine-shape cache at {}",
                    path.display()
                )
            })
        },
        "AWS runtime-data",
    )
}

fn save_machine_shape_cache_store(store: &AwsMachineShapeCacheStore) -> Result<PathBuf> {
    let path = aws_machine_shape_cache_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS machine-shape cache path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content =
        toml::to_string_pretty(store).context("Failed to serialize AWS machine-shape cache")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&AWS_MACHINE_SHAPE_CACHE_STORE_CACHE, "AWS runtime-data")?;
    Ok(path)
}

fn load_machine_pricing_map_store() -> Result<Arc<AwsMachinePricingMapStore>> {
    load_cached_arc(
        &AWS_MACHINE_PRICING_MAP_STORE_CACHE,
        || {
            let path = aws_machine_pricing_map_path()?;
            let content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Ok(AwsMachinePricingMapStore::default());
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("Failed to read AWS pricing map at {}", path.display())
                    });
                }
            };
            if content.trim().is_empty() {
                return Ok(AwsMachinePricingMapStore::default());
            }
            let store =
                toml::from_str::<AwsMachinePricingMapStore>(&content).with_context(|| {
                    format!("Failed to parse AWS pricing map at {}", path.display())
                })?;
            build_machine_region_index(&store.entries, |key| {
                format!(
                    "Duplicate AWS pricing map entry for `{}` in `{}`.",
                    key.machine, key.region
                )
            })?;
            if store
                .entries
                .iter()
                .any(|entry| entry.skip_reason.trim().is_empty())
            {
                bail!(
                    "AWS pricing map at {} contains an empty skip reason.",
                    path.display()
                );
            }
            Ok(store)
        },
        "AWS runtime-data",
    )
}

fn save_machine_pricing_map_store(store: &AwsMachinePricingMapStore) -> Result<PathBuf> {
    let path = aws_machine_pricing_map_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS pricing map path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content = toml::to_string_pretty(store).context("Failed to serialize AWS pricing map")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&AWS_MACHINE_PRICING_MAP_STORE_CACHE, "AWS runtime-data")?;
    Ok(path)
}

fn load_region_offerings_cache_store() -> Result<Arc<AwsRegionOfferingsCacheStore>> {
    load_cached_arc(
        &AWS_REGION_OFFERINGS_CACHE_STORE_CACHE,
        || {
            let path = aws_region_offerings_cache_path()?;
            let content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Ok(AwsRegionOfferingsCacheStore::default());
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "Failed to read AWS region-offerings cache at {}",
                            path.display()
                        )
                    });
                }
            };
            if content.trim().is_empty() {
                return Ok(AwsRegionOfferingsCacheStore::default());
            }
            toml::from_str::<AwsRegionOfferingsCacheStore>(&content).with_context(|| {
                format!(
                    "Failed to parse AWS region-offerings cache at {}",
                    path.display()
                )
            })
        },
        "AWS runtime-data",
    )
}

fn save_region_offerings_cache_store(store: &AwsRegionOfferingsCacheStore) -> Result<PathBuf> {
    let path = aws_region_offerings_cache_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS region-offerings cache path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content =
        toml::to_string_pretty(store).context("Failed to serialize AWS region-offerings cache")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&AWS_REGION_OFFERINGS_CACHE_STORE_CACHE, "AWS runtime-data")?;
    Ok(path)
}

fn load_zone_offerings_cache_store() -> Result<Arc<AwsZoneOfferingsCacheStore>> {
    load_cached_arc(
        &AWS_ZONE_OFFERINGS_CACHE_STORE_CACHE,
        || {
            let path = aws_zone_offerings_cache_path()?;
            let content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Ok(AwsZoneOfferingsCacheStore::default());
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "Failed to read AWS zone-offerings cache at {}",
                            path.display()
                        )
                    });
                }
            };
            if content.trim().is_empty() {
                return Ok(AwsZoneOfferingsCacheStore::default());
            }
            toml::from_str::<AwsZoneOfferingsCacheStore>(&content).with_context(|| {
                format!(
                    "Failed to parse AWS zone-offerings cache at {}",
                    path.display()
                )
            })
        },
        "AWS runtime-data",
    )
}

fn save_zone_offerings_cache_store(store: &AwsZoneOfferingsCacheStore) -> Result<PathBuf> {
    let path = aws_zone_offerings_cache_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid AWS zone-offerings cache path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content =
        toml::to_string_pretty(store).context("Failed to serialize AWS zone-offerings cache")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&AWS_ZONE_OFFERINGS_CACHE_STORE_CACHE, "AWS runtime-data")?;
    Ok(path)
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
                entry.ram_mb == 0
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

fn aws_cache_is_fresh(refreshed_at_unix: u64, max_age_secs: u64) -> bool {
    refreshed_at_unix != 0 && now_unix_secs().saturating_sub(refreshed_at_unix) <= max_age_secs
}

fn changed_catalog_entry_count(
    previous: &[AwsMachineCatalogEntry],
    current: &[AwsMachineCatalogEntry],
) -> usize {
    changed_entry_count_by_key(previous, current, |entry| {
        (
            entry.region.clone(),
            entry.zone.clone(),
            entry.machine.clone(),
        )
    })
}

fn aws_machine_pricing_skip_reason(warning: &AwsCatalogWarning) -> Option<&'static str> {
    match warning {
        AwsCatalogWarning::PricingMapEntryNowPriced { .. }
        | AwsCatalogWarning::StaleMachinePricingMap { .. } => None,
        AwsCatalogWarning::MissingPrice { .. } => Some(
            "intentionally skipped from the local AWS catalog because the AWS Pricing API returned no live Shared Linux/NA/Used on-demand price for this machine/region during refresh",
        ),
        AwsCatalogWarning::HostTenancyPriceOnly { .. } => Some(
            "intentionally skipped from the local AWS catalog because the AWS Pricing API only returned Host-tenancy pricing for this machine/region, and ice does not provision dedicated hosts",
        ),
    }
}

fn build_machine_pricing_map_store(
    warnings: &BTreeMap<AwsCatalogWarning, Vec<String>>,
) -> AwsMachinePricingMapStore {
    let mut entries = warnings
        .keys()
        .filter_map(|warning| {
            aws_machine_pricing_skip_reason(warning).map(|skip_reason| match warning {
                AwsCatalogWarning::MissingPrice { machine, region }
                | AwsCatalogWarning::HostTenancyPriceOnly { machine, region } => {
                    AwsMachinePricingMapEntry {
                        machine: machine.clone(),
                        region: region.clone(),
                        skip_reason: skip_reason.to_owned(),
                    }
                }
                AwsCatalogWarning::PricingMapEntryNowPriced { .. }
                | AwsCatalogWarning::StaleMachinePricingMap { .. } => unreachable!(),
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.region
            .cmp(&right.region)
            .then_with(|| left.machine.cmp(&right.machine))
    });
    AwsMachinePricingMapStore {
        refreshed_at_unix: now_unix_secs(),
        entries,
    }
}

fn aws_machine_pricing_skip_note(machine: &str, region: &str) -> Option<String> {
    let store = load_machine_pricing_map_store().ok()?;
    machine_region_skip_reason(&store.entries, machine, region)
}

fn missing_catalog_entry_error(machine: &str, region: &str, zone: Option<&str>) -> anyhow::Error {
    if let Some(skip_reason) = aws_machine_pricing_skip_note(machine, region) {
        return match zone {
            Some(zone) => anyhow!(
                "Machine type `{machine}` in `{region}` / `{zone}` is not present in the local AWS catalog. {skip_reason}."
            ),
            None => anyhow!(
                "Machine type `{machine}` in `{region}` is not present in the local AWS catalog. {skip_reason}."
            ),
        };
    }
    match zone {
        Some(zone) => anyhow!(
            "Machine type `{machine}` in `{region}` / `{zone}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`."
        ),
        None => anyhow!(
            "Machine type `{machine}` in `{region}` is not present in the local AWS catalog. Run `ice refresh-catalog --cloud aws`."
        ),
    }
}

fn ensure_machine_offered_in_zone(
    config: &IceConfig,
    region: &str,
    zone: &str,
    machine: &str,
) -> Result<()> {
    let offered_machines = load_zone_instance_offerings(config, region, zone)?;
    if offered_machines
        .iter()
        .any(|item| item.eq_ignore_ascii_case(machine))
    {
        return Ok(());
    }
    bail!("Machine type `{machine}` is not currently offered in `{region}` / `{zone}`.")
}

fn aws_catalog_warning_priority(warning: &AwsCatalogWarning) -> u8 {
    match warning {
        AwsCatalogWarning::PricingMapEntryNowPriced { .. } => 0,
        AwsCatalogWarning::StaleMachinePricingMap { .. } => 1,
        AwsCatalogWarning::MissingPrice { .. } => 2,
        AwsCatalogWarning::HostTenancyPriceOnly { .. } => 3,
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
        .map(|(warning, details)| match warning {
            AwsCatalogWarning::PricingMapEntryNowPriced { machine, region } => format!(
                "stale pricing map for `{machine}` in `{region}`: AWS now returns a live Shared Linux/NA/Used on-demand price."
            ),
            AwsCatalogWarning::StaleMachinePricingMap { machine, region } => format!(
                "stale pricing map for `{machine}` in `{region}`: not returned by the live AWS region-offerings list."
            ),
            AwsCatalogWarning::MissingPrice { machine, region } => details
                .first()
                .map(|detail| format!("missing live AWS price for `{machine}` in `{region}`. {detail}"))
                .unwrap_or_else(|| format!("missing live AWS price for `{machine}` in `{region}`.")),
            AwsCatalogWarning::HostTenancyPriceOnly { machine, region } => details
                .first()
                .map(|detail| {
                    format!(
                        "live AWS price for `{machine}` in `{region}` exists only with `Host` tenancy. {detail}"
                    )
                })
                .unwrap_or_else(|| {
                    format!("live AWS price for `{machine}` in `{region}` exists only with `Host` tenancy.")
                }),
        })
        .collect()
}

fn reconcile_aws_catalog_warnings(
    previous: &AwsMachinePricingMapStore,
    current_skip_warnings: &BTreeMap<AwsCatalogWarning, Vec<String>>,
    current_offered_keys: &HashSet<MachineRegionKey>,
    prices: &AwsLivePricing,
) -> BTreeMap<AwsCatalogWarning, Vec<String>> {
    let previous_skip_keys = skipped_machine_region_key_set(&previous.entries);
    let current_skip_keys = current_skip_warnings
        .keys()
        .map(AwsCatalogWarning::machine_region_key)
        .collect::<HashSet<_>>();
    let mut warnings = current_skip_warnings
        .iter()
        .filter(|(warning, _)| !previous_skip_keys.contains(&warning.machine_region_key()))
        .map(|(warning, details)| (warning.clone(), details.clone()))
        .collect::<BTreeMap<_, _>>();

    for entry in &previous.entries {
        let key = entry.machine_region_key();
        if current_skip_keys.contains(&key) {
            continue;
        }
        if prices
            .shared_prices
            .contains_key(&(entry.region.clone(), entry.machine.clone()))
        {
            warnings
                .entry(AwsCatalogWarning::PricingMapEntryNowPriced {
                    machine: entry.machine.clone(),
                    region: entry.region.clone(),
                })
                .or_default();
            continue;
        }
        if !current_offered_keys.contains(&key) {
            warnings
                .entry(AwsCatalogWarning::StaleMachinePricingMap {
                    machine: entry.machine.clone(),
                    region: entry.region.clone(),
                })
                .or_default();
        }
    }
    warnings
}

fn catalog_entry_for_candidate<'a>(
    catalog: &'a AwsMachineCatalogStore,
    candidate: &CloudMachineCandidate,
) -> Option<&'a AwsMachineCatalogEntry> {
    catalog.entries.iter().find(|entry| {
        entry.machine.eq_ignore_ascii_case(&candidate.machine)
            && entry.region.eq_ignore_ascii_case(&candidate.region)
    })
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
    zone_offered_machines: Option<&HashSet<String>>,
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
    let mut matches_by_region_machine = BTreeMap::new();

    for entry in catalog {
        if let Some(region) = preferred_region
            && !entry.region.eq_ignore_ascii_case(region)
        {
            continue;
        }
        if search_zone.is_some()
            && zone_offered_machines.is_some_and(|machines| !machines.contains(&entry.machine))
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
        matches_by_region_machine
            .entry((entry.region.clone(), entry.machine.clone()))
            .or_insert_with(|| entry.clone());
    }

    let mut candidates = matches_by_region_machine.into_values().collect::<Vec<_>>();

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
            .then_with(|| left.region.cmp(&right.region))
            .then_with(|| left.machine.cmp(&right.machine))
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
        zone: search_zone.map(str::to_owned),
    })
}

fn load_region_instance_offerings(
    config: &IceConfig,
    regions: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    if regions.is_empty() {
        return Ok(HashMap::new());
    }

    let previous_cache = load_region_offerings_cache_store()?;
    let cached_by_region = previous_cache
        .entries
        .iter()
        .map(|entry| (entry.region.as_str(), entry))
        .collect::<HashMap<_, _>>();
    let requested_regions = regions.iter().map(String::as_str).collect::<HashSet<_>>();
    let retained_cache_entries = previous_cache
        .entries
        .iter()
        .filter(|entry| !requested_regions.contains(entry.region.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut offerings = HashMap::<String, Vec<String>>::new();
    let mut next_cache_entries = Vec::new();
    let mut stale_regions = Vec::new();

    for region in regions {
        let cached = cached_by_region
            .get(region.as_str())
            .filter(|entry| {
                aws_cache_is_fresh(
                    entry.refreshed_at_unix,
                    AWS_REGION_OFFERINGS_CACHE_MAX_AGE_SECS,
                ) && !entry.machines.is_empty()
            })
            .copied();
        if let Some(cached) = cached {
            offerings.insert(region.clone(), cached.machines.clone());
            next_cache_entries.push(cached.clone());
        } else {
            stale_regions.push(region.clone());
        }
    }

    if stale_regions.is_empty() {
        let entry_count = offerings.values().map(Vec::len).sum::<usize>();
        let spinner = spinner("Loading offerings: reusing cached AWS region offerings...");
        spinner.finish_with_message(format!(
            "Loaded AWS region offerings for {entry_count} region/type pairs ({} cached regions reused, 0 regions refreshed).",
            next_cache_entries.len(),
        ));
        return Ok(offerings);
    }

    let progress = progress_bar(
        "Loading offerings:",
        &format!(
            "{} cached regions reused, 0 region/type pairs",
            next_cache_entries.len()
        ),
        stale_regions.len() as u64,
    );
    let queue = Mutex::new(stale_regions.clone().into_iter().collect::<VecDeque<_>>());
    let (sender, receiver) = mpsc::channel();
    let worker_count = aws_parallelism(stale_regions.len());

    let result = thread::scope(|scope| -> Result<HashMap<String, Vec<String>>> {
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
                    let result =
                        load_live_region_instance_offerings_for_region(config, &region, None);
                    let _ = sender.send((region, result));
                }
            });
        }
        drop(sender);

        let mut completed = 0_u64;
        let mut refreshed = HashMap::new();
        while let Ok((region, result)) = receiver.recv() {
            completed += 1;
            progress.set_position(completed);
            let machines = result
                .with_context(|| format!("Failed to load AWS region offerings in {region}"))?
                .into_iter()
                .collect::<Vec<_>>();
            progress.set_message(format!(
                "{} cached regions reused, {} region/type pairs",
                next_cache_entries.len(),
                offerings.values().map(Vec::len).sum::<usize>()
                    + refreshed.values().map(Vec::len).sum::<usize>()
                    + machines.len()
            ));
            refreshed.insert(region, machines);
        }
        Ok(refreshed)
    });

    match result {
        Ok(refreshed) => {
            for (region, machines) in refreshed {
                offerings.insert(region.clone(), machines.clone());
                next_cache_entries.push(AwsRegionOfferingsCacheEntry {
                    region,
                    refreshed_at_unix: now_unix_secs(),
                    machines,
                });
            }
            next_cache_entries.extend(retained_cache_entries);
            next_cache_entries.sort_by(|left, right| left.region.cmp(&right.region));
            save_region_offerings_cache_store(&AwsRegionOfferingsCacheStore {
                entries: next_cache_entries,
            })?;
            let entry_count = offerings.values().map(Vec::len).sum::<usize>();
            progress.finish_with_message(format!(
                "Loaded AWS region offerings for {entry_count} region/type pairs ({} cached regions reused, {} regions refreshed).",
                regions.len().saturating_sub(stale_regions.len()),
                stale_regions.len(),
            ));
            Ok(offerings)
        }
        Err(err) => {
            progress.finish_and_clear();
            Err(err)
        }
    }
}

fn load_live_region_instance_offerings_for_region(
    config: &IceConfig,
    region: &str,
    instance_types: Option<&[String]>,
) -> Result<BTreeSet<String>> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-instance-type-offerings",
        "--location-type",
        "region",
        "--output",
        "json",
        "--region",
        region,
    ]);
    if let Some(instance_types) = instance_types
        && !instance_types.is_empty()
    {
        command.arg("--filters").arg(format!(
            "Name=instance-type,Values={}",
            instance_types.join(",")
        ));
    }
    let value = run_command_json(
        &mut command,
        &format!("describe aws region instance type offerings in {region}"),
    )?;
    let machines = parse_instance_offering_machines(&value, Some(region));
    if instance_types.is_none() && machines.is_empty() {
        bail!("AWS region offerings in {region} returned no machine types.");
    }
    Ok(machines)
}

fn load_zone_instance_offerings(
    config: &IceConfig,
    region: &str,
    zone: &str,
) -> Result<HashSet<String>> {
    let cache_store = load_zone_offerings_cache_store()?;
    if let Some(entry) = cache_store.entries.iter().find(|entry| {
        entry.region.eq_ignore_ascii_case(region)
            && entry.zone.eq_ignore_ascii_case(zone)
            && aws_cache_is_fresh(
                entry.refreshed_at_unix,
                AWS_ZONE_OFFERINGS_CACHE_MAX_AGE_SECS,
            )
    }) {
        return Ok(entry.machines.iter().cloned().collect());
    }

    let spinner = spinner(&format!(
        "Loading AWS zone offerings for {region} / {zone}..."
    ));
    let machines = load_live_zone_instance_offerings_for_zone(config, region, zone)?;
    let mut next_entries = cache_store
        .entries
        .iter()
        .filter(|entry| {
            !(entry.region.eq_ignore_ascii_case(region) && entry.zone.eq_ignore_ascii_case(zone))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut sorted_machines = machines.iter().cloned().collect::<Vec<_>>();
    sorted_machines.sort();
    next_entries.push(AwsZoneOfferingsCacheEntry {
        region: region.to_owned(),
        zone: zone.to_owned(),
        refreshed_at_unix: now_unix_secs(),
        machines: sorted_machines,
    });
    next_entries.sort_by(|left, right| {
        left.region
            .cmp(&right.region)
            .then_with(|| left.zone.cmp(&right.zone))
    });
    save_zone_offerings_cache_store(&AwsZoneOfferingsCacheStore {
        entries: next_entries,
    })?;
    spinner.finish_with_message(format!(
        "Loaded AWS zone offerings for {} machine types in {region} / {zone}.",
        machines.len()
    ));
    Ok(machines)
}

fn load_live_zone_instance_offerings_for_zone(
    config: &IceConfig,
    region: &str,
    zone: &str,
) -> Result<HashSet<String>> {
    let mut command = command(config, region);
    command.args([
        "ec2",
        "describe-instance-type-offerings",
        "--location-type",
        "availability-zone",
        "--filters",
        &format!("Name=location,Values={zone}"),
        "--output",
        "json",
        "--region",
        region,
    ]);
    let value = run_command_json(
        &mut command,
        &format!("describe aws zone instance type offerings in {region} / {zone}"),
    )?;
    let machines = parse_instance_offering_machines(&value, Some(zone));
    if machines.is_empty() {
        bail!("AWS zone offerings in {region} / {zone} returned no machine types.");
    }
    Ok(machines.into_iter().collect())
}

fn parse_instance_offering_machines(
    value: &Value,
    expected_location: Option<&str>,
) -> BTreeSet<String> {
    let mut machines = BTreeSet::new();
    for row in value
        .get("InstanceTypeOfferings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(location) = expected_location
            && !row
                .get("Location")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case(location))
        {
            continue;
        }
        let Some(machine) = row
            .get("InstanceType")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        machines.insert(machine);
    }
    machines
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
    let previous_pricing_map = match load_machine_pricing_map_store() {
        Ok(store) => store,
        Err(err) => {
            print_warning(&format!(
                "Failed to load the previous AWS pricing map before refresh: {err:#}. \
                 Skip invalidation warnings may be incomplete."
            ));
            Arc::new(AwsMachinePricingMapStore::default())
        }
    };
    let regions = list_active_regions(config)?;
    let offerings = load_region_instance_offerings(config, &regions)?;
    let shapes = load_machine_shapes_for_offerings(config, &offerings)?;
    let shapes_by_machine = shapes
        .iter()
        .map(|shape| (shape.machine.as_str(), shape))
        .collect::<HashMap<_, _>>();
    let unique_types = offerings
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let prices = load_live_prices_for_regions(&regions, &unique_types)?;
    let current_offered_keys = offerings
        .iter()
        .flat_map(|(region, machines)| {
            machines
                .iter()
                .map(|machine| MachineRegionKey::new(region.clone(), machine.clone()))
        })
        .collect::<HashSet<_>>();
    let mut current_skip_warnings = BTreeMap::<AwsCatalogWarning, Vec<String>>::new();
    let mut entries = Vec::new();

    for (region, machines) in &offerings {
        for machine in machines {
            let shape = shapes_by_machine.get(machine.as_str()).copied().ok_or_else(|| {
                anyhow!(
                    "AWS machine-shape cache is missing metadata for `{machine}` while building the refreshed catalog."
                )
            })?;
            let key = (region.clone(), machine.clone());
            let Some(hourly_usd) = prices.shared_prices.get(&key).copied() else {
                let warning = if prices
                    .available_tenancies
                    .get(&key)
                    .is_some_and(|tenancies| tenancies.contains("Host"))
                {
                    AwsCatalogWarning::HostTenancyPriceOnly {
                        machine: machine.clone(),
                        region: region.clone(),
                    }
                } else {
                    AwsCatalogWarning::MissingPrice {
                        machine: machine.clone(),
                        region: region.clone(),
                    }
                };
                current_skip_warnings.entry(warning).or_default();
                continue;
            };
            entries.push(AwsMachineCatalogEntry {
                machine: shape.machine.clone(),
                region: region.clone(),
                zone: None,
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
    let warnings = reconcile_aws_catalog_warnings(
        &previous_pricing_map,
        &current_skip_warnings,
        &current_offered_keys,
        &prices,
    );

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
    save_machine_pricing_map_store(&build_machine_pricing_map_store(&current_skip_warnings))?;
    let path = save_local_catalog_store(&store)?;
    let warning_count = warnings.len();
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
    run_ssh_command(config, instance, &shell_remote_command(instance)?, true)
}

fn shell_remote_command(instance: &AwsInstance) -> Result<String> {
    match instance.workload.as_ref() {
        Some(InstanceWorkload::Unpack(_)) => Ok(unpack_shell_remote_command(
            &remote_unpack_dir_for_aws(instance),
        )),
        Some(workload) => Ok(instance_shell_remote_command(workload)),
        None => bail!(
            "Instance `{}` is missing workload metadata; refuse to guess its shell mode.",
            instance.instance_id
        ),
    }
}

fn build_shell_connect_command(config: &IceConfig, instance: &AwsInstance) -> Result<String> {
    let key_path = ssh_key_path(config)?;
    let user = ssh_user(config);
    let host = ssh_host(instance)?;
    Ok(render_command_line(
        "ssh",
        [
            "-i".to_owned(),
            key_path.display().to_string(),
            "-o".to_owned(),
            "StrictHostKeyChecking=accept-new".to_owned(),
            "-t".to_owned(),
            format!("{user}@{host}"),
            shell_remote_command(instance)?,
        ],
    ))
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
    if let Some(zone) = candidate.zone.as_deref() {
        ensure_machine_offered_in_zone(config, &region, zone, &candidate.machine)?;
    }
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
            missing_catalog_entry_error(&candidate.machine, &candidate.region, None)
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
        zone: Option<&str>,
        gpus: &[&str],
        has_accelerators: bool,
        architecture: &str,
        hourly_usd: f64,
    ) -> AwsMachineCatalogEntry {
        AwsMachineCatalogEntry {
            machine: machine.to_owned(),
            region: region.to_owned(),
            zone: zone.map(str::to_owned),
            vcpus: 4,
            ram_mb: 16_384,
            gpus: gpus.iter().map(|gpu| (*gpu).to_owned()).collect(),
            has_accelerators,
            architecture: architecture.to_owned(),
            virtualization_types: vec![AWS_DEFAULT_VIRTUALIZATION_TYPE.to_owned()],
            hourly_usd,
        }
    }

    fn pricing_query_product_fixture(
        product_family: &str,
        machine: &str,
        region: &str,
        tenancy: &str,
        operating_system: &str,
        price_per_hour: &str,
    ) -> String {
        json!({
            "product": {
                "productFamily": product_family,
                "attributes": {
                    "capacitystatus": "Used",
                    "instanceType": machine,
                    "locationType": AWS_REGION_FILTER_LOCATION_TYPE,
                    "operatingSystem": operating_system,
                    "preInstalledSw": "NA",
                    "regionCode": region,
                    "tenancy": tenancy,
                }
            },
            "terms": {
                "OnDemand": {
                    "sku.ondemand": {
                        "priceDimensions": {
                            "sku.ondemand.hourly": {
                                "unit": "Hrs",
                                "pricePerUnit": {
                                    "USD": price_per_hour,
                                }
                            }
                        }
                    }
                }
            }
        })
        .to_string()
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

        let shape = parse_machine_shape(&value).expect("shape should parse");
        assert_eq!(shape.machine, "g5.12xlarge");
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
                None,
                &["A10"],
                true,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                1.10,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                None,
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
                None,
                &["NVIDIA A10G Tensor Core"],
                true,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                1.10,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                None,
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
                None,
                &[],
                false,
                AWS_ARM64_IMAGE_ARCHITECTURE,
                0.05,
            ),
            test_catalog_entry(
                "c7i.large",
                "us-east-1",
                None,
                &[],
                false,
                AWS_DEFAULT_IMAGE_ARCHITECTURE,
                0.08,
            ),
        ];
        let zone_offered_machines = HashSet::from(["c7i.large".to_owned()]);

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
            Some(&zone_offered_machines),
        )
        .expect("candidate");
        assert_eq!(candidate.machine, "c7i.large");
        assert_eq!(candidate.zone.as_deref(), Some("us-east-1a"));
    }

    #[test]
    fn pricing_query_ingest_accepts_standard_and_bare_metal_linux_rows() {
        let value = json!({
            "PriceList": [
                pricing_query_product_fixture(
                    AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY,
                    "c7i.large",
                    "us-east-1",
                    "Shared",
                    "Linux",
                    "0.1234000000",
                ),
                pricing_query_product_fixture(
                    AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY,
                    "c7i.large",
                    "us-east-1",
                    "Dedicated",
                    "Linux",
                    "0.4567000000",
                ),
                pricing_query_product_fixture(
                    AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY,
                    "m7i.large",
                    "us-east-1",
                    "Shared",
                    "Windows",
                    "0.7890000000",
                ),
                pricing_query_product_fixture(
                    AWS_COMPUTE_INSTANCE_PRODUCT_FAMILY,
                    "c7i.large",
                    "us-east-1",
                    "Shared",
                    "Linux",
                    "0.1111000000",
                ),
                pricing_query_product_fixture(
                    AWS_BARE_METAL_PRODUCT_FAMILY,
                    "c5.metal",
                    "us-east-1",
                    "Shared",
                    "Linux",
                    "4.0800000000",
                )
            ]
        });
        let wanted_regions = HashSet::from(["us-east-1".to_owned()]);
        let wanted_instance_types = HashSet::from(["c5.metal".to_owned(), "c7i.large".to_owned()]);
        let mut pricing = AwsLivePricing::default();

        ingest_pricing_query_results(
            &mut pricing,
            &value,
            &wanted_regions,
            &wanted_instance_types,
        )
        .expect("AWS pricing query result should parse");

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
    fn pricing_query_ingest_tracks_host_only_rows() {
        let value = json!({
            "PriceList": [
                pricing_query_product_fixture(
                    AWS_BARE_METAL_PRODUCT_FAMILY,
                    "mac2.metal",
                    "us-east-1",
                    "Host",
                    "Linux",
                    "14.0000000000",
                )
            ]
        });
        let wanted_regions = HashSet::from(["us-east-1".to_owned()]);
        let wanted_instance_types = HashSet::from(["mac2.metal".to_owned()]);
        let mut pricing = AwsLivePricing::default();

        ingest_pricing_query_results(
            &mut pricing,
            &value,
            &wanted_regions,
            &wanted_instance_types,
        )
        .expect("AWS pricing query result should parse");

        assert!(pricing.shared_prices.is_empty());
        assert!(
            pricing
                .available_tenancies
                .get(&(String::from("us-east-1"), String::from("mac2.metal")))
                .is_some_and(|tenancies| tenancies.contains("Host"))
        );
    }

    #[test]
    fn pricing_cache_region_round_trip_preserves_shared_and_host_only_entries() {
        let mut pricing = AwsLivePricing::default();
        pricing
            .shared_prices
            .insert(("us-east-1".to_owned(), "c7i.large".to_owned()), 0.1111);
        pricing
            .available_tenancies
            .entry(("us-east-1".to_owned(), "c7i.large".to_owned()))
            .or_default()
            .extend(["Dedicated".to_owned(), "Shared".to_owned()]);
        pricing
            .available_tenancies
            .entry(("us-east-1".to_owned(), "mac2.metal".to_owned()))
            .or_default()
            .insert("Host".to_owned());

        let cached_region = cached_pricing_region_from_live_pricing(
            "us-east-1",
            "/offers/v1.0/aws/AmazonEC2/20260305205955/us-east-1/index.json",
            &pricing,
            &HashSet::from(["c7i.large".to_owned(), "mac2.metal".to_owned()]),
        );
        let mut merged = AwsLivePricing::default();
        merge_cached_pricing_region(&mut merged, &cached_region);

        assert_eq!(
            merged
                .shared_prices
                .get(&(String::from("us-east-1"), String::from("c7i.large"))),
            Some(&0.1111)
        );
        assert!(
            merged
                .available_tenancies
                .get(&(String::from("us-east-1"), String::from("c7i.large")))
                .is_some_and(|tenancies| tenancies.contains("Shared"))
        );
        assert!(
            merged
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
    fn machine_pricing_map_store_records_intentional_aws_skips() {
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

        let store = build_machine_pricing_map_store(&warnings);

        assert_eq!(store.entries.len(), 2);
        assert_eq!(
            store.entries,
            vec![
                AwsMachinePricingMapEntry {
                    machine: "trn2.3xlarge".to_owned(),
                    region: "sa-east-1".to_owned(),
                    skip_reason: aws_machine_pricing_skip_reason(
                        &AwsCatalogWarning::MissingPrice {
                            machine: String::new(),
                            region: String::new(),
                        }
                    )
                    .expect("skip reason")
                    .to_owned(),
                },
                AwsMachinePricingMapEntry {
                    machine: "mac2.metal".to_owned(),
                    region: "us-east-1".to_owned(),
                    skip_reason: aws_machine_pricing_skip_reason(
                        &AwsCatalogWarning::HostTenancyPriceOnly {
                            machine: String::new(),
                            region: String::new(),
                        },
                    )
                    .expect("skip reason")
                    .to_owned(),
                },
            ]
        );
    }

    #[test]
    fn reconcile_aws_catalog_warnings_suppresses_known_skips_and_reports_invalidated_entries() {
        let previous = AwsMachinePricingMapStore {
            refreshed_at_unix: 1,
            entries: vec![
                AwsMachinePricingMapEntry {
                    machine: "mac2.metal".to_owned(),
                    region: "us-east-1".to_owned(),
                    skip_reason: "host-only".to_owned(),
                },
                AwsMachinePricingMapEntry {
                    machine: "trn2.3xlarge".to_owned(),
                    region: "sa-east-1".to_owned(),
                    skip_reason: "missing-price".to_owned(),
                },
                AwsMachinePricingMapEntry {
                    machine: "c7i.large".to_owned(),
                    region: "us-east-1".to_owned(),
                    skip_reason: "old-missing-price".to_owned(),
                },
            ],
        };
        let current_skip_warnings = BTreeMap::from([
            (
                AwsCatalogWarning::HostTenancyPriceOnly {
                    machine: "mac2.metal".to_owned(),
                    region: "us-east-1".to_owned(),
                },
                Vec::new(),
            ),
            (
                AwsCatalogWarning::MissingPrice {
                    machine: "p5e.48xlarge".to_owned(),
                    region: "us-west-2".to_owned(),
                },
                Vec::new(),
            ),
        ]);
        let current_offered_keys = HashSet::from([
            MachineRegionKey::new("us-east-1", "mac2.metal"),
            MachineRegionKey::new("us-west-2", "p5e.48xlarge"),
            MachineRegionKey::new("us-east-1", "c7i.large"),
        ]);
        let mut prices = AwsLivePricing::default();
        prices
            .shared_prices
            .insert(("us-east-1".to_owned(), "c7i.large".to_owned()), 14.0);

        let warnings = reconcile_aws_catalog_warnings(
            &previous,
            &current_skip_warnings,
            &current_offered_keys,
            &prices,
        );

        assert_eq!(warnings.len(), 3);
        assert!(warnings.contains_key(&AwsCatalogWarning::MissingPrice {
            machine: "p5e.48xlarge".to_owned(),
            region: "us-west-2".to_owned(),
        }));
        assert!(
            warnings.contains_key(&AwsCatalogWarning::StaleMachinePricingMap {
                machine: "trn2.3xlarge".to_owned(),
                region: "sa-east-1".to_owned(),
            })
        );
        assert!(
            warnings.contains_key(&AwsCatalogWarning::PricingMapEntryNowPriced {
                machine: "c7i.large".to_owned(),
                region: "us-east-1".to_owned(),
            })
        );
        assert!(
            !warnings.contains_key(&AwsCatalogWarning::HostTenancyPriceOnly {
                machine: "mac2.metal".to_owned(),
                region: "us-east-1".to_owned(),
            })
        );
    }

    #[test]
    fn aws_any_of_filter_chunking_respects_the_1024_character_limit() {
        let values = vec!["a".repeat(700), "b".repeat(300), "c".repeat(100)];
        let batches = chunk_aws_any_of_filter_values(&values).expect("batches");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec!["a".repeat(700), "b".repeat(300)]);
        assert_eq!(batches[1], vec!["c".repeat(100)]);
    }
}
