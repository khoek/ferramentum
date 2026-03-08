use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use capulus::gcp::{self, AccessTokenRequest};
use indicatif::ProgressBar;
use reqwest::{Url, blocking::Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::gpu::{
    ProviderGpuAliasEntry, bundled_gcp_machine_pricing_map, canonicalize_gpu_name_for_cloud,
    normalize_gpu_name_token, runtime_gcp_machine_pricing_map_path,
};
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color,
    listed_instance as base_listed_instance, present_field, push_field,
};
use crate::model::{
    Cloud, CloudMachineCandidate, CreateSearchRequirements, IceConfig, RegistryAuth,
};
use crate::providers::{
    CloudInstance, CloudProvider, RemoteCloudProvider, RemoteSshProvider, clear_cached_arc,
    load_cached_arc,
};
use crate::provision::{estimated_machine_hourly_price, short_gcp_zone};
use crate::support::{
    GCP_CLOUD_PLATFORM_SCOPE, GCP_CONTAINER_IMAGE_FAMILY, GCP_CONTAINER_IMAGE_PROJECT,
    ICE_LABEL_PREFIX, ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, VAST_POLL_INTERVAL_SECS,
    VAST_WAIT_TIMEOUT_SECS, build_cloud_instance_name, elapsed_hours_from_rfc3339, elapsed_since,
    now_unix_secs, prefix_lookup_indices, progress_bar, run_command_json, run_command_status,
    run_command_text, spinner, visible_instance_name, write_temp_file,
};
use crate::ui::print_warning;
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

const GCP_BILLING_SERVICE: &str = "6F81-5844-456A";
const GCP_BILLING_PAGE_SIZE: &str = "5000";
const GCP_COMPUTE_PAGE_SIZE: &str = "500";
const GCP_LOCAL_CATALOG_MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;
static GCP_MACHINE_PRICING_MAP_STORE_CACHE: LazyLock<
    Mutex<Option<Arc<GcpMachinePricingMapStore>>>,
> = LazyLock::new(|| Mutex::new(None));
static GCP_LOCAL_CATALOG_STORE_CACHE: LazyLock<Mutex<Option<Arc<GcpMachineCatalogStore>>>> =
    LazyLock::new(|| Mutex::new(None));
static GCP_SKU_PRICING_CACHE_STORE_CACHE: LazyLock<Mutex<Option<Arc<GcpSkuPricingCacheStore>>>> =
    LazyLock::new(|| Mutex::new(None));
static GCP_SKU_PRICING_CACHE_INDEX_CACHE: LazyLock<Mutex<Option<Arc<GcpSkuPricingCacheIndex>>>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone)]
struct GcpMachineShape {
    machine: String,
    zone: String,
    region: String,
    vcpus: u32,
    billable_vcpus: f64,
    ram_mb: u32,
    accelerators: Vec<GcpAccelerator>,
    bundled_local_ssd_partitions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct GcpAccelerator {
    raw_type: String,
    label: String,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct GcpMachineCatalogEntry {
    pub(crate) machine: String,
    pub(crate) zone: String,
    pub(crate) region: String,
    pub(crate) vcpus: u32,
    #[serde(default)]
    pub(crate) billable_vcpus: f64,
    #[serde(default)]
    pub(crate) ram_mb: u32,
    #[serde(default)]
    pub(crate) accelerators: Vec<GcpAccelerator>,
    #[serde(default)]
    pub(crate) bundled_local_ssd_partitions: u32,
    pub(crate) gpus: Vec<String>,
    pub(crate) hourly_usd: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
enum GcpSkuRateUnit {
    PerHour,
    PerGibHour,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum GcpMachinePricingQuantitySource {
    PerMachine,
    BillableVcpu,
    RamGib,
    BundledLocalSsdPartitions,
    AcceleratorCount,
}

fn default_machine_pricing_quantity_multiplier() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcpMachinePricingComponent {
    sku_id: String,
    quantity_source: GcpMachinePricingQuantitySource,
    #[serde(default = "default_machine_pricing_quantity_multiplier")]
    quantity_multiplier: f64,
    #[serde(default)]
    accelerator_raw_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcpMachinePricingMapEntry {
    machine: String,
    region: String,
    #[serde(default)]
    skip_reason: Option<String>,
    #[serde(default)]
    components: Vec<GcpMachinePricingComponent>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpMachinePricingMapStore {
    #[serde(default)]
    gpu_aliases: Vec<ProviderGpuAliasEntry>,
    #[serde(default)]
    entries: Vec<GcpMachinePricingMapEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcpSkuPricingCacheEntry {
    sku_id: String,
    description: String,
    rate_unit: GcpSkuRateUnit,
    usd_per_unit: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpSkuPricingCacheStore {
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    entries: Vec<GcpSkuPricingCacheEntry>,
}

type GcpSkuPricingCacheIndex = HashMap<String, GcpSkuPricingCacheEntry>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GcpCatalogWarning {
    MissingMachinePricingMap { machine: String, region: String },
    StaleMachinePricingMap { machine: String, region: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GcpCatalogError {
    DuplicateMachinePricingMap {
        machine: String,
        region: String,
    },
    EmptyMachinePricingComponentList {
        machine: String,
        region: String,
    },
    SkippedMachineHasPricingComponents {
        machine: String,
        region: String,
    },
    MissingAcceleratorRawType {
        machine: String,
        region: String,
        sku_id: String,
    },
    UnexpectedAcceleratorRawType {
        machine: String,
        region: String,
        sku_id: String,
    },
    MissingMachineAccelerator {
        machine: String,
        region: String,
        accelerator_raw_type: String,
    },
    MissingBundledLocalSsdPartitionCount {
        machine: String,
        region: String,
    },
    MissingSkuPrice {
        sku_id: String,
        machine: String,
        region: String,
    },
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpMachineCatalogStore {
    #[serde(default)]
    refreshed_at_unix: u64,
    #[serde(default)]
    entries: Vec<GcpMachineCatalogEntry>,
}

#[derive(Debug)]
pub(crate) struct RefreshCatalogOutcome {
    pub(crate) path: PathBuf,
    pub(crate) entry_count: usize,
    pub(crate) changed_entry_count: usize,
    pub(crate) warning_count: usize,
    pub(crate) warning_summary: Vec<String>,
}

#[derive(Debug, Default)]
struct CatalogPricingOutcome {
    entries: Vec<GcpMachineCatalogEntry>,
    warnings: BTreeMap<GcpCatalogWarning, Vec<String>>,
}

#[derive(Debug)]
struct GcpMappedMachine<'a> {
    shape: &'a GcpMachineShape,
    mapping: &'a GcpMachinePricingMapEntry,
}

#[derive(Debug, Default)]
struct GcpMappedCatalogPreparation<'a> {
    mapped: Vec<GcpMappedMachine<'a>>,
    warnings: BTreeMap<GcpCatalogWarning, Vec<String>>,
    required_sku_ids: BTreeSet<String>,
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
            cached_machine_hourly_price(&self.machine_type)
                .or_else(|| estimated_machine_hourly_price(Cloud::Gcp, &self.machine_type))
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

    fn refresh_machine_offer(
        config: &IceConfig,
        candidate: &CloudMachineCandidate,
    ) -> Result<CloudMachineCandidate> {
        refresh_machine_offer(config, candidate)
    }
}

pub(crate) fn registry_login(config: &IceConfig) -> Result<RegistryAuth> {
    Ok(RegistryAuth {
        username: "oauth2accesstoken",
        secret: registry_access_token(config)?,
    })
}

fn build_machine_pricing_map_index(
    store: &GcpMachinePricingMapStore,
) -> Result<HashMap<(String, String), &GcpMachinePricingMapEntry>> {
    let mut index = HashMap::with_capacity(store.entries.len());
    for entry in &store.entries {
        let key = (entry.region.clone(), entry.machine.clone());
        if index.insert(key.clone(), entry).is_some() {
            bail!(
                "{}",
                render_catalog_error(&GcpCatalogError::DuplicateMachinePricingMap {
                    machine: key.1,
                    region: key.0,
                })
            );
        }
        if entry.skip_reason.is_some() && !entry.components.is_empty() {
            bail!(
                "{}",
                render_catalog_error(&GcpCatalogError::SkippedMachineHasPricingComponents {
                    machine: entry.machine.clone(),
                    region: entry.region.clone(),
                })
            );
        }
        if entry.skip_reason.is_none() && entry.components.is_empty() {
            bail!(
                "{}",
                render_catalog_error(&GcpCatalogError::EmptyMachinePricingComponentList {
                    machine: entry.machine.clone(),
                    region: entry.region.clone(),
                })
            );
        }
        for component in &entry.components {
            if !(component.quantity_multiplier.is_finite() && component.quantity_multiplier > 0.0) {
                bail!(
                    "GCP pricing map entry for `{}` in `{}` has invalid `quantity_multiplier` for `{}`.",
                    entry.machine,
                    entry.region,
                    component.sku_id
                );
            }
            match component.quantity_source {
                GcpMachinePricingQuantitySource::AcceleratorCount => {
                    if component
                        .accelerator_raw_type
                        .as_deref()
                        .is_none_or(str::is_empty)
                    {
                        bail!(
                            "{}",
                            render_catalog_error(&GcpCatalogError::MissingAcceleratorRawType {
                                machine: entry.machine.clone(),
                                region: entry.region.clone(),
                                sku_id: component.sku_id.clone(),
                            })
                        );
                    }
                }
                _ => {
                    if component.accelerator_raw_type.is_some() {
                        bail!(
                            "{}",
                            render_catalog_error(&GcpCatalogError::UnexpectedAcceleratorRawType {
                                machine: entry.machine.clone(),
                                region: entry.region.clone(),
                                sku_id: component.sku_id.clone(),
                            })
                        );
                    }
                }
            }
        }
    }
    Ok(index)
}

fn build_sku_pricing_cache_index(store: &GcpSkuPricingCacheStore) -> GcpSkuPricingCacheIndex {
    store
        .entries
        .iter()
        .cloned()
        .map(|entry| (entry.sku_id.clone(), entry))
        .collect()
}

fn machine_pricing_component_rate_unit(
    quantity_source: GcpMachinePricingQuantitySource,
) -> GcpSkuRateUnit {
    match quantity_source {
        GcpMachinePricingQuantitySource::PerMachine
        | GcpMachinePricingQuantitySource::BillableVcpu
        | GcpMachinePricingQuantitySource::AcceleratorCount => GcpSkuRateUnit::PerHour,
        GcpMachinePricingQuantitySource::RamGib
        | GcpMachinePricingQuantitySource::BundledLocalSsdPartitions => GcpSkuRateUnit::PerGibHour,
    }
}

fn machine_pricing_component_quantity(
    machine: &GcpMachineShape,
    component: &GcpMachinePricingComponent,
) -> Result<f64> {
    let quantity = match component.quantity_source {
        GcpMachinePricingQuantitySource::PerMachine => 1.0,
        GcpMachinePricingQuantitySource::BillableVcpu => machine.billable_vcpus,
        GcpMachinePricingQuantitySource::RamGib => f64::from(machine.ram_mb) / 1024.0,
        GcpMachinePricingQuantitySource::BundledLocalSsdPartitions => {
            if machine.bundled_local_ssd_partitions == 0 {
                bail!(
                    "{}",
                    render_catalog_error(&GcpCatalogError::MissingBundledLocalSsdPartitionCount {
                        machine: machine.machine.clone(),
                        region: machine.region.clone(),
                    })
                );
            }
            f64::from(machine.bundled_local_ssd_partitions)
        }
        GcpMachinePricingQuantitySource::AcceleratorCount => {
            let accelerator_raw_type =
                component.accelerator_raw_type.as_deref().ok_or_else(|| {
                    anyhow!(
                        "{}",
                        render_catalog_error(&GcpCatalogError::MissingAcceleratorRawType {
                            machine: machine.machine.clone(),
                            region: machine.region.clone(),
                            sku_id: component.sku_id.clone(),
                        })
                    )
                })?;
            let Some(count) = machine
                .accelerators
                .iter()
                .find(|accelerator| {
                    accelerator
                        .raw_type
                        .eq_ignore_ascii_case(accelerator_raw_type)
                })
                .map(|accelerator| accelerator.count)
            else {
                bail!(
                    "{}",
                    render_catalog_error(&GcpCatalogError::MissingMachineAccelerator {
                        machine: machine.machine.clone(),
                        region: machine.region.clone(),
                        accelerator_raw_type: accelerator_raw_type.to_owned(),
                    })
                );
            };
            f64::from(count)
        }
    };
    Ok(quantity * component.quantity_multiplier)
}

fn resolve_machine_hourly_price_from_map(
    pricing: &HashMap<String, GcpSkuPricingCacheEntry>,
    machine: &GcpMachineShape,
    map_entry: &GcpMachinePricingMapEntry,
) -> Result<f64> {
    let mut hourly_usd = 0.0;
    for component in &map_entry.components {
        let Some(price) = pricing.get(&component.sku_id) else {
            bail!(
                "{}",
                render_catalog_error(&GcpCatalogError::MissingSkuPrice {
                    sku_id: component.sku_id.clone(),
                    machine: machine.machine.clone(),
                    region: machine.region.clone(),
                })
            );
        };
        let expected_rate_unit = machine_pricing_component_rate_unit(component.quantity_source);
        if price.rate_unit != expected_rate_unit {
            bail!(
                "Mapped GCP SKU `{}` for `{}` in `{}` expected rate unit `{:?}` but cached `{:?}`.",
                component.sku_id,
                machine.machine,
                machine.region,
                expected_rate_unit,
                price.rate_unit
            );
        }
        hourly_usd += price.usd_per_unit * machine_pricing_component_quantity(machine, component)?;
    }
    Ok(hourly_usd)
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

fn gcp_provider_dir() -> Result<PathBuf> {
    let path = runtime_gcp_machine_pricing_map_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid GCP pricing map path {}", path.display()))?;
    Ok(parent.to_path_buf())
}

fn gcp_sku_pricing_cache_path() -> Result<PathBuf> {
    Ok(gcp_provider_dir()?.join("sku-pricing-cache.toml"))
}

fn local_catalog_path() -> Result<PathBuf> {
    Ok(gcp_provider_dir()?.join("machine-catalog.toml"))
}

fn load_machine_pricing_map_store() -> Result<Arc<GcpMachinePricingMapStore>> {
    load_cached_arc(
        &GCP_MACHINE_PRICING_MAP_STORE_CACHE,
        || {
            let path = runtime_gcp_machine_pricing_map_path()?;
            let content = match fs::read_to_string(&path) {
                Ok(content) => {
                    if content.trim().is_empty() {
                        bail!("GCP machine pricing map at {} is empty.", path.display());
                    }
                    content
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    bundled_gcp_machine_pricing_map().to_owned()
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "Failed to read GCP machine pricing map at {}",
                            path.display()
                        )
                    });
                }
            };
            toml::from_str::<GcpMachinePricingMapStore>(&content).with_context(|| {
                format!(
                    "Failed to parse GCP machine pricing map at {}",
                    path.display()
                )
            })
        },
        "GCP runtime-data",
    )
}

fn load_local_catalog_store() -> Result<Arc<GcpMachineCatalogStore>> {
    load_cached_arc(
        &GCP_LOCAL_CATALOG_STORE_CACHE,
        || {
            let path = local_catalog_path()?;
            let content = fs::read_to_string(&path).with_context(|| {
                format!("Failed to read local GCP catalog at {}", path.display())
            })?;
            if content.trim().is_empty() {
                bail!(
                    "Local GCP catalog at {} is empty. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            let store = toml::from_str::<GcpMachineCatalogStore>(&content).with_context(|| {
                format!("Failed to parse local GCP catalog at {}", path.display())
            })?;
            if store.entries.is_empty() {
                bail!(
                    "Local GCP catalog at {} contains no priced machine entries. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            if store.entries.iter().any(|entry| entry.ram_mb == 0) {
                bail!(
                    "Local GCP catalog at {} contains legacy RAM data. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            if store
                .entries
                .iter()
                .any(|entry| !(entry.billable_vcpus.is_finite() && entry.billable_vcpus > 0.0))
            {
                bail!(
                    "Local GCP catalog at {} contains legacy pricing-shape data. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            Ok(store)
        },
        "GCP runtime-data",
    )
}

fn load_sku_pricing_cache_store() -> Result<Arc<GcpSkuPricingCacheStore>> {
    load_cached_arc(
        &GCP_SKU_PRICING_CACHE_STORE_CACHE,
        || {
            let path = gcp_sku_pricing_cache_path()?;
            let content = fs::read_to_string(&path).with_context(|| {
                format!("Failed to read local GCP SKU cache at {}", path.display())
            })?;
            if content.trim().is_empty() {
                bail!(
                    "Local GCP SKU cache at {} is empty. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            let store = toml::from_str::<GcpSkuPricingCacheStore>(&content).with_context(|| {
                format!("Failed to parse local GCP SKU cache at {}", path.display())
            })?;
            if store.entries.is_empty() {
                bail!(
                    "Local GCP SKU cache at {} contains no SKU prices. Run `ice refresh-catalog --cloud gcp`.",
                    path.display()
                );
            }
            Ok(store)
        },
        "GCP runtime-data",
    )
}

fn load_sku_pricing_cache_index() -> Result<Arc<GcpSkuPricingCacheIndex>> {
    load_cached_arc(
        &GCP_SKU_PRICING_CACHE_INDEX_CACHE,
        || {
            let store = load_sku_pricing_cache_store()?;
            Ok(build_sku_pricing_cache_index(&store))
        },
        "GCP runtime-data",
    )
}

fn load_previous_local_catalog_store() -> Result<Option<GcpMachineCatalogStore>> {
    let path = local_catalog_path()?;
    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("Failed to read local GCP catalog at {}", path.display())
            });
        }
    };
    if content.trim().is_empty() {
        return Ok(None);
    }
    let store = toml::from_str::<GcpMachineCatalogStore>(&content)
        .with_context(|| format!("Failed to parse local GCP catalog at {}", path.display()))?;
    Ok(Some(store))
}

fn save_local_catalog_store(store: &GcpMachineCatalogStore) -> Result<PathBuf> {
    let path = local_catalog_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid GCP catalog path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content = toml::to_string_pretty(store).context("Failed to serialize local GCP catalog")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&GCP_LOCAL_CATALOG_STORE_CACHE, "GCP runtime-data")?;
    Ok(path)
}

fn save_sku_pricing_cache_store(store: &GcpSkuPricingCacheStore) -> Result<PathBuf> {
    let path = gcp_sku_pricing_cache_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Invalid GCP SKU cache path {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    let content =
        toml::to_string_pretty(store).context("Failed to serialize GCP SKU pricing cache")?;
    fs::write(&path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    clear_cached_arc(&GCP_SKU_PRICING_CACHE_STORE_CACHE, "GCP runtime-data")?;
    clear_cached_arc(&GCP_SKU_PRICING_CACHE_INDEX_CACHE, "GCP runtime-data")?;
    Ok(path)
}

fn warn_if_catalog_stale(store: &GcpMachineCatalogStore) {
    if store.refreshed_at_unix == 0 {
        print_warning(
            "GCP catalog has no refresh timestamp; search quality may be degraded. Run `ice refresh-catalog --cloud gcp`.",
        );
        return;
    }
    let age_secs = now_unix_secs().saturating_sub(store.refreshed_at_unix);
    if age_secs > GCP_LOCAL_CATALOG_MAX_AGE_SECS {
        let age_days = age_secs as f64 / 86_400.0;
        print_warning(&format!(
            "GCP catalog is {:.1} days old; search quality may be degraded. Run `ice refresh-catalog --cloud gcp`.",
            age_days
        ));
    }
}

fn changed_catalog_entry_count(
    previous: &[GcpMachineCatalogEntry],
    current: &[GcpMachineCatalogEntry],
) -> usize {
    let previous_by_key = previous
        .iter()
        .map(|entry| ((entry.machine.clone(), entry.zone.clone()), entry))
        .collect::<HashMap<_, _>>();
    let current_by_key = current
        .iter()
        .map(|entry| ((entry.machine.clone(), entry.zone.clone()), entry))
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

pub(crate) fn refresh_local_catalog(config: &IceConfig) -> Result<RefreshCatalogOutcome> {
    let previous_store = match load_previous_local_catalog_store() {
        Ok(store) => store,
        Err(err) => {
            print_warning(&format!(
                "Failed to compare refreshed GCP catalog against the previous cache: \
                 {err:#}. Reporting all entries as changed."
            ));
            None
        }
    };
    let machine_shapes = load_live_machine_shapes(config)?;
    let mapping_store = load_machine_pricing_map_store()?;
    let mapping_index = build_machine_pricing_map_index(&mapping_store)?;
    let preparation =
        prepare_mapped_catalog(&machine_shapes, &mapping_store.entries, &mapping_index);
    let pricing_store = refresh_sku_pricing_cache(
        config,
        &preparation.required_sku_ids,
        preparation.mapped.len(),
        true,
    )?;
    save_sku_pricing_cache_store(&pricing_store)?;
    let pricing = build_sku_pricing_cache_index(&pricing_store);
    let mut outcome = priced_catalog_entries(&preparation, &pricing)?;
    outcome.entries.sort_by(|left, right| {
        left.machine
            .cmp(&right.machine)
            .then_with(|| left.zone.cmp(&right.zone))
    });
    if outcome.entries.is_empty() {
        bail!(
            "No mapped GCP machine types were priced. Add entries to {} and rerun `ice refresh-catalog --cloud gcp`.",
            runtime_gcp_machine_pricing_map_path()?.display()
        );
    }
    let warning_count = outcome
        .warnings
        .values()
        .map(|zones| zones.len().max(1))
        .sum();
    let warning_summary = render_catalog_warnings(&outcome.warnings);
    let store = GcpMachineCatalogStore {
        refreshed_at_unix: now_unix_secs(),
        entries: outcome.entries,
    };
    let changed_entry_count = previous_store
        .as_ref()
        .map_or(store.entries.len(), |previous| {
            changed_catalog_entry_count(&previous.entries, &store.entries)
        });
    let path = save_local_catalog_store(&store)?;
    Ok(RefreshCatalogOutcome {
        path,
        entry_count: store.entries.len(),
        changed_entry_count,
        warning_count,
        warning_summary,
    })
}

fn priced_catalog_entries(
    preparation: &GcpMappedCatalogPreparation<'_>,
    pricing: &HashMap<String, GcpSkuPricingCacheEntry>,
) -> Result<CatalogPricingOutcome> {
    let mut outcome = CatalogPricingOutcome {
        entries: Vec::with_capacity(preparation.mapped.len()),
        warnings: preparation.warnings.clone(),
        ..CatalogPricingOutcome::default()
    };
    for mapped in &preparation.mapped {
        let hourly_usd =
            resolve_machine_hourly_price_from_map(pricing, mapped.shape, mapped.mapping)?;
        outcome.entries.push(GcpMachineCatalogEntry {
            machine: mapped.shape.machine.clone(),
            zone: mapped.shape.zone.clone(),
            region: mapped.shape.region.clone(),
            vcpus: mapped.shape.vcpus,
            billable_vcpus: mapped.shape.billable_vcpus,
            ram_mb: mapped.shape.ram_mb,
            accelerators: mapped.shape.accelerators.clone(),
            bundled_local_ssd_partitions: mapped.shape.bundled_local_ssd_partitions,
            gpus: expand_accelerator_labels(&mapped.shape.accelerators),
            hourly_usd,
        });
    }
    Ok(outcome)
}

fn prepare_mapped_catalog<'a>(
    machine_shapes: &'a [GcpMachineShape],
    mapping_entries: &'a [GcpMachinePricingMapEntry],
    mapping_index: &HashMap<(String, String), &'a GcpMachinePricingMapEntry>,
) -> GcpMappedCatalogPreparation<'a> {
    let mut preparation = GcpMappedCatalogPreparation::default();
    let live_keys = machine_shapes
        .iter()
        .map(|shape| (shape.region.as_str(), shape.machine.as_str()))
        .collect::<HashSet<_>>();
    for entry in mapping_entries {
        if live_keys.contains(&(entry.region.as_str(), entry.machine.as_str())) {
            continue;
        }
        preparation
            .warnings
            .entry(GcpCatalogWarning::StaleMachinePricingMap {
                machine: entry.machine.clone(),
                region: entry.region.clone(),
            })
            .or_default();
    }
    for shape in machine_shapes {
        let key = (shape.region.clone(), shape.machine.clone());
        let Some(mapping) = mapping_index.get(&key).copied() else {
            preparation
                .warnings
                .entry(GcpCatalogWarning::MissingMachinePricingMap {
                    machine: shape.machine.clone(),
                    region: shape.region.clone(),
                })
                .or_default()
                .push(shape.zone.clone());
            continue;
        };
        if mapping.skip_reason.is_some() {
            continue;
        }
        for component in &mapping.components {
            preparation
                .required_sku_ids
                .insert(component.sku_id.clone());
        }
        preparation.mapped.push(GcpMappedMachine { shape, mapping });
    }
    preparation
}

fn render_catalog_warnings(warnings: &BTreeMap<GcpCatalogWarning, Vec<String>>) -> Vec<String> {
    sorted_catalog_warnings(warnings)
        .into_iter()
        .take(8)
        .map(|(warning, zones)| {
            let preview = zones.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
            match warning {
                GcpCatalogWarning::MissingMachinePricingMap { machine, region } => format!(
                    "{} zones: missing pricing map for `{}` in `{}`. Examples: {}",
                    zones.len(),
                    machine,
                    region,
                    preview
                ),
                GcpCatalogWarning::StaleMachinePricingMap { machine, region } => format!(
                    "stale pricing map for `{}` in `{}`: not returned by the live GCP machine-types list.",
                    machine, region
                ),
            }
        })
        .collect()
}

fn sorted_catalog_warnings(
    warnings: &BTreeMap<GcpCatalogWarning, Vec<String>>,
) -> Vec<(&GcpCatalogWarning, &Vec<String>)> {
    let mut groups = warnings.iter().collect::<Vec<_>>();
    groups.sort_by(|(left_warning, left_zones), (right_warning, right_zones)| {
        right_zones
            .len()
            .max(1)
            .cmp(&left_zones.len().max(1))
            .then_with(|| left_warning.cmp(right_warning))
    });
    groups
}

fn render_catalog_error(error: &GcpCatalogError) -> String {
    match error {
        GcpCatalogError::DuplicateMachinePricingMap { machine, region } => {
            format!("Duplicate GCP pricing map entry for `{machine}` in `{region}`.")
        }
        GcpCatalogError::EmptyMachinePricingComponentList { machine, region } => {
            format!("GCP pricing map entry for `{machine}` in `{region}` has no components.")
        }
        GcpCatalogError::SkippedMachineHasPricingComponents { machine, region } => format!(
            "GCP pricing map entry for `{machine}` in `{region}` cannot set both `skip_reason` and `components`."
        ),
        GcpCatalogError::MissingAcceleratorRawType {
            machine,
            region,
            sku_id,
        } => format!(
            "GCP pricing map entry for `{machine}` in `{region}` uses accelerator-count for `{sku_id}` without `accelerator_raw_type`."
        ),
        GcpCatalogError::UnexpectedAcceleratorRawType {
            machine,
            region,
            sku_id,
        } => format!(
            "GCP pricing map entry for `{machine}` in `{region}` set `accelerator_raw_type` for `{sku_id}` without using accelerator-count."
        ),
        GcpCatalogError::MissingMachineAccelerator {
            machine,
            region,
            accelerator_raw_type,
        } => format!(
            "GCP machine `{machine}` in `{region}` has no `{accelerator_raw_type}` accelerator required by the pricing map."
        ),
        GcpCatalogError::MissingBundledLocalSsdPartitionCount { machine, region } => format!(
            "GCP machine `{machine}` in `{region}` needs bundled local SSD pricing but no bundled local SSD partition count was discovered."
        ),
        GcpCatalogError::MissingSkuPrice {
            sku_id,
            machine,
            region,
        } => format!(
            "Missing refreshed GCP price for SKU `{sku_id}` required by `{machine}` in `{region}`."
        ),
    }
}

pub(crate) fn find_cheapest_machine_candidate(
    config: &IceConfig,
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
) -> Result<CloudMachineCandidate> {
    let store = load_local_catalog_store().with_context(
        || "GCP search uses the local catalog only. Run `ice refresh-catalog --cloud gcp` first.",
    )?;
    warn_if_catalog_stale(&store);
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
            "Machine type `{name}` is not present in the local GCP catalog. Run `ice refresh-catalog --cloud gcp`."
        );
    }

    let preferred_region = preferred_region(config);
    let preferred_zone = preferred_zone(config);
    select_cheapest_machine_candidate(
        &store.entries,
        req,
        machine_override,
        preferred_region.as_deref(),
        preferred_zone.as_deref(),
    )
}

fn refresh_machine_offer(
    _config: &IceConfig,
    candidate: &CloudMachineCandidate,
) -> Result<CloudMachineCandidate> {
    let zone = candidate
        .zone
        .as_deref()
        .ok_or_else(|| anyhow!("Missing zone for selected GCP machine."))?;
    let catalog = load_local_catalog_store().with_context(
        || "GCP create uses the local catalog only. Run `ice refresh-catalog --cloud gcp` first.",
    )?;
    let entry = catalog
        .entries
        .iter()
        .find(|entry| {
            entry.machine.eq_ignore_ascii_case(&candidate.machine) && entry.zone.eq_ignore_ascii_case(zone)
        })
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "Machine type `{}` in zone `{zone}` is not present in the local GCP catalog. Run `ice refresh-catalog --cloud gcp`.",
                candidate.machine
            )
        })?;
    let mapping_store = load_machine_pricing_map_store()?;
    let map_entry = mapping_store
        .entries
        .iter()
        .find(|map_entry| {
            map_entry.region.eq_ignore_ascii_case(&entry.region)
                && map_entry.machine.eq_ignore_ascii_case(&entry.machine)
        })
        .ok_or_else(|| {
            anyhow!(
                "No GCP pricing map entry exists for `{}` in `{}`.",
                entry.machine,
                entry.region
            )
        })?;
    if let Some(reason) = map_entry.skip_reason.as_deref() {
        bail!(
            "GCP pricing map explicitly skips `{}` in `{}`: {reason}.",
            entry.machine,
            entry.region
        );
    }
    let pricing = load_sku_pricing_cache_index()?;
    let shape = machine_shape_from_catalog_entry(&entry);
    let hourly_usd = resolve_machine_hourly_price_from_map(&pricing, &shape, map_entry)?;
    Ok(CloudMachineCandidate {
        machine: entry.machine,
        vcpus: entry.vcpus,
        ram_mb: entry.ram_mb,
        gpus: entry.gpus,
        hourly_usd,
        region: entry.region,
        zone: Some(entry.zone),
    })
}

pub(crate) fn select_cheapest_machine_candidate(
    catalog: &[GcpMachineCatalogEntry],
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
    preferred_region: Option<&str>,
    preferred_zone: Option<&str>,
) -> Result<CloudMachineCandidate> {
    let override_name = machine_override
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let allowed_gpu_set = req
        .allowed_gpus
        .iter()
        .map(|gpu| {
            canonicalize_gpu_name_for_cloud(Cloud::Gcp, gpu)
                .unwrap_or_else(|| gpu.trim().to_owned())
        })
        .map(|gpu| normalize_gpu_name_token(&gpu))
        .collect::<HashSet<_>>();
    let min_ram_mb = req.min_ram_gb * 1000.0;

    let mut candidates = Vec::new();
    for entry in catalog {
        if let Some(name) = override_name
            && !entry.machine.eq_ignore_ascii_case(name)
        {
            continue;
        }
        if entry.vcpus < req.min_cpus || f64::from(entry.ram_mb) + 0.000_001 < min_ram_mb {
            continue;
        }
        if !allowed_gpu_set.is_empty() {
            let gpu_match = entry.gpus.iter().any(|gpu| {
                let canonical = canonicalize_gpu_name_for_cloud(Cloud::Gcp, gpu)
                    .unwrap_or_else(|| gpu.trim().to_owned());
                allowed_gpu_set.contains(&normalize_gpu_name_token(&canonical))
            });
            if !gpu_match {
                continue;
            }
        }
        candidates.push(entry.clone());
    }

    if candidates.is_empty() {
        bail!(
            "No gcp machine type matches filters (min_cpus={}, min_ram_gb={}, allowed_gpus=[{}]){}.",
            req.min_cpus,
            req.min_ram_gb,
            req.allowed_gpus.join(", "),
            override_name
                .map(|name| format!(", machine={name}"))
                .unwrap_or_default()
        );
    }

    candidates.sort_by(|left, right| {
        let price = left.hourly_usd.total_cmp(&right.hourly_usd);
        if price != Ordering::Equal {
            return price;
        }
        let left_region_pref = preferred_region
            .map(|region| left.region.eq_ignore_ascii_case(region))
            .unwrap_or(false);
        let right_region_pref = preferred_region
            .map(|region| right.region.eq_ignore_ascii_case(region))
            .unwrap_or(false);
        let region_pref = right_region_pref.cmp(&left_region_pref);
        if region_pref != Ordering::Equal {
            return region_pref;
        }
        let left_zone_pref = preferred_zone
            .map(|zone| left.zone.eq_ignore_ascii_case(zone))
            .unwrap_or(false);
        let right_zone_pref = preferred_zone
            .map(|zone| right.zone.eq_ignore_ascii_case(zone))
            .unwrap_or(false);
        let zone_pref = right_zone_pref.cmp(&left_zone_pref);
        if zone_pref != Ordering::Equal {
            return zone_pref;
        }
        left.zone.cmp(&right.zone)
    });

    let winner = candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No priced gcp candidate after sort"))?;
    Ok(CloudMachineCandidate {
        machine: winner.machine,
        vcpus: winner.vcpus,
        ram_mb: winner.ram_mb,
        gpus: winner.gpus,
        hourly_usd: winner.hourly_usd,
        region: winner.region,
        zone: Some(winner.zone),
    })
}

fn load_live_machine_shapes(config: &IceConfig) -> Result<Vec<GcpMachineShape>> {
    let token = registry_access_token(config)?;
    let project = resolved_gcp_project(config)?;
    let client = Client::new();
    let zones = load_active_gcp_zones(&client, token.trim(), &project)?;
    let active_zones = zones.iter().cloned().collect::<HashSet<_>>();
    let progress = progress_bar("Loading machine types:", "0 types", zones.len() as u64);
    let mut seen_zones = HashSet::new();
    let mut page_token: Option<String> = None;
    let mut page_count = 0_u64;
    let mut machines = Vec::new();

    loop {
        let value =
            fetch_gcp_machine_types_page(&client, token.trim(), &project, page_token.as_deref())?;
        let (page_machines, page_zones) = aggregated_machine_shapes_page(&value, &active_zones)?;
        machines.extend(page_machines);
        seen_zones.extend(page_zones);
        page_count += 1;
        progress.set_position(seen_zones.len() as u64);
        progress.set_message(format!("{} types · page {}", machines.len(), page_count));
        page_token = gcp_next_page_token(&value);
        if page_token.is_none() {
            break;
        }
    }

    if seen_zones.len() != active_zones.len() {
        let missing = zones
            .iter()
            .filter(|zone| !seen_zones.contains(*zone))
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "GCP machine-type catalog covered {}/{} active zones. Missing zones include: {}.",
            seen_zones.len(),
            active_zones.len(),
            missing
        );
    }

    progress.finish_with_message(format!(
        "Loaded {} GCP machine types from {} zones.",
        machines.len(),
        zones.len(),
    ));
    Ok(machines)
}

fn refresh_sku_pricing_cache(
    config: &IceConfig,
    required_sku_ids: &BTreeSet<String>,
    mapped_machine_count: usize,
    show_progress: bool,
) -> Result<GcpSkuPricingCacheStore> {
    if required_sku_ids.is_empty() {
        return Ok(GcpSkuPricingCacheStore {
            refreshed_at_unix: now_unix_secs(),
            entries: Vec::new(),
        });
    }

    let token = registry_access_token(config)?;
    let client = Client::new();
    let progress = show_progress.then(|| {
        progress_bar(
            "Loading pricing:",
            &format!("SKUs · {} mapped machine types", mapped_machine_count),
            required_sku_ids.len() as u64,
        )
    });
    let mut page_token: Option<String> = None;
    let mut page_count = 0_u64;
    let mut remaining = required_sku_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();

    loop {
        let value = fetch_gcp_billing_catalog_page(&client, token.trim(), page_token.as_deref())?;
        let rows = gcp_billing_sku_rows(&value)?;
        for row in rows {
            let Some(sku_id) = row.get("skuId").and_then(Value::as_str) else {
                continue;
            };
            if !remaining.contains(sku_id) {
                continue;
            }
            let entry = billing_sku_pricing_cache_entry(row)?;
            remaining.remove(sku_id);
            entries.insert(entry.sku_id.clone(), entry);
        }
        page_count += 1;
        if let Some(progress) = progress.as_ref() {
            progress.set_position(entries.len() as u64);
            progress.set_message(format!(
                "SKUs · {} mapped machine types · page {}",
                mapped_machine_count, page_count
            ));
        }
        if remaining.is_empty() {
            break;
        }
        page_token = gcp_next_page_token(&value);
        if page_token.is_none() {
            break;
        }
    }

    if !remaining.is_empty() {
        finish_progress_and_clear(&progress);
        let missing = remaining
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "Failed to refresh {} mapped GCP SKU prices. Missing SKU IDs include: {}.",
            remaining.len(),
            missing
        );
    }

    if let Some(progress) = progress {
        progress.finish_with_message(format!(
            "Loaded {} GCP pricing SKUs for {} mapped machine types.",
            entries.len(),
            mapped_machine_count
        ));
    }

    Ok(GcpSkuPricingCacheStore {
        refreshed_at_unix: now_unix_secs(),
        entries: entries.into_values().collect(),
    })
}

fn resolved_gcp_project(config: &IceConfig) -> Result<String> {
    if let Some(project) = config.auth.gcp.project.as_deref()
        && !project.trim().is_empty()
    {
        return Ok(project.trim().to_owned());
    }
    for env_key in [
        "CLOUDSDK_CORE_PROJECT",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
    ] {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_owned());
            }
        }
    }

    let mut command = command(config);
    command.args(["config", "get-value", "project", "--quiet"]);
    let project = run_command_text(&mut command, "resolve the active GCP project")?;
    if project.is_empty() || project.eq_ignore_ascii_case("(unset)") {
        bail!("No GCP project configured. Run `ice login --cloud gcp` first.");
    }
    Ok(project)
}

fn load_active_gcp_zones(client: &Client, token: &str, project: &str) -> Result<Vec<String>> {
    let progress = spinner("Loading GCP zones...");
    let mut page_token: Option<String> = None;
    let mut zones = Vec::new();

    loop {
        let value = fetch_gcp_zone_page(client, token, project, page_token.as_deref())?;
        let rows = value
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Unexpected GCP zones response shape"))?;
        for row in rows {
            let Some(zone) = row.get("name").and_then(Value::as_str) else {
                continue;
            };
            let status = row
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN");
            if status.eq_ignore_ascii_case("UP") {
                zones.push(zone.to_owned());
            }
        }
        page_token = gcp_next_page_token(&value);
        progress.set_message(format!("Loading GCP zones... {} up", zones.len()));
        if page_token.is_none() {
            break;
        }
    }

    zones.sort();
    progress.finish_and_clear();
    if zones.is_empty() {
        bail!("No active GCP zones were returned for project `{project}`.");
    }
    Ok(zones)
}

fn aggregated_machine_shapes_page(
    value: &Value,
    active_zones: &HashSet<String>,
) -> Result<(Vec<GcpMachineShape>, HashSet<String>)> {
    let items = value
        .get("items")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Unexpected GCP machine-types response shape"))?;
    let mut machines = Vec::new();
    let mut zones = HashSet::new();

    for (scope, item) in items {
        let zone = scope.rsplit('/').next().unwrap_or(scope);
        if active_zones.contains(zone) {
            zones.insert(zone.to_owned());
        }
        let Some(rows) = item.get("machineTypes").and_then(Value::as_array) else {
            continue;
        };
        machines.extend(rows.iter().filter_map(parse_machine_shape_row));
    }

    Ok((machines, zones))
}

fn fetch_gcp_billing_catalog_page(
    client: &Client,
    token: &str,
    page_token: Option<&str>,
) -> Result<Value> {
    let mut url = Url::parse(&format!(
        "https://cloudbilling.googleapis.com/v1/services/{GCP_BILLING_SERVICE}/skus"
    ))
    .context("Failed to build the GCP Billing Catalog URL")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("currencyCode", "USD");
        pairs.append_pair("pageSize", GCP_BILLING_PAGE_SIZE);
        if let Some(value) = page_token {
            pairs.append_pair("pageToken", value);
        }
    }
    client
        .get(url)
        .bearer_auth(token)
        .send()
        .context("Failed to query the GCP Billing Catalog API")?
        .error_for_status()
        .context("GCP Billing Catalog API returned an error")?
        .json::<Value>()
        .context("Failed to decode the GCP Billing Catalog API response")
}

fn fetch_gcp_zone_page(
    client: &Client,
    token: &str,
    project: &str,
    page_token: Option<&str>,
) -> Result<Value> {
    let mut url = Url::parse(&format!(
        "https://compute.googleapis.com/compute/v1/projects/{project}/zones"
    ))
    .context("Failed to build the GCP zones URL")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("filter", "status = UP");
        pairs.append_pair("maxResults", GCP_COMPUTE_PAGE_SIZE);
        if let Some(value) = page_token {
            pairs.append_pair("pageToken", value);
        }
    }
    client
        .get(url)
        .bearer_auth(token)
        .send()
        .context("Failed to query the GCP zones API")?
        .error_for_status()
        .context("GCP zones API returned an error")?
        .json::<Value>()
        .context("Failed to decode the GCP zones API response")
}

fn fetch_gcp_machine_types_page(
    client: &Client,
    token: &str,
    project: &str,
    page_token: Option<&str>,
) -> Result<Value> {
    let mut url = Url::parse(&format!(
        "https://compute.googleapis.com/compute/v1/projects/{project}/aggregated/machineTypes"
    ))
    .context("Failed to build the GCP aggregated machine-types URL")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("maxResults", GCP_COMPUTE_PAGE_SIZE);
        if let Some(value) = page_token {
            pairs.append_pair("pageToken", value);
        }
    }
    client
        .get(url)
        .bearer_auth(token)
        .send()
        .context("Failed to query the GCP aggregated machine-types API")?
        .error_for_status()
        .context("GCP aggregated machine-types API returned an error")?
        .json::<Value>()
        .context("Failed to decode the GCP aggregated machine-types API response")
}

fn gcp_billing_sku_rows(value: &Value) -> Result<&Vec<Value>> {
    value
        .get("skus")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Unexpected GCP Billing Catalog response shape"))
}

fn gcp_next_page_token(value: &Value) -> Option<String> {
    value
        .get("nextPageToken")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn billing_sku_pricing_cache_entry(sku: &Value) -> Result<GcpSkuPricingCacheEntry> {
    let sku_id = sku
        .get("skuId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Encountered GCP billing SKU without `skuId`."))?;
    let usage_type = sku
        .get("category")
        .and_then(|category| category.get("usageType"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN");
    if !usage_type.eq_ignore_ascii_case("OnDemand") {
        bail!("Mapped GCP SKU `{sku_id}` uses billing usage type `{usage_type}`, not `OnDemand`.");
    }
    Ok(GcpSkuPricingCacheEntry {
        sku_id: sku_id.to_owned(),
        description: sku
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        rate_unit: billing_sku_rate_unit(sku)?,
        usd_per_unit: billing_sku_usd_per_unit(sku)?,
    })
}

fn billing_sku_rate_unit(sku: &Value) -> Result<GcpSkuRateUnit> {
    match billing_sku_usage_unit(sku)? {
        "h" => Ok(GcpSkuRateUnit::PerHour),
        "GiBy.h" | "GiBy.mo" | "GBy.h" | "GBy.mo" => Ok(GcpSkuRateUnit::PerGibHour),
        usage_unit => bail!("Mapped GCP SKU uses unsupported usage unit `{usage_unit}`."),
    }
}

fn billing_sku_usage_unit(sku: &Value) -> Result<&str> {
    billing_sku_latest_pricing_expression(sku)?
        .get("usageUnit")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Mapped GCP SKU is missing `pricingExpression.usageUnit`."))
}

fn billing_sku_usd_per_unit(sku: &Value) -> Result<f64> {
    match billing_sku_usage_unit(sku)? {
        "h" => sku_unit_price_usd(sku)
            .ok_or_else(|| anyhow!("Mapped GCP SKU is missing a latest hourly USD unit price.")),
        "GiBy.h" | "GiBy.mo" | "GBy.h" | "GBy.mo" => sku_gib_hourly_usd(sku)
            .ok_or_else(|| anyhow!("Mapped GCP SKU is missing a latest GiB-hour USD unit price.")),
        usage_unit => bail!("Mapped GCP SKU uses unsupported usage unit `{usage_unit}`."),
    }
}

fn billing_sku_latest_pricing_expression(sku: &Value) -> Result<&Value> {
    sku.get("pricingInfo")
        .and_then(Value::as_array)
        .and_then(|rows| rows.last())
        .and_then(|row| row.get("pricingExpression"))
        .ok_or_else(|| anyhow!("Mapped GCP SKU is missing latest pricing info."))
}

fn finish_progress_and_clear(progress: &Option<ProgressBar>) {
    if let Some(progress) = progress {
        progress.finish_and_clear();
    }
}

fn parse_machine_shape_row(row: &Value) -> Option<GcpMachineShape> {
    let machine = row.get("name")?.as_str()?.to_owned();
    if should_skip_machine_type(&machine) {
        return None;
    }
    let zone = short_gcp_zone(row.get("zone").and_then(Value::as_str).unwrap_or(""));
    let region = region_from_zone(&zone);
    let vcpus = row
        .get("guestCpus")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())?;
    let ram_mb = row
        .get("memoryMb")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())?;
    let is_shared_cpu = row
        .get("isSharedCpu")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let description = row
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let bundled_local_ssd_partitions = row
        .get("bundledLocalSsds")
        .and_then(|value| value.get("partitionCount"))
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| parse_bundled_local_ssd_partitions(description))
        .unwrap_or(0);
    let billable_vcpus = if is_shared_cpu {
        shared_cpu_billable_vcpus(description, vcpus).unwrap_or(vcpus as f64)
    } else {
        vcpus as f64
    };
    let accelerators = row
        .get("accelerators")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_machine_accelerator)
        .collect::<Vec<_>>();

    Some(GcpMachineShape {
        machine,
        zone,
        region,
        vcpus,
        billable_vcpus,
        ram_mb,
        accelerators,
        bundled_local_ssd_partitions,
    })
}

fn parse_machine_accelerator(row: &Value) -> Option<GcpAccelerator> {
    let raw_type = row.get("guestAcceleratorType")?.as_str()?.to_owned();
    let count = row
        .get("guestAcceleratorCount")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(1);
    Some(GcpAccelerator {
        label: canonicalize_gpu_name_for_cloud(Cloud::Gcp, &raw_type)
            .unwrap_or_else(|| humanize_gpu_identifier(&raw_type)),
        raw_type,
        count,
    })
}

fn expand_accelerator_labels(accelerators: &[GcpAccelerator]) -> Vec<String> {
    accelerators
        .iter()
        .flat_map(|accelerator| {
            std::iter::repeat_n(accelerator.label.clone(), accelerator.count as usize)
        })
        .collect()
}

fn machine_shape_from_catalog_entry(entry: &GcpMachineCatalogEntry) -> GcpMachineShape {
    GcpMachineShape {
        machine: entry.machine.clone(),
        zone: entry.zone.clone(),
        region: entry.region.clone(),
        vcpus: entry.vcpus,
        billable_vcpus: entry.billable_vcpus,
        ram_mb: entry.ram_mb,
        accelerators: entry.accelerators.clone(),
        bundled_local_ssd_partitions: entry.bundled_local_ssd_partitions,
    }
}

fn shared_cpu_billable_vcpus(description: &str, guest_vcpus: u32) -> Option<f64> {
    let (_, tail) = description.split_once('(')?;
    let (fraction, _) = tail.split_once(" shared physical core")?;
    let (numerator, denominator) = fraction.split_once('/')?;
    let numerator = numerator.trim().parse::<f64>().ok()?;
    let denominator = denominator.trim().parse::<f64>().ok()?;
    if denominator <= 0.0 {
        return None;
    }
    Some((guest_vcpus as f64) * numerator / denominator)
}

fn should_skip_machine_type(_machine: &str) -> bool {
    false
}

fn sku_gib_hourly_usd(sku: &Value) -> Option<f64> {
    let expression = billing_sku_latest_pricing_expression(sku).ok()?;
    let price = sku_unit_price_usd(sku)?;
    match expression.get("usageUnit").and_then(Value::as_str)? {
        "GiBy.h" => Some(price),
        "GiBy.mo" | "GBy.h" | "GBy.mo" => {
            let factor = expression
                .get("baseUnitConversionFactor")
                .and_then(Value::as_f64)?;
            Some(price / factor * 1024_f64.powi(3) * 3600.0)
        }
        _ => None,
    }
}

fn sku_unit_price_usd(sku: &Value) -> Option<f64> {
    let rate = sku
        .get("pricingInfo")
        .and_then(Value::as_array)?
        .last()?
        .get("pricingExpression")?
        .get("tieredRates")?
        .as_array()?
        .iter()
        .find(|tier| {
            tier.get("startUsageAmount")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                == 0.0
        })?
        .get("unitPrice")?;
    let units = rate
        .get("units")
        .and_then(Value::as_str)
        .unwrap_or("0")
        .parse::<f64>()
        .ok()?;
    let nanos = rate.get("nanos").and_then(Value::as_i64).unwrap_or(0) as f64 / 1_000_000_000.0;
    Some(units + nanos)
}

fn humanize_gpu_identifier(raw_type: &str) -> String {
    raw_type
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .map(|token| {
            if token.chars().all(|ch| ch.is_ascii_digit()) {
                token.to_owned()
            } else if token.chars().any(|ch| ch.is_ascii_digit()) || token.len() <= 4 {
                token.to_ascii_uppercase()
            } else {
                let mut chars = token.chars();
                chars
                    .next()
                    .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                    .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_bundled_local_ssd_partitions(description: &str) -> Option<u32> {
    let (prefix, _) = description.split_once(" Local SSD")?;
    prefix.split_whitespace().last()?.parse().ok()
}

fn preferred_region(config: &IceConfig) -> Option<String> {
    config
        .default
        .gcp
        .region
        .clone()
        .or_else(|| config.default.gcp.zone.as_deref().map(region_from_zone))
}

fn preferred_zone(config: &IceConfig) -> Option<String> {
    config.default.gcp.zone.as_deref().map(short_gcp_zone)
}

fn region_from_zone(zone: &str) -> String {
    let zone = short_gcp_zone(zone);
    let mut parts = zone.split('-').collect::<Vec<_>>();
    if parts.len() >= 3 {
        parts.pop();
        parts.join("-")
    } else {
        zone
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::{
        GcpAccelerator, GcpMachineCatalogEntry, GcpMachinePricingComponent,
        GcpMachinePricingMapEntry, GcpMachinePricingMapStore, GcpMachinePricingQuantitySource,
        GcpMachineShape, GcpSkuPricingCacheEntry, GcpSkuRateUnit, aggregated_machine_shapes_page,
        billing_sku_pricing_cache_entry, build_machine_pricing_map_index,
        changed_catalog_entry_count, prepare_mapped_catalog, resolve_machine_hourly_price_from_map,
    };

    fn test_catalog_entry(machine: &str, zone: &str, hourly_usd: f64) -> GcpMachineCatalogEntry {
        GcpMachineCatalogEntry {
            machine: machine.to_owned(),
            zone: zone.to_owned(),
            region: zone
                .rsplit_once('-')
                .map_or_else(|| zone.to_owned(), |(region, _)| region.to_owned()),
            vcpus: 4,
            billable_vcpus: 4.0,
            ram_mb: 16_384,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: Vec::new(),
            hourly_usd,
        }
    }

    #[test]
    fn build_machine_pricing_map_index_rejects_skipped_entries_with_components() {
        let store = GcpMachinePricingMapStore {
            gpu_aliases: Vec::new(),
            entries: vec![GcpMachinePricingMapEntry {
                machine: "c4-standard-4".to_owned(),
                region: "us-central1".to_owned(),
                skip_reason: Some("manual".to_owned()),
                components: vec![GcpMachinePricingComponent {
                    sku_id: "sku-1".to_owned(),
                    quantity_source: GcpMachinePricingQuantitySource::BillableVcpu,
                    quantity_multiplier: 1.0,
                    accelerator_raw_type: None,
                }],
            }],
        };

        let err = build_machine_pricing_map_index(&store).expect_err("invalid skip entry");
        assert!(
            err.to_string()
                .contains("cannot set both `skip_reason` and `components`")
        );
    }

    #[test]
    fn prepare_mapped_catalog_warns_for_missing_and_stale_entries() {
        let machine_shapes = vec![
            GcpMachineShape {
                machine: "c4a-highcpu-1".to_owned(),
                zone: "africa-south1-a".to_owned(),
                region: "africa-south1".to_owned(),
                vcpus: 1,
                billable_vcpus: 1.0,
                ram_mb: 2_048,
                accelerators: Vec::new(),
                bundled_local_ssd_partitions: 0,
            },
            GcpMachineShape {
                machine: "c4-standard-4".to_owned(),
                zone: "us-central1-a".to_owned(),
                region: "us-central1".to_owned(),
                vcpus: 4,
                billable_vcpus: 4.0,
                ram_mb: 16_384,
                accelerators: Vec::new(),
                bundled_local_ssd_partitions: 0,
            },
            GcpMachineShape {
                machine: "c4-standard-8".to_owned(),
                zone: "us-central1-b".to_owned(),
                region: "us-central1".to_owned(),
                vcpus: 8,
                billable_vcpus: 8.0,
                ram_mb: 32_768,
                accelerators: Vec::new(),
                bundled_local_ssd_partitions: 0,
            },
        ];
        let store = GcpMachinePricingMapStore {
            gpu_aliases: Vec::new(),
            entries: vec![
                GcpMachinePricingMapEntry {
                    machine: "c4-standard-4".to_owned(),
                    region: "us-central1".to_owned(),
                    skip_reason: None,
                    components: vec![GcpMachinePricingComponent {
                        sku_id: "sku-1".to_owned(),
                        quantity_source: GcpMachinePricingQuantitySource::BillableVcpu,
                        quantity_multiplier: 1.0,
                        accelerator_raw_type: None,
                    }],
                },
                GcpMachinePricingMapEntry {
                    machine: "c4-standard-8".to_owned(),
                    region: "us-central1".to_owned(),
                    skip_reason: Some("unsupported".to_owned()),
                    components: Vec::new(),
                },
                GcpMachinePricingMapEntry {
                    machine: "c4-standard-16".to_owned(),
                    region: "us-central1".to_owned(),
                    skip_reason: None,
                    components: vec![GcpMachinePricingComponent {
                        sku_id: "sku-2".to_owned(),
                        quantity_source: GcpMachinePricingQuantitySource::BillableVcpu,
                        quantity_multiplier: 1.0,
                        accelerator_raw_type: None,
                    }],
                },
            ],
        };

        let index = build_machine_pricing_map_index(&store).expect("map index");
        let preparation = prepare_mapped_catalog(&machine_shapes, &store.entries, &index);

        assert_eq!(preparation.mapped.len(), 1);
        assert_eq!(preparation.required_sku_ids.len(), 1);
        assert_eq!(preparation.warnings.len(), 2);
    }

    #[test]
    fn changed_catalog_entry_count_counts_added_updated_and_deleted_entries() {
        let previous = vec![
            test_catalog_entry("c4-standard-4", "us-central1-a", 0.25),
            test_catalog_entry("c4-standard-8", "us-central1-b", 0.50),
            test_catalog_entry("c4-standard-16", "us-central1-c", 1.00),
        ];
        let current = vec![
            test_catalog_entry("c4-standard-8", "us-central1-b", 0.75),
            test_catalog_entry("c4-standard-4", "us-central1-a", 0.25),
            test_catalog_entry("c4-standard-32", "us-central1-f", 2.00),
        ];

        assert_eq!(changed_catalog_entry_count(&previous, &current), 3);
    }

    #[test]
    fn changed_catalog_entry_count_treats_missing_previous_catalog_as_all_added() {
        let current = vec![
            test_catalog_entry("c4-standard-4", "us-central1-a", 0.25),
            test_catalog_entry("c4-standard-8", "us-central1-b", 0.50),
        ];

        assert_eq!(changed_catalog_entry_count(&[], &current), current.len());
    }

    #[test]
    fn resolve_machine_hourly_price_from_map_uses_shape_quantities() {
        let shape = GcpMachineShape {
            machine: "a3-highgpu-1g".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 26,
            billable_vcpus: 24.5,
            ram_mb: 239_616,
            accelerators: vec![GcpAccelerator {
                raw_type: "nvidia-h100-80gb".to_owned(),
                label: "H100 80GB".to_owned(),
                count: 2,
            }],
            bundled_local_ssd_partitions: 2,
        };
        let mapping = GcpMachinePricingMapEntry {
            machine: shape.machine.clone(),
            region: shape.region.clone(),
            skip_reason: None,
            components: vec![
                GcpMachinePricingComponent {
                    sku_id: "cpu".to_owned(),
                    quantity_source: GcpMachinePricingQuantitySource::BillableVcpu,
                    quantity_multiplier: 1.0,
                    accelerator_raw_type: None,
                },
                GcpMachinePricingComponent {
                    sku_id: "ram".to_owned(),
                    quantity_source: GcpMachinePricingQuantitySource::RamGib,
                    quantity_multiplier: 1.0,
                    accelerator_raw_type: None,
                },
                GcpMachinePricingComponent {
                    sku_id: "gpu".to_owned(),
                    quantity_source: GcpMachinePricingQuantitySource::AcceleratorCount,
                    quantity_multiplier: 1.0,
                    accelerator_raw_type: Some("nvidia-h100-80gb".to_owned()),
                },
                GcpMachinePricingComponent {
                    sku_id: "ssd".to_owned(),
                    quantity_source: GcpMachinePricingQuantitySource::BundledLocalSsdPartitions,
                    quantity_multiplier: 375.0,
                    accelerator_raw_type: None,
                },
            ],
        };
        let pricing = [
            GcpSkuPricingCacheEntry {
                sku_id: "cpu".to_owned(),
                description: String::new(),
                rate_unit: GcpSkuRateUnit::PerHour,
                usd_per_unit: 0.25,
            },
            GcpSkuPricingCacheEntry {
                sku_id: "ram".to_owned(),
                description: String::new(),
                rate_unit: GcpSkuRateUnit::PerGibHour,
                usd_per_unit: 0.01,
            },
            GcpSkuPricingCacheEntry {
                sku_id: "gpu".to_owned(),
                description: String::new(),
                rate_unit: GcpSkuRateUnit::PerHour,
                usd_per_unit: 1.5,
            },
            GcpSkuPricingCacheEntry {
                sku_id: "ssd".to_owned(),
                description: String::new(),
                rate_unit: GcpSkuRateUnit::PerGibHour,
                usd_per_unit: 0.02,
            },
        ]
        .into_iter()
        .map(|entry| (entry.sku_id.clone(), entry))
        .collect();

        let hourly_usd =
            resolve_machine_hourly_price_from_map(&pricing, &shape, &mapping).expect("price");

        assert!((hourly_usd - 26.465).abs() < 0.000_001);
    }

    #[test]
    fn billing_sku_pricing_cache_entry_uses_latest_pricing_info() {
        let sku = json!({
            "skuId": "sku-1",
            "description": "Test SKU",
            "category": { "usageType": "OnDemand" },
            "pricingInfo": [
                {
                    "pricingExpression": {
                        "usageUnit": "h",
                        "tieredRates": [
                            {
                                "startUsageAmount": 0,
                                "unitPrice": { "units": "1", "nanos": 0 }
                            }
                        ]
                    }
                },
                {
                    "pricingExpression": {
                        "usageUnit": "h",
                        "tieredRates": [
                            {
                                "startUsageAmount": 0,
                                "unitPrice": { "units": "2", "nanos": 500000000 }
                            }
                        ]
                    }
                }
            ]
        });

        let entry = billing_sku_pricing_cache_entry(&sku).expect("sku cache entry");

        assert_eq!(entry.sku_id, "sku-1");
        assert_eq!(entry.rate_unit, GcpSkuRateUnit::PerHour);
        assert!((entry.usd_per_unit - 2.5).abs() < 0.000_001);
    }

    #[test]
    fn billing_sku_pricing_cache_entry_converts_decimal_gigabyte_hours() {
        let sku = json!({
            "skuId": "sku-2",
            "description": "Decimal memory SKU",
            "category": { "usageType": "OnDemand" },
            "pricingInfo": [{
                "pricingExpression": {
                    "usageUnit": "GBy.h",
                    "baseUnitConversionFactor": 3_600_000_000_000.0,
                    "tieredRates": [{
                        "startUsageAmount": 0,
                        "unitPrice": { "units": "0", "nanos": 100000000 }
                    }]
                }
            }]
        });

        let entry = billing_sku_pricing_cache_entry(&sku).expect("sku cache entry");

        assert_eq!(entry.rate_unit, GcpSkuRateUnit::PerGibHour);
        assert!((entry.usd_per_unit - 0.1073741824).abs() < 0.000_000_001);
    }

    #[test]
    fn parse_machine_shape_row_reads_bundled_local_ssd_partition_count() {
        let value = json!({
            "name": "c4-standard-4-lssd",
            "zone": "https://www.googleapis.com/compute/v1/projects/test/zones/us-central1-a",
            "guestCpus": 4,
            "memoryMb": 15360,
            "description": "4 vCPUs, 15 GB RAM, 1 local SSD",
            "bundledLocalSsds": {
                "partitionCount": 1,
                "defaultInterface": "NVME"
            }
        });

        let shape = super::parse_machine_shape_row(&value).expect("machine shape");

        assert_eq!(shape.bundled_local_ssd_partitions, 1);
    }

    #[test]
    fn aggregated_machine_shapes_page_tracks_active_zones_from_scope_keys() {
        let value = json!({
            "items": {
                "zones/us-central1-a": {
                    "machineTypes": [{
                        "name": "e2-standard-2",
                        "zone": "https://www.googleapis.com/compute/v1/projects/test/zones/us-central1-a",
                        "guestCpus": 2,
                        "memoryMb": 8192,
                        "description": "2 vCPUs, 8 GB RAM"
                    }]
                },
                "zones/us-central1-b": {
                    "warning": { "code": "NO_RESULTS_ON_PAGE" }
                }
            }
        });
        let active_zones = ["us-central1-a".to_owned(), "us-central1-b".to_owned()]
            .into_iter()
            .collect::<HashSet<_>>();

        let (machines, seen_zones) =
            aggregated_machine_shapes_page(&value, &active_zones).expect("aggregated page");

        assert_eq!(machines.len(), 1);
        assert_eq!(seen_zones.len(), 2);
        assert!(seen_zones.contains("us-central1-a"));
        assert!(seen_zones.contains("us-central1-b"));
    }
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
    let cache_path = gcp_provider_dir()?.join("gcp-access-token.toml");
    gcp::access_token(AccessTokenRequest {
        configured_credentials_path: config.auth.gcp.service_account_json.as_deref(),
        cache_path: &cache_path,
    })
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
    gcp::command(config.auth.gcp.service_account_json.as_deref())
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
