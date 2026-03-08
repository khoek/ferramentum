use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::model::Cloud;
use crate::support::{CONFIG_DIR_NAME, PROVIDER_DIR_NAME};

const BUNDLED_GPU_CATALOG: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/gpu-catalog.toml"
));
const BUNDLED_AWS_GPU_ALIASES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/provider/aws/gpu-aliases.toml"
));
const BUNDLED_GCP_MACHINE_PRICING_MAP: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/provider/gcp/machine-pricing-map.toml"
));
const BUNDLED_VAST_GPU_ALIASES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/provider/vast-ai/gpu-aliases.toml"
));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProviderGpuAliasEntry {
    pub(crate) raw_name: String,
    pub(crate) canonical_name: String,
    #[serde(default)]
    pub(crate) search_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GpuCatalogEntry {
    name: String,
    vram_gb: f64,
    fp32_tflops: f64,
}

#[derive(Debug, Default, Deserialize)]
struct GpuCatalogStore {
    #[serde(default)]
    reference_model: Option<String>,
    #[serde(default)]
    entries: Vec<GpuCatalogEntry>,
}

#[derive(Debug, Default, Deserialize)]
struct ProviderGpuAliasStore {
    #[serde(default)]
    aliases: Vec<ProviderGpuAliasEntry>,
}

#[derive(Debug, Default, Deserialize)]
struct GcpGpuAliasStore {
    #[serde(default)]
    gpu_aliases: Vec<ProviderGpuAliasEntry>,
}

static GPU_CATALOG: LazyLock<GpuCatalogStore> = LazyLock::new(load_gpu_catalog);
static AWS_GPU_ALIASES: LazyLock<ProviderGpuAliasStore> =
    LazyLock::new(|| load_provider_gpu_aliases(Cloud::Aws, BUNDLED_AWS_GPU_ALIASES));
static GCP_GPU_ALIASES: LazyLock<ProviderGpuAliasStore> = LazyLock::new(load_gcp_gpu_aliases);
static VAST_GPU_ALIASES: LazyLock<ProviderGpuAliasStore> =
    LazyLock::new(|| load_provider_gpu_aliases(Cloud::VastAi, BUNDLED_VAST_GPU_ALIASES));

pub(crate) fn ensure_runtime_gpu_data_files() -> Result<()> {
    ensure_gpu_catalog_file()?;
    ensure_provider_alias_file(Cloud::Aws, BUNDLED_AWS_GPU_ALIASES)?;
    ensure_gcp_machine_pricing_map_file()?;
    ensure_provider_alias_file(Cloud::VastAi, BUNDLED_VAST_GPU_ALIASES)?;
    Ok(())
}

pub(crate) fn normalize_gpu_name_token(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

pub(crate) fn canonicalize_gpu_name(input: &str) -> Option<String> {
    let normalized = normalize_gpu_name_token(input);
    canonical_name_index().get(&normalized).cloned()
}

pub(crate) fn canonicalize_gpu_name_for_cloud(cloud: Cloud, input: &str) -> Option<String> {
    let normalized = normalize_gpu_name_token(input);
    provider_alias_index(cloud)
        .get(&normalized)
        .cloned()
        .or_else(|| {
            GPU_CATALOG
                .entries
                .iter()
                .find(|entry| normalize_gpu_name_token(&entry.name) == normalized)
                .map(|entry| entry.name.clone())
        })
}

pub(crate) fn provider_gpu_options(cloud: Cloud) -> Vec<String> {
    let mut options = provider_alias_entries(cloud)
        .iter()
        .map(|entry| entry.canonical_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    options.sort_by(|left, right| {
        gpu_sort_key(left)
            .cmp(&gpu_sort_key(right))
            .then_with(|| left.cmp(right))
    });
    options
}

pub(crate) fn gpu_quality_score(model: &str) -> i64 {
    gpu_fp32_tflops(model)
        .map(|value| (value * 1000.0).round() as i64)
        .unwrap_or(0)
}

pub(crate) fn gpu_selector_label(model: &str) -> String {
    gpu_vram_gb(model).map_or_else(
        || model.to_owned(),
        |vram_gb| {
            let rendered = if (vram_gb.fract()).abs() < 1e-9 {
                format!("{vram_gb:.0}")
            } else {
                format!("{vram_gb:.1}")
            };
            format!("{model} ({rendered} GB)")
        },
    )
}

pub(crate) fn gpu_fp32_tflops(model: &str) -> Option<f64> {
    let normalized = normalize_gpu_name_token(model);
    GPU_CATALOG
        .entries
        .iter()
        .find(|entry| normalize_gpu_name_token(&entry.name) == normalized)
        .map(|entry| entry.fp32_tflops)
}

pub(crate) fn gpu_vram_gb(model: &str) -> Option<f64> {
    let normalized = normalize_gpu_name_token(model);
    GPU_CATALOG
        .entries
        .iter()
        .find(|entry| normalize_gpu_name_token(&entry.name) == normalized)
        .map(|entry| entry.vram_gb)
}

pub(crate) fn gpu_reference_model() -> Option<&'static str> {
    GPU_CATALOG.reference_model.as_deref()
}

pub(crate) fn runtime_gcp_machine_pricing_map_path() -> Result<PathBuf> {
    provider_data_path(Cloud::Gcp, "machine-pricing-map.toml")
}

pub(crate) fn runtime_provider_data_path(cloud: Cloud, file_name: &str) -> Result<PathBuf> {
    provider_data_path(cloud, file_name)
}

pub(crate) fn bundled_gcp_machine_pricing_map() -> &'static str {
    BUNDLED_GCP_MACHINE_PRICING_MAP
}

fn gpu_sort_key(model: &str) -> (bool, i64) {
    let score = gpu_quality_score(model);
    (score == 0, score)
}

fn canonical_name_index() -> &'static HashMap<String, String> {
    static LOOKUP: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        for entry in &GPU_CATALOG.entries {
            map.insert(normalize_gpu_name_token(&entry.name), entry.name.clone());
        }
        for cloud in [Cloud::Aws, Cloud::Gcp, Cloud::VastAi] {
            for entry in provider_alias_entries(cloud) {
                map.insert(
                    normalize_gpu_name_token(&entry.raw_name),
                    entry.canonical_name.clone(),
                );
            }
        }
        map
    });
    &LOOKUP
}

fn provider_alias_index(cloud: Cloud) -> HashMap<String, String> {
    provider_alias_entries(cloud)
        .iter()
        .map(|entry| {
            (
                normalize_gpu_name_token(&entry.raw_name),
                entry.canonical_name.clone(),
            )
        })
        .collect()
}

fn provider_alias_entries(cloud: Cloud) -> &'static [ProviderGpuAliasEntry] {
    match cloud {
        Cloud::Aws => &AWS_GPU_ALIASES.aliases,
        Cloud::Gcp => &GCP_GPU_ALIASES.aliases,
        Cloud::VastAi => &VAST_GPU_ALIASES.aliases,
        Cloud::Local => &[],
    }
}

fn load_gcp_gpu_aliases() -> ProviderGpuAliasStore {
    let bundled = parse_bundled_toml::<GcpGpuAliasStore>(BUNDLED_GCP_MACHINE_PRICING_MAP);
    let runtime = parse_runtime_toml::<GcpGpuAliasStore>(runtime_gcp_machine_pricing_map_path());
    merge_provider_alias_stores(
        ProviderGpuAliasStore {
            aliases: bundled.gpu_aliases,
        },
        runtime
            .map(|store| ProviderGpuAliasStore {
                aliases: store.gpu_aliases,
            })
            .unwrap_or_default(),
    )
}

fn load_gpu_catalog() -> GpuCatalogStore {
    merge_gpu_catalog_stores(
        parse_bundled_toml(BUNDLED_GPU_CATALOG),
        parse_runtime_toml(gpu_catalog_path()).unwrap_or_default(),
    )
}

fn load_provider_gpu_aliases(cloud: Cloud, bundled: &str) -> ProviderGpuAliasStore {
    merge_provider_alias_stores(
        parse_bundled_toml(bundled),
        parse_runtime_toml(provider_gpu_alias_path(cloud)).unwrap_or_default(),
    )
}

fn merge_gpu_catalog_stores(bundled: GpuCatalogStore, runtime: GpuCatalogStore) -> GpuCatalogStore {
    let mut entries = bundled
        .entries
        .into_iter()
        .map(|entry| (normalize_gpu_name_token(&entry.name), entry))
        .collect::<HashMap<_, _>>();
    for entry in runtime.entries {
        entries.insert(normalize_gpu_name_token(&entry.name), entry);
    }
    GpuCatalogStore {
        reference_model: runtime.reference_model.or(bundled.reference_model),
        entries: entries.into_values().collect(),
    }
}

fn merge_provider_alias_stores(
    bundled: ProviderGpuAliasStore,
    runtime: ProviderGpuAliasStore,
) -> ProviderGpuAliasStore {
    let mut aliases = bundled
        .aliases
        .into_iter()
        .map(|entry| (normalize_gpu_name_token(&entry.raw_name), entry))
        .collect::<HashMap<_, _>>();
    for entry in runtime.aliases {
        aliases.insert(normalize_gpu_name_token(&entry.raw_name), entry);
    }
    ProviderGpuAliasStore {
        aliases: aliases.into_values().collect(),
    }
}

fn parse_runtime_toml<T>(path: Result<PathBuf>) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    let path = path.ok()?;
    let content = fs::read_to_string(path).ok()?;
    if content.trim().is_empty() {
        return None;
    }
    toml::from_str(&content).ok()
}

fn parse_bundled_toml<T>(bundled: &str) -> T
where
    T: for<'de> Deserialize<'de>,
{
    toml::from_str(bundled).expect("bundled GPU data must be valid TOML")
}

fn seed_file(path: PathBuf, bundled: &str) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let Some(parent) = path.parent() else {
        return Err(anyhow!("Invalid GPU data path {}", path.display()));
    };
    fs::create_dir_all(parent).with_context(|| format!("Failed to create {}", parent.display()))?;
    fs::write(&path, bundled).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

fn ensure_gpu_catalog_file() -> Result<()> {
    let path = gpu_catalog_path()?;
    if !path.exists() {
        return seed_file(path, BUNDLED_GPU_CATALOG);
    }
    let Some(store) = parse_runtime_toml::<GpuCatalogStore>(Ok(path.clone())) else {
        return seed_file(path, BUNDLED_GPU_CATALOG);
    };
    if store.reference_model.is_some() {
        return Ok(());
    }
    upgrade_top_level_prefix(&path, BUNDLED_GPU_CATALOG, "[[entries]]", "reference_model")
}

fn ensure_provider_alias_file(cloud: Cloud, bundled: &str) -> Result<()> {
    seed_file(provider_gpu_alias_path(cloud)?, bundled)
}

fn ensure_gcp_machine_pricing_map_file() -> Result<()> {
    let path = runtime_gcp_machine_pricing_map_path()?;
    if !path.exists() {
        return seed_file(path, BUNDLED_GCP_MACHINE_PRICING_MAP);
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))?;
    if content.contains("[[gpu_aliases]]") {
        return Ok(());
    }
    upgrade_top_level_prefix(
        &path,
        BUNDLED_GCP_MACHINE_PRICING_MAP,
        "[[entries]]",
        "gpu_aliases",
    )
}

fn upgrade_top_level_prefix(
    path: &PathBuf,
    bundled: &str,
    marker: &str,
    field_name: &str,
) -> Result<()> {
    let content =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    if content.trim().is_empty() {
        fs::write(path, bundled).with_context(|| format!("Failed to write {}", path.display()))?;
        return Ok(());
    }
    let Some(index) = bundled.find(marker) else {
        return Err(anyhow!(
            "Bundled GPU data missing `{marker}` while upgrading {}.",
            path.display()
        ));
    };
    let prefix = bundled[..index].trim_end();
    if prefix.is_empty() {
        return Err(anyhow!(
            "Bundled GPU data missing `{field_name}` prefix while upgrading {}.",
            path.display()
        ));
    }
    let upgraded = format!("{prefix}\n\n{}", content.trim_start());
    fs::write(path, upgraded).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

fn gpu_catalog_path() -> Result<PathBuf> {
    Ok(ice_root_dir()?.join("gpu-catalog.toml"))
}

fn provider_gpu_alias_path(cloud: Cloud) -> Result<PathBuf> {
    provider_data_path(cloud, "gpu-aliases.toml")
}

fn provider_data_path(cloud: Cloud, file_name: &str) -> Result<PathBuf> {
    Ok(provider_dir_path(cloud)?.join(file_name))
}

fn provider_dir_path(cloud: Cloud) -> Result<PathBuf> {
    Ok(ice_root_dir()?
        .join(PROVIDER_DIR_NAME)
        .join(provider_slug(cloud)?))
}

fn provider_slug(cloud: Cloud) -> Result<&'static str> {
    match cloud {
        Cloud::Aws => Ok("aws"),
        Cloud::Gcp => Ok("gcp"),
        Cloud::VastAi => Ok("vast-ai"),
        Cloud::Local => Err(anyhow!("{cloud} does not use provider runtime data")),
    }
}

fn ice_root_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME))
}
