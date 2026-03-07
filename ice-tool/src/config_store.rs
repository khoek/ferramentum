use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};

use crate::model::{Cloud, IceConfig};
use crate::provision::canonicalize_gpu_name;
use crate::support::{CONFIG_DIR_NAME, CONFIG_FILE_NAME, nonempty_string};

pub(crate) fn parse_key_value_pair(pair: &str) -> Result<(String, String)> {
    let (key, value) = pair.split_once('=').ok_or_else(|| {
        anyhow!("Expected `KEY=VALUE`. Example: `ice config set default.cloud=vast.ai`.")
    })?;

    let key = normalize_config_key(key)?;

    Ok((key.to_owned(), value.trim().to_owned()))
}

pub(crate) fn supported_config_keys() -> &'static [&'static str] {
    &[
        "default.cloud",
        "default.runtime_hours",
        "default.vast_ai.min_cpus",
        "default.vast_ai.min_ram_gb",
        "default.vast_ai.allowed_gpus",
        "default.vast_ai.max_price_per_hr",
        "default.gcp.min_cpus",
        "default.gcp.min_ram_gb",
        "default.gcp.allowed_gpus",
        "default.gcp.max_price_per_hr",
        "default.aws.min_cpus",
        "default.aws.min_ram_gb",
        "default.aws.allowed_gpus",
        "default.aws.max_price_per_hr",
        "default.gcp.region",
        "default.gcp.zone",
        "default.gcp.image_family",
        "default.gcp.image_project",
        "default.gcp.boot_disk_gb",
        "default.aws.region",
        "default.aws.ami",
        "default.aws.key_name",
        "default.aws.ssh_key_path",
        "default.aws.ssh_user",
        "default.aws.security_group_id",
        "default.aws.subnet_id",
        "default.aws.root_disk_gb",
        "auth.vast_ai.api_key",
        "auth.gcp.project",
        "auth.gcp.service_account_json",
        "auth.aws.access_key_id",
        "auth.aws.secret_access_key",
    ]
}

pub(crate) fn normalize_config_key(key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        bail!("Config key cannot be empty.");
    }
    if !supported_config_keys()
        .iter()
        .any(|candidate| *candidate == key)
    {
        bail!("Unknown config key `{key}`. Use `ice config list` to see supported keys.");
    }
    Ok(key.to_owned())
}

pub(crate) fn get_config_value(config: &IceConfig, key: &str) -> Result<String> {
    let key = normalize_config_key(key)?;
    let value = match key.as_str() {
        "default.cloud" => config.default.cloud.map(|cloud| cloud.to_string()),
        "default.runtime_hours" => config
            .default
            .runtime_hours
            .map(|value| format!("{value:.3}")),
        "default.vast_ai.min_cpus" => config
            .default
            .vast_ai
            .min_cpus
            .map(|value| value.to_string()),
        "default.vast_ai.min_ram_gb" => config
            .default
            .vast_ai
            .min_ram_gb
            .map(|value| value.to_string()),
        "default.vast_ai.allowed_gpus" => config
            .default
            .vast_ai
            .allowed_gpus
            .as_ref()
            .map(|values| values.join(",")),
        "default.vast_ai.max_price_per_hr" => config
            .default
            .vast_ai
            .max_price_per_hr
            .map(|value| format!("{value:.4}")),
        "default.gcp.min_cpus" => config.default.gcp.min_cpus.map(|value| value.to_string()),
        "default.gcp.min_ram_gb" => config.default.gcp.min_ram_gb.map(|value| value.to_string()),
        "default.gcp.allowed_gpus" => config
            .default
            .gcp
            .allowed_gpus
            .as_ref()
            .map(|values| values.join(",")),
        "default.gcp.max_price_per_hr" => config
            .default
            .gcp
            .max_price_per_hr
            .map(|value| format!("{value:.4}")),
        "default.aws.min_cpus" => config.default.aws.min_cpus.map(|value| value.to_string()),
        "default.aws.min_ram_gb" => config.default.aws.min_ram_gb.map(|value| value.to_string()),
        "default.aws.allowed_gpus" => config
            .default
            .aws
            .allowed_gpus
            .as_ref()
            .map(|values| values.join(",")),
        "default.aws.max_price_per_hr" => config
            .default
            .aws
            .max_price_per_hr
            .map(|value| format!("{value:.4}")),
        "default.gcp.region" => config.default.gcp.region.clone(),
        "default.gcp.zone" => config.default.gcp.zone.clone(),
        "default.gcp.image_family" => config.default.gcp.image_family.clone(),
        "default.gcp.image_project" => config.default.gcp.image_project.clone(),
        "default.gcp.boot_disk_gb" => config
            .default
            .gcp
            .boot_disk_gb
            .map(|value| value.to_string()),
        "default.aws.region" => config.default.aws.region.clone(),
        "default.aws.ami" => config.default.aws.ami.clone(),
        "default.aws.key_name" => config.default.aws.key_name.clone(),
        "default.aws.ssh_key_path" => config.default.aws.ssh_key_path.clone(),
        "default.aws.ssh_user" => config.default.aws.ssh_user.clone(),
        "default.aws.security_group_id" => config.default.aws.security_group_id.clone(),
        "default.aws.subnet_id" => config.default.aws.subnet_id.clone(),
        "default.aws.root_disk_gb" => config
            .default
            .aws
            .root_disk_gb
            .map(|value| value.to_string()),
        "auth.vast_ai.api_key" => config
            .auth
            .vast_ai
            .api_key
            .as_ref()
            .map(|_| "<redacted>".to_owned()),
        "auth.gcp.project" => config.auth.gcp.project.clone(),
        "auth.gcp.service_account_json" => config.auth.gcp.service_account_json.clone(),
        "auth.aws.access_key_id" => config
            .auth
            .aws
            .access_key_id
            .as_ref()
            .map(|_| "<redacted>".to_owned()),
        "auth.aws.secret_access_key" => config
            .auth
            .aws
            .secret_access_key
            .as_ref()
            .map(|_| "<redacted>".to_owned()),
        _ => unreachable!(),
    };

    Ok(value.unwrap_or_else(|| "<unset>".to_owned()))
}

fn parse_positive_u32_config_value(key: &str, value: &str) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .with_context(|| format!("`{key}` expects an integer"))?;
    if parsed == 0 {
        bail!("`{key}` must be >= 1");
    }
    Ok(parsed)
}

fn parse_allowed_gpus_config_value(key: &str, value: &str) -> Result<Option<Vec<String>>> {
    if value.trim().is_empty() {
        return Ok(None);
    }

    let mut parsed = Vec::new();
    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let canonical =
            canonicalize_gpu_name(token).ok_or_else(|| anyhow!("Unknown GPU model `{token}`."))?;
        parsed.push(canonical);
    }

    parsed.sort();
    parsed.dedup();
    if parsed.is_empty() {
        bail!("`{key}` cannot be empty.");
    }

    Ok(Some(parsed))
}

fn parse_positive_f64_config_value(key: &str, value: &str) -> Result<f64> {
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("`{key}` expects a float"))?;
    if !(parsed.is_finite() && parsed > 0.0) {
        bail!("`{key}` must be a finite number > 0.");
    }
    Ok(parsed)
}

pub(crate) fn set_config_value(config: &mut IceConfig, key: &str, value: &str) -> Result<String> {
    let key = normalize_config_key(key)?;
    match key.as_str() {
        "default.cloud" => {
            let cloud = parse_cloud(value)?;
            config.default.cloud = Some(cloud);
            Ok(cloud.to_string())
        }
        "default.runtime_hours" => {
            let parsed = parse_positive_f64_config_value(&key, value)?;
            config.default.runtime_hours = Some(parsed);
            Ok(format!("{parsed:.3}"))
        }
        "default.vast_ai.min_cpus" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.vast_ai.min_cpus = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.vast_ai.min_ram_gb" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.vast_ai.min_ram_gb = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.vast_ai.allowed_gpus" => {
            let parsed = parse_allowed_gpus_config_value(&key, value)?;
            config.default.vast_ai.allowed_gpus = parsed.clone();
            Ok(parsed
                .map(|values| values.join(","))
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.vast_ai.max_price_per_hr" => {
            let parsed = parse_positive_f64_config_value(&key, value)?;
            config.default.vast_ai.max_price_per_hr = Some(parsed);
            Ok(format!("{parsed:.4}"))
        }
        "default.gcp.min_cpus" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.gcp.min_cpus = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.gcp.min_ram_gb" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.gcp.min_ram_gb = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.gcp.allowed_gpus" => {
            let parsed = parse_allowed_gpus_config_value(&key, value)?;
            config.default.gcp.allowed_gpus = parsed.clone();
            Ok(parsed
                .map(|values| values.join(","))
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.gcp.max_price_per_hr" => {
            let parsed = parse_positive_f64_config_value(&key, value)?;
            config.default.gcp.max_price_per_hr = Some(parsed);
            Ok(format!("{parsed:.4}"))
        }
        "default.aws.min_cpus" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.aws.min_cpus = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.aws.min_ram_gb" => {
            let parsed = parse_positive_u32_config_value(&key, value)?;
            config.default.aws.min_ram_gb = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.aws.allowed_gpus" => {
            let parsed = parse_allowed_gpus_config_value(&key, value)?;
            config.default.aws.allowed_gpus = parsed.clone();
            Ok(parsed
                .map(|values| values.join(","))
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.max_price_per_hr" => {
            let parsed = parse_positive_f64_config_value(&key, value)?;
            config.default.aws.max_price_per_hr = Some(parsed);
            Ok(format!("{parsed:.4}"))
        }
        "auth.vast_ai.api_key" => {
            if value.trim().is_empty() {
                config.auth.vast_ai.api_key = None;
                return Ok("<unset>".to_owned());
            }
            config.auth.vast_ai.api_key = Some(value.to_owned());
            Ok("<redacted>".to_owned())
        }
        "auth.gcp.project" => {
            config.auth.gcp.project = nonempty_string(value.to_owned());
            Ok(config
                .auth
                .gcp
                .project
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "auth.gcp.service_account_json" => {
            config.auth.gcp.service_account_json = nonempty_string(value.to_owned());
            Ok(config
                .auth
                .gcp
                .service_account_json
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "auth.aws.access_key_id" => {
            config.auth.aws.access_key_id = nonempty_string(value.to_owned());
            Ok(if config.auth.aws.access_key_id.is_some() {
                "<redacted>".to_owned()
            } else {
                "<unset>".to_owned()
            })
        }
        "auth.aws.secret_access_key" => {
            config.auth.aws.secret_access_key = nonempty_string(value.to_owned());
            Ok(if config.auth.aws.secret_access_key.is_some() {
                "<redacted>".to_owned()
            } else {
                "<unset>".to_owned()
            })
        }
        "default.gcp.region" => {
            config.default.gcp.region = nonempty_string(value.to_owned());
            Ok(config
                .default
                .gcp
                .region
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.gcp.zone" => {
            config.default.gcp.zone = nonempty_string(value.to_owned());
            Ok(config
                .default
                .gcp
                .zone
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.gcp.image_family" => {
            config.default.gcp.image_family = nonempty_string(value.to_owned());
            Ok(config
                .default
                .gcp
                .image_family
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.gcp.image_project" => {
            config.default.gcp.image_project = nonempty_string(value.to_owned());
            Ok(config
                .default
                .gcp
                .image_project
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.gcp.boot_disk_gb" => {
            if value.trim().is_empty() {
                config.default.gcp.boot_disk_gb = None;
                return Ok("<unset>".to_owned());
            }
            let parsed = value
                .parse::<u32>()
                .with_context(|| format!("`{key}` expects an integer"))?;
            if parsed == 0 {
                bail!("`{key}` must be >= 1");
            }
            config.default.gcp.boot_disk_gb = Some(parsed);
            Ok(parsed.to_string())
        }
        "default.aws.region" => {
            config.default.aws.region = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .region
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.ami" => {
            config.default.aws.ami = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .ami
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.key_name" => {
            config.default.aws.key_name = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .key_name
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.ssh_key_path" => {
            config.default.aws.ssh_key_path = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .ssh_key_path
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.ssh_user" => {
            config.default.aws.ssh_user = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .ssh_user
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.security_group_id" => {
            config.default.aws.security_group_id = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .security_group_id
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.subnet_id" => {
            config.default.aws.subnet_id = nonempty_string(value.to_owned());
            Ok(config
                .default
                .aws
                .subnet_id
                .clone()
                .unwrap_or_else(|| "<unset>".to_owned()))
        }
        "default.aws.root_disk_gb" => {
            if value.trim().is_empty() {
                config.default.aws.root_disk_gb = None;
                return Ok("<unset>".to_owned());
            }
            let parsed = value
                .parse::<u32>()
                .with_context(|| format!("`{key}` expects an integer"))?;
            if parsed == 0 {
                bail!("`{key}` must be >= 1");
            }
            config.default.aws.root_disk_gb = Some(parsed);
            Ok(parsed.to_string())
        }
        _ => unreachable!(),
    }
}

pub(crate) fn unset_config_value(config: &mut IceConfig, key: &str) -> Result<()> {
    let key = normalize_config_key(key)?;
    match key.as_str() {
        "default.cloud" => config.default.cloud = None,
        "default.runtime_hours" => config.default.runtime_hours = None,
        "default.vast_ai.min_cpus" => config.default.vast_ai.min_cpus = None,
        "default.vast_ai.min_ram_gb" => config.default.vast_ai.min_ram_gb = None,
        "default.vast_ai.allowed_gpus" => config.default.vast_ai.allowed_gpus = None,
        "default.vast_ai.max_price_per_hr" => config.default.vast_ai.max_price_per_hr = None,
        "default.gcp.min_cpus" => config.default.gcp.min_cpus = None,
        "default.gcp.min_ram_gb" => config.default.gcp.min_ram_gb = None,
        "default.gcp.allowed_gpus" => config.default.gcp.allowed_gpus = None,
        "default.gcp.max_price_per_hr" => config.default.gcp.max_price_per_hr = None,
        "default.aws.min_cpus" => config.default.aws.min_cpus = None,
        "default.aws.min_ram_gb" => config.default.aws.min_ram_gb = None,
        "default.aws.allowed_gpus" => config.default.aws.allowed_gpus = None,
        "default.aws.max_price_per_hr" => config.default.aws.max_price_per_hr = None,
        "default.gcp.region" => config.default.gcp.region = None,
        "default.gcp.zone" => config.default.gcp.zone = None,
        "default.gcp.image_family" => config.default.gcp.image_family = None,
        "default.gcp.image_project" => config.default.gcp.image_project = None,
        "default.gcp.boot_disk_gb" => config.default.gcp.boot_disk_gb = None,
        "default.aws.region" => config.default.aws.region = None,
        "default.aws.ami" => config.default.aws.ami = None,
        "default.aws.key_name" => config.default.aws.key_name = None,
        "default.aws.ssh_key_path" => config.default.aws.ssh_key_path = None,
        "default.aws.ssh_user" => config.default.aws.ssh_user = None,
        "default.aws.security_group_id" => config.default.aws.security_group_id = None,
        "default.aws.subnet_id" => config.default.aws.subnet_id = None,
        "default.aws.root_disk_gb" => config.default.aws.root_disk_gb = None,
        "auth.vast_ai.api_key" => config.auth.vast_ai.api_key = None,
        "auth.gcp.project" => config.auth.gcp.project = None,
        "auth.gcp.service_account_json" => config.auth.gcp.service_account_json = None,
        "auth.aws.access_key_id" => config.auth.aws.access_key_id = None,
        "auth.aws.secret_access_key" => config.auth.aws.secret_access_key = None,
        _ => unreachable!(),
    }
    Ok(())
}

pub(crate) fn parse_cloud(value: &str) -> Result<Cloud> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "vast.ai" | "vast" => Ok(Cloud::VastAi),
        "gcp" => Ok(Cloud::Gcp),
        "aws" => Ok(Cloud::Aws),
        "local" => Ok(Cloud::Local),
        _ => bail!("Invalid cloud `{value}`. Use `vast.ai`, `gcp`, `aws`, `local`, etc."),
    }
}

fn config_path() -> Result<PathBuf> {
    Ok(ice_root_dir()?.join(CONFIG_FILE_NAME))
}

pub(crate) fn ice_root_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME))
}

pub(crate) fn load_config() -> Result<IceConfig> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(IceConfig::default());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    if content.trim().is_empty() {
        return Ok(IceConfig::default());
    }

    let config: IceConfig = toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
    Ok(config)
}

pub(crate) fn save_config(config: &IceConfig) -> Result<PathBuf> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
    }

    let content = toml::to_string_pretty(config).context("Failed to serialize config TOML")?;
    fs::write(&path, content)
        .with_context(|| format!("Failed to write config file: {}", path.display()))?;
    Ok(path)
}
