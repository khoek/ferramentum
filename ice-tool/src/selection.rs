//! Resolve per-invocation selection without writing user configuration.
use std::sync::Mutex;

use anyhow::Result;
use serde_json::{Value, json};

use crate::automation::error;
use crate::cli::CreateArgs;
use crate::model::{Cloud, CreateSearchRequirements, IceConfig};

static EFFECTIVE: Mutex<Option<Value>> = Mutex::new(None);

const EXTRA: [&str; 5] = [
    "gpu_count",
    "min_gpu_memory_gb",
    "disk_gb",
    "min_download_mbps",
    "min_upload_mbps",
];
const BASIC: [&str; 4] = ["min_cpus", "min_ram_gb", "allowed_gpus", "max_price_per_hr"];

fn provider_key(cloud: Cloud) -> &'static str {
    match cloud {
        Cloud::Verda => "verda",
        Cloud::VastAi => "vast_ai",
        Cloud::Gcp => "gcp",
        Cloud::Aws => "aws",
        Cloud::Local => "local",
    }
}

pub(crate) fn snapshot() -> Option<Value> {
    EFFECTIVE
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone()
}

pub(crate) fn resolve(config: &IceConfig, cloud: Cloud, args: &CreateArgs) -> Result<IceConfig> {
    if cloud != Cloud::Verda
        && (args.manual_cleanup
            || args.image.is_some()
            || args.location.is_some()
            || args.ssh_key_id.is_some())
    {
        return Err(error(
            "unsupported_arguments",
            "--manual-cleanup, --image, --location and --ssh-key-id currently require --cloud verda.",
            json!({"cloud": cloud}),
        ));
    }
    let runtime_hours = crate::workload::resolve_deploy_hours(config, args.hours)?;
    let mut invalid = Vec::new();
    for (flag, value) in [
        ("min_ram_gb", args.min_ram_gb),
        ("min_gpu_memory_gb", args.min_gpu_memory_gb),
        ("min_download_mbps", args.min_download_mbps),
        ("min_upload_mbps", args.min_upload_mbps),
        ("max_price_per_hr", args.max_price_per_hr),
    ] {
        if value.is_some_and(|v| !v.is_finite() || v <= 0.0) {
            invalid.push(flag);
        }
    }
    if args.min_cpus == Some(0) {
        invalid.push("min_cpus");
    }
    if args.disk_gb == Some(0) {
        invalid.push("disk_gb");
    }
    if !invalid.is_empty() {
        return Err(error(
            "invalid_arguments",
            "Search minima and price limits must be finite positive numbers.",
            json!({"filters": invalid}),
        ));
    }
    let mut supplied = json!({
        "min_cpus": args.min_cpus, "min_ram_gb": args.min_ram_gb,
        "allowed_gpus": if args.no_gpu { Some(Vec::<String>::new()) } else if args.gpus.is_empty() { None } else { Some(args.gpus.clone()) },
        "max_price_per_hr": args.max_price_per_hr, "gpu_count": args.gpu_count,
        "min_gpu_memory_gb": args.min_gpu_memory_gb, "disk_gb": args.disk_gb,
        "min_download_mbps": args.min_download_mbps, "min_upload_mbps": args.min_upload_mbps,
    });
    if cloud == Cloud::Local {
        let unsupported = supplied
            .as_object()
            .unwrap()
            .iter()
            .filter(|(_, value)| !value.is_null())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            return Err(error(
                "unsupported_filter",
                "Local workloads do not support cloud machine filters.",
                json!({"filters": unsupported}),
            ));
        }
        return Ok(config.clone());
    }
    let key = provider_key(cloud);
    // TOML permits NaN/inf but JSON represents them as null. Validate saved
    // floating-point requirements before conversion so they cannot disappear.
    let saved_floats = match cloud {
        Cloud::Verda => {
            let d = &config.default.verda;
            [
                d.min_ram_gb,
                d.max_price_per_hr,
                d.min_gpu_memory_gb,
                d.min_download_mbps,
                d.min_upload_mbps,
            ]
        }
        Cloud::VastAi => {
            let d = &config.default.vast_ai;
            [
                d.min_ram_gb,
                d.max_price_per_hr,
                d.min_gpu_memory_gb,
                d.min_download_mbps,
                d.min_upload_mbps,
            ]
        }
        Cloud::Gcp => {
            let d = &config.default.gcp;
            [
                d.min_ram_gb,
                d.max_price_per_hr,
                d.min_gpu_memory_gb,
                d.min_download_mbps,
                d.min_upload_mbps,
            ]
        }
        Cloud::Aws => {
            let d = &config.default.aws;
            [
                d.min_ram_gb,
                d.max_price_per_hr,
                d.min_gpu_memory_gb,
                d.min_download_mbps,
                d.min_upload_mbps,
            ]
        }
        Cloud::Local => unreachable!(),
    };
    if !args.no_defaults {
        let invalid = [
            "min_ram_gb",
            "max_price_per_hr",
            "min_gpu_memory_gb",
            "min_download_mbps",
            "min_upload_mbps",
        ]
        .into_iter()
        .zip(saved_floats)
        .filter(|(field, value)| {
            supplied[*field].is_null() && value.is_some_and(|v| !v.is_finite() || v <= 0.0)
        })
        .map(|(field, _)| field)
        .collect::<Vec<_>>();
        if !invalid.is_empty() {
            return Err(error(
                "invalid_arguments",
                "Saved search requirements must be finite positive numbers. Override them or use --no-defaults.",
                json!({"filters": invalid, "source": "saved_configuration"}),
            ));
        }
    }
    let mut raw = serde_json::to_value(config)?;
    if args.no_defaults {
        for field in BASIC.into_iter().chain(EXTRA) {
            raw["default"][key][field] = Value::Null;
        }
        match cloud {
            Cloud::Gcp => raw["default"][key]["boot_disk_gb"] = Value::Null,
            Cloud::Aws => raw["default"][key]["root_disk_gb"] = Value::Null,
            Cloud::Verda => {
                raw["default"][key]["image"] = Value::Null;
                raw["default"][key]["location"] = Value::Null;
            }
            _ => (),
        }
    }
    for field in EXTRA {
        if !supplied[field].is_null() {
            raw["default"][key][field] = supplied[field].clone();
        }
    }
    let mut effective: IceConfig = serde_json::from_value(raw)?;
    crate::app::apply_create_search_overrides(&mut effective, cloud, args)?;
    if cloud == Cloud::VastAi
        && let Some(machine) = args
            .machine
            .as_deref()
            .filter(|name| !name.trim().is_empty())
    {
        let gpu =
            crate::gpu::canonicalize_gpu_name(machine).unwrap_or_else(|| machine.trim().to_owned());
        effective.default.vast_ai.allowed_gpus = Some(vec![gpu.clone()]);
        supplied["allowed_gpus"] = json!([gpu]);
    }
    let aws_cpu_default = cloud == Cloud::Aws
        && effective.default.aws.gpu_count.is_none()
        && effective
            .default
            .aws
            .allowed_gpus
            .as_ref()
            .is_none_or(|gpus| gpus.is_empty());
    if aws_cpu_default {
        effective.default.aws.gpu_count = Some(0);
    }
    if cloud == Cloud::Verda {
        let d = &mut effective.default.verda;
        if let Some(value) = &args.image {
            d.image = Some(value.clone());
        }
        if let Some(value) = &args.location {
            d.location = Some(value.clone());
        }
        if let Some(value) = &args.ssh_key_id {
            d.ssh_key_id = Some(value.clone());
        }
    }
    let raw = serde_json::to_value(&effective)?;
    let defaults = &raw["default"][key];
    let mut invalid = Vec::new();
    for field in [
        "min_cpus",
        "min_ram_gb",
        "max_price_per_hr",
        "min_gpu_memory_gb",
        "disk_gb",
        "min_download_mbps",
        "min_upload_mbps",
    ] {
        if !defaults[field].is_null()
            && !defaults[field]
                .as_f64()
                .is_some_and(|v| v.is_finite() && v > 0.0)
        {
            invalid.push(field);
        }
    }
    let count = defaults["gpu_count"].as_u64();
    if count == Some(0)
        && (defaults["allowed_gpus"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
            || !defaults["min_gpu_memory_gb"].is_null())
    {
        invalid.push("gpu_count");
    }
    if !invalid.is_empty() {
        return Err(error(
            "invalid_arguments",
            "Invalid or conflicting search requirements. GPU count zero conflicts with GPU models/memory; numeric minima must be positive.",
            json!({"filters": invalid}),
        ));
    }
    // Existing catalogs do not contain per-card memory, measured network rates
    // or a trustworthy GPU count. Reject unsupported requirements explicitly.
    if cloud != Cloud::VastAi {
        let mut unsupported = ["min_gpu_memory_gb", "min_download_mbps", "min_upload_mbps"]
            .into_iter()
            .filter(|field| !defaults[*field].is_null())
            .collect::<Vec<_>>();
        if cloud == Cloud::Verda {
            unsupported.retain(|field| *field != "min_gpu_memory_gb");
        }
        if cloud != Cloud::Verda && count.is_some_and(|n| n > 0) {
            unsupported.push("gpu_count");
        }
        if !unsupported.is_empty() {
            return Err(error(
                "unsupported_filter",
                format!(
                    "{cloud} cannot enforce these filters. Remove them or choose a provider that supports them."
                ),
                json!({"filters": unsupported, "cloud": cloud}),
            ));
        }
    }
    if let Some(size) = defaults["disk_gb"].as_u64() {
        match cloud {
            Cloud::Gcp => effective.default.gcp.boot_disk_gb = Some(size as u32),
            Cloud::Aws => effective.default.aws.root_disk_gb = Some(size as u32),
            _ => (),
        }
    }
    let mut filters = json!({});
    for field in BASIC.into_iter().chain(EXTRA) {
        let mut value = defaults[field].clone();
        let source = if !supplied[field].is_null() {
            "command_line"
        } else if aws_cpu_default && field == "gpu_count" {
            "built_in"
        } else if !value.is_null() {
            "saved_configuration"
        } else if field == "disk_gb" {
            value = crate::output::allocated_disk_gb(&effective, cloud)
                .map_or(Value::Null, |v| json!(v));
            match cloud {
                Cloud::Gcp if effective.default.gcp.boot_disk_gb.is_some() => "saved_configuration",
                Cloud::Aws if effective.default.aws.root_disk_gb.is_some() => "saved_configuration",
                _ => "built_in",
            }
        } else {
            "unspecified"
        };
        filters[field] = json!({"value": value, "source": source});
    }
    let mut provider_options = json!({});
    if cloud == Cloud::Verda {
        provider_options["rental_type"] = json!({"value":"on_demand", "source":"built_in"});
        for (field, supplied) in [
            ("image", &args.image),
            ("location", &args.location),
            ("ssh_key_id", &args.ssh_key_id),
        ] {
            provider_options[field] = json!({"value": defaults[field], "source": if supplied.is_some() { "command_line" } else if !defaults[field].is_null() { "saved_configuration" } else { "unspecified" }});
        }
    }
    *EFFECTIVE.lock().unwrap_or_else(|err| err.into_inner()) = Some(json!({
        "config_path": crate::config_store::config_path()?, "saved_filters_ignored": args.no_defaults,
        "filters": filters,
        "runtime_hours": {"value": runtime_hours, "source": if args.hours.is_some() { "command_line" } else if config.default.runtime_hours.is_some() { "saved_configuration" } else { "built_in" }},
        "machine": args.machine,
        "provider_options": provider_options,
        "price_scope": if cloud == Cloud::Verda { "compute_and_os_storage" } else if cloud == Cloud::VastAi { "compute_and_allocated_storage" } else { "compute" },
    }));
    crate::provision::build_search_requirements(&effective, cloud)?;
    Ok(effective)
}

pub(crate) fn record(search: &CreateSearchRequirements) {
    let mut guard = EFFECTIVE.lock().unwrap_or_else(|err| err.into_inner());
    let Some(effective) = guard.as_mut() else {
        return;
    };
    let raw = serde_json::to_value(search).expect("finite validated requirements");
    for field in BASIC {
        let old = &effective["filters"][field]["value"];
        if old.is_null()
            && (raw[field] == json!(0) || raw[field] == json!(0.0) || raw[field] == json!([]))
        {
            continue;
        }
        if *old != raw[field] {
            effective["filters"][field] = json!({"value": raw[field], "source": "interactive"});
        }
    }
    if !crate::output::is_json() {
        eprintln!(
            "Search filters (config: {}):",
            effective["config_path"].as_str().unwrap_or("unknown")
        );
        for (field, setting) in effective["filters"].as_object().unwrap() {
            eprintln!(
                "  {field}={} ({})",
                setting["value"],
                setting["source"].as_str().unwrap()
            );
        }
    }
}
