use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use dialoguer::console::{Key, Term};
use dialoguer::{Input, Select};
use serde_json::json;

use crate::config_store::save_config;
use crate::model::{
    Cloud, CloudMachineCandidate, CreateSearchRequirements, IceConfig, MachineTypeSpec,
    OfferDecision, RuntimeCostEstimate, VastAutoStopPlan,
};
use crate::providers::vast::{VastClient, VastOffer};
use crate::support::{
    KNOWN_VAST_GPU_MODELS, now_unix_secs, prompt_f64, prompt_theme, prompt_u32,
    require_interactive, required_runtime_seconds, spinner,
};

const VAST_DEFAULT_DISK_GB: f64 = 32.0;
const VAST_DEFAULT_SEARCH_LIMIT: u64 = 200;

pub(crate) const GCP_MACHINE_SPECS: &[MachineTypeSpec] = &[
    MachineTypeSpec {
        cloud: Cloud::Gcp,
        machine: "g2-standard-4",
        vcpus: 4,
        ram_gb: 16,
        gpus: &["L4"],
        hourly_usd: 0.71,
        regions: &["us-central1", "us-east1", "us-west4", "europe-west4"],
    },
    MachineTypeSpec {
        cloud: Cloud::Gcp,
        machine: "g2-standard-8",
        vcpus: 8,
        ram_gb: 32,
        gpus: &["L4"],
        hourly_usd: 1.42,
        regions: &["us-central1", "us-east1", "us-west4", "europe-west4"],
    },
    MachineTypeSpec {
        cloud: Cloud::Gcp,
        machine: "a2-highgpu-1g",
        vcpus: 12,
        ram_gb: 85,
        gpus: &["A100 PCIE"],
        hourly_usd: 2.93,
        regions: &["us-central1", "us-east1", "us-west4"],
    },
    MachineTypeSpec {
        cloud: Cloud::Gcp,
        machine: "a3-highgpu-1g",
        vcpus: 26,
        ram_gb: 234,
        gpus: &["H100 SXM"],
        hourly_usd: 7.20,
        regions: &["us-central1", "us-east1", "us-west4"],
    },
    MachineTypeSpec {
        cloud: Cloud::Gcp,
        machine: "n2-standard-8",
        vcpus: 8,
        ram_gb: 32,
        gpus: &[],
        hourly_usd: 0.38,
        regions: &["us-central1", "us-east1", "us-west4", "europe-west4"],
    },
];

pub(crate) const AWS_MACHINE_SPECS: &[MachineTypeSpec] = &[
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "g4dn.xlarge",
        vcpus: 4,
        ram_gb: 16,
        gpus: &["Tesla T4"],
        hourly_usd: 0.526,
        regions: &["us-east-1", "us-west-2", "eu-west-1"],
    },
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "g5.xlarge",
        vcpus: 4,
        ram_gb: 16,
        gpus: &["A10"],
        hourly_usd: 1.006,
        regions: &["us-east-1", "us-west-2", "eu-west-1"],
    },
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "g6.xlarge",
        vcpus: 4,
        ram_gb: 16,
        gpus: &["L4"],
        hourly_usd: 0.89,
        regions: &["us-east-1", "us-west-2"],
    },
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "p3.2xlarge",
        vcpus: 8,
        ram_gb: 61,
        gpus: &["Tesla V100"],
        hourly_usd: 3.06,
        regions: &["us-east-1", "us-west-2"],
    },
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "p4d.24xlarge",
        vcpus: 96,
        ram_gb: 1152,
        gpus: &["A100 SXM4"],
        hourly_usd: 32.77,
        regions: &["us-east-1", "us-west-2"],
    },
    MachineTypeSpec {
        cloud: Cloud::Aws,
        machine: "c7i.2xlarge",
        vcpus: 8,
        ram_gb: 16,
        gpus: &[],
        hourly_usd: 0.34,
        regions: &["us-east-1", "us-west-2", "eu-west-1"],
    },
];

fn cloud_search_key_prefix(cloud: Cloud) -> &'static str {
    match cloud {
        Cloud::VastAi => "default.vast_ai",
        Cloud::Gcp => "default.gcp",
        Cloud::Aws => "default.aws",
        Cloud::Local => unreachable!("local does not use marketplace search defaults"),
    }
}

fn cloud_search_defaults_mut(
    config: &mut IceConfig,
    cloud: Cloud,
) -> (
    &mut Option<u32>,
    &mut Option<u32>,
    &mut Option<Vec<String>>,
    &mut Option<f64>,
) {
    match cloud {
        Cloud::VastAi => (
            &mut config.default.vast_ai.min_cpus,
            &mut config.default.vast_ai.min_ram_gb,
            &mut config.default.vast_ai.allowed_gpus,
            &mut config.default.vast_ai.max_price_per_hr,
        ),
        Cloud::Gcp => (
            &mut config.default.gcp.min_cpus,
            &mut config.default.gcp.min_ram_gb,
            &mut config.default.gcp.allowed_gpus,
            &mut config.default.gcp.max_price_per_hr,
        ),
        Cloud::Aws => (
            &mut config.default.aws.min_cpus,
            &mut config.default.aws.min_ram_gb,
            &mut config.default.aws.allowed_gpus,
            &mut config.default.aws.max_price_per_hr,
        ),
        Cloud::Local => unreachable!("local does not use marketplace search defaults"),
    }
}

fn cloud_search_defaults(
    config: &IceConfig,
    cloud: Cloud,
) -> (
    &Option<u32>,
    &Option<u32>,
    &Option<Vec<String>>,
    &Option<f64>,
) {
    match cloud {
        Cloud::VastAi => (
            &config.default.vast_ai.min_cpus,
            &config.default.vast_ai.min_ram_gb,
            &config.default.vast_ai.allowed_gpus,
            &config.default.vast_ai.max_price_per_hr,
        ),
        Cloud::Gcp => (
            &config.default.gcp.min_cpus,
            &config.default.gcp.min_ram_gb,
            &config.default.gcp.allowed_gpus,
            &config.default.gcp.max_price_per_hr,
        ),
        Cloud::Aws => (
            &config.default.aws.min_cpus,
            &config.default.aws.min_ram_gb,
            &config.default.aws.allowed_gpus,
            &config.default.aws.max_price_per_hr,
        ),
        Cloud::Local => unreachable!("local does not use marketplace search defaults"),
    }
}

pub(crate) fn ensure_default_create_config(
    config: &mut IceConfig,
    cloud: Cloud,
    gpu_options: &[String],
) -> Result<()> {
    let mut changed = false;
    let key_prefix = cloud_search_key_prefix(cloud);

    {
        let (min_cpus, min_ram_gb, allowed_gpus, max_price_per_hr) =
            cloud_search_defaults_mut(config, cloud);

        if min_cpus.is_none() {
            let value = prompt_u32(&format!("Minimum vCPUs ({cloud})"), Some(8), 1)?;
            *min_cpus = Some(value);
            changed = true;
        }

        if min_ram_gb.is_none() {
            let value = prompt_u32(&format!("Minimum RAM (GB) ({cloud})"), Some(32), 1)?;
            *min_ram_gb = Some(value);
            changed = true;
        }

        if allowed_gpus
            .as_ref()
            .map(|items| items.is_empty())
            .unwrap_or(true)
        {
            let selected = prompt_gpu_checklist(gpu_options, &[])?;
            *allowed_gpus = Some(selected);
            changed = true;
        }

        if max_price_per_hr.is_none() {
            let value = prompt_f64(
                &format!("Max price per hour (USD) ({cloud})"),
                Some(1.0),
                0.0001,
            )?;
            *max_price_per_hr = Some(value);
            changed = true;
        }
    }

    if changed {
        let path = save_config(config)?;
        eprintln!("Updated {key_prefix} search defaults in {}", path.display());
    }

    Ok(())
}

pub(crate) fn build_search_requirements(
    config: &IceConfig,
    cloud: Cloud,
) -> Result<CreateSearchRequirements> {
    let key_prefix = cloud_search_key_prefix(cloud);
    let (min_cpus_ref, min_ram_gb_ref, allowed_gpus_ref, max_price_per_hr_ref) =
        cloud_search_defaults(config, cloud);

    let min_cpus = (*min_cpus_ref).ok_or_else(|| anyhow!("{key_prefix}.min_cpus is not set"))?;
    let min_ram_gb =
        (*min_ram_gb_ref).ok_or_else(|| anyhow!("{key_prefix}.min_ram_gb is not set"))?;
    let allowed_gpus = allowed_gpus_ref
        .clone()
        .ok_or_else(|| anyhow!("{key_prefix}.allowed_gpus is not set"))?;
    if allowed_gpus.is_empty() {
        bail!("{key_prefix}.allowed_gpus cannot be empty");
    }

    let max_price_per_hr = (*max_price_per_hr_ref)
        .ok_or_else(|| anyhow!("{key_prefix}.max_price_per_hr is not set"))?;

    Ok(CreateSearchRequirements {
        min_cpus,
        min_ram_gb,
        allowed_gpus,
        max_price_per_hr,
    })
}

pub(crate) fn find_cheapest_offer(
    client: &VastClient,
    req: &CreateSearchRequirements,
    hours: f64,
    machine_override: Option<&str>,
    excluded_offer_ids: &HashSet<u64>,
) -> Result<VastOffer> {
    let duration_seconds = required_runtime_seconds(hours) as f64;
    let min_ram_mb = (req.min_ram_gb as f64) * 1000.0;

    let allowed_gpus = req
        .allowed_gpus
        .iter()
        .map(|name| canonicalize_gpu_name(name).unwrap_or_else(|| name.clone()))
        .collect::<Vec<_>>();

    let mut query = json!({
        "verified": {"eq": true},
        "external": {"eq": false},
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "cpu_cores_effective": {"gte": req.min_cpus as f64},
        "cpu_ram": {"gte": min_ram_mb},
        "duration": {"gte": duration_seconds},
        "gpu_name": {"in": allowed_gpus},
        "direct_port_count": {"gte": 1},
        "order": [["dph_total", "asc"], ["duration", "asc"], ["reliability", "desc"]],
        "type": "on-demand",
        "limit": VAST_DEFAULT_SEARCH_LIMIT,
        "allocated_storage": VAST_DEFAULT_DISK_GB,
    });

    if let Some(machine) = machine_override
        && !machine.trim().is_empty()
    {
        let machine = canonicalize_gpu_name(machine).unwrap_or_else(|| machine.trim().to_owned());
        query["gpu_name"] = json!({"eq": machine});
    }

    let spinner = spinner("Searching vast.ai offers...");
    let mut offers = client.search_offers(&query)?;
    spinner.finish_with_message(format!("Found {} matching offers.", offers.len()));

    offers.retain(|offer| {
        !excluded_offer_ids.contains(&offer.id)
            && offer.hourly_price().is_finite()
            && offer_duration_seconds(offer)
                .map(|duration| duration >= duration_seconds)
                .unwrap_or(false)
    });
    if offers.is_empty() {
        let excluded_count = excluded_offer_ids.len();
        if excluded_count == 0 {
            bail!(
                "No offers match the filters (min_cpus={}, min_ram_gb={}, allowed_gpus={}, min_duration_hours={:.2}).",
                req.min_cpus,
                req.min_ram_gb,
                req.allowed_gpus.join(", "),
                hours
            );
        }
        bail!(
            "No offers remain after excluding {excluded_count} failed offer(s) (min_cpus={}, min_ram_gb={}, allowed_gpus={}, min_duration_hours={:.2}).",
            req.min_cpus,
            req.min_ram_gb,
            req.allowed_gpus.join(", "),
            hours
        );
    }

    offers.sort_by(compare_offer_price_then_duration);
    offers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No offers remained after sorting"))
}

fn offer_duration_seconds(offer: &VastOffer) -> Option<f64> {
    offer
        .duration
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn compare_offer_price_then_duration(a: &VastOffer, b: &VastOffer) -> Ordering {
    let price_cmp = a.hourly_price().total_cmp(&b.hourly_price());
    if price_cmp != Ordering::Equal {
        return price_cmp;
    }

    let a_duration = offer_duration_seconds(a).unwrap_or(f64::INFINITY);
    let b_duration = offer_duration_seconds(b).unwrap_or(f64::INFINITY);
    let duration_cmp = a_duration.total_cmp(&b_duration);
    if duration_cmp != Ordering::Equal {
        return duration_cmp;
    }

    let a_rel = a.reliability.unwrap_or(0.0);
    let b_rel = b.reliability.unwrap_or(0.0);
    b_rel.total_cmp(&a_rel)
}

pub(crate) fn estimate_runtime_cost(
    cloud: Cloud,
    hourly_usd: f64,
    requested_hours: f64,
) -> Result<RuntimeCostEstimate> {
    if !(hourly_usd.is_finite() && hourly_usd > 0.0) {
        bail!("Expected finite positive hourly price, got {hourly_usd}.");
    }
    if !(requested_hours.is_finite() && requested_hours > 0.0) {
        bail!("Expected requested HOURS > 0, got {requested_hours}.");
    }

    let billed_hours = estimated_billed_hours(cloud, requested_hours);
    if !(billed_hours.is_finite() && billed_hours > 0.0) {
        bail!("Expected finite positive billed hours, got {billed_hours}.");
    }
    if billed_hours + 0.000_001 < requested_hours {
        bail!(
            "Billed runtime {:.3}h cannot be lower than requested runtime {:.3}h.",
            billed_hours,
            requested_hours
        );
    }
    let total_usd = hourly_usd * billed_hours;
    Ok(RuntimeCostEstimate {
        requested_hours,
        billed_hours,
        hourly_usd,
        total_usd,
    })
}

fn estimated_billed_hours(cloud: Cloud, requested_hours: f64) -> f64 {
    match cloud {
        Cloud::VastAi => requested_hours,
        Cloud::Gcp | Cloud::Aws => required_runtime_seconds(requested_hours) as f64 / 3600.0,
        Cloud::Local => requested_hours,
    }
}

pub(crate) fn apply_vast_autostop_cost_estimate(
    cost: RuntimeCostEstimate,
) -> Result<RuntimeCostEstimate> {
    let plan = build_vast_autostop_plan(now_unix_secs(), cost.requested_hours)?;
    let total_usd = cost.hourly_usd * plan.runtime_hours;
    Ok(RuntimeCostEstimate {
        requested_hours: cost.requested_hours,
        billed_hours: plan.runtime_hours,
        hourly_usd: cost.hourly_usd,
        total_usd,
    })
}

pub(crate) fn build_vast_autostop_plan(
    start_unix: u64,
    requested_hours: f64,
) -> Result<VastAutoStopPlan> {
    if !(requested_hours.is_finite() && requested_hours > 0.0) {
        bail!("Expected requested HOURS > 0, got {requested_hours}.");
    }

    let min_runtime_secs = required_runtime_seconds(requested_hours);
    let min_stop_unix = start_unix.saturating_add(min_runtime_secs);
    let stop_at_unix = round_up_to_hour_unix(min_stop_unix);
    let runtime_secs = stop_at_unix
        .saturating_sub(start_unix)
        .max(min_runtime_secs)
        .max(1);

    Ok(VastAutoStopPlan {
        stop_at_unix,
        schedule_end_unix: stop_at_unix.saturating_add(60),
        runtime_hours: runtime_secs as f64 / 3600.0,
    })
}

fn round_up_to_hour_unix(unix_ts: u64) -> u64 {
    let rem = unix_ts % 3600;
    if rem == 0 {
        unix_ts
    } else {
        unix_ts.saturating_add(3600 - rem)
    }
}

pub(crate) fn build_accept_prompt(cost: &RuntimeCostEstimate) -> String {
    if (cost.billed_hours - cost.requested_hours).abs() > 0.000_001 {
        format!(
            "Accept and create? ({:.3}h requested, {:.3}h scheduled, est total ${:.4})",
            cost.requested_hours, cost.billed_hours, cost.total_usd
        )
    } else {
        format!(
            "Accept and create? ({:.3}h, est total ${:.4})",
            cost.billed_hours, cost.total_usd
        )
    }
}

fn gpu_relative_to_rtx_pro_6000(gpu_model: &str, gpu_count: u32) -> String {
    let baseline = gpu_fp32_tflops_estimate("RTX PRO 6000 WS");
    if !(baseline.is_finite() && baseline > 0.0) {
        return String::new();
    }

    let total = gpu_fp32_tflops_estimate(gpu_model) * f64::from(gpu_count.max(1));
    if !(total.is_finite() && total > 0.0) {
        return String::new();
    }

    format!(" (x{:.3} RTX Pro 6000)", total / baseline)
}

fn print_two_column_stats(entries: &[(String, String)]) {
    if entries.is_empty() {
        return;
    }

    let mut rows = Vec::new();
    for chunk in entries.chunks(2) {
        let left = format!("{}: {}", chunk[0].0, chunk[0].1);
        let right = chunk
            .get(1)
            .map(|(label, value)| format!("{label}: {value}"))
            .unwrap_or_default();
        rows.push((left, right));
    }

    let left_width = rows
        .iter()
        .map(|(left, _)| left.chars().count())
        .max()
        .unwrap_or(0)
        .min(64);

    for (left, right) in rows {
        if right.is_empty() {
            println!("  {left}");
        } else {
            println!("  {left:<left_width$}  {right}");
        }
    }
}

pub(crate) fn print_offer_summary(
    offer: &VastOffer,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
) {
    let cpu = offer.cpu_cores_effective.unwrap_or(0.0);
    let ram_gb = offer.cpu_ram.unwrap_or(0.0) / 1000.0;
    let gpu = offer.gpu_name();
    let num_gpus = offer.num_gpus.unwrap_or(1);
    let reliability_pct = offer.reliability.unwrap_or(0.0) * 100.0;
    let duration_hours = offer.duration.unwrap_or(0.0) / 3600.0;
    let gpu_relative = gpu_relative_to_rtx_pro_6000(gpu, num_gpus);

    println!();
    println!("Best matching offer:");
    let mut entries = vec![
        ("Offer ID".to_owned(), offer.id.to_string()),
        ("Price".to_owned(), format!("${:.4}/hr", cost.hourly_usd)),
        ("GPU".to_owned(), format!("{gpu} x{num_gpus}{gpu_relative}")),
        ("CPU".to_owned(), format!("{cpu:.1} vCPU")),
        ("RAM".to_owned(), format!("{ram_gb:.1} GB")),
        ("Reliability".to_owned(), format!("{reliability_pct:.2}%")),
        (
            "Available duration".to_owned(),
            format!("{duration_hours:.2}h"),
        ),
        (
            "Requested runtime".to_owned(),
            format!("{:.3}h", cost.requested_hours),
        ),
    ];
    if (cost.billed_hours - cost.requested_hours).abs() > 0.000_001 {
        entries.push((
            "Scheduled runtime".to_owned(),
            format!("{:.3}h", cost.billed_hours),
        ));
    }
    entries.push((
        "Estimated compute cost".to_owned(),
        format!("${:.4}", cost.total_usd),
    ));
    if let Some(location) = offer.geolocation.as_deref() {
        entries.push(("Location".to_owned(), location.to_owned()));
    }
    if let Some(verification) = offer.verification.as_deref() {
        entries.push(("Verification".to_owned(), verification.to_owned()));
    }

    print_two_column_stats(&entries);
    println!();
    println!(
        "  Your filters: min_cpus={} min_ram_gb={} allowed_gpus=[{}] max_price_per_hr=${:.4}/hr required_hours={:.2}",
        req.min_cpus,
        req.min_ram_gb,
        req.allowed_gpus.join(", "),
        req.max_price_per_hr,
        cost.requested_hours
    );
    println!();
}

pub(crate) fn prompt_offer_decision(prompt: &str) -> Result<OfferDecision> {
    require_interactive("Offer acceptance prompt requires interactive stdin.")?;

    let labels = ["yes", "no", "change filter"];
    let mapping = [
        OfferDecision::Accept,
        OfferDecision::Reject,
        OfferDecision::ChangeFilter,
    ];

    let choice = Select::with_theme(prompt_theme())
        .with_prompt(prompt)
        .items(labels)
        .default(0)
        .interact()
        .context("Failed to read selection")?;

    Ok(match mapping[choice] {
        OfferDecision::Accept => OfferDecision::Accept,
        OfferDecision::Reject => OfferDecision::Reject,
        OfferDecision::ChangeFilter => OfferDecision::ChangeFilter,
    })
}

pub(crate) fn prompt_adjust_search_filters(
    req: &mut CreateSearchRequirements,
    gpu_choices: &[String],
) -> Result<()> {
    require_interactive("Filter adjustment requires interactive stdin.")?;

    let options = [
        "Minimum vCPUs",
        "Minimum RAM (GB)",
        "Allowed GPU models",
        "Max price per hour (USD)",
        "Back",
    ];
    let choice = Select::with_theme(prompt_theme())
        .with_prompt("Change a filter")
        .items(options)
        .default(0)
        .interact()
        .context("Failed to read selection")?;

    match options[choice] {
        "Minimum vCPUs" => {
            req.min_cpus = prompt_u32("Minimum vCPUs", Some(req.min_cpus), 1)?;
        }
        "Minimum RAM (GB)" => {
            req.min_ram_gb = prompt_u32("Minimum RAM (GB)", Some(req.min_ram_gb), 1)?;
        }
        "Allowed GPU models" => {
            req.allowed_gpus = prompt_gpu_checklist(gpu_choices, &req.allowed_gpus)?;
        }
        "Max price per hour (USD)" => {
            req.max_price_per_hr = prompt_f64(
                "Max price per hour (USD)",
                Some(req.max_price_per_hr),
                0.0001,
            )?;
        }
        "Back" => {}
        _ => unreachable!(),
    }

    Ok(())
}

pub(crate) fn prompt_create_search_filters(
    req: &mut CreateSearchRequirements,
    gpu_choices: &[String],
) -> Result<()> {
    require_interactive("`ice deploy --custom` requires interactive stdin.")?;
    req.min_cpus = prompt_u32("Minimum vCPUs", Some(req.min_cpus), 1)?;
    req.min_ram_gb = prompt_u32("Minimum RAM (GB)", Some(req.min_ram_gb), 1)?;
    req.allowed_gpus = prompt_gpu_checklist(gpu_choices, &req.allowed_gpus)?;
    req.max_price_per_hr = prompt_f64(
        "Max price per hour (USD)",
        Some(req.max_price_per_hr),
        0.0001,
    )?;
    Ok(())
}

fn prompt_gpu_checklist(options: &[String], current_values: &[String]) -> Result<Vec<String>> {
    require_interactive("Interactive GPU checklist requires stdin terminal.")?;

    if options.is_empty() {
        bail!("GPU option list is empty.");
    }

    let selected_map = current_values
        .iter()
        .filter_map(|value| canonicalize_gpu_name(value))
        .collect::<BTreeSet<_>>();

    let mut selected_flags = options
        .iter()
        .map(|candidate| selected_map.contains(candidate))
        .collect::<Vec<_>>();
    let labels = options
        .iter()
        .map(|model| gpu_selector_label(model))
        .collect::<Vec<_>>();

    let term = Term::stderr();
    let mut cursor_index = 0usize;
    let mut scroll_offset = 0usize;
    let mut rendered_lines = 0usize;

    loop {
        if rendered_lines > 0 {
            term.clear_last_lines(rendered_lines)
                .context("Failed to refresh GPU checklist display")?;
        }

        let term_rows = usize::from(term.size().0);
        let header_rows = 4usize;
        let footer_rows = 1usize;
        let min_page_size = 6usize;
        let page_size = options.len().min(
            term_rows
                .saturating_sub(header_rows + footer_rows)
                .max(min_page_size),
        );
        let max_scroll = options.len().saturating_sub(page_size);

        if cursor_index < scroll_offset {
            scroll_offset = cursor_index;
        } else if cursor_index >= scroll_offset + page_size {
            scroll_offset = cursor_index + 1 - page_size;
        }
        if scroll_offset > max_scroll {
            scroll_offset = max_scroll;
        }

        let page_start = scroll_offset;
        let page_end = (page_start + page_size).min(options.len());
        let page_number = (page_start / page_size) + 1;
        let total_pages = options.len().div_ceil(page_size);

        let selected_count = selected_flags.iter().filter(|flag| **flag).count();
        let mut lines = Vec::with_capacity(page_size + header_rows + footer_rows + 1);
        lines.push(format!(
            "Allowed GPU models (page {page_number}/{total_pages})"
        ));
        lines.push(format!(
            "Selected: {selected_count}/{}  Showing {}-{} of {}",
            options.len(),
            page_start + 1,
            page_end,
            options.len()
        ));
        lines.push(
            "Keys: up/down (j/k), PgUp/PgDn or n/p page, / find, space toggle, a select-below, z unselect-below, enter confirm"
                .to_owned(),
        );
        lines.push("Legend: ✓ selected, × unselected".to_owned());
        for (index, label) in labels.iter().enumerate().skip(page_start).take(page_size) {
            let cursor = if index == cursor_index { ">" } else { " " };
            let marker = if selected_flags[index] { "✓" } else { "×" };
            lines.push(format!("{cursor} {marker} {label}"));
        }
        lines.push("Press Esc to abort.".to_owned());

        for line in &lines {
            term.write_line(line)
                .context("Failed to render GPU checklist")?;
        }
        rendered_lines = lines.len();

        match term
            .read_key()
            .context("Failed to read GPU checklist keypress")?
        {
            Key::ArrowUp | Key::Char('k') | Key::Char('K') => {
                cursor_index = cursor_index.saturating_sub(1);
            }
            Key::ArrowDown | Key::Char('j') | Key::Char('J') => {
                if cursor_index + 1 < options.len() {
                    cursor_index += 1;
                }
            }
            Key::PageUp => {
                cursor_index = cursor_index.saturating_sub(page_size);
            }
            Key::PageDown | Key::Char('n') | Key::Char('N') => {
                if !options.is_empty() {
                    cursor_index = (cursor_index + page_size).min(options.len() - 1);
                }
            }
            Key::Char('p') | Key::Char('P') => {
                cursor_index = cursor_index.saturating_sub(page_size);
            }
            Key::Home => {
                cursor_index = 0;
            }
            Key::End => {
                if !options.is_empty() {
                    cursor_index = options.len() - 1;
                }
            }
            Key::Char('/') => {
                term.clear_last_lines(rendered_lines)
                    .context("Failed to clear GPU checklist display")?;
                rendered_lines = 0;
                let query = Input::<String>::with_theme(prompt_theme())
                    .with_prompt("Find GPU (substring)")
                    .allow_empty(true)
                    .interact_text()
                    .context("Failed to read GPU finder query")?;
                let query = query.trim().to_ascii_lowercase();
                if !query.is_empty()
                    && let Some(index) = options.iter().enumerate().find_map(|(idx, model)| {
                        (model.to_ascii_lowercase().contains(&query)
                            || labels[idx].to_ascii_lowercase().contains(&query))
                        .then_some(idx)
                    })
                {
                    cursor_index = index;
                }
            }
            Key::Char(' ') => {
                selected_flags[cursor_index] = !selected_flags[cursor_index];
            }
            Key::Char('a') | Key::Char('A') => {
                for flag in &mut selected_flags[cursor_index..] {
                    *flag = true;
                }
            }
            Key::Char('z') | Key::Char('Z') => {
                for flag in &mut selected_flags[cursor_index..] {
                    *flag = false;
                }
            }
            Key::Enter => {
                let mut selected = options
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| selected_flags[index].then_some(value.clone()))
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    continue;
                }
                selected.sort();
                selected.dedup();
                term.clear_last_lines(rendered_lines)
                    .context("Failed to clear GPU checklist display")?;
                return Ok(selected);
            }
            Key::Escape => {
                term.clear_last_lines(rendered_lines)
                    .context("Failed to clear GPU checklist display")?;
                bail!("GPU selection aborted.");
            }
            _ => {}
        }
    }
}

pub(crate) fn load_gpu_options(cloud: Cloud, vast_client: Option<&VastClient>) -> Vec<String> {
    let mut all = BTreeSet::new();

    if cloud == Cloud::VastAi {
        for value in KNOWN_VAST_GPU_MODELS {
            all.insert((*value).to_owned());
        }
    }

    for spec in all_machine_specs_for_cloud(cloud) {
        for gpu in spec.gpus {
            all.insert((*gpu).to_owned());
        }
    }

    if cloud == Cloud::VastAi
        && let Some(client) = vast_client
        && let Ok(remote) = client.fetch_gpu_names()
    {
        for gpu in remote {
            if !gpu.trim().is_empty() {
                all.insert(gpu.trim().to_owned());
            }
        }
    }

    let mut options = all.into_iter().collect::<Vec<_>>();
    options.sort_by(|a, b| {
        gpu_quality_score(a)
            .cmp(&gpu_quality_score(b))
            .then_with(|| a.cmp(b))
    });
    options
}

pub(crate) fn gpu_quality_score(model: &str) -> i64 {
    (gpu_fp32_tflops_estimate(model) * 1000.0).round() as i64
}

pub(crate) fn gpu_selector_label(model: &str) -> String {
    if let Some(vram_gb) = gpu_vram_gb(model) {
        let rendered = if (vram_gb.fract()).abs() < 1e-9 {
            format!("{:.0}", vram_gb)
        } else {
            format!("{vram_gb:.1}")
        };
        format!("{model} ({rendered} GB)")
    } else {
        model.to_owned()
    }
}

fn gpu_vram_gb(model: &str) -> Option<f64> {
    let token = normalize_gpu_name_token(model);
    if let Some(value) = known_gpu_vram_gb_lookup().get(&token) {
        return Some(*value);
    }
    gpu_vram_gb_fallback(&token)
}

fn known_gpu_vram_gb_lookup() -> &'static HashMap<String, f64> {
    static LOOKUP: std::sync::LazyLock<HashMap<String, f64>> = std::sync::LazyLock::new(|| {
        let mut map = HashMap::new();
        for (model, gb) in [
            ("A10", 24.0),
            ("A100 PCIE", 40.0),
            ("A100 SXM4", 80.0),
            ("A100X", 80.0),
            ("A40", 48.0),
            ("A800 PCIE", 80.0),
            ("B200", 180.0),
            ("CMP 50HX", 10.0),
            ("GTX 1050", 2.0),
            ("GTX 1050 Ti", 4.0),
            ("GTX 1060", 6.0),
            ("GTX 1070", 8.0),
            ("GTX 1070 Ti", 8.0),
            ("GTX 1080", 8.0),
            ("GTX 1080 Ti", 11.0),
            ("GTX 1650", 4.0),
            ("GTX 1650 S", 4.0),
            ("GTX 1660", 6.0),
            ("GTX 1660 S", 6.0),
            ("GTX 1660 Ti", 6.0),
            ("H100 NVL", 94.0),
            ("H100 PCIE", 80.0),
            ("H100 SXM", 80.0),
            ("H200", 141.0),
            ("H200 NVL", 141.0),
            ("L4", 24.0),
            ("L40", 48.0),
            ("L40S", 48.0),
            ("Q RTX 4000", 8.0),
            ("Q RTX 6000", 24.0),
            ("Q RTX 8000", 48.0),
            ("Quadro P2000", 5.0),
            ("Quadro P4000", 8.0),
            ("Radeon VII", 16.0),
            ("RTX 2000Ada", 16.0),
            ("RTX 2060", 6.0),
            ("RTX 2060S", 8.0),
            ("RTX 2070", 8.0),
            ("RTX 2070S", 8.0),
            ("RTX 2080", 8.0),
            ("RTX 2080 Ti", 11.0),
            ("RTX 3050", 8.0),
            ("RTX 3060", 12.0),
            ("RTX 3060 laptop", 6.0),
            ("RTX 3060 Ti", 8.0),
            ("RTX 3070", 8.0),
            ("RTX 3070 laptop", 8.0),
            ("RTX 3070 Ti", 8.0),
            ("RTX 3080", 10.0),
            ("RTX 3080 Ti", 12.0),
            ("RTX 3090", 24.0),
            ("RTX 3090 Ti", 24.0),
            ("RTX 4000Ada", 20.0),
            ("RTX 4060", 8.0),
            ("RTX 4060 Ti", 16.0),
            ("RTX 4070", 12.0),
            ("RTX 4070 laptop", 8.0),
            ("RTX 4070S", 12.0),
            ("RTX 4070S Ti", 16.0),
            ("RTX 4070 Ti", 12.0),
            ("RTX 4080", 16.0),
            ("RTX 4080S", 16.0),
            ("RTX 4090", 24.0),
            ("RTX 4090D", 24.0),
            ("RTX 4500Ada", 24.0),
            ("RTX 5000Ada", 32.0),
            ("RTX 5060", 8.0),
            ("RTX 5060 Ti", 16.0),
            ("RTX 5070", 12.0),
            ("RTX 5070 Ti", 16.0),
            ("RTX 5080", 16.0),
            ("RTX 5090", 32.0),
            ("RTX 5880Ada", 48.0),
            ("RTX 6000Ada", 48.0),
            ("RTX A2000", 12.0),
            ("RTX A4000", 16.0),
            ("RTX A4500", 20.0),
            ("RTX A5000", 24.0),
            ("RTX A6000", 48.0),
            ("RTX PRO 4000", 24.0),
            ("RTX PRO 4500", 32.0),
            ("RTX PRO 5000", 48.0),
            ("RTX PRO 6000 S", 96.0),
            ("RTX PRO 6000 WS", 96.0),
            ("RX 6950 XT", 16.0),
            ("Tesla P100", 16.0),
            ("Tesla P4", 8.0),
            ("Tesla P40", 24.0),
            ("Tesla T4", 16.0),
            ("Tesla V100", 16.0),
            ("Titan RTX", 24.0),
            ("Titan V", 12.0),
            ("Titan Xp", 12.0),
        ] {
            map.insert(normalize_gpu_name_token(model), gb);
        }
        map
    });
    &LOOKUP
}

fn gpu_vram_gb_fallback(token: &str) -> Option<f64> {
    if token.contains("b200") {
        return Some(180.0);
    }
    if token.contains("h200") {
        return Some(141.0);
    }
    if token.contains("h100") {
        return Some(80.0);
    }
    if token.contains("a100") {
        return Some(40.0);
    }
    if token.contains("a800") {
        return Some(80.0);
    }
    if token.contains("a40") || token.contains("l40") || token.contains("6000") {
        return Some(48.0);
    }
    if token == "l4" || token.contains("l4") {
        return Some(24.0);
    }
    if token.contains("teslat4") {
        return Some(16.0);
    }
    if token.contains("teslav100") {
        return Some(16.0);
    }

    None
}

fn gpu_fp32_tflops_estimate(model: &str) -> f64 {
    let token = normalize_gpu_name_token(model);
    if let Some(value) = known_gpu_fp32_tflops_lookup().get(&token) {
        return *value;
    }
    gpu_fp32_tflops_fallback(&token)
}

fn known_gpu_fp32_tflops_lookup() -> &'static HashMap<String, f64> {
    static LOOKUP: std::sync::LazyLock<HashMap<String, f64>> = std::sync::LazyLock::new(|| {
        let mut map = HashMap::new();
        for (model, tflops) in [
            ("A10", 31.2),
            ("A100 PCIE", 19.5),
            ("A100 SXM4", 19.5),
            ("A100X", 19.5),
            ("A40", 37.4),
            ("A800 PCIE", 19.5),
            ("B200", 75.0),
            ("CMP 50HX", 10.0),
            ("GTX 1050", 1.8),
            ("GTX 1050 Ti", 2.1),
            ("GTX 1060", 4.4),
            ("GTX 1070", 6.5),
            ("GTX 1070 Ti", 8.2),
            ("GTX 1080", 8.9),
            ("GTX 1080 Ti", 11.3),
            ("GTX 1650", 3.0),
            ("GTX 1650 S", 4.4),
            ("GTX 1660", 5.0),
            ("GTX 1660 S", 5.0),
            ("GTX 1660 Ti", 5.4),
            ("H100 NVL", 60.0),
            ("H100 PCIE", 51.0),
            ("H100 SXM", 67.0),
            ("H200", 67.0),
            ("H200 NVL", 60.0),
            ("L4", 30.3),
            ("L40", 90.5),
            ("L40S", 91.6),
            ("Q RTX 4000", 7.1),
            ("Q RTX 6000", 16.3),
            ("Q RTX 8000", 16.3),
            ("Quadro P2000", 3.0),
            ("Quadro P4000", 5.3),
            ("Radeon VII", 13.4),
            ("RTX 2000Ada", 12.0),
            ("RTX 2060", 6.5),
            ("RTX 2060S", 7.2),
            ("RTX 2070", 7.5),
            ("RTX 2070S", 9.1),
            ("RTX 2080", 10.1),
            ("RTX 2080 Ti", 13.4),
            ("RTX 3050", 9.1),
            ("RTX 3060", 12.7),
            ("RTX 3060 laptop", 13.0),
            ("RTX 3060 Ti", 16.2),
            ("RTX 3070", 20.3),
            ("RTX 3070 laptop", 20.3),
            ("RTX 3070 Ti", 21.8),
            ("RTX 3080", 29.8),
            ("RTX 3080 Ti", 34.1),
            ("RTX 3090", 35.6),
            ("RTX 3090 Ti", 40.0),
            ("RTX 4000Ada", 26.7),
            ("RTX 4060", 15.1),
            ("RTX 4060 Ti", 22.1),
            ("RTX 4070", 29.1),
            ("RTX 4070 laptop", 28.0),
            ("RTX 4070S", 35.5),
            ("RTX 4070S Ti", 44.0),
            ("RTX 4070 Ti", 40.1),
            ("RTX 4080", 48.7),
            ("RTX 4080S", 52.2),
            ("RTX 4090", 82.6),
            ("RTX 4090D", 73.0),
            ("RTX 4500Ada", 39.6),
            ("RTX 5000Ada", 65.3),
            ("RTX 5060", 19.0),
            ("RTX 5060 Ti", 24.0),
            ("RTX 5070", 30.9),
            ("RTX 5070 Ti", 43.9),
            ("RTX 5080", 56.3),
            ("RTX 5090", 104.8),
            ("RTX 5880Ada", 69.0),
            ("RTX 6000Ada", 91.1),
            ("RTX A2000", 8.0),
            ("RTX A4000", 19.2),
            ("RTX A4500", 23.7),
            ("RTX A5000", 27.8),
            ("RTX A6000", 38.7),
            ("RTX PRO 4000", 50.0),
            ("RTX PRO 4500", 70.0),
            ("RTX PRO 5000", 95.0),
            ("RTX PRO 6000 S", 125.0),
            ("RTX PRO 6000 WS", 125.0),
            ("RX 6950 XT", 23.6),
            ("Tesla P100", 10.6),
            ("Tesla P4", 5.5),
            ("Tesla P40", 12.0),
            ("Tesla T4", 8.1),
            ("Tesla V100", 15.7),
            ("Titan RTX", 16.3),
            ("Titan V", 13.8),
            ("Titan Xp", 12.1),
        ] {
            map.insert(normalize_gpu_name_token(model), tflops);
        }
        map
    });
    &LOOKUP
}

fn gpu_fp32_tflops_fallback(token: &str) -> f64 {
    if token.contains("b200") {
        return 75.0;
    }
    if token.contains("h200") {
        return 67.0;
    }
    if token.contains("h100") {
        return 60.0;
    }
    if token.contains("a100") || token.contains("a800") {
        return 19.5;
    }
    if token.contains("l40s") {
        return 91.6;
    }
    if token.contains("l40") {
        return 90.5;
    }
    if token == "l4" || token.contains("l4") {
        return 30.3;
    }
    if token.contains("a40") {
        return 37.4;
    }
    if token.contains("a10") {
        return 31.2;
    }
    if token.contains("teslat4") {
        return 8.1;
    }
    if token.contains("teslav100") {
        return 15.7;
    }

    if let Some(num) = first_number_in(token) {
        if token.starts_with("rtxpro") {
            return 20.0 + (num as f64 / 60.0);
        }
        if token.starts_with("rtxa") || token.contains("ada") {
            return 10.0 + (num as f64 / 100.0);
        }
        if token.starts_with("rtx") {
            return match num {
                5000..=9999 => 14.0 + ((num - 5000) as f64 / 9.0),
                4000..=4999 => 12.0 + ((num - 4000) as f64 / 11.0),
                3000..=3999 => 8.0 + ((num - 3000) as f64 / 11.0),
                2000..=2999 => 5.0 + ((num - 2000) as f64 / 12.0),
                _ => 5.0,
            };
        }
        if token.starts_with("gtx") {
            return match num {
                1600..=1999 => 3.0 + ((num - 1600) as f64 / 80.0),
                1000..=1599 => 1.6 + ((num - 1000) as f64 / 90.0),
                _ => 2.0,
            };
        }
        if token.starts_with("rx") {
            return 10.0 + (num as f64 / 350.0);
        }
        if token.starts_with("quadro") {
            return 2.0 + (num as f64 / 1000.0);
        }
    }

    5.0
}

fn first_number_in(token: &str) -> Option<i64> {
    let mut digits = String::new();
    let mut seen = false;
    for ch in token.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            seen = true;
        } else if seen {
            break;
        }
    }
    if digits.is_empty() {
        None
    } else {
        digits.parse::<i64>().ok()
    }
}

pub(crate) fn canonicalize_gpu_name(input: &str) -> Option<String> {
    let lookup = known_gpu_lookup();
    let normalized = normalize_gpu_name_token(input);
    lookup.get(&normalized).cloned()
}

fn known_gpu_lookup() -> HashMap<String, String> {
    let mut map = HashMap::new();
    for model in KNOWN_VAST_GPU_MODELS {
        map.insert(normalize_gpu_name_token(model), (*model).to_owned());
    }
    for spec in GCP_MACHINE_SPECS {
        for gpu in spec.gpus {
            map.insert(normalize_gpu_name_token(gpu), (*gpu).to_owned());
        }
    }
    for spec in AWS_MACHINE_SPECS {
        for gpu in spec.gpus {
            map.insert(normalize_gpu_name_token(gpu), (*gpu).to_owned());
        }
    }
    map
}

pub(crate) fn normalize_gpu_name_token(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| {
            ch.to_ascii_lowercase()
                .to_string()
                .chars()
                .collect::<Vec<_>>()
        })
        .collect::<String>()
}

fn all_machine_specs_for_cloud(cloud: Cloud) -> &'static [MachineTypeSpec] {
    match cloud {
        Cloud::VastAi => &[],
        Cloud::Gcp => GCP_MACHINE_SPECS,
        Cloud::Aws => AWS_MACHINE_SPECS,
        Cloud::Local => &[],
    }
}

pub(crate) fn estimated_machine_hourly_price(cloud: Cloud, machine: &str) -> Option<f64> {
    all_machine_specs_for_cloud(cloud)
        .iter()
        .find(|spec| spec.machine.eq_ignore_ascii_case(machine))
        .map(|spec| spec.hourly_usd)
}

pub(crate) fn find_cheapest_cloud_machine(
    cloud: Cloud,
    config: &IceConfig,
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
) -> Result<CloudMachineCandidate> {
    let specs = all_machine_specs_for_cloud(cloud);
    if specs.is_empty() {
        bail!("No machine catalog for cloud `{cloud}`");
    }

    let override_name = machine_override
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(name) = override_name
        && !specs
            .iter()
            .any(|spec| spec.machine.eq_ignore_ascii_case(name))
    {
        bail!("Unknown machine type `{name}` for cloud `{cloud}`.");
    }

    let allowed_gpu_set: HashSet<String> = req
        .allowed_gpus
        .iter()
        .filter_map(|gpu| canonicalize_gpu_name(gpu))
        .map(|gpu| normalize_gpu_name_token(&gpu))
        .collect();

    let preferred_region = preferred_region_for_cloud(config, cloud);

    let mut candidates = Vec::new();
    for spec in specs {
        if spec.cloud != cloud {
            continue;
        }
        if let Some(name) = override_name
            && !spec.machine.eq_ignore_ascii_case(name)
        {
            continue;
        }
        if spec.vcpus < req.min_cpus || spec.ram_gb < req.min_ram_gb {
            continue;
        }

        if !allowed_gpu_set.is_empty() {
            let gpu_match = spec.gpus.iter().any(|gpu| {
                let canonical = canonicalize_gpu_name(gpu).unwrap_or_else(|| (*gpu).to_owned());
                allowed_gpu_set.contains(&normalize_gpu_name_token(&canonical))
            });
            if !gpu_match {
                continue;
            }
        }

        let region = select_region(spec.regions, preferred_region.as_deref())
            .ok_or_else(|| anyhow!("Machine `{}` has no regions in catalog", spec.machine))?;
        let zone = if cloud == Cloud::Gcp {
            Some(select_zone_for_region(config, &region))
        } else {
            None
        };
        candidates.push(CloudMachineCandidate {
            machine: spec.machine.to_owned(),
            vcpus: spec.vcpus,
            ram_gb: spec.ram_gb,
            gpus: spec.gpus.iter().map(|value| (*value).to_owned()).collect(),
            hourly_usd: spec.hourly_usd,
            region,
            zone,
        });
    }

    if candidates.is_empty() {
        bail!(
            "No {} machine type matches filters (min_cpus={}, min_ram_gb={}, allowed_gpus=[{}]){}.",
            cloud,
            req.min_cpus,
            req.min_ram_gb,
            req.allowed_gpus.join(", "),
            override_name
                .map(|name| format!(", machine={name}"))
                .unwrap_or_default()
        );
    }

    candidates.sort_by(|a, b| {
        let price = a.hourly_usd.total_cmp(&b.hourly_usd);
        if price != Ordering::Equal {
            return price;
        }
        let region_pref = preferred_region.as_deref().unwrap_or("");
        let a_pref = a.region.eq_ignore_ascii_case(region_pref);
        let b_pref = b.region.eq_ignore_ascii_case(region_pref);
        b_pref.cmp(&a_pref)
    });

    candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No candidate machine after sort"))
}

fn select_region(regions: &[&str], preferred: Option<&str>) -> Option<String> {
    if regions.is_empty() {
        return None;
    }

    if let Some(preferred) = preferred {
        for region in regions {
            if region.eq_ignore_ascii_case(preferred) {
                return Some((*region).to_owned());
            }
        }
    }

    Some(regions[0].to_owned())
}

fn preferred_region_for_cloud(config: &IceConfig, cloud: Cloud) -> Option<String> {
    match cloud {
        Cloud::VastAi => None,
        Cloud::Gcp => config.default.gcp.region.clone().or_else(|| {
            config
                .default
                .gcp
                .zone
                .as_deref()
                .map(region_from_zone_name)
        }),
        Cloud::Aws => config.default.aws.region.clone(),
        Cloud::Local => None,
    }
}

fn select_zone_for_region(config: &IceConfig, region: &str) -> String {
    if let Some(zone) = config.default.gcp.zone.as_deref() {
        let zone_name = short_gcp_zone(zone);
        if region_from_zone_name(&zone_name).eq_ignore_ascii_case(region) {
            return zone_name;
        }
    }
    format!("{region}-a")
}

pub(crate) fn short_gcp_zone(zone: &str) -> String {
    zone.rsplit('/').next().unwrap_or(zone).to_owned()
}

fn region_from_zone_name(zone: &str) -> String {
    let zone = short_gcp_zone(zone);
    let mut parts = zone.split('-').collect::<Vec<_>>();
    if parts.len() >= 3 {
        parts.pop();
        parts.join("-")
    } else {
        zone
    }
}

pub(crate) fn print_machine_candidate_summary(
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
) {
    let gpu = if candidate.gpus.is_empty() {
        "-".to_owned()
    } else {
        candidate.gpus.join(",")
    };
    println!();
    println!("Cheapest matching machine:");
    println!("  Cloud: {cloud}");
    println!("  Machine: {}", candidate.machine);
    println!("  Price: ${:.4}/hr", cost.hourly_usd);
    println!("  Region: {}", candidate.region);
    if let Some(zone) = candidate.zone.as_deref() {
        println!("  Zone: {zone}");
    }
    println!("  CPU: {} vCPU", candidate.vcpus);
    println!("  RAM: {} GB", candidate.ram_gb);
    println!("  GPU: {gpu}");
    println!("  Requested runtime: {:.3}h", cost.requested_hours);
    if (cost.billed_hours - cost.requested_hours).abs() > 0.000_001 {
        println!("  Scheduled runtime: {:.3}h", cost.billed_hours);
    }
    println!("  Estimated compute cost: ${:.4}", cost.total_usd);
    println!(
        "  Your filters: min_cpus={} min_ram_gb={} allowed_gpus=[{}] max_price_per_hr=${:.4}/hr required_hours={:.2}",
        req.min_cpus,
        req.min_ram_gb,
        req.allowed_gpus.join(", "),
        req.max_price_per_hr,
        cost.requested_hours
    );
    println!();
}
