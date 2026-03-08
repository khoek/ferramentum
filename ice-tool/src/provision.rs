use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use dialoguer::console::{Key, Term};
use dialoguer::{Input, Select};
use serde_json::json;

use crate::config_store::save_config;
use crate::gpu::{
    canonicalize_gpu_name, canonicalize_gpu_name_for_cloud, gpu_fp32_tflops, gpu_quality_score,
    gpu_reference_model, gpu_selector_label, provider_gpu_options,
};
use crate::model::{
    Cloud, CloudMachineCandidate, CreateSearchRequirements, IceConfig, OfferDecision,
    RuntimeCostEstimate, VastAutoStopPlan,
};
use crate::providers::{aws, gcp, vast::VastClient, vast::VastOffer};
use crate::support::{
    now_unix_secs, prompt_f64, prompt_theme, prompt_u32, require_interactive,
    required_runtime_seconds, spinner,
};
use crate::ui::print_notice;

const VAST_DEFAULT_DISK_GB: f64 = 32.0;
const VAST_DEFAULT_SEARCH_LIMIT: u64 = 200;

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
    &mut Option<f64>,
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
    &Option<f64>,
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
            let value = prompt_f64(&format!("Minimum RAM (GB) ({cloud})"), Some(32.0), 0.001)?;
            *min_ram_gb = Some(value);
            changed = true;
        }

        if cloud == Cloud::VastAi
            && allowed_gpus
                .as_ref()
                .map(|items| items.is_empty())
                .unwrap_or(true)
        {
            let selected = prompt_gpu_checklist(gpu_options, &[], false)?;
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
        print_notice(&format!(
            "Updated {key_prefix} search defaults in {}",
            path.display()
        ));
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
    let allowed_gpus = match cloud {
        Cloud::VastAi => {
            let values = allowed_gpus_ref
                .clone()
                .ok_or_else(|| anyhow!("{key_prefix}.allowed_gpus is not set"))?;
            if values.is_empty() {
                bail!("{key_prefix}.allowed_gpus cannot be empty");
            }
            values
        }
        Cloud::Gcp | Cloud::Aws => allowed_gpus_ref.clone().unwrap_or_default(),
        Cloud::Local => Vec::new(),
    };

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
    let min_ram_mb = req.min_ram_gb * 1000.0;

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

fn gpu_relative_to_reference_model(gpu_model: &str, gpu_count: u32) -> String {
    let Some(reference_model) = gpu_reference_model() else {
        return String::new();
    };
    let Some(baseline) = gpu_fp32_tflops(reference_model) else {
        return String::new();
    };
    if !(baseline.is_finite() && baseline > 0.0) {
        return String::new();
    }

    let Some(total_per_gpu) = gpu_fp32_tflops(gpu_model) else {
        return String::new();
    };
    let total = total_per_gpu * f64::from(gpu_count.max(1));
    if !(total.is_finite() && total > 0.0) {
        return String::new();
    }

    format!(" (x{:.3} {})", total / baseline, reference_model)
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
    let gpu_relative = gpu_relative_to_reference_model(gpu, num_gpus);

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
    cloud: Cloud,
    req: &mut CreateSearchRequirements,
    gpu_choices: &[String],
) -> Result<()> {
    prompt_create_search_filters(cloud, req, gpu_choices)
}

pub(crate) fn prompt_create_search_filters(
    cloud: Cloud,
    req: &mut CreateSearchRequirements,
    gpu_choices: &[String],
) -> Result<()> {
    require_interactive("`ice create --custom` requires interactive stdin.")?;
    req.min_cpus = prompt_u32("Minimum vCPUs", Some(req.min_cpus), 1)?;
    req.min_ram_gb = prompt_f64("Minimum RAM (GB)", Some(req.min_ram_gb), 0.001)?;
    req.allowed_gpus =
        prompt_gpu_checklist(gpu_choices, &req.allowed_gpus, cloud != Cloud::VastAi)?;
    req.max_price_per_hr = prompt_f64(
        "Max price per hour (USD)",
        Some(req.max_price_per_hr),
        0.0001,
    )?;
    Ok(())
}

fn prompt_gpu_checklist(
    options: &[String],
    current_values: &[String],
    allow_empty: bool,
) -> Result<Vec<String>> {
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
                if selected.is_empty() && !allow_empty {
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
    let mut all = provider_gpu_options(cloud)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if cloud == Cloud::VastAi
        && let Some(client) = vast_client
        && let Ok(remote) = client.fetch_gpu_names()
    {
        for gpu in remote {
            let raw = gpu.trim();
            if raw.is_empty() {
                continue;
            }
            all.insert(
                canonicalize_gpu_name_for_cloud(cloud, raw).unwrap_or_else(|| raw.to_owned()),
            );
        }
    }

    let mut options = all.into_iter().collect::<Vec<_>>();
    options.sort_by(|a, b| {
        gpu_fp32_tflops(a)
            .is_none()
            .cmp(&gpu_fp32_tflops(b).is_none())
            .then_with(|| gpu_quality_score(a).cmp(&gpu_quality_score(b)))
            .then_with(|| a.cmp(b))
    });
    options
}

pub(crate) fn estimated_machine_hourly_price(cloud: Cloud, machine: &str) -> Option<f64> {
    match cloud {
        Cloud::Gcp => gcp::cached_machine_hourly_price(machine),
        Cloud::Aws => aws::cached_machine_hourly_price(machine),
        Cloud::VastAi | Cloud::Local => None,
    }
}

pub(crate) fn find_cheapest_cloud_machine(
    cloud: Cloud,
    config: &IceConfig,
    req: &CreateSearchRequirements,
    machine_override: Option<&str>,
) -> Result<CloudMachineCandidate> {
    match cloud {
        Cloud::Gcp => gcp::find_cheapest_machine_candidate(config, req, machine_override),
        Cloud::Aws => aws::find_cheapest_machine_candidate(config, req, machine_override),
        Cloud::VastAi | Cloud::Local => bail!("No machine catalog for cloud `{cloud}`"),
    }
}

pub(crate) fn short_gcp_zone(zone: &str) -> String {
    zone.rsplit('/').next().unwrap_or(zone).to_owned()
}

pub(crate) fn machine_candidate_summary_lines(
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
    price_note: Option<&str>,
) -> Vec<String> {
    let gpu = if candidate.gpus.is_empty() {
        "-".to_owned()
    } else {
        candidate.gpus.join(",")
    };
    let mut lines = vec![
        "Cheapest matching machine:".to_owned(),
        format!("  Cloud: {cloud}"),
        format!("  Machine: {}", candidate.machine),
        format!(
            "  Price: ${:.4}/hr{}",
            cost.hourly_usd,
            price_note
                .map(|note| format!(" {note}"))
                .unwrap_or_default()
        ),
        format!("  Region: {}", candidate.region),
    ];
    if let Some(zone) = candidate.zone.as_deref() {
        lines.push(format!("  Zone: {zone}"));
    }
    lines.push(format!("  CPU: {} vCPU", candidate.vcpus));
    lines.push(format!("  RAM: {} GB", format_ram_mb_gb(candidate.ram_mb)));
    lines.push(format!("  GPU: {gpu}"));
    lines.push(format!("  Requested runtime: {:.3}h", cost.requested_hours));
    if (cost.billed_hours - cost.requested_hours).abs() > 0.000_001 {
        lines.push(format!("  Scheduled runtime: {:.3}h", cost.billed_hours));
    }
    lines.push(format!("  Estimated compute cost: ${:.4}", cost.total_usd));
    lines.push(format!(
        "  Your filters: min_cpus={} min_ram_gb={} allowed_gpus=[{}] max_price_per_hr=${:.4}/hr required_hours={:.2}",
        req.min_cpus,
        req.min_ram_gb,
        req.allowed_gpus.join(", "),
        req.max_price_per_hr,
        cost.requested_hours
    ));
    lines
}

fn format_ram_mb_gb(value: u32) -> String {
    let value = f64::from(value) / 1000.0;
    if (value.fract()).abs() < 1e-9 {
        format!("{value:.0}")
    } else {
        format!("{value:.2}")
    }
}
