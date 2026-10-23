use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::client::Client;
use crate::model::{CreateSearchRequirements, VerdaDefaults};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Machine {
    pub instance_type: String,
    pub model: String,
    pub name: String,
    pub manufacturer: String,
    pub currency: String,
    pub price_per_hour: Value,
    pub cpu: Cpu,
    pub gpu: Gpu,
    pub gpu_memory: Memory,
    pub memory: Memory,
    pub supported_os: Vec<String>,
    #[serde(default)]
    pub description: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Cpu {
    pub number_of_cores: u32,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Gpu {
    pub number_of_gpus: u32,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Memory {
    pub size_in_gigabytes: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Image {
    pub id: String,
    pub image_type: String,
    pub name: String,
    pub category: String,
    pub is_cluster: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Availability {
    pub location_code: String,
    pub availabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct VolumeType {
    #[serde(rename = "type")]
    pub kind: String,
    pub price: VolumePrice,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct VolumePrice {
    pub price_per_month_per_gb: Value,
    pub currency: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Catalog {
    pub machines: Vec<Machine>,
    pub images: Vec<Image>,
    pub availability: Vec<Availability>,
    pub volume_types: Vec<VolumeType>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Offer {
    pub machine: Machine,
    pub gpu_memory_per_gpu_gb: f64,
    pub location: String,
    pub image: Image,
    pub allocated_disk_gb: u32,
    pub compute_hourly_usd: f64,
    pub storage_hourly_usd: f64,
    pub hourly_usd: f64,
    pub cost_scope: &'static str,
}

fn price(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|p| p.is_finite() && *p > 0.0)
}

fn confidential(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("confidential") || lower.split(['-', '.', '_', ' ']).any(|part| part == "cc")
}

pub(super) fn ordinary_image(image: &Image) -> bool {
    !image.is_cluster
        && [&image.category, &image.image_type, &image.name]
            .iter()
            .all(|value| {
                let value = value.to_ascii_lowercase();
                !confidential(&value) && !value.contains("cluster") && !value.contains("kubernetes")
            })
}

pub(super) fn required_image(defaults: &VerdaDefaults) -> Result<&str> {
    defaults.image.as_deref().filter(|image| !image.trim().is_empty()).ok_or_else(|| crate::automation::error(
        "image_required", "Choose a provider OS image with --image or default.verda.image. Ice does not select or install a workload environment.",
        json!({"flag":"--image", "config_key":"default.verda.image", "discovery_command":"ice catalog --cloud verda --json"})))
}

impl Catalog {
    pub(super) fn load(client: &Client) -> Result<Self> {
        // Independent catalog reads share authentication and run concurrently.
        // Reuse this snapshot through confirmation; refresh it once at purchase.
        std::thread::scope(|scope| {
            let machines = scope.spawn(|| client.list("/instance-types?currency=usd"));
            let images = scope.spawn(|| client.list("/images"));
            let availability = scope.spawn(|| client.list("/instance-availability?is_spot=false"));
            let volumes = scope.spawn(|| client.list("/volume-types"));
            Ok(Self {
                machines: machines
                    .join()
                    .map_err(|_| anyhow::anyhow!("Verda machine discovery worker failed"))??,
                images: images
                    .join()
                    .map_err(|_| anyhow::anyhow!("Verda image discovery worker failed"))??,
                availability: availability
                    .join()
                    .map_err(|_| anyhow::anyhow!("Verda availability worker failed"))??,
                volume_types: volumes
                    .join()
                    .map_err(|_| anyhow::anyhow!("Verda storage discovery worker failed"))??,
            })
        })
    }

    pub(super) fn select(
        &self,
        requirements: &CreateSearchRequirements,
        defaults: &VerdaDefaults,
        pinned_machine: Option<&str>,
    ) -> Result<Offer> {
        let reference = required_image(defaults)?;
        let matching: Vec<_> = self
            .images
            .iter()
            .filter(|image| image.id == reference || image.image_type == reference)
            .collect();
        if matching.len() != 1 {
            return Err(crate::automation::error(
                "image_not_found",
                "The image reference must identify exactly one advertised provider image. Use its UUID from ice catalog.",
                json!({"image_reference":reference,"matches":matching.len(),"discovery_command":"ice catalog --cloud verda --json"}),
            ));
        }
        let image = matching[0];
        if !ordinary_image(image) {
            return Err(crate::automation::error(
                "unsupported_image",
                "This adapter supports ordinary VM images; cluster and confidential-computing images are unsupported.",
                json!({"image_id":image.id}),
            ));
        }
        let disk = requirements.disk_gb.unwrap_or(100);
        let nvme = self
            .volume_types
            .iter()
            .find(|v| v.kind == "NVMe" && v.price.currency.eq_ignore_ascii_case("usd"))
            .context(
                "Verda has no USD NVMe storage quote; cannot enforce the full price ceiling",
            )?;
        let monthly = price(&nvme.price.price_per_month_per_gb)
            .context("Missing or invalid Verda NVMe price")?;
        // Same conversion and rounding as Verda CLI's VolumeHourlyPrice.
        let storage_hourly_usd = (monthly * f64::from(disk) / 730.0 * 10_000.0).ceil() / 10_000.0;
        let mut offers = Vec::new();
        let mut resource_rejections = 0_u32;
        let mut image_rejections = 0_u32;
        let mut availability_rejections = 0_u32;
        for machine in &self.machines {
            let Some(compute_hourly_usd) = price(&machine.price_per_hour) else {
                resource_rejections += 1;
                continue;
            };
            let gpu_count = machine.gpu.number_of_gpus;
            // Verda reports aggregate VRAM (e.g. 96 GB for two 48 GB Ada GPUs).
            let per_gpu_memory = machine.gpu_memory.size_in_gigabytes / f64::from(gpu_count);
            let hourly_usd = compute_hourly_usd + storage_hourly_usd;
            if !machine.manufacturer.eq_ignore_ascii_case("nvidia")
                || gpu_count == 0
                || !per_gpu_memory.is_finite()
                || per_gpu_memory <= 0.0
                || !machine.currency.eq_ignore_ascii_case("usd")
                || !machine.instance_type.ends_with('V')
                || confidential(&machine.instance_type)
                || confidential(&machine.description)
                || confidential(&machine.name)
                || pinned_machine.is_some_and(|name| name != machine.instance_type)
                || machine.cpu.number_of_cores < requirements.min_cpus
                || !machine.memory.size_in_gigabytes.is_finite()
                || machine.memory.size_in_gigabytes < requirements.min_ram_gb
                || requirements
                    .gpu_count
                    .is_some_and(|count| count != gpu_count)
                || requirements
                    .min_gpu_memory_gb
                    .is_some_and(|size| per_gpu_memory < size)
                || (!requirements.allowed_gpus.is_empty()
                    && !requirements.allowed_gpus.iter().any(|gpu| {
                        gpu.eq_ignore_ascii_case(&machine.model)
                            || gpu.eq_ignore_ascii_case(&machine.name)
                    }))
                || !hourly_usd.is_finite()
                || hourly_usd > requirements.max_price_per_hr
            {
                resource_rejections += 1;
                continue;
            }
            if !machine
                .supported_os
                .iter()
                .any(|os| os == &image.image_type || os == &image.id)
            {
                image_rejections += 1;
                continue;
            }
            let mut available = false;
            for location in &self.availability {
                if defaults
                    .location
                    .as_ref()
                    .is_some_and(|wanted| wanted != &location.location_code)
                    || !location.availabilities.contains(&machine.instance_type)
                {
                    continue;
                }
                offers.push(Offer {
                    machine: machine.clone(),
                    gpu_memory_per_gpu_gb: per_gpu_memory,
                    image: image.clone(),
                    location: location.location_code.clone(),
                    allocated_disk_gb: disk,
                    compute_hourly_usd,
                    storage_hourly_usd,
                    hourly_usd,
                    cost_scope: "compute_and_os_storage",
                });
                available = true;
            }
            if !available {
                availability_rejections += 1;
            }
        }
        offers.sort_by(|a, b| {
            a.hourly_usd
                .total_cmp(&b.hourly_usd)
                .then(a.machine.instance_type.cmp(&b.machine.instance_type))
                .then(a.location.cmp(&b.location))
        });
        offers.into_iter().next().ok_or_else(|| crate::automation::error("no_matching_offers",
            "No available on-demand ordinary NVIDIA GPU VM matches the resource filters, selected image, location and compute-plus-OS-storage price ceiling. Spot capacity is not searched.",
            json!({"rental_type":"on_demand", "max_price_per_hr": requirements.max_price_per_hr, "allocated_disk_gb": disk, "cost_scope":"compute_and_os_storage",
                "rejected_machines":{"resources_or_price":resource_rejections,"compatible_image":image_rejections,"availability_or_location":availability_rejections}})))
    }
}
