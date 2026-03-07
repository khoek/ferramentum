use std::path::PathBuf;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
pub(crate) enum Cloud {
    #[value(name = "vast.ai")]
    #[serde(rename = "vast.ai")]
    VastAi,

    #[value(name = "gcp")]
    #[serde(rename = "gcp")]
    Gcp,

    #[value(name = "aws")]
    #[serde(rename = "aws")]
    Aws,

    #[value(name = "local")]
    #[serde(rename = "local")]
    Local,
}

impl std::fmt::Display for Cloud {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VastAi => write!(f, "vast.ai"),
            Self::Gcp => write!(f, "gcp"),
            Self::Aws => write!(f, "aws"),
            Self::Local => write!(f, "local"),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IceConfig {
    #[serde(default)]
    pub(crate) default: DefaultConfig,
    #[serde(default)]
    pub(crate) auth: AuthConfig,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DefaultConfig {
    pub(crate) cloud: Option<Cloud>,
    pub(crate) runtime_hours: Option<f64>,
    #[serde(default)]
    pub(crate) vast_ai: VastDefaults,
    #[serde(default)]
    pub(crate) gcp: GcpDefaults,
    #[serde(default)]
    pub(crate) aws: AwsDefaults,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VastDefaults {
    pub(crate) min_cpus: Option<u32>,
    pub(crate) min_ram_gb: Option<u32>,
    pub(crate) allowed_gpus: Option<Vec<String>>,
    pub(crate) max_price_per_hr: Option<f64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GcpDefaults {
    pub(crate) min_cpus: Option<u32>,
    pub(crate) min_ram_gb: Option<u32>,
    pub(crate) allowed_gpus: Option<Vec<String>>,
    pub(crate) max_price_per_hr: Option<f64>,
    pub(crate) region: Option<String>,
    pub(crate) zone: Option<String>,
    pub(crate) image_family: Option<String>,
    pub(crate) image_project: Option<String>,
    pub(crate) boot_disk_gb: Option<u32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AwsDefaults {
    pub(crate) min_cpus: Option<u32>,
    pub(crate) min_ram_gb: Option<u32>,
    pub(crate) allowed_gpus: Option<Vec<String>>,
    pub(crate) max_price_per_hr: Option<f64>,
    pub(crate) region: Option<String>,
    pub(crate) ami: Option<String>,
    pub(crate) key_name: Option<String>,
    pub(crate) ssh_key_path: Option<String>,
    pub(crate) ssh_user: Option<String>,
    pub(crate) security_group_id: Option<String>,
    pub(crate) subnet_id: Option<String>,
    pub(crate) root_disk_gb: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DeployTargetRequest {
    pub(crate) ssh: bool,
    pub(crate) container: Option<String>,
    pub(crate) unpack: Option<String>,
    pub(crate) arca: Option<String>,
    pub(crate) positional: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthConfig {
    #[serde(default)]
    pub(crate) vast_ai: VastAuth,
    #[serde(default)]
    pub(crate) gcp: GcpAuth,
    #[serde(default)]
    pub(crate) aws: AwsAuth,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VastAuth {
    pub(crate) api_key: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GcpAuth {
    pub(crate) project: Option<String>,
    pub(crate) service_account_json: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AwsAuth {
    pub(crate) access_key_id: Option<String>,
    pub(crate) secret_access_key: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MachineTypeSpec {
    pub(crate) cloud: Cloud,
    pub(crate) machine: &'static str,
    pub(crate) vcpus: u32,
    pub(crate) ram_gb: u32,
    pub(crate) gpus: &'static [&'static str],
    pub(crate) hourly_usd: f64,
    pub(crate) regions: &'static [&'static str],
}

#[derive(Debug, Clone)]
pub(crate) struct CreateSearchRequirements {
    pub(crate) min_cpus: u32,
    pub(crate) min_ram_gb: u32,
    pub(crate) allowed_gpus: Vec<String>,
    pub(crate) max_price_per_hr: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct RegistryAuth {
    pub(crate) username: &'static str,
    pub(crate) secret: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeCostEstimate {
    pub(crate) requested_hours: f64,
    pub(crate) billed_hours: f64,
    pub(crate) hourly_usd: f64,
    pub(crate) total_usd: f64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VastAutoStopPlan {
    pub(crate) stop_at_unix: u64,
    pub(crate) schedule_end_unix: u64,
    pub(crate) runtime_hours: f64,
}

#[derive(Debug)]
pub(crate) enum OfferDecision {
    Accept,
    Reject,
    ChangeFilter,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LoginMethod {
    Cached,
    AutoDetected,
    Prompted,
}

#[derive(Debug, Clone)]
pub(crate) struct LoginOutcome {
    pub(crate) method: LoginMethod,
    pub(crate) saved_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub(crate) enum PrefixLookup {
    Unique(usize),
    Ambiguous(Vec<usize>),
    None,
}

#[derive(Debug, Clone)]
pub(crate) struct CloudMachineCandidate {
    pub(crate) machine: String,
    pub(crate) vcpus: u32,
    pub(crate) ram_gb: u32,
    pub(crate) gpus: Vec<String>,
    pub(crate) hourly_usd: f64,
    pub(crate) region: String,
    pub(crate) zone: Option<String>,
}
