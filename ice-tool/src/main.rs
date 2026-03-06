use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::LazyLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use dialoguer::console::{Key, Term};
use dialoguer::{Confirm, Input, Password, Select, theme::ColorfulTheme};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use names::{ADJECTIVES as NAMES_ADJECTIVES, Generator, NOUNS as NAMES_NOUNS, Name};
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

mod http_retry;

const CONFIG_DIR_NAME: &str = ".ice";
const CONFIG_FILE_NAME: &str = "config.toml";
const ICE_LABEL_PREFIX: &str = "ice-";
const VAST_BASE_URL: &str = "https://console.vast.ai";
const VAST_DEFAULT_IMAGE: &str = "vastai/base-image:@vastai-automatic-tag";
const VAST_DEFAULT_DISK_GB: f64 = 32.0;
const VAST_DEFAULT_SEARCH_LIMIT: u64 = 200;
const VAST_WAIT_TIMEOUT_SECS: u64 = 900;
const VAST_POLL_INTERVAL_SECS: u64 = 5;
const ICE_MANAGED_CONTAINER_NAME: &str = "ice-workload";
const ICE_REMOTE_CONTAINER_ARCHIVE_PATH: &str = "/tmp/ice-container-image.tar";
const ANSI_BOLD_RED: &str = "\x1b[1;31m";
const ANSI_BOLD_WHITE_RED_BG: &str = "\x1b[1;37;41m";
const ANSI_RESET: &str = "\x1b[0m";
const RANDOM_NAME_COLLISION_RETRIES: usize = 256;
const NUMBERED_NAME_COLLISION_RETRIES: usize = 2048;
const NUMBERED_NAME_SUFFIX_MAX: u16 = 9_999;

const KNOWN_VAST_GPU_MODELS: &[&str] = &[
    "A10",
    "A100 PCIE",
    "A100 SXM4",
    "A100X",
    "A40",
    "A800 PCIE",
    "B200",
    "CMP 50HX",
    "GTX 1050",
    "GTX 1050 Ti",
    "GTX 1060",
    "GTX 1070",
    "GTX 1070 Ti",
    "GTX 1080",
    "GTX 1080 Ti",
    "GTX 1650",
    "GTX 1650 S",
    "GTX 1660",
    "GTX 1660 S",
    "GTX 1660 Ti",
    "H100 NVL",
    "H100 PCIE",
    "H100 SXM",
    "H200",
    "H200 NVL",
    "L4",
    "L40",
    "L40S",
    "Q RTX 4000",
    "Q RTX 6000",
    "Q RTX 8000",
    "Quadro P2000",
    "Quadro P4000",
    "Radeon VII",
    "RTX 2000Ada",
    "RTX 2060",
    "RTX 2060S",
    "RTX 2070",
    "RTX 2070S",
    "RTX 2080",
    "RTX 2080 Ti",
    "RTX 3050",
    "RTX 3060",
    "RTX 3060 laptop",
    "RTX 3060 Ti",
    "RTX 3070",
    "RTX 3070 laptop",
    "RTX 3070 Ti",
    "RTX 3080",
    "RTX 3080 Ti",
    "RTX 3090",
    "RTX 3090 Ti",
    "RTX 4000Ada",
    "RTX 4060",
    "RTX 4060 Ti",
    "RTX 4070",
    "RTX 4070 laptop",
    "RTX 4070S",
    "RTX 4070S Ti",
    "RTX 4070 Ti",
    "RTX 4080",
    "RTX 4080S",
    "RTX 4090",
    "RTX 4090D",
    "RTX 4500Ada",
    "RTX 5000Ada",
    "RTX 5060",
    "RTX 5060 Ti",
    "RTX 5070",
    "RTX 5070 Ti",
    "RTX 5080",
    "RTX 5090",
    "RTX 5880Ada",
    "RTX 6000Ada",
    "RTX A2000",
    "RTX A4000",
    "RTX A4500",
    "RTX A5000",
    "RTX A6000",
    "RTX PRO 4000",
    "RTX PRO 4500",
    "RTX PRO 5000",
    "RTX PRO 6000 S",
    "RTX PRO 6000 WS",
    "RX 6950 XT",
    "Tesla P100",
    "Tesla P4",
    "Tesla P40",
    "Tesla T4",
    "Tesla V100",
    "Titan RTX",
    "Titan V",
    "Titan Xp",
];

const NAMEGEN_ADJECTIVES: &[&str] = &[
    "agile",
    "adamant",
    "adept",
    "adventurous",
    "airy",
    "amber",
    "balanced",
    "arcadian",
    "auspicious",
    "awesome",
    "blossoming",
    "brave",
    "bright",
    "calm",
    "candid",
    "careful",
    "celestial",
    "charming",
    "chatty",
    "circular",
    "clever",
    "coastal",
    "considerate",
    "cosmic",
    "cubic",
    "curious",
    "dapper",
    "delighted",
    "didactic",
    "diligent",
    "eager",
    "earnest",
    "effulgent",
    "erudite",
    "excellent",
    "exquisite",
    "fabulous",
    "fascinating",
    "fluent",
    "forgiving",
    "friendly",
    "gallant",
    "gentle",
    "golden",
    "glowing",
    "gracious",
    "gregarious",
    "harmonic",
    "hearty",
    "honest",
    "hopeful",
    "humble",
    "implacable",
    "inventive",
    "jovial",
    "joyous",
    "judicious",
    "jumping",
    "keen",
    "kind",
    "likable",
    "lively",
    "lucid",
    "loyal",
    "lucky",
    "marvellous",
    "mellifluous",
    "nimble",
    "nautical",
    "oblong",
    "outstanding",
    "patient",
    "playful",
    "polished",
    "polite",
    "profound",
    "quadratic",
    "quiet",
    "radiant",
    "rectangular",
    "remarkable",
    "resolute",
    "rusty",
    "sensible",
    "serene",
    "shining",
    "sincere",
    "sparkling",
    "splendid",
    "spry",
    "steady",
    "stellar",
    "sunny",
    "tenacious",
    "tidy",
    "tremendous",
    "triangular",
    "undulating",
    "unflappable",
    "upbeat",
    "unique",
    "verdant",
    "vivid",
    "vitreous",
    "whimsical",
    "witty",
    "wise",
    "zippy",
];

const NAMEGEN_NOUNS: &[&str] = &[
    "aardvark",
    "accordion",
    "albatross",
    "apple",
    "apricot",
    "anvil",
    "asteroid",
    "banjo",
    "beacon",
    "bee",
    "beetle",
    "bison",
    "bonsai",
    "brachiosaur",
    "breeze",
    "brook",
    "cactus",
    "canary",
    "capsicum",
    "cedar",
    "chisel",
    "clarinet",
    "comet",
    "coral",
    "cowbell",
    "crab",
    "cuckoo",
    "cymbal",
    "dahlia",
    "diplodocus",
    "dingo",
    "donkey",
    "drum",
    "duck",
    "echidna",
    "elephant",
    "falcon",
    "fern",
    "firefly",
    "fjord",
    "foxglove",
    "galaxy",
    "geyser",
    "glockenspiel",
    "goose",
    "hammer",
    "harbor",
    "hazelnut",
    "heron",
    "hill",
    "horizon",
    "horse",
    "hyacinth",
    "iguanadon",
    "jasmine",
    "jellyfish",
    "kangaroo",
    "kestrel",
    "lake",
    "lantern",
    "lark",
    "lemon",
    "lemur",
    "lotus",
    "lyrebird",
    "magpie",
    "megalodon",
    "meteor",
    "mongoose",
    "mountain",
    "mouse",
    "muskrat",
    "nebula",
    "newt",
    "oboe",
    "ocelot",
    "otter",
    "orange",
    "owl",
    "panda",
    "peach",
    "pebble",
    "pelican",
    "pepper",
    "pinecone",
    "plum",
    "poppy",
    "prairie",
    "petunia",
    "pheasant",
    "piano",
    "pigeon",
    "platypus",
    "quasar",
    "quokka",
    "raven",
    "reef",
    "rhinoceros",
    "river",
    "rustacean",
    "saffron",
    "salamander",
    "seahorse",
    "sitar",
    "sparrow",
    "spruce",
    "starling",
    "stegosaurus",
    "sunflower",
    "tambourine",
    "thistle",
    "tiger",
    "tomato",
    "toucan",
    "triceratops",
    "turnip",
    "ukulele",
    "viola",
    "violet",
    "walrus",
    "weasel",
    "willow",
    "wombat",
    "xylophone",
    "yak",
    "zebra",
];

#[derive(Debug, Clone, Copy)]
struct MachineTypeSpec {
    cloud: Cloud,
    machine: &'static str,
    vcpus: u32,
    ram_gb: u32,
    gpus: &'static [&'static str],
    hourly_usd: f64,
    regions: &'static [&'static str],
}

const GCP_MACHINE_SPECS: &[MachineTypeSpec] = &[
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

const AWS_MACHINE_SPECS: &[MachineTypeSpec] = &[
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

#[derive(Debug, Parser)]
#[command(
    name = "ice",
    about = "Manage cloud VM instances and marketplace offers.",
    after_help = "Shorthands:\n  sh  shell\n"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(
        name = "login",
        about = "Ensure credentials exist for a cloud provider."
    )]
    Login(LoginArgs),

    #[command(name = "config", about = "Read/write ice configuration values.")]
    Config(ConfigArgs),

    #[command(
        name = "list",
        about = "List current instances created by ice on a cloud provider."
    )]
    List(CloudArgs),

    #[command(
        name = "shell",
        alias = "sh",
        about = "Open shell into an instance (container-first when present)."
    )]
    Shell(ShellArgs),

    #[command(
        name = "dl",
        about = "Download file/dir from instance via rsync over SSH."
    )]
    Dl(DownloadArgs),

    #[command(name = "stop", about = "Stop an instance.")]
    Stop(InstanceArgs),

    #[command(name = "start", about = "Start an instance.")]
    Start(InstanceArgs),

    #[command(name = "delete", about = "Stop then delete an instance.")]
    Delete(InstanceArgs),

    #[command(
        name = "create",
        about = "Search marketplace offers and create the cheapest matching instance."
    )]
    Create(CreateArgs),
}

#[derive(Debug, Args)]
struct CloudArgs {
    /// Cloud provider (`vast.ai`, `gcp`, `aws`). Optional if default.cloud is set.
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,
}

#[derive(Debug, Args)]
struct LoginArgs {
    /// Cloud provider (`vast.ai`, `gcp`, `aws`). Optional if default.cloud is set.
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,

    /// Force re-auth/login flow, ignoring cached credentials in ~/.ice/config.toml.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommands,
}

#[derive(Debug, Subcommand)]
enum ConfigCommands {
    #[command(name = "list", about = "List all supported config keys and values.")]
    List(ConfigListArgs),

    #[command(name = "get", about = "Read a single config key.")]
    Get(ConfigGetArgs),

    #[command(name = "set", about = "Set a single config key.")]
    Set(ConfigSetArgs),

    #[command(name = "unset", about = "Unset a single config key.")]
    Unset(ConfigUnsetArgs),
}

#[derive(Debug, Args)]
struct ConfigListArgs {}

#[derive(Debug, Args)]
struct ConfigGetArgs {
    /// Config key to read.
    key: String,
}

#[derive(Debug, Args)]
struct ConfigSetArgs {
    /// Key/value pair in KEY=VALUE form.
    pair: String,
}

#[derive(Debug, Args)]
struct ConfigUnsetArgs {
    /// Config key to unset.
    key: String,
}

#[derive(Debug, Args)]
struct ShellArgs {
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,

    /// Instance ID or label.
    instance: String,
}

#[derive(Debug, Args)]
struct DownloadArgs {
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,

    /// Instance ID or label.
    instance: String,

    /// Source file or directory on the remote instance.
    remote_path: String,

    /// Optional destination path on local machine (default: current directory).
    local_path: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct InstanceArgs {
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,

    /// Instance ID or label.
    instance: String,
}

#[derive(Debug, Args)]
struct CreateArgs {
    #[arg(long, value_enum)]
    cloud: Option<Cloud>,

    /// Required minimum offer duration in hours (float allowed, e.g. `0.1`).
    hours: f64,

    /// Cloud-specific machine type override (for example `g2-standard-4`, `g5.xlarge`).
    #[arg(long)]
    machine: Option<String>,

    /// Prompt for search filters even when defaults are already set.
    #[arg(long)]
    custom: bool,

    /// Run search and selection, but abort before accept/pay/create step.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
enum Cloud {
    #[value(name = "vast.ai")]
    #[serde(rename = "vast.ai")]
    VastAi,

    #[value(name = "gcp")]
    #[serde(rename = "gcp")]
    Gcp,

    #[value(name = "aws")]
    #[serde(rename = "aws")]
    Aws,
}

impl std::fmt::Display for Cloud {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cloud::VastAi => write!(f, "vast.ai"),
            Cloud::Gcp => write!(f, "gcp"),
            Cloud::Aws => write!(f, "aws"),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IceConfig {
    #[serde(default)]
    default: DefaultConfig,
    #[serde(default)]
    auth: AuthConfig,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DefaultConfig {
    cloud: Option<Cloud>,
    #[serde(default)]
    vast_ai: VastDefaults,
    #[serde(default)]
    gcp: GcpDefaults,
    #[serde(default)]
    aws: AwsDefaults,
    // Legacy global search defaults (migrated into cloud-specific defaults on use).
    min_cpus: Option<u32>,
    min_ram_gb: Option<u32>,
    allowed_gpus: Option<Vec<String>>,
    #[serde(alias = "max_price")]
    max_price_per_hr: Option<f64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct VastDefaults {
    min_cpus: Option<u32>,
    min_ram_gb: Option<u32>,
    allowed_gpus: Option<Vec<String>>,
    #[serde(alias = "max_price")]
    max_price_per_hr: Option<f64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpDefaults {
    min_cpus: Option<u32>,
    min_ram_gb: Option<u32>,
    allowed_gpus: Option<Vec<String>>,
    max_price_per_hr: Option<f64>,
    region: Option<String>,
    zone: Option<String>,
    image_family: Option<String>,
    image_project: Option<String>,
    boot_disk_gb: Option<u32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsDefaults {
    min_cpus: Option<u32>,
    min_ram_gb: Option<u32>,
    allowed_gpus: Option<Vec<String>>,
    max_price_per_hr: Option<f64>,
    region: Option<String>,
    ami: Option<String>,
    key_name: Option<String>,
    ssh_key_path: Option<String>,
    ssh_user: Option<String>,
    security_group_id: Option<String>,
    subnet_id: Option<String>,
    root_disk_gb: Option<u32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AuthConfig {
    #[serde(default)]
    vast_ai: VastAuth,
    #[serde(default)]
    gcp: GcpAuth,
    #[serde(default)]
    aws: AwsAuth,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct VastAuth {
    api_key: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpAuth {
    project: Option<String>,
    service_account_json: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsAuth {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

#[derive(Debug, Clone)]
struct CreateSearchRequirements {
    min_cpus: u32,
    min_ram_gb: u32,
    allowed_gpus: Vec<String>,
    max_price_per_hr: f64,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeCostEstimate {
    requested_hours: f64,
    billed_hours: f64,
    hourly_usd: f64,
    total_usd: f64,
}

#[derive(Debug, Clone, Copy)]
struct VastAutoStopPlan {
    stop_at_unix: u64,
    schedule_end_unix: u64,
    runtime_hours: f64,
}

#[derive(Debug, Clone)]
struct SetupPlan {
    container_archive_path: Option<PathBuf>,
}

#[derive(Debug)]
enum OfferDecision {
    Accept,
    Reject,
    ChangeFilter,
}

#[derive(Debug, Clone, Copy)]
enum LoginMethod {
    Cached,
    AutoDetected,
    Prompted,
}

#[derive(Debug, Clone)]
struct LoginOutcome {
    method: LoginMethod,
    saved_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
enum PrefixLookup {
    Unique(usize),
    Ambiguous(Vec<usize>),
    None,
}

#[derive(Debug, Clone)]
struct CloudMachineCandidate {
    machine: String,
    vcpus: u32,
    ram_gb: u32,
    gpus: Vec<String>,
    hourly_usd: f64,
    region: String,
    zone: Option<String>,
}

#[derive(Debug, Clone)]
struct GcpInstance {
    name: String,
    zone: String,
    status: String,
    machine_type: String,
    creation_timestamp: Option<String>,
    last_start_timestamp: Option<String>,
}

#[derive(Debug, Clone)]
struct AwsInstance {
    instance_id: String,
    name: Option<String>,
    region: String,
    state: String,
    instance_type: String,
    launch_time: Option<String>,
    public_ip: Option<String>,
    public_dns: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VastInstanceCacheEntry {
    id: u64,
    label: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct VastInstanceCache {
    #[serde(default)]
    entries: Vec<VastInstanceCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcpInstanceCacheEntry {
    name: String,
    zone: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GcpInstanceCache {
    #[serde(default)]
    entries: Vec<GcpInstanceCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsInstanceCacheEntry {
    instance_id: String,
    name: Option<String>,
    region: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AwsInstanceCache {
    #[serde(default)]
    entries: Vec<AwsInstanceCacheEntry>,
}

#[derive(Debug, Deserialize)]
struct VastOffersResponse {
    #[serde(default)]
    offers: Vec<VastOffer>,
}

#[derive(Debug, Clone, Deserialize)]
struct VastOffer {
    id: u64,
    #[serde(default)]
    gpu_name: Option<String>,
    #[serde(default)]
    num_gpus: Option<u32>,
    #[serde(default)]
    cpu_cores_effective: Option<f64>,
    #[serde(default)]
    cpu_ram: Option<f64>,
    #[serde(default)]
    dph_total: Option<f64>,
    #[serde(default)]
    reliability: Option<f64>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    geolocation: Option<String>,
    #[serde(default)]
    verification: Option<String>,
    #[serde(default)]
    search: Option<VastHourlyBreakdown>,
}

impl VastOffer {
    fn hourly_price(&self) -> f64 {
        if let Some(value) = self.dph_total {
            return value;
        }
        if let Some(search) = &self.search {
            if let Some(value) = search.total_hour {
                return value;
            }
            if let Some(value) = search.discounted_total_per_hour {
                return value;
            }
        }
        f64::INFINITY
    }

    fn gpu_name(&self) -> &str {
        self.gpu_name.as_deref().unwrap_or("unknown")
    }
}

#[derive(Debug, Clone, Deserialize)]
struct VastHourlyBreakdown {
    #[serde(default, rename = "totalHour")]
    total_hour: Option<f64>,
    #[serde(default, rename = "discountedTotalPerHour")]
    discounted_total_per_hour: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct VastInstancesResponse {
    #[serde(default)]
    instances: Vec<VastInstance>,
}

#[derive(Debug, Clone, Deserialize)]
struct VastInstance {
    id: u64,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    cur_state: Option<String>,
    #[serde(default)]
    next_state: Option<String>,
    #[serde(default)]
    intended_status: Option<String>,
    #[serde(default)]
    actual_status: Option<String>,
    #[serde(default)]
    status_msg: Option<String>,
    #[serde(default)]
    start_date: Option<f64>,
    #[serde(default)]
    uptime_mins: Option<f64>,
    #[serde(default)]
    gpu_name: Option<String>,
    #[serde(default)]
    dph_total: Option<f64>,
    #[serde(default)]
    end_date: Option<f64>,
    #[serde(default)]
    ssh_host: Option<String>,
    #[serde(default)]
    ssh_port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
struct VastScheduledJob {
    #[serde(default)]
    instance_id: Option<u64>,
    #[serde(default)]
    api_endpoint: Option<String>,
    #[serde(default)]
    request_method: Option<String>,
    #[serde(default)]
    request_body: Option<Value>,
    #[serde(default)]
    start_time: Option<f64>,
}

impl VastInstance {
    fn label_str(&self) -> &str {
        self.label.as_deref().unwrap_or("")
    }

    fn state_str(&self) -> &str {
        self.cur_state
            .as_deref()
            .or(self.next_state.as_deref())
            .unwrap_or("unknown")
    }

    fn is_running(&self) -> bool {
        self.state_str().eq_ignore_ascii_case("running")
    }

    fn is_stopped(&self) -> bool {
        self.state_str().eq_ignore_ascii_case("stopped")
    }

    fn health_hint(&self) -> &'static str {
        if self
            .status_msg
            .as_deref()
            .map(|msg| msg.to_ascii_lowercase().contains("unhealthy"))
            .unwrap_or(false)
        {
            return "unhealthy";
        }

        let expected_running = self
            .intended_status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("running"))
            .unwrap_or(false);
        let actual_running = self
            .actual_status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("running"))
            .unwrap_or(self.is_running());

        if expected_running && !actual_running {
            "unhealthy"
        } else {
            "ok"
        }
    }

    fn runtime_hours(&self) -> f64 {
        if let Some(uptime_mins) = self.uptime_mins
            && uptime_mins > 0.0
        {
            return uptime_mins / 60.0;
        }

        if self.is_running()
            && let Some(start) = self.start_date
        {
            let now = now_unix_secs_f64();
            if now > start {
                return (now - start) / 3600.0;
            }
        }

        0.0
    }
}

impl GcpInstance {
    fn is_running(&self) -> bool {
        self.status.eq_ignore_ascii_case("RUNNING")
    }

    fn is_stopped(&self) -> bool {
        matches!(
            self.status.as_str(),
            "TERMINATED" | "STOPPING" | "SUSPENDED" | "SUSPENDING"
        )
    }

    fn runtime_hours(&self) -> f64 {
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

impl AwsInstance {
    fn label_str(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }

    fn is_running(&self) -> bool {
        self.state.eq_ignore_ascii_case("running")
    }

    fn is_stopped(&self) -> bool {
        self.state.eq_ignore_ascii_case("stopped")
    }

    fn runtime_hours(&self) -> f64 {
        if !self.is_running() {
            return 0.0;
        }
        let Some(launch) = self.launch_time.as_deref() else {
            return 0.0;
        };
        elapsed_hours_from_rfc3339(launch).unwrap_or(0.0)
    }
}

#[derive(Debug, Deserialize)]
struct VastSimpleResponse {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    new_contract: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct VastGpuNamesResponse {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    gpu_names: Vec<String>,
}

struct VastClient {
    http: Client,
    api_key: String,
}

#[derive(Debug, Clone, Copy)]
enum InstanceSshKeyAttachStatus {
    Attached,
    AlreadyAssociated,
}

impl VastClient {
    fn new(api_key: &str) -> Result<Self> {
        let api_key = api_key.trim();
        if api_key.is_empty() {
            bail!("Missing Vast API key. Run `ice login --cloud vast.ai`.");
        }

        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self {
            http,
            api_key: api_key.to_owned(),
        })
    }

    fn validate_api_key(&self) -> Result<()> {
        let _ = self.get_json("/api/v0/users/current/", "validate vast.ai API key")?;
        Ok(())
    }

    fn fetch_gpu_names(&self) -> Result<Vec<String>> {
        let value = self.get_json("/api/v0/gpu_names/unique/", "fetch gpu names")?;
        let parsed: VastGpuNamesResponse = serde_json::from_value(value)
            .context("Failed to parse gpu names response from vast.ai")?;
        if parsed.success == Some(false) {
            bail!("vast.ai rejected GPU names request");
        }
        Ok(parsed.gpu_names)
    }

    fn list_instances(&self) -> Result<Vec<VastInstance>> {
        let value = self.get_json("/api/v0/instances/", "list instances")?;
        let parsed: VastInstancesResponse =
            serde_json::from_value(value).context("Failed to parse vast.ai instances response")?;
        Ok(parsed.instances)
    }

    fn list_scheduled_jobs(&self) -> Result<Vec<VastScheduledJob>> {
        let value = self.get_json("/api/v0/commands/schedule_job/", "list scheduled jobs")?;
        let rows = if let Some(array) = value.as_array() {
            array.clone()
        } else if let Some(array) = value.get("results").and_then(Value::as_array) {
            array.clone()
        } else {
            Vec::new()
        };

        let mut jobs = Vec::new();
        for row in rows {
            if let Ok(parsed) = serde_json::from_value::<VastScheduledJob>(row) {
                jobs.push(parsed);
            }
        }
        Ok(jobs)
    }

    fn get_instance(&self, id: u64) -> Result<Option<VastInstance>> {
        match self.get_instance_by_id(id) {
            Ok(Some(instance)) => Ok(Some(instance)),
            Ok(None) => {
                let instances = self.list_instances()?;
                Ok(instances.into_iter().find(|instance| instance.id == id))
            }
            Err(err) if should_fallback_to_vast_list_lookup(&err) => {
                let instances = self.list_instances()?;
                Ok(instances.into_iter().find(|instance| instance.id == id))
            }
            Err(err) => Err(err),
        }
    }

    fn get_instance_by_id(&self, id: u64) -> Result<Option<VastInstance>> {
        let path = format!("/api/v0/instances/{id}/");
        let value = self.get_json(&path, "get instance")?;
        parse_vast_instance_from_value(&value)
    }

    fn search_offers(&self, body: &Value) -> Result<Vec<VastOffer>> {
        let value = self.post_json("/api/v0/bundles/", body, "search offers")?;
        let parsed: VastOffersResponse =
            serde_json::from_value(value).context("Failed to parse vast.ai offers response")?;
        Ok(parsed.offers)
    }

    fn create_instance(&self, offer_id: u64, body: &Value) -> Result<u64> {
        let path = format!("/api/v0/asks/{offer_id}/");
        let value = self.put_json(&path, body, "create instance")?;
        let parsed: VastSimpleResponse =
            serde_json::from_value(value).context("Failed to parse create instance response")?;
        if parsed.success != Some(true) {
            let msg = parsed
                .msg
                .or(parsed.error)
                .unwrap_or_else(|| "unknown create error".to_owned());
            bail!("Failed to create instance: {msg}");
        }
        parsed
            .new_contract
            .ok_or_else(|| anyhow!("Vast API response missing `new_contract`"))
    }

    fn set_instance_state(&self, id: u64, state: &str) -> Result<()> {
        let path = format!("/api/v0/instances/{id}/");
        let body = json!({"state": state});
        let value = self.put_json(&path, &body, &format!("set instance {id} to {state}"))?;
        let parsed: VastSimpleResponse =
            serde_json::from_value(value).context("Failed to parse set state response")?;
        if parsed.success != Some(true) {
            let msg = parsed
                .msg
                .or(parsed.error)
                .unwrap_or_else(|| "unknown state update error".to_owned());
            bail!("Failed to set instance state: {msg}");
        }
        Ok(())
    }

    fn delete_instance(&self, id: u64) -> Result<()> {
        let path = format!("/api/v0/instances/{id}/");
        let value = self.delete_json(&path, "delete instance")?;
        let parsed: VastSimpleResponse =
            serde_json::from_value(value).context("Failed to parse delete response")?;
        if parsed.success != Some(true) {
            let msg = parsed
                .msg
                .or(parsed.error)
                .unwrap_or_else(|| "unknown delete error".to_owned());
            bail!("Failed to delete instance: {msg}");
        }
        Ok(())
    }

    fn schedule_instance_stop(
        &self,
        id: u64,
        stop_at_unix: u64,
        schedule_end_unix: u64,
    ) -> Result<()> {
        if schedule_end_unix <= stop_at_unix {
            bail!(
                "Invalid auto-stop schedule for instance {id}: end ({schedule_end_unix}) must be greater than stop time ({stop_at_unix})."
            );
        }

        let body = json!({
            "start_time": stop_at_unix as f64,
            "end_time": schedule_end_unix as f64,
            "api_endpoint": format!("/api/v0/instances/{id}/"),
            "request_method": "PUT",
            "request_body": { "state": "stopped" },
            "day_of_the_week": Value::Null,
            "hour_of_the_day": Value::Null,
            "frequency": "HOURLY",
            "instance_id": id
        });
        let value = self.post_json(
            "/api/v0/commands/schedule_job/",
            &body,
            "schedule vast.ai instance auto-stop",
        )?;

        if value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
        {
            let msg = value
                .get("msg")
                .and_then(Value::as_str)
                .or_else(|| value.get("error").and_then(Value::as_str))
                .unwrap_or("unknown schedule error");
            bail!("Failed to schedule instance auto-stop: {msg}");
        }

        Ok(())
    }

    fn attach_instance_ssh_key(
        &self,
        id: u64,
        ssh_key: &str,
    ) -> Result<InstanceSshKeyAttachStatus> {
        let path = format!("/api/v0/instances/{id}/ssh/");
        let body = json!({ "ssh_key": ssh_key });
        let value = self.post_json(&path, &body, "attach ssh key to instance")?;
        let parsed: VastSimpleResponse =
            serde_json::from_value(value).context("Failed to parse attach ssh key response")?;
        if parsed.success == Some(false) {
            let msg = parsed
                .msg
                .or(parsed.error)
                .unwrap_or_else(|| "unknown attach ssh key error".to_owned());
            let msg_lc = msg.to_ascii_lowercase();
            if msg_lc.contains("already associated with instance")
                || msg_lc.contains("already associated")
            {
                return Ok(InstanceSshKeyAttachStatus::AlreadyAssociated);
            }
            bail!("Failed to attach ssh key: {msg}");
        }
        Ok(InstanceSshKeyAttachStatus::Attached)
    }

    fn list_account_ssh_keys(&self) -> Result<Vec<String>> {
        let value = self.get_json("/api/v0/ssh/", "list account ssh keys")?;

        let rows = if let Some(array) = value.as_array() {
            array.clone()
        } else if let Some(array) = value.get("keys").and_then(Value::as_array) {
            array.clone()
        } else {
            Vec::new()
        };

        let mut keys = Vec::new();
        for row in rows {
            if let Some(key) = row
                .get("key")
                .and_then(Value::as_str)
                .or_else(|| row.get("public_key").and_then(Value::as_str))
            {
                let trimmed = key.trim();
                if !trimmed.is_empty() {
                    keys.push(trimmed.to_owned());
                }
            }
        }
        Ok(keys)
    }

    fn create_account_ssh_key(&self, ssh_key: &str) -> Result<()> {
        let value = self.post_json(
            "/api/v0/ssh/",
            &json!({ "ssh_key": ssh_key }),
            "create account ssh key",
        )?;
        if value
            .get("success")
            .and_then(Value::as_bool)
            .map(|success| !success)
            .unwrap_or(false)
        {
            let msg = value
                .get("msg")
                .and_then(Value::as_str)
                .or_else(|| value.get("error").and_then(Value::as_str))
                .unwrap_or("unknown create account ssh key error");
            bail!("Failed to create account ssh key: {msg}");
        }
        Ok(())
    }

    fn ensure_account_ssh_key(&self, ssh_key: &str) -> Result<bool> {
        let target = ssh_key.trim();
        if target.is_empty() {
            bail!("SSH key cannot be empty.");
        }
        let existing = self.list_account_ssh_keys()?;
        if existing.iter().any(|candidate| candidate.trim() == target) {
            return Ok(false);
        }
        self.create_account_ssh_key(target)?;
        Ok(true)
    }

    fn get_json(&self, path: &str, context: &str) -> Result<Value> {
        let url = format!("{VAST_BASE_URL}{path}");
        self.send_json(|| self.auth(self.http.get(&url)), context)
    }

    fn post_json(&self, path: &str, body: &Value, context: &str) -> Result<Value> {
        let url = format!("{VAST_BASE_URL}{path}");
        self.send_json(|| self.auth(self.http.post(&url).json(body)), context)
    }

    fn put_json(&self, path: &str, body: &Value, context: &str) -> Result<Value> {
        let url = format!("{VAST_BASE_URL}{path}");
        self.send_json(|| self.auth(self.http.put(&url).json(body)), context)
    }

    fn delete_json(&self, path: &str, context: &str) -> Result<Value> {
        let url = format!("{VAST_BASE_URL}{path}");
        self.send_json(
            || self.auth(self.http.delete(&url).json(&json!({}))),
            context,
        )
    }

    fn auth(&self, request: RequestBuilder) -> RequestBuilder {
        request.header("Authorization", format!("Bearer {}", self.api_key))
    }

    fn send_json<F>(&self, make_request: F, context: &str) -> Result<Value>
    where
        F: FnMut() -> RequestBuilder,
    {
        let response = http_retry::send_with_429_backoff(
            make_request,
            context,
            http_retry::BackoffPolicy::default(),
        )?;
        parse_json_response(response, context)
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            print_big_red_error(&format!("{err:#}"));
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let mut config = load_config()?;

    match cli.command {
        Commands::Login(args) => cmd_login(args, &mut config)?,
        Commands::Config(args) => cmd_config(args, &mut config)?,
        Commands::List(args) => cmd_list(args, &config)?,
        Commands::Shell(args) => cmd_shell(args, &config)?,
        Commands::Dl(args) => cmd_download(args, &config)?,
        Commands::Stop(args) => cmd_stop(args, &config)?,
        Commands::Start(args) => cmd_start(args, &config)?,
        Commands::Delete(args) => cmd_delete(args, &config)?,
        Commands::Create(args) => cmd_create(args, &mut config)?,
    }

    Ok(ExitCode::SUCCESS)
}

fn cmd_login(args: LoginArgs, config: &mut IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    ensure_provider_cli_installed(cloud)?;
    let outcome = match cloud {
        Cloud::VastAi => login_vast(config, args.force)?,
        Cloud::Gcp => login_gcp(config, args.force)?,
        Cloud::Aws => login_aws(config, args.force)?,
    };
    print_login_outcome(cloud, &outcome);
    Ok(())
}

fn cmd_config(args: ConfigArgs, config: &mut IceConfig) -> Result<()> {
    match args.command {
        ConfigCommands::List(_args) => cmd_config_list(config),
        ConfigCommands::Get(get_args) => cmd_config_get(get_args, config),
        ConfigCommands::Set(set_args) => cmd_config_set(set_args, config),
        ConfigCommands::Unset(unset_args) => cmd_config_unset(unset_args, config),
    }
}

fn cmd_config_list(config: &IceConfig) -> Result<()> {
    for key in supported_config_keys() {
        let value = get_config_value(config, key)?;
        println!("{key} = {value}");
    }
    Ok(())
}

fn cmd_config_get(args: ConfigGetArgs, config: &IceConfig) -> Result<()> {
    let key = normalize_config_key(&args.key)?;
    let value = get_config_value(config, &key)?;
    println!("{key} = {value}");
    Ok(())
}

fn cmd_config_set(args: ConfigSetArgs, config: &mut IceConfig) -> Result<()> {
    let (key, value) = parse_key_value_pair(&args.pair)?;
    let rendered = set_config_value(config, &key, &value)?;
    let path = save_config(config)?;
    eprintln!("Set `{key}` = {rendered} ({})", path.display());
    Ok(())
}

fn cmd_config_unset(args: ConfigUnsetArgs, config: &mut IceConfig) -> Result<()> {
    let key = normalize_config_key(&args.key)?;
    unset_config_value(config, &key)?;
    let path = save_config(config)?;
    eprintln!("Unset `{key}` ({})", path.display());
    Ok(())
}

fn cmd_list(args: CloudArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let mut instances: Vec<VastInstance> = client
                .list_instances()?
                .into_iter()
                .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
                .collect();
            persist_vast_instance_cache(&instances);
            let scheduled_termination = match client.list_scheduled_jobs() {
                Ok(jobs) => nearest_vast_scheduled_termination_by_instance(&jobs),
                Err(err) => {
                    eprintln!("Warning: failed to load Vast scheduled jobs: {err:#}");
                    HashMap::new()
                }
            };

            instances.sort_by(|a, b| b.id.cmp(&a.id));

            if instances.is_empty() {
                println!("No `ice`-managed instances found on `vast.ai`.");
                return Ok(());
            }

            println!(
                "{:<10} {:<28} {:<10} {:>8} {:>9} {:<10} {:>8} {:<16}",
                "ID", "LABEL", "STATE", "HOURS", "REMAIN", "HEALTH", "$/hr", "GPU"
            );
            println!("{}", "-".repeat(108));
            for instance in instances {
                let label = truncate_ellipsis(visible_instance_name(instance.label_str()), 28);
                let state = truncate_ellipsis(instance.state_str(), 10);
                let hours = format!("{:.2}", instance.runtime_hours());
                let remaining = vast_remaining_hours_display(
                    &instance,
                    scheduled_termination.get(&instance.id).copied(),
                );
                let health = instance.health_hint();
                let hourly = instance
                    .dph_total
                    .map(|price| format!("{price:.4}"))
                    .unwrap_or_else(|| "-".to_owned());
                let gpu = truncate_ellipsis(instance.gpu_name.as_deref().unwrap_or("-"), 16);
                println!(
                    "{:<10} {:<28} {:<10} {:>8} {:>9} {:<10} {:>8} {:<16}",
                    instance.id, label, state, hours, remaining, health, hourly, gpu
                );
            }
            Ok(())
        }
        Cloud::Gcp => {
            let mut instances = gcp_list_instances(config)?;
            persist_gcp_instance_cache(&instances);
            instances.sort_by(|a, b| b.name.cmp(&a.name));

            if instances.is_empty() {
                println!("No `ice`-managed instances found on `gcp`.");
                return Ok(());
            }

            println!(
                "{:<26} {:<16} {:<10} {:>8} {:>9} {:<10} {:>8} {:<16}",
                "NAME", "MACHINE", "STATE", "HOURS", "REMAIN", "HEALTH", "$/hr", "ZONE"
            );
            println!("{}", "-".repeat(112));
            for instance in instances {
                let health = if instance.is_running() { "ok" } else { "-" };
                let hours = format!("{:.2}", instance.runtime_hours());
                let price = estimated_machine_hourly_price(cloud, &instance.machine_type)
                    .map(|value| format!("{value:.4}"))
                    .unwrap_or_else(|| "-".to_owned());
                println!(
                    "{:<26} {:<16} {:<10} {:>8} {:>9} {:<10} {:>8} {:<16}",
                    truncate_ellipsis(visible_instance_name(&instance.name), 26),
                    truncate_ellipsis(&instance.machine_type, 16),
                    truncate_ellipsis(&instance.status, 10),
                    hours,
                    "-",
                    health,
                    price,
                    truncate_ellipsis(&instance.zone, 16)
                );
            }
            Ok(())
        }
        Cloud::Aws => {
            let mut instances = aws_list_instances(config)?;
            persist_aws_instance_cache(&instances);
            instances.sort_by(|a, b| b.instance_id.cmp(&a.instance_id));

            if instances.is_empty() {
                println!("No `ice`-managed instances found on `aws`.");
                return Ok(());
            }

            println!(
                "{:<20} {:<24} {:<14} {:>8} {:>9} {:<10} {:>8} {:<12}",
                "ID", "NAME", "STATE", "HOURS", "REMAIN", "HEALTH", "$/hr", "REGION"
            );
            println!("{}", "-".repeat(116));
            for instance in instances {
                let health = if instance.is_running() { "ok" } else { "-" };
                let hours = format!("{:.2}", instance.runtime_hours());
                let price = estimated_machine_hourly_price(cloud, &instance.instance_type)
                    .map(|value| format!("{value:.4}"))
                    .unwrap_or_else(|| "-".to_owned());
                println!(
                    "{:<20} {:<24} {:<14} {:>8} {:>9} {:<10} {:>8} {:<12}",
                    truncate_ellipsis(&instance.instance_id, 20),
                    truncate_ellipsis(visible_instance_name(instance.label_str()), 24),
                    truncate_ellipsis(&instance.state, 14),
                    hours,
                    "-",
                    health,
                    price,
                    truncate_ellipsis(&instance.region, 12)
                );
            }
            Ok(())
        }
    }
}

fn cmd_shell(args: ShellArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let mut instance = resolve_vast_instance(&client, &args.instance)?;

            if instance.is_stopped() {
                let should_start =
                    prompt_confirm("Instance is stopped. Start it before opening shell?", true)?;
                if !should_start {
                    bail!("Aborted: instance is stopped.");
                }
                let spinner = spinner("Starting instance...");
                client
                    .set_instance_state(instance.id, "running")
                    .context("Failed to start stopped instance")?;
                spinner.finish_with_message("Start requested.");
                instance = wait_for_instance_state(
                    &client,
                    instance.id,
                    "running",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }

            instance = wait_for_instance_ssh_ready(
                &client,
                instance.id,
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            open_vast_shell_with_auto_key(&client, &instance)
        }
        Cloud::Gcp => {
            let mut instance = resolve_gcp_instance(config, &args.instance)?;
            if instance.is_stopped() {
                let should_start =
                    prompt_confirm("Instance is stopped. Start it before opening shell?", true)?;
                if !should_start {
                    bail!("Aborted: instance is stopped.");
                }
                gcp_set_instance_state(config, &instance, true)?;
                instance = wait_for_gcp_instance_state(
                    config,
                    &instance.name,
                    &instance.zone,
                    "RUNNING",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }
            gcp_open_shell(config, &instance)
        }
        Cloud::Aws => {
            let mut instance = resolve_aws_instance(config, &args.instance)?;
            if instance.is_stopped() {
                let should_start =
                    prompt_confirm("Instance is stopped. Start it before opening shell?", true)?;
                if !should_start {
                    bail!("Aborted: instance is stopped.");
                }
                aws_set_instance_state(config, &instance, true)?;
                instance = wait_for_aws_instance_state(
                    config,
                    &instance.instance_id,
                    &instance.region,
                    "running",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }
            aws_open_shell(config, &instance)
        }
    }
}

fn cmd_download(args: DownloadArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let instance = resolve_vast_instance(&client, &args.instance)?;
            if !instance.is_running() {
                bail!(
                    "Instance `{}` is not running (state: {}).",
                    instance.id,
                    instance.state_str()
                );
            }
            ensure_instance_has_ssh(&instance)?;
            run_vast_download_with_auto_key(
                &client,
                &instance,
                &args.remote_path,
                args.local_path.as_deref(),
            )
        }
        Cloud::Gcp => {
            let instance = resolve_gcp_instance(config, &args.instance)?;
            if !instance.is_running() {
                bail!(
                    "Instance `{}` is not running (state: {}).",
                    instance.name,
                    instance.status
                );
            }
            gcp_download(
                config,
                &instance,
                &args.remote_path,
                args.local_path.as_deref(),
            )
        }
        Cloud::Aws => {
            let instance = resolve_aws_instance(config, &args.instance)?;
            if !instance.is_running() {
                bail!(
                    "Instance `{}` is not running (state: {}).",
                    instance.instance_id,
                    instance.state
                );
            }
            aws_download(
                config,
                &instance,
                &args.remote_path,
                args.local_path.as_deref(),
            )
        }
    }
}

fn cmd_stop(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let instance = resolve_vast_instance(&client, &args.instance)?;
            if instance.is_stopped() {
                println!("Instance {} is already stopped.", instance.id);
                return Ok(());
            }

            let spinner = spinner(&format!("Stopping instance {}...", instance.id));
            client
                .set_instance_state(instance.id, "stopped")
                .context("Failed to stop instance")?;
            spinner.finish_with_message("Stop requested.");

            let _ = wait_for_instance_state(
                &client,
                instance.id,
                "stopped",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;

            println!("Stopped instance {}.", instance.id);
            Ok(())
        }
        Cloud::Gcp => {
            let instance = resolve_gcp_instance(config, &args.instance)?;
            if instance.is_stopped() {
                println!(
                    "Instance {} is already stopped.",
                    visible_instance_name(&instance.name)
                );
                return Ok(());
            }
            gcp_set_instance_state(config, &instance, false)?;
            let _ = wait_for_gcp_instance_state(
                config,
                &instance.name,
                &instance.zone,
                "TERMINATED",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            println!(
                "Stopped instance {}.",
                visible_instance_name(&instance.name)
            );
            Ok(())
        }
        Cloud::Aws => {
            let instance = resolve_aws_instance(config, &args.instance)?;
            if instance.is_stopped() {
                println!("Instance {} is already stopped.", instance.instance_id);
                return Ok(());
            }
            aws_set_instance_state(config, &instance, false)?;
            let _ = wait_for_aws_instance_state(
                config,
                &instance.instance_id,
                &instance.region,
                "stopped",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            println!("Stopped instance {}.", instance.instance_id);
            Ok(())
        }
    }
}

fn cmd_start(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let instance = resolve_vast_instance(&client, &args.instance)?;
            if instance.is_running() {
                println!("Instance {} is already running.", instance.id);
                return Ok(());
            }

            let spinner = spinner(&format!("Starting instance {}...", instance.id));
            client
                .set_instance_state(instance.id, "running")
                .context("Failed to start instance")?;
            spinner.finish_with_message("Start requested.");

            let _ = wait_for_instance_state(
                &client,
                instance.id,
                "running",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;

            println!("Started instance {}.", instance.id);
            Ok(())
        }
        Cloud::Gcp => {
            let instance = resolve_gcp_instance(config, &args.instance)?;
            if instance.is_running() {
                println!(
                    "Instance {} is already running.",
                    visible_instance_name(&instance.name)
                );
                return Ok(());
            }
            gcp_set_instance_state(config, &instance, true)?;
            let _ = wait_for_gcp_instance_state(
                config,
                &instance.name,
                &instance.zone,
                "RUNNING",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            println!(
                "Started instance {}.",
                visible_instance_name(&instance.name)
            );
            Ok(())
        }
        Cloud::Aws => {
            let instance = resolve_aws_instance(config, &args.instance)?;
            if instance.is_running() {
                println!("Instance {} is already running.", instance.instance_id);
                return Ok(());
            }
            aws_set_instance_state(config, &instance, true)?;
            let _ = wait_for_aws_instance_state(
                config,
                &instance.instance_id,
                &instance.region,
                "running",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
            println!("Started instance {}.", instance.instance_id);
            Ok(())
        }
    }
}

fn cmd_delete(args: InstanceArgs, config: &IceConfig) -> Result<()> {
    let cloud = resolve_cloud(args.cloud, config)?;
    if cloud != Cloud::VastAi {
        ensure_provider_cli_installed(cloud)?;
    }
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let mut instance = resolve_vast_instance(&client, &args.instance)?;

            if !instance.is_stopped() {
                let spinner = spinner(&format!(
                    "Stopping instance {} before delete...",
                    instance.id
                ));
                client
                    .set_instance_state(instance.id, "stopped")
                    .context("Failed to stop instance before delete")?;
                spinner.finish_with_message("Stop requested.");
                instance = wait_for_instance_state(
                    &client,
                    instance.id,
                    "stopped",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }

            let spinner = spinner(&format!("Deleting instance {}...", instance.id));
            client
                .delete_instance(instance.id)
                .context("Failed to delete instance")?;
            spinner.finish_with_message("Deleted.");
            println!("Deleted instance {}.", instance.id);
            Ok(())
        }
        Cloud::Gcp => {
            let mut instance = resolve_gcp_instance(config, &args.instance)?;
            if !instance.is_stopped() {
                gcp_set_instance_state(config, &instance, false)?;
                instance = wait_for_gcp_instance_state(
                    config,
                    &instance.name,
                    &instance.zone,
                    "TERMINATED",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }

            let spinner = spinner(&format!(
                "Deleting instance {}...",
                visible_instance_name(&instance.name)
            ));
            gcp_delete_instance(config, &instance)?;
            spinner.finish_with_message("Deleted.");
            println!(
                "Deleted instance {}.",
                visible_instance_name(&instance.name)
            );
            Ok(())
        }
        Cloud::Aws => {
            let mut instance = resolve_aws_instance(config, &args.instance)?;
            if !instance.is_stopped() {
                aws_set_instance_state(config, &instance, false)?;
                instance = wait_for_aws_instance_state(
                    config,
                    &instance.instance_id,
                    &instance.region,
                    "stopped",
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
            }

            let spinner = spinner(&format!("Deleting instance {}...", instance.instance_id));
            aws_terminate_instance(config, &instance)?;
            spinner.finish_with_message("Deleted.");
            println!("Deleted instance {}.", instance.instance_id);
            Ok(())
        }
    }
}

fn cmd_create(args: CreateArgs, config: &mut IceConfig) -> Result<()> {
    if args.hours <= 0.0 {
        bail!("HOURS must be > 0.");
    }

    let cloud = resolve_cloud(args.cloud, config)?;
    match cloud {
        Cloud::VastAi => {
            let client = vast_client_from_config(config)?;
            let gpu_options = load_gpu_options(cloud, Some(&client));
            ensure_default_create_config(config, cloud, &gpu_options)?;

            let mut search = build_search_requirements(config, cloud)?;
            if args.custom {
                prompt_create_search_filters(&mut search, &gpu_options)?;
            }

            let (offer, decision) = loop {
                let offer =
                    find_cheapest_offer(&client, &search, args.hours, args.machine.as_deref())?;

                let price = offer.hourly_price();
                if !price.is_finite() {
                    bail!("Vast returned an offer without usable hourly price.");
                }
                let cost = estimate_runtime_cost(cloud, price, args.hours)?;
                let cost = apply_vast_autostop_cost_estimate(cost)?;

                print_offer_summary(&offer, &cost, &search);

                if cost.hourly_usd > search.max_price_per_hr {
                    let available_hours = offer_duration_seconds(&offer)
                        .map(|seconds| seconds / 3600.0)
                        .unwrap_or(0.0);
                    bail!(
                        "No offer meets max price ${:.4}/hr. Best matching offer is ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested). Offer {} is available for {:.3}h.",
                        search.max_price_per_hr,
                        price,
                        cost.total_usd,
                        cost.billed_hours,
                        cost.requested_hours,
                        offer.id,
                        available_hours
                    );
                }

                if args.dry_run {
                    println!(
                        "Dry run: best matching offer is {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before accept/pay/create.",
                        offer.id, price, cost.total_usd, cost.billed_hours, cost.requested_hours
                    );
                    return Ok(());
                }

                let decision = prompt_offer_decision(&build_accept_prompt(&cost))?;
                match decision {
                    OfferDecision::ChangeFilter => {
                        let choices = load_gpu_options(cloud, Some(&client));
                        prompt_adjust_search_filters(&mut search, &choices)?;
                    }
                    OfferDecision::Reject => {
                        println!("Aborted.");
                        return Ok(());
                    }
                    OfferDecision::Accept => {
                        break (offer, decision);
                    }
                }
            };

            let setup_plan = match decision {
                OfferDecision::Accept => prompt_container_setup_plan()?,
                OfferDecision::Reject | OfferDecision::ChangeFilter => unreachable!(),
            };

            let existing_names = collect_vast_existing_visible_names(&client)?;
            let label = build_ice_instance_label(&existing_names)?;
            let create_body = json!({
                "client_id": "me",
                "image": VAST_DEFAULT_IMAGE,
                "disk": VAST_DEFAULT_DISK_GB,
                "runtype": "ssh",
                "label": label,
                "cancel_unavail": true
            });

            let create_spinner = spinner("Accepting offer and creating instance...");
            let instance_id = client
                .create_instance(offer.id, &create_body)
                .with_context(|| format!("Failed to create instance from offer {}", offer.id))?;
            create_spinner.finish_with_message(format!("Created instance {instance_id}."));

            let auto_stop_plan = build_vast_autostop_plan(now_unix_secs(), args.hours)?;
            let auto_stop_spinner = spinner("Scheduling instance auto-stop...");
            client
                .schedule_instance_stop(
                    instance_id,
                    auto_stop_plan.stop_at_unix,
                    auto_stop_plan.schedule_end_unix,
                )
                .with_context(|| {
                    format!("Failed to schedule auto-stop for vast.ai instance {instance_id}.")
                })?;
            auto_stop_spinner.finish_with_message(format!(
                "Auto-stop scheduled for {} ({:.3}h planned runtime).",
                format_unix_utc(auto_stop_plan.stop_at_unix),
                auto_stop_plan.runtime_hours
            ));

            let instance = wait_for_instance_ssh_ready(
                &client,
                instance_id,
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;

            let ssh_identity = attach_local_ssh_key_to_vast_instance(&client, instance.id)?;
            let ssh_identity_path = ssh_identity.as_ref().map(|(path, _)| path.as_path());
            apply_setup_plan(&instance, &setup_plan, ssh_identity_path)?;

            let drop_in_shell = prompt_confirm("Open shell in the new instance now?", true)?;
            if drop_in_shell {
                open_vast_shell_with_auto_key(&client, &instance)?;
            }

            Ok(())
        }
        Cloud::Gcp | Cloud::Aws => {
            ensure_provider_cli_installed(cloud)?;
            let gpu_options = load_gpu_options(cloud, None);
            ensure_default_create_config(config, cloud, &gpu_options)?;
            let mut search = build_search_requirements(config, cloud)?;
            if args.custom {
                prompt_create_search_filters(&mut search, &gpu_options)?;
            }

            let (candidate, decision) = loop {
                let candidate =
                    find_cheapest_cloud_machine(cloud, config, &search, args.machine.as_deref())?;
                let cost = estimate_runtime_cost(cloud, candidate.hourly_usd, args.hours)?;

                print_machine_candidate_summary(cloud, &candidate, &cost, &search);

                if cost.hourly_usd > search.max_price_per_hr {
                    bail!(
                        "No machine meets max price ${:.4}/hr. Cheapest matching machine is {} in {} at ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested).",
                        search.max_price_per_hr,
                        candidate.machine,
                        candidate.region,
                        candidate.hourly_usd,
                        cost.total_usd,
                        cost.billed_hours,
                        cost.requested_hours
                    );
                }

                if args.dry_run {
                    println!(
                        "Dry run: cheapest matching machine is {} in {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before create.",
                        candidate.machine,
                        candidate.region,
                        candidate.hourly_usd,
                        cost.total_usd,
                        cost.billed_hours,
                        cost.requested_hours
                    );
                    return Ok(());
                }

                let decision = prompt_offer_decision(&build_accept_prompt(&cost))?;
                match decision {
                    OfferDecision::ChangeFilter => {
                        let choices = load_gpu_options(cloud, None);
                        prompt_adjust_search_filters(&mut search, &choices)?;
                    }
                    OfferDecision::Reject => {
                        println!("Aborted.");
                        return Ok(());
                    }
                    OfferDecision::Accept => {
                        break (candidate, decision);
                    }
                }
            };

            let setup_plan = match decision {
                OfferDecision::Accept => prompt_container_setup_plan()?,
                OfferDecision::Reject | OfferDecision::ChangeFilter => unreachable!(),
            };

            match cloud {
                Cloud::Gcp => {
                    let instance = gcp_create_instance(config, &candidate, args.hours)?;
                    apply_setup_plan_gcp(config, &instance, &setup_plan)?;
                    let drop_in_shell =
                        prompt_confirm("Open shell in the new instance now?", true)?;
                    if drop_in_shell {
                        gcp_open_shell(config, &instance)?;
                    }
                }
                Cloud::Aws => {
                    let instance = aws_create_instance(config, &candidate, args.hours)?;
                    apply_setup_plan_aws(config, &instance, &setup_plan)?;
                    let drop_in_shell =
                        prompt_confirm("Open shell in the new instance now?", true)?;
                    if drop_in_shell {
                        aws_open_shell(config, &instance)?;
                    }
                }
                Cloud::VastAi => unreachable!(),
            }

            Ok(())
        }
    }
}

fn cloud_search_key_prefix(cloud: Cloud) -> &'static str {
    match cloud {
        Cloud::VastAi => "default.vast_ai",
        Cloud::Gcp => "default.gcp",
        Cloud::Aws => "default.aws",
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
    }
}

fn migrate_legacy_search_defaults_into_cloud(config: &mut IceConfig, cloud: Cloud) -> bool {
    let mut changed = false;
    let legacy_min_cpus = config.default.min_cpus;
    let legacy_min_ram_gb = config.default.min_ram_gb;
    let legacy_allowed_gpus = config.default.allowed_gpus.clone();
    let legacy_max_price = config.default.max_price_per_hr;

    let (min_cpus, min_ram_gb, allowed_gpus, max_price_per_hr) =
        cloud_search_defaults_mut(config, cloud);

    if min_cpus.is_none() && legacy_min_cpus.is_some() {
        *min_cpus = legacy_min_cpus;
        changed = true;
    }
    if min_ram_gb.is_none() && legacy_min_ram_gb.is_some() {
        *min_ram_gb = legacy_min_ram_gb;
        changed = true;
    }
    if allowed_gpus.is_none() && legacy_allowed_gpus.is_some() {
        *allowed_gpus = legacy_allowed_gpus;
        changed = true;
    }
    if max_price_per_hr.is_none() && legacy_max_price.is_some() {
        *max_price_per_hr = legacy_max_price;
        changed = true;
    }

    changed
}

fn ensure_default_create_config(
    config: &mut IceConfig,
    cloud: Cloud,
    gpu_options: &[String],
) -> Result<()> {
    let mut changed = migrate_legacy_search_defaults_into_cloud(config, cloud);
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

fn build_search_requirements(config: &IceConfig, cloud: Cloud) -> Result<CreateSearchRequirements> {
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

fn find_cheapest_offer(
    client: &VastClient,
    req: &CreateSearchRequirements,
    hours: f64,
    machine_override: Option<&str>,
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
        offer.hourly_price().is_finite()
            && offer_duration_seconds(offer)
                .map(|duration| duration >= duration_seconds)
                .unwrap_or(false)
    });
    if offers.is_empty() {
        bail!(
            "No offers match the filters (min_cpus={}, min_ram_gb={}, allowed_gpus={}, min_duration_hours={:.2}).",
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

fn required_runtime_seconds(hours: f64) -> u64 {
    ((hours * 3600.0).ceil().max(1.0)) as u64
}

fn estimated_billed_hours(cloud: Cloud, requested_hours: f64) -> f64 {
    match cloud {
        Cloud::VastAi => requested_hours,
        Cloud::Gcp | Cloud::Aws => required_runtime_seconds(requested_hours) as f64 / 3600.0,
    }
}

fn estimate_runtime_cost(
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

fn apply_vast_autostop_cost_estimate(cost: RuntimeCostEstimate) -> Result<RuntimeCostEstimate> {
    let plan = build_vast_autostop_plan(now_unix_secs(), cost.requested_hours)?;
    let total_usd = cost.hourly_usd * plan.runtime_hours;
    Ok(RuntimeCostEstimate {
        requested_hours: cost.requested_hours,
        billed_hours: plan.runtime_hours,
        hourly_usd: cost.hourly_usd,
        total_usd,
    })
}

fn build_vast_autostop_plan(start_unix: u64, requested_hours: f64) -> Result<VastAutoStopPlan> {
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
        // Keep scheduler window narrow so this runs once at the target hourly tick.
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

fn build_accept_prompt(cost: &RuntimeCostEstimate) -> String {
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

fn print_offer_summary(
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

fn prompt_offer_decision(prompt: &str) -> Result<OfferDecision> {
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

fn prompt_adjust_search_filters(
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

fn prompt_create_search_filters(
    req: &mut CreateSearchRequirements,
    gpu_choices: &[String],
) -> Result<()> {
    require_interactive("`ice create --custom` requires interactive stdin.")?;
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

fn prompt_container_setup_plan() -> Result<SetupPlan> {
    require_interactive("Container setup prompt requires interactive stdin.")?;
    let should_upload = prompt_confirm(
        "Upload a local container archive to this instance and auto-start it?",
        false,
    )?;
    if !should_upload {
        return Ok(SetupPlan {
            container_archive_path: None,
        });
    }

    let mut input = Input::<String>::with_theme(prompt_theme());
    input = input.with_prompt("Local path to container archive (.tar / .tar.gz)");
    let value = input
        .interact_text()
        .context("Failed to read container archive path")?;
    let archive_path = PathBuf::from(value.trim());
    if archive_path.as_os_str().is_empty() {
        bail!("Container archive path cannot be empty.");
    }
    if !archive_path.exists() {
        bail!(
            "Container archive path does not exist: {}",
            archive_path.display()
        );
    }
    if !archive_path.is_file() {
        bail!(
            "Container archive path must be a file: {}",
            archive_path.display()
        );
    }

    Ok(SetupPlan {
        container_archive_path: Some(archive_path),
    })
}

fn apply_setup_plan(
    instance: &VastInstance,
    plan: &SetupPlan,
    identity_file: Option<&Path>,
) -> Result<()> {
    if let Some(container_archive_path) = plan.container_archive_path.as_deref() {
        ensure_instance_has_ssh(instance)?;
        setup_instance_container_vast(instance, container_archive_path, identity_file)?;
    } else {
        println!("No container archive provided; skipping container upload.");
    }
    Ok(())
}

fn setup_instance_container_vast(
    instance: &VastInstance,
    container_archive_path: &Path,
    identity_file: Option<&Path>,
) -> Result<()> {
    let upload_spinner = spinner("Uploading container archive to vast.ai instance...");
    run_rsync_upload_vast(
        instance,
        container_archive_path,
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
        identity_file,
    )?;
    upload_spinner.finish_with_message("Container archive uploaded.");

    let setup_spinner = spinner("Loading container image and starting container...");
    let command_str = wrap_remote_shell_script(&container_setup_remote_script(
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
    ));
    run_vast_remote_command(
        instance,
        &command_str,
        identity_file,
        "load and start container on vast.ai instance",
    )?;
    setup_spinner.finish_with_message(format!(
        "Container `{}` started on instance {}.",
        ICE_MANAGED_CONTAINER_NAME, instance.id
    ));
    Ok(())
}

fn setup_instance_container_gcp(
    config: &IceConfig,
    instance: &GcpInstance,
    container_archive_path: &Path,
) -> Result<()> {
    let upload_spinner = spinner("Uploading container archive to gcp instance...");
    gcp_upload_file(
        config,
        instance,
        container_archive_path,
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
    )?;
    upload_spinner.finish_with_message("Container archive uploaded.");

    let setup_spinner = spinner("Loading container image and starting container...");
    let command_str = wrap_remote_shell_script(&container_setup_remote_script(
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
    ));
    gcp_run_remote_command(config, instance, &command_str)?;
    setup_spinner.finish_with_message(format!(
        "Container `{}` started on instance {}.",
        ICE_MANAGED_CONTAINER_NAME,
        visible_instance_name(&instance.name)
    ));
    Ok(())
}

fn setup_instance_container_aws(
    config: &IceConfig,
    instance: &AwsInstance,
    container_archive_path: &Path,
) -> Result<()> {
    let upload_spinner = spinner("Uploading container archive to aws instance...");
    aws_upload_file(
        config,
        instance,
        container_archive_path,
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
    )?;
    upload_spinner.finish_with_message("Container archive uploaded.");

    let setup_spinner = spinner("Loading container image and starting container...");
    let command_str = wrap_remote_shell_script(&container_setup_remote_script(
        ICE_REMOTE_CONTAINER_ARCHIVE_PATH,
    ));
    aws_run_remote_command(config, instance, &command_str)?;
    setup_spinner.finish_with_message(format!(
        "Container `{}` started on instance {}.",
        ICE_MANAGED_CONTAINER_NAME, instance.instance_id
    ));
    Ok(())
}

fn wrap_remote_shell_script(script: &str) -> String {
    format!("sh -lc {}", shell_quote_single(script))
}

fn container_setup_remote_script(remote_archive_path: &str) -> String {
    let quoted_archive = shell_quote_single(remote_archive_path);
    let quoted_container = shell_quote_single(ICE_MANAGED_CONTAINER_NAME);
    format!(
        r#"set -eu
archive_path={quoted_archive}
container_name={quoted_container}

runtime=""
if command -v docker >/dev/null 2>&1; then
  runtime="docker"
elif command -v podman >/dev/null 2>&1; then
  runtime="podman"
else
  echo "No supported container runtime found (docker or podman)." >&2
  exit 1
fi

runtime_cmd="$runtime"
if ! $runtime_cmd version >/dev/null 2>&1; then
  if command -v sudo >/dev/null 2>&1; then
    runtime_cmd="sudo $runtime"
  else
    echo "Container runtime $runtime requires elevated privileges, but sudo is unavailable." >&2
    exit 1
  fi
fi

if [ ! -f "$archive_path" ]; then
  echo "Container archive not found at $archive_path" >&2
  exit 1
fi

load_output="$($runtime_cmd load -i "$archive_path" 2>&1)"
echo "$load_output"

image_ref="$(printf '%s\n' "$load_output" | sed -n -e 's/^Loaded image: //p' -e 's/^Loaded image(s): //p' -e 's/^Loaded image ID: //p' | tail -n1)"
if [ -z "$image_ref" ]; then
  image_ref="$($runtime_cmd image ls --format '{{{{.Repository}}}}:{{{{.Tag}}}} {{{{.ID}}}}' | awk '$1 != "<none>:<none>" {{ print $1; exit }}')"
fi
if [ -z "$image_ref" ]; then
  echo "Failed to determine container image name after loading archive." >&2
  exit 1
fi

$runtime_cmd rm -f "$container_name" >/dev/null 2>&1 || true
$runtime_cmd run -d --name "$container_name" --restart unless-stopped "$image_ref" >/dev/null
rm -f "$archive_path"
echo "Container $container_name started from $image_ref."
"#
    )
}

fn container_or_host_shell_remote_command() -> String {
    let quoted_container = shell_quote_single(ICE_MANAGED_CONTAINER_NAME);
    let script = format!(
        r#"set -eu
container_name={quoted_container}

runtime=""
if command -v docker >/dev/null 2>&1; then
  runtime="docker"
elif command -v podman >/dev/null 2>&1; then
  runtime="podman"
fi

if [ -n "$runtime" ]; then
  runtime_cmd="$runtime"
  if ! $runtime_cmd version >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
    runtime_cmd="sudo $runtime"
  fi

  if $runtime_cmd ps -a --format '{{{{.Names}}}}' | grep -Fxq "$container_name"; then
    if ! $runtime_cmd ps --format '{{{{.Names}}}}' | grep -Fxq "$container_name"; then
      $runtime_cmd start "$container_name" >/dev/null
    fi

    if $runtime_cmd exec "$container_name" sh -lc 'command -v bash >/dev/null 2>&1'; then
      exec $runtime_cmd exec -it "$container_name" bash -l
    fi
    exec $runtime_cmd exec -it "$container_name" sh
  fi
fi

if command -v bash >/dev/null 2>&1; then
  exec bash -l
fi
exec sh
"#
    );
    wrap_remote_shell_script(&script)
}

fn build_ice_instance_label(existing_names: &HashSet<String>) -> Result<String> {
    let name = generate_unique_verb_noun_name(existing_names)?;
    Ok(format!("{ICE_LABEL_PREFIX}{name}"))
}

fn wait_for_instance_state(
    client: &VastClient,
    instance_id: u64,
    desired_state: &str,
    timeout: Duration,
) -> Result<VastInstance> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for instance {instance_id} to reach state `{desired_state}`..."
    ));

    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            bail!(
                "Timed out waiting for instance {} to reach state `{}`.",
                instance_id,
                desired_state
            );
        }

        if let Some(instance) = client.get_instance(instance_id)? {
            if instance.state_str().eq_ignore_ascii_case(desired_state) {
                spinner.finish_with_message(format!(
                    "Instance {} is now {}.",
                    instance_id,
                    instance.state_str()
                ));
                return Ok(instance);
            }
        }

        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn wait_for_instance_ssh_ready(
    client: &VastClient,
    instance_id: u64,
    timeout: Duration,
) -> Result<VastInstance> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for instance {instance_id} to be running with SSH..."
    ));
    let mut last_ssh_issue: Option<String> = None;

    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            if let Some(issue) = last_ssh_issue {
                bail!(
                    "Timed out waiting for SSH readiness on instance {instance_id}. Last issue: {issue}"
                );
            }
            bail!("Timed out waiting for SSH readiness on instance {instance_id}.");
        }

        if let Some(instance) = client.get_instance(instance_id)?
            && instance.is_running()
            && instance.ssh_host.as_deref().is_some()
            && instance.ssh_port.is_some()
        {
            let (host, port) = ssh_target(&instance)?;
            match tcp_port_open(&host, port, Duration::from_secs(3)) {
                Ok(()) => {
                    spinner.finish_with_message(format!("Instance {instance_id} is SSH-ready."));
                    return Ok(instance);
                }
                Err(err) => {
                    last_ssh_issue =
                        Some(format!("{host}:{port} not accepting connections ({err})"));
                    spinner.set_message(format!(
                        "Waiting for instance {instance_id} SSH endpoint to accept connections..."
                    ));
                }
            }
        }

        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn resolve_vast_instance(client: &VastClient, identifier: &str) -> Result<VastInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }

    if let Ok(id) = identifier.parse::<u64>() {
        if let Some(instance) = try_vast_instance_by_id(client, id)? {
            return Ok(instance);
        }
        bail!("No instance found with ID `{id}`.");
    }

    let cache = load_cloud_cache_or_default::<VastInstanceCache>(Cloud::VastAi);
    if let Some(instance) = resolve_vast_instance_from_cache(client, &cache, identifier)? {
        return Ok(instance);
    }

    let instances: Vec<VastInstance> = client
        .list_instances()?
        .into_iter()
        .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
        .collect();
    persist_vast_instance_cache(&instances);
    resolve_vast_instance_from_list(instances, identifier)
}

fn resolve_vast_instance_from_cache(
    client: &VastClient,
    cache: &VastInstanceCache,
    identifier: &str,
) -> Result<Option<VastInstance>> {
    match prefix_lookup_indices(&cache.entries, identifier, |entry| entry.label.as_str())? {
        PrefixLookup::Unique(index) => {
            let entry = &cache.entries[index];
            try_vast_instance_by_id(client, entry.id)
        }
        PrefixLookup::Ambiguous(_) | PrefixLookup::None => Ok(None),
    }
}

fn try_vast_instance_by_id(client: &VastClient, id: u64) -> Result<Option<VastInstance>> {
    match client.get_instance_by_id(id) {
        Ok(Some(instance)) if instance.label_str().starts_with(ICE_LABEL_PREFIX) => {
            Ok(Some(instance))
        }
        Ok(Some(_)) => Ok(None),
        Ok(None) => {
            let instances = client.list_instances()?;
            Ok(instances.into_iter().find(|instance| {
                instance.id == id && instance.label_str().starts_with(ICE_LABEL_PREFIX)
            }))
        }
        Err(err) if should_fallback_to_vast_list_lookup(&err) => {
            let instances = client.list_instances()?;
            Ok(instances.into_iter().find(|instance| {
                instance.id == id && instance.label_str().starts_with(ICE_LABEL_PREFIX)
            }))
        }
        Err(err) => Err(err),
    }
}

fn resolve_vast_instance_from_list(
    instances: Vec<VastInstance>,
    identifier: &str,
) -> Result<VastInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.label_str())? {
        PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        PrefixLookup::Ambiguous(indices) => {
            let listing = indices
                .into_iter()
                .map(|index| {
                    let instance = &instances[index];
                    format!(
                        "{} ({})",
                        instance.id,
                        visible_instance_name(instance.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn normalize_instance_identifier_for_name_match(identifier: &str) -> Result<String> {
    let needle = normalize_instance_name_for_match(identifier);
    if needle.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }
    Ok(needle)
}

fn normalize_instance_name_for_match(name: &str) -> String {
    let lowered = name.trim().to_ascii_lowercase();
    lowered
        .strip_prefix(ICE_LABEL_PREFIX)
        .unwrap_or(&lowered)
        .to_owned()
}

fn visible_instance_name(name: &str) -> &str {
    name.strip_prefix(ICE_LABEL_PREFIX).unwrap_or(name)
}

fn prefix_lookup_indices<T, F>(items: &[T], identifier: &str, name_of: F) -> Result<PrefixLookup>
where
    F: Fn(&T) -> &str,
{
    let needle = normalize_instance_identifier_for_name_match(identifier)?;
    let mut exact = Vec::new();
    let mut prefixed = Vec::new();

    for (index, item) in items.iter().enumerate() {
        let candidate = normalize_instance_name_for_match(name_of(item));
        if candidate.is_empty() {
            continue;
        }
        if candidate == needle {
            exact.push(index);
        } else if candidate.starts_with(&needle) {
            prefixed.push(index);
        }
    }

    Ok(match exact.len() {
        1 => PrefixLookup::Unique(exact[0]),
        n if n > 1 => PrefixLookup::Ambiguous(exact),
        _ => match prefixed.len() {
            1 => PrefixLookup::Unique(prefixed[0]),
            n if n > 1 => PrefixLookup::Ambiguous(prefixed),
            _ => PrefixLookup::None,
        },
    })
}

fn should_fallback_to_vast_list_lookup(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("http 404") || msg.contains("http 405") || msg.contains("not found")
}

fn parse_vast_instance_from_value(value: &Value) -> Result<Option<VastInstance>> {
    if value.is_null() {
        return Ok(None);
    }

    if let Some(instance_val) = value.get("instance") {
        let parsed: VastInstance = serde_json::from_value(instance_val.clone())
            .context("Failed to parse vast instance payload from `instance` key")?;
        return Ok(Some(parsed));
    }

    if let Some(instances) = value.get("instances").and_then(Value::as_array) {
        if let Some(first) = instances.first() {
            let parsed: VastInstance = serde_json::from_value(first.clone())
                .context("Failed to parse vast instance payload from `instances[0]`")?;
            return Ok(Some(parsed));
        }
        return Ok(None);
    }

    if value.is_object() {
        if let Ok(parsed) = serde_json::from_value::<VastInstance>(value.clone()) {
            return Ok(Some(parsed));
        }
    }

    Ok(None)
}

fn open_ssh_shell(
    instance: &VastInstance,
    identity_file: Option<&Path>,
    remote_command: Option<&str>,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;
    wait_for_ssh_port_preflight(instance.id, &host, port, Duration::from_secs(30))?;

    let mut command = Command::new("ssh");
    command.args(vast_ssh_args(&host, port, identity_file));
    if let Some(remote_command) = remote_command {
        command.arg("-t").arg(remote_command);
    }

    let status = command
        .status()
        .with_context(|| format!("Failed to run ssh into instance {}", instance.id))?;

    if !status.success() {
        bail!("ssh exited with status {status}");
    }

    Ok(())
}

fn wait_for_ssh_port_preflight(
    instance_id: u64,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<()> {
    let start = SystemTime::now();
    let mut last_error: Option<String> = None;
    loop {
        if elapsed_since(start)? >= timeout {
            let tail = last_error.unwrap_or_else(|| "unknown network error".to_owned());
            bail!(
                "SSH endpoint for instance {instance_id} is not accepting connections yet ({host}:{port}). Last issue: {tail}"
            );
        }

        match tcp_port_open(host, port, Duration::from_secs(3)) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = Some(err.to_string());
            }
        }

        thread::sleep(Duration::from_secs(2));
    }
}

fn vast_ssh_args(host: &str, port: u16, identity_file: Option<&Path>) -> Vec<String> {
    let mut args = vec![
        "-p".to_owned(),
        port.to_string(),
        "-o".to_owned(),
        "StrictHostKeyChecking=accept-new".to_owned(),
    ];
    if let Some(identity) = identity_file {
        args.push("-i".to_owned());
        args.push(identity.display().to_string());
        args.push("-o".to_owned());
        args.push("IdentitiesOnly=yes".to_owned());
    }
    args.push(format!("root@{host}"));
    args
}

fn open_vast_shell_with_auto_key(client: &VastClient, instance: &VastInstance) -> Result<()> {
    let remote_command = container_or_host_shell_remote_command();
    match open_ssh_shell(instance, None, Some(&remote_command)) {
        Ok(()) => Ok(()),
        Err(first_err) => {
            let first_err_text = format!("{first_err:#}");
            let Some((identity, attach_status)) =
                attach_local_ssh_key_to_vast_instance(client, instance.id)?
            else {
                return Err(first_err.context(
                    "Initial SSH attempt failed, and no local SSH keypair was found in `~/.ssh` to attach to this instance.",
                ));
            };

            thread::sleep(Duration::from_secs(2));

            if open_ssh_shell(instance, None, Some(&remote_command)).is_ok() {
                return Ok(());
            }

            if open_ssh_shell(instance, Some(identity.as_path()), Some(&remote_command)).is_ok() {
                return Ok(());
            }

            let account_identity = ensure_local_ssh_key_on_vast_account(client)?;
            thread::sleep(Duration::from_secs(2));
            if open_ssh_shell(instance, None, Some(&remote_command)).is_ok() {
                return Ok(());
            }

            let final_identity = account_identity.as_deref().unwrap_or(identity.as_path());
            open_ssh_shell(instance, Some(final_identity), Some(&remote_command)).with_context(|| {
                let attach_hint = match attach_status {
                    InstanceSshKeyAttachStatus::Attached => "The key attach call succeeded.",
                    InstanceSshKeyAttachStatus::AlreadyAssociated => {
                        "Vast reports an SSH key is already associated with this instance."
                    }
                };
                format!(
                    "Initial SSH attempt failed: {first_err_text}. Retried with instance-level key attach and then account-level key sync, but authentication still failed. Local key: `{}`. {attach_hint}",
                    identity.display()
                )
            })
        }
    }
}

fn attach_local_ssh_key_to_vast_instance(
    client: &VastClient,
    instance_id: u64,
) -> Result<Option<(PathBuf, InstanceSshKeyAttachStatus)>> {
    let Some((private_key_path, public_key)) = discover_local_ssh_keypair()? else {
        return Ok(None);
    };

    let status = client
        .attach_instance_ssh_key(instance_id, &public_key)
        .with_context(|| {
            format!("Failed to attach local SSH key to vast.ai instance {instance_id}")
        })?;
    Ok(Some((private_key_path, status)))
}

fn ensure_local_ssh_key_on_vast_account(client: &VastClient) -> Result<Option<PathBuf>> {
    let Some((private_key_path, public_key)) = discover_local_ssh_keypair()? else {
        return Ok(None);
    };
    client
        .ensure_account_ssh_key(&public_key)
        .context("Failed to ensure local SSH key is present on vast.ai account")?;
    Ok(Some(private_key_path))
}

fn run_vast_download_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    match run_rsync_download(instance, remote_path, local_path, None) {
        Ok(()) => Ok(()),
        Err(first_err) => {
            let Some((identity, attach_status)) =
                attach_local_ssh_key_to_vast_instance(client, instance.id)?
            else {
                return Err(first_err.context(
                    "Rsync download failed, and no local SSH keypair was found in `~/.ssh` to attach to this instance.",
                ));
            };
            // Same propagation window as shell auth.
            thread::sleep(Duration::from_secs(2));
            if run_rsync_download(instance, remote_path, local_path, Some(identity.as_path()))
                .is_ok()
            {
                return Ok(());
            }

            let account_identity = ensure_local_ssh_key_on_vast_account(client)?;
            thread::sleep(Duration::from_secs(2));
            let final_identity = account_identity.as_deref().unwrap_or(identity.as_path());

            run_rsync_download(instance, remote_path, local_path, Some(final_identity)).with_context(
                || {
                    let attach_hint = match attach_status {
                        InstanceSshKeyAttachStatus::Attached => "The key attach call succeeded.",
                        InstanceSshKeyAttachStatus::AlreadyAssociated => {
                            "Vast reports an SSH key is already associated with this instance."
                        }
                    };
                    format!(
                        "Rsync download failed before key attach: {first_err:#}. Retried with instance-level key attach and then account-level key sync, but it still failed. Local key: `{}`. {attach_hint}",
                        identity.display()
                    )
                },
            )
        }
    }
}

fn discover_local_ssh_keypair() -> Result<Option<(PathBuf, String)>> {
    let Some(home) = dirs::home_dir() else {
        return Ok(None);
    };
    let ssh_dir = home.join(".ssh");
    if !ssh_dir.is_dir() {
        return Ok(None);
    }

    let preferred_private_names = [
        "id_ed25519",
        "id_rsa",
        "id_ecdsa",
        "id_ed25519_sk",
        "id_ecdsa_sk",
        "id_dsa",
    ];

    let mut candidate_privates = Vec::new();
    for name in preferred_private_names {
        let path = ssh_dir.join(name);
        if path.is_file() {
            candidate_privates.push(path);
        }
    }

    for entry in
        fs::read_dir(&ssh_dir).with_context(|| format!("Failed to read {}", ssh_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("Failed to read entry in {}", ssh_dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("pub") {
            continue;
        }
        let private = path.with_extension("");
        if private.is_file() && !candidate_privates.iter().any(|p| p == &private) {
            candidate_privates.push(private);
        }
    }

    for private in candidate_privates {
        let public = private.with_extension("pub");
        if !public.is_file() {
            continue;
        }
        let public_key = read_first_ssh_public_key_line(&public)?;
        if let Some(public_key) = public_key {
            return Ok(Some((private, public_key)));
        }
    }

    Ok(None)
}

fn read_first_ssh_public_key_line(path: &Path) -> Result<Option<String>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if looks_like_ssh_public_key(trimmed) {
            return Ok(Some(trimmed.to_owned()));
        }
    }
    Ok(None)
}

fn looks_like_ssh_public_key(line: &str) -> bool {
    line.starts_with("ssh-")
        || line.starts_with("ecdsa-")
        || line.starts_with("sk-ssh-")
        || line.starts_with("sk-ecdsa-")
}

fn run_rsync_download(
    instance: &VastInstance,
    remote_path: &str,
    local_path: Option<&Path>,
    identity_file: Option<&Path>,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;

    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let ssh_transport = if let Some(identity) = identity_file {
        format!(
            "ssh -i {} -o IdentitiesOnly=yes -p {} -o StrictHostKeyChecking=accept-new",
            shell_quote_single(&identity.display().to_string()),
            port
        )
    } else {
        format!("ssh -p {} -o StrictHostKeyChecking=accept-new", port)
    };
    let remote_spec = format!("root@{host}:{remote_path}");

    let status = Command::new("rsync")
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport)
        .arg(remote_spec)
        .arg(destination)
        .status()
        .with_context(|| format!("Failed to run rsync for instance {}", instance.id))?;

    if !status.success() {
        bail!("rsync exited with status {status}");
    }

    Ok(())
}

fn run_rsync_upload_vast(
    instance: &VastInstance,
    local_path: &Path,
    remote_path: &str,
    identity_file: Option<&Path>,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;
    let ssh_transport = if let Some(identity) = identity_file {
        format!(
            "ssh -i {} -o IdentitiesOnly=yes -p {} -o StrictHostKeyChecking=accept-new",
            shell_quote_single(&identity.display().to_string()),
            port
        )
    } else {
        format!("ssh -p {} -o StrictHostKeyChecking=accept-new", port)
    };
    let remote_spec = format!("root@{host}:{remote_path}");
    let status = Command::new("rsync")
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport)
        .arg(local_path)
        .arg(remote_spec)
        .status()
        .with_context(|| {
            format!(
                "Failed to upload container archive to instance {}",
                instance.id
            )
        })?;
    if !status.success() {
        bail!("rsync exited with status {status}");
    }
    Ok(())
}

fn run_vast_remote_command(
    instance: &VastInstance,
    command_str: &str,
    identity_file: Option<&Path>,
    context: &str,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;
    let mut command = Command::new("ssh");
    command
        .args(vast_ssh_args(&host, port, identity_file))
        .arg(command_str);
    run_command_status(&mut command, context)
}

fn ssh_target(instance: &VastInstance) -> Result<(String, u16)> {
    let host = instance
        .ssh_host
        .as_ref()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Instance {} has no ssh_host", instance.id))?;
    let port = instance
        .ssh_port
        .ok_or_else(|| anyhow!("Instance {} has no ssh_port", instance.id))?;
    Ok((host, port))
}

fn ensure_instance_has_ssh(instance: &VastInstance) -> Result<()> {
    let _ = ssh_target(instance)?;
    Ok(())
}

fn tcp_port_open(host: &str, port: u16, timeout: Duration) -> Result<()> {
    let addrs = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("Could not resolve {host}:{port}"))?;
    let mut attempted = false;
    let mut last_err = None;
    for addr in addrs {
        attempted = true;
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(_stream) => return Ok(()),
            Err(err) => {
                last_err = Some(format!("{addr}: {err}"));
            }
        }
    }
    if !attempted {
        bail!("No network address resolved for {host}:{port}");
    }
    bail!(
        "Failed to connect to {host}:{port}: {}",
        last_err.unwrap_or_else(|| "unknown network error".to_owned())
    );
}

fn nearest_vast_scheduled_termination_by_instance(jobs: &[VastScheduledJob]) -> HashMap<u64, f64> {
    let now = now_unix_secs_f64();
    let mut nearest = HashMap::new();

    for job in jobs {
        let Some(instance_id) = job.instance_id else {
            continue;
        };
        let Some(termination_unix) = vast_job_termination_unix(job) else {
            continue;
        };
        if termination_unix <= now {
            continue;
        }
        nearest
            .entry(instance_id)
            .and_modify(|existing: &mut f64| *existing = existing.min(termination_unix))
            .or_insert(termination_unix);
    }

    nearest
}

fn vast_job_termination_unix(job: &VastScheduledJob) -> Option<f64> {
    let Some(start_time) = job.start_time else {
        return None;
    };
    let endpoint = job.api_endpoint.as_deref().unwrap_or("");
    if !endpoint.contains("/api/v0/instances/") {
        return None;
    }

    let method = job
        .request_method
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    if method == "DELETE" {
        return Some(start_time);
    }
    if method == "PUT" {
        let Some(body) = job.request_body.as_ref() else {
            return None;
        };
        let state = body
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if state == "stopped" || state == "deleted" {
            return Some(start_time);
        }
    }
    None
}

fn remaining_contract_hours_at(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
    now: f64,
) -> f64 {
    let contract_remaining = instance.end_date.and_then(|end_date| {
        if end_date > now {
            Some((end_date - now) / 3600.0)
        } else {
            None
        }
    });
    let scheduled_remaining = scheduled_termination_unix.and_then(|time| {
        if time > now {
            Some((time - now) / 3600.0)
        } else {
            None
        }
    });

    match (contract_remaining, scheduled_remaining) {
        (Some(contract), Some(scheduled)) => contract.min(scheduled),
        (Some(contract), None) => contract,
        (None, Some(scheduled)) => scheduled,
        (None, None) => 0.0,
    }
}

fn remaining_contract_hours(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
) -> f64 {
    remaining_contract_hours_at(instance, scheduled_termination_unix, now_unix_secs_f64())
}

fn vast_remaining_hours_display(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
) -> String {
    if instance.end_date.is_none() && scheduled_termination_unix.is_none() {
        return "-".to_owned();
    }
    let remaining = remaining_contract_hours(instance, scheduled_termination_unix).max(0.0);
    format!("{remaining:.2}h")
}

fn print_login_outcome(cloud: Cloud, outcome: &LoginOutcome) {
    let source = match outcome.method {
        LoginMethod::Cached => "using cached credentials",
        LoginMethod::AutoDetected => "using auto-detected credentials",
        LoginMethod::Prompted => "using newly entered credentials",
    };
    if let Some(path) = outcome.saved_path.as_deref() {
        println!("Login `{cloud}`: {source}. Updated {}.", path.display());
    } else {
        println!("Login `{cloud}`: {source}. No config changes.");
    }
}

fn login_vast(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    if !force && let Some(existing_key) = config.auth.vast_ai.api_key.as_deref() {
        match VastClient::new(existing_key)?.validate_api_key() {
            Ok(()) => {
                return Ok(LoginOutcome {
                    method: LoginMethod::Cached,
                    saved_path: None,
                });
            }
            Err(err) => {
                eprintln!("Stored vast.ai API key is invalid: {err:#}");
            }
        }
    }

    require_interactive("`ice login --cloud vast.ai` requires interactive stdin.")?;
    let key_page = "https://cloud.vast.ai/manage-keys/";
    eprintln!("Open {key_page}, copy/create an API key, then paste it below.");
    maybe_open_browser(key_page);

    let api_key = Password::with_theme(prompt_theme())
        .with_prompt("Paste Vast API key")
        .interact()
        .context("Failed to read API key")?;
    let api_key = api_key.trim().to_owned();
    if api_key.is_empty() {
        bail!("API key cannot be empty.");
    }

    let spinner = spinner("Validating vast.ai API key...");
    let client = VastClient::new(&api_key)?;
    client.validate_api_key()?;
    spinner.finish_with_message("vast.ai API key validated.");

    config.auth.vast_ai.api_key = Some(api_key);
    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn login_gcp(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    ensure_command_available("gcloud")?;

    let detected_project = detect_gcp_project(config, !force);
    let detected_creds_path = detect_gcp_credentials_path(config, !force);
    let has_active_account = gcp_has_active_account()?;

    if has_active_account || detected_creds_path.is_some() {
        let mut changed = false;
        if force {
            if detected_project.is_none() && config.auth.gcp.project.take().is_some() {
                changed = true;
            }
            if detected_creds_path.is_none()
                && config.auth.gcp.service_account_json.take().is_some()
            {
                changed = true;
            }
        }
        if let Some(project) = detected_project
            && config.auth.gcp.project.as_deref() != Some(project.as_str())
        {
            config.auth.gcp.project = Some(project);
            changed = true;
        }
        if let Some(path) = detected_creds_path
            && config.auth.gcp.service_account_json.as_deref() != Some(path.as_str())
        {
            config.auth.gcp.service_account_json = Some(path);
            changed = true;
        }

        let saved_path = if changed {
            Some(save_config(config)?)
        } else {
            None
        };
        return Ok(LoginOutcome {
            method: LoginMethod::AutoDetected,
            saved_path,
        });
    }

    require_interactive("`ice login --cloud gcp` requires interactive stdin.")?;
    maybe_open_browser("https://console.cloud.google.com/");

    eprintln!(
        "Could not auto-detect GCP credentials. Provide a service-account JSON path, or run `gcloud auth login` and retry."
    );

    let project_seed = detect_gcp_project(config, true)
        .or_else(|| {
            if force {
                None
            } else {
                config.auth.gcp.project.clone()
            }
        })
        .unwrap_or_default();
    let service_account_seed = detect_gcp_credentials_path(config, true)
        .or_else(|| {
            if force {
                None
            } else {
                config.auth.gcp.service_account_json.clone()
            }
        })
        .unwrap_or_default();

    let project = Input::<String>::with_theme(prompt_theme())
        .with_prompt("GCP project ID (optional)")
        .with_initial_text(project_seed)
        .allow_empty(true)
        .interact_text()
        .context("Failed to read GCP project ID")?;

    let service_account_json = Input::<String>::with_theme(prompt_theme())
        .with_prompt("Service-account JSON path")
        .with_initial_text(service_account_seed)
        .allow_empty(true)
        .interact_text()
        .context("Failed to read GCP credentials path")?;

    let service_account_json = nonempty_string(service_account_json);
    if service_account_json.is_none() {
        bail!(
            "No credentials configured. Provide a service-account JSON path, or run `gcloud auth login` and retry."
        );
    }

    config.auth.gcp.project = nonempty_string(project);
    config.auth.gcp.service_account_json = service_account_json;

    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn login_aws(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    ensure_command_available("aws")?;

    let mut changed = false;
    let env_keypair = detect_aws_env_keypair();
    if let Some((access_key_id, secret_access_key)) = env_keypair.as_ref() {
        if config.auth.aws.access_key_id.as_deref() != Some(access_key_id.as_str()) {
            config.auth.aws.access_key_id = Some(access_key_id.clone());
            changed = true;
        }
        if config.auth.aws.secret_access_key.as_deref() != Some(secret_access_key.as_str()) {
            config.auth.aws.secret_access_key = Some(secret_access_key.clone());
            changed = true;
        }
    } else if force {
        if config.auth.aws.access_key_id.take().is_some() {
            changed = true;
        }
        if config.auth.aws.secret_access_key.take().is_some() {
            changed = true;
        }
    }

    if aws_identity_detected(config, !force)? {
        let saved_path = if changed {
            Some(save_config(config)?)
        } else {
            None
        };
        return Ok(LoginOutcome {
            method: LoginMethod::AutoDetected,
            saved_path,
        });
    }

    require_interactive("`ice login --cloud aws` requires interactive stdin.")?;
    maybe_open_browser("https://console.aws.amazon.com/");
    eprintln!("Could not auto-detect AWS credentials. Enter an access key pair.");

    let access_key_seed = if force {
        env_keypair
            .as_ref()
            .map(|(key, _)| key.clone())
            .unwrap_or_default()
    } else {
        config.auth.aws.access_key_id.clone().unwrap_or_default()
    };
    let access_key_id = Input::<String>::with_theme(prompt_theme())
        .with_prompt("AWS access key ID")
        .with_initial_text(access_key_seed)
        .interact_text()
        .context("Failed to read AWS access key ID")?;

    let secret_access_key = Password::with_theme(prompt_theme())
        .with_prompt("AWS secret access key")
        .allow_empty_password(false)
        .interact()
        .context("Failed to read AWS secret access key")?;

    let access_key_id = nonempty_string(access_key_id)
        .ok_or_else(|| anyhow!("AWS access key ID cannot be empty."))?;
    let secret_access_key = secret_access_key.trim().to_owned();
    if secret_access_key.is_empty() {
        bail!("AWS secret access key cannot be empty.");
    }

    config.auth.aws.access_key_id = Some(access_key_id);
    config.auth.aws.secret_access_key = Some(secret_access_key);

    let path = save_config(config)?;
    Ok(LoginOutcome {
        method: LoginMethod::Prompted,
        saved_path: Some(path),
    })
}

fn detect_gcp_project(config: &IceConfig, include_cached: bool) -> Option<String> {
    if include_cached
        && let Some(project) = config.auth.gcp.project.as_deref()
        && !project.trim().is_empty()
    {
        return Some(project.trim().to_owned());
    }
    for env_key in [
        "CLOUDSDK_CORE_PROJECT",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
    ] {
        if let Ok(value) = std::env::var(env_key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }

    let mut command = Command::new("gcloud");
    command.args(["config", "get-value", "project", "--quiet"]);
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("(unset)") {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn detect_gcp_credentials_path(config: &IceConfig, include_cached: bool) -> Option<String> {
    let mut candidates = Vec::new();
    if include_cached && let Some(path) = config.auth.gcp.service_account_json.as_deref() {
        candidates.push(path.to_owned());
    }
    if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        if !path.trim().is_empty() {
            candidates.push(path);
        }
    }
    if let Some(home) = dirs::home_dir() {
        candidates.push(
            home.join(".config/gcloud/application_default_credentials.json")
                .display()
                .to_string(),
        );
    }

    candidates.into_iter().find(|candidate| {
        let path = Path::new(candidate);
        path.is_file()
    })
}

fn gcp_has_active_account() -> Result<bool> {
    let mut command = Command::new("gcloud");
    command.args([
        "auth",
        "list",
        "--filter=status:ACTIVE",
        "--format=value(account)",
    ]);
    let output = command
        .output()
        .context("Failed to run `gcloud auth list` for credential detection")?;
    if !output.status.success() {
        return Ok(false);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().any(|line| !line.trim().is_empty()))
}

fn detect_aws_env_keypair() -> Option<(String, String)> {
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    let access_key_id = access_key_id.trim().to_owned();
    let secret_access_key = secret_access_key.trim().to_owned();
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }
    Some((access_key_id, secret_access_key))
}

fn aws_identity_detected(config: &IceConfig, include_cached: bool) -> Result<bool> {
    let mut default_chain = Command::new("aws");
    default_chain.args([
        "sts",
        "get-caller-identity",
        "--output",
        "json",
        "--region",
        "us-east-1",
    ]);
    if default_chain.status().is_ok_and(|status| status.success()) {
        return Ok(true);
    }

    if !include_cached {
        return Ok(false);
    }

    let Some(access_key_id) = config.auth.aws.access_key_id.as_deref() else {
        return Ok(false);
    };
    let Some(secret_access_key) = config.auth.aws.secret_access_key.as_deref() else {
        return Ok(false);
    };
    if access_key_id.trim().is_empty() || secret_access_key.trim().is_empty() {
        return Ok(false);
    }

    let mut explicit_keys = Command::new("aws");
    explicit_keys
        .env("AWS_ACCESS_KEY_ID", access_key_id.trim())
        .env("AWS_SECRET_ACCESS_KEY", secret_access_key.trim())
        .args([
            "sts",
            "get-caller-identity",
            "--output",
            "json",
            "--region",
            "us-east-1",
        ]);
    Ok(explicit_keys.status().is_ok_and(|status| status.success()))
}

fn vast_client_from_config(config: &IceConfig) -> Result<VastClient> {
    let api_key =
        config.auth.vast_ai.api_key.as_deref().ok_or_else(|| {
            anyhow!("Missing Vast API key. Run `ice login --cloud vast.ai` first.")
        })?;
    VastClient::new(api_key)
}

fn resolve_cloud(explicit_cloud: Option<Cloud>, config: &IceConfig) -> Result<Cloud> {
    if let Some(cloud) = explicit_cloud {
        return Ok(cloud);
    }

    if let Some(default_cloud) = config.default.cloud {
        return Ok(default_cloud);
    }

    bail!(
        "Missing `--cloud CLOUD`, or set a default with e.g. `ice config set default.cloud=vast.ai` (or `gcp`, `aws`, etc.)."
    )
}

fn parse_key_value_pair(pair: &str) -> Result<(String, String)> {
    let (key, value) = pair.split_once('=').ok_or_else(|| {
        anyhow!("Expected `KEY=VALUE`. Example: `ice config set default.cloud=vast.ai`.")
    })?;

    let key = normalize_config_key(key)?;

    Ok((key.to_owned(), value.trim().to_owned()))
}

fn supported_config_keys() -> &'static [&'static str] {
    &[
        "default.cloud",
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

fn normalize_config_key(key: &str) -> Result<String> {
    let key = key.trim();
    let key = match key {
        "default.min_cpus" => "default.vast_ai.min_cpus",
        "default.min_ram_gb" => "default.vast_ai.min_ram_gb",
        "default.allowed_gpus" => "default.vast_ai.allowed_gpus",
        "default.max_price_per_hr" | "default.max_price" => "default.vast_ai.max_price_per_hr",
        _ => key,
    };
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

fn get_config_value(config: &IceConfig, key: &str) -> Result<String> {
    let key = normalize_config_key(key)?;
    let value = match key.as_str() {
        "default.cloud" => config.default.cloud.map(|cloud| cloud.to_string()),
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

fn set_config_value(config: &mut IceConfig, key: &str, value: &str) -> Result<String> {
    let key = normalize_config_key(key)?;
    match key.as_str() {
        "default.cloud" => {
            let cloud = parse_cloud(value)?;
            config.default.cloud = Some(cloud);
            Ok(cloud.to_string())
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

fn unset_config_value(config: &mut IceConfig, key: &str) -> Result<()> {
    let key = normalize_config_key(key)?;
    match key.as_str() {
        "default.cloud" => config.default.cloud = None,
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

fn parse_cloud(value: &str) -> Result<Cloud> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "vast.ai" | "vast" => Ok(Cloud::VastAi),
        "gcp" => Ok(Cloud::Gcp),
        "aws" => Ok(Cloud::Aws),
        _ => bail!("Invalid cloud `{value}`. Use `vast.ai`, `gcp`, `aws`, etc."),
    }
}

fn config_path() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir.join(CONFIG_DIR_NAME).join(CONFIG_FILE_NAME))
}

fn load_config() -> Result<IceConfig> {
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

fn save_config(config: &IceConfig) -> Result<PathBuf> {
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

fn cloud_cache_slug(cloud: Cloud) -> &'static str {
    match cloud {
        Cloud::VastAi => "vast-ai",
        Cloud::Gcp => "gcp",
        Cloud::Aws => "aws",
    }
}

fn cloud_cache_path(cloud: Cloud) -> Result<PathBuf> {
    let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Failed to determine home directory"))?;
    Ok(home_dir
        .join(CONFIG_DIR_NAME)
        .join(format!("instance-cache-{}.json", cloud_cache_slug(cloud))))
}

fn load_cloud_cache_or_default<T>(cloud: Cloud) -> T
where
    T: DeserializeOwned + Default,
{
    let Ok(path) = cloud_cache_path(cloud) else {
        return T::default();
    };
    if !path.exists() {
        return T::default();
    }

    let content = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(_) => return T::default(),
    };
    if content.trim().is_empty() {
        return T::default();
    }

    serde_json::from_str::<T>(&content).unwrap_or_default()
}

fn save_cloud_cache_best_effort<T>(cloud: Cloud, value: &T)
where
    T: Serialize,
{
    let Ok(path) = cloud_cache_path(cloud) else {
        return;
    };

    let Some(parent) = path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }

    let Ok(content) = serde_json::to_string_pretty(value) else {
        return;
    };
    let _ = fs::write(path, content);
}

fn persist_vast_instance_cache(instances: &[VastInstance]) {
    let entries = instances
        .iter()
        .filter_map(|instance| {
            let label = instance.label_str();
            if !label.starts_with(ICE_LABEL_PREFIX) {
                return None;
            }
            Some(VastInstanceCacheEntry {
                id: instance.id,
                label: label.to_owned(),
            })
        })
        .collect::<Vec<_>>();
    save_cloud_cache_best_effort(Cloud::VastAi, &VastInstanceCache { entries });
}

fn persist_gcp_instance_cache(instances: &[GcpInstance]) {
    let entries = instances
        .iter()
        .filter(|instance| instance.name.starts_with(ICE_LABEL_PREFIX))
        .map(|instance| GcpInstanceCacheEntry {
            name: instance.name.clone(),
            zone: instance.zone.clone(),
        })
        .collect::<Vec<_>>();
    save_cloud_cache_best_effort(Cloud::Gcp, &GcpInstanceCache { entries });
}

fn persist_aws_instance_cache(instances: &[AwsInstance]) {
    let entries = instances
        .iter()
        .map(|instance| AwsInstanceCacheEntry {
            instance_id: instance.instance_id.clone(),
            name: instance.name.clone(),
            region: instance.region.clone(),
        })
        .collect::<Vec<_>>();
    save_cloud_cache_best_effort(Cloud::Aws, &AwsInstanceCache { entries });
}

fn collect_vast_existing_visible_names(client: &VastClient) -> Result<HashSet<String>> {
    let instances = client
        .list_instances()?
        .into_iter()
        .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
        .collect::<Vec<_>>();
    persist_vast_instance_cache(&instances);
    Ok(instances
        .iter()
        .map(|instance| visible_instance_name(instance.label_str()).to_owned())
        .filter(|name| !name.is_empty())
        .collect::<HashSet<_>>())
}

fn prompt_theme() -> &'static ColorfulTheme {
    static THEME: LazyLock<ColorfulTheme> = LazyLock::new(ColorfulTheme::default);
    &THEME
}

fn prompt_confirm(prompt: &str, default: bool) -> Result<bool> {
    require_interactive("Interactive confirmation required.")?;
    Confirm::with_theme(prompt_theme())
        .with_prompt(prompt)
        .default(default)
        .interact()
        .context("Failed to read confirmation")
}

fn prompt_u32(prompt: &str, default: Option<u32>, min_value: u32) -> Result<u32> {
    require_interactive("Interactive numeric input required.")?;
    let mut input = Input::<u32>::with_theme(prompt_theme());
    input = input.with_prompt(prompt);
    if let Some(value) = default {
        input = input.default(value);
    }
    let value = input
        .interact_text()
        .context("Failed to read integer input")?;
    if value < min_value {
        bail!("{prompt} must be >= {min_value}");
    }
    Ok(value)
}

fn prompt_f64(prompt: &str, default: Option<f64>, min_value: f64) -> Result<f64> {
    require_interactive("Interactive numeric input required.")?;
    let mut input = Input::<f64>::with_theme(prompt_theme());
    input = input.with_prompt(prompt);
    if let Some(value) = default {
        input = input.default(value);
    }

    let value = input
        .interact_text()
        .context("Failed to read numeric input")?;
    if !(value.is_finite() && value >= min_value) {
        bail!("{prompt} must be a finite value >= {min_value}");
    }
    Ok(value)
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
            Key::PageDown => {
                if !options.is_empty() {
                    cursor_index = (cursor_index + page_size).min(options.len() - 1);
                }
            }
            Key::Char('n') | Key::Char('N') => {
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
                    .filter_map(|(index, value)| {
                        if selected_flags[index] {
                            Some(value.clone())
                        } else {
                            None
                        }
                    })
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

fn load_gpu_options(cloud: Cloud, vast_client: Option<&VastClient>) -> Vec<String> {
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

fn gpu_quality_score(model: &str) -> i64 {
    (gpu_fp32_tflops_estimate(model) * 1000.0).round() as i64
}

fn gpu_selector_label(model: &str) -> String {
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
    static LOOKUP: LazyLock<HashMap<String, f64>> = LazyLock::new(|| {
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
    static LOOKUP: LazyLock<HashMap<String, f64>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        // Approximate peak FP32 TFLOPS. Primary source preference: vendor spec sheets.
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

fn canonicalize_gpu_name(input: &str) -> Option<String> {
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

fn normalize_gpu_name_token(value: &str) -> String {
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
    }
}

fn estimated_machine_hourly_price(cloud: Cloud, machine: &str) -> Option<f64> {
    all_machine_specs_for_cloud(cloud)
        .iter()
        .find(|spec| spec.machine.eq_ignore_ascii_case(machine))
        .map(|spec| spec.hourly_usd)
}

fn find_cheapest_cloud_machine(
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

fn short_gcp_zone(zone: &str) -> String {
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

fn print_machine_candidate_summary(
    cloud: Cloud,
    candidate: &CloudMachineCandidate,
    cost: &RuntimeCostEstimate,
    req: &CreateSearchRequirements,
) {
    let gpu = if candidate.gpus.is_empty() {
        "none".to_owned()
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

fn ensure_provider_cli_installed(cloud: Cloud) -> Result<()> {
    match cloud {
        Cloud::VastAi => Ok(()),
        Cloud::Gcp => ensure_command_available("gcloud"),
        Cloud::Aws => ensure_command_available("aws"),
    }
}

fn ensure_command_available(command: &str) -> Result<()> {
    if Command::new(command).arg("--version").output().is_ok() {
        Ok(())
    } else {
        bail!("Missing required command `{command}` in `PATH`.");
    }
}

fn run_command_output(command: &mut Command, context: &str) -> Result<std::process::Output> {
    let output = command
        .output()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let msg = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit status {}", output.status)
        };
        bail!("Failed to {context}: {msg}");
    }
    Ok(output)
}

fn run_command_json(command: &mut Command, context: &str) -> Result<Value> {
    let output = run_command_output(command, context)?;
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("Non-UTF8 command output while trying to {context}"))?;
    serde_json::from_str::<Value>(&stdout).with_context(|| {
        format!(
            "Failed to parse JSON output while trying to {context}: {}",
            truncate_ellipsis(&stdout, 280)
        )
    })
}

fn run_command_text(command: &mut Command, context: &str) -> Result<String> {
    let output = run_command_output(command, context)?;
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("Non-UTF8 command output while trying to {context}"))?;
    Ok(stdout.trim().to_owned())
}

fn run_command_status(command: &mut Command, context: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("Failed to run command while trying to {context}"))?;
    if !status.success() {
        bail!("Failed to {context}: command exited with status {status}");
    }
    Ok(())
}

fn gcp_command(config: &IceConfig) -> Command {
    let mut command = Command::new("gcloud");
    if let Some(path) = config.auth.gcp.service_account_json.as_deref()
        && !path.trim().is_empty()
    {
        command.env("GOOGLE_APPLICATION_CREDENTIALS", path.trim());
    }
    command
}

fn maybe_add_gcp_project_arg(command: &mut Command, config: &IceConfig) {
    if let Some(project) = config.auth.gcp.project.as_deref()
        && !project.trim().is_empty()
    {
        command.arg("--project").arg(project.trim());
    }
}

fn gcp_list_instances(config: &IceConfig) -> Result<Vec<GcpInstance>> {
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "instances",
        "list",
        "--filter=labels.ice_managed=true OR name~'^ice-.*'",
        "--format=json",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    let value = run_command_json(&mut command, "list gcp instances")?;
    parse_gcp_instances(value)
}

fn parse_gcp_instances(value: Value) -> Result<Vec<GcpInstance>> {
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("Unexpected gcp instances response shape"))?;
    let mut instances = Vec::new();
    for row in rows {
        if let Some(instance) = parse_gcp_instance_row(row) {
            instances.push(instance);
        }
    }
    Ok(instances)
}

fn parse_gcp_instance_row(row: &Value) -> Option<GcpInstance> {
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

    let creation_timestamp = row
        .get("creationTimestamp")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let last_start_timestamp = row
        .get("lastStartTimestamp")
        .and_then(Value::as_str)
        .map(str::to_owned);

    Some(GcpInstance {
        name,
        zone,
        status,
        machine_type,
        creation_timestamp,
        last_start_timestamp,
    })
}

fn gcp_describe_instance(config: &IceConfig, name: &str, zone: &str) -> Result<GcpInstance> {
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "instances",
        "describe",
        name,
        "--zone",
        zone,
        "--format=json",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    let value = run_command_json(&mut command, "describe gcp instance")?;
    parse_gcp_instance_row(&value)
        .ok_or_else(|| anyhow!("Could not parse gcp instance description for {name}"))
}

fn resolve_gcp_instance(config: &IceConfig, identifier: &str) -> Result<GcpInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }

    let cache = load_cloud_cache_or_default::<GcpInstanceCache>(Cloud::Gcp);
    if let PrefixLookup::Unique(index) =
        prefix_lookup_indices(&cache.entries, identifier, |entry| entry.name.as_str())?
    {
        let entry = &cache.entries[index];
        if let Ok(instance) = gcp_describe_instance(config, &entry.name, &entry.zone)
            && instance.name.starts_with(ICE_LABEL_PREFIX)
        {
            return Ok(instance);
        }
    }

    let instances = gcp_list_instances(config)?;
    persist_gcp_instance_cache(&instances);
    resolve_gcp_instance_from_list(instances, identifier)
}

fn resolve_gcp_instance_from_list(
    instances: Vec<GcpInstance>,
    identifier: &str,
) -> Result<GcpInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.name.as_str())? {
        PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        PrefixLookup::Ambiguous(indices) => {
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
        PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn gcp_set_instance_state(config: &IceConfig, instance: &GcpInstance, running: bool) -> Result<()> {
    let action = if running { "start" } else { "stop" };
    let spinner = spinner(&format!(
        "{action}ing instance {}...",
        visible_instance_name(&instance.name)
    ));
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "instances",
        action,
        &instance.name,
        "--zone",
        &instance.zone,
        "--quiet",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, &format!("{action} gcp instance"))?;
    spinner.finish_with_message(format!("{action} requested."));
    Ok(())
}

fn wait_for_gcp_instance_state(
    config: &IceConfig,
    name: &str,
    zone: &str,
    desired: &str,
    timeout: Duration,
) -> Result<GcpInstance> {
    let start = SystemTime::now();
    loop {
        if elapsed_since(start)? > timeout {
            bail!("Timed out waiting for gcp instance `{name}` to reach `{desired}`");
        }
        let instance = gcp_describe_instance(config, name, zone)?;
        if instance.status.eq_ignore_ascii_case(desired) {
            return Ok(instance);
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn gcp_open_shell(config: &IceConfig, instance: &GcpInstance) -> Result<()> {
    let remote_command = container_or_host_shell_remote_command();
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "ssh",
        &instance.name,
        "--zone",
        &instance.zone,
        "--command",
        &remote_command,
        "--ssh-flag=-t",
        "--ssh-flag=-o",
        "--ssh-flag=StrictHostKeyChecking=accept-new",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "open gcp shell")
}

fn gcp_download(
    config: &IceConfig,
    instance: &GcpInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut command = gcp_command(config);
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
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "download from gcp instance")
}

fn gcp_delete_instance(config: &IceConfig, instance: &GcpInstance) -> Result<()> {
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "instances",
        "delete",
        &instance.name,
        "--zone",
        &instance.zone,
        "--quiet",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "delete gcp instance")
}

fn gcp_create_instance(
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
    hours: f64,
) -> Result<GcpInstance> {
    let zone = candidate
        .zone
        .clone()
        .ok_or_else(|| anyhow!("Missing zone for selected GCP machine type."))?;
    let existing_names = gcp_list_instances(config)?
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

    let auto_stop_script = format!(
        "#!/bin/bash\nnohup bash -lc 'sleep {}; /sbin/shutdown -h now' >/var/log/ice-autostop.log 2>&1 &\n",
        required_runtime_seconds(hours)
    );
    let script_path = write_temp_file("ice-gcp-autostop", ".sh", &auto_stop_script)?;

    let spinner = spinner("Creating gcp instance...");
    let mut command = gcp_command(config);
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
        "--metadata-from-file",
        &format!("startup-script={}", script_path.display()),
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "create gcp instance")?;
    spinner.finish_with_message("Creation requested.");
    let _ = fs::remove_file(script_path);

    let instance = wait_for_gcp_instance_state(
        config,
        &name,
        &zone,
        "RUNNING",
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )?;
    Ok(instance)
}

fn gcp_run_remote_command(
    config: &IceConfig,
    instance: &GcpInstance,
    command_str: &str,
) -> Result<()> {
    let mut command = gcp_command(config);
    command.args([
        "compute",
        "ssh",
        &instance.name,
        "--zone",
        &instance.zone,
        "--command",
        command_str,
        "--ssh-flag=-o",
        "--ssh-flag=StrictHostKeyChecking=accept-new",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "run command on gcp instance")
}

fn gcp_upload_file(
    config: &IceConfig,
    instance: &GcpInstance,
    local_path: &Path,
    remote_path: &str,
) -> Result<()> {
    let remote_spec = format!("{}:{remote_path}", instance.name);
    let mut command = gcp_command(config);
    command.arg("compute").arg("scp");
    command.arg(local_path);
    command.arg(remote_spec);
    command.args([
        "--zone",
        &instance.zone,
        "--scp-flag=-o",
        "--scp-flag=StrictHostKeyChecking=accept-new",
    ]);
    maybe_add_gcp_project_arg(&mut command, config);
    run_command_status(&mut command, "upload file to gcp instance")
}

fn apply_setup_plan_gcp(
    config: &IceConfig,
    instance: &GcpInstance,
    plan: &SetupPlan,
) -> Result<()> {
    if let Some(container_archive_path) = plan.container_archive_path.as_deref() {
        setup_instance_container_gcp(config, instance, container_archive_path)?;
    } else {
        println!("No container archive provided; skipping container upload.");
    }
    Ok(())
}

fn aws_command(config: &IceConfig, region: &str) -> Command {
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

fn aws_regions_to_query(config: &IceConfig) -> Vec<String> {
    if let Some(region) = config.default.aws.region.clone() {
        return vec![region];
    }
    let mut regions = BTreeSet::new();
    for spec in AWS_MACHINE_SPECS {
        for region in spec.regions {
            regions.insert((*region).to_owned());
        }
    }
    regions.into_iter().collect()
}

fn aws_list_instances(config: &IceConfig) -> Result<Vec<AwsInstance>> {
    let mut all = Vec::new();
    for region in aws_regions_to_query(config) {
        let mut command = aws_command(config, &region);
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
        let value = run_command_json(&mut command, &format!("list aws instances in {region}"))?;
        all.extend(parse_aws_instances(&value, &region)?);
    }
    Ok(all)
}

fn parse_aws_instances(value: &Value, region: &str) -> Result<Vec<AwsInstance>> {
    let mut instances = Vec::new();
    let Some(reservations) = value.get("Reservations").and_then(Value::as_array) else {
        return Ok(instances);
    };
    for reservation in reservations {
        let Some(rows) = reservation.get("Instances").and_then(Value::as_array) else {
            continue;
        };
        for row in rows {
            if let Some(instance) = parse_aws_instance_row(row, region) {
                instances.push(instance);
            }
        }
    }
    Ok(instances)
}

fn parse_aws_instance_row(row: &Value, region: &str) -> Option<AwsInstance> {
    let instance_id = row.get("InstanceId")?.as_str()?.to_owned();
    let state = row
        .get("State")
        .and_then(|value| value.get("Name"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let instance_type = row
        .get("InstanceType")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let launch_time = row
        .get("LaunchTime")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let public_ip = row
        .get("PublicIpAddress")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let public_dns = row
        .get("PublicDnsName")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let tags = row
        .get("Tags")
        .and_then(Value::as_array)
        .map(|values| extract_aws_tags(values))
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
        instance_id,
        name,
        region: region.to_owned(),
        state,
        instance_type,
        launch_time,
        public_ip,
        public_dns,
    })
}

fn extract_aws_tags(values: &[Value]) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    for value in values {
        if let (Some(key), Some(val)) = (
            value.get("Key").and_then(Value::as_str),
            value.get("Value").and_then(Value::as_str),
        ) {
            tags.insert(key.to_owned(), val.to_owned());
        }
    }
    tags
}

fn resolve_aws_instance(config: &IceConfig, identifier: &str) -> Result<AwsInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }
    let needle = identifier.to_ascii_lowercase();
    let cache = load_cloud_cache_or_default::<AwsInstanceCache>(Cloud::Aws);

    if needle.starts_with("i-") {
        if let Some(entry) = cache
            .entries
            .iter()
            .find(|entry| entry.instance_id.eq_ignore_ascii_case(identifier))
            && let Ok(instance) = aws_describe_instance(config, &entry.instance_id, &entry.region)
        {
            return Ok(instance);
        }
    }

    let named_cache = cache
        .entries
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
    if let PrefixLookup::Unique(index) = prefix_lookup_indices(&named_cache, identifier, |entry| {
        entry.name.as_deref().unwrap_or("")
    })? {
        let entry = &named_cache[index];
        if let Ok(instance) = aws_describe_instance(config, &entry.instance_id, &entry.region) {
            return Ok(instance);
        }
    }

    let instances = aws_list_instances(config)?;
    persist_aws_instance_cache(&instances);
    if needle.starts_with("i-") {
        if let Some(instance) = instances
            .into_iter()
            .find(|instance| instance.instance_id.eq_ignore_ascii_case(identifier))
        {
            return Ok(instance);
        }
        bail!("No AWS instance found with ID `{identifier}`.");
    }
    resolve_aws_instance_from_list(instances, identifier)
}

fn resolve_aws_instance_from_list(
    instances: Vec<AwsInstance>,
    identifier: &str,
) -> Result<AwsInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.label_str())? {
        PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        PrefixLookup::Ambiguous(indices) => {
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
        PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn aws_describe_instance(config: &IceConfig, id: &str, region: &str) -> Result<AwsInstance> {
    let mut command = aws_command(config, region);
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
    let value = run_command_json(&mut command, "describe aws instance")?;
    parse_aws_instances(&value, region)?
        .into_iter()
        .find(|instance| instance.instance_id == id)
        .ok_or_else(|| anyhow!("No AWS instance found with ID `{id}` in region `{region}`."))
}

fn aws_set_instance_state(config: &IceConfig, instance: &AwsInstance, running: bool) -> Result<()> {
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
    let mut command = aws_command(config, &instance.region);
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

fn wait_for_aws_instance_state(
    config: &IceConfig,
    id: &str,
    region: &str,
    desired: &str,
    timeout: Duration,
) -> Result<AwsInstance> {
    let start = SystemTime::now();
    loop {
        if elapsed_since(start)? > timeout {
            bail!("Timed out waiting for aws instance {id} to reach `{desired}`");
        }
        let instance = aws_describe_instance(config, id, region)?;
        if instance.state.eq_ignore_ascii_case(desired) {
            return Ok(instance);
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

fn aws_ssh_user(config: &IceConfig) -> String {
    config
        .default
        .aws
        .ssh_user
        .clone()
        .unwrap_or_else(|| "ec2-user".to_owned())
}

fn aws_ssh_key_path(config: &IceConfig) -> Result<PathBuf> {
    let Some(path) = config.default.aws.ssh_key_path.as_deref() else {
        bail!(
            "Missing `default.aws.ssh_key_path`. Set it with e.g. `ice config set default.aws.ssh_key_path=/path/to/key.pem`."
        );
    };
    Ok(PathBuf::from(path))
}

fn aws_ssh_host(instance: &AwsInstance) -> Result<String> {
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

fn aws_open_shell(config: &IceConfig, instance: &AwsInstance) -> Result<()> {
    let key_path = aws_ssh_key_path(config)?;
    let user = aws_ssh_user(config);
    let host = aws_ssh_host(instance)?;
    let remote_command = container_or_host_shell_remote_command();
    let mut command = Command::new("ssh");
    command
        .arg("-i")
        .arg(key_path)
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg("-t")
        .arg(format!("{user}@{host}"))
        .arg(remote_command);
    run_command_status(&mut command, "open aws shell")
}

fn aws_download(
    config: &IceConfig,
    instance: &AwsInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let key_path = aws_ssh_key_path(config)?;
    let user = aws_ssh_user(config);
    let host = aws_ssh_host(instance)?;
    let destination = local_path
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let ssh_transport = format!(
        "ssh -i {} -o StrictHostKeyChecking=accept-new",
        key_path.display()
    );
    let remote_spec = format!("{user}@{host}:{remote_path}");
    let mut command = Command::new("rsync");
    command
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport)
        .arg(remote_spec)
        .arg(destination);
    run_command_status(&mut command, "download from aws instance")
}

fn aws_upload_file(
    config: &IceConfig,
    instance: &AwsInstance,
    local_path: &Path,
    remote_path: &str,
) -> Result<()> {
    let key_path = aws_ssh_key_path(config)?;
    let user = aws_ssh_user(config);
    let host = aws_ssh_host(instance)?;
    let ssh_transport = format!(
        "ssh -i {} -o StrictHostKeyChecking=accept-new",
        key_path.display()
    );
    let remote_spec = format!("{user}@{host}:{remote_path}");
    let mut command = Command::new("rsync");
    command
        .arg("-az")
        .arg("--progress")
        .arg("-e")
        .arg(ssh_transport)
        .arg(local_path)
        .arg(remote_spec);
    run_command_status(&mut command, "upload file to aws instance")
}

fn aws_run_remote_command(
    config: &IceConfig,
    instance: &AwsInstance,
    command_str: &str,
) -> Result<()> {
    let key_path = aws_ssh_key_path(config)?;
    let user = aws_ssh_user(config);
    let host = aws_ssh_host(instance)?;
    let mut command = Command::new("ssh");
    command
        .arg("-i")
        .arg(key_path)
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg(format!("{user}@{host}"))
        .arg(command_str);
    run_command_status(&mut command, "run command on aws instance")
}

fn aws_terminate_instance(config: &IceConfig, instance: &AwsInstance) -> Result<()> {
    let mut command = aws_command(config, &instance.region);
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

fn aws_lookup_default_ami(config: &IceConfig, region: &str) -> Result<String> {
    let mut command = aws_command(config, region);
    command.args([
        "ssm",
        "get-parameter",
        "--name",
        "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
        "--query",
        "Parameter.Value",
        "--output",
        "text",
        "--region",
        region,
    ]);
    run_command_text(&mut command, "lookup default aws ami")
}

fn aws_create_instance(
    config: &IceConfig,
    candidate: &CloudMachineCandidate,
    hours: f64,
) -> Result<AwsInstance> {
    let region = candidate.region.clone();
    let ami = if let Some(ami) = config.default.aws.ami.as_deref()
        && !ami.trim().is_empty()
    {
        ami.trim().to_owned()
    } else {
        aws_lookup_default_ami(config, &region)?
    };

    let existing_names = aws_list_instances(config)?
        .into_iter()
        .map(|instance| visible_instance_name(instance.label_str()).to_owned())
        .filter(|name| !name.is_empty())
        .collect::<HashSet<_>>();
    let name = build_cloud_instance_name(&existing_names)?;

    let auto_stop_script = format!(
        "#!/bin/bash\nnohup bash -lc 'sleep {}; shutdown -h now' >/var/log/ice-autostop.log 2>&1 &\n",
        required_runtime_seconds(hours)
    );
    let script_path = write_temp_file("ice-aws-autostop", ".sh", &auto_stop_script)?;

    let mut command = aws_command(config, &region);
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
        &format!(
            "ResourceType=instance,Tags=[{{Key=Name,Value={name}}},{{Key=ice-managed,Value=true}},{{Key=ice-created-by,Value=ice}}]"
        ),
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
            "[{{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{{\"VolumeSize\":{},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}}}]",
            size
        ));
    }

    let spinner = spinner("Creating aws instance...");
    let value = run_command_json(&mut command, "create aws instance")?;
    spinner.finish_with_message("Creation requested.");
    let _ = fs::remove_file(script_path);
    let instance_id = value
        .get("Instances")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("InstanceId"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("AWS create response missing instance ID"))?
        .to_owned();

    let instance = wait_for_aws_instance_state(
        config,
        &instance_id,
        &region,
        "running",
        Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
    )?;
    Ok(instance)
}

fn apply_setup_plan_aws(
    config: &IceConfig,
    instance: &AwsInstance,
    plan: &SetupPlan,
) -> Result<()> {
    if let Some(container_archive_path) = plan.container_archive_path.as_deref() {
        setup_instance_container_aws(config, instance, container_archive_path)?;
    } else {
        println!("No container archive provided; skipping container upload.");
    }
    Ok(())
}

fn write_temp_file(prefix: &str, suffix: &str, contents: &str) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "{prefix}-{}-{}{}",
        now_unix_secs(),
        std::process::id(),
        suffix
    ));
    fs::write(&path, contents)
        .with_context(|| format!("Failed to write temporary file: {}", path.display()))?;
    Ok(path)
}

fn build_cloud_instance_name(existing_names: &HashSet<String>) -> Result<String> {
    let name = generate_unique_verb_noun_name(existing_names)?;
    Ok(format!("{ICE_LABEL_PREFIX}{name}"))
}

fn generate_unique_verb_noun_name(existing_names: &HashSet<String>) -> Result<String> {
    let adjectives = extended_namegen_adjectives();
    let nouns = extended_namegen_nouns();
    if adjectives.is_empty() || nouns.is_empty() {
        bail!("Name generator has no words configured.");
    }

    let taken = existing_names
        .iter()
        .map(|value| normalize_instance_name_for_match(value))
        .collect::<HashSet<_>>();

    let plain_total = adjectives.len().saturating_mul(nouns.len());
    let plain_retry_budget = plain_total.min(RANDOM_NAME_COLLISION_RETRIES).max(1);
    let mut plain_generator = Generator::new(adjectives, nouns, Name::Plain);
    let mut seen_plain = HashSet::with_capacity(plain_retry_budget);
    for _ in 0..plain_retry_budget {
        let candidate = plain_generator
            .next()
            .ok_or_else(|| anyhow!("Name generator exhausted while generating plain names."))?;
        let key = normalize_instance_name_for_match(&candidate);
        if !seen_plain.insert(key.clone()) {
            continue;
        }
        if !taken.contains(&key) {
            return Ok(candidate);
        }
    }

    for adjective in adjectives {
        for noun in nouns {
            let candidate = format!("{adjective}-{noun}");
            let key = normalize_instance_name_for_match(&candidate);
            if !taken.contains(&key) {
                return Ok(candidate);
            }
        }
    }

    let numbered_total = plain_total.saturating_mul(NUMBERED_NAME_SUFFIX_MAX as usize);
    let numbered_retry_budget = numbered_total.min(NUMBERED_NAME_COLLISION_RETRIES).max(1);
    let mut numbered_generator = Generator::new(adjectives, nouns, Name::Numbered);
    let mut seen_numbered = HashSet::with_capacity(numbered_retry_budget);
    for _ in 0..numbered_retry_budget {
        let candidate = numbered_generator
            .next()
            .ok_or_else(|| anyhow!("Name generator exhausted while generating numbered names."))?;
        let key = normalize_instance_name_for_match(&candidate);
        if !seen_numbered.insert(key.clone()) {
            continue;
        }
        if !taken.contains(&key) {
            return Ok(candidate);
        }
    }

    for adjective in adjectives {
        for noun in nouns {
            for suffix in 1..=NUMBERED_NAME_SUFFIX_MAX {
                let candidate = format!("{adjective}-{noun}-{suffix:04}");
                let key = normalize_instance_name_for_match(&candidate);
                if !taken.contains(&key) {
                    return Ok(candidate);
                }
            }
        }
    }

    bail!("Could not generate a unique instance name (all adjective-noun combinations are taken).")
}

fn extended_namegen_adjectives() -> &'static [&'static str] {
    static ADJECTIVES: LazyLock<Vec<&'static str>> =
        LazyLock::new(|| merge_unique_words(NAMEGEN_ADJECTIVES, NAMES_ADJECTIVES));
    ADJECTIVES.as_slice()
}

fn extended_namegen_nouns() -> &'static [&'static str] {
    static NOUNS: LazyLock<Vec<&'static str>> =
        LazyLock::new(|| merge_unique_words(NAMEGEN_NOUNS, NAMES_NOUNS));
    NOUNS.as_slice()
}

fn merge_unique_words(primary: &[&'static str], extra: &[&'static str]) -> Vec<&'static str> {
    let mut merged = Vec::with_capacity(primary.len().saturating_add(extra.len()));
    let mut seen = HashSet::with_capacity(primary.len().saturating_add(extra.len()));
    for word in primary.iter().chain(extra.iter()) {
        if seen.insert(*word) {
            merged.push(*word);
        }
    }
    merged
}

fn elapsed_hours_from_rfc3339(ts: &str) -> Option<f64> {
    let parsed = DateTime::parse_from_rfc3339(ts).ok()?;
    let parsed_utc = parsed.with_timezone(&Utc);
    let elapsed = Utc::now().signed_duration_since(parsed_utc);
    Some(elapsed.num_seconds().max(0) as f64 / 3600.0)
}

fn parse_json_response(response: Response, context: &str) -> Result<Value> {
    let status = response.status();
    let text = response
        .text()
        .with_context(|| format!("Failed to read response body while trying to {context}"))?;

    if !status.is_success() {
        let message = extract_api_error_message(&text);
        bail!("Failed to {context}: HTTP {} {}", status.as_u16(), message);
    }

    let value = serde_json::from_str::<Value>(&text).with_context(|| {
        format!(
            "Failed to parse JSON response while trying to {context}. Body: {}",
            truncate_ellipsis(&text, 300)
        )
    })?;

    Ok(value)
}

fn extract_api_error_message(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        if let Some(message) = value
            .get("msg")
            .and_then(Value::as_str)
            .or_else(|| value.get("detail").and_then(Value::as_str))
            .or_else(|| value.get("error").and_then(Value::as_str))
        {
            return message.to_owned();
        }
    }

    let trimmed = body.trim();
    if trimmed.is_empty() {
        "empty response body".to_owned()
    } else {
        truncate_ellipsis(trimmed, 280)
    }
}

fn spinner(message: &str) -> ProgressBar {
    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    let style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    progress.set_style(style);
    progress.enable_steady_tick(Duration::from_millis(90));
    progress.set_message(message.to_owned());
    progress
}

fn require_interactive(message: &str) -> Result<()> {
    if !io::stdin().is_terminal() {
        bail!("{message}");
    }
    Ok(())
}

fn maybe_open_browser(url: &str) {
    if let Err(err) = webbrowser::open(url) {
        eprintln!("Could not open browser automatically: {err}");
        eprintln!("Open this URL manually: {url}");
    }
}

fn nonempty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn now_unix_secs() -> u64 {
    now_unix_secs_f64() as u64
}

fn now_unix_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs_f64()
}

fn format_unix_utc(unix_ts: u64) -> String {
    if let Some(dt) = DateTime::<Utc>::from_timestamp(unix_ts as i64, 0) {
        dt.format("%Y-%m-%d %H:%M UTC").to_string()
    } else {
        format!("{unix_ts} (unix)")
    }
}

fn elapsed_since(start: SystemTime) -> Result<Duration> {
    SystemTime::now()
        .duration_since(start)
        .context("System clock moved backwards")
}

fn truncate_ellipsis(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }

    if max_chars <= 1 {
        return "…".to_owned();
    }

    let keep = max_chars.saturating_sub(1);
    let mut output = value.chars().take(keep).collect::<String>();
    output.push('…');
    output
}

fn shell_quote_single(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn print_big_red_error(message: &str) {
    if io::stderr().is_terminal() {
        eprintln!(
            "{} ERROR {} {}{}{}",
            ANSI_BOLD_WHITE_RED_BG, ANSI_RESET, ANSI_BOLD_RED, message, ANSI_RESET
        );
    } else {
        eprintln!("ERROR: {message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vast_instance(end_date: Option<f64>) -> VastInstance {
        VastInstance {
            id: 42,
            label: Some("ice-test".to_owned()),
            cur_state: Some("running".to_owned()),
            next_state: None,
            intended_status: None,
            actual_status: None,
            status_msg: None,
            start_date: None,
            uptime_mins: None,
            gpu_name: None,
            dph_total: None,
            end_date,
            ssh_host: None,
            ssh_port: None,
        }
    }

    #[test]
    fn gpu_fp32_ordering_sanity() {
        assert!(gpu_quality_score("Tesla T4") < gpu_quality_score("L4"));
        assert!(gpu_quality_score("L4") < gpu_quality_score("RTX 5090"));
        assert!(gpu_quality_score("Tesla T4") < gpu_quality_score("RTX 6000Ada"));
        assert!(gpu_quality_score("RTX 4090") < gpu_quality_score("RTX 5090"));
        assert!(gpu_quality_score("A100 SXM4") < gpu_quality_score("H100 SXM"));
        assert!(gpu_quality_score("H100 SXM") < gpu_quality_score("B200"));
    }

    #[test]
    fn prefix_lookup_accepts_prefix_without_internal_label_prefix() {
        let names = vec![
            "ice-fortuitous-dog".to_owned(),
            "ice-gentle-otter".to_owned(),
            "ice-misty-river".to_owned(),
        ];

        let lookup = prefix_lookup_indices(&names, "f", |name| name).expect("lookup should work");
        match lookup {
            PrefixLookup::Unique(index) => assert_eq!(index, 0),
            other => panic!("Expected unique match for `f`, got {other:?}"),
        }

        let lookup_prefixed =
            prefix_lookup_indices(&names, "ice-gent", |name| name).expect("lookup should work");
        match lookup_prefixed {
            PrefixLookup::Unique(index) => assert_eq!(index, 1),
            other => panic!("Expected unique match for `ice-gent`, got {other:?}"),
        }
    }

    #[test]
    fn verb_noun_name_generation_avoids_taken_names() {
        let mut taken = HashSet::new();
        taken.insert("fortuitous-dog".to_owned());
        taken.insert("gentle-otter".to_owned());

        let generated =
            generate_unique_verb_noun_name(&taken).expect("should generate a fresh name");
        let key = normalize_instance_name_for_match(&generated);
        assert!(!taken.contains(&key));
        assert!(generated.contains('-'));
    }

    #[test]
    fn vast_gpu_catalog_includes_a100_and_h100_family() {
        let options = load_gpu_options(Cloud::VastAi, None);
        assert!(options.iter().any(|gpu| gpu.contains("A100")));
        assert!(options.iter().any(|gpu| gpu.contains("H100")));
    }

    #[test]
    fn gpu_selector_label_includes_vram_when_known() {
        let label = gpu_selector_label("RTX 5090");
        assert!(label.contains("RTX 5090"));
        assert!(label.contains("(32 GB)"));
    }

    #[test]
    fn vast_cost_estimate_uses_requested_runtime() {
        let cost = estimate_runtime_cost(Cloud::VastAi, 0.5422, 0.1).expect("cost should compute");
        assert!((cost.billed_hours - 0.1).abs() < 1e-9);
        assert!((cost.total_usd - 0.05422).abs() < 1e-9);
    }

    #[test]
    fn vast_autostop_plan_rounds_up_to_hour_boundary() {
        let start_unix = 1_700_000_000u64;
        let requested_hours = 0.1;
        let plan = build_vast_autostop_plan(start_unix, requested_hours).expect("plan");
        assert_eq!(plan.stop_at_unix % 3600, 0);
        assert!(plan.stop_at_unix >= start_unix + required_runtime_seconds(requested_hours));
        assert!(plan.runtime_hours >= requested_hours);
        assert_eq!(plan.schedule_end_unix, plan.stop_at_unix + 60);
    }

    #[test]
    fn vast_autostop_cost_estimate_respects_plan_runtime() {
        let base =
            estimate_runtime_cost(Cloud::VastAi, 0.5, 0.25).expect("base estimate should work");
        let adjusted = apply_vast_autostop_cost_estimate(base).expect("adjusted estimate");
        assert!(adjusted.billed_hours >= adjusted.requested_hours);
        assert!((adjusted.total_usd - (adjusted.hourly_usd * adjusted.billed_hours)).abs() < 1e-9);
    }

    #[test]
    fn gcp_cost_estimate_rounds_to_second_granularity() {
        let requested = 0.10001;
        let cost = estimate_runtime_cost(Cloud::Gcp, 1.0, requested).expect("cost should compute");
        let expected_billed = required_runtime_seconds(requested) as f64 / 3600.0;
        assert!((cost.billed_hours - expected_billed).abs() < 1e-9);
        assert!(cost.billed_hours >= requested);
    }

    #[test]
    fn vast_ssh_args_place_identity_options_before_destination() {
        let args = vast_ssh_args("ssh1.vast.ai", 10135, Some(Path::new("/tmp/id_ed25519")));
        let host_index = args
            .iter()
            .position(|value| value == "root@ssh1.vast.ai")
            .expect("host arg");
        let identity_index = args
            .iter()
            .position(|value| value == "-i")
            .expect("identity flag");
        assert!(identity_index < host_index);
        assert!(args.iter().any(|value| value == "IdentitiesOnly=yes"));
    }

    #[test]
    fn vast_ssh_args_without_identity_keep_destination_last() {
        let args = vast_ssh_args("ssh1.vast.ai", 10135, None);
        assert_eq!(
            args.last().expect("destination argument"),
            "root@ssh1.vast.ai"
        );
        assert!(!args.iter().any(|value| value == "-i"));
        assert!(!args.iter().any(|value| value == "IdentitiesOnly=yes"));
    }

    #[test]
    fn vast_job_termination_unix_recognizes_stop_and_delete_actions() {
        let stop_job = VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("PUT".to_owned()),
            request_body: Some(json!({"state":"stopped"})),
            start_time: Some(1_700_000_000.0),
        };
        let delete_job = VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("DELETE".to_owned()),
            request_body: None,
            start_time: Some(1_700_000_100.0),
        };
        let irrelevant_job = VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("PUT".to_owned()),
            request_body: Some(json!({"state":"running"})),
            start_time: Some(1_700_000_200.0),
        };

        assert_eq!(vast_job_termination_unix(&stop_job), Some(1_700_000_000.0));
        assert_eq!(
            vast_job_termination_unix(&delete_job),
            Some(1_700_000_100.0)
        );
        assert_eq!(vast_job_termination_unix(&irrelevant_job), None);
    }

    #[test]
    fn nearest_vast_scheduled_termination_picks_earliest_future_job() {
        let now = now_unix_secs_f64();
        let jobs = vec![
            VastScheduledJob {
                instance_id: Some(42),
                api_endpoint: Some("/api/v0/instances/42/".to_owned()),
                request_method: Some("PUT".to_owned()),
                request_body: Some(json!({"state":"stopped"})),
                start_time: Some(now + 7_200.0),
            },
            VastScheduledJob {
                instance_id: Some(42),
                api_endpoint: Some("/api/v0/instances/42/".to_owned()),
                request_method: Some("DELETE".to_owned()),
                request_body: None,
                start_time: Some(now + 3_600.0),
            },
            VastScheduledJob {
                instance_id: Some(42),
                api_endpoint: Some("/api/v0/instances/42/".to_owned()),
                request_method: Some("DELETE".to_owned()),
                request_body: None,
                start_time: Some(now - 60.0),
            },
        ];

        let nearest = nearest_vast_scheduled_termination_by_instance(&jobs);
        let value = nearest.get(&42).copied().expect("nearest time");
        assert!(value >= now + 3_599.0);
        assert!(value <= now + 3_601.0);
    }

    #[test]
    fn remaining_contract_hours_at_prefers_scheduled_termination_when_sooner() {
        let now = 1_700_000_000.0;
        let instance = test_vast_instance(Some(now + 10.0 * 3600.0));
        let remaining = remaining_contract_hours_at(&instance, Some(now + 2.0 * 3600.0), now);
        assert!((remaining - 2.0).abs() < 1e-9);
    }

    #[test]
    fn remaining_contract_hours_at_uses_scheduled_when_no_contract_end() {
        let now = 1_700_000_000.0;
        let instance = test_vast_instance(None);
        let remaining = remaining_contract_hours_at(&instance, Some(now + 1.5 * 3600.0), now);
        assert!((remaining - 1.5).abs() < 1e-9);
    }

    #[test]
    fn unset_config_value_clears_values() {
        let mut config = IceConfig::default();
        set_config_value(&mut config, "default.cloud", "aws").expect("set cloud");
        set_config_value(&mut config, "default.aws.region", "us-west-2").expect("set region");
        set_config_value(&mut config, "auth.aws.access_key_id", "AKIA_TEST").expect("set key");

        unset_config_value(&mut config, "default.cloud").expect("unset cloud");
        unset_config_value(&mut config, "default.aws.region").expect("unset region");
        unset_config_value(&mut config, "auth.aws.access_key_id").expect("unset key");

        assert_eq!(
            get_config_value(&config, "default.cloud").expect("cloud"),
            "<unset>"
        );
        assert_eq!(
            get_config_value(&config, "default.aws.region").expect("region"),
            "<unset>"
        );
        assert_eq!(
            get_config_value(&config, "auth.aws.access_key_id").expect("access key"),
            "<unset>"
        );
    }

    #[test]
    fn unset_config_value_accepts_legacy_alias_keys() {
        let mut config = IceConfig::default();
        set_config_value(&mut config, "default.vast_ai.min_cpus", "4").expect("set min_cpus");
        unset_config_value(&mut config, "default.min_cpus").expect("unset min_cpus alias");
        assert_eq!(
            get_config_value(&config, "default.vast_ai.min_cpus").expect("min_cpus"),
            "<unset>"
        );
    }
}
