use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::model::{Cloud, DeployTargetRequest};

#[derive(Debug, Parser)]
#[command(
    name = "ice",
    version = crate::version::display(),
    about = "Manage cloud VM instances and local workload containers.",
    infer_subcommands = true,
    after_help = "Examples:\n  ice create test-crate\n  ice create --arca test-crate --hours 0.25\n  ice create --unpack arca:test-crate --cloud vast.ai\n  ice create --container us-central1-docker.pkg.dev/my-project/arca/my-image:tag --cloud vast.ai\n  ice create --ssh --cloud gcp --machine g2-standard-4"
)]
pub(crate) struct Cli {
    /// Never prompt or open a browser, including when stdin is a terminal.
    #[arg(long, global = true)]
    pub(crate) non_interactive: bool,
    /// Write versioned JSON results (JSON Lines for logs). Shell requires --print-creds.
    #[arg(long, global = true)]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
    /// Report the installed build and JSON schema, without configuration or provider access.
    Version,

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
        name = "logs",
        about = "Show stdout/stderr logs for an instance workload."
    )]
    Logs(LogsArgs),

    #[command(name = "shell", about = "Open shell into an instance workload.")]
    Shell(ShellArgs),

    #[command(
        name = "pull",
        about = "Download file/dir from an instance or managed local container."
    )]
    Pull(PullArgs),

    #[command(
        name = "push",
        about = "Upload file/dir to an instance or managed local container."
    )]
    Push(PushArgs),

    #[command(name = "stop", about = "Stop an instance.")]
    Stop(InstanceArgs),

    #[command(name = "start", about = "Start an instance.")]
    Start(InstanceArgs),

    #[command(name = "delete", about = "Delete an instance and verify its removal.")]
    Delete(InstanceArgs),

    #[command(
        name = "create",
        about = "Create the cheapest matching instance for a workload, or a managed local container."
    )]
    Create(Box<CreateArgs>),

    #[command(
        name = "refresh-catalog",
        about = "Refresh a locally cached machine/pricing catalog for a cloud provider."
    )]
    RefreshCatalog(RefreshCatalogArgs),

    /// Discover live machine types, images, on-demand availability and storage prices (Verda).
    Catalog(CloudArgs),
}

#[derive(Debug, Args)]
pub(crate) struct CloudArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
}

#[derive(Debug, Args)]
pub(crate) struct RefreshCatalogArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
}

#[derive(Debug, Args)]
pub(crate) struct LogsArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
    #[arg(long, default_value_t = 200)]
    pub(crate) tail: u32,
    #[arg(long)]
    pub(crate) filter: Option<String>,
    #[arg(long)]
    pub(crate) daemon: bool,
    /// Vast only: fetch provider container logs, including for unpack workloads.
    #[arg(long)]
    pub(crate) provider_logs: bool,
    #[arg(long)]
    pub(crate) follow: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LoginArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    /// Refresh login; Vast/Verda prompt again unless environment credentials are set.
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ConfigArgs {
    #[command(subcommand)]
    pub(crate) command: ConfigCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommands {
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
pub(crate) struct ConfigListArgs {}

#[derive(Debug, Args)]
pub(crate) struct ConfigGetArgs {
    pub(crate) key: String,
}

#[derive(Debug, Args)]
pub(crate) struct ConfigSetArgs {
    pub(crate) pair: String,
}

#[derive(Debug, Args)]
pub(crate) struct ConfigUnsetArgs {
    pub(crate) key: String,
}

#[derive(Debug, Args)]
pub(crate) struct ShellArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    /// Print a connection command after readiness checks and any instance-key recovery.
    #[arg(long)]
    pub(crate) print_creds: bool,
    /// Vast/Verda: print reported endpoints without readiness checks, SSH probes, or key changes.
    #[arg(long, requires = "print_creds", conflicts_with = "preserve_ephemeral")]
    pub(crate) no_probe: bool,
    /// Deprecated for Vast: recovery no longer creates temporary account keys.
    #[arg(long)]
    pub(crate) preserve_ephemeral: bool,
    pub(crate) instance: String,
}

#[derive(Debug, Args)]
pub(crate) struct PullArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
    pub(crate) remote_path: String,
    pub(crate) local_path: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PushArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
    pub(crate) local_path: PathBuf,
    pub(crate) remote_path: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct InstanceArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
    /// Total lifecycle command budget, including provider requests and state verification.
    #[arg(long, default_value="5m", value_parser=parse_duration)]
    pub(crate) timeout: u64,
}

#[derive(Debug, Args)]
pub(crate) struct CreateArgs {
    /// Verda only: acknowledge that --hours is an estimate; arrange and verify deletion yourself.
    #[arg(long)]
    pub(crate) manual_cleanup: bool,
    /// Verda only: select a provider OS image UUID/type from `ice catalog` (no local upload).
    #[arg(long)]
    pub(crate) image: Option<String>,
    /// Verda only: datacenter location code; otherwise choose cheapest available.
    #[arg(long)]
    pub(crate) location: Option<String>,
    /// Verda only: existing provider SSH key ID.
    #[arg(long)]
    pub(crate) ssh_key_id: Option<String>,
    /// Accept creation within the supplied filters and price ceiling.
    #[arg(long)]
    pub(crate) yes: bool,
    /// Ignore saved search filters/disk size; also image/location on Verda. Retains credentials and runtime.
    #[arg(long)]
    pub(crate) no_defaults: bool,
    /// Exact GPU count (positive counts: Vast/Verda). Zero requires CPU-only.
    #[arg(long, value_name = "COUNT")]
    pub(crate) gpu_count: Option<u32>,
    /// Minimum memory per GPU in GB (Vast/Verda).
    #[arg(long, value_name = "GB", alias = "min-vram-gb")]
    pub(crate) min_gpu_memory_gb: Option<f64>,
    /// Disk allocation in provider GB units (shown in the quote).
    #[arg(long, value_name = "GB")]
    pub(crate) disk_gb: Option<u32>,
    /// Minimum reported host internet download speed, in Mbps (Vast only).
    #[arg(long, value_name = "MBPS")]
    pub(crate) min_download_mbps: Option<f64>,
    /// Minimum reported host internet upload speed, in Mbps (Vast only).
    #[arg(long, value_name = "MBPS")]
    pub(crate) min_upload_mbps: Option<f64>,
    /// Maximum readiness wait, e.g. 900s or 30m. Does not extend auto-stop.
    #[arg(long, default_value = "15m", value_parser = parse_duration)]
    pub(crate) startup_timeout: u64,
    /// Target cloud. Defaults to `default.cloud`.
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    /// Override the minimum vCPU search filter.
    #[arg(long, value_name = "COUNT")]
    pub(crate) min_cpus: Option<u32>,
    /// Override the minimum RAM search filter in GB.
    #[arg(long, value_name = "GB")]
    pub(crate) min_ram_gb: Option<f64>,
    /// Override the allowed GPU filter. Repeat or separate with commas.
    #[arg(long = "gpu", value_name = "GPU", action = ArgAction::Append, value_delimiter = ',')]
    pub(crate) gpus: Vec<String>,
    /// Clear the saved GPU model filter. Use --gpu-count 0 to require CPU-only.
    #[arg(long, conflicts_with = "gpus")]
    pub(crate) no_gpu: bool,
    /// Maximum USD/hr: compute + allocated disk on Vast/Verda; compute on GCP/AWS. Excludes bandwidth/taxes.
    #[arg(long, value_name = "USD")]
    pub(crate) max_price_per_hr: Option<f64>,
    /// Runtime hours (Verda: cost estimate only, no enforced deadline). Defaults to saved value, then 1.
    #[arg(long, value_name = "HOURS")]
    pub(crate) hours: Option<f64>,
    /// Exact type on AWS/GCP/Verda; GPU-model selector on Vast (not a host or offer ID).
    #[arg(long)]
    pub(crate) machine: Option<String>,
    /// Prompt interactively for marketplace search filters.
    #[arg(long)]
    pub(crate) custom: bool,
    /// Resolve the deployment and chosen machine without creating anything.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Deploy a remote container image ref such as `LOCATION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG`.
    #[arg(long, value_name = "IMAGE_REF")]
    pub(crate) container: Option<String>,
    /// Unpack a workload from `arca:selector`, a local image name, a saved `image.tar`, or a full image ref.
    #[arg(long, value_name = "SOURCE")]
    pub(crate) unpack: Option<String>,
    /// Deploy a shell-only machine with no managed workload.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(crate) ssh: bool,
    /// Shorthand for `--unpack arca:ARTIFACT`. With no value, selects the newest local `arca` artifact.
    #[arg(long, value_name = "ARTIFACT", num_args = 0..=1, default_missing_value = "")]
    pub(crate) arca: Option<String>,
    #[arg(
        value_name = "TARGET",
        help = "Defaults to a local `arca` artifact selector."
    )]
    pub(crate) target: Option<String>,
}

fn parse_duration(value: &str) -> Result<u64, String> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix('m') {
        (value, 60)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1)
    } else {
        (value, 1)
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| *n > 0)
        .ok_or_else(|| "Use a positive duration such as 900s or 30m".to_owned())
}

impl Commands {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Login(_) => "login",
            Self::Config(args) => match args.command {
                ConfigCommands::List(_) => "config list",
                ConfigCommands::Get(_) => "config get",
                ConfigCommands::Set(_) => "config set",
                ConfigCommands::Unset(_) => "config unset",
            },
            Self::List(_) => "list",
            Self::Logs(_) => "logs",
            Self::Shell(_) => "shell",
            Self::Pull(_) => "pull",
            Self::Push(_) => "push",
            Self::Stop(_) => "stop",
            Self::Start(_) => "start",
            Self::Delete(_) => "delete",
            Self::Create(_) => "create",
            Self::RefreshCatalog(_) => "refresh-catalog",
            Self::Catalog(_) => "catalog",
        }
    }

    pub(crate) fn cloud(&self, config: &crate::model::IceConfig) -> Option<Cloud> {
        let requested = match self {
            Self::Config(_) | Self::Version => return None,
            Self::RefreshCatalog(args) => return args.cloud,
            Self::Login(args) => args.cloud,
            Self::List(args) | Self::Catalog(args) => args.cloud,
            Self::Logs(args) => args.cloud,
            Self::Shell(args) => args.cloud,
            Self::Pull(args) => args.cloud,
            Self::Push(args) => args.cloud,
            Self::Stop(args) | Self::Start(args) | Self::Delete(args) => args.cloud,
            Self::Create(args) => args.cloud,
        };
        requested.or(config.default.cloud)
    }
}

impl CreateArgs {
    pub(crate) fn target_request(&self) -> DeployTargetRequest {
        DeployTargetRequest {
            ssh: self.ssh,
            container: self.container.clone(),
            unpack: self.unpack.clone(),
            arca: self.arca.clone(),
            positional: self.target.clone(),
        }
    }
}
