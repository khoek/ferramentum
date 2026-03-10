use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::model::{Cloud, DeployTargetRequest};

#[derive(Debug, Parser)]
#[command(
    name = "ice",
    about = "Manage cloud VM instances and local workload containers.",
    infer_subcommands = true,
    after_help = "Examples:\n  ice create test-crate\n  ice create --arca test-crate --hours 0.25\n  ice create --unpack arca:test-crate --cloud vast.ai\n  ice create --container us-central1-docker.pkg.dev/my-project/arca/my-image:tag --cloud vast.ai\n  ice create --ssh --cloud gcp --machine g2-standard-4"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
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
        name = "dl",
        about = "Download file/dir from an instance or managed local container."
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
        about = "Create the cheapest matching instance for a workload, or a managed local container."
    )]
    Create(CreateArgs),

    #[command(
        name = "refresh-catalog",
        about = "Refresh a locally cached machine/pricing catalog for a cloud provider."
    )]
    RefreshCatalog(RefreshCatalogArgs),
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
    #[arg(long)]
    pub(crate) follow: bool,
}

#[derive(Debug, Args)]
pub(crate) struct LoginArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
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
    #[arg(long)]
    pub(crate) print_creds: bool,
    #[arg(long)]
    pub(crate) preserve_ephemeral: bool,
    pub(crate) instance: String,
}

#[derive(Debug, Args)]
pub(crate) struct DownloadArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
    pub(crate) remote_path: String,
    pub(crate) local_path: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct InstanceArgs {
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
    pub(crate) instance: String,
}

#[derive(Debug, Args)]
pub(crate) struct CreateArgs {
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
    /// Clear any default GPU filter.
    #[arg(long, conflicts_with = "gpus")]
    pub(crate) no_gpu: bool,
    /// Override the maximum hourly price filter in USD/hr.
    #[arg(long, value_name = "USD")]
    pub(crate) max_price_per_hr: Option<f64>,
    /// Runtime duration in hours. Defaults to `default.runtime_hours`, then `1.0`.
    #[arg(long, value_name = "HOURS")]
    pub(crate) hours: Option<f64>,
    /// Pin a cloud-specific machine type on marketplace-backed clouds.
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
