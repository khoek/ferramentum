use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};

use crate::model::{Cloud, DeployTargetRequest};

#[derive(Debug, Parser)]
#[command(
    name = "ice",
    about = "Manage cloud VM instances and local workload containers.",
    infer_subcommands = true,
    after_help = "Examples:\n  ice deploy test-crate\n  ice deploy --arca test-crate --hours 0.25\n  ice deploy --unpack arca:test-crate --cloud vast.ai\n  ice deploy --container us-central1-docker.pkg.dev/my-project/arca/my-image:tag --cloud vast.ai\n  ice deploy --ssh --cloud gcp --machine g2-standard-4"
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
        name = "deploy",
        about = "Deploy a workload onto the cheapest matching instance, or a managed local container."
    )]
    Deploy(DeployArgs),
}

#[derive(Debug, Args)]
pub(crate) struct CloudArgs {
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
pub(crate) struct DeployArgs {
    /// Target cloud. Defaults to `default.cloud`.
    #[arg(long, value_enum)]
    pub(crate) cloud: Option<Cloud>,
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

impl DeployArgs {
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
