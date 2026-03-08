use std::path::PathBuf;

use clap::{ArgAction, ArgGroup, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "arca",
    about = "Build and publish containerized crate artifacts.",
    after_help = "Examples:\n  arca login --repo us-west1-docker.pkg.dev/my-project/arca/my-image\n  arca build rust ./my-crate --profile dev --features '' --base-image nvidia/cuda:12.8.1-runtime-ubuntu24.04 --set-default\n  arca build rust ./my-crate\n  arca push\n  arca push deadbeef\n  arca list\n  arca prune local --days 7\n  arca prune remote --days 7"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        name = "login",
        about = "Detect GCP credentials and configure Google registry publishing."
    )]
    Login(LoginArgs),

    #[command(name = "build", about = "Build a managed runnable container artifact.")]
    #[command(subcommand)]
    Build(BuildCommands),

    #[command(
        name = "push",
        about = "Push local artifacts to the configured Google registry."
    )]
    Push(PushArgs),

    #[command(
        name = "list",
        about = "List all arca-tracked local and remote artifacts in a compact view."
    )]
    List,

    #[command(
        name = "prune",
        about = "Delete arca-tracked local, remote, or all artifacts older than a given age."
    )]
    Prune(PruneArgs),
}

#[derive(Debug, Subcommand)]
pub enum BuildCommands {
    #[command(
        name = "rust",
        about = "Build a runnable container for the Rust crate at PATH."
    )]
    Rust(RustArgs),
}

#[derive(Debug, Args, Clone)]
pub struct RustArgs {
    /// Path to a Rust crate directory or Cargo.toml manifest.
    #[arg(value_name = "PATH")]
    pub path: PathBuf,

    /// Cargo profile passed through to `cargo build --profile`. If omitted, `arca` uses the saved crate-local default.
    #[arg(long = "profile", value_name = "PROFILE")]
    pub profile: Option<String>,

    /// Cargo features passed through to `cargo build --features`. Use `--features ''` for an empty saved feature set.
    #[arg(
        short = 'F',
        long = "features",
        value_name = "FEATURE",
        value_delimiter = ',',
        action = ArgAction::Append
    )]
    pub features: Option<Vec<String>>,

    /// Binary target to package when the crate exposes more than one.
    #[arg(long = "bin", value_name = "NAME")]
    pub bin: Option<String>,

    /// Runtime base image for the generated container. If omitted, `arca` uses the saved crate-local default.
    #[arg(long = "base-image", value_name = "IMAGE")]
    pub base_image: Option<String>,

    /// Save the resolved build settings into `PATH/.arca/config.toml` for future invocations.
    #[arg(short = 'u', long = "set-default")]
    pub save_defaults: bool,

    /// Build with local host cargo instead of the default cached builder container.
    #[arg(long = "host-build")]
    pub host_build: bool,
}

#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Ignore cached `~/.arca/config.toml` values when refreshing auth settings.
    #[arg(long = "force")]
    pub force: bool,

    /// Registry image prefix, for example `us-west1-docker.pkg.dev/my-project/my-repo/my-image`.
    #[arg(long = "repo", value_name = "REGISTRY_REPO")]
    pub repo: Option<String>,
}

#[derive(Debug, Args)]
pub struct PushArgs {
    /// Artifact ID prefix or crate name. Defaults to all local artifacts not currently present remotely.
    #[arg(value_name = "ARTIFACT")]
    pub artifact: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PruneTarget {
    All,
    Local,
    Remote,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("age")
        .args(["hours", "days"])
        .required(true)
        .multiple(false)
))]
pub struct PruneArgs {
    /// Where to delete old artifacts from.
    #[arg(value_enum, value_name = "TARGET", default_value = "all")]
    pub target: PruneTarget,

    /// Delete artifacts older than this many hours.
    #[arg(long = "hours", value_name = "HOURS")]
    pub hours: Option<u64>,

    /// Delete artifacts older than this many days.
    #[arg(long = "days", value_name = "DAYS")]
    pub days: Option<u64>,
}
