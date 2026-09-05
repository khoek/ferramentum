mod codex;
mod credential_provider;
mod llm_get;
mod terminal;

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

const GIT_TERMINAL_PROMPT_ENV: &str = "GIT_TERMINAL_PROMPT";

#[derive(Debug, Parser)]
#[command(
    name = "kai",
    version,
    about = "Launch and resume Codex.",
    infer_subcommands = true
)]
struct Cli {
    /// Bash script that supplies credentials to compatible +k Codex builds.
    #[arg(long, value_name = "SCRIPT", global = true)]
    credential_provider: Option<PathBuf>,

    /// Start Codex using the Fast service tier.
    #[arg(long, global = true)]
    fast: bool,

    /// Disable automatic credential rotation and conversation recovery.
    #[arg(long, global = true)]
    no_auto_restart: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Resume a conversation, or open the all-sessions picker.
    Resume {
        #[arg(value_name = "ID")]
        id: Option<String>,
    },
    /// Concatenate files into a listing for LLM consumption.
    LlmGet(llm_get::LlmGetArgs),
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => code,
        Err(error) => {
            let message = format!("{error:#}");
            if terminal::write_stderr_line(&message).is_err() {
                eprintln!("{message}");
            }
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode> {
    let args = match cli.command {
        Some(Commands::LlmGet(args)) => {
            if cli.fast || cli.no_auto_restart || cli.credential_provider.is_some() {
                bail!("Codex launch options cannot be used with llm-get");
            }
            llm_get::run(args)?;
            return Ok(ExitCode::SUCCESS);
        }
        Some(Commands::Resume { id }) => vec![
            OsString::from("resume"),
            id.map_or_else(|| OsString::from("--all"), OsString::from),
            codex::APPROVAL_BYPASS_FLAG.into(),
        ],
        None => vec![codex::APPROVAL_BYPASS_FLAG.into()],
    };
    let cwd = std::env::current_dir().context("could not read the current directory")?;
    let launcher = codex::Launcher::detect()?;
    let service_tier = if cli.fast {
        codex::ServiceTier::Fast
    } else {
        codex::ServiceTier::Default
    };
    if !launcher.supervision_enabled(cli.no_auto_restart) {
        return Ok(ExitCode::from(launcher.run_direct(
            args,
            &cwd,
            service_tier,
        )?));
    }
    let provider =
        credential_provider::CredentialProvider::load(cli.credential_provider.as_deref())?;
    let environment = credential_provider::CodexEnvironment::from_env(&cwd)?;
    Ok(ExitCode::from(launcher.run_supervised(
        args,
        &cwd,
        service_tier,
        codex::SupervisedEnvironment::new(&environment.codex_home, &environment.sqlite_home),
        |previous| {
            provider
                .as_ref()
                .map(|provider| provider.select(&environment, previous))
                .transpose()
        },
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_accepts_unambiguous_prefixes() {
        for name in ["r", "res", "resume"] {
            assert!(matches!(
                Cli::try_parse_from(["kai", name]).unwrap().command,
                Some(Commands::Resume { id: None }),
            ));
            assert!(matches!(
                Cli::try_parse_from(["kai", name, "session"]).unwrap().command,
                Some(Commands::Resume { id: Some(id) }) if id == "session",
            ));
        }
    }

    #[test]
    fn default_launch_and_global_options_parse() {
        assert!(Cli::try_parse_from(["kai"]).unwrap().command.is_none());
        let cli = Cli::try_parse_from([
            "kai",
            "resume",
            "--fast",
            "--no-auto-restart",
            "--credential-provider",
            "/provider",
        ])
        .unwrap();
        assert!(cli.fast && cli.no_auto_restart);
        assert_eq!(cli.credential_provider, Some(PathBuf::from("/provider")));
    }

    #[test]
    fn obsolete_commands_and_options_are_rejected() {
        for name in [
            "agent", "a", "ar", "worktree", "wc", "wa", "wo", "wd", "init", "bump", "cred", "next",
            "lg",
        ] {
            assert!(Cli::try_parse_from(["kai", name]).is_err(), "{name}");
        }
        for flag in [
            "--model",
            "--resume",
            "--resume-all",
            "--quota-auto-restart",
            "--credential-provider-script",
            "--credential-provider-unique",
        ] {
            assert!(Cli::try_parse_from(["kai", flag]).is_err(), "{flag}");
        }
        for flag in ["-o", "-S"] {
            assert!(
                Cli::try_parse_from(["kai", "llm-get", flag, "source.rs"]).is_err(),
                "{flag}"
            );
        }
    }
}
