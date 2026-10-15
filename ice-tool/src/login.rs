//! Shared credential-login policy; provider adapters supply validation and storage.
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use dialoguer::Password;
use serde_json::json;

use crate::model::{Cloud, IceConfig, LoginMethod, LoginOutcome};
use crate::support::{maybe_open_browser, prompt_theme, require_interactive};
use crate::ui::{print_notice, print_warning};

#[cfg(test)]
mod tests;

pub(crate) struct CredentialLogin<C> {
    pub(crate) cloud: Cloud,
    pub(crate) environment: Option<C>,
    pub(crate) cached: Option<C>,
    pub(crate) force: bool,
    pub(crate) interactive: bool,
}

impl<C> CredentialLogin<C> {
    pub(crate) fn run(
        self,
        validate: impl Fn(&C) -> Result<()>,
        prompt: impl FnOnce() -> Result<C>,
        save: impl FnOnce(C) -> Result<PathBuf>,
    ) -> Result<LoginOutcome> {
        // Explicit environment credentials always win, even with --force. Never
        // persist them or silently fall back to a different account on failure.
        if let Some(credentials) = self.environment {
            validate(&credentials)?;
            return Ok(LoginOutcome {
                method: LoginMethod::AutoDetected,
                saved_path: None,
            });
        }
        if !self.force
            && let Some(credentials) = self.cached
        {
            match validate(&credentials) {
                Ok(()) => {
                    return Ok(LoginOutcome {
                        method: LoginMethod::Cached,
                        saved_path: None,
                    });
                }
                Err(error) if !self.interactive => return Err(error),
                Err(_) => print_warning(&format!(
                    "Stored {} credentials could not be validated. Enter replacement credentials to try again.",
                    self.cloud,
                )),
            }
        }
        check_interactive(self.cloud, self.interactive)?;
        let credentials = prompt()?;
        validate(&credentials)?;
        Ok(LoginOutcome {
            method: LoginMethod::Prompted,
            saved_path: Some(save(credentials)?),
        })
    }
}

fn check_interactive(cloud: Cloud, interactive: bool) -> Result<()> {
    if !interactive {
        return Err(crate::automation::error(
            "authentication_required",
            format!("Supply valid {cloud} credentials before running this command."),
            json!({"cloud": cloud}),
        ));
    }
    Ok(())
}

/// Shared prompt guard for API credentials and provider-CLI login flows.
pub(crate) fn begin_prompt(cloud: Cloud, url: &str) -> Result<()> {
    check_interactive(cloud, !crate::automation::non_interactive())?;
    require_interactive(&format!(
        "`ice login --cloud {cloud}` requires interactive stdin."
    ))?;
    print_notice(&format!("Open {url} to obtain credentials for {cloud}."));
    maybe_open_browser(url);
    Ok(())
}

pub(crate) fn prompt_secret(label: &str) -> Result<String> {
    let value = Password::with_theme(prompt_theme())
        .with_prompt(label)
        .allow_empty_password(false)
        .interact()
        .with_context(|| format!("Failed to read {label}"))?;
    let value = value.trim();
    if value.is_empty() {
        bail!("{label} cannot be empty.");
    }
    Ok(value.to_owned())
}

/// Do not change the in-memory config unless its atomic save succeeds.
pub(crate) fn save_credentials(
    config: &mut IceConfig,
    update: impl FnOnce(&mut IceConfig),
) -> Result<PathBuf> {
    let mut replacement = config.clone();
    update(&mut replacement);
    let path = crate::config_store::save_config(&replacement)?;
    *config = replacement;
    Ok(path)
}
