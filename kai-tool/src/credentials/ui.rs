use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use capulus::ui::{Color, RenderTarget, stdout_render_target};
use serde::Serialize;

const FIELD_SEPARATOR: &str = " · ";

#[derive(Debug, Serialize)]
pub struct ListView {
    pub active: Option<String>,
    pub next: Option<String>,
    pub accounts: Vec<AccountView>,
}

#[derive(Debug, Serialize)]
pub struct AccountView {
    pub email: String,
    pub active: bool,
    pub plan: Option<String>,
    pub access_expires_at: Option<i64>,
    pub last_refresh: Option<String>,
    pub status: AccountStatus,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountStatus {
    Ready,
    Invalid { error: String },
}

pub fn print_list(view: &ListView, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(view)?);
        return Ok(());
    }
    if view.accounts.is_empty() {
        println!("No Codex accounts enrolled.");
        println!("Run `kai cred add <email>` to import the current account or enroll a new one.");
        return Ok(());
    }

    let target = stdout_render_target();
    let email_width = view
        .accounts
        .iter()
        .map(|account| account.email.chars().count())
        .max()
        .unwrap_or_default();
    for account in &view.accounts {
        let (bullet, color) = match (&account.status, account.active) {
            (AccountStatus::Invalid { .. }, _) => ("×", Color::Red),
            (_, true) => ("●", Color::Green),
            _ => ("○", Color::Cyan),
        };
        let mut fields = Vec::new();
        if account.active {
            fields.push(target.paint("active", Color::Green));
        }
        if let Some(plan) = &account.plan {
            fields.push(plan.to_ascii_uppercase());
        }
        match &account.status {
            AccountStatus::Ready => {
                if let Some(expires_at) = account.access_expires_at {
                    fields.push(render_access_expiry(expires_at, &target));
                }
            }
            AccountStatus::Invalid { error } => {
                fields.push(target.paint(&format!("invalid: {error}"), Color::Red));
            }
        }
        println!(
            "{} {:email_width$}{}{}",
            target.paint(bullet, color),
            account.email,
            if fields.is_empty() {
                ""
            } else {
                FIELD_SEPARATOR
            },
            fields.join(FIELD_SEPARATOR),
        );
    }

    println!();
    let noun = if view.accounts.len() == 1 {
        "account"
    } else {
        "accounts"
    };
    match &view.next {
        Some(next) => println!("{} {noun} enrolled · next: {next}", view.accounts.len()),
        None => println!("{} {noun} enrolled", view.accounts.len()),
    }
    Ok(())
}

fn render_access_expiry(expires_at: i64, target: &impl RenderTarget) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let remaining = expires_at - now;
    if remaining <= 0 {
        return target.paint("access refresh due", Color::Yellow);
    }
    if remaining < 120 {
        return "access <2m".to_owned();
    }
    if remaining < 7200 {
        return format!("access {}m", remaining / 60);
    }
    format!("access {}h", remaining / 3600)
}
