use std::io::{self, IsTerminal};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use capulus::ui::{Color, RenderTarget, stdout_render_target};
use chrono::{DateTime, Local, Utc};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::Serialize;

use super::quota;

const FIELD_SEPARATOR: &str = " · ";
const QUOTA_BAR_SEGMENTS: usize = 16;

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
    pub quota: QuotaStatus,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountStatus {
    Ready,
    Invalid { error: String },
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuotaStatus {
    Loading,
    Available {
        #[serde(flatten)]
        snapshot: quota::Snapshot,
    },
    Unavailable {
        error: String,
    },
}

pub struct LiveList {
    progress: MultiProgress,
    bars: Vec<ProgressBar>,
    email_width: usize,
}

impl AccountView {
    pub fn set_quota(&mut self, result: Result<quota::Snapshot>) {
        self.quota = match result {
            Ok(snapshot) => QuotaStatus::Available { snapshot },
            Err(err) => QuotaStatus::Unavailable {
                error: format!("{err:#}"),
            },
        };
    }
}

impl LiveList {
    pub fn start(view: &ListView) -> Option<Self> {
        if !io::stdout().is_terminal() || view.accounts.is_empty() {
            return None;
        }
        let progress = MultiProgress::with_draw_target(ProgressDrawTarget::stdout());
        progress.set_move_cursor(true);
        let email_width = email_width(view);
        let bars = (0..view.accounts.len())
            .map(|_| progress.add(ProgressBar::new_spinner()))
            .collect::<Vec<_>>();
        let live = Self {
            progress,
            bars,
            email_width,
        };
        for (bar, account) in live.bars.iter().zip(&view.accounts) {
            render_live_account(bar, account, email_width);
        }
        Some(live)
    }

    pub fn update(&self, index: usize, account: &AccountView) {
        if let Some(bar) = self.bars.get(index) {
            render_live_account(bar, account, self.email_width);
        }
    }

    pub fn finish(self, view: &ListView) {
        for (bar, account) in self.bars.iter().zip(&view.accounts) {
            if matches!(account.quota, QuotaStatus::Loading) {
                render_live_account(bar, account, self.email_width);
            }
        }
        drop(self.bars);
        drop(self.progress);
        println!();
        print_summary(view);
    }
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

    let email_width = email_width(view);
    for account in &view.accounts {
        println!("{}", render_account(account, email_width));
    }
    println!();
    print_summary(view);
    Ok(())
}

pub fn print_quota(snapshot: &quota::Snapshot) {
    capulus::ui::detail(&format!("Quota: {}", render_quota(snapshot)));
}

fn render_live_account(bar: &ProgressBar, account: &AccountView, email_width: usize) {
    bar.disable_steady_tick();
    let base = render_account_base(account, email_width);
    match (&account.status, &account.quota) {
        (AccountStatus::Ready, QuotaStatus::Loading) => {
            bar.set_style(
                ProgressStyle::with_template("{msg} · {spinner:.cyan} loading quota")
                    .expect("valid quota loading progress template")
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            bar.set_message(base);
            bar.enable_steady_tick(Duration::from_millis(80));
        }
        (AccountStatus::Ready, QuotaStatus::Available { snapshot }) => {
            let color = quota_color(snapshot.remaining_percent);
            let template = format!(
                "{{prefix}} [{{bar:{QUOTA_BAR_SEGMENTS}.{color}/blue}}] \
                 {{pos:>3}}% remaining · resets {{msg}}"
            );
            bar.set_style(
                ProgressStyle::with_template(&template)
                    .expect("valid quota progress template")
                    .progress_chars("█▓░"),
            );
            bar.set_length(100);
            bar.set_position(snapshot.remaining_percent.clamp(0.0, 100.0).round() as u64);
            bar.set_prefix(format!(
                "{base}{}{}",
                FIELD_SEPARATOR,
                quota_window_label(snapshot.window_seconds)
            ));
            bar.set_message(format_reset_datetime(snapshot.resets_at));
            bar.finish();
        }
        _ => {
            bar.set_style(
                ProgressStyle::with_template("{msg}").expect("valid quota result template"),
            );
            bar.finish_with_message(render_account(account, email_width));
        }
    }
}

fn render_account(account: &AccountView, email_width: usize) -> String {
    let base = render_account_base(account, email_width);
    match (&account.status, &account.quota) {
        (AccountStatus::Ready, QuotaStatus::Available { snapshot }) => {
            format!("{base}{}{}", FIELD_SEPARATOR, render_quota(snapshot))
        }
        (AccountStatus::Ready, QuotaStatus::Loading) => {
            format!("{base}{FIELD_SEPARATOR}loading quota")
        }
        (AccountStatus::Ready, QuotaStatus::Unavailable { error }) => {
            format!("{base}{FIELD_SEPARATOR}quota unavailable: {error}")
        }
        (AccountStatus::Invalid { .. }, _) => base,
    }
}

fn render_account_base(account: &AccountView, email_width: usize) -> String {
    let target = stdout_render_target();
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
    format!(
        "{} {:email_width$}{}{}",
        target.paint(bullet, color),
        account.email,
        if fields.is_empty() {
            ""
        } else {
            FIELD_SEPARATOR
        },
        fields.join(FIELD_SEPARATOR),
    )
}

fn render_quota(snapshot: &quota::Snapshot) -> String {
    render_quota_values(
        snapshot.remaining_percent,
        snapshot.resets_at,
        snapshot.window_seconds,
    )
}

fn render_quota_values(
    remaining_percent: f64,
    resets_at: i64,
    window_seconds: Option<i64>,
) -> String {
    format!(
        "{} [{}] {:.0}% remaining · resets {}",
        quota_window_label(window_seconds),
        render_quota_bar(remaining_percent),
        remaining_percent.clamp(0.0, 100.0),
        format_reset_datetime(resets_at),
    )
}

fn render_quota_bar(remaining_percent: f64) -> String {
    let filled = ((remaining_percent.clamp(0.0, 100.0) / 100.0) * QUOTA_BAR_SEGMENTS as f64).round()
        as usize;
    format!(
        "{}{}",
        "█".repeat(filled.min(QUOTA_BAR_SEGMENTS)),
        "░".repeat(QUOTA_BAR_SEGMENTS.saturating_sub(filled))
    )
}

fn quota_window_label(window_seconds: Option<i64>) -> String {
    match window_seconds {
        Some(seconds) if seconds % 86_400 == 0 => format!("{}d quota", seconds / 86_400),
        Some(seconds) if seconds % 3_600 == 0 => format!("{}h quota", seconds / 3_600),
        Some(seconds) if seconds % 60 == 0 => format!("{}m quota", seconds / 60),
        _ => "quota".to_owned(),
    }
}

fn quota_color(remaining_percent: f64) -> &'static str {
    if remaining_percent <= 20.0 {
        "red"
    } else if remaining_percent <= 50.0 {
        "yellow"
    } else {
        "green"
    }
}

fn format_reset_datetime(resets_at: i64) -> String {
    DateTime::<Utc>::from_timestamp(resets_at, 0)
        .map(|datetime| {
            datetime
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M %Z")
                .to_string()
        })
        .unwrap_or_else(|| format!("Unix timestamp {resets_at}"))
}

fn email_width(view: &ListView) -> usize {
    view.accounts
        .iter()
        .map(|account| account.email.chars().count())
        .max()
        .unwrap_or_default()
}

fn print_summary(view: &ListView) {
    let noun = if view.accounts.len() == 1 {
        "account"
    } else {
        "accounts"
    };
    match &view.next {
        Some(next) => println!("{} {noun} enrolled · next: {next}", view.accounts.len()),
        None => println!("{} {noun} enrolled", view.accounts.len()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_bar_and_summary_show_remaining_capacity() {
        assert_eq!(render_quota_bar(75.0), "████████████░░░░");
        assert!(render_quota_values(75.0, 2_000_000_000, Some(18_000)).contains("5h quota"));
        assert!(render_quota_values(75.0, 2_000_000_000, Some(18_000)).contains("75% remaining"));
    }

    #[test]
    fn quota_bar_clamps_backend_percentages() {
        assert_eq!(render_quota_bar(-5.0), "░░░░░░░░░░░░░░░░");
        assert_eq!(render_quota_bar(105.0), "████████████████");
    }
}
