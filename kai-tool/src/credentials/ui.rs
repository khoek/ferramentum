use std::io::{self, IsTerminal, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use capulus::ui::{Color, RenderTarget, stderr_render_target, stdout_render_target};
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
        Some(Self::with_draw_target(view, ProgressDrawTarget::stdout()))
    }

    fn with_draw_target(view: &ListView, target: ProgressDrawTarget) -> Self {
        let progress = MultiProgress::with_draw_target(target);
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
        live
    }

    pub fn update(&self, index: usize, account: &AccountView) {
        if let Some(bar) = self.bars.get(index) {
            render_live_account(bar, account, self.email_width);
        }
    }

    pub fn finish(self, view: &ListView) -> Result<()> {
        self.finish_with(view, |view| print_list(view, false))
    }

    fn finish_with(
        self,
        view: &ListView,
        write_final: impl FnOnce(&ListView) -> Result<()>,
    ) -> Result<()> {
        for (bar, account) in self.bars.iter().zip(&view.accounts) {
            if matches!(account.quota, QuotaStatus::Loading) {
                render_live_account(bar, account, self.email_width);
            }
        }
        self.progress.clear()?;
        drop(self.bars);
        drop(self.progress);
        write_final(view)
    }
}

pub fn print_list(view: &ListView, json: bool) -> Result<()> {
    io::stdout().write_all(render_list(view, json)?.as_bytes())?;
    Ok(())
}

fn render_list(view: &ListView, json: bool) -> Result<String> {
    if json {
        return Ok(format!("{}\n", serde_json::to_string_pretty(view)?));
    }
    if view.accounts.is_empty() {
        return Ok(concat!(
            "No Codex accounts enrolled.\n",
            "Run `kai cred add <email>` to import the current account or enroll a new one.\n",
        )
        .to_owned());
    }

    let email_width = email_width(view);
    let mut lines = view
        .accounts
        .iter()
        .map(|account| render_account(account, email_width))
        .collect::<Vec<_>>();
    lines.push(String::new());
    lines.push(render_summary(view));
    let mut output = lines.join("\n");
    output.push('\n');
    Ok(output)
}

pub fn print_quota(snapshot: &quota::Snapshot) {
    capulus::ui::detail(&format!(
        "Quota: {}",
        render_quota(snapshot, &stderr_render_target())
    ));
}

fn render_live_account(bar: &ProgressBar, account: &AccountView, email_width: usize) {
    bar.disable_steady_tick();
    match (&account.status, &account.quota) {
        (AccountStatus::Ready, QuotaStatus::Loading) => {
            bar.set_style(
                ProgressStyle::with_template("{msg} · {spinner:.cyan} loading quota")
                    .expect("valid quota loading progress template")
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            bar.set_message(render_account_base(
                account,
                email_width,
                &stdout_render_target(),
            ));
            bar.enable_steady_tick(Duration::from_millis(80));
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
    let target = stdout_render_target();
    let base = render_account_base(account, email_width, &target);
    match (&account.status, &account.quota) {
        (AccountStatus::Ready, QuotaStatus::Available { snapshot }) => {
            format!(
                "{base}{}{}",
                FIELD_SEPARATOR,
                render_quota(snapshot, &target)
            )
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

fn render_account_base(
    account: &AccountView,
    email_width: usize,
    target: &impl RenderTarget,
) -> String {
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
                fields.push(render_access_expiry(expires_at, target));
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

fn render_quota(snapshot: &quota::Snapshot, target: &impl RenderTarget) -> String {
    render_quota_values(
        snapshot.remaining_percent,
        snapshot.resets_at,
        snapshot.window_seconds,
        target,
    )
}

fn render_quota_values(
    remaining_percent: f64,
    resets_at: i64,
    window_seconds: Option<i64>,
    target: &impl RenderTarget,
) -> String {
    let rounded_percent = remaining_percent.clamp(0.0, 100.0).round() as u64;
    format!(
        "{} [{}] {rounded_percent:>3}% remaining · resets {}",
        quota_window_label(window_seconds),
        render_quota_bar(remaining_percent, target),
        format_reset_datetime(resets_at),
    )
}

fn render_quota_bar(remaining_percent: f64, target: &impl RenderTarget) -> String {
    let rounded_percent = remaining_percent.clamp(0.0, 100.0).round() as u64;
    let fill = rounded_percent as f64 / 100.0 * QUOTA_BAR_SEGMENTS as f64;
    let entirely_filled = fill as usize;
    let current = (fill > 0.0 && entirely_filled < QUOTA_BAR_SEGMENTS) as usize;
    let completed = format!("{}{}", "█".repeat(entirely_filled), "▓".repeat(current));
    format!(
        "{}{}",
        target.paint(&completed, quota_color(rounded_percent)),
        target.paint(
            &"░".repeat(
                QUOTA_BAR_SEGMENTS
                    .saturating_sub(entirely_filled)
                    .saturating_sub(current)
            ),
            Color::Blue
        )
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

fn quota_color(remaining_percent: u64) -> Color {
    if remaining_percent <= 20 {
        Color::Red
    } else if remaining_percent <= 50 {
        Color::Yellow
    } else {
        Color::Green
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

fn render_summary(view: &ListView) -> String {
    let noun = if view.accounts.len() == 1 {
        "account"
    } else {
        "accounts"
    };
    match &view.next {
        Some(next) => format!("{} {noun} enrolled · next: {next}", view.accounts.len()),
        None => format!("{} {noun} enrolled", view.accounts.len()),
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
    use capulus::ui::TextEffect;
    use indicatif::InMemoryTerm;

    use super::*;

    struct PlainTarget;

    impl RenderTarget for PlainTarget {
        fn style(&self, text: &str, _color: Option<Color>, _effect: TextEffect) -> String {
            text.to_owned()
        }
    }

    #[test]
    fn quota_bar_and_summary_show_remaining_capacity() {
        assert_eq!(render_quota_bar(75.0, &PlainTarget), "████████████▓░░░");
        assert!(
            render_quota_values(75.0, 2_000_000_000, Some(18_000), &PlainTarget)
                .contains("5h quota")
        );
        assert!(
            render_quota_values(75.0, 2_000_000_000, Some(18_000), &PlainTarget)
                .contains(" 75% remaining")
        );
    }

    #[test]
    fn quota_bar_clamps_backend_percentages() {
        assert_eq!(render_quota_bar(-5.0, &PlainTarget), "░░░░░░░░░░░░░░░░");
        assert_eq!(render_quota_bar(105.0, &PlainTarget), "████████████████");
    }

    #[test]
    fn finished_live_rows_are_replaced_with_static_lines() {
        let view = ListView {
            active: Some("alice@example.com".to_owned()),
            next: None,
            accounts: vec![AccountView {
                email: "alice@example.com".to_owned(),
                active: true,
                plan: Some("pro".to_owned()),
                access_expires_at: None,
                last_refresh: None,
                status: AccountStatus::Ready,
                quota: QuotaStatus::Available {
                    snapshot: quota::Snapshot {
                        remaining_percent: 75.0,
                        resets_at: 2_000_000_000,
                        window_seconds: Some(18_000),
                    },
                },
            }],
        };
        let terminal = InMemoryTerm::new(10, 160);
        let live = LiveList::with_draw_target(
            &view,
            ProgressDrawTarget::term_like(Box::new(terminal.clone())),
        );
        let account = render_account(&view.accounts[0], email_width(&view));
        assert_eq!(terminal.contents(), account);

        let mut final_output = None;
        live.finish_with(&view, |view| {
            final_output = Some(render_list(view, false)?);
            Ok(())
        })
        .unwrap();

        assert_eq!(terminal.contents(), "");
        assert_eq!(
            final_output.unwrap(),
            format!("{account}\n\n1 account enrolled\n")
        );
    }
}
