use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::Result;
use capulus::ui::{Color, RenderTarget, stderr_render_target, stdout_render_target};
use chrono::Utc;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::Serialize;

use super::quota;

const FIELD_SEPARATOR: &str = " · ";
const ACTIVE_LABEL: &str = "active";
const QUOTA_BAR_SEGMENTS: usize = 16;
const USAGE_BAR_HALF_SEGMENTS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DurationUnit {
    Second,
    Minute,
    Hour,
    Day,
}

#[derive(Debug, Clone, Copy)]
struct QuotaRenderTiming {
    now: i64,
    reset_alignment: Option<DurationUnit>,
    highlight_countdown: bool,
}

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
        authentication_required: bool,
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
                authentication_required: quota::requires_authentication(&err),
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
    let now = Utc::now().timestamp();
    let reset_alignment = reset_duration_alignment(view, now);
    let mut lines = view
        .accounts
        .iter()
        .map(|account| render_account_at(account, email_width, now, reset_alignment))
        .collect::<Vec<_>>();
    lines.push(String::new());
    lines.push(render_summary(view));
    if view.accounts.len() > 1 {
        lines.push(String::new());
        lines.push(render_total(view, &stdout_render_target(), now));
    }
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

pub fn print_reset_credit_notice(email: &str, reset_credits: &quota::ResetCredits) {
    let noun = if reset_credits.available_count == 1 {
        "credit is"
    } else {
        "credits are"
    };
    let expiry = reset_credits
        .latest_expires_at
        .map(|expires_at| {
            format!(
                "; the latest expires in {}",
                format_time_remaining(expires_at)
            )
        })
        .unwrap_or_default();
    eprintln!(
        "{} {email} has no remaining quota, but {} usable rate-limit reset {noun} available{expiry}. \
         Run `/usage` in Codex to redeem one.",
        stderr_render_target().paint("notice:", Color::Cyan),
        reset_credits.available_count,
    );
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
                false,
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
    render_account_at(account, email_width, Utc::now().timestamp(), None)
}

fn render_account_at(
    account: &AccountView,
    email_width: usize,
    now: i64,
    reset_alignment: Option<DurationUnit>,
) -> String {
    let target = stdout_render_target();
    render_account_at_with_target(account, email_width, now, reset_alignment, &target)
}

fn render_account_at_with_target(
    account: &AccountView,
    email_width: usize,
    now: i64,
    reset_alignment: Option<DurationUnit>,
    target: &impl RenderTarget,
) -> String {
    let highlight_countdown = matches!(
        &account.quota,
        QuotaStatus::Available { snapshot } if quota::countdown_has_not_started(snapshot, now)
    );
    let base = render_account_base(account, email_width, target, highlight_countdown);
    match (&account.status, &account.quota) {
        (AccountStatus::Ready, QuotaStatus::Available { snapshot }) => {
            format!(
                "{base}{}{}",
                FIELD_SEPARATOR,
                render_quota_at(snapshot, target, now, reset_alignment)
            )
        }
        (AccountStatus::Ready, QuotaStatus::Loading) => {
            format!("{base}{FIELD_SEPARATOR}loading quota")
        }
        (
            AccountStatus::Ready,
            QuotaStatus::Unavailable {
                error,
                authentication_required,
            },
        ) => {
            let hint = if *authentication_required {
                "; run `kai cred fix`".to_owned()
            } else {
                String::new()
            };
            format!("{base}{FIELD_SEPARATOR}quota unavailable: {error}{hint}")
        }
        (AccountStatus::Invalid { .. }, _) => base,
    }
}

fn render_account_base(
    account: &AccountView,
    email_width: usize,
    target: &impl RenderTarget,
    highlight_countdown: bool,
) -> String {
    let (bullet, color) = match (&account.status, account.active) {
        (AccountStatus::Invalid { .. }, _) => ("×", Color::Red),
        (_, true) => ("●", Color::Green),
        _ => ("○", Color::Cyan),
    };
    let mut fields = vec![if account.active {
        target.paint(ACTIVE_LABEL, Color::Green)
    } else {
        " ".repeat(ACTIVE_LABEL.len())
    }];
    if let Some(plan) = &account.plan {
        fields.push(plan.to_ascii_uppercase());
    }
    match &account.status {
        AccountStatus::Ready => {}
        AccountStatus::Invalid { error } => {
            fields.push(target.paint(&format!("invalid: {error}; run `kai cred fix`"), Color::Red));
        }
    }
    let padded_email = format!("{:email_width$}", account.email);
    let rendered_email = if highlight_countdown {
        target.paint(&padded_email, Color::Yellow)
    } else {
        padded_email
    };
    format!(
        "{} {}{}{}",
        target.paint(bullet, color),
        rendered_email,
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
        snapshot.rate_limit_reset_credits.as_ref(),
        target,
    )
}

fn render_quota_at(
    snapshot: &quota::Snapshot,
    target: &impl RenderTarget,
    now: i64,
    reset_alignment: Option<DurationUnit>,
) -> String {
    render_quota_values_at(
        snapshot.remaining_percent,
        snapshot.resets_at,
        snapshot.window_seconds,
        snapshot.rate_limit_reset_credits.as_ref(),
        target,
        QuotaRenderTiming {
            now,
            reset_alignment,
            highlight_countdown: quota::countdown_has_not_started(snapshot, now),
        },
    )
}

fn render_quota_values(
    remaining_percent: f64,
    resets_at: i64,
    window_seconds: Option<i64>,
    reset_credits: Option<&quota::ResetCredits>,
    target: &impl RenderTarget,
) -> String {
    render_quota_values_at(
        remaining_percent,
        resets_at,
        window_seconds,
        reset_credits,
        target,
        QuotaRenderTiming {
            now: Utc::now().timestamp(),
            reset_alignment: None,
            highlight_countdown: false,
        },
    )
}

fn render_quota_values_at(
    remaining_percent: f64,
    resets_at: i64,
    window_seconds: Option<i64>,
    reset_credits: Option<&quota::ResetCredits>,
    target: &impl RenderTarget,
    timing: QuotaRenderTiming,
) -> String {
    let rounded_percent = remaining_percent.clamp(0.0, 100.0).round() as u64;
    let reset_time = format_time_remaining_at(resets_at, timing.now, timing.reset_alignment);
    let reset_time = if timing.highlight_countdown {
        target.paint(&reset_time, Color::Yellow)
    } else {
        reset_time
    };
    let mut rendered = format!(
        "{} [{}] {rounded_percent:>3}% remaining · resets in {}",
        quota_window_label(window_seconds),
        render_quota_bar(remaining_percent, target),
        reset_time,
    );
    if let Some(reset_credits) = reset_credits {
        let noun = if reset_credits.available_count == 1 {
            "credit"
        } else {
            "credits"
        };
        rendered.push_str(&format!(
            " · {} reset {noun}",
            reset_credits.available_count
        ));
        if let Some(expires_at) = reset_credits.latest_expires_at {
            rendered.push_str(&format!(
                " · {}expires in {}",
                if reset_credits.available_count == 1 {
                    ""
                } else {
                    "latest "
                },
                format_time_remaining_at(expires_at, timing.now, None)
            ));
        }
    }
    rendered
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

fn format_time_remaining(timestamp: i64) -> String {
    format_duration(timestamp.saturating_sub(Utc::now().timestamp()))
}

fn format_time_remaining_at(timestamp: i64, now: i64, alignment: Option<DurationUnit>) -> String {
    format_duration_aligned(timestamp.saturating_sub(now), alignment)
}

fn format_duration(total_seconds: i64) -> String {
    format_duration_aligned(total_seconds, None)
}

fn format_duration_aligned(total_seconds: i64, alignment: Option<DurationUnit>) -> String {
    let (leading_unit, components) = duration_components(total_seconds);
    let alignment = alignment.unwrap_or(leading_unit).max(leading_unit);
    let omitted_leading_units = alignment as usize - leading_unit as usize;
    let mut rendered = " ".repeat(omitted_leading_units * 4);
    rendered.push_str(
        &components
            .into_iter()
            .map(|(value, suffix)| format_duration_component(value, suffix))
            .collect::<Vec<_>>()
            .join(" "),
    );
    rendered
}

fn duration_components(total_seconds: i64) -> (DurationUnit, Vec<(i64, &'static str)>) {
    if total_seconds <= 0 {
        return (DurationUnit::Second, vec![(0, "s")]);
    }

    if total_seconds >= 86_400 {
        let rounded_hours = total_seconds.saturating_add(1_800) / 3_600;
        return (
            DurationUnit::Day,
            vec![(rounded_hours / 24, "d"), (rounded_hours % 24, "h")],
        );
    }

    if total_seconds >= 3_600 {
        let rounded_minutes = total_seconds.saturating_add(30) / 60;
        if rounded_minutes >= 1_440 {
            return (
                DurationUnit::Day,
                vec![(rounded_minutes / 1_440, "d"), (0, "h")],
            );
        }
        return (
            DurationUnit::Hour,
            vec![(rounded_minutes / 60, "h"), (rounded_minutes % 60, "m")],
        );
    }

    if total_seconds >= 60 {
        return (
            DurationUnit::Minute,
            vec![(total_seconds / 60, "m"), (total_seconds % 60, "s")],
        );
    }

    (DurationUnit::Second, vec![(total_seconds, "s")])
}

fn format_duration_component(value: i64, suffix: &str) -> String {
    format!("{value:>2}{suffix}")
}

fn reset_duration_alignment(view: &ListView, now: i64) -> Option<DurationUnit> {
    view.accounts
        .iter()
        .filter_map(|account| match &account.quota {
            QuotaStatus::Available { snapshot } => {
                Some(duration_components(snapshot.resets_at.saturating_sub(now)).0)
            }
            QuotaStatus::Loading | QuotaStatus::Unavailable { .. } => None,
        })
        .max()
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

fn render_total(view: &ListView, target: &impl RenderTarget, now: i64) -> String {
    let (remaining_percent, known_quotas) = view
        .accounts
        .iter()
        .filter_map(|account| match &account.quota {
            QuotaStatus::Available { snapshot } => Some(snapshot.remaining_percent),
            QuotaStatus::Loading | QuotaStatus::Unavailable { .. } => None,
        })
        .fold((0.0, 0_usize), |(total, count), remaining| {
            (total + remaining, count + 1)
        });

    let total = if known_quotas == 0 {
        format!(
            "total: [{}] quota unavailable",
            render_quota_bar(0.0, target)
        )
    } else {
        let remaining_percent = remaining_percent / known_quotas as f64;
        let rounded_percent = remaining_percent.clamp(0.0, 100.0).round() as u64;
        format!(
            "total: [{}] {rounded_percent:>3}% remaining",
            render_quota_bar(remaining_percent, target)
        )
    };

    let usage = match average_quota_pace(view, now) {
        Some(value) => format!(
            "usage: [{}] {:+.2}",
            render_usage_bar(value, target),
            normalize_signed_zero(value)
        ),
        None => format!("usage: [{}] unavailable", render_usage_bar(0.0, target)),
    };

    format!("{total}{FIELD_SEPARATOR}{usage}")
}

fn average_quota_pace(view: &ListView, now: i64) -> Option<f64> {
    let (total, count) = view
        .accounts
        .iter()
        .filter_map(|account| match &account.quota {
            QuotaStatus::Available { snapshot } => quota_pace(snapshot, now),
            QuotaStatus::Loading | QuotaStatus::Unavailable { .. } => None,
        })
        .fold((0.0, 0_usize), |(total, count), pace| {
            (total + pace, count + 1)
        });

    (count > 0).then(|| (total / count as f64).clamp(-1.0, 1.0))
}

fn quota_pace(snapshot: &quota::Snapshot, now: i64) -> Option<f64> {
    let window_seconds = snapshot.window_seconds?;
    if window_seconds <= 0 {
        return None;
    }

    let usage_fraction = 1.0 - snapshot.remaining_percent.clamp(0.0, 100.0) / 100.0;
    let time_until_reset_fraction =
        (snapshot.resets_at.saturating_sub(now) as f64 / window_seconds as f64).clamp(0.0, 1.0);
    Some(((1.0 - time_until_reset_fraction) - usage_fraction).clamp(-1.0, 1.0))
}

fn render_usage_bar(value: f64, target: &impl RenderTarget) -> String {
    let value = value.clamp(-1.0, 1.0);
    let magnitude = value.abs() * USAGE_BAR_HALF_SEGMENTS as f64;
    let filled = magnitude.floor() as usize;
    let partial = usize::from(magnitude > filled as f64 && filled < USAGE_BAR_HALF_SEGMENTS);
    let empty = USAGE_BAR_HALF_SEGMENTS.saturating_sub(filled + partial);

    let left = if value < 0.0 {
        format!(
            "{}{}",
            target.paint(&"░".repeat(empty), Color::Blue),
            target.paint(
                &format!("{}{}", "▓".repeat(partial), "█".repeat(filled)),
                Color::Red
            )
        )
    } else {
        target.paint(&"░".repeat(USAGE_BAR_HALF_SEGMENTS), Color::Blue)
    };
    let right = if value > 0.0 {
        format!(
            "{}{}",
            target.paint(
                &format!("{}{}", "█".repeat(filled), "▓".repeat(partial)),
                Color::Green
            ),
            target.paint(&"░".repeat(empty), Color::Blue)
        )
    } else {
        target.paint(&"░".repeat(USAGE_BAR_HALF_SEGMENTS), Color::Blue)
    };

    format!("{left}│{right}")
}

fn normalize_signed_zero(value: f64) -> f64 {
    if value.abs() < 0.005 { 0.0 } else { value }
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

    struct TaggedTarget;

    impl RenderTarget for TaggedTarget {
        fn style(&self, text: &str, color: Option<Color>, _effect: TextEffect) -> String {
            if color == Some(Color::Yellow) {
                format!("<yellow>{text}</yellow>")
            } else {
                text.to_owned()
            }
        }
    }

    #[test]
    fn quota_bar_and_summary_show_remaining_capacity() {
        assert_eq!(render_quota_bar(75.0, &PlainTarget), "████████████▓░░░");
        assert!(
            render_quota_values(75.0, 2_000_000_000, Some(18_000), None, &PlainTarget)
                .contains("5h quota")
        );
        assert!(
            render_quota_values(75.0, 2_000_000_000, Some(18_000), None, &PlainTarget)
                .contains(" 75% remaining")
        );
        assert!(
            render_quota_values(75.0, 2_000_000_000, Some(18_000), None, &PlainTarget)
                .contains("resets in ")
        );
    }

    #[test]
    fn relative_durations_are_padded_limited_and_rounded() {
        assert_eq!(format_duration(604_799), " 7d  0h");
        assert_eq!(format_duration(90_061), " 1d  1h");
        assert_eq!(format_duration(91_800), " 1d  2h");
        assert_eq!(format_duration(86_399), " 1d  0h");
        assert_eq!(format_duration(3_661), " 1h  1m");
        assert_eq!(format_duration(61), " 1m  1s");
        assert_eq!(format_duration(0), " 0s");
    }

    #[test]
    fn untouched_seven_day_countdowns_highlight_the_credential_and_reset_time() {
        let now = 1_000_000;
        let account = |reset_after_seconds| AccountView {
            email: "idle@example.com".to_owned(),
            active: false,
            plan: Some("pro".to_owned()),
            last_refresh: None,
            status: AccountStatus::Ready,
            quota: QuotaStatus::Available {
                snapshot: quota::Snapshot {
                    remaining_percent: 75.0,
                    resets_at: now + 604_800,
                    window_seconds: Some(604_800),
                    reset_after_seconds: Some(reset_after_seconds),
                    rate_limit_reset_credits: None,
                },
            },
        };

        let untouched = render_account_at_with_target(
            &account(604_800),
            "idle@example.com".len(),
            now,
            None,
            &TaggedTarget,
        );
        assert!(untouched.contains("<yellow>idle@example.com</yellow>"));
        assert!(untouched.contains("resets in <yellow> 7d  0h</yellow>"));

        let running = render_account_at_with_target(
            &account(604_799),
            "idle@example.com".len(),
            now,
            None,
            &TaggedTarget,
        );
        assert!(!running.contains("<yellow>idle@example.com</yellow>"));
        assert!(!running.contains("resets in <yellow>"));
    }

    #[test]
    fn shorter_duration_units_align_under_their_matching_columns() {
        let snapshot = |resets_at| QuotaStatus::Available {
            snapshot: quota::Snapshot {
                remaining_percent: 50.0,
                resets_at,
                window_seconds: Some(604_800),
                reset_after_seconds: None,
                rate_limit_reset_credits: None,
            },
        };
        let now = 1_000_000;
        let view = ListView {
            active: None,
            next: None,
            accounts: vec![
                AccountView {
                    email: "days@example.com".to_owned(),
                    active: false,
                    plan: None,
                    last_refresh: None,
                    status: AccountStatus::Ready,
                    quota: snapshot(now + 2 * 86_400),
                },
                AccountView {
                    email: "hours@example.com".to_owned(),
                    active: false,
                    plan: None,
                    last_refresh: None,
                    status: AccountStatus::Ready,
                    quota: snapshot(now + 4 * 3_600 + 20 * 60),
                },
            ],
        };
        let alignment = reset_duration_alignment(&view, now);

        assert_eq!(alignment, Some(DurationUnit::Day));
        assert_eq!(
            format_time_remaining_at(now + 4 * 3_600 + 20 * 60, now, alignment),
            "     4h 20m"
        );
        assert_eq!(
            format_duration_aligned(9 * 60 + 12, alignment),
            "         9m 12s"
        );
        assert_eq!(format_duration_aligned(12, alignment), "            12s");
    }

    #[test]
    fn quota_summary_reports_reset_credit_count_and_expiry() {
        let rendered = render_quota_values(
            0.0,
            2_000_000_000,
            Some(604_800),
            Some(&quota::ResetCredits {
                available_count: 2,
                latest_expires_at: Some(2_100_000_000),
            }),
            &PlainTarget,
        );
        assert!(rendered.contains("2 reset credits"));
        assert!(rendered.contains("latest expires in "));
    }

    #[test]
    fn quota_bar_clamps_backend_percentages() {
        assert_eq!(render_quota_bar(-5.0, &PlainTarget), "░░░░░░░░░░░░░░░░");
        assert_eq!(render_quota_bar(105.0, &PlainTarget), "████████████████");
    }

    #[test]
    fn quota_pace_compares_elapsed_time_with_consumed_quota() {
        let now = 1_000_000;
        let snapshot = |remaining_percent| quota::Snapshot {
            remaining_percent,
            resets_at: now + 60,
            window_seconds: Some(100),
            reset_after_seconds: None,
            rate_limit_reset_credits: None,
        };

        assert!((quota_pace(&snapshot(70.0), now).unwrap() - 0.1).abs() < f64::EPSILON * 4.0);
        assert!((quota_pace(&snapshot(20.0), now).unwrap() + 0.4).abs() < f64::EPSILON * 4.0);
    }

    #[test]
    fn usage_bar_is_centered_across_its_signed_range() {
        assert_eq!(render_usage_bar(-1.0, &PlainTarget), "████████│░░░░░░░░");
        assert_eq!(render_usage_bar(-0.5, &PlainTarget), "░░░░████│░░░░░░░░");
        assert_eq!(render_usage_bar(0.0, &PlainTarget), "░░░░░░░░│░░░░░░░░");
        assert_eq!(render_usage_bar(0.5, &PlainTarget), "░░░░░░░░│████░░░░");
        assert_eq!(render_usage_bar(1.0, &PlainTarget), "░░░░░░░░│████████");
    }

    #[test]
    fn total_bar_averages_known_quotas_and_is_separated_at_the_end() {
        let available = |email: &str, remaining_percent| AccountView {
            email: email.to_owned(),
            active: false,
            plan: Some("pro".to_owned()),
            last_refresh: None,
            status: AccountStatus::Ready,
            quota: QuotaStatus::Available {
                snapshot: quota::Snapshot {
                    remaining_percent,
                    resets_at: 2_000_000_000,
                    window_seconds: Some(604_800),
                    reset_after_seconds: None,
                    rate_limit_reset_credits: None,
                },
            },
        };
        let view = ListView {
            active: None,
            next: None,
            accounts: vec![
                available("alice@example.com", 75.0),
                available("bob@example.com", 25.0),
                AccountView {
                    email: "offline@example.com".to_owned(),
                    active: false,
                    plan: Some("pro".to_owned()),
                    last_refresh: None,
                    status: AccountStatus::Ready,
                    quota: QuotaStatus::Unavailable {
                        error: "offline".to_owned(),
                        authentication_required: false,
                    },
                },
            ],
        };

        assert_eq!(
            render_total(&view, &PlainTarget, 1_000_000),
            concat!(
                "total: [████████▓░░░░░░░]  50% remaining · ",
                "usage: [░░░░████│░░░░░░░░] -0.50",
            )
        );
        assert!(render_list(&view, false).unwrap().ends_with(concat!(
            "\n\n3 accounts enrolled\n\n",
            "total: [████████▓░░░░░░░]  50% remaining · ",
            "usage: [░░░░████│░░░░░░░░] -0.50\n",
        )));
    }

    #[test]
    fn total_bar_reports_unavailable_when_no_quota_is_known() {
        let view = ListView {
            active: None,
            next: None,
            accounts: vec!["alice@example.com", "bob@example.com"]
                .into_iter()
                .map(|email| AccountView {
                    email: email.to_owned(),
                    active: false,
                    plan: None,
                    last_refresh: None,
                    status: AccountStatus::Ready,
                    quota: QuotaStatus::Unavailable {
                        error: "offline".to_owned(),
                        authentication_required: false,
                    },
                })
                .collect(),
        };

        assert_eq!(
            render_total(&view, &PlainTarget, 1_000_000),
            concat!(
                "total: [░░░░░░░░░░░░░░░░] quota unavailable · ",
                "usage: [░░░░░░░░│░░░░░░░░] unavailable",
            )
        );
    }

    #[test]
    fn inactive_accounts_reserve_the_active_column() {
        let account = |email: &str, active| AccountView {
            email: email.to_owned(),
            active,
            plan: Some("pro".to_owned()),
            last_refresh: None,
            status: AccountStatus::Ready,
            quota: QuotaStatus::Loading,
        };
        let inactive = account("one@example.com", false);
        let active = account("two@example.com", true);

        assert_eq!(
            render_account_base(&inactive, inactive.email.len(), &PlainTarget, false),
            "○ one@example.com ·        · PRO"
        );
        assert_eq!(
            render_account_base(&active, active.email.len(), &PlainTarget, false),
            "● two@example.com · active · PRO"
        );
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
                last_refresh: None,
                status: AccountStatus::Ready,
                quota: QuotaStatus::Available {
                    snapshot: quota::Snapshot {
                        remaining_percent: 75.0,
                        resets_at: 2_000_000_000,
                        window_seconds: Some(18_000),
                        reset_after_seconds: None,
                        rate_limit_reset_credits: None,
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
        let final_output = final_output.unwrap();
        assert!(final_output.starts_with("● alice@example.com · active · PRO · 5h quota"));
        assert!(final_output.ends_with("\n\n1 account enrolled\n"));
    }
}
