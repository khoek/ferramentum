use auc_tool::application::{CredentialSummary, Status as ApplicationStatus};
use capulus::managed::{JobPhase, RedeployJob};
use capulus::ui::{Color, ColorMode, ProgressMode, RenderTarget, UiOptions};
use clap::{Args, ValueEnum};

#[derive(Clone, Copy, Debug, Args)]
pub(super) struct UiArgs {
    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = UiProgressMode::Auto,
        help = "Progress rendering mode (auto uses a live display on a terminal and plain status otherwise)"
    )]
    progress: UiProgressMode,

    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = UiColorMode::Auto,
        help = "Color rendering mode"
    )]
    color: UiColorMode,
}

impl UiArgs {
    pub(super) fn options(self) -> UiOptions {
        UiOptions {
            progress: match self.progress {
                UiProgressMode::Auto => ProgressMode::Auto,
                UiProgressMode::Interactive => ProgressMode::Interactive,
                UiProgressMode::Plain => ProgressMode::Plain,
                UiProgressMode::Off => ProgressMode::Off,
            },
            color: match self.color {
                UiColorMode::Auto => ColorMode::Auto,
                UiColorMode::Always => ColorMode::Always,
                UiColorMode::Never => ColorMode::Never,
            },
            ..UiOptions::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum UiProgressMode {
    #[default]
    Auto,
    Interactive,
    Plain,
    Off,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum UiColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

pub(super) fn render_status(status: &ApplicationStatus, target: &impl RenderTarget) -> String {
    let marker_color = if !status.device_present {
        Color::Red
    } else if status.pending_touch {
        Color::Yellow
    } else {
        Color::Green
    };
    let authenticator = if status.device_present {
        target.paint("ready", Color::Green)
    } else {
        target.paint("absent", Color::Red)
    };
    let presence = if status.pending_touch {
        target.paint("waiting", Color::Yellow)
    } else {
        "idle".to_string()
    };
    format!(
        "{} auc-agent v{} · authenticator {authenticator} · presence {presence} · {}",
        target.paint("●", marker_color),
        terminal_text(&status.version),
        count_text(status.credential_count, "credential")
    )
}

fn count_text(count: usize, singular: &str) -> String {
    format!("{count} {singular}{}", if count == 1 { "" } else { "s" })
}

pub(super) fn render_credential(
    credential: &CredentialSummary,
    target: &impl RenderTarget,
) -> [String; 2] {
    let mut facts = vec![terminal_text(&credential.rp_id)];
    if let Some(user_name) = &credential.user_name {
        facts.push(format!("account {}", terminal_text(user_name)));
    }
    facts.push(
        if credential.discoverable {
            "discoverable"
        } else {
            "non-discoverable"
        }
        .to_string(),
    );
    if credential.backup_eligible {
        facts.push("backup eligible".to_string());
        facts.push(if credential.backed_up {
            target.paint("backed up", Color::Green)
        } else {
            "local only".to_string()
        });
    } else {
        facts.push("device-bound".to_string());
    }
    [
        format!("{} {}", target.paint("•", Color::Cyan), facts.join(" · ")),
        format!("  id {}", terminal_text(&credential.credential_id)),
    ]
}

pub(super) fn terminal_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control()
            || matches!(
                character,
                '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
            )
        {
            escaped.extend(character.escape_unicode());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

pub(super) fn redeploy_recorded_state(job: &RedeployJob) -> String {
    let system = if !job.system_committed {
        "system changes were not committed"
    } else {
        match job.rollback_succeeded {
            Some(true) => "system commit was rolled back successfully",
            Some(false) => "system commit occurred and rollback did not succeed",
            None => "system commit occurred; no rollback result was recorded",
        }
    };
    system.to_string()
}

pub(super) fn render_job(job: &RedeployJob, target: &impl RenderTarget) -> Vec<String> {
    let color = phase_color(&job.phase);
    let mut state = vec![if job.system_committed {
        "system committed".to_string()
    } else if job.phase.is_terminal() {
        "system not committed".to_string()
    } else {
        "system pending".to_string()
    }];
    if let Some(rollback) = job.rollback_succeeded {
        state.push(if rollback {
            target.paint("rollback succeeded", Color::Green)
        } else {
            target.paint("rollback failed", Color::Red)
        });
    } else if job.phase == JobPhase::Failed && job.system_committed {
        state.push(target.paint("rollback outcome unrecorded", Color::Yellow));
    }
    state.push(format!("unit {}", terminal_text(&job.unit)));
    vec![
        format!(
            "{} job {} · auc v{} · {}",
            target.paint("●", color),
            job.job,
            terminal_text(&job.version),
            target.paint(phase_name(&job.phase), color)
        ),
        format!("  {}", terminal_text(&job.detail)),
        format!("  {}", state.join(" · ")),
    ]
}

fn phase_color(phase: &JobPhase) -> Color {
    match phase {
        JobPhase::Complete => Color::Green,
        JobPhase::Failed => Color::Red,
        JobPhase::Queued => Color::Yellow,
        _ => Color::Cyan,
    }
}

pub(super) fn phase_name(phase: &JobPhase) -> &'static str {
    match phase {
        JobPhase::Queued => "queued",
        JobPhase::Preparing => "preparing",
        JobPhase::Toolchain => "checking Rust toolchain",
        JobPhase::Building => "building binaries",
        JobPhase::Validating => "validating release",
        JobPhase::Staging => "staging installation",
        JobPhase::CommittingSystem => "committing system files",
        JobPhase::RestartingAgent => "restarting auc-agent",
        JobPhase::Complete => "complete",
        JobPhase::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use capulus::managed::JobId;
    use capulus::ui::TextEffect;

    use super::*;

    struct PlainTarget;

    impl RenderTarget for PlainTarget {
        fn style(&self, text: &str, _color: Option<Color>, _effect: TextEffect) -> String {
            text.to_string()
        }
    }

    #[test]
    fn terminal_text_escapes_control_and_bidirectional_formatting_characters() {
        let escaped = terminal_text("alice\n\u{1b}[31m\u{202e}");
        assert_eq!(escaped, "alice\\u{a}\\u{1b}[31m\\u{202e}");
    }

    #[test]
    fn status_is_one_compact_semantic_line() {
        let status = ApplicationStatus {
            product: "auc".to_string(),
            package: "auc-tool".to_string(),
            version: "0.1.0".to_string(),
            protocol_major: 1,
            device_present: true,
            pending_touch: true,
            credential_count: 1,
        };
        assert_eq!(
            render_status(&status, &PlainTarget),
            "● auc-agent v0.1.0 · authenticator ready · presence waiting · 1 credential"
        );
    }

    #[test]
    fn credential_rows_are_compact_and_keep_the_complete_safe_id() {
        let credential = CredentialSummary {
            credential_id: "0123456789abcdef".to_string(),
            rp_id: "example.test".to_string(),
            user_name: Some("alice\nadmin".to_string()),
            discoverable: true,
            backup_eligible: true,
            backed_up: false,
        };
        assert_eq!(
            render_credential(&credential, &PlainTarget),
            [
                "• example.test · account alice\\u{a}admin · discoverable · backup eligible · local only",
                "  id 0123456789abcdef",
            ]
        );
    }

    #[test]
    fn redeploy_job_rows_preserve_operational_recovery_state() {
        let job = RedeployJob {
            job: JobId::parse("deadbeefdeadbeefdeadbeefdeadbeef").unwrap(),
            product: "auc".to_string(),
            version: "0.2.0".to_string(),
            unit: "auc-redeploy-deadbeef.service".to_string(),
            phase: JobPhase::Failed,
            detail: "validation failed".to_string(),
            system_committed: true,
            rollback_succeeded: Some(false),
        };
        assert_eq!(
            render_job(&job, &PlainTarget),
            [
                "● job deadbeefdeadbeefdeadbeefdeadbeef · auc v0.2.0 · failed",
                "  validation failed",
                "  system committed · rollback failed · unit auc-redeploy-deadbeef.service",
            ]
        );
        assert_eq!(
            redeploy_recorded_state(&job),
            "system commit occurred and rollback did not succeed"
        );
    }
}
