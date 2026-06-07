use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use crate::transcript::{TranscriptBlock, TranscriptKind, TranscriptLineKind};

pub struct AttachDocument {
    pub lines: Vec<Line<'static>>,
    pub markers: Vec<AttachReplyMarker>,
}

pub struct AttachReplyMarker {
    pub offset: usize,
    pub label: String,
}

pub fn push_attach_transcript_block(
    document: &mut AttachDocument,
    run_id: u64,
    block: TranscriptBlock,
    collapse_thinking: bool,
) {
    let kind = if block.kind == TranscriptKind::Assistant
        && crate::transcript::block_looks_like_thinking(&block)
    {
        TranscriptKind::Thinking
    } else {
        block.kind
    };
    if kind == TranscriptKind::Thinking && collapse_thinking {
        let line_count = block
            .lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .count();
        document.lines.push(Line::from(vec![
            Span::styled("  ◌ ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("thinking collapsed · {line_count} lines · press t to show"),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        return;
    }
    if matches!(kind, TranscriptKind::Assistant | TranscriptKind::Thinking) {
        document.markers.push(AttachReplyMarker {
            offset: document.lines.len(),
            label: format!("run {run_id} {}", block.label),
        });
    }
    document
        .lines
        .push(attach_transcript_header_line(kind, &block.label));
    document.lines.extend(
        block
            .lines
            .iter()
            .map(|line| attach_transcript_content_line(kind, line)),
    );
}

pub fn attach_markdown_line(line: &str, color: Color) -> Line<'static> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') {
        Line::from(Span::styled(
            line.to_owned(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
    } else if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
        prefixed_attach_line("  ", line, Color::White)
    } else if trimmed.starts_with("```") {
        prefixed_attach_line("", line, Color::Yellow)
    } else {
        prefixed_attach_line("", line, color)
    }
}

pub fn current_attach_marker_index(markers: &[AttachReplyMarker], scroll: usize) -> Option<usize> {
    markers
        .iter()
        .enumerate()
        .take_while(|(_, marker)| marker.offset <= scroll)
        .map(|(index, _)| index)
        .last()
}

pub fn attach_marker_is_assistant(label: &str) -> bool {
    ["reply", "assistant", "codex"]
        .into_iter()
        .any(|needle| label.contains(needle))
}

pub fn attach_search_matches(document: &AttachDocument, query: &str) -> Vec<usize> {
    crate::tui::search_matches(&document.lines, query)
}

pub fn attach_match_position(matches: &[usize], scroll: usize) -> usize {
    crate::tui::match_position(matches, scroll)
}

fn attach_transcript_header_line(kind: TranscriptKind, label: &str) -> Line<'static> {
    let color = match kind {
        TranscriptKind::Header => Color::Blue,
        TranscriptKind::User => Color::Magenta,
        TranscriptKind::Assistant => Color::Green,
        TranscriptKind::Exec => Color::Yellow,
        TranscriptKind::Thinking => Color::DarkGray,
    };
    Line::from(vec![
        Span::styled("  ▸ ", Style::default().fg(color)),
        Span::styled(
            label.to_owned(),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

fn attach_transcript_content_line(kind: TranscriptKind, line: &str) -> Line<'static> {
    match kind {
        TranscriptKind::Assistant => attach_markdown_line(line, Color::White),
        TranscriptKind::User => prefixed_attach_line("    ", line, Color::Gray),
        TranscriptKind::Exec => attach_exec_line(line),
        TranscriptKind::Thinking => prefixed_attach_line("    ", line, Color::DarkGray),
        TranscriptKind::Header => prefixed_attach_line("    ", line, Color::DarkGray),
    }
}

fn attach_exec_line(line: &str) -> Line<'static> {
    let color = match crate::transcript::classify_line(line) {
        TranscriptLineKind::Success => Color::Green,
        TranscriptLineKind::Failure | TranscriptLineKind::Error => Color::Red,
        TranscriptLineKind::Quota => Color::Yellow,
        TranscriptLineKind::Command => Color::Cyan,
        TranscriptLineKind::Path => Color::Blue,
        TranscriptLineKind::Plain => Color::Gray,
    };
    prefixed_attach_line("    ", line, color)
}

fn prefixed_attach_line(prefix: &str, line: &str, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(prefix.to_owned(), Style::default().fg(Color::DarkGray)),
        Span::styled(line.to_owned(), Style::default().fg(color)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_index_tracks_current_scroll_position() {
        let markers = vec![
            AttachReplyMarker {
                offset: 3,
                label: "run 1 user".to_owned(),
            },
            AttachReplyMarker {
                offset: 9,
                label: "run 1 assistant".to_owned(),
            },
        ];

        assert_eq!(current_attach_marker_index(&markers, 8), Some(0));
        assert_eq!(current_attach_marker_index(&markers, 9), Some(1));
    }
}
