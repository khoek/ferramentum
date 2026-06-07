use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use std::borrow::Cow;

use crate::tui::ellipsize_display;
use crate::ui;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PaletteGroup {
    Current,
    Navigate,
    Operate,
    Maintenance,
    Help,
}

impl PaletteGroup {
    pub const ALL: [Self; 5] = [
        Self::Current,
        Self::Navigate,
        Self::Operate,
        Self::Maintenance,
        Self::Help,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Current => "Current",
            Self::Navigate => "Navigate",
            Self::Operate => "Operate",
            Self::Maintenance => "Maintenance",
            Self::Help => "Help",
        }
    }
}

pub fn draw_dashboard_footer(frame: &mut Frame<'_>, area: Rect, fallback: Line<'static>) {
    frame.render_widget(
        Paragraph::new(fallback).style(Style::default().fg(Color::White).bg(Color::Black)),
        area,
    );
}

pub fn footer_line_from_pairs(
    pairs: &[(&'static str, &'static str)],
    width: usize,
) -> Line<'static> {
    footer_line_from_items(pairs.iter().copied().map(FooterPair::from), width)
}

pub struct FooterPair {
    key: Cow<'static, str>,
    label: Cow<'static, str>,
}

impl FooterPair {
    pub fn new(key: impl Into<Cow<'static, str>>, label: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
        }
    }
}

impl From<(&'static str, &'static str)> for FooterPair {
    fn from((key, label): (&'static str, &'static str)) -> Self {
        Self::new(key, label)
    }
}

pub fn footer_line_from_items(
    pairs: impl IntoIterator<Item = FooterPair>,
    width: usize,
) -> Line<'static> {
    let mut accepted = Vec::new();
    let mut used = 0;
    let mut omitted = false;
    for pair in pairs {
        let pair_width = footer_pair_width(&pair, !accepted.is_empty());
        if used + pair_width <= width {
            accepted.push(pair);
            used += pair_width;
        } else {
            omitted = true;
        }
    }
    if omitted {
        let ellipsis_width = if accepted.is_empty() { 1 } else { 3 };
        while !accepted.is_empty() && used + ellipsis_width > width {
            let pair = accepted.pop().expect("accepted was checked non-empty");
            used = used.saturating_sub(footer_pair_width(&pair, !accepted.is_empty()));
        }
    }

    let mut spans: Vec<Span<'static>> = Vec::new();
    for pair in accepted {
        if !spans.is_empty() {
            spans.push(footer_text("  "));
        }
        spans.push(footer_text("("));
        spans.push(footer_key_span(pair.key));
        spans.push(footer_text(" "));
        spans.push(footer_hint_label_span(pair.label));
        spans.push(footer_text(")"));
    }
    let ellipsis_width = if spans.is_empty() { 1 } else { 3 };
    if omitted && used + ellipsis_width <= width {
        if !spans.is_empty() {
            spans.push(footer_text("  "));
        }
        spans.push(footer_text("…"));
    }
    Line::from(spans)
}

fn footer_pair_width(pair: &FooterPair, needs_separator: bool) -> usize {
    usize::from(needs_separator) * 2 + pair.key.chars().count() + 3 + pair.label.chars().count()
}

pub fn footer_key(value: &'static str) -> Span<'static> {
    footer_key_span(Cow::Borrowed(value))
}

fn footer_key_span(value: Cow<'static, str>) -> Span<'static> {
    Span::styled(
        value,
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn footer_text(value: &'static str) -> Span<'static> {
    Span::styled(value, Style::default().fg(Color::Gray).bg(Color::Black))
}

fn footer_hint_label_span(value: Cow<'static, str>) -> Span<'static> {
    Span::styled(
        value,
        Style::default()
            .fg(Color::DarkGray)
            .bg(Color::Black)
            .add_modifier(Modifier::ITALIC),
    )
}

pub fn palette_detail(
    label: &str,
    key: Option<&'static str>,
    detail: &str,
    width: usize,
) -> String {
    let key_width = key.map(|key| key.chars().count() + 4).unwrap_or(0);
    let fixed_width = 2 + label.chars().count() + key_width + ui::FIELD_SEPARATOR.chars().count();
    let detail_width = width.saturating_sub(fixed_width);
    if detail_width == 0 {
        String::new()
    } else {
        format!(
            "{}{}",
            ui::FIELD_SEPARATOR,
            ellipsize_display(detail, detail_width)
        )
    }
}
