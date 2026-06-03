use ratatui::text::{Line, Span};

pub fn line_text(line: &Line<'_>) -> String {
    line.spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect::<Vec<_>>()
        .join("")
}

pub fn search_matches(lines: &[Line<'_>], query: &str) -> Vec<usize> {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return Vec::new();
    }
    lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            line_text(line)
                .to_ascii_lowercase()
                .contains(&query)
                .then_some(index)
        })
        .collect()
}

pub fn match_position(matches: &[usize], scroll: usize) -> usize {
    matches
        .iter()
        .position(|offset| *offset == scroll)
        .map(|index| index + 1)
        .unwrap_or_else(|| {
            matches
                .iter()
                .take_while(|offset| **offset <= scroll)
                .count()
                .max(1)
        })
}

pub fn text_matches_query<const N: usize>(fields: [String; N], query: &str) -> bool {
    let haystack = fields.join(" ").to_ascii_lowercase();
    query
        .to_ascii_lowercase()
        .split_whitespace()
        .all(|term| haystack.contains(term))
}

pub fn ellipsize_display(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len <= width {
        return value.to_owned();
    }
    match width {
        0 => String::new(),
        1..=3 => ".".repeat(width),
        _ => format!("{}...", value.chars().take(width - 3).collect::<String>()),
    }
}

pub fn wrap_lines(lines: &[Line<'static>], width: usize) -> Vec<Line<'static>> {
    if width == 0 {
        return lines.to_vec();
    }
    let mut wrapped = Vec::new();
    for line in lines {
        wrap_line(line, width, &mut wrapped);
    }
    wrapped
}

fn wrap_line(line: &Line<'static>, width: usize, output: &mut Vec<Line<'static>>) {
    let mut current = Vec::new();
    let mut current_width = 0;
    for span in &line.spans {
        let style = span.style;
        for ch in span.content.chars() {
            if current_width >= width {
                output.push(Line::from(current));
                current = Vec::new();
                current_width = 0;
            }
            current.push(Span::styled(ch.to_string(), style));
            current_width += 1;
        }
    }
    if current.is_empty() {
        output.push(Line::from(""));
    } else {
        let wrapped = Line::from(current);
        output.push(match line.alignment {
            Some(alignment) => wrapped.alignment(alignment),
            None => wrapped,
        });
    }
}

#[cfg(test)]
mod tests {
    use ratatui::style::{Color, Style};

    use super::*;

    #[test]
    fn wraps_styled_lines_without_dropping_text() {
        let lines = vec![Line::from(Span::styled(
            "abcdef",
            Style::default().fg(Color::Green),
        ))];
        let wrapped = wrap_lines(&lines, 2);

        assert_eq!(wrapped.len(), 3);
        assert_eq!(line_text(&wrapped[0]), "ab");
        assert_eq!(line_text(&wrapped[2]), "ef");
    }

    #[test]
    fn finds_query_matches() {
        let lines = vec![Line::from("alpha"), Line::from("beta")];

        assert_eq!(search_matches(&lines, "BET"), vec![1]);
        assert_eq!(match_position(&[2, 5], 4), 1);
    }
}
