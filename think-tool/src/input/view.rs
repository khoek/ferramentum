use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::input::buffer::TextBuffer;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WrappedCursor {
    pub row: usize,
    pub col: usize,
}

#[derive(Clone, Copy)]
pub enum CursorRender {
    Terminal,
    InlineMarker { marker: char, style: Style },
}

pub trait EditableText {
    fn lines(&self) -> &[String];

    fn cursor_line(&self) -> usize;

    fn cursor_col(&self) -> usize;
}

impl EditableText for TextBuffer {
    fn lines(&self) -> &[String] {
        self.lines()
    }

    fn cursor_line(&self) -> usize {
        self.cursor_line()
    }

    fn cursor_col(&self) -> usize {
        self.cursor_col()
    }
}

pub struct WrappedInput<'a, T: EditableText + ?Sized> {
    input: &'a T,
    width: usize,
    style: Style,
    cursor: CursorRender,
}

impl<'a, T: EditableText + ?Sized> WrappedInput<'a, T> {
    pub fn new(input: &'a T, width: usize) -> Self {
        Self {
            input,
            width: width.max(1),
            style: Style::default(),
            cursor: CursorRender::Terminal,
        }
    }

    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    pub fn cursor(mut self, cursor: CursorRender) -> Self {
        self.cursor = cursor;
        self
    }

    pub fn layout(self) -> WrappedInputLayout {
        let mut lines = Vec::new();
        let mut cursor = WrappedCursor { row: 0, col: 0 };
        for (line_index, source_line) in self.input.lines().iter().enumerate() {
            let start_row = lines.len();
            let is_cursor_line = line_index == self.input.cursor_line();
            let inline_cursor = if is_cursor_line {
                match self.cursor {
                    CursorRender::Terminal => None,
                    CursorRender::InlineMarker { marker, style } => Some(InlineCursor {
                        col: self.input.cursor_col(),
                        marker,
                        style,
                    }),
                }
            } else {
                None
            };
            if is_cursor_line {
                cursor = cursor_position(
                    source_line,
                    self.input.cursor_col(),
                    self.width,
                    self.cursor,
                );
                cursor.row += start_row;
            }
            push_wrapped_line(
                &mut lines,
                source_line,
                self.width,
                self.style,
                inline_cursor,
            );
        }
        WrappedInputLayout { lines, cursor }
    }
}

pub struct WrappedInputLayout {
    lines: Vec<Line<'static>>,
    cursor: WrappedCursor,
}

impl WrappedInputLayout {
    pub fn cursor(&self) -> WrappedCursor {
        self.cursor
    }

    pub fn max_scroll(&self, height: usize) -> usize {
        self.lines.len().saturating_sub(height)
    }

    pub fn scroll_for_cursor(&self, mut scroll: usize, height: usize) -> usize {
        if height == 0 {
            return 0;
        }
        if self.cursor.row < scroll {
            scroll = self.cursor.row;
        } else if self.cursor.row >= scroll + height {
            scroll = self.cursor.row + 1 - height;
        }
        scroll.min(self.max_scroll(height))
    }

    pub fn visible_lines(
        &self,
        scroll: usize,
        height: usize,
        filler: Line<'static>,
    ) -> Vec<Line<'static>> {
        let mut visible = self
            .lines
            .iter()
            .skip(scroll)
            .take(height)
            .cloned()
            .collect::<Vec<_>>();
        while visible.len() < height {
            visible.push(filler.clone());
        }
        visible
    }
}

#[derive(Clone, Copy)]
struct InlineCursor {
    col: usize,
    marker: char,
    style: Style,
}

#[derive(Clone, Copy)]
struct StyledCell {
    ch: char,
    style: Style,
}

fn cursor_position(
    line: &str,
    cursor_col: usize,
    width: usize,
    cursor: CursorRender,
) -> WrappedCursor {
    let len = line.chars().count();
    let cursor_col = cursor_col.min(len);
    let display_col = match cursor {
        CursorRender::Terminal
            if cursor_col == len && cursor_col > 0 && cursor_col % width == 0 =>
        {
            cursor_col - 1
        }
        _ => cursor_col,
    };
    WrappedCursor {
        row: display_col / width,
        col: display_col % width,
    }
}

fn wrapped_cells(
    line: &str,
    text_style: Style,
    inline_cursor: Option<InlineCursor>,
) -> Vec<StyledCell> {
    let line_len = line.chars().count();
    let mut cells = Vec::with_capacity(line_len + usize::from(inline_cursor.is_some()));
    let cursor_col = inline_cursor.map(|cursor| cursor.col.min(line_len));
    for (col, ch) in line.chars().enumerate() {
        if cursor_col == Some(col) {
            let cursor = inline_cursor.expect("cursor column came from cursor");
            cells.push(StyledCell {
                ch: cursor.marker,
                style: cursor.style,
            });
        }
        cells.push(StyledCell {
            ch,
            style: text_style,
        });
    }
    if cursor_col == Some(line_len) {
        let cursor = inline_cursor.expect("cursor column came from cursor");
        cells.push(StyledCell {
            ch: cursor.marker,
            style: cursor.style,
        });
    }
    cells
}

fn line_from_cells(cells: &[StyledCell]) -> Line<'static> {
    if cells.is_empty() {
        return Line::from("");
    }
    let mut spans = Vec::new();
    let mut text = String::new();
    let mut style = cells[0].style;
    for cell in cells {
        if cell.style != style {
            spans.push(Span::styled(std::mem::take(&mut text), style));
            style = cell.style;
        }
        text.push(cell.ch);
    }
    if !text.is_empty() {
        spans.push(Span::styled(text, style));
    }
    Line::from(spans)
}

fn empty_line(text_style: Style, inline_cursor: Option<InlineCursor>) -> Line<'static> {
    if let Some(cursor) = inline_cursor {
        Line::from(Span::styled(cursor.marker.to_string(), cursor.style))
    } else {
        Line::from(Span::styled(String::new(), text_style))
    }
}

fn push_wrapped_line(
    output: &mut Vec<Line<'static>>,
    line: &str,
    width: usize,
    text_style: Style,
    inline_cursor: Option<InlineCursor>,
) {
    if line.is_empty() {
        output.push(empty_line(text_style, inline_cursor));
        return;
    }
    for cells in wrapped_cells(line, text_style, inline_cursor).chunks(width) {
        output.push(line_from_cells(cells));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_long_input_rows() {
        let mut buffer = TextBuffer::new(Vec::new());
        buffer.set_text("abcdef");

        let layout = WrappedInput::new(&buffer, 3).layout();

        assert_eq!(line_text(&layout.lines[0]), "abc");
        assert_eq!(line_text(&layout.lines[1]), "def");
        assert_eq!(layout.cursor(), WrappedCursor { row: 1, col: 2 });
    }

    #[test]
    fn inline_cursor_marker_wraps_at_boundary() {
        let mut buffer = TextBuffer::new(Vec::new());
        buffer.set_text("abc");

        let layout = WrappedInput::new(&buffer, 3)
            .cursor(CursorRender::InlineMarker {
                marker: '|',
                style: Style::default(),
            })
            .layout();

        assert_eq!(line_text(&layout.lines[0]), "abc");
        assert_eq!(line_text(&layout.lines[1]), "|");
    }

    #[test]
    fn scroll_tracks_cursor_in_wrapped_rows() {
        let mut buffer = TextBuffer::new(Vec::new());
        buffer.set_text("abcdefghij");

        let layout = WrappedInput::new(&buffer, 2).layout();

        assert_eq!(layout.scroll_for_cursor(0, 3), 2);
        assert_eq!(layout.visible_lines(2, 3, Line::from("")).len(), 3);
    }

    #[test]
    fn cursor_row_is_not_offset_by_later_lines() {
        let mut buffer = TextBuffer::new(Vec::new());
        buffer.set_text("abc\ndef\nghi");
        buffer.move_to_line(1);
        buffer.move_to_line_end();

        let layout = WrappedInput::new(&buffer, 10).layout();

        assert_eq!(layout.cursor(), WrappedCursor { row: 1, col: 3 });
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<Vec<_>>()
            .join("")
    }
}
