#[derive(Clone)]
pub struct TextBuffer {
    lines: Vec<String>,
    cursor_line: usize,
    cursor_col: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    draft_before_history: Vec<String>,
}

impl TextBuffer {
    pub fn new(history: Vec<String>) -> Self {
        Self {
            lines: vec![String::new()],
            cursor_line: 0,
            cursor_col: 0,
            history,
            history_index: None,
            draft_before_history: Vec::new(),
        }
    }

    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    pub fn cursor_line(&self) -> usize {
        self.cursor_line
    }

    pub fn cursor_col(&self) -> usize {
        self.cursor_col
    }

    pub fn history_active(&self) -> bool {
        self.history_index.is_some()
    }

    pub fn current_line(&self) -> &str {
        &self.lines[self.cursor_line]
    }

    pub fn move_to_line_start(&mut self) {
        self.cursor_col = 0;
    }

    pub fn move_to_line_end(&mut self) {
        self.cursor_col = self.current_line().chars().count();
    }

    pub fn move_to_line(&mut self, line: usize) {
        self.cursor_line = line.min(self.lines.len() - 1);
        self.clamp_cursor_col();
    }

    pub fn insert(&mut self, ch: char) {
        self.begin_edit();
        let line = &mut self.lines[self.cursor_line];
        line.insert(char_to_byte_index(line, self.cursor_col), ch);
        self.cursor_col += 1;
    }

    pub fn insert_newline(&mut self) {
        self.begin_edit();
        let line = &mut self.lines[self.cursor_line];
        let tail = line.split_off(char_to_byte_index(line, self.cursor_col));
        self.cursor_line += 1;
        self.cursor_col = 0;
        self.lines.insert(self.cursor_line, tail);
    }

    pub fn backspace(&mut self) {
        self.begin_edit();
        if self.cursor_col > 0 {
            let line = &mut self.lines[self.cursor_line];
            let start = char_to_byte_index(line, self.cursor_col - 1);
            let end = char_to_byte_index(line, self.cursor_col);
            line.replace_range(start..end, "");
            self.cursor_col -= 1;
        } else if self.cursor_line > 0 {
            let removed = self.lines.remove(self.cursor_line);
            self.cursor_line -= 1;
            self.cursor_col = self.lines[self.cursor_line].chars().count();
            self.lines[self.cursor_line].push_str(&removed);
        }
    }

    pub fn delete(&mut self) {
        self.begin_edit();
        if self.cursor_col < self.current_line().chars().count() {
            let start = char_to_byte_index(self.current_line(), self.cursor_col);
            let end = char_to_byte_index(self.current_line(), self.cursor_col + 1);
            self.lines[self.cursor_line].replace_range(start..end, "");
        } else if self.cursor_line + 1 < self.lines.len() {
            let next = self.lines.remove(self.cursor_line + 1);
            self.lines[self.cursor_line].push_str(&next);
        }
    }

    pub fn move_left(&mut self) {
        if self.cursor_col > 0 {
            self.cursor_col -= 1;
        } else if self.cursor_line > 0 {
            self.cursor_line -= 1;
            self.cursor_col = self.lines[self.cursor_line].chars().count();
        }
    }

    pub fn move_right(&mut self) {
        if self.cursor_col < self.current_line().chars().count() {
            self.cursor_col += 1;
        } else if self.cursor_line + 1 < self.lines.len() {
            self.cursor_line += 1;
            self.cursor_col = 0;
        }
    }

    pub fn move_vertical(&mut self, delta: isize) {
        self.cursor_line = if delta < 0 {
            self.cursor_line.saturating_sub(delta.unsigned_abs())
        } else {
            (self.cursor_line + delta as usize).min(self.lines.len() - 1)
        };
        self.clamp_cursor_col();
    }

    pub fn clear(&mut self) {
        self.begin_edit();
        self.lines = vec![String::new()];
        self.cursor_line = 0;
        self.cursor_col = 0;
    }

    pub fn history_previous(&mut self) {
        if self.history.is_empty() {
            return;
        }
        self.history_index = Some(match self.history_index {
            Some(0) => 0,
            Some(index) => index - 1,
            None => {
                self.draft_before_history = self.lines.clone();
                self.history.len() - 1
            }
        });
        self.load_history_entry();
    }

    pub fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            self.history_index = Some(index + 1);
            self.load_history_entry();
        } else {
            self.history_index = None;
            self.lines = if self.draft_before_history.is_empty() {
                vec![String::new()]
            } else {
                std::mem::take(&mut self.draft_before_history)
            };
            self.move_to_line(self.lines.len().saturating_sub(1));
        }
    }

    pub fn set_text(&mut self, value: &str) {
        self.lines = value.lines().map(str::to_owned).collect();
        if self.lines.is_empty() {
            self.lines.push(String::new());
        }
        self.cursor_line = self.lines.len() - 1;
        self.cursor_col = self.current_line().chars().count();
    }

    fn load_history_entry(&mut self) {
        if let Some(index) = self.history_index {
            let value = self.history[index].clone();
            self.set_text(&value);
        }
    }

    fn clamp_cursor_col(&mut self) {
        self.cursor_col = self.cursor_col.min(self.current_line().chars().count());
    }

    fn begin_edit(&mut self) {
        self.history_index = None;
        self.draft_before_history.clear();
    }
}

pub fn char_to_byte_index(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(index, _)| index)
        .unwrap_or(text.len())
}
