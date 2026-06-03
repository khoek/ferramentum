#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TranscriptKind {
    Header,
    User,
    Assistant,
    Exec,
    Thinking,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptBlock {
    pub kind: TranscriptKind,
    pub label: String,
    pub lines: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TranscriptLineKind {
    Plain,
    Command,
    Success,
    Failure,
    Path,
    Quota,
    Error,
}

pub fn parse(text: &str) -> Vec<TranscriptBlock> {
    let mut blocks = Vec::new();
    let mut current = TranscriptBlock {
        kind: TranscriptKind::Header,
        label: "session".to_owned(),
        lines: Vec::new(),
    };
    for raw_line in text.lines() {
        let line = strip_ansi(raw_line);
        let trimmed = line.trim();
        if let Some(kind) = label_kind(trimmed) {
            if !current.lines.is_empty() {
                blocks.push(current);
            }
            current = TranscriptBlock {
                kind,
                label: trimmed.to_ascii_lowercase(),
                lines: Vec::new(),
            };
        } else if !trimmed.is_empty() || !current.lines.last().is_some_and(String::is_empty) {
            current.lines.push(line);
        }
    }
    if !current.lines.is_empty() {
        blocks.push(current);
    }
    blocks
}

pub fn label_kind(label: &str) -> Option<TranscriptKind> {
    match label.to_ascii_lowercase().as_str() {
        "user" => Some(TranscriptKind::User),
        "codex" | "assistant" => Some(TranscriptKind::Assistant),
        "exec" | "tool" => Some(TranscriptKind::Exec),
        "thinking" | "reasoning" | "analysis" => Some(TranscriptKind::Thinking),
        _ => None,
    }
}

pub fn block_looks_like_thinking(block: &TranscriptBlock) -> bool {
    let label = block.label.to_ascii_lowercase();
    label.contains("thinking")
        || label.contains("reasoning")
        || block.lines.first().is_some_and(|line| {
            let lower = line.trim().to_ascii_lowercase();
            lower == "thinking"
                || lower.starts_with("thinking…")
                || lower.starts_with("thinking...")
        })
}

pub fn classify_line(line: &str) -> TranscriptLineKind {
    let trimmed = line.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("quota") || lower.contains("rate limit") || lower.contains("try again") {
        TranscriptLineKind::Quota
    } else if lower.contains("error")
        || lower.contains("failed")
        || lower.contains("panic")
        || lower.contains("traceback")
    {
        TranscriptLineKind::Error
    } else if lower.starts_with("succeeded in ") {
        TranscriptLineKind::Success
    } else if lower.starts_with("exited ")
        || lower.starts_with("failed in ")
        || lower.contains(" exited with ")
    {
        TranscriptLineKind::Failure
    } else if trimmed.starts_with('/')
        || trimmed.starts_with("./")
        || trimmed.starts_with("../")
        || trimmed.contains("/roles/")
        || trimmed.contains("/runs/")
    {
        TranscriptLineKind::Path
    } else if trimmed.starts_with("$ ")
        || trimmed.starts_with("/bin/")
        || trimmed.starts_with("bash ")
        || trimmed.starts_with("cargo ")
        || trimmed.starts_with("python")
    {
        TranscriptLineKind::Command
    } else {
        TranscriptLineKind::Plain
    }
}

pub fn strip_ansi(line: &str) -> String {
    let mut output = String::new();
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else if ch != '\r' {
            output.push(ch);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_codex_transcript_blocks() {
        let blocks = parse("OpenAI Codex\nuser\nhi\ncodex\nhello\nthinking\nprivate\nexec\nls\n");

        assert!(
            blocks
                .iter()
                .any(|block| block.kind == TranscriptKind::Assistant)
        );
        assert!(
            blocks
                .iter()
                .any(|block| block.kind == TranscriptKind::Thinking)
        );
        assert_eq!(
            classify_line("succeeded in 4ms:"),
            TranscriptLineKind::Success
        );
        assert_eq!(classify_line("quota reached"), TranscriptLineKind::Quota);
    }
}
