use std::io::{self, IsTerminal};

use serde::{Deserialize, Serialize};

const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_BOLD_RED: &str = "\x1b[1;31m";
const ANSI_BOLD_WHITE_RED_BG: &str = "\x1b[1;37;41m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_STRIKETHROUGH: &str = "\x1b[9m";
const ANSI_YELLOW: &str = "\x1b[33m";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Color {
    Blue,
    Cyan,
    Green,
    Red,
    Yellow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) enum TextEffect {
    #[default]
    None,
    Strikethrough,
}

pub(crate) trait RenderTarget {
    fn style(&self, text: &str, color: Option<Color>, effect: TextEffect) -> String;

    fn paint(&self, text: &str, color: Color) -> String {
        self.style(text, Some(color), TextEffect::None)
    }

    fn effect(&self, text: &str, effect: TextEffect) -> String {
        self.style(text, None, effect)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StdoutRenderTarget;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StderrRenderTarget;

pub(crate) fn stdout_is_interactive() -> bool {
    io::stdout().is_terminal()
}

pub(crate) fn stderr_is_interactive() -> bool {
    io::stderr().is_terminal()
}

pub(crate) fn print_big_red_error(message: &str) {
    if stderr_is_interactive() {
        eprintln!(
            "{} ERROR {} {}{}{}",
            ANSI_BOLD_WHITE_RED_BG, ANSI_RESET, ANSI_BOLD_RED, message, ANSI_RESET
        );
    } else {
        eprintln!("ERROR: {message}");
    }
}

pub(crate) fn print_stage(message: &str) {
    let prefix = StderrRenderTarget.paint("==>", Color::Cyan);
    eprintln!("{prefix} {message}");
}

impl RenderTarget for StdoutRenderTarget {
    fn style(&self, text: &str, color: Option<Color>, effect: TextEffect) -> String {
        style_for_terminal(text, color, effect, stdout_is_interactive())
    }
}

impl RenderTarget for StderrRenderTarget {
    fn style(&self, text: &str, color: Option<Color>, effect: TextEffect) -> String {
        style_for_terminal(text, color, effect, stderr_is_interactive())
    }
}

fn ansi_code(color: Color) -> &'static str {
    match color {
        Color::Blue => ANSI_BLUE,
        Color::Cyan => ANSI_CYAN,
        Color::Green => ANSI_GREEN,
        Color::Red => ANSI_BOLD_RED,
        Color::Yellow => ANSI_YELLOW,
    }
}

fn style_for_terminal(
    text: &str,
    color: Option<Color>,
    effect: TextEffect,
    is_interactive: bool,
) -> String {
    if !is_interactive || (color.is_none() && effect == TextEffect::None) {
        return text.to_owned();
    }
    let mut prefix = String::new();
    if effect == TextEffect::Strikethrough {
        prefix.push_str(ANSI_STRIKETHROUGH);
    }
    if let Some(color) = color {
        prefix.push_str(ansi_code(color));
    }
    format!("{prefix}{text}{ANSI_RESET}")
}
