use std::io::{self, IsTerminal};
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Result, bail};
use dialoguer::theme::ColorfulTheme;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_BOLD_CYAN: &str = "\x1b[1;36m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_BOLD_GREEN: &str = "\x1b[1;32m";
const ANSI_BOLD_YELLOW: &str = "\x1b[1;33m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_RED: &str = "\x1b[31m";
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_YELLOW: &str = "\x1b[33m";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    Blue,
    Cyan,
    Green,
    Red,
    Yellow,
}

pub trait RenderTarget {
    fn paint(&self, text: &str, color: Color) -> String;
    fn hyperlink(&self, text: &str, url: Option<&str>) -> String;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StdoutRenderTarget;

#[derive(Debug, Clone, Copy, Default)]
pub struct StderrRenderTarget;

pub fn spinner(message: &str) -> ProgressBar {
    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    let style = ProgressStyle::with_template("{spinner:.cyan} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    progress.set_style(style);
    progress.enable_steady_tick(Duration::from_millis(90));
    progress.set_message(message.to_owned());
    progress
}

pub fn stdout_render_target() -> StdoutRenderTarget {
    StdoutRenderTarget
}

pub fn stderr_render_target() -> StderrRenderTarget {
    StderrRenderTarget
}

pub fn stdin_is_interactive() -> bool {
    io::stdin().is_terminal()
}

pub fn stdout_is_interactive() -> bool {
    io::stdout().is_terminal()
}

pub fn stderr_is_interactive() -> bool {
    io::stderr().is_terminal()
}

pub fn require_interactive(message: &str) -> Result<()> {
    if !stdin_is_interactive() {
        bail!("{message}");
    }
    Ok(())
}

pub fn prompt_theme() -> &'static ColorfulTheme {
    static THEME: LazyLock<ColorfulTheme> = LazyLock::new(ColorfulTheme::default);
    &THEME
}

pub fn maybe_open_browser(url: &str) {
    if let Err(err) = webbrowser::open(url) {
        eprintln!("Could not open browser automatically: {err}");
        eprintln!("Open this URL manually: {url}");
    }
}

pub fn warn(message: &str) {
    eprintln!("{ANSI_BOLD_YELLOW}warning:{ANSI_RESET} {message}");
}

pub fn stage(title: &str) {
    if stderr_is_interactive() {
        eprintln!("{ANSI_BOLD_CYAN}==>{ANSI_RESET} {title}");
    } else {
        eprintln!("==> {title}");
    }
}

pub fn detail(message: &str) {
    eprintln!("    {message}");
}

pub fn success(message: &str) {
    if stderr_is_interactive() {
        eprintln!("{ANSI_BOLD_GREEN}ok:{ANSI_RESET} {message}");
    } else {
        eprintln!("ok: {message}");
    }
}

impl RenderTarget for StdoutRenderTarget {
    fn paint(&self, text: &str, color: Color) -> String {
        paint_for_terminal(text, color, stdout_is_interactive())
    }

    fn hyperlink(&self, text: &str, url: Option<&str>) -> String {
        if !stdout_is_interactive() {
            return text.to_owned();
        }
        let Some(url) = url else {
            return text.to_owned();
        };
        format!("\x1b]8;;{url}\x1b\\{text}\x1b]8;;\x1b\\")
    }
}

impl RenderTarget for StderrRenderTarget {
    fn paint(&self, text: &str, color: Color) -> String {
        paint_for_terminal(text, color, stderr_is_interactive())
    }

    fn hyperlink(&self, text: &str, _url: Option<&str>) -> String {
        text.to_owned()
    }
}

fn ansi_code(color: Color) -> &'static str {
    match color {
        Color::Blue => ANSI_BLUE,
        Color::Cyan => ANSI_CYAN,
        Color::Green => ANSI_GREEN,
        Color::Red => ANSI_RED,
        Color::Yellow => ANSI_YELLOW,
    }
}

fn paint_for_terminal(text: &str, color: Color, is_interactive: bool) -> String {
    if !is_interactive {
        return text.to_owned();
    }
    format!("{}{text}{ANSI_RESET}", ansi_code(color))
}
