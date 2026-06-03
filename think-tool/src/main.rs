mod agent;
mod app;
mod cli;
mod config;
mod git;
mod ids;
mod input_history;
mod io;
mod lock;
mod maintenance;
mod prompt;
mod provider;
mod runner;
mod selection;
mod session;
mod state;
mod template;
mod terminal_editor;
mod transcript;
mod tui_text;
mod ui;

use std::any::Any;
use std::panic;
use std::process::ExitCode;

use clap::Parser;

use crate::cli::Cli;

fn main() -> ExitCode {
    install_broken_pipe_hook();
    match panic::catch_unwind(|| app::run(Cli::parse())) {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(err)) if terminal_editor::is_cancelled(&err) => {
            if let Some(message) = terminal_editor::cancellation_message(&err) {
                eprintln!("{message}");
            }
            ExitCode::SUCCESS
        }
        Ok(Err(err)) => {
            eprintln!("{err:#}");
            ExitCode::FAILURE
        }
        Err(payload) if panic_payload_is_broken_pipe(payload.as_ref()) => ExitCode::SUCCESS,
        Err(payload) => panic::resume_unwind(payload),
    }
}

fn install_broken_pipe_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        if panic_payload_is_broken_pipe(info.payload()) {
            return;
        }
        default_hook(info);
    }));
}

fn panic_payload_is_broken_pipe(payload: &(dyn Any + Send)) -> bool {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied());
    message.is_some_and(|message| {
        message.contains("failed printing to stdout") && message.contains("Broken pipe")
    })
}
