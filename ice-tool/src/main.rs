use std::process::ExitCode;

mod app;
mod arca;
mod cache;
mod cli;
mod commands;
mod config_store;
mod http_retry;
mod listing;
mod local;
mod model;
mod providers;
mod provision;
mod remote;
mod support;
mod ui;
mod unpack;
mod workload;

#[cfg(test)]
mod tests;

fn main() -> ExitCode {
    app::main()
}
