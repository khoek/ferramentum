use std::process::ExitCode;

mod app;
mod arca;
mod automation;
mod cache;
mod cli;
mod commands;
mod config_store;
mod gpu;
mod http_retry;
mod lifecycle;
mod listing;
mod local;
mod login;
mod model;
mod output;
mod providers;
mod provision;
mod remote;
mod selection;
mod ssh_probe;
mod support;
mod ui;
mod unpack;
mod version;
mod workload;

#[cfg(test)]
mod tests;

fn main() -> ExitCode {
    app::main()
}
