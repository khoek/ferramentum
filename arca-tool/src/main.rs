mod app;
mod artifact;
mod backend;
mod cli;
mod command;
mod config;
mod gcp;
mod runtime;
mod ui;

use clap::Parser;

use crate::cli::Cli;

fn main() {
    let cli = Cli::parse();
    if let Err(err) = app::run(cli) {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}
