mod cli;
mod commands;
mod model;
mod util;

use anyhow::Result;
use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, Commands};

fn main() {
    init_tracing();

    if let Err(err) = run() {
        error!(error = %err, "command failed");
        for cause in err.chain().skip(1) {
            error!(cause = %cause, "caused by");
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inventory(args) => commands::inventory::run(args),
        Commands::Ingest(args) => commands::ingest::run(args),
        Commands::Status(args) => commands::status::run(args),
    }
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}
