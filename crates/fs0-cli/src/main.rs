mod cli;
mod commands;
mod output;

use clap::Parser;
use fs0_core::Fs0Result;
use std::process::ExitCode;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> ExitCode {
    fmt()
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("fs0=info")),
        )
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fs0: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Fs0Result<()> {
    commands::run(cli::Cli::parse()).await
}
