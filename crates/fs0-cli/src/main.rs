mod cli;
mod commands;
mod output;

use clap::Parser;
use fs0_core::Fs0Result;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
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
