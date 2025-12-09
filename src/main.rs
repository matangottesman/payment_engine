use std::{io, path::PathBuf};

use clap::Parser;
use color_eyre::Result;
use payments_engine::Engine;

#[derive(Debug, Parser)]
#[command(about = "Payment engine that tracks and emits account balances from an input transaction stream")]
struct Cli {
    input_transactions_file: PathBuf,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    // Write errors to std::error. I don't want to assume that I can open a file for logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let mut engine = Engine::new();
    engine.apply_transactions_from_file(cli.input_transactions_file)?;
    engine.write_accounts(io::stdout())?;
    Ok(())
}
