use clap::Parser;
use color_eyre::Result;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Stream a transactions CSV and emit account balances as CSV")]
struct Cli {
    /// Path to the transactions CSV input
    input: PathBuf,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    payments_engine::run_from_path(cli.input)?;
    Ok(())
}
