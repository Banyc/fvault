use clap::Parser;
use fvault::cli::main::{MainArgs, exec_main};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = MainArgs::try_parse()?;
    exec_main(cli).await?;
    Ok(())
}
