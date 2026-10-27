
mod cli;
mod proxy;

use clap::Parser;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Record { listen, target } => {
            let listen = listen.parse()?;
            proxy::start(listen, target).await?;
        }
    }

    Ok(())
}
