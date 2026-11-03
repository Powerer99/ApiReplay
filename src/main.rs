use clap::Parser;

use apireplay::{
    cli::{Cli, Commands},
    proxy,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Record {
            listen,
            target,
            output,
        } => {
            let listen = listen.parse()?;
            proxy::start(listen, target, output).await?;
        }
    }

    Ok(())
}
