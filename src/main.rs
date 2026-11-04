use clap::Parser;

use apireplay::{
    cli::{Cli, Commands},
    proxy, replay,
};

#[tokio::main]
async fn main() {
    let code = match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            2
        }
    };
    std::process::exit(code.into());
}

async fn run() -> anyhow::Result<u8> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Compare {
            baseline,
            actual,
            output,
            ignore_header,
            ignore_json_path,
        } => {
            return apireplay::diff::comparator::run(
                &baseline,
                &actual,
                output.as_deref(),
                &ignore_header,
                &ignore_json_path,
            );
        }
        Commands::Replay {
            input,
            target,
            output,
            overrides,
            timeout_ms,
        } => {
            return replay::runner::run(&input, &target, &output, overrides.as_deref(), timeout_ms)
                .await;
        }
        Commands::Record {
            listen,
            target,
            output,
        } => {
            let listen = listen.parse()?;
            proxy::start(listen, target, output).await?;
        }
    }

    Ok(0)
}
