use clap::{Parser, Subcommand};


#[derive(Parser)]
#[command(name = "apireplay")]
#[command(about = "Record and replay HTTP API traffic")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start an HTTP recording proxy
    Record {
        /// The address to listen on (default: 127.0.0.1:8080)
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,

        /// Target API URL
        #[arg(long)]
        target: String,
    },
}