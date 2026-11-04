use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    /// Compare recorded responses with correlated replay results
    Compare {
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        actual: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Ignore a response header (repeatable, case insensitive)
        #[arg(long)]
        ignore_header: Vec<String>,
        /// Ignore a JSON body field by JSON Pointer (repeatable)
        #[arg(long)]
        ignore_json_path: Vec<String>,
    },
    /// Replay supported recorded requests against a local HTTP service
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        target: String,
        #[arg(long)]
        output: PathBuf,
        /// JSON object mapping one-based recording line numbers to complete replacement requests
        #[arg(long)]
        overrides: Option<PathBuf>,
        #[arg(long, default_value_t = 10000, value_parser = clap::value_parser!(u64).range(1..=600000))]
        timeout_ms: u64,
    },
    /// Start an HTTP recording proxy
    Record {
        /// The address to listen on (default: 127.0.0.1:8080)
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,

        /// Target API URL
        #[arg(long)]
        target: String,

        /// JSONL file to append recorded HTTP exchanges to
        #[arg(long)]
        output: PathBuf,
    },
}
