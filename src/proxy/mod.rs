pub mod handler;

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use axum::{Router, extract::DefaultBodyLimit, routing::any};
use reqwest::{Client, Url, redirect::Policy};
use tracing::info;

use crate::{
    proxy::handler::{ProxyState, proxy_handler},
    recording::RecordingWriter,
};

pub async fn start(listen: SocketAddr, target: String, output: PathBuf) -> anyhow::Result<()> {
    let url = Url::parse(&target)?;

    anyhow::ensure!(
        url.scheme() == "http"
            && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")),
        "Target must be a local HTTP server (http://localhost or http://127.0.0.1 or http://[::1])"
    );

    anyhow::ensure!(
        listen.ip().is_loopback(),
        "APIReplay must listen on a loopback address"
    );

    let recorder = RecordingWriter::create(&output)
        .await
        .with_context(|| format!("failed to initialize recording output {}", output.display()))?;
    let client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .build()?;

    let state = ProxyState {
        target: url,
        client,
        recorder,
    };

    let app = Router::new()
        .fallback(any(proxy_handler))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    let listen = listener.local_addr()?;

    info!(%listen, %target, output = %output.display(), "APIReplay recording proxy started");

    axum::serve(listener, app).await?;

    Ok(())
}
