pub mod handler;

use std::net::SocketAddr;

use axum::{extract::DefaultBodyLimit, Router};
use axum::routing::any;
use reqwest::{redirect::Policy, Client, Url};

use crate::proxy::handler::{proxy_handler, ProxyState};

pub async fn start(
    listen: SocketAddr,
    target: String,
) -> anyhow::Result<()> {
    let url = Url::parse(&target)?;

    // MVP: only forward requests to loopback HTTPS servers.

    anyhow::ensure!(
        url.scheme() == "http" && matches!(
            url.host_str(),
            Some("localhost" | "127.0.0.1" |"[::1]")
        ),
        "Target must be a local HTTP server (http://localhost or http://127.0.0.1 or http://[::1])"
    );

    anyhow::ensure!(
        listen.ip().is_loopback(),
        "APIReplay must listen on a loopback address"
    );

    let client = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .build()?;

    let state = ProxyState {
        target: url,
        client,
    };

    let app = Router::new()
        .fallback(any(proxy_handler))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // 10 MB
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;

    println!("APIReplay listening on https://{listen} and forwarding to {target}");

    axum::serve(listener, app).await?;

    Ok(())
}