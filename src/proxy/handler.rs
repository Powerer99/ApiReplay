
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{
        header::{
            HeaderMap, HeaderName, CONNECTION, CONTENT_LENGTH,
            HOST, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
        },
        Method, StatusCode, Uri,
    },
    response::Response,
};

use reqwest::{Client, Url};

#[derive(Clone)]
pub struct ProxyState {
    pub target: Url,
    pub client: Client,
}

fn is_hop_header(name: &HeaderName) -> bool {
    matches!(
        *name,
        CONNECTION | HOST | CONTENT_LENGTH |
        TRANSFER_ENCODING | UPGRADE | TE | TRAILER
    ) || matches!(
        name.as_str(),
        "keep-alive" | "proxy-authenticate" |
        "proxy-authorization"
    )
}

fn filter_headers(headers: &HeaderMap) -> HeaderMap {
    let mut result = HeaderMap::new();

    let connection_tokens: Vec<String> = headers
        .get(CONNECTION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(',')
        .map(|v| v.trim().to_ascii_lowercase())
        .collect();

    for (name, value) in headers {
        if !is_hop_header(name)
            && !connection_tokens.iter().any(|v| v == name.as_str())
        {
            result.append(name, value.clone());
        }
    }

    result
}

pub async fn proxy_handler(
    State(state): State<ProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut url = state.target.clone();

    let base = state.target.path().trim_end_matches('/');
    let path = uri.path();

    url.set_path(&format!("{base}{path}"));
    url.set_query(uri.query());

    println!("{} {}", method, url);

    let request = state.client
        .request(method, url)
        .headers(filter_headers(&headers))
        .body(body);

    let upstream = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            eprintln!("Upstream error: {error}");

            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from("Bad Gateway"))
                .unwrap();
        }
    };

    let status = upstream.status();
    let headers = filter_headers(upstream.headers());

    // Stream the upstream response rather than buffer it.
    let mut response = Response::new(
        Body::from_stream(upstream.bytes_stream())
    );

    *response.status_mut() = status;
    *response.headers_mut() = headers;

    response
}
