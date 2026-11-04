use std::time::Instant;

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{
        Method, StatusCode, Uri,
        header::{
            CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderMap, HeaderName, TE, TRAILER,
            TRANSFER_ENCODING, UPGRADE,
        },
    },
    response::Response,
};
use reqwest::{Client, Url};
use tracing::{error, info};

use crate::recording::{
    RecordedBody, RecordedExchange, RecordedRequest, RecordedResponse, RecordingWriter,
    capture_body, capture_body_with_redactions, collect_sensitive_values, is_sensitive_name,
    model::{MAX_RECORDED_BODY_BYTES, OmissionReason, redact_recorded_body},
    redact_known_secrets, sanitize_headers_with_redactions,
};

#[derive(Clone)]
pub struct ProxyState {
    pub target: Url,
    pub client: Client,
    pub recorder: RecordingWriter,
}

fn is_hop_header(name: &HeaderName) -> bool {
    matches!(
        *name,
        CONNECTION | HOST | CONTENT_LENGTH | TRANSFER_ENCODING | UPGRADE | TE | TRAILER
    ) || matches!(
        name.as_str(),
        "keep-alive" | "proxy-authenticate" | "proxy-authorization"
    )
}

pub fn filter_headers(headers: &HeaderMap) -> HeaderMap {
    let connection_tokens: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect();

    let mut result = HeaderMap::new();
    for (name, value) in headers {
        if !is_hop_header(name) && !connection_tokens.iter().any(|token| token == name.as_str()) {
            result.append(name, value.clone());
        }
    }
    result
}

fn content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn sanitize_path_and_query(uri: &Uri, known_secrets: &mut Vec<String>) -> String {
    let mut url = Url::parse("http://localhost").expect("static URL must be valid");
    url.set_path(uri.path());
    url.set_query(uri.query());

    let query_pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    for (name, value) in &query_pairs {
        if is_sensitive_name(name) && !value.is_empty() {
            known_secrets.push(value.clone());
        }
    }

    let mut path = uri.path().to_owned();
    redact_known_secrets(&mut path, known_secrets);
    if query_pairs.is_empty() {
        return path;
    }

    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, mut value) in query_pairs {
            if is_sensitive_name(&name) {
                value = "[REDACTED]".to_owned();
            } else {
                redact_known_secrets(&mut value, known_secrets);
            }
            query.append_pair(&name, &value);
        }
    }
    format!("{path}?{}", url.query().unwrap_or_default())
}

async fn persist_completed_exchange(
    recorder: &RecordingWriter,
    started_at: Instant,
    request: RecordedRequest,
    response: RecordedResponse,
) {
    let method = request.method.clone();
    let path = request.path_and_query.clone();
    let status = response.status;
    let duration_ms = started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let exchange = RecordedExchange::new(duration_ms, request, response);

    if let Err(error) = recorder.write(&exchange).await {
        error!(%error, %method, %path, status, "failed to write HTTP exchange recording");
    }
    info!(%method, %path, status, duration_ms, "completed HTTP exchange");
}

pub async fn proxy_handler(
    State(state): State<ProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let forwarded_headers = filter_headers(&headers);
    let mut known_secrets = collect_sensitive_values(&headers, &body);
    let path_and_query = sanitize_path_and_query(&uri, &mut known_secrets);
    let mut recorded_request = RecordedRequest {
        method: method.to_string(),
        path_and_query,
        headers: sanitize_headers_with_redactions(&forwarded_headers, &known_secrets),
        body: capture_body_with_redactions(
            &body,
            content_type(&headers).as_deref(),
            &known_secrets,
        ),
    };

    let mut url = state.target.clone();
    let base = state.target.path().trim_end_matches('/');
    url.set_path(&format!("{base}{}", uri.path()));
    url.set_query(uri.query());

    let request = state
        .client
        .request(method.clone(), url)
        .headers(forwarded_headers)
        .body(body);

    let mut upstream = match request.send().await {
        Ok(response) => response,
        Err(upstream_error) => {
            error!(error = %upstream_error, "upstream request failed");
            let response_body = Bytes::from_static(b"Bad Gateway");
            persist_completed_exchange(
                &state.recorder,
                started_at,
                recorded_request,
                RecordedResponse {
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                    headers: [(
                        "content-type".to_owned(),
                        vec!["text/plain; charset=utf-8".to_owned()],
                    )]
                    .into(),
                    body: capture_body(&response_body, Some("text/plain; charset=utf-8")),
                },
            )
            .await;

            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from(response_body))
                .expect("static bad gateway response must be valid");
        }
    };

    let status = upstream.status();
    let response_headers = filter_headers(upstream.headers());
    let response_content_type = content_type(upstream.headers());
    known_secrets.extend(collect_sensitive_values(upstream.headers(), &[]));
    // Hyper need not poll body streams for HEAD/204/304 (or known empty bodies).
    // Persist these completed header-only exchanges before handing off the response.
    if method == Method::HEAD
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED
        || upstream.content_length() == Some(0)
    {
        scrub_request(&mut recorded_request, &known_secrets);
        persist_completed_exchange(
            &state.recorder,
            started_at,
            recorded_request,
            RecordedResponse {
                status: status.as_u16(),
                headers: sanitize_headers_with_redactions(&response_headers, &known_secrets),
                body: RecordedBody::Empty,
            },
        )
        .await;
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        *response.headers_mut() = response_headers;
        return response;
    }
    let response_headers_for_recording = response_headers.clone();
    let recorder = state.recorder;

    // Forward every chunk immediately while retaining at most the recording limit.
    let response_stream = async_stream::stream! {
        let mut captured = Vec::new();
        let mut too_large = false;

        loop {
            match upstream.chunk().await {
                Ok(Some(chunk)) => {
                    if !too_large {
                        if captured.len().saturating_add(chunk.len()) <= MAX_RECORDED_BODY_BYTES {
                            captured.extend_from_slice(&chunk);
                        } else {
                            captured.clear();
                            too_large = true;
                        }
                    }
                    yield Ok::<Bytes, std::io::Error>(chunk);
                }
                Ok(None) => break,
                Err(stream_error) => {
                    error!(error = %stream_error, "upstream response stream failed");
                    yield Err(std::io::Error::other(stream_error));
                    return;
                }
            }
        }

        known_secrets.extend(collect_sensitive_values(&HeaderMap::new(), &captured));
        scrub_request(&mut recorded_request, &known_secrets);
        let recorded_response_headers = sanitize_headers_with_redactions(
            &response_headers_for_recording,
            &known_secrets,
        );
        let mut response_body = if too_large {
            RecordedBody::Omitted {
                content_type: response_content_type,
                reason: OmissionReason::TooLarge,
            }
        } else {
            capture_body_with_redactions(
                &captured,
                response_content_type.as_deref(),
                &known_secrets,
            )
        };
        redact_recorded_body(&mut response_body, &known_secrets);
        persist_completed_exchange(
            &recorder,
            started_at,
            recorded_request,
            RecordedResponse {
                status: status.as_u16(),
                headers: recorded_response_headers,
                body: response_body,
            },
        )
        .await;
    };

    let mut response = Response::new(Body::from_stream(response_stream));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}

fn scrub_request(request: &mut RecordedRequest, secrets: &[String]) {
    redact_known_secrets(&mut request.path_and_query, secrets);
    for values in request.headers.values_mut() {
        for value in values {
            redact_known_secrets(value, secrets);
        }
    }
    redact_recorded_body(&mut request.body, secrets);
}
