use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    proxy::handler::filter_headers,
    recording::{
        RecordedBody, RecordedExchange, RecordedRequest, RecordedResponse,
        capture_body_with_redactions, collect_sensitive_values, is_sensitive_name,
        model::{MAX_RECORDED_BODY_BYTES, OmissionReason, redact_recorded_body},
        sanitize_headers_with_redactions,
    },
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayResult {
    pub schema_version: u8,
    pub source_index: usize,
    pub outcome: Outcome,
    pub reason: Option<String>,
    pub duration_ms: u64,
    pub response: Option<RecordedResponse>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Skipped,
    Failed,
}

pub fn local_target(target: &str) -> anyhow::Result<Url> {
    let url = Url::parse(target).map_err(|_| anyhow::anyhow!("invalid target URL"))?;
    ensure!(
        url.scheme() == "http"
            && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "target must be a local HTTP URL without credentials, query or fragment"
    );
    Ok(url)
}

pub fn read_recordings(path: &Path) -> anyhow::Result<Vec<RecordedExchange>> {
    let contents = std::fs::read_to_string(path).context("cannot read recording input")?;
    let mut records = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let record: RecordedExchange = serde_json::from_str(line)
            .map_err(|_| anyhow::anyhow!("invalid recording at line {}", index + 1))?;
        ensure!(
            record.schema_version == 1,
            "unsupported schema version at line {}",
            index + 1
        );
        ensure!(
            (100..=599).contains(&record.response.status),
            "invalid response status at line {}",
            index + 1
        );
        records.push(record);
    }
    ensure!(!records.is_empty(), "recording is empty");
    Ok(records)
}

pub fn has_redactions(value: &impl Serialize) -> bool {
    let text = serde_json::to_string(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    text.contains("[redacted]") || text.contains("%5bredacted%5d")
}

struct Prepared {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Vec<u8>,
    secrets: Vec<String>,
    skip: bool,
}

fn prepare(request: &RecordedRequest, target: &Url) -> anyhow::Result<Prepared> {
    let method = Method::from_bytes(request.method.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid request method"))?;
    ensure!(method != Method::CONNECT, "CONNECT replay is unsupported");
    let path = &request.path_and_query;
    ensure!(
        path.starts_with('/') && !path.starts_with("//") && !path.contains('#'),
        "request path must be origin-form without fragment"
    );
    let (path, query) = path
        .split_once('?')
        .map_or((path.as_str(), None), |(p, q)| (p, Some(q)));
    let mut url = target.clone();
    url.set_path(&format!("{}{}", target.path().trim_end_matches('/'), path));
    url.set_query(query);
    let mut headers = HeaderMap::new();
    for (name, values) in &request.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| anyhow::anyhow!("invalid request header name"))?;
        for value in values {
            headers.append(
                name.clone(),
                HeaderValue::from_str(value)
                    .map_err(|_| anyhow::anyhow!("invalid request header value"))?,
            );
        }
    }
    let (body, content_type) = match &request.body {
        RecordedBody::Empty | RecordedBody::Omitted { .. } => (Vec::new(), None),
        RecordedBody::Text {
            content,
            content_type,
        } => (content.as_bytes().to_vec(), content_type.as_deref()),
        RecordedBody::Json {
            content,
            content_type,
        } => (serde_json::to_vec(content)?, content_type.as_deref()),
    };
    ensure!(
        body.len() <= 10 * 1024 * 1024,
        "request body exceeds 10 MiB"
    );
    if !headers.contains_key("content-type")
        && let Some(content_type) = content_type
    {
        headers.insert(
            "content-type",
            HeaderValue::from_str(content_type)
                .map_err(|_| anyhow::anyhow!("invalid content type"))?,
        );
    }
    let mut secrets = collect_sensitive_values(&headers, &body);
    for (name, value) in url.query_pairs() {
        if is_sensitive_name(&name) && !value.is_empty() {
            secrets.push(value.into_owned());
        }
    }
    Ok(Prepared {
        method,
        url,
        headers: filter_headers(&headers),
        body,
        secrets,
        skip: has_redactions(request) || matches!(request.body, RecordedBody::Omitted { .. }),
    })
}

async fn execute(client: &Client, prepared: Prepared) -> Result<RecordedResponse, &'static str> {
    let mut secrets = prepared.secrets;
    let mut upstream = client
        .request(prepared.method, prepared.url)
        .headers(prepared.headers)
        .body(prepared.body)
        .send()
        .await
        .map_err(|_| "upstream request failed or timed out")?;
    let status = upstream.status().as_u16();
    secrets.extend(collect_sensitive_values(upstream.headers(), &[]));
    let headers = filter_headers(upstream.headers());
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut captured = Vec::new();
    let mut too_large = false;
    while let Some(chunk) = upstream
        .chunk()
        .await
        .map_err(|_| "upstream stream failed or timed out")?
    {
        if !too_large {
            if captured.len().saturating_add(chunk.len()) > MAX_RECORDED_BODY_BYTES {
                captured.clear();
                too_large = true;
            } else {
                captured.extend_from_slice(&chunk);
            }
        }
    }
    secrets.extend(collect_sensitive_values(&HeaderMap::new(), &captured));
    let mut body = if too_large {
        RecordedBody::Omitted {
            content_type,
            reason: OmissionReason::TooLarge,
        }
    } else {
        capture_body_with_redactions(&captured, content_type.as_deref(), &secrets)
    };
    redact_recorded_body(&mut body, &secrets);
    Ok(RecordedResponse {
        status,
        headers: sanitize_headers_with_redactions(&headers, &secrets),
        body,
    })
}

pub async fn run(
    input: &Path,
    target: &str,
    output: &Path,
    overrides: Option<&Path>,
    timeout_ms: u64,
) -> anyhow::Result<u8> {
    ensure!(
        (1..=600000).contains(&timeout_ms),
        "timeout must be between 1 and 600000 ms"
    );
    let target = local_target(target)?;
    let records = read_recordings(input)?;
    let overrides: BTreeMap<usize, RecordedRequest> = match overrides {
        Some(path) => {
            serde_json::from_slice(&std::fs::read(path).context("cannot read overrides file")?)
                .map_err(|_| {
                    anyhow::anyhow!(
                        "invalid overrides file; expected line-number to complete request mapping"
                    )
                })?
        }
        None => BTreeMap::new(),
    };
    ensure!(
        overrides.keys().all(|i| *i > 0 && *i <= records.len()),
        "override index outside recording"
    );
    let mut prepared = Vec::new();
    for (index, record) in records.iter().enumerate() {
        prepared.push(
            prepare(
                overrides.get(&(index + 1)).unwrap_or(&record.request),
                &target,
            )
            .with_context(|| format!("invalid request at line {}", index + 1))?,
        );
    }
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(Duration::from_millis(timeout_ms))
        .build()?;
    // Never overwrite input, override files, or an existing result artifact.
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .await
        .context("cannot create new replay output (destination must not exist)")?;
    let mut completed = 0;
    let mut skipped = 0;
    let mut failed = 0;
    for (index, request) in prepared.into_iter().enumerate() {
        let start = Instant::now();
        let (outcome, reason, response) = if request.skip {
            skipped += 1;
            (
                Outcome::Skipped,
                Some(
                    "request has redacted or omitted data; provide a complete override".to_owned(),
                ),
                None,
            )
        } else {
            match execute(&client, request).await {
                Ok(response) => {
                    completed += 1;
                    (Outcome::Completed, None, Some(response))
                }
                Err(reason) => {
                    failed += 1;
                    (Outcome::Failed, Some(reason.to_owned()), None)
                }
            }
        };
        let result = ReplayResult {
            schema_version: 1,
            source_index: index + 1,
            outcome,
            reason,
            response,
            duration_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        };
        let mut line = serde_json::to_vec(&result)?;
        line.push(b'\n');
        output
            .write_all(&line)
            .await
            .context("cannot write replay results")?;
        output
            .flush()
            .await
            .context("cannot flush replay results")?;
    }
    println!("replay: {completed} completed, {skipped} skipped, {failed} failed");
    Ok(if skipped + failed == 0 { 0 } else { 2 })
}
