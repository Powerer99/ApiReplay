use std::{
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::Response,
    routing::any,
};
use serde_json::Value;
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};

const RECORDING_LIMIT: usize = 1024 * 1024;
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempRecording(PathBuf);

impl TempRecording {
    fn new(name: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the Unix epoch")
            .as_nanos();
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "apireplay-{name}-{}-{timestamp}-{id}.apireplay",
            std::process::id()
        ));
        Self(path)
    }
}

impl Drop for TempRecording {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Clone, Copy)]
struct UpstreamState {
    large_body_size: usize,
}

async fn upstream_handler(
    State(state): State<UpstreamState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match uri.path() {
        "/binary" => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(vec![0, 159, 146, 150]))
            .expect("binary response should build"),
        "/large" => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Body::from(vec![b'x'; state.large_body_size]))
            .expect("large response should build"),
        _ => {
            let request_body = String::from_utf8_lossy(&body);
            let response_body = serde_json::json!({
                "received_method": method.as_str(),
                "received_uri": uri.to_string(),
                "received_body": request_body,
                "nested": { "access_token": "response-secret-token" },
                "authorization_seen": headers.contains_key(header::AUTHORIZATION),
            });

            Response::builder()
                .status(StatusCode::CREATED)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::SET_COOKIE, "session=response-secret-cookie")
                .header("x-debug-token", "request-secret-token")
                .body(Body::from(response_body.to_string()))
                .expect("JSON response should build")
        }
    }
}

async fn spawn_upstream() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("upstream should bind");
    let address = listener
        .local_addr()
        .expect("upstream should have an address");
    let app = Router::new()
        .fallback(any(upstream_handler))
        .with_state(UpstreamState {
            large_body_size: RECORDING_LIMIT + 1,
        });
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("upstream server should run");
    });
    (address, task)
}

fn unused_loopback_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("temporary port should bind");
    let address = listener
        .local_addr()
        .expect("temporary listener should have an address");
    drop(listener);
    address
}

fn spawn_proxy(listen: SocketAddr, target: SocketAddr, output: &PathBuf) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_apireplay"))
        .args([
            "record",
            "--listen",
            &listen.to_string(),
            "--target",
            &format!("http://{target}"),
            "--output",
        ])
        .arg(output)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("proxy process should start");
    ChildGuard(child)
}

async fn send_when_ready(client: &reqwest::Client, request_url: &str) -> reqwest::Response {
    let mut last_error = None;
    for _ in 0..100 {
        match client
            .post(request_url)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
            .header(
                header::AUTHORIZATION.as_str(),
                "Bearer request-secret-token",
            )
            .header(header::COOKIE.as_str(), "session=request-secret-cookie")
            .header("x-echo", "response-secret-token")
            .body(r#"{"username":"alice","echo":"response-secret-token","nested":{"password":"request-secret-password"}}"#)
            .send()
            .await
        {
            Ok(response) => return response,
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
    panic!("proxy did not become ready at {request_url}: {last_error:?}");
}

async fn read_records_when_ready(path: &PathBuf, minimum_lines: usize) -> Vec<Value> {
    for _ in 0..100 {
        if let Ok(contents) = tokio::fs::read_to_string(path).await {
            let lines = contents.lines().collect::<Vec<_>>();
            if lines.len() >= minimum_lines {
                return lines
                    .into_iter()
                    .map(|line| serde_json::from_str(line).expect("each JSONL line should parse"))
                    .collect();
            }
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("recording did not contain {minimum_lines} lines in time");
}

fn record_for_path<'a>(records: &'a [Value], path: &str) -> &'a Value {
    records
        .iter()
        .find(|record| record.to_string().contains(path))
        .unwrap_or_else(|| panic!("no record contained path {path}"))
}

fn json_paths_containing(value: &Value, needle: &str, path: &str, found: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                json_paths_containing(value, needle, &format!("{path}/{key}"), found);
            }
        }
        Value::Array(items) => {
            for (index, value) in items.iter().enumerate() {
                json_paths_containing(value, needle, &format!("{path}/{index}"), found);
            }
        }
        Value::String(text) if text.contains(needle) => found.push(path.to_owned()),
        _ => {}
    }
}

#[tokio::test]
async fn records_sanitized_bounded_exchanges_and_concurrent_requests() {
    let (upstream, upstream_task) = spawn_upstream().await;
    let proxy = unused_loopback_address();
    let output = TempRecording::new("exchanges");
    let _proxy_process = spawn_proxy(proxy, upstream, &output.0);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let response = send_when_ready(
        &client,
        &format!("http://{proxy}/users?active=true&role=admin&access_token=query-secret-token"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let response_text = response
        .text()
        .await
        .expect("response body should be readable");
    assert!(response_text.contains("active=true"));

    let binary_response = client
        .get(format!("http://{proxy}/binary"))
        .send()
        .await
        .expect("binary request should succeed");
    assert_eq!(
        binary_response.bytes().await.unwrap().as_ref(),
        [0, 159, 146, 150]
    );

    let large_response = client
        .get(format!("http://{proxy}/large"))
        .send()
        .await
        .expect("large request should succeed");
    assert_eq!(
        large_response
            .bytes()
            .await
            .expect("large body should stream")
            .len(),
        RECORDING_LIMIT + 1
    );

    let concurrent_count = 12;
    let oversized_request = client
        .post(format!("http://{proxy}/rejected"))
        .body(vec![b'x'; 10 * RECORDING_LIMIT + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(oversized_request.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let accepted_request = client
        .post(format!("http://{proxy}/omitted-request"))
        .header("content-type", "text/plain")
        .body(vec![b'x'; RECORDING_LIMIT + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(accepted_request.status(), StatusCode::CREATED);
    accepted_request.bytes().await.unwrap();
    let mut tasks = Vec::with_capacity(concurrent_count);
    for index in 0..concurrent_count {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .get(format!("http://{proxy}/concurrent/{index}"))
                .send()
                .await
                .expect("concurrent request should succeed")
                .error_for_status()
                .expect("concurrent response should be successful")
                .bytes()
                .await
                .expect("concurrent response body should finish");
        }));
    }
    for task in tasks {
        task.await.expect("concurrent request task should finish");
    }

    let expected_records = 4 + concurrent_count;
    let records = read_records_when_ready(&output.0, expected_records).await;
    assert_eq!(records.len(), expected_records);
    assert!(records.iter().all(|record| record["schema_version"] == 1));
    let omitted = &record_for_path(&records, "/omitted-request")["request"]["body"];
    assert_eq!(omitted["reason"], "too_large");
    assert!(omitted.get("content").is_none());

    let successful_record = record_for_path(&records, "/users");
    let serialized = successful_record.to_string();
    assert!(serialized.contains("active=true"));
    assert!(serialized.contains("alice"));
    assert!(serialized.contains("[REDACTED]"));
    assert!(successful_record["duration_ms"].is_number());
    assert!(successful_record["recorded_at_unix_ms"].as_u64().unwrap() > 0);
    assert_eq!(successful_record["request"]["method"], "POST");
    assert_eq!(successful_record["response"]["status"], 201);
    assert_eq!(
        successful_record["request"]["headers"]["x-echo"][0],
        "[REDACTED]"
    );
    assert_eq!(
        successful_record["request"]["body"]["content"]["echo"],
        "[REDACTED]"
    );

    let whole_file = tokio::fs::read_to_string(&output.0)
        .await
        .expect("recording should be readable");
    for secret in [
        "request-secret-token",
        "request-secret-cookie",
        "request-secret-password",
        "response-secret-token",
        "response-secret-cookie",
        "query-secret-token",
    ] {
        let mut leaked_at = Vec::new();
        for (index, record) in records.iter().enumerate() {
            json_paths_containing(record, secret, &format!("record[{index}]"), &mut leaked_at);
        }
        assert!(
            leaked_at.is_empty() && !whole_file.contains(secret),
            "recording leaked {secret} at {leaked_at:?}"
        );
    }

    assert!(
        record_for_path(&records, "/binary")
            .to_string()
            .contains("binary")
    );
    assert!(
        record_for_path(&records, "/large")
            .to_string()
            .contains("too_large")
    );

    upstream_task.abort();
}

#[tokio::test]
async fn records_client_visible_bad_gateway_when_upstream_is_unavailable() {
    let unavailable_upstream = unused_loopback_address();
    let proxy = unused_loopback_address();
    let output = TempRecording::new("bad-gateway");
    let _proxy_process = spawn_proxy(proxy, unavailable_upstream, &output.0);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let response = send_when_ready(&client, &format!("http://{proxy}/unavailable")).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let records = read_records_when_ready(&output.0, 1).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["schema_version"], 1);
    assert_eq!(
        records[0]["response"]["headers"]["content-type"][0],
        "text/plain; charset=utf-8"
    );
    assert!(records[0].to_string().contains("unavailable"));
    assert!(records[0].to_string().contains("502"));
}

#[test]
fn refuses_to_start_when_output_parent_does_not_exist() {
    let listen = unused_loopback_address();
    let target = unused_loopback_address();
    let missing_parent = std::env::temp_dir().join(format!(
        "apireplay-missing-parent-{}-{}",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let output = missing_parent.join("recording.apireplay");

    let result = Command::new(env!("CARGO_BIN_EXE_apireplay"))
        .args([
            "record",
            "--listen",
            &listen.to_string(),
            "--target",
            &format!("http://{target}"),
            "--output",
        ])
        .arg(output)
        .output()
        .expect("proxy process should execute");

    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr).to_ascii_lowercase();
    assert!(
        stderr.contains("output") || stderr.contains("record"),
        "startup error should identify the recording destination: {stderr}"
    );
}

#[tokio::test]
async fn streams_before_upstream_finishes_and_records_after_completion() {
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let stream_gate = gate.clone();
    let app = Router::new().route(
        "/stream",
        axum::routing::get(move || {
            let gate = stream_gate.clone();
            async move {
                Response::builder()
                    .header("content-type", "text/plain")
                    .body(Body::from_stream(async_stream::stream! {
                        yield Ok::<_, std::io::Error>(Bytes::from_static(b"first"));
                        gate.notified().await;
                        yield Ok::<_, std::io::Error>(Bytes::from_static(b"last"));
                    }))
                    .unwrap()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let proxy = unused_loopback_address();
    let output = TempRecording::new("stream");
    let _proxy_process = spawn_proxy(proxy, target, &output.0);
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    // A 404 readiness exchange also verifies ordinary upstream statuses are recorded.
    send_when_ready(&client, &format!("http://{proxy}/ready"))
        .await
        .bytes()
        .await
        .unwrap();
    read_records_when_ready(&output.0, 1).await;
    let mut response = client
        .get(format!("http://{proxy}/stream"))
        .send()
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), response.chunk())
        .await
        .expect("first chunk must arrive before the upstream is released")
        .unwrap()
        .unwrap();
    assert_eq!(first.as_ref(), b"first");
    assert_eq!(
        tokio::fs::read_to_string(&output.0)
            .await
            .unwrap()
            .lines()
            .count(),
        1
    );
    gate.notify_one();
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"last");
    let records = read_records_when_ready(&output.0, 2).await;
    assert_eq!(
        record_for_path(&records, "/stream")["response"]["body"]["content"],
        "firstlast"
    );
    upstream_task.abort();
}

#[test]
fn requires_output_and_rejects_nonlocal_addresses() {
    for (args, expected) in [
        (
            vec!["record", "--target", "http://127.0.0.1:3000"],
            "--output",
        ),
        (
            vec![
                "record",
                "--target",
                "http://example.com",
                "--output",
                "unused.jsonl",
            ],
            "local HTTP",
        ),
        (
            vec![
                "record",
                "--target",
                "http://127.0.0.1:3000",
                "--listen",
                "0.0.0.0:8080",
                "--output",
                "unused.jsonl",
            ],
            "loopback",
        ),
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_apireplay"))
            .args(args)
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(expected));
    }
}
