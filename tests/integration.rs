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
        .stderr(Stdio::null())
        .spawn()
        .expect("proxy process should start");
    ChildGuard(child)
}

async fn send_when_ready(client: &reqwest::Client, request_url: &str) -> reqwest::Response {
    for _ in 0..100 {
        match client
            .post(request_url)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
            .header(
                header::AUTHORIZATION.as_str(),
                "Bearer request-secret-token",
            )
            .header(header::COOKIE.as_str(), "session=request-secret-cookie")
            .body(r#"{"username":"alice","nested":{"password":"request-secret-password"}}"#)
            .send()
            .await
        {
            Ok(response) => return response,
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("proxy did not become ready at {request_url}");
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
    let client = reqwest::Client::new();

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
                .expect("concurrent response should be successful");
        }));
    }
    for task in tasks {
        task.await.expect("concurrent request task should finish");
    }

    let expected_records = 3 + concurrent_count;
    let records = read_records_when_ready(&output.0, expected_records).await;
    assert_eq!(records.len(), expected_records);
    assert!(records.iter().all(|record| record["schema_version"] == 1));

    let successful_record = record_for_path(&records, "/users");
    let serialized = successful_record.to_string();
    assert!(serialized.contains("active=true"));
    assert!(serialized.contains("alice"));
    assert!(serialized.contains("[REDACTED]"));
    assert!(successful_record["duration_ms"].is_number());

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
    let client = reqwest::Client::new();

    let response = send_when_ready(&client, &format!("http://{proxy}/unavailable")).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let records = read_records_when_ready(&output.0, 1).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["schema_version"], 1);
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
