use axum::{
    Json, Router,
    body::Bytes,
    http::{HeaderMap, Method, Uri},
    routing::any,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, process::Command};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "apireplay-replay-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn record(method: &str, path: &str) -> Value {
    json!({"schema_version":1,"recorded_at_unix_ms":1,"duration_ms":0,
        "request":{"method":method,"path_and_query":path,"headers":{},"body":{"kind":"empty"}},
        "response":{"status":200,"headers":{},"body":{"kind":"empty"}}})
}
fn write_input(files: &Files, records: &[Value]) {
    std::fs::write(
        files.path("in.jsonl"),
        records
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}
async fn replay(
    files: &Files,
    target: &str,
    output: &str,
    overrides: bool,
) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_apireplay"));
    cmd.args(["replay", "--input"])
        .arg(files.path("in.jsonl"))
        .args(["--target", target, "--output"])
        .arg(files.path(output))
        .args(["--timeout-ms", "300"]);
    if overrides {
        cmd.arg("--overrides").arg(files.path("overrides.json"));
    }
    tokio::time::timeout(Duration::from_secs(10), cmd.kill_on_drop(true).output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn replay_preserves_order_and_supports_explicit_secret_overrides() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let app = Router::new().fallback(any(move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
        let observed=observed.clone();
        async move {
            let value=json!({"method":method.as_str(),"path":uri.to_string(),"body":String::from_utf8_lossy(&body),"auth":headers.get("authorization").and_then(|h|h.to_str().ok())});
            observed.lock().unwrap().push(value.clone()); Json(value)
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}/api", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new();
    let mut first = record("POST", "/item?active=true");
    first["request"]["body"] =
        json!({"kind":"json","content_type":"application/json","content":{"name":"Alice"}});
    let mut second = record("PUT", "/item");
    second["request"]["headers"] = json!({"authorization":["[REDACTED]"]});
    write_input(&files, &[first.clone(), second.clone()]);
    let result = replay(&files, &target, "skipped.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    assert_eq!(seen.lock().unwrap().len(), 1);
    let skipped: Vec<Value> = std::fs::read_to_string(files.path("skipped.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(skipped[1]["outcome"], "skipped");
    second["request"]["headers"] = json!({"authorization":["Bearer synthetic-private-credential"]});
    std::fs::write(
        files.path("overrides.json"),
        json!({"2":second["request"]}).to_string(),
    )
    .unwrap();
    seen.lock().unwrap().clear();
    let result = replay(&files, &target, "actual.jsonl", true).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let observed = seen.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0]["method"], "POST");
    assert_eq!(observed[0]["path"], "/api/item?active=true");
    assert_eq!(
        serde_json::from_str::<Value>(observed[0]["body"].as_str().unwrap()).unwrap(),
        json!({"name":"Alice"})
    );
    assert_eq!(observed[1]["method"], "PUT");
    assert_eq!(observed[1]["auth"], "Bearer synthetic-private-credential");
    let actual = std::fs::read_to_string(files.path("actual.jsonl")).unwrap();
    assert!(!actual.contains("synthetic-private-credential"));
    let lines: Vec<Value> = actual
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines[0]["source_index"], 1);
    assert_eq!(lines[1]["source_index"], 2);
    assert!(lines.iter().all(|r| r["outcome"] == "completed"));
    task.abort();
}

#[tokio::test]
async fn replay_validates_all_input_before_creating_output_and_reports_failures() {
    let files = Files::new();
    let mut unsupported = record("GET", "/ok");
    unsupported["schema_version"] = 2.into();
    write_input(&files, &[record("GET", "/ok"), unsupported]);
    let result = replay(&files, "http://127.0.0.1:1", "invalid.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&result.stderr).contains("line 2"));
    assert!(!files.path("invalid.jsonl").exists());
    write_input(&files, &[record("GET", "/ok")]);
    let result = replay(&files, "http://127.0.0.1:1", "failed.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    let failed: Value =
        serde_json::from_str(&std::fs::read_to_string(files.path("failed.jsonl")).unwrap())
            .unwrap();
    assert_eq!(failed["outcome"], "failed");
    assert!(failed["response"].is_null());
    let result = replay(&files, "http://example.com", "remote.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    assert!(!files.path("remote.jsonl").exists());
    let result = replay(&files, "http://127.0.0.1:1", "in.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    assert!(
        std::fs::read_to_string(files.path("in.jsonl"))
            .unwrap()
            .contains("request")
    );
    let mut omitted = record("POST", "/missing");
    omitted["request"]["body"] =
        json!({"kind":"omitted","content_type":"text/plain","reason":"too_large"});
    write_input(&files, &[omitted]);
    let result = replay(&files, "http://127.0.0.1:1", "omitted.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    let skipped: Value =
        serde_json::from_str(&std::fs::read_to_string(files.path("omitted.jsonl")).unwrap())
            .unwrap();
    assert_eq!(skipped["outcome"], "skipped");
}

#[tokio::test]
async fn replay_bounds_streams_times_out_and_does_not_follow_redirects() {
    let app = Router::new().fallback(any(|uri: Uri| async move {
        match uri.path() {
            "/slow" => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                axum::response::Response::new(axum::body::Body::empty())
            }
            "/redirect" => axum::response::Response::builder()
                .status(302)
                .header("location", "/unexpected")
                .body(axum::body::Body::empty())
                .unwrap(),
            _ => axum::response::Response::builder()
                .header("content-type", "text/plain")
                .body(axum::body::Body::from(vec![b'x'; 1024 * 1024 + 1]))
                .unwrap(),
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let files = Files::new();
    write_input(
        &files,
        &[
            record("GET", "/large"),
            record("GET", "/slow"),
            record("GET", "/redirect"),
        ],
    );
    let result = replay(&files, &target, "results.jsonl", false).await;
    assert_eq!(result.status.code(), Some(2));
    let results: Vec<Value> = std::fs::read_to_string(files.path("results.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(results[0]["response"]["body"]["reason"], "too_large");
    assert!(results[0]["response"]["body"].get("content").is_none());
    assert_eq!(results[1]["outcome"], "failed");
    assert_eq!(results[2]["response"]["status"], 302);
    task.abort();
}
