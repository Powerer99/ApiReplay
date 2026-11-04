use crate::{
    recording::{
        RecordedBody, RecordedExchange, RecordedResponse, collect_sensitive_values,
        redact_known_secrets,
    },
    replay::runner::{Outcome, ReplayResult, has_redactions, read_recordings},
};
use anyhow::{Context, ensure};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
};

const IGNORED_HEADERS: &[&str] = &[
    "date",
    "server",
    "connection",
    "content-length",
    "transfer-encoding",
    "keep-alive",
    "trailer",
    "te",
    "upgrade",
    "proxy-authenticate",
    "proxy-authorization",
];

#[derive(Debug, Serialize)]
pub struct Difference {
    pub field: String,
    pub kind: &'static str,
}
#[derive(Debug, Serialize)]
pub struct ExchangeComparison {
    pub source_index: usize,
    pub outcome: &'static str,
    pub differences: Vec<Difference>,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u8,
    pub matched: usize,
    pub mismatched: usize,
    pub incomplete: usize,
    pub ignored_headers: Vec<String>,
    pub ignored_json_paths: Vec<String>,
    pub exchanges: Vec<ExchangeComparison>,
}
impl Report {
    pub fn exit_code(&self) -> u8 {
        if self.incomplete > 0 {
            2
        } else if self.mismatched > 0 {
            1
        } else {
            0
        }
    }
}

fn difference(out: &mut Vec<Difference>, field: impl Into<String>, kind: &'static str) {
    out.push(Difference {
        field: field.into(),
        kind,
    });
}
fn pointer_token(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}
fn json_diff(expected: &Value, actual: &Value, path: &str, out: &mut Vec<Difference>) {
    match (expected, actual) {
        (Value::Object(a), Value::Object(b)) => {
            for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
                let path = format!("{path}/{}", pointer_token(key));
                match (a.get(key), b.get(key)) {
                    (Some(a), Some(b)) => json_diff(a, b, &path, out),
                    (Some(_), None) => difference(out, path, "missing"),
                    (None, Some(_)) => difference(out, path, "extra"),
                    _ => {}
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            for index in 0..a.len().max(b.len()) {
                let path = format!("{path}/{index}");
                match (a.get(index), b.get(index)) {
                    (Some(a), Some(b)) => json_diff(a, b, &path, out),
                    (Some(_), None) => difference(out, path, "missing"),
                    (None, Some(_)) => difference(out, path, "extra"),
                    _ => {}
                }
            }
        }
        _ if expected != actual => difference(out, path, "changed"),
        _ => {}
    }
}

fn ignore_pointer(value: &mut Value, pointer: &str) {
    if let Some((parent, key)) = pointer.rsplit_once('/')
        && let Some(parent) = value.pointer_mut(parent)
    {
        let key = key.replace("~1", "/").replace("~0", "~");
        match parent {
            Value::Object(object) => {
                object.remove(&key);
            }
            Value::Array(array) => {
                if let Ok(index) = key.parse::<usize>()
                    && let Some(value) = array.get_mut(index)
                {
                    *value = Value::Null;
                }
            }
            _ => {}
        }
    }
}

fn normalize(
    response: &RecordedResponse,
    ignored: &BTreeSet<String>,
    paths: &[String],
) -> RecordedResponse {
    let mut normalized = response.clone();
    normalized.headers = response
        .headers
        .iter()
        .filter(|(key, _)| !ignored.contains(&key.to_ascii_lowercase()))
        .map(|(key, values)| (key.to_ascii_lowercase(), values.clone()))
        .collect();
    if let RecordedBody::Json { content, .. } = &mut normalized.body {
        for path in paths {
            ignore_pointer(content, path);
        }
    }
    normalized
}

pub fn compare(
    baseline: &[RecordedExchange],
    actual: &[ReplayResult],
    ignore_headers: &[String],
    ignore_paths: &[String],
) -> anyhow::Result<Report> {
    for pointer in ignore_paths {
        ensure!(
            pointer.starts_with('/')
                && !pointer
                    .as_bytes()
                    .windows(2)
                    .any(|w| w[0] == b'~' && w[1] != b'0' && w[1] != b'1')
                && !pointer.ends_with('~'),
            "ignore JSON path must be a non-root JSON Pointer"
        );
    }
    let ignored: BTreeSet<String> = IGNORED_HEADERS
        .iter()
        .map(|s| s.to_string())
        .chain(ignore_headers.iter().map(|s| s.to_ascii_lowercase()))
        .collect();
    let mut indexed = BTreeMap::new();
    for result in actual {
        ensure!(
            result.schema_version == 1 && result.source_index > 0,
            "invalid replay result version or source index"
        );
        ensure!(
            (result.outcome == Outcome::Completed) == result.response.is_some(),
            "inconsistent replay outcome/response"
        );
        if let Some(response) = &result.response {
            ensure!(
                (100..=599).contains(&response.status),
                "invalid replay response status"
            );
        }
        ensure!(
            indexed.insert(result.source_index, result).is_none(),
            "duplicate replay source index"
        );
    }
    let mut report = Report {
        schema_version: 1,
        matched: 0,
        mismatched: 0,
        incomplete: 0,
        ignored_headers: ignored.iter().cloned().collect(),
        ignored_json_paths: ignore_paths.to_vec(),
        exchanges: Vec::new(),
    };
    for (index, baseline) in baseline.iter().enumerate() {
        let source_index = index + 1;
        let mut diffs = Vec::new();
        let mut incomplete = false;
        if let Some(result) = indexed.remove(&source_index) {
            if let Some(actual) = &result.response {
                let a = normalize(&baseline.response, &ignored, ignore_paths);
                let b = normalize(actual, &ignored, ignore_paths);
                if has_redactions(&a)
                    || has_redactions(&b)
                    || matches!(a.body, RecordedBody::Omitted { .. })
                    || matches!(b.body, RecordedBody::Omitted { .. })
                {
                    incomplete = true;
                    difference(&mut diffs, "response", "redacted_or_omitted");
                }
                if a.status != b.status {
                    difference(&mut diffs, "status", "changed");
                }
                json_diff(
                    &serde_json::to_value(&a.headers)?,
                    &serde_json::to_value(&b.headers)?,
                    "headers",
                    &mut diffs,
                );
                match (&a.body, &b.body) {
                    (
                        RecordedBody::Json { content: a, .. },
                        RecordedBody::Json { content: b, .. },
                    ) => json_diff(a, b, "body", &mut diffs),
                    (
                        RecordedBody::Text { content: a, .. },
                        RecordedBody::Text { content: b, .. },
                    ) if a != b => difference(&mut diffs, "body", "changed"),
                    (RecordedBody::Text { .. }, RecordedBody::Text { .. })
                    | (RecordedBody::Empty, RecordedBody::Empty) => {}
                    (RecordedBody::Omitted { .. }, _) | (_, RecordedBody::Omitted { .. }) => {}
                    _ => difference(&mut diffs, "body", "kind_changed"),
                }
                // Reports contain locations/categories only, never response values.
                // Scrub known credentials from keys used in those locations as well.
                let mut secrets = Vec::new();
                for response in [&baseline.response, actual] {
                    let bytes = serde_json::to_vec(response)?;
                    secrets.extend(collect_sensitive_values(
                        &axum::http::HeaderMap::new(),
                        &bytes,
                    ));
                    let mut headers = axum::http::HeaderMap::new();
                    for (key, values) in &response.headers {
                        if let Ok(key) = axum::http::HeaderName::from_bytes(key.as_bytes()) {
                            for value in values {
                                if let Ok(value) = axum::http::HeaderValue::from_str(value) {
                                    headers.append(key.clone(), value);
                                }
                            }
                        }
                    }
                    secrets.extend(collect_sensitive_values(&headers, &[]));
                }
                secrets.extend(secrets.clone().iter().map(|s| pointer_token(s)));
                for diff in &mut diffs {
                    redact_known_secrets(&mut diff.field, &secrets);
                }
            } else {
                incomplete = true;
                difference(
                    &mut diffs,
                    "exchange",
                    if result.outcome == Outcome::Skipped {
                        "skipped"
                    } else {
                        "failed"
                    },
                );
            }
        } else {
            incomplete = true;
            difference(&mut diffs, "exchange", "missing");
        }
        let outcome = if incomplete {
            report.incomplete += 1;
            "incomplete"
        } else if diffs.is_empty() {
            report.matched += 1;
            "matched"
        } else {
            report.mismatched += 1;
            "mismatched"
        };
        report.exchanges.push(ExchangeComparison {
            source_index,
            outcome,
            differences: diffs,
        });
    }
    for (source_index, _) in indexed {
        report.incomplete += 1;
        report.exchanges.push(ExchangeComparison {
            source_index,
            outcome: "incomplete",
            differences: vec![Difference {
                field: "exchange".into(),
                kind: "extra",
            }],
        });
    }
    Ok(report)
}

pub fn run(
    baseline: &Path,
    actual: &Path,
    output: Option<&Path>,
    ignore_headers: &[String],
    ignore_paths: &[String],
) -> anyhow::Result<u8> {
    let baseline = read_recordings(baseline)?;
    let contents = std::fs::read_to_string(actual).context("cannot read replay results")?;
    let actual: Vec<ReplayResult> = contents
        .lines()
        .enumerate()
        .map(|(i, l)| {
            serde_json::from_str(l)
                .map_err(|_| anyhow::anyhow!("invalid replay result at line {}", i + 1))
        })
        .collect::<Result<_, _>>()?;
    let report = compare(&baseline, &actual, ignore_headers, ignore_paths)?;
    if let Some(path) = output {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .context("cannot create new comparison report")?;
        file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    }
    println!(
        "compare: {} matched, {} mismatched, {} incomplete",
        report.matched, report.mismatched, report.incomplete
    );
    for exchange in &report.exchanges {
        for diff in &exchange.differences {
            println!(
                "source {}: {} {}",
                exchange.source_index, diff.field, diff.kind
            );
        }
    }
    Ok(report.exit_code())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn baseline() -> RecordedExchange {
        serde_json::from_value(json!({"schema_version":1,"recorded_at_unix_ms":1,"duration_ms":0,"request":{"method":"GET","path_and_query":"/same","headers":{},"body":{"kind":"empty"}},"response":{"status":200,"headers":{"date":["yesterday"],"content-type":["application/json"]},"body":{"kind":"json","content_type":"application/json","content":{"items":[1,2],"timestamp":1}}}})).unwrap()
    }
    fn result(index: usize, response: RecordedResponse) -> ReplayResult {
        ReplayResult {
            schema_version: 1,
            source_index: index,
            outcome: Outcome::Completed,
            reason: None,
            duration_ms: 0,
            response: Some(response),
        }
    }
    #[test]
    fn compares_structure_and_explicit_ignores_without_values_in_reports() {
        let baseline = baseline();
        let mut actual = baseline.response.clone();
        actual.headers.insert("date".into(), vec!["today".into()]);
        assert_eq!(
            compare(
                std::slice::from_ref(&baseline),
                &[result(1, actual.clone())],
                &[],
                &[]
            )
            .unwrap()
            .exit_code(),
            0
        );
        actual.body = RecordedBody::Json {
            content_type: Some("application/json".into()),
            content: json!({"items":[2,1],"timestamp":"private-output-value"}),
        };
        let report = compare(
            std::slice::from_ref(&baseline),
            &[result(1, actual.clone())],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(report.exit_code(), 1);
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("private-output-value")
        );
        assert!(
            report.exchanges[0]
                .differences
                .iter()
                .any(|d| d.field == "body/items/0")
        );
        assert_eq!(
            compare(
                &[baseline],
                &[result(1, actual)],
                &[],
                &["/items".into(), "/timestamp".into()]
            )
            .unwrap()
            .exit_code(),
            0
        );
    }
    #[test]
    fn compares_status_headers_and_text_and_rejects_incomplete_bodies() {
        let mut baseline = baseline();
        baseline.response.body = RecordedBody::Text {
            content_type: Some("text/plain".into()),
            content: "before".into(),
        };
        let mut actual = baseline.response.clone();
        actual.status = 503;
        actual
            .headers
            .insert("x-app".into(), vec!["private-header-value".into()]);
        actual.body = RecordedBody::Text {
            content_type: Some("text/plain".into()),
            content: "private-body-value".into(),
        };
        let report = compare(
            std::slice::from_ref(&baseline),
            &[result(1, actual)],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(report.exit_code(), 1);
        for field in ["status", "headers/x-app", "body"] {
            assert!(
                report.exchanges[0]
                    .differences
                    .iter()
                    .any(|d| d.field == field)
            );
        }
        assert!(!serde_json::to_string(&report).unwrap().contains("private"));
        let mut omitted = baseline.response.clone();
        omitted.body = RecordedBody::Omitted {
            content_type: None,
            reason: crate::recording::model::OmissionReason::Binary,
        };
        assert_eq!(
            compare(
                std::slice::from_ref(&baseline),
                &[result(1, omitted)],
                &[],
                &[]
            )
            .unwrap()
            .exit_code(),
            2
        );
        assert!(compare(&[baseline], &[], &[], &["invalid".into()]).is_err());
    }

    #[test]
    fn missing_extra_skipped_redacted_and_duplicate_results_are_not_matches() {
        let baseline = baseline();
        let mut redacted = baseline.response.clone();
        redacted
            .headers
            .insert("set-cookie".into(), vec!["[REDACTED]".into()]);
        assert_eq!(
            compare(
                std::slice::from_ref(&baseline),
                &[result(1, redacted)],
                &[],
                &[]
            )
            .unwrap()
            .exit_code(),
            2
        );
        let report = compare(
            &[baseline.clone(), baseline.clone()],
            &[
                result(1, baseline.response.clone()),
                result(3, baseline.response.clone()),
            ],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(report.matched, 1);
        assert_eq!(report.incomplete, 2);
        let skipped = ReplayResult {
            schema_version: 1,
            source_index: 1,
            outcome: Outcome::Skipped,
            reason: None,
            duration_ms: 0,
            response: None,
        };
        assert_eq!(
            compare(std::slice::from_ref(&baseline), &[skipped], &[], &[])
                .unwrap()
                .exit_code(),
            2
        );
        assert!(
            compare(
                std::slice::from_ref(&baseline),
                &[
                    result(1, baseline.response.clone()),
                    result(1, baseline.response.clone())
                ],
                &[],
                &[]
            )
            .is_err()
        );
    }
}
