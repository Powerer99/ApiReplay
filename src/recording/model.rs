use std::{collections::BTreeMap, time::SystemTime};

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA_VERSION: u8 = 1;
pub const MAX_RECORDED_BODY_BYTES: usize = 1024 * 1024;
pub const REDACTED: &str = "[REDACTED]";

pub type RecordedHeaders = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedExchange {
    pub schema_version: u8,
    pub recorded_at_unix_ms: u64,
    pub duration_ms: u64,
    pub request: RecordedRequest,
    pub response: RecordedResponse,
}

impl RecordedExchange {
    pub fn new(duration_ms: u64, request: RecordedRequest, response: RecordedResponse) -> Self {
        let recorded_at_unix_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
            .unwrap_or_default();

        Self {
            schema_version: SCHEMA_VERSION,
            recorded_at_unix_ms,
            duration_ms,
            request,
            response,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedRequest {
    pub method: String,
    pub path_and_query: String,
    pub headers: RecordedHeaders,
    pub body: RecordedBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedResponse {
    pub status: u16,
    pub headers: RecordedHeaders,
    pub body: RecordedBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecordedBody {
    Empty,
    Text {
        content_type: Option<String>,
        content: String,
    },
    Json {
        content_type: Option<String>,
        content: Value,
    },
    Omitted {
        content_type: Option<String>,
        reason: OmissionReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmissionReason {
    Binary,
    TooLarge,
}

pub fn sanitize_headers(headers: &HeaderMap) -> RecordedHeaders {
    let mut sanitized = RecordedHeaders::new();

    for (name, value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        let value = if is_sensitive_header(&name) {
            REDACTED.to_owned()
        } else {
            String::from_utf8_lossy(value.as_bytes()).into_owned()
        };

        sanitized.entry(name).or_default().push(value);
    }

    sanitized
}

pub fn sanitize_headers_with_redactions(
    headers: &HeaderMap,
    secrets: &[String],
) -> RecordedHeaders {
    let mut headers = sanitize_headers(headers);
    for values in headers.values_mut() {
        for value in values {
            redact_known_secrets(value, secrets);
        }
    }
    headers
}

pub fn capture_body(bytes: &[u8], content_type: Option<&str>) -> RecordedBody {
    let content_type = content_type.map(str::to_owned);

    if bytes.is_empty() {
        return RecordedBody::Empty;
    }

    if bytes.len() > MAX_RECORDED_BODY_BYTES {
        return RecordedBody::Omitted {
            content_type,
            reason: OmissionReason::TooLarge,
        };
    }

    if content_type.as_deref().is_some_and(is_binary_content_type) {
        return RecordedBody::Omitted {
            content_type,
            reason: OmissionReason::Binary,
        };
    }

    let Ok(text) = std::str::from_utf8(bytes) else {
        return RecordedBody::Omitted {
            content_type,
            reason: OmissionReason::Binary,
        };
    };

    if let Ok(mut json) = serde_json::from_str::<Value>(text) {
        redact_sensitive_json(&mut json);
        return RecordedBody::Json {
            content_type,
            content: json,
        };
    }

    RecordedBody::Text {
        content_type,
        content: text.to_owned(),
    }
}

pub fn capture_body_with_redactions(
    bytes: &[u8],
    content_type: Option<&str>,
    secrets: &[String],
) -> RecordedBody {
    let mut body = capture_body(bytes, content_type);

    match &mut body {
        RecordedBody::Text { content, .. } => redact_known_secrets(content, secrets),
        RecordedBody::Json { content, .. } => redact_known_secrets_in_json(content, secrets),
        RecordedBody::Empty | RecordedBody::Omitted { .. } => {}
    }

    body
}

pub fn collect_sensitive_values(headers: &HeaderMap, body: &[u8]) -> Vec<String> {
    let mut secrets = Vec::new();

    for (name, value) in headers {
        let value = String::from_utf8_lossy(value.as_bytes());
        collect_sensitive_header_values(name.as_str(), &value, &mut secrets);
    }

    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        collect_sensitive_json_values(&value, &mut secrets);
    }

    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets.dedup();
    secrets
}

fn collect_sensitive_header_values(name: &str, value: &str, secrets: &mut Vec<String>) {
    if !is_sensitive_header(name) {
        return;
    }

    let value = value.trim();
    if value.is_empty() {
        return;
    }

    secrets.push(value.to_owned());
    match name.to_ascii_lowercase().as_str() {
        "cookie" => {
            for cookie in value.split(';').map(str::trim) {
                if let Some((_, secret)) = cookie.split_once('=')
                    && !secret.is_empty()
                {
                    secrets.push(secret.to_owned());
                }
            }
        }
        "set-cookie" => {
            if let Some((_, secret)) = value
                .split(';')
                .next()
                .and_then(|cookie| cookie.split_once('='))
                && !secret.is_empty()
            {
                secrets.push(secret.to_owned());
            }
        }
        "authorization" | "proxy-authorization" => {
            if let Some((_, secret)) = value.split_once(char::is_whitespace) {
                let secret = secret.trim();
                if !secret.is_empty() {
                    secrets.push(secret.to_owned());
                }
            }
        }
        "proxy-authenticate" => {}
        _ => {}
    }
}

fn collect_sensitive_json_values(value: &Value, secrets: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if is_sensitive_json_key(key) {
                    if let Some(secret) = value.as_str().filter(|secret| !secret.is_empty()) {
                        secrets.push(secret.to_owned());
                    }
                } else {
                    collect_sensitive_json_values(value, secrets);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_sensitive_json_values(item, secrets);
            }
        }
        Value::String(text) => {
            if let Ok(embedded_json) = serde_json::from_str::<Value>(text) {
                collect_sensitive_json_values(&embedded_json, secrets);
            }
        }
        _ => {}
    }
}

fn redact_known_secrets_in_json(value: &mut Value, secrets: &[String]) {
    match value {
        Value::Object(object) => {
            for value in object.values_mut() {
                redact_known_secrets_in_json(value, secrets);
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_known_secrets_in_json(item, secrets);
            }
        }
        Value::String(text) => redact_known_secrets(text, secrets),
        _ => {}
    }
}

pub fn redact_known_secrets(text: &mut String, secrets: &[String]) {
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        if text.contains(secret) {
            *text = text.replace(secret, REDACTED);
        }
    }
}

fn is_binary_content_type(content_type: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    !(media_type.starts_with("text/")
        || media_type == "application/json"
        || media_type.ends_with("+json")
        || media_type == "application/xml"
        || media_type.ends_with("+xml")
        || matches!(
            media_type.as_str(),
            "application/x-www-form-urlencoded"
                | "application/graphql"
                | "application/javascript"
                | "application/x-javascript"
        ))
}

pub fn redact_sensitive_json(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if is_sensitive_json_key(key) {
                    *value = Value::String(REDACTED.to_owned());
                } else {
                    redact_sensitive_json(value);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_sensitive_json(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "authorization" | "cookie" | "set-cookie" | "proxy-authorization" | "proxy-authenticate"
    ) || name.ends_with("-token")
        || name.ends_with("-secret")
        || name.ends_with("-api-key")
}

pub fn is_sensitive_name(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();

    matches!(
        normalized.as_str(),
        "password"
            | "passwd"
            | "token"
            | "accesstoken"
            | "refreshtoken"
            | "apikey"
            | "secret"
            | "clientsecret"
    )
}

fn is_sensitive_json_key(key: &str) -> bool {
    is_sensitive_name(key)
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::*;

    #[test]
    fn sanitizes_credential_headers_case_insensitively() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("cookie", HeaderValue::from_static("session=secret"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let sanitized = sanitize_headers(&headers);

        assert_eq!(sanitized["authorization"], [REDACTED]);
        assert_eq!(sanitized["cookie"], [REDACTED]);
        assert_eq!(sanitized["content-type"], ["application/json"]);
    }

    #[test]
    fn recursively_redacts_sensitive_json_keys() {
        let mut value = json!({
            "Password": "first-secret",
            "profile": {
                "api-key": "second-secret",
                "safe": "visible"
            },
            "items": [{ "refresh_token": "third-secret" }]
            ,"camelCase": { "accessToken": "fourth-secret", "clientSecret": "fifth-secret" }
        });

        redact_sensitive_json(&mut value);

        assert_eq!(value["Password"], REDACTED);
        assert_eq!(value["profile"]["api-key"], REDACTED);
        assert_eq!(value["items"][0]["refresh_token"], REDACTED);
        assert_eq!(value["camelCase"]["accessToken"], REDACTED);
        assert_eq!(value["camelCase"]["clientSecret"], REDACTED);
        assert_eq!(value["profile"]["safe"], "visible");
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(!serialized.contains("first-secret"));
        assert!(!serialized.contains("second-secret"));
        assert!(!serialized.contains("third-secret"));
        assert!(!serialized.contains("fourth-secret"));
        assert!(!serialized.contains("fifth-secret"));
    }

    #[test]
    fn captures_and_sanitizes_json_body() {
        let body = capture_body(
            br#"{"user":"Ada","client_secret":"hidden"}"#,
            Some("application/json"),
        );

        assert_eq!(
            body,
            RecordedBody::Json {
                content_type: Some("application/json".to_owned()),
                content: json!({"user": "Ada", "client_secret": REDACTED}),
            }
        );
    }

    #[test]
    fn scrubs_request_secrets_echoed_inside_response_strings() {
        let request_headers = HeaderMap::new();
        let request_body = br#"{"nested":{"password":"do-not-record"}}"#;
        let secrets = collect_sensitive_values(&request_headers, request_body);
        let response = br#"{"received_body":"{\"nested\":{\"password\":\"do-not-record\"}}"}"#;

        let captured = capture_body_with_redactions(response, Some("application/json"), &secrets);
        let serialized = serde_json::to_string(&captured).unwrap();

        assert!(!serialized.contains("do-not-record"));
        assert!(serialized.contains(REDACTED));
    }

    #[test]
    fn cookie_attributes_are_not_treated_as_secret_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            HeaderValue::from_static("session=cookie-secret; Path=/; HttpOnly; SameSite=Lax"),
        );

        let secrets = collect_sensitive_values(&headers, &[]);

        assert!(secrets.contains(&"cookie-secret".to_owned()));
        assert!(!secrets.contains(&"/".to_owned()));
        assert!(!secrets.contains(&"Lax".to_owned()));
    }

    #[test]
    fn scrubs_authorization_tokens_echoed_in_ordinary_headers() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer request-secret-token"),
        );
        let secrets = collect_sensitive_values(&request_headers, &[]);
        let mut response_headers = HeaderMap::new();
        response_headers.insert(
            "x-debug-token",
            HeaderValue::from_static("request-secret-token"),
        );

        let sanitized = sanitize_headers_with_redactions(&response_headers, &secrets);

        assert_eq!(sanitized["x-debug-token"], [REDACTED]);
    }

    #[test]
    fn omits_binary_and_oversized_bodies() {
        assert_eq!(
            capture_body(&[0xff, 0xfe], Some("application/octet-stream")),
            RecordedBody::Omitted {
                content_type: Some("application/octet-stream".to_owned()),
                reason: OmissionReason::Binary,
            }
        );

        assert_eq!(
            capture_body(b"plain-looking bytes", Some("application/octet-stream")),
            RecordedBody::Omitted {
                content_type: Some("application/octet-stream".to_owned()),
                reason: OmissionReason::Binary,
            }
        );

        let oversized = vec![b'a'; MAX_RECORDED_BODY_BYTES + 1];
        assert_eq!(
            capture_body(&oversized, Some("text/plain")),
            RecordedBody::Omitted {
                content_type: Some("text/plain".to_owned()),
                reason: OmissionReason::TooLarge,
            }
        );
    }
}
