# ApiReplay

ApiReplay is a Rust command-line tool for capturing HTTP API traffic during local
development. It runs a recording proxy between your client and a local API,
forwards requests to that API, and appends request/response exchanges to a JSON
Lines (JSONL) file for inspection and debugging.

The current implementation provides the `record` command. Replay and response
comparison are planned areas represented by empty source files; they are not yet
available as commands.

## How it works

```text
Client -> ApiReplay (127.0.0.1:8080) -> Local API (127.0.0.1:3000)
                  |
                  +-> recordings.jsonl
```

- Forwards HTTP methods, paths, query strings, and request bodies.
- Streams upstream response bodies back to the client.
- Records response status, sanitized headers and bodies, a completion timestamp,
  and exchange duration.
- Redacts recognized credentials in recordings while forwarding the original
  application data to the upstream API.
- Serializes concurrent recording writes so each exchange occupies one JSON line.
- Returns and records `502 Bad Gateway` when an upstream request cannot be sent.

## Build and run

Install a recent stable Rust toolchain with Cargo and the native build tools
required by your platform. The project uses Rust edition 2024.

From the repository root:

```sh
cargo build --locked
cargo run --locked -- --help
```

For an optimized executable, use `cargo build --locked --release`. The binary is
written to `target/release/apireplay` (`apireplay.exe` on Windows).

## Record your local API

Start your API on `http://127.0.0.1:3000`, then start ApiReplay in another terminal:

```sh
cargo run --locked -- record --target http://127.0.0.1:3000 --output recordings.jsonl
```

Point your client at `http://127.0.0.1:8080` instead of port `3000`. For example,
if your API exposes `/health`, send a request through the proxy:

```sh
curl http://127.0.0.1:8080/health
```

In Windows PowerShell, use `curl.exe` to invoke curl explicitly. The response comes
from your API, and the completed exchange is appended to `recordings.jsonl`.
Press `Ctrl+C` to stop the proxy after your requests have finished.

### Command options

```text
apireplay record --target <TARGET> --output <OUTPUT> [--listen <LISTEN>]
```

| Option | Description | Default |
| --- | --- | --- |
| `--target` | Local HTTP API base URL; required. | None |
| `--output` | File to create or append JSONL recordings to; required. | None |
| `--listen` | Loopback IP address and port for the proxy. | `127.0.0.1:8080` |

Use `cargo run --locked -- record --help` for command-specific help.

Targets must use `http://` with host `localhost`, `127.0.0.1`, or `[::1]`.
The listening address must also be loopback. HTTPS and remote upstream APIs are
not supported. A target path acts as a prefix: target
`http://127.0.0.1:3000/api` plus incoming path `/users` forwards to `/api/users`.
Incoming query parameters are forwarded as well.

The output file's parent directory must already exist. Existing files are checked
for valid JSON on every line before recording starts, and new exchanges are
appended without replacing earlier data. The file extension is your choice;
`.jsonl` describes the format.

## Recording format

Each line contains one exchange with `schema_version: 1`. This illustrative record
is expanded for readability; actual records are written on a single line:

```json
{
  "schema_version": 1,
  "recorded_at_unix_ms": 1793700000000,
  "duration_ms": 12,
  "request": {
    "method": "GET",
    "path_and_query": "/health",
    "headers": {},
    "body": { "kind": "empty" }
  },
  "response": {
    "status": 200,
    "headers": { "content-type": ["application/json"] },
    "body": {
      "kind": "json",
      "content_type": "application/json",
      "content": { "status": "ok" }
    }
  }
}
```

`recorded_at_unix_ms` is the recording timestamp at completion, in milliseconds
since the Unix epoch. `duration_ms` measures handler time through response stream
completion, before writing the record. Header names map to arrays of values.

Bodies use one of four `kind` values:

| Kind | Stored data |
| --- | --- |
| `empty` | No body content. |
| `json` | Parsed JSON in `content`, plus `content_type`. |
| `text` | UTF-8 text in `content`, plus `content_type`. |
| `omitted` | `content_type` and a `reason` of `binary` or `too_large`. |

## Redaction and limits

Recordings replace recognized sensitive values with `[REDACTED]`. This includes
authorization and cookie headers, headers ending in `-token`, `-secret`, or
`-api-key`, and recognized query parameter and nested JSON field names such as
`password`, `access_token`, `refresh_token`, `api_key`, and `client_secret`.
Detected secret strings are also scrubbed from captured content where available,
including request credentials echoed in a response.
Known secret strings are also removed from JSON property names. When sanitized
names collide, a deterministic `#2`, `#3`, ... suffix preserves each value;
existing property names are reserved so they are not overwritten.
Sensitive numbers, booleans and nulls are redacted wherever their values recur in
captured application data. For sensitive objects/arrays, all scalar leaves and
object keys are treated as secrets. This conservative policy can redact ordinary
data with the same value (for example `true`); redacted data cannot be exactly
reconstructed for replay. Empty strings contain no recoverable secret value.

Redaction is based on recognized names and detected values. Arbitrary secrets in
free text, form data, or unrecognized fields may still be recorded; review
recordings before sharing them.

- Each captured request or response body is limited to **1 MiB**. Larger bodies
  are marked `too_large` in the recording, rather than storing a partial body.
- Binary bodies are marked `binary`; their bytes still pass through the proxy.
- Incoming request bodies have a separate **10 MiB** limit and are buffered
  before forwarding. Responses are streamed, including those over 1 MiB.
- The proxy does not follow upstream redirects. Hop-by-hop headers are filtered
  when forwarding traffic.
- Successful exchanges are written after the response stream completes.
  HEAD, 204, 304 and known empty responses are recorded on header completion,
  without depending on the client polling a body.
  Interrupted streams and requests rejected before the handler runs may have no
  recording. Recording write failures are logged.

## Development

```sh
cargo test --locked
cargo fmt --check
cargo clippy --locked --all-targets
```

Unit tests cover body capture, redaction, and JSONL storage. Integration tests
start local upstream servers and a proxy process to exercise recording,
concurrent traffic, body limits for recording, upstream failures, and invalid
output destinations.

| Location | Purpose |
| --- | --- |
| `src/main.rs`, `src/cli.rs` | Executable entry point and command-line arguments. |
| `src/proxy/` | Local HTTP proxy setup, forwarding, and capture. |
| `src/recording/` | Recording schema, sanitization, and JSONL storage. |
| `src/replay/`, `src/diff/` | Empty placeholders for future functionality. |
| `tests/integration.rs` | End-to-end tests against local HTTP servers. |
