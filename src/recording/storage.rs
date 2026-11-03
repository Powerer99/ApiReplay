use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::Mutex,
};

use super::model::RecordedExchange;

#[derive(Debug, Clone)]
pub struct RecordingWriter {
    inner: Arc<Mutex<BufWriter<File>>>,
    path: Arc<PathBuf>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to serialize recording: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to open recording file '{}': {source}", path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("recording file '{}' contains invalid JSONL at line {line}: {source}", path.display())]
    InvalidExisting {
        path: PathBuf,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to write recording file '{}': {source}", path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl RecordingWriter {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_owned();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|source| StorageError::Open {
                path: path.clone(),
                source,
            })?;

        let file = validate_existing_jsonl(file, &path).await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(BufWriter::new(file))),
            path: Arc::new(path),
        })
    }

    pub async fn write(&self, exchange: &RecordedExchange) -> Result<(), StorageError> {
        let mut line = serde_json::to_vec(exchange)?;
        line.push(b'\n');

        let mut writer = self.inner.lock().await;
        writer
            .write_all(&line)
            .await
            .map_err(|source| StorageError::Write {
                path: (*self.path).clone(),
                source,
            })?;
        writer.flush().await.map_err(|source| StorageError::Write {
            path: (*self.path).clone(),
            source,
        })?;

        Ok(())
    }
}

async fn validate_existing_jsonl(file: File, path: &Path) -> Result<File, StorageError> {
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut line_number = 0;
    let mut final_line_had_newline = true;

    loop {
        line.clear();
        let bytes_read =
            reader
                .read_line(&mut line)
                .await
                .map_err(|source| StorageError::Open {
                    path: path.to_owned(),
                    source,
                })?;
        if bytes_read == 0 {
            break;
        }

        line_number += 1;
        final_line_had_newline = line.ends_with('\n');
        serde_json::from_str::<serde_json::Value>(line.trim_end()).map_err(|source| {
            StorageError::InvalidExisting {
                path: path.to_owned(),
                line: line_number,
                source,
            }
        })?;
    }

    let mut file = reader.into_inner();
    if line_number > 0 && !final_line_had_newline {
        file.write_all(b"\n")
            .await
            .map_err(|source| StorageError::Write {
                path: path.to_owned(),
                source,
            })?;
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

    use super::*;
    use crate::recording::model::{
        RecordedBody, RecordedRequest, RecordedResponse, SCHEMA_VERSION,
    };

    fn exchange(id: usize) -> RecordedExchange {
        RecordedExchange::new(
            id as u64,
            RecordedRequest {
                method: "GET".to_owned(),
                path_and_query: format!("/items/{id}"),
                headers: BTreeMap::new(),
                body: RecordedBody::Empty,
            },
            RecordedResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: RecordedBody::Empty,
            },
        )
    }

    fn temporary_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "apireplay-recording-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn concurrent_writes_produce_complete_json_lines() {
        let path = temporary_path();
        let writer = RecordingWriter::create(&path).await.unwrap();
        let mut tasks = Vec::new();

        for id in 0..20 {
            let writer = writer.clone();
            tasks.push(tokio::spawn(async move {
                writer.write(&exchange(id)).await.unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(lines.len(), 20);
        for line in lines {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(value["schema_version"], SCHEMA_VERSION);
        }

        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn reopening_an_existing_recording_appends() {
        let path = temporary_path();
        let first_writer = RecordingWriter::create(&path).await.unwrap();
        first_writer.write(&exchange(1)).await.unwrap();
        drop(first_writer);

        let second_writer = RecordingWriter::create(&path).await.unwrap();
        second_writer.write(&exchange(2)).await.unwrap();
        drop(second_writer);

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.lines().count(), 2);

        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn repairs_a_valid_final_line_without_a_newline_before_appending() {
        let path = temporary_path();
        tokio::fs::write(&path, br#"{"schema_version":1}"#)
            .await
            .unwrap();

        let writer = RecordingWriter::create(&path).await.unwrap();
        writer.write(&exchange(2)).await.unwrap();
        drop(writer);

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines
                .iter()
                .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok())
        );

        tokio::fs::remove_file(path).await.unwrap();
    }
}
