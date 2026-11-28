// Adapted from Macro's workspace_execution/tool_output.rs.
use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use serde::Serialize;
use tempfile::NamedTempFile;

const MAX_LINES: usize = 2000;
const MAX_BYTES: usize = 50 * 1024;
const MAX_ARTIFACT_BYTES_PER_WORK: usize = 64 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct Page {
    pub text: String,
    pub next_cursor: Option<String>,
    #[serde(skip)]
    pub overflow: Option<NamedTempFile>,
}

impl Page {
    /// Bound a tool response without a cursor that would repeat the action.
    pub fn one_shot(text: &str, scope: &str) -> io::Result<Self> {
        Self::one_shot_screened(text, scope, None)
    }

    /// Bound a one-shot response after screening its complete contents.
    pub fn one_shot_screened(
        text: &str,
        scope: &str,
        credential: Option<&str>,
    ) -> io::Result<Self> {
        ToolOutput {
            credential: credential.map(Arc::from),
            available: None,
        }
        .one_shot(text, scope)
    }
}

/// Screens output and reserves space for overflow files before creating them.
#[derive(Clone, Default)]
pub struct ToolOutput {
    credential: Option<Arc<str>>,
    available: Option<Arc<AtomicUsize>>,
}

impl ToolOutput {
    pub(crate) fn for_work(credential: Option<&str>) -> Self {
        Self {
            credential: credential.map(Arc::from),
            available: Some(Arc::new(AtomicUsize::new(MAX_ARTIFACT_BYTES_PER_WORK))),
        }
    }

    /// Bound a tool response without a cursor that would repeat the action.
    pub fn one_shot(&self, text: &str, scope: &str) -> io::Result<Page> {
        let mut page = self.paginate(text, scope, None, None)?;
        page.next_cursor = None;
        Ok(page)
    }

    fn retain(&self, text: &str) -> io::Result<NamedTempFile> {
        if let Some(available) = &self.available {
            available
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(text.len())
                })
                .map_err(|_| io::Error::other("Artifact byte budget exceeded for this work"))?;
        }
        let result = (|| {
            let mut file = NamedTempFile::new()?;
            file.write_all(text.as_bytes())?;
            Ok(file)
        })();
        if result.is_err() {
            if let Some(available) = &self.available {
                available.fetch_add(text.len(), Ordering::Relaxed);
            }
        }
        result
    }

    pub(super) fn paginate(
        &self,
        text: &str,
        scope: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> io::Result<Page> {
        let screened = self
            .credential
            .as_deref()
            .filter(|key| key.len() >= crate::providers::MIN_SCREENED_CREDENTIAL_BYTES)
            .map(|key| crate::providers::credential::redact(text, key));
        let text = screened.as_deref().unwrap_or(text);
        let hash = fingerprint(scope);
        let prefix = format!("v1:{hash:016x}:");
        let invalid =
            || io::Error::new(io::ErrorKind::InvalidInput, "Invalid or mismatched cursor");
        let start = match cursor {
            None => 0,
            Some(cursor) => cursor
                .strip_prefix(&prefix)
                .and_then(|offset| offset.parse::<usize>().ok())
                .filter(|offset| text.is_char_boundary(*offset))
                .ok_or_else(invalid)?,
        };

        let limit = limit.unwrap_or(MAX_LINES).clamp(1, MAX_LINES);
        let suffix = &text[start..];
        let end = suffix.floor_char_boundary(suffix.len().min(MAX_BYTES));
        let end = suffix[..end]
            .match_indices('\n')
            .nth(limit - 1)
            .map_or(end, |(index, _)| index + 1);
        let next = start + end;
        let truncated = next < text.len();
        let overflow = (truncated && start == 0)
            .then(|| self.retain(text))
            .transpose()?;
        Ok(Page {
            text: suffix[..end].into(),
            next_cursor: truncated.then(|| format!("{prefix}{next}")),
            overflow,
        })
    }
}

fn fingerprint(scope: &str) -> u64 {
    scope.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}
