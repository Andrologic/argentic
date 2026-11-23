//! Shared limits for work and allocation during source processing.
use crate::{calculation::Failure, GRAPH_LIMIT_CODE};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Maximum work units spent on a source operation, including all its stages.
pub const MAX_SOURCE_WORK: usize = 8_000_000;
/// Maximum cumulative estimated allocation during a source operation.
pub const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
/// Maximum JSON container depth checked before allocating the parsed tree.
pub const MAX_SOURCE_JSON_DEPTH: usize = 128;

/// Budget for one source operation. Clones share the same counters. Hosts that
/// parse and assemble components can pass the same budget to validation.
#[derive(Clone, Default)]
pub struct SourceBudget(Arc<Usage>);

#[derive(Default)]
struct Usage {
    work: AtomicUsize,
    bytes: AtomicUsize,
}

impl SourceBudget {
    fn spend(used: &AtomicUsize, count: usize, limit: usize) -> bool {
        used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |before| {
            before.checked_add(count).filter(|total| *total <= limit)
        })
        .is_ok()
    }

    pub(crate) fn charge_work(&self, units: usize) -> bool {
        Self::spend(&self.0.work, units, MAX_SOURCE_WORK)
    }

    pub(crate) fn charge_bytes(&self, bytes: usize) -> bool {
        Self::spend(&self.0.bytes, bytes, MAX_SOURCE_BYTES)
    }

    /// Reserve units before an input-dependent traversal.
    pub fn work(&self, units: usize, pointer: &str) -> Result<(), Failure> {
        self.charge_work(units)
            .then_some(())
            .ok_or_else(|| limit(pointer, "source work units", MAX_SOURCE_WORK))
    }

    /// Reserve bytes before an input-dependent allocation.
    pub fn bytes(&self, bytes: usize, pointer: &str) -> Result<(), Failure> {
        self.charge_bytes(bytes)
            .then_some(())
            .ok_or_else(|| limit(pointer, "source allocation bytes", MAX_SOURCE_BYTES))
    }

    /// Reserve input scanning and tree allocation before parsing. The lexer
    /// only counts separators and nesting here. The parser checks JSON syntax.
    pub fn json_input(&self, input: &[u8], pointer: &str) -> Result<(), Failure> {
        self.work(input.len().div_ceil(8) + 1, pointer)?;
        self.bytes(input.len().saturating_mul(4), pointer)?;
        let mut depth = 0usize;
        let mut slots = 1usize;
        let mut string = None;
        let mut escape = false;
        for (offset, &byte) in input.iter().enumerate() {
            if let Some(start) = string {
                if escape {
                    escape = false;
                } else if byte == b'\\' {
                    escape = true;
                } else if byte == b'"' {
                    string = None;
                    const MAX_JSON_KEY_BYTES: usize = 4096;
                    if offset - start > MAX_JSON_KEY_BYTES
                        && input[offset + 1..]
                            .iter()
                            .copied()
                            .find(|byte| !byte.is_ascii_whitespace())
                            == Some(b':')
                    {
                        return Err(limit(
                            pointer,
                            "JSON property name bytes",
                            MAX_JSON_KEY_BYTES,
                        ));
                    }
                }
                continue;
            }
            match byte {
                b'"' => string = Some(offset + 1),
                b'{' | b'[' => {
                    depth += 1;
                    slots += 1;
                    if depth > MAX_SOURCE_JSON_DEPTH {
                        return Err(limit(pointer, "JSON nesting depth", MAX_SOURCE_JSON_DEPTH));
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                b',' | b':' => slots += 1,
                _ => {}
            }
        }
        self.work(slots, pointer)?;
        self.bytes(slots.saturating_mul(192), pointer)
    }

    #[cfg(test)]
    pub(crate) fn usage(&self) -> (usize, usize) {
        (
            self.0.work.load(Ordering::Relaxed),
            self.0.bytes.load(Ordering::Relaxed),
        )
    }
}

pub(crate) const MAX_DIAGNOSTICS: usize = 1024;
pub(crate) const MAX_DIAGNOSTIC_BYTES: usize = 512 * 1024;

pub(crate) fn limit(pointer: &str, label: &str, maximum: usize) -> Failure {
    Failure {
        code: GRAPH_LIMIT_CODE.into(),
        pointer: pointer.into(),
        message: format!("Maximum {label} exceeded ({maximum})."),
    }
}

pub(crate) fn excerpt(text: &str) -> &str {
    const BYTES: usize = 160;
    &text[..text.floor_char_boundary(BYTES.min(text.len()))]
}

pub(crate) fn number_excerpt(text: &str) -> String {
    let prefix = excerpt(text);
    if prefix.len() == text.len() {
        prefix.to_owned()
    } else {
        format!("{prefix}…")
    }
}

pub(crate) fn value_excerpt(value: &crate::exact::Value) -> String {
    struct Excerpt(String);
    impl std::fmt::Write for Excerpt {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let remaining = 160 - self.0.len();
            if text.len() > remaining {
                self.0
                    .push_str(&text[..text.floor_char_boundary(remaining)]);
                return Err(std::fmt::Error);
            }
            self.0.push_str(text);
            Ok(())
        }
    }
    let mut out = Excerpt(String::new());
    if crate::exact::values::stringify_to(&mut out, value).is_err() {
        out.0.push('…');
    }
    out.0
}

#[cfg(test)]
#[path = "resource_tests.rs"]
mod resource_tests;
