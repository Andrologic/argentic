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
/// Maximum bytes of a prepared route result (`resultJson`). It matches the
/// runtime's 8,000,000-byte route transport limit, so preparation fails while
/// building instead of producing a result the host would refuse.
pub const MAX_ROUTE_RESULT_BYTES: usize = 8_000_000;
/// Maximum size of one flow invocation's human-clock journal, counted as the
/// JSON string (`stateJson`) that carries it. The runtime refuses a clock
/// input above 8,000,000 bytes; the difference leaves room for the next event
/// and its envelope, so a transition is refused before the journal it
/// produces could no longer be submitted.
pub const MAX_HUMAN_CLOCK_JOURNAL_BYTES: usize = 7_500_000;

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

    /// Cumulative work units and estimated allocation bytes already reserved.
    pub fn usage(&self) -> (usize, usize) {
        (
            self.0.work.load(Ordering::Relaxed),
            self.0.bytes.load(Ordering::Relaxed),
        )
    }
}

/// Container nesting depth of JSON text, without parsing it. Brackets inside
/// strings do not count. Invalid text yields a meaningless but finite depth.
pub(crate) fn json_depth(text: &str) -> usize {
    let (mut depth, mut deepest) = (0usize, 0usize);
    let (mut string, mut escape) = (false, false);
    for byte in text.bytes() {
        if string {
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                string = false;
            }
            continue;
        }
        match byte {
            b'"' => string = true,
            b'{' | b'[' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Counts encoded bytes without keeping them.
#[derive(Default)]
pub(crate) struct ByteCount(pub(crate) usize);
impl std::fmt::Write for ByteCount {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0 += s.len();
        Ok(())
    }
}

pub(crate) const MAX_DIAGNOSTICS: usize = 1024;
pub(crate) const MAX_DIAGNOSTIC_BYTES: usize = 512 * 1024;

/// Size of the string in JSON, including delimiters and escapes.
pub(crate) fn quoted_bytes(text: &str) -> usize {
    text.len()
        + 2
        + text
            .bytes()
            .map(|b| match b {
                b'"' | b'\\' | 8 | 9 | 10 | 12 | 13 => 1,
                0..=31 => 5,
                _ => 0,
            })
            .sum::<usize>()
}

pub(crate) const DIAGNOSTIC_LIMIT_MESSAGE: &str =
    "Maximum validation diagnostic count or bytes exceeded.";

// Buffer a small amount of work to avoid a shared atomic update for every
// punctuation mark and character emitted by the JSON encoders.
pub(crate) struct EncodeCount<F> {
    pub(crate) bytes: usize,
    pub(crate) units: usize,
    pub(crate) error: Option<F>,
    pending: usize,
    max: usize,
}

impl<F> EncodeCount<F> {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            bytes: 0,
            units: 0,
            error: None,
            pending: 0,
            max,
        }
    }

    pub(crate) fn write(
        &mut self,
        text: &str,
        count_utf16: bool,
        charge: impl FnOnce(usize) -> Result<(), F>,
    ) -> std::fmt::Result {
        let fits = text.len() <= self.max - self.bytes;
        self.pending += if fits {
            text.len()
        } else {
            (self.max - self.bytes).min(text.len()) + 1
        };
        if !fits || self.pending >= 512 {
            self.flush(charge)?;
        }
        if !fits {
            return Err(std::fmt::Error);
        }
        self.bytes += text.len();
        if count_utf16 {
            self.units += if text.is_ascii() {
                text.len()
            } else {
                text.encode_utf16().count()
            };
        }
        Ok(())
    }

    pub(crate) fn flush(
        &mut self,
        charge: impl FnOnce(usize) -> Result<(), F>,
    ) -> std::fmt::Result {
        if self.error.is_some() {
            return Err(std::fmt::Error);
        }
        let pending = std::mem::take(&mut self.pending);
        if pending > 0 {
            if let Err(error) = charge(pending.div_ceil(16)) {
                self.error = Some(error);
                return Err(std::fmt::Error);
            }
        }
        Ok(())
    }
}

pub(crate) fn encode_string(
    budget: &SourceBudget,
    max: usize,
    pointer: &str,
    write: impl Fn(&mut dyn std::fmt::Write) -> std::fmt::Result,
) -> Result<Option<String>, Failure> {
    struct Count<'a> {
        count: EncodeCount<Failure>,
        budget: &'a SourceBudget,
        pointer: &'a str,
    }

    impl std::fmt::Write for Count<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.count
                .write(text, false, |work| self.budget.work(work, self.pointer))
        }
    }

    let mut counter = Count {
        count: EncodeCount::new(max),
        budget,
        pointer,
    };
    let measured = write(&mut counter);
    let finished = counter.count.flush(|work| budget.work(work, pointer));
    if let Some(error) = counter.count.error {
        return Err(error);
    }
    if measured.is_err() || finished.is_err() {
        return Ok(None);
    }
    let bytes = counter.count.bytes;
    budget.work(bytes.div_ceil(16), pointer)?;
    budget.bytes(bytes, pointer)?;
    let mut result = String::with_capacity(bytes);
    write(&mut result).expect("writing to a String");
    Ok(Some(result))
}

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
