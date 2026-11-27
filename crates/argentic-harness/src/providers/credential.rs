use std::borrow::Cow;

use serde_json::Value;

/// Short authentication placeholders do not participate in echo screening.
pub const MIN_SCREENED_CREDENTIAL_BYTES: usize = 16;

/// Check decoded JSON string values, or literal text when the input is not JSON.
pub fn contains_credential(raw: &str, key: &str) -> bool {
    key.len() >= MIN_SCREENED_CREDENTIAL_BYTES
        && serde_json::from_str(raw).map_or_else(
            |_| raw.contains(key),
            |value| value_contains_credential(&value, key),
        )
}

pub(super) fn value_contains_credential(value: &Value, key: &str) -> bool {
    match value {
        Value::String(value) => value.contains(key),
        Value::Array(values) => values
            .iter()
            .any(|value| value_contains_credential(value, key)),
        Value::Object(fields) => fields
            .values()
            .any(|value| value_contains_credential(value, key)),
        _ => false,
    }
}

/// Redact a structured result's string values in place before serializing it.
pub fn redact_credential_values(value: &mut Value, key: &str) {
    if key.len() < MIN_SCREENED_CREDENTIAL_BYTES {
        return;
    }
    match value {
        Value::String(text) => {
            if let Cow::Owned(screened) = redact(text, key) {
                *text = screened;
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_credential_values(value, key);
            }
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                redact_credential_values(value, key);
            }
        }
        _ => {}
    }
}

fn replacement(key: &str) -> &'static str {
    // Keep the replacement from recreating the key alongside neighbouring
    // text. HTTP header credentials cannot contain newlines.
    if key.contains(['[', ']']) {
        "\n"
    } else {
        "[redacted]"
    }
}

pub(crate) fn redact<'a>(text: &'a str, key: &str) -> Cow<'a, str> {
    if !text.contains(key) {
        return Cow::Borrowed(text);
    }
    Cow::Owned(text.replace(key, replacement(key)))
}

/// Hold a KMP prefix across deltas. Each byte is matched in amortized constant time.
pub(crate) struct TextScreen<'a> {
    key: Option<&'a str>,
    replacement: &'static str,
    prefix: Vec<usize>,
    matched: usize,
}

impl<'a> TextScreen<'a> {
    pub(crate) fn new(key: Option<&'a str>) -> Self {
        let mut prefix = Vec::new();
        if let Some(key) = key {
            let key = key.as_bytes();
            prefix.resize(key.len(), 0);
            for index in 1..key.len() {
                let mut matched = prefix[index - 1];
                while matched != 0 && key[index] != key[matched] {
                    matched = prefix[matched - 1];
                }
                prefix[index] = matched + usize::from(key[index] == key[matched]);
            }
        }
        Self {
            key,
            replacement: key.map_or("", replacement),
            prefix,
            matched: 0,
        }
    }

    pub(crate) fn push(&mut self, part: String) -> String {
        let Some(key) = self.key else {
            return part;
        };
        let pattern = key.as_bytes();
        let mut output = Vec::with_capacity(part.len());
        for byte in part.bytes() {
            while self.matched != 0 && byte != pattern[self.matched] {
                let fallback = self.prefix[self.matched - 1];
                output.extend_from_slice(&pattern[..self.matched - fallback]);
                self.matched = fallback;
            }
            if byte == pattern[self.matched] {
                self.matched += 1;
                if self.matched == pattern.len() {
                    output.extend_from_slice(self.replacement.as_bytes());
                    self.matched = 0;
                }
            } else {
                output.push(byte);
            }
        }
        // A pending prefix starts on a UTF-8 boundary and each fragment ends on one.
        String::from_utf8(output).expect("screened text remains UTF-8")
    }

    pub(crate) fn finish(&mut self) -> String {
        let matched = std::mem::take(&mut self.matched);
        self.key.unwrap_or_default()[..matched].to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_screen_preserves_partial_unicode_and_repetitive_prefixes() {
        let key = "étéétéétéété!";
        let text = format!("été{key} et {key} suivi de été");
        let mut screen = TextScreen::new(Some(key));
        let mut actual = String::new();
        for ch in text.chars() {
            actual.push_str(&screen.push(ch.to_string()));
        }
        actual.push_str(&screen.finish());
        assert_eq!(actual, text.replace(key, "[redacted]"));
    }
}
