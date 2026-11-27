use std::borrow::Cow;

use serde_json::Value;

/// Short authentication placeholders do not participate in echo screening.
pub const MIN_SCREENED_CREDENTIAL_BYTES: usize = 16;

/// Check decoded JSON strings (values and member names), or literal text when
/// the input is not JSON.
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
            .iter()
            .any(|(name, value)| name.contains(key) || value_contains_credential(value, key)),
        _ => false,
    }
}

/// Redact a structured result's strings (values and member names) in place
/// before serializing it.
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
            if fields.keys().any(|name| name.contains(key)) {
                let mut screened = serde_json::Map::with_capacity(fields.len());
                for (name, value) in std::mem::take(fields) {
                    let mut name = redact(&name, key).into_owned();
                    // Keep every member when two redacted names collide.
                    while screened.contains_key(&name) {
                        name.push('_');
                    }
                    screened.insert(name, value);
                }
                *fields = screened;
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

/// Hold a KMP prefix across deltas. Each character is matched in amortized constant time.
pub(crate) struct TextScreen {
    pattern: Vec<char>,
    replacement: &'static str,
    prefix: Vec<usize>,
    matched: usize,
}

impl TextScreen {
    pub(crate) fn new(key: Option<&str>) -> Self {
        let pattern: Vec<char> = key.unwrap_or_default().chars().collect();
        let mut prefix = vec![0; pattern.len()];
        for index in 1..pattern.len() {
            let mut matched = prefix[index - 1];
            while matched != 0 && pattern[index] != pattern[matched] {
                matched = prefix[matched - 1];
            }
            prefix[index] = matched + usize::from(pattern[index] == pattern[matched]);
        }
        Self {
            pattern,
            replacement: key.map_or("", replacement),
            prefix,
            matched: 0,
        }
    }

    pub(crate) fn push(&mut self, part: String) -> String {
        if self.pattern.is_empty() {
            return part;
        }
        let mut output = String::with_capacity(part.len());
        for ch in part.chars() {
            while self.matched != 0 && ch != self.pattern[self.matched] {
                let fallback = self.prefix[self.matched - 1];
                output.extend(&self.pattern[..self.matched - fallback]);
                self.matched = fallback;
            }
            if ch == self.pattern[self.matched] {
                self.matched += 1;
                if self.matched == self.pattern.len() {
                    output.push_str(self.replacement);
                    self.matched = 0;
                }
            } else {
                output.push(ch);
            }
        }
        output
    }

    pub(crate) fn finish(&mut self) -> String {
        let matched = std::mem::take(&mut self.matched);
        self.pattern[..matched].iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_names_are_screened() {
        const KEY: &str = "sk-test-0123456789abcdef";
        let arguments = serde_json::json!({"headers": {KEY: "x"}}).to_string();
        assert!(contains_credential(&arguments, KEY));

        let mut result = serde_json::json!({"map": {KEY: 1, "[redacted]": 2}, "ok": "value"});
        redact_credential_values(&mut result, KEY);
        assert!(!result.to_string().contains(KEY));
        assert_eq!(result["map"].as_object().map(serde_json::Map::len), Some(2));
        assert_eq!(result["ok"], "value");
    }

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
