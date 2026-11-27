use std::borrow::Cow;

use serde_json::Value;

/// Check both literal text and decoded JSON keys and string values.
pub fn contains_credential(raw: &str, key: &str) -> bool {
    fn contains(value: &Value, key: &str) -> bool {
        match value {
            Value::String(value) => value.contains(key),
            Value::Array(values) => values.iter().any(|value| contains(value, key)),
            Value::Object(fields) => fields
                .iter()
                .any(|(name, value)| name.contains(key) || contains(value, key)),
            _ => false,
        }
    }
    !key.is_empty()
        && (raw.contains(key) || serde_json::from_str(raw).is_ok_and(|value| contains(&value, key)))
}

pub(crate) fn redact<'a>(text: &'a str, key: &str) -> Cow<'a, str> {
    if !text.contains(key) {
        return Cow::Borrowed(text);
    }
    // Keep the replacement from recreating the key alongside neighbouring
    // text. HTTP header credentials cannot contain newlines.
    let marker = if key.contains(['[', ']']) || "[redacted]".contains(key) {
        "\n"
    } else {
        "[redacted]"
    };
    Cow::Owned(text.replace(key, marker))
}

/// Hold a possible prefix across deltas so the same credential is screened
/// whether it arrives in one chunk or many.
#[derive(Default)]
pub(crate) struct TextScreen(String);

impl TextScreen {
    pub(crate) fn push(&mut self, part: String, key: Option<&str>) -> String {
        let Some(key) = key else {
            return part;
        };
        if self.0.is_empty() {
            self.0 = part;
        } else {
            self.0.push_str(&part);
        }
        let end_of_match = self
            .0
            .match_indices(key)
            .last()
            .map_or(0, |(start, _)| start + key.len());
        let tail = &self.0[end_of_match..];
        let safe = tail
            .char_indices()
            .rev()
            .take_while(|(start, _)| tail.len() - start < key.len())
            .filter(|(start, _)| key.starts_with(&tail[*start..]))
            .last()
            .map_or(self.0.len(), |(start, _)| end_of_match + start);
        let tail = self.0.split_off(safe);
        let text = std::mem::replace(&mut self.0, tail);
        match redact(&text, key) {
            Cow::Borrowed(_) => text,
            Cow::Owned(text) => text,
        }
    }

    pub(crate) fn finish(&mut self) -> String {
        std::mem::take(&mut self.0)
    }
}
