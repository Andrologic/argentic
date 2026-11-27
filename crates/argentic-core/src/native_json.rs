//! Structural half of the existing ordinary JSON codec. Scalar conversion is
//! supplied explicitly by the runtime; authored values retain their own scanner.
use crate::argentic_core::native::{JsonDecoder, Number, SyntaxError, Text, Value};
#[path = "native_json_syntax.rs"]
mod syntax;
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::value::RawValue;
use std::fmt;
#[derive(Debug)]
pub(crate) struct Error(pub(crate) String);
struct Bytes(Vec<u8>);
impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Bytes;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("native JSON string")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(v.to_vec()))
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Bytes, E> {
                Ok(Bytes(v))
            }
        }
        d.deserialize_bytes(V)
    }
}
fn text(bytes: &[u8]) -> Text {
    let mut units = vec![];
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let (n, mut scalar) = if b < 128 {
            (1, u32::from(b))
        } else if b < 224 {
            (2, u32::from(b & 31))
        } else if b < 240 {
            (3, u32::from(b & 15))
        } else {
            (4, u32::from(b & 7))
        };
        for k in 1..n {
            scalar = (scalar << 6) | u32::from(bytes[i + k] & 63);
        }
        if scalar <= 65535 {
            units.push(scalar as u16)
        } else {
            let v = scalar - 65536;
            units.push(0xd800 + (v >> 10) as u16);
            units.push(0xdc00 + (v & 1023) as u16);
        }
        i += n;
    }
    Text::from_units(units)
}
struct Members<'a>(Vec<(Text, &'a RawValue)>);
impl<'de> Deserialize<'de> for Members<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Members<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("native JSON object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut entries = vec![];
                while let Some((key, value)) = map.next_entry::<Bytes, &RawValue>()? {
                    entries.push((text(&key.0), value));
                }
                Ok(Members(entries))
            }
        }
        d.deserialize_map(V)
    }
}
pub(crate) fn parse(raw: &str, number: super::calculation::NativeNumber) -> Result<Value, Error> {
    parse_charged(raw, number, |_| Ok(()))
}

fn parse_charged(
    raw: &str,
    number: super::calculation::NativeNumber,
    mut visit: impl FnMut(&str) -> Result<(), Error>,
) -> Result<Value, Error> {
    let root: &RawValue = serde_json::from_str(raw).map_err(|e| Error(e.to_string()))?;
    enum Task<'a> {
        Visit(&'a RawValue),
        Array(usize),
        Object(Vec<Text>),
    }
    let mut tasks = vec![Task::Visit(root)];
    let mut values = vec![];
    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit(raw) => {
                let json = raw.get();
                visit(json)?;
                match json.as_bytes()[0] {
                    b'n' => values.push(Value::Null),
                    b't' => values.push(Value::Bool(true)),
                    b'f' => values.push(Value::Bool(false)),
                    b'"' => {
                        let bytes: Bytes =
                            serde_json::from_str(json).map_err(|e| Error(e.to_string()))?;
                        values.push(Value::String(text(&bytes.0)));
                    }
                    b'[' => {
                        let items: Vec<&RawValue> =
                            serde_json::from_str(json).map_err(|e| Error(e.to_string()))?;
                        tasks.push(Task::Array(items.len()));
                        tasks.extend(items.into_iter().rev().map(Task::Visit));
                    }
                    b'{' => {
                        let Members(entries) =
                            serde_json::from_str(json).map_err(|e| Error(e.to_string()))?;
                        tasks.push(Task::Object(
                            entries.iter().map(|(k, _)| k.clone()).collect(),
                        ));
                        tasks.extend(entries.into_iter().rev().map(|(_, v)| Task::Visit(v)));
                    }
                    _ => {
                        let normalized = number(json).map_err(Error)?;
                        values.push(Value::Number(match normalized.as_str() {
                            "NaN" => Number::NaN,
                            "Infinity" => Number::PositiveInfinity,
                            "-Infinity" => Number::NegativeInfinity,
                            _ => {
                                Number::finite(normalized).map_err(|e| Error(e.message.lossy()))?
                            }
                        }));
                    }
                }
            }
            Task::Array(n) => {
                let items = values.split_off(values.len() - n);
                values.push(Value::Array(items));
            }
            Task::Object(keys) => {
                let items = values.split_off(values.len() - keys.len());
                values.push(Value::from_entries(keys.into_iter().zip(items)));
            }
        }
    }
    Ok(values.pop().unwrap())
}

/// The integrated codec implements the core's durable, pure decoding port.
pub(crate) struct Decoder(pub(crate) super::calculation::NativeNumber);
impl Decoder {
    /// The compiler already holds valid UTF-8 transport bytes. Charge each
    /// nested raw parse and pass those bytes directly to the native decoder.
    pub(crate) fn parse_source(
        &self,
        raw: &str,
        at: &str,
        budget: &crate::SourceBudget,
    ) -> Result<Value, super::calculation::Failure> {
        use super::calculation::Failure;
        let syntax_error = |message| Failure {
            code: "NATIVE_JSON".into(),
            pointer: String::new(),
            message,
        };
        budget.bytes(raw.len() * 2, at)?;
        syntax::validate(&Text::from(raw)).map_err(|e| syntax_error(e.message.lossy()))?;
        let mut limit = None;
        let parsed = parse_charged(raw, self.0, |value| {
            // Containers are scanned again by serde for each enclosing frame.
            let size = value.len();
            let charge = budget.work(1 + size.div_ceil(16), at).and_then(|()| {
                if matches!(value.as_bytes()[0], b'"' | b'-' | b'0'..=b'9') {
                    budget.bytes(size * 8, at)?;
                }
                Ok(())
            });
            charge.map_err(|error| {
                limit = Some(error);
                Error(String::new())
            })
        });
        parsed.map_err(|error| limit.unwrap_or_else(|| syntax_error(error.0)))
    }
}

impl JsonDecoder for Decoder {
    fn parse(&self, raw: &Text) -> Result<Value, SyntaxError> {
        syntax::validate(raw)?;
        // Literal isolated UTF-16 units are legal inside ordinary JSON strings.
        // Escape units before the byte parser, without repairing their value.
        let mut bytes = String::new();
        for &unit in raw.units() {
            if unit < 128 {
                bytes.push(char::from_u32(u32::from(unit)).unwrap());
            } else {
                bytes.push_str(&format!("\\u{unit:04x}"));
            }
        }
        parse(&bytes, self.0).map_err(|e| SyntaxError {
            message: Text::from(e.0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn boundary(raw: &str) -> Result<String, String> {
        Ok(match raw {
            "9007199254740993" => "9007199254740992".into(),
            _ => raw.to_owned(),
        })
    }
    #[test]
    fn scalar_port_does_not_convert_authored_json_inside_text() {
        let raw = r#"{"native":9007199254740993,"authored":"{\"n\":9007199254740993}","text":"\ud800","zero":-0}"#;
        let value = Decoder(boundary).parse(&Text::from(raw)).unwrap();
        assert_eq!(
            value.stringify(),
            r#"{"native":9007199254740992,"authored":"{\"n\":9007199254740993}","text":"\ud800","zero":0}"#
        );
        assert_eq!(
            value.get("authored").text().unwrap().utf8().unwrap(),
            r#"{"n":9007199254740993}"#
        );
    }
    #[test]
    fn native_json_respects_source_depth_limit() {
        let depth = crate::MAX_SOURCE_JSON_DEPTH;
        let decoder = Decoder(boundary);
        let raw = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert!(decoder.parse(&Text::from(raw.as_str())).is_ok());
        let raw = format!(r#"{{"value":{raw}}}"#);
        let error = decoder.parse(&Text::from(raw.as_str())).unwrap_err();
        assert!(error.message.lossy().contains("JSON nesting depth"));
    }

    #[test]
    fn syntax_refusal_precedes_scalar_conversion() {
        fn forbidden(_: &str) -> Result<String, String> {
            panic!("conversion before syntax validation")
        }
        assert!(Decoder(forbidden).parse(&Text::from("[1,]")).is_err());
    }
}
