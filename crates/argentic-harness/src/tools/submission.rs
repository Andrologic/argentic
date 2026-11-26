use std::fmt;

use serde::{
    de::{self, MapAccess, Visitor},
    Deserializer,
};
use serde_json::{value::RawValue, Map, Value};

pub(super) fn validate(
    validator: &jsonschema::Validator,
    arguments: &str,
) -> Result<Value, String> {
    let input: &RawValue = serde_json::from_str(arguments).map_err(|e| e.to_string())?;
    let value = exact_value(input, 0).map_err(|e| e.to_string())?;
    let object = value
        .as_object()
        .ok_or("Result tool arguments must be an object")?;
    for key in object.keys() {
        if !matches!(key.as_str(), "results" | "choice" | "instructions") {
            return Err(format!("Unknown result tool argument: {key}"));
        }
    }
    if object
        .get("results")
        .is_some_and(|results| !results.is_object())
    {
        return Err("/results: must be an object".into());
    }
    validator
        .validate(&value)
        .map_err(|e| format!("{}: {e}", e.instance_path()))?;
    Ok(value)
}

fn exact_value(raw: &RawValue, depth: usize) -> serde_json::Result<Value> {
    if depth >= 128 {
        return Err(de::Error::custom(
            "Result tool arguments exceed the nesting limit",
        ));
    }
    let raw = raw.get();
    match raw.as_bytes().first() {
        Some(b'{') => {
            serde_json::Deserializer::from_str(raw).deserialize_map(UniqueObject(depth + 1))
        }
        Some(b'[') => {
            let values: Vec<&RawValue> = serde_json::from_str(raw)?;
            values
                .into_iter()
                .map(|value| exact_value(value, depth + 1))
                .collect::<Result<_, _>>()
                .map(Value::Array)
        }
        _ => serde_json::from_str(raw),
    }
}

struct UniqueObject(usize);

impl<'de> Visitor<'de> for UniqueObject {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON object without duplicate members")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = entries.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "Duplicate result tool object member: {key}"
                )));
            }
            let raw = entries.next_value::<&RawValue>()?;
            object.insert(key, exact_value(raw, self.0).map_err(de::Error::custom)?);
        }
        Ok(Value::Object(object))
    }
}
