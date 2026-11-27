//! Pure human waits and work budgets. Every observation is supplied by the
//! caller; this module neither reads time nor creates effects or ownership.
mod journal;
mod projection;
mod registry;
mod work_clock;
pub(crate) use super::work::{Error, Result};
use super::{
    exact::BigInteger,
    integers,
    native::{self, a, o, s, JsonDecoder, Number, Text, Value},
    work::{access, correlation, exception, integer_string, native_string, safe_integer, trimmed},
};
pub(crate) use journal::evaluate_host;
pub(crate) use registry::HumanWaitRegistry;
use std::{cell::RefCell, cmp::Ordering, rc::Rc};
pub(crate) use work_clock::WorkClock;
pub(crate) type SharedClock = Rc<RefCell<WorkClock>>;
fn plain<T>(message: &str) -> Result<T> {
    exception("Error", format!("CORRELATION: {message}"))
}
// String coercion can fail for ordinary JSON objects that shadow toString.
fn coerced_string(value: &Value) -> Result<Text> {
    if value.object().is_some() && value.has("toString") {
        return exception("TypeError", "No default value");
    }
    if let Some(items) = value.array() {
        let mut out = Text::default();
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                out.push(&Text::from(","));
            }
            if !item.is_nullish() {
                out.push(&coerced_string(item)?);
            }
        }
        return Ok(out);
    }
    Ok(native_string(value))
}
fn monotone(value: &Value) -> Result<BigInteger> {
    let raw = value.text().and_then(Text::utf8);
    let Some(raw) = raw else {
        return correlation("", "Expected canonical monotone milliseconds.");
    };
    Ok(integers::monotone_ms030(&raw)?)
}
fn integer(value: &Value) -> Result<Option<String>> {
    if matches!(value, Value::Null) {
        Ok(None)
    } else {
        Ok(Some(integer_string(value, true)?))
    }
}
fn timer(limit: Option<&str>, elapsed: &BigInteger, slice: &Value) -> Result<Option<u32>> {
    let slice = if matches!(slice, Value::Undefined) {
        integers::MAX_SLICE_MS
    } else {
        if !safe_integer(slice) {
            return correlation("", "Invalid timer slice.");
        }
        let Value::Number(Number::Finite(raw)) = slice else {
            unreachable!()
        };
        let Some(n) = BigInteger::parse(raw).and_then(|n| n.to_usize()) else {
            return correlation("", "Invalid timer slice.");
        };
        if n > integers::MAX_SLICE_MS as usize {
            return correlation("", "Invalid timer slice.");
        }
        n as i64
    };
    Ok(integers::timer_delay030(limit, elapsed, slice)?)
}
fn number(n: usize) -> Value {
    Value::Number(Number::finite(n.to_string()).unwrap())
}
pub(crate) fn delay(n: Option<u32>) -> Value {
    n.map_or(Value::Null, |n| number(n as usize))
}
fn optional_integer(n: &Option<String>) -> Value {
    n.as_ref().map_or(Value::Null, |s| native::s(s.clone()))
}
fn contains(values: &[Value], value: &Value) -> bool {
    values.iter().any(|v| strict_equal(v, value))
}
// JSON data has no shared object references. Object/array equality must not
// turn a structurally identical foreign key into a Map/Set identity.
fn strict_equal(a: &Value, b: &Value) -> bool {
    !matches!(a, Value::Array(_) | Value::Object(_)) && native::equal(a, b)
}
fn text_key(value: &Value) -> Option<&Text> {
    value.text()
}
pub(crate) fn find<T>(items: &[(Text, T)], key: &Value) -> Option<usize> {
    text_key(key).and_then(|k| items.iter().position(|(id, _)| id == k))
}
pub(crate) fn put<T>(items: &mut Vec<(Text, T)>, key: &Value, value: T) {
    if let Some(i) = find(items, key) {
        items[i].1 = value;
    } else {
        items.push((key.text().unwrap().clone(), value));
    }
}
fn object(value: &Value) -> Result<()> {
    if value.object().is_none() {
        plain("expected an object.")
    } else {
        Ok(())
    }
}
fn keys(value: &Value, allowed: &[&str]) -> Result<()> {
    let Some(entries) = value.object() else {
        return plain("expected an object.");
    };
    if entries
        .iter()
        .any(|(k, _)| !allowed.iter().any(|s| k.eq_str(s)))
    {
        plain("unexpected field.")
    } else {
        Ok(())
    }
}
fn id(value: &Value) -> Result<Value> {
    if value.text().is_none_or(Text::trimmed_empty) {
        plain("invalid identity.")
    } else {
        Ok(value.clone())
    }
}
fn nullable_string(value: &Value) -> Result<Value> {
    if !matches!(value, Value::Null | Value::String(_)) {
        plain("invalid exact limit.")
    } else {
        Ok(value.clone())
    }
}
fn ids(value: &Value) -> Result<Value> {
    let Some(items) = value.array() else {
        return plain("invalid work identities.");
    };
    Ok(a(items.iter().map(id).collect::<Result<Vec<_>>>()?))
}
fn parse_exact(value: &Value, decoder: &impl JsonDecoder) -> Result<Value> {
    let Some(text) = value.text() else {
        return plain("expected JSON text.");
    };
    super::work::exact_text(text)?;
    Ok(decoder.parse(text)?)
}
fn owner(value: &Value) -> Result<Value> {
    object(value)?;
    keys(value, &["systemStartId", "ownerEpoch"])?;
    Ok(o([
        ("systemStartId", id(value.get("systemStartId"))?),
        ("ownerEpoch", id(value.get("ownerEpoch"))?),
    ]))
}
