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
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ops::{Index, IndexMut},
    rc::Rc,
};
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
/// Entries in first-insertion order, found by text identity without a scan.
/// A journal replays every entry on each event, so linear lookups made a
/// replay quadratic in the number of works and requests.
pub(crate) struct Keyed<T> {
    items: Vec<(Text, T)>,
    index: HashMap<Text, usize>,
}
impl<T> Default for Keyed<T> {
    fn default() -> Self {
        Self {
            items: vec![],
            index: HashMap::new(),
        }
    }
}
impl<T> Keyed<T> {
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, (Text, T)> {
        self.items.iter()
    }
    pub(crate) fn iter_mut(&mut self) -> std::slice::IterMut<'_, (Text, T)> {
        self.items.iter_mut()
    }
    /// Removal keeps the order of the remaining entries.
    pub(crate) fn remove(&mut self, i: usize) {
        let (key, _) = self.items.remove(i);
        self.index.remove(&key);
        for (position, (key, _)) in self.items.iter().enumerate().skip(i) {
            *self.index.get_mut(key).expect("indexed entry") = position;
        }
    }
}
impl<T> Index<usize> for Keyed<T> {
    type Output = (Text, T);
    fn index(&self, i: usize) -> &Self::Output {
        &self.items[i]
    }
}
impl<T> IndexMut<usize> for Keyed<T> {
    fn index_mut(&mut self, i: usize) -> &mut Self::Output {
        &mut self.items[i]
    }
}
pub(crate) fn find<T>(items: &Keyed<T>, key: &Value) -> Option<usize> {
    key.text().and_then(|k| items.index.get(k).copied())
}
pub(crate) fn put<T>(items: &mut Keyed<T>, key: &Value, value: T) {
    if let Some(i) = find(items, key) {
        items.items[i].1 = value;
    } else {
        let key = key.text().unwrap().clone();
        items.index.insert(key.clone(), items.items.len());
        items.items.push((key, value));
    }
}
/// Identities are validated text, so set membership matches strict equality.
fn has_text(set: &HashSet<Text>, value: &Value) -> bool {
    value.text().is_some_and(|text| set.contains(text))
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
