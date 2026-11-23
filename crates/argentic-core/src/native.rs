//! Data exchanged with the explicit native JSON codec. This is not an authored
//! value tree. Numbers hold their one canonical rounded decimal representation;
//! no redundant bit pattern can disagree with it. Native text keeps UTF-16.
use super::exact;

/// Ordinary JSON syntax errors retain native text, including isolated units.
/// They are not authored-value or work-correlation failures.
#[derive(Clone, Debug)]
pub(crate) struct SyntaxError {
    pub(crate) message: Text,
}

/// Pure decoding boundary. Implemented by the ordinary JSON codec; authored
/// values continue to use the exact reader. Parsing happens when requested,
/// so a journal replay retains the original validation and error order.
pub(crate) trait JsonDecoder {
    fn parse(&self, raw: &Text) -> Result<Value, SyntaxError>;
}
#[derive(Clone, Debug)]
pub(crate) struct Error {
    pub(crate) code: &'static str,
    pub(crate) pointer: Text,
    pub(crate) message: Text,
}
impl Error {
    pub(crate) fn new(
        code: &'static str,
        pointer: impl Into<Text>,
        message: impl Into<Text>,
    ) -> Self {
        Self {
            code,
            pointer: pointer.into(),
            message: message.into(),
        }
    }
}
impl From<super::error::Failure> for Error {
    fn from(e: super::error::Failure) -> Self {
        Self::new(e.code, e.pointer, e.message)
    }
}
pub(crate) fn fail<T>(
    code: &'static str,
    p: impl Into<Text>,
    m: impl Into<Text>,
) -> Result<T, Error> {
    Err(Error::new(code, p, m))
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct Text(Vec<u16>);
impl Text {
    pub(crate) fn from_units(units: Vec<u16>) -> Self {
        Self(units)
    }
    pub(crate) fn units(&self) -> &[u16] {
        &self.0
    }
    pub(crate) fn utf8(&self) -> Option<String> {
        String::from_utf16(&self.0).ok()
    }
    pub(crate) fn lossy(&self) -> String {
        String::from_utf16_lossy(&self.0)
    }
    pub(crate) fn eq_str(&self, s: &str) -> bool {
        self.0.iter().copied().eq(s.encode_utf16())
    }
    pub(crate) fn trimmed_empty(&self) -> bool {
        self.0.iter().all(|u|matches!(u,0x0009..=0x000d|0x0020|0x00a0|0x1680|0x2000..=0x200a|0x2028|0x2029|0x202f|0x205f|0x3000|0xfeff))
    }
    pub(crate) fn push(&mut self, s: &Self) {
        self.0.extend_from_slice(&s.0)
    }
    fn quote_to(&self, out: &mut (impl std::fmt::Write + ?Sized)) -> std::fmt::Result {
        out.write_char('"')?;
        for c in char::decode_utf16(self.0.iter().copied()) {
            match c {
                Err(e) => write!(out, "\\u{:04x}", e.unpaired_surrogate())?,
                Ok(c) => exact::quote_char(out, c)?,
            }
        }
        out.write_char('"')
    }
}
impl From<&str> for Text {
    fn from(s: &str) -> Self {
        let mut units = Vec::with_capacity(s.len());
        units.extend(s.encode_utf16());
        Self(units)
    }
}
impl From<String> for Text {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Number {
    Finite(String),
    PositiveInfinity,
    NegativeInfinity,
    NaN,
}
impl Number {
    /// The boundary supplies a rounded, JS-canonical finite decimal, or -0 to
    /// retain the native sign. No authored NumberToken uses this constructor.
    pub(crate) fn finite(decimal: String) -> Result<Self, Error> {
        if exact::NumberToken::lexeme(&decimal).is_none() {
            return fail("NATIVE_JSON", "", "Invalid native finite decimal.");
        }
        Ok(Self::Finite(decimal))
    }
    pub(crate) fn is_finite(&self) -> bool {
        matches!(self, Self::Finite(_))
    }
    fn json(&self) -> &str {
        match self {
            Self::Finite(s) if s == "-0" => "0",
            Self::Finite(s) => s,
            _ => "null",
        }
    }
}
#[derive(Debug)]
pub(crate) enum Value {
    Undefined,
    Null,
    Bool(bool),
    String(Text),
    Number(Number),
    Array(Vec<Value>),
    Object(Vec<(Text, Value)>),
}
impl Clone for Value {
    fn clone(&self) -> Self {
        enum Task<'a> {
            Visit(&'a Value),
            Array(usize),
            Object(Vec<Text>),
        }
        let mut pending = vec![Task::Visit(self)];
        let mut done = Vec::new();
        while let Some(task) = pending.pop() {
            match task {
                Task::Visit(Self::Array(v)) => {
                    pending.push(Task::Array(v.len()));
                    pending.extend(v.iter().rev().map(Task::Visit));
                }
                Task::Visit(Self::Object(v)) => {
                    pending.push(Task::Object(v.iter().map(|(k, _)| k.clone()).collect()));
                    pending.extend(v.iter().rev().map(|(_, v)| Task::Visit(v)));
                }
                Task::Visit(v) => done.push(match v {
                    Self::Undefined => Self::Undefined,
                    Self::Null => Self::Null,
                    Self::Bool(v) => Self::Bool(*v),
                    Self::String(v) => Self::String(v.clone()),
                    Self::Number(v) => Self::Number(v.clone()),
                    _ => unreachable!(),
                }),
                Task::Array(n) => {
                    let values = done.split_off(done.len() - n);
                    done.push(Self::Array(values));
                }
                Task::Object(keys) => {
                    let values = done.split_off(done.len() - keys.len());
                    done.push(Self::Object(keys.into_iter().zip(values).collect()));
                }
            }
        }
        done.pop().unwrap()
    }
}
impl Drop for Value {
    fn drop(&mut self) {
        fn detach(v: &mut Value, p: &mut Vec<Value>) {
            match v {
                Value::Array(v) => p.append(v),
                Value::Object(v) => p.extend(std::mem::take(v).into_iter().map(|(_, v)| v)),
                _ => {}
            }
        }
        let mut pending = vec![];
        detach(self, &mut pending);
        while let Some(mut v) = pending.pop() {
            detach(&mut v, &mut pending)
        }
    }
}
static UNDEFINED: Value = Value::Undefined;
impl Value {
    pub(crate) fn get(&self, key: &str) -> &Self {
        self.get_text(&Text::from(key))
    }
    pub(crate) fn get_text(&self, key: &Text) -> &Self {
        if let Self::Object(v) = self {
            if let Some((_, v)) = v.iter().find(|(k, _)| k == key) {
                return v;
            }
        }
        &UNDEFINED
    }
    pub(crate) fn has(&self, key: &str) -> bool {
        !matches!(self.get(key), Self::Undefined)
    }
    pub(crate) fn text(&self) -> Option<&Text> {
        if let Self::String(s) = self {
            Some(s)
        } else {
            None
        }
    }
    pub(crate) fn eq_str(&self, s: &str) -> bool {
        self.text().is_some_and(|t| t.eq_str(s))
    }
    pub(crate) fn array(&self) -> Option<&[Value]> {
        if let Self::Array(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn object(&self) -> Option<&[(Text, Value)]> {
        if let Self::Object(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn is_nullish(&self) -> bool {
        matches!(self, Self::Null | Self::Undefined)
    }
    pub(crate) fn truthy(&self) -> bool {
        match self {
            Self::Undefined | Self::Null | Self::Bool(false) => false,
            Self::String(s) => !s.units().is_empty(),
            Self::Number(Number::Finite(s)) => s != "0" && s != "-0",
            Self::Number(Number::NaN) => false,
            _ => true,
        }
    }
    /// JS property order, with duplicate keys replaced in their original slot.
    pub(crate) fn from_entries(entries: impl IntoIterator<Item = (Text, Value)>) -> Self {
        let entries = entries.into_iter();
        let size = entries.size_hint().0;
        if entries.size_hint().1.is_some_and(|max| max <= 16) {
            let mut value = Self::Object(Vec::with_capacity(size));
            for (key, entry) in entries {
                value.set(key, entry);
            }
            return value;
        }
        let mut positions = std::collections::HashMap::<Text, usize>::with_capacity(size);
        let mut fields: Vec<(Text, Value)> = Vec::with_capacity(size);
        for (key, value) in entries {
            if let Some(&position) = positions.get(&key) {
                fields[position].1 = value;
            } else {
                positions.insert(key.clone(), fields.len());
                fields.push((key, value));
            }
        }
        if fields.iter().any(|(key, _)| index(key).is_some()) {
            fields.sort_by_cached_key(|(key, _)| index(key).unwrap_or(u32::MAX));
        }
        Self::Object(fields)
    }

    pub(crate) fn clone_with_budget(
        &self,
        budget: &crate::SourceBudget,
        at: &str,
    ) -> Result<Self, Error> {
        use crate::compile::limit_error;
        budget.work(1, at).map_err(limit_error)?;
        match self {
            Self::Array(values) => {
                budget
                    .bytes(std::mem::size_of_val(values.as_slice()), at)
                    .map_err(limit_error)?;
                Ok(Self::Array(
                    values
                        .iter()
                        .map(|value| value.clone_with_budget(budget, at))
                        .collect::<Result<_, _>>()?,
                ))
            }
            Self::Object(fields) => {
                budget
                    .bytes(std::mem::size_of_val(fields.as_slice()), at)
                    .map_err(limit_error)?;
                Ok(Self::Object(
                    fields
                        .iter()
                        .map(|(key, value)| {
                            budget
                                .bytes(std::mem::size_of_val(key.units()), at)
                                .map_err(limit_error)?;
                            Ok((key.clone(), value.clone_with_budget(budget, at)?))
                        })
                        .collect::<Result<_, Error>>()?,
                ))
            }
            Self::String(text) => {
                budget
                    .bytes(std::mem::size_of_val(text.units()), at)
                    .map_err(limit_error)?;
                budget
                    .work(text.units().len().div_ceil(16), at)
                    .map_err(limit_error)?;
                Ok(Self::String(text.clone()))
            }
            Self::Number(Number::Finite(number)) => {
                budget.bytes(number.len(), at).map_err(limit_error)?;
                Ok(Self::Number(Number::Finite(number.clone())))
            }
            scalar => Ok(scalar.clone()),
        }
    }

    pub(crate) fn set(&mut self, key: impl Into<Text>, value: Value) {
        let key = key.into();
        let Self::Object(entries) = self else {
            panic!("native object assignment requires an object")
        };
        if let Some((_, v)) = entries.iter_mut().find(|(k, _)| k == &key) {
            *v = value;
        } else {
            if let Some(order) = index(&key) {
                let position =
                    entries.partition_point(|(key, _)| index(key).is_some_and(|key| key < order));
                entries.insert(position, (key, value));
            } else {
                entries.push((key, value));
            }
        }
    }
    pub(crate) fn stringify(&self) -> String {
        let mut out = String::new();
        self.stringify_to(&mut out).expect("writing to a String");
        out
    }

    pub(crate) fn stringify_to(
        &self,
        out: &mut (impl std::fmt::Write + ?Sized),
    ) -> std::fmt::Result {
        enum Task<'a> {
            Value(&'a Value),
            Array(std::slice::Iter<'a, Value>),
            Object(std::slice::Iter<'a, (Text, Value)>),
        }
        let mut tasks = vec![Task::Value(self)];
        while let Some(task) = tasks.pop() {
            match task {
                Task::Array(mut items) => {
                    if let Some(item) = items.next() {
                        out.write_char(',')?;
                        tasks.push(Task::Array(items));
                        tasks.push(Task::Value(item));
                    } else {
                        out.write_char(']')?;
                    }
                }
                Task::Object(mut fields) => {
                    if let Some((key, value)) = fields.find(|(_, v)| !matches!(v, Self::Undefined))
                    {
                        out.write_char(',')?;
                        key.quote_to(out)?;
                        out.write_char(':')?;
                        tasks.push(Task::Object(fields));
                        tasks.push(Task::Value(value));
                    } else {
                        out.write_char('}')?;
                    }
                }
                Task::Value(v) => match v {
                    Self::Undefined | Self::Null => out.write_str("null")?,
                    Self::Bool(b) => out.write_str(if *b { "true" } else { "false" })?,
                    Self::String(t) => t.quote_to(out)?,
                    Self::Number(n) => out.write_str(n.json())?,
                    Self::Array(values) => {
                        let mut items = values.iter();
                        if let Some(first) = items.next() {
                            out.write_char('[')?;
                            tasks.push(Task::Array(items));
                            tasks.push(Task::Value(first));
                        } else {
                            out.write_str("[]")?;
                        }
                    }
                    Self::Object(values) => {
                        let mut fields = values.iter();
                        if let Some((key, first)) =
                            fields.find(|(_, v)| !matches!(v, Self::Undefined))
                        {
                            out.write_char('{')?;
                            key.quote_to(out)?;
                            out.write_char(':')?;
                            tasks.push(Task::Object(fields));
                            tasks.push(Task::Value(first));
                        } else {
                            out.write_str("{}")?;
                        }
                    }
                },
            }
        }
        Ok(())
    }
}
fn index(key: &Text) -> Option<u32> {
    let s = key.utf8()?;
    exact::scan::array_index(&s)
}
pub(crate) fn s(value: impl Into<Text>) -> Value {
    Value::String(value.into())
}
pub(crate) fn o<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::from_entries(
        fields
            .into_iter()
            .map(|(key, value)| (Text::from(key), value)),
    )
}
pub(crate) fn a(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(values.into_iter().collect())
}
/// Structural equality for native DTO fields, separate from exact token equality.
pub(crate) fn equal(left: &Value, right: &Value) -> bool {
    let mut todo = Vec::new();
    let mut first = Some((left, right));
    while let Some((l, r)) = first.take().or_else(|| todo.pop()) {
        match (l, r) {
            (Value::Undefined, Value::Undefined) | (Value::Null, Value::Null) => {}
            (Value::Bool(a), Value::Bool(b)) if a == b => {}
            (Value::String(a), Value::String(b)) if a == b => {}
            (Value::Number(Number::Finite(a)), Value::Number(Number::Finite(b)))
                if a.trim_start_matches('-') == "0" && b.trim_start_matches('-') == "0"
                    || a == b => {}
            (Value::Number(Number::PositiveInfinity), Value::Number(Number::PositiveInfinity))
            | (Value::Number(Number::NegativeInfinity), Value::Number(Number::NegativeInfinity)) => {
            }
            (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
                todo.extend(a.iter().zip(b))
            }
            (Value::Object(a), Value::Object(b)) if a.len() == b.len() => {
                for (k, v) in a {
                    let Some((_, w)) = b.iter().find(|(key, _)| key == k) else {
                        return false;
                    };
                    todo.push((v, w));
                }
            }
            _ => return false,
        }
    }
    true
}
