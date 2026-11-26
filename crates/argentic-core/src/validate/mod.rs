// SPDX-License-Identifier: Apache-2.0
//! Static validation of AgSDL 0.3 documents. Rust translation of
//! `src/vendor/agsdl/v0.3/validator.mjs` (port of AgSDL v0.3.0
//! tooling/agsdl/validator.py, upstream commit
//! bd9f8a5b16923476fd20ac2650bd1729ee151917). Provenance and license:
//! `src/vendor/agsdl/NOTICE.md`, `src/vendor/agsdl/v0.3/LICENSE`.
//!
//! Diagnostics keep the JS order, deduplication and pointers. Large authored
//! values in messages are excerpted. Approval prompt propagation is iterative,
//! as are selection and resource walks over author-controlled depth.
//! [`Internal`] stands for an exception other than `ModuleError` in the JS
//! code; none is reachable from a shape-valid document and pinned schemas.
pub(crate) mod permissions;

use super::{
    exact::{
        quote, quote_to,
        scan::array_index,
        values::{compare_decimals, decimal_v030, is_integer_number, Num},
        Object, Value,
    },
    graph::{
        address_parts, analyse_joins, empty_object, is_route_map, key, open_body_forks,
        ordered::{OrderedMap, OrderedSet},
        route_items, route_len, route_targets, sort_utf16, sorted_addresses, step_routes, Edge,
        ExpansionBudget, Graph, InBranch, Node, RouteKind, TargetKind, Unshaped, GRAPH_LIMIT_CODE,
    },
    schema::{self, truthy, SchemaError},
    valueschema::{self, path_pointer, Descent, OutsideShape, PointerPart},
};
use crate::{
    calculation::Failure,
    resource::{
        encode_string, excerpt, number_excerpt, quoted_bytes, value_excerpt,
        DIAGNOSTIC_LIMIT_MESSAGE, MAX_DIAGNOSTICS, MAX_DIAGNOSTIC_BYTES,
    },
    SourceBudget,
};
use std::{
    cmp::Ordering,
    collections::{hash_map::RandomState, HashMap, HashSet, VecDeque},
    hash::BuildHasher,
    rc::Rc,
};

pub(crate) const CONTRACT: &str = "agsdl-0.3.0";

/// The JS `ModuleError`, with its message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModuleError {
    pub(crate) message: String,
}

/// Any other JS exception, with its message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Internal {
    pub(crate) message: String,
    pub(crate) limit_pointer: Option<String>,
}

impl Internal {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            limit_pointer: None,
        }
    }
}

impl From<SchemaError> for Internal {
    fn from(error: SchemaError) -> Self {
        Self {
            message: error.message,
            limit_pointer: error.limit_pointer,
        }
    }
}

impl From<Failure> for Internal {
    fn from(error: Failure) -> Self {
        Self {
            message: error.message,
            limit_pointer: Some(error.pointer),
        }
    }
}

impl From<Unshaped> for Internal {
    fn from(error: Unshaped) -> Self {
        Self {
            message: error.message,
            limit_pointer: error.limit_pointer,
        }
    }
}

impl From<OutsideShape> for Internal {
    fn from(error: OutsideShape) -> Self {
        match error.limit {
            Some(limit) => limit.into(),
            None => Self::new(format!(
                "value schema outside the published shape at {}",
                error.pointer
            )),
        }
    }
}

/// Failure of a reader entry point: a `ModuleError` or another exception.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReadError {
    Module(ModuleError),
    Internal(Internal),
}

impl From<Internal> for ReadError {
    fn from(error: Internal) -> Self {
        ReadError::Internal(error)
    }
}

impl From<SchemaError> for ReadError {
    fn from(error: SchemaError) -> Self {
        ReadError::Internal(error.into())
    }
}

impl From<ModuleError> for ReadError {
    fn from(error: ModuleError) -> Self {
        ReadError::Module(error)
    }
}

fn internal<T>(message: impl Into<String>) -> Result<T, Internal> {
    Err(Internal::new(message))
}

/// Harness modules by name with the semantics of a JS `Map`: `set` on a known
/// name replaces the module in place, so an overwritten module is never seen.
#[derive(Default)]
pub(crate) struct ModuleMap {
    entries: OrderedMap<String, Value>,
}

impl ModuleMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set(&mut self, name: String, module: Value) {
        self.entries.set(name, module);
    }

    pub(crate) fn get(&self, name: &str) -> Option<&Value> {
        self.entries.get(name)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.entries.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Diagnostic {
    pub(crate) severity: &'static str,
    pub(crate) code: &'static str,
    pub(crate) path: String,
    pub(crate) message: String,
    pub(crate) step: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReportJoin {
    pub(crate) origin: String,
    pub(crate) branches: Vec<String>,
}

const MAX_MESSAGE_BYTES: usize = 2048;
// Keep room for the report envelope and the final limit diagnostic.
const REPORT_RESERVE: usize = 4608;
const DIAGNOSTIC_OVERHEAD: usize = 64;
/// The JS `Report`.
#[derive(Default)]
pub(crate) struct Report {
    current_edition: bool,
    budget: SourceBudget,
    pub(crate) diagnostics: Vec<Diagnostic>,
    pub(crate) joins: OrderedMap<String, ReportJoin>,
    seen: HashMap<u64, Vec<usize>>,
    hash: RandomState,
    diagnostic_bytes: usize,
    limited: bool,
}

impl Report {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn with_budget(budget: &SourceBudget) -> Self {
        Self {
            budget: budget.clone(),
            ..Self::default()
        }
    }

    /// `report.add(severity, code, where, message, step)`.
    pub(crate) fn add(
        &mut self,
        severity: &'static str,
        code: &'static str,
        path: String,
        mut message: String,
        step: Option<Vec<String>>,
    ) {
        if self.limited {
            return;
        }
        if code == GRAPH_LIMIT_CODE {
            self.limit(path, message);
            return;
        }
        if message.len() > MAX_MESSAGE_BYTES {
            let end = message.floor_char_boundary(MAX_MESSAGE_BYTES - '…'.len_utf8());
            message.truncate(end);
            message.push('…');
        }
        let text_bytes =
            path.len() + message.len() + step.iter().flatten().map(String::len).sum::<usize>();
        if let Err(error) = self.budget.work(text_bytes.div_ceil(8) + 1, &path) {
            self.limit(path, error.message);
            return;
        }
        let key = self.hash.hash_one((severity, code, &path, &message));
        if self.seen.get(&key).is_some_and(|indices| {
            indices.iter().any(|&index| {
                let item = &self.diagnostics[index];
                item.severity == severity
                    && item.code == code
                    && item.path == path
                    && item.message == message
                    && (step.is_none()
                        || item.step.as_deref().unwrap_or(&[]) == step.as_deref().unwrap_or(&[]))
            })
        }) {
            return;
        }
        let encoded = DIAGNOSTIC_OVERHEAD
            + code.len()
            + severity.len()
            + quoted_bytes(&path)
            + quoted_bytes(&message)
            + step
                .iter()
                .flatten()
                .map(|item| quoted_bytes(item) + 1)
                .sum::<usize>();
        if self.diagnostics.len() >= MAX_DIAGNOSTICS
            || encoded > MAX_DIAGNOSTIC_BYTES - REPORT_RESERVE - self.diagnostic_bytes
        {
            self.limit(path, DIAGNOSTIC_LIMIT_MESSAGE.into());
            return;
        }
        if let Err(error) = self.budget.bytes(text_bytes + encoded * 2 + 160, &path) {
            self.limit(path, error.message);
            return;
        }
        self.diagnostic_bytes += encoded;
        self.seen
            .entry(key)
            .or_default()
            .push(self.diagnostics.len());
        self.diagnostics.push(Diagnostic {
            severity,
            code,
            path,
            message,
            step,
        });
    }

    fn limit(&mut self, mut path: String, message: String) {
        if self.limited {
            return;
        }
        self.limited = true;
        // Keep an ancestor pointer if a path alone would exceed the reserve.
        if quoted_bytes(&path) > 4096 {
            let end = path.floor_char_boundary(680);
            path.truncate(path[..end].rfind('/').unwrap_or(0));
        }
        self.diagnostics.push(Diagnostic {
            severity: "error",
            code: GRAPH_LIMIT_CODE,
            path,
            message,
            step: None,
        });
    }

    pub(crate) fn error(&mut self, code: &'static str, parts: &[PointerPart], message: String) {
        self.add("error", code, path_pointer(parts), message, None);
    }

    fn error_step(
        &mut self,
        code: &'static str,
        parts: &[PointerPart],
        message: String,
        step: Option<Vec<String>>,
    ) {
        self.add("error", code, path_pointer(parts), message, step);
    }

    fn warning_step(
        &mut self,
        code: &'static str,
        parts: &[PointerPart],
        message: String,
        step: Option<Vec<String>>,
    ) {
        self.add("warning", code, path_pointer(parts), message, step);
    }

    pub(crate) fn error_at(&mut self, code: &'static str, path: &str, message: String) {
        self.add("error", code, path.to_owned(), message, None);
    }

    fn value_problem(
        &mut self,
        code: &'static str,
        at: &str,
        make: &dyn Fn() -> String,
    ) -> Result<(), OutsideShape> {
        if !self.limited {
            let minimum = DIAGNOSTIC_OVERHEAD + quoted_bytes(at);
            if self.diagnostics.len() >= MAX_DIAGNOSTICS
                || minimum > MAX_DIAGNOSTIC_BYTES - REPORT_RESERVE - self.diagnostic_bytes
            {
                self.limit(at.into(), DIAGNOSTIC_LIMIT_MESSAGE.into());
            } else if let Err(error) = self.budget.bytes(at.len() + 2048, at) {
                self.limit(at.into(), error.message);
            } else {
                self.error_at(code, at, make());
            }
        }
        if self.limited {
            let diagnostic = self.diagnostics.last().expect("limit diagnostic");
            return Err(Failure {
                code: GRAPH_LIMIT_CODE.into(),
                pointer: diagnostic.path.clone(),
                message: diagnostic.message.clone(),
            }
            .into());
        }
        Ok(())
    }

    /// `report.valid`.
    pub(crate) fn valid(&self) -> bool {
        !self.diagnostics.iter().any(|item| item.severity == "error")
    }

    /// `JSON.stringify(report.as_json())`. `joins` is an ordinary JS object:
    /// array-index names first, ascending, then insertion order.
    #[cfg(test)]
    pub(crate) fn to_json(&self) -> String {
        self.to_json_with_budget(&SourceBudget::default())
            .expect("test report within budget")
    }

    pub(crate) fn to_json_with_budget(&self, budget: &SourceBudget) -> Result<String, Failure> {
        budget.work(self.joins.len() * 16, "/flow")?;
        budget.bytes(self.joins.len() * 40, "/flow")?;
        let (mut indexed, named): (Vec<_>, Vec<_>) = self
            .joins
            .keys()
            .partition(|name| array_index(name).is_some());
        indexed.sort_by_key(|name| array_index(name));
        Ok(encode_string(budget, usize::MAX, "", |out| {
            write!(
                out,
                "{{\"contract\":\"{}\",\"valid\":{},\"diagnostics\":[",
                if self.current_edition {
                    crate::argentic_core::AGSDL_040
                } else {
                    CONTRACT
                },
                self.valid()
            )?;
            for (index, item) in self.diagnostics.iter().enumerate() {
                if index > 0 {
                    out.write_char(',')?;
                }
                write!(
                    out,
                    "{{\"severity\":\"{}\",\"code\":\"{}\",\"path\":",
                    item.severity, item.code
                )?;
                quote_to(out, &item.path)?;
                out.write_str(",\"message\":")?;
                quote_to(out, &item.message)?;
                if let Some(step) = &item.step {
                    out.write_str(",\"step\":")?;
                    write_string_array(out, step)?;
                }
                out.write_char('}')?;
            }
            out.write_str("],\"joins\":{")?;
            for (index, name) in indexed.iter().chain(&named).enumerate() {
                if index > 0 {
                    out.write_char(',')?;
                }
                let join = self.joins.get(*name).expect("join name");
                quote_to(out, name)?;
                out.write_str(":{\"origin\":")?;
                quote_to(out, &join.origin)?;
                out.write_str(",\"branches\":")?;
                write_string_array(out, &join.branches)?;
                out.write_char('}')?;
            }
            out.write_str("}}")
        })?
        .expect("unlimited report writer"))
    }
}

fn write_string_array(out: &mut dyn std::fmt::Write, values: &[String]) -> std::fmt::Result {
    out.write_char('[')?;
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.write_char(',')?;
        }
        quote_to(out, value)?;
    }
    out.write_char(']')
}

/// `check_module(module)`: the module itself, or the first problem.
#[cfg(test)]
pub(crate) fn check_module(module: Value) -> Result<Value, ReadError> {
    check_module_with_budget(module, &SourceBudget::default())
}

pub(crate) fn check_module_with_budget(
    module: Value,
    budget: &SourceBudget,
) -> Result<Value, ReadError> {
    let problems = schema::edition_schema(&module, true)?.check_with_budget(&module, budget)?;
    if let Some((at, message)) = problems.first() {
        let at = if at.is_empty() { "/" } else { at.as_str() };
        return Err(ModuleError {
            message: format!("{at}: {message}"),
        }
        .into());
    }
    if let Some(settings) = module.as_object().and_then(|object| object.get("settings")) {
        let shape = schema::value_schema_shape()?.check_with_budget(settings, budget)?;
        let consistency = if shape.is_empty() {
            valueschema::consistency_with_budget(settings, &[key("settings")], budget)
                .map_err(Internal::from)?
        } else {
            Vec::new()
        };
        let first = if shape.is_empty() {
            consistency.first()
        } else {
            shape.first()
        };
        if let Some((at, message)) = first {
            return Err(ModuleError {
                message: format!("settings {at}: {message}"),
            }
            .into());
        }
    }
    Ok(module)
}

pub(crate) fn validate_document_with_budget(
    document: &Value,
    modules: &ModuleMap,
    budget: &SourceBudget,
) -> Result<Report, Internal> {
    let mut report = Report::with_budget(budget);
    if let Err(error) = validate_into(document, modules, &mut report) {
        if let Some(pointer) = error.limit_pointer {
            report.error_at(GRAPH_LIMIT_CODE, &pointer, error.message);
        } else {
            return Err(error);
        }
    }
    Ok(report)
}

fn validate_into(
    document: &Value,
    modules: &ModuleMap,
    report: &mut Report,
) -> Result<(), Internal> {
    report.current_edition = matches!(member(Some(document), "contract"),
        Some(Value::String(contract)) if contract == crate::argentic_core::AGSDL_040);
    let problems =
        schema::edition_schema(document, false)?.check_with_budget(document, &report.budget)?;
    if !problems.is_empty() {
        for (at, message) in problems {
            report.error_at("SHAPE", &at, message);
            if report.limited {
                break;
            }
        }
        return Ok(());
    }
    let Value::Object(document) = document else {
        return internal("a shape-valid document is an object");
    };
    Checker::new(document, modules, report)?.run()
}

/// `validate_bytes(bytes, modules)`.
pub(crate) fn validate_bytes(bytes: &[u8], modules: &ModuleMap) -> Result<Report, Internal> {
    let budget = SourceBudget::default();
    if let Err(error) = budget.json_input(bytes, "") {
        let mut report = Report::with_budget(&budget);
        report.error_at(GRAPH_LIMIT_CODE, &error.pointer, error.message);
        return Ok(report);
    }
    match super::exact::scan::parse_v030(bytes) {
        Err(error) => {
            let mut report = Report::new();
            report.error_at("PARSE", "", error.message());
            Ok(report)
        }
        Ok(parsed) => validate_document_with_budget(&parsed.tree, modules, &budget),
    }
}

fn approval_comments() -> &'static Value {
    static SCHEMA: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| {
        super::exact::scan::parse_v030(
            br#"{
            "type":"array","items":{"type":"object","additionalProperties":false,
            "properties":{"step":{"type":"string"},"comment":{"type":"string"}}}
        }"#,
        )
        .expect("approval comments schema")
        .tree
    })
}

fn article(word: &str) -> String {
    let an = word.chars().next().is_some_and(|c| "aeiou".contains(c));
    format!("{}{word}", if an { "an " } else { "a " })
}

/// `repr` of a string: Python-like single quotes.
fn repr_str(text: &str) -> String {
    let escaped = excerpt(text)
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("'{escaped}'")
}

fn repr_value(value: &Value) -> String {
    match value {
        Value::String(text) => repr_str(text),
        Value::Number(token) => number_excerpt(token.raw()),
        other => value_excerpt(other),
    }
}

fn repr_part(part: &PointerPart) -> String {
    match part {
        PointerPart::Key(name) => repr_str(name),
        PointerPart::Index(index) => index.to_string(),
        PointerPart::Number(token) => number_excerpt(token.raw()),
    }
}

fn is_json_media(media: &str) -> bool {
    media == "application/json" || media.ends_with("+json")
}

fn instance_label(instance: Option<&str>) -> String {
    match instance {
        Some(name) if !name.is_empty() => format!("instance '{name}'"),
        _ => "shared instance".to_owned(),
    }
}

/// `expanded(addr)`: the address parts, or `null` for local and exit nodes.
fn expanded(addr: &str) -> Option<Vec<String>> {
    let parts = address_parts(addr);
    if parts.len() < 2 || parts[0].starts_with('#') {
        None
    } else {
        Some(parts.into_iter().map(str::to_owned).collect())
    }
}

/// `scope_member(addr, prefix)`.
fn scope_member<'a>(addr: &'a str, prefix: &str) -> Option<&'a str> {
    let suffix = if prefix.is_empty() {
        addr
    } else {
        addr.strip_prefix(prefix)?.strip_prefix('/')?
    };
    (!suffix.is_empty()).then(|| suffix.split('/').next().expect("first address segment"))
}

fn first_part(addr: &str) -> Result<String, Internal> {
    if addr.is_empty() {
        return internal("empty graph address");
    }
    Ok(addr
        .split('/')
        .next()
        .expect("first address segment")
        .to_owned())
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Object, Internal> {
    match value {
        Value::Object(object) => Ok(object),
        _ => internal(format!("{what} is not an object")),
    }
}

fn text<'a>(value: &'a Value, what: &str) -> Result<&'a str, Internal> {
    match value {
        Value::String(text) => Ok(text),
        _ => internal(format!("{what} is not a string")),
    }
}

fn required<'a>(object: &'a Object, key: &str) -> Result<&'a Value, Internal> {
    match object.get(key) {
        Some(value) => Ok(value),
        None => internal(format!("missing {key}")),
    }
}

/// `get(value, key, {})` for an object-valued member.
fn object_or_empty<'a>(value: Option<&'a Value>, what: &str) -> Result<&'a Object, Internal> {
    match value {
        None => Ok(empty_object()),
        Some(value) => object(value, what),
    }
}

fn list_or_empty<'a>(value: Option<&'a Value>, what: &str) -> Result<&'a [Value], Internal> {
    match value {
        None => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        Some(_) => internal(format!("{what} is not an array")),
    }
}

/// `get(value, key)` on an object value; `null` members read as absent where
/// the JS compares with `null`.
fn member<'a>(value: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    match value {
        Some(Value::Object(object)) => object.get(key),
        _ => None,
    }
}

fn is_text(value: Option<&Value>, expected: &str) -> bool {
    matches!(value, Some(Value::String(text)) if text == expected)
}

fn non_null(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !matches!(value, Value::Null))
}

/// `equal(value, [name])` for a one-string path.
fn is_single_path(value: &Value, name: &str) -> bool {
    matches!(value, Value::Array(items) if items.len() == 1 && matches!(&items[0], Value::String(text) if text == name))
}

fn path_parts(value: &Value, graph: &Graph<'_>) -> Result<Vec<PointerPart>, Internal> {
    let Value::Array(items) = value else {
        return internal("a path is not an array");
    };
    graph.work(items.len())?;
    graph.bytes(
        items
            .iter()
            .map(|part| {
                32 + match part {
                    Value::String(text) => text.len(),
                    Value::Number(token) => token.raw().len(),
                    _ => 0,
                }
            })
            .sum(),
    )?;
    items
        .iter()
        .map(|item| match item {
            Value::String(name) => Ok(PointerPart::Key(name.clone())),
            Value::Number(token) => Ok(PointerPart::Number(token.clone())),
            _ => internal("a path part is neither a string nor a number"),
        })
        .collect()
}

fn part_is(part: Option<&PointerPart>, name: &str) -> bool {
    matches!(part, Some(PointerPart::Key(text)) if text == name)
}

fn part_text(part: &PointerPart) -> Option<&str> {
    match part {
        PointerPart::Key(text) => Some(text),
        _ => None,
    }
}

/// `valueschema.is_index(part)`: an integer-valued number.
fn is_index(part: Option<&PointerPart>) -> bool {
    match part {
        Some(PointerPart::Index(_)) => true,
        Some(PointerPart::Number(token)) => is_integer_number(Num::Token(token)),
        _ => false,
    }
}

fn parts_are(parts: &[PointerPart], names: &[&str]) -> bool {
    parts.len() == names.len()
        && parts
            .iter()
            .zip(names)
            .all(|(part, name)| part_is(Some(part), name))
}

fn with(parts: &[PointerPart], extra: impl IntoIterator<Item = PointerPart>) -> Vec<PointerPart> {
    let mut out = parts.to_vec();
    out.extend(extra);
    out
}

/// Join information as the validator keeps it per scope: branch and fan-out
/// names instead of addresses, origin and members as local names.
#[derive(Clone)]
struct LocalJoin {
    origin: String,
    branches: OrderedMap<String, OrderedSet<String>>,
    fanout: OrderedSet<String>,
    prefix: String,
}

struct Usage {
    new: bool,
    steer: bool,
    choices: OrderedSet<String>,
    used: bool,
}

/// One pending loop of `selection_problem` over candidate producers.
struct Scan {
    candidates: Vec<usize>,
    next: usize,
    rest: Vec<PointerPart>,
    prefix: String,
}

enum SelectionStep {
    Done(Option<String>),
    Tail(usize, Vec<PointerPart>, bool),
    Scan(Scan),
}

struct Checker<'a, 'r> {
    doc: &'a Object,
    modules: &'r ModuleMap,
    report: &'r mut Report,
    agents: &'a Object,
    compositions: &'a Object,
    content: &'a Object,
    uses: HashMap<String, Usage>,
    cycle: bool,
}

impl<'a, 'r> Checker<'a, 'r> {
    fn new(
        doc: &'a Object,
        modules: &'r ModuleMap,
        report: &'r mut Report,
    ) -> Result<Self, Internal> {
        let agents = object(required(doc, "agents")?, "agents")?;
        let uses = agents
            .iter()
            .map(|(name, _)| {
                (
                    name.to_owned(),
                    Usage {
                        new: false,
                        steer: false,
                        choices: OrderedSet::new(),
                        used: false,
                    },
                )
            })
            .collect();
        Ok(Self {
            doc,
            modules,
            report,
            agents,
            compositions: object_or_empty(doc.get("compositions"), "compositions")?,
            content: object_or_empty(doc.get("content"), "content")?,
            uses,
            cycle: false,
        })
    }

    fn run(&mut self) -> Result<(), Internal> {
        let mut scopes: Vec<(&'a Object, Vec<PointerPart>, Option<&'a str>)> = Vec::new();
        if let Some(flow) = self.doc.get("flow") {
            scopes.push((object(flow, "flow")?, vec![key("flow")], None));
        }
        let compositions: &'a Object = self.compositions;
        for (name, composition) in compositions.iter() {
            scopes.push((
                object(composition, "composition")?,
                vec![key("compositions"), key(name)],
                Some(name),
            ));
        }
        self.check_compositions()?;
        for (container, at, composition) in scopes {
            if self.report.limited {
                return Ok(());
            }
            self.check_scope(container, &at, composition)?;
        }
        self.check_agents()?;
        if !self.cycle && !self.report.limited {
            self.check_graphs()?;
        }
        if self.report.limited {
            return Ok(());
        }
        self.check_completion_choices()?;
        self.check_harnesses()
    }

    fn check_content_uses(
        &mut self,
        uses: &'a Value,
        parts: &[PointerPart],
    ) -> Result<(), Internal> {
        let size = match uses {
            Value::Array(items) => items.len(),
            Value::Object(items) => items.len(),
            _ => 0,
        };
        self.report.budget.work(size, &path_pointer(parts))?;
        self.report.budget.bytes(size * 64, &path_pointer(parts))?;
        let items: Vec<(PointerPart, &'a Value)> = match uses {
            Value::String(_) => return Ok(()),
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(index, item)| (PointerPart::Index(index), item))
                .collect(),
            Value::Object(object) => object
                .iter()
                .map(|(name, item)| (key(name), item))
                .collect(),
            Value::Null => return internal("content uses are null"),
            _ => Vec::new(),
        };
        for (part, item) in items {
            if let Some(reference) = member(Some(item), "ref") {
                let reference = text(reference, "content ref")?;
                if !self.content.contains_key(reference) {
                    self.report.error(
                        "REFERENCE",
                        &with(parts, [part, key("ref")]),
                        format!("no content named '{reference}'"),
                    );
                }
            }
        }
        Ok(())
    }

    fn check_message(&mut self, message: &'a Value, parts: &[PointerPart]) -> Result<(), Internal> {
        if let Some(prompt) = member(Some(message), "prompt") {
            self.check_content_uses(prompt, &with(parts, [key("prompt")]))?;
        }
        if let Some(resources) = member(Some(message), "resources") {
            self.check_content_uses(resources, &with(parts, [key("resources")]))?;
        }
        Ok(())
    }

    fn check_compositions(&mut self) -> Result<(), Internal> {
        let compositions: &'a Object = self.compositions;
        let mut uses: OrderedMap<String, Vec<(String, Vec<PointerPart>)>> = OrderedMap::new();
        for (name, composition) in compositions.iter() {
            let composition = object(composition, "composition")?;
            let steps = object(required(composition, "steps")?, "steps")?;
            self.report.budget.work(steps.len() + 1, "/compositions")?;
            self.report
                .budget
                .bytes(steps.len() * 192, "/compositions")?;
            let mut dependencies = Vec::new();
            for (step_name, step) in steps.iter() {
                let step = object(step, "step")?;
                if is_text(step.get("type"), "compose") {
                    dependencies.push((
                        text(required(step, "composition")?, "composition")?.to_owned(),
                        vec![
                            key("compositions"),
                            key(name),
                            key("steps"),
                            key(step_name),
                            key("composition"),
                        ],
                    ));
                }
            }
            uses.set(name.to_owned(), dependencies);
            let mut exported = HashSet::new();
            for (_, step) in steps.iter() {
                for route in step_routes(object(step, "step")?, &[]) {
                    for item in route_targets(route.route, &[])? {
                        if item.kind == TargetKind::Output {
                            exported.insert(item.name);
                        }
                    }
                }
            }
            for (index, output) in list_or_empty(composition.get("outputs"), "outputs")?
                .iter()
                .enumerate()
            {
                let output = text(output, "output")?;
                if !exported.contains(output) {
                    self.report.error(
                        "COMPOSITION",
                        &[
                            key("compositions"),
                            key(name),
                            key("outputs"),
                            PointerPart::Index(index),
                        ],
                        format!(
                            "no route of composition '{name}' leaves through output '{output}'"
                        ),
                    );
                }
            }
        }
        #[derive(PartialEq)]
        enum State {
            Active,
            Done,
        }
        let mut state: HashMap<String, State> = HashMap::new();
        for name in uses.keys() {
            if state.contains_key(name) {
                continue;
            }
            state.insert(name.clone(), State::Active);
            let mut stack: Vec<(String, usize)> = vec![(name.clone(), 0)];
            while let Some(frame) = stack.last_mut() {
                self.report.budget.work(1, "/compositions")?;
                let dependencies = uses.get(&frame.0).expect("composition dependencies");
                if frame.1 == dependencies.len() {
                    state.insert(frame.0.clone(), State::Done);
                    stack.pop();
                    continue;
                }
                let (target, at) = &dependencies[frame.1];
                frame.1 += 1;
                if !uses.contains_key(target) {
                    continue;
                }
                match state.get(target) {
                    Some(State::Active) => {
                        self.cycle = true;
                        self.report.error(
                            "COMPOSITION",
                            at,
                            format!("composition '{target}' uses itself, directly or through other compositions"),
                        );
                    }
                    Some(State::Done) => {}
                    None => {
                        state.insert(target.clone(), State::Active);
                        stack.push((target.clone(), 0));
                    }
                }
            }
        }
        Ok(())
    }

    fn composition(&self, name: &str) -> Result<&'a Object, Internal> {
        let compositions: &'a Object = self.compositions;
        object(required(compositions, name)?, "composition")
    }

    fn check_scope(
        &mut self,
        container: &'a Object,
        at: &[PointerPart],
        composition: Option<&'a str>,
    ) -> Result<(), Internal> {
        let steps = object(required(container, "steps")?, "steps")?;
        self.report.budget.work(steps.len(), &path_pointer(at))?;
        let mut parameters: HashSet<&'a str> = HashSet::new();
        let mut outputs: HashSet<&'a str> = HashSet::new();
        if let Some(name) = composition {
            let declared = self.composition(name)?;
            for parameter in list_or_empty(declared.get("parameters"), "parameters")? {
                parameters.insert(text(parameter, "parameter")?);
            }
            for output in list_or_empty(declared.get("outputs"), "outputs")? {
                outputs.insert(text(output, "output")?);
            }
        }
        let entry = text(required(container, "entry")?, "entry")?;
        match steps.get(entry) {
            None => self.report.error(
                "REFERENCE",
                &with(at, [key("entry")]),
                format!("no step named '{entry}'"),
            ),
            Some(step) => {
                let kind = text(required(object(step, "step")?, "type")?, "type")?;
                if kind == "join" {
                    self.report.error(
                        "ROUTES",
                        &with(at, [key("entry")]),
                        format!("the entry cannot be {}", article(kind)),
                    );
                }
            }
        }
        for (name, step) in steps.iter() {
            if self.report.limited {
                break;
            }
            self.check_step(
                object(step, "step")?,
                &with(at, [key("steps"), key(name)]),
                steps,
                &parameters,
                &outputs,
                composition,
            )?;
        }
        let mut roots: Vec<&'a str> = Vec::new();
        if composition.is_none() {
            if let Some(on_error) = container.get("onError") {
                self.check_route(
                    on_error,
                    &[key("flow"), key("onError")],
                    steps,
                    &HashSet::new(),
                    None,
                )?;
                roots = route_targets(on_error, &[])?
                    .into_iter()
                    .filter(|item| item.kind == TargetKind::Step)
                    .map(|item| item.name)
                    .collect();
            }
        }
        if steps.contains_key(entry) {
            self.check_reachability(entry, steps, &with(at, [key("steps")]), &roots)?;
        }
        Ok(())
    }

    fn check_agent_ref(
        &mut self,
        reference: &'a Value,
        parts: &[PointerPart],
        parameters: &HashSet<&'a str>,
        composition: Option<&str>,
    ) -> Result<(), Internal> {
        if let Value::String(name) = reference {
            if !self.agents.contains_key(name) {
                self.report
                    .error("REFERENCE", parts, format!("no agent named '{name}'"));
            }
            return Ok(());
        }
        let name = text(
            required(object(reference, "agent reference")?, "parameter")?,
            "parameter",
        )?;
        match composition {
            None => self.report.error(
                "REFERENCE",
                parts,
                "agent parameters exist only inside compositions".to_owned(),
            ),
            Some(composition) if !parameters.contains(name) => self.report.error(
                "REFERENCE",
                parts,
                format!("composition '{composition}' has no parameter '{name}'"),
            ),
            Some(_) => {}
        }
        Ok(())
    }

    fn check_step(
        &mut self,
        step: &'a Object,
        parts: &[PointerPart],
        steps: &'a Object,
        parameters: &HashSet<&'a str>,
        outputs: &HashSet<&'a str>,
        composition: Option<&'a str>,
    ) -> Result<(), Internal> {
        let kind = text(required(step, "type")?, "type")?;
        if kind == "agent" {
            let agent = required(step, "agent")?;
            self.check_agent_ref(agent, &with(parts, [key("agent")]), parameters, composition)?;
            if let Some(prompt) = step.get("prompt") {
                self.check_content_uses(prompt, &with(parts, [key("prompt")]))?;
            }
            if is_text(step.get("instance"), "new") && step.contains_key("delivery") {
                self.report.error(
                    "DELIVERY",
                    &with(parts, [key("delivery")]),
                    "a fresh instance is never working: delivery cannot be used with instance \"new\"".to_owned(),
                );
            }
            if let Value::String(name) = agent {
                if let Some(usage) = self.uses.get_mut(name.as_str()) {
                    usage.used = true;
                    usage.new |= is_text(step.get("instance"), "new");
                    usage.steer |= is_text(step.get("delivery"), "steer");
                }
            }
        } else if kind == "call" && step.contains_key("mcp") {
            let server = text(
                required(object(required(step, "mcp")?, "mcp")?, "server")?,
                "server",
            )?;
            let servers = object_or_empty(self.doc.get("mcpServers"), "mcpServers")?;
            if !servers.contains_key(server) {
                self.report.error(
                    "REFERENCE",
                    &with(parts, [key("mcp"), key("server")]),
                    format!("no MCP server named '{server}'"),
                );
            }
        } else if kind == "compose" {
            self.check_compose(step, parts, parameters, composition)?;
        }
        self.check_routes(step, kind, parts, steps, outputs, composition)
    }

    fn check_compose(
        &mut self,
        step: &'a Object,
        parts: &[PointerPart],
        parameters: &HashSet<&'a str>,
        composition: Option<&'a str>,
    ) -> Result<(), Internal> {
        let target = text(required(step, "composition")?, "composition")?;
        if !self.compositions.contains_key(target) {
            self.report.error(
                "REFERENCE",
                &with(parts, [key("composition")]),
                format!("no composition named '{target}'"),
            );
            return Ok(());
        }
        let declared = self.composition(target)?;
        let parameters_at_target = list_or_empty(declared.get("parameters"), "parameters")?;
        let given = object_or_empty(step.get("agents"), "agents")?;
        let pointer = path_pointer(parts);
        self.report
            .budget
            .work(parameters_at_target.len() + given.len(), &pointer)?;
        self.report
            .budget
            .bytes((parameters_at_target.len() + given.len()) * 96, &pointer)?;
        let mut expected: OrderedSet<String> = OrderedSet::new();
        for parameter in parameters_at_target {
            let parameter = text(parameter, "parameter")?;
            self.report
                .budget
                .work(parameter.len().div_ceil(8), &pointer)?;
            self.report.budget.bytes(parameter.len() * 4, &pointer)?;
            expected.insert(parameter.to_owned());
        }
        let mut missing: Vec<String> = expected
            .iter()
            .filter(|name| !given.contains_key(name))
            .cloned()
            .collect();
        sort_utf16(&mut missing);
        for name in missing {
            self.report.error(
                "COMPOSITION",
                &with(parts, [key("agents")]),
                format!("missing agent for parameter '{name}'"),
            );
        }
        let mut extra: Vec<String> = given
            .iter()
            .map(|(name, _)| name.to_owned())
            .filter(|name| !expected.contains(name))
            .collect();
        sort_utf16(&mut extra);
        for name in extra {
            self.report.error(
                "COMPOSITION",
                &with(parts, [key("agents"), key(&name)]),
                format!("composition '{target}' has no parameter '{name}'"),
            );
        }
        for (parameter, reference) in given.iter() {
            self.check_agent_ref(
                reference,
                &with(parts, [key("agents"), key(parameter)]),
                parameters,
                composition,
            )?;
        }
        let mut outputs: OrderedSet<String> = OrderedSet::new();
        for output in list_or_empty(declared.get("outputs"), "outputs")? {
            outputs.insert(text(output, "output")?.to_owned());
        }
        let next = object(required(step, "next")?, "compose next")?;
        if !same_keys(next, &outputs) {
            let mut sorted: Vec<String> = outputs.iter().cloned().collect();
            sort_utf16(&mut sorted);
            let shown: Vec<String> = sorted.iter().map(|name| repr_str(name)).collect();
            self.report.error(
                "COMPOSITION",
                &with(parts, [key("next")]),
                format!(
                    "routes must cover exactly the outputs [{}]",
                    shown.join(", ")
                ),
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn check_routes(
        &mut self,
        step: &'a Object,
        kind: &str,
        parts: &[PointerPart],
        steps: &'a Object,
        outputs: &HashSet<&'a str>,
        composition: Option<&'a str>,
    ) -> Result<(), Internal> {
        let next = non_null(step.get("next"));
        let map = next.and_then(|next| match next {
            Value::Object(map) if is_route_map(next) => Some(map),
            _ => None,
        });
        let names = |expected: &[&str]| -> OrderedSet<String> {
            expected.iter().map(|name| (*name).to_owned()).collect()
        };
        let next_at = with(parts, [key("next")]);
        if kind == "call" && map.is_some() {
            if !same_keys(map.expect("map"), &names(&["passed", "failed"])) {
                self.report.error(
                    "ROUTES",
                    &next_at,
                    "a call routes with a route, or a map with exactly passed and failed"
                        .to_owned(),
                );
            }
        } else if kind == "approval" {
            if next.is_none() || matches!(next, Some(Value::Object(map)) if map.len() == 0) {
                self.report.error(
                    "ROUTES",
                    &next_at,
                    "an approval offers at least one choice".to_owned(),
                );
            }
        } else if kind == "join" && (next.is_some() || step.contains_key("quorum")) {
            if step.contains_key("quorum") {
                if map.map_or(true, |map| !same_keys(map, &names(&["reached", "missed"]))) {
                    self.report.error(
                        "ROUTES",
                        &next_at,
                        "a quorum join routes with exactly reached and missed".to_owned(),
                    );
                }
            } else if is_text(step.get("failures"), "keep") {
                if map.is_some_and(|map| !same_keys(map, &names(&["done", "incomplete"]))) {
                    self.report.error(
                        "ROUTES",
                        &next_at,
                        "a join that keeps failures routes with a route, or a map with exactly done and incomplete".to_owned(),
                    );
                }
            } else if map.is_some_and(|map| !same_keys(map, &names(&["done"]))) {
                self.report.error(
                    "ROUTES",
                    &next_at,
                    "a join without quorum routes with a route, or a map with only done".to_owned(),
                );
            }
        }
        if kind == "join" {
            if step.contains_key("quorum")
                && (step.contains_key("failures") || step.contains_key("maxFailures"))
            {
                let field = if step.contains_key("failures") {
                    "failures"
                } else {
                    "maxFailures"
                };
                self.report.error(
                    "JOIN",
                    &with(parts, [key(field)]),
                    "failures and maxFailures apply to all mode only; a quorum already keeps failed branches".to_owned(),
                );
            } else if step.contains_key("maxFailures") && !is_text(step.get("failures"), "keep") {
                self.report.error(
                    "JOIN",
                    &with(parts, [key("maxFailures")]),
                    "maxFailures requires failures: \"keep\"".to_owned(),
                );
            }
        }
        if kind == "agent" {
            if let (Some(map), Some(Value::String(agent))) = (map, step.get("agent")) {
                if let Some(usage) = self.uses.get_mut(agent.as_str()) {
                    for (choice, _) in map.iter() {
                        usage.choices.insert(choice.to_owned());
                    }
                }
            }
        }
        for route in step_routes(step, parts) {
            self.check_route(route.route, &route.pointer, steps, outputs, composition)?;
        }
        Ok(())
    }

    fn check_route(
        &mut self,
        route: &'a Value,
        at: &[PointerPart],
        steps: &'a Object,
        outputs: &HashSet<&'a str>,
        composition: Option<&'a str>,
    ) -> Result<(), Internal> {
        let pointer = path_pointer(at);
        let count = route_len(route);
        self.report.budget.work(count * 2, &pointer)?;
        self.report
            .budget
            .bytes(count.saturating_mul(path_allocation(at) + 112), &pointer)?;
        let (items, listed): (Vec<&Value>, bool) = match route {
            Value::Array(items) => (items.iter().collect(), true),
            single => (vec![single], false),
        };
        for (index, item) in items.into_iter().enumerate() {
            let Value::Object(item) = item else { continue };
            if item.contains_key("each") {
                continue;
            }
            let here = if listed {
                with(at, [PointerPart::Index(index)])
            } else {
                at.to_vec()
            };
            for name in ["concurrency", "max"] {
                if item.contains_key(name) {
                    self.report.error(
                        "ROUTES",
                        &with(&here, [key(name)]),
                        format!("{name} limits a fan-out; it requires each"),
                    );
                }
            }
        }
        let mut seen: HashSet<(TargetKind, &'a str)> = HashSet::new();
        for item in route_items(route, at)? {
            let target = item.name;
            if !seen.insert((item.kind, target)) {
                self.report.error(
                    "DUPLICATE",
                    &item.pointer,
                    format!("'{target}' appears twice in this route"),
                );
            }
            if item.kind == TargetKind::Step {
                match steps.get(target) {
                    None => self.report.error(
                        "REFERENCE",
                        &item.pointer,
                        format!("no step named '{target}'"),
                    ),
                    Some(found) => {
                        let kind = text(required(object(found, "step")?, "type")?, "type")?;
                        let silent = kind == "call" || kind == "join";
                        if item.message.is_some() && silent {
                            self.report.error(
                                "ROUTES",
                                &with(&item.pointer, [key("message")]),
                                format!("{} cannot receive a message", article(kind)),
                            );
                        } else if item.fanout.is_some() && silent {
                            self.report.error(
                                "ROUTES",
                                &with(&item.pointer, [key("each")]),
                                format!(
                                    "{} receives no message, so it cannot tell the elements of a fan-out apart",
                                    article(kind)
                                ),
                            );
                        }
                    }
                }
            } else {
                match composition {
                    None => self.report.error(
                        "ROUTES",
                        &item.pointer,
                        "outputs exist only inside compositions".to_owned(),
                    ),
                    Some(composition) if !outputs.contains(target) => self.report.error(
                        "REFERENCE",
                        &item.pointer,
                        format!("composition '{composition}' has no output '{target}'"),
                    ),
                    Some(_) => {}
                }
                if item.message.is_some() {
                    self.report.error(
                        "ROUTES",
                        &with(&item.pointer, [key("message")]),
                        "put the message on the compose step's route, not on the output".to_owned(),
                    );
                }
            }
            if let Some(message) = item.message {
                self.check_message(message, &with(&item.pointer, [key("message")]))?;
            }
        }
        Ok(())
    }

    fn check_reachability(
        &mut self,
        entry: &'a str,
        steps: &'a Object,
        parts: &[PointerPart],
        roots: &[&'a str],
    ) -> Result<(), Internal> {
        let pointer = path_pointer(parts);
        self.report
            .budget
            .work(steps.len() + roots.len(), &pointer)?;
        self.report.budget.bytes(steps.len() * 72, &pointer)?;
        let mut seen = OrderedSet::new();
        seen.insert(entry);
        for &root in roots {
            if steps.contains_key(root) {
                seen.insert(root);
            }
        }
        let mut queue: Vec<_> = seen.iter().copied().collect();
        let mut index = 0;
        while index < queue.len() {
            let step = object(required(steps, queue[index])?, "step")?;
            for route in step_routes(step, &[]) {
                let count = route_len(route.route);
                self.report.budget.work(count, &pointer)?;
                self.report.budget.bytes(count * 88, &pointer)?;
                for item in route_targets(route.route, &[])? {
                    if item.kind == TargetKind::Step
                        && steps.contains_key(item.name)
                        && seen.insert(item.name)
                    {
                        queue.push(item.name);
                    }
                }
            }
            index += 1;
        }
        let origin = if roots.is_empty() {
            "the entry"
        } else {
            "the entry or the flow's onError"
        };
        for (name, _) in steps.iter() {
            if !seen.contains(name) {
                self.report.error(
                    "UNREACHABLE",
                    &with(parts, [key(name)]),
                    format!("step '{name}' cannot be reached from {origin}"),
                );
            }
        }
        Ok(())
    }

    fn check_agents(&mut self) -> Result<(), Internal> {
        let skills = object_or_empty(self.doc.get("skills"), "skills")?;
        let servers = object_or_empty(self.doc.get("mcpServers"), "mcpServers")?;
        let agents: &'a Object = self.agents;
        for (name, agent) in agents.iter() {
            if self.report.limited {
                break;
            }
            let parts = vec![key("agents"), key(name)];
            let agent = object(agent, "agent")?;
            if let Some(instructions) = agent.get("instructions") {
                self.check_content_uses(instructions, &with(&parts, [key("instructions")]))?;
            }
            if let Some(resources) = agent.get("resources") {
                self.check_content_uses(resources, &with(&parts, [key("resources")]))?;
            }
            for (index, skill) in list_or_empty(agent.get("skills"), "skills")?
                .iter()
                .enumerate()
            {
                let skill = text(skill, "skill")?;
                if !skills.contains_key(skill) {
                    self.report.error(
                        "REFERENCE",
                        &with(&parts, [key("skills"), PointerPart::Index(index)]),
                        format!("no skill named '{skill}'"),
                    );
                }
            }
            for (index, server) in list_or_empty(agent.get("mcpServers"), "mcpServers")?
                .iter()
                .enumerate()
            {
                let server = text(server, "MCP server")?;
                if !servers.contains_key(server) {
                    self.report.error(
                        "REFERENCE",
                        &with(&parts, [key("mcpServers"), PointerPart::Index(index)]),
                        format!("no MCP server named '{server}'"),
                    );
                }
            }
            for (result_name, result) in object_or_empty(agent.get("results"), "results")?.iter() {
                let at = with(&parts, [key("results"), key(result_name)]);
                if result_name == "text" {
                    self.report.error(
                        "RESULT_NAME",
                        &at,
                        "a result cannot be named 'text'".to_owned(),
                    );
                }
                let result = object(result, "result")?;
                let media = match result.get("mediaType") {
                    None => "application/json",
                    Some(media) => text(media, "mediaType")?,
                };
                if let Some(schema) = result.get("schema") {
                    if !is_json_media(media) {
                        self.report.error(
                            "SCHEMA",
                            &with(&at, [key("schema")]),
                            format!("a schema needs a JSON media type, not {media}"),
                        );
                    }
                    let budget = self.report.budget.clone();
                    valueschema::check_consistency(
                        schema,
                        &with(&at, [key("schema")]),
                        &budget,
                        &mut |at, make| self.report.value_problem("SCHEMA", at, make),
                    )?;
                }
            }
            let permissions = agent.get("permissions");
            if let Some(Value::Object(object_permissions)) = permissions {
                if is_text(object_permissions.get("preset"), "read-only") {
                    for (path, access) in
                        object_or_empty(object_permissions.get("directories"), "directories")?
                            .iter()
                    {
                        if matches!(access, Value::String(access) if access == "write") {
                            self.report.error(
                                "PERMISSIONS",
                                &with(&parts, [key("permissions"), key("directories"), key(path)]),
                                "read-only never writes; this directory asks for write".to_owned(),
                            );
                        }
                    }
                }
            }
            if let Some(check) = non_null(agent.get("completionCheck")) {
                let command = text(
                    required(object(check, "completionCheck")?, "command")?,
                    "command",
                )?;
                let at = with(&parts, [key("completionCheck"), key("command")]);
                if permissions::effect_for(
                    permissions,
                    "shell",
                    command,
                    &self.report.budget,
                    &path_pointer(&at),
                )? == "deny"
                {
                    self.report.error(
                        "PERMISSIONS",
                        &at,
                        "the agent's own permissions deny this command".to_owned(),
                    );
                }
            }
        }
        Ok(())
    }

    fn check_completion_choices(&mut self) -> Result<(), Internal> {
        let agents: &'a Object = self.agents;
        for (name, agent) in agents.iter() {
            let Some(check) = non_null(object(agent, "agent")?.get("completionCheck")) else {
                continue;
            };
            let usage = self.uses.get(name).expect("agent usage");
            if !usage.used {
                continue;
            }
            let mut missing = Vec::new();
            for (index, choice) in
                list_or_empty(object(check, "completionCheck")?.get("choices"), "choices")?
                    .iter()
                    .enumerate()
            {
                let choice = text(choice, "choice")?;
                if !usage.choices.contains(choice) {
                    missing.push((index, choice));
                }
            }
            for (index, choice) in missing {
                self.report.error(
                    "REFERENCE",
                    &[
                        key("agents"),
                        key(name),
                        key("completionCheck"),
                        key("choices"),
                        PointerPart::Index(index),
                    ],
                    format!("no step using agent '{name}' routes by choice '{choice}'"),
                );
            }
        }
        Ok(())
    }

    fn check_graphs(&mut self) -> Result<(), Internal> {
        let doc: &'a Object = self.doc;
        let compositions: &'a Object = self.compositions;
        let mut scopes: Vec<(Option<String>, Graph<'a>)> = Vec::new();
        let mut local_budget = ExpansionBudget::new(&self.report.budget);
        let mut covered: OrderedSet<String> = OrderedSet::new();
        if let Some(flow) = doc.get("flow") {
            let flow = object(flow, "flow")?;
            let steps = object(required(flow, "steps")?, "steps")?;
            if steps.contains_key(text(required(flow, "entry")?, "entry")?) {
                let mut initial_budget = ExpansionBudget::new(&self.report.budget);
                let initial = Graph::for_scope(doc, None, None, &mut initial_budget)?;
                let (infos, _) = analyse_joins(&initial, &mut initial_budget)?;
                let mut members = OrderedSet::new();
                for info in infos.values() {
                    for found in info.branches.values() {
                        for member in found {
                            members.insert(first_part(member)?);
                        }
                    }
                }
                covered = Graph::flow_error_covered(doc, &members, &initial_budget)?;
                scopes.push((
                    None,
                    Graph::for_scope(doc, None, Some(covered.clone()), &mut local_budget)?,
                ));
            }
        }
        for (name, composition) in compositions.iter() {
            let composition = object(composition, "composition")?;
            let steps = object(required(composition, "steps")?, "steps")?;
            if steps.contains_key(text(required(composition, "entry")?, "entry")?) {
                scopes.push((
                    Some(name.to_owned()),
                    Graph::for_scope(doc, Some(name), None, &mut local_budget)?,
                ));
            }
        }
        let mut joins: OrderedMap<Option<String>, OrderedMap<String, LocalJoin>> =
            OrderedMap::new();
        for (scope_key, graph) in &scopes {
            let (infos, problems) = analyse_joins(graph, &mut local_budget)?;
            for problem in problems {
                self.report.error("JOIN", &problem.pointer, problem.message);
            }
            if scope_key.is_some() {
                for problem in open_body_forks(graph, &infos)? {
                    self.report
                        .error("COMPOSITION", &problem.pointer, problem.message);
                }
            }
            let mut local: OrderedMap<String, LocalJoin> = OrderedMap::new();
            for (addr, info) in infos.iter() {
                let name_of = |start: &String| -> Result<String, Internal> {
                    match info.names.get(start).cloned().flatten() {
                        Some(name) => Ok(name),
                        None => internal("a join branch has no name"),
                    }
                };
                let mut branches: OrderedMap<String, OrderedSet<String>> = OrderedMap::new();
                for (start, members) in info.branches.iter() {
                    let mut locals = OrderedSet::new();
                    for member in members {
                        locals.insert(first_part(member)?);
                    }
                    branches.set(name_of(start)?, locals);
                }
                let mut fanout = OrderedSet::new();
                for start in &info.fanout {
                    fanout.insert(name_of(start)?);
                }
                let name = first_part(addr)?;
                let origin = first_part(&info.origin)?;
                if scope_key.is_none() {
                    self.report.joins.set(
                        name.clone(),
                        ReportJoin {
                            origin: origin.clone(),
                            branches: branches.keys().cloned().collect(),
                        },
                    );
                }
                local.set(
                    name,
                    LocalJoin {
                        origin,
                        branches,
                        fanout,
                        prefix: String::new(),
                    },
                );
            }
            joins.set(scope_key.clone(), local);
        }
        let mut in_branch: InBranch = OrderedMap::new();
        for (scope_key, found) in joins.iter() {
            let mut members = OrderedSet::new();
            for info in found.values() {
                for branch in info.branches.values() {
                    for member in branch {
                        members.insert(member.clone());
                    }
                }
            }
            in_branch.set(scope_key.clone(), members);
        }
        let in_branch = Rc::new(in_branch);
        let has_flow = scopes.iter().any(|(scope_key, _)| scope_key.is_none());
        let mut graphs: Vec<Graph<'a>> = Vec::new();
        let mut budget = ExpansionBudget::new(&self.report.budget);
        if has_flow {
            graphs.push(Graph::for_flow(
                doc,
                Some(in_branch.clone()),
                Some(covered.clone()),
                &mut budget,
            )?);
        }
        for (name, _) in compositions.iter() {
            budget.work(scopes.len(), Some(name))?;
            if scopes
                .iter()
                .any(|(scope_key, _)| scope_key.as_deref() == Some(name))
            {
                graphs.push(Graph::for_composition(
                    doc,
                    name,
                    Some(in_branch.clone()),
                    &mut budget,
                )?);
            }
        }
        for (index, graph) in graphs.iter().enumerate() {
            if self.report.limited {
                break;
            }
            graph.work(graph.nodes.len() * 4 + graph.edges.len() * 3)?;
            if index == 0 && has_flow {
                for use_addr in sorted_addresses(graph.composes.keys()) {
                    let compose = graph.composes.get(&use_addr).expect("compose");
                    if compose.child.is_none() {
                        continue;
                    }
                    let target = text(required(compose.decl, "composition")?, "composition")?;
                    if let Some(found) = joins.get(&Some(target.to_owned())) {
                        for (name, info) in found.iter() {
                            budget.join_analysis(info.branches.len(), Some(target))?;
                            self.report.joins.set(
                                format!("{use_addr}/{name}"),
                                ReportJoin {
                                    origin: format!("{use_addr}/{}", info.origin),
                                    branches: info.branches.keys().cloned().collect(),
                                },
                            );
                        }
                    }
                }
            }
            let mut infos: OrderedMap<String, LocalJoin> = OrderedMap::new();
            for (addr, node) in graph.nodes.iter() {
                if node.kind != "join" {
                    continue;
                }
                let scope = &graph.scopes[node.scope];
                if let Some(info) = joins
                    .get(&scope.composition)
                    .and_then(|local| local.get(&node.name))
                {
                    budget.join_analysis(
                        info.branches
                            .values()
                            .map(|members| members.len() + 1)
                            .sum(),
                        scope.composition.as_deref(),
                    )?;
                    let mut info = info.clone();
                    info.prefix = scope.prefix.clone();
                    infos.set(addr.clone(), info);
                }
            }
            if index == 0 && has_flow {
                self.record_uses(graph);
            }
            self.record_choices(graph);
            self.check_selections(graph, &infos)?;
            if self.report.limited {
                break;
            }
            self.check_missions(graph)?;
            self.check_shared_instances(graph, &infos, &mut budget)?;
        }
        Ok(())
    }

    fn record_uses(&mut self, graph: &Graph<'a>) {
        for node in graph.nodes.values() {
            if node.kind != "agent" {
                continue;
            }
            let Some(agent) = &node.agent else { continue };
            if let (Some(usage), Some(decl)) = (self.uses.get_mut(agent), node.decl) {
                usage.new |= is_text(decl.get("instance"), "new");
                usage.steer |= is_text(decl.get("delivery"), "steer");
            }
        }
    }

    fn record_choices(&mut self, graph: &Graph<'a>) {
        for node in graph.nodes.values() {
            if node.kind != "agent" {
                continue;
            }
            let Some(agent) = &node.agent else { continue };
            let Some(usage) = self.uses.get_mut(agent) else {
                continue;
            };
            usage.used = true;
            if let Some(Value::Object(map)) = node.decl.and_then(|decl| decl.get("next")) {
                if !map.contains_key("step") && !map.contains_key("output") {
                    for (choice, _) in map.iter() {
                        usage.choices.insert(choice.to_owned());
                    }
                }
            }
        }
    }

    fn agent_of(&self, node: &Node<'a>) -> Option<&'a Value> {
        let agents: &'a Object = self.agents;
        node.agent.as_deref().and_then(|name| agents.get(name))
    }

    fn instructs(&self, node: &Node<'a>) -> bool {
        node.kind == "agent" && member(self.agent_of(node), "instructs").is_some_and(truthy)
    }

    fn prompt_leaf<'g>(
        &self,
        graph: &'g Graph<'a>,
        edge: &'g Edge<'a>,
    ) -> Result<Result<Option<bool>, &'g str>, Internal> {
        if let Some(message) = edge.message {
            return Ok(Ok(Some(member(Some(message), "prompt").is_some())));
        }
        if edge.kind == RouteKind::Error {
            return Ok(Ok(Some(false)));
        }
        let node = graph.node_at(&edge.src)?;
        if node.kind == "agent" && node.agent.is_none() {
            return Ok(Ok(None));
        }
        if node.kind == "approval" {
            return Ok(Err(&node.addr));
        }
        Ok(Ok(Some(self.instructs(node))))
    }

    fn approval_prompts<'g>(
        &self,
        graph: &'g Graph<'a>,
        leaves: &[Result<Option<bool>, &'g str>],
    ) -> Result<HashMap<&'g str, Option<bool>>, Internal> {
        // Unknown leaves propagate through every dependent approval. A prompt
        // exists on all paths only when all dependencies have proved true.
        // Defaults for unresolved dependencies also cover approval cycles.
        graph.work(graph.nodes.len() + graph.edges.len() * 3)?;
        graph.bytes(graph.nodes.len() * 144 + graph.edges.len() * 48)?;
        let mut results = HashMap::new();
        let mut pending = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut queue = VecDeque::new();
        for (addr, node) in graph.nodes.iter() {
            if node.kind != "approval" {
                continue;
            }
            let addr = addr.as_str();
            let incoming = graph.incoming(addr)?;
            let mut waits = 0;
            let mut possible = !incoming.is_empty();
            let mut unknown = false;
            for &edge in incoming {
                match leaves[edge] {
                    Ok(None) => unknown = true,
                    Ok(Some(has_prompt)) => possible &= has_prompt,
                    Err(source) => {
                        dependents.entry(source).or_default().push(addr);
                        waits += 1;
                    }
                }
            }
            if unknown {
                results.insert(addr, None);
                queue.push_back(addr);
            } else if possible && waits == 0 {
                results.insert(addr, Some(true));
                queue.push_back(addr);
            } else {
                results.insert(addr, Some(false));
                if possible {
                    pending.insert(addr, waits);
                }
            }
        }
        while let Some(addr) = queue.pop_front() {
            for &dependent in dependents.get(addr).into_iter().flatten() {
                if results[dependent].is_none() {
                    continue;
                }
                if results[addr].is_none() {
                    results.insert(dependent, None);
                    queue.push_back(dependent);
                } else if let Some(waits) = pending.get_mut(dependent) {
                    *waits -= 1;
                    if *waits == 0 {
                        results.insert(dependent, Some(true));
                        queue.push_back(dependent);
                    }
                }
            }
        }
        Ok(results)
    }

    fn check_missions(&mut self, graph: &Graph<'a>) -> Result<(), Internal> {
        graph.work(graph.nodes.len() + graph.edges.len() * 2)?;
        graph.bytes(graph.nodes.len() * 96 + graph.edges.len() * 48)?;
        let mut incoming: OrderedMap<String, Vec<usize>> = OrderedMap::new();
        let mut leaves = Vec::with_capacity(graph.edges.len());
        for (index, edge) in graph.edges.iter().enumerate() {
            leaves.push(self.prompt_leaf(graph, edge)?);
            match incoming.get_mut(&edge.dst) {
                Some(list) => list.push(index),
                None => incoming.set(edge.dst.clone(), vec![index]),
            }
        }
        let approvals = self.approval_prompts(graph, &leaves)?;
        for (addr, edges) in incoming.iter() {
            let node = graph.node_at(addr)?;
            if node.kind != "agent" || node.decl.is_some_and(|decl| decl.contains_key("prompt")) {
                continue;
            }
            let mut brings = Vec::with_capacity(edges.len());
            for &edge in edges {
                brings.push(leaves[edge].unwrap_or_else(|addr| approvals[addr]));
            }
            if brings.contains(&None) {
                continue;
            }
            let missing: Vec<usize> = edges
                .iter()
                .zip(&brings)
                .filter(|(_, found)| **found != Some(true))
                .map(|(edge, _)| *edge)
                .collect();
            if !missing.is_empty() && missing.len() < edges.len() {
                let mut sources = HashSet::new();
                for &edge in &missing {
                    sources.insert(graph.node_at(&graph.edges[edge].src)?.name.clone());
                }
                let mut sources: Vec<_> = sources.into_iter().collect();
                sort_utf16(&mut sources);
                self.report.error_step(
                    "PROMPT",
                    &node.pointer,
                    format!(
                        "step '{}' receives a prompt on some routes but not from {}; give it a default prompt",
                        node.name,
                        sources.join(", ")
                    ),
                    expanded(addr),
                );
            }
        }
        Ok(())
    }

    fn check_selections(
        &mut self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
    ) -> Result<(), Internal> {
        self.check_instruction_fanouts(graph)?;
        let mut reported: HashSet<(String, String)> = HashSet::new();
        for (index, edge) in graph.edges.iter().enumerate() {
            let target = graph.node_at(&edge.dst)?;
            if self.report.limited {
                return Ok(());
            }
            if target.kind == "call" {
                let arguments = object_or_empty(
                    target.decl.and_then(|decl| decl.get("arguments")),
                    "arguments",
                )?;
                graph.work(arguments.len())?;
                for (name, operand) in arguments.iter() {
                    let Some(path) = member(Some(operand), "path") else {
                        continue;
                    };
                    let reason = self.selection_problem(
                        graph,
                        infos,
                        index,
                        path_parts(path, graph)?,
                        false,
                    )?;
                    let at = with(&target.pointer, [key("arguments"), key(name), key("path")]);
                    if let Some(reason) = reason {
                        let identity = (parts_identity(&at), reason.clone());
                        if reported.insert(identity) {
                            self.report.error_step(
                                "MESSAGE_PATH",
                                &at,
                                reason,
                                expanded(&edge.src),
                            );
                        }
                    }
                }
            }
            if let Some(each) = edge.each {
                let mut reason =
                    self.selection_problem(graph, infos, index, path_parts(each, graph)?, false)?;
                if reason.is_none() {
                    reason = self.fanout_problem(graph, index, infos)?;
                }
                if let Some(reason) = reason {
                    self.report.error_step(
                        "MESSAGE_PATH",
                        &with(&edge.pointer, [key("each")]),
                        reason,
                        expanded(&edge.src),
                    );
                }
            }
            let Some(message) = edge.message else {
                continue;
            };
            if let Some(Value::Object(prompt)) = member(Some(message), "prompt") {
                if let Some(path) = prompt.get("from") {
                    let at = with(&edge.pointer, [key("message"), key("prompt"), key("from")]);
                    let source = graph.node_at(&edge.src)?;
                    let parts = path_parts(path, graph)?;
                    let allowed = (source.kind == "agent" && parts_are(&parts, &["instructions"]))
                        || (source.kind == "join"
                            && parts.len() == 3
                            && part_is(parts.first(), "branches")
                            && part_is(parts.get(2), "instructions"))
                        || (parts_are(&parts, &["item"])
                            && edge
                                .each
                                .is_some_and(|each| is_single_path(each, "instructions")));
                    if edge.kind == RouteKind::Error || !allowed {
                        self.report.error_step(
                            "MESSAGE_PATH",
                            &at,
                            "a prompt selects only the instructions of an agent or of a join branch, or an element of instructions in a fan-out".to_owned(),
                            expanded(&edge.src),
                        );
                    } else if let Some(reason) =
                        self.selection_problem(graph, infos, index, parts, true)?
                    {
                        self.report
                            .error_step("MESSAGE_PATH", &at, reason, expanded(&edge.src));
                    }
                }
            }
            let resources = object_or_empty(member(Some(message), "resources"), "resources")?;
            graph.work(resources.len())?;
            for (name, used) in resources.iter() {
                let Some(path) = member(Some(used), "from") else {
                    continue;
                };
                if let Some(reason) =
                    self.selection_problem(graph, infos, index, path_parts(path, graph)?, true)?
                {
                    self.report.error_step(
                        "MESSAGE_PATH",
                        &with(
                            &edge.pointer,
                            [key("message"), key("resources"), key(name), key("from")],
                        ),
                        reason,
                        expanded(&edge.src),
                    );
                }
            }
        }
        Ok(())
    }

    /// `selection_problem(graph, edge, path, infos, inMessage)`. The JS
    /// recursion becomes a loop over pending producer scans; the first
    /// failing producer wraps its reason, as each JS frame does on return.
    fn selection_problem(
        &self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
        edge: usize,
        path: Vec<PointerPart>,
        in_message: bool,
    ) -> Result<Option<String>, Internal> {
        let mut visited = HashSet::new();
        let mut stack: Vec<Scan> = Vec::new();
        let mut call = Some((edge, path, in_message));
        let mut returned: Option<Option<String>> = None;
        loop {
            if let Some((edge, path, in_message)) = call.take() {
                graph.work(path.len() + 1)?;
                graph.bytes(96 + path_allocation(&path))?;
                let first_visit = visited.insert((edge, path.clone(), in_message));
                let step = if first_visit {
                    self.selection_step(graph, infos, edge, &path, in_message)?
                } else {
                    SelectionStep::Done(None)
                };
                match step {
                    SelectionStep::Done(result) => returned = Some(result),
                    SelectionStep::Tail(edge, path, in_message) => {
                        call = Some((edge, path, in_message));
                        continue;
                    }
                    SelectionStep::Scan(scan) => {
                        stack.push(scan);
                        returned = None;
                    }
                }
            }
            let Some(top) = stack.last_mut() else {
                return Ok(returned.expect("selection result"));
            };
            if let Some(Some(reason)) = returned.take() {
                let wrapped = format!("{}{}", top.prefix, excerpt(&reason));
                stack.pop();
                returned = Some(Some(wrapped));
                continue;
            }
            if top.next < top.candidates.len() {
                let candidate = top.candidates[top.next];
                top.next += 1;
                graph.bytes(path_allocation(&top.rest))?;
                call = Some((candidate, top.rest.clone(), true));
            } else {
                stack.pop();
                returned = Some(None);
            }
        }
    }

    fn selection_step(
        &self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
        index: usize,
        path: &[PointerPart],
        in_message: bool,
    ) -> Result<SelectionStep, Internal> {
        use SelectionStep::{Done, Scan as Pending, Tail};
        let say = |text: String| -> Result<SelectionStep, Internal> { Ok(Done(Some(text))) };
        let fine = || -> Result<SelectionStep, Internal> { Ok(Done(None)) };
        let Some(head) = path.first() else {
            return fine();
        };
        let edge = &graph.edges[index];
        if part_is(Some(head), "item") {
            let Some(each) = edge.each.filter(|_| in_message) else {
                return say("'item' exists only in the message of a fan-out".to_owned());
            };
            if is_single_path(each, "instructions") {
                return if path.len() == 1 {
                    fine()
                } else {
                    say("'item' is an instruction, a string".to_owned())
                };
            }
            let mut next = path_parts(each, graph)?;
            next.push(PointerPart::Index(0));
            next.extend(path[1..].iter().cloned());
            return Ok(Tail(index, next, false));
        }
        if edge.kind == RouteKind::Error {
            return Ok(Done(error_path_problem(path)));
        }
        let node = graph.node_at(&edge.src)?;
        match node.kind {
            "agent" => {
                let Some(agent) = self.agent_of(node) else {
                    return fine();
                };
                let agent_name = node.agent.as_deref().unwrap_or_default();
                if part_is(Some(head), "instructions") {
                    if !member(Some(agent), "instructs").is_some_and(truthy) {
                        return say(format!("agent '{agent_name}' does not instruct"));
                    }
                    if edge
                        .each
                        .is_some_and(|each| is_single_path(each, "instructions"))
                    {
                        if path.len() > 1 && !is_index(path.get(1)) {
                            return say(
                                "'instructions' is a list here: select one by its index".to_owned()
                            );
                        }
                        return if path.len() <= 2 {
                            fine()
                        } else {
                            say("an instruction is a string".to_owned())
                        };
                    }
                    return if path.len() == 1 {
                        fine()
                    } else {
                        say("'instructions' is a string".to_owned())
                    };
                }
                if let Some(name @ ("text" | "choice")) = part_text(head) {
                    if path.len() > 1 {
                        return say(format!("'{name}' is a string"));
                    }
                    let next = node.decl.and_then(|decl| decl.get("next"));
                    if name == "choice" && !next.is_some_and(is_route_map) {
                        return say(format!("step '{}' does not route by choice", node.name));
                    }
                    return fine();
                }
                if !part_is(Some(head), "results") {
                    return say(format!(
                        "an agent output has text, results, choice and instructions, not {}",
                        repr_part(head)
                    ));
                }
                if path.len() == 1 {
                    return fine();
                }
                let results = object_or_empty(member(Some(agent), "results"), "results")?;
                let result = part_text(&path[1]).and_then(|name| results.get(name));
                let Some(result) = result else {
                    return say(format!(
                        "agent '{agent_name}' declares no result {}",
                        repr_part(&path[1])
                    ));
                };
                let rest = &path[2..];
                if rest.is_empty() {
                    return fine();
                }
                let media = match member(Some(result), "mediaType") {
                    None => "application/json",
                    Some(media) => text(media, "mediaType")?,
                };
                if !is_json_media(media) {
                    return say(format!(
                        "result {} is not JSON and has no fields",
                        repr_part(&path[1])
                    ));
                }
                if let Some(schema) = member(Some(result), "schema") {
                    if let Descent::Refused(reason) =
                        valueschema::descend_checked(schema, rest, &self.report.budget, "/flow")?
                    {
                        return say(reason);
                    }
                }
                fine()
            }
            "call" => {
                if part_is(Some(head), "outcome") {
                    return if path.len() == 1 {
                        fine()
                    } else {
                        say("'outcome' has no fields".to_owned())
                    };
                }
                if !part_is(Some(head), "data") {
                    return say("a call output has only 'outcome' and 'data'".to_owned());
                }
                let decl = node.decl.expect("call step");
                if decl.contains_key("command") && path.len() > 1 {
                    let parse = non_null(decl.get("parse"));
                    if part_is(path.get(1), "value") {
                        let Some(parse) = parse else {
                            return say(format!(
                                "step '{}' declares no parse, so its result has no value",
                                node.name
                            ));
                        };
                        if matches!(parse, Value::String(mode) if mode == "lines") && path.len() > 2
                        {
                            if !is_index(path.get(2)) {
                                return say(
                                    "'value' is a list of lines: select a line by its index"
                                        .to_owned(),
                                );
                            }
                            if path.len() > 3 {
                                return say("a line is a string".to_owned());
                            }
                        }
                        return fine();
                    }
                    let Some(field @ ("exitCode" | "output")) = part_text(&path[1]) else {
                        return say(format!(
                            "a command result has exitCode, output and, with parse, value, not {}",
                            repr_part(&path[1])
                        ));
                    };
                    if path.len() > 2 {
                        return say(format!("'{field}' has no fields"));
                    }
                }
                fine()
            }
            "approval" => {
                let Some(head @ ("choice" | "comment" | "message")) = part_text(head) else {
                    return say(format!(
                        "an approval output has choice, comment and message, not {}",
                        repr_part(head)
                    ));
                };
                if head != "message" {
                    return if path.len() > 1 {
                        say(format!("'{head}' is a string"))
                    } else {
                        fine()
                    };
                }
                if path.len() == 1 {
                    return fine();
                }
                if !matches!(
                    part_text(&path[1]),
                    Some("prompt" | "resources" | "item" | "comments")
                ) {
                    return say(format!(
                        "a message has prompt, resources, item and comments, not {}",
                        repr_part(&path[1])
                    ));
                }
                if self.report.current_edition && part_is(path.get(1), "comments") {
                    return Ok(Done(
                        match valueschema::descend_checked(
                            approval_comments(),
                            &path[2..],
                            &self.report.budget,
                            "/flow",
                        )? {
                            Descent::Refused(reason) => Some(reason),
                            _ => None,
                        },
                    ));
                }
                if part_is(path.get(1), "resources") && path.len() > 2 {
                    if let Some(names) = self.received_resources(graph, &node.addr, infos)? {
                        if !part_text(&path[2]).is_some_and(|name| names.contains(name)) {
                            return say(format!(
                                "approval '{}' receives no resource {}",
                                node.name,
                                repr_part(&path[2])
                            ));
                        }
                    }
                }
                fine()
            }
            "join" => {
                if !matches!(part_text(head), Some("outcome" | "branches")) {
                    return say(format!(
                        "a join output has outcome and branches, not {}",
                        repr_part(head)
                    ));
                }
                if part_is(Some(head), "outcome") && path.len() > 1 {
                    return say("'outcome' is a string".to_owned());
                }
                let info = infos.get(&node.addr);
                let (true, Some(info)) = (part_is(Some(head), "branches") && path.len() > 1, info)
                else {
                    return fine();
                };
                let branch = part_text(&path[1]).filter(|name| info.branches.contains_key(*name));
                let Some(branch) = branch else {
                    return say(format!(
                        "join '{}' has no branch {}",
                        node.name,
                        repr_part(&path[1])
                    ));
                };
                let prefix = format!("branch {}: ", repr_str(branch));
                let mut rest = &path[2..];
                if info.fanout.contains(branch) && !rest.is_empty() {
                    if !is_index(rest.first()) {
                        return say(format!(
                            "branch {} is a fan-out: select an entry by its index",
                            repr_str(branch)
                        ));
                    }
                    rest = &rest[1..];
                    if part_is(rest.first(), "item") {
                        graph.work(graph.edges.len())?;
                        graph.bytes(graph.edges.len() * 8 + path_allocation(rest))?;
                        let candidates = graph
                            .edges
                            .iter()
                            .enumerate()
                            .filter(|(_, edge)| {
                                edge.each.is_some()
                                    && scope_member(&edge.src, &info.prefix)
                                        == Some(info.origin.as_str())
                                    && scope_member(&edge.dst, &info.prefix) == Some(branch)
                            })
                            .map(|(index, _)| index)
                            .collect();
                        return Ok(Pending(Scan {
                            candidates,
                            next: 0,
                            rest: rest.to_vec(),
                            prefix,
                        }));
                    }
                }
                if let Some(field @ ("ended" | "by")) = rest.first().and_then(part_text) {
                    return if rest.len() > 1 {
                        say(format!("'{field}' is a string"))
                    } else {
                        fine()
                    };
                }
                if part_is(rest.first(), "error") {
                    let decl = node.decl.expect("join step");
                    if !decl.contains_key("quorum") && !is_text(decl.get("failures"), "keep") {
                        return say(
                            "only a quorum or failures: \"keep\" keeps a failed branch".to_owned()
                        );
                    }
                    return Ok(Done(error_path_problem(rest)));
                }
                if !rest.is_empty() {
                    let incoming = graph.incoming(&node.addr)?;
                    graph.bytes(incoming.len() * 8 + path_allocation(rest))?;
                    let candidates = Self::branch_producers(graph, incoming, info, branch)?
                        .map(|(index, _)| index)
                        .collect();
                    return Ok(Pending(Scan {
                        candidates,
                        next: 0,
                        rest: rest.to_vec(),
                        prefix,
                    }));
                }
                fine()
            }
            _ => fine(),
        }
    }

    /// `received_resources(graph, addr, infos, new Set())`, on a heap stack.
    fn received_resources(
        &self,
        graph: &Graph<'a>,
        addr: &str,
        infos: &OrderedMap<String, LocalJoin>,
    ) -> Result<Option<OrderedSet<String>>, Internal> {
        if graph.root.as_deref() == Some(addr) {
            return Ok(None);
        }
        graph.bytes(192)?;
        let mut active = HashSet::from([addr]);
        let mut visited = HashSet::new();
        let mut stack = vec![(addr, graph.incoming(addr)?.iter())];
        let mut names = OrderedSet::new();
        while let Some((addr, incoming)) = stack.last_mut() {
            graph.work(1)?;
            let Some(&index) = incoming.next() else {
                active.remove(*addr);
                visited.insert(*addr);
                stack.pop();
                continue;
            };
            let edge = &graph.edges[index];
            if let Some(message) = edge.message {
                let resources = object_or_empty(member(Some(message), "resources"), "resources")?;
                graph.work(resources.len())?;
                for (name, _) in resources.iter() {
                    graph.bytes(96 + name.len() * 2)?;
                    names.insert(name.to_owned());
                }
                continue;
            }
            if edge.kind == RouteKind::Error {
                names.insert("error".to_owned());
                continue;
            }
            let node = graph.node_at(&edge.src)?;
            match node.kind {
                "agent" => {
                    let Some(agent) = self.agent_of(node) else {
                        return Ok(None);
                    };
                    names.insert("text".to_owned());
                    let results = object_or_empty(member(Some(agent), "results"), "results")?;
                    graph.work(results.len())?;
                    for (name, _) in results.iter() {
                        graph.bytes(96 + name.len() * 2)?;
                        names.insert(name.to_owned());
                    }
                }
                "call" => {
                    names.insert("result".to_owned());
                }
                "join" => {
                    let Some(info) = infos.get(&node.addr) else {
                        return Ok(None);
                    };
                    graph.work(info.branches.len())?;
                    for name in info.branches.keys() {
                        graph.bytes(96 + name.len() * 2)?;
                        names.insert(name.clone());
                    }
                }
                "approval" => {
                    let source = node.addr.as_str();
                    if graph.root.as_deref() == Some(source) || active.contains(source) {
                        return Ok(None);
                    }
                    if !visited.contains(source) {
                        graph.bytes(128)?;
                        active.insert(source);
                        stack.push((source, graph.incoming(source)?.iter()));
                    }
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(names))
    }

    fn branch_producers<'b>(
        graph: &'b Graph<'a>,
        incoming: &'b [usize],
        info: &'b LocalJoin,
        branch: &'b str,
    ) -> Result<impl Iterator<Item = (usize, &'b Edge<'a>)> + 'b, Internal> {
        graph.work(incoming.len())?;
        Ok(incoming
            .iter()
            .map(|&i| (i, &graph.edges[i]))
            .filter(move |(_, edge)| {
                edge.kind == RouteKind::Normal
                    && scope_member(&edge.src, &info.prefix).is_some_and(|local| {
                        info.branches
                            .get(branch)
                            .expect("branch members")
                            .contains(local)
                            || (local == info.origin && local == branch)
                    })
            }))
    }

    fn declared_kind(
        &self,
        graph: &Graph<'a>,
        index: usize,
        path: &[PointerPart],
        infos: &OrderedMap<String, LocalJoin>,
        memo: &mut HashMap<(usize, usize), Option<String>>,
    ) -> Result<Option<String>, Internal> {
        graph.work(path.len() + 1)?;
        // Recursive calls only visit suffixes of the same path. The suffix
        // length identifies the selection independently of the branch taken.
        let key = (index, path.len());
        if let Some(kind) = memo.get(&key) {
            return Ok(kind.clone());
        }
        graph.bytes(96)?;
        let kind = self.compute_declared_kind(graph, &graph.edges[index], path, infos, memo)?;
        memo.insert(key, kind.clone());
        Ok(kind)
    }

    fn compute_declared_kind(
        &self,
        graph: &Graph<'a>,
        edge: &Edge<'a>,
        path: &[PointerPart],
        infos: &OrderedMap<String, LocalJoin>,
        memo: &mut HashMap<(usize, usize), Option<String>>,
    ) -> Result<Option<String>, Internal> {
        let kind = |name: &str| Ok(Some(name.to_owned()));
        let Some(head) = path.first() else {
            return kind("object");
        };
        if edge.kind == RouteKind::Error {
            if parts_are(path, &["error", "details"]) {
                return kind("array");
            }
            return match path.len() {
                1 => kind("object"),
                2 => kind("string"),
                _ => Ok(None),
            };
        }
        let node = graph.node_at(&edge.src)?;
        match node.kind {
            "agent" => {
                if matches!(part_text(head), Some("text" | "choice" | "instructions")) {
                    return kind("string");
                }
                if path.len() == 1 {
                    return kind("object");
                }
                let results = object_or_empty(member(self.agent_of(node), "results"), "results")?;
                let Some(result) = part_text(&path[1]).and_then(|name| results.get(name)) else {
                    return Ok(None);
                };
                let media = match member(Some(result), "mediaType") {
                    None => "application/json",
                    Some(media) => text(media, "mediaType")?,
                };
                if !is_json_media(media) {
                    return kind("string");
                }
                match member(Some(result), "schema") {
                    Some(schema) => Ok(valueschema::kind_at_checked(
                        schema,
                        &path[2..],
                        &self.report.budget,
                        "/flow",
                    )?),
                    None => Ok(None),
                }
            }
            "call" => {
                if part_is(Some(head), "outcome") {
                    return kind("string");
                }
                let decl = node.decl.expect("call step");
                if !decl.contains_key("command") {
                    return Ok(None);
                }
                if path.len() == 1 {
                    return kind("object");
                }
                if part_is(path.get(1), "value") && is_text(decl.get("parse"), "lines") {
                    return kind(if path.len() == 2 { "array" } else { "string" });
                }
                match part_text(&path[1]) {
                    Some("exitCode") => kind("integer"),
                    Some("output") => kind("string"),
                    _ => Ok(None),
                }
            }
            "approval" => {
                if !part_is(Some(head), "message") {
                    return kind("string");
                }
                if path.len() == 1 || parts_are(&path[1..], &["resources"]) {
                    return kind("object");
                }
                if part_is(path.get(1), "comments") {
                    if !self.report.current_edition {
                        return if path.len() == 2 {
                            kind("array")
                        } else {
                            Ok(None)
                        };
                    }
                    return Ok(valueschema::kind_at_checked(
                        approval_comments(),
                        &path[2..],
                        &self.report.budget,
                        "/flow",
                    )?);
                }
                Ok(None)
            }
            "join" => {
                if part_is(Some(head), "outcome") {
                    return kind("string");
                }
                if path.len() == 1 {
                    return kind("object");
                }
                if !self.report.current_edition {
                    return match infos.get(&node.addr) {
                        Some(info) if path.len() == 2 => {
                            let fan =
                                part_text(&path[1]).is_some_and(|name| info.fanout.contains(name));
                            kind(if fan { "array" } else { "object" })
                        }
                        _ => Ok(None),
                    };
                }
                let Some((info, branch)) = infos
                    .get(&node.addr)
                    .zip(part_text(&path[1]))
                    .filter(|(info, branch)| info.branches.contains_key(*branch))
                else {
                    return Ok(None);
                };
                let fan = info.fanout.contains(branch);
                let mut rest = &path[2..];
                if rest.is_empty() {
                    return kind(if fan { "array" } else { "object" });
                }
                if fan {
                    if !is_index(rest.first()) {
                        return Ok(None);
                    }
                    rest = &rest[1..];
                    if rest.is_empty() {
                        return kind("object");
                    }
                    if part_is(rest.first(), "item") {
                        return Ok(None);
                    }
                }
                match rest.first().and_then(part_text) {
                    Some("ended" | "by") => return kind("string"),
                    Some("error") => return Ok(None),
                    _ => {}
                }
                let mut producers =
                    Self::branch_producers(graph, graph.incoming(&node.addr)?, info, branch)?;
                let Some((first, _)) = producers.next() else {
                    return Ok(None);
                };
                let first = self.declared_kind(graph, first, rest, infos, memo)?;
                for (index, _) in producers {
                    if self.declared_kind(graph, index, rest, infos, memo)? != first {
                        return Ok(None);
                    }
                }
                Ok(first)
            }
            _ => Ok(None),
        }
    }

    fn fanout_problem(
        &self,
        graph: &Graph<'a>,
        index: usize,
        infos: &OrderedMap<String, LocalJoin>,
    ) -> Result<Option<String>, Internal> {
        let Some(each) = graph.edges[index].each else {
            return Ok(None);
        };
        if is_single_path(each, "instructions") {
            return Ok(None);
        }
        let path = path_parts(each, graph)?;
        const MAX_SELECTION_PARTS: usize = 128;
        if path.len() > MAX_SELECTION_PARTS {
            let edge = &graph.edges[index];
            return Err(crate::resource::limit(
                &path_pointer(&edge.pointer),
                "fan-out selection path parts",
                MAX_SELECTION_PARTS,
            )
            .into());
        }
        Ok(
            match self.declared_kind(graph, index, &path, infos, &mut HashMap::new())? {
                Some(kind) if kind != "array" => Some(format!(
                    "a fan-out reads a list, and this value is declared as {}",
                    article(&kind)
                )),
                _ => None,
            },
        )
    }

    fn check_instruction_fanouts(&mut self, graph: &Graph<'a>) -> Result<(), Internal> {
        let mut routes: OrderedMap<(String, RouteKind, Option<String>), Vec<usize>> =
            OrderedMap::new();
        for (index, edge) in graph.edges.iter().enumerate() {
            let group = (edge.src.clone(), edge.kind, edge.key.clone());
            match routes.get_mut(&group) {
                Some(list) => list.push(index),
                None => routes.set(group, vec![index]),
            }
        }
        let instructions = |edge: &Edge<'a>| {
            edge.each
                .is_some_and(|each| is_single_path(each, "instructions"))
        };
        for edges in routes.values() {
            if !edges.iter().any(|&edge| instructions(&graph.edges[edge])) {
                continue;
            }
            for &index in edges {
                let edge = &graph.edges[index];
                if instructions(edge) {
                    continue;
                }
                let target = graph.node_at(&edge.dst)?;
                let uses = match edge.message {
                    None => target.kind == "agent" || target.kind == "approval",
                    Some(message) => member(Some(message), "prompt").is_some_and(|prompt| {
                        matches!(prompt, Value::Object(prompt)
                            if prompt.len() == 1 && prompt.get("from").is_some_and(|from| is_single_path(from, "instructions")))
                    }),
                };
                if uses {
                    let source = graph.node_at(&edge.src)?;
                    self.report.error_step(
                        "MESSAGE_PATH",
                        &edge.pointer,
                        format!(
                            "this route fans out over the instructions of '{}', so they are a list; '{}' cannot receive them as one prompt",
                            source.name, target.name
                        ),
                        expanded(&edge.src),
                    );
                }
            }
        }
        Ok(())
    }

    fn instance_of(node: &Node<'a>) -> Result<Option<&'a str>, Internal> {
        match non_null(node.decl.and_then(|decl| decl.get("instance"))) {
            None => Ok(None),
            Some(instance) => Ok(Some(text(instance, "instance")?)),
        }
    }

    fn check_shared_instances(
        &mut self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
        budget: &mut ExpansionBudget,
    ) -> Result<(), Internal> {
        let sorted = sorted_addresses(graph.nodes.keys());
        for (addr, info) in infos.iter() {
            let mut seen: HashMap<(String, Option<&'a str>), (String, String)> = HashMap::new();
            let mut finishing: HashMap<(String, Option<&'a str>), String> = HashMap::new();
            for (start, members) in info.branches.iter() {
                for member_addr in &sorted {
                    if !scope_member(member_addr, &info.prefix)
                        .is_some_and(|local| members.contains(local))
                    {
                        continue;
                    }
                    let node = graph.node_at(member_addr)?;
                    let (true, Some(agent)) = (node.kind == "agent", &node.agent) else {
                        continue;
                    };
                    let instance = Self::instance_of(node)?;
                    if instance == Some("new") {
                        continue;
                    }
                    let identity = (agent.clone(), instance);
                    finishing
                        .entry(identity.clone())
                        .or_insert_with(|| start.clone());
                    if node.decl.is_some_and(|decl| decl.contains_key("delivery")) {
                        continue;
                    }
                    match seen.get(&identity) {
                        Some(other) if other.0 != *start => {
                            let message = format!(
                                "steps '{}' and '{}' use the {} of agent '{}' in parallel branches; their work will be serialized",
                                other.1,
                                node.name,
                                instance_label(instance),
                                agent
                            );
                            self.report.warning_step(
                                "SHARED_INSTANCE_PARALLEL",
                                &node.pointer,
                                message,
                                expanded(member_addr),
                            );
                        }
                        Some(_) => {}
                        None => {
                            seen.insert(identity, (start.clone(), node.name.clone()));
                        }
                    }
                }
            }
            let join = graph.node_at(addr)?;
            if is_text(join.decl.and_then(|decl| decl.get("remaining")), "finish") {
                self.check_after_finish(graph, addr, &finishing, budget)?;
            }
        }
        self.check_fanout_instances(graph, infos)?;
        self.check_open_fork_instances(graph, infos)
    }

    fn check_open_fork_instances(
        &mut self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
    ) -> Result<(), Internal> {
        let origins: HashSet<String> = infos
            .values()
            .map(|info| {
                if info.prefix.is_empty() {
                    info.origin.clone()
                } else {
                    format!("{}/{}", info.prefix, info.origin)
                }
            })
            .collect();
        let mut warned: HashSet<String> = HashSet::new();
        let none = OrderedSet::new();
        for addr in sorted_addresses(graph.nodes.keys()) {
            if origins.contains(&addr) {
                continue;
            }
            let mut groups: OrderedMap<(RouteKind, Option<String>), OrderedSet<String>> =
                OrderedMap::new();
            for &edge in graph.outgoing(&addr)? {
                let edge = &graph.edges[edge];
                if edge.each.is_some() {
                    continue;
                }
                match groups.get_mut(&edge.group()) {
                    Some(targets) => {
                        targets.insert(edge.dst.clone());
                    }
                    None => groups.set(edge.group(), [edge.dst.clone()].into_iter().collect()),
                }
            }
            for targets in groups.values() {
                if targets.len() < 2 {
                    continue;
                }
                let mut users: HashMap<(String, Option<&'a str>), (String, String)> =
                    HashMap::new();
                for target in sorted_addresses(targets) {
                    for member_addr in sorted_addresses(&graph.reachable(&target, &none)?) {
                        let node = graph.node_at(&member_addr)?;
                        let (true, Some(agent)) = (node.kind == "agent", &node.agent) else {
                            continue;
                        };
                        if warned.contains(&member_addr) {
                            continue;
                        }
                        let instance = Self::instance_of(node)?;
                        if instance == Some("new")
                            || node.decl.is_some_and(|decl| decl.contains_key("delivery"))
                        {
                            continue;
                        }
                        let identity = (agent.clone(), instance);
                        match users.get(&identity) {
                            Some(other) if other.0 != target => {
                                warned.insert(member_addr.clone());
                                let message = format!(
                                    "steps '{}' and '{}' can use the {} of agent '{}' at the same time; their work will be serialized",
                                    other.1,
                                    node.name,
                                    instance_label(instance),
                                    agent
                                );
                                self.report.warning_step(
                                    "SHARED_INSTANCE_PARALLEL",
                                    &node.pointer,
                                    message,
                                    expanded(&member_addr),
                                );
                            }
                            Some(_) => {}
                            None => {
                                users.insert(identity, (target.clone(), node.name.clone()));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn check_fanout_instances(
        &mut self,
        graph: &Graph<'a>,
        infos: &OrderedMap<String, LocalJoin>,
    ) -> Result<(), Internal> {
        let mut warned: HashSet<String> = HashSet::new();
        let mut bounded: HashSet<String> = HashSet::new();
        for edge in &graph.edges {
            if edge.each.is_none() {
                continue;
            }
            graph.work(infos.len())?;
            let closing: OrderedSet<String> = infos
                .iter()
                .filter(|(_, info)| {
                    scope_member(&edge.src, &info.prefix) == Some(info.origin.as_str())
                        && scope_member(&edge.dst, &info.prefix)
                            .is_some_and(|local| info.fanout.contains(local))
                })
                .map(|(addr, _)| addr.clone())
                .collect();
            let limit = non_null(edge.fanout.and_then(|fanout| fanout.get("max")));
            graph.work(limit.map_or(0, numeric_size).div_ceil(8))?;
            let decimal_limit = limit
                .and_then(Value::as_number)
                .map(|number| decimal_v030(Num::Token(number)));
            let source = graph.node_at(&edge.src)?;
            for member_addr in sorted_addresses(&graph.reachable(&edge.dst, &closing)?) {
                let node = graph.node_at(&member_addr)?;
                let visits = non_null(node.decl.and_then(|decl| decl.get("maxVisits")));
                if let Some(visits) = visits.filter(|_| !bounded.contains(&member_addr)) {
                    let below = match &decimal_limit {
                        None => true,
                        Some(max) => {
                            let visits = visits
                                .as_number()
                                .ok_or_else(|| Internal::new("maxVisits is not a number"))?;
                            graph.work(visits.raw().len().div_ceil(8))?;
                            compare_decimals(&decimal_v030(Num::Token(visits)), max)
                                == Ordering::Less
                        }
                    };
                    if below {
                        bounded.insert(member_addr.clone());
                        let reason = match limit {
                            Some(Value::Number(limit)) => {
                                format!(
                                    "below the fan-out's max of {}",
                                    number_excerpt(limit.raw())
                                )
                            }
                            Some(_) => return internal("fan-out max is not a number"),
                            None => "while the fan-out has no max".to_owned(),
                        };
                        self.report.warning_step(
                            "FANOUT_VISITS",
                            &with(&node.pointer, [key("maxVisits")]),
                            format!(
                                "step '{}' runs once per element of the fan-out from '{}', and its maxVisits is {reason}; bound the step that fans out instead",
                                node.name, source.name
                            ),
                            expanded(&member_addr),
                        );
                    }
                }
                let (true, Some(agent)) = (node.kind == "agent", &node.agent) else {
                    continue;
                };
                if warned.contains(&member_addr) {
                    continue;
                }
                let instance = Self::instance_of(node)?;
                if instance == Some("new")
                    || node.decl.is_some_and(|decl| decl.contains_key("delivery"))
                {
                    continue;
                }
                warned.insert(member_addr.clone());
                self.report.warning_step(
                    "SHARED_INSTANCE_PARALLEL",
                    &node.pointer,
                    format!(
                        "step '{}' uses the {} of agent '{}' in every path of the fan-out from '{}'; their work will be serialized",
                        node.name,
                        instance_label(instance),
                        agent,
                        source.name
                    ),
                    expanded(&member_addr),
                );
            }
        }
        Ok(())
    }

    fn check_after_finish(
        &mut self,
        graph: &Graph<'a>,
        join: &str,
        finishing: &HashMap<(String, Option<&'a str>), String>,
        budget: &mut ExpansionBudget,
    ) -> Result<(), Internal> {
        let join_node = graph.node_at(join)?;
        for later in sorted_addresses(&graph.reachable(join, &OrderedSet::new())?) {
            if later == join {
                continue;
            }
            let node = graph.node_at(&later)?;
            let (true, Some(agent)) = (node.kind == "agent", &node.agent) else {
                continue;
            };
            let instance = Self::instance_of(node)?;
            if instance == Some("new")
                || is_text(node.decl.and_then(|decl| decl.get("delivery")), "queue")
            {
                continue;
            }
            let Some(branch) = finishing.get(&(agent.clone(), instance)) else {
                continue;
            };
            budget.join_analysis(1, graph.scopes[join_node.scope].composition.as_deref())?;
            self.report.warning_step(
                "SHARED_INSTANCE_PARALLEL",
                &node.pointer,
                format!(
                    "step '{}' uses the {} of agent '{}', which branch '{}' of join '{}' may still use while it finishes; its work will wait",
                    node.name,
                    instance_label(instance),
                    agent,
                    branch,
                    join_node.name
                ),
                expanded(&later),
            );
        }
        Ok(())
    }

    fn check_harnesses(&mut self) -> Result<(), Internal> {
        let agents: &'a Object = self.agents;
        for (name, agent) in agents.iter() {
            if self.report.limited {
                break;
            }
            let parts = vec![key("agents"), key(name)];
            let agent = object(agent, "agent")?;
            let harness_name = text(required(agent, "harness")?, "harness")?;
            let modules: &'r ModuleMap = self.modules;
            let Some(module) = modules.get(harness_name) else {
                self.report.add(
                    "warning",
                    "HARNESS_UNVERIFIED",
                    path_pointer(&with(&parts, [key("harness")])),
                    format!("no module for harness '{harness_name}': the agent is not verified"),
                    None,
                );
                continue;
            };
            let module = object(module, "module")?;
            let harness = text(required(module, "name")?, "module name")?;
            let usage = self.uses.get(name).expect("agent usage");
            let (usage_new, usage_steer) = (usage.new, usage.steer);
            let mut used: Vec<(String, Vec<PointerPart>)> = Vec::new();
            if object_or_empty(agent.get("resources"), "resources")?.len() > 0 {
                used.push(("resources".to_owned(), with(&parts, [key("resources")])));
            }
            let model = object_or_empty(agent.get("model"), "model")?;
            if model.contains_key("name") {
                used.push((
                    "model.name".to_owned(),
                    with(&parts, [key("model"), key("name")]),
                ));
            }
            if model.contains_key("effort") {
                used.push((
                    "model.effort".to_owned(),
                    with(&parts, [key("model"), key("effort")]),
                ));
            }
            if !list_or_empty(agent.get("skills"), "skills")?.is_empty() {
                used.push(("skills".to_owned(), with(&parts, [key("skills")])));
            }
            if !list_or_empty(agent.get("mcpServers"), "mcpServers")?.is_empty() {
                used.push(("mcpServers".to_owned(), with(&parts, [key("mcpServers")])));
            }
            if agent.get("plan").is_some_and(truthy) {
                used.push(("plan".to_owned(), with(&parts, [key("plan")])));
            }
            if let Some(plan_first) = agent.get("planFirst") {
                used.push((
                    format!("planFirst.{}", text(plan_first, "planFirst")?),
                    with(&parts, [key("planFirst")]),
                ));
            }
            let permissions = agent.get("permissions");
            if let Some(Value::Object(object_permissions)) = permissions {
                if let Some(reviewer) = object_permissions.get("reviewer") {
                    used.push((
                        format!("reviewer.{}", text(reviewer, "reviewer")?),
                        with(&parts, [key("permissions"), key("reviewer")]),
                    ));
                }
                if object_or_empty(object_permissions.get("directories"), "directories")?.len() > 0
                {
                    used.push((
                        "directories".to_owned(),
                        with(&parts, [key("permissions"), key("directories")]),
                    ));
                }
            }
            if usage_new {
                used.push(("instance.new".to_owned(), parts.clone()));
            }
            if usage_steer {
                used.push(("delivery.steer".to_owned(), parts.clone()));
            }
            for (property, at) in &used {
                if self.report.limited {
                    return Ok(());
                }
                self.check_property(module, harness, property, at, agent)?;
            }
            self.check_permissions(module, harness, agent, &parts)?;
            if let Some(settings_value) = agent.get("harnessSettings") {
                let at = with(&parts, [key("harnessSettings")]);
                match non_null(module.get("settings")) {
                    None => self.report.error(
                        "HARNESS_SETTINGS",
                        &at,
                        format!("module '{harness}' declares no settings"),
                    ),
                    Some(settings) => {
                        let budget = self.report.budget.clone();
                        valueschema::check_value(
                            settings_value,
                            settings,
                            &at,
                            &budget,
                            &mut |at, make| self.report.value_problem("HARNESS_SETTINGS", at, make),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn check_property(
        &mut self,
        module: &Object,
        harness: &str,
        property: &str,
        at: &[PointerPart],
        agent: &Object,
    ) -> Result<(), Internal> {
        let properties = object_or_empty(module.get("properties"), "properties")?;
        let entry = properties.get(property);
        let support = match entry {
            None => "absent",
            Some(entry) => text(
                required(object(entry, "support entry")?, "support")?,
                "support",
            )?,
        };
        let note = match member(entry, "note") {
            Some(note) if truthy(note) => text(note, "note")?,
            _ => "",
        };
        // Retain the same truncated message prefix without cloning the full
        // module note on every agent that uses this property.
        let note = if note.is_empty() {
            String::new()
        } else {
            let end = note.floor_char_boundary(note.len().min(MAX_MESSAGE_BYTES));
            format!(" {}", &note[..end])
        };
        let label = property.replace('.', " ");
        if support == "absent" {
            self.report.error(
                "HARNESS_UNSUPPORTED",
                at,
                format!("{harness} does not support {label}.{note}"),
            );
            return Ok(());
        }
        if support == "approximate" {
            self.report.add(
                "warning",
                "HARNESS_APPROXIMATE",
                path_pointer(at),
                format!("{harness} approximates {label}.{note}"),
                None,
            );
        }
        if let Some(versions) = member(entry, "versions") {
            self.report.add(
                "warning",
                "HARNESS_VERSION",
                path_pointer(at),
                format!("{harness} supports {label} only for versions {}.", {
                    let versions = text(versions, "versions")?;
                    &versions[..versions.floor_char_boundary(versions.len().min(MAX_MESSAGE_BYTES))]
                }),
                None,
            );
        }
        if property == "model.effort" {
            if let Some(values) = member(entry, "values") {
                let Value::Array(values) = values else {
                    return internal("support values are not an array");
                };
                let effort = required(object(required(agent, "model")?, "model")?, "effort")?;
                let at_ptr = path_pointer(at);
                let effort_bytes = effort.as_str().map_or(0, str::len);
                self.report.budget.work(
                    values
                        .iter()
                        .map(|value| {
                            1 + value
                                .as_str()
                                .map_or(0, str::len)
                                .min(effort_bytes)
                                .div_ceil(8)
                        })
                        .sum(),
                    &at_ptr,
                )?;
                let accepted = values
                    .iter()
                    .any(|value| matches!((value, effort), (Value::String(a), Value::String(b)) if a == b));
                if !accepted {
                    let mut shown = String::new();
                    for value in values {
                        if !shown.is_empty() {
                            shown.push_str(", ");
                        }
                        let value = text(value, "support value")?;
                        let remaining = MAX_MESSAGE_BYTES.saturating_sub(shown.len());
                        shown.push_str(
                            &value[..value.floor_char_boundary(value.len().min(remaining))],
                        );
                        if shown.len() + 4 >= MAX_MESSAGE_BYTES {
                            break;
                        }
                    }
                    self.report.error(
                        "HARNESS_VALUE",
                        at,
                        format!("{harness} accepts {}; got {}", shown, repr_value(effort)),
                    );
                }
            }
        }
        Ok(())
    }

    fn check_permissions(
        &mut self,
        module: &Object,
        harness: &str,
        agent: &Object,
        parts: &[PointerPart],
    ) -> Result<(), Internal> {
        let at = with(parts, [key("permissions")]);
        let enforcement = object_or_empty(module.get("permissions"), "module permissions")?;
        let permissions = agent.get("permissions");
        let level_of = |action: &str, field: &str| -> Option<&Value> {
            member(enforcement.get(action), field)
        };
        let is_absent = |level: Option<&Value>| level.is_none() || is_text(level, "absent");
        let shell_open =
            !permissions::shell_denied(permissions)? && !is_absent(level_of("shell", "allow"));
        let bypass = format!(
            "shell commands bypass {harness}'s rules for it; deny shell, or use a harness that enforces it with a sandbox"
        );
        let enforced = |action: &str, level: Option<&Value>| {
            !is_absent(level)
                && !(shell_open
                    && is_text(level, "harness")
                    && matches!(action, "read" | "write" | "network"))
        };
        for (action, effects) in permissions::effects_in_use(permissions)? {
            if action == "mcp" && list_or_empty(agent.get("mcpServers"), "mcpServers")?.is_empty() {
                continue;
            }
            for effect in ["deny", "ask"] {
                if !effects.iter().any(|known| known == effect) {
                    continue;
                }
                let level = level_of(action, effect);
                if is_absent(level) {
                    self.report.error(
                        "HARNESS_UNSUPPORTED",
                        &at,
                        format!(
                            "{harness} cannot enforce {} for {}",
                            repr_str(effect),
                            repr_str(action)
                        ),
                    );
                } else if !enforced(action, level) {
                    self.report.error(
                        "HARNESS_UNSUPPORTED",
                        &at,
                        format!(
                            "{harness} cannot enforce {} for {}: {bypass}",
                            repr_str(effect),
                            repr_str(action)
                        ),
                    );
                }
            }
        }
        let (preset, rules) = permissions::normalize(permissions)?;
        if is_text(level_of("shell", "match"), "prefix") {
            for (index, rule) in rules.iter().enumerate() {
                if rule.action != "shell" || rule.effect == "allow" || permissions::covers_all(rule)
                {
                    continue;
                }
                let matcher = rule.matcher.expect("a partial rule has a match");
                if !permissions::command_prefix(matcher) {
                    self.report.error(
                        "HARNESS_UNSUPPORTED",
                        &with(&at, [key("rules"), PointerPart::Index(index), key("match")]),
                        format!("{harness} matches commands by their leading words only; write the words, then \" *\" for any arguments"),
                    );
                }
            }
        }
        if preset.is_some() && permissions::outside(permissions)? == "deny" {
            let level = level_of("read", "outside");
            if is_absent(level) {
                self.report.error(
                    "HARNESS_UNSUPPORTED",
                    &at,
                    format!("{harness} cannot deny reads outside the zone; set \"outside\": \"read\" or use another harness"),
                );
            } else if !enforced("read", level) {
                self.report.error(
                    "HARNESS_UNSUPPORTED",
                    &at,
                    format!("{harness} cannot deny reads outside the zone: {bypass}"),
                );
            }
        }
        Ok(())
    }
}

/// `sameKeys(value, expected)`.
fn same_keys(value: &Object, expected: &OrderedSet<String>) -> bool {
    value.len() == expected.len() && value.iter().all(|(name, _)| expected.contains(name))
}

/// `compareNumbers(a, b)` over two authored numbers.
fn path_allocation(path: &[PointerPart]) -> usize {
    path.iter()
        .map(|part| {
            32 + match part {
                PointerPart::Key(key) => key.len(),
                PointerPart::Index(_) => 0,
                PointerPart::Number(number) => number.raw().len(),
            }
        })
        .sum()
}

fn numeric_size(value: &Value) -> usize {
    match value {
        Value::Number(number) => number.raw().len(),
        _ => 0,
    }
}

/// `JSON.stringify(parts)` identity of a pointer path.
fn parts_identity(parts: &[PointerPart]) -> String {
    let items: Vec<String> = parts
        .iter()
        .map(|part| match part {
            PointerPart::Key(name) => quote(name),
            PointerPart::Index(index) => index.to_string(),
            PointerPart::Number(token) => token.raw().to_owned(),
        })
        .collect();
    format!("[{}]", items.join(","))
}

/// `error_path_problem(path)`.
fn error_path_problem(path: &[PointerPart]) -> Option<String> {
    if !part_is(path.first(), "error") {
        return Some("an error route carries only 'error'".to_owned());
    }
    let field = path.get(1)?;
    match part_text(field) {
        Some(name @ ("code" | "message")) => {
            (path.len() > 2).then(|| format!("'{name}' is a string"))
        }
        Some("details") => (path.len() > 2 && !is_index(path.get(2)))
            .then(|| "'details' is a list: select an entry by its index".to_owned()),
        _ => Some(format!("an error has no field {}", repr_part(field))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::argentic_core::exact::scan::parse_v030;

    fn read(text: &str) -> Report {
        validate_bytes(text.as_bytes(), &ModuleMap::new()).unwrap()
    }

    #[test]
    fn directory_errors_keep_source_order() {
        let report = read(
            r#"{"contract":"agsdl-0.3.0","id":"order","agents":{"a":{"harness":"unknown","permissions":{"preset":"read-only","directories":{"2":"write","1":"write"}}}}}"#,
        );
        let found: Vec<(&str, &str)> = report
            .diagnostics
            .iter()
            .map(|d| (d.code, d.path.as_str()))
            .collect();
        assert_eq!(
            found,
            [
                ("PERMISSIONS", "/agents/a/permissions/directories/2"),
                ("PERMISSIONS", "/agents/a/permissions/directories/1"),
                ("HARNESS_UNVERIFIED", "/agents/a/harness"),
            ]
        );
        assert!(!report.valid());
    }

    #[test]
    fn approval_resource_analysis_unions_paths_and_leaves_cycles_unknown() {
        for cyclic in [false, true] {
            let steps = serde_json::json!({
                "prepare":{"type":"agent","agent":"a","next":[
                    "left",{"step":"right","message":{"resources":{"extra":{"from":["text"]}}}}
                ]},
                "left":{"type":"approval","next":{"yes":"check","no":[]}},
                "right":{"type":"approval","next":{"yes":"check","no":[]}},
                "check":{"type":"approval","next":{"yes":if cyclic {serde_json::json!("left")} else {serde_json::json!([])},"no":[]}}
            });
            let source = serde_json::json!({
                "agents":{"a":{"results":{"notes":{}}}},
                "flow":{"entry":"prepare","steps":steps}
            });
            let parsed = parse_v030(source.to_string().as_bytes()).unwrap();
            let doc = parsed.tree.as_object().unwrap();
            let graph = Graph::for_scope(doc, None, None, &mut ExpansionBudget::default()).unwrap();
            let modules = ModuleMap::new();
            let mut report = Report::new();
            let checker = Checker::new(doc, &modules, &mut report).unwrap();
            let names = checker
                .received_resources(&graph, "check", &OrderedMap::new())
                .unwrap();
            if cyclic {
                assert!(names.is_none());
            } else {
                let names = names.unwrap();
                assert_eq!(
                    names.iter().map(String::as_str).collect::<Vec<_>>(),
                    ["text", "notes", "extra"]
                );
            }
        }
    }

    #[test]
    fn report_json_uses_js_member_order() {
        let mut report = Report::new();
        report.joins.set(
            "b".to_owned(),
            ReportJoin {
                origin: "o".to_owned(),
                branches: vec!["x".to_owned()],
            },
        );
        report.joins.set(
            "1".to_owned(),
            ReportJoin {
                origin: "o".to_owned(),
                branches: vec![],
            },
        );
        report.add(
            "warning",
            "HARNESS_UNVERIFIED",
            "/a".to_owned(),
            "m".to_owned(),
            Some(vec!["a".to_owned(), "b".to_owned()]),
        );
        report.add(
            "warning",
            "HARNESS_UNVERIFIED",
            "/a".to_owned(),
            "m".to_owned(),
            None,
        );
        assert_eq!(
            report.to_json(),
            r#"{"contract":"agsdl-0.3.0","valid":true,"diagnostics":[{"severity":"warning","code":"HARNESS_UNVERIFIED","path":"/a","message":"m","step":["a","b"]}],"joins":{"1":{"origin":"o","branches":[]},"b":{"origin":"o","branches":["x"]}}}"#
        );
    }

    #[test]
    fn module_checks_report_the_first_problem() {
        let module = parse_v030(br#"{"2":0}"#).unwrap().tree;
        let Err(ReadError::Module(error)) = check_module(module) else {
            panic!("module error")
        };
        assert!(
            error.message.starts_with("/: missing field"),
            "{}",
            error.message
        );
    }
}
