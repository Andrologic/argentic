// SPDX-License-Identifier: Apache-2.0
//! Reader entry points for AgSDL 0.3. Rust translation of
//! `src/vendor/agsdl/v0.3/reader.mjs` (based on AgSDL v0.3.0
//! tooling/agsdl/validator.py, upstream commit
//! bd9f8a5b16923476fd20ac2650bd1729ee151917). Provenance and license:
//! `src/vendor/agsdl/NOTICE.md`, `src/vendor/agsdl/v0.3/LICENSE`.
//!
//! Inputs are explicit: document bytes or an exact value, and either the four
//! pinned modules or a caller [`ModuleMap`]. Every module is detached through
//! exact serialization and the 0.3 scanner before use, as `copyModule` does:
//! lexemes and member order survive, including array-index keys. A module that
//! declares support for a property qualifies no adapter; it only feeds the
//! static diagnostics.
use super::{
    calculation::Failure,
    exact::{
        scan::{parse_shared, parse_v030},
        stringify, Value,
    },
    validate::{
        check_module_with_budget, validate_document_with_budget, Internal, ModuleError, ModuleMap,
        ReadError, Report,
    },
};
use crate::{SourceBudget, GRAPH_LIMIT_CODE};
use std::sync::OnceLock;

/// The pinned harness modules, in the import order of `reader.mjs`.
pub(crate) const PINNED_MODULES: [(&str, &str); 4] = [
    (
        "claude-code",
        include_str!("../../assets/harnesses/claude-code/module.json"),
    ),
    (
        "codex",
        include_str!("../../assets/harnesses/codex/module.json"),
    ),
    (
        "opencode",
        include_str!("../../assets/harnesses/opencode/module.json"),
    ),
    ("pi", include_str!("../../assets/harnesses/pi/module.json")),
];

/// `options.modules`: omitted (the pinned four) or an explicit map, which
/// stays empty when it is empty.
#[derive(Clone, Copy)]
pub(crate) enum Modules<'m> {
    Pinned,
    Explicit(&'m ModuleMap),
    Encoded(&'m [&'m [u8]]),
}

/// `loadHarnessModule(bytes)`.
pub(crate) fn load_harness_module(bytes: &[u8]) -> Result<Value, ReadError> {
    load_harness_module_with_budget(bytes, &SourceBudget::default())
}

pub(crate) fn load_harness_module_with_budget(
    bytes: &[u8],
    budget: &SourceBudget,
) -> Result<Value, ReadError> {
    budget.json_input(bytes, "").map_err(Internal::from)?;
    match parse_v030(bytes) {
        Err(error) => Err(ModuleError {
            message: error.message(),
        }
        .into()),
        Ok(parsed) => check_module_with_budget(parsed.tree, budget),
    }
}

/// `copyModule(module)`: exact text, rescanned and checked again. Any other
/// failure becomes a `ModuleError` with its message.
pub(crate) fn copy_module(module: &Value) -> Result<Value, ModuleError> {
    match load_harness_module(stringify(module).as_bytes()) {
        Ok(copy) => Ok(copy),
        Err(ReadError::Module(error)) => Err(error),
        Err(ReadError::Internal(error)) => Err(ModuleError {
            message: error.message,
        }),
    }
}

fn module_name(module: &Value) -> Result<&str, Internal> {
    match module.as_object().and_then(|object| object.get("name")) {
        Some(Value::String(name)) => Ok(name),
        _ => Err(Internal::new("a checked module has no name")),
    }
}

/// The detached pinned modules, computed once like the JS import.
fn defaults() -> Result<&'static ModuleMap, ReadError> {
    static CELL: OnceLock<Result<ModuleMap, ReadError>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut modules = ModuleMap::new();
        for (name, text) in PINNED_MODULES {
            // The JSON import is `JSON.parse`: ordinary JS member order.
            let tree = parse_shared(text.as_bytes()).map_err(|error| {
                Internal::new(format!(
                    "pinned module {name} is not exact JSON: {}",
                    error.message()
                ))
            })?;
            let detached = copy_module(&tree.tree).map_err(ReadError::Module)?;
            modules.set(name.into(), detached);
        }
        Ok(modules)
    })
    .as_ref()
    .map_err(Clone::clone)
}

/// `registry(options)`: a detached copy of every module, keyed as given.
#[cfg(test)]
pub(crate) fn registry(modules: Modules<'_>) -> Result<ModuleMap, ReadError> {
    registry_with_budget(modules, &SourceBudget::default())
}

fn registry_with_budget(
    modules: Modules<'_>,
    budget: &SourceBudget,
) -> Result<ModuleMap, ReadError> {
    let source: Vec<(&str, &Value)> = match modules {
        Modules::Pinned => defaults()?
            .iter()
            .map(|(name, module)| (name.as_str(), module))
            .collect(),
        Modules::Explicit(map) => map
            .iter()
            .map(|(name, module)| (name.as_str(), module))
            .collect(),
        Modules::Encoded(sources) => {
            let mut modules = ModuleMap::new();
            for source in sources {
                let module = load_harness_module_with_budget(source, budget)?;
                let name = module_name(&module)?.to_owned();
                if modules.get(&name).is_some() {
                    return Err(ModuleError {
                        message: format!("Duplicate harness module '{name}'."),
                    }
                    .into());
                }
                modules.set(name, module);
            }
            return Ok(modules);
        }
    };
    let mut detached = ModuleMap::new();
    for (name, value) in source {
        let module = copy_module(value)?;
        let declared = module_name(&module)?;
        if name != declared {
            return Err(ModuleError {
                message: format!("module registry key '{name}' does not match name '{declared}'"),
            }
            .into());
        }
        detached.set(name.to_owned(), module);
    }
    Ok(detached)
}

fn parse_report(message: String) -> Report {
    let mut report = Report::new();
    report.error_at("PARSE", "", message);
    report
}

/// `read(bytes, options)`.
pub(crate) fn read(bytes: &[u8], modules: Modules<'_>) -> Result<Report, ReadError> {
    read_budgeted(bytes, modules, &SourceBudget::default())
}

fn read_budgeted(
    bytes: &[u8],
    modules: Modules<'_>,
    budget: &SourceBudget,
) -> Result<Report, ReadError> {
    if let Err(error) = budget.json_input(bytes, "") {
        let mut report = Report::new();
        report.error_at(GRAPH_LIMIT_CODE, &error.pointer, error.message);
        return Ok(report);
    }
    let parsed = match parse_v030(bytes) {
        Ok(parsed) => parsed,
        Err(error) => return Ok(parse_report(error.message())),
    };
    let detached;
    let registry = match modules {
        Modules::Pinned => defaults()?,
        other => {
            detached = registry_with_budget(other, budget)?;
            &detached
        }
    };
    Ok(validate_document_with_budget(
        &parsed.tree,
        registry,
        budget,
    )?)
}

/// Check JSON syntax and unique property names within a source budget.
pub fn check_source_json(text: &str, pointer: &str, budget: &SourceBudget) -> Result<(), Failure> {
    budget.json_input(text.as_bytes(), pointer)?;
    parse_v030(text.as_bytes())
        .map(|_| ())
        .map_err(|error| Failure {
            code: "PARSE".into(),
            pointer: pointer.into(),
            message: error.message(),
        })
}

/// Compare source values using exact numeric equality within the operation budget.
pub fn source_json_equal(
    a: &str,
    b: &str,
    at: &str,
    budget: &SourceBudget,
) -> Result<bool, Failure> {
    compare_source(a, b, at, budget, false)
}

/// Compare source values while preserving authored number tokens.
pub fn source_document_tokens_equal(
    a: &str,
    b: &str,
    at: &str,
    budget: &SourceBudget,
) -> Result<bool, Failure> {
    compare_source(a, b, at, budget, true)
}

fn compare_source(
    a: &str,
    b: &str,
    at: &str,
    budget: &SourceBudget,
    tokens: bool,
) -> Result<bool, Failure> {
    let parse = |text: &str| {
        budget.json_input(text.as_bytes(), at)?;
        parse_v030(text.as_bytes()).map_err(|error| Failure {
            code: "PARSE".into(),
            pointer: at.into(),
            message: error.message(),
        })
    };
    let a = parse(a)?;
    let b = parse(b)?;
    crate::exact::values::equal_with_budget(&a.tree, &b.tree, budget, at, tokens)
}

/// Parse source-component JSON with exact numbers and unique object keys.
pub fn parse_source_json(
    bytes: &[u8],
    budget: &SourceBudget,
) -> Result<serde_json::Value, Failure> {
    use crate::exact::Value as Exact;
    use serde_json::Value;
    fn convert(value: &Exact) -> Value {
        match value {
            Exact::Null => Value::Null,
            Exact::Bool(value) => Value::Bool(*value),
            Exact::String(value) => Value::String(value.clone()),
            Exact::Number(value) => {
                Value::Number(serde_json::from_str(value.raw()).expect("scanned JSON number"))
            }
            Exact::Array(values) => Value::Array(values.iter().map(convert).collect()),
            Exact::Object(values) => Value::Object(
                values
                    .iter()
                    .map(|(name, value)| (name.to_owned(), convert(value)))
                    .collect(),
            ),
        }
    }
    budget.json_input(bytes, "")?;
    let parsed = parse_v030(bytes).map_err(|error| Failure {
        code: "PARSE".into(),
        pointer: "".into(),
        message: error.message(),
    })?;
    // Conversion is bounded by the depth and allocation allowance reserved
    // by json_input. Building objects directly preserves all user keys.
    Ok(convert(&parsed.tree))
}

/// Validate an assembled source and return its first error. A resource-limit
/// refusal takes precedence so hosts can distinguish it from content errors.
pub fn validate_source(bytes: &[u8], budget: &SourceBudget) -> Result<(), Failure> {
    let report = read_budgeted(bytes, Modules::Pinned, budget).map_err(read_failure)?;
    let diagnostic = report
        .diagnostics
        .iter()
        .find(|d| d.code == GRAPH_LIMIT_CODE)
        .or_else(|| report.diagnostics.iter().find(|d| d.severity == "error"));
    match diagnostic {
        Some(diagnostic) => Err(Failure {
            code: diagnostic.code.into(),
            pointer: diagnostic.path.clone(),
            message: diagnostic.message.clone(),
        }),
        None => Ok(()),
    }
}

fn read_failure(error: ReadError) -> Failure {
    match error {
        ReadError::Module(error) => Failure {
            code: "HARNESS_MODULE".into(),
            pointer: "/modules".into(),
            message: error.message,
        },
        ReadError::Internal(error) => Failure {
            code: if error.limit_pointer.is_some() {
                GRAPH_LIMIT_CODE
            } else {
                "VALIDATION"
            }
            .into(),
            pointer: error.limit_pointer.unwrap_or_default(),
            message: error.message,
        },
    }
}

/// Validate authored AgSDL 0.3.0 bytes and return the standard JSON report.
///
/// `None` selects the bundled modules. `Some` uses only the supplied module
/// documents, including when the slice is empty. Invalid authored documents
/// yield a report with `valid: false`. Configuration and internal errors
/// return a [`Failure`]. Validation does not fetch content or observe a harness.
pub fn validate_document_json(bytes: &[u8], modules: Option<&[&[u8]]>) -> Result<String, Failure> {
    validate_document_json_with_budget(bytes, modules, &SourceBudget::default())
}

/// Validate and encode a report using the supplied operation budget.
pub fn validate_document_json_with_budget(
    bytes: &[u8],
    modules: Option<&[&[u8]]>,
    budget: &SourceBudget,
) -> Result<String, Failure> {
    let modules = modules.map_or(Modules::Pinned, Modules::Encoded);
    let report = read_budgeted(bytes, modules, budget).map_err(read_failure)?;
    report.to_json_with_budget(budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_files_parse_identically_with_both_member_orders() {
        // JSON.parse (ordinary order) and the 0.3 scanner agree on these
        // bytes: no array-index key and no number lexeme in the modules.
        for (name, text) in PINNED_MODULES {
            let shared = stringify(&parse_shared(text.as_bytes()).unwrap().tree);
            let v030 = stringify(&parse_v030(text.as_bytes()).unwrap().tree);
            assert_eq!(shared, v030, "{name}");
            let tree = parse_v030(text.as_bytes()).unwrap().tree;
            let mut pending = vec![&tree];
            while let Some(node) = pending.pop() {
                match node {
                    Value::Number(token) => panic!("{name} has the number {}", token.raw()),
                    Value::Array(items) => pending.extend(items),
                    Value::Object(object) => {
                        for (key, item) in object.iter() {
                            assert!(
                                crate::argentic_core::exact::scan::array_index(key).is_none(),
                                "{name}: {key}"
                            );
                            pending.push(item);
                        }
                    }
                    _ => {}
                }
            }
        }
        let pinned = registry(Modules::Pinned).unwrap();
        let names: Vec<_> = pinned.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["claude-code", "codex", "opencode", "pi"]);
    }

    #[test]
    fn copies_keep_array_index_keys_in_insertion_order() {
        let text = r#"{"name":"custom","version":"1.0.0","contract":"agsdl-0.3.0","harnessVersions":"*","properties":{},"permissions":{},"settings":{"type":"object","properties":{"2":{"type":"string","enum":["x"]},"1":{"type":"array","maxItems":12345678901234567890123,"items":{"type":"integer","enum":[1e400]}}}}}"#;
        let module = load_harness_module(text.as_bytes()).unwrap();
        let copy = copy_module(&module).unwrap();
        let encoded = stringify(&copy);
        assert!(encoded.contains(r#""properties":{"2":"#), "{encoded}");
        assert!(encoded.contains("12345678901234567890123") && encoded.contains("1e400"));
    }

    #[test]
    fn explicit_maps_replace_in_place_and_check_names() {
        let mut map = ModuleMap::new();
        let broken = parse_v030(br#"{"name":"codex"}"#).unwrap().tree;
        map.set("codex".to_owned(), broken);
        let codex = load_harness_module(PINNED_MODULES[1].1.as_bytes()).unwrap();
        map.set("codex".to_owned(), codex);
        assert_eq!(registry(Modules::Explicit(&map)).unwrap().len(), 1);
        let mut renamed = ModuleMap::new();
        renamed.set(
            "other".to_owned(),
            load_harness_module(PINNED_MODULES[1].1.as_bytes()).unwrap(),
        );
        let Err(ReadError::Module(error)) = registry(Modules::Explicit(&renamed)) else {
            panic!("mismatch")
        };
        assert_eq!(
            error.message,
            "module registry key 'other' does not match name 'codex'"
        );
        let parse = read(b"{", Modules::Explicit(&renamed)).unwrap();
        assert_eq!(
            parse.to_json(),
            r#"{"contract":"agsdl-0.3.0","valid":false,"diagnostics":[{"severity":"error","code":"PARSE","path":"","message":"Invalid JSON at byte 1"}],"joins":{}}"#
        );
    }
}
