//! Complete compileHost030 facade, including entry/delivery checks and the
//! original UTF-8 transport refusal. No broker, discovery or execution occurs.
use super::{encode as encode_bounded, graph, limit_error, plan};
use crate::argentic_core::{
    exact::{self, Object, Value as E},
    messages::{self, CapturedContent},
    native::{a, o, s, Error, Text, Value},
    route::{ItemContext, Provenance, RouteSource, RouteTarget, SourceKind},
    schema::{self, SchemaChecker},
};
use crate::{SourceBudget, GRAPH_LIMIT_CODE};
pub(crate) const RESULT_LIMIT: usize = 8_000_000;
fn invalid() -> Value {
    o([
        ("valid", Value::Bool(false)),
        ("shapeValid", Value::Bool(false)),
        ("unassessed", a([])),
    ])
}
fn empty() -> &'static Object {
    crate::argentic_core::graph::empty_object()
}
fn host_message(raw: &Value, document: &Object, budget: &SourceBudget) -> Result<Value, Error> {
    let result = (|| -> Result<Value, Error> {
        let units = raw.text().map_or(0, |text| text.units().len());
        budget.bytes(units * 4, "/message").map_err(limit_error)?;
        budget
            .work(units.div_ceil(8), "/message")
            .map_err(limit_error)?;
        let Some(raw) = raw.text().and_then(Text::utf8) else {
            return Ok(invalid());
        };
        budget
            .json_input(raw.as_bytes(), "/message")
            .map_err(limit_error)?;
        let value = exact::decode(&raw)?;
        let message = exact::record(Some(&value), "")?;
        static ROOT: std::sync::OnceLock<SchemaChecker> = std::sync::OnceLock::new();
        if ROOT.get().is_none() {
            // The first entry check compiles a root over the bundled definitions.
            budget
                .json_input(schema::DOCUMENT_SCHEMA_JSON.as_bytes(), "/message")
                .map_err(limit_error)?;
            budget
                .bytes(schema::DOCUMENT_SCHEMA_JSON.len() * 16, "/message")
                .map_err(limit_error)?;
        }
        let root=ROOT.get_or_init(||SchemaChecker::new(schema::root_with_document_defs(r##""type":"object","additionalProperties":false,"properties":{"prompt":{"$ref":"#/$defs/instructions"},"resources":{"$ref":"#/$defs/resources"}}"##).expect("code-defined host message schema")));
        if !root
            .check_with_budget(&value, budget)
            .map_err(|error| {
                if let Some(pointer) = error.limit_pointer {
                    return Error::new(GRAPH_LIMIT_CODE, pointer, error.message);
                }
                Error::new(
                    "COMPILATION",
                    "",
                    "Invalid compilation input or inconsistent dependency contract.",
                )
            })?
            .is_empty()
        {
            return Ok(invalid());
        }
        let mut unassessed = vec![];
        let mut source = |v: &E| -> Result<(), Error> {
            let mut source = exact::record(Some(v), "")?;
            if let Some(reference) = source.get("ref") {
                let content = match document.get("content") {
                    None | Some(E::Null) => empty(),
                    value => exact::record(value, "")?,
                };
                let name = reference.as_str().filter(|name| content.contains_key(name));
                let Some(name) = name else {
                    return Err(Error::new("REFERENCE", "", "Unknown content reference."));
                };
                source = exact::record(content.get(name), "")?;
            }
            if source.contains_key("uri") {
                unassessed.push("Native content capture for URI");
            }
            Ok(())
        };
        if let Some(E::Array(prompt)) = message.get("prompt") {
            for item in prompt {
                source(item)?;
            }
        }
        if message.contains_key("resources") {
            let resources = exact::record(message.get("resources"), "")?;
            for (_, v) in resources.iter() {
                source(v)?;
            }
            if resources.len() > 0 {
                unassessed.push("Native resource delivery qualification");
            }
        }
        let mut unique = vec![];
        for item in unassessed {
            if !unique.contains(&item) {
                unique.push(item);
            }
        }
        Ok(o([
            ("valid", Value::Bool(unique.is_empty())),
            ("shapeValid", Value::Bool(true)),
            ("unassessed", a(unique.into_iter().map(s))),
        ]))
    })();
    match result {
        Err(error) if error.code != GRAPH_LIMIT_CODE => Ok(invalid()),
        result => result,
    }
}
fn encode(
    mut result: Value,
    registry: &Value,
    validations: &Value,
    budget: &SourceBudget,
) -> Result<String, Error> {
    let encoded_registry = encode_bounded(budget, RESULT_LIMIT, |out| registry.stringify_to(out));
    let encoding = encoded_registry.and_then(|encoded_registry| {
        let Some(encoded_registry) = encoded_registry else {
            return Ok(None);
        };
        budget
            .bytes(encoded_registry.len() * 2, "/registry")
            .map_err(limit_error)?;
        result.set("capturedRegistryJson", s(encoded_registry));
        if let Some(fields) = validations.object() {
            for (k, v) in fields {
                result.set(k.clone(), v.clone());
            }
        }
        encode_bounded(budget, RESULT_LIMIT, |out| result.stringify_to(out))
    });
    let (code, pointer, message) = match encoding {
        Ok(Some(encoded)) => return Ok(encoded),
        Ok(None) => (
            "INPUT_LIMIT",
            s(""),
            s("Expanded compilation exceeds the native result limit."),
        ),
        Err(error) => (error.code, s(error.pointer), s(error.message)),
    };
    let mut runtime = result.get("runtime").clone();
    runtime.set("supported", Value::Bool(false));
    runtime.set(
        "diagnostics",
        a([o([
            ("code", s(code)),
            ("pointer", pointer),
            ("message", message),
        ])]),
    );
    runtime.set("unassessed", a([]));
    let mut refusal = o([
        (
            "static",
            o([
                ("valid", result.get("static").get("valid").clone()),
                ("diagnostics", a([])),
            ]),
        ),
        ("runtime", runtime),
        ("plan", Value::Null),
        ("compiledPlanJson", Value::Null),
        ("productRuntimeAvailable", Value::Bool(false)),
    ]);
    for (field, property) in [
        ("entryValidation", "messageJson"),
        ("deliveryValidation", "input"),
        ("callValidation", "input"),
    ] {
        let validation = validations.get(field);
        let value = if validation.truthy() {
            if field == "callValidation" {
                o([
                    ("input", validation.get(property).clone()),
                    ("prepared", Value::Null),
                ])
            } else {
                o([
                    (property, validation.get(property).clone()),
                    ("valid", Value::Bool(false)),
                ])
            }
        } else {
            validation.clone()
        };
        refusal.set(field, value);
    }
    encode_bounded(budget, RESULT_LIMIT, |out| refusal.stringify_to(out))?.ok_or_else(|| {
        Error::new(
            code,
            "",
            "Expanded compilation exceeds the native result limit.",
        )
    })
}
pub(crate) fn compile(
    authored: &Value,
    source: &Value,
    observation: &Value,
    budget: &SourceBudget,
) -> Result<String, Error> {
    let mut registry = o([]);
    for field in ["format", "version", "modules", "harnesses", "agents"] {
        registry.set(
            field,
            observation
                .get(field)
                .clone_with_budget(budget, "/registry")?,
        );
    }
    if !matches!(observation.get("hostCapabilities"), Value::Undefined) {
        registry.set(
            "hostCapabilities",
            observation
                .get("hostCapabilities")
                .clone_with_budget(budget, "/registry")?,
        );
    }
    let mut prepared = plan::prepare(authored, source, &registry, budget);
    let mut result = plan::finish(&mut prepared);
    let mut runtime = result.get("runtime").clone();
    runtime.set("source", source.clone());
    runtime.set("registryVersion", observation.get("version").clone());
    result.set("runtime", runtime);
    let document = prepared.document.as_ref().and_then(E::as_object);
    let mut fallback = o([]);
    let plan = result.get("plan");
    let graph = if !plan.is_nullish() {
        Some(plan)
    } else if result.get("static").get("valid").truthy()
        && (observation.has("startEntry") || observation.has("deliveryInput"))
        && !result
            .get("runtime")
            .get("diagnostics")
            .array()
            .is_some_and(|diagnostics| {
                diagnostics
                    .iter()
                    .any(|d| d.get("code").eq_str(GRAPH_LIMIT_CODE))
            })
    {
        let graph = match prepared.host_graph.take() {
            Some(graph) => Ok(Some(graph)),
            None => document
                .map(|doc| graph::compile_with_budget(doc, budget))
                .transpose(),
        };
        match graph {
            Ok(Some(graph)) => {
                graph.append_to(&mut fallback);
                Some(&fallback)
            }
            Err(error) if error.code == GRAPH_LIMIT_CODE => return Err(error),
            _ => None,
        }
    } else {
        None
    };
    let entry = observation.get("startEntry").get("messageJson");
    let entry = if entry.is_nullish() {
        &Value::Null
    } else {
        entry
    };
    let mut check = if let (Some(document), Some(graph)) = (document, graph) {
        if !matches!(graph.get("entry"), Value::Null) {
            host_message(entry, document, budget)?
        } else if matches!(observation.get("startEntry"), Value::Null) {
            o([
                ("valid", Value::Bool(true)),
                ("shapeValid", Value::Bool(true)),
                ("unassessed", a([])),
            ])
        } else {
            invalid()
        }
    } else {
        invalid()
    };
    if let (Some(document), Some(graph)) = (document, graph) {
        if let Some(step) = graph.get("steps").array().unwrap().iter().find(|step| {
            crate::argentic_core::native::equal(step.get("address"), graph.get("entry"))
        }) {
            if check.get("valid").truthy()
                && entry.text().is_some()
                && ["agent", "approval", "compose"]
                    .iter()
                    .any(|kind| step.get("type").eq_str(kind))
            {
                let parts = step
                    .get("address")
                    .array()
                    .unwrap()
                    .iter()
                    .map(|v| v.text().unwrap().utf8().unwrap())
                    .collect();
                let declaration = step.get("declarationJson").text().unwrap();
                let output = entry.text().unwrap();
                budget
                    .bytes(
                        declaration.units().len() * 8 + output.units().len() * 4,
                        "/flow",
                    )
                    .map_err(limit_error)?;
                let raw = declaration.utf8().unwrap();
                let output = output.utf8().unwrap();
                budget
                    .json_input(raw.as_bytes(), "/flow")
                    .map_err(limit_error)?;
                budget
                    .json_input(output.as_bytes(), "/message")
                    .map_err(limit_error)?;
                let target = RouteTarget::Step {
                    address: parts,
                    step_json: raw.clone(),
                    declaration_json: raw,
                    message_json: None,
                    each_path_json: None,
                    concurrency_json: None,
                    max_json: None,
                };
                let source = RouteSource {
                    kind: SourceKind::Entry,
                    terminal_id: "".into(),
                    producer_step: None,
                    producer_agent: None,
                    output_json: output,
                    provenance: Provenance {
                        instructions: vec![],
                        results: None,
                        prompt: None,
                    },
                };
                if messages::resolve_route_message_in_document(
                    document,
                    &target,
                    &source,
                    &ItemContext::Absent,
                    &mut CapturedContent::default(),
                )
                .is_err()
                {
                    check = invalid();
                }
            }
        }
    }
    let input = observation.get("deliveryInput");
    let delivery = if input.object().is_some() {
        input
    } else {
        &Value::Undefined
    };
    let delivery_check = if let (Some(document), Some(graph)) = (document, graph) {
        // The live host authenticates a held target before accepting a message
        // into a flow. The compiler checks the message against its document.
        let valid = (matches!(graph.get("entry"), Value::Null)
            || document.get("contract").and_then(|value| value.as_str())
                == Some(crate::argentic_core::AGSDL_040))
            && delivery
                .get("agentName")
                .text()
                .and_then(Text::utf8)
                .is_some_and(|name| {
                    document
                        .get("agents")
                        .and_then(E::as_object)
                        .is_some_and(|a| a.contains_key(&name))
                });
        if valid {
            host_message(delivery.get("messageJson"), document, budget)?
        } else {
            invalid()
        }
    } else {
        invalid()
    };
    let mut validations = o([]);
    if observation.has("startEntry") {
        let mut validation = o([("messageJson", entry.clone_with_budget(budget, "/message")?)]);
        for (k, v) in check.object().unwrap() {
            validation.set(k.clone(), v.clone());
        }
        validations.set("entryValidation", validation);
    }
    if observation.has("deliveryInput") {
        let mut validation = o([("input", input.clone_with_budget(budget, "/message")?)]);
        for (k, v) in delivery_check.object().unwrap() {
            validation.set(k.clone(), v.clone());
        }
        validations.set("deliveryValidation", validation);
    }
    if observation.has("callInput") {
        validations.set(
            "callValidation",
            o([
                (
                    "input",
                    observation
                        .get("callInput")
                        .clone_with_budget(budget, "/registry/callInput")?,
                ),
                ("prepared", Value::Null),
            ]),
        );
    }
    encode(result, &registry, &validations, budget)
}
