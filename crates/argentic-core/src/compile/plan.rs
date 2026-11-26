//! compiler030.ts, with an owned pre-digest capture and a separate pure finish.
use super::{configuration as c, encode, graph, limit_error, metadata as m};
use crate::argentic_core::{
    exact::{self, Value as E},
    native::{self, a, o, s, Error, Text, Value},
    validate::{validate_document_with_budget, ModuleMap, Report},
    SourceBudget, GRAPH_LIMIT_CODE, MAX_COMPILED_GRAPH_JSON_BYTES,
};
use sha2::{Digest, Sha256};
pub(crate) const PROFILE: &str = "macro-agsdl-030-v1";
pub(crate) const COMPILER: &str = "macro-agsdl-compiler/030";
pub(crate) const RUNTIME: &str = "macro-agsdl-runtime/030";
pub(crate) struct Prepared {
    assessment: Value,
    runtime: Value,
    pub(crate) document: Option<E>,
    pub(crate) host_graph: Option<graph::CompiledGraph>,
    budget: SourceBudget,
    modules: ModuleMap,
    registry: Option<Value>,
    ranges: std::collections::HashMap<String, std::collections::HashSet<String>>,
    source: Option<Value>,
    bytes: Vec<u8>,
    ready: bool,
}
fn diagnostic(code: &str, pointer: Value, message: Value, severity: bool) -> Value {
    let mut d = o([
        ("code", s(code)),
        ("pointer", pointer),
        ("message", message),
    ]);
    if severity {
        d.set("severity", s("error"));
    }
    d
}
fn report(report: Report) -> Value {
    o([
        ("valid", Value::Bool(report.valid())),
        (
            "diagnostics",
            a(report.diagnostics.into_iter().map(|d| {
                let mut out = o([
                    ("code", s(d.code)),
                    ("pointer", s(d.path)),
                    ("message", s(d.message)),
                    ("severity", s(d.severity)),
                ]);
                if let Some(step) = d.step.filter(|s| !s.is_empty()) {
                    out.set("step", a(step.into_iter().map(s)));
                }
                out
            })),
        ),
    ])
}
fn internal<T>(_: T) -> Error {
    Error::new(
        "COMPILATION",
        "",
        "Invalid compilation input or inconsistent dependency contract.",
    )
}
impl Prepared {
    fn catch(&mut self, error: Error) {
        let mut d = diagnostic(error.code, s(error.pointer), s(error.message), true);
        if self
            .assessment
            .get("diagnostics")
            .array()
            .unwrap()
            .is_empty()
            && !self.assessment.get("valid").truthy()
        {
            if error.code != GRAPH_LIMIT_CODE {
                d.set("code", s("PARSE"));
            }
            self.assessment = o([("valid", Value::Bool(false)), ("diagnostics", a([d]))]);
        } else {
            self.runtime.set("supported", Value::Bool(false));
            self.runtime.set("diagnostics", a([d]));
        }
        self.ready = false;
    }
    fn unavailable(&mut self) -> Value {
        o([
            (
                "static",
                std::mem::replace(&mut self.assessment, Value::Null),
            ),
            ("runtime", std::mem::replace(&mut self.runtime, Value::Null)),
            ("plan", Value::Null),
            ("compiledPlanJson", Value::Null),
            ("productRuntimeAvailable", Value::Bool(false)),
        ])
    }
}
/// Every caller-owned input needed after the digest is copied before returning.
pub(crate) fn prepare(
    authored: &Value,
    source: &Value,
    input: &Value,
    budget: &SourceBudget,
) -> Prepared {
    let mut state = Prepared {
        assessment: o([("valid", Value::Bool(false)), ("diagnostics", a([]))]),
        runtime: o([
            ("profile", s(PROFILE)),
            ("registryVersion", s("")),
            ("source", Value::Null),
            ("supported", Value::Bool(false)),
            ("diagnostics", a([])),
            ("unassessed", a([])),
        ]),
        document: None,
        host_graph: None,
        budget: budget.clone(),
        modules: ModuleMap::new(),
        registry: None,
        ranges: Default::default(),
        source: None,
        bytes: vec![],
        ready: false,
    };
    let result = (|| -> Result<(), Error> {
        let Some(text) = authored.text() else {
            return native::fail("PARSE", "", "Authored source must be primitive text.");
        };
        state.bytes = text
            .utf8()
            .ok_or_else(|| Error::new("PARSE", "", "Invalid JSON: unpaired UTF-16 surrogate."))?
            .into_bytes();
        budget.json_input(&state.bytes, "").map_err(limit_error)?;
        state.document = Some(
            exact::scan::parse_v030(&state.bytes)
                .map_err(|error| Error::new("PARSE", "", error.message()))?
                .tree,
        );
        if state.document.as_ref().unwrap().as_object().is_none() {
            state.assessment = report(
                validate_document_with_budget(
                    state.document.as_ref().unwrap(),
                    &state.modules,
                    budget,
                )
                .map_err(internal)?,
            );
            return Ok(());
        }
        let source = m::capture_source(source)?;
        state.runtime.set("source", source.clone());
        state.source = Some(source);
        let c::CapturedRegistry {
            json: registry,
            modules,
            ranges,
        } = c::capture_registry(input, budget)?;
        state.ranges = ranges;
        state
            .runtime
            .set("registryVersion", registry.get("version").clone());
        let mut unassessed = vec![];
        for observation in registry.get("harnesses").array().unwrap() {
            if let Some(module) = observation
                .get("harness")
                .text()
                .and_then(Text::utf8)
                .and_then(|name| modules.get(&name))
            {
                if observation.get("installation").eq_str("available") {
                    let mut line = Text::from("Harness ");
                    line.push(observation.get("harness").text().unwrap());
                    line.push(&Text::from(" version "));
                    line.push(
                        observation
                            .get("version")
                            .text()
                            .unwrap_or(&Text::from("unknown")),
                    );
                    line.push(&Text::from(" against module range "));
                    line.push(&Text::from(exact::text(
                        module.as_object().unwrap().get("harnessVersions"),
                        "",
                    )?));
                    unassessed.push(s(line));
                }
            }
        }
        unassessed.push(s(
            "Native admission of resources in later graphless messages",
        ));
        state.runtime.set("unassessed", a(unassessed));
        state.assessment = report(
            validate_document_with_budget(state.document.as_ref().unwrap(), &modules, budget)
                .map_err(internal)?,
        );
        state.modules = modules;
        state.ready = state.assessment.get("valid").truthy();
        state.registry = Some(registry);
        Ok(())
    })();
    if let Err(error) = result {
        if let Some(document) = &state.document {
            if let Ok(assessment) = validate_document_with_budget(document, &state.modules, budget)
            {
                state.assessment = report(assessment);
            }
        }
        state.catch(error);
    }
    state
}
pub(crate) fn finish(state: &mut Prepared) -> Value {
    if !state.ready {
        return state.unavailable();
    }
    let result = (|| -> Result<(Value, Value, bool, String), Error> {
        let source = state.source.as_ref().unwrap();
        let digest = format!("{:x}", Sha256::digest(&state.bytes));
        if !source.get("sourceSha256").eq_str(&digest) {
            return native::fail(
                "CORRELATION",
                "/source/sourceSha256",
                "Captured SHA does not match authored bytes.",
            );
        }
        let document = state.document.as_ref().unwrap();
        let document_json = encode(&state.budget, MAX_COMPILED_GRAPH_JSON_BYTES, |out| {
            exact::values::stringify_to(out, document)
        })?
        .ok_or_else(|| {
            Error::new(
                GRAPH_LIMIT_CODE,
                "",
                "Maximum compiled document JSON bytes exceeded.",
            )
        })?;
        state
            .budget
            .bytes(document_json.len() * 2, "")
            .map_err(limit_error)?;
        let document = exact::record(Some(document), "")?;
        let registry = state.registry.as_ref().unwrap();
        let by_agent = c::step_uses(document, &state.budget)?;
        let definitions = exact::record(document.get("agents"), "")?;
        let mut agents = vec![];
        let mut warnings = vec![];
        for (name, definition) in definitions.iter() {
            let definition = exact::record(Some(definition), "")?;
            let p = exact::at("/agents", name);
            let uses = c::used_properties(
                definition,
                &p,
                by_agent.get(name).map(Vec::as_slice).unwrap_or(&[]),
            )?;
            let proof_count = registry.get("agents").array().unwrap().len();
            state
                .budget
                .work(proof_count + uses.len(), &p)
                .map_err(limit_error)?;
            let agent = c::capture_agent(
                document,
                &s(name),
                registry,
                &state.modules,
                &uses,
                &state.budget,
                &state.ranges,
            )?;
            let observation = registry
                .get("harnesses")
                .array()
                .unwrap()
                .iter()
                .find(|v| native::equal(v.get("harness"), agent.get("harness")))
                .ok_or_else(|| internal(()))?;
            let concrete: std::collections::HashSet<_> = uses
                .iter()
                .filter(|(_, _, step)| step.is_some())
                .map(|(name, p, _)| format!("{name}\0{p}"))
                .collect();
            for (name, p, step) in uses {
                state
                    .budget
                    .work(
                        observation.get("capabilities").array().unwrap().len()
                            + state.assessment.get("diagnostics").array().unwrap().len(),
                        &p,
                    )
                    .map_err(limit_error)?;
                if step.is_none() && concrete.contains(&format!("{name}\0{p}")) {
                    continue;
                }
                let capability = observation
                    .get("capabilities")
                    .array()
                    .unwrap()
                    .iter()
                    .find(|v| v.get("name").eq_str(&name));
                let reported = state
                    .assessment
                    .get("diagnostics")
                    .array()
                    .unwrap()
                    .iter()
                    .any(|d| {
                        d.get("code").eq_str("HARNESS_APPROXIMATE")
                            && d.get("severity").eq_str("warning")
                            && d.get("pointer").eq_str(&p)
                            && step.as_ref().is_none_or(|step| {
                                d.get("step").array().is_some_and(|v| {
                                    v.iter()
                                        .map(|s| s.text().unwrap().utf8().unwrap())
                                        .collect::<Vec<_>>()
                                        .join("/")
                                        == step.join("/")
                                })
                            })
                    });
                if capability.is_some_and(|v| v.get("support").eq_str("approximate")) && !reported {
                    let harness = agent.get("harness").text().unwrap().utf8().unwrap();
                    let mut d = o([
                        ("code", s("HARNESS_APPROXIMATE")),
                        ("pointer", s(p)),
                        ("severity", s("warning")),
                        (
                            "message",
                            s(format!(
                                "The observed {harness} adapter approximates {name}."
                            )),
                        ),
                    ]);
                    if let Some(step) = step {
                        d.set("step", a(step.into_iter().map(s)));
                    }
                    state.budget.bytes(256, "/agents").map_err(limit_error)?;
                    if warnings.len() >= 1024 {
                        return native::fail(
                            GRAPH_LIMIT_CODE,
                            "/agents",
                            "Maximum compilation diagnostic count exceeded.",
                        );
                    }
                    warnings.push(d);
                }
            }
            agents.push(agent);
        }
        state.host_graph = Some(graph::compile_with_budget(document, &state.budget)?);
        let graph = state.host_graph.as_ref().unwrap();
        for step in &graph.steps {
            let required = if step.get("type").eq_str("call") {
                let raw = step.get("declarationJson").text().unwrap().utf8().unwrap();
                state
                    .budget
                    .json_input(raw.as_bytes(), "/flow")
                    .map_err(limit_error)?;
                let value = exact::decode(&raw)?;
                let declaration = exact::record(Some(&value), "")?;
                Some(if declaration.contains_key("command") {
                    "call.command"
                } else {
                    "call.mcp"
                })
            } else if step.get("type").eq_str("approval") {
                Some("approval")
            } else {
                None
            };
            if let Some(required) = required {
                state
                    .budget
                    .work(
                        registry
                            .get("hostCapabilities")
                            .array()
                            .map_or(0, <[Value]>::len),
                        "/flow",
                    )
                    .map_err(limit_error)?;
                if !registry
                    .get("hostCapabilities")
                    .array()
                    .is_some_and(|caps| {
                        caps.iter().any(|v| {
                            v.get("name").eq_str(required) && v.get("support").eq_str("native")
                        })
                    })
                {
                    let scope = graph
                        .scopes
                        .iter()
                        .find(|v| native::equal(v.get("address"), step.get("scope")));
                    let owner = scope
                        .and_then(|v| v.get("composition").text())
                        .and_then(Text::utf8)
                        .map(|c| format!("/compositions/{c}"))
                        .unwrap_or("/flow".into());
                    let name = step
                        .get("address")
                        .array()
                        .unwrap()
                        .last()
                        .unwrap()
                        .text()
                        .unwrap()
                        .utf8()
                        .unwrap();
                    return native::fail(
                        "HOST_CAPABILITY",
                        format!("{owner}/steps/{name}"),
                        format!("No native host qualification for {required}."),
                    );
                }
            }
        }
        let product = (!agents.is_empty()
            || graph.steps.iter().any(|s| s.get("type").eq_str("call")))
            && agents.iter().all(|agent| {
                registry.get("harnesses").array().unwrap().iter().any(|v| {
                    native::equal(v.get("harness"), agent.get("harness"))
                        && v.get("harness").eq_str("codex")
                        && v.get("installation").eq_str("available")
                        && v.get("qualified").truthy()
                        && v.get("adapterId").eq_str("codex-app-server-v1")
                })
            });
        let versions = o([
            ("snapshot", s("2")),
            (
                "contract",
                s(exact::text(document.get("contract"), "/contract")?),
            ),
            ("compiler", s(COMPILER)),
            (
                "runtime",
                s(if graph.has_shared_stop() {
                    crate::argentic_core::flow::SHARED_STOP_RUNTIME
                } else {
                    RUNTIME
                }),
            ),
            ("registry", registry.get("version").clone()),
            ("profile", s(PROFILE)),
        ]);
        let mut plan = o([
            ("format", s("macro-agsdl-compiled-plan/2")),
            ("versions", versions),
            ("source", source.clone()),
            ("documentJson", s(document_json)),
            ("agents", a(agents)),
        ]);
        let remaining = graph.remaining_json;
        state.host_graph.take().unwrap().append_to(&mut plan);
        let encoded = encode(&state.budget, remaining, |out| plan.stringify_to(out))?
            .ok_or_else(|| Error::new(GRAPH_LIMIT_CODE, "/flow", format!("Maximum compiled graph JSON bytes exceeded ({MAX_COMPILED_GRAPH_JSON_BYTES}).")))?;
        state
            .budget
            .bytes(encoded.len() * 2, "/flow")
            .map_err(limit_error)?;
        Ok((plan, a(warnings), product, encoded))
    })();
    match result {
        Err(error) => {
            state.catch(error);
            state.unavailable()
        }
        Ok((plan, warnings, product, encoded)) => {
            state.runtime.set("supported", Value::Bool(true));
            state.runtime.set("diagnostics", warnings);
            o([
                (
                    "static",
                    std::mem::replace(&mut state.assessment, Value::Null),
                ),
                (
                    "runtime",
                    std::mem::replace(&mut state.runtime, Value::Null),
                ),
                ("plan", plan),
                ("compiledPlanJson", s(encoded)),
                ("productRuntimeAvailable", Value::Bool(product)),
            ])
        }
    }
}
