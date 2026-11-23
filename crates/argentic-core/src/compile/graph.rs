//! Static compilation, not scheduling. Port of compilerGraph030.ts using the
//! integrated graph analysis and route/contract APIs.
use crate::argentic_core::{
    contract,
    exact::{self, Object, Value as E},
    graph::{
        analyse_joins,
        ordered::{OrderedMap, OrderedSet},
        step_routes, ExpansionBudget, Graph, JoinInfo,
    },
    native::{a, fail, o, s, Error, Text, Value},
    route::{self, NormalizedTarget},
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
};

type Result<T> = std::result::Result<T, Error>;
pub(crate) struct CompiledGraph {
    pub(crate) entry: Value,
    pub(crate) scopes: Vec<Value>,
    pub(crate) steps: Vec<Value>,
    pub(crate) routes: Vec<Value>,
    pub(crate) joins: Vec<Value>,
    pub(crate) handlers: Vec<Value>,
    pub(crate) graphless: Value,
}
impl CompiledGraph {
    pub(crate) fn has_shared_stop(&self) -> bool {
        let mut targets = std::collections::BTreeMap::new();
        for join in &self.joins {
            let origin = join.get("origin").stringify();
            let stops = join.get("remaining").eq_str("stop");
            for branch in join.get("branches").array().unwrap() {
                let target = (origin.clone(), branch.get("target").stringify());
                if targets
                    .insert(target, stops)
                    .is_some_and(|previous| previous || stops)
                {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn value(self) -> Value {
        o([
            ("entry", self.entry),
            ("scopes", a(self.scopes)),
            ("steps", a(self.steps)),
            ("routes", a(self.routes)),
            ("joins", a(self.joins)),
            ("handlers", a(self.handlers)),
            ("graphlessSubmissionContracts", self.graphless),
        ])
    }
}
fn addr(prefix: &[String], name: &str) -> Vec<String> {
    let mut parts = prefix.to_vec();
    parts.push(name.to_owned());
    parts
}
fn address(parts: &[String]) -> Value {
    a(parts.iter().cloned().map(s))
}
fn optional(v: Option<&E>) -> Result<&Object> {
    match v {
        None | Some(E::Null) => Ok(crate::argentic_core::graph::empty_object()),
        v => Ok(exact::record(v, "")?),
    }
}
enum JsonWriter {
    Count { bytes: usize, units: usize },
    Text(Vec<u16>),
}

impl fmt::Write for JsonWriter {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        match self {
            Self::Count { bytes, units } => {
                *bytes += text.len();
                *units += if text.is_ascii() {
                    text.len()
                } else {
                    text.encode_utf16().count()
                };
            }
            Self::Text(output) => output.extend(text.encode_utf16()),
        }
        Ok(())
    }
}

fn budgeted_json(
    budget: &mut ExpansionBudget,
    composition: Option<&str>,
    write: impl Fn(&mut JsonWriter) -> fmt::Result,
) -> Result<Value> {
    // Reserve the serialized byte length, then write directly into the plan's
    // native UTF-16 representation.
    let mut output = JsonWriter::Count { bytes: 0, units: 0 };
    write(&mut output).expect("counting writer");
    let JsonWriter::Count { bytes, units } = output else {
        unreachable!()
    };
    budget.json(bytes, composition)?;
    let mut output = JsonWriter::Text(Vec::with_capacity(units));
    write(&mut output).expect("text writer");
    let JsonWriter::Text(text) = output else {
        unreachable!()
    };
    Ok(s(Text::from_units(text)))
}

fn encoded_json(
    value: Option<&E>,
    budget: &mut ExpansionBudget,
    composition: Option<&str>,
) -> Result<Value> {
    match value {
        Some(value) => budgeted_json(budget, composition, |out| {
            exact::values::stringify_to(out, value)
        }),
        None => Ok(Value::Null),
    }
}
/// The document schema restricts names to ASCII letters/digits/_/-. Expanded
/// addresses add '/'. These are the en collation primary ranks on that alphabet;
/// case is a tertiary tie-breaker, lowercase first. Tested independently against
/// JavaScript's English collation; this is not a general Unicode implementation.
pub(crate) fn compare_addresses_en(left: &str, right: &str) -> std::cmp::Ordering {
    fn rank(c: u8) -> u8 {
        match c {
            b'_' => 0,
            b'-' => 1,
            b'/' => 2,
            b'0'..=b'9' => 3 + c - b'0',
            b'a'..=b'z' => 20 + c - b'a',
            b'A'..=b'Z' => 20 + c - b'A',
            _ => unreachable!("statically validated address alphabet"),
        }
    }
    let primary = left.bytes().map(rank).cmp(right.bytes().map(rank));
    if primary != std::cmp::Ordering::Equal {
        return primary;
    }
    left.bytes()
        .map(|c| u8::from(c.is_ascii_uppercase()))
        .cmp(right.bytes().map(|c| u8::from(c.is_ascii_uppercase())))
}
struct Context<'a> {
    container: &'a Object,
    prefix: Vec<String>,
    composition: Option<String>,
    bindings: Vec<(String, String)>,
    stack: Vec<String>,
    scope_key: String,
    outputs: Vec<String>,
    steps: HashMap<&'a str, &'a E>,
}
fn resolve_agent(
    value: Option<&E>,
    bindings: &[(String, String)],
    agents: &Object,
) -> Result<String> {
    let name = if let Some(E::String(s)) = value {
        Some(s.clone())
    } else {
        let record = exact::record(value, "")?;
        let parameter = exact::text(record.get("parameter"), "")?;
        bindings
            .iter()
            .find(|(k, _)| k == parameter)
            .map(|(_, v)| v.clone())
    };
    match name {
        Some(name) if !name.is_empty() && agents.contains_key(&name) => Ok(name),
        _ => fail(
            "REFERENCE",
            "",
            "Composition parameter has no agent definition.",
        ),
    }
}
struct CompiledRoute<'a> {
    scope: usize,
    source: String,
    kind: &'static str,
    choice: Option<&'a str>,
    targets: Vec<NormalizedTarget<'a, ()>>,
    value: Value,
}

fn target_value(
    target: &NormalizedTarget<'_, ()>,
    context: &Context<'_>,
    budget: &mut ExpansionBudget,
) -> Result<Value> {
    let composition = context.composition.as_deref();
    Ok(match target {
        NormalizedTarget::Output { name } => o([
            ("kind", s("output")),
            ("scope", address(&context.prefix)),
            ("name", s(*name)),
        ]),
        NormalizedTarget::Step {
            address: parts,
            message,
            each,
            concurrency,
            max,
            ..
        } => o([
            ("kind", s("step")),
            ("address", address(parts)),
            ("messageJson", encoded_json(*message, budget, composition)?),
            ("eachPathJson", encoded_json(*each, budget, composition)?),
            (
                "concurrencyJson",
                encoded_json(*concurrency, budget, composition)?,
            ),
            ("maxJson", encoded_json(*max, budget, composition)?),
        ]),
    })
}

fn add_route<'a>(
    output: (&mut Vec<CompiledRoute<'a>>, &mut ExpansionBudget),
    scope: (usize, &Context<'a>),
    step: &[String],
    kind: &'static str,
    choice: Option<&'a str>,
    raw: &'a E,
    inherited: bool,
) -> Result<()> {
    let (routes, budget) = output;
    let (index, context) = scope;
    let composition = context.composition.as_deref();
    budget.route(raw, composition)?;
    let declaration = encoded_json(Some(raw), budget, composition)?;
    let targets =
        route::normalize_targets(raw, &context.prefix, Some(&context.outputs), |address| {
            let definition = context
                .steps
                .get(address.last().unwrap().as_str())
                .ok_or_else(|| {
                    crate::argentic_core::error::Failure::new(
                        "REFERENCE",
                        "",
                        "Unknown step in route scope.",
                    )
                })?;
            let step = exact::record(Some(definition), "")?;
            Ok((
                (),
                route::receives_messages(exact::text(step.get("type"), "")?),
            ))
        })?;
    let target_values = targets
        .iter()
        .map(|t| target_value(t, context, budget))
        .collect::<Result<Vec<_>>>()?;
    routes.push(CompiledRoute {
        scope: index,
        source: step.join("/"),
        kind,
        choice,
        targets,
        value: o([
            ("source", address(step)),
            ("kind", s(kind)),
            ("key", choice.map(s).unwrap_or(Value::Null)),
            ("declarationJson", declaration),
            ("targets", a(target_values)),
            ("inherited", Value::Bool(inherited)),
        ]),
    });
    Ok(())
}

pub(crate) fn compile(document: &Object) -> Result<CompiledGraph> {
    let agents = exact::record(document.get("agents"), "")?;
    let compositions = optional(document.get("compositions"))?;
    let mut scopes = vec![];
    let mut budget = ExpansionBudget::default();
    let mut local_budget = ExpansionBudget::default();
    let mut steps = vec![];
    let mut step_contexts = vec![];

    let mut routes = vec![];
    let mut joins = vec![];
    let mut handlers = vec![];
    let mut cache: OrderedMap<Option<String>, OrderedMap<String, JoinInfo>> = OrderedMap::new();
    let mut covered = OrderedSet::new();
    if document.contains_key("flow") {
        let graph = Graph::for_scope(document, None, None, &mut ExpansionBudget::default())?;
        let (initial, _) = analyse_joins(&graph, &mut ExpansionBudget::default())?;
        let members = initial
            .values()
            .flat_map(|i| i.branches.values().flat_map(|b| b.iter().cloned()))
            .collect();
        covered = Graph::flow_error_covered(document, &members)?;
    }
    enum Task<'a> {
        Visit(
            &'a Object,
            Vec<String>,
            Option<Vec<String>>,
            Option<String>,
            Vec<(String, String)>,
            Vec<String>,
        ),
        Step(usize, &'a str, &'a E),
    }
    let mut tasks = vec![];
    let mut contexts = Vec::<Context<'_>>::new();
    let empty_route = E::Array(vec![]);
    if document.contains_key("flow") {
        let flow = exact::record(document.get("flow"), "")?;
        budget.scope(exact::record(flow.get("steps"), "")?.len(), 0, None)?;
        tasks.push(Task::Visit(flow, vec![], None, None, vec![], vec![]));
    }
    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit(container, prefix, parent, composition, bindings, stack) => {
                let declarations = exact::record(container.get("steps"), "")?;
                let outputs = match container.get("outputs") {
                    None | Some(E::Null) => vec![],
                    v => exact::list(v, "")?
                        .iter()
                        .map(|v| exact::text(Some(v), "").map(str::to_owned))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                };
                let mut binding_value = o([]);
                for (k, v) in &bindings {
                    binding_value.set(k.as_str(), s(v.clone()));
                }
                scopes.push(o([
                    ("address", address(&prefix)),
                    (
                        "parent",
                        parent.as_ref().map(|p| address(p)).unwrap_or(Value::Null),
                    ),
                    (
                        "composition",
                        composition
                            .as_ref()
                            .map(|v| s(v.clone()))
                            .unwrap_or(Value::Null),
                    ),
                    (
                        "entry",
                        address(&addr(&prefix, exact::text(container.get("entry"), "")?)),
                    ),
                    (
                        "outputs",
                        a(outputs.iter().map(|output| s(output.as_str()))),
                    ),
                    ("agents", binding_value),
                ]));
                if !cache.contains_key(&composition) {
                    let graph = Graph::for_scope(
                        document,
                        composition.as_deref(),
                        composition.is_none().then(|| covered.clone()),
                        &mut local_budget,
                    )?;
                    let (infos, problems) = analyse_joins(&graph, &mut local_budget)?;
                    if !problems.is_empty() {
                        return fail("JOIN", "", "Validated graph disagrees with join analysis.");
                    }
                    cache.set(composition.clone(), infos);
                }
                let ctx = contexts.len();
                contexts.push(Context {
                    container,
                    scope_key: prefix.join("/"),
                    prefix,
                    composition: composition.clone(),
                    bindings,
                    stack,
                    outputs,
                    steps: declarations.iter().collect(),
                });
                tasks.extend(
                    declarations
                        .iter()
                        .rev()
                        .map(|(name, decl)| Task::Step(ctx, name, decl)),
                );
            }
            Task::Step(index, name, value) => {
                let context = &contexts[index];
                let composition = context.composition.as_deref();
                let decl = exact::record(Some(value), "")?;
                let step = addr(&context.prefix, name);
                let kind = exact::text(decl.get("type"), "")?;
                let agent = if kind == "agent" {
                    s(resolve_agent(decl.get("agent"), &context.bindings, agents)?)
                } else {
                    Value::Null
                };
                let instance = if kind != "agent" {
                    Value::Null
                } else {
                    match decl.get("instance") {
                        None => o([("kind", s("shared"))]),
                        Some(E::String(v)) if v == "new" => o([("kind", s("new"))]),
                        v => o([("kind", s("named")), ("name", s(exact::text(v, "")?))]),
                    }
                };
                let delivery = if kind != "agent" || instance.get("kind").eq_str("new") {
                    Value::Null
                } else {
                    s(
                        if decl.get("delivery").and_then(E::as_str) == Some("steer") {
                            "steer"
                        } else {
                            "queue"
                        },
                    )
                };
                let first_route = routes.len();
                steps.push(o([
                    ("address", address(&step)),
                    ("scope", address(&context.prefix)),
                    ("type", s(kind)),
                    (
                        "declarationJson",
                        encoded_json(Some(value), &mut budget, composition)?,
                    ),
                    ("agent", agent),
                    ("instance", instance),
                    ("delivery", delivery),
                    (
                        "maxVisitsJson",
                        encoded_json(decl.get("maxVisits"), &mut budget, composition)?,
                    ),
                    (
                        "timeoutMsJson",
                        encoded_json(decl.get("timeoutMs"), &mut budget, composition)?,
                    ),
                    ("submissionContractJson", Value::Null),
                ]));
                for route in step_routes(decl, &[]) {
                    add_route(
                        (&mut routes, &mut budget),
                        (index, context),
                        &step,
                        route.kind.as_str(),
                        route.key,
                        route.route,
                        false,
                    )?;
                }
                if !decl.contains_key("next") {
                    add_route(
                        (&mut routes, &mut budget),
                        (index, context),
                        &step,
                        "normal",
                        None,
                        &empty_route,
                        false,
                    )?;
                }
                let flow = context.composition.is_none() && covered.contains(name);
                if flow {
                    add_route(
                        (&mut routes, &mut budget),
                        (index, context),
                        &step,
                        "error",
                        None,
                        context.container.get("onError").unwrap(),
                        true,
                    )?;
                }
                step_contexts.push((index, first_route..routes.len()));
                let mut containing = vec![];
                let scope_joins = cache
                    .get(&context.composition)
                    .expect("compiled scope joins");
                for (join, info) in scope_joins.iter() {
                    for (start, members) in info.branches.iter() {
                        if members.contains(name) {
                            budget.join_analysis(1, composition)?;
                            containing.push((
                                addr(&context.prefix, join),
                                info.names.get(start).unwrap().clone(),
                                members.len(),
                            ));
                        }
                    }
                }
                containing.sort_by(|(a, _, n), (b, _, m)| {
                    n.cmp(m)
                        .then_with(|| compare_addresses_en(&a.join("/"), &b.join("/")))
                });
                handlers.push(o([
                    ("address", address(&step)),
                    ("scope", address(&context.prefix)),
                    ("local", Value::Bool(decl.contains_key("onError"))),
                    ("flow", Value::Bool(flow)),
                    (
                        "joins",
                        a(containing.into_iter().map(|(join, branch, _)| {
                            o([
                                ("join", address(&join)),
                                ("branch", branch.map(s).unwrap_or(Value::Undefined)),
                            ])
                        })),
                    ),
                ]));
                if kind == "join" {
                    let Some(info) = scope_joins.get(name) else {
                        return fail("JOIN", "", "Validated join has no origin.");
                    };
                    let quorum = if decl.contains_key("quorum") {
                        Some(exact::record(decl.get("quorum"), "")?)
                    } else {
                        None
                    };
                    let mut branches = Vec::with_capacity(info.branches.len());
                    for (target, members) in info.branches.iter() {
                        budget.join_analysis(members.len() + 1, composition)?;
                        branches.push(o([
                            (
                                "name",
                                info.names
                                    .get(target)
                                    .unwrap()
                                    .as_ref()
                                    .map(|n| s(n.clone()))
                                    .unwrap_or(Value::Undefined),
                            ),
                            ("target", address(&addr(&context.prefix, target))),
                            (
                                "members",
                                a(members.iter().map(|m| address(&addr(&context.prefix, m)))),
                            ),
                            ("fanout", Value::Bool(info.fanout.contains(target))),
                        ]));
                    }
                    joins.push(o([
                        ("address", address(&step)),
                        ("scope", address(&context.prefix)),
                        ("origin", address(&addr(&context.prefix, &info.origin))),
                        ("branches", a(branches)),
                        ("mode", s(if quorum.is_some() { "quorum" } else { "all" })),
                        (
                            "failures",
                            s(
                                if decl.get("failures").and_then(E::as_str) == Some("keep") {
                                    "keep"
                                } else {
                                    "fail"
                                },
                            ),
                        ),
                        (
                            "maxFailuresJson",
                            encoded_json(decl.get("maxFailures"), &mut budget, composition)?,
                        ),
                        (
                            "quorumCountJson",
                            encoded_json(
                                quorum.and_then(|q| q.get("count")),
                                &mut budget,
                                composition,
                            )?,
                        ),
                        (
                            "accept",
                            if let Some(q) = quorum {
                                a(exact::list(q.get("accept"), "")?
                                    .iter()
                                    .map(|v| exact::text(Some(v), "").map(|s| s.to_owned()))
                                    .collect::<std::result::Result<Vec<_>, _>>()?
                                    .into_iter()
                                    .map(s))
                            } else {
                                a([])
                            },
                        ),
                        (
                            "remaining",
                            s(match decl.get("remaining").and_then(E::as_str) {
                                Some("finish") => "finish",
                                Some("wait") => "wait",
                                _ => "stop",
                            }),
                        ),
                    ]));
                }
                if kind == "compose" {
                    let target = exact::text(decl.get("composition"), "")?;
                    if context.stack.iter().any(|v| v == target) {
                        return fail("COMPOSITION", "", "Composition cycle reached the compiler.");
                    }
                    let composition = exact::record(compositions.get(target), "")?;
                    budget.scope(
                        exact::record(composition.get("steps"), "")?.len(),
                        context.stack.len() + 1,
                        Some(target),
                    )?;
                    let child_bindings = optional(decl.get("agents"))?
                        .iter()
                        .map(|(parameter, v)| {
                            resolve_agent(Some(v), &context.bindings, agents)
                                .map(|name| (parameter.to_owned(), name))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut stack = context.stack.clone();
                    stack.push(target.to_owned());
                    tasks.push(Task::Visit(
                        composition,
                        step,
                        Some(context.prefix.clone()),
                        Some(target.to_owned()),
                        child_bindings,
                        stack,
                    ));
                }
            }
        }
    }
    let outward: HashMap<_, _> = routes
        .iter()
        .filter(|route| route.kind == "normal")
        .map(|route| ((route.source.as_str(), route.choice), route))
        .collect();
    let mut expectation_budget = ExpansionBudget::default();
    let mut expectation =
        |instructs, route: &CompiledRoute<'_>, composition: Option<&str>| -> Result<Value> {
            expectation_budget.edges(route.targets.len(), composition)?;
            let mut todo: Vec<_> = route
                .targets
                .iter()
                .rev()
                .map(|target| (target, route.scope))
                .collect();
            let mut targets = Vec::new();
            let mut seen = HashSet::new();
            while let Some((target, scope)) = todo.pop() {
                match target {
                    NormalizedTarget::Step {
                        address,
                        receives,
                        message,
                        each,
                        ..
                    } => {
                        if !seen.insert(address) {
                            return fail("DUPLICATE", "", "Repeated route target.");
                        }
                        if *receives {
                            targets.push((*message, *each));
                        }
                    }
                    NormalizedTarget::Output { name } => {
                        let key = (contexts[scope].scope_key.as_str(), Some(*name));
                        let Some(route) = outward.get(&key) else {
                            return fail(
                                "COMPOSITION",
                                "",
                                "Composition output has no enclosing route.",
                            );
                        };
                        expectation_budget.edges(route.targets.len(), composition)?;
                        todo.extend(
                            route
                                .targets
                                .iter()
                                .rev()
                                .map(|target| (target, route.scope)),
                        );
                    }
                }
            }
            Ok(s(route::instruction_kind(instructs, targets)?))
        };
    let agent_definitions: HashMap<_, _> = agents.iter().collect();
    for (step, (index, range)) in steps.iter_mut().zip(step_contexts) {
        let Some(agent) = step.get("agent").text().and_then(|name| name.utf8()) else {
            continue;
        };
        let composition = contexts[index].composition.as_deref();
        let agent = agent_definitions[agent.as_str()];
        let declaration = exact::record(Some(agent), "")?;
        let instructs = matches!(declaration.get("instructs"), Some(E::Bool(true)));
        let normal: Vec<_> = routes[range]
            .iter()
            .filter(|r| r.kind == "normal")
            .collect();
        let choices = if normal.first().is_some_and(|r| r.choice.is_none()) {
            Value::Null
        } else {
            a(normal.iter().map(|r| r.value.get("key").clone()))
        };
        let mut instructions = o([]);
        if !matches!(choices, Value::Null) {
            for route in &normal {
                instructions.set(
                    route.choice.unwrap(),
                    expectation(instructs, route, composition)?,
                );
            }
        }
        let default = if matches!(choices, Value::Null) {
            expectation(instructs, normal[0], composition)?
        } else {
            s("none")
        };
        let input = o([
            ("choices", choices),
            ("instructionsByChoice", instructions),
            ("defaultInstructions", default),
            ("timeoutMs", step.get("timeoutMsJson").clone()),
        ]);
        let agent_json = encoded_json(Some(agent), &mut budget, composition)?;
        let contract = contract::create_with_agent(&input, declaration, agent_json)?;
        step.set(
            "submissionContractJson",
            budgeted_json(&mut budget, composition, |out| contract.stringify_to(out))?,
        );
    }
    drop(outward);
    let mut graphless = o([]);
    for (name, agent) in agents.iter() {
        let declaration = exact::record(Some(agent), "")?;
        let agent_json = encoded_json(Some(agent), &mut budget, None)?;
        let contract = contract::create_with_agent(&o([]), declaration, agent_json)?;
        graphless.set(
            name,
            budgeted_json(&mut budget, None, |out| contract.stringify_to(out))?,
        );
    }
    Ok(CompiledGraph {
        entry: scopes
            .first()
            .map(|s| s.get("entry").clone())
            .unwrap_or(Value::Null),
        scopes,
        steps,
        routes: routes.into_iter().map(|route| route.value).collect(),
        joins,
        handlers,
        graphless,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn repeated_composition(copies: usize, payload: serde_json::Value) -> E {
        let steps = (0..copies)
            .map(|i| {
                let next = if i + 1 == copies {
                    json!([])
                } else {
                    json!(format!("review{}", i + 1))
                };
                (
                    format!("review{i}"),
                    json!({"type":"compose","composition":"review","next":{"done":next}}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let source = json!({
            "contract":"agsdl-0.4.0",
            "id":"repeated",
            "agents":{},
            "flow":{"entry":"review0","steps":steps},
            "compositions":{"review":{"entry":"ask","outputs":["done"],"steps":{"ask":payload}}}
        });
        exact::decode(&source.to_string()).unwrap()
    }

    #[test]
    fn compiled_json_budget_counts_repeated_bodies_and_route_payloads() {
        const KIB: usize = 1024;
        let size = 128 * KIB;
        let output = json!({"output":"done"});

        // The authored step is encoded once per compiled instance. The
        // previous compiler kept three full encodings of this declaration.
        let steps_only = repeated_composition(
            96,
            json!({
                "type":"approval","title":"x".repeat(size),
                "next":{"yes":output,"no":[]}
            }),
        );
        let graph = compile(steps_only.as_object().unwrap()).unwrap();
        assert_eq!(graph.steps.len(), 192);
        assert_eq!(graph.routes.len(), 288);
        drop(graph);

        // A message survives in the step declaration, route declaration and
        // normalized target. All three count toward the compilation budget.
        let with_message = repeated_composition(
            96,
            json!({
                "type":"approval","next":{"yes":{
                    "step":"ask","message":{"prompt":[{"text":"x".repeat(size)}]}
                },"no":output}
            }),
        );
        let error = compile(with_message.as_object().unwrap()).err().unwrap();
        assert_eq!(error.code, crate::graph::GRAPH_LIMIT_CODE);
        assert!(error.pointer.eq_str("/compositions/review"));
        assert!(error.message.lossy().contains("compiled graph JSON bytes"));
    }

    #[test]
    fn escaped_json_is_reserved_before_serialization() {
        let value = E::String("\u{0001}é𝄞".into());
        let size = exact::encode(&value).len();
        let mut budget = ExpansionBudget::default();
        budget
            .json(crate::graph::MAX_COMPILED_GRAPH_JSON_BYTES - size + 1, None)
            .unwrap();
        let error = encoded_json(Some(&value), &mut budget, None).unwrap_err();
        assert_eq!(error.code, crate::graph::GRAPH_LIMIT_CODE);
        budget.json(size - 1, None).unwrap();
    }
}
