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
    native::{self, a, fail, o, s, Error, Value},
    route::{self, RouteTarget, Scope, ScopeStep},
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
fn key(v: &Value) -> String {
    v.array()
        .unwrap()
        .iter()
        .map(|v| v.text().unwrap().utf8().unwrap())
        .collect::<Vec<_>>()
        .join("/")
}
fn optional(v: Option<&E>) -> Result<&Object> {
    match v {
        None | Some(E::Null) => Ok(crate::argentic_core::graph::empty_object()),
        v => Ok(exact::record(v, "")?),
    }
}
fn number_json(decl: &Object, key: &str) -> Value {
    decl.get(key)
        .map(|v| s(exact::encode(v)))
        .unwrap_or(Value::Null)
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
    route_scope: Scope,
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
fn target_value(target: &RouteTarget, prefix: &[String]) -> Value {
    match target {
        RouteTarget::Output { name } => o([
            ("kind", s("output")),
            ("scope", address(prefix)),
            ("name", s(name.clone())),
        ]),
        RouteTarget::Step {
            address: parts,
            message_json,
            each_path_json,
            concurrency_json,
            max_json,
            ..
        } => o([
            ("kind", s("step")),
            ("address", address(parts)),
            (
                "messageJson",
                message_json
                    .as_ref()
                    .map(|v| s(v.clone()))
                    .unwrap_or(Value::Null),
            ),
            (
                "eachPathJson",
                each_path_json
                    .as_ref()
                    .map(|v| s(v.clone()))
                    .unwrap_or(Value::Null),
            ),
            (
                "concurrencyJson",
                concurrency_json
                    .as_ref()
                    .map(|v| s(v.clone()))
                    .unwrap_or(Value::Null),
            ),
            (
                "maxJson",
                max_json
                    .as_ref()
                    .map(|v| s(v.clone()))
                    .unwrap_or(Value::Null),
            ),
        ]),
    }
}
fn add_route(
    output: (&mut Vec<Value>, &mut ExpansionBudget),
    context: &Context<'_>,
    step: &[String],
    kind: &str,
    choice: Option<&str>,
    raw: &E,
    inherited: bool,
) -> Result<()> {
    let (routes, budget) = output;
    budget.route(raw, context.composition.as_deref())?;
    let declaration = exact::encode(raw);
    budget.json(declaration.len(), context.composition.as_deref())?;
    let targets = route::normalize_route(&declaration, &context.route_scope)?;
    routes.push(o([
        ("source", address(step)),
        ("kind", s(kind)),
        ("key", choice.map(s).unwrap_or(Value::Null)),
        ("declarationJson", s(declaration)),
        (
            "targets",
            a(targets.iter().map(|t| target_value(t, &context.prefix))),
        ),
        ("inherited", Value::Bool(inherited)),
    ]));
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
        let (initial, _) = analyse_joins(&graph)?;
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
                let route_scope = Scope {
                    prefix: prefix.clone(),
                    outputs: Some(outputs.clone()),
                    steps: declarations
                        .iter()
                        .map(|(name, decl)| {
                            let declaration_json = exact::encode(decl);
                            budget.json(declaration_json.len(), composition.as_deref())?;
                            Ok(ScopeStep {
                                address: addr(&prefix, name),
                                declaration_json,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    output_routes: None,
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
                    ("outputs", a(outputs.into_iter().map(s))),
                    ("agents", binding_value),
                ]));
                if !cache.contains_key(&composition) {
                    let graph = Graph::for_scope(
                        document,
                        composition.as_deref(),
                        composition.is_none().then(|| covered.clone()),
                        &mut local_budget,
                    )?;
                    let (infos, problems) = analyse_joins(&graph)?;
                    if !problems.is_empty() {
                        return fail("JOIN", "", "Validated graph disagrees with join analysis.");
                    }
                    cache.set(composition.clone(), infos);
                }
                let ctx = contexts.len();
                contexts.push(Context {
                    container,
                    prefix,
                    composition: composition.clone(),
                    bindings,
                    stack,
                    route_scope,
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
                step_contexts.push(index);
                steps.push(o([
                    ("address", address(&step)),
                    ("scope", address(&context.prefix)),
                    ("type", s(kind)),
                    ("declarationJson", s(exact::encode(value))),
                    ("agent", agent),
                    ("instance", instance),
                    ("delivery", delivery),
                    ("maxVisitsJson", number_json(decl, "maxVisits")),
                    ("timeoutMsJson", number_json(decl, "timeoutMs")),
                    ("submissionContractJson", Value::Null),
                ]));
                for route in step_routes(decl, &[]) {
                    add_route(
                        (&mut routes, &mut budget),
                        context,
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
                        context,
                        &step,
                        "normal",
                        None,
                        &E::Array(vec![]),
                        false,
                    )?;
                }
                let flow = context.composition.is_none() && covered.contains(name);
                if flow {
                    add_route(
                        (&mut routes, &mut budget),
                        context,
                        &step,
                        "error",
                        None,
                        context.container.get("onError").unwrap(),
                        true,
                    )?;
                }
                let mut containing = vec![];
                let scope_joins = cache
                    .get(&context.composition)
                    .expect("compiled scope joins");
                for (join, info) in scope_joins.iter() {
                    for (start, members) in info.branches.iter() {
                        if members.contains(name) {
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
                    let branches = info.branches.iter().map(|(target, members)| {
                        o([
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
                        ])
                    });
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
                        ("maxFailuresJson", number_json(decl, "maxFailures")),
                        (
                            "quorumCountJson",
                            quorum
                                .and_then(|q| q.get("count"))
                                .map(|v| s(exact::encode(v)))
                                .unwrap_or(Value::Null),
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
    let expectation_scope = Scope {
        prefix: vec![],
        outputs: None,
        output_routes: None,
        steps: steps
            .iter()
            .map(|step| ScopeStep {
                address: vec![key(step.get("address"))],
                declaration_json: step.get("declarationJson").text().unwrap().utf8().unwrap(),
            })
            .collect(),
    };
    let mut expectation_budget = ExpansionBudget::default();
    let mut expectation =
        |agent_json: &str, route: &Value, composition: Option<&str>| -> Result<Value> {
            let initial = route.get("targets").array().unwrap();
            expectation_budget.edges(initial.len(), composition)?;
            let mut todo: Vec<&Value> = initial.iter().rev().collect();
            let mut targets = vec![];
            while let Some(target) = todo.pop() {
                if target.get("kind").eq_str("step") {
                    targets.push(target);
                } else {
                    let outward = routes.iter().find(|r| {
                        key(r.get("source")) == key(target.get("scope"))
                            && r.get("kind").eq_str("normal")
                            && native::equal(r.get("key"), target.get("name"))
                    });
                    let Some(outward) = outward else {
                        return fail(
                            "COMPOSITION",
                            "",
                            "Composition output has no enclosing route.",
                        );
                    };
                    let targets = outward.get("targets").array().unwrap();
                    expectation_budget.edges(targets.len(), composition)?;
                    todo.extend(targets.iter().rev());
                }
            }
            let route = E::Array(
                targets
                    .into_iter()
                    .map(|target| {
                        let mut object = exact::Object::from_entries([(
                            "step".to_owned(),
                            E::String(key(target.get("address"))),
                        )]);
                        for (field, json) in [
                            ("message", "messageJson"),
                            ("each", "eachPathJson"),
                            ("concurrency", "concurrencyJson"),
                            ("max", "maxJson"),
                        ] {
                            if let Some(text) = target.get(json).text() {
                                let json = text.utf8().unwrap();
                                budget.json(json.len(), composition)?;
                                object.set(field.to_owned(), exact::decode(&json)?);
                            }
                        }
                        Ok(E::Object(object))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Ok(s(route::instructions_expectation(
                agent_json,
                &exact::encode(&route),
                std::slice::from_ref(&expectation_scope),
                Some(0),
            )?))
        };
    for (step, index) in steps.iter_mut().zip(step_contexts) {
        let composition = contexts[index].composition.as_deref();
        if matches!(step.get("agent"), Value::Null) {
            continue;
        }
        let agent_name = step.get("agent").text().unwrap().utf8().unwrap();
        let agent_json = exact::encode(agents.get(&agent_name).unwrap());
        let normal: Vec<_> = routes
            .iter()
            .filter(|r| {
                key(r.get("source")) == key(step.get("address")) && r.get("kind").eq_str("normal")
            })
            .collect();
        let choices = if normal
            .first()
            .is_some_and(|r| matches!(r.get("key"), Value::Null))
        {
            Value::Null
        } else {
            a(normal.iter().map(|r| r.get("key").clone()))
        };
        let mut instructions = o([]);
        if !matches!(choices, Value::Null) {
            for r in &normal {
                instructions.set(
                    r.get("key").text().unwrap().clone(),
                    expectation(&agent_json, r, composition)?,
                );
            }
        }
        let default = if matches!(choices, Value::Null) {
            expectation(&agent_json, normal[0], composition)?
        } else {
            s("none")
        };
        let input = o([
            ("agentJson", s(agent_json)),
            ("choices", choices),
            ("instructionsByChoice", instructions),
            ("defaultInstructions", default),
            ("timeoutMs", step.get("timeoutMsJson").clone()),
        ]);
        let contract = contract::create(&input)?;
        step.set("submissionContractJson", s(contract.stringify()));
    }
    let mut graphless = o([]);
    for (name, agent) in agents.iter() {
        let contract = contract::create(&o([("agentJson", s(exact::encode(agent)))]))?;
        graphless.set(name, s(contract.stringify()));
    }
    Ok(CompiledGraph {
        entry: scopes
            .first()
            .map(|s| s.get("entry").clone())
            .unwrap_or(Value::Null),
        scopes,
        steps,
        routes,
        joins,
        handlers,
        graphless,
    })
}
