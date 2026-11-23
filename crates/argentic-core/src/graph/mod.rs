// SPDX-License-Identifier: Apache-2.0
//! Flow graphs, dominators and join analysis. Rust translation of
//! `src/vendor/agsdl/v0.3/graph.mjs` (port of AgSDL v0.3.0
//! tooling/agsdl/graph.py, upstream commit
//! bd9f8a5b16923476fd20ac2650bd1729ee151917, modified for Macro: slash
//! addresses, insertion-ordered collections, exact quorum comparison). Provenance
//! and license: `src/vendor/agsdl/NOTICE.md`, `src/vendor/agsdl/v0.3/LICENSE`.
//!
//! Precondition: the document has the published shape of `agsdl.schema.json`
//! (every caller checks it first). A shape the JS code would trip over is
//! refused with [`Unshaped`] instead of being guessed. Sets and maps keep the
//! JS insertion order; sorting compares UTF-16 code units like JS strings.
pub(crate) mod ordered;

use super::{
    exact::{
        values::{compare_numbers, Num},
        Object, Value,
    },
    resource::{
        number_excerpt, SourceBudget, MAX_DIAGNOSTICS, MAX_DIAGNOSTIC_BYTES, MAX_SOURCE_BYTES,
        MAX_SOURCE_WORK,
    },
    valueschema::{path_pointer, PointerPart},
};
use ordered::{OrderedMap, OrderedSet};
use std::{
    cell::OnceCell,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::OnceLock,
};

/// A document outside the shape the graph code assumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Unshaped {
    pub(crate) message: String,
    pub(crate) limit_pointer: Option<String>,
}

pub(crate) type GraphResult<T> = Result<T, Unshaped>;

fn unshaped<T>(message: impl Into<String>) -> GraphResult<T> {
    Err(Unshaped {
        message: message.into(),
        limit_pointer: None,
    })
}

/// Step instances (including compose calls and synthetic exits) in expanded graphs.
pub const MAX_EXPANDED_GRAPH_NODES: usize = 8192;
/// Route targets processed during expansion, including intermediate outputs.
pub const MAX_EXPANDED_GRAPH_EDGES: usize = 32_768;
/// Composition bodies on one expansion path. The enclosing flow has depth zero.
pub const MAX_COMPOSITION_DEPTH: usize = 32;
/// Serialized step, route, message and submission contract data materialized during compilation.
pub const MAX_COMPILED_GRAPH_JSON_BYTES: usize = 32 * 1024 * 1024;
/// Stable diagnostic code for an expansion budget refusal.
pub const GRAPH_LIMIT_CODE: &str = "GRAPH_LIMIT";

const MAX_JOIN_ANALYSIS_ITEMS: usize = 131_072;

#[derive(Default)]
pub(crate) struct ExpansionBudget {
    source: SourceBudget,
    nodes: usize,
    edges: usize,
    json_bytes: usize,
    join_items: usize,
    diagnostic_bytes: usize,
    diagnostics: usize,
}

impl ExpansionBudget {
    pub(crate) fn new(source: &SourceBudget) -> Self {
        Self {
            source: source.clone(),
            ..Self::default()
        }
    }

    pub(crate) fn work(&self, count: usize, composition: Option<&str>) -> GraphResult<()> {
        graph_work(&self.source, count, composition)
    }

    pub(crate) fn bytes(&self, count: usize, composition: Option<&str>) -> GraphResult<()> {
        graph_bytes(&self.source, count, composition)
    }

    pub(crate) fn scope(
        &mut self,
        count: usize,
        depth: usize,
        composition: Option<&str>,
    ) -> GraphResult<()> {
        if depth > MAX_COMPOSITION_DEPTH {
            return Err(expansion_limit(
                composition,
                "composition depth",
                MAX_COMPOSITION_DEPTH,
            ));
        }
        self.nodes(count, composition)
    }

    fn nodes(&mut self, count: usize, composition: Option<&str>) -> GraphResult<()> {
        self.work(count, composition)?;
        self.bytes(count.saturating_mul(288), composition)?;
        spend(
            &mut self.nodes,
            count,
            MAX_EXPANDED_GRAPH_NODES,
            composition,
            "expanded graph nodes",
        )
    }

    pub(crate) fn edges(&mut self, count: usize, composition: Option<&str>) -> GraphResult<()> {
        self.work(count, composition)?;
        self.bytes(count.saturating_mul(152), composition)?;
        spend(
            &mut self.edges,
            count,
            MAX_EXPANDED_GRAPH_EDGES,
            composition,
            "expanded graph route targets",
        )
    }

    pub(crate) fn json(&mut self, bytes: usize, composition: Option<&str>) -> GraphResult<()> {
        self.bytes(bytes.saturating_mul(2), composition)?;
        spend(
            &mut self.json_bytes,
            bytes,
            MAX_COMPILED_GRAPH_JSON_BYTES,
            composition,
            "compiled graph JSON bytes",
        )
    }

    pub(crate) fn remaining_json(&self) -> usize {
        MAX_COMPILED_GRAPH_JSON_BYTES - self.json_bytes
    }

    pub(crate) fn json_limit(&self, composition: Option<&str>) -> Unshaped {
        expansion_limit(
            composition,
            "compiled graph JSON bytes",
            MAX_COMPILED_GRAPH_JSON_BYTES,
        )
    }

    pub(crate) fn join_analysis(
        &mut self,
        count: usize,
        composition: Option<&str>,
    ) -> GraphResult<()> {
        // Branch names, memberships and diagnostics can outnumber nodes and
        // edges, even though each individual reachability scan is linear space.
        self.work(count, composition)?;
        self.bytes(count.saturating_mul(128), composition)?;
        spend(
            &mut self.join_items,
            count,
            MAX_JOIN_ANALYSIS_ITEMS,
            composition,
            "join analysis items",
        )
    }

    fn diagnostic(&mut self, problem: &GraphProblem, composition: Option<&str>) -> GraphResult<()> {
        let pointer = path_pointer(&problem.pointer);
        let bytes = pointer.len() + problem.addr.len() + problem.message.len() + 128;
        if self.diagnostics >= MAX_DIAGNOSTICS
            || bytes > MAX_DIAGNOSTIC_BYTES - self.diagnostic_bytes
        {
            return Err(Unshaped {
                message: "Maximum join diagnostic count or bytes exceeded.".into(),
                limit_pointer: Some(pointer),
            });
        }
        self.bytes(bytes, composition)?;
        self.diagnostics += 1;
        self.diagnostic_bytes += bytes;
        Ok(())
    }

    pub(crate) fn route(&mut self, route: &Value, composition: Option<&str>) -> GraphResult<()> {
        self.edges(route_len(route), composition)
    }
}

pub(crate) fn route_len(route: &Value) -> usize {
    match route {
        Value::Array(items) => items.len().max(1),
        _ => 1,
    }
}

fn graph_work(budget: &SourceBudget, count: usize, composition: Option<&str>) -> GraphResult<()> {
    budget
        .charge_work(count)
        .then_some(())
        .ok_or_else(|| expansion_limit(composition, "source work units", MAX_SOURCE_WORK))
}

fn graph_bytes(budget: &SourceBudget, count: usize, composition: Option<&str>) -> GraphResult<()> {
    budget
        .charge_bytes(count)
        .then_some(())
        .ok_or_else(|| expansion_limit(composition, "source allocation bytes", MAX_SOURCE_BYTES))
}

fn spend(
    used: &mut usize,
    count: usize,
    limit: usize,
    composition: Option<&str>,
    label: &str,
) -> GraphResult<()> {
    if count > limit - *used {
        return Err(expansion_limit(composition, label, limit));
    }
    *used += count;
    Ok(())
}

fn expansion_limit(composition: Option<&str>, label: &str, limit: usize) -> Unshaped {
    Unshaped {
        message: format!("Maximum {label} exceeded ({limit})."),
        limit_pointer: Some(composition.map_or_else(
            || "/flow".to_owned(),
            |name| super::exact::at("/compositions", name),
        )),
    }
}

pub(crate) fn empty_object() -> &'static Object {
    static EMPTY: OnceLock<Object> = OnceLock::new();
    EMPTY.get_or_init(Object::default)
}

pub(crate) fn object_of<'a>(value: &'a Value, what: &str) -> GraphResult<&'a Object> {
    match value {
        Value::Object(object) => Ok(object),
        _ => unshaped(format!("{what} is not an object")),
    }
}

pub(crate) fn string_of<'a>(value: &'a Value, what: &str) -> GraphResult<&'a str> {
    match value {
        Value::String(text) => Ok(text),
        _ => unshaped(format!("{what} is not a string")),
    }
}

fn member<'a>(object: &'a Object, key: &str, what: &str) -> GraphResult<&'a Value> {
    match object.get(key) {
        Some(value) => Ok(value),
        None => unshaped(format!("{what} has no {key}")),
    }
}

/// JS `<` on strings: UTF-16 code units.
pub(crate) fn compare_utf16(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// `[...strings].sort()` with the default comparator.
pub(crate) fn sort_utf16(values: &mut [String]) {
    values.sort_by(|a, b| compare_utf16(a, b));
}

pub(crate) fn address(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}/{name}")
    }
}

/// `addressParts(addr)`.
pub(crate) fn address_parts(addr: &str) -> Vec<&str> {
    if addr.is_empty() {
        Vec::new()
    } else {
        addr.split('/').collect()
    }
}

/// `last(addr)`: `undefined` for the empty address.
pub(crate) fn last(addr: &str) -> Option<&str> {
    if addr.is_empty() {
        None
    } else {
        Some(addr.rsplit('/').next().unwrap_or(addr))
    }
}

/// `compare_addresses(left, right)`: by segment, shorter first.
pub(crate) fn compare_addresses(left: &str, right: &str) -> Ordering {
    if left.is_empty() || right.is_empty() {
        return right.is_empty().cmp(&left.is_empty());
    }
    let mut a = left.split('/');
    let mut b = right.split('/');
    loop {
        match (a.next(), b.next()) {
            (Some(a), Some(b)) if a != b => return compare_utf16(a, b),
            (Some(_), Some(_)) => {}
            (a, b) => return a.is_some().cmp(&b.is_some()),
        }
    }
}

pub(crate) fn sorted_addresses<'s>(values: impl IntoIterator<Item = &'s String>) -> Vec<String> {
    let mut sorted: Vec<String> = values.into_iter().cloned().collect();
    sorted.sort_by(|a, b| compare_addresses(a, b));
    sorted
}

/// `local_name(addr, prefix)`.
pub(crate) fn local_name(addr: &str, prefix: &str) -> Option<String> {
    let (parts, before) = (address_parts(addr), address_parts(prefix));
    if parts.len() > before.len() && before.iter().zip(&parts).all(|(a, b)| a == b) {
        return Some(parts[before.len()].to_owned());
    }
    parts.last().map(|part| (*part).to_owned())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RouteKind {
    Normal,
    Error,
}

impl RouteKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RouteKind::Normal => "normal",
            RouteKind::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TargetKind {
    Step,
    Output,
}

/// `[kind, name, message, where, each]` of `route_items`. `fanout` is the
/// route item object when it has `each`.
#[derive(Clone)]
pub(crate) struct RouteItem<'a> {
    pub(crate) kind: TargetKind,
    pub(crate) name: &'a str,
    pub(crate) message: Option<&'a Value>,
    pub(crate) pointer: Vec<PointerPart>,
    pub(crate) fanout: Option<&'a Object>,
}

/// `[kind, key, route, pointer]` of `step_routes`.
#[derive(Clone)]
pub(crate) struct StepRoute<'a> {
    pub(crate) kind: RouteKind,
    pub(crate) key: Option<&'a str>,
    pub(crate) route: &'a Value,
    pub(crate) pointer: Vec<PointerPart>,
}

fn with(pointer: &[PointerPart], extra: impl IntoIterator<Item = PointerPart>) -> Vec<PointerPart> {
    let mut out = pointer.to_vec();
    out.extend(extra);
    out
}

pub(crate) fn key(name: &str) -> PointerPart {
    PointerPart::Key(name.to_owned())
}

/// `item.message ?? null`.
fn message_of(item: &Object) -> Option<&Value> {
    item.get("message")
        .filter(|value| !matches!(value, Value::Null))
}

/// `route_items(route, pointer)`.
pub(crate) fn route_items<'a>(
    route: &'a Value,
    pointer: &[PointerPart],
) -> GraphResult<Vec<RouteItem<'a>>> {
    let (items, listed): (Vec<&Value>, bool) = match route {
        Value::Array(items) => (items.iter().collect(), true),
        single => (vec![single], false),
    };
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let at = if listed {
            with(pointer, [PointerPart::Index(index)])
        } else {
            pointer.to_vec()
        };
        match item {
            Value::String(name) => out.push(RouteItem {
                kind: TargetKind::Step,
                name,
                message: None,
                pointer: at,
                fanout: None,
            }),
            Value::Object(object) if object.contains_key("step") => out.push(RouteItem {
                kind: TargetKind::Step,
                name: string_of(member(object, "step", "route item")?, "route step")?,
                message: message_of(object),
                pointer: at,
                fanout: object.contains_key("each").then_some(object),
            }),
            Value::Object(object) => out.push(RouteItem {
                kind: TargetKind::Output,
                name: string_of(member(object, "output", "route item")?, "route output")?,
                message: message_of(object),
                pointer: at,
                fanout: None,
            }),
            _ => return unshaped("route item is neither a name nor an object"),
        }
    }
    Ok(out)
}

/// `route_targets(route, pointer)`: the items without their fan-out.
pub(crate) fn route_targets<'a>(
    route: &'a Value,
    pointer: &[PointerPart],
) -> GraphResult<Vec<RouteItem<'a>>> {
    route_items(route, pointer).map(|items| {
        items
            .into_iter()
            .map(|item| RouteItem {
                fanout: None,
                ..item
            })
            .collect()
    })
}

/// `object(next) && !own(next, 'step') && !own(next, 'output')`.
pub(crate) fn is_route_map(next: &Value) -> bool {
    matches!(next, Value::Object(map) if !map.contains_key("step") && !map.contains_key("output"))
}

/// `step_routes(decl, pointer)`.
pub(crate) fn step_routes<'a>(decl: &'a Object, pointer: &[PointerPart]) -> Vec<StepRoute<'a>> {
    let mut out = Vec::new();
    if let Some(next) = decl.get("next") {
        match next {
            Value::Object(map) if is_route_map(next) => {
                for (choice, route) in map.iter() {
                    out.push(StepRoute {
                        kind: RouteKind::Normal,
                        key: Some(choice),
                        route,
                        pointer: with(pointer, [key("next"), key(choice)]),
                    });
                }
            }
            _ => out.push(StepRoute {
                kind: RouteKind::Normal,
                key: None,
                route: next,
                pointer: with(pointer, [key("next")]),
            }),
        }
    }
    if let Some(route) = decl.get("onError") {
        out.push(StepRoute {
            kind: RouteKind::Error,
            key: None,
            route,
            pointer: with(pointer, [key("onError")]),
        });
    }
    out
}

pub(crate) struct Scope<'a> {
    pub(crate) steps: &'a Object,
    pub(crate) pointer: Vec<PointerPart>,
    pub(crate) prefix: String,
    pub(crate) composition: Option<String>,
    pub(crate) bindings: OrderedMap<String, Option<String>>,
    pub(crate) compose: Option<String>,
    pub(crate) parent: Option<usize>,
    pub(crate) compose_name: Option<String>,
}

pub(crate) struct Node<'a> {
    pub(crate) addr: String,
    pub(crate) decl: Option<&'a Object>,
    pub(crate) scope: usize,
    pub(crate) name: String,
    pub(crate) kind: &'a str,
    pub(crate) pointer: Vec<PointerPart>,
    pub(crate) agent: Option<String>,
}

pub(crate) struct Edge<'a> {
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) kind: RouteKind,
    pub(crate) key: Option<String>,
    pub(crate) message: Option<&'a Value>,
    pub(crate) pointer: Vec<PointerPart>,
    pub(crate) fanout: Option<&'a Object>,
    pub(crate) each: Option<&'a Value>,
}

impl<'a> Edge<'a> {
    fn new(
        src: String,
        dst: String,
        kind: RouteKind,
        key: Option<String>,
        message: Option<&'a Value>,
        pointer: Vec<PointerPart>,
        fanout: Option<&'a Object>,
    ) -> Self {
        // `fanout.each`: `undefined` reads as absent here, as in every caller.
        let each = fanout.and_then(|item| item.get("each"));
        Self {
            src,
            dst,
            kind,
            key,
            message,
            pointer,
            fanout,
            each,
        }
    }

    /// `JSON.stringify([edge.kind, edge.key])` as a comparable value.
    pub(crate) fn group(&self) -> (RouteKind, Option<String>) {
        (self.kind, self.key.clone())
    }
}

/// `[decl, child scope or null, parent scope]`.
pub(crate) struct Compose<'a> {
    pub(crate) decl: &'a Object,
    pub(crate) child: Option<usize>,
    pub(crate) parent: usize,
}

/// Composition name (`None` for the flow) to the steps inside join branches.
pub(crate) type InBranch = OrderedMap<Option<String>, OrderedSet<String>>;

/// Edge indices by node address.
pub(crate) type Adjacency = HashMap<String, Vec<usize>>;

pub(crate) struct Graph<'a> {
    pub(crate) document: &'a Object,
    in_branch: Option<Rc<InBranch>>,
    pub(crate) covered: OrderedSet<String>,
    pub(crate) compositions: &'a Object,
    pub(crate) scopes: Vec<Scope<'a>>,
    pub(crate) nodes: OrderedMap<String, Node<'a>>,
    pub(crate) composes: OrderedMap<String, Compose<'a>>,
    pub(crate) edges: Vec<Edge<'a>>,
    pub(crate) root: Option<String>,
    source: SourceBudget,
    successors: OnceCell<Adjacency>,
    predecessors: OnceCell<Adjacency>,
}

fn compositions_of(document: &Object) -> GraphResult<&Object> {
    match document.get("compositions") {
        None | Some(Value::Null) => Ok(empty_object()),
        Some(value) => object_of(value, "compositions"),
    }
}

impl<'a> Graph<'a> {
    fn new(
        document: &'a Object,
        in_branch: Option<Rc<InBranch>>,
        covered: Option<OrderedSet<String>>,
        budget: &ExpansionBudget,
    ) -> GraphResult<Self> {
        Ok(Self {
            document,
            in_branch,
            covered: covered.unwrap_or_default(),
            compositions: compositions_of(document)?,
            scopes: Vec::new(),
            nodes: OrderedMap::new(),
            composes: OrderedMap::new(),
            edges: Vec::new(),
            root: None,
            source: budget.source.clone(),
            successors: OnceCell::new(),
            predecessors: OnceCell::new(),
        })
    }

    fn flow(document: &'a Object) -> GraphResult<&'a Object> {
        object_of(member(document, "flow", "document")?, "flow")
    }

    fn composition(&self, name: &str) -> GraphResult<&'a Object> {
        object_of(
            member(self.compositions, name, "compositions")?,
            "composition",
        )
    }

    fn add_scope_record(&mut self, scope: Scope<'a>) -> usize {
        self.scopes.push(scope);
        self.scopes.len() - 1
    }

    fn node(
        &self,
        addr: &str,
        decl: Option<&'a Object>,
        scope: usize,
        name: &str,
    ) -> GraphResult<Node<'a>> {
        let kind: &'a str = match decl {
            None => "exit",
            Some(decl) => string_of(member(decl, "type", "step")?, "step type")?,
        };
        let pointer = match decl {
            None => Vec::new(),
            Some(_) => with(&self.scopes[scope].pointer, [key(name)]),
        };
        let mut agent = None;
        if kind == "agent" {
            let reference = member(decl.expect("agent step"), "agent", "agent step")?;
            agent = match reference {
                Value::String(name) => Some(name.clone()),
                Value::Object(reference) => {
                    let parameter = string_of(
                        member(reference, "parameter", "agent reference")?,
                        "parameter",
                    )?;
                    self.scopes[scope]
                        .bindings
                        .get(parameter)
                        .cloned()
                        .flatten()
                }
                _ => return unshaped("agent reference is neither a name nor an object"),
            };
        }
        Ok(Node {
            addr: addr.to_owned(),
            decl,
            scope,
            name: name.to_owned(),
            kind,
            pointer,
            agent,
        })
    }

    /// `Graph.for_flow(document, in_branch, covered)`.
    pub(crate) fn for_flow(
        document: &'a Object,
        in_branch: Option<Rc<InBranch>>,
        covered: Option<OrderedSet<String>>,
        budget: &mut ExpansionBudget,
    ) -> GraphResult<Self> {
        let mut graph = Graph::new(document, in_branch, covered, budget)?;
        let flow = Self::flow(document)?;
        let steps = object_of(member(flow, "steps", "flow")?, "steps")?;
        budget.scope(steps.len(), 0, None)?;
        let scope = graph.add_scope_record(Scope {
            steps,
            pointer: vec![key("flow"), key("steps")],
            prefix: String::new(),
            composition: None,
            bindings: OrderedMap::new(),
            compose: None,
            parent: None,
            compose_name: None,
        });
        graph.add_scope(scope, &[], budget)?;
        graph.connect(budget)?;
        let entry = string_of(member(flow, "entry", "flow")?, "entry")?;
        graph.root = graph.resolve(scope, entry)?;
        Ok(graph)
    }

    /// `Graph.flow_error_covered(document, branch_members)`.
    pub(crate) fn flow_error_covered(
        document: &'a Object,
        branch_members: &OrderedSet<String>,
        budget: &ExpansionBudget,
    ) -> GraphResult<OrderedSet<String>> {
        let flow = Self::flow(document)?;
        let steps = object_of(member(flow, "steps", "flow")?, "steps")?;
        let entry = string_of(member(flow, "entry", "flow")?, "entry")?;
        let Some(on_error) = flow.get("onError") else {
            return Ok(OrderedSet::new());
        };
        if !steps.contains_key(entry) {
            return Ok(OrderedSet::new());
        }
        budget.work(route_len(on_error), None)?;
        budget.bytes(route_len(on_error) * 88, None)?;
        let targets: HashSet<&str> = route_targets(on_error, &[])?
            .into_iter()
            .filter(|item| item.kind == TargetKind::Step)
            .map(|item| item.name)
            .collect();
        let mut seen = OrderedSet::new();
        seen.insert(entry.to_owned());
        let mut queue = vec![entry.to_owned()];
        let mut i = 0;
        while i < queue.len() {
            budget.work(1, None)?;
            let decl = object_of(member(steps, &queue[i], "steps")?, "step")?;
            for route in step_routes(decl, &[]) {
                budget.work(route_len(route.route), None)?;
                budget.bytes(route_len(route.route) * 88, None)?;
                for item in route_targets(route.route, &[])? {
                    if item.kind == TargetKind::Step
                        && steps.contains_key(item.name)
                        && seen.insert(item.name.to_owned())
                    {
                        queue.push(item.name.to_owned());
                    }
                }
            }
            i += 1;
        }
        let mut covered = OrderedSet::new();
        for name in &seen {
            let decl = object_of(member(steps, name, "steps")?, "step")?;
            if !targets.contains(name.as_str())
                && !branch_members.contains(name)
                && !decl.contains_key("onError")
            {
                covered.insert(name.clone());
            }
        }
        Ok(covered)
    }

    /// `Graph.for_scope(document, composition, covered)`.
    pub(crate) fn for_scope(
        document: &'a Object,
        composition: Option<&str>,
        covered: Option<OrderedSet<String>>,
        budget: &mut ExpansionBudget,
    ) -> GraphResult<Self> {
        let mut graph = Graph::new(
            document,
            None,
            if composition.is_none() { covered } else { None },
            budget,
        )?;
        let (container, pointer) = match composition {
            None => (Self::flow(document)?, vec![key("flow"), key("steps")]),
            Some(name) => (
                graph.composition(name)?,
                vec![key("compositions"), key(name), key("steps")],
            ),
        };
        let steps = object_of(member(container, "steps", "container")?, "steps")?;
        budget.scope(steps.len(), usize::from(composition.is_some()), composition)?;
        let scope = graph.add_scope_record(Scope {
            steps,
            pointer,
            prefix: String::new(),
            composition: composition.map(str::to_owned),
            bindings: OrderedMap::new(),
            compose: None,
            parent: None,
            compose_name: None,
        });
        for (name, decl) in steps.iter() {
            budget.bytes(name.len() * 8, composition)?;
            let node = graph.node(name, Some(object_of(decl, "step")?), scope, name)?;
            graph.nodes.set(name.to_owned(), node);
        }
        let snapshot: Vec<String> = graph.nodes.keys().cloned().collect();
        for addr in snapshot {
            let (decl, pointer) = {
                let node = graph.nodes.get(&addr).expect("snapshot node");
                (node.decl.expect("step node"), node.pointer.clone())
            };
            for route in step_routes(decl, &pointer) {
                budget.route(route.route, composition)?;
                for item in route_items(route.route, &route.pointer)? {
                    if let Some(dst) = graph.local_target(scope, item.kind, item.name, budget)? {
                        graph.edges.push(Edge::new(
                            addr.clone(),
                            dst,
                            route.kind,
                            route.key.map(str::to_owned),
                            item.message,
                            item.pointer,
                            item.fanout,
                        ));
                    }
                }
            }
        }
        let mut covered: Vec<String> = graph.covered.iter().cloned().collect();
        sort_utf16(&mut covered);
        for name in covered {
            let on_error = member(container, "onError", "flow")?;
            budget.route(on_error, composition)?;
            for item in route_items(on_error, &[key("flow"), key("onError")])? {
                if let Some(dst) = graph.local_target(scope, item.kind, item.name, budget)? {
                    graph.edges.push(Edge::new(
                        name.clone(),
                        dst,
                        RouteKind::Error,
                        None,
                        item.message,
                        item.pointer,
                        item.fanout,
                    ));
                }
            }
        }
        let entry = string_of(member(container, "entry", "container")?, "entry")?;
        graph.root = graph.nodes.contains_key(entry).then(|| entry.to_owned());
        Ok(graph)
    }

    fn exit_node(
        &mut self,
        scope: usize,
        name: &str,
        budget: &mut ExpansionBudget,
    ) -> GraphResult<String> {
        let addr = format!("#exit/{name}");
        if !self.nodes.contains_key(&addr) {
            budget.nodes(1, self.scopes[scope].composition.as_deref())?;
            let node = self.node(&addr, None, scope, name)?;
            self.nodes.set(addr.clone(), node);
        }
        Ok(addr)
    }

    fn local_target(
        &mut self,
        scope: usize,
        kind: TargetKind,
        name: &str,
        budget: &mut ExpansionBudget,
    ) -> GraphResult<Option<String>> {
        if kind == TargetKind::Step {
            return Ok(self.nodes.contains_key(name).then(|| name.to_owned()));
        }
        if self.scopes[scope].composition.is_none() {
            return Ok(None);
        }
        self.exit_node(scope, name, budget).map(Some)
    }

    /// `Graph.for_composition(document, name, in_branch)`.
    pub(crate) fn for_composition(
        document: &'a Object,
        name: &str,
        in_branch: Option<Rc<InBranch>>,
        budget: &mut ExpansionBudget,
    ) -> GraphResult<Self> {
        let mut graph = Graph::new(document, in_branch, None, budget)?;
        let composition = graph.composition(name)?;
        let steps = object_of(member(composition, "steps", "composition")?, "steps")?;
        budget.scope(steps.len(), 1, Some(name))?;
        let scope = graph.add_scope_record(Scope {
            steps,
            pointer: vec![key("compositions"), key(name), key("steps")],
            prefix: format!("#{name}"),
            composition: Some(name.to_owned()),
            bindings: OrderedMap::new(),
            compose: None,
            parent: None,
            compose_name: None,
        });
        graph.add_scope(scope, &[name.to_owned()], budget)?;
        graph.connect(budget)?;
        let entry = string_of(member(composition, "entry", "composition")?, "entry")?;
        graph.root = graph.resolve(scope, entry)?;
        Ok(graph)
    }

    fn add_scope(
        &mut self,
        scope: usize,
        stack: &[String],
        budget: &mut ExpansionBudget,
    ) -> GraphResult<()> {
        struct Frame<'a> {
            scope: usize,
            entries: Vec<(&'a str, &'a Value)>,
            index: usize,
            target: Option<String>,
        }
        let mut active: HashSet<String> = stack.iter().cloned().collect();
        let steps: &'a Object = self.scopes[scope].steps;
        let mut frames = vec![Frame {
            scope,
            entries: steps.iter().collect(),
            index: 0,
            target: None,
        }];
        while let Some(frame) = frames.last_mut() {
            if frame.index == frame.entries.len() {
                if let Some(target) = &frame.target {
                    active.remove(target);
                }
                frames.pop();
                continue;
            }
            let scope = frame.scope;
            let (name, decl) = frame.entries[frame.index];
            frame.index += 1;
            let decl = object_of(decl, "step")?;
            let scope_record = &self.scopes[scope];
            budget.bytes(
                8 * (scope_record.prefix.len() + name.len() + 1),
                scope_record.composition.as_deref(),
            )?;
            let addr = address(&scope_record.prefix, name);
            if string_of(member(decl, "type", "step")?, "step type")? != "compose" {
                let node = self.node(&addr, Some(decl), scope, name)?;
                self.nodes.set(addr, node);
                continue;
            }
            let target = string_of(member(decl, "composition", "compose step")?, "composition")?;
            let composition = match self.compositions.get(target) {
                Some(found) => Some(object_of(found, "composition")?),
                None => None,
            };
            let Some(composition) = composition.filter(|_| !active.contains(target)) else {
                self.composes.set(
                    addr,
                    Compose {
                        decl,
                        child: None,
                        parent: scope,
                    },
                );
                continue;
            };
            let child_steps = object_of(member(composition, "steps", "composition")?, "steps")?;
            budget.scope(child_steps.len(), active.len() + 1, Some(target))?;
            let mut bindings = OrderedMap::new();
            let agents = match decl.get("agents") {
                None | Some(Value::Null) => empty_object(),
                Some(value) => object_of(value, "compose agents")?,
            };
            budget.work(agents.len(), Some(target))?;
            budget.bytes(agents.len() * 96, Some(target))?;
            for (parameter, value) in agents.iter() {
                budget.bytes(parameter.len() * 2, Some(target))?;
                let bound = match value {
                    Value::String(name) => Some(name.clone()),
                    Value::Object(reference) => {
                        let name = string_of(
                            member(reference, "parameter", "agent reference")?,
                            "parameter",
                        )?;
                        self.scopes[scope].bindings.get(name).cloned().flatten()
                    }
                    _ => return unshaped("agent binding is neither a name nor an object"),
                };
                bindings.set(parameter.to_owned(), bound);
            }
            let prefix = &self.scopes[scope].prefix;
            let compose_name = (!name.is_empty() || !prefix.is_empty())
                .then(|| name.rsplit('/').next().unwrap_or(name).to_owned());
            let child = self.add_scope_record(Scope {
                steps: child_steps,
                pointer: vec![key("compositions"), key(target), key("steps")],
                prefix: addr.clone(),
                composition: Some(target.to_owned()),
                bindings,
                compose: Some(addr.clone()),
                parent: Some(scope),
                compose_name,
            });
            self.composes.set(
                addr,
                Compose {
                    decl,
                    child: Some(child),
                    parent: scope,
                },
            );
            active.insert(target.to_owned());
            frames.push(Frame {
                scope: child,
                entries: child_steps.iter().collect(),
                index: 0,
                target: Some(target.to_owned()),
            });
        }
        Ok(())
    }

    /// `graph.resolve(scope, name)`.
    pub(crate) fn resolve(&self, scope: usize, name: &str) -> GraphResult<Option<String>> {
        let mut scope = scope;
        let mut addr = address(&self.scopes[scope].prefix, name);
        while let Some(compose) = self.composes.get(&addr) {
            let Some(child) = compose.child else {
                return Ok(None);
            };
            scope = child;
            let target = string_of(
                member(compose.decl, "composition", "compose step")?,
                "composition",
            )?;
            let entry = string_of(
                member(self.composition(target)?, "entry", "composition")?,
                "entry",
            )?;
            addr = address(&self.scopes[scope].prefix, entry);
        }
        Ok(self.nodes.contains_key(&addr).then_some(addr))
    }

    /// `graph._targets(...)`: `[addr, message, pointer, fanout]` in order.
    #[allow(clippy::type_complexity)]
    fn targets(
        &mut self,
        scope: usize,
        item: RouteItem<'a>,
        budget: &mut ExpansionBudget,
        source_composition: Option<&str>,
    ) -> GraphResult<
        Vec<(
            String,
            Option<&'a Value>,
            Vec<PointerPart>,
            Option<&'a Object>,
        )>,
    > {
        let mut found = Vec::new();
        let mut pending = vec![(scope, item)];
        while let Some((scope, item)) = pending.pop() {
            if item.kind == TargetKind::Step {
                if let Some(addr) = self.resolve(scope, item.name)? {
                    found.push((addr, item.message, item.pointer, item.fanout));
                }
                continue;
            }
            let Some(compose) = self.scopes[scope].compose.clone() else {
                if self.scopes[scope].composition.is_none() {
                    continue;
                }
                let addr = self.exit_node(scope, item.name, budget)?;
                found.push((addr, item.message, item.pointer, item.fanout));
                continue;
            };
            let record = self.composes.get(&compose).expect("compose scope record");
            let (decl, parent): (&'a Object, usize) = (record.decl, record.parent);
            let next = match decl.get("next") {
                None | Some(Value::Null) => empty_object(),
                Some(value) => object_of(value, "compose next")?,
            };
            let Some(route) = next.get(item.name) else {
                continue;
            };
            let segment = match &self.scopes[scope].compose_name {
                Some(name) => name.clone(),
                None => last(&compose).unwrap_or_default().to_owned(),
            };
            let outer = with(
                &self.scopes[parent].pointer,
                [key(&segment), key("next"), key(item.name)],
            );
            budget.route(route, source_composition)?;
            let items = route_items(route, &outer)?;
            for item in items.into_iter().rev() {
                pending.push((parent, item));
            }
        }
        Ok(found)
    }

    /// `graph._inherited_error(scope, name)`: `[scope, route, pointer]`.
    fn inherited_error(
        &self,
        scope: usize,
        name: &str,
    ) -> GraphResult<Option<(usize, &'a Value, Vec<PointerPart>)>> {
        let mut scope = scope;
        let mut name = name.to_owned();
        while let Some(compose) = self.scopes[scope].compose.clone() {
            let composition = self.scopes[scope].composition.clone();
            if self
                .in_branch
                .as_ref()
                .and_then(|members| members.get(&composition))
                .is_some_and(|members| members.contains(&name))
            {
                return Ok(None);
            }
            let record = self.composes.get(&compose).expect("compose scope record");
            let (decl, parent): (&'a Object, usize) = (record.decl, record.parent);
            let last_segment = last(&compose).unwrap_or_default().to_owned();
            if let Some(route) = decl.get("onError") {
                let pointer = with(
                    &self.scopes[parent].pointer,
                    [key(&last_segment), key("onError")],
                );
                return Ok(Some((parent, route, pointer)));
            }
            name = self.scopes[scope]
                .compose_name
                .clone()
                .unwrap_or(last_segment);
            scope = parent;
        }
        if self.scopes[scope].composition.is_none() && self.covered.contains(&name) {
            let route = member(Self::flow(self.document)?, "onError", "flow")?;
            return Ok(Some((scope, route, vec![key("flow"), key("onError")])));
        }
        Ok(None)
    }

    fn connect(&mut self, budget: &mut ExpansionBudget) -> GraphResult<()> {
        let snapshot: Vec<String> = self.nodes.keys().cloned().collect();
        for addr in snapshot {
            let (decl, pointer, scope, name) = {
                let node = self.nodes.get(&addr).expect("snapshot node");
                let Some(decl) = node.decl else { continue };
                (decl, node.pointer.clone(), node.scope, node.name.clone())
            };
            let mut routes: Vec<(StepRoute<'a>, usize)> = step_routes(decl, &pointer)
                .into_iter()
                .map(|route| (route, scope))
                .collect();
            if !decl.contains_key("onError") {
                if let Some((inherited_scope, route, at)) = self.inherited_error(scope, &name)? {
                    routes.push((
                        StepRoute {
                            kind: RouteKind::Error,
                            key: None,
                            route,
                            pointer: at,
                        },
                        inherited_scope,
                    ));
                }
            }
            let composition = self.scopes[scope].composition.clone();
            for (route, scope) in routes {
                budget.route(route.route, composition.as_deref())?;
                for item in route_items(route.route, &route.pointer)? {
                    for (dst, carried, at, fanout) in
                        self.targets(scope, item, budget, composition.as_deref())?
                    {
                        self.edges.push(Edge::new(
                            addr.clone(),
                            dst,
                            route.kind,
                            route.key.map(str::to_owned),
                            carried,
                            at,
                            fanout,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn composition_name(&self) -> Option<&str> {
        self.scopes
            .first()
            .and_then(|scope| scope.composition.as_deref())
    }

    pub(crate) fn work(&self, units: usize) -> GraphResult<()> {
        graph_work(&self.source, units, self.composition_name())
    }

    pub(crate) fn bytes(&self, bytes: usize) -> GraphResult<()> {
        graph_bytes(&self.source, bytes, self.composition_name())
    }

    fn adjacency(&self, source: bool) -> GraphResult<Adjacency> {
        self.work(self.nodes.len() + self.edges.len())?;
        self.bytes(self.nodes.len() * 96 + self.edges.len() * 16)?;
        let mut result: Adjacency = self
            .nodes
            .keys()
            .map(|addr| (addr.clone(), Vec::new()))
            .collect();
        for (index, edge) in self.edges.iter().enumerate() {
            let end = if source { &edge.src } else { &edge.dst };
            match result.get_mut(end) {
                Some(list) => list.push(index),
                None => return unshaped(format!("edge end {end} is not a node")),
            }
        }
        Ok(result)
    }

    /// `graph.successors()`: outgoing edge indices in edge order.
    pub(crate) fn successors(&self) -> GraphResult<&Adjacency> {
        if self.successors.get().is_none() {
            let built = self.adjacency(true)?;
            let _ = self.successors.set(built);
        }
        Ok(self.successors.get().expect("successors"))
    }

    /// `graph.predecessors()`: incoming edge indices in edge order.
    pub(crate) fn predecessors(&self) -> GraphResult<&Adjacency> {
        if self.predecessors.get().is_none() {
            let built = self.adjacency(false)?;
            let _ = self.predecessors.set(built);
        }
        Ok(self.predecessors.get().expect("predecessors"))
    }

    fn adjacent<'g>(adjacency: &'g Adjacency, addr: &str) -> GraphResult<&'g [usize]> {
        match adjacency.get(addr) {
            Some(list) => Ok(list),
            None => unshaped(format!("{addr} is not a node")),
        }
    }

    pub(crate) fn outgoing(&self, addr: &str) -> GraphResult<&[usize]> {
        Self::adjacent(self.successors()?, addr)
    }

    pub(crate) fn incoming(&self, addr: &str) -> GraphResult<&[usize]> {
        Self::adjacent(self.predecessors()?, addr)
    }

    /// `graph.reachable(start, blocked)`.
    pub(crate) fn reachable(
        &self,
        start: &str,
        blocked: &OrderedSet<String>,
    ) -> GraphResult<OrderedSet<String>> {
        self.bytes(96 + start.len() * 3)?;
        let mut seen = OrderedSet::new();
        seen.insert(start.to_owned());
        let mut queue = vec![start.to_owned()];
        let mut i = 0;
        while i < queue.len() {
            let outgoing = self.outgoing(&queue[i])?;
            self.work(1 + outgoing.len())?;
            for &edge in outgoing {
                let dst = &self.edges[edge].dst;
                if blocked.contains(dst) || seen.contains(dst) {
                    continue;
                }
                self.bytes(96 + dst.len() * 3)?;
                seen.insert(dst.clone());
                queue.push(dst.clone());
            }
            i += 1;
        }
        Ok(seen)
    }

    /// `graph.reaching(target, blocked)`.
    pub(crate) fn reaching(
        &self,
        target: &str,
        blocked: &OrderedSet<String>,
    ) -> GraphResult<OrderedSet<String>> {
        self.bytes(96 + target.len())?;
        let mut seen = OrderedSet::new();
        let mut queue = vec![target.to_owned()];
        let mut i = 0;
        while i < queue.len() {
            let incoming = self.incoming(&queue[i])?;
            self.work(1 + incoming.len())?;
            for &edge in incoming {
                let src = &self.edges[edge].src;
                if blocked.contains(src) || seen.contains(src) || src == target {
                    continue;
                }
                self.bytes(96 + src.len() * 3)?;
                seen.insert(src.clone());
                queue.push(src.clone());
            }
            i += 1;
        }
        Ok(seen)
    }

    /// Compute immediate dominators in reverse postorder using the
    /// Cooper–Harvey–Kennedy algorithm. Only one parent per node is stored.
    fn immediate_dominators(&self) -> GraphResult<HashMap<&str, &str>> {
        let Some(root) = self.root.as_deref() else {
            return Ok(HashMap::new());
        };
        self.work(self.nodes.len() + self.edges.len())?;
        self.bytes(self.nodes.len() * 128)?;
        let mut seen = HashSet::from([root]);
        let mut order = Vec::new();
        let mut stack = vec![(root, self.outgoing(root)?.iter())];
        while let Some((addr, outgoing)) = stack.last_mut() {
            if let Some(&edge) = outgoing.next() {
                let dst = self.edges[edge].dst.as_str();
                if seen.insert(dst) {
                    stack.push((dst, self.outgoing(dst)?.iter()));
                }
            } else {
                order.push(*addr);
                stack.pop();
            }
        }
        order.reverse();
        let positions: HashMap<_, _> = order
            .iter()
            .enumerate()
            .map(|(i, &addr)| (addr, i))
            .collect();
        let mut parents = vec![None; order.len()];
        parents[0] = Some(0);
        loop {
            let mut changed = false;
            for (index, addr) in order.iter().enumerate().skip(1) {
                let incoming = self.incoming(addr)?;
                self.work(1 + incoming.len())?;
                let mut incoming = incoming
                    .iter()
                    .filter_map(|&edge| positions.get(self.edges[edge].src.as_str()).copied())
                    .filter(|&i| parents[i].is_some());
                let first = incoming.next().expect("reachable DFS predecessor");
                let parent = incoming.try_fold(first, |mut a, mut b| {
                    while a != b {
                        self.work(1)?;
                        if a > b {
                            a = parents[a].expect("known parent");
                        } else {
                            b = parents[b].expect("known parent");
                        }
                    }
                    Ok(a)
                })?;
                changed |= parents[index].replace(parent) != Some(parent);
            }
            if !changed {
                break;
            }
        }
        Ok(order
            .iter()
            .zip(parents)
            .skip(1)
            .map(|(&addr, parent)| (addr, order[parent.expect("reachable node")]))
            .collect())
    }

    pub(crate) fn node_at(&self, addr: &str) -> GraphResult<&Node<'a>> {
        match self.nodes.get(addr) {
            Some(node) => Ok(node),
            None => unshaped(format!("{addr} is not a node")),
        }
    }
}

/// `route_groups(graph, addr, succ)`: destinations by `[kind, key]`.
pub(crate) fn route_groups(
    graph: &Graph<'_>,
    addr: &str,
) -> GraphResult<OrderedMap<(RouteKind, Option<String>), Vec<String>>> {
    let outgoing = graph.outgoing(addr)?;
    graph.work(1 + outgoing.len())?;
    graph.bytes(outgoing.len() * 96)?;
    let mut groups: OrderedMap<(RouteKind, Option<String>), Vec<String>> = OrderedMap::new();
    for &edge in outgoing {
        let edge = &graph.edges[edge];
        match groups.get_mut(&edge.group()) {
            Some(list) => list.push(edge.dst.clone()),
            None => groups.set(edge.group(), vec![edge.dst.clone()]),
        }
    }
    Ok(groups)
}

/// `analyse_joins` information for one join.
#[derive(Clone, Debug)]
pub(crate) struct JoinInfo {
    pub(crate) origin: String,
    pub(crate) branches: OrderedMap<String, OrderedSet<String>>,
    pub(crate) names: OrderedMap<String, Option<String>>,
    pub(crate) fanout: OrderedSet<String>,
}

/// `['JOIN', pointer, addr, message]` or `[pointer, addr, message]`.
#[derive(Clone, Debug)]
pub(crate) struct GraphProblem {
    pub(crate) pointer: Vec<PointerPart>,
    pub(crate) addr: String,
    pub(crate) message: String,
}

/// `open_body_forks(graph, infos)`.
pub(crate) fn open_body_forks(
    graph: &Graph<'_>,
    infos: &OrderedMap<String, JoinInfo>,
) -> GraphResult<Vec<GraphProblem>> {
    let mut problems = Vec::new();
    let Some(root) = &graph.root else {
        return Ok(problems);
    };
    let reach = graph.reachable(root, &OrderedSet::new())?;
    for addr in sorted_addresses(&reach) {
        let node = graph.node_at(&addr)?;
        if node.decl.is_none() {
            continue;
        }
        for (group, dsts) in route_groups(graph, &addr)?.iter() {
            graph
                .work(graph.outgoing(&addr)?.len() + infos.len().saturating_mul(dsts.len() + 1))?;
            let targets: OrderedSet<String> = dsts.iter().cloned().collect();
            let fans = graph.outgoing(&addr)?.iter().any(|&edge| {
                let edge = &graph.edges[edge];
                edge.group() == *group && edge.each.is_some()
            });
            if targets.len() < 2 && !fans {
                continue;
            }
            if infos.values().any(|info| {
                info.origin == addr
                    && targets
                        .iter()
                        .all(|target| info.branches.contains_key(target))
            }) {
                continue;
            }
            problems.push(GraphProblem {
                pointer: node.pointer.clone(),
                addr: addr.clone(),
                message: format!(
                    "the fork at '{}' does not start the branches of one join; a composition leaves at most once",
                    node.name
                ),
            });
            break;
        }
    }
    Ok(problems)
}

/// `join_origin(graph, join, idom, succ)`.
fn join_origin(graph: &Graph<'_>, join: &str, idom: &HashMap<&str, &str>) -> GraphResult<String> {
    let Some(&first) = idom.get(join) else {
        return unshaped(format!("join {join} has no immediate dominator"));
    };
    let mut chain = Vec::new();
    let mut current = Some(first);
    while let Some(step) = current {
        graph.work(1)?;
        graph.bytes(64)?;
        current = idom.get(step).copied();
        chain.push(step);
    }
    let dominators: HashSet<_> = chain.iter().copied().collect();
    let fork = |step: &str, fanout_only: bool| -> GraphResult<bool> {
        let mut groups: OrderedMap<(RouteKind, Option<String>), Vec<usize>> = OrderedMap::new();
        let outgoing = graph.outgoing(step)?;
        graph.work(outgoing.len() * 3 + 1)?;
        graph.bytes(outgoing.len() * 64)?;
        for &edge in outgoing {
            let group = graph.edges[edge].group();
            match groups.get_mut(&group) {
                Some(list) => list.push(edge),
                None => groups.set(group, vec![edge]),
            }
        }
        let mut reaching = None;
        for edges in groups.values() {
            if edges.iter().any(|&edge| {
                let edge = &graph.edges[edge];
                edge.each.is_some() && (dominators.contains(edge.dst.as_str()) || edge.dst == join)
            }) {
                return Ok(true);
            }
            if !fanout_only && edges.len() >= 2 {
                if reaching.is_none() {
                    reaching =
                        Some(graph.reaching(join, &[step.to_owned()].into_iter().collect())?);
                }
                let reaching = reaching.as_ref().unwrap();
                let mut first = None;
                for &edge in edges {
                    let dst = &graph.edges[edge].dst;
                    if dst == join
                        || reaching.contains(dst)
                        || (dst == step
                            && graph
                                .reachable(dst, &[step.to_owned()].into_iter().collect())?
                                .contains(join))
                    {
                        if first.is_some_and(|first| first != dst) {
                            return Ok(true);
                        }
                        first = Some(dst);
                    }
                }
            }
        }
        Ok(false)
    };
    if fork(first, false)? {
        return Ok(first.to_owned());
    }
    let mut closed = 0usize;
    for step in &chain {
        if graph.node_at(step)?.kind == "join" {
            closed += 1;
        } else if fork(step, false)? {
            if closed == 0 {
                return Ok(if fork(step, true)? { *step } else { first }.to_owned());
            }
            closed -= 1;
        }
    }
    Ok(first.to_owned())
}

/// `analyse_joins(graph)`: join information in node order, then problems.
pub(crate) fn analyse_joins(
    graph: &Graph<'_>,
    budget: &mut ExpansionBudget,
) -> GraphResult<(OrderedMap<String, JoinInfo>, Vec<GraphProblem>)> {
    let mut infos = OrderedMap::new();
    let mut problems = Vec::new();
    let composition = graph
        .scopes
        .first()
        .and_then(|scope| scope.composition.as_deref());
    graph.work(graph.nodes.len())?;
    if !graph.nodes.iter().any(|(_, node)| node.kind == "join") {
        return Ok((infos, problems));
    }
    let idom = graph.immediate_dominators()?;
    let mut origins: OrderedMap<String, String> = OrderedMap::new();
    for (addr, node) in graph.nodes.iter() {
        if node.kind == "join" && idom.contains_key(addr.as_str()) {
            origins.set(addr.clone(), join_origin(graph, addr, &idom)?);
        }
    }
    let one = |value: &str| -> OrderedSet<String> { [value.to_owned()].into_iter().collect() };
    for (addr, origin) in origins.iter() {
        let node = graph.node_at(addr)?;
        let joins_to_j = graph.reaching(addr, &one(origin))?;
        let groups = route_groups(graph, origin)?;
        let mut starts = OrderedSet::new();
        for dsts in groups.values() {
            for dst in dsts {
                if !starts.contains(dst) && (dst == addr || joins_to_j.contains(dst)) {
                    budget.join_analysis(1, composition)?;
                    starts.insert(dst.clone());
                }
            }
        }
        let mut branches = OrderedMap::new();
        let blocked = [origin.clone(), addr.clone()].into_iter().collect();
        for start in &starts {
            let mut members = OrderedSet::new();
            if start != addr {
                for member in &graph.reachable(start, &blocked)? {
                    if member == start || joins_to_j.contains(member) {
                        budget.join_analysis(1, composition)?;
                        members.insert(member.clone());
                    }
                }
            }
            branches.set(start.clone(), members);
        }
        let mut record = |problem| {
            budget.diagnostic(&problem, composition)?;
            budget.join_analysis(1, composition)?;
            problems.push(problem);
            Ok(())
        };
        let prefix = &graph.scopes[node.scope].prefix;
        let mut names: OrderedMap<String, Option<String>> = OrderedMap::new();
        for start in &starts {
            let at = if start == addr { origin } else { start };
            names.set(start.clone(), local_name(at, prefix));
        }
        let started: Vec<usize> = graph
            .outgoing(origin)?
            .iter()
            .copied()
            .filter(|&edge| branches.contains_key(&graph.edges[edge].dst))
            .collect();
        let fanout: OrderedSet<String> = started
            .iter()
            .filter(|&&edge| graph.edges[edge].each.is_some())
            .map(|&edge| graph.edges[edge].dst.clone())
            .collect();
        let name_of = |start: &str| {
            names
                .get(start)
                .cloned()
                .flatten()
                .unwrap_or_else(|| "undefined".to_owned())
        };
        let last_of = |addr: &str| last(addr).unwrap_or("undefined").to_owned();
        if fanout.is_empty()
            && !groups.values().any(|dsts| {
                dsts.iter()
                    .filter(|dst| branches.contains_key(*dst))
                    .collect::<HashSet<_>>()
                    .len()
                    >= 2
            })
        {
            record(GraphProblem {
                pointer: node.pointer.clone(),
                addr: addr.clone(),
                message: format!(
                    "no route of the origin '{}' starts two branches or more at once",
                    last_of(origin)
                ),
            })?;
        }
        for &edge in &started {
            let edge = &graph.edges[edge];
            if fanout.contains(&edge.dst) && edge.each.is_none() {
                record(GraphProblem {
                    pointer: edge.pointer.clone(),
                    addr: origin.clone(),
                    message: format!(
                        "branch '{}' of join '{}' is a fan-out on another route; start it with each on every route",
                        name_of(&edge.dst),
                        last_of(addr)
                    ),
                })?;
            }
        }
        let listed: Vec<_> = branches.values().collect();
        let mut seen_members = HashSet::new();
        for (i, members) in listed.iter().enumerate() {
            let mut overlaps = false;
            for member in *members {
                overlaps |= !seen_members.insert(member);
            }
            if !overlaps {
                continue;
            }
            for prior in &listed[..i] {
                graph.work(members.len() + 1)?;
                let shared = members
                    .iter()
                    .filter(|step| prior.contains(*step))
                    .min_by(|a, b| compare_addresses(a, b));
                if let Some(step) = shared {
                    record(GraphProblem {
                        pointer: graph.node_at(step)?.pointer.clone(),
                        addr: step.clone(),
                        message: format!(
                            "step '{}' belongs to two branches of join '{}'",
                            last_of(step),
                            last_of(addr)
                        ),
                    })?;
                }
            }
        }
        for (start, members) in branches.iter() {
            for member in members {
                let incoming = graph.incoming(member)?;
                graph.work(incoming.len() + 1)?;
                for &edge in incoming {
                    let edge = &graph.edges[edge];
                    if members.contains(&edge.src) || (edge.src == *origin && member == start) {
                        continue;
                    }
                    record(GraphProblem {
                        pointer: edge.pointer.clone(),
                        addr: edge.src.clone(),
                        message: format!(
                            "a route from '{}' enters branch '{}' of join '{}' from outside",
                            last_of(&edge.src),
                            name_of(start),
                            last_of(addr)
                        ),
                    })?;
                }
            }
        }
        let all_members: HashSet<&String> = branches
            .values()
            .flat_map(|members| members.iter())
            .collect();
        for &edge in graph.incoming(addr)? {
            let edge = &graph.edges[edge];
            if edge.kind == RouteKind::Normal
                && (edge.src == *origin || all_members.contains(&edge.src))
            {
                continue;
            }
            record(GraphProblem {
                pointer: edge.pointer.clone(),
                addr: edge.src.clone(),
                message: format!(
                    "join '{}' receives a route from '{}', which is not one of its branches",
                    last_of(addr),
                    last_of(&edge.src)
                ),
            })?;
        }
        for (start, members) in branches.iter() {
            for member in members {
                for (group, dsts) in route_groups(graph, member)?.iter() {
                    graph.work(
                        dsts.len() + graph.outgoing(member)?.len().saturating_mul(dsts.len()),
                    )?;
                    let inside: Vec<&String> = dsts
                        .iter()
                        .filter(|dst| members.contains(*dst) || *dst == addr)
                        .collect();
                    if !inside.is_empty() && inside.len() < dsts.len() {
                        record(GraphProblem {
                            pointer: graph.node_at(member)?.pointer.clone(),
                            addr: member.clone(),
                            message: format!(
                                "a route of '{}' starts targets inside and outside branch '{}' of join '{}' at once",
                                last_of(member),
                                name_of(start),
                                last_of(addr)
                            ),
                        })?;
                        continue;
                    }
                    let fans = graph.outgoing(member)?.iter().any(|&edge| {
                        let edge = &graph.edges[edge];
                        edge.group() == *group && edge.each.is_some() && inside.contains(&&edge.dst)
                    });
                    if inside.iter().collect::<HashSet<_>>().len() < 2 && !fans {
                        continue;
                    }
                    graph.work(members.len())?;
                    graph.bytes(members.len() * 72)?;
                    let mut blocked: OrderedSet<String> = members
                        .iter()
                        .filter(|k| origins.get(*k) == Some(member))
                        .cloned()
                        .collect();
                    blocked.insert(origin.clone());
                    let mut open_fork = false;
                    for dst in &inside {
                        if *dst == addr || graph.reachable(dst, &blocked)?.contains(addr) {
                            open_fork = true;
                            break;
                        }
                    }
                    if open_fork {
                        record(GraphProblem {
                            pointer: graph.node_at(member)?.pointer.clone(),
                            addr: member.clone(),
                            message: format!(
                                "the fork at '{}' is not closed by its own join before join '{}'",
                                last_of(member),
                                last_of(addr)
                            ),
                        })?;
                    }
                }
            }
        }
        if let Some(quorum) = node.decl.and_then(|decl| decl.get("quorum")) {
            if super::schema::truthy(quorum) && fanout.is_empty() {
                let count = member(object_of(quorum, "quorum")?, "count", "quorum")?;
                let Value::Number(count) = count else {
                    return unshaped("quorum count is not a number");
                };
                graph.work(count.raw().len().div_ceil(8))?;
                if compare_numbers(Num::Token(count), Num::Host(starts.len())) == Ordering::Greater
                {
                    let mut pointer = node.pointer.clone();
                    pointer.extend([key("quorum"), key("count")]);
                    record(GraphProblem {
                        pointer,
                        addr: addr.clone(),
                        message: format!(
                            "count {} exceeds the {} branches of join '{}'",
                            number_excerpt(count.raw()),
                            starts.len(),
                            last_of(addr)
                        ),
                    })?;
                }
            }
        }
        infos.set(
            addr.clone(),
            JoinInfo {
                origin: origin.clone(),
                branches,
                names,
                fanout,
            },
        );
    }
    Ok((infos, problems))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::argentic_core::exact::scan::parse_v030;

    #[test]
    fn expansion_budget_checks_nodes_and_route_targets_before_allocation() {
        let mut budget = ExpansionBudget::default();
        budget.scope(MAX_EXPANDED_GRAPH_NODES - 1, 0, None).unwrap();
        let source = super::super::exact::scan::parse_v030(
            br#"{
            "flow":{"entry":"start","steps":{"start":{"type":"compose","composition":"review"}}},
            "compositions":{"review":{"entry":"end","steps":{"end":{"type":"agent","agent":"a","next":"end"}}}}
        }"#,
        )
        .unwrap();
        let document = source.tree.as_object().unwrap();
        let error = Graph::for_flow(document, None, None, &mut budget)
            .err()
            .unwrap();
        assert_eq!(error.limit_pointer.as_deref(), Some("/compositions/review"));
        assert_eq!(budget.nodes, MAX_EXPANDED_GRAPH_NODES);
        let mut budget = ExpansionBudget::default();
        budget.edges(MAX_EXPANDED_GRAPH_EDGES, None).unwrap();
        let error = Graph::for_flow(document, None, None, &mut budget)
            .err()
            .unwrap();
        assert_eq!(error.limit_pointer.as_deref(), Some("/compositions/review"));
        assert_eq!(budget.edges, MAX_EXPANDED_GRAPH_EDGES);
        budget.json(MAX_COMPILED_GRAPH_JSON_BYTES, None).unwrap();
        let error = budget.json(1, Some("review")).unwrap_err();
        assert_eq!(error.limit_pointer.as_deref(), Some("/compositions/review"));
    }

    #[test]
    fn join_analysis_at_expanded_node_limit() {
        use serde_json::json;

        let count = MAX_EXPANDED_GRAPH_NODES - 3;
        let mut steps = serde_json::Map::new();
        steps.insert(
            "fork".into(),
            json!({"type":"agent","agent":"a","next":["s0","short"]}),
        );
        steps.insert(
            "short".into(),
            json!({"type":"agent","agent":"a","next":"merge"}),
        );
        for i in 0..count {
            let next = if i + 1 == count {
                "merge".into()
            } else {
                format!("s{}", i + 1)
            };
            steps.insert(
                format!("s{i}"),
                json!({"type":"agent","agent":"a","next":next}),
            );
        }
        steps.insert("merge".into(), json!({"type":"join"}));
        let doc = json!({"flow":{"entry":"fork","steps":steps}});
        let parsed = parse_v030(&serde_json::to_vec(&doc).unwrap()).unwrap();
        let mut budget = ExpansionBudget::default();
        let graph =
            Graph::for_scope(parsed.tree.as_object().unwrap(), None, None, &mut budget).unwrap();
        assert_eq!(graph.nodes.len(), MAX_EXPANDED_GRAPH_NODES);
        let (joins, problems) = analyse_joins(&graph, &mut budget).unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        let join = joins.get("merge").unwrap();
        assert_eq!(join.origin, "fork");
        assert_eq!(join.branches.get("s0").unwrap().len(), count);
        assert_eq!(join.branches.get("short").unwrap().len(), 1);

        let mut budget = ExpansionBudget::default();
        budget
            .join_analysis(MAX_JOIN_ANALYSIS_ITEMS - count, None)
            .unwrap();
        let error = analyse_joins(&graph, &mut budget).unwrap_err();
        assert_eq!(error.limit_pointer.as_deref(), Some("/flow"));
        assert!(error.message.contains("join analysis items"));
    }

    #[test]
    fn validation_and_compiler_enforce_composition_depth() {
        let compose = |target: String, route: serde_json::Value| {
            serde_json::json!({
                "type":"compose", "composition":target, "next":{"done":route},
            })
        };
        let document = |depth: usize| {
            use serde_json::json;
            let bodies = (1..=depth)
                .map(|index| {
                    let output = json!({"output":"done"});
                    let step = if index == depth {
                        json!({"type":"agent","agent":"a","next":output})
                    } else {
                        compose(format!("body{}", index + 1), output)
                    };
                    (
                        format!("body{index}"),
                        json!({"entry":"step","outputs":["done"],"steps":{"step":step}}),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            json!({
                "contract":"agsdl-0.4.0","id":"depth",
                "agents":{"a":{"harness":"codex","permissions":"read-only"}},
                "flow":{"entry":"step","steps":{"step":compose("body1".into(), json!([]))}},
                "compositions":bodies,
            })
            .to_string()
        };
        let accepted = document(MAX_COMPOSITION_DEPTH);
        let report: serde_json::Value = serde_json::from_str(
            &crate::validate_document_json(accepted.as_bytes(), None).unwrap(),
        )
        .unwrap();
        assert_eq!(report["valid"], true, "{report}");
        let refused = document(MAX_COMPOSITION_DEPTH + 1);
        let pointer = format!("/compositions/body{}", MAX_COMPOSITION_DEPTH + 1);
        let report: serde_json::Value =
            serde_json::from_str(&crate::validate_document_json(refused.as_bytes(), None).unwrap())
                .unwrap();
        assert!(
            report["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| { entry["code"] == GRAPH_LIMIT_CODE && entry["path"] == pointer }),
            "{report}"
        );
        let parsed = crate::exact::decode(&refused).unwrap();
        let error = crate::compile::graph::compile(parsed.as_object().unwrap())
            .err()
            .unwrap();
        assert_eq!(error.code, GRAPH_LIMIT_CODE);
        assert_eq!(error.pointer.utf8().as_deref(), Some(pointer.as_str()));
    }

    #[test]
    fn utf16_order_differs_from_utf8_for_astral_characters() {
        let mut values = vec![
            "\u{FF61}".to_owned(),
            "\u{1F600}".to_owned(),
            "a".to_owned(),
        ];
        sort_utf16(&mut values);
        assert_eq!(values, ["a", "\u{1F600}", "\u{FF61}"]);
        assert_eq!(compare_addresses("a/b", "a"), Ordering::Greater);
        assert_eq!(local_name("#exit/x", ""), Some("#exit".to_owned()));
    }

    #[test]
    fn parallel_join_has_branches_in_route_order() {
        let text = r#"{"contract":"agsdl-0.3.0","id":"j","agents":{"a":{"harness":"x"}},"flow":{"entry":"s","steps":{
            "s":{"type":"agent","agent":"a","next":["l","r"]},
            "l":{"type":"agent","agent":"a","next":"j"},
            "r":{"type":"agent","agent":"a","next":"j"},
            "j":{"type":"join"}}}}"#;
        let parsed = parse_v030(text.as_bytes()).unwrap();
        let document = parsed.tree.as_object().unwrap();
        let graph =
            Graph::for_scope(document, None, None, &mut ExpansionBudget::default()).unwrap();
        let (infos, problems) = analyse_joins(&graph, &mut ExpansionBudget::default()).unwrap();
        assert!(problems.is_empty());
        let info = infos.get("j").unwrap();
        assert_eq!(info.origin, "s");
        assert_eq!(
            info.branches.keys().cloned().collect::<Vec<_>>(),
            ["l", "r"]
        );
    }
}
