//! Argentic core: pure engine logic for AgSDL 0.3.
//!
//! D15 2.c ports the TypeScript evaluators here one sub-lot at a time; 2.c.1
//! brought exact JSON values, value schemas and work integers, 2.c.2 the
//! reader, the schema checker, flow graphs and static validation. 2.c.5a adds
//! descriptive routes and captured message resolution. 2.c.3 adds compilation,
//! contracts and explicit native data; numeric conversion stays at the boundary.
//! Nothing
//! outside tests calls this module before the 2.c.7 switch. The module must
//! not use the desktop shell, storage drivers, generated IPC types or IO; the
//! architecture guard counts forbidden names here, comments included. Besides
//! the standard library, the staged ports use the locked schema regex, serde
//! JSON data types, SHA-256 and the approved pure URL syntax parser.
pub(crate) mod calls_data;
pub(crate) mod clock;
pub(crate) mod compile;
pub(crate) mod contract;
pub(crate) mod error;
pub(crate) mod exact;
pub(crate) mod graph;
pub(crate) mod integers;
pub(crate) mod messages;
pub(crate) mod native;
pub(crate) mod reader;
mod resource;
pub(crate) mod route;
pub(crate) mod schema;

pub const AGSDL_040: &str = "agsdl-0.4.0";

/// Published document editions understood by the reader and runtime.
pub fn supports_contract(contract: &str) -> bool {
    matches!(contract, "agsdl-0.3.0" | AGSDL_040)
}
pub(crate) mod validate;
pub(crate) mod valueschema;
pub(crate) mod work;

extern crate self as argentic_core;

pub mod calculation;
mod native_json;

pub mod durable_json;
pub mod flow;

pub use graph::{
    GRAPH_LIMIT_CODE, MAX_COMPILED_GRAPH_JSON_BYTES, MAX_COMPOSITION_DEPTH,
    MAX_EXPANDED_GRAPH_EDGES, MAX_EXPANDED_GRAPH_NODES,
};
pub use reader::{
    check_source_json, parse_source_json, source_document_tokens_equal, source_json_equal,
    validate_document_json, validate_document_json_with_budget, validate_source,
};
pub use resource::{SourceBudget, MAX_SOURCE_BYTES, MAX_SOURCE_JSON_DEPTH, MAX_SOURCE_WORK};

/// Exact bundled harness-module bytes. Descriptive support never grants an effect.
pub fn harness_module(name: &str) -> Option<&'static str> {
    reader::PINNED_MODULES
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, text)| *text)
}
