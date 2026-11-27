//! Pure Argentic engine logic for AgSDL 0.3.0 and 0.4.0.
//!
//! This crate validates and compiles authored documents, and calculates route,
//! work and human-clock transitions from captured data. The runtime owns I/O,
//! storage and native number conversion. Authored JSON stays lossless.
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

/// Whether an AgSDL result media type carries JSON, exactly as the published
/// AgSDL reference validator decides it.
pub fn is_json_media(media: &str) -> bool {
    media == "application/json" || media.ends_with("+json")
}

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

/// Optional local version probe for a bundled descriptive harness.
pub fn harness_version_probe(name: &str) -> Option<(&'static str, &'static str)> {
    reader::BUNDLED_VERSION_PROBES
        .iter()
        .find(|(id, _, _)| *id == name)
        .map(|(_, executable, argument)| (*executable, *argument))
}

/// Exact bundled harness-module bytes. Descriptive support never grants an effect.
pub fn harness_module(name: &str) -> Option<&'static str> {
    reader::PINNED_MODULES
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, text)| *text)
}
