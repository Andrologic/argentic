// Adapted from Macro's reasoning_catalog.rs and reasoningCatalog.json.

use std::sync::LazyLock;

use regex::RegexSet;
use serde::Deserialize;

#[derive(Deserialize)]
struct Catalog {
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    provider_types: Vec<String>,
    model_patterns: Vec<String>,
    supported_efforts: Vec<String>,
    default_effort: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReasoningCapability {
    pub efforts: Vec<String>,
    pub default_effort: String,
}

type CatalogEntry = (Vec<String>, RegexSet, ReasoningCapability);

static CATALOG: LazyLock<Result<Vec<CatalogEntry>, String>> = LazyLock::new(|| {
    let catalog: Catalog =
        serde_json::from_str(include_str!("reasoning_catalog.json")).map_err(|e| e.to_string())?;
    catalog
        .entries
        .into_iter()
        .map(|entry| {
            let patterns = RegexSet::new(entry.model_patterns).map_err(|e| e.to_string())?;
            let capability = ReasoningCapability {
                efforts: entry.supported_efforts,
                default_effort: entry.default_effort,
            };
            Ok((entry.provider_types, patterns, capability))
        })
        .collect()
});

/// Catalog suggestions for host discovery. Explicit provider-defined efforts
/// remain open, and the catalog never changes an authored request.
pub fn reasoning_capability(
    provider: &str,
    model: &str,
) -> Result<Option<&'static ReasoningCapability>, String> {
    Ok(CATALOG
        .as_ref()
        .map_err(Clone::clone)?
        .iter()
        .find(|(providers, models, _)| {
            providers
                .iter()
                .any(|name| name.eq_ignore_ascii_case(provider))
                && models.is_match(model)
        })
        .map(|(_, _, capability)| capability))
}
