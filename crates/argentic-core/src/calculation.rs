//! Encoded calculations used by the runtime. Authored JSON stays lossless.
use crate::argentic_core::{
    compile,
    native::{self, JsonDecoder, Text},
    native_json::Decoder,
};

/// Converts one ordinary native scalar to its existing canonical decimal or
/// non-finite marker. The runtime owns rounding; authored tokens bypass this port.
#[doc(hidden)]
pub type NativeNumber = fn(&str) -> Result<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub code: String,
    pub pointer: String,
    pub message: String,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.pointer, self.message)
    }
}
impl std::error::Error for Failure {}
impl From<native::Error> for Failure {
    fn from(e: native::Error) -> Self {
        Self {
            code: e.code.into(),
            pointer: e.pointer.lossy(),
            message: e.message.lossy(),
        }
    }
}
fn syntax(e: native::SyntaxError) -> Failure {
    Failure {
        code: "NATIVE_JSON".into(),
        pointer: String::new(),
        message: e.message.lossy(),
    }
}

/// Compile an authenticated authored document and its captured source/registry.
/// This computes data only; a result does not establish admission or ownership.
pub fn compile(
    authored: &str,
    source_json: &str,
    registry_json: &str,
    number: NativeNumber,
) -> Result<String, Failure> {
    compile_with_budget(
        authored,
        source_json,
        registry_json,
        number,
        &crate::SourceBudget::default(),
    )
}

/// Compile as part of a source operation with a shared budget.
pub fn compile_with_budget(
    authored: &str,
    source_json: &str,
    registry_json: &str,
    number: NativeNumber,
    budget: &crate::SourceBudget,
) -> Result<String, Failure> {
    budget.json_input(registry_json.as_bytes(), "/registry")?;
    budget.json_input(source_json.as_bytes(), "/source")?;
    budget.bytes(authored.len().saturating_mul(2), "")?;
    let decoder = Decoder(number);
    let registry = decoder.parse_source(registry_json, "/registry", budget)?;
    if !registry.get("format").eq_str("macro-agsdl-registry/030") {
        return Err(Failure {
            code: "CORRELATION".into(),
            pointer: "registry".into(),
            message: "Invalid captured registry".into(),
        });
    }
    let source = decoder.parse_source(source_json, "/source", budget)?;
    let source = compile::metadata::capture_source(&source)?;
    Ok(compile::host::compile(
        &native::s(authored),
        &source,
        &registry,
        budget,
    )?)
}

/// Evaluate the existing work journal protocol without executing an effect.
pub fn work(input_json: &str, number: NativeNumber) -> Result<String, Failure> {
    let decoder = Decoder(number);
    let input = decoder.parse(&Text::from(input_json)).map_err(syntax)?;
    crate::argentic_core::work::evaluate_host(&input, &decoder)
        .map(|v| v.stringify())
        .map_err(|e| match e {
            crate::argentic_core::work::Error::Failure(e) => e.into(),
            crate::argentic_core::work::Error::Exception { name, message } => Failure {
                code: name.lossy(),
                pointer: String::new(),
                message: message.lossy(),
            },
        })
}

/// Prepare routes, call data or human-clock data through the existing protocol.
pub fn route(input_json: &str, number: NativeNumber) -> Result<String, Failure> {
    crate::argentic_core::route::data::evaluate(&Text::from(input_json), &Decoder(number))
        .map(|v| v.stringify())
        .map_err(|e| match e {
            crate::argentic_core::calls_data::Error::Failure(e) => e.into(),
            crate::argentic_core::calls_data::Error::Exception { name, message } => Failure {
                code: name.lossy(),
                pointer: String::new(),
                message: message.lossy(),
            },
        })
}
