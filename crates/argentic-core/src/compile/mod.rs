pub(crate) mod configuration;
pub(crate) mod graph;
pub(crate) mod host;
pub(crate) mod metadata;
pub(crate) mod plan;

impl From<crate::graph::Unshaped> for crate::native::Error {
    fn from(error: crate::graph::Unshaped) -> Self {
        match error.limit_pointer {
            Some(pointer) => Self::new(crate::graph::GRAPH_LIMIT_CODE, pointer, error.message),
            None => Self::new(
                "COMPILATION",
                "",
                "Invalid compilation input or inconsistent dependency contract.",
            ),
        }
    }
}

pub(crate) fn limit_error(error: crate::calculation::Failure) -> crate::native::Error {
    crate::native::Error::new(crate::GRAPH_LIMIT_CODE, error.pointer, error.message)
}

fn encode(
    budget: &crate::SourceBudget,
    max: usize,
    write: impl Fn(&mut dyn std::fmt::Write) -> std::fmt::Result,
) -> Result<Option<String>, crate::native::Error> {
    crate::resource::encode_string(budget, max, "/flow", write).map_err(limit_error)
}
