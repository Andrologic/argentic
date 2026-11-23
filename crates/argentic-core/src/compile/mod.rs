pub(crate) mod configuration;
pub(crate) mod graph;
pub(crate) mod host;
pub(crate) mod metadata;
pub(crate) mod plan;
pub(crate) mod source;

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
