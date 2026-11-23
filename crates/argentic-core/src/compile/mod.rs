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
    struct Count(usize, usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            if text.len() > self.1 - self.0 {
                return Err(std::fmt::Error);
            }
            self.0 += text.len();
            Ok(())
        }
    }
    let mut count = Count(0, max);
    if write(&mut count).is_err() {
        return Ok(None);
    }
    budget
        .work(count.0.div_ceil(8), "/flow")
        .map_err(limit_error)?;
    budget.bytes(count.0, "/flow").map_err(limit_error)?;
    let mut result = String::with_capacity(count.0);
    write(&mut result).expect("writing to a String");
    Ok(Some(result))
}
