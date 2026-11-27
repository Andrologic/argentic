use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HumanAnswer030 {
    Decision {
        decision: String,
        comment: String,
    },
    Approval {
        choice: String,
        comment: String,
    },
    /// The Call adapter must validate the exact answer against its attested
    /// protocol schema before delivering; a JSON object is not that proof.
    Question {
        answer_json: String,
    },
}

/// ECMAScript WhiteSpace and LineTerminator, as used by String.prototype.trim.
/// NEL (U+0085) is content; BOM (U+FEFF) is whitespace. Keep one native
/// definition for both resolution validation and the enforced plan lock.
pub(crate) fn human_comment_is_empty030(comment: &str) -> bool {
    comment.chars().all(|c| {
        matches!(c,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' |
            '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
            '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    })
}
