use std::{collections::BTreeMap, sync::Mutex};

use argentic_harness::{providers::Message, r#loop::history_bytes};
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};

use crate::persistence::{failure, Result};

pub(super) enum IdleHistory {
    Messages(Vec<Message>),
    Compressed(Box<[u8]>),
}

impl IdleHistory {
    pub(super) fn sleep(mut messages: Vec<Message>) -> Self {
        let bytes = history_bytes(&messages);
        if bytes >= 64 * 1024 {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
            if serde_json::to_writer(&mut encoder, &messages).is_ok() {
                if let Ok(encoded) = encoder.finish() {
                    if encoded.len() <= bytes * 3 / 4
                        && Self::decode(&encoded).is_ok_and(|decoded| decoded == messages)
                    {
                        return Self::Compressed(encoded.into_boxed_slice());
                    }
                }
            }
        }
        messages.shrink_to_fit();
        Self::Messages(messages)
    }

    fn decode(bytes: &[u8]) -> std::io::Result<Vec<Message>> {
        use std::io::Read;

        // Inflate first: serde_json reads byte by byte from a reader.
        let mut json = Vec::with_capacity(bytes.len() * 4);
        DeflateDecoder::new(bytes).read_to_end(&mut json)?;
        Ok(serde_json::from_slice(&json)?)
    }

    /// Removes the instance history and decodes it outside the sessions lock.
    /// A history that cannot be decoded is put back unchanged.
    #[allow(clippy::result_large_err, reason = "Preserve by-value errors")]
    pub(super) fn take(
        sessions: &Mutex<BTreeMap<String, Self>>,
        instance: &str,
    ) -> Result<Vec<Message>> {
        let Some(idle) = sessions.lock().unwrap().remove(instance) else {
            return Ok(Vec::new());
        };
        let bytes = match idle {
            Self::Messages(messages) => return Ok(messages),
            Self::Compressed(bytes) => bytes,
        };
        Self::decode(&bytes).map_err(|_| {
            sessions
                .lock()
                .unwrap()
                .insert(instance.to_owned(), Self::Compressed(bytes));
            failure(
                "ARGENTIC_SESSION_HISTORY",
                "work",
                "Could not restore the continuous session history. The stored history was retained.",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_history_round_trips_provider_replay() {
        let mut history = vec![Message::User("texte français".repeat(8192))];
        history.push(
            serde_json::from_value(serde_json::json!({
                "Assistant": {
                    "text": "done",
                    "reasoning": "private",
                    "calls": [{"id":"call-1","name":"read","arguments":"{\"path\":\"file\"}"}],
                    "replay": {
                        "format": "Responses",
                        "model": {"provider":"local","model":"model","variant":null},
                        "items": [{"type":"reasoning","encrypted_content":"opaque"}]
                    }
                }
            }))
            .unwrap(),
        );
        history.push(Message::Tool {
            call_id: "call-1".into(),
            content: "résultat".into(),
        });
        let initial = history_bytes(&history);
        let idle = IdleHistory::sleep(history.clone());
        let IdleHistory::Compressed(compressed) = &idle else {
            panic!("large idle history should hibernate");
        };
        let retained = compressed.len() + std::mem::size_of::<IdleHistory>();
        println!("idle instance: {retained} B retained for {initial} B of conversation");
        assert!(retained < initial / 4);
        let sessions = Mutex::new(BTreeMap::from([("worker".into(), idle)]));
        assert_eq!(IdleHistory::take(&sessions, "worker").unwrap(), history);
        assert!(sessions.lock().unwrap().is_empty());
    }

    #[test]
    fn provider_replay_at_the_json_depth_limit_stays_live() {
        let wire = format!(
            r#"{{"item":{{"type":"reasoning","metadata":{}0{}}}}}"#,
            "[".repeat(124),
            "]".repeat(124)
        );
        let event: serde_json::Value = serde_json::from_str(&wire).unwrap();
        let assistant = serde_json::from_value(serde_json::json!({
            "Assistant": {
                "text": "",
                "reasoning": "",
                "calls": [],
                "replay": {
                    "format": "Responses",
                    "model": {"provider":"local","model":"model","variant":null},
                    "items": [event["item"].clone()]
                }
            }
        }))
        .unwrap();
        let history = vec![Message::User("large".repeat(16384)), assistant];
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
        serde_json::to_writer(&mut encoder, &history).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(IdleHistory::decode(&compressed).is_err());

        let idle = IdleHistory::sleep(history.clone());
        assert!(matches!(idle, IdleHistory::Messages(_)));
        let sessions = Mutex::new(BTreeMap::from([("worker".into(), idle)]));
        assert_eq!(IdleHistory::take(&sessions, "worker").unwrap(), history);

        // Verify waking a stored history from an older implementation is non-destructive.
        sessions.lock().unwrap().insert(
            "worker".into(),
            IdleHistory::Compressed(compressed.clone().into_boxed_slice()),
        );
        let error = IdleHistory::take(&sessions, "worker").unwrap_err();
        assert_eq!(error.code, "ARGENTIC_SESSION_HISTORY");
        assert!(
            matches!(&sessions.lock().unwrap()["worker"], IdleHistory::Compressed(bytes) if **bytes == compressed)
        );
    }
}
