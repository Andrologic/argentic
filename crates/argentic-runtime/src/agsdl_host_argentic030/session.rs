use argentic_harness::{providers::Message, r#loop::history_bytes};
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};

use crate::persistence::Result;

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
                    if encoded.len() <= bytes * 3 / 4 {
                        return Self::Compressed(encoded.into_boxed_slice());
                    }
                }
            }
        }
        messages.shrink_to_fit();
        Self::Messages(messages)
    }

    pub(super) fn wake(self) -> Result<Vec<Message>> {
        match self {
            Self::Messages(messages) => Ok(messages),
            Self::Compressed(bytes) => Ok(serde_json::from_reader(DeflateDecoder::new(&*bytes))?),
        }
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
        assert_eq!(idle.wake().unwrap(), history);
    }
}
