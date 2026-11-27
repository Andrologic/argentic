#![allow(dead_code)]

use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Json, Router,
};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub const WAIT_FOR: Duration = Duration::from_secs(15);

pub struct Request {
    pub body: Value,
    pub headers: HeaderMap,
    respond: oneshot::Sender<Response>,
}

impl Request {
    pub fn send(self, status: StatusCode, body: impl Into<Body>, content_type: &str) {
        assert!(self
            .respond
            .send(
                Response::builder()
                    .status(status)
                    .header("content-type", content_type)
                    .body(body.into())
                    .unwrap()
            )
            .is_ok());
    }

    pub fn tool_response(call: &str, args: Value) -> String {
        let chunk = json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"same-wire-id",
            "function":{"name":call,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}]});
        format!("data: {chunk}\n\ndata: [DONE]\n\n")
    }

    pub fn tool(self, call: &str, args: Value) {
        self.send(
            StatusCode::OK,
            Self::tool_response(call, args),
            "text/event-stream",
        );
    }
}

pub struct Server {
    pub base_url: String,
    requests: mpsc::Receiver<Request>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    pub async fn start() -> Self {
        Self::start_at("/v1/chat/completions").await
    }

    pub async fn start_at(path: &str) -> Self {
        async fn respond(
            State(send): State<mpsc::Sender<Request>>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Response {
            let (respond, receive) = oneshot::channel();
            send.send(Request {
                body,
                headers,
                respond,
            })
            .await
            .unwrap();
            receive.await.unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let (send, requests) = mpsc::channel(4);
        let app = Router::new().route(path, post(respond)).with_state(send);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base_url,
            requests,
            task,
        }
    }

    pub async fn next(&mut self) -> Request {
        tokio::time::timeout(WAIT_FOR, self.requests.recv())
            .await
            .expect("provider request timed out")
            .unwrap()
    }

    pub fn config(&self, key_env: Option<&str>) -> super::ArgenticConfig030 {
        serde_json::from_value(config(&self.base_url, key_env)).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn config(url: &str, key_env: Option<&str>) -> Value {
    json!({"providers":{"local":{"protocol":"chat-completions","baseUrl":url,
        "models":["gpt-5.5#fixture"],"reasoningCatalog":"openai","apiKeyEnv":key_env}}})
}

pub fn document() -> Value {
    json!({"contract":"agsdl-0.4.0","id":"argentic-fixture",
        "agents":{"worker":{"harness":"argentic","model":{"name":"local/gpt-5.5#fixture","effort":"high"},
            "permissions":"read-only","harnessSettings":{"maxOutputTokens":256},
            "results":{"report":{"required":true,"schema":{"type":"object","required":["body"],
                "properties":{"body":{"type":"string"}}}}}}},
        "flow":{"entry":"read","steps":{"read":{"type":"agent","agent":"worker","instance":"new",
            "prompt":"Read memo.txt and submit its content.","next":[]}}}})
}
