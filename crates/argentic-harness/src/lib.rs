//! In-process agent execution for AgSDL 0.4.0.
#![deny(clippy::unwrap_used, clippy::expect_used)]

pub mod config;
pub mod r#loop;
pub mod policy;
pub mod providers;
pub mod tools;

pub const MODULE_JSON: &str = include_str!("../module.json");

#[doc(hidden)]
pub mod seatbelt;
