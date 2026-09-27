//! WebSocket gateway for Brahmaputra. See `gateway` for the design and
//! README.md for operating it.

pub mod auth;
pub mod config;
pub mod gateway;
pub mod http;
pub mod metrics;
pub mod protocol;

pub use config::GatewayConfig;
pub use gateway::{start, RunningGateway, SUBPROTOCOL};
