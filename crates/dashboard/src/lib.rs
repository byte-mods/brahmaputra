//! Embedded operations surface: metrics API, admin endpoints, login with
//! role-based access, and a dashboard served from the same binary
//! (DESIGN.md §9.2–§9.4).
//!
//! Every broker runs this on its own HTTP port, separate from the data
//! plane. There is nothing to install and nothing external to run: the
//! metrics come from the in-process registry, the users come from the Raft
//! metadata, and the UI is a single static page compiled into the binary.

mod auth;
mod routes;
mod ui;

pub use auth::{hash_password, AuthError, Claims, SESSION_HOURS};
pub use routes::{router, serve, ControllerClient, DashboardState};
