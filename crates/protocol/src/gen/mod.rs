//! Generated message types from `schemas/protocol.buff` (BitPacker).
//!
//! Regenerate with `bash scripts/gen-protocol.sh`, which produces
//! `structs.rs` (all message structs) and `impls.rs` (their codecs plus the
//! `ZeroCopyByteBuff` runtime). Both files are textually included below so
//! the impl blocks land in the same module scope as the structs.
//!
//! Generated code is std-only and has its own style; keep it as-is.

#![allow(clippy::all, dead_code, unused_imports)]

include!("structs.rs");
include!("impls.rs");
