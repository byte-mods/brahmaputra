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

#[cfg(test)]
mod hostile_input_tests {
    use super::*;

    #[test]
    fn every_truncated_prefix_is_rejected_without_crossing_an_unsafe_boundary() {
        let response = AuthenticateResponse {
            error_code: 0,
            principal: "alice".into(),
            role: "operator".into(),
            payload: "server-proof".into(),
            done: true,
        }
        .encode()
        .unwrap();

        for cut in 0..response.len() {
            assert!(
                AuthenticateResponse::decode(&response[..cut]).is_err(),
                "truncated prefix {cut} was accepted"
            );
        }
    }

    #[test]
    fn invalid_utf8_overlong_varints_and_impossible_collections_are_rejected() {
        let mut invalid_utf8 = ZeroCopyByteBuff::new_writer(16, Endian::Big);
        invalid_utf8.put_str(VERSION);
        invalid_utf8.put_i32(1);
        let mut invalid_utf8 = invalid_utf8.finish();
        invalid_utf8.push(0xff);
        assert!(ProduceRequest::decode(&invalid_utf8).is_err());

        assert!(FetchRequest::decode(&[0x80; 10]).is_err());

        let mut impossible_collection = ZeroCopyByteBuff::new_writer(16, Endian::Big);
        impossible_collection.put_str(VERSION);
        impossible_collection.put_i32(i32::MAX);
        assert!(MetadataRequest::decode(&impossible_collection.finish()).is_err());
    }

    #[test]
    fn arbitrary_one_byte_payloads_are_errors_not_panics() {
        for byte in 0_u8..=u8::MAX {
            assert!(FetchRequest::decode(&[byte]).is_err());
            assert!(MetadataResponse::decode(&[byte]).is_err());
            assert!(AuthenticateResponse::decode(&[byte]).is_err());
        }
    }
}
