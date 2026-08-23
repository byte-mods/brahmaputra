//! SCRAM-SHA-256 credentials and the server half of RFC 5802.
//!
//! # Why a second credential exists at all
//!
//! The password hash a user record already carries is Argon2: deliberately
//! slow, deliberately one-way, and useless for SCRAM. SCRAM is not a
//! password check — it is a mutual challenge–response in which *neither*
//! side ever sends the password, and that requires the server to hold a key
//! derived from it in a specific way (PBKDF2 with a known salt and
//! iteration count) rather than a hash of whatever shape it likes.
//!
//! So both are stored. Argon2 remains what the dashboard login checks,
//! because there the password does arrive and the right property is that a
//! stolen database is expensive to attack. The SCRAM credential is what the
//! data plane uses, because there the right property is that the password
//! never crosses the wire at all — which is what makes authentication
//! meaningful on a plaintext listener, where sending it would not be.
//!
//! # What the server stores, and what it gives away
//!
//! `stored_key` and `server_key` are enough to *verify* a client and to
//! prove the server's own identity, and not enough to impersonate the
//! client to a third party — that would need `client_key`, which the server
//! never sees. A stolen credential store therefore lets an attacker
//! impersonate the *server*, not the user, which is the security property
//! SCRAM is designed around and the reason it is worth the extra state.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Iterations for PBKDF2. Kafka's default, and the number a client is told
/// in the server-first message, so changing it costs nothing to clients.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// The mechanism name as it appears on the wire.
pub const MECHANISM: &str = "SCRAM-SHA-256";

/// What a broker stores so it can complete a SCRAM exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredential {
    /// Base64, because it travels to the client verbatim in the
    /// server-first message.
    pub salt: String,
    pub iterations: u32,
    /// Base64 of `SHA256(HMAC(salted_password, "Client Key"))`.
    pub stored_key: String,
    /// Base64 of `HMAC(salted_password, "Server Key")`.
    pub server_key: String,
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// PBKDF2-HMAC-SHA256 with a single output block.
///
/// One block is all SCRAM-SHA-256 needs — the derived key is exactly the
/// hash length — so the general multi-block form would be code that is
/// never exercised.
fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(password, &block);
    let mut result = u.clone();
    for _ in 1..iterations {
        u = hmac(password, &u);
        for (accumulated, byte) in result.iter_mut().zip(u.iter()) {
            *accumulated ^= byte;
        }
    }
    result
}

impl ScramCredential {
    /// Derive a credential from a password, with a fresh random salt.
    pub fn derive(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
        let salted = pbkdf2(password.as_bytes(), salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(&client_key);
        let server_key = hmac(&salted, b"Server Key");
        ScramCredential {
            salt: BASE64.encode(salt),
            iterations,
            stored_key: BASE64.encode(stored_key),
            server_key: BASE64.encode(server_key),
        }
    }

    /// Whether `proof` (base64, from the client-final message) is correct
    /// for `auth_message`.
    ///
    /// The check is indirect by construction: the server recovers the
    /// client key from the proof and the signature it can compute, hashes
    /// it, and compares against what it stored. It never held the client
    /// key, so it cannot have produced the proof itself — which is exactly
    /// what makes a stolen credential store unable to impersonate a user.
    pub fn verify_proof(&self, auth_message: &str, proof: &str) -> bool {
        let (Ok(proof), Ok(stored_key)) = (BASE64.decode(proof), BASE64.decode(&self.stored_key))
        else {
            return false;
        };
        if proof.len() != stored_key.len() {
            return false;
        }
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let client_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(proof_byte, signature_byte)| proof_byte ^ signature_byte)
            .collect();
        Sha256::digest(&client_key).as_slice() == stored_key.as_slice()
    }

    /// The server's own proof, which the client checks to know it is not
    /// talking to an impostor.
    pub fn server_signature(&self, auth_message: &str) -> String {
        let Ok(server_key) = BASE64.decode(&self.server_key) else {
            return String::new();
        };
        BASE64.encode(hmac(&server_key, auth_message.as_bytes()))
    }

    /// The client-side computation, kept beside the server's so the two
    /// cannot drift apart. Used by the client driver and by the tests.
    pub fn client_proof(password: &str, salt: &str, iterations: u32, auth_message: &str) -> String {
        let Ok(salt) = BASE64.decode(salt) else {
            return String::new();
        };
        let salted = pbkdf2(password.as_bytes(), &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(&client_key);
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(key_byte, signature_byte)| key_byte ^ signature_byte)
            .collect();
        BASE64.encode(proof)
    }
}

/// A fresh salt.
pub fn random_salt() -> Vec<u8> {
    use rand_core::RngCore;
    let mut salt = vec![0u8; 16];
    rand_core::OsRng.fill_bytes(&mut salt);
    salt
}

/// A fresh nonce, base64 of random bytes so it is safe in the comma- and
/// equals-delimited SCRAM syntax.
pub fn random_nonce() -> String {
    use rand_core::RngCore;
    let mut bytes = [0u8; 18];
    rand_core::OsRng.fill_bytes(&mut bytes);
    BASE64.encode(bytes).replace(',', ".")
}

/// One `key=value` field out of a SCRAM message.
pub fn field<'a>(message: &'a str, key: &str) -> Option<&'a str> {
    message.split(',').find_map(|part| {
        part.strip_prefix(key)
            .and_then(|rest| rest.strip_prefix('='))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole exchange, both halves, as the broker and client run it.
    #[test]
    fn a_correct_password_produces_a_proof_the_server_accepts() {
        let credential = ScramCredential::derive("hunter2", &random_salt(), 4_096);
        let auth_message = "n=alice,r=abc,r=abcdef,s=salt,i=4096,c=biws,r=abcdef";
        let proof = ScramCredential::client_proof(
            "hunter2",
            &credential.salt,
            credential.iterations,
            auth_message,
        );
        assert!(credential.verify_proof(auth_message, &proof));
    }

    /// The property that makes SCRAM worth the extra state: the password
    /// never travels, and a wrong one cannot produce a valid proof.
    #[test]
    fn a_wrong_password_cannot_produce_a_valid_proof() {
        let credential = ScramCredential::derive("hunter2", &random_salt(), 4_096);
        let auth_message = "n=alice,r=abc,r=abcdef,s=salt,i=4096,c=biws,r=abcdef";
        let proof = ScramCredential::client_proof(
            "hunter3",
            &credential.salt,
            credential.iterations,
            auth_message,
        );
        assert!(!credential.verify_proof(auth_message, &proof));
    }

    /// A proof is bound to the exact exchange it was made in, so one
    /// captured from a previous session cannot be replayed into this one.
    #[test]
    fn a_proof_from_another_exchange_is_refused() {
        let credential = ScramCredential::derive("hunter2", &random_salt(), 4_096);
        let proof = ScramCredential::client_proof(
            "hunter2",
            &credential.salt,
            credential.iterations,
            "n=alice,r=one,r=onetwo,s=salt,i=4096,c=biws,r=onetwo",
        );
        assert!(!credential.verify_proof(
            "n=alice,r=three,r=threefour,s=salt,i=4096,c=biws,r=threefour",
            &proof
        ));
    }

    /// The client checks the server too. Without this, anything that can
    /// take the connection can collect proofs.
    #[test]
    fn the_server_proves_itself_as_well() {
        let credential = ScramCredential::derive("hunter2", &random_salt(), 4_096);
        let signature = credential.server_signature("auth");
        assert!(!signature.is_empty());
        let other = ScramCredential::derive("hunter2", &random_salt(), 4_096);
        assert_ne!(
            signature,
            other.server_signature("auth"),
            "a different salt is a different server key"
        );
    }

    /// PBKDF2-HMAC-SHA256 against RFC 6070's SHA-1 vectors is not
    /// applicable, so this pins the property that actually matters: the
    /// derivation depends on every input.
    #[test]
    fn every_input_changes_the_derived_key() {
        let salt = random_salt();
        let base = ScramCredential::derive("password", &salt, 4_096);
        assert_ne!(
            base.stored_key,
            ScramCredential::derive("password!", &salt, 4_096).stored_key
        );
        assert_ne!(
            base.stored_key,
            ScramCredential::derive("password", &random_salt(), 4_096).stored_key
        );
        assert_ne!(
            base.stored_key,
            ScramCredential::derive("password", &salt, 8_192).stored_key
        );
    }

    #[test]
    fn fields_are_read_out_of_a_scram_message() {
        let message = "n,,n=alice,r=nonce123";
        assert_eq!(field(message, "n"), Some("alice"));
        assert_eq!(field(message, "r"), Some("nonce123"));
        assert_eq!(field(message, "s"), None);
    }
}
