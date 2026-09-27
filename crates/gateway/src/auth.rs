//! Handshake authentication: HS256 JSON Web Tokens.
//!
//! A mobile or browser client authenticates once, in the WebSocket upgrade
//! request, and the identity it proves (`sub`) is bound to the connection
//! for its whole life. Nothing a client sends afterwards can change it.
//!
//! Only HS256 is accepted. The algorithm in the token header is checked
//! against that rather than trusted, which closes the `alg: none` and
//! algorithm-confusion holes that come from letting the token choose how
//! it is verified.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::hmac;
use serde::{Deserialize, Serialize};

/// Longest token the gateway will parse. A JWT carrying a subject and a
/// topic list is a few hundred bytes; a multi-kilobyte one is an attempt
/// to make the handshake expensive.
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_SUBJECT_BYTES: usize = 256;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("no credentials in the upgrade request")]
    Missing,
    #[error("malformed token: {0}")]
    Malformed(&'static str),
    #[error("unsupported token algorithm (only HS256 is accepted)")]
    Algorithm,
    #[error("unknown signing key id")]
    UnknownKey,
    #[error("bad signature")]
    Signature,
    #[error("token expired")]
    Expired,
    #[error("token not yet valid")]
    NotYetValid,
    #[error("wrong issuer")]
    Issuer,
    #[error("wrong audience")]
    Audience,
}

/// The claims the gateway reads. Unknown claims are ignored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Claims {
    /// The user (or device) the connection acts as. Required.
    pub sub: String,
    /// Expiry, seconds since the epoch. Required: a token that never
    /// expires is a password that cannot be rotated.
    pub exp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iat: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aud: Option<Audience>,
    /// Topic patterns this token may publish to (`orders`, `orders.*`,
    /// `*`). Narrows the gateway's own allow-list; never widens it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<String>>,
    /// Topic patterns this token may subscribe to. Narrows the gateway's
    /// `--allow-subscribe`; never widens it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<Vec<String>>,
}

/// `aud` may be one string or an array of them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn contains(&self, wanted: &str) -> bool {
        match self {
            Audience::One(a) => a == wanted,
            Audience::Many(all) => all.iter().any(|a| a == wanted),
        }
    }
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

/// One accepted signing key, optionally named so tokens can select it
/// with `kid`. Several keys at once is how a secret is rotated without
/// disconnecting everyone: add the new one, reissue tokens, drop the old.
pub struct SigningKey {
    pub kid: Option<String>,
    key: hmac::Key,
}

impl SigningKey {
    pub fn new(kid: Option<String>, secret: &[u8]) -> Self {
        SigningKey {
            kid,
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }

    /// Parse `kid:secret` or a bare `secret`.
    pub fn parse(spec: &str) -> Self {
        match spec.split_once(':') {
            Some((kid, secret)) if !kid.is_empty() => {
                SigningKey::new(Some(kid.to_owned()), secret.as_bytes())
            }
            _ => SigningKey::new(None, spec.as_bytes()),
        }
    }
}

pub struct Authenticator {
    keys: Vec<SigningKey>,
    issuer: Option<String>,
    audience: Option<String>,
    leeway_secs: i64,
}

impl Authenticator {
    pub fn new(
        keys: Vec<SigningKey>,
        issuer: Option<String>,
        audience: Option<String>,
        leeway_secs: i64,
    ) -> Self {
        Authenticator {
            keys,
            issuer,
            audience,
            leeway_secs,
        }
    }

    pub fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        self.verify_at(token, now_secs())
    }

    pub fn verify_at(&self, token: &str, now: i64) -> Result<Claims, AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::Malformed("token too long"));
        }
        let mut parts = token.split('.');
        let (Some(header_b64), Some(payload_b64), Some(signature_b64), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(AuthError::Malformed("expected three dot-separated parts"));
        };
        let header: Header = decode_json(header_b64, "header")?;
        if header.alg != "HS256" {
            return Err(AuthError::Algorithm);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|_| AuthError::Malformed("signature is not base64url"))?;
        let signed = &token.as_bytes()[..header_b64.len() + 1 + payload_b64.len()];

        // With a kid, only that key may verify: trying the rest would make
        // a retired key's tokens valid again under a new name.
        let verified = match &header.kid {
            Some(kid) => {
                let key = self
                    .keys
                    .iter()
                    .find(|k| k.kid.as_deref() == Some(kid))
                    .ok_or(AuthError::UnknownKey)?;
                hmac::verify(&key.key, signed, &signature).is_ok()
            }
            None => self
                .keys
                .iter()
                .any(|k| hmac::verify(&k.key, signed, &signature).is_ok()),
        };
        if !verified {
            return Err(AuthError::Signature);
        }

        // Claims are only looked at once the signature holds: before that
        // they are attacker-controlled text.
        let claims: Claims = decode_json(payload_b64, "payload")?;
        if claims.sub.is_empty() || claims.sub.len() > MAX_SUBJECT_BYTES {
            return Err(AuthError::Malformed("sub must be 1..=256 bytes"));
        }
        if now > claims.exp.saturating_add(self.leeway_secs) {
            return Err(AuthError::Expired);
        }
        if let Some(nbf) = claims.nbf {
            if now.saturating_add(self.leeway_secs) < nbf {
                return Err(AuthError::NotYetValid);
            }
        }
        if let Some(issuer) = &self.issuer {
            if claims.iss.as_deref() != Some(issuer.as_str()) {
                return Err(AuthError::Issuer);
            }
        }
        if let Some(audience) = &self.audience {
            if !claims.aud.as_ref().is_some_and(|a| a.contains(audience)) {
                return Err(AuthError::Audience);
            }
        }
        Ok(claims)
    }
}

fn decode_json<T: for<'de> Deserialize<'de>>(
    part: &str,
    what: &'static str,
) -> Result<T, AuthError> {
    let bytes = URL_SAFE_NO_PAD.decode(part).map_err(|_| {
        AuthError::Malformed(if what == "header" {
            "header is not base64url"
        } else {
            "payload is not base64url"
        })
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        AuthError::Malformed(if what == "header" {
            "header is not a JSON object"
        } else {
            "payload is not valid claims (sub and exp are required)"
        })
    })
}

/// Sign `claims` with HS256. For tests, the load generator and the
/// `mint-token` subcommand; production tokens come from your identity
/// provider, which shares the secret with the gateway.
pub fn sign_hs256(claims: &Claims, kid: Option<&str>, secret: &[u8]) -> String {
    let header = match kid {
        Some(kid) => serde_json::json!({"alg": "HS256", "typ": "JWT", "kid": kid}),
        None => serde_json::json!({"alg": "HS256", "typ": "JWT"}),
    };
    let mut token = URL_SAFE_NO_PAD.encode(header.to_string());
    token.push('.');
    token.push_str(&URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims")));
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let signature = hmac::sign(&key, token.as_bytes());
    token.push('.');
    token.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
    token
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(exp: i64) -> Claims {
        Claims {
            sub: "user-7".into(),
            exp,
            nbf: None,
            iat: None,
            iss: Some("idp".into()),
            aud: Some(Audience::One("gateway".into())),
            topics: None,
            subscribe: None,
        }
    }

    fn auth() -> Authenticator {
        Authenticator::new(
            vec![SigningKey::new(None, b"secret")],
            Some("idp".into()),
            Some("gateway".into()),
            0,
        )
    }

    #[test]
    fn a_valid_token_verifies() {
        let token = sign_hs256(&claims(2_000), None, b"secret");
        assert_eq!(auth().verify_at(&token, 1_000).unwrap().sub, "user-7");
    }

    #[test]
    fn expiry_issuer_audience_and_signature_are_enforced() {
        let a = auth();
        let token = sign_hs256(&claims(2_000), None, b"secret");
        assert_eq!(a.verify_at(&token, 2_001), Err(AuthError::Expired));
        let forged = sign_hs256(&claims(2_000), None, b"wrong");
        assert_eq!(a.verify_at(&forged, 1_000), Err(AuthError::Signature));
        let mut other = claims(2_000);
        other.iss = Some("evil".into());
        let token = sign_hs256(&other, None, b"secret");
        assert_eq!(a.verify_at(&token, 1_000), Err(AuthError::Issuer));
        let mut other = claims(2_000);
        other.aud = Some(Audience::Many(vec!["x".into(), "gateway".into()]));
        let token = sign_hs256(&other, None, b"secret");
        assert!(a.verify_at(&token, 1_000).is_ok());
        other.aud = None;
        let token = sign_hs256(&other, None, b"secret");
        assert_eq!(a.verify_at(&token, 1_000), Err(AuthError::Audience));
    }

    #[test]
    fn alg_none_and_tampered_payloads_are_refused() {
        let a = auth();
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims(2_000)).unwrap());
        let none_header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
        assert_eq!(
            a.verify_at(&format!("{none_header}.{payload}."), 1_000),
            Err(AuthError::Algorithm)
        );
        let token = sign_hs256(&claims(2_000), None, b"secret");
        let mut parts: Vec<&str> = token.split('.').collect();
        let mut elevated = claims(2_000);
        elevated.sub = "admin".into();
        let elevated = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&elevated).unwrap());
        parts[1] = &elevated;
        assert_eq!(
            a.verify_at(&parts.join("."), 1_000),
            Err(AuthError::Signature)
        );
        assert!(matches!(
            a.verify_at("not-a-jwt", 1_000),
            Err(AuthError::Malformed(_))
        ));
    }

    #[test]
    fn a_kid_selects_exactly_one_key() {
        let a = Authenticator::new(
            vec![
                SigningKey::parse("old:first"),
                SigningKey::parse("new:second"),
            ],
            None,
            None,
            0,
        );
        let mut c = claims(2_000);
        c.iss = None;
        c.aud = None;
        let token = sign_hs256(&c, Some("new"), b"second");
        assert!(a.verify_at(&token, 1_000).is_ok());
        // Signed by the old key but claiming the new one's id.
        let token = sign_hs256(&c, Some("new"), b"first");
        assert_eq!(a.verify_at(&token, 1_000), Err(AuthError::Signature));
        let token = sign_hs256(&c, Some("gone"), b"first");
        assert_eq!(a.verify_at(&token, 1_000), Err(AuthError::UnknownKey));
    }
}
