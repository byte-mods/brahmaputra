//! Login, sessions and role checks (DESIGN.md §9.4).
//!
//! Users live in the Raft metadata log, so every broker can authenticate a
//! login locally without a round trip to the controller and without a
//! separate user database to keep in sync. Passwords are stored as argon2
//! hashes — never reversible, never logged. A successful login returns a
//! JWT signed with a cluster-wide secret that also lives in the metadata,
//! which is what lets a token issued by one broker be accepted by another.
//!
//! Roles are ordered: `viewer < operator < admin`. A route declares the
//! minimum role it needs and the middleware refuses anything lower, so
//! adding a route cannot accidentally default to "anyone".

use std::time::{SystemTime, UNIX_EPOCH};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use brahmaputra_metadata::Role;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// How long a session lasts before the user must log in again.
pub const SESSION_HOURS: i64 = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Wrong username or password. Deliberately one variant for both: a
    /// distinct "no such user" would let anyone enumerate accounts.
    InvalidCredentials,
    TokenExpired,
    TokenInvalid,
    /// Authenticated, but the role is below what the route requires.
    Forbidden,
    /// The cluster has no signing secret yet (controller still starting).
    NotReady,
}

impl AuthError {
    pub fn status(&self) -> u16 {
        match self {
            AuthError::InvalidCredentials | AuthError::TokenExpired | AuthError::TokenInvalid => {
                401
            }
            AuthError::Forbidden => 403,
            AuthError::NotReady => 503,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            AuthError::InvalidCredentials => "invalid username or password",
            AuthError::TokenExpired => "session expired",
            AuthError::TokenInvalid => "invalid session token",
            AuthError::Forbidden => "insufficient role",
            AuthError::NotReady => "cluster is not ready to authenticate",
        }
    }
}

/// JWT payload. `sub` is the username, `role` the role at issue time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// Hash a password for storage. Each call salts randomly, so the same
/// password never produces the same hash twice.
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut rand_core::OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AuthError::InvalidCredentials)
}

/// Constant-time-ish verification via argon2; a wrong password and an
/// unparseable hash both fail the same way.
pub fn verify_password(password: &str, stored_hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// Issue a session token for a user the caller has already authenticated.
pub fn issue_token(secret: &str, username: &str, role: Role) -> Result<String, AuthError> {
    let issued = now_secs();
    let claims = Claims {
        sub: username.to_owned(),
        role,
        iat: issued,
        exp: issued + SESSION_HOURS * 3600,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|_| AuthError::TokenInvalid)
}

/// Validate a token and return its claims.
pub fn verify_token(secret: &str, token: &str) -> Result<Claims, AuthError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &validation)
        .map(|data| data.claims)
        .map_err(|error| match error.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            _ => AuthError::TokenInvalid,
        })
}

/// Check a role against what a route requires.
pub fn require_role(claims: &Claims, required: Role) -> Result<(), AuthError> {
    if claims.role.permits(required) {
        Ok(())
    } else {
        Err(AuthError::Forbidden)
    }
}

/// Pull a bearer token out of an `Authorization` header.
pub fn bearer_token(header: Option<&str>) -> Option<&str> {
    header?
        .strip_prefix("Bearer ")
        .or_else(|| header?.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_verifies_against_its_own_hash_only() {
        let hash = hash_password("correct horse").unwrap();
        assert!(verify_password("correct horse", &hash));
        assert!(!verify_password("Correct horse", &hash));
        assert!(!verify_password("", &hash));
    }

    #[test]
    fn the_same_password_hashes_differently_every_time() {
        let first = hash_password("same").unwrap();
        let second = hash_password("same").unwrap();
        assert_ne!(first, second, "hashes are salted per call");
        assert!(verify_password("same", &first) && verify_password("same", &second));
    }

    #[test]
    fn a_hash_never_contains_the_password() {
        let hash = hash_password("hunter2").unwrap();
        assert!(!hash.contains("hunter2"));
    }

    #[test]
    fn a_token_round_trips_with_its_role() {
        let token = issue_token("secret", "alice", Role::Operator).unwrap();
        let claims = verify_token("secret", &token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.role, Role::Operator);
        assert!(claims.exp > claims.iat);
    }

    #[test]
    fn a_token_signed_with_another_secret_is_rejected() {
        let token = issue_token("secret", "alice", Role::Admin).unwrap();
        assert_eq!(
            verify_token("different", &token).unwrap_err(),
            AuthError::TokenInvalid
        );
    }

    #[test]
    fn an_expired_token_is_rejected_as_expired() {
        let claims = Claims {
            sub: "bob".into(),
            role: Role::Viewer,
            iat: now_secs() - 7200,
            exp: now_secs() - 3600,
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"secret"),
        )
        .unwrap();
        assert_eq!(
            verify_token("secret", &token).unwrap_err(),
            AuthError::TokenExpired
        );
    }

    #[test]
    fn roles_are_ordered_and_routes_refuse_anything_lower() {
        let viewer = Claims {
            sub: "v".into(),
            role: Role::Viewer,
            iat: 0,
            exp: 0,
        };
        let admin = Claims {
            sub: "a".into(),
            role: Role::Admin,
            iat: 0,
            exp: 0,
        };
        assert!(require_role(&viewer, Role::Viewer).is_ok());
        assert_eq!(
            require_role(&viewer, Role::Operator).unwrap_err(),
            AuthError::Forbidden
        );
        assert_eq!(
            require_role(&viewer, Role::Admin).unwrap_err(),
            AuthError::Forbidden
        );
        // Admin passes every gate, including the lower ones.
        for required in [Role::Viewer, Role::Operator, Role::Admin] {
            assert!(require_role(&admin, required).is_ok());
        }
    }

    #[test]
    fn bearer_tokens_are_extracted_and_junk_is_not() {
        assert_eq!(bearer_token(Some("Bearer abc")), Some("abc"));
        assert_eq!(bearer_token(Some("bearer abc")), Some("abc"));
        assert_eq!(bearer_token(Some("Basic abc")), None);
        assert_eq!(bearer_token(Some("Bearer ")), None);
        assert_eq!(bearer_token(None), None);
    }
}
