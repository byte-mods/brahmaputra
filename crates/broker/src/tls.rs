//! TLS identity for the data plane: whose certificate the broker presents,
//! and whose certificates it will accept.
//!
//! Until now a broker generated a fresh self-signed certificate at startup
//! and asked for nothing from the client. That buys confidentiality on the
//! wire and nothing else: a client cannot tell the broker apart from
//! anything else that answers on the port, and the broker cannot tell one
//! client from another at all. Both directions matter, and they are
//! separable —
//!
//! * **`--tls-cert`/`--tls-key`** let an operator present a certificate
//!   from their own CA, so a client can *verify* it is talking to the real
//!   broker instead of accepting whatever it is handed.
//! * **`--tls-client-ca`** makes the broker require a client certificate
//!   signed by that CA, and binds the certificate's common name to the
//!   connection as its principal. That is authentication with no password
//!   crossing the wire at all, and it is what makes ACLs enforceable
//!   against a client that never calls `Authenticate`.
//!
//! Leaving all three unset keeps the previous behaviour exactly, so a
//! development broker still starts with no files to create.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::BrokerError;

/// Where the broker's TLS material comes from.
#[derive(Debug, Clone, Default)]
pub struct TlsIdentity {
    /// PEM certificate chain to present. `None` generates a self-signed one
    /// at startup, as before.
    pub cert_path: Option<PathBuf>,
    /// PEM private key matching `cert_path`.
    pub key_path: Option<PathBuf>,
    /// PEM CA bundle that client certificates must chain to. `Some`
    /// *requires* a client certificate: a connection without one is refused
    /// during the handshake rather than allowed through as anonymous.
    pub client_ca_path: Option<PathBuf>,
}

impl TlsIdentity {
    pub fn requires_client_certificates(&self) -> bool {
        self.client_ca_path.is_some()
    }
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, BrokerError> {
    let file = File::open(path)
        .map_err(|error| BrokerError::Meta(format!("cannot open {}: {error}", path.display())))?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            BrokerError::Meta(format!("cannot read certificates from {}: {error}", path.display()))
        })?;
    if certificates.is_empty() {
        return Err(BrokerError::Meta(format!(
            "{} contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, BrokerError> {
    let file = File::open(path)
        .map_err(|error| BrokerError::Meta(format!("cannot open {}: {error}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| {
            BrokerError::Meta(format!("cannot read a key from {}: {error}", path.display()))
        })?
        .ok_or_else(|| BrokerError::Meta(format!("{} contains no private key", path.display())))
}

/// The certificate chain and key the broker should present.
pub(crate) fn server_identity(
    identity: &TlsIdentity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), BrokerError> {
    match (&identity.cert_path, &identity.key_path) {
        (Some(cert), Some(key)) => Ok((read_certificates(cert)?, read_private_key(key)?)),
        (None, None) => {
            let (cert, key) = crate::quic::self_signed_identity()?;
            Ok((vec![cert], key.into()))
        }
        // Half a configured identity is a misconfiguration, not a default:
        // silently falling back to self-signed would mean an operator who
        // typoed one flag gets an unverifiable broker and no warning.
        _ => Err(BrokerError::Meta(
            "--tls-cert and --tls-key must be given together".into(),
        )),
    }
}

/// Build the rustls server configuration for this identity.
pub(crate) fn server_config(identity: &TlsIdentity) -> Result<rustls::ServerConfig, BrokerError> {
    let (chain, key) = server_identity(identity)?;
    let builder = match &identity.client_ca_path {
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            for certificate in read_certificates(ca_path)? {
                roots.add(certificate).map_err(|error| {
                    BrokerError::Meta(format!("cannot trust {}: {error}", ca_path.display()))
                })?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| {
                    BrokerError::Meta(format!("cannot build client verifier: {error}"))
                })?;
            rustls::ServerConfig::builder().with_client_cert_verifier(verifier)
        }
        None => rustls::ServerConfig::builder().with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(|error| BrokerError::Meta(format!("cannot build tls config: {error}")))?;
    config.alpn_protocols = vec![crate::quic::ALPN.to_vec()];
    Ok(config)
}

/// One DER element: its tag, its contents, and where the next one starts.
struct DerElement<'a> {
    tag: u8,
    contents: &'a [u8],
    end: usize,
}

/// Read the DER element at `start`.
///
/// Handles the definite-length forms only — short form, and long form up to
/// four length bytes. Indefinite length is not legal in DER, so refusing it
/// is correctness rather than a limitation.
fn read_element(der: &[u8], start: usize) -> Option<DerElement<'_>> {
    let tag = *der.get(start)?;
    let first_length_byte = *der.get(start + 1)?;
    let (length, header_len) = if first_length_byte & 0x80 == 0 {
        (usize::from(first_length_byte), 2)
    } else {
        let count = usize::from(first_length_byte & 0x7f);
        if count == 0 || count > 4 {
            return None;
        }
        let bytes = der.get(start + 2..start + 2 + count)?;
        let mut length = 0usize;
        for byte in bytes {
            length = length.checked_mul(256)?.checked_add(usize::from(*byte))?;
        }
        (length, 2 + count)
    };
    let contents_start = start + header_len;
    let end = contents_start.checked_add(length)?;
    Some(DerElement {
        tag,
        contents: der.get(contents_start..end)?,
        end,
    })
}

/// The common name of a verified peer certificate, used as the principal.
///
/// Read out of the certificate's **subject**, which is the field the CA
/// bound to this client. Nothing the client says at the protocol level is
/// consulted: a name a client could choose for itself would authorize
/// exactly nothing.
///
/// The subject has to be located properly rather than by scanning for the
/// common-name OID, because in an X.509 certificate the *issuer* name comes
/// first and carries the same OID. Scanning finds the CA's own name — which
/// means every client the CA ever signed would present as the same
/// principal, and ACLs could not tell two of them apart. So this walks the
/// TBSCertificate fields in order:
///
/// ```text
/// TBSCertificate ::= SEQUENCE {
///   version         [0] EXPLICIT Version OPTIONAL,
///   serialNumber        CertificateSerialNumber,
///   signature           AlgorithmIdentifier,
///   issuer              Name,
///   validity            Validity,
///   subject             Name,          <- this one
///   ... }
/// ```
///
/// Returns `None` for a certificate with no common name, which is refused
/// rather than mapped to an empty principal — an empty principal would
/// match an ACL written for `""`, and that is not an identity anybody meant
/// to grant.
pub(crate) fn common_name(certificate: &CertificateDer<'_>) -> Option<String> {
    let der = certificate.as_ref();
    // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signature }
    let tbs_certificate = read_element(read_element(der, 0)?.contents, 0)?;
    let tbs = tbs_certificate.contents;

    let mut cursor = 0;
    // The version tag is context-specific [0] and optional; without it the
    // certificate is v1 and serialNumber comes first.
    let first = read_element(tbs, cursor)?;
    if first.tag == 0xa0 {
        cursor = first.end;
    }
    // serialNumber, signature, issuer, validity — skipped in order.
    for _ in 0..4 {
        cursor = read_element(tbs, cursor)?.end;
    }
    let subject = read_element(tbs, cursor)?;
    common_name_in_name(subject.contents)
}

/// The common name inside a DER `Name` (a SEQUENCE of RelativeDistinguishedName
/// SETs, each holding AttributeTypeAndValue SEQUENCEs).
fn common_name_in_name(name: &[u8]) -> Option<String> {
    /// OID 2.5.4.3, id-at-commonName.
    const COMMON_NAME_OID: [u8; 3] = [0x55, 0x04, 0x03];
    let mut cursor = 0;
    while cursor < name.len() {
        let rdn = read_element(name, cursor)?;
        cursor = rdn.end;
        let mut attribute_cursor = 0;
        while attribute_cursor < rdn.contents.len() {
            let attribute = read_element(rdn.contents, attribute_cursor)?;
            attribute_cursor = attribute.end;
            let oid = read_element(attribute.contents, 0)?;
            if oid.contents != COMMON_NAME_OID {
                continue;
            }
            let value = read_element(attribute.contents, oid.end)?;
            let text = std::str::from_utf8(value.contents).ok()?;
            if text.is_empty() {
                return None;
            }
            return Some(text.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A certificate whose *subject* common name is `name`.
    ///
    /// `generate_simple_self_signed` puts its argument in the subject
    /// alternative name and leaves the common name at rcgen's default, so
    /// building the distinguished name explicitly is the only way to test
    /// what the principal is actually derived from.
    fn certificate_named(name: &str) -> rustls_pki_types::CertificateDer<'static> {
        let mut params = rcgen::CertificateParams::new(Vec::new()).expect("certificate params");
        let mut subject = rcgen::DistinguishedName::new();
        subject.push(rcgen::DnType::CommonName, name);
        params.distinguished_name = subject;
        let key = rcgen::KeyPair::generate().expect("key pair");
        params
            .self_signed(&key)
            .expect("self-signed certificate")
            .der()
            .clone()
    }

    /// A CA, and two client certificates it signs.
    ///
    /// The issuer name differs from both subject names, which is the whole
    /// point: a self-signed certificate has issuer == subject and so cannot
    /// tell a parser that reads the wrong field from one that reads the
    /// right one.
    fn ca_signed(names: &[&str]) -> Vec<rustls_pki_types::CertificateDer<'static>> {
        let mut ca_params =
            rcgen::CertificateParams::new(Vec::new()).expect("certificate authority params");
        let mut ca_subject = rcgen::DistinguishedName::new();
        ca_subject.push(rcgen::DnType::CommonName, "example-certificate-authority");
        ca_params.distinguished_name = ca_subject;
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().expect("ca key pair");
        let ca = ca_params.self_signed(&ca_key).expect("ca certificate");

        names
            .iter()
            .map(|name| {
                let mut params =
                    rcgen::CertificateParams::new(Vec::new()).expect("certificate params");
                let mut subject = rcgen::DistinguishedName::new();
                subject.push(rcgen::DnType::CommonName, *name);
                params.distinguished_name = subject;
                let key = rcgen::KeyPair::generate().expect("key pair");
                params
                    .signed_by(&key, &ca, &ca_key)
                    .expect("signed certificate")
                    .der()
                    .clone()
            })
            .collect()
    }

    #[test]
    fn the_principal_is_the_subject_common_name() {
        assert_eq!(
            common_name(&certificate_named("alice")).as_deref(),
            Some("alice")
        );
        let simple = rcgen::generate_simple_self_signed(vec!["san-only".to_owned()])
            .expect("generate certificate");
        assert_ne!(
            common_name(&simple.cert.der().clone()).as_deref(),
            Some("san-only"),
            "a subject alternative name is not an identity to authorize against"
        );
    }

    #[test]
    fn two_clients_of_one_authority_are_distinct_principals() {
        let signed = ca_signed(&["alice", "mallory"]);
        // Reading the issuer instead of the subject would make both of
        // these "example-certificate-authority" — one identity for every
        // client the CA ever signed, and ACLs unable to separate them.
        assert_eq!(common_name(&signed[0]).as_deref(), Some("alice"));
        assert_eq!(common_name(&signed[1]).as_deref(), Some("mallory"));
    }

    #[test]
    fn half_an_identity_is_refused_rather_than_silently_self_signed() {
        let identity = TlsIdentity {
            cert_path: Some(PathBuf::from("cert.pem")),
            key_path: None,
            client_ca_path: None,
        };
        let error = server_identity(&identity).unwrap_err();
        assert!(error.to_string().contains("must be given together"));
    }

    #[test]
    fn no_paths_at_all_still_produces_a_working_self_signed_identity() {
        let (chain, _key) = server_identity(&TlsIdentity::default()).expect("self-signed");
        assert_eq!(chain.len(), 1);
    }
}
