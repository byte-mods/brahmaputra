//! Client-side TLS material.
//!
//! Two independent decisions, and both used to be made for the client with
//! no way to change them:
//!
//! * **Whose broker is this?** The default accepts any certificate, which
//!   buys confidentiality but not authenticity — anything that answers on
//!   the port is accepted. Pointing `ca` at the CA that signed the broker's
//!   certificate turns that into a real check.
//! * **Who is this client?** A broker started with `--tls-client-ca`
//!   demands a certificate, and derives the connection's principal from its
//!   common name. `cert`/`key` are how a client presents one, and that is
//!   authentication with no password crossing the wire.
//!
//! Leaving all of it unset preserves the previous behaviour exactly.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::ClientError;

/// TLS material for connecting to a broker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsSettings {
    /// PEM CA bundle the broker's certificate must chain to. `None` accepts
    /// any certificate, which is the development default.
    pub ca_path: Option<PathBuf>,
    /// PEM certificate chain to present to the broker.
    pub cert_path: Option<PathBuf>,
    /// PEM private key matching `cert_path`.
    pub key_path: Option<PathBuf>,
    /// Name to check the broker's certificate against. Brokers that
    /// generate their own certificate use `brahmaputra`.
    pub server_name: Option<String>,
}

impl TlsSettings {
    pub fn is_default(&self) -> bool {
        self == &TlsSettings::default()
    }

    /// The name to validate the server certificate against.
    pub(crate) fn server_name(&self) -> &str {
        self.server_name.as_deref().unwrap_or("brahmaputra")
    }
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, ClientError> {
    let file = File::open(path).map_err(|error| {
        ClientError::Configuration(format!("cannot open {}: {error}", path.display()))
    })?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ClientError::Configuration(format!(
                "cannot read certificates from {}: {error}",
                path.display()
            ))
        })?;
    if certificates.is_empty() {
        return Err(ClientError::Configuration(format!(
            "{} contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, ClientError> {
    let file = File::open(path).map_err(|error| {
        ClientError::Configuration(format!("cannot open {}: {error}", path.display()))
    })?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| {
            ClientError::Configuration(format!(
                "cannot read a key from {}: {error}",
                path.display()
            ))
        })?
        .ok_or_else(|| {
            ClientError::Configuration(format!("{} contains no private key", path.display()))
        })
}

/// Build the rustls client configuration these settings describe.
pub(crate) fn client_config(
    settings: &TlsSettings,
) -> Result<rustls::ClientConfig, ClientError> {
    let builder = match &settings.ca_path {
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            for certificate in read_certificates(ca_path)? {
                roots.add(certificate).map_err(|error| {
                    ClientError::Configuration(format!(
                        "cannot trust {}: {error}",
                        ca_path.display()
                    ))
                })?;
            }
            rustls::ClientConfig::builder().with_root_certificates(roots)
        }
        // No CA given: accept whatever the broker presents. Confidentiality
        // without authenticity, which is what this has always done, and why
        // `--tls-ca` exists.
        None => rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(crate::quic::AcceptAnyServerCert)),
    };

    let mut config = match (&settings.cert_path, &settings.key_path) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(read_certificates(cert)?, read_private_key(key)?)
            .map_err(|error| {
                ClientError::Configuration(format!("cannot use client certificate: {error}"))
            })?,
        (None, None) => builder.with_no_client_auth(),
        // Half an identity is a misconfiguration. Falling back to "no
        // client certificate" would turn a typo into an authentication
        // failure at the broker, three layers away from the cause.
        _ => {
            return Err(ClientError::Configuration(
                "--tls-cert and --tls-key must be given together".into(),
            ))
        }
    };
    config.alpn_protocols = vec![crate::quic::ALPN.to_vec()];
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_settings_build_a_config_that_needs_no_files() {
        let config = client_config(&TlsSettings::default()).expect("default config");
        assert_eq!(config.alpn_protocols, vec![crate::quic::ALPN.to_vec()]);
    }

    #[test]
    fn half_a_client_identity_is_refused() {
        let settings = TlsSettings {
            cert_path: Some(PathBuf::from("client.pem")),
            ..TlsSettings::default()
        };
        let error = client_config(&settings).unwrap_err();
        assert!(error.to_string().contains("must be given together"));
    }

    #[test]
    fn the_server_name_defaults_to_the_generated_certificates_name() {
        assert_eq!(TlsSettings::default().server_name(), "brahmaputra");
        let named = TlsSettings {
            server_name: Some("broker.example.com".into()),
            ..TlsSettings::default()
        };
        assert_eq!(named.server_name(), "broker.example.com");
    }
}
