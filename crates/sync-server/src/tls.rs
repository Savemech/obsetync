use crate::ca;
use crate::storage::StorageLayout;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::sync::Arc;

/// Build the rustls ServerConfig with mTLS client cert verification.
pub fn build_tls_config(
    layout: &StorageLayout,
) -> Result<rustls::ServerConfig, Box<dyn std::error::Error>> {
    // Load CA cert as the trust anchor for client certs.
    let ca_der = ca::load_ca_cert_der(layout)?;
    let ca_cert = CertificateDer::from(ca_der);

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(ca_cert)?;

    // Client cert verifier: cert signed by our CA is accepted, but not required.
    // Mobile clients (iOS) cannot present client certs from JS — they authenticate
    // via bearer token at the application layer instead.
    let client_auth = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .allow_unauthenticated()
        .build()
        .map_err(|e| format!("client verifier: {}", e))?;

    // Load server cert chain and key.
    let (cert_chain_der, key_der) = ca::load_server_cert_and_key(layout)?;
    let certs: Vec<CertificateDer<'static>> = cert_chain_der
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der));

    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, key)
        .map_err(|e| format!("server config: {}", e))?;

    Ok(config)
}
