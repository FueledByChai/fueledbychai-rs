//! A certificate authority the test generates in memory, and the server side of TLS for the
//! local servers. No certificate or key is ever written to disk or committed (0009).

use std::sync::{Arc, Mutex};

use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// A CA generated for one test: its certificate is the trust anchor the client adds.
pub struct TestCa {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

impl TestCa {
    pub fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "fbc-runtime test CA");
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        TestCa { issuer }
    }

    /// The CA certificate, DER-encoded.
    pub fn der(&self) -> Vec<u8> {
        self.issuer.der().to_vec()
    }

    /// The server side of TLS with a certificate this CA issued for `names` (and only those).
    pub fn server(&self, names: &[&str]) -> TlsServer {
        let key = KeyPair::generate().unwrap();
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let cert = CertificateParams::new(names)
            .unwrap()
            .signed_by(&key, &self.issuer)
            .unwrap();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(cert.der().to_vec())], key)
            .unwrap();
        TlsServer {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            names: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

/// Accepts TLS for a local server and records the server name (SNI) each client sent.
#[derive(Clone)]
pub struct TlsServer {
    acceptor: TlsAcceptor,
    names: Arc<Mutex<Vec<Option<String>>>>,
}

impl TlsServer {
    /// The TLS stream, or `None` when the client gave up on the handshake.
    pub async fn accept(&self, stream: TcpStream) -> Option<TlsStream<TcpStream>> {
        let tls = self.acceptor.accept(stream).await.ok()?;
        let sni = tls.get_ref().1.server_name().map(str::to_owned);
        self.names.lock().unwrap().push(sni);
        Some(tls)
    }

    /// The server name each completed handshake carried, in order.
    pub fn server_names(&self) -> Vec<Option<String>> {
        self.names.lock().unwrap().clone()
    }
}
