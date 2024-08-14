// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{Cursor, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use nativelink_config::stores::{ClientTlsConfig, GrpcEndpoint};
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{lookup_host, TcpStream};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tonic::transport::Uri;

pub fn make_tls_config(config: &Option<ClientTlsConfig>) -> Result<Option<ClientConfig>, Error> {
    let Some(config) = config else {
        return Ok(None);
    };
    let mut roots = RootCertStore::empty();
    {
        let cert = std::fs::read_to_string(&config.ca_file)?;
        for cert in rustls_pemfile::certs(&mut Cursor::new(cert)).collect::<Result<Vec<_>, _>>()? {
            roots
                .add(cert)
                .map_err(|e| make_input_err!("Could not add certificate to root store: {e:?}"))?;
        }
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);

    let config = if let Some(client_certificate) = &config.cert_file {
        let Some(client_key) = &config.key_file else {
            return Err(make_err!(
                Code::Internal,
                "Client certificate specified, but no key"
            ));
        };
        let cert = rustls_pemfile::certs(&mut Cursor::new(std::fs::read_to_string(
            client_certificate,
        )?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| make_input_err!("Could not parse client certificate: {e:?}"))?;

        let key = rustls_pemfile::private_key(&mut Cursor::new(client_key))
            .map_err(|e| make_input_err!("Could not parse client key: {e:?}"))?
            .err_tip(|| "No keys found in key file")?;

        builder
            .with_client_auth_cert(cert, key)
            .map_err(|e| make_input_err!("Could not set client auth: {e:?}"))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Some(config))
}

pub async fn make_tcp_or_tls_stream(
    config: &Option<ClientTlsConfig>,
    uri: &Uri,
) -> Result<MaybeTlsStream, Error> {
    let host = uri
        .host()
        .ok_or_else(|| make_input_err!("No host in URI"))?
        .to_string();
    let port = uri.port_u16().unwrap_or_else(|| {
        if uri.scheme_str() == Some("http") || uri.scheme_str() == Some("grpc") {
            80
        } else {
            443
        }
    });
    let addr = lookup_host((host.clone(), port))
        .await
        .map_err(|e| make_input_err!("Could not resolve host {host}:{port}: {e:?}"))?
        .next()
        .ok_or_else(|| make_input_err!("No dns records found for {host}:{port}"))?;
    let tcp = TcpStream::connect(addr)
        .await
        .err_tip(|| "Could not create tcp stream")?;

    let Some(config) = make_tls_config(config).err_tip(|| "in make_tcp_or_tls_stream()")? else {
        // No TLS configuration, just return the TCP stream.
        return Ok(MaybeTlsStream::Tcp(tcp));
    };

    let hostname = ServerName::try_from(host.clone())
        .map_err(|e| make_input_err!("Could not parse hostname {host}: {e:?}"))?;
    let tls = TlsConnector::from(Arc::new(config))
        .connect(hostname, tcp)
        .await
        .err_tip(|| "Could not connect with TLS")?;
    Ok(MaybeTlsStream::Tls(tls))
}

pub enum MaybeTlsStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
            MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tcp(tcp) => Pin::new(tcp).poll_shutdown(cx),
            MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Tcp(tcp) => Pin::new(tcp).poll_write_vectored(cx, bufs),
            MaybeTlsStream::Tls(tls) => Pin::new(tls).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            MaybeTlsStream::Tcp(tcp) => tcp.is_write_vectored(),
            MaybeTlsStream::Tls(tls) => tls.is_write_vectored(),
        }
    }
}

pub fn load_client_config(
    config: &Option<ClientTlsConfig>,
) -> Result<Option<tonic::transport::ClientTlsConfig>, Error> {
    let Some(config) = config else {
        return Ok(None);
    };

    let read_config = tonic::transport::ClientTlsConfig::new().ca_certificate(
        tonic::transport::Certificate::from_pem(std::fs::read_to_string(&config.ca_file)?),
    );
    let config = if let Some(client_certificate) = &config.cert_file {
        let Some(client_key) = &config.key_file else {
            return Err(make_err!(
                Code::Internal,
                "Client certificate specified, but no key"
            ));
        };
        read_config.identity(tonic::transport::Identity::from_pem(
            std::fs::read_to_string(client_certificate)?,
            std::fs::read_to_string(client_key)?,
        ))
    } else {
        if config.key_file.is_some() {
            return Err(make_err!(
                Code::Internal,
                "Client key specified, but no certificate"
            ));
        }
        read_config
    };

    Ok(Some(config))
}

pub fn endpoint_from(
    endpoint: &str,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
) -> Result<tonic::transport::Endpoint, Error> {
    let endpoint = Uri::try_from(endpoint)
        .map_err(|e| make_err!(Code::Internal, "Unable to parse endpoint {endpoint}: {e:?}"))?;

    // Tonic uses the TLS configuration if the scheme is "https", so replace
    // grpcs with https.
    let endpoint = if endpoint.scheme_str() == Some("grpcs") {
        let mut parts = endpoint.into_parts();
        parts.scheme = Some("https".parse().map_err(|e| {
            make_err!(
                Code::Internal,
                "https is an invalid scheme apparently? {e:?}"
            )
        })?);
        parts.try_into().map_err(|e| {
            make_err!(
                Code::Internal,
                "Error changing Uri from grpcs to https: {e:?}"
            )
        })?
    } else {
        endpoint
    };

    let endpoint_transport = if let Some(tls_config) = tls_config {
        let Some(authority) = endpoint.authority() else {
            return Err(make_input_err!(
                "Unable to determine authority of endpont: {endpoint}"
            ));
        };
        if endpoint.scheme_str() != Some("https") {
            return Err(make_input_err!(
                "You have set TLS configuration on {endpoint}, but the scheme is not https or grpcs"
            ));
        }
        let tls_config = tls_config.domain_name(authority.host());
        tonic::transport::Endpoint::from(endpoint)
            .tls_config(tls_config)
            .map_err(|e| make_input_err!("Setting mTLS configuration: {e:?}"))?
    } else {
        tonic::transport::Endpoint::from(endpoint)
    };

    Ok(endpoint_transport)
}

pub fn endpoint(endpoint_config: &GrpcEndpoint) -> Result<tonic::transport::Endpoint, Error> {
    let endpoint = endpoint_from(
        &endpoint_config.address,
        load_client_config(&endpoint_config.tls_config)?,
    )?;
    if let Some(concurrency_limit) = endpoint_config.concurrency_limit {
        Ok(endpoint.concurrency_limit(concurrency_limit))
    } else {
        Ok(endpoint)
    }
}
