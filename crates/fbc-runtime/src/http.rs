//! The HTTP/1.1 call (`http://`), on the one connector.

use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::client::conn::http1;
use hyper::header::{HOST, HeaderValue};
use hyper_util::rt::TokioIo;

pub use hyper::body::Bytes;
pub use hyper::{Method, Request, Response, StatusCode, Uri, header};

use crate::connector::Connector;
use crate::error::{Cause, NetError, Step};
use crate::target;

impl Connector {
    /// Sends `request`, whose URI is an absolute `http://` URL, on a new connection through
    /// this connector, and reads the whole response, failing once its body passes `max_body`
    /// bytes (the caller's limit, so a large or unending body cannot exhaust memory). The
    /// request goes in origin form (path and query) with a Host header from the URL unless the
    /// caller set one; user information in the URL is never sent.
    pub async fn http(
        &self,
        request: Request<Bytes>,
        max_body: usize,
    ) -> Result<Response<Bytes>, NetError> {
        let (mut parts, body) = request.into_parts();
        let to = target::target(&parts.uri, "http")?;
        if !parts.headers.contains_key(HOST) {
            parts.headers.insert(HOST, host_header(&parts.uri)?);
        }
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        parts.uri = target::parse(path)?;
        let request = Request::from_parts(parts, Full::new(body));

        let stream = self.connect(&to.host, to.port).await?;
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(http_error)?;
        let exchange = async move {
            let response = sender.send_request(request).await.map_err(http_error)?;
            let (parts, body) = response.into_parts();
            let body = Limited::new(body, max_body);
            let body = body.collect().await.map_err(body_error)?.to_bytes();
            Ok(Response::from_parts(parts, body))
        };
        // The connection runs beside the exchange and ends once the exchange drops its sender.
        let (result, _) = tokio::join!(exchange, connection);
        result
    }
}

const BAD_HOST: &str = "the host is not a valid Host header";

/// `host[:port]` as the URL wrote it, without user information.
fn host_header(uri: &Uri) -> Result<HeaderValue, NetError> {
    let host = uri.host().unwrap_or_default();
    let value = match uri.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    HeaderValue::try_from(value).map_err(|_| NetError::protocol(Step::Url, BAD_HOST))
}

/// A body read failure: over the caller's limit, or hyper's own error.
fn body_error(e: Box<dyn std::error::Error + Send + Sync>) -> NetError {
    if e.is::<LengthLimitError>() {
        NetError::protocol(Step::Http, OVER_LIMIT)
    } else {
        NetError::new(Step::Http, Cause::Detail(e.to_string()))
    }
}

const OVER_LIMIT: &str = "the response body is over the caller's limit";

fn http_error(e: hyper::Error) -> NetError {
    NetError::new(Step::Http, Cause::Detail(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_header_keeps_the_port_and_brackets_but_not_user_information() {
        let header = |url: &str| host_header(&url.parse().unwrap()).unwrap();
        assert_eq!(header("http://u:p@api.test:8080/x"), "api.test:8080");
        assert_eq!(header("http://api.test/x"), "api.test");
        assert_eq!(header("http://[::1]:9/"), "[::1]:9");
    }
}
