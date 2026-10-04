//! The HTTP/1.1 call (`http://`, and `https://` over TLS), on the one connector, and a codec's
//! [`Effect::Http`](fbc_core::Effect::Http) made with it under its deadline (decision 0027).

use fbc_core::{HttpFailure, HttpMethod, HttpRequest};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::client::conn::http1;
use hyper::header::{HOST, HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use tokio::time::{Instant, timeout_at};

pub use hyper::body::Bytes;
pub use hyper::{Method, Request, Response, StatusCode, Uri, header};

use crate::connector::Connector;
use crate::error::{Cause, NetError, Step};
use crate::target;

impl Connector {
    /// Sends `request`, whose URI is an absolute `http://` or `https://` URL, on a new
    /// connection through this connector, and reads the whole response, failing once its body passes `max_body`
    /// bytes (the caller's limit, so a large or unending body cannot exhaust memory). The
    /// request goes in origin form (path and query) with a Host header from the URL unless the
    /// caller set one; user information in the URL is never sent.
    pub async fn http(
        &self,
        request: Request<Bytes>,
        max_body: usize,
    ) -> Result<Response<Bytes>, NetError> {
        let (to, request) = prepare(request)?;
        let stream = self.open(&to).await?;
        exchange(stream, request, max_body).await
    }

    /// Makes a codec's request by `deadline`, reading at most `max_body` response bytes. A
    /// failure before the connection is open (no deadline, as for a timeout past the end of
    /// the clock; a request the runtime cannot make; the connect, the proxy, TLS, or the
    /// deadline passing meanwhile) wrote no byte of the request and is
    /// [`HttpFailure::NotSent`]; after that, the deadline passing is [`HttpFailure::TimedOut`]
    /// and any other failure, a body over `max_body` included, [`HttpFailure::Lost`], since the
    /// request may have been written.
    pub(crate) async fn http_by(
        &self,
        req: &HttpRequest,
        deadline: Option<Instant>,
        max_body: usize,
    ) -> Result<Response<Bytes>, HttpFailure> {
        let deadline = deadline.ok_or(HttpFailure::NotSent)?;
        let request = to_hyper(req).ok_or(HttpFailure::NotSent)?;
        let (to, request) = prepare(request).map_err(|_| HttpFailure::NotSent)?;
        let stream = match timeout_at(deadline, self.open(&to)).await {
            Ok(Ok(stream)) => stream,
            _ => return Err(HttpFailure::NotSent),
        };
        match timeout_at(deadline, exchange(stream, request, max_body)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(HttpFailure::Lost),
            Err(_) => Err(HttpFailure::TimedOut),
        }
    }
}

/// A codec's request as hyper's, or `None` for a URL, header name or value hyper refuses.
fn to_hyper(req: &HttpRequest) -> Option<Request<Bytes>> {
    let method = match req.method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
    };
    let mut builder = Request::builder().method(method).uri(req.url.as_str());
    for header in &req.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).ok()?;
        let value = HeaderValue::from_str(&header.value).ok()?;
        builder = builder.header(name, value);
    }
    builder.body(Bytes::copy_from_slice(req.body.bytes())).ok()
}

/// Where `request` goes, and the request in origin form with its Host header.
fn prepare(request: Request<Bytes>) -> Result<(target::Target, Request<Full<Bytes>>), NetError> {
    let (mut parts, body) = request.into_parts();
    let to = target::target(&parts.uri, "http", "https")?;
    if !parts.headers.contains_key(HOST) {
        parts.headers.insert(HOST, host_header(&parts.uri)?);
    }
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    parts.uri = target::parse(path)?;
    Ok((to, Request::from_parts(parts, Full::new(body))))
}

/// Sends `request` on the open `stream` and reads the whole response, at most `max_body`
/// bytes of body.
async fn exchange(
    stream: crate::transport::Transport,
    request: Request<Full<Bytes>>,
    max_body: usize,
) -> Result<Response<Bytes>, NetError> {
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

    fn request(method: HttpMethod, url: &str, name: &'static str, value: &str) -> HttpRequest {
        HttpRequest {
            method,
            url: fbc_core::WireUrl::plain(url),
            headers: vec![fbc_core::Header {
                name,
                value: value.into(),
                redact: false,
            }],
            body: fbc_core::WireSlice::plain(b"x".to_vec()),
        }
    }

    #[test]
    fn a_codecs_request_keeps_its_method_headers_and_body_and_a_bad_one_is_refused() {
        let methods = [
            (HttpMethod::Get, Method::GET),
            (HttpMethod::Post, Method::POST),
            (HttpMethod::Put, Method::PUT),
            (HttpMethod::Delete, Method::DELETE),
        ];
        for (ours, theirs) in methods {
            let req = to_hyper(&request(ours, "http://a.test/p?q=1", "x-k", "v")).unwrap();
            assert_eq!(req.method(), theirs);
            assert_eq!(req.uri(), "http://a.test/p?q=1");
            assert_eq!(req.headers()["x-k"], "v");
            assert_eq!(req.body().as_ref(), b"x");
        }
        assert!(to_hyper(&request(HttpMethod::Get, "http://a.test/", "bad name", "v")).is_none());
        assert!(to_hyper(&request(HttpMethod::Get, "http://a.test/", "x-k", "a\nb")).is_none());
        assert!(to_hyper(&request(HttpMethod::Get, "http://a test/", "x-k", "v")).is_none());
    }

    #[tokio::test]
    async fn a_request_the_runtime_cannot_make_is_not_sent() {
        let connector = Connector::new(crate::ProxyConfig::Direct);
        let bad = request(HttpMethod::Get, "http://a.test/", "bad name", "v");
        let relative = request(HttpMethod::Get, "/only/a/path", "x-k", "v");
        let later = Instant::now().checked_add(std::time::Duration::from_secs(60));
        for req in [bad, relative] {
            let result = connector.http_by(&req, later, 1).await;
            assert_eq!(result.unwrap_err(), HttpFailure::NotSent);
        }
    }
}
