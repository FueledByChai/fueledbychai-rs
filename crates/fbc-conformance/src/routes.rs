//! What the stub's HTTP endpoint answers (decision 0047, augmenting 0025): rules tried in the
//! order they were added, each a method and a [`PathPattern`] with a fixed [`HttpReply`] or one
//! a function computes from the [`HttpRequest`]. 0025's fixed replies by path are rules too:
//! an [`HttpRoutes`] map becomes one exact-path rule per entry, answering any method.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use fbc_runtime::http::Method;

/// A fixed HTTP response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpReply {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Fixed responses by request path (the target without its query), whatever the method.
pub type HttpRoutes = BTreeMap<String, HttpReply>;

/// A request as the stub read it, handed to a rule's function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: Method,
    /// The target's path, as sent (not percent-decoded).
    pub path: String,
    /// The target's query, without its `?`, if it had one.
    pub query: Option<String>,
    /// Each header's name and value, in the order sent.
    pub headers: Vec<(String, String)>,
    /// The body, exactly its Content-Length.
    pub body: Vec<u8>,
    /// The variable segments the matching pattern captured, by name, in path order.
    pub params: Vec<(String, String)>,
}

impl HttpRequest {
    /// The segment the matching template captured as `{name}`.
    pub fn param(&self, name: &str) -> Option<&str> {
        let found = self.params.iter().find(|(k, _)| k == name);
        found.map(|(_, v)| v.as_str())
    }

    /// The first header named `name`, ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        let found = self
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name));
        found.map(|(_, v)| v.as_str())
    }
}

/// Why a path template was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatternError {
    /// It does not start with `/`.
    NotAbsolute,
    /// A segment holds a brace but is not exactly `{name}` with a name.
    Segment(String),
    /// A variable's name appears twice.
    Repeated(String),
}

impl fmt::Display for PatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternError::NotAbsolute => f.write_str("a path template starts with /"),
            PatternError::Segment(s) => {
                write!(f, "segment {s} is neither literal nor one {{name}}")
            }
            PatternError::Repeated(name) => write!(f, "variable {name} is named twice"),
        }
    }
}

impl std::error::Error for PatternError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Variable(String),
}

/// Which request paths a rule answers. Paths are compared as sent, never percent-decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathPattern(Kind);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Prefix(String),
    /// Whole paths of these segments.
    Template(Vec<Segment>),
}

impl PathPattern {
    /// The path itself, or anything below it: a prefix ending in `/` matches every path that
    /// starts with it; any other prefix matches itself and every path continuing it with `/`
    /// (`/orders` matches `/orders` and `/orders/9`, never `/ordersx`).
    pub fn prefix(prefix: impl Into<String>) -> PathPattern {
        PathPattern(Kind::Prefix(prefix.into()))
    }

    /// Exactly `path`, captured nothing: a template of literal segments.
    pub fn exact(path: &str) -> PathPattern {
        let literal = |s: &str| Segment::Literal(s.to_owned());
        PathPattern(Kind::Template(path.split('/').map(literal).collect()))
    }

    /// A path with variable segments, e.g. `/orders/by-client/{client_id}`: whole paths of its
    /// segments, a `{name}` segment matching any one non-empty segment and capturing it under
    /// that name.
    pub fn template(template: &str) -> Result<PathPattern, PatternError> {
        if !template.starts_with('/') {
            return Err(PatternError::NotAbsolute);
        }
        let mut segments = Vec::new();
        for s in template.split('/') {
            if !s.contains(['{', '}']) {
                segments.push(Segment::Literal(s.to_owned()));
                continue;
            }
            let name = s.strip_prefix('{').and_then(|s| s.strip_suffix('}'));
            let name = name.filter(|n| !n.is_empty() && !n.contains(['{', '}']));
            let name = name.ok_or_else(|| PatternError::Segment(s.to_owned()))?;
            let seen = |seg: &Segment| *seg == Segment::Variable(name.to_owned());
            if segments.iter().any(seen) {
                return Err(PatternError::Repeated(name.to_owned()));
            }
            segments.push(Segment::Variable(name.to_owned()));
        }
        Ok(PathPattern(Kind::Template(segments)))
    }

    /// The variables `path` binds, by name in path order, if it matches.
    pub fn matches(&self, path: &str) -> Option<Vec<(String, String)>> {
        match &self.0 {
            Kind::Prefix(prefix) => {
                let rest = path.strip_prefix(prefix.as_str())?;
                let boundary = prefix.ends_with('/') || rest.is_empty() || rest.starts_with('/');
                boundary.then(Vec::new)
            }
            Kind::Template(segments) => {
                let parts: Vec<&str> = path.split('/').collect();
                if parts.len() != segments.len() {
                    return None;
                }
                let mut params = Vec::new();
                for (segment, part) in segments.iter().zip(parts) {
                    match segment {
                        Segment::Literal(literal) if literal == part => {}
                        Segment::Variable(name) if !part.is_empty() => {
                            params.push((name.clone(), part.to_owned()));
                        }
                        _ => return None,
                    }
                }
                Some(params)
            }
        }
    }
}

type ReplyFn = dyn Fn(&HttpRequest) -> HttpReply + Send + Sync;

enum Answer {
    Fixed(HttpReply),
    Computed(Arc<ReplyFn>),
}

struct Rule {
    /// `None` answers any method (0025's fixed routes).
    method: Option<Method>,
    pattern: PathPattern,
    answer: Answer,
}

/// The stub's HTTP rules, tried in the order they were added; the first whose method and
/// pattern match answers. A request no rule matches is answered 404 with an empty body, and one
/// the stub cannot read 400.
#[derive(Default)]
pub struct HttpRouter {
    rules: Vec<Rule>,
}

impl HttpRouter {
    pub fn new() -> HttpRouter {
        HttpRouter::default()
    }

    /// Answers `method` on paths `pattern` matches with `reply`.
    pub fn route(self, method: Method, pattern: PathPattern, reply: HttpReply) -> HttpRouter {
        self.rule(Some(method), pattern, Answer::Fixed(reply))
    }

    /// Answers `method` on paths `pattern` matches with what `reply` computes from the request.
    /// A panic in `reply` drops the connection unanswered, so the client sees the failure.
    pub fn route_fn(
        self,
        method: Method,
        pattern: PathPattern,
        reply: impl Fn(&HttpRequest) -> HttpReply + Send + Sync + 'static,
    ) -> HttpRouter {
        self.rule(Some(method), pattern, Answer::Computed(Arc::new(reply)))
    }

    fn rule(mut self, method: Option<Method>, pattern: PathPattern, answer: Answer) -> HttpRouter {
        let rule = Rule {
            method,
            pattern,
            answer,
        };
        self.rules.push(rule);
        self
    }

    /// The reply to `request`, its `params` filled from the rule that matched.
    pub(crate) fn answer(&self, mut request: HttpRequest) -> HttpReply {
        for rule in &self.rules {
            let method = rule.method.as_ref().is_none_or(|m| *m == request.method);
            let Some(params) = method
                .then(|| rule.pattern.matches(&request.path))
                .flatten()
            else {
                continue;
            };
            request.params = params;
            return match &rule.answer {
                Answer::Fixed(reply) => reply.clone(),
                Answer::Computed(reply) => reply(&request),
            };
        }
        HttpReply {
            status: 404,
            body: Vec::new(),
        }
    }
}

impl From<HttpRoutes> for HttpRouter {
    /// One exact-path rule per entry, answering any method, in path order.
    fn from(routes: HttpRoutes) -> HttpRouter {
        let fixed = |(path, reply): (String, HttpReply)| Rule {
            method: None,
            pattern: PathPattern::exact(&path),
            answer: Answer::Fixed(reply),
        };
        HttpRouter {
            rules: routes.into_iter().map(fixed).collect(),
        }
    }
}
