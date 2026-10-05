//! Credentials: the one way a venue's private key, API key or secret reaches a codec
//! (decisions 0009 and 0043, design §4.7).
//!
//! The consumer reads an account's credentials from its own store and hands them over as
//! [`Secrets`], by the configuration keys the venue's schema names, to
//! [`VenueFactory::exec_codec`](crate::VenueFactory::exec_codec) and
//! [`VenueFactory::test_connection`](crate::VenueFactory::test_connection). Each value is a
//! [`Secret`]:
//!
//! - its `Debug` and `Display` show no byte of it, nor its length, so a diagnostic that formats
//!   one, or a struct holding one, prints nothing a reader could use;
//! - it is not `Clone`: a value exists once, where the consumer built it or where a codec moved
//!   it to, and the consumer builds a fresh one from its store for each call;
//! - its memory is overwritten with zeros when it is dropped (the `zeroize` crate, 0043), and it
//!   takes the `String` it holds by move, so building one leaves no other copy behind.
//!
//! Reading a value is the explicit [`Secret::expose`]: the venue's own `src/auth` or `src/sign`
//! code calls it to sign or to fill a request, and nothing else should (0009). Bytes a codec
//! writes into a request it asks for are the runtime's from then on, marked as credentials
//! (`WireSlice` spans, a redacted `Header`) so that no `Debug` or journal record shows them.

use core::fmt;
use std::collections::BTreeMap;

use zeroize::Zeroizing;

/// One credential value: a private key, an API key or secret, a password. Its `Debug` and
/// `Display` show nothing of it, it cannot be cloned, and it is zeroed when dropped.
pub struct Secret(Zeroizing<String>);

impl Secret {
    /// Holds `value`, taking its buffer by move, so no copy of it is left behind.
    pub fn new(value: String) -> Secret {
        Secret(Zeroizing::new(value))
    }

    /// The value, for the venue's auth or signing code to use. Never format or keep it
    /// anywhere a log, an error or the journal could reach (0009).
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Secret {
        Secret::new(value)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// An account's credentials by configuration key, the keys a venue's
/// [`config_schema`](crate::VenueFactory::config_schema) names. Its `Debug` shows which keys
/// are set and nothing of their values; its `Display` shows how many. It is not `Clone`, and
/// every value is zeroed when it is dropped, replaced or the whole set is dropped.
#[derive(Default)]
pub struct Secrets {
    values: BTreeMap<&'static str, Secret>,
}

impl Secrets {
    /// No credentials.
    pub fn new() -> Secrets {
        Secrets::default()
    }

    /// Sets `key` to `value`. A value it replaces is dropped, and so zeroed; returns whether
    /// there was one.
    pub fn insert(&mut self, key: &'static str, value: Secret) -> bool {
        self.values.insert(key, value).is_some()
    }

    /// The value of `key`.
    pub fn get(&self, key: &str) -> Option<&Secret> {
        self.values.get(key)
    }

    /// Moves the value of `key` out, for a codec to keep.
    pub fn take(&mut self, key: &str) -> Option<Secret> {
        self.values.remove(key)
    }

    /// The keys set, in order.
    pub fn keys(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.values.keys().copied()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for Secrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secrets")
            .field("keys", &Vec::from_iter(self.keys()))
            .finish()
    }
}

impl fmt::Display for Secrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} credentials redacted>", self.values.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeroize::{Zeroize, ZeroizeOnDrop};

    const VALUE: &str = "SYNTHETIC-not-a-key-0123456789abcdef";

    #[test]
    fn a_secret_shows_nothing_of_its_value() {
        let secret = Secret::new(VALUE.to_owned());
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{secret:#?}"), "Secret(<redacted>)");
        assert_eq!(secret.to_string(), "<redacted>");
        assert_eq!(secret.expose(), VALUE);
        assert_eq!(Secret::from(VALUE.to_owned()).expose(), VALUE);
    }

    #[test]
    fn a_secret_keeps_the_buffer_it_was_given() {
        // Built by move, a secret holds the caller's own buffer: no unzeroed copy is made.
        let value = VALUE.to_owned();
        let at = value.as_ptr();
        let secret = Secret::new(value);
        assert_eq!(secret.expose().as_ptr(), at);
        // Moving it in and out of a set keeps that buffer too.
        let mut secrets = Secrets::new();
        secrets.insert("k", secret);
        assert_eq!(secrets.get("k").unwrap().expose().as_ptr(), at);
        assert_eq!(secrets.take("k").unwrap().expose().as_ptr(), at);
        assert!(secrets.is_empty());
    }

    #[test]
    fn the_held_value_is_zeroed_on_drop() {
        // What a `Secret` holds zeroes itself when it is dropped (`ZeroizeOnDrop`): this does
        // not compile if the field becomes a plain `String`. Reading freed memory to watch it
        // happen would be undefined behaviour, so the rest shows what that zeroing does to a
        // `String`: every byte of the buffer, spare capacity included, then the length.
        fn held_is_zeroed_on_drop<T: ZeroizeOnDrop>(_: &T) {}
        held_is_zeroed_on_drop(&Secret::new(VALUE.to_owned()).0);
        let mut value = String::with_capacity(64);
        value.push_str(VALUE);
        value.zeroize();
        assert!(value.is_empty());
    }

    #[test]
    fn secrets_show_their_keys_only() {
        let mut secrets = Secrets::new();
        assert!(!secrets.insert("toy.key", Secret::new(VALUE.to_owned())));
        assert!(secrets.insert("toy.key", Secret::new(format!("{VALUE}-2"))));
        secrets.insert("toy.account", Secret::new(VALUE.to_owned()));
        assert_eq!(secrets.len(), 2);
        assert_eq!(Vec::from_iter(secrets.keys()), ["toy.account", "toy.key"]);
        assert_eq!(
            secrets.get("toy.key").unwrap().expose(),
            "SYNTHETIC-not-a-key-0123456789abcdef-2"
        );
        assert!(secrets.get("missing").is_none());
        for shown in [
            format!("{secrets:?}"),
            format!("{secrets:#?}"),
            secrets.to_string(),
        ] {
            assert!(!shown.contains(VALUE), "{shown}");
        }
        assert_eq!(
            format!("{secrets:?}"),
            r#"Secrets { keys: ["toy.account", "toy.key"] }"#
        );
        assert_eq!(secrets.to_string(), "<2 credentials redacted>");
    }
}
