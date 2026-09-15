//! Request fingerprints and the strategies that compute them.

use std::hash::Hash;
use std::ops::Deref;

use xxhash_rust::xxh3;

/// A hash of the request operation and body.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fingerprint(pub(crate) u128);

impl Fingerprint {
    /// Creates a fingerprint from a precomputed 128-bit hash.
    pub const fn new(value: u128) -> Self {
        Self(value)
    }
}

#[cfg(feature = "valkey")]
const _: () = {
    use redis::RedisWrite;
    use redis::ToRedisArgs;
    impl ToRedisArgs for Fingerprint {
        fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
            self.0.write_redis_args(out);
        }
    }
};

/// Encodes a value as fingerprint body bytes.
///
/// # Examples
///
/// ```
/// use idempotent::fingerprint::body;
///
/// #[derive(Hash)]
/// struct Charge {
///     account: &'static str,
///     minor_units: i64,
/// }
///
/// let charge = Charge {
///     account: "account1",
///     minor_units: 250,
/// };
/// let restated = Charge {
///     account: "account_1",
///     minor_units: 251,
/// };
///
/// assert_eq!(body(&charge), body(&charge));
/// assert_ne!(body(&charge), body(&restated));
/// ```
#[must_use]
pub fn body<T: Hash + ?Sized>(value: &T) -> [u8; 16] {
    let mut hasher = xxh3::Xxh3::new();
    value.hash(&mut hasher);
    hasher.digest128().to_le_bytes()
}

/// The method, the path, and the query of the request an idempotency key protects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation(String);

impl Deref for Operation {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Operation {
    fn from(operation: &str) -> Self {
        Self(operation.to_owned())
    }
}

impl From<String> for Operation {
    fn from(operation: String) -> Self {
        Self(operation)
    }
}

#[cfg(feature = "middleware")]
impl From<&http::request::Parts> for Operation {
    fn from(parts: &http::request::Parts) -> Self {
        let target = parts
            .uri
            .path_and_query()
            .map_or(parts.uri.path(), http::uri::PathAndQuery::as_str);
        Self(format!("{} {target}", parts.method))
    }
}

/// Trait for computing request fingerprints.
pub trait FingerprintStrategy: Send + Sync + 'static {
    /// Computes a fingerprint from `operation` and `body`.
    fn compute(&self, operation: &Operation, body: &[u8]) -> Fingerprint;
}

/// Default strategy using xxHash3.
pub struct DefaultFingerprintStrategy;

impl FingerprintStrategy for DefaultFingerprintStrategy {
    fn compute(&self, operation: &Operation, body: &[u8]) -> Fingerprint {
        let mut hasher = xxh3::Xxh3::new();

        hasher.update(&(operation.len() as u64).to_le_bytes());
        hasher.update(operation.as_bytes());
        hasher.update(body);
        Fingerprint(hasher.digest128())
    }
}

#[cfg(test)]
mod tests {
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::eq;
    use googletest::matchers::not;

    use super::*;

    #[gtest]
    fn field_separation_prevents_collision() {
        let strat = DefaultFingerprintStrategy;
        let f1 = strat.compute(&"GET/ab".into(), b"");
        let f2 = strat.compute(&"GET".into(), b"/ab");
        expect_that!(f1, not(eq(f2)));
    }

    #[cfg(feature = "middleware")]
    #[test]
    fn operation_covers_method_path_and_query() {
        let request = http::Request::post("/charges?amount=1")
            .body(())
            .expect("valid request");
        let (parts, ()) = request.into_parts();
        let operation = Operation::from(&parts);
        assert_eq!(&*operation, "POST /charges?amount=1");
    }
}
