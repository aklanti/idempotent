//! Idempotency key type.

use std::fmt;

use crate::Error;

/// A validated idempotency key extracted from a request metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// The maximum allowed length of an idempotency key
    const MAX_LEN: usize = u8::MAX as usize;

    /// Prefix / tenancy boundary. Reserved: forbidden in keys and prefixes.
    const PREFIX_SEPARATOR: char = ':';

    /// Scope boundary (Change 24). Reserved likewise.
    pub const SCOPE_SEPARATOR: char = '/';

    /// Creates a new idempotency key
    ///
    /// # Examples
    ///
    /// ```
    /// # use idempotent::IdempotencyKey;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let key = IdempotencyKey::new("xxxx")?;
    /// assert_eq!(key.as_str(), "xxxx");
    /// let result = IdempotencyKey::new("x".repeat(256));
    /// assert!(result.is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();

        if value.is_empty() {
            return Err(Error::EmptyKey);
        }
        if value.len() > Self::MAX_LEN {
            return Err(Error::KeyTooLong(value.len()));
        }

        if value.chars().any(Self::is_reserved) {
            return Err(Error::InvalidKey);
        }

        Ok(Self(value))
    }

    /// Returns a string slice of the idempotency key
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives a scoped child key for one sub-operation of this key.
    ///
    /// The derived key is `{self}/{scope}` and the derivation is deterministic.
    /// `(key, scope)` always yields the same scoped key, so each step is
    /// independently idempotent and replays on retry.
    ///
    /// # Errors
    ///
    /// Returns an error when the scope is empty, invalid or the key is too long.
    pub fn scoped(&self, scope: impl AsRef<str>) -> Result<Self, Error> {
        let scope = scope.as_ref();
        self.check_scope(scope)?;
        let mut derived = String::with_capacity(self.0.len() + 1 + scope.len());
        derived.push_str(&self.0);
        derived.push(Self::SCOPE_SEPARATOR);
        derived.push_str(scope);
        Ok(Self(derived))
    }

    /// Like [`scoped`](Self::scoped) but **consumes** the key for a linear
    /// cursor advancing through states, where the previous key should become
    /// inaccessible.
    ///
    /// On error the key is consumed. Use [`scoped`](Self::scoped)
    /// if you need to keep the original when validation fails.
    ///
    /// # Errors
    /// Same as [`scoped`](Self::scoped).
    pub fn into_scoped(mut self, scope: impl AsRef<str>) -> Result<Self, Error> {
        let scope = scope.as_ref();
        self.check_scope(scope)?;
        self.0.reserve(1 + scope.len());
        self.0.push(Self::SCOPE_SEPARATOR);
        self.0.push_str(scope);
        Ok(self)
    }

    /// Validates a scope segment and the resulting length.
    fn check_scope(&self, scope: &str) -> Result<(), Error> {
        if scope.is_empty() {
            return Err(Error::EmptyScope);
        }
        if scope.chars().any(Self::is_reserved) {
            return Err(Error::InvalidScope);
        }
        let len = self.0.len() + 1 + scope.len();
        if len > Self::MAX_LEN {
            return Err(Error::KeyTooLong(len));
        }
        Ok(())
    }

    /// A char that may not appear in a user-supplied key OR a service-name prefix.
    pub(crate) const fn is_reserved(c: char) -> bool {
        c.is_ascii_control() || c == Self::PREFIX_SEPARATOR || c == Self::SCOPE_SEPARATOR
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "uuid")]
impl Default for IdempotencyKey {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().into())
    }
}

#[cfg(test)]
mod tests {
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::anything;
    use googletest::matchers::err;
    use googletest::matchers::ok;
    use googletest::matchers::pat;

    use super::*;

    #[gtest]
    fn empty_key_is_rejected() {
        let result = IdempotencyKey::new("");
        expect_that!(result, err(pat!(Error::EmptyKey)));
    }

    #[gtest]
    fn key_exceeding_max_len_rejected() {
        let result = IdempotencyKey::new("x".repeat(u16::MAX as usize));
        expect_that!(result, err(pat!(Error::KeyTooLong(anything()))));
    }

    #[cfg(feature = "uuid")]
    #[gtest]
    fn default_key_is_valid_uuid() {
        let key = IdempotencyKey::default();
        let parsed = uuid::Uuid::parse_str(key.as_str());
        expect_that!(parsed, ok(anything()));
    }
}
