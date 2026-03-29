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
