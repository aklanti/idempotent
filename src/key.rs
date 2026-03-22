//! Idempotency key type.

use std::fmt;

use crate::error::IdempotencyError;

/// A validated idempotency key extracted from a request metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// The maximum allowed length of an idempotency key
    const MAX_LEN: usize = u8::MAX as usize;

    /// Creates a new idempotency key
    ///
    /// # Examples
    ///
    /// ```
    /// # use idempotent::IdempotencyKey;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let key = IdempotencyKey::new("xxxx")?;
    ///     assert_eq!(key.as_str(), "xxxx");
    ///     let result =  IdempotencyKey::new("x".repeat(256));
    ///     assert!(result.is_err());
    ///     # Ok(())
    /// # }
    /// ```
    pub fn new(raw: impl Into<String>) -> Result<Self, IdempotencyError> {
        let inner = raw.into();

        if inner.is_empty() {
            return Err(IdempotencyError::EmptyKey);
        }
        if inner.len() > Self::MAX_LEN {
            return Err(IdempotencyError::KeyTooLong(inner.len()));
        }

        Ok(Self(inner))
    }

    /// Returns a string slice of the idempotency key
    pub fn as_str(&self) -> &str {
        &self.0
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
    use googletest::matchers::{anything, err, ok, pat};
    use googletest::{expect_that, gtest};

    use super::*;

    #[gtest]
    fn empty_key_is_rejected() {
        let result = IdempotencyKey::new("");
        expect_that!(result, err(pat!(IdempotencyError::EmptyKey)));
    }

    #[gtest]
    fn key_exceeding_max_len_rejected() {
        let result = IdempotencyKey::new("x".repeat(u16::MAX as usize));
        expect_that!(result, err(pat!(IdempotencyError::KeyTooLong(anything()))));
    }

    #[cfg(feature = "uuid")]
    #[gtest]
    fn default_key_is_valid_uuid() {
        let key = IdempotencyKey::default();
        let parsed = uuid::Uuid::parse_str(key.as_str());
        expect_that!(parsed, ok(anything()));
    }
}
