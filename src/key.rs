//! This module provides the idempotency key data structure

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

        if inner.is_empty() || inner.len() > Self::MAX_LEN {
            return Err(IdempotencyError::InvalidKey(inner));
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

#[cfg(test)]
mod tests {
    use googletest::matchers::{err, pat};
    use googletest::{expect_that, gtest};

    use super::*;

    #[gtest]
    fn empty_key_is_rejected() {
        let result = IdempotencyKey::new("");
        expect_that!(result, err(pat!(IdempotencyError::InvalidKey(..))));
    }

    #[gtest]
    fn key_exceeding_max_len_rejected() {
        let result = IdempotencyKey::new("x".repeat(u16::MAX as usize));
        expect_that!(result, err(pat!(IdempotencyError::InvalidKey(..))));
    }
}
