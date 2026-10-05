//! Idempotency key type.

use std::fmt;

use sha2::Digest;
use sha2::Sha256;

use crate::Error;

/// A validated idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// The maximum allowed length of an idempotency key
    const MAX_LEN: usize = u8::MAX as usize;
    /// Separates a store prefix from a key, so keys and prefixes cannot contain it.
    const PREFIX_SEPARATOR: char = ':';
    /// The length of a hashed principal, 128 bits in hex.
    const PRINCIPAL_LEN: usize = 32;
    /// Separates a principal from the key it owns. Only [`Self::with_principal`] writes it,
    /// since no key and no scope may contain it.
    const PRINCIPAL_SEPARATOR: char = ':';
    /// Separates a key from its scope, and is reserved in the same way.
    pub(crate) const SCOPE_SEPARATOR: char = '/';

    /// Creates an idempotency key, validating its length and character set.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is empty, exceeds 255 bytes, or contains a control character
    /// or a reserved separator (`:` or `/`).
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

    /// Creates a key scoped to a principal.
    ///
    /// Two principals sending the same key never share an entry.
    ///
    /// The principal is hashed, so any identity works, a DID included, and it never appears in
    /// the store. Take it from authentication, never from the request's own claims.
    ///
    /// # Errors
    ///
    /// Returns an error if the principal is empty, if the key is not valid, or if the
    /// result exceeds 255 bytes, which leaves the key 222.
    ///
    /// # Examples
    ///
    /// ```
    /// # use idempotent::IdempotencyKey;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let alice = IdempotencyKey::with_principal("did:web:alice.example", "offer-8f21")?;
    /// let bob = IdempotencyKey::with_principal("did:web:bob.example", "offer-8f21")?;
    /// assert_ne!(alice, bob);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_principal(
        principal: impl AsRef<str>,
        value: impl Into<String>,
    ) -> Result<Self, Error> {
        let principal = principal.as_ref();
        if principal.is_empty() {
            return Err(Error::EmptyPrincipal);
        }
        let Self(value) = Self::new(value)?;
        let len = Self::PRINCIPAL_LEN + 1 + value.len();
        if len > Self::MAX_LEN {
            return Err(Error::KeyTooLong(len));
        }

        let digest = Sha256::digest(principal.as_bytes());
        let hash = digest[..16]
            .iter()
            .fold(0u128, |acc, &byte| (acc << 8) | u128::from(byte));
        Ok(Self(format!(
            "{hash:032x}{}{value}",
            Self::PRINCIPAL_SEPARATOR
        )))
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

    /// Scopes the key like [`scoped`](Self::scoped), but consumes it.
    ///
    /// Use it for a cursor that advances through states, where the previous key must not be
    /// used again. On error the key is consumed. Use [`scoped`](Self::scoped) to keep the
    /// original when validation fails.
    ///
    /// # Errors
    ///
    /// Returns an error in the same cases as [`scoped`](Self::scoped).
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

    /// Returns `true` if the character may not appear in a key or a service-name prefix.
    pub(crate) const fn is_reserved(c: char) -> bool {
        c.is_ascii_control() || c == Self::PREFIX_SEPARATOR || c == Self::SCOPE_SEPARATOR
    }
}

/// Reads the key from a header.
///
/// A value that is not visible ASCII is an invalid key, as is one that fails
/// [`IdempotencyKey::new`].
#[cfg(feature = "middleware")]
impl TryFrom<&http::HeaderValue> for IdempotencyKey {
    type Error = Error;

    fn try_from(value: &http::HeaderValue) -> Result<Self, Error> {
        let value = value.to_str().map_err(|_| Error::InvalidKey)?;
        Self::new(value)
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

    #[gtest]
    fn key_with_reserved_character_is_rejected() {
        expect_that!(
            IdempotencyKey::new("tenant:key"),
            err(pat!(Error::InvalidKey))
        );
        expect_that!(
            IdempotencyKey::new("offer/8f21"),
            err(pat!(Error::InvalidKey))
        );
        expect_that!(
            IdempotencyKey::new("offer\n8f21"),
            err(pat!(Error::InvalidKey))
        );
        expect_that!(IdempotencyKey::new("offre-\u{e9}t\u{e9}"), ok(anything()));
    }

    #[cfg(feature = "uuid")]
    #[gtest]
    fn default_key_is_valid_uuid() {
        let key = IdempotencyKey::default();
        let parsed = uuid::Uuid::parse_str(key.as_str());
        expect_that!(parsed, ok(anything()));
    }

    #[gtest]
    fn principal_is_hashed_above_the_key() {
        let alice = IdempotencyKey::with_principal("did:web:alice.example", "cred-offer-123")
            .expect("a DID is a principal");
        let again = IdempotencyKey::with_principal("did:web:alice.example", "cred-offer-123")
            .expect("a DID is a principal");
        let bob = IdempotencyKey::with_principal("did:web:bob.example", "cred-offer-123")
            .expect("a DID is a principal");

        assert_eq!(alice, again);
        assert_ne!(alice, bob);
        assert!(alice.as_str().ends_with(":cred-offer-123"));
        assert_eq!(alice.as_str().len(), 32 + 1 + "cred-offer-123".len());
    }

    #[gtest]
    fn principal_leaves_the_key_222_bytes() {
        let longest = IdempotencyKey::with_principal("tenant", "k".repeat(222));
        let too_long = IdempotencyKey::with_principal("tenant", "k".repeat(223));

        expect_that!(longest, ok(anything()));
        expect_that!(too_long, err(pat!(Error::KeyTooLong(_))));
    }

    #[gtest]
    fn empty_principal_is_rejected() {
        let result = IdempotencyKey::with_principal("", "cred-offer-123");
        expect_that!(result, err(pat!(Error::EmptyPrincipal)));
    }

    #[gtest]
    fn principal_path_cannot_be_forged_from_a_key_or_a_scope() {
        let owned = IdempotencyKey::with_principal("tenant", "cred-offer-123").expect("valid");
        let (hash, _) = owned.as_str().split_once(':').expect("a principal path");

        // Neither a client key nor a step may contain the separator a principal path uses.
        expect_that!(
            IdempotencyKey::new(format!("{hash}:cred-offer-123")),
            err(pat!(Error::InvalidKey))
        );
        let plain = IdempotencyKey::new(hash).expect("valid key");
        expect_that!(
            plain.scoped(":cred-offer-123"),
            err(pat!(Error::InvalidScope))
        );
        assert_ne!(plain.scoped("cred-offer-123").expect("valid scope"), owned);
    }
}
