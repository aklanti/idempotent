//! Fencing tokens and fenced-operation outcomes.

/// A token generated when a key is claimed.
///
/// Prevents zombie completions from overwriting a reclaimed key's result.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FencingToken(pub(crate) u64);

impl FencingToken {
    /// Creates a fencing token.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fencing token value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[cfg(feature = "valkey")]
const _: () = {
    use redis::RedisWrite;
    use redis::ToRedisArgs;
    impl ToRedisArgs for FencingToken {
        fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
            self.0.write_redis_args(out)
        }
    }
};

/// The outcome of a fencing-guarded store operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FencedOutcome {
    /// The operation is complete and the result stored.
    Applied,
    /// The supplied and expected fencing token do not match.
    FencingMismatch,
    /// The idempotency key has expired.
    KeyExpired,
    /// The completing request's fingerprint does not match the claimed request.
    FingerprintMismatch,
}

#[cfg(feature = "valkey")]
impl FencedOutcome {
    /// Decodes the sentinel returned by the Valkey Lua scripts.
    pub(crate) const fn from_sentinel(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Applied),
            1 => Some(Self::FencingMismatch),
            2 => Some(Self::KeyExpired),
            3 => Some(Self::FingerprintMismatch),
            _ => None,
        }
    }
}
