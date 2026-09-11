//! Fencing tokens and fenced-operation outcomes.

/// A token issued when a key is claimed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FencingToken {
    /// The lifetime of the store process that issued the token.
    pub run_id: u64,
    /// A monotonic increasing claim number.
    pub sequence: u64,
}

impl FencingToken {
    /// Creates a token from the store lifetime that issues it and the claim's number within it.
    pub const fn new(run_id: u64, sequence: u64) -> Self {
        Self { run_id, sequence }
    }
}

#[cfg(feature = "valkey")]
const _: () = {
    use redis::RedisWrite;
    use redis::ToRedisArgs;
    impl ToRedisArgs for FencingToken {
        // Only the sequence is sent until the scripts store the run id as well.
        fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
            self.sequence.write_redis_args(out);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_sequences_from_different_lifetimes_differ() {
        assert_ne!(FencingToken::new(1, 7), FencingToken::new(2, 7));
    }

    #[test]
    fn order_follows_the_sequence_within_a_lifetime() {
        assert!(FencingToken::new(1, 7) < FencingToken::new(1, 8));
    }
}
