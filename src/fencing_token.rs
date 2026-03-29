use crate::Error;

/// A token generated when a key is claimed.
///
/// Prevents zombie completions from overwriting a reclaimed key's result.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FencingToken(pub(crate) u64);

impl FencingToken {
    /// Returns the fencing token value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl TryFrom<i64> for FencingToken {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value)
            .map_err(|_| Error::NegativeFencingToken)
            .map(Self)
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

/// The result when the operation completes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FencedOutcome {
    /// The operation is complete and the result stored.
    Applied,
    /// The supplied and expected fencing token do not match.
    FencingMismatch,
    /// The idempotency key has expired.
    KeyExpired,
}

impl TryFrom<i64> for FencedOutcome {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        let me = match value {
            0 => Self::Applied,
            1 => Self::FencingMismatch,
            2 => Self::KeyExpired,
            other => {
                return Err(Error::UnexpectedFencedOutcome(other));
            }
        };

        Ok(me)
    }
}
