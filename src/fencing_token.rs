use crate::Error;

/// A token generated when a key is claimed.
///
/// Prevents zombie completions from overwriting a reclaimed key's result.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FencingToken(pub(crate) u64);

impl FencingToken {
    /// Creates a new fencing token
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl TryFrom<i64> for FencingToken {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value)
            .map_err(|_| Error::InvalidFencingToken)
            .map(Self)
    }
}
