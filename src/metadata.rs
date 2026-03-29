//! Response metadata

use bytes::Bytes;

/// Response metadata as ordered name/value pairs.
///
/// Names may repeat, matching HTTP headers and gRPC metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metadata(Vec<(String, Bytes)>);

impl Metadata {
    /// Creates empty metadata.
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Appends a name/value pair, keeping any existing values for the name.
    pub fn append(&mut self, name: impl Into<String>, value: Bytes) {
        self.0.push((name.into(), value));
    }

    /// Returns the first value for `name`.
    pub fn get(&self, name: &str) -> Option<&Bytes> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    /// Returns an iterator over the name/value pairs in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Bytes)> {
        self.0.iter().map(|(n, v)| (n.as_str(), v))
    }
}

impl FromIterator<(String, Bytes)> for Metadata {
    fn from_iter<T: IntoIterator<Item = (String, Bytes)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
