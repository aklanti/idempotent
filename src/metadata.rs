//! Response metadata

use std::collections::HashMap;

use bytes::Bytes;

/// Response metadata stored as string-keyed byte values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metadata(pub(crate) HashMap<String, Bytes>);

impl Metadata {
    /// Creates empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a key/value pair, returning the previous value for `key` if it was set.
    pub fn insert(&mut self, key: String, value: Bytes) -> Option<Bytes> {
        self.0.insert(key, value)
    }

    /// Returns the value for `key`.
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.0.get(key)
    }

    /// Returns an iterator over the key/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Bytes)> {
        self.0.iter()
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Removes and returns the value for `key`.
    pub fn remove(&mut self, key: &str) -> Option<Bytes> {
        self.0.remove(key)
    }
}

impl FromIterator<(String, Bytes)> for Metadata {
    fn from_iter<T: IntoIterator<Item = (String, Bytes)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
