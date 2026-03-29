//! Response metadata

use std::collections::HashMap;

use bytes::Bytes;

/// Response metadata stored as string-keyed byte values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metadata(pub(crate) HashMap<String, Bytes>);

impl Metadata {
    /// Create new empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert new metadata.
    pub fn insert(&mut self, key: String, value: Bytes) -> Option<Bytes> {
        self.0.insert(key, value)
    }

    /// Returns the metadata value for key.
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.0.get(key)
    }

    /// Returns an iterator over the key/value pair.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Bytes)> {
        self.0.iter()
    }

    /// Returns the number of metadata.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if there are no metadata otherwise false.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Remove the metadata associated with the key.
    pub fn remove(&mut self, key: &str) -> Option<Bytes> {
        self.0.remove(key)
    }
}

impl FromIterator<(String, Bytes)> for Metadata {
    fn from_iter<T: IntoIterator<Item = (String, Bytes)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
