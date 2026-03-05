//! Idempotency store for distributed deployment backed by Valkey or Redis
//!
//! Uses a shared backend so entries are visible across all nodes. Claiming
//! and completing entries are atomic using a single Lua round-trips with no TOCTOU
//! risk. The idempotency entries expiration is handled by native TTL;
//!
//! The server configuration must have AOF persistence enabled (`appendonly yes`) and
//! eviction disabled (`maxmemory-policy noeviction`). Silent eviction under
//! memory pressure breaks the at-most-once guarantee.

use redis::aio::MultiplexedConnection;

use crate::key::IdempotencyKey;

/// Idempotency store backed by Valkey or Redis.
///
/// See [module-level documentation](self) for the confiration
pub struct ValkeyStore {
    /// A connection object allowing requests to be sent concurrently.
    conn: MultiplexedConnection,
    /// Key prefix to allow multiple services to share the same Valkey
    /// service without collisions
    pub key_prefix: Option<String>,
}

impl ValkeyStore {
    /// Creates a new store instance without key prefix
    pub const fn new(conn: MultiplexedConnection) -> Self {
        Self {
            conn,
            key_prefix: None,
        }
    }

    /// Creates a new store instance with prefix key
    pub const fn with_prefix(conn: MultiplexedConnection, key_prefix: String) -> Self {
        Self {
            conn,
            key_prefix: Some(key_prefix),
        }
    }

    /// Returns a prefixed key
    fn prefixed_key(&self, key: &IdempotencyKey) -> String {
        self.key_prefix
            .as_ref()
            .map(|prefix| format!("{prefix}{key}"))
            .unwrap_or_else(|| key.to_string())
    }
}
