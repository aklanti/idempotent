//! Idempotency store for distributed deployment backed by Valkey or Redis
//!
//! Uses a shared backend so entries are visible across all nodes. Claiming
//! and completing entries are atomic using a single Lua round-trips with no TOCTOU
//! risk. The idempotency entries expiration is handled by native TTL;
//!
//! The server configuration must have AOF persistence enabled (`appendonly yes`) and
//! eviction disabled (`maxmemory-policy noeviction`). Silent eviction under
//! memory pressure breaks the at-most-once guarantee.

use std::borrow::Cow;
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::{Client, RedisError, RedisWrite, Script, ToRedisArgs};
use serde::{Deserialize, Serialize};

use super::{IdempotencyStore, InsertResult};
use crate::entry::{
    CachedResponse, Completed, ExistingEntry, FencingToken, IdempotencyEntry, Processing,
};
use crate::fingerprint::Fingerprint;
use crate::key::IdempotencyKey;

/// Idempotency store backed by Valkey or Redis.
///
/// See [module-level documentation](self) for the confiration
pub struct ValkeyStore {
    /// A connector to the server
    conn: ConnectionManager,
    /// Key prefix to allow multiple services to share the same Valkey
    /// service without collisions
    pub key_prefix: Option<String>,
}

impl ValkeyStore {
    /// Lua script for atomic key claiming. It returns `nil` if claimed
    /// successfully or the existing entry bytes if the key is taken
    const CLAIM_SCRIPT: &str = include_str!("valkey/scripts/claim.lua");
    /// Lua script for atomic entry completion. It verify the fencing token before
    /// overwriting and reject stale completions from zombie handlers.
    const COMPLETE_SCRIPT: &str = include_str!("valkey/scripts/complete.lua");

    /// Creates a new store instance without key prefix
    pub async fn new(client: Client) -> Result<Self, ValkeyError> {
        Self::with_prefix(client, None).await
    }

    /// Creates a new store instance with prefix key
    pub async fn with_prefix(
        client: Client,
        key_prefix: Option<&str>,
    ) -> Result<Self, ValkeyError> {
        let conn = client.get_connection_manager().await?;
        let store = Self {
            conn,
            key_prefix: key_prefix.map(ToOwned::to_owned),
        };

        Ok(store)
    }

    /// Returns a prefixed key
    fn prefixed_key<'a>(&'a self, key: &'a IdempotencyKey) -> Cow<'a, str> {
        self.key_prefix.as_ref().map_or_else(
            || Cow::Borrowed(key.as_str()),
            |prefix| Cow::Owned(format!("{prefix}-{key}")),
        )
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for ValkeyStore {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "ValkeyStore::try_insert", skip(self), err(Debug))
    )]
    async fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Result<InsertResult, Self::Error> {
        let fencing_token = entry.fencing_token();
        let prefixed = self.prefixed_key(key);
        let wire = WireEntry::from(&entry);
        let ttl_ms = entry.ttl.as_millis();
        let serialized = wire.to_bytes()?;

        let script = Script::new(Self::CLAIM_SCRIPT);
        let result: Option<Vec<u8>> = script
            .key(prefixed)
            .arg(serialized)
            .arg(fencing_token)
            .arg(ttl_ms)
            .invoke_async(&mut self.conn.clone())
            .await?;

        match result {
            None => Ok(InsertResult::Claimed { fencing_token }),
            Some(data) => {
                let wire = WireEntry::try_from(data.as_slice())?;
                let existing = ExistingEntry::try_from(wire)?;
                Ok(InsertResult::Exists(existing))
            }
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "ValkeyStore::complete", skip(self), err(Debug))
    )]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<(), Self::Error> {
        let prefixed = self.prefixed_key(key);
        let ttl_ms = entry.ttl.as_millis();
        let serialized = WireEntry::from(&entry).to_bytes()?;
        let script = Script::new(Self::COMPLETE_SCRIPT);
        script
            .key(&prefixed)
            .arg(serialized)
            .arg(fencing_token)
            .arg(ttl_ms)
            .invoke_async::<()>(&mut self.conn.clone())
            .await?;
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "ValkeyStore::remove", skip(self), err(Debug))
    )]
    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let prefixed = self.prefixed_key(key);
        redis::cmd("DEL")
            .arg(&prefixed)
            .exec_async(&mut self.conn.clone())
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireEntry {
    status: WireStatus,
    fingerprint: Fingerprint,
    fencing_token: Option<FencingToken>,
    ttl: Duration,
    response: Option<CachedResponse>,
}

impl WireEntry {
    /// Serializes the wire entry with a version byte prefix followed by the paylaod
    fn to_bytes(&self) -> Result<Vec<u8>, ValkeyError> {
        let payload = postcard::to_allocvec(self)?;
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(WIRE_VERSION);
        buf.extend_from_slice(&payload);
        Ok(buf)
    }
}

impl From<&IdempotencyEntry<Processing>> for WireEntry {
    fn from(entry: &IdempotencyEntry<Processing>) -> Self {
        Self {
            status: WireStatus::Processing,
            fingerprint: entry.fingerprint,
            fencing_token: Some(entry.fencing_token()),
            response: None,
            ttl: entry.ttl,
        }
    }
}

impl From<&IdempotencyEntry<Completed>> for WireEntry {
    fn from(entry: &IdempotencyEntry<Completed>) -> Self {
        Self {
            status: WireStatus::Complete,
            fingerprint: entry.fingerprint,
            fencing_token: None,
            response: Some(entry.response().clone()),
            ttl: entry.ttl,
        }
    }
}

/// Current wire version
const WIRE_VERSION: u8 = WireVersion::V1 as u8;

impl TryFrom<&[u8]> for WireEntry {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "WireEntry::try_from", err(Debug))
    )]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (&version, payload) = bytes
            .split_first()
            .ok_or_else(|| ValkeyError::Decode("empty wire data".into()))?;
        match version {
            WIRE_VERSION => Ok(postcard::from_bytes(payload)?),
            v => Err(ValkeyError::Decode(
                format!("unknown wire version: {v}").into(),
            )),
        }
    }
}

impl TryFrom<WireEntry> for ExistingEntry {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "ExistingEntry::try_from", err(Debug))
    )]
    fn try_from(wire: WireEntry) -> Result<Self, ValkeyError> {
        let fingerprint = wire.fingerprint;
        let ttl = wire.ttl;

        match wire.status {
            WireStatus::Processing => {
                let entry = IdempotencyEntry::new(fingerprint, ttl);
                Ok(ExistingEntry::Processing(entry))
            }
            WireStatus::Complete => {
                let response = wire.response.ok_or_else(|| {
                    ValkeyError::Decode("completed entry missing response".into())
                })?;
                let entry = IdempotencyEntry::new(fingerprint, ttl).complete(response);
                Ok(ExistingEntry::Completed(entry))
            }
        }
    }
}

/// Entry state as persisted in the store.
///
/// It maps the idempotency entry typestate variants reconstructed in [`ExistingEntry`]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[repr(u8)]
enum WireStatus {
    Complete,
    Processing,
}

/// Schema version for the wire format
///
/// The version is bumped when changing [`WireEntry`] fields to
/// support rolling deploys where old and new nodes coexist.
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum WireVersion {
    V1 = 1,
}

/// Errors from [`ValkeyStore`] operations
#[derive(Debug, thiserror::Error)]
pub enum ValkeyError {
    /// A transient network issue, a retry may help
    #[error("connection error")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A data corruption error
    #[error("wire format error")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
}
impl From<RedisError> for ValkeyError {
    fn from(error: RedisError) -> Self {
        Self::Connection(Box::new(error))
    }
}

impl From<postcard::Error> for ValkeyError {
    fn from(error: postcard::Error) -> Self {
        Self::Decode(Box::new(error))
    }
}

impl ToRedisArgs for FencingToken {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.0.write_redis_args(out)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use googletest::matchers::{anything, eq, ok, pat};
    use googletest::{expect_that, gtest};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::valkey::Valkey;

    use super::*;
    use crate::entry::{CachedResponse, Metadata};
    use crate::fingerprint::{DefaultFingerprintStrategy, FingerprintStrategy};

    const SECONDS: u64 = 60;

    async fn new_store() -> (ValkeyStore, impl Drop) {
        let container = Valkey::default().start().await.expect("Valkey to start");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("to get container port");
        let client =
            redis::Client::open(format!("redis://127.0.0.1:{port}")).expect("to connect to Valkey");
        let store = ValkeyStore::new(client).await.expect("to create a store");
        (store, container)
    }

    #[tokio::test]
    #[gtest]
    async fn insert_and_claim() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/list", &[]);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(SECONDS));
        let result = store.try_insert(&key, entry).await;
        expect_that!(
            result,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        )
    }

    #[tokio::test]
    #[gtest]
    async fn complete_and_replay() {
        let (store, _container) = new_store().await;

        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/list", &[]);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(SECONDS));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(b"ok"),
        };
        let first = store.try_insert(&key, entry.clone()).await;
        let InsertResult::Claimed { fencing_token } = first.expect("a result") else {
            return;
        };
        let completed = entry.complete(response);
        let result = store.complete(&key, completed, fencing_token).await;

        expect_that!(result, pat!(Ok(())));
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(SECONDS));
        let replay = store.try_insert(&key, entry).await;
        let Ok(InsertResult::Exists(ExistingEntry::Completed(entry))) = replay else {
            panic!("expected Exists(Completed), got {replay:?}")
        };
        let response = entry.response();
        expect_that!(response.status_code, eq(200));
        expect_that!(response.body, eq(&Bytes::from_static(b"ok")));
    }
}
