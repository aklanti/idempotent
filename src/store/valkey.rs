//! Valkey / Redis idempotency store.
//!
//! Claiming and completing are atomic via Lua scripts with no TOCTOU risk.
//! The key-expiration uses native key TTL.
//!
//! The server must have AOF persistence enabled (`appendonly yes`) and eviction disabled
//! (`maxmemory-policy noeviction`) because a silent eviction under memory pressure breaks
//! the at-most-once guarantee.

use std::sync::LazyLock;
use std::time::Duration;

use redis::Client;
use redis::RedisError;
use redis::RedisWrite;
use redis::Script;
use redis::ToRedisArgs;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde::Serialize;

use self::claim::ClaimReply;
use crate::FencedOutcome;
use crate::IdempotencyStore;
use crate::InsertResult;
use crate::entry::CachedResponse;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::fingerprint::Fingerprint;
use crate::key::IdempotencyKey;

mod claim;

/// Lua script for atomic key claiming. Returns `nil` on success
/// or the existing entry bytes if the key is already taken.
static CLAIM_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    let code = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/valkey/claim.lua"));
    Script::new(code)
});

/// Lua script for atomic entry completion. Verifies the fencing token
/// and rejects stale completions.
static COMPLETE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    let code = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/valkey/complete.lua"));
    Script::new(code)
});

/// Lua script for atomic entry.
static TOUCH_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    let code = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/valkey/touch.lua"));
    Script::new(code)
});

/// An [`IdempotencyStore`] backed by Valkey or Redis.
///
/// See the [module-level documentation](self) for server requirements.
pub struct ValkeyStore {
    /// The service or application name used as fencing token.
    pub service_name: String,

    /// The store connection manager.
    conn: ConnectionManager,
}

impl ValkeyStore {
    /// Creates a new store instance without key prefix.
    pub async fn new(service_name: &str, client: Client) -> Result<Self, ValkeyError> {
        let conn = client.get_connection_manager().await?;
        let store = Self {
            service_name: service_name.to_owned(),
            conn,
        };

        Ok(store)
    }

    /// Returns a prefixed key.
    fn prefixed_key(&self, key: &IdempotencyKey) -> String {
        format!("{}__{key}", self.service_name)
    }

    /// Returns the fencing token key name.
    fn counter_key(&self) -> String {
        format!("{}__idempotent_ft_seq", self.service_name)
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for ValkeyStore {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::try_insert",
            skip(self),
            fields(key = %key, prefix = ?self.service_name),
            err(Display))
    )]
    async fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Result<InsertResult, Self::Error> {
        let prefixed_key = self.prefixed_key(key);
        let wire = WireEntry::from(&entry);
        let ttl_ms = entry.ttl.as_millis();
        let serialized = wire.to_bytes()?;

        let reply: ClaimReply = CLAIM_SCRIPT
            .key(prefixed_key)
            .key(self.counter_key())
            .arg(serialized)
            .arg(ttl_ms)
            .invoke_async(&mut self.conn.clone())
            .await?;

        match reply {
            ClaimReply::Created { fencing_token } => Ok(InsertResult::Claimed { fencing_token }),
            ClaimReply::InProgress { data } | ClaimReply::Complete { data } => {
                let wire = WireEntry::try_from(data.as_slice())?;
                let existing = ExistingEntry::try_from(wire)?;
                Ok(InsertResult::Exists(existing))
            }
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::complete",
            skip(self),
            fields(key = %key, prefix = ?self.service_name),
            err(Display),
        )
    )]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, Self::Error> {
        let prefixed = self.prefixed_key(key);
        let serialized = WireEntry::from(&entry).to_bytes()?;
        let value: i64 = COMPLETE_SCRIPT
            .key(&prefixed)
            .arg(serialized)
            .arg(fencing_token)
            .arg(completed_ttl.as_millis())
            .invoke_async(&mut self.conn.clone())
            .await?;

        FencedOutcome::try_from(value)
            .map_err(|_| ValkeyError::Decode("invalid return type".into()))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::remove",
            fields(key = %key, prefix = ?self.service_name),
            skip(self),
            err(Display),
        )
    )]
    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let prefixed = self.prefixed_key(key);
        redis::cmd("DEL")
            .arg(&prefixed)
            .exec_async(&mut self.conn.clone())
            .await?;
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::touch",
            fields(key = %key, prefix = ?self.service_name),
            skip(self),
            err(Display),
        )
    )]
    async fn touch(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> Result<FencedOutcome, Self::Error> {
        let prefixed = self.prefixed_key(key);
        let ttl_ms = ttl.as_millis();

        let value: i64 = TOUCH_SCRIPT
            .key(&prefixed)
            .arg(fencing_token)
            .arg(ttl_ms)
            .invoke_async(&mut self.conn.clone())
            .await?;

        FencedOutcome::try_from(value)
            .map_err(|_| ValkeyError::Decode("invalid return type".into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireEntry {
    status: WireStatus,
    fingerprint: Fingerprint,
    ttl: Duration,
    response: Option<CachedResponse>,
}

impl WireEntry {
    /// Serializes the wire entry with a version byte prefix followed by the payload.
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
        match wire.status {
            WireStatus::Processing => {
                let entry = IdempotencyEntry::new(wire.fingerprint, wire.ttl);
                Ok(ExistingEntry::Processing(entry))
            }
            WireStatus::Complete => {
                let response = wire.response.ok_or_else(|| {
                    ValkeyError::Decode("completed entry missing response".into())
                })?;
                let entry = IdempotencyEntry::new(wire.fingerprint, wire.ttl).complete(response);
                Ok(ExistingEntry::Completed(entry))
            }
        }
    }
}

/// Entry state as persisted in the store.
///
/// It maps the idempotency entry typestate variants reconstructed in [`ExistingEntry`]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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

/// Errors returned by [`ValkeyStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ValkeyError {
    /// A connection or network error.
    #[error("connection error")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The stored entry could not be decoded.
    #[error("decode error")]
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
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::anything;
    use googletest::matchers::eq;
    use googletest::matchers::ok;
    use googletest::matchers::pat;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::valkey::Valkey;

    use super::*;
    use crate::Metadata;
    use crate::entry::CachedResponse;
    use crate::fingerprint::DefaultFingerprintStrategy;
    use crate::fingerprint::FingerprintStrategy;

    const TTL: Duration = Duration::from_secs(60);

    async fn new_store() -> (ValkeyStore, impl Drop) {
        let container = Valkey::default().start().await.expect("Valkey to start");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("to get container port");
        let client =
            redis::Client::open(format!("redis://127.0.0.1:{port}")).expect("to connect to Valkey");
        let store = ValkeyStore::new("test", client)
            .await
            .expect("to create a store");
        (store, container)
    }

    #[gtest]
    #[tokio::test]
    async fn insert_and_claim() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/list", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let result = store.try_insert(&key, entry).await;
        expect_that!(
            result,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        )
    }

    #[gtest]
    #[tokio::test]
    async fn complete_and_replay() {
        let (store, _container) = new_store().await;

        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/list", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(b"ok"),
        };
        let first = store.try_insert(&key, entry.clone()).await;
        let InsertResult::Claimed { fencing_token } = first.expect("a result") else {
            return;
        };

        expect_that!(fencing_token.value(), eq(1));
        let completed = entry.complete(response);
        let result = store.complete(&key, completed, fencing_token, TTL).await;

        expect_that!(result, ok(eq(&FencedOutcome::Applied)));
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let replay = store.try_insert(&key, entry).await;
        let Ok(InsertResult::Exists(ExistingEntry::Completed(entry))) = replay else {
            panic!("expected Exists(Completed), got {replay:?}")
        };
        let response = entry.response();
        expect_that!(response.status_code, eq(200));
        expect_that!(response.body, eq(&Bytes::from_static(b"ok")));
    }

    #[gtest]
    #[tokio::test]
    async fn consecutive_call_generate_consecutive_fencing_token() {
        let (store, _container) = new_store().await;

        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/list", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let first = store.try_insert(&key, entry.clone()).await;

        expect_that!(
            first,
            ok(pat!(&InsertResult::Claimed {
                fencing_token: FencingToken(1)
            }))
        );

        let key = IdempotencyKey::new("soyinka").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/accept", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(&key, entry.clone()).await;
        expect_that!(
            second,
            ok(pat!(&InsertResult::Claimed {
                fencing_token: FencingToken(2)
            }))
        );
    }
}
