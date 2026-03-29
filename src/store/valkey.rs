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
use redis::Script;
use redis::aio::ConnectionManager;

use self::claim::ClaimReply;
use crate::FencedOutcome;
use crate::IdempotencyStore;
use crate::InsertResult;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;

mod claim;
mod error;
mod wire;

use self::error::ValkeyError;
use self::wire::WireEntry;

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

/// Lua script for atomic entry removal.
static REMOVE_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    let code = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/valkey/remove.lua"));
    Script::new(code)
});

/// Lua script for atomic TTL extension.
static TOUCH_SCRIPT: LazyLock<Script> = LazyLock::new(|| {
    let code = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/valkey/touch.lua"));
    Script::new(code)
});

/// An [`IdempotencyStore`] backed by Valkey or Redis.
///
/// See the [module-level documentation](self) for server requirements.
pub struct ValkeyStore {
    /// The service or application name used as fencing token.
    service_name: String,

    /// The store connection manager.
    conn: ConnectionManager,
}

impl ValkeyStore {
    /// Opens the managed connection from `client` and creates a store.
    ///
    /// Use [`Self::from_connection_manager`] to wrap an already-connected manager.
    pub async fn connect(
        service_name: impl Into<String>,
        client: Client,
    ) -> Result<Self, ValkeyError> {
        let conn = client.get_connection_manager().await?;
        let store = Self {
            service_name: service_name.into(),
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

    /// Creates a store from an already-connected manager.
    pub fn from_connection_manager(
        service_name: impl Into<String>,
        conn: ConnectionManager,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            conn,
        }
    }

    /// Starts building a store backed by `client`, with no key prefix by default.
    pub const fn with_client(client: Client) -> ValkeyStoreBuilder {
        ValkeyStoreBuilder {
            client,
            prefix: None,
        }
    }
}

impl IdempotencyStore for ValkeyStore {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::try_insert",
            skip(self, entry),
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
            skip(self, entry, fencing_token),
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
            skip(self, fencing_token),
            err(Display),
        )
    )]
    async fn remove(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Result<FencedOutcome, Self::Error> {
        let prefixed = self.prefixed_key(key);
        let value: i64 = REMOVE_SCRIPT
            .key(&prefixed)
            .arg(fencing_token)
            .invoke_async(&mut self.conn.clone())
            .await?;

        FencedOutcome::try_from(value)
            .map_err(|_| ValkeyError::Decode("invalid return type".into()))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::touch",
            fields(key = %key, prefix = ?self.service_name),
            skip(self, fencing_token),
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

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::touch",
            fields(key = %key, prefix = ?self.service_name),
            skip(self),
            err(Display),
        )
    )]
    async fn purge(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let key = self.prefixed_key(key);
        redis::cmd("DEL")
            .arg(key)
            .exec_async(&mut self.conn.clone())
            .await?;
        Ok(())
    }
}

/// Builder for [`ValkeyStore`].
pub struct ValkeyStoreBuilder {
    client: Client,
    prefix: Option<String>,
}

impl ValkeyStoreBuilder {
    /// Sets the key prefix (service name).
    ///
    /// The prefix must not contain the reserved separator or scope `/`.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Resolves the `Client` to a `ConnectionManager` and builds the store.
    pub async fn build(self) -> Result<ValkeyStore, ValkeyError> {
        let conn = self.client.get_connection_manager().await?;
        Ok(ValkeyStore::from_connection_manager(
            self.prefix.unwrap_or_default(),
            conn,
        ))
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
        let store = ValkeyStore::connect("test", client)
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
