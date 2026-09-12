//! Valkey / Redis backed idempotency store.
//!
//! Claiming and completing are atomic via Lua scripts with no TOCTOU risk.
//! The key-expiration uses native key TTL.
//!
//! The server must have AOF persistence enabled (`appendonly yes`) and eviction disabled
//! because a silent eviction under memory pressure breaks the at-most-once guarantee.
//! Valkey Cluster is not supported, because the claim script writes the entry and the
//! fencing-token counter, two keys in different slots, which a cluster refuses.
//!
//! The fencing tokens have an associated server ID, so a token issued before a restart or a
//! failover never matches one issued after it.

use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

use redis::Client;
use redis::Script;
use redis::aio::ConnectionManager;
use redis::aio::ConnectionManagerConfig;

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

#[doc(inline)]
pub use self::error::ValkeyError;
#[doc(inline)]
use self::wire::WireEntry;

/// Lua script for atomic key claiming.
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

/// The default connection and response timeout, five seconds.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// An [`IdempotencyStore`] backed by Valkey or Redis.
#[derive(Clone)]
pub struct ValkeyStore {
    /// Key prefix that isolates this store's keys and its fencing-token counter
    /// from other services on the same server.
    prefix: String,

    /// The store connection manager.
    conn: ConnectionManager,
}

impl ValkeyStore {
    /// Returns a prefixed key.
    fn prefixed_key(&self, key: &IdempotencyKey) -> String {
        format!("{}:{key}", self.prefix)
    }

    /// Returns the fencing token key name.
    fn counter_key(&self) -> String {
        format!("{}::idempotent_ft_seq", self.prefix)
    }

    /// Sends a `PING` to the server, for readiness probes.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::ping",
            skip(self),
            fields(prefix = ?self.prefix),
            err(Display),
        )
    )]
    pub async fn ping(&self) -> Result<(), ValkeyError> {
        redis::cmd("PING")
            .query_async::<()>(&mut self.conn.clone())
            .await?;
        Ok(())
    }

    /// Starts building a store that connects to `url` without a key prefix.
    pub fn with_url(url: impl Into<String>) -> ValkeyStoreBuilder {
        ValkeyStoreBuilder {
            source: Source::Url(url.into()),
            prefix: None,
            connection_timeout: DEFAULT_TIMEOUT,
            response_timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Starts building a store backed with the given client and no key prefix.
    pub const fn with_client(client: Client) -> ValkeyStoreBuilder {
        ValkeyStoreBuilder {
            source: Source::Client(client),
            prefix: None,
            connection_timeout: DEFAULT_TIMEOUT,
            response_timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Starts building a store over an already-connected manager, with no key prefix by default.
    pub const fn with_connection_manager(conn: ConnectionManager) -> ValkeyStoreBuilder {
        ValkeyStoreBuilder {
            source: Source::Manager(conn),
            prefix: None,
            connection_timeout: DEFAULT_TIMEOUT,
            response_timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl fmt::Debug for ValkeyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValkeyStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl IdempotencyStore for ValkeyStore {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::try_insert",
            skip(self, entry),
            fields(key = %key, prefix = ?self.prefix),
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
            .arg(entry.fingerprint)
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
            fields(key = %key, prefix = ?self.prefix),
            err(Display),
        )
    )]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<FencedOutcome, Self::Error> {
        let prefixed = self.prefixed_key(key);
        let serialized = WireEntry::from(&entry).to_bytes()?;
        let value: i64 = COMPLETE_SCRIPT
            .key(&prefixed)
            .arg(serialized)
            .arg(format!("{:016x}", fencing_token.run_id))
            .arg(fencing_token.sequence)
            .arg(entry.ttl.as_millis())
            .arg(entry.fingerprint)
            .invoke_async(&mut self.conn.clone())
            .await?;

        decode_sentinel(value)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::remove",
            fields(key = %key, prefix = ?self.prefix),
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
            .arg(format!("{:016x}", fencing_token.run_id))
            .arg(fencing_token.sequence)
            .invoke_async(&mut self.conn.clone())
            .await?;

        decode_sentinel(value)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::touch",
            fields(key = %key, prefix = ?self.prefix),
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
            .arg(format!("{:016x}", fencing_token.run_id))
            .arg(fencing_token.sequence)
            .arg(ttl_ms)
            .invoke_async(&mut self.conn.clone())
            .await?;

        decode_sentinel(value)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "ValkeyStore::purge",
            fields(key = %key, prefix = ?self.prefix),
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

/// Decodes a Lua script sentinel into a [`FencedOutcome`].
fn decode_sentinel(value: i64) -> Result<FencedOutcome, ValkeyError> {
    FencedOutcome::from_sentinel(value)
        .ok_or_else(|| ValkeyError::Decode(format!("unexpected fenced outcome {value}").into()))
}

/// A [`ValkeyStore`] builder.
pub struct ValkeyStoreBuilder {
    source: Source,
    prefix: Option<String>,
    connection_timeout: Duration,
    response_timeout: Duration,
}

/// The connection source the builder resolves at build time.
#[allow(
    clippy::large_enum_variant,
    reason = "the builder is made once and consumed by try_build; its size never matters"
)]
enum Source {
    Url(String),
    Client(Client),
    Manager(ConnectionManager),
}

impl ValkeyStoreBuilder {
    /// Sets the key prefix (service name).
    ///
    /// The prefix must not contain a reserved separator (`:` or `/`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Sets how long a connection attempt may take.
    pub const fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Sets how long a command may take to answer.
    pub const fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Builds the store, connecting when it was given a URL or a client.
    ///
    /// A manager passed to [`ValkeyStore::with_connection_manager`] is used as it is, with the
    /// timeouts it was created with.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix contains a reserved separator or a control character, if
    /// the URL cannot be parsed, or if the connection manager cannot be created.
    pub async fn try_build(self) -> Result<ValkeyStore, ValkeyError> {
        let prefix = self.prefix.unwrap_or_default();
        if prefix.chars().any(IdempotencyKey::is_reserved) {
            return Err(ValkeyError::InvalidPrefix(prefix));
        }
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connection_timeout))
            .set_response_timeout(Some(self.response_timeout));
        let conn = match self.source {
            Source::Url(url) => {
                let client =
                    Client::open(url).map_err(|error| ValkeyError::InvalidUrl(Box::new(error)))?;
                client.get_connection_manager_with_config(config).await?
            }
            Source::Client(client) => client.get_connection_manager_with_config(config).await?,
            Source::Manager(conn) => conn,
        };
        Ok(ValkeyStore { prefix, conn })
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::anything;
    use googletest::matchers::eq;
    use googletest::matchers::err;
    use googletest::matchers::not;
    use googletest::matchers::ok;
    use googletest::matchers::pat;
    use testcontainers::ContainerAsync;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::valkey::Valkey;

    use super::*;
    use crate::Metadata;
    use crate::entry::CachedResponse;
    use crate::fingerprint::DefaultFingerprintStrategy;
    use crate::fingerprint::FingerprintStrategy;

    const fn assert_usable_with_middleware<S: IdempotencyStore + Clone + Send + Sync + 'static>() {}
    const _: () = assert_usable_with_middleware::<ValkeyStore>();

    const TTL: Duration = Duration::from_secs(60);

    async fn new_store() -> (ValkeyStore, impl Drop) {
        let container = Valkey::default().start().await.expect("Valkey to start");
        let store = connect(&container).await;
        (store, container)
    }

    async fn connect(container: &ContainerAsync<Valkey>) -> ValkeyStore {
        let host = container.get_host().await.expect("to get container host");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("to get container port");
        let client =
            redis::Client::open(format!("redis://{host}:{port}")).expect("to parse the URL");
        let attempts = async {
            loop {
                let built = ValkeyStore::with_client(client.clone())
                    .prefix("test")
                    .try_build()
                    .await;
                if let Ok(store) = built
                    && store.ping().await.is_ok()
                {
                    return store;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(30), attempts)
            .await
            .expect("Valkey did not become reachable within thirty seconds")
    }

    /// The stored server run id.
    async fn server_run_id(store: &ValkeyStore) -> u64 {
        let info: String = redis::cmd("INFO")
            .arg("server")
            .query_async(&mut store.conn.clone())
            .await
            .expect("INFO server");
        let run_id = info
            .lines()
            .find_map(|line| line.strip_prefix("run_id:"))
            .expect("a run_id line")
            .trim();
        u64::from_str_radix(&run_id[..16], 16).expect("hex run id")
    }

    fn response(body: &'static [u8]) -> CachedResponse {
        CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(body),
        }
    }

    #[gtest]
    #[tokio::test]
    async fn try_build_rejects_reserved_prefix() {
        let client = redis::Client::open("redis://127.0.0.1:1").expect("to parse the URL");
        let result = ValkeyStore::with_client(client)
            .prefix("bad:prefix")
            .try_build()
            .await;
        expect_that!(result, err(pat!(ValkeyError::InvalidPrefix(anything()))));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_and_replay() {
        let (store, _container) = new_store().await;

        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/list".into(), &[]);
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

        let duplicate = store.try_insert(&key, entry.clone()).await;
        let Ok(InsertResult::Exists(ExistingEntry::Processing(existing))) = duplicate else {
            panic!("expected Exists(Processing), got {duplicate:?}")
        };
        expect_that!(existing.fingerprint, eq(fingerprint));
        expect_that!(existing.ttl, eq(TTL));

        let completed = entry.complete(response, TTL);
        let result = store.complete(&key, completed, fencing_token).await;

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
    async fn token_carries_server_run_id() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/list".into(), &[]);

        let Ok(InsertResult::Claimed { fencing_token }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        expect_that!(fencing_token.run_id, eq(server_run_id(&store).await));
        expect_that!(fencing_token.sequence, eq(1));

        let other = IdempotencyKey::new("soyinka").expect("valid key");
        let Ok(InsertResult::Claimed {
            fencing_token: next,
        }) = store
            .try_insert(&other, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };
        expect_that!(next.run_id, eq(fencing_token.run_id));
        expect_that!(next.sequence, eq(2));
    }

    #[gtest]
    #[tokio::test]
    async fn token_issued_before_restart_is_rejected_after_it() {
        let container = Valkey::default().start().await.expect("Valkey to start");
        let store = connect(&container).await;
        let key = IdempotencyKey::new("achebe").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/charge".into(), &[]);

        let Ok(InsertResult::Claimed {
            fencing_token: before,
        }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        container.stop_with_timeout(None).await.expect("to stop");
        container.start().await.expect("to start");
        let store = connect(&container).await;
        let mut conn = store.conn.clone();
        redis::cmd("DEL")
            .arg(store.prefixed_key(&key))
            .exec_async(&mut conn)
            .await
            .expect("to drop the claim");
        redis::cmd("SET")
            .arg(store.counter_key())
            .arg(before.sequence - 1)
            .exec_async(&mut conn)
            .await
            .expect("to roll the counter back");

        let Ok(InsertResult::Claimed {
            fencing_token: after,
        }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected the key to be free after the restart");
        };
        expect_that!(after.sequence, eq(before.sequence));
        expect_that!(after.run_id, not(eq(before.run_id)));
        expect_that!(after.run_id, eq(server_run_id(&store).await));

        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(b"ok"),
        };
        let completed = IdempotencyEntry::new(fingerprint, TTL).complete(response, TTL);
        let stale = store.complete(&key, completed.clone(), before).await;
        expect_that!(stale, ok(eq(&FencedOutcome::FencingMismatch)));
        let stale = store.touch(&key, before, TTL).await;
        expect_that!(stale, ok(eq(&FencedOutcome::FencingMismatch)));
        let live = store.complete(&key, completed, after).await;
        expect_that!(live, ok(eq(&FencedOutcome::Applied)));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_after_complete_is_rejected() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/list".into(), &[]);
        let Ok(InsertResult::Claimed { fencing_token }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        let first = IdempotencyEntry::new(fingerprint, TTL).complete(response(b"first"), TTL);
        let applied = store.complete(&key, first, fencing_token).await;
        expect_that!(applied, ok(eq(&FencedOutcome::Applied)));

        let second = IdempotencyEntry::new(fingerprint, TTL).complete(response(b"second"), TTL);
        let rejected = store.complete(&key, second, fencing_token).await;
        expect_that!(rejected, ok(eq(&FencedOutcome::KeyExpired)));

        let replay = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await;
        let Ok(InsertResult::Exists(ExistingEntry::Completed(entry))) = replay else {
            panic!("expected Exists(Completed), got {replay:?}")
        };
        expect_that!(entry.response().body, eq(&Bytes::from_static(b"first")));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_with_foreign_fingerprint_is_rejected() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let claimed = DefaultFingerprintStrategy.compute(&"/list".into(), b"original");
        let Ok(InsertResult::Claimed { fencing_token }) = store
            .try_insert(&key, IdempotencyEntry::new(claimed, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        let foreign = DefaultFingerprintStrategy.compute(&"/list".into(), b"different");
        let completed = IdempotencyEntry::new(foreign, TTL).complete(response(b"ok"), TTL);
        let rejected = store.complete(&key, completed, fencing_token).await;
        expect_that!(rejected, ok(eq(&FencedOutcome::FingerprintMismatch)));

        let replay = store
            .try_insert(&key, IdempotencyEntry::new(claimed, TTL))
            .await;
        expect_that!(
            replay,
            ok(pat!(InsertResult::Exists(pat!(ExistingEntry::Processing(
                _
            )))))
        );
    }

    #[gtest]
    #[tokio::test]
    async fn touch_after_complete_is_rejected() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/list".into(), &[]);
        let Ok(InsertResult::Claimed { fencing_token }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        let live = store.touch(&key, fencing_token, TTL).await;
        expect_that!(live, ok(eq(&FencedOutcome::Applied)));

        let completed = IdempotencyEntry::new(fingerprint, TTL).complete(response(b"ok"), TTL);
        let applied = store.complete(&key, completed, fencing_token).await;
        expect_that!(applied, ok(eq(&FencedOutcome::Applied)));

        let rejected = store.touch(&key, fencing_token, TTL).await;
        expect_that!(rejected, ok(eq(&FencedOutcome::KeyExpired)));
    }

    #[gtest]
    #[tokio::test]
    async fn remove_requires_the_token_and_purge_does_not() {
        let (store, _container) = new_store().await;
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"/list".into(), &[]);
        let Ok(InsertResult::Claimed { fencing_token }) = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await
        else {
            panic!("expected a fresh claim");
        };

        let foreign = FencingToken::new(fencing_token.run_id, fencing_token.sequence + 1);
        expect_that!(
            store.remove(&key, foreign).await,
            ok(eq(&FencedOutcome::FencingMismatch))
        );
        expect_that!(
            store.remove(&key, fencing_token).await,
            ok(eq(&FencedOutcome::Applied))
        );
        expect_that!(
            store.remove(&key, fencing_token).await,
            ok(eq(&FencedOutcome::KeyExpired))
        );

        let reclaimed = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await;
        expect_that!(reclaimed, ok(pat!(InsertResult::Claimed { .. })));
        expect_that!(store.purge(&key).await, ok(anything()));
        let free = store
            .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
            .await;
        expect_that!(free, ok(pat!(InsertResult::Claimed { .. })));
    }

    #[gtest]
    #[tokio::test]
    async fn with_url_builds_or_rejects_the_url() {
        let container = Valkey::default().start().await.expect("Valkey to start");
        let host = container.get_host().await.expect("to get container host");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("to get container port");

        let store = ValkeyStore::with_url(format!("redis://{host}:{port}"))
            .prefix("test")
            .try_build()
            .await
            .expect("to build from a url");
        expect_that!(store.ping().await, ok(anything()));

        let rejected = ValkeyStore::with_url("not a url").try_build().await;
        expect_that!(rejected, err(pat!(ValkeyError::InvalidUrl(anything()))));
    }
}
