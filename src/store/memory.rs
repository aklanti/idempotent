//! In-memory idempotency store.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::IdempotencyStore;
use super::InsertResult;
use crate::FencedOutcome;
use crate::entry::Completed;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;

mod actor;
mod command;
mod error;

use self::actor::MemoryStoreActor;
use self::command::Command;
#[doc(inline)]
pub use self::error::MemoryStoreError;

/// An in-memory [`IdempotencyStore`].
///
/// Entries are automatically swept at the configured interval.
#[derive(Clone)]
pub struct MemoryStore {
    tx: mpsc::Sender<Command>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl MemoryStore {
    /// Returns `true` while the background task is alive and accepting commands.
    pub fn is_healthy(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Drops this handle's sender and awaits task exit. The task exits when the
    /// LAST sender drops.
    pub async fn close(self) {
        let Self { tx, task } = self;
        drop(tx);
        let handle = match task.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
            let outcome = handle.await;
            #[cfg(feature = "tracing")]
            if let Err(error) = outcome {
                tracing::error!(%error, "memory store background task panicked");
            }
            #[cfg(not(feature = "tracing"))]
            let _ = outcome;
        }
    }

    /// Starts building a store with default settings
    pub const fn builder() -> MemoryStoreBuilder {
        MemoryStoreBuilder {
            buffer: 64,
            sweep_interval: Duration::from_secs(60),
            runtime: None,
        }
    }

    /// The number of entries in memory.
    pub async fn len(&self) -> Result<usize, MemoryStoreError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Len { reply })
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;

        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }

    /// Returns `true` when the store holds no entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the background task has stopped.
    pub async fn is_empty(&self) -> Result<bool, MemoryStoreError> {
        self.len().await.map(|count| count == 0)
    }
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

impl IdempotencyStore for MemoryStore {
    type Error = MemoryStoreError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "MemoryStore::try_insert",
            skip(self),
            fields(key = %key),
            err(Display),
        )
    )]
    async fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Result<InsertResult, Self::Error> {
        let (reply, rx) = oneshot::channel();
        let cmd = Command::TryInsert {
            key: key.clone(),
            entry,
            reply,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;
        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "MemoryStore::complete",
            skip(self, entry, fencing_token),
            fields(key  = %key),
            err(Display),
        )
    )]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<FencedOutcome, Self::Error> {
        let (reply, rx) = oneshot::channel();
        let cmd = Command::Complete {
            key: key.clone(),
            entry,
            fencing_token,
            reply,
        };

        self.tx
            .send(cmd)
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;
        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStore::remove", skip(self, fencing_token), err(Display),)
    )]
    async fn remove(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Result<FencedOutcome, Self::Error> {
        let (reply, rx) = oneshot::channel();
        let cmd = Command::Remove {
            key: key.clone(),
            fencing_token,
            reply,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;
        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStore::touch", skip(self, fencing_token), err(Display))
    )]
    async fn touch(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> Result<FencedOutcome, Self::Error> {
        let (reply, rx) = oneshot::channel();
        let cmd = Command::Touch {
            key: key.clone(),
            fencing_token,
            ttl,
            reply,
        };

        self.tx
            .send(cmd)
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;
        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }

    async fn purge(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let (reply, rx) = oneshot::channel();
        let cmd = Command::Purge {
            key: key.clone(),
            reply,
        };
        self.tx
            .send(cmd)
            .await
            .map_err(|_| MemoryStoreError::TaskStopped)?;
        rx.await.map_err(|_| MemoryStoreError::TaskStopped)
    }
}

/// Memory store builder.
pub struct MemoryStoreBuilder {
    buffer: usize,
    sweep_interval: Duration,
    runtime: Option<Handle>,
}

impl MemoryStoreBuilder {
    /// Sets the command-channel capacity. The value must be greater than `0`.
    pub const fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer;
        self
    }

    /// Sets how often expired entries are swept.
    pub const fn sweep_interval(mut self, interval: Duration) -> Self {
        self.sweep_interval = interval;
        self
    }

    /// Set the runtime handle.
    pub fn runtime(mut self, handle: Handle) -> Self {
        self.runtime = Some(handle);
        self
    }

    /// Builds the store and spawns its background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer or sweep interval is zero, or if no runtime was set and
    /// none is currently running.
    pub fn try_build(self) -> Result<MemoryStore, MemoryStoreError> {
        if self.buffer == 0 {
            return Err(MemoryStoreError::ZeroBuffer);
        }
        if self.sweep_interval.is_zero() {
            return Err(MemoryStoreError::ZeroSweepInterval);
        }
        let handle = match self.runtime {
            Some(handle) => handle,
            None => Handle::try_current().map_err(|_| MemoryStoreError::NoRuntime)?,
        };
        let (tx, rx) = mpsc::channel(self.buffer);
        let task = handle.spawn(MemoryStoreActor::new().run(rx, self.sweep_interval));
        Ok(MemoryStore {
            tx,
            task: Arc::new(Mutex::new(Some(task))),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::anything;
    use googletest::matchers::eq;
    use googletest::matchers::err;
    use googletest::matchers::ok;
    use googletest::matchers::pat;

    use super::*;
    use crate::Metadata;
    use crate::entry::CachedResponse;
    use crate::entry::ExistingEntry;
    use crate::fingerprint::DefaultFingerprintStrategy;
    use crate::fingerprint::FingerprintStrategy;

    const fn assert_usable_with_middleware<S: IdempotencyStore + Clone + Send + Sync + 'static>() {}
    const _: () = assert_usable_with_middleware::<MemoryStore>();

    const TTL: Duration = Duration::from_secs(60);

    fn response(body: &'static [u8]) -> CachedResponse {
        CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(body),
        }
    }

    #[gtest]
    fn insert_vacant_return_a_claim_with_fencing_token() {
        let mut store = MemoryStoreActor::new();
        let key = IdempotencyKey::new("chimamanda").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let result = store.try_insert(key, entry);
        expect_that!(
            result,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );
    }

    #[gtest]
    fn insert_existing_processing_returns_existing_processing() {
        let mut store = MemoryStoreActor::new();
        let key = IdempotencyKey::new("chimamanda").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);

        let first = store.try_insert(key.clone(), entry.clone());
        expect_that!(
            first,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );

        let second = store.try_insert(key, entry);
        expect_that!(second, pat!(InsertResult::Exists(_)));
    }
    #[gtest]
    fn insert_existing_completed_return_existing_completed() {
        let mut store = MemoryStoreActor::default();
        let key = IdempotencyKey::new("lumumba").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };

        let first = store.try_insert(key.clone(), entry.clone());
        expect_that!(
            first,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );

        let InsertResult::Claimed { fencing_token } = first else {
            return;
        };

        let completed = entry.complete(response, TTL);
        store.complete(key.clone(), completed, fencing_token);

        let second = store.try_insert(
            key,
            IdempotencyEntry::new(DefaultFingerprintStrategy.compute("/submit", &[]), TTL),
        );
        expect_that!(
            second,
            pat!(InsertResult::Exists(pat!(ExistingEntry::Completed(
                anything()
            ))))
        );
    }
    #[gtest]
    fn insert_on_expired_key_claims_entry() {
        let mut store = MemoryStoreActor::default();
        let key = IdempotencyKey::new("lumumba").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, Duration::ZERO);

        let first = store.try_insert(key.clone(), entry);
        expect_that!(
            first,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );

        std::thread::sleep(Duration::from_millis(1));

        let entry = IdempotencyEntry::new(fingerprint, Duration::ZERO);
        let second = store.try_insert(key, entry);
        expect_that!(
            second,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );
    }

    #[gtest]
    fn complete_with_mismatched_fencing_token_is_noop() {
        let mut store = MemoryStoreActor::default();
        let key = IdempotencyKey::new("lumumba").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };

        let first = store.try_insert(key.clone(), entry.clone());
        expect_that!(
            first,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );

        let completed = entry.complete(response, TTL);
        let wrong_token = FencingToken::new(0, u64::MAX);
        store.complete(key.clone(), completed, wrong_token);

        let second = store.try_insert(key, IdempotencyEntry::new(fingerprint, TTL));
        expect_that!(
            second,
            pat!(InsertResult::Exists(pat!(ExistingEntry::Processing(
                anything()
            ))))
        );
    }

    #[gtest]
    fn remove_allows_reinsert() {
        let mut store = MemoryStoreActor::default();
        let key = IdempotencyKey::new("lumumba").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);

        let first = store.try_insert(key.clone(), entry);
        expect_that!(
            first,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );

        let InsertResult::Claimed { fencing_token } = first else {
            panic!("expected fencing token");
        };

        store.remove(&key, fencing_token);

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(key, entry);
        expect_that!(
            second,
            pat!(InsertResult::Claimed {
                fencing_token: anything()
            })
        );
    }

    #[gtest]
    fn sweep_removes_expired_and_keeps_live() {
        let mut store = MemoryStoreActor::default();
        let expired = IdempotencyKey::new("lumumba").expect("valid key");
        let live = IdempotencyKey::new("achebe").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        store.try_insert(
            expired.clone(),
            IdempotencyEntry::new(fingerprint, Duration::ZERO),
        );
        store.try_insert(live.clone(), IdempotencyEntry::new(fingerprint, TTL));

        std::thread::sleep(Duration::from_millis(1));
        store.sweep();

        expect_that!(store.contains(&expired), eq(false));
        expect_that!(store.contains(&live), eq(true));
    }

    #[gtest]
    #[tokio::test]
    async fn concurrent_insert_one_wins() {
        let store = Arc::new(
            MemoryStore::builder()
                .buffer(16)
                .sweep_interval(TTL)
                .try_build()
                .expect("build memory store"),
        );
        let key = IdempotencyKey::new("makeba").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/force", &[]);

        let mut handles = Vec::with_capacity(10);

        for _ in 0..10 {
            let store = Arc::clone(&store);
            let key = key.clone();
            let handle = tokio::spawn(async move {
                let entry = IdempotencyEntry::new(fingerprint, TTL);
                store.try_insert(&key, entry).await.expect("to succeed")
            });
            handles.push(handle);
        }

        let mut claimed = 0;
        let mut existed = 0;

        for handle in handles {
            match handle.await.expect("to get a result") {
                InsertResult::Claimed { .. } => claimed += 1,
                InsertResult::Exists(..) => existed += 1,
            }
        }

        expect_that!(claimed, eq(1));
        expect_that!(existed, eq(9));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_under_contention() {
        let store = Arc::new(
            MemoryStore::builder()
                .buffer(16)
                .sweep_interval(TTL)
                .try_build()
                .expect("build memory store"),
        );
        let key = IdempotencyKey::new("sankara").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/one-africa", &[]);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: Bytes::from_static(b"ok"),
        };

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let InsertResult::Claimed { fencing_token } = store
            .try_insert(&key, entry.clone())
            .await
            .expect("to insert entry")
        else {
            panic!("expected claimed result");
        };

        let completed = entry.complete(response, TTL);
        store
            .complete(&key, completed, fencing_token)
            .await
            .expect("to complete side effect");
        let mut handles = Vec::with_capacity(10);

        for _ in 0..10 {
            let key = key.clone();
            let store = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let entry = IdempotencyEntry::new(fingerprint, TTL);
                store.try_insert(&key, entry).await.expect("to succeed")
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.expect("to get a result");
            let InsertResult::Exists(ExistingEntry::Completed(entry)) = result else {
                panic!("expected existing completed entry");
            };

            expect_that!(entry.response().status_code, eq(200));
        }
    }

    #[gtest]
    fn complete_after_complete_is_rejected() {
        let mut store = MemoryStoreActor::new();
        let key = IdempotencyKey::new("chimamanda").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let InsertResult::Claimed { fencing_token } =
            store.try_insert(key.clone(), IdempotencyEntry::new(fingerprint, TTL))
        else {
            panic!("expected a fresh claim");
        };

        let first = IdempotencyEntry::new(fingerprint, TTL).complete(response(b"first"), TTL);
        let applied = store.complete(key.clone(), first, fencing_token);
        expect_that!(applied, eq(FencedOutcome::Applied));

        let second = IdempotencyEntry::new(fingerprint, TTL).complete(response(b"second"), TTL);
        let rejected = store.complete(key.clone(), second, fencing_token);
        expect_that!(rejected, eq(FencedOutcome::KeyExpired));

        let replay = store.try_insert(key, IdempotencyEntry::new(fingerprint, TTL));
        let InsertResult::Exists(ExistingEntry::Completed(entry)) = replay else {
            panic!("expected Exists(Completed), got {replay:?}")
        };
        expect_that!(entry.response().body, eq(&Bytes::from_static(b"first")));
    }

    #[gtest]
    fn complete_with_foreign_fingerprint_is_rejected() {
        let mut store = MemoryStoreActor::new();
        let key = IdempotencyKey::new("chimamanda").expect("valid key");
        let claimed = DefaultFingerprintStrategy.compute("/submit", b"original");
        let InsertResult::Claimed { fencing_token } =
            store.try_insert(key.clone(), IdempotencyEntry::new(claimed, TTL))
        else {
            panic!("expected a fresh claim");
        };

        let foreign = DefaultFingerprintStrategy.compute("/submit", b"different");
        let completed = IdempotencyEntry::new(foreign, TTL).complete(response(b"ok"), TTL);
        let rejected = store.complete(key.clone(), completed, fencing_token);
        expect_that!(rejected, eq(FencedOutcome::FingerprintMismatch));

        let replay = store.try_insert(key, IdempotencyEntry::new(claimed, TTL));
        expect_that!(
            replay,
            pat!(InsertResult::Exists(pat!(ExistingEntry::Processing(_))))
        );
    }

    #[gtest]
    fn try_build_rejects_invalid_config() {
        let zero_buffer = MemoryStore::builder().buffer(0).try_build();
        expect_that!(zero_buffer, err(pat!(MemoryStoreError::ZeroBuffer)));

        let zero_sweep = MemoryStore::builder()
            .sweep_interval(Duration::ZERO)
            .try_build();
        expect_that!(zero_sweep, err(pat!(MemoryStoreError::ZeroSweepInterval)));

        // A plain test has no Tokio runtime to spawn the task on.
        let no_runtime = MemoryStore::builder().try_build();
        expect_that!(no_runtime, err(pat!(MemoryStoreError::NoRuntime)));
    }

    #[gtest]
    #[tokio::test]
    async fn store_outliving_its_runtime_reports_task_stopped() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let store = MemoryStore::builder()
            .runtime(runtime.handle().clone())
            .try_build()
            .expect("build");
        expect_that!(store.is_healthy(), eq(true));

        // The task goes down with its runtime, so the command channel closes.
        runtime.shutdown_background();
        expect_that!(store.is_healthy(), eq(false));
        expect_that!(store.len().await, err(pat!(MemoryStoreError::TaskStopped)));
    }

    #[gtest]
    #[tokio::test]
    async fn len_counts_live_entries() {
        let store = MemoryStore::builder().try_build().expect("build");
        expect_that!(store.len().await, ok(eq(&0)));
        expect_that!(store.is_empty().await, ok(eq(&true)));

        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let first = IdempotencyKey::new("achebe").expect("valid key");
        let second = IdempotencyKey::new("soyinka").expect("valid key");
        store
            .try_insert(&first, IdempotencyEntry::new(fingerprint, TTL))
            .await
            .expect("claim");
        store
            .try_insert(&second, IdempotencyEntry::new(fingerprint, TTL))
            .await
            .expect("claim");
        expect_that!(store.len().await, ok(eq(&2)));

        store.purge(&first).await.expect("purge");
        expect_that!(store.len().await, ok(eq(&1)));
    }
}
