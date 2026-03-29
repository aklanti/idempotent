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

/// An in-memory [`IdempotencyStore`] backed by a `HashMap`.
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
            let _ = handle.await; // JoinError here = the task had panicked; best-effort
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
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, Self::Error> {
        let (reply, rx) = oneshot::channel();

        let mut entry = entry;
        entry.ttl = completed_ttl;

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

    /// Spawns the background task on `handle` instead of the ambient runtime.
    pub fn runtime(mut self, handle: Handle) -> Self {
        self.runtime = Some(handle);
        self
    }

    /// Builds the store, spawning its background task.
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
    use googletest::matchers::ok;
    use googletest::matchers::pat;

    use super::*;
    use crate::Metadata;
    use crate::entry::CachedResponse;
    use crate::entry::ExistingEntry;
    use crate::fingerprint::DefaultFingerprintStrategy;
    use crate::fingerprint::FingerprintStrategy;

    const TTL: Duration = Duration::from_secs(60);

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

        let completed = entry.complete(response);
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

        let completed = entry.complete(response);
        let wrong_token = FencingToken(u64::MAX);
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
    fn sweep_removes_expired() {
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
        store.sweep();

        expect_that!(store.contains(&key), eq(false));
    }

    #[gtest]
    fn sweep_keeps_live() {
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

        store.sweep();

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(key, entry);
        expect_that!(second, pat!(InsertResult::Exists(_)));
    }

    #[gtest]
    #[tokio::test]
    async fn insert_and_claim() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(TTL)
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("wangari").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);

        let result = store.try_insert(&key, entry).await;
        expect_that!(
            result,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        );
    }

    #[gtest]
    #[tokio::test]
    async fn insert_duplicate_exists() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(TTL)
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("wangari").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);

        let first = store.try_insert(&key, entry.clone()).await;
        expect_that!(
            first,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        );

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(&key, entry).await;
        expect_that!(second, ok(pat!(InsertResult::Exists(_))));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_and_replay() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(TTL)
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("wangari").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let response = CachedResponse {
            status_code: 201,
            metadata: Metadata::default(),
            body: Bytes::from_static(b"ok"),
        };

        let first = store.try_insert(&key, entry.clone()).await;
        let InsertResult::Claimed { fencing_token } = first.expect("an insertion result") else {
            return;
        };

        let completed = entry.complete(response.clone());
        store
            .complete(&key, completed, fencing_token, TTL)
            .await
            .expect("an insertion result");

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let replay = store.try_insert(&key, entry).await;
        let Ok(InsertResult::Exists(ExistingEntry::Completed(entry))) = replay else {
            panic!("expected Exists(Completed), got {replay:?}");
        };
        let response = entry.response();
        expect_that!(response.status_code, eq(201));
        expect_that!(response.body, eq(&Bytes::from_static(b"ok")));
    }

    #[gtest]
    #[tokio::test]
    async fn complete_wrong_token() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(TTL)
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("wangari").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };

        let first = store.try_insert(&key, entry).await;
        expect_that!(
            first,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        );

        let completed = IdempotencyEntry::new(fingerprint, TTL).complete(response);
        let wrong_token = FencingToken(4);
        store
            .complete(&key, completed, wrong_token, TTL)
            .await
            .expect("entry to complete");

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(&key, entry).await;
        expect_that!(
            second,
            ok(pat!(InsertResult::Exists(pat!(ExistingEntry::Processing(
                anything()
            )))))
        );
    }

    #[gtest]
    #[tokio::test]
    async fn remove_and_reclaim() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(TTL)
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("wangari").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute("/submit", &[]);
        let entry = IdempotencyEntry::new(fingerprint, TTL);

        let first = store.try_insert(&key, entry).await;
        expect_that!(
            first,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        );

        let Ok(InsertResult::Claimed { fencing_token }) = first else {
            panic!("expected claimed");
        };

        store
            .remove(&key, fencing_token)
            .await
            .expect("entry to be removed");

        let entry = IdempotencyEntry::new(fingerprint, TTL);
        let second = store.try_insert(&key, entry).await;
        expect_that!(
            second,
            ok(pat!(InsertResult::Claimed {
                fencing_token: anything()
            }))
        );
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

        let completed = entry.complete(response);
        store
            .complete(&key, completed, fencing_token, TTL)
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
}
