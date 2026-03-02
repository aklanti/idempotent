//! This module define the in-memory store

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use super::{IdempotencyStore, InsertResult};
use crate::entry::{Completed, ExistingEntry, FencingToken, IdempotencyEntry, Processing};
use crate::key::IdempotencyKey;

/// In-memory idempotency entry store
pub struct MemoryStore {
    tx: mpsc::Sender<StoreAction>,
}

impl MemoryStore {
    /// Creates a new memory store
    ///
    /// It also spawns a background task that processes each storage action
    #[cfg_attr(feature = "tracing", tracing::instrument(name = "MemoryStore::new"))]
    pub fn new(buffer: usize, sweep_interval: Duration) -> Self {
        let (tx, rx) = mpsc::channel(buffer);
        let store_state = StoreState::new();
        tokio::spawn(store_state.run(rx, sweep_interval));
        Self { tx }
    }
}

#[async_trait::async_trait]
impl IdempotencyStore for MemoryStore {
    type Error = Infallible;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStore::try_insert", skip(self), err(Debug))
    )]
    async fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Result<InsertResult, Self::Error> {
        let (reply, rx) = oneshot::channel();
        let action = StoreAction::TryInsert {
            key: key.clone(),
            entry,
            reply,
        };
        let _ = self.tx.send(action).await;
        let result = rx.await.expect("a response");
        Ok(result)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStore::complete", skip(self), err(Debug))
    )]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<(), Self::Error> {
        let (reply, rx) = oneshot::channel();
        let action = StoreAction::Complete {
            key: key.clone(),
            entry,
            fencing_token,
            reply,
        };
        let _ = self.tx.send(action).await;
        rx.await.expect("a response");
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStore::remove", skip(self), err(Debug))
    )]
    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error> {
        let (reply, rx) = oneshot::channel();
        let action = StoreAction::Remove {
            key: key.clone(),
            reply,
        };
        let _ = self.tx.send(action).await;
        rx.await.expect("a response");
        Ok(())
    }
}

/// A storage for the in-memory data
#[derive(Debug, Default)]
struct StoreState {
    entries: HashMap<IdempotencyKey, StoreRecord>,
}

impl StoreState {
    fn new() -> Self {
        Self::default()
    }
}

impl StoreState {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "StoreState::run", skip(self, rx))
    )]
    async fn run(mut self, mut rx: mpsc::Receiver<StoreAction>, sweep_interval: Duration) {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            tokio::select! {
                action = rx.recv() => {
                    let Some(action) = action else { break };
                    match action {
                        StoreAction::TryInsert {
                         key, entry, reply
                        } => {
                            let _ = reply.send(self.try_insert(key, entry));
                        },
                        StoreAction::Complete {
                            key, entry, fencing_token,
                            reply,
                        } => {
                            self.complete(key, entry, fencing_token);
                            let _ = reply.send(());
                        },
                        StoreAction::Remove {key, reply} => {
                            self.remove(&key);
                            let _ = reply.send(());
                        }
                    }
                },
                _ = interval.tick() => self.sweep()
            }
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "StoreState::try_insert")
    )]
    fn try_insert(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> InsertResult {
        if let Some(record) = self.entries.get(&key).filter(|r| !r.is_expired()) {
            return InsertResult::Exists(record.existing.clone());
        }

        let fencing_token = entry.fencing_token();
        let ttl = entry.ttl;
        let record = StoreRecord {
            existing: ExistingEntry::Processing(entry),
            created_at: Instant::now(),
            ttl,
        };
        self.entries.insert(key, record);
        InsertResult::Claimed { fencing_token }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "StoreState::complete")
    )]
    fn complete(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) {
        if let Some(record) = self.entries.get_mut(&key)
            && let ExistingEntry::Processing(existing) = &record.existing
            && existing.fencing_token() == fencing_token
        {
            record.ttl = entry.ttl;
            record.existing = ExistingEntry::Completed(entry);
            record.created_at = Instant::now();
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(name = "StoreState::remove"))]
    fn remove(&mut self, key: &IdempotencyKey) {
        self.entries.remove(key);
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(name = "StoreState::sweep"))]
    fn sweep(&mut self) {
        self.entries.retain(|_, record| !record.is_expired());
    }
}

/// Represent the idempotency entry data stored in the memory
#[derive(Debug)]
struct StoreRecord {
    existing: ExistingEntry,
    created_at: Instant,
    ttl: Duration,
}

impl StoreRecord {
    /// Whether a record has expired
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() >= self.ttl
    }
}
/// Actions to perform to perform on the memory store
enum StoreAction {
    /// Insertion attempt request
    TryInsert {
        key: IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
        reply: oneshot::Sender<InsertResult>,
    },
    Complete {
        key: IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        /// A fencing token from the claimed result
        fencing_token: FencingToken,
        reply: oneshot::Sender<()>,
    },
    Remove {
        key: IdempotencyKey,
        reply: oneshot::Sender<()>,
    },
}
