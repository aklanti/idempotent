use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::mpsc;

use super::InsertResult;
use super::command::Command;
use crate::FencedOutcome;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;

#[derive(Debug, Default)]
pub struct MemoryStoreActor {
    entries: HashMap<IdempotencyKey, StoreRecord>,
    next_token: u64,
}

impl MemoryStoreActor {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::run", skip(self, rx))
    )]
    pub async fn run(mut self, mut rx: mpsc::Receiver<Command>, sweep_interval: Duration) {
        let mut interval = tokio::time::interval(sweep_interval);
        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    match cmd {
                        Command::TryInsert {
                         key, entry, reply,
                        } => {
                            let _ = reply.send(self.try_insert(key, entry));
                        },
                        Command::Complete {
                            key, entry, fencing_token,
                            reply,
                        } => {
                            let result = self.complete(key, entry, fencing_token);
                            let _ = reply.send(result);

                        },
                        Command::Remove {key, reply, fencing_token} => {
                            let result = self.remove(&key, fencing_token);
                            let _ = reply.send(result);
                        },
                        Command::Touch {key, fencing_token, ttl, reply} => {
                            let result = self.touch(&key, fencing_token, ttl);
                            let _ = reply.send(result);
                        },
                        Command::Purge{key, reply} => {
                            self.purge(&key);
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
        tracing::instrument(name = "MemoryStoreActor::try_insert", skip(self), fields(key = %key)),
    )]
    pub fn try_insert(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> InsertResult {
        if let Some(record) = self.entries.get(&key).filter(|r| !r.is_expired()) {
            return InsertResult::Exists(record.existing.clone());
        }

        #[cfg(feature = "tracing")]
        if self.contains(&key) {
            tracing::info!(key = %key, "reclaimed expired key");
        }

        self.next_token += 1;
        let fencing_token = FencingToken(self.next_token);
        let ttl = entry.ttl;
        let record = StoreRecord {
            existing: ExistingEntry::Processing(entry),
            fencing_token,
            created_at: Instant::now(),
            ttl,
        };
        self.entries.insert(key, record);
        InsertResult::Claimed { fencing_token }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::complete")
    )]
    pub fn complete(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> FencedOutcome {
        if let Some(record) = self
            .entries
            .get_mut(&key)
            .filter(|record| !record.is_expired())
            && let ExistingEntry::Processing(_) = &record.existing
        {
            if record.fencing_token == fencing_token {
                record.ttl = entry.ttl;
                record.existing = ExistingEntry::Completed(entry);
                record.created_at = Instant::now();
                return FencedOutcome::Applied;
            }
            #[cfg(feature = "tracing")]
            tracing::warn!(key = %key, "fencing mismatch: zombie completion rejected");
            return FencedOutcome::FencingMismatch;
        }

        #[cfg(feature = "tracing")]
        tracing::warn!(key = %key, "key expired before completion");
        FencedOutcome::KeyExpired
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::remove", fields(key = %key)),
    )]
    pub fn remove(&mut self, key: &IdempotencyKey, fencing_token: FencingToken) -> FencedOutcome {
        match self.entries.get(key).filter(|record| !record.is_expired()) {
            Some(record) if record.fencing_token == fencing_token => {
                self.entries.remove(key);
                FencedOutcome::Applied
            }
            Some(_) => FencedOutcome::FencingMismatch,
            None => FencedOutcome::KeyExpired,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::touch", fields(key = %key)),
    )]
    fn touch(
        &mut self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> FencedOutcome {
        if let Some(record) = self
            .entries
            .get_mut(key)
            .filter(|record| !record.is_expired())
            && let ExistingEntry::Processing(_) = &record.existing
        {
            if record.fencing_token == fencing_token {
                record.created_at = Instant::now();
                record.ttl = ttl;
                return FencedOutcome::Applied;
            }
            return FencedOutcome::FencingMismatch;
        }
        FencedOutcome::KeyExpired
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::sweep", skip(self))
    )]
    pub fn sweep(&mut self) {
        let before = self.entries.len();
        self.entries.retain(|_, record| !record.is_expired());
        let removed = before - self.entries.len();
        #[cfg(feature = "tracing")]
        tracing::debug!(removed, remaining = self.entries.len(), "sweep complete");
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "MemoryStoreActor::purge", fields(key = %key)),
    )]
    pub fn purge(&mut self, key: &IdempotencyKey) {
        self.entries.remove(key);
    }

    pub fn contains(&self, key: &IdempotencyKey) -> bool {
        self.entries.contains_key(key)
    }
}

/// A record stored in the in-memory map, pairing an entry with its expiry.
#[derive(Debug)]
struct StoreRecord {
    existing: ExistingEntry,
    fencing_token: FencingToken,
    created_at: Instant,
    ttl: Duration,
}

impl StoreRecord {
    /// Whether a record has expired
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() >= self.ttl
    }
}
