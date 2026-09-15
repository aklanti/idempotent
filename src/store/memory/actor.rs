use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use super::InsertResult;
use super::command::Command;
use crate::FencedOutcome;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::fencing_token::Rejection;
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

    pub async fn run(mut self, mut rx: mpsc::Receiver<Command>, sweep_interval: Duration) {
        let mut interval = tokio::time::interval(sweep_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                        },
                        Command::Len{reply} => {
                            let _ = reply.send(self.entries.len());
                        }
                  }
              },
               _ = interval.tick() => self.sweep()
            }
        }

        #[cfg(feature = "tracing")]
        tracing::debug!("idempotency store background task stopped");
    }

    pub fn try_insert(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> InsertResult {
        if let Some(record) = self.entries.get(&key).filter(|r| !r.is_expired()) {
            return InsertResult::Exists(record.existing.clone());
        }

        #[cfg(feature = "tracing")]
        if self.entries.contains_key(&key) {
            tracing::info!(key = %key, "reclaimed expired key");
        }

        self.next_token += 1;
        let fencing_token = FencingToken::new(0, self.next_token);
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

    pub fn complete(
        &mut self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> FencedOutcome {
        let Some(record) = self
            .entries
            .get_mut(&key)
            .filter(|record| !record.is_expired())
        else {
            return Rejection::KeyExpired.into();
        };
        if record.fencing_token != fencing_token {
            return Rejection::FencingMismatch.into();
        }
        let ExistingEntry::Processing(processing) = &record.existing else {
            return Rejection::KeyExpired.into();
        };
        if processing.fingerprint != entry.fingerprint {
            return Rejection::FingerprintMismatch.into();
        }
        record.ttl = entry.ttl;
        record.existing = ExistingEntry::Completed(entry);
        record.created_at = Instant::now();
        FencedOutcome::Applied
    }

    pub fn remove(&mut self, key: &IdempotencyKey, fencing_token: FencingToken) -> FencedOutcome {
        match self.entries.get(key).filter(|record| !record.is_expired()) {
            Some(record) if record.fencing_token == fencing_token => {
                self.entries.remove(key);
                FencedOutcome::Applied
            }
            Some(_) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(key = %key, "remove rejected: fencing mismatch");
                Rejection::FencingMismatch.into()
            }
            None => Rejection::KeyExpired.into(),
        }
    }

    pub fn touch(
        &mut self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> FencedOutcome {
        let Some(record) = self
            .entries
            .get_mut(key)
            .filter(|record| !record.is_expired())
        else {
            return Rejection::KeyExpired.into();
        };
        if record.fencing_token != fencing_token {
            return Rejection::FencingMismatch.into();
        }
        let ExistingEntry::Processing(_) = &record.existing else {
            return Rejection::KeyExpired.into();
        };
        record.created_at = Instant::now();
        record.ttl = ttl;
        FencedOutcome::Applied
    }

    pub fn sweep(&mut self) {
        #[cfg(feature = "tracing")]
        let before = self.entries.len();
        self.entries.retain(|_, record| !record.is_expired());
        #[cfg(feature = "tracing")]
        tracing::debug!(
            removed = before - self.entries.len(),
            remaining = self.entries.len(),
            "sweep complete"
        );
    }

    pub fn purge(&mut self, key: &IdempotencyKey) {
        #[cfg(feature = "tracing")]
        tracing::warn!(key = %key, "purge: unfenced delete");
        self.entries.remove(key);
    }

    #[cfg(test)]
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
