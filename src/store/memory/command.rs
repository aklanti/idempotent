use std::time::Duration;

use tokio::sync::oneshot;

use crate::IdempotencyEntry;
use crate::IdempotencyKey;
use crate::InsertResult;
use crate::entry::Completed;
use crate::entry::Processing;
use crate::fencing_token::FencedOutcome;
use crate::fencing_token::FencingToken;

/// The command to execute on the store.background store task.
pub enum Command {
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
        reply: oneshot::Sender<FencedOutcome>,
    },

    Remove {
        key: IdempotencyKey,
        fencing_token: FencingToken,
        reply: oneshot::Sender<FencedOutcome>,
    },

    Touch {
        key: IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
        reply: oneshot::Sender<FencedOutcome>,
    },

    Purge {
        key: IdempotencyKey,
        reply: oneshot::Sender<()>,
    },

    Len {
        reply: oneshot::Sender<usize>,
    },
}
