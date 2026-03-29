/// The error type for operations on the in-memory store.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MemoryStoreError {
    /// The background task has stopped.
    #[error("memory store task stopped")]
    TaskStopped,
    /// The configured channel buffer was zero.
    #[error("buffer must be greater than zero")]
    ZeroBuffer,
    /// The configured sweep interval was zero.
    #[error("sweep interval must be greater than zero")]
    ZeroSweepInterval,
    /// No Tokio runtime was available to spawn the background task on.
    #[error("no tokio runtime")]
    NoRuntime,
}
