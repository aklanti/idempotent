[![Build Status][badge-actions]][url-actions]
[![Crates.io][badge-crate]][url-crate]
[![Documentation][badge-docs]][url-docs]
[![MPL-2.0 license][badge-license]][url-license]

# idempotent

Idempotency library with at-most-once execution and response caching.

## Highlights

- **Typestate entries:** `Processing` → `Completed` is checked at compile time, so you can't forget to complete an entry or complete one twice
- **Pluggable stores:** comes with an in-memory store and a Valkey/Redis store; implement [`IdempotencyStore`][url-docs-store] for your own backend
- **Fencing tokens:** rejects stale completions when a key expires and gets reclaimed while the original request is still running
- **Fingerprint matching:** returns a conflict when a retry carries a different request body than the original
- **UUID keys by default:** `IdempotencyKey::default()` generates a random UUID v4

## Usage

Add to your `Cargo.toml`

```toml
[dependencies]
idempotent = { version = "0.3.0", features = ["memory"] }
```

### Quick example

```rust
use idempotent::{
    IdempotencyKey, IdempotencyEntry, IdempotencyStore,
    InsertResult, Processing, Completed, CachedResponse,
};
use idempotent::memory::MemoryStore;

let store = MemoryStore::new();
let key = IdempotencyKey::default();
let entry = IdempotencyEntry::new(fingerprint);

match store.try_insert(&key, entry).await? {
    InsertResult::Claimed { fencing_token } => {
        // Execute your side effect (e.g. charge a payment)
        let response = handle_request().await;

        let completed = entry.complete(CachedResponse::from(response));
        store.complete(&key, completed, fencing_token).await?;
    }
    InsertResult::Exists(existing) => {
        // Return the cached response or signal a conflict
    }
}
```

## Optional features

- **memory:** enables the in-memory store, suitable for development or single-node deployment
- **valkey:** enables the Valkey/Redis store, using Lua scripts for atomic operations
- **tracing:** instruments store operations with [`tracing`][url-tracing] spans and events
- **serde:** derives [`Serialize`][url-serde-serialize] and [`Deserialize`][url-serde-deserialize] on `IdempotencyKey`, `IdempotencyEntry`, `Fingerprint`, and `FencingToken`

## Supported Rust versions

The minimum supported Rust version is **1.94.0**.

## License

Unless otherwise noted, this project is licensed under the [Mozilla Public License Version 2.0][url-license].

[badge-actions]: https://github.com/aklanti/idempotent/workflows/CI/badge.svg
[url-actions]: https://github.com/aklanti/idempotent/actions/workflows/main.yaml
[badge-crate]: https://img.shields.io/crates/v/idempotent
[url-crate]: https://crates.io/crates/idempotent
[badge-docs]: https://img.shields.io/docsrs/idempotent/latest
[url-docs]: https://docs.rs/idempotent/latest/idempotent
[url-docs-store]: https://docs.rs/idempotent/latest/idempotent/trait.IdempotencyStore.html
[badge-license]: https://img.shields.io/badge/License-MPL_2.0-blue.svg
[url-license]: LICENSE
[url-serde-serialize]: https://docs.rs/serde/1/serde/trait.Serialize.html
[url-serde-deserialize]: https://docs.rs/serde/1/serde/trait.Deserialize.html
[url-tracing]: https://docs.rs/tracing/latest/tracing
