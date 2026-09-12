[![Build Status][badge-actions]][url-actions]
[![Crates.io][badge-crate]][url-crate]
[![Documentation][badge-docs]][url-docs]
[![MPL-2.0 license][badge-license]][url-license]

# idempotent

At-most-once execution of side effects: for a given idempotency key the side effect runs at most once, and every retry within the TTL window receives the cached response.

## Highlights

- **Claim, run, cache in one call:** `execute_or_replay` claims the key, runs your side effect once, caches its response, and replays it to every retry within the TTL
- **Typestate entries:** `Processing` → `Completed` is checked at compile time, so an entry cannot be completed twice or without its replay lease
- **Fencing tokens:** a completion from an attempt that lost its claim is rejected, including an attempt that started before a Valkey restart
- **Fingerprint matching:** a retry that carries a different request than the original is rejected; ships with an xxHash3 default, implement `FingerprintStrategy` for your own
- **Pluggable stores:** an in-memory store and a Valkey/Redis store; implement [`IdempotencyStore`][url-docs-store] for another backend
- **UUID keys by default:** `IdempotencyKey::default()` generates a random UUID v4

## Usage

Add to your `Cargo.toml`

```toml
[dependencies]
idempotent = { version = "1.0.0", features = ["memory"] }
```

### Quick example

A wallet asks an issuer for a credential and retries when the answer is lost. The issuer signs the credential once and hands the same one to every retry.

```rust
use std::time::Duration;

use idempotent::memory::MemoryStore;
use idempotent::{CachedResponse, ExecutionOutcome, IdempotencyKey, IdempotencyStore, Metadata};

/// Signs the credential and returns it as the response to cache.
async fn issue_credential() -> CachedResponse {
    CachedResponse {
        status_code: 201,
        metadata: Metadata::new(),
        body: br#"{"type": "UniversityDegreeCredential", "proof": "..."}"#.to_vec().into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::builder().try_build()?;
    let key = IdempotencyKey::new("offer-8f21")?;
    let request = br#"{"holder": "did:web:alice.example", "type": "UniversityDegreeCredential"}"#;

    let outcome = store
        .claim(&key, Duration::from_secs(30))
        .fingerprint("POST /credentials/issue", request)
        .execute_or_replay(Duration::from_secs(24 * 60 * 60), |_token| async {
            Ok(issue_credential().await)
        })
        .await?;

    match outcome {
        ExecutionOutcome::Executed(credential) => println!("issued: {}", credential.status_code),
        ExecutionOutcome::Replayed(credential) => println!("same credential again: {}", credential.status_code),
        ExecutionOutcome::InFlight => println!("the first request is still signing it"),
        ExecutionOutcome::FingerprintMismatch => println!("the key was reused for a different request"),
        ExecutionOutcome::Fenced { rejection, .. } => println!("issued, but the claim was lost: {rejection:?}"),
    }
    Ok(())
}
```

The first request for the offer claims its key for 30 seconds, the processing lease, while the credential is signed. The signed credential is cached for a day, the completed lease, so a retry within that day gets the same credential rather than a second one. A retry while the first request is still signing gets `InFlight`, and a retry with a different request body gets `FingerprintMismatch`. If signing fails, the claim is left to expire so a later retry tries again.

For control over each step, `try_insert` returns a `ClaimGuard` to `touch` while the work runs and to `complete` with the response. For a side effect slower than the processing lease, `keep_alive` on the builder renews the lease while it runs, up to a ceiling; past the ceiling the lease lapses and the completion reports `Fenced`.

### Owned claims

`claim_owned` returns a builder whose futures own a clone of the store and the key, so a claim can live in a struct, move into a spawned task, or run on another runtime. Dropping an owned claim mid side effect frees the key at once; a failed side effect leaves it to expire, as on the borrowing path; `leave` keeps it deliberately.

```rust
use std::time::Duration;

use idempotent::memory::MemoryStore;
use idempotent::{IdempotencyKey, IdempotencyStore, OwnedClaimOutcome};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::builder().try_build()?;
    let key = IdempotencyKey::new("offer-8f21")?;

    let claimed = tokio::spawn(async move {
        store
            .claim_owned(key, Duration::from_secs(30))
            .fingerprint("POST /credentials/issue", b"{}")
            .try_insert()
            .await
    })
    .await??;

    if let OwnedClaimOutcome::Claimed(guard) = claimed {
        guard.leave();
    }
    Ok(())
}
```

### Fingerprinting a typed request

The fingerprint covers the operation and the request bytes. When the request is already a value, `fingerprint::body` hashes it through `Hash`, so field order and formatting do not matter:

```rust
use std::time::Duration;

use idempotent::fingerprint;
use idempotent::memory::MemoryStore;
use idempotent::{IdempotencyKey, IdempotencyStore};

#[derive(Hash)]
struct IssueRequest {
    holder: String,
    credential_type: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::builder().try_build()?;
    let key = IdempotencyKey::new("cred-offer-123")?;
    let request = IssueRequest {
        holder: "did:web:alice.example".to_owned(),
        credential_type: "UniversityDegreeCredential".to_owned(),
    };

    let _claim = store
        .claim(&key, Duration::from_secs(30))
        .fingerprint("POST /credentials/issue", &fingerprint::body(&request))
        .try_insert()
        .await?;
    Ok(())
}
```

### How a retry is answered

| The key holds | The retry's fingerprint | Result |
|---|---|---|
| a running request | the same | `InFlight`: answer 409 and let the client retry later |
| a cached response | the same | `Replayed`: return the cached response |
| either | different | `FingerprintMismatch`: answer 422, the key was reused for another request |

### Choosing keys and leases

- One key per operation, generated by the client: a UUID v4, or `IdempotencyKey::default()`. A retry reuses the key; a new operation gets a new key. Fingerprints are 128-bit xxHash3, so a collision between two different requests under one key is negligible.
- The processing lease must outlast the slowest run of the side effect; seconds to a minute is typical. `ClaimGuard::touch` extends it while the work runs.
- The completed lease is how long a retry can replay the response: hours to a day, matching how long your clients keep retrying.

### Side effects that cannot be undone

A credential issued twice is two valid credentials in circulation, and a card charged twice is two charges. Complete the claim only when the side effect fully succeeded. If it fails part way, return an error from the side effect so the claim is left to expire, and reconcile before it does. The library cannot know what the side effect did.

If the process dies after the side effect ran but before the completion was stored, the retry runs the side effect again. At-most-once holds only when the side effect and its completion share a failure domain, for example a database transaction that writes both, or when the side effect is itself idempotent.

### Running on Valkey

Enable AOF persistence (`appendonly yes`) and disable eviction (`maxmemory-policy noeviction`): an evicted key loses its claim, and the retry runs again. Use a single node. A failover to a replica changes the server's run id, so attempts started on the old primary are fenced after promotion, but claims that had not replicated are gone with their keys. With `appendfsync everysec` a crash can still lose the last second of claims; `appendfsync always` closes that window at one fsync per claim.

`with_url` builds a store from a connection string; `with_client` and `with_connection_manager` take a redis client or a manager the application already holds. Every connection attempt and every command is bounded, five seconds each by default:

```rust,no_run
use std::time::Duration;

use idempotent::valkey::ValkeyStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _store = ValkeyStore::with_url("redis://127.0.0.1:6379")
        .prefix("issuer")
        .connection_timeout(Duration::from_secs(2))
        .response_timeout(Duration::from_secs(2))
        .try_build()
        .await?;
    Ok(())
}
```

## Optional features

- **memory:** the in-memory store, for development or a single process
- **valkey:** the Valkey/Redis store, using Lua scripts for atomic operations
- **tracing:** instruments store operations with [`tracing`][url-tracing] spans and events
- **serde:** derives [`Serialize`][url-serde-serialize] and [`Deserialize`][url-serde-deserialize] on `IdempotencyEntry`, `CachedResponse`, `Metadata`, `Fingerprint`, and `FencingToken`
- **uuid:** `IdempotencyKey::default()`, on by default

## Supported Rust versions

The minimum supported Rust version is **1.98.0**.

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
[url-license]: https://github.com/aklanti/idempotent/blob/main/LICENSE
[url-serde-serialize]: https://docs.rs/serde/1/serde/trait.Serialize.html
[url-serde-deserialize]: https://docs.rs/serde/1/serde/trait.Deserialize.html
[url-tracing]: https://docs.rs/tracing/latest/tracing
