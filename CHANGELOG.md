# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0](https://github.com/aklanti/idempotent/compare/v0.3.0...v1.0.0) - 2026-09-12

### Breaking changes

- Rename `IdempotencyError` to `Error`, with `EmptyKey`, `KeyTooLong`, `InvalidKey`, `EmptyScope`, and `InvalidScope` in place of `InvalidKey(String)`
- Reject a control character, `:`, or `/` in an `IdempotencyKey`, the separators of a store prefix and of a scope, and drop its serde derives
- Remove `IdempotencyConfig`; the leases are arguments of `claim` and `complete`, and the fingerprint strategy an argument of `fingerprint_with`
- Drop `async-trait` from `IdempotencyStore` for `impl Future` return types; `complete` and `remove` return a `FencedOutcome`, `remove` takes the fencing token, and the trait gains `touch` and `purge`
- Take the fencing token out of the entry: `Processing` is a unit state, `IdempotencyEntry::fencing_token` and `fingerprint_matches` are gone, and `InsertResult::Claimed` carries the token
- Make `IdempotencyEntry::complete` take the completed lease, so a completed entry cannot exist without its replay lease, and drop that argument from `IdempotencyStore::complete`
- Make `FencingToken` a server run id and a sequence number issued by the store, in place of a random `u64` with `new()` and `Default`
- Widen `Fingerprint` to 128 bits and separate the operation from the body in the hash, so an operation with an empty body no longer collides with a shorter operation and a body
- Store `Metadata` as an ordered list of names and `Bytes` values instead of a `HashMap<String, Vec<u8>>`, with `append`, `get`, `iter`, and `FromIterator`
- Replace `MemoryStore::new(buffer, sweep_interval)` with `MemoryStore::builder()` and `try_build`, which fails with `MemoryStoreError` on a zero buffer, a zero sweep interval, or a missing runtime instead of panicking
- Replace `ValkeyStore::new` and `with_prefix` with the `with_client`, `with_connection_manager`, and `with_url` builders, and rename `key_prefix` to `prefix`, which rejects a reserved character with `ValkeyError::InvalidPrefix`
- Remove the `tokio` feature; tokio is a dependency of the crate, and `memory` needs no feature to enable it
- Mark `Error` and `CachedResponse` `#[non_exhaustive]`; `CachedResponse::new` builds one from its status code, metadata, and body

### Features

- Add `ClaimBuilder` and `OwnedClaimBuilder` behind `IdempotencyStore::claim` and `claim_owned`, with `fingerprint`, `fingerprint_with`, `try_insert`, and `execute_or_replay`, which claims the key, runs the side effect, caches its response, and answers a retry with an `ExecutionOutcome`
- Add `ClaimGuard` and `OwnedClaimGuard`, with `touch`, `complete`, and `keep_alive`; a dropped owned guard frees its claim, and `OwnedClaimGuard::leave` keeps it until its lease ends
- Add `FencedOutcome`, the store's verdict on a completion, a touch, or a removal, and `ExecutionOutcome::Fenced`, which carries the response the side effect produced when the store rejected the completion
- Add `FencedOutcome::FingerprintMismatch`; a completion whose fingerprint differs from the claim's is rejected, as is a completion of an entry already completed
- Add `FencingToken::new` and `Fingerprint::new`, so a store or a fingerprint strategy can be implemented outside the crate
- Add `Operation`, the method, path, and query a fingerprint hashes; `FingerprintStrategy::compute` takes one, and `fingerprint` and `fingerprint_with` accept anything that converts into it
- Add `fingerprint::body`, which encodes any `Hash` value as fingerprint bytes
- Add `IdempotencyKey::scoped` and `into_scoped`, deriving a child key for one sub-operation, and `Display` for the key
- Add `ExistingEntry::replay` and `ReplayOutcome`, the answer to a retry from the entry that holds the key
- Add `ExecutionError::Completion`, which carries the response when the store fails after the side effect ran
- Add `keep_alive` on both claim builders, renewing the processing lease while the side effect runs up to a ceiling
- Add `MemoryStore::is_healthy`, `close`, `len`, and `is_empty`, a `runtime` setter on its builder, and `Debug` for both stores
- Add `ValkeyStore::ping`, and bound every connection attempt and command with `connection_timeout` and `response_timeout`, five seconds by default
- Add the `middleware` feature: `IdempotencyLayer` wraps a Tower service so that a request with an idempotency key runs once and replays after, `IdempotencyRejection` is what the layer sends on its own, and `stored_key` is the key the store holds under a scope
- Add the `axum` feature: `IdempotencyRejection` is an `IntoResponse` and `IdempotencyKey` a `FromRequestParts` extractor
- Add `TryFrom<&HeaderValue>` for `IdempotencyKey` under the `middleware` feature
- Re-export the claim, guard, and outcome types and the `memory` and `valkey` modules from the crate root

### Fixes

- Fence `remove` on the token, so a claim is only removed by the attempt that holds it
- Extend the processing lease with `touch`, so a handler that outlives the lease is not overtaken by a retry
- Issue Valkey fencing tokens from the server's run id and a counter, so a token issued before a restart is rejected after it, and the counter is monotonic across calls
- Compare the fingerprint when completing on Valkey, and report a missing key and a completed key as distinct rejections
- Mark a claim in progress on Valkey when it is created, and check for the entry rather than its status on replay
- Fix a replay bug on Valkey when extending a TTL
- Compile each Lua script once

### Chores

- Bump the MSRV to 1.98.0
- Remove `async-trait`, `rand`, and `proptest`
- Enable optional dependencies only in the features that use them
- Run doctests with all features

## [0.3.0](https://github.com/aklanti/idempotent/compare/v0.2.0...v0.3.0) - 2026-03-07

### Features

- Add a Valkey or Redis backed idempotency store

### Chores

- Bump MSRV to 1.94
- Bump GitHub actions versions 
