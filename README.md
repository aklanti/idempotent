[![Build Status][badge-actions]][url-actions]
[![Crates.io][badge-crate]][url-crate]
[![Documentation][badge-docs]][url-docs]
[![MPL-2.0 license][badge-license]][url-license]

# idempotent

Idempotency library with at-most-once execution and response caching.

## Features

- **Typestate entries:** `Processing` → `Completed` enforced at compile time
- **Pluggable stores:** an in-memory suitable for development or single node deployment
- **Fencing tokens:** prevents stale completions after key expiry and reclaim
- **Fingerprint matching:** detects mismatched request bodies under the same key

## Usage

Add to your `Cargo.toml`

```toml
[dependencies]
idempotency = { version = "0.1.2", features = ["memory"] }
```

## Optional Features

- **serde:** adds [`Serialize`][url-serde-serialize] and [`Deserialize`][url-serde-deserialize] to idempotency key and entries

## Supported Rust versions

The minimum supported Rust version is **1.93.0**.

## License

Unless otherwise noted, this project is licensed under the [Mozilla Public License Version 2.0][url-license].

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `idempotent` by you, shall be licensed as MPL-2.0, without any additional
terms or conditions.

[badge-actions]: https://github.com/aklanti/idempotent/workflows/CI/badge.svg
[url-actions]: https://github.com/aklanti/idempotent/actions/workflows/main.yaml
[badge-crate]: https://img.shields.io/crates/v/idempotent
[url-crate]: https://crates.io/crates/idempotent
[badge-docs]: https://img.shields.io/docsrs/idempotent/latest
[url-docs]: https://docs.rs/idempotent/latest/idempotent
[badge-license]: https://img.shields.io/badge/License-MPL_2.0-blue.svg
[url-license]: LICENSE
[url-serde-serialize]: https://docs.rs/serde/1/serde/trait.Serialize.html
[url-serde-deserialize]: https://docs.rs/serde/1/serde/trait.Deserialize.html
