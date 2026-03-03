[![Build Status][actions-badge]][actions-url]
[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MPL-2.0 license][mpl-2.0-badge]][mpl-2.0-license]

# idempotent

Idempotency primitives for Rust. Ensures at-most-once execution with
cached response replay.

## Features

- **Typestate entries:** `Processing` → `Completed` enforced at compile time
- **Pluggable stores:** in-memory (dev/single-node)
- **Fencing tokens:** prevents stale completions after key expiry and reclaim
- **Fingerprint matching:** detects mismatched request bodies under the same key

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
idempotent = "0.1.0"
````

## Optional Features

- **serde:** adds [`Serialize`] and [`Deserialize`] to idempotency key and entries

### Supported Rust Versions

The minimum supported Rust version is **1.93.0**.

### License

Unless otherwise noted, this project is licensed under the [Mozilla Public License Version 2.0.](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `idempotent` by you, shall be licensed as MPL-2.0, without any additional
terms or conditions.


[crates-badge]: https://img.shields.io/crates/v/idempotent
[crates-url]: https://crates.io/crates/idempotent
[docs-badge]: https://img.shields.io/docsrs/idempotent/latest
[docs-url]: https://docs.rs/idempotent/latest/idempotent/
[mpl-2.0-badge]: https://img.shields.io/badge/License-MPL_2.0-blue.svg
[mpl-2.0-license]: LICENSE
[actions-badge]: https://github.com/aklanti/idempotent/workflows/CI/badge.svg
[actions-url]: https://github.com/aklanti/idempotent/actions/workflows/main.yaml
[`Serialize`]: https://docs.rs/serde/1/serde/trait.Serialize.html
[`Deserialize`]: https://docs.rs/serde/1/serde/trait.Deserialize.html
