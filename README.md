[![Build Status][actions-badge]][actions-url]
[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MPL-2.0 license][mpl-2.0-badge]][mpl-2.0-license]

[crates-badge]: https://img.shields.io/crates/v/idempotent
[crates-url]: https://crates.io/crates/idempotent
[docs-badge]: https://img.shields.io/docsrs/idempotent/latest
[docs-url]: https://docs.rs/idempotent/latest/idempotent/
[mpl-2.0-badge]: https://img.shields.io/badge/License-MPL_2.0-blue.svg
[mpl-2.0-license]: LICENSE
[actions-badge]: https://github.com/aklanti/idempotent/workflows/CI/badge.svg
[actions-url]: https://github.com/aklanti/idempotent/actions/workflows/main.yaml

# idempotent

A lightweight library for generating and validating idempotency keys.

Idempotency keys ensure that duplicate requests (e.g., due to retries or network issues) are handled safely, enabling exactly-once semantics in your APIs.

## Features

TODO

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
idempotent = "0.0.0"
````

## Optional Features

TBD

 ### Supported Rust Versions

The minimum supported Rust version is **1.93.0**.

### License

Unless otherwise noted, this project is licensed under the [Mozilla Public License Version 2.0.](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `idempotent` by you, shall be licensed as MPL-2.0, without any additional
terms or conditions.
