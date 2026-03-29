export RUST_LOG := env("RUST_LOG", "debug")

# List available recipe.
@help:
    just --list

alias t := test

[doc('Build the documentation.')]
[group('doc')]
@doc *args:
    cargo doc --no-deps --all-features {{ args }}

[doc('Run test coverage.')]
[group('test')]
@cov *args:
    cargo llvm-cov clean
    cargo llvm-cov nextest --all-features {{ args }}

[doc('Check for common mistakes.')]
[group('lint')]
@clippy:
    cargo clippy --all-features --all-targets

[doc('Check whether the dependencies follow the established rules.')]
[group('deps')]
@deny:
    cargo deny check

[doc('Run various formatter')]
[group('fmt')]
@fmt *args:
    cargo +nightly fmt --all {{ if args == "--check" { "-- --check" } else { "" } }}
    cargo sort --grouped
    just --fmt --unstable

[doc('Checks combinations of features flags to ensure that features are all additive as required for feature unification.')]
[group('deps')]
@hack:
    cargo hack --feature-powerset check

[doc('Install workspace tools.')]
[group('tools')]
@install-tools:
    cargo install cargo-binstall
    cargo binstall --no-confirm cargo-deny
    cargo binstall --no-confirm cargo-hack
    cargo binstall --no-confirm cargo-llvm-cov
    cargo binstall --no-confirm cargo-nextest
    cargo binstall --no-confirm cargo-shear
    cargo binstall --no-confirm cargo-sort

# Run all linters
[group('lint')]
@lint: clippy deny hack shear
    cargo sort --grouped --check
    just --fmt --unstable --check

# Check for unused dependencies
[group('deps')]
@shear *args:
    cargo shear {{ args }}

# Run tests
[group('test')]
@test *args:
    cargo nextest run --all-features -j 12 {{ args }}
    cargo test --doc
