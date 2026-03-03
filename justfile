export RUST_LOG := env("RUST_LOG", "debug")

# List available recipe.
@help:
    just --list

alias t := test

# Build the documentation.
@doc *args:
    cargo doc --no-deps --all-features {{ args }}

# Run test coverage.
@cov *args:
    cargo llvm-cov clean
    cargo llvm-cov nextest --all-features {{ args }}

# Check for common mistakes
@clippy:
    cargo clippy --all-features --all-targets

# Check whether the dependencies follow the established rules
@deny:
    cargo deny check

# Run various formatter
@fmt:
    cargo fmt --all
    cargo sort --grouped
    just --fmt --unstable

# Checks combinations of features flags to ensure that features are all additive as required for feature unification.
@hack:
    cargo hack --feature-powerset check

# Install workspace tools
@install-tools:
    cargo install cargo-binstall
    cargo binstall --no-confirm cargo-deny
    cargo binstall --no-confirm cargo-hack
    cargo binstall --no-confirm cargo-llvm-cov
    cargo binstall --no-confirm cargo-nextest
    cargo binstall --no-confirm cargo-shear
    cargo binstall --no-confirm cargo-sort

# Run all linters
@lint: clippy deny hack shear
    cargo sort --grouped --check
    just --fmt --unstable --check

# Check for unused dependencies
@shear *args:
    cargo shear {{ args }}

# Run tests
@test *args:
    cargo nextest run --all-features -j 12 {{ args }}
    cargo test --doc
