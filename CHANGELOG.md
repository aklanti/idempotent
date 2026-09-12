# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features

- Add `ValkeyStore::with_url`, and bound every connection attempt and command with `connection_timeout` and `response_timeout`, five seconds by default
- Add `OwnedClaimBuilder`: `claim_owned` takes the processing lease and builds an owned claim the way `claim` does; `OwnedClaimGuard::leave` keeps a claim until its lease ends
- Add `ExistingEntry::replay` and `ReplayOutcome`, the answer to a retry from the entry that holds the key
- Add `ExecutionError::Completion`, which carries the response when the store fails after the side effect ran
- Add `keep_alive` on both claim builders, renewing the processing lease while the side effect runs up to a ceiling

## [0.3.0](https://github.com/aklanti/idempotent/compare/v0.2.0...v0.3.0) - 2026-03-07

### Features

- Add a Valkey or Redis backed idempotency store

### Chores

- Bump MSRV to 1.94
- Bump GitHub actions versions 
