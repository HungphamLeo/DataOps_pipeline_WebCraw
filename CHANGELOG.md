
---
```go
# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog 1.0.0](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- ST01: `BaseHttpCrawler` with exponential backoff + full jitter, `Retry-After` handling.
- ST02: PostgreSQL pool stale-connection detection and pool metrics logging.
- ST06: GitHub Actions CI (lint, typecheck, test, docker-build) and CD (GHCR publish).
- Pre-commit hooks: ruff, mypy, detect-secrets.

### Changed
- `build_pg_writer` accepts `pool_min`, `pool_max`, `metrics_interval`.
```
---


