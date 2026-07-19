# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project overview

GCP connectors for Apache Flink: BigQuery, Cloud Pub/Sub and Cloud Tasks (Bigtable and Spanner
planned). Independent OSS project — not affiliated with the Apache Software Foundation or Google.
Maven multi-module build based on `org.apache.flink:flink-connector-parent`, with Google Cloud
dependencies managed through `com.google.cloud:libraries-bom`.

## Build

- `./mvnw verify` — full build: spotless/checkstyle (validate), unit tests, integration tests,
  apache-rat license-header check. Requires JDK 17 (`mise.toml` pins java/maven; `mise x maven java -- ./mvnw ...` works without global installs)
- `./mvnw spotless:apply` — run before committing; CI fails on unformatted code
- Single module: `./mvnw -pl flink-connector-gcp-bigquery verify`

## Workflow rules

- **One git worktree per PR** under `/tmp/worktrees/flink-connector-gcp/`; never switch branches
  in the main checkout. Remove the worktree and local branch after merge
- All changes go through **draft PRs**; nothing is pushed directly to `main` after the initial
  skeleton
- **After creating a draft PR, always self-review it** (`/code-review`, applying
  simplification/efficiency findings as well) and push the fixes before asking for review.
  Record the findings and deferrals as a PR comment
- Pin GitHub Actions to commit SHAs with pinact (`mise x pinact -- pinact run`) whenever a
  workflow is added or an action version changes
- Commit messages, PR titles/descriptions, code comments, javadoc and issues are written in
  English
- Issues use milestones `v0.1.0` / `v0.2.0` / `v0.3.0+` and GitHub sub-issues; PRs close their
  issue with `Closes #N`

## Version policy

- Releases follow full semver (`v0.1.0`, `v0.2.0`, ...). Early milestones are **tags only** — no
  artifact publishing. Publishing to Maven Central happens once all connectors are implemented,
  as `v1.0.0` (Central namespace registration, signing and the Flink 1.x/2.x publishing strategy
  are decided then; see issues #29 and #39)
- `main` targets **Flink 2.1.x** (planned artifact suffix `-2.1`). Do not bump `flink.version`
  to a newer minor/major via dependabot — that is a deliberate, manual decision (see closed PR
  #42). Flink 1.20 support will live on a dedicated `v1.20` branch
- JUnit stays on 5.x and testcontainers on 1.x for now; their major-version dependabot PRs are
  intentionally left open/deferred
- Google Cloud library versions come only from `libraries-bom`; never pin individual
  google-cloud artifact versions

## Licensing and provenance

- Files written for this project carry the plain Apache-2.0 header
  (`Copyright 2026 laughingman7743`). Files copied from Apache projects keep their ASF header.
  apache-rat enforces this (configuration overridden in the root POM; new unheaderable file
  types need a rat exclude there)
- When adapting Apache-2.0 code from other projects (Beam, Dataproc connector,
  google/flink-connector-gcp, java-bigquerystorage, apache/flink-connector-gcp-pubsub):
  record the provenance in the module README and the repository `NOTICE`, and keep original
  headers where applicable. Keep each module README's "no code copied" claim accurate
- Never open or reference the private in-house implementation this project supersedes; design
  references must be public OSS or official documentation only

## Design decisions (do not silently revisit)

- **BigQuery**: `BigQueryIO`-style facade — one builder, per-write-method SinkV2 implementations.
  Storage Write API connection multiplexing is delegated to the client SDK connection pool
  (`setEnableConnectionPool`); no self-built keyed writer pool. The serializer SPI is an abstract
  class (`BigQueryProtoSerializer`) with `getDescriptor(TableDestination)` + `ByteString`
  rows — not a functional interface (descriptors are not Java-serializable)
- **Pub/Sub**: base implementation is vendored from `GoogleCloudPlatform/pubsub`
  `flink-connector/` (decision record: issues #17 and #31); the Apache connector is only a
  design reference (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`
- Deferred decisions are recorded on PR #46: per-write-method option scoping (decide in #14) and
  `location()` granularity (decide in #10)
