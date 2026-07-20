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

## Package layout convention (all connector modules)

Under `io.github.flink.gcp.connector.<product>` (decided in #63, applied to BigQuery first;
Pub/Sub, Cloud Tasks and later modules follow the same skeleton):

- `sink` — public sink API only: the facade + builder, write-method enum, shared options/enums,
  destination types, and the `@Internal` types shared by every write method (the sink config,
  the fixed-destination resolver, `RetrySchedule` until #61 extracts a shared retry module)
- `sink.<writepath>` — one subpackage per write-path family, which may host several write
  methods (BigQuery: `sink.storageapi` holds the Storage Write API family — the default-stream
  at-least-once method today, and the #30 buffered-stream exactly-once method beside it,
  sharing the appender machinery; `sink.fileloads` holds FILE_LOADS). The package root holds
  the Sink classes, the family's public options objects and committable contracts; internal
  stages follow the Flink FileSink precedent with `.writer`, `.committer` and
  post-commit-topology subpackages (`.loadjob` here, FileSink's `.compactor`) as the topology
  requires — a family without 2PC simply has no `.committer` package
- `sink.tables` — shared table-metadata layer consumed by every write method: the `TableAdmin`
  SPI and its REST implementation, schema snapshot/unifier, REST↔Storage schema converters
- `sink.serializer` — record-conversion SPI and its implementations
- `sink.failure` — row-level failure SPI (`FailedRow`, handlers, DLQ stub), kept separate so the
  cross-connector extraction planned in #37 stays cheap
- `source` / `table` — reserved for sources (#31, #34, #64) and Table API (#47, #57), with the
  same philosophy: public API at the package root, implementation subpackages beneath

A new top-level class in a module's `sink` root needs a reason to be public API; implementation
types belong in the subpackages. Test sources mirror the main-tree packages.

## Design decisions (do not silently revisit)

- **BigQuery**: `BigQueryIO`-style facade — one builder, per-write-method SinkV2 implementations.
  Storage Write API connection multiplexing is delegated to the client SDK connection pool
  (`setEnableConnectionPool`); no self-built keyed writer pool. The serializer SPI is an abstract
  class (`BigQueryProtoSerializer`) with `getDescriptor(TableDestination)` + `ByteString`
  rows — not a functional interface (descriptors are not Java-serializable)
- **BigQuery error handling** (#13): a single `FailedRowHandler` SPI covers all row-level
  failure policies — fail-job (default), log-and-drop, and DLQ routing (the `DeadLetterQueue`
  interface is an experimental stub; lifecycle and shared-module extraction are decided in #37).
  `FailedRow` carries serialized protobuf bytes, not the original record (the writer is
  stateless). SDK in-stream retry settings are hardcoded in `StreamWriterRowAppenderFactory`;
  exposing them is deferred until a real-world need shows which knobs matter
- **BigQuery FILE_LOADS** (#14, load stage revised in #69): exactly-once via deterministic
  BigQuery job ids (hash of destination + sorted staged URIs) with get-then-submit re-attach.
  Avro-only staging in v0.1, written with the `google-cloud-storage` client directly (no Flink
  filesystem plugin dependency). Load jobs run **in the committer** behind a pre-commit topology
  (`SupportsPreCommitTopology`) whose trailing `.global()` routes every subtask's committables to
  committer subtask 0 (the #14 post-commit-topology design was replaced in #69: records emitted
  to a post-commit topology during job shutdown are not guaranteed to be processed — verified
  empirically, the final streaming batch was lost — while committer commits ride the
  final-checkpoint wait and the framework's committer state). Jobs are submitted all at once then
  awaited. Cleanup is best-effort on success only; a staging bucket lifecycle rule is the
  documented mitigation for orphans
- **BigQuery streaming FILE_LOADS** (#69): same `WriteMethod.FILE_LOADS` value, allowed under
  explicit `STREAMING` + checkpointing (`AUTOMATIC` stays rejected); `WRITE_APPEND` only. The
  checkpoint is the trigger: each completed checkpoint's committables are committed
  synchronously (a slow load delays the next checkpoint = backpressure; async in-flight loads
  were evaluated and rejected — `commit()` must mean durable, or a crash after the next
  checkpoint strands submitted-but-unconfirmed loads). A `FileLoadsCheckpointStamper` pre-commit
  operator stamps the checkpoint id onto committables (the `Committer` SPI cannot see it); job
  ids gain a visible `-c<checkpointId>` segment (hash material unchanged); streaming overflow
  submits multiple direct append jobs instead of temp-table+copy. Quota guard at graph
  construction: interval < `minCheckpointInterval` (default 2 min) errors, < 5 min warns (1,500
  load jobs/table/day), plus a runtime cadence warning in the committer
- **Per-write-method option scoping** (decided in #14, was deferred on PR #46): write-method-only
  options live in a nested immutable options object set on the builder (`FileLoadsOptions` via
  `fileLoadsOptions(...)`); `build()` requires it for its write method and rejects it for
  others. Future write methods (#30) follow the same pattern
- **Pub/Sub**: base implementation is vendored from `GoogleCloudPlatform/pubsub`
  `flink-connector/` (decision record: issues #17 and #31); the Apache connector is only a
  design reference (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`
- **Pub/Sub sink** (#18): Publisher-based flush-on-checkpoint stateless writer; FLIP-171
  `AsyncSinkBase` evaluated and rejected (SDK `Publisher` already batches; AsyncSink persists
  buffers into writer state). Mailbox-based backpressure with an in-flight cap; writer-owned
  per-topic publishers (no JVM-wide cache); publish failures are capture-and-rethrow (the
  Apache connector's infinite republish is deliberately not adopted). Decision record in the
  module README
- Deferred decisions are recorded on PR #46: `location()` granularity (decide in #10)
