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
- Documentation site: `mise x -- hugo serve --source docs` to preview,
  `mise x -- hugo --gc --minify --source docs --panicOnWarning` for the check CI runs (a
  deprecation, a broken `relref` or a missing shortcode fails the build). `mise.toml` pins
  hugo-extended and Go; hugo-book is a Hugo module pinned in `docs/go.mod`

## Documentation (`docs/` vs module READMEs)

- `docs/content/docs/connectors/datastream/<connector>.md` is **the design record**: API notes,
  design decisions, delivery guarantees, error handling, tuning tables and the testing strategy.
  Behavior or public API changed → update the docs page, not the README
- The module `README.md` is an **overview only**: title, one-paragraph description, the
  feature-status table (`Implemented (#N)` / `Planned (#N)`), a minimal code sample, a link to the
  docs page, and the **provenance/attribution section** — provenance pairs with `NOTICE` and is a
  licensing obligation, so it stays in the repository
- Implementation status lives in the README table only; the docs page links to it instead of
  repeating it. Keep the two from drifting by adding status nowhere else
- Pages are plain markdown with front matter (`title`, `type: docs`, `weight` — spaced by 10 so a
  new connector slots in without renumbering) and the plain Apache-2.0 header as an HTML comment.
  **No Flink shortcodes and no vendored Flink layout code** — `artifact`/`tabs`/`hint` do not
  exist here, and staying clear of them is why `NOTICE` needs no entry. Hugo's own built-ins
  (`relref`, `param`) are fine; prefer `{{< param BookRepo >}}` over hardcoding the repository URL
- Syntax highlighting is class-based (`markup.highlight.noClasses = false`) with the palettes
  selected by `prefers-color-scheme` in `docs/assets/_custom.scss`, which hugo-book bundles into
  its own stylesheet. Regenerate the palettes with, from `docs/`:
  `hugo gen chromastyles --style=github > assets/_chroma-light.scss` and
  `--style=github-dark > assets/_chroma-dark.scss` (verbatim output; apache-rat excludes them)
- The site is built as a CI check only; GitHub Pages publishing waits until the repository is
  public (#6). Each module README links to its docs page by in-repo relative path — those links
  become site URLs when Pages goes live, which is a checklist item on #6

## Workflow rules

- **One git worktree per PR** under `/tmp/worktrees/flink-connector-gcp/`; never switch branches
  in the main checkout. Remove the worktree and local branch after merge
- All changes go through **draft PRs**; nothing is pushed directly to `main` after the initial
  skeleton
- **After creating a draft PR, always self-review it** — applying simplification and efficiency
  findings, not only correctness ones — and push the fixes before asking for review. Record the
  findings *and the deferrals, with their reasons* as a PR comment. Which command to use:
  - `/review <pr>` reviews a pull request and **Claude can start it itself**, so this is the one
    to reach for once the draft PR exists
  - `/code-review` reviews the working diff and is **user-invocable only** — Claude gets
    `disable-model-invocation` if it tries, so ask the user to run it rather than assuming it will
    happen
  - With neither, fall back to review subagents given *distinct* lenses (correctness and
    concurrency, public API and simplification, test quality and flakiness). One agent asked for
    "a review" returns much less than three asked for different things — and verify each finding
    against the code before acting on it
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
  map stamps the checkpoint id onto committables (the `Committer` SPI cannot see it); job ids
  gain a visible `-c<checkpointId>` segment (hash material unchanged) and derive their Flink-job
  segment from the committable's originating job id (stamped by the writer) so re-commits after
  a new-JobID restore still re-attach; streaming overflow submits direct append jobs
  sequentially instead of temp-table+copy. Streaming also requires EXACTLY_ONCE checkpointing
  and checkpoints-after-tasks-finish (the final batch rides the post-finish checkpoint). Quota
  guard at graph construction: interval < `minCheckpointInterval` (default 2 min) errors, < 5
  min warns (1,500 load jobs/table/day), plus a runtime cadence warning in the committer
- **BigQuery STORAGE_API_EXACTLY_ONCE** (#30): buffered streams + 2PC on checkpoints. **One
  stream per writer subtask, reused across checkpoints and tracked in Flink writer state**
  (Dataproc-connector style; stream-per-checkpoint explicitly rejected — GCP support told the
  user frequent CreateWriteStream churn is not intended usage). Committable = (streamName,
  inclusive flushOffset, subtaskId); committer calls `FlushRows` synchronously, `ALREADY_EXISTS`
  = already flushed = success, everything else throws (restart + idempotent re-commit; no
  deterministic-id machinery, no checkpoint stamper, no `.global()` — committer runs at sink
  parallelism, the pre-commit topology is identity and exists only as the validation hook).
  Restore: synchronous probe append at the restored offset; offset conflicts / dead stream /
  reopen failure abandon the stream for a fresh one at offset 0 (rows past the restored offset
  were never committable, so they stay invisible). **Streams are never finalized anywhere** —
  real BigQuery rejects `FlushRows` on a finalized stream (verified; the batch IT caught it),
  so finalizing races restored-but-uncommitted commits; open streams' unflushed tails are
  invisible and cost nothing. Server-side row-level errors route to `FailedRowHandler` with
  offset-recompute recovery (atomic request rejection → route failing rows, replay survivors +
  trailing batches; `ALREADY_EXISTS` during an offset-shifting replay is terminal). v1 scope:
  fixed destination only (builder rejects `destinationResolver`), no mid-stream schema
  evolution (stream schema pinned at creation), BATCH supported (commit at end of input),
  streaming requires EXACTLY_ONCE + checkpoints-after-tasks-finish; retry knobs are
  builder-configurable via `BufferedStreamOptions` with defaults. The goccy emulator keeps no
  flush cursor (re-flush duplicates), so exactly-once ITs run against real GCP; the emulator
  gets a single-flush smoke test only
- **Per-write-method option scoping** (decided in #14, was deferred on PR #46): write-method-only
  options live in a nested immutable options object set on the builder (`FileLoadsOptions` via
  `fileLoadsOptions(...)`, `BufferedStreamOptions` via `bufferedStreamOptions(...)`); `build()`
  requires it for its write method and rejects it for others
- **Pub/Sub**: base implementation is vendored from `GoogleCloudPlatform/pubsub`
  `flink-connector/` (decision record: issues #17 and #31); the Apache connector is only a
  design reference (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`
- **Pub/Sub sink** (#18): Publisher-based flush-on-checkpoint stateless writer; FLIP-171
  `AsyncSinkBase` evaluated and rejected (SDK `Publisher` already batches; AsyncSink persists
  buffers into writer state). Mailbox-based backpressure with an in-flight cap; writer-owned
  per-topic publishers (no JVM-wide cache); publish failures are capture-and-rethrow (the
  Apache connector's infinite republish is deliberately not adopted). Topic auto-creation (#19)
  is reactive — NOT_FOUND publishes are parked and republished after creating the topic via the
  `TopicAdmin` SPI (`sink.topics`, ALREADY_EXISTS = success), gated by `CreateDisposition`.
  Tuning (#20) lives in one `PubSubPublisherOptions` object (nested-options pattern; plain
  serializable values, no gax types on the public API; unset = SDK/sink default): batching,
  flow control (Block-only; the builder rejects combining with ordering — SDK 1.152.0 leaks
  permits on paused keys), publish retries, `enableMessageOrdering`, the in-flight cap and the
  recovery backoff.
  Ordering×repair: cascade cancellations park behind the NOT_FOUND root (mailbox FIFO preserves
  per-key order) and every repair attempt calls `resumePublish` before republishing. Emulator
  support (#21) is a builder option `emulatorEndpoint(host:port)` — plaintext + no credentials
  for publishers (each owning its channel) and the auto-creation admin, mirroring the Apache
  connector's `withHostAndPortForEmulator`; the emulator ITs (including a MiniCluster streaming
  test through the public builder) reuse the production factory/admin, no test-only factory.
  Per-record failure policy and the fatal-exception classifier moved to #37. Decision record in
  the connector documentation page
- **Pub/Sub source** (#79, #80): FLIP-27 streaming-pull source; split = (subscription, uid), ack on
  checkpoint completion, nack on close. Tuning lives in one `PubSubSubscriberOptions` object
  (nested-options pattern, same shape as `PubSubPublisherOptions`). Two decisions deviate from the
  #80 issue text and must not be silently re-litigated:
  (a) the **subscriber shutdown mode is not exposed** — `NACK_IMMEDIATELY` is fixed because
  `WAIT_FOR_PROCESSING` waits for acknowledgements that only arrive at checkpoint completion, which
  never happens during close; only `shutdownTimeout` is a knob (an SDK enum on the public API would
  also break the #47 SQL mapping);
  (b) the "**fail when running without checkpointing**" guard **cannot read the configuration** —
  `SourceReaderContext.getConfiguration()` is the TaskManager configuration
  (`SourceOperatorFactory` passes `getTaskManagerInfo().getConfiguration()`), while
  `env.enableCheckpointing(...)` writes into the job configuration, so absence proves nothing and
  failing on it would break jobs that enable checkpointing programmatically while passing every
  MiniCluster test. Replaced by `MissingCheckpointDetector` (no checkpoint taken + messages
  outstanding + budget spent → fail), **evaluated from `PubSubSplitReader.fetch()`, not the record
  path** — once flow control fills, the client stops delivering and `pollNext` is never called
  again, so a record-driven check would go silent in exactly the state it exists to catch; the
  detector bounds the fetch park only while armed, so a healthy reader parks indefinitely as
  before. The config-derived ack-extension check is a best-effort warning only.
  `parallelPullCount > 1` is rejected with `orderingMode(PER_KEY)` rather than silently forced to 1
  (the factory still force-sets 1 so the guarantee does not rest on the SDK default). The `NACK` deserialization-failure policy is deferred to #81, where
  the `GetSubscription` preflight can verify a dead-letter policy exists
- **Cloud Tasks sink** (#23, design settled; implemented in #24): Cloud Tasks is an HTTP dispatch
  queue whose **pacing lives on the queue** (`maxDispatchesPerSecond`, `maxConcurrentDispatches`,
  retry config), so the sink has no rate knobs and there is **no queue auto-creation** — an
  auto-created queue would carry default limits, discarding the throttling that is the reason to
  use the service, and a deleted queue name cannot be reused for 3 days. HTTP targets only (App
  Engine targets are region-locked, invert 429/503 overload behaviour and are "less common");
  targets need an external IP; OIDC vs OAuth is chosen by the target, not by preference, so the
  builder rejects setting both. Fixed **and** per-record queue destinations from v1 — unlike
  Pub/Sub topics and BigQuery tables this costs nothing, since one `CloudTasksClient` serves every
  queue with no per-destination stream. **Unnamed tasks by default**; `withTaskId(...)` opts into
  deduplication (`ALREADY_EXISTS` = success, window ≤24 h) and the sink **hashes the extracted key
  with SHA-256**, because Google documents that sequential ids raise latency *and* error rates —
  the footgun is removed by construction rather than by a warning. **Retries are the sink's
  responsibility**: the generated client gives `CreateTask` an empty retryable-code set and a 20 s
  timeout (verified in `CloudTasksStubSettings` 2.94.0) precisely because an unnamed create is not
  idempotent; the sink retries `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED` and treats
  `NOT_FOUND` as briefly transient (a 30-day-idle queue re-activates slowly). No batch API in GA v2
  and no client-side batching in the SDK, so one RPC per record with a mailbox-based in-flight cap.
  At-least-once, stateless writer, flush on checkpoint. Decision record in the connector
  documentation page
- Deferred decisions are recorded on PR #46: `location()` granularity (decide in #10)
