# Detailed guidance — flink-connector-gcp-cloudtasks

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink (`docs/adr/0048`, `docs/adr/0129`, `docs/adr/0134`, `docs/adr/0158`)

- **The eager, stateless writer is the default delivery mode.** Opt-in DataStream and Table
  `EXACTLY_ONCE` uses the staging writer and production committer from `docs/adr/0158`.
  Preserve immutable named envelopes, no sends before checkpoint completion, deadline checks before
  every send, strict fail-before-send expiry, and no Flink writer state. Reuse the v1
  `CloudTasksCommittableSerializer` compatibility contract when changing durable state.
  `CloudTasksStagedCreationLifecycleTest` pins operator restore/rescale and
  `CloudTasksStagedRecoveryITCase` exercises retained filesystem checkpoints and stop/savepoint recovery.
  Do not equate those fake-service tests with #1245's real-service acceptance.
  Keep the fixed-queue/failJob restrictions, v2beta3 retention readback and explicit out-of-guarantee
  expiry policies aligned with the DataStream recovery runbook.
- Use `docs/adr/0162` for the delivery order: implementation can start from ADR-0158 without a
  separate primitive performance pass; correctness and final performance acceptance still govern
  release. Keep #1241's inconclusive result distinct from that sequencing decision.
- **One `CreateTask` RPC per record**; the v2beta3 `BatchCreateTasks` was measured and declined
  (`docs/adr/0129`) — do not adopt a batch create without superseding that record.
- No rate knobs and **no queue auto-creation** — pacing lives on the queue, and an auto-created
  queue would discard the throttling that is the reason to use the service. External HTTP and
  App Engine targets are separate serializer arms. OIDC vs OAuth is the external HTTP target's
  choice, so that builder rejects setting both; App Engine uses the task `oneof`'s internal
  request arm and optional same-project service/version/instance routing.
- **Retries belong to one sink-owned loop in the eager writer or staged committer**, never gax `createTaskSettings`;
  `NOT_FOUND` keeps its separate short budget. A failed create parks with a due time; parked
  creates count against `maxInFlightTasks` and drop on close.
- Keep `CloudTasksCredentials` package-private in `sink.writer`; do not lift it to the module
  root to match its four siblings. They are `@Internal`-public only because sub-packages must
  import them (`docs/adr/0055`'s scope reading), and this module's one consumer
  (`DefaultTaskCreatorFactory`) shares its package — the
  [#1043](https://github.com/flink-gcp/flink-connector-gcp/issues/1043) review judged the
  divergence deliberate.
- **Transport sizing controls channel capacity** (`docs/adr/0134`): an explicit
  `channelPoolSize` resizes the production channel pool; unset leaves the client's default single
  channel, it is never derived from `maxInFlightTasks`, and beside `emulatorEndpoint` it is
  rejected — the emulator arm keeps its caller-owned single channel (`docs/adr/0081`). gax retry
  and batching policy stay untouched; staged creates additionally carry an absolute per-attempt deadline. `ChannelPoolSettings` is class-level `@BetaApi` in the
  pinned gax — an internal call, tier-irrelevant under `docs/adr/0141`; reread it on a BOM bump.
- Recheck the v2beta3 retention client on a BOM bump: `CloudTasksClient` and `CloudTasksSettings`
  are `@BetaApi` in the pinned 2.96.0 SDK. ADR-0158 records this dependency; losing that surface
  requires a replacement retention readback or an explicit operator-verified deployment decision,
  never silently disabling the default check.
- Task naming: unnamed in the default eager mode; staged envelopes always carry a persisted name; `taskIdExtractor(...)` on the **sink builder**, key hashed
  SHA-256, `ALREADY_EXISTS` = success; do not treat the contradictory name-release estimates as a
  precise minimum retention guarantee.
  Use Google's documented `tombstoneTtl` semantics where that configuration applies, and direct
  users to the official creation and Queue references (`docs/adr/0154`).
  Do not require individual vendor confirmation or guarantees beyond the published specification;
  validate connector behavior within the supported configuration and recovery scope.
  `httpTarget(url)` uses the existing two-stage immutable schema
  API. `appEngineTarget(relativeUri)` returns a mutable builder: `withBody(...)` binds the body
  type, optional settings stay on that builder, and `build()` produces the immutable serializer.
  External HTTP bodies go
  out only under POST/PUT/PATCH; App Engine bodies only under POST/PUT, matching their distinct
  proto contracts. Queue-level `appEngineRoutingOverride` remains authoritative over task-level
  routing.

## Lineage (`docs/adr/0160`)

- Report only the effective `FixedDestinationResolver` queue through `LineageVertexProvider`.
  Never evaluate a user resolver, serializer or task-ID extractor, or enter client/credential code
  during extraction. Unknown DataStream queues have an empty dataset list in a non-null vertex.
- Pass the catalog identity through the Table sink's copy/runtime path and use the shared Table
  adapter to retain the physical queue facet. HTTP/App Engine targets and task naming do not add
  datasets. Keep Flink 2.x graph/listener tests in `src/test/java-flink2`; direct inspection and
  serialization tests also run on 1.20.

## Failure policy and metrics (`docs/adr/0049`)

- **In the default eager mode, exactly three failures are routed**: serializer rejection, extractor throw, creation
  rejected `INVALID_ARGUMENT`. An extractor's missing key is per-stream and fatal; a serializer
  `null` is a skip (`docs/adr/0001`).
- The transient half of classification scans the whole chain; the `INVALID_ARGUMENT` half
  deliberately does **not** — the asymmetry was found by mutation testing, and the scanning
  variant is the worse code. The routing branch sits before the `asyncError` early-return.
- `numRecordsSend` counts inside `dispatch(...)` under `pending == null`; error classes classify
  every failed attempt under its outermost status, so no sum is exact retry volume: first failures
  count, while a retry selected by a nested transient status is counted under the outer status.
  No separate retries counter exists. `ALREADY_EXISTS` is `tasksDeduplicated`, never an error;
  per-queue counters are looked up per record (no per-destination state exists to cache a handle
  on).

## Table sink (`docs/adr/0107`; shared rules `docs/adr/0139`)

- A single option value a DataStream builder rejects is renamed to its option key through the
  module's `table.OptionSetters` (`docs/adr/0133`); a new mapper line goes through it, and
  cross-field checks keep the restate-in-DDL-keys judgment (`docs/adr/0007`).
- A mapped option carries no default and no description restates one (`docs/adr/0139`); the
  table-owned target selection and method options carry theirs, recorded in
  `CloudTasksConnectorOptionsTest`, whose `noDescriptionRestatesADefault` guards the
  descriptions — the reference page or the table page's row is where a default is written. "The
  default HTTP method" naming a per-row-overridable option's role, and the constraint absence
  imposes on `url`/`relative-uri` metadata, are deliberately outside the rule.
- Preserve the Table delivery/staged option mapping to the shared runtime and SQL-key diagnostics.
  `Context.isBounded()` rejects the batch-planner context, not finite STREAMING input; ADR-0158
  records this distinction. Keep the common time metrics scoped to writer ownership/current commit
  invocation and use Flink's pending count for the full collector.
- `cloud-tasks` is insert-only and maps one table to one fixed queue. `target.type` defaults to
  external HTTP and can select App Engine without translating between their protobuf request arms.
  A generic Flink format sees physical columns only; writable target metadata configures the task
  around the body.
- HTTP POST/PUT/PATCH and App Engine POST/PUT invoke the body format. Fixed request options are
  defaults and non-null row metadata overrides them; header names compare case-insensitively. With
  no fixed address, the selected target's `url` or `relative-uri` metadata must be declared `STRING
  NOT NULL` in the catalog schema. Wrong-family options and metadata are rejected.
- Selecting `task-id` installs the existing sink-builder extractor — never name a task in the table
  serializer. API credentials, OIDC/OAuth dispatch tokens and network reachability remain three
  separate concerns. App Engine rejects dispatch-token options and exposes separate service,
  version and instance metadata; queue-level routing override remains authoritative.
- The built-in `form-urlencoded` format accepts only `STRING` and `ARRAY<STRING>`, preserves
  physical schema and array order, and owns `application/x-www-form-urlencoded` for body-carrying
  methods. Fixed and metadata headers may repeat that value but may not conflict with it.
