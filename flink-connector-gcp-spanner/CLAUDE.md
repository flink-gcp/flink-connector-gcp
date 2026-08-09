# CLAUDE.md — flink-connector-gcp-spanner

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0075`)

- **The destination is a database, not a table.** The serializer's `Mutation` names its own table,
  so one sink writes to as many tables as it produces. That is why `SpannerDatabase` is at the
  module root, why the cell weights cover the whole database, and why there are no
  per-destination metrics — the cardinality would be the serializer's to choose.
- **One mutation per `MutationGroup`.** The group is the unit `BatchWriteResponse` reports a
  status for; anything else gives up per-record failure routing, which is the whole reason this
  sink uses `batchWriteAtLeastOnce` rather than a commit. Measured, not assumed: every rejection
  shape `SpannerRejectionITCase` provokes comes back per group.
- **The writer is synchronous — no mailbox, no in-flight bookkeeping.** This RPC has no
  asynchronous or self-batching form, so there is nothing to wrap. Do not port the Bigtable or
  Pub/Sub writer's machinery here on the assumption that it was forgotten.
- **The client library retries this RPC not at all** (`SpannerStubSettings.batchWriteSettings` is
  `no_retry_0_codes`; `runWithSessionRetry` recovers only a lost session). The retry loop and its
  knobs are the connector's, the Cloud Tasks shape rather than the Bigtable one. Recheck on a
  client upgrade.
- **Batch write has no replay protection**, so idempotence is the serializer's to supply; it is
  documented on the SPI and on the sink, never enforced.
- `SpannerDatabaseAccess` holds its three operations as functional values over the `Spanner`
  service handle, for the reason `docs/adr/0047` gives for the Bigtable batcher adapter. A test
  faking `DatabaseClient` itself is the thing this exists to avoid.
- `null` from the serializer is skip, and the check sits immediately after the serializer's
  `catch`, ahead of the buffer, the weights and every metric that counts a send (`docs/adr/0001`).

## Error handling (`docs/adr/0076`)

- **Routed: `INVALID_ARGUMENT` and `ALREADY_EXISTS`, per group only.** Retried: `ABORTED`,
  `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`. Everything else fails the job.
- **A constraint violation is `constraintViolationPolicy`'s decision, defaulting to `FAIL_JOB`.**
  It covers **both** `FAILED_PRECONDITION` (NOT NULL, over-long value, foreign key) and
  `OUT_OF_RANGE` (`CHECK`) — measured; a policy covering one of them is a bug, not a subset.
  The default is failing for two reasons, and the first is the load-bearing one: a constraint
  violation usually means the record-to-column mapping is wrong rather than that one record is
  anomalous, so failing is right *even under a dropping handler*; and `FAILED_PRECONDITION` is
  also what every write gets while a CMEK key is unreachable, which self-heals.
- **Never split a status by its message text** to separate those cases. Spanner's own error-codes
  page says the message may change at any time.
- **Do not add a three-valued fail/DLQ/drop option.** `failedMutationHandler` already is that
  choice; two policies over one decision can contradict each other (ADR-0076).
- **A failure of the request is never routed**, whatever its status: it names no mutation, and the
  mutations it carried have no reported outcome.
- Both code mappings (`ErrorCode` → gax, canonical number → gax) are written-out switches, and
  `SpannerErrorClassifierTest` iterates every value of both. A code added to the client library
  must fail a test rather than silently become unclassifiable.

## Batching and cell weights (`docs/adr/0077`)

- **Bounding the request the writer builds is correctness, not tuning** — but read the quotas page,
  not only the batch-write how-to, before saying which limit a knob defends. 80,000 mutations and
  100 MiB are `Commit`'s; the only row naming batch write is **per mutation group** (80,000), and
  the batch-write page's one sentence is about **size**. So no per-request mutation count is
  documented for this RPC at all: `maxBatchBytes` defends a documented request-level limit,
  `maxBatchCells` and `maxBatchMutations` are proxies for size. Beam's defaults are commit-shaped,
  which is how the per-request framing got in.
- **All three batch knobs are bounded at the setter** (#435): 80,000 / 80,000 / 100 MiB,
  package-private `*_LIMIT` constants with the figure named in the `@param` (the `OptionChecks`
  rule — a public compile-time constant inlines into callers, and #441 may lower the byte one).
  The byte ceiling is the **looser** of two readings, so it rejects only what is illegal under
  both, and it is the one that defends a refusal Spanner documents; the **cell ceiling is
  precautionary** — no request-level mutation count is documented either way.
- **`MAX_BATCH_MUTATIONS_LIMIT` is *derived* — `= MAX_BATCH_CELLS_LIMIT`, never a second literal.**
  Every mutation costs at least one cell, so a batch never holds more mutations than cells; that
  derivation is *why* the number is the same, and writing 80,000 twice would leave the mutation
  ceiling cutting below what a batch may hold the moment the cell ceiling moved.
  `SpannerWriterOptionsTest` pins the equality rather than the value.
- **The three limits are ANDed**, so a batch flushes on whichever binds first and raising one alone
  usually changes nothing. Any claim of the form "setting X large breaks the job" has to name the
  knob that *binds*, or it is false — round two of #435 caught exactly that claim, inherited from
  the issue. The one statically detectable case is `maxBatchMutations` above the *configured*
  `maxBatchCells`, which can never take effect; `build()` **warns** rather than rejecting, because
  the configuration works and refusing it would reject something harmless. That is the only log
  statement in an options class in this repository, and the reason is that nothing else — no
  exception, no changed value — could tell the user.
- **A single mutation that breaches a limit on its own is still never rejected** — the check runs
  before a mutation joins the batch, so Spanner's own refusal names the real limit. A range delete
  over an indexed table costs one for the table **plus one per index per row it matches**, which is
  the one way this sink can reach the per-group 80,000 at all; with no secondary index it costs one
  however many rows it hits, and the sink's single-row estimate is then exact.
- `maxBatchCells` counts index entries, read once from `INFORMATION_SCHEMA` when the writer opens
  — dialect-branched, primary-key index excluded by name, names folded to lower case. Reading it
  at creation is what makes an unreadable schema a job that never starts.
- **A table the weights do not know is counted without index entries, never rejected.** The
  default's 16-fold headroom under the 80,000 ceiling is what absorbs that, so raising
  `maxBatchCells` toward it is a real trade and the docs say so.
- **The client library exposes no public route from a `Mutation` to its wire form**, and
  `Mutation.toString()` truncates strings at 36 characters. Hence: the byte limit is an estimate,
  and the dead-letter payload is the Java-serialized `Mutation`. Recheck both on a client upgrade
  — a public conversion appearing upstream would reopen the payload encoding.

## Testing

- Emulator ITs pin `gcr.io/cloud-spanner-emulator/emulator`, **not** the `google-cloud-cli` bundle
  the other connectors use: its Spanner emulator predates `BatchWrite` (added in v1.5.31), so the
  whole write path would answer `UNIMPLEMENTED`.
- Identifiers in the ITs are lower case and unquoted, which is what lets one set of mutations and
  one query serve both dialects.
- The production `createWriter(WriterInitContext)` is covered by the emulator ITs rather than by a
  closed-port unit test: this sink reads the schema while creating the writer, so a closed port
  costs the client's whole retry budget (27 s, measured 2026-08-09) to prove less.
- An emulator is never the authority. Real-GCP confirmation of the rejection table is `#224`.
