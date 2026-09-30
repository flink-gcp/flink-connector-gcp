# Detailed guidance — flink-connector-gcp-firestore

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Module shape (`docs/adr/0170`)

- **One module, two package roots**: `connector.firestore` (Native mode, `firestore.v1`) and
  `connector.datastore` (Datastore mode, `datastore.v1`). They share nothing beyond
  `flink-connector-gcp-base`; extract a helper only when its second consumer exists. Each root
  carries its own `CrossVersionSink` under `src/main/java-flink1` / `java-flink2`.
- The design comment and implementation order on #355 settled the sub-issues #1540–#1546; read
  them before starting one. CDC and an exactly-once mode are deferred there with reopen conditions
  — do not start either without engaging those conditions.
- Public types enter at `@PublicEvolving` (ADR-0141's youth clause).

## Native-mode sink (`docs/adr/0171`)

- **The writer sends through the client library's `BulkWriter`, on an executor the writer owns,
  and never calls `BulkWriter.close()`** (it waits without a bound). Closing the access shuts the
  executor down.
- **Two library defects are worked around, and `BulkWriterDefectsITCase` pins both**: a failed
  write never releases its pending-operation slot (the writer replaces the `BulkWriter` before a
  submission could reach 500), and a synchronous refusal corrupts its request (the closed
  `FirestoreWrite` value vocabulary keeps it unreachable; a refusal that still arrives fails the
  job). When that test fails after a BOM bump, re-examine the workaround rather than the test.
- **The library sends on no timer** — at 20 writes (10 with a retry) or on `flush()`, and a
  retried write joins whatever batch is open. Every pass of a wait that finds the mailbox empty
  asks it to send (`sendOutstanding`); asking once per wait can strand a retry. Never wait on the
  library's `flush()` future; wait on the writer's own ledger.
- **Rates below 20 are refused** (the library's first batch is 20 and never passes a lower
  rate), and `maxInFlightWrites` defaults to 250 so failed writes do not force a replacement each.
- **`DocumentSizeEstimator` is the `Write`'s wire size**, not Firestore's storage formula;
  `DocumentSizeEstimatorTest` holds it equal to a proto reconstruction of the library's `Write`,
  and `DocumentSizeEstimatorITCase` to the request the library actually sends — add a new
  vocabulary type to `everyValueType()` or an operation to `everyOperation()`, which both read.
- **The library reports a request-level status against every write of the request.** That is why
  `INVALID_ARGUMENT` is parked and confirmed alone before routing, and why its `errorClass` count
  waits for the solo verdict. Measured on the emulator: `INVALID_ARGUMENT` fans out across the
  request; `ALREADY_EXISTS`, `NOT_FOUND` and `FAILED_PRECONDITION` answer only their write.
- **Routing**: `INVALID_ARGUMENT` (after solo confirmation); `ALREADY_EXISTS` for `CREATE` only,
  outside `maxConsecutiveRejections`; `FAILED_PRECONDITION` only for a write carrying
  `lastUpdateTime` and only under `preconditionFailurePolicy(ROUTE_TO_FAILURE_HANDLER)`.
  `NOT_FOUND` is never routed. Both ADR-0042 halves apply.
  `FirestoreErrorClassifierTest` iterates every gRPC code.
- **Retries are the library's, in two layers**: `BulkWriterRetryPolicy` keeps the library's
  retryable set (pinned against the `BatchWrite` call settings) and makes the write's attempt
  count configurable as `writeMaxAttempts`; that backoff has no knobs because the library exposes
  none. The transport's per-call retries are the Pub/Sub-shaped `retry*` options, mapped by
  `DefaultFirestoreDatabaseAccessFactory.retrySettings` onto the library's `BatchWrite` values.
- **Field names are literal in every operation**; `update` goes through the `FieldPath` overload.
- **Same-document order is not kept** — documented, and #1556 is the opt-in mode. Do not add a
  gate outside that issue.
- The emulator endpoint reaches the client only through the builder; the library also reads
  `FIRESTORE_EMULATOR_HOST` itself, and `FirestoreClients` warns when it is set without an
  endpoint.

## Testing

- The emulator is `gcloud emulators firestore` from the shared `google-cloud-cli` image
  (`FirestoreEmulatorContainers` in test-utils, pinned with the Bigtable and Pub/Sub classes).
  It does not enforce the 10 MiB request limit or IAM, and its statuses are not evidence about
  the service; the real-GCP suite is #1546.
