# CLAUDE.md — flink-connector-gcp-bigtable

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **Implemented, not adopted or vendored** (#33, design settled 2026-08-02; the issue holds the
  full comparison): `com.google.flink.connector.gcp:flink-connector-gcp-bigtable` is a Flink
  2.1-only artifact with `GoogleCredentials` on its public API and no failure-handler SPI, and its
  upstream is dormant — so depending on it was rejected, and vendoring buys nothing once package
  normalization, AutoValue→builder conversion, options objects, `CrossVersionSink` and
  `base.failure` wiring have changed essentially every line. What *is* adopted is its
  `BaseRowMutationSerializer`'s **shape** — `@Nullable RowMutationEntry serialize(element,
  context)` — so its users port by changing the interface name. Null = skip is no longer an
  adoption but the repository's own contract, decided in #230 and now carried by all four SPIs;
  the root `CLAUDE.md` has the reasoning and the three implementation rules. Its built-in
  `GenericRecord`/`RowData` serializers are deliberately not ported: `RowData` belongs to #217, and
  an Avro convenience is additive whenever a use case appears.
- **Four SDK facts this module is built on**, each checked against `google-cloud-bigtable` sources
  rather than assumed:
  - `newBulkMutationBatcher(TargetId)` is `@BetaApi` while the `String` overload is `@Deprecated`,
    so the `TableId.of(...)` form is taken and the beta annotation accepted — there is no
    non-beta, non-deprecated way to get a batcher.
  - gax's `Batcher` is `@InternalExtensionOnly`, so a test fake must not implement it. That is why
    `MutationBatcher` exists as this module's own narrow SPI, wrapping the client batcher exactly
    as `TopicPublisher` (Pub/Sub) and `TaskCreator` (Cloud Tasks) wrap theirs. It is also why
    `sendOutstanding()` is called rather than `Batcher.flush()`: the blocking one would stall the
    task thread while the completion mails the writer's state is mutated by pile up behind it.
  - The client's bulk-mutation path has a **flow controller of its own** — 1000 entries per
    channel, 100 MB, `LimitExceededBehavior.Block` — whose static limits its public API does not
    expose (only `enableBatchMutationLatencyBasedThrottling` / `disableBatchMutationLatencyBasedThrottling`
    and the `@InternalApi` server-initiated switch). So `Batcher.add()` *can* block the task
    thread, and keeping the writer's own bounds below the client's is the only available way to
    preserve the #85 property that a full writer yields to the mailbox rather than blocking. The
    defaults (1000 / 64 MiB) do; that is why the reference page documents raising
    `maxInFlightMutations` as *moving* the bound rather than raising it, and why exposing the
    client's flow-control knobs is not the fix (it is the #85 defect class itself).
  - `RowMutationEntry.toProto()` is `@InternalApi`, and it is the only route to both the byte size
    the in-flight bound counts and the `FailedElement` payload — the entry exposes neither its key
    nor its mutations. Accepted deliberately: nothing mechanical flags it, since
    `check-flink-api-tiers` audits `org.apache.flink` imports only. If it ever disappears, the byte
    bound and `FailedMutation.getPayloadBytes()`/`getRowKey()` are the call sites to revisit.
- **`INVALID_ARGUMENT` alone is routed, and `FAILED_PRECONDITION` deliberately is not** — reversing
  #33's design comment, which listed both. The rule is the repository's, settled on #207 the same
  day: only a status an authority defines as *state-independent* may reach a handler that may drop
  it. gRPC defines `INVALID_ARGUMENT` as "problematic regardless of the state of the system" and
  AIP-194 lists it must-not-retry, while `FAILED_PRECONDITION` and `OUT_OF_RANGE` are explicitly
  state-dependent — so a mutation rejected with one of those might be accepted later, and dropping
  it is data loss. Cite the definition rather than the plausibility of the failures a code names.
  Everything else — `NOT_FOUND` (a missing table *or column family*), `PERMISSION_DENIED`,
  `UNAUTHENTICATED`, and anything the client's own retries gave up on — is fatal.
- **Routing takes both halves of a condition, and they read the cause chain differently.** No
  transient status *anywhere* in the chain (so an unstable service cannot produce a dead letter even
  behind a data-shaped status — a property of this code, not of the client surfacing one status per
  failure), **and** the chain's *first* classifiable status is `INVALID_ARGUMENT` (so an
  `INVALID_ARGUMENT` buried under an `INTERNAL` describes the inner call and does not discard a
  record over a server-side failure). The two mistakes are mirror images; both are pinned by test.
  `BigtableErrorClassifier.firstMatching(throwable, codes)` is the shared primitive, the same shape
  `CloudTasksWriter` uses.
- **Retries stay in the client, so this module has no `RetrySchedule` and no retry knobs.**
  `MutateRows` ships a non-empty retryable-code set and retries per entry
  (`EnhancedBigtableStubSettings`: `DEADLINE_EXCEEDED`, `UNAVAILABLE`, 10 ms doubling to 1 min, 10
  min total). That is the exact opposite of Cloud Tasks, whose generated client retries
  `CreateTask` on nothing and therefore owns a sink-side loop; the difference is in the clients, so
  neither is a precedent for the other.
- **One fixed table per sink, and no auto-creation.** A batcher is bound to one table, so per-record
  destinations would mean a batcher pool, a share of the in-flight budget each and an eviction
  policy — deferred until there is demand, and recorded on #33. Auto-creation is a poorer fit here
  than in the Pub/Sub sink: a table's schema *is* its column families and their garbage-collection
  policies, which a sink cannot guess.
- **`TableDestination` sits at the module root, not under `sink`.** The root file's layout rule puts
  destination types in `sink`; this deviates because #216's source facade takes the same value, and
  moving it later would churn every importer. `appProfileId` is deliberately *not* part of it: a
  profile selects client routing, not a data address, so it is a builder option. The sink has no
  family layer (#119: one write family, `MutateRows`, with no sibling in prospect —
  `checkAndMutateRow` and `readModifyWriteRow` are request-response primitives, not batched write
  paths), so `BigtableMutateRowsSink` sits beside its facade as the single-family modules' rule
  allows, and `FailedMutation` sits at the `sink` root as the post-#213 placement rule prescribes.
- **The metrics are the #37 series' standard, reached late** (#237, which also absorbed #234 —
  its "counter for *dropped* elements" is `numRecordsSendErrors`, the series having settled on one
  counter for everything routed rather than a second one for what a handler then discarded).
  #33 shipped `numRecordsSend`/`numBytesSend` ahead of the series because it was scoped to the
  three connectors that existed when it was designed; the rest arrived here. Bytes come from
  `entry.toProto().getSerializedSize()`, counted at admission (the Kafka connector's placement).
  Five decisions:
  - **No per-destination counters, and so no `perDestinationMetrics` option.** One fixed table per
    sink means `destination.TABLE.*` restates the writer's totals — the same reason #210 leaves
    them off the buffered-stream path, under its rule that no writer registers a metric it can
    never increment. A batcher pool (see the one-table decision above) is what would change this.
  - **`errorClass` counts RPC failures only, never the serializer's.** A serialization failure
    carries no status, so counting it would put every one under `UNCLASSIFIED` beside the RPC
    failures that genuinely carry none. Both sibling sinks draw the line in the same place: one
    `errorClass` call site against three routing ones.
  - **`BigtableErrorClassifier.statusCode` reports the chain's outermost classifiable status, not
    the code `classify` acted on**, mirroring `PubSubErrorClassifier.statusCode`. The two diverge
    exactly when a transient status is buried under a data-shaped one: routing scans the whole
    chain and calls it fatal, while the counter names what the failure was reported with. That is a
    real divergence rather than a theoretical one, and it is pinned by
    `namesTheOutermostStatusOfAChainItTreatsAsFatalForABuriedTransientOne` — added because the
    mutant reporting the routing decision instead **survived** the first test set.
  - **`close()` zeroes the two gauge-backing counters, and does it *before* `Closers.closeAll`.** A
    reporter can sample between `close()` and the metric group's own close, and nothing decrements
    them afterwards (the completions that would run as mailbox mails no longer run), so a writer
    torn down mid-flight would keep reporting mutations it will never wait for — the same
    lifecycle gap #210 found in the BigQuery writers and #208 had already closed in
    `PubSubWriter`. The *placement* is this module's own: either close below can still throw — the
    client's shutdown, an `InterruptedException` from the batcher's wait, the handler's own close —
    and a mid-flight teardown is when both this clear and those failures happen, so a clear after
    `Closers.closeAll` would be skipped in exactly the case it exists for. The throw that first
    argued this was the batcher's own shutdown report; absorbing that one (#238) left the
    placement where it was, on the throws that remain. Found in review round 2, having been missed
    by a round 1 that looked only at increment sites — **when a series brings the same shape to
    another connector, diff the `close()` paths too.**
  - **Every failure reaching the writer is counted, fatal ones included and fatal ones after the
    first.** Only the first becomes `asyncError`, but each is a mutation the client gave up on.
    The consequence, stated on the docs page rather than left to a reader to infer: since the
    retries are the client's, the sum over the transient codes is *not* this connector's retry
    volume, which is exactly what that sum means on the Cloud Tasks page. The client's own attempts
    are invisible here. **The one exclusion is a batched row-level rejection** (#239): the client
    reports one request-level status against every co-batched entry, so counting them all would
    multiply a single incident by the batch size, in `numRecordsSendErrors` and
    `errorClass.INVALID_ARGUMENT.errors` alike. The isolation pass counts what it confirms, which is
    what makes both counters report rejected *records*. That is the Pub/Sub cascade-exclusion
    argument arriving here by the same route it did there, and `parkedMutations` is the gauge that
    took the excluded reports' place: a parked mutation has left the in-flight counters and has not
    reached the handler, so between the two nothing else reports it at all.
- **The E2E suite creates an ephemeral instance per gated *class*, not per run** (#218) — the one
  deviation from that issue's settled design. When this landed it was forced: `reuseForks=false`
  meant a fresh JVM per class, where a JVM-scoped holder buys nothing. #243's root-pom override
  (`reuseForks=true`) changed the calculus — the two forks' classes run sequentially in
  long-lived JVMs, so a per-fork holder became possible — and per-class was kept anyway: a shared
  holder would still be raced by the two forks, a single class must stay runnable by hand, and
  best-effort deletion tracks per class. Nothing persistent exists to run against because a one-node instance stands at roughly
  $470/month, so `opentofu/flink-gcp` carries only the two API enablements and `roles/bigtable.admin`
  — admin because *instance* lifecycle is administrator-level, not because the data path needs it.
  Leak control is a name-encoded creation time (`flink-it-<epochSeconds>-<runId>`, 28 characters
  inside Bigtable's 33) plus a sweep of anything older than two hours at the start of each class;
  the threshold sits far above the workflow's 40-minute ceiling, so the sweep cannot reach a live
  run. The cluster id is built from the run id rather than the instance id, which at 28 characters
  leaves no room under a cluster id's own 30-character limit. Measured 2026-08-02: the two classes
  together, instance provisioning included, take about 7½ minutes.
  **`BIGTABLE_IT_PROJECT` in a shell used to make every `just verify` create instances**, because
  the gate is on the classes and `verify` runs the same `integration-tests` execution `just e2e`
  does — ~7½ minutes and a node-hour fraction on every full build the variable was visible to. The
  BigQuery and Pub/Sub gates had that shape all along and were merely cheap; being the first one
  billed per run is what forced #245, which closed it: every gated class also carries
  `@Tag("gated")`, which surefire excludes by default, so the suite is opt-in per *command*
  (`just e2e`, or `-Dtest.excluded.groups=` by hand) rather than per shell. The variable is still
  required to run the suite — the environment gate is unchanged — but setting it no longer costs
  anything by itself.
- **What real Bigtable answers, measured 2026-08-02** (client 2.80.0), which is what the connector
  page's error-handling table now states rather than infers. Routed (`INVALID_ARGUMENT`): a cell
  timestamp that is not a multiple of 1000 ("Timestamp granularity mismatch"), and an empty row key
  ("Row keys must be non-empty"). Fatal (`NOT_FOUND`): a mutation naming a column family the table
  lacks — and the service reports it for **every** entry of the batch, the good ones included.
  **Two conditions #218's text expected to measure are unmeasurable through this connector**:
  `Mutation` enforces its own limits in the private `addMutation` every mutation-adding method funnels
  through — so `deleteCells` and `deleteRow` are covered as much as `setCell`, and "more mutations
  than a row accepts" and an oversized entry are thrown client-side and arrive as *serialization*
  failures with no entry and no row key; the service never sees them. The mutation-count half was run
  (110,000 mutations, which never reached the wire); the byte half is `MAX_BYTE_SIZE` = 200 MiB read
  from the client's class file beside `MAX_MUTATIONS` = 100,000, not exercised. A
  single-cell size violation is unreachable for a second reason too: the client's bulk flow
  controller caps accumulated size at 100 MB, below Bigtable's 256 MB per row.
- **A `ROW_LEVEL` verdict is confirmed solo before it is routed** (#239, the other defect the #218
  run turned up; #238 is the one settled in the next bullet). Bigtable rejects the whole
  `MutateRows` request rather than the entry that provoked it, gax fans that one status out over
  every entry future, and the sink routed each — so under `logAndDrop` one malformed record silently
  discarded its whole batch while the job stayed green. The writer now **parks** a row-level verdict
  answering a batched submission and `runIsolationPass()` re-submits each parked mutation as the only
  entry of its own request: a solo success was collateral damage and is now applied, a solo rejection
  is what reaches the handler. The pass runs from `flush()` *and* from `write()`, which is what bounds
  the park to one batch rather than to a checkpoint interval, and it terminates because every
  submission inside it is solo.
  - **The discriminator is our own submission, not the exception**, and that is the decision not to
    re-argue. gax's `BatcherImpl.sendOutstanding()` swaps the open batch, so an entry added to an
    emptied accumulator and flushed at once travels alone — a property of code in this repository.
    The alternative was measured and declined: `MutateRowsBatchingDescriptor.splitException` unwraps
    the `MutateRowsException` and sets one `ApiException` per entry future, and the two shapes *do*
    differ — `MutateRowsAttemptCallable.createEntryError` gives a per-entry rejection an
    `io.grpc.StatusRuntimeException` cause while `createSyntheticErrorForRpcFailure` gives the
    request-level one the original `ApiException` as its cause, so `cause instanceof ApiException`
    discriminates at bigtable 2.80.0 / gax 2.82.0 (read 2026-08-07). Nothing documents it, nothing
    would flag a change, and a silent change re-opens a P0 in the unsafe direction. So **every**
    non-solo row-level verdict is isolated, at one extra request per genuinely bad record. What that
    optimisation would buy is **unmeasured**, and stated as such rather than guessed: of the two
    `INVALID_ARGUMENT` conditions this suite exercises, only the timestamp mismatch has ever been
    run with a second entry in the request, so only it is *known* to be request-level — the empty
    row key was measured on a single-entry request, which cannot tell the two apart. A sample of
    one, and the round-2 review of #239's own pull request is what caught the claim that it was two.
  - **Fail-on-batched-rejection was declined** — it defeats the dropping policy the user opted into
    and turns a poison record into a restart loop — as was **client-side limit validation**, which
    covers only the limits we encode. Both were settled on Pub/Sub #264, whose solo-verdict isolation
    republish this adopts wholesale; what Bigtable does *not* need is the half of #264 that exists
    for ordering keys and topic creation, so there is no `DestinationState`, no recovery budget and
    no resume between publishes.
  - **The cost is real and belongs in the documentation, not only here**: while isolating, the sink
    spends roughly one request per record. `parkedMutations` is what reports it. Measured on PR
    #360, and narrower than it first reads: under the default `failJob()` the pass issues **one**
    solo request before the handler's throw becomes `asyncError` and the pass's own drain rethrows
    it (`[[row-1, row-2, row-3], [row-1]]` for three rejected records), so the unbounded case is
    *only* a dropping policy — where nothing ends the pass, because nothing is meant to. Bounding it
    with a configurable threshold, defaulting to a value that fails a stream whose data is broken
    rather than anomalous, is #361; the same shape probably exists in Pub/Sub's #264 pass, which
    that issue is scoped to confirm.
  - Pinned offline by `BigtableWriterTest` through a `FakeMutationBatcher` that decides outcomes
    **per request** — a request carrying a rejected row key fails every entry of that request — so
    the pass's behaviour emerges from the fake rather than being scripted, and against the service by
    `BigtableRejectionRealGcpITCase.routesOnlyTheRejectedEntryAndAppliesTheRestOfItsBatch`, which
    asserts the *outcome* rather than the rejection's granularity: the service answers per entry for
    some conditions (the missing column family below), and the sink must behave the same either way.
- **The batcher's shutdown report is absorbed, not thrown** (#238). gax's `BatcherImpl.close()` ends
  with `batcherStats.asException()`, an accumulator of every entry failure of the batcher's
  *lifetime* that consuming an entry's future does not clear — so a `logAndDrop` job that dropped
  one mutation still failed at task close, and the dropping policies did not survive the end of a
  job. `DefaultMutationBatcherFactory.shutDownAbsorbingTheLifetimeFailureReport` catches that one
  type and logs it at WARN; `InterruptedException` and gax's own `IllegalStateException("unexpected
  error closing the batcher")` still propagate, and `MutationBatcher.close()`'s contract carries the
  rule the writer relies on. **Both alternatives the issue floated were eliminated by measurement,
  not judgement** (Flink 2.2.1 and gax 2.82.0 sources, 2026-08-06): narrowing on gax's side is not
  reachable — `BatcherStats` is package-private with no reset and no accessor, and `close(Duration)`
  rebuilds the exception as `new BatchingException(cause.getMessage())`, discarding the cause chain
  — and draining the writer's own in-flight set first would **hang the task**, since
  `StreamTask.afterInvoke()` calls `prepareClose()` before `closeAllOperators()` and a quiesced
  mailbox rejects `put` while `take` still blocks, so `drainInFlight()` would park forever on a mail
  nothing can enqueue. A third option, not in the issue — swallow only when nothing is in flight,
  which would make the report provably a duplicate — fails because `drainInFlight()` short-circuits
  on `asyncError`, leaving that count non-zero after exactly the failures this is about. That same
  quiescing is what makes the log line worth writing rather than a formality: a failure of a batch
  the shutdown itself sent reaches neither the handler nor `asyncError`, so the absorbed report is
  its only record. Pinned twice — `DefaultMutationBatcherFactoryTest` over the seam, with the
  exception built reflectively because gax keeps its constructor package-private, and both failure
  ITCases closing their writers plainly. Only a `finally` whose case actually provoked a rejection
  asserts anything, which is every gated case but two of the three emulator ones: the emulator
  *accepts* an empty row key, so that batcher accumulates nothing. **The log line itself is covered
  since #323**, which built `LogCapture` in test-utils and used this very call site as one of its
  two motivating cases: `absorbsTheBatchersReportOfItsAccumulatedEntryFailures` now asserts the
  event carries the destination and the report as its throwable. (This bullet said the opposite
  until #324 rebased over it — #323 pinned the line and left the claim standing.) **#325 then
  measured whether the SPI contract above is a property of the pattern or of gax**, across all nine
  client-wrapping SPIs in this repository, and the answer is neither purely: a *second* connector
  has the shape — the Pub/Sub source's subscriber, through Guava's `Service.awaitTerminated`
  rethrowing `failureCause()` — by a mechanism unrelated to `BatcherStats`. So the rule is stated
  once in the root `CLAUDE.md`, with what was measured *not* to have it, and nothing here changed:
  the absorb is per-connector because the two mechanisms share no type to catch, and the third
  implementation the contract is written for still does not exist. What the measurement does say
  about this module is that the duplicate here is the **severe** one of the two — it lands after
  the `FailureHandler` may have deliberately dropped those entry failures, so it fails a job the
  policy kept running, where Pub/Sub's only competes with a failure that has already failed the job.
- **`BigtableBatcherAdapter` holds its batcher as three functional values and its client as an
  `AutoCloseable`** (#324), which is the only seam a test can drive — and it is **two** vendor
  constraints rather than one, which is why both halves are injected. The batcher's is the
  annotation: `Batcher` is `@InternalExtensionOnly`, the same fact that gives this module its own
  `MutationBatcher` SPI one layer up, so a fake would be legal Java and an unsupported extension.
  The client's is plain unextendability — `BigtableDataClient` reports no closed state and its only
  constructor is package-private (`create(...)` is `public static`, which is what the offline tests
  use). **Note the base module's `BoundedShutdown` is the same shape, not a different one**: its
  javadoc and the Pub/Sub `CLAUDE.md` both *said* `Publisher` is `final`, which it is not — it is a
  non-final class with a private constructor, unextendable by exactly the mechanism above. Both were
  corrected by #324 and two more sites by #325, so read that sentence as the record of a mistake
  rather than as a live one. A
  **production constructor takes the batcher and delegates**, so the three method references binding
  one adapter to one batcher sit inside a class a test can construct rather than at the `create()`
  call site, where nothing would reach them — #321's "a seam whose wiring no test covered", avoided
  rather than repeated. `create(BigtableDataClient)` exists for the other half of that wiring: an
  adapter handed any other closeable passes every test that injects its own client while leaking a
  channel pool per writer, and the only observable is a *consequence* of the client's close
  (`BigtableClientContext.close()` shuts down the background executor gax schedules a batcher's
  delay-threshold push on, so a later `newBulkMutationBatcher` is rejected — measured, SDK internals,
  reread it on a client upgrade). The *smaller* shape #324 offered, a static seam beside
  `shutDownAbsorbingTheLifetimeFailureReport`, was measured and rejected: a test calling the seam
  cannot reach `close()` or `sendOutstanding()`, so it kills one of the three live mutants.
  **What hid the `sendOutstanding` gap is worth keeping**: gax pushes a batch on its own
  delay-threshold timer, 1 s for bulk mutations (set in `ClientOperationSettings`, documented on
  `EnhancedBigtableStubSettings.bulkMutateRowsSettings()`), so a `sendOutstanding()` reaching nothing
  still lets every row land one `drainInFlight()` later and every emulator IT passes — a row-count
  assertion is not evidence that a flush flushed. `add` is the one operation those ITs do pin, by
  reading the rows back, so its unit test is a restatement rather than new coverage. One thing the
  tests still do not reach, stated so it is not mistaken for pinned: **"the shutdown waits"** —
  `BatcherImpl.add`'s precondition reads `closeFuture`, which `closeAsync()` sets too, so binding
  the shutdown to `closeAsync()` or `close(Duration.ZERO)` would survive
  `shutsDownTheBatcherTheFactoryItselfBuilt`, and the adapter's documented "no timeout" decision is
  argued but unpinned. The `LOG.warn` was the second such gap and is closed by #323; `destination`
  is read by nothing else on the adapter, which is what makes that assertion the only thing keeping
  the field honest.
- **That teardown closes through `Closers.closeAll`, not a `try`/`finally`** (#324). Both release the
  client whichever way the shutdown ends and both propagate an `Error` unchanged; the difference is
  confined to the case where **both** steps throw, where a `finally` completing abruptly discards
  the `try`'s reason outright (JLS 14.20.2 — only try-with-resources suppresses). So the failure
  explaining the teardown was lost in favour of the one that followed from it, and the pair is
  reachable: `EnhancedBigtableStub` reports a failing context close as an `IllegalStateException`.
  Found while making the teardown testable and folded in with the user rather than filed.
  **Not the same defect as #276**, which was later resources being *abandoned* and which replaced
  thirteen `IOUtils.closeAll` lines and no `try`/`finally` at all — but the same primitive, and
  #276's reason for that primitive is why a new call site of it owes the `Error` test beside the
  exception ones (`IOUtils.closeAll(…, Exception.class)` rethrows from inside its loop, abandoning
  the client; the `Throwable.class` form collects an `Error` as `new Exception(e)`).
  **The cost is real and is on the escalation axis**: `closeAll` reports the *first* failure, so the
  throwable Flink inspects is now the shutdown's rather than the client's — a JVM-fatal *client*
  close arrives suppressed and unescalated, where before it replaced the shutdown's failure and did
  halt, while a JVM-fatal *shutdown*, previously discarded outright, now escalates correctly.
  Accepted deliberately (the shutdown is where gax's own code runs, the client's close being a thin
  wrapper), and it is the bound `Closers.closeAll`'s own javadoc already names rather than a new one
  to chase here.
- **`BigtableEmulatorDeviationITCase` asserts what the *emulator* does**, which is not a breach of
  the emulator-is-not-an-authority rule but its enforcement: it records the traps so an image bump
  has to declare them. The one that matters is the **status** — the emulator answers `INTERNAL`
  where the service answers `INVALID_ARGUMENT` or `NOT_FOUND`, and `INTERNAL` is fatal to this sink,
  so an emulator-only test would conclude "fails the job" for a condition the service makes
  droppable. It also **accepts an empty row key**, storing a row that breaks the client's own read
  state machine — a state the service cannot reach. Every row of the documentation page's deviation
  table is asserted from both sides.
- **`StubWriterInitContext` cannot drive this writer**, because its metric group is a
  null-returning proxy and the writer dereferences the group in its constructor. The emulator tests
  therefore build writers through the sink's injecting `createWriter(batcher, mailbox, metricGroup)`
  overload with a batcher the production factory created — the Cloud Tasks emulator tests' shape —
  and the MiniCluster job tests are what cover the `WriterInitContext` path end to end. The
  module's own `RecordingSinkWriterMetricGroup` is **gone since #237**: it predated test-utils'
  `TestSinkWriterMetricGroup` and was superseded by it, and the shared one is strictly better here —
  it asserts by *registered name*, which is what a gauge or an `errorClass` subgroup needs and what
  a counter-holding stub cannot offer. (`UnregisteredMetricsGroup`'s own sink writer group remains
  unusable for either: it hands out a fresh `SimpleCounter` per call, leaving what the writer
  captured unreachable.)
