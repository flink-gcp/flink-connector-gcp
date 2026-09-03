# Detailed guidance — flink-connector-gcp-base

Rules for the shared main-code module (#61). Read before adding anything here; each decision's
record — context, evidence, declined alternatives — is the named ADR under `docs/adr/`.

## Scope and dependencies

- **Main-code shared infrastructure only** — test-support code stays in
  `flink-connector-gcp-test-utils`, whose detailed guidance records the mirror-image rule. Everything
  here is `@Internal` **except `base.failure`** (a user-implemented SPI cannot be internal —
  `docs/adr/0036`) and `base.source.StartPosition` (the user-configured value shared by the
  Bigtable and Spanner change-stream sources — `docs/adr/0094`). **A type only moves in once it
  has multiple consumers.**
- Dependencies are `flink-core` (provided) plus `gax`/`grpc-api`/`protobuf-java` (BOM-managed).
  Consumers depend on this module at **compile** scope, so it is bundled into the
  `flink-sql-connector-gcp-*` uber-jars and must be relocated there (`docs/adr/0015`), and it is
  on the justfile `binary-compat`/`e2e` install lists for the reactor-resolution reason
  test-utils is (#181).
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here touches a 1.x/2.x
  API gap — **not only the `Sink` one**, since the roots hold whatever differs across the majors
  (BigQuery's `CrossVersionCheckpointId` is a `CommittableMessage` accessor, #404).
  `DefaultFailureHandlerContext.of(WriterInitContext)` is not a counter-example — the type and
  both methods it reads exist identically in 1.20 and 2.x.

## `base.failure` (`docs/adr/0036`)

- Handler `flush()` runs after each writer's write-path drain; the guarantee is at-least-once
  for failures that recur on replay; `open()` grows only for a real consumer. Which failures are
  row-level stays per-connector. `getConnector()` values are lower-case module words and are
  API. The policy semantics every writer shares live in the `FailureHandler` javadoc.

## `base.metrics` and metric naming (`docs/adr/0037`, `0038`)

- `MetricValues.elapsedMillis` clamps equal or future values to zero and saturates subtraction
  overflow at `Long.MAX_VALUE`, so connector lag and health gauges share one boundary policy.
- `ErrorClassCounters` and `DestinationMetrics` default to plain `SimpleCounter`s, valid on the
  task thread only; a connector counting from a callback thread passes the `Supplier<Counter>`
  overload a `ThreadSafeSimpleCounter` (the Bigtable single-row async function; ADR-0148). Entries
  are never removed — Flink cannot unregister a metric.
- **`numRecordsSend` counts each record once, at the first hand-off**, inside the send call
  under a first-attempt flag (`docs/adr/0037`).
- Every connector declares its names in one `<Product>MetricNames` inventory at its module root;
  counters name events, gauges name states, and **no name takes Flink's `num` prefix**
  (`docs/adr/0038`; the mechanical half is `just check-metric-docs`).

## `base.source` (`docs/adr/0083`, `0108`)

- The one `AutoCloseable` `PullAssignmentSplitEnumerator` takes is the enumerator's for its
  lifetime, and `close()` ends it. A source therefore mints one per `createEnumerator` and
  `restoreEnumerator` rather than carrying one on its serialized configuration, because the
  JobManager reuses one `Source` object for a job's whole life (`docs/adr/0128`). Do not add a
  `protected` accessor for it here: a subclass that also plans through the seam keeps its own typed
  field, which is what all three subclasses do.
- `SynchronousDeserializationCollector.deserialize` is the one direct adapter for all collector-
  shaped source deserializers. It creates one collector per input, forwards each record immediately,
  returns the successful count, and clears its downstream function in `finally`. Do not add a record
  buffer, monitor, owner-thread field, lifecycle flag or failure latch. Async transport callbacks
  hand records to the task thread before this boundary; a genuinely async deserializer needs a
  separate bounded and checkpoint-aware protocol (`docs/adr/0108`).
- `StartPosition` applies only when no source state is restored; checkpointed per-partition state
  wins. `StartPositionResolver` captures the startup clock once, discovers retention lazily and at
  most once, and owns the one-minute safety margin, clamp-and-WARN behavior, future rejection and
  restore-expiry policy (`docs/adr/0094`). A fresh `latest()` is the only path that needs no
  retention lookup. Keep service error translation and retention discovery in each connector.
- An expired restore fails unless the builder supplied a fallback. A fallback is resolved against
  the same startup instant and retained window, and its warning names the affected partition or
  dependent ledger and lost window. Bigtable restarts each expired range; Spanner must inspect the
  whole unfinished ledger and replace it with one null-token query if any entry expired, because an
  advanced old token can skip the child record that carries its descendants. Never classify expiry
  by vendor error-message text.
- `PullAssignmentSplitEnumerator` is the assignment protocol every **bounded, pull-assigned**
  source shares: the queue, the parked requests, `serve`, the returned splits, the one-shot
  `callAsync` plan with its `closed` guard, and the close of the seam. `start()`, `close()` and the
  three assignment methods are **`final`** — a connector able to override them could lose the guard
  in one module and keep it in the other. A connector supplies five hooks plus its own
  `snapshotState`, and **the checkpointed state type stays per connector** (`StateT`).
- **The counters are registered by the connector, in `registerCounters`, and that is not style**:
  `scripts/check-metric-docs.py` reads a literal `.counter(<Product>MetricNames.X)` out of the
  module's own sources to decide a documented metric is registered, so a name passed in here as a
  string would report as unregistered. Keep the registration call where the name lives;
  `EnumeratorCounters.unregistered()` is what stands in when a context offers no metric group.
- Logging goes through `LoggerFactory.getLogger(getClass())` — the concrete connector's category,
  so a connector-scoped log configuration keeps matching. **Deliberately the opposite of
  `BoundedShutdown`**, which logs under its own name (`docs/adr/0007`): there the shared class is
  the subject of the message, here the connector is.
- **Nothing continuous belongs here.** This class plans once; a change-streams enumerator
  discovering partitions as it goes is a different shape, and widening this one is an argument to
  make on evidence rather than a hook to add.

## `base.retry` and `base.rpc` (`docs/adr/0039`)

- Retry loops stay in the connectors; do not add a `Retries.run` executor. Every schedule
  jitters at `RetrySchedule.DEFAULT_JITTER_RATIO` — a literal ratio is a review finding, and the
  ratio is never a knob. Knob-to-schedule mapping lives on the options class that owns the
  knobs (`toRetrySchedule()`), never in the consumer.
- `Retries.sleep` call sites name what is being waited for. `StatusCodes.codeOf` inspects one
  throwable and never walks the cause chain — traversal and classification stay per-connector.
- `EmulatorEndpoint` is the only form an endpoint travels in past the setter; whitespace is
  rejected, never trimmed, and the host splits at the last colon, kept verbatim. Public
  signatures stay `String`. **`parse` takes the setting's name and has no one-argument form**
  (#895): both messages name what the caller was given — the setter for a builder, the option key
  for a table factory — and a defaulted name is exactly how BigQuery's two `emulatorRestEndpoint`
  setters spent their life naming a setter the user may not have called.
- **The check runs where the value is configured**, which for SQL is the factory — not the runtime,
  and not a builder setter (#1009, #1013, #1019, `docs/adr/0127`). **All five table factories parse**,
  through a private `validateEmulatorEndpoint(ReadableConfig)` passing `EMULATOR_ENDPOINT.key()` —
  `validateEmulatorEndpoints` on BigQuery, which covers `emulator-rest-endpoint` too under its own
  key, in both directions, even on a direct-table source that leaves the value unused (measured:
  before this nothing parsed it there at all). Being on the client is not enough on its own:
  Pub/Sub, BigQuery and Cloud Tasks already failed during plan-to-runtime translation, but through
  the builder setter, so the message named `emulatorEndpoint` to a caller who had written a DDL key.
  The later parse sites all stay — `BigtableDataClientRowLookup`, `BigtableFullCacheInputFormat` and
  `SpannerDatabaseRowLookup` at `open()`, and every builder setter — because they are the checks
  behind `@Internal` constructors and behind the DataStream API, not the ones a SQL caller meets.
- **A factory's endpoint parse goes behind every check that refuses an option outright**, in every
  direction of all five connectors. The option pre-empted need not be `emulator-endpoint` itself:
  Spanner accepts one in every mode, and the call still follows `validateSourceMode` because that
  refuses *other* options. Each ordering carries a test asserting the removal message and asserting
  the shape message is absent, because a wrong order passes every build. It goes behind the
  **required-option** checks too, so a table that has not said where it points hears that first:
  three connectors get that from `helper.validate()`, while Pub/Sub and BigQuery declare their
  destination options conditionally and check them as ordinary statements. **The limit is assembly**:
  an option mapper that refuses an option while a source or sink is being built —
  `TopicCreateOptionsMapper`, `PublisherOptionsMapper`, `SubscriptionCreateOptionsMapper`,
  `TableCreateOptionsMapper` — runs after the parse and is pre-empted by it, which `docs/adr/0127`
  records rather than fixes.
- `EmulatorChannels` is split **by who owns the channel**, not by settings type (`docs/adr/0081`):
  `plaintextProvider` where the client closes its own channel, `openPlaintextChannel` +
  `fixedProvider` where the caller does — and the ownership difference is load-bearing at three
  sites, so never unify them. **Always pass an API's own
  `defaultGrpcTransportProviderBuilder()`**, never a bare one: the API's defaults include the
  inbound message limit, and losing it fails only on an emulator and only past 4 MiB.
  `NoCredentialsProvider` stays at each call site — the three builder types share no supertype —
  and `PubSubTestClients` cannot use any of this, since `base` depends on test-utils and not the
  reverse. `plaintextProvider` reaches the channel through the `@BetaApi`
  `setChannelConfigurator`, so its five client-owned emulator sites ride that Beta surface — the
  three caller-owned `openPlaintextChannel`/`fixedProvider` sites do not. An internal call,
  tier-irrelevant under `docs/adr/0141`; reread the annotation on a gax bump.

## `base.options` (`docs/adr/0068`)

- `OptionChecks` holds `checkPositive`, `checkAtLeastOneMilli`, `checkAtLeastOneMilliOrZero` and
  `checkExpressibleInNanos`; each clears the multiple-consumer bar on its own — `docs/adr/0068`
  carries the dated call-site survey. **A new check owes an argument of that shape** — "it is a
  precondition too" is not one, and this is not a general-purpose precondition library.
- **Every `Duration` positivity message carries the offending value**, which is what settled the
  three shapes the tree had grown for one check. A rejection that names only the knob leaves a
  builder chain setting several durations ambiguous.
- `checkExpressibleInNanos` deliberately does **not** check positivity: `BoundedShutdown` accepts
  a spent budget and the first-checkpoint watchdog accepts `Duration.ZERO`, so a caller wanting
  both calls both. Its message names the ceiling **and the year count**, because
  `Duration.toString()` renders it as an hour count that a SQL user is shown verbatim; tests pin
  the year count, so dropping it fails.
- `checkAtLeastOneMilli` is the floor for a `Duration` **the connector converts with
  `toMillis()`**, and that conversion — never the knob's name — is what decides whether a setter
  needs it: the Pub/Sub source's `shutdownTimeout` carries the floor and the sink's knob of the
  same name does not, because only one of them truncates (`docs/adr/0068`). Adding one means
  finding the conversion first, and a sub-millisecond value that is harmless where it lands
  (a warning threshold, an already-`Math.max(1, …)`-floored park) is left alone. It folds
  positivity in rather than composing with `checkPositive`, so zero and negative get the floor's
  message; its parenthetical asserts the granularity, so a site where that is false is a site
  where the check does not belong.
- **A value the vendor SDK itself defines is not the floor's to refuse**, which is what
  `checkAtLeastOneMilliOrZero` is for: **a knob this project only forwards stays settable as the
  vendor defines it**. Twelve knobs across pubsub and bigquery pass `Duration.ZERO` through,
  because gax, the Pub/Sub subscriber and the BigQuery Storage writer each document a meaning for
  it (`docs/adr/0068` carries the table). The floor is what makes that safe rather than what it
  trades against: the vendor reads these with `toMillis()`, so a *positive* sub-millisecond value
  would arrive as the sentinel. A `Duration` this project **spends itself** keeps the plain floor —
  nothing on the other side gives its zero a meaning — so choosing between the two means asking
  who consumes the value, not how the knob reads. **And a forwarded knob the vendor does not
  truncate takes neither**: `maxAckExtensionPeriod` is spent as `now().plus(period)` at nanosecond
  resolution, so it carries a non-negative check at its own setter. The test for which one applies
  is the message — a floor that would promise millisecond granularity where none applies does not
  belong there.
- Numeric (`int`/`long`) positivity checks stay inline in their builders — the helper is
  `Duration`-typed.

## `base.lifecycle` (`docs/adr/0040`, `docs/adr/0007`, `docs/adr/0068`)

- **`BoundedShutdown`**: why it exists, and the decisions inside it, are `docs/adr/0007`; the
  class contract (daemon thread, nullable-`Runnable` release hook, idempotent `close()`,
  one-thread precondition, per-field threading, the caller-supplied `LongAdder`'s ownership
  argument) lives in its own javadoc, published by the API reference. What belongs here: it is
  client-agnostic by construction (two functional values, a `String description`, no gax or gRPC
  import — the module gained no dependency); its warnings log under
  `base.lifecycle.BoundedShutdown`, which a `…connector.pubsub`-scoped log configuration stops
  matching; its give-up message carries **no issue link**, deliberately — a shared class must
  not send one client's operator after another client's defect. `timeout()` is the module's
  first `@VisibleForTesting public` method, public only because a sibling module's tests read
  it; the next seam here should cite this rather than widen by default. The Pub/Sub source's
  subscriber teardown was measured (#325) and is **not** a candidate adopter
  (`docs/adr/0012`). A first draft held an `AtomicLong` here; do not reintroduce it.
  **The budget is a precondition, not just a parameter**: at most
  `Duration.ofNanos(Long.MAX_VALUE)`, rejected by the constructor as well as by every setter that
  feeds one, because a consumer building its budget in code reaches no setter (#334;
  `docs/adr/0068`). At that ceiling `deadlineNanos` overflows and `remainingNanos()` is **still
  correct** — the subtraction wraps back, measured — so leave the arithmetic alone: an
  `Math.addExact` or a clamp added to "harden" it is the change that would break a legal budget,
  and is what `theLargestExpressibleBudgetIsNotSpentTheInstantItStarts` exists to catch.
- **`Closers`**: every `close()`-shaped call site goes through `closeAll` /
  `closeAllSuppressing`; the contract lives in the class javadoc, the written-out-loop decision
  and its `Error`-type reasoning in `docs/adr/0040`. A new call site owes the `Error` test
  beside the exception ones, and creation guards catch `Throwable`, not `Exception`.
