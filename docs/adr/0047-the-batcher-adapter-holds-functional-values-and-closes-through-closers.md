<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0047: The batcher adapter holds functional values, and its teardown closes through `Closers`

- Status: Accepted
- Date: 2026-08-06 (a fourth functional value added the same day by the teardown seam; the
  client half reversed by [#232] / ADR-0074 on 2026-08-09)
- Issues: [#324]
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` (teardown)

> **Refinement.** Two counts below describe the adapter as it stood when this was written, and the
> shape has moved twice since. `shutdown()` was added as a fourth functional value, so the adapter
> binds `add`, `sendOutstanding`, `closeAsync` and `close` rather than three method references. And
> it holds **no client**: ADR-0074 reversed that half when the writer gained per-record
> destinations, because an adapter closing the client it was built over would tear down every
> sibling batcher of the same instance. What this ADR decided — the batcher's operations as
> functional values, and a teardown through `Closers` — is unchanged, and ADR-0074 says so. The
> prose is left as written, per the pointer-rather-than-rewrite convention ADR-0073 records.

## Decision

**`BigtableBatcherAdapter` holds its batcher as three functional values and its client as an
`AutoCloseable`** — the only seam a test can drive — and it is **two** vendor constraints rather
than one, which is why both halves are injected. The batcher's is the annotation: `Batcher` is
`@InternalExtensionOnly`, so a fake would be legal Java and an unsupported extension. The
client's is plain unextendability — `BigtableDataClient` reports no closed state and its only
constructor is package-private (`create(...)` is `public static`, which the offline tests use).
(`BoundedShutdown` is the same shape, not a different one — ADR-0003 tallies the copied-but-
never-checked "`final`" claims.)

- A **production constructor takes the batcher and delegates**, so the three method references
  binding one adapter to one batcher sit inside a class a test can construct rather than at the
  `create()` call site, where nothing would reach them — [#321]'s "a seam whose wiring no test
  covered", avoided rather than repeated. `create(BigtableDataClient)` exists for the other half
  of that wiring: an adapter handed any other closeable passes every test that injects its own
  client while leaking a channel pool per writer, and the only observable is a *consequence* of
  the client's close (`BigtableClientContext.close()` shuts down the background executor gax
  schedules a batcher's delay-threshold push on, so a later `newBulkMutationBatcher` is rejected
  — measured, SDK internals, reread on a client upgrade). The *smaller* shape [#324] offered — a
  static seam beside the absorb — was measured and rejected: a test calling it cannot reach
  `close()` or `sendOutstanding()`, so it kills one of the three live mutants.
- **What hid the `sendOutstanding` gap is worth keeping**: gax pushes a batch on its own
  delay-threshold timer, 1 s for bulk mutations (documented on
  `EnhancedBigtableStubSettings.bulkMutateRowsSettings()`), so a `sendOutstanding()` reaching
  nothing still lets every row land one `drainInFlight()` later and every emulator IT passes — a
  row-count assertion is not evidence that a flush flushed. One thing the tests still do not
  reach, stated so it is not mistaken for pinned: **"the shutdown waits"** — `BatcherImpl.add`'s
  precondition reads `closeFuture`, which `closeAsync()` sets too, so binding the shutdown to
  `closeAsync()` or `close(Duration.ZERO)` would survive
  `shutsDownTheBatcherTheFactoryItselfBuilt`, and the adapter's documented "no timeout" decision
  is argued but unpinned.
- **The teardown closes through `Closers.closeAll`, not a `try`/`finally`.** Both release the
  client whichever way the shutdown ends and both propagate an `Error` unchanged; the difference
  is confined to the case where **both** steps throw, where a `finally` completing abruptly
  discards the `try`'s reason outright (JLS 14.20.2 — only try-with-resources suppresses). The
  pair is reachable: `EnhancedBigtableStub` reports a failing context close as an
  `IllegalStateException`. Found while making the teardown testable and folded in with the user
  rather than filed. **Not the same defect as [#276]** (later resources being *abandoned*) — but
  the same primitive, and [#276]'s reason is why a new call site owes the `Error` test beside
  the exception ones. **The cost is on the escalation axis**: `closeAll` reports the *first*
  failure, so the throwable Flink inspects is the shutdown's rather than the client's — a
  JVM-fatal *client* close arrives suppressed and unescalated. Accepted deliberately (the
  shutdown is where gax's own code runs), and it is the bound `Closers.closeAll`'s own javadoc
  names.

[#232]: https://github.com/laughingman7743/flink-connector-gcp/issues/232
[#276]: https://github.com/laughingman7743/flink-connector-gcp/issues/276
[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#324]: https://github.com/laughingman7743/flink-connector-gcp/issues/324
