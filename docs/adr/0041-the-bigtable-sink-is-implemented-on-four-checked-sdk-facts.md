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

# ADR-0041: The Bigtable sink is implemented — not adopted or vendored — on four checked SDK facts

- Status: Accepted
- Date: 2026-08-02 (design settled on [#33], which holds the full comparison), revised by [#236]
  (2026-08-08), by [#436] (2026-08-10, the flow controller's figures), and by [#1175]
  (2026-09-05, protobuf wrappers for aggregate state)
- Issues: [#33], [#216], [#217], [#232], [#236], [#436], [#1175]
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md`

## Decision

`com.google.flink.connector.gcp:flink-connector-gcp-bigtable` is a Flink 2.1-only artifact with
`GoogleCredentials` on its public API and no failure-handler SPI, and its upstream is dormant —
so depending on it was rejected, and vendoring buys nothing once package normalization,
AutoValue→builder conversion, options objects, `CrossVersionSink` and `base.failure` wiring have
changed essentially every line. What *is* adopted is its `BaseRowMutationSerializer`'s **shape**
— `@Nullable RowMutationEntry serialize(element, context)` — so its users port by changing the
interface name (null = skip is the repository's own contract since ADR-0001). Its built-in
`GenericRecord`/`RowData` serializers are deliberately not ported: `RowData` belongs to [#217],
and an Avro convenience is additive whenever a use case appears.

**Four SDK facts this module is built on**, each checked against `google-cloud-bigtable` sources
rather than assumed:

- `newBulkMutationBatcher(TargetId)` is `@BetaApi` while the `String` overload is `@Deprecated`,
  so the `TableId.of(...)` form is taken and the beta annotation accepted — there is no
  non-beta, non-deprecated way to get a batcher.
- gax's `Batcher` is `@InternalExtensionOnly`, so a test fake must not implement it. That is why
  `MutationBatcher` exists as this module's own narrow SPI, wrapping the client batcher exactly
  as `TopicPublisher` and `TaskCreator` wrap theirs. It is also why `sendOutstanding()` is
  called rather than `Batcher.flush()`: the blocking one would stall the task thread while the
  completion mails the writer's state is mutated by pile up behind it.
- The client's bulk-mutation path has a **flow controller of its own** — 20,000 outstanding
  entries, 100 MiB, `LimitExceededBehavior.Block` — whose static limits its public API does not
  expose. So `Batcher.add()` *can* block the task thread, and keeping the writer's own bounds
  below the client's is the only available way to preserve the [#85] property that a full writer
  yields to the mailbox rather than blocking. The defaults (1000 / 64 MiB) do; that is why the
  reference page documents raising `maxInFlightEntries` as *moving* the bound rather than
  raising it, and why exposing the client's flow-control knobs is not the fix (it is the [#85]
  defect class itself). **Both figures are also what bound the two batch thresholds** ([#436],
  `docs/adr/0082`): `BigtableBatchingCallSettings.Builder.build()` requires each threshold to stay
  strictly below the matching budget and throws otherwise, so a threshold past one of them is a
  client that cannot be built rather than a batch that is too large.

  Both numbers were **read from the code** by [#436], and the pair this ADR carried until then —
  "1000 entries per channel, 100 MB" — was not: `ClientOperationSettings` sets
  `maxBulkMutateOutstandingElementCount = 20_000L` flat, with no channel term, and
  `setMaxOutstandingRequestBytes(100L * 1024 * 1024)`, which is 100 MiB rather than 100 MB
  (`google-cloud-bigtable` 2.80.0, read 2026-08-10). "1000 outstanding row keys per channel" is
  the javadoc on `EnhancedBigtableStubSettings.bulkReadRowsSettings()` — a different operation,
  and stale against its own code, which sets 20,000 there too. Nothing in the conclusion moves
  and the margin is wider than was claimed; what the correction shows is this ADR's own rule
  applied to itself, that an SDK fact is read from the code and never from the prose beside it.
- `RowMutationEntry.toProto()` is `@InternalApi`, and it is the only route to both the byte size
  the in-flight bound counts and the `FailedElement` payload — the entry exposes neither its key
  nor its mutations. Accepted deliberately: nothing mechanical flags it, since
  `check-flink-api-tiers` audits `org.apache.flink` imports only. If it ever disappears, the
  byte bound and `FailedMutation.getPayloadBytes()`/`getRowKey()` are the call sites to revisit.
  **Its per-record cost is measured, not assumed** ([#236]): the writer's call is one of four
  identical constructions per record, three of which are the client's own, and it is immaterial —
  the Evidence section carries the numbers and their limits. There is no local fix to weigh
  against them, which is why the knob to reach for is upstream, not here.

The admin path the auto-creation feature later added has vendor tiers of its own — the
client's `GCRules` is class-level `@BetaApi`, and the admin methods the ensure calls are
`@ObsoleteApi` —
recorded in [ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)'s
inventory under the same accepted-internal-call reading as the batcher facts above.

Further design decisions of the same cluster:

- **Retries stay in the client, so this module has no `RetrySchedule` and no retry knobs.**
  `MutateRows` ships a non-empty retryable-code set and retries per entry
  (`DEADLINE_EXCEEDED`, `UNAVAILABLE`, 10 ms doubling to 1 min, 10 min total). The exact
  opposite of Cloud Tasks, whose generated client retries `CreateTask` on nothing; the
  difference is in the clients, so neither is a precedent for the other.
- **One fixed table per sink, and no auto-creation by default.** A batcher is bound to one
  table, so per-record destinations would mean a batcher pool, a share of the in-flight budget
  each and an eviction policy — deferred until there is demand ([#232] records the deferral).
  A disposition-only auto-creation is a poorer fit here than in the Pub/Sub sink: a table's
  schema *is* its column families and their garbage-collection policies, which a sink cannot
  guess. Refined by ADR-0073 ([#233]): opt-in creation exists, gated on the user declaring that
  schema, with `CREATE_NEVER` still the default. **Reversed for the destination half by
  ADR-0074** ([#232]): the pool exists, over a client shared per (project, instance) — and of the
  three costs predicted here, the budget share is the one that was *not* paid, since the bounds
  stayed writer-global.
- **`TableDestination` sits at the module root, not under `sink`.** The root layout rule puts
  destination types in `sink`; this deviates because [#216]'s source facade takes the same
  value, and moving it later would churn every importer. `appProfileId` is deliberately *not*
  part of it: a profile selects client routing, not a data address, so it is a builder option.
  The sink has no family layer ([#119]: one write family, `MutateRows`, with no sibling in
  prospect — `checkAndMutateRow` and `readModifyWriteRow` are request-response primitives), so
  `BigtableMutateRowsSink` sits beside its facade and `FailedMutation` at the `sink` root (the
  post-[#213] placement rule). **Refined by ADR-0148** ([#1178], 2026-09-03): the sibling
  arrived — exactly those two request-response primitives — and lives in its own family layer,
  `sink.singlerow`, while `sink.writer` stays where it is until a separate mechanical move.

## Evidence

Concerns the fourth SDK fact only — what the writer's per-record `toProto()` costs ([#236]).
Measured against `google-cloud-bigtable` 2.80.0, `gax` 2.82.0 and `protobuf-java` 4.33.2, the
versions `libraries-bom` 26.85.1 resolves for this module.

**The writer's call is one of four, and three of the four are the client's own.** Read from the
sources rather than inferred: `MutateRowsBatchingDescriptor.createResource()` builds the proto
**twice** — once through `countBytes()`, once for `element.toProto().getMutationsCount()` — and
gax's `BatcherImpl.add()` calls it on every element; `BulkMutation.add(entry)`, reached from the
same `add()` through `RequestBuilder`, builds a third, and that one is the request that goes on
the wire. `BigtableWriter.write()` builds the fourth. So removing the writer's call would cut the
count by a quarter, not the half the issue assumed.

**The serialization walk in the writer's call is shallow, and it copies nothing.**
`Mutation.addMutation()` calls `mutation.getSerializedSize()` on every cell as it is added, to
maintain a running `byteSize`, and protobuf memoizes serialized size per message instance. Those
child `com.google.bigtable.v2.Mutation` instances are shared by reference into every `Entry`
built afterwards, so what `getSerializedSize()` does here is read memoized child sizes and sum
their tag and length prefixes — never re-serialize a payload. The memoization is not assumed
either: `Mutation.getSerializedSize()`'s bytecode at `protobuf-java` 4.33.2 opens on
`getfield memoizedSize` and returns early. The construction around it is `ImmutableList.build()`,
an `Entry.Builder`, `addAllMutations` and `build()`: reference copying.

Measured 2026-08-08 on an Apple aarch64 laptop, OpenJDK 21.0.5, `-Xmx4g` and the JDK 21 default
collector; five JVM forks, each 4 s warmup then 7 timed iterations of ≥2 s per arm, per-iteration
medians then the median across forks. Entries are pre-built into a pool so the child protos
arrive memoized exactly as they do in production. Arms: `build` (constructing the entry, which
every record pays anyway), `ours` (the writer's line), `client` (the three constructions above),
and `both`, whose `both − client` is the marginal cost the writer really adds.

| Mutation shape | Entry | `build` | `ours` | `client` | `ours` allocation |
|---|---|---|---|---|---|
| 1 cell / 64 B | 88 B | 61.5 ns | **33.0 ns** | 96.0 ns | 176 B |
| 8 cells / 128 B | 1191 B | 394.3 ns | **154.1 ns** | 351.7 ns | 336 B |
| 64 cells / 128 B | 9533 B | 2889.2 ns | **912.0 ns** | 1279.1 ns | 1232 B |
| 1000 cells / 128 B | 149 897 B | 44 034.1 ns | **20 415.0 ns** | 24 438.2 ns | 16 208 B |

- **The writer's line allocates almost exactly a third of what the client's own proto work
  allocates** — 32.1%, 32.7%, 33.1%, 33.3% across the four shapes — which is the four-versus-three
  reading confirmed by measurement rather than by reading the same sources twice.
- **Its allocation is ~16 B per mutation on a ~200 B fixed cost** (the 1-cell case measures
  176 B): two eight-byte reference copies per mutation, no payload. Note this also rules out the
  JIT quietly deleting the work — the writer's `Entry` never escapes, so scalar replacement could
  in principle have elided it and made the arm meaningless; the allocation counter says it did
  not.
- **It is ~17% of the writer's in-process per-record path** (`build` + `ours` + `client`) — 17.3%,
  17.1%, 18.0%, 23.0% across three orders of magnitude. That is an *upper* bound on the share:
  the real path also carries `awaitCapacity`, the callback allocation, `ApiFutures.addCallback`
  and the metric updates, none of which are in the denominator. So even with a free service,
  removing the line would raise a CPU-bound ceiling by at most about a fifth.
- **Against the service it is noise.** Bigtable publishes up to 14,000 rows per second per SSD
  node, estimated at 1 KB rows — 71.4 µs of node budget per row. At the measured shape closest to
  that assumption (8 cells / 128 B, a 1191 B entry), the line costs 154 ns, or **0.22%** of it.

**Verdict: immaterial.** The local code is left alone.

**Why the allocation ratio is a flat third while the time ratio is not.** `ours` takes 34.4%,
43.8%, 71.3% and 83.5% of `client`'s *time* as the mutation count grows, against a *bytes* ratio
pinned at a third — which looks like a contradiction and is not. Only constructions allocate, and
there are three of them in `client` against one in `ours`; but only *one* of the client's three
walks the size, since `createResource`'s second call reads `getMutationsCount()` and
`BulkMutation.add` reads nothing. So `ours` is one construction plus a walk while `client` is
three constructions plus one walk, and the walk grows with the mutation count. A fifth arm
measured the split directly — `entry.toProto().getMutationsCount()`, construction with no walk,
three forks:

| Mutation shape | construction | size walk | walk share | construction allocation |
|---|---|---|---|---|
| 1 cell / 64 B | 27.1 ns | 3.8 ns | 12.3% | 176 B |
| 8 cells / 128 B | 95.1 ns | 60.1 ns | 38.7% | 336 B |
| 64 cells / 128 B | 213.7 ns | 545.3 ns | 71.8% | 1232 B |
| 1000 cells / 128 B | 2433.1 ns | 16 251.1 ns | 87.0% | 16 208 B |

Two things fall out, and both are checks rather than restatements. The construction arm allocates
**byte-for-byte** what `ours` does at every shape, so the walk allocates nothing — the shallow-walk
claim measured rather than argued from the sources. And `3 × construction + 1 walk` predicts the
independently measured `client` to within 2–11% at every shape (85.1 vs 96.0, 345.4 vs 351.7,
1186.4 vs 1279.1, 23 551 vs 24 438), which is the whole structural model confirmed by arms that
share no code path.

**What the measurement is and is not good for.** The allocation figures are exact: `both − client`
matched `ours` to 0.0% in all twenty fork×shape cells, which is the harness's own validity check.
The nanosecond figures are not that good — the same check brackets them to roughly ±30% at the
realistic shapes and worse at 1000 cells, where per-record allocation of 341 KB makes the arm
GC-bound; that shape is a slope probe and not a workload anything should send. The conclusion
survives an order of magnitude of error, so ±30% does not threaten it, but a future comparison of
two candidate *implementations* would need a better harness than this.
A JVM-configuration change moved every arm by 2–4.5× while leaving every allocation figure
untouched, which is why the configuration above is stated rather than assumed; a rerun that does
not reproduce these ratios should suspect its own configuration first. The harness itself is not
in the repository (it is a one-off decision input, not a regression guard); its source is on the
[#236] pull request.

**The upstream lever, recorded because it is where a real fix lives.** Two findings in
`googleapis/google-cloud-java/java-bigtable` — `googleapis/java-bigtable` is archived:
`createResource()` builds the proto twice where once would do, and `RowMutationEntry` exposes no
size accessor even though `Mutation` already tracks `byteSize` and `numMutations` as private
fields. Both are the client's own hot path for every bulk-mutation user, not only this connector.
Both were re-read on that repository's **`main`** on 2026-08-08, not only in the pinned release,
so neither is a fix already shipped and waiting on a bump, and both are now filed with a pull
request each: `googleapis/google-cloud-java#14016` with #14017, and #14018 with #14019.

Implementing the second turned up a **third** defect, filed as #14020 with #14021: `Mutation`
enforces `MAX_MUTATIONS` and `MAX_BYTE_SIZE` against counters that only `addMutation` maintains,
while three `fromProto` factories add to the list directly — so mutations wrapped from a proto
count towards neither. Measured rather than inferred: five wrapped mutations plus `MAX_MUTATIONS`
added through `setCell` produced 100,005 mutations with no exception. The count is backstopped by
`RowMutationEntry.toProto()` and `BulkMutation.add`, which re-check the real list size, so it
surfaces late and as a different exception type; the **byte** limit has no backstop anywhere, and
that is the half genuinely lost. It reaches this connector only through
`createFromMutationUnsafe`. The sink runtime does not construct entries through that factory,
but the aggregate-state serializer example added by [#1175] does; its limits are recorded below.

**Aggregate state through protobuf wrappers, measured 2026-09-05 ([#1175]).** With
`google-cloud-bigtable` 2.82.0, the first real-GCP run of `BigtableSinkRealGcpITCase` passed five
cases and failed `mergesAnAccumulatorAndRepeatsItsEffectOnReplay`: an Int64 Sum state read from
the service and passed to the SDK's `mergeToCell` convenience overload was rejected with
`INVALID_ARGUMENT` and `Error in field 'input' : must use bytes_value`.
`MutationApi` wraps this input as `Value.RawValue`, and the SDK `Value` model has no
`bytes_value` variant. These are versioned client facts; the rejection is an observation for
Int64 Sum on that date, not a claim about every aggregate type or future service behavior.

The example therefore sets `MergeToCell.input.bytes_value` in a protobuf and uses the public
beta `Mutation.fromProtoUnsafe` and `RowMutationEntry.createFromMutationUnsafe` factories.
The second run passed all six cases. Its merge case read an Int64 Sum accumulator from the
service, wrote it through the public sink, and observed 7 then 14 after a second completed job
serialized the same input. This does not simulate an SDK retry or checkpoint restoration.
`BigtableAdvancedMutationTest` pins both the convenience overload's current encoding and the
explicit protobuf's preservation through schema serialization, writer submission, isolation,
and failure payloads.

The wrappers' costs remain: `fromProtoUnsafe` bypasses `Mutation`'s 200 MiB byte-size guard
and returns a mutation that permits server-side timestamps, so a chained
`setCell(family, qualifier, -1L, value)` accepts a timestamp the ordinary builder rejects.
The row-entry count check in `toProto()` still reads the actual mutation list, but no analogous
byte-size check is restored. The example supplies one mutation, an explicit timestamp, and
service-produced Int64 Sum state; copying the wrapper into a general serializer does not
establish a byte bound or retry idempotence.

Adopting the accessors once a released client has them is [#400], deliberately a separate issue
from [#236]: the measurement [#236] asked for is finished and its answer was "change nothing
locally", while adoption waits on three approvals, a release and a BOM bump that are not this
repository's to give. [#131]'s mechanism — a test pinning the broken behaviour, which fails the
moment a bump fixes it — does not transfer, because nothing here depends on the missing accessors,
so no test changes when they arrive.

## Alternatives declined

Concerns the fourth SDK fact only ([#236]); the rest of this ADR's alternatives are on [#33].

- **Dropping the byte bound**, so no size is needed. No: it is the bound that actually bounds
  memory. A single row mutation can be megabytes, so a count alone bounds nothing — the same
  argument [#85] settled for Pub/Sub.
- **Estimating the size** instead of computing it. No: an estimate that is wrong high stalls the
  writer, and one that is wrong low makes the bound meaningless. A guess is worse than a real
  number that costs something — and the measurement above says the real number costs 0.22% of a
  node's per-row budget, so there is nothing to buy.
- **Caching the proto on the completion callback**, so the failure path does not build a fifth one
  in `FailedMutation`. No: that doubles retention for every in-flight mutation to save an
  allocation on a path only failures reach.
- **Any local fix at all.** There is none to weigh: `RowMutationEntry` exposes neither its key,
  its mutations nor its size, verified against 2.80.0 with `javap` and re-read on upstream `main`
  — beyond the mutation builders and the static factories, `toProto()` is its only instance
  method. This is why the fix is upstream by elimination rather than by preference.

[#33]: https://github.com/flink-gcp/flink-connector-gcp/issues/33
[#85]: https://github.com/flink-gcp/flink-connector-gcp/issues/85
[#119]: https://github.com/flink-gcp/flink-connector-gcp/issues/119
[#213]: https://github.com/flink-gcp/flink-connector-gcp/issues/213
[#216]: https://github.com/flink-gcp/flink-connector-gcp/issues/216
[#217]: https://github.com/flink-gcp/flink-connector-gcp/issues/217
[#232]: https://github.com/flink-gcp/flink-connector-gcp/issues/232
[#233]: https://github.com/flink-gcp/flink-connector-gcp/issues/233
[#131]: https://github.com/flink-gcp/flink-connector-gcp/issues/131
[#236]: https://github.com/flink-gcp/flink-connector-gcp/issues/236
[#400]: https://github.com/flink-gcp/flink-connector-gcp/issues/400
[#436]: https://github.com/flink-gcp/flink-connector-gcp/issues/436
[#1178]: https://github.com/flink-gcp/flink-connector-gcp/issues/1178
[#1175]: https://github.com/flink-gcp/flink-connector-gcp/issues/1175
