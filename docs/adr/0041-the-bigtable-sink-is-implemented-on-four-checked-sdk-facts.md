<!--
Copyright 2026 laughingman7743

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
- Date: 2026-08-02 (design settled on [#33], which holds the full comparison)
- Issues: [#33], [#216], [#217], [#232]
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
- The client's bulk-mutation path has a **flow controller of its own** — 1000 entries per
  channel, 100 MB, `LimitExceededBehavior.Block` — whose static limits its public API does not
  expose. So `Batcher.add()` *can* block the task thread, and keeping the writer's own bounds
  below the client's is the only available way to preserve the [#85] property that a full writer
  yields to the mailbox rather than blocking. The defaults (1000 / 64 MiB) do; that is why the
  reference page documents raising `maxInFlightMutations` as *moving* the bound rather than
  raising it, and why exposing the client's flow-control knobs is not the fix (it is the [#85]
  defect class itself).
- `RowMutationEntry.toProto()` is `@InternalApi`, and it is the only route to both the byte size
  the in-flight bound counts and the `FailedElement` payload — the entry exposes neither its key
  nor its mutations. Accepted deliberately: nothing mechanical flags it, since
  `check-flink-api-tiers` audits `org.apache.flink` imports only. If it ever disappears, the
  byte bound and `FailedMutation.getPayloadBytes()`/`getRowKey()` are the call sites to revisit.

Further design decisions of the same cluster:

- **Retries stay in the client, so this module has no `RetrySchedule` and no retry knobs.**
  `MutateRows` ships a non-empty retryable-code set and retries per entry
  (`DEADLINE_EXCEEDED`, `UNAVAILABLE`, 10 ms doubling to 1 min, 10 min total). The exact
  opposite of Cloud Tasks, whose generated client retries `CreateTask` on nothing; the
  difference is in the clients, so neither is a precedent for the other.
- **One fixed table per sink, and no auto-creation.** A batcher is bound to one table, so
  per-record destinations would mean a batcher pool, a share of the in-flight budget each and an
  eviction policy — deferred until there is demand ([#232] records the deferral). Auto-creation
  is a poorer fit here than in the Pub/Sub sink: a table's schema *is* its column families and
  their garbage-collection policies, which a sink cannot guess.
- **`TableDestination` sits at the module root, not under `sink`.** The root layout rule puts
  destination types in `sink`; this deviates because [#216]'s source facade takes the same
  value, and moving it later would churn every importer. `appProfileId` is deliberately *not*
  part of it: a profile selects client routing, not a data address, so it is a builder option.
  The sink has no family layer ([#119]: one write family, `MutateRows`, with no sibling in
  prospect — `checkAndMutateRow` and `readModifyWriteRow` are request-response primitives), so
  `BigtableMutateRowsSink` sits beside its facade and `FailedMutation` at the `sink` root (the
  post-[#213] placement rule).

[#33]: https://github.com/laughingman7743/flink-connector-gcp/issues/33
[#85]: https://github.com/laughingman7743/flink-connector-gcp/issues/85
[#119]: https://github.com/laughingman7743/flink-connector-gcp/issues/119
[#213]: https://github.com/laughingman7743/flink-connector-gcp/issues/213
[#216]: https://github.com/laughingman7743/flink-connector-gcp/issues/216
[#217]: https://github.com/laughingman7743/flink-connector-gcp/issues/217
[#232]: https://github.com/laughingman7743/flink-connector-gcp/issues/232
