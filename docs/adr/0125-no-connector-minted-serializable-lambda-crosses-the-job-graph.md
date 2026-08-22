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

# ADR-0125: No connector-minted serializable lambda crosses the job graph

- Status: Accepted
- Date: 2026-08-18 (measured 2026-08-18)
- Issues: [#822](https://github.com/flink-gcp/flink-connector-gcp/issues/822),
  [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777)
- Modules: bigquery, bigtable, cloudtasks (rule applies to all)
- Current behavior: no user-visible change

## Context

Four connectors already state the rule in one place each — `FixedDestinationResolver` is a named
class because "lambda serialization would tie the job graph to fragile `SerializedLambda`
synthetic-method identity across connector versions" — and no ADR recorded it. The audit finding
[#822] asked whether that reasoning is real before spreading it, and required the decision rule to
be posted before the measurement rather than after.

**Reachability.** Flink "persists metadata and the job artifacts" in order to recover submitted
jobs, and its `Dispatcher` is constructed with a `Collection<JobGraph> recoveredJobs` that it runs:
recovery replays the *persisted* graph rather than regenerating one. The Kubernetes operator's
`last-state` upgrade mode suspends with "Cancel / Delete (keep HA metadata)" and restores by "Use
HA metadata", while the spec change that triggered the upgrade may carry a new image — hence a
different connector jar under a graph serialized by the old one. ADR-0112 already reasons this way: "A job
graph serialized before this decision retains that legacy pre-created-table behavior after an
upgrade."

**What a serialized lambda's identity actually is.** Measured, not assumed. javac names a
serializable lambda `lambda$<enclosingMethod>$<hash>$<index>`. Two lambdas share the hash when they
share an **enclosing declaration** and a descriptor — a method body is one declaration, and each
field initializer is its own, so the two in `BigQuerySinkBuilder`'s field initializers have
separate hashes while the eight in `CellValueCodec.encoder` share one. Where the hash is shared,
the trailing index is the only thing left telling them apart. A two-version probe — compile,
serialize, restore against a build differing by one edit — gives:

| edit to the capturing class | result |
| --- | --- |
| an unrelated lambda added ahead | restores correctly |
| an unrelated field added | restores correctly |
| a same-shaped lambda added ahead **in the same method body** | **restores a different lambda, no exception** |
| the same method's lambdas reordered | **restores a different lambda, no exception** |
| a same-shaped lambda added ahead as a separate field initializer | restores correctly — its own hash |
| the enclosing method gains a parameter | fails loudly (`InvalidObjectException`) |

So the hazard is not the one the finding assumed. Isolated lambdas — the four in
`BigQuerySinkBuilder`, the one in `CdcChangeTypeProvider.upsertOnly()` — each carry a distinct hash
at index `$1` and survive ordinary edits. The hazard is a **run of lambdas sharing one hash**,
where only the trailing index separates them.

Both codec switches were exactly that: `CellValueCodec.encoder` minted eight lambdas under one
hash and `decoder` six, `RowDataJsonRenderer.build` eleven, each switch documented as ordered the
way `LogicalTypeRoot` declares its constants — so supporting one more root inserts a case *in the
middle*. Measured on the real class: serializing `CellValueCodec.encoder(BIGINT)` and restoring it
against a build with one added root yields the INTEGER encoder, which writes four bytes
`[0, 0, 0, 1]` where the BIGINT layout is `[0, 0, 0, 1, 0, 0, 0, 1]` — a truncated value in a
Bigtable cell, with no error anywhere.

A repository-wide sweep (javac gives serializable lambdas the hash form and plain lambdas the bare
`lambda$m$N` form, so they are mechanically separable) found the escaping set was wider than the
finding listed: `BigQuerySinkConfig`'s own null-fallback provider, `CdcProtoRowFields`' two value
providers, `ProtoRowAugmentationField.physical`'s encoder, and the Cloud Tasks table layer's
`taskIdExtractor` were all connector-minted lambdas in the job graph.

## Decision

**Nothing the connector mints on the user's behalf enters the job graph as a lambda.** Three shapes
satisfy it, and which one applies is a property of the value, not a preference:

1. **A named class** when the value carries state — `FixedDestinationResolver`,
   `FixedCdcTableOptionsProvider`, `FixedTableCreateOptionsProvider`, `MetadataColumnTaskId`.
2. **An enum singleton** when it carries none — `CdcChangeTypeProviders.UpsertOnly`, as
   `FailureHandlers.FailJob` already did. An enum is bound back by its own constant name.
3. **`transient` plus `readObject`** when the value is a *tree* of lambdas derived from something
   already serializable — `CellValueCodec`'s type-carrying codecs, `RowDataJsonRenderer`, and
   `GenericRecordToRowDataConverter`, which reached this answer first. Resolution stays eager in
   the constructor as well as in `readObject`, so an unsupported type is still rejected where it
   was before rather than at the first record.

**A lambda the user passes in stays theirs.** `destinationResolver(e -> ...)`, a
`CdcChangeTypeProvider`, an `AdditionalFieldValueProvider`: the connector does not wrap, copy or
reject them. The rule is about what this project mints without being asked.

**Each family carries a guard test** that serializes the job-graph object, asserts the bytes hold
no `SerializedLambda`, and then asserts behaviour through a restored copy — absence alone would
pass on a rebuild that produced nothing. Nine mutations, one per converted site, were each caught
by the guard that claims it.

## Consequences

The serialized form of the affected objects changed, so a job graph written by an earlier build
does not restore against this one. That is acceptable before 1.0.0 and is the last release in which
it is: after the tag, the same change would need the upgrade path, not just the fix.

Two limits this rule does not claim to cover.

- **Flink's own lambdas.** A produced `TypeInformation` carries Flink's `RowData` field getters,
  which are lambdas this project neither mints nor controls. Where an object embeds them — the
  Pub/Sub and Spanner table schemas — the guard asserts the property that keeps *our* converters
  out instead: the schema holds `ReadableMetadata`/`WritableMetadata` **enum constants**, so only
  their names serialize. Hoisting those converters into a field would reintroduce the hazard, and
  that is what the guard is watching.
- **The compiler is not pinned.** These names are javac's; Eclipse JDT names lambdas `lambda$0`,
  `lambda$1` — a per-class counter, under which even isolated lambdas are index-fragile. Released
  artifacts are javac's, so the conclusion holds for them, but a measurement taken from an
  IDE-built `target/classes` would invert it. Check what compiled a class before reading its names.

The finding's own priority moved the other way: the five sites [#822] named are insertion-stable,
so converting them is consistency and independence from a compiler detail, not a fix. The codec
families it set aside were the defect.

[#822]: https://github.com/flink-gcp/flink-connector-gcp/issues/822
