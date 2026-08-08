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

# ADR-0055: Connector packages follow one skeleton, and a layer exists only where a sibling can arrive

- Status: Accepted
- Date: 2026-07-20 ([#63], applied to BigQuery first); the layer and naming rules settled
  2026-07-26 ([#119], [#121], [#125])
- Issues: [#63], [#119], [#121], [#123], [#125], [#280]
- Modules: all connector modules
- Current behavior: root `CLAUDE.md` § Package layout convention (the imperative form)

## Context

[#63] reorganised the BigQuery sink's packages before v0.1.0; Pub/Sub, Cloud Tasks and every
later module follow the same skeleton under
`io.github.flink.gcp.connector.<product>`. The philosophy throughout: public API at a
package's root, implementation in subpackages beneath it. Test sources mirror the main-tree
packages — which is also what lets package-private coupling stay package-private.

## Decision

**The skeleton:**

- `sink` — public sink API only: the facade + builder, write-method enum, shared
  options/enums, destination types, and the `@Internal` types shared by every write method
  (the sink config, the fixed-destination resolver; retry machinery lives in `base.retry`
  since [#61] extracted it — ADR-0039).
- `sink.<writepath>` — one subpackage per write-path **family**, which may host several write
  methods (BigQuery: `sink.storage` holds the Storage Write API family, `sink.fileloads`
  holds FILE_LOADS). The package root holds the Sink classes, the family's public options
  objects and committable contracts; internal stages follow the Flink FileSink precedent with
  `.writer`, `.committer` and post-commit-topology subpackages (`.loadjob` here, FileSink's
  `.compactor`) as the topology requires — a family without 2PC simply has no `.committer`
  package.
- `sink.tables` — shared table-metadata layer consumed by every write method: the
  `TableAdmin` SPI and its REST implementation, schema snapshot/unifier, REST↔Storage schema
  converters.
- `sink.serializer` — the record-conversion SPI alone, with `sink.serializer.<format>`
  beneath it per input format (below, [#125]).
- `sink.failure` — in BigQuery, the connector-specific failure type only (`FailedRow`); the
  handler/DLQ SPI itself is the shared `base.failure` package since [#37] extracted it
  ([#205], ADR-0036). The package's original purpose ("keep the extraction cheap") is
  discharged; it stays in place because moving `FailedRow` would churn about a dozen files
  (10 importers plus the class and its test, measured on PR
  [#213](https://github.com/laughingman7743/flink-connector-gcp/pull/213)) for no behavioural
  gain. Later connectors put their failure type at the `sink` root instead — a one-class
  `sink.failure` fails the layer test below.
- `source` / `table` — sources and Table API, same philosophy. The family rule applies here
  too, and `source.streamingpull` **keeps** its layer: the sibling Cloud Tasks cannot have is
  real there, since a unary-`Pull` source is a live alternative — weighed, and rejected on
  trade-offs the connector documentation records as cutting both ways.
- The **module root** holds what belongs to the connector as a whole rather than to one
  direction: the `@Internal` `<Product>MetricNames` inventory every connector carries
  ([#280], ADR-0038) and, in Bigtable, the `@PublicEvolving` `TableDestination`.
  `PubSubMetricNames` is why the placement is a rule rather than a preference: its names span
  `sink.writer`, `source.streamingpull.reader` and `.enumerator`, so the module root is the
  only package that can hold one inventory. What the two residents share is the *scope*, not
  the visibility — a names class is `public` because Java has no module-internal access and
  its sub-packages must import it, which is what the `@Internal` annotation is there to say.

**One family, with no second one in prospect, means no layer** ([#119]): the module goes
straight to `sink` + `sink.writer` (`sink.committer`, … as the topology requires). Decided
where Cloud Tasks' `sink.createtask` was named after the `CreateTask` RPC rather than after a
design and no sibling can arrive at all — `BatchCreateTasks` and `BufferTask` are REST-only
and absent from the Java client. Pub/Sub's `sink.publisher` went with it so the two
single-family modules stay alike. Adding the layer back is what a second family costs, and it
is a mechanical move — the two layers [#119] removed held nothing public, though a family
layer generally may (BigQuery's `BufferedStreamOptions` and `FileLoadsOptions` are
`@PublicEvolving` in theirs). The rule is a **test, not a count**. This ADR is the canonical
record behind the "[#119] layer test" that ADR-0005, ADR-0009, ADR-0039, ADR-0041 and
ADR-0049 cite in passing.

**The family layer is spelled the way Google spells it in code, with no `api` suffix**
([#121]): `sink.storage` mirrors `com.google.cloud.bigquery.storage.v1` and the
`google-cloud-bigquerystorage` artifact, as `sink.fileloads` already drops the Jobs-API word.
The public `WriteMethod.STORAGE_API_*` constants keep the product's documented name on
purpose — the package names an implementation family to maintainers, the enum names a feature
to users. Inside a family, an SPI's real implementation is named after **the SDK resource it
owns** (the one its `close()` releases) — `WriteClientBufferedStreamService` over
`BigQueryWriteClient`, as `StreamWriterRowAppenderFactory` is over `StreamWriter`. Neither
`Storage*` nor the repository's usual `Default*` works there: the SPI is equally a Storage
Write API type, so that prefix distinguishes nothing, and *default stream* is BigQuery's
implicit always-on stream, named throughout that package and the opposite of a buffered one.

**Serializer input formats are subpackages of the SPI, and they never import each other**
([#125]): `sink.serializer.<format>` per input format (`.proto`, `.avro`, `.json`), each
holding its facade, its `@PublicEvolving` options object and the `@Internal` types behind
them — a public-API layer, not merely an internals split, mirroring how the family packages
keep their options objects. The trigger was [#123] taking the flat package to ten classes with
the names already doing the package's job (`Proto*` ×4, `Avro*` ×4). It **passes** the [#119]
rule rather than contradicting it: two formats existed while the issue that introduced the
second already planned a third — the exact opposite of Cloud Tasks' `sink.createtask`. Every
package-private coupling stays inside one format, so nothing had to widen to `public`; that
holds only because the tests move with their format. The format packages must not import each
other: the three Avro→proto javadoc references are **fully-qualified `{@link}`s rather than
imports**, so the independence is a property of the import graph and not just of the call
graph. Spotless does keep a javadoc-only import (measured, not assumed), so the short form
was available and was declined for that reason.

**A new top-level class in a module's `sink` root needs a reason to be public API**;
implementation types belong in the subpackages. The one standing exception is a single-family
module's `@Internal` `Sink` class (`CloudTasksCreateTaskSink`, `PubSubPublisherSink`), which
sits beside its facade because there is no family package left to hold it. Every module's
`sink` root also carries the `@Internal` `CrossVersionSink` seam in the per-major source
roots (ADR-0054): it must be importable by every sink in the module, and its two variants
share one FQCN on purpose.

## Alternatives declined

- **Renaming `BigQueryProtoSerializer` when the format packages split** ([#125]): the split
  makes it *read* as the proto family's SPI when it means "the wire form is protobuf,
  whatever the input was" — but its javadoc says so, it keeps its place at the `sink.serializer`
  root, and renaming would touch the sink core, the writers and ~20 tests for no behavioural
  gain.
- **Moving `FailedRow` out of BigQuery's discharged `sink.failure`** — the ~dozen-file churn
  measured on PR [#213](https://github.com/laughingman7743/flink-connector-gcp/pull/213),
  above.

[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#61]: https://github.com/laughingman7743/flink-connector-gcp/issues/61
[#63]: https://github.com/laughingman7743/flink-connector-gcp/issues/63
[#119]: https://github.com/laughingman7743/flink-connector-gcp/issues/119
[#121]: https://github.com/laughingman7743/flink-connector-gcp/issues/121
[#123]: https://github.com/laughingman7743/flink-connector-gcp/issues/123
[#125]: https://github.com/laughingman7743/flink-connector-gcp/issues/125
[#205]: https://github.com/laughingman7743/flink-connector-gcp/issues/205
[#280]: https://github.com/laughingman7743/flink-connector-gcp/issues/280
