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

# ADR-0160: Lineage reports configured resources through a shared listener contract

- Status: Accepted
- Date: 2026-09-06; revised by [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271) (2026-09-06)
- Issues: [#1269](https://github.com/flink-gcp/flink-connector-gcp/issues/1269), [#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274), [#354](https://github.com/flink-gcp/flink-connector-gcp/issues/354), [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271)
- Modules: base, test-utils, pubsub, cloudtasks, all SQL connector artifacts
- Partially supersedes: ADR-0015's relocation rule for two listener-facing classes; ADR-0050's absence of compatibility source roots in test-utils
- Current behavior: [Lineage](../content/docs/connectors/lineage.md)

## Context

The five connector implementations need one metadata contract before adopting FLIP-314.
Flink 1.20.4 and 2.2.1 declare the same public evolving lineage provider, dataset, facet and vertex interfaces.
Their owning artifact changes from `flink-streaming-java` in 1.20 to `flink-runtime` in 2.2.
Interface availability alone does not imply framework extraction: 1.20 lacks the automatic Source/Sink extraction and `JobCreatedEvent` integration of 2.2.1.

Flink 2.2.1's `TableLineageUtils` chooses a connector dataset only when there is exactly one, or a dataset's name equals the catalog identifier.
`TableLineageDatasetImpl` then uses the catalog identifier as its name and copies the selected dataset's namespace and facets.
Handing the planner several physical datasets therefore loses information when none has the logical name.

The base package is relocated separately in every SQL connector jar under ADR-0015.
A listener could not cast five differently relocated facets to one shared API type.
The configured listener must be able to consume physical resource identities for SQL deployments too.

## Decision

### Metadata inspection

`getLineageVertex()` is pure inspection of immutable, already configured metadata.
It must not resolve credentials, open clients, issue RPCs, consult the clock, inspect records, call a serializer, or evaluate a resolver.
Store serializable resource values in Source/Sink configuration and construct vertices when requested; the vertex objects themselves are not job configuration.
A source returns a `SourceLineageVertex` with its own boundedness; a sink returns a `LineageVertex` that is not a source vertex.
An unknown DataStream resource set produces an empty dataset list in a non-null vertex.

`base.lineage.ResourceIdentifier` and `PhysicalResourceFacet` are immutable, serializable, `@PublicEvolving` read-only listener values.
Their constructors are `@Internal` and public solely to cross the relocated-helper boundary.
No connector builder gains a lineage setter, and these constructors are not a supported user dataset-declaration API.
The values depend only on JDK types, Flink annotations and the public Flink facet interface.
All other construction and vertex implementation code lives in `base.lineage.internal` and is `@Internal`.
This shared prerequisite is an explicit exception to requiring already-implemented multiple consumers: the five linked connector issues consume this one contract.

The facet key and `name()` are `gcp`.
Its `resources()` list orders identifiers lexicographically by namespace, name, kind, then ordered identity key/value pairs, and removes only equal identifiers.
The identifier's `identity()` map is ordered by key; all returned collections are immutable snapshots.
Identity participates in equality so rendered names that obscure configured quoting or component boundaries do not collapse distinct resource information.
Distinct configured identities may therefore produce DataStream datasets with the same canonical namespace/name and different physical-resource facets.
Factories accept only resource components, not a connector option map.
Credentials, SQL, task URLs, payload schemas and record contents are excluded.

### Names and kinds

| Kind | Namespace | Name | Identity keys |
| --- | --- | --- | --- |
| `bigquery-table` | `bigquery` | `{project}.{dataset}.{table}` | project, dataset, table |
| `pubsub-topic` | `pubsub` | `topic:{project}:{topic}` | project, topic |
| `pubsub-subscription` | `pubsub` | `subscription:{project}:{subscription}` | project, subscription |
| `bigtable-table` | `bigtable://{project}/{instance}` | `{table}` | project, instance, table |
| `spanner-table` | `spanner://{project}:{instance}` | `{database}.{schema}.{table}` | project, instance, database, optional schema, table |
| `spanner-change-stream` | `spanner://{project}:{instance}` | `{database}/changeStreams/{stream}` | project, instance, database, stream |
| `cloudtasks-queue` | `cloudtasks://{project}/{location}` | `{queue}` | project, location, queue |

BigQuery, Pub/Sub and Spanner table names follow [OpenLineage's naming convention](https://openlineage.io/docs/spec/naming/).
The other forms are project conventions.
`bigquery-table` also covers an explicitly named view: no RPC discovers the resource's service-side type.
An absent Spanner schema segment is omitted from the canonical name.
The Spanner factory accepts parsed schema/table components separately from the configured schema/table syntax retained in the identity.
Existing connector parsers decide identifier semantics; lineage adds no service-name grammar, naive splitting, or case folding.
An absent configured schema remains absent from the identity, even when the existing parser resolves a default schema for the canonical name.
Absence here means an omitted option, represented by null; the existing Spanner parser rejects an explicitly blank schema option.

### Internal Table adapter

`Lineage.source` and `sink` produce one dataset per physical resource.
`Lineage.tableSource` and `tableSink` instead take the known logical name, connector namespace and complete resource list, and produce one dataset containing the full `gcp` facet.
The caller supplies its catalog identifier and canonical connector namespace; Flink owns the final catalog name.
A known logical table with unknown physical resources still has a logical dataset, with an empty facet resource list and no invented physical placeholder.
The adapter changes metadata only, not runtime provider interfaces, record routing, pushdown, checkpointing or source boundedness.
A Table CDC source may carry its table and configured stream provenance together in this list without claiming all watched tables.

### Dependencies and packaging

The root manages `flink-runtime` at `${flink.version}`.
Base and test-utils declare runtime and streaming-java as `provided`; each later connector adopter declares runtime directly when it imports lineage interfaces.
The base module's planner and client dependencies are test-scoped.
Test-utils keeps every dependency provided and depends on no base class.
Its reusable Flink 2.x listener capture lives in `src/main/java-flink2`; base's graph/planner tests live in `src/test/java-flink2`.
The existing per-major source selection also selects test sources, so 2.x-only event imports never enter a 1.20 compilation.

Only `PhysicalResourceFacet` and `ResourceIdentifier` are exempt from base relocation in the five SQL jars.
Internal helpers remain independently relocated, and no Flink runtime or streaming classes are bundled, relocated or otherwise.
Packaging exemptions name these exact class entries rather than exempting the base package.
The public constructors avoid package-private access between an unrelocated value and a relocated helper.

Matching class names alone do not unify different class loaders.
A listener and these two API classes must be loaded together; a parent-loaded listener serving child-loaded job jars needs these API names in `classloader.parent-first-patterns.additional`.
The packaging tests exercise the boundary with a child loader for relocated helpers and parent-loaded API values, and verify the SQL-jar copies resolve to the same listener types.
Ordinary packaging tests inspect only their current artifact; the simultaneous five-jar measurement supplies an explicit manifest of the clean build artifacts.
It never discovers sibling targets that a scoped reactor may have left stale.
The combined check runs explicitly after a clean full build; ordinary verification and CI exercise each module separately.
This establishes the current release's shared API, not arbitrary compatibility between future incompatible versions.

### Compatibility and evidence

| Flink | Contract |
| --- | --- |
| 1.20.4 | Build/source compatibility and direct metadata inspection; no automatic FLIP-314 listener delivery |
| 2.2.1 | Build floor; Source/Sink graph extraction, configured listener delivery and Table planner behavior tested |
| 2.3.0 | The 2.2.1-built artifacts and test classes rerun without recompilation under the binary compatibility recipe |

`LineageTest` covers immutable values, equality, order, duplicates, serialization, names, empty vertices and boundedness.
`LineageClassLoaderTest` uses Flink's actual user-code loader to verify the documented parent-first configuration and a negative control in which identical API class names cannot be cast across loaders.
`LineageGraphITCase` serializes fake Source/Sink configurations, checks graph extraction before runtime creation, then submits an empty local job and captures Flink's real `JobCreatedEvent` through the configured listener factory.
`TableLineageTest` checks catalog names plus the complete facet, including an unadapted multi-dataset control that loses the facet, and a lookup provider whose lineage method is not called.
The fixture imports the public `JobCreatedEvent`; it does not manufacture an event or call the capture factory directly.
The required clean reactor, 1.20, binary compatibility and packaging measurements are recorded against the PR's tested commits.

### Cloud Tasks adoption

[#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274) adopts the shared contract in `CloudTasksCreateTaskSink`.
The effective `FixedDestinationResolver` supplies the queue through its accessor; extraction never evaluates a user resolver.
The Table factory carries its catalog identifier through `CloudTasksDynamicSink` into the runtime sink, which uses `Lineage.tableSink` with the queue's namespace and complete physical-resource facet.
The internal sink stores only the optional logical name alongside its existing serializable configuration and constructs the vertex on demand.
No builder option or manual dataset declaration API is added.

Cloud Tasks tests cover the actual builder result, both destination setter orders, target and task-naming independence, serialization without connector-minted lambdas, and extraction without user-code or client-factory calls.
Flink 2.x graph and planner tests check the logical/physical distinction, and an emulator-backed job verifies the production sink's metadata reaches the configured listener while its task is dispatched.
These tests do not claim real-service dispatch acceptance or add lineage for handler execution.

## Alternatives and limits

Flink's internal `Default*Lineage*` implementations and 2.3-only APIs are not production dependencies.
Reflection into relocated facet implementations would make the listener contract connector-specific; two shared API values keep it typed.
Leaving the entire base package unrelocated would reintroduce the mixed-connector dependency collision ADR-0015 prevents.

Runtime resource discovery, arbitrary SQL parsing, a user declaration API, column/schema lineage and OpenLineage-specific adapters are excluded.
Lookup joins and Bigtable Async I/O do not use the FLIP-314 Source/Sink extraction path.
An unmodified OpenLineage listener is not assumed to understand this custom facet.
Connector-specific runtime support is tracked in [#1270](https://github.com/flink-gcp/flink-connector-gcp/issues/1270), [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271), [#1272](https://github.com/flink-gcp/flink-connector-gcp/issues/1272), [#1273](https://github.com/flink-gcp/flink-connector-gcp/issues/1273), and [#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274).
Each adopts the merged shared PR/ADR and adds extraction tests against its actual builder-returned Source/Sink objects.

## Pub/Sub adoption

The Pub/Sub source and publisher sink added by [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271) consume the shared helpers introduced by [PR #1276](https://github.com/flink-gcp/flink-connector-gcp/pull/1276).
They construct vertices on inspection from their existing resource values, without storing a vertex or introducing a serializable lambda.
Only the effective `FixedDestinationResolver` exposes a known sink topic; a dynamic resolver remains unknown.
The source reports subscriptions without consulting creation settings or discovering their backing topics.
The source builder continues to reject repeated subscriptions: metadata deduplication does not relax that assignment contract.

The Table factory supplies its catalog identifier, and internal Source/Sink copies keep that logical name alongside the same runtime configuration.
Their `Lineage.tableSource` and `tableSink` calls retain all physical identifiers in one facet.
The ordering-key runtime provider implements both `DataStreamSinkProvider` and `SinkV2Provider`.
On Flink 2.2.1 and 2.3.0, the planner obtains metadata through `createSink()` and prioritizes `consumeDataStream(...)` when building the routed topology.
The same sink object serves both paths; parallelism one still omits the keyed exchange.
`PubSubLineageGraphTest` exercises both provider paths, configured and inherited parallelism, and single/multiple subscriptions without starting the Pub/Sub runtime.
The direct metadata and serialization tests also compile on Flink 1.20.4; native extraction coverage stays in the Flink 2.x test source root.

## Spanner adoption

[Issue #1273](https://github.com/flink-gcp/flink-connector-gcp/issues/1273) adopts the shared API merged in [PR #1276](https://github.com/flink-gcp/flink-connector-gcp/pull/1276).
The builder-returned batch source, Change Streams source, and mutations sink implement `LineageVertexProvider` through `Lineage.source` and `Lineage.sink`.
Explicit native reads identify the base table; queries and generic mutation serializers establish no physical table set.
The DataStream Change Streams source identifies its configured stream, regardless of its regex record filters.

The Table factory captures its logical identifier, parsed table components, original option syntax, and configured stream provenance in immutable serializable metadata.
Named internal adapters delegate the existing runtime Source/Sink operations and expose `Lineage.tableSource` or `Lineage.tableSink`.
This keeps deferred index/filter resolution independent of lineage extraction, and keeps the fixed sink table outside the public serializer SPI.
No lineage setter is added to a public builder.
Legacy native table names remain complete names; a PostgreSQL default schema is prefixed only when the Table path has an unqualified native name.
DataStream batch configuration has no dialect, so it does not infer that default.

`SpannerLineageTest` inspects actual builder results with throwing runtime hooks and serialization round trips.
`SpannerTableLineageTest` covers factory runtime objects, copies, quoted/default/named schemas, deferred pushdown, and table-plus-stream provenance.
`SpannerLineageGraphTest` exercises Flink 2.x DataStream extraction and the actual Spanner SQL factory, including the unsupported lookup path.
The shared listener-delivery and class-loader evidence above remains the foundation; the Spanner PR records its focused version and packaging measurements.
