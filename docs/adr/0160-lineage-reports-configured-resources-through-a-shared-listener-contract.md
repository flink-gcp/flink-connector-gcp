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
- Date: 2026-09-06; revised by [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271) (2026-09-06); Spanner adoption (2026-09-06); BigQuery, Cloud Tasks and Bigtable adoptions (2026-09-07); Firestore adoption (2026-09-27; source 2026-09-30; Datastore-mode source 2026-10-03; Table sink and scan 2026-10-04; Datastore-mode Table sink and scan 2026-10-10); Table adapters moved to base by [#1635](https://github.com/flink-gcp/flink-connector-gcp/issues/1635) (2026-10-04); runtime metadata shared by [#1660](https://github.com/flink-gcp/flink-connector-gcp/issues/1660) (2026-10-04)
- Issues: [#1269](https://github.com/flink-gcp/flink-connector-gcp/issues/1269), [#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274), [#354](https://github.com/flink-gcp/flink-connector-gcp/issues/354), [#1270](https://github.com/flink-gcp/flink-connector-gcp/issues/1270), [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271), [#1272](https://github.com/flink-gcp/flink-connector-gcp/issues/1272), [#1273](https://github.com/flink-gcp/flink-connector-gcp/issues/1273), [#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540), [#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541), [#1543](https://github.com/flink-gcp/flink-connector-gcp/issues/1543), [#1635](https://github.com/flink-gcp/flink-connector-gcp/issues/1635), [#1660](https://github.com/flink-gcp/flink-connector-gcp/issues/1660)
- Modules: base, test-utils, bigquery, pubsub, cloudtasks, bigtable, spanner, firestore, all SQL connector artifacts
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
| `firestore-collection-group` | `firestore://{project}/{database}` | `{collectionGroup}` | project, database, collectionGroup |
| `firestore-collection` | `firestore://{project}/{database}` | `{collection}`, relative to the database | project, database, collection |
| `datastore-kind` | `datastore://{project}/{database}`, then `/{namespace}` outside the default namespace | `{kind}` | project, database, optional namespace, kind |

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

A connector whose runtime Source or Sink does not carry the logical name itself wraps it in `TableLineageSource` or `TableLineageSink` from `base.lineage.internal`.
Each takes the delegate, the logical name, the namespace and the resource list, and nothing connector-specific; the connector's own table-lineage value still builds the identity through `LineageIdentifiers`.
The source adapter forwards reader creation, enumerator creation and restoration, split and checkpoint serializers, and the delegate's boundedness.
It reports the produced type the table layer supplies.
On Flink 2.x it also forwards `Source.declareWatermarks()` through a package-private `CrossVersionSource` in the per-major roots.
The previous connector wrappers inherited the empty default, so a delegate declaring generalized watermarks would lose those declarations; emitting one then fails Flink's declaration check.
The current Spanner and Firestore sources do not declare them, but the shared adapter preserves them for any delegate that does.
Flink 1.20 has neither this method nor its `@Experimental` return type, `WatermarkDeclaration`; the Flink 2.x seam carries that required SPI type without exposing it to 1.20 compilation.
If the declaration API changes, adapt the per-major bridge before admitting sources that emit generalized watermarks.
The sink adapter forwards `createWriter` only.
It therefore refuses a delegate implementing `SupportsWriterState`, `SupportsCommitter` or `SupportsPreWriteTopology`, whose state, commits or pre-write topology it would otherwise drop from the job without an error.
The `@Experimental` pre-write marker is used only to reject such a delegate, not to implement a topology.
The marker has the same interface in Flink 1.20.4 and 2.2.1; if it moves, the rejection check must follow the supported Flink API before admitting delegates.
Spanner and Firestore wrap their Table runtimes this way.
BigQuery, Bigtable, Pub/Sub and Cloud Tasks instead keep `LineageMetadata` in their own runtime objects, retaining their existing Source/Sink interfaces and execution capabilities.

The sink adapter is why base has per-major production source roots.
Flink 1.20 declares the deprecated `createWriter(Sink.InitContext)` abstract, and Flink 2.x removed that type, so one shared-source class cannot implement `Sink` on both lines.
Measured with the adapter implementing `Sink` directly: Flink 2.2.1 compiles it, and Flink 1.20.4 fails with "does not override abstract method createWriter(Sink.InitContext)".
Base therefore carries its own `CrossVersionSink` in `src/main/java-flink1` and `java-flink2`, package-private beside the adapter, as ADR-0054 allows for a compat file with one caller.
The root POM already adds those roots to every module, so the build needed no change.
Two alternatives were declined.
Keeping the sink adapter in each connector module and moving only the source adapter would have left a copy per module, and the Datastore-mode Table API ([#1545](https://github.com/flink-gcp/flink-connector-gcp/issues/1545)) would have added another.
Making the base seam public for connector sinks to share is a separate consolidation that this change does not need.

### Runtime metadata selection

The follow-up [#1660](https://github.com/flink-gcp/flink-connector-gcp/issues/1660) shares the logical-name storage and DataStream/Table selection left in the concrete runtimes after the Table adapters moved to base.
The inventory before this refinement was:

| Consumer | Runtime objects storing a nullable logical name | Selection sites |
| --- | --- | --- |
| Pub/Sub | StreamingPull source and publisher sink (2) | Each runtime (2) |
| Cloud Tasks | Eager and staged CreateTask sinks (2) | Each runtime (2) |
| Bigtable | Scan and Change Streams sources; MutateRows, conditional, read-modify-write and staged sinks (6) | `BigtableLineage.source` and `sink` (2) |
| BigQuery | Storage Read source; default-stream, buffered-stream and FILE_LOADS sinks (4) | Each runtime (4) |
| Base Table adapters | Source and sink adapters (2, always logical) | Table assembly only |

`base.lineage.internal.LineageMetadata` is one immutable, serializable, `@Internal` value holding the optional logical name.
Its `source` and `sink` methods select the corresponding existing `Lineage` helper, replacing the ten selection sites with two methods.
The fourteen connector runtimes and both Table adapters retain this value, sharing the logical-name representation across both integration styles.
The existing constructors and Table-copy entry points still accept their current arguments; public builders gain no option.

Each inspection supplies the connector's namespace and resource identifiers, and a source supplies its actual boundedness.
`BigtableLineage` still constructs Bigtable identities and inspects fixed resolvers; the other connectors retain their existing identity construction too.
Unknown Bigtable and Cloud Tasks destinations retain their existing Table namespace fallback, while unknown DataStream resources still produce no dataset.
The shared value stores no resource list, vertex, resolver or client, so identity discovery, deferred resolution and record routing remain in their current layers.
Vertices continue to be assembled on inspection by `Lineage`, with its ordering and immutable physical-resource facets.

The concrete runtimes continue to expose their existing writer state, committers and topology interfaces directly to Flink.
The base adapters retain their delegation contracts, including produced types, boundedness and Flink 2.x generalized watermark declarations.
`LineageMetadataTest` checks both selections and boundedness before and after serialization, unknown resources, ordering, duplicate removal, per-inspection namespaces and resource snapshots.
Connector lineage tests retain factory, graph and serialization coverage; capability assertions cover BigQuery buffered-stream and FILE_LOADS sinks and staged Cloud Tasks and Bigtable sinks.

A static selection helper was declined because it would leave the nullable logical-name representation in each runtime and outside the shared Table adapters.
A Source/Sink hierarchy would couple metadata to unrelated execution capabilities, while the two-method value requires no runtime inheritance or delegation.
Wrapping all sinks in the writer-only Table adapter was declined because it refuses stateful, committing and pre-write-topology delegates.
Moving resource construction or namespaces into the value was declined because those identities and unknown-resource fallbacks belong to each connector.

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
`TableLineageAdaptersTest` checks argument and result identity, exception propagation, boundedness, produced type, immutable resource snapshots, serialization and logical datasets with empty resources, on both Flink lines.
It also checks the sink adapter's refusal of writer state, commits and a pre-write topology.
The Flink 2.x `TableLineageWatermarksTest` checks nonempty declaration-set identity and forwarding after serialization.
`TableLineageTest` checks catalog names plus the complete facet, including an unadapted multi-dataset control that loses the facet, and a lookup provider whose lineage method is not called.
The fixture imports the public `JobCreatedEvent`; it does not manufacture an event or call the capture factory directly.
The required clean reactor, 1.20, binary compatibility and packaging measurements are recorded against the PR's tested commits.

### Cloud Tasks adoption

[#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274) adopts the shared contract in `CloudTasksCreateTaskSink`.
The effective `FixedDestinationResolver` supplies the queue through its accessor; extraction never evaluates a user resolver.
The Table factory carries its catalog identifier through `CloudTasksDynamicSink` into the runtime sink's `LineageMetadata`, which selects `Lineage.tableSink` with the queue's namespace and complete physical-resource facet.
The internal sink stores only the optional logical name alongside its existing serializable configuration and constructs the vertex on demand.
No builder option or manual dataset declaration API is added.

Cloud Tasks tests cover the actual builder result, both destination setter orders, target and task-naming independence, serialization without connector-minted lambdas, and extraction without user-code or client-factory calls.
Flink 2.x graph and planner tests check the logical/physical distinction, and an emulator-backed job verifies the production sink's metadata reaches the configured listener while its task is dispatched.
These tests do not claim real-service dispatch acceptance or add lineage for handler execution.

## BigQuery adoption

[Issue #1270](https://github.com/flink-gcp/flink-connector-gcp/issues/1270) adopts the shared contract merged in [PR #1276](https://github.com/flink-gcp/flink-connector-gcp/pull/1276).
The public builder returns a lineage provider for the Storage Read source and each of the three sink write methods.
The source uses its configured `TableDestination`, including with view materialization; arbitrary queries have no known physical input.
Sinks inspect the effective `FixedDestinationResolver.getDestination()` without evaluating any resolver.
The same identity covers default-stream CDC, buffered streams and FILE_LOADS.
BigQuery resource components retain their configured syntax, including project qualification and table decorators; no metadata lookup or new identifier parser is introduced.

The SQL factory carries its catalog identifier through the Dynamic Source/Sink's copy and identity methods.
The runtime Source/Sink holds its optional logical name in `LineageMetadata`, which selects `Lineage.tableSource` or `tableSink` on inspection.
The internal `BigQueryLineageSink` handoff returns the same concrete sink class, preserving writer-state, committer and pre-commit topology interfaces.
It is not a public builder setting or a manual physical-dataset declaration API.
SQL query sources retain a logical dataset with an empty physical-resource list.

Connector tests inspect the actual public-builder outputs before and after Java serialization, with forbidden callbacks and absent credential files.
Flink 2.x graph/planner tests cover table, view, query and projected/filtered paths, all three write methods, and default-stream CDC.
The common prerequisite's listener and classloader measurements remain the shared evidence; BigQuery adoption does not claim additional service semantics or require real-GCP execution.

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
Their shared `LineageMetadata` selects `Lineage.tableSource` and `tableSink`, retaining all physical identifiers in one facet.
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
The base Table adapters delegate the existing runtime Source/Sink operations and expose `Lineage.tableSource` or `Lineage.tableSink`.
This keeps deferred index/filter resolution independent of lineage extraction, and keeps the fixed sink table outside the public serializer SPI.
No lineage setter is added to a public builder.
Legacy native table names remain complete names; a PostgreSQL default schema is prefixed only when the Table path has an unqualified native name.
DataStream batch configuration has no dialect, so it does not infer that default.

`SpannerLineageTest` inspects actual builder results with throwing runtime hooks and serialization round trips.
`SpannerTableLineageTest` covers factory runtime objects, copies, quoted/default/named schemas, deferred pushdown, and table-plus-stream provenance.
`SpannerLineageGraphTest` exercises Flink 2.x DataStream extraction and the actual Spanner SQL factory, including the unsupported lookup path.
The shared listener-delivery and class-loader evidence above remains the foundation; the Spanner PR records its focused version and packaging measurements.

## Bigtable adoption

The existing sources and three sink families adopt lineage in [#1272](https://github.com/flink-gcp/flink-connector-gcp/issues/1272), using `LineageIdentifiers.bigtableTable` and the DataStream/Table vertex helpers from [PR #1276](https://github.com/flink-gcp/flink-connector-gcp/pull/1276).
Each source reads its configured table and its own boundedness.
The sinks inspect the effective `FixedDestinationResolver`, including the resolver retained by `SingleRowRequestConfig`, without invoking it.
The writer's per-record resolution path remains independent of this metadata inspection.

The Table factory supplies its catalog identifier, retained by copies of all three Dynamic Table implementations and the concrete runtime objects.
The MutateRows copy also retains the aggregate mode's initial destination and expected families, so adding lineage does not remove startup type validation.
All six write modes keep their existing `SinkV2Provider` routing, and both scan families keep `SourceProvider`.
The Change Streams envelope and selected-cell modes identify only the data table; coordinator state stays in Flink checkpoints under ADR-0097.
Lookup joins and result-emitting Async I/O/SQL functions remain outside this Source/Sink contract.

## Firestore adoption

The Firestore sink ([#1540](https://github.com/flink-gcp/flink-connector-gcp/issues/1540)) implements `LineageVertexProvider` through `Lineage.sink` with an empty resource list.
Its configuration names a database, and each `FirestoreWrite` names its own document path, so the collections a job writes are the serializer's and extraction never invokes it; this is the Spanner mutations-sink case.
The bounded source ([#1541](https://github.com/flink-gcp/flink-connector-gcp/issues/1541)) is the first configuration that names a fixed resource: a collection-group scan names the group, so `firestore-collection-group` arrives with it and the source reports it through `Lineage.source`.
The group is every collection with that id at any depth of one database, which is why the kind is a group rather than a collection path; the Table API ([#1544](https://github.com/flink-gcp/flink-connector-gcp/issues/1544)) adds a collection kind if its sink names one.
A source built from a query factory reports no dataset, for the sink's reason: the query's collections are the factory's, and extraction never calls it.
The Table sink ([#1607](https://github.com/flink-gcp/flink-connector-gcp/issues/1607)) does name one: its `collection` option is a collection path, so it adds `firestore-collection`, named by that path relative to the database, and reports it through `Lineage.tableSink` under the table's catalog identifier, through the same base Table adapter as Spanner. The kind names the documents directly in the collection, not those of its subcollections, which is what the sink writes. `FirestoreDynamicTableFactoryTest` reads the dataset from the runtime sink the factory builds. The Table scan ([#1608](https://github.com/flink-gcp/flink-connector-gcp/issues/1608)) reports through `Lineage.tableSource` the same `firestore-collection`, or under `scan.collection-group` a `firestore-collection-group` named by the collection id; `FirestoreDynamicTableSourceFactoryTest` reads both from the runtime source.
`FirestoreSourceLineageTest` covers both shapes before and after a serialization round trip, and `FirestoreLineageGraphTest` extracts the scan from a Flink 2.x graph.
`FirestoreBulkWriterSinkTest` inspects the builder result before and after a serialization round trip with an unreadable key-file path, and `FirestoreLineageGraphTest` exercises Flink 2.x DataStream extraction.

The Datastore-mode sink ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) reports an empty resource list for the Firestore sink's reason: each mutation's key names its kind and namespace.
The Datastore-mode bounded source ([#1543](https://github.com/flink-gcp/flink-connector-gcp/issues/1543)) brings `datastore-kind`, reported when the configuration names the kind: a `kind(...)`, or a `query(...)` naming exactly one kind.
The namespace is part of the canonical namespace rather than of the name, because a kind may contain `/` and a namespace may not, so the two cannot collide; the default namespace, which the Datastore API spells as the empty string, is absent from both the canonical namespace and the identity.
The default database is spelled `(default)`, as for `firestore-collection-group`, although the Datastore API names it with an empty id.
A GQL query, which the service parses only when the read is planned, and a query naming no kind or several report no dataset.
`DatastoreSourceLineageTest` covers these shapes before and after a serialization round trip, and `DatastoreLineageGraphTest` extracts a kind in a namespace from a Flink 2.x graph.
The Datastore-mode Table sink ([#1651](https://github.com/flink-gcp/flink-connector-gcp/issues/1651)) reports the same `datastore-kind` for its `kind` and `namespace` options, through the base `TableLineageSink` under the table's catalog identifier and with the default database spelled `(default)` as the source spells it; `DatastoreDynamicTableFactoryTest` reads the dataset from the runtime sink the factory builds.
The Datastore-mode Table scan ([#1652](https://github.com/flink-gcp/flink-connector-gcp/issues/1652)) reports the same resource through `Lineage.tableSource`; `DatastoreDynamicTableSourceFactoryTest` reads it from the runtime source and compares it with the DataStream source's own.
