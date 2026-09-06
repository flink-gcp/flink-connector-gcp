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
- Date: 2026-09-06
- Issues: [#1269](https://github.com/flink-gcp/flink-connector-gcp/issues/1269), [#354](https://github.com/flink-gcp/flink-connector-gcp/issues/354)
- Modules: base, test-utils, all SQL connector artifacts
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

## Alternatives and limits

Flink's internal `Default*Lineage*` implementations and 2.3-only APIs are not production dependencies.
Reflection into relocated facet implementations would make the listener contract connector-specific; two shared API values keep it typed.
Leaving the entire base package unrelocated would reintroduce the mixed-connector dependency collision ADR-0015 prevents.

Runtime resource discovery, arbitrary SQL parsing, a user declaration API, column/schema lineage and OpenLineage-specific adapters are excluded.
Lookup joins and Bigtable Async I/O do not use the FLIP-314 Source/Sink extraction path.
An unmodified OpenLineage listener is not assumed to understand this custom facet.
Connector-specific runtime support is tracked in [#1270](https://github.com/flink-gcp/flink-connector-gcp/issues/1270), [#1271](https://github.com/flink-gcp/flink-connector-gcp/issues/1271), [#1272](https://github.com/flink-gcp/flink-connector-gcp/issues/1272), [#1273](https://github.com/flink-gcp/flink-connector-gcp/issues/1273), and [#1274](https://github.com/flink-gcp/flink-connector-gcp/issues/1274).
Each adopts the merged shared PR/ADR and adds extraction tests against its actual builder-returned Source/Sink objects.
