---
title: Lineage
type: docs
weight: 50
---

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

# Lineage

The shared lineage contract describes the physical resources known from a connector's configuration.
It does not discover resources at runtime or infer them by inspecting records.
Connector adoption is tracked separately for [BigQuery]({{< param BookRepo >}}/issues/1270), [Pub/Sub]({{< param BookRepo >}}/issues/1271), [Bigtable]({{< param BookRepo >}}/issues/1272), [Spanner]({{< param BookRepo >}}/issues/1273), and [Cloud Tasks]({{< param BookRepo >}}/issues/1274).
The common graph and listener tests establish the contract; they do not yet establish those connectors' individual coverage.

## Metadata and resource identities

A DataStream lineage vertex contains one dataset per configured physical resource.
An unknown physical resource set is an empty dataset list, not a dataset with a blank or guessed name.
Source vertices retain the source's boundedness.
Extraction performs no authentication, RPC, clock lookup, serializer call or resolver evaluation.

Each dataset has a `gcp` facet, exposed through `PhysicalResourceFacet` in the base module's Java API.
Its immutable `resources()` list contains `ResourceIdentifier` values with a resource kind, canonical namespace/name, and configured identity components.
These components retain information such as Spanner schema and table quoting that a rendered dataset name can obscure.
Distinct configured identities can share a canonical namespace/name and remain distinct DataStream entries so their facets preserve those details.
They exclude credentials, SQL text, task URLs, record contents and payload schemas.
The constructors are internal integration plumbing, not a supported manual dataset declaration API.

| Physical resource | Namespace | Name |
| --- | --- | --- |
| BigQuery table or explicitly named view | `bigquery` | `{project}.{dataset}.{table}` |
| Pub/Sub topic | `pubsub` | `topic:{project}:{topic}` |
| Pub/Sub subscription | `pubsub` | `subscription:{project}:{subscription}` |
| Bigtable table | `bigtable://{project}/{instance}` | `{table}` |
| Spanner table | `spanner://{project}:{instance}` | `{database}.{schema}.{table}`; omit an absent schema |
| Spanner Change Stream | `spanner://{project}:{instance}` | `{database}/changeStreams/{stream}` |
| Cloud Tasks queue | `cloudtasks://{project}/{location}` | `{queue}` |

BigQuery, Pub/Sub and Spanner table names follow [OpenLineage's naming convention](https://openlineage.io/docs/spec/naming/).
The Bigtable, Cloud Tasks and Change Stream names are project conventions.
A Change Stream identity names the stream, not the complete set of tables it watches.

## Table planner behavior

Flink owns SQL catalog identities.
The internal Table adapter supplies one logical dataset with its complete physical-resource list in the `gcp` facet.
Flink 2.2.1 replaces the dataset name with the SQL catalog identifier and retains the facet.
This avoids the planner's selection of at most one dataset from a connector's physical list.
A known logical table with unknown physical resources retains an empty resource list in its facet.
The adapter does not change record routing or provide a user-facing lineage option.

## Flink versions and listeners

| Flink version | Shared contract coverage |
| --- | --- |
| 1.20 | Source/build compatibility and direct metadata inspection; no automatic FLIP-314 listener delivery |
| 2.2 | Graph extraction, configured `JobCreatedEvent` listener delivery, and Table planner tests at the build floor |
| 2.3 | The floor-built artifacts tested without recompilation on the supported ceiling |

The pinned patch versions and the compatibility measurement are recorded in [ADR-0160]({{< param BookRepo >}}/blob/main/docs/adr/0160-lineage-reports-configured-resources-through-a-shared-listener-contract.md).
A custom listener must consume the `gcp` facet explicitly.
Passing it to Flink does not mean an unmodified OpenLineage listener understands it.

`PhysicalResourceFacet` and `ResourceIdentifier` retain the same package names in all SQL connector jars.
Load the listener and these API classes in the same class loader, for example from the cluster's `lib/` directory.
When a listener is parent-loaded and job jars are child-loaded, place the API classes on the parent classpath and add the following names to any existing additional parent-first patterns:

```yaml
classloader.parent-first-patterns.additional:
  - io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet
  - io.github.flink.gcp.connector.base.lineage.ResourceIdentifier
```

This shares the listener API while the SQL jars' internal helpers remain relocated per connector.
Use mutually compatible connector releases; the shared class names do not establish compatibility with a future incompatible API version.

## Coverage limits

The initial contract excludes runtime resource discovery, arbitrary SQL parsing, a user dataset-declaration API, column/schema lineage, and OpenLineage-specific adapters.
A dynamic destination without a known configured resource set contributes no physical dataset.
Lookup joins and Bigtable Async I/O functions are outside the FLIP-314 Source/Sink extraction path.
