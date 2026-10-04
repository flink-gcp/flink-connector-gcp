---
title: Table API Connectors
bookCollapseSection: true
weight: 20
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

# Table API Connectors

Use these pages for DDL, SQL type mappings, metadata columns, and planner-specific restrictions.
Each Table connector maps onto the DataStream connector with the same name rather than providing a
separate implementation.
The corresponding [DataStream connector]({{< relref "docs/connectors/datastream" >}}) page
documents runtime behavior and builder-only features.
The [connector overview]({{< relref "docs/connectors" >}}) explains how to choose between the two
APIs.

## Capability map

Pushdown describes bounded scans; the lookup column separately describes point-read behavior.
Metadata stays with the direction that reads or writes it.
A catalog lists a service's tables and derives each schema, in place of a hand-written `CREATE TABLE`.
`Not applicable` means that the capability does not fit the service's role; it is not an
implementation-status claim.

| Connector pages | Primary role | Scan and pushdown | Sink changelog | Lookup | CDC composition | Metadata | Catalog |
|---|---|---|---|---|---|---|---|
| BigQuery: [Quickstart]({{< relref "docs/quickstart/bigquery" >}}), Table examples [source]({{< relref "docs/examples/bigquery" >}}#table-source), [sink]({{< relref "docs/examples/bigquery" >}}#table-sink), and [CDC]({{< relref "docs/examples/bigquery" >}}#change-data-capture), Table reference [source]({{< relref "docs/connectors/table/bigquery" >}}#source), [sink]({{< relref "docs/connectors/table/bigquery" >}}#sink), and [CDC]({{< relref "docs/connectors/table/bigquery" >}}#change-data-capture), DataStream reference [source]({{< relref "docs/connectors/datastream/bigquery" >}}#source) and [sink]({{< relref "docs/connectors/datastream/bigquery" >}}#sink) | Analytics warehouse | Bounded; projection and filter pushdown | Insert or keyed CDC upsert/delete; Experimental ([#706]({{< param BookRepo >}}/issues/706)) | Not supported by the bounded Storage Read source | Consumes an upsert/delete changelog | Sink-writable CDC sequence inputs; no readable metadata | Read-only; datasets as databases and tables resolved from their BigQuery schemas ([catalog]({{< relref "docs/connectors/table/bigquery" >}}#catalog)) |
| Cloud Pub/Sub: [Quickstart]({{< relref "docs/quickstart/pubsub" >}}), [Examples]({{< relref "docs/examples/pubsub" >}}), [DataStream]({{< relref "docs/connectors/datastream/pubsub" >}}) ([source]({{< relref "docs/connectors/datastream/pubsub" >}}#source), [sink]({{< relref "docs/connectors/datastream/pubsub" >}}#sink)), [Table]({{< relref "docs/connectors/table/pubsub" >}}) ([source]({{< relref "docs/connectors/table/pubsub" >}}#source), [sink]({{< relref "docs/connectors/table/pubsub" >}}#sink)) | Messaging | Unbounded | Insert | Not applicable | Carries a format-provided source changelog | Readable and writable message metadata | Not applicable; the connector reads no topic schema, and a topic without one has no columns to derive |
| Cloud Tasks: [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}), [Examples]({{< relref "docs/examples/cloudtasks" >}}), [DataStream]({{< relref "docs/connectors/datastream/cloudtasks" >}}), [Table]({{< relref "docs/connectors/table/cloudtasks" >}}) | Task delivery | Not applicable; sink only | Insert; opt-in checkpointed creation within its recovery window | Not applicable | Not applicable | Writable task and request metadata | Not applicable; sink only, and a queue carries no schema |
| Bigtable: [Quickstart]({{< relref "docs/quickstart/bigtable" >}}), [Examples]({{< relref "docs/examples/bigtable" >}}), DataStream [source]({{< relref "docs/connectors/datastream/bigtable" >}}#source), [sink]({{< relref "docs/connectors/datastream/bigtable" >}}#sink), and [Change Streams]({{< relref "docs/connectors/datastream/bigtable" >}}#change-streams-source), Table [source]({{< relref "docs/connectors/table/bigtable" >}}#source), [sink]({{< relref "docs/connectors/table/bigtable" >}}#sink), [lookup]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins), and [Change Streams]({{< relref "docs/connectors/table/bigtable" >}}#change-streams) | Wide-column store | Bounded; projection and row-key filter pushdown | Upsert and atomic keep-latest; insert-only compatibility mode; atomic insert-if-absent and DDL-defined conditional commands with INSERT-only input | Sync or async with partial cache; synchronous full cache; projected families | Envelope for transformation, or keyed upsert/delete directly to an upsert sink | Readable CDC and writable cell-timestamp metadata | Read-only; an instance as one database and its tables resolved as a row key and one map per column family ([catalog]({{< relref "docs/connectors/table/bigtable" >}}#catalog)) |
| Spanner: [Quickstart]({{< relref "docs/quickstart/spanner" >}}), [Examples]({{< relref "docs/examples/spanner" >}}), [DataStream]({{< relref "docs/connectors/datastream/spanner" >}}), [Table]({{< relref "docs/connectors/table/spanner" >}}) | Relational database | Bounded; projection and primary-key filter pushdown | Insert or upsert | Sync or async; primary-key filter pushdown; partial cache | Full retract or keyed upsert/delete source to keyed sink | Readable CDC transaction metadata | Read-only; an instance's databases as databases and base tables resolved from `INFORMATION_SCHEMA` in both dialects ([catalog]({{< relref "docs/connectors/table/spanner" >}}#catalog)) |
| Firestore: [Quickstart]({{< relref "docs/quickstart/firestore" >}}), [Examples]({{< relref "docs/examples/firestore" >}}), [DataStream]({{< relref "docs/connectors/datastream/firestore" >}}), [Table]({{< relref "docs/connectors/table/firestore" >}}) | Document database | Not yet ([#1608]({{< param BookRepo >}}/issues/1608)); sink only | Insert as new documents, or keyed upsert/delete (`set`, `merge`) or upsert (`update`); same-document order not kept | Not yet ([#1609]({{< param BookRepo >}}/issues/1609)) | Consumes a keyed upsert/delete changelog | None | Not applicable; a collection carries no schema |

The Bigtable and Spanner keyed source-to-sink paths describe compatible changelog shapes, not a
stronger replication guarantee.
For example, a Spanner Change Streams table in `upsert` mode can feed a separate primary-key
Spanner sink table, but that sink remains at-least-once and does not guarantee the application
order of successive writes to one key.
