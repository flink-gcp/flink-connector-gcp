---
title: Examples
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

# Examples

Worked examples of the things the connector pages describe at length but never show whole. Each
page starts from the [Quickstart]({{< relref "docs/quickstart" >}}) job for its connector, so only
the parts that change are shown; the reasoning behind every option stays on the connector's own
page, linked from each section.

## Capability map

The Quickstart owns the basic DataStream jobs, while these pages add the feature-specific worked
cases.
The map highlights the strengths that distinguish each service role; the Worked cases table below
says which advanced cases this section currently contains.

| Connector pages | Primary role | DataStream paths | Table paths |
|---|---|---|---|
| BigQuery: [Quickstart]({{< relref "docs/quickstart/bigquery" >}}), [Examples]({{< relref "docs/examples/bigquery" >}}), [DataStream]({{< relref "docs/connectors/datastream/bigquery" >}}), [Table]({{< relref "docs/connectors/table/bigquery" >}}) | Analytics warehouse | Split bounded reads; dynamic table destinations; at-least-once or exactly-once service writes; CDC on the at-least-once default stream, Experimental ([#706]({{< param BookRepo >}}/issues/706)) | Bounded scan pushdown; insert sink; keyed CDC sink, Experimental ([#706]({{< param BookRepo >}}/issues/706)); sink-writable CDC sequence inputs |
| Cloud Pub/Sub: [Quickstart]({{< relref "docs/quickstart/pubsub" >}}), [Examples]({{< relref "docs/examples/pubsub" >}}), [DataStream]({{< relref "docs/connectors/datastream/pubsub" >}}) ([source]({{< relref "docs/connectors/datastream/pubsub" >}}#source), [sink]({{< relref "docs/connectors/datastream/pubsub" >}}#sink)), [Table]({{< relref "docs/connectors/table/pubsub" >}}) ([source]({{< relref "docs/connectors/table/pubsub" >}}#source), [sink]({{< relref "docs/connectors/table/pubsub" >}}#sink)) | Messaging | Subscription splits; dynamic topic destinations; ordering | Format-provided source changelog; readable and writable message metadata |
| Cloud Tasks: [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}), [Examples]({{< relref "docs/examples/cloudtasks" >}}), [DataStream]({{< relref "docs/connectors/datastream/cloudtasks" >}}), [Table]({{< relref "docs/connectors/table/cloudtasks" >}}) | Task delivery | Dynamic queue destinations; HTTP and App Engine targets | HTTP or App Engine sink; writable task and request metadata |
| Bigtable: [Quickstart]({{< relref "docs/quickstart/bigtable" >}}), DataStream [source]({{< relref "docs/examples/bigtable" >}}#datastream-source) and [sink]({{< relref "docs/examples/bigtable" >}}#datastream-sink), Table [source]({{< relref "docs/examples/bigtable" >}}#table-source), [sink]({{< relref "docs/examples/bigtable" >}}#table-sink), and [lookup]({{< relref "docs/examples/bigtable" >}}#lookup-joins), [Change Streams]({{< relref "docs/examples/bigtable" >}}#change-streams), [DataStream reference]({{< relref "docs/connectors/datastream/bigtable" >}}), [Table reference]({{< relref "docs/connectors/table/bigtable" >}}) | Wide-column store | Row-range splits; dynamic table destinations; Change Streams | Scan pushdown; upsert and insert-only sink modes; writable cell timestamps; lookup modes and caching; envelope and selected-cell CDC composition |
| Spanner: [Quickstart]({{< relref "docs/quickstart/spanner" >}}), [Examples]({{< relref "docs/examples/spanner" >}}), [DataStream]({{< relref "docs/connectors/datastream/spanner" >}}), [Table]({{< relref "docs/connectors/table/spanner" >}}) | Relational database | Partitioned snapshots and queries; mutation table routing; Change Streams | Scan pushdown; lookup modes and caching; keyed CDC-to-upsert composition |

The Bigtable and Spanner Table paths describe compatible changelog shapes, not a stronger
replication guarantee; see the [Table capability map]({{< relref "docs/connectors/table" >}}) for
the endpoint caveat.

## Worked cases

| Page | Covers |
|---|---|
| [Dynamic destinations]({{< relref "docs/examples/dynamic-destinations" >}}) | The shared resolver contract, per-destination resources, idle eviction, auto-creation and Pub/Sub ordering across all five sinks |
| [BigQuery]({{< relref "docs/examples/bigquery" >}}) | Debezium MySQL, Debezium PostgreSQL, TiCDC and Spanner CDC; dynamic tables, both exactly-once write methods and redeployment, table auto-creation, an SQL table read, DataStream table/query/view/snapshot/public-dataset reads, and the emulator |
| [Cloud Pub/Sub]({{< relref "docs/examples/pubsub" >}}) | DataStream subscription and topic creation, dynamic topics, Table source startup and ordering, readable and writable message metadata, and the emulator |
| [Cloud Tasks]({{< relref "docs/examples/cloudtasks" >}}) | Sharding across queues, the emulator |
| [Bigtable]({{< relref "docs/examples/bigtable" >}}) | Bounded and filtered DataStream reads; multi-cell and dynamic-table writes, skipping and dropping records; Table scans, upsert and insert-only sinks, writable timestamps, and lookup joins; Pub/Sub attribute enrichment into Cloud Tasks; envelope CDC and a selected-cell BigQuery analytics replica; application profiles and the emulator |
| [Spanner]({{< relref "docs/examples/spanner" >}}) | SQL lookup joins and upsert writes; several tables, deletes, skipping and dropping records, batch tuning, key-range and fixed-timestamp reads, Data Boost, Change Streams metadata and filtering, and the emulator |

Two things cut across most of them, and are stated once here rather than five times.

## Dynamic per-record destinations share one shape

The [Dynamic destinations]({{< relref "docs/examples/dynamic-destinations" >}}) guide expands this
shared shape into one implementation path for all five sinks.
It covers the resolver contract, Spanner's mutation-inherent table routing, destination identity,
resource lifetimes, idle eviction, auto-creation and Pub/Sub ordering.

## An emulator is a convenience, not an authority

Every emulator page below is for fast local feedback and **never evidence about the service's
behaviour**. Where the two disagree the real service decides, and each emulator has blind spots
that matter: a green emulator run is not a green integration. The pages say what each one cannot
show, which is the part worth reading before trusting one.

BigQuery is the one that takes two endpoints rather than one, because it serves table metadata over
REST and the Storage Write API over gRPC —
[the BigQuery page]({{< relref "docs/examples/bigquery" >}}#pointing-the-sink-at-an-emulator) sets
out both, and what a run against that emulator cannot show.
