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
`Not applicable` means that the capability does not fit the service's role; it is not an
implementation-status claim.

| Connector pages | Primary role | Scan and pushdown | Sink changelog | Lookup | CDC composition | Metadata |
|---|---|---|---|---|---|---|
| BigQuery: [Quickstart]({{< relref "docs/quickstart/bigquery" >}}), [Examples]({{< relref "docs/examples/bigquery" >}}), [DataStream]({{< relref "docs/connectors/datastream/bigquery" >}}), [Table]({{< relref "docs/connectors/table/bigquery" >}}) | Analytics warehouse | Bounded; projection and filter pushdown | Insert or keyed CDC upsert/delete; Experimental ([#706]({{< param BookRepo >}}/issues/706)) | Not supported by the bounded Storage Read source | Consumes an upsert/delete changelog | Sink-writable CDC sequence inputs; no readable metadata |
| Cloud Pub/Sub: [Quickstart]({{< relref "docs/quickstart/pubsub" >}}), [Examples]({{< relref "docs/examples/pubsub" >}}), [DataStream]({{< relref "docs/connectors/datastream/pubsub" >}}) ([source]({{< relref "docs/connectors/datastream/pubsub" >}}#source), [sink]({{< relref "docs/connectors/datastream/pubsub" >}}#sink)), [Table]({{< relref "docs/connectors/table/pubsub" >}}) ([source]({{< relref "docs/connectors/table/pubsub" >}}#source), [sink]({{< relref "docs/connectors/table/pubsub" >}}#sink)) | Messaging | Unbounded | Insert | Not applicable | Carries a format-provided source changelog | Readable and writable message metadata |
| Cloud Tasks: [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}), [Examples]({{< relref "docs/examples/cloudtasks" >}}), [DataStream]({{< relref "docs/connectors/datastream/cloudtasks" >}}), [Table]({{< relref "docs/connectors/table/cloudtasks" >}}) | Task delivery | Not applicable; sink only | Insert | Not applicable | Not applicable | Writable task and request metadata |
| Bigtable: [Quickstart]({{< relref "docs/quickstart/bigtable" >}}), [Examples]({{< relref "docs/examples/bigtable" >}}), DataStream [source]({{< relref "docs/connectors/datastream/bigtable" >}}#source), [sink]({{< relref "docs/connectors/datastream/bigtable" >}}#sink), and [Change Streams]({{< relref "docs/connectors/datastream/bigtable" >}}#change-streams-source), Table [source]({{< relref "docs/connectors/table/bigtable" >}}#source), [sink]({{< relref "docs/connectors/table/bigtable" >}}#sink), [lookup]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins), and [Change Streams]({{< relref "docs/connectors/table/bigtable" >}}#change-streams) | Wide-column store | Bounded; projection and row-key filter pushdown | Upsert; insert-only compatibility mode | Sync or async with partial cache; synchronous full cache; projected families | Envelope for transformation, or keyed upsert/delete directly to an upsert sink | Readable CDC and writable cell-timestamp metadata |
| Spanner: [Quickstart]({{< relref "docs/quickstart/spanner" >}}), [Examples]({{< relref "docs/examples/spanner" >}}), [DataStream]({{< relref "docs/connectors/datastream/spanner" >}}), [Table]({{< relref "docs/connectors/table/spanner" >}}) | Relational database | Bounded; projection and primary-key filter pushdown | Insert or upsert | Sync or async; primary-key filter pushdown; partial cache | Full retract or keyed upsert/delete source to keyed sink | Readable CDC transaction metadata |

The Bigtable and Spanner keyed source-to-sink paths describe compatible changelog shapes, not a
stronger replication guarantee.
For example, a Spanner Change Streams table in `upsert` mode can feed a separate primary-key
Spanner sink table, but that sink remains at-least-once and does not guarantee the application
order of successive writes to one key.
