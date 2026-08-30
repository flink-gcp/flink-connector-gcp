---
title: DataStream Connectors
bookCollapseSection: true
weight: 10
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

# DataStream Connectors

Use these pages for Java builder APIs, connector-specific serialization and deserialization
schemas, and the runtime behavior that the Table connectors reuse.
For DDL, SQL type mappings, metadata columns, and planner-specific restrictions, see the
[Table API connectors]({{< relref "docs/connectors/table" >}}).
The [connector overview]({{< relref "docs/connectors" >}}) explains how to choose between the two
APIs.

## Capability map

The source column names how Flink distributes reads, while the destination column names the
record-level routing surface of the sink.
The delivery column summarizes connector-to-service writes; the [delivery guarantees]({{< relref "docs/connectors/delivery-guarantees" >}}) page defines the checkpoint and state qualifications.
`Not applicable` means that the capability does not fit the service's role; it is not an
implementation-status claim.

| Connector pages | Primary role | Source and split strategy | Sink delivery | Per-record destination | CDC |
|---|---|---|---|---|---|
| BigQuery: [Quickstart]({{< relref "docs/quickstart/bigquery" >}}), DataStream examples [source]({{< relref "docs/examples/bigquery" >}}#datastream-source) and [sink]({{< relref "docs/examples/bigquery" >}}#datastream-sink), DataStream reference [source]({{< relref "docs/connectors/datastream/bigquery" >}}#source) and [sink]({{< relref "docs/connectors/datastream/bigquery" >}}#sink), Table reference [source]({{< relref "docs/connectors/table/bigquery" >}}#source), [sink]({{< relref "docs/connectors/table/bigquery" >}}#sink), and [CDC]({{< relref "docs/connectors/table/bigquery" >}}#change-data-capture) | Analytics warehouse | Bounded; Storage Read API stream splits | At-least-once or exactly-once service writes | Table through a resolver | Upsert/delete sink on the at-least-once default stream; Experimental ([#706]({{< param BookRepo >}}/issues/706)) |
| Cloud Pub/Sub: [Quickstart]({{< relref "docs/quickstart/pubsub" >}}), [Examples]({{< relref "docs/examples/pubsub" >}}), [DataStream]({{< relref "docs/connectors/datastream/pubsub" >}}) ([source]({{< relref "docs/connectors/datastream/pubsub" >}}#source), [sink]({{< relref "docs/connectors/datastream/pubsub" >}}#sink)), [Table]({{< relref "docs/connectors/table/pubsub" >}}) ([source]({{< relref "docs/connectors/table/pubsub" >}}#source), [sink]({{< relref "docs/connectors/table/pubsub" >}}#sink)) | Messaging | Unbounded; subscription splits | At-least-once | Topic through a resolver | No native DataStream changelog; Table formats can carry one |
| Cloud Tasks: [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}), [Examples]({{< relref "docs/examples/cloudtasks" >}}), [DataStream]({{< relref "docs/connectors/datastream/cloudtasks" >}}), [Table]({{< relref "docs/connectors/table/cloudtasks" >}}) | Task delivery | Not applicable; sink only | At-least-once | Queue through a resolver | Not applicable |
| Bigtable: [Quickstart]({{< relref "docs/quickstart/bigtable" >}}), [Examples]({{< relref "docs/examples/bigtable" >}}), DataStream [source]({{< relref "docs/connectors/datastream/bigtable" >}}#source), [sink]({{< relref "docs/connectors/datastream/bigtable" >}}#sink), and [Change Streams]({{< relref "docs/connectors/datastream/bigtable" >}}#change-streams-source), Table [source]({{< relref "docs/connectors/table/bigtable" >}}#source), [sink]({{< relref "docs/connectors/table/bigtable" >}}#sink), [lookup]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins), and [Change Streams]({{< relref "docs/connectors/table/bigtable" >}}#change-streams) | Wide-column store | Bounded; sampled row-range splits | At-least-once | Table through a resolver | Change Streams partitions; unbounded or bounded |
| Spanner: [Quickstart]({{< relref "docs/quickstart/spanner" >}}), [Examples]({{< relref "docs/examples/spanner" >}}), [DataStream]({{< relref "docs/connectors/datastream/spanner" >}}), [Table]({{< relref "docs/connectors/table/spanner" >}}) | Relational database | Bounded; service-planned partitions | At-least-once | Table in each serialized mutation | Change Streams partitions; unbounded |
