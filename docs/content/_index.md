---
type: docs
bookToc: false
---

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

# GCP Connectors for Apache Flink

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/).

> **Status: early development.** Nothing is released yet; APIs and coordinates will change.
> Building from source is described in the
> [repository README]({{< param BookRepo >}}#build).

## Connectors

| Connector | Documentation |
|---|---|
| BigQuery | [Sink]({{< relref "docs/connectors/datastream/bigquery" >}}) — unified write API over the Storage Write API (at-least-once and exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations |
| Cloud Pub/Sub | [Sink and source]({{< relref "docs/connectors/datastream/pubsub" >}}) — dynamic per-record topic destinations, and a FLIP-27 source with multi-subscription splits |
| Cloud Tasks | [Sink]({{< relref "docs/connectors/datastream/cloudtasks" >}}) — dispatch a stream as HTTP tasks the service executes later, paced by the queue's rate limit (design settled, implementation in progress) |

Bigtable and Spanner connectors are planned.

## Disclaimer

This is an independent open-source project, licensed under the
[Apache License 2.0]({{< param BookRepo >}}/blob/main/LICENSE). It is not affiliated with,
endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and
the Flink logo are trademarks of the Apache Software Foundation.
