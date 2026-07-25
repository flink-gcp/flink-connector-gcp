---
title: GCP Connectors for Apache Flink
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

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/):
BigQuery, Cloud Pub/Sub and Cloud Tasks, with Bigtable and Spanner planned.

> **Status: early development.** Nothing is released yet; APIs and coordinates will change.

## Connectors

| Module | Documentation |
|---|---|
| `flink-connector-gcp-bigquery` | [BigQuery]({{< relref "docs/connectors/datastream/bigquery" >}}) — unified write API: Storage Write API (at-least-once / exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations and native protobuf serialization |
| `flink-connector-gcp-pubsub` | [Cloud Pub/Sub]({{< relref "docs/connectors/datastream/pubsub" >}}) — sink with dynamic topic destinations, and a FLIP-27 source with multi-subscription splits |
| `flink-connector-gcp-cloudtasks` | Cloud Tasks — in design |

## Build

Requires JDK 17 and Maven (or use the included wrapper):

```
./mvnw verify
```

`main` targets Flink 2.x. Flink 1.20 support is planned on a dedicated branch.

## License

[Apache License 2.0](https://github.com/laughingman7743/flink-connector-gcp/blob/main/LICENSE)

## Disclaimer

This is an independent open-source project. It is not affiliated with, endorsed by, or
supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the
Flink logo are trademarks of the Apache Software Foundation.
