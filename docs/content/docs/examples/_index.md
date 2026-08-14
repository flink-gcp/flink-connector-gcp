---
title: Examples
bookCollapseSection: true
weight: 20
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

# Examples

Worked examples of the things the connector pages describe at length but never show whole. Each
page starts from the [Quickstart]({{< relref "docs/quickstart" >}}) job for its connector, so only
the parts that change are shown; the reasoning behind every option stays on the connector's own
page, linked from each section.

| Page | Covers |
|---|---|
| [Dynamic destinations]({{< relref "docs/examples/dynamic-destinations" >}}) | The shared resolver contract, per-destination resources, idle eviction, auto-creation and Pub/Sub ordering across all five sinks |
| [BigQuery]({{< relref "docs/examples/bigquery" >}}) | A table per day from the event timestamp, both exactly-once write methods and how to redeploy them, table auto-creation |
| [Cloud Pub/Sub]({{< relref "docs/examples/pubsub" >}}) | A topic per record, topic and subscription auto-creation, the emulator |
| [Cloud Tasks]({{< relref "docs/examples/cloudtasks" >}}) | Sharding across queues, the emulator |
| [Bigtable]({{< relref "docs/examples/bigtable" >}}) | Several mutations per record, a table per day, skipping records, dropping bad rows, reading a key range, filtering on the server, the emulator |
| [Spanner]({{< relref "docs/examples/spanner" >}}) | Several tables from one stream, deletes, skipping records, dropping refused mutations, tuning the batch, the emulator |

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

BigQuery has no emulator path on its public API at all, which is a decision with a reason —
[the BigQuery page]({{< relref "docs/examples/bigquery" >}}#no-emulator-path) records it.
