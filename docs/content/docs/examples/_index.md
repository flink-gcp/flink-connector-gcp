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
| [BigQuery]({{< relref "docs/examples/bigquery" >}}) | A table per day from the event timestamp, both exactly-once write methods and how to redeploy them, table auto-creation |
| [Cloud Pub/Sub]({{< relref "docs/examples/pubsub" >}}) | A topic per record, topic and subscription auto-creation, the emulator |
| [Cloud Tasks]({{< relref "docs/examples/cloudtasks" >}}) | Sharding across queues, the emulator |
| [Bigtable]({{< relref "docs/examples/bigtable" >}}) | Several mutations per record, a table per day, skipping records, dropping bad rows, reading a key range, filtering on the server, the emulator |
| [Spanner]({{< relref "docs/examples/spanner" >}}) | Several tables from one stream, deletes, skipping records, dropping refused mutations, tuning the batch, the emulator |

Two things cut across most of them, and are stated once here rather than five times.

## Dynamic per-record destinations share one shape

The BigQuery, Pub/Sub and Cloud Tasks sinks resolve their destination per record the same way — a
`destinationResolver` in place of the fixed `destination` / `topic` / `queue` — so one sink instance
fans out across tables, topics or queues, and what changes between them is only the destination
type. The Bigtable sink joined them with [#232]({{< param BookRepo >}}/issues/232), where a
destination costs a batcher rather than only a map entry — its page says what that means.
The Spanner sink is the one that does not: it is fixed to a database, and takes each
write's table from the mutation its serializer returns rather than from a resolver.

**The resolver runs once per record on the write path.** That is the constraint every example is
built around: it must be cheap, deterministic, and it should hand back cached destination instances
rather than allocating one per record, because destination identity is what the sinks key their
per-destination state on. How much that costs differs — a BigQuery destination holds a stream
writer and a Pub/Sub topic holds an SDK publisher, while a Cloud Tasks queue holds nothing at all —
which each page says under its own example.

## An emulator is a convenience, not an authority

Every emulator page below is for fast local feedback and **never evidence about the service's
behaviour**. Where the two disagree the real service decides, and each emulator has blind spots
that matter: a green emulator run is not a green integration. The pages say what each one cannot
show, which is the part worth reading before trusting one.

BigQuery has no emulator path on its public API at all, which is a decision with a reason —
[the BigQuery page]({{< relref "docs/examples/bigquery" >}}#no-emulator-path) records it.
