---
type: docs
bookToc: false
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

# GCP Connectors for Apache Flink

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/).

> **Status: released.** Artifacts are on
> [Maven Central](https://central.sonatype.com/namespace/io.github.flink-gcp): `1.0.0` for the
> supported Flink 2.x range and `1.0.0-1.20` for the Flink 1.20 LTS. The
> [Quickstart]({{< relref "docs/quickstart" >}}) shows the coordinates; building from source
> is covered by [Development]({{< relref "docs/development" >}}).

The [Quickstart]({{< relref "docs/quickstart" >}}) puts the connectors on a job's classpath, sets up
credentials and runs one complete job per connector; [Examples]({{< relref "docs/examples" >}})
covers dynamic destinations, exactly-once, auto-creation and emulator-backed local runs; and the
[configuration reference]({{< relref "docs/reference" >}}) lists every option each connector takes,
with its default.

## Supported versions

| | Supported |
|---|---|
| Apache Flink | 2.2, 2.3, and 1.20 (LTS) |
| Java | 17, 21 |

The Flink range mirrors [Flink's own support policy](https://flink.apache.org/downloads/): the
current and the previous minor release. A new Flink minor therefore moves both ends of the
range, which is a deliberate change here rather than an automatic one — a weekly build covers
every supported version, so widening or moving the range is backed by a green matrix instead of
an assumption.

One artifact covers the whole 2.x range. The connectors are compiled against the oldest supported
minor, because compiling against the oldest and running on a newer one is the direction that
works. That this actually holds is measured, not assumed: Flink promises source compatibility
across minors for `@Public` API and nothing for `@PublicEvolving` or `@Experimental`, and only
about half the Flink API these connectors touch is `@Public`. The weekly build rebuilds against
the oldest supported minor and re-runs the entire test suite with the newest one on the
classpath without recompiling, and also builds against the next unreleased Flink so that
upstream changes surface before they ship.

Java 11 is not supported even though Flink 2.x declares it, because the build targets bytecode
17.

Flink 1.20, the 1.x LTS release, is supported from this same source rather than from a
dedicated branch: the connectors use only APIs that exist identically in 1.20 and 2.x, apart
from a small seam of per-major source files selected at build time, so the weekly build
compiles the whole suite against 1.20 as well. This is source-level support — a jar is
compiled per major, and the one-artifact claim above spans the 2.x range only. The 1.20 lane
is verified on Java 17; the Java 21 row above is a 2.x claim.

Building for 1.20 means selecting that seam along with the version — `just verify-flink 1.20.4`
does both, and the raw Maven form is `./mvnw verify -Dflink.version=1.20.4
-Dflink.compat=flink1` (the `flink.compat` property's comment in the root `pom.xml` is where
the mechanism is documented). A 1.20 cluster does not need that build: the published
`X.Y.Z-1.20` version line is this same compilation, released.

## Connectors

| Connector | Documentation |
|---|---|
| BigQuery | [Sink]({{< relref "docs/connectors/datastream/bigquery" >}}) — unified write API over the Storage Write API (at-least-once and exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations. Writable [from SQL]({{< relref "docs/connectors/table/bigquery" >}}) as well |
| Cloud Pub/Sub | [Sink and source]({{< relref "docs/connectors/datastream/pubsub" >}}) — dynamic per-record topic destinations, and a FLIP-27 source with multi-subscription splits. Writable [from SQL]({{< relref "docs/connectors/table/pubsub" >}}) as well |
| Cloud Tasks | [Sink]({{< relref "docs/connectors/datastream/cloudtasks" >}}) — dispatch a stream as HTTP tasks the service executes later, paced by the queue's rate limit. Writable [from SQL]({{< relref "docs/connectors/table/cloudtasks" >}}) as well |
| Bigtable | [Sink and source]({{< relref "docs/connectors/datastream/bigtable" >}}) — one row mutation per record through the bulk `MutateRows` batcher, into a fixed table or one the record names; a bounded scan source splitting a table by sampled row-key range; and a Change Streams source over `ReadChangeStream`. Readable and writable [from SQL]({{< relref "docs/connectors/table/bigtable" >}}) as well |
| Spanner | [Sink and source]({{< relref "docs/connectors/datastream/spanner" >}}) — one `Mutation` per record through `batchWriteAtLeastOnce`; a bounded source reading a database at one snapshot on partitions the service planned; and a Change Streams source. Both dialects, GoogleSQL and PostgreSQL. Readable and writable [from SQL]({{< relref "docs/connectors/table/spanner" >}}) as well |

## API reference

The [Java API reference]({{< param ApiDocsURL >}}) is generated from the source of every module. It
goes live with the rest of the site ([#93]({{< param BookRepo >}}/issues/93)).

Flink's API stability annotations mark what is safe to depend on, and this project applies them to
its own types. `@Public` marks the frozen surface — entry classes, builders, and the
interfaces user code implements — which does not change incompatibly within a major version; the
build holds that promise by comparing each connector jar against the latest published release
(japicmp).
`@PublicEvolving` marks supported API that may still change at a minor release, announced in the
release notes, and never at a patch release. `@Experimental` marks a type still taking shape, and
`@Internal` implementation detail that may change or disappear in any release. The reference
documents the internals rather than hiding them — the annotation appears on the class page, so a
type's tier is visible at the point where someone is deciding whether to depend on it.

It is generated from `main`, so it describes the current source rather than any release. References
per released version arrive with artifact publishing
([#39]({{< param BookRepo >}}/issues/39)).

The youngest surfaces sit below the frozen tier, each for a recorded reason
([ADR-0141]({{< param BookRepo >}}/blob/main/docs/adr/0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)):
the Bigtable and Spanner Change Streams sources are `@PublicEvolving` — Bigtable's record model
mirrors a vendor model that is still growing, and neither source has yet survived a release under
real use — and the BigQuery CDC surface is `@Experimental` while the open upstream question in
[#706]({{< param BookRepo >}}/issues/706) can still reshape what its sequence-number providers
receive.

## Disclaimer

This is an independent open-source project, licensed under the
[Apache License 2.0]({{< param BookRepo >}}/blob/main/LICENSE). It is not affiliated with,
endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and
the Flink logo are trademarks of the Apache Software Foundation.
