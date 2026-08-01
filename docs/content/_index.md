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

One artifact covers the whole range. The connectors are compiled against the oldest supported
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
from one compile-only seam selected at build time, so the weekly build compiles the whole
suite against 1.20 as well. This is source-level support — a jar is compiled per major, and
the one-artifact claim above spans the 2.x range only. The 1.20 lane is verified on Java 17;
the Java 21 row above is a 2.x claim.

Building for 1.20 means selecting that seam along with the version — `just verify-flink 1.20.4`
does both, and the raw Maven form is `./mvnw verify -Dflink.version=1.20.4
-Dflink.compat=flink1` (the `flink.compat` property's comment in the root `pom.xml` is where
the mechanism is documented). Until artifacts are published, running on a 1.20 cluster means
building from source this way.

## Connectors

| Connector | Documentation |
|---|---|
| BigQuery | [Sink]({{< relref "docs/connectors/datastream/bigquery" >}}) — unified write API over the Storage Write API (at-least-once and exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations |
| Cloud Pub/Sub | [Sink and source]({{< relref "docs/connectors/datastream/pubsub" >}}) — dynamic per-record topic destinations, and a FLIP-27 source with multi-subscription splits. Writable [from SQL]({{< relref "docs/connectors/table/pubsub" >}}) as well |
| Cloud Tasks | [Sink]({{< relref "docs/connectors/datastream/cloudtasks" >}}) — dispatch a stream as HTTP tasks the service executes later, paced by the queue's rate limit |

Bigtable and Spanner connectors are planned.

## Disclaimer

This is an independent open-source project, licensed under the
[Apache License 2.0]({{< param BookRepo >}}/blob/main/LICENSE). It is not affiliated with,
endorsed by, or supported by the Apache Software Foundation or Google. Apache Flink, Flink, and
the Flink logo are trademarks of the Apache Software Foundation.
