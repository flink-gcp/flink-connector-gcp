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

# ADR-0200: Job-graph objects carry no fallback for a form serialized by an older connector version

- Status: Accepted
- Date: 2026-11-07
- Issues: [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071)
- Modules: all connectors
- Current behavior: none on the documentation site; the rule binds connector code

## Context

Options objects, sink configurations and Table API codecs are Java-serialized into the job graph.
Since 1.0.0, a field added to one of them has kept `serialVersionUID = 1L` and come with a
fallback: the field is nullable or uses zero as a sentinel, and its getter substitutes the default.
Java deserialization runs no constructor and leaves a field absent from the stream at its JVM
default, so the fallback was meant for a job graph written by an earlier connector version and read
by a later one.
ADR-0146, ADR-0182, ADR-0189, ADR-0195 and ADR-0196 each recorded one such fallback.
ADR-0112 also kept a CDC sink's pre-created-table behavior for a graph serialized before CDC
auto-creation existed.

The premise came from [ADR-0125](0125-no-connector-minted-serializable-lambda-crosses-the-job-graph.md)'s
reachability paragraph.
That paragraph read the Flink Kubernetes Operator's `last-state` upgrade, which keeps HA metadata,
as a path that recovers a graph serialized by the old image under the new one.
[ADR-0024](0024-the-avro-serializer-mirrors-the-proto-path-and-derives-eagerly.md) repeated it and
concluded that a serialized-form change after 1.0.0 owes an upgrade path.
[ADR-0128](0128-an-enumerators-closeable-seam-is-minted-per-enumerator.md) said the opposite: that
deserializing a job graph written by a different connector build is not a path this project
supports.

## Evidence

The Operator source settles the premise.
`AbstractFlinkService.submitApplicationCluster` deletes the job-graph entries from the Kubernetes HA
ConfigMaps before it deploys an application cluster, "so that the newly changed job config (e.g.
parallelism) could take effect", and from ZooKeeper when that is the HA service.
This is the method a `last-state` upgrade deploys through, with `requireHaMetadata` set.
`FlinkUtils.deleteJobGraphInKubernetesHA` removes every job-graph key and leaves the checkpoint
pointers.
The Kubernetes HA deletion is present in every Operator release checked: 1.0.0 (in `FlinkService`),
and 1.4.0, 1.10.0, 1.13.0 and `main` (in `AbstractFlinkService`), read on 2026-11-07.
The ZooKeeper deletion is absent from 1.0.0 and present from 1.4.0.
A `last-state` upgrade therefore restores the checkpoint under a graph that the new image's user
code built, which is the same graph a savepoint resubmission builds.

No other upgrade path carries a serialized graph to a new connector version.
A session-cluster submission and a savepoint restore both build the graph from user code.
Within one connector version, the HA JobGraph store hands a graph back to the jar that wrote it.

What crosses versions is checkpoint and savepoint state.
Every connector writes that state through its own `SimpleVersionedSerializer`, and none of those
serializers Java-serializes an options or configuration object.
The only Java serialization inside one is the Spanner batch split's `Partition` and
`BatchTransactionId`, which are the client library's own classes.
No connector registers Flink managed state (no `StateDescriptor` in any connector module's main
source; only the Tier-3 test applications under `kubernetes/apps` use one, for their own input).

## Decision

**A job graph is read only by the connector version that wrote it.**
Moving a job to another connector version means a new graph: a savepoint or checkpoint restore
from a resubmission, or an Operator upgrade, which regenerates the graph.
A JobManager that recovers a persisted graph after its image or classpath changed underneath it,
outside the Operator, is not a supported upgrade path.

**An object that travels only in the job graph has no fallback for an older serialized form.**
Its fields are set by its builder or constructor and are non-null where the builder never leaves
them null.
Its getters return the field.
`serialVersionUID` stays `1L`, because nothing reads a form written by another version.

**State a checkpoint or savepoint carries keeps its compatibility.**
A split, an enumerator state, a committable or a writer state changes only through a new
`SimpleVersionedSerializer` version that still reads the old ones.
That rule is unchanged.

**The runtime re-checks stay.**
A writer that re-checks an option it relies on, because the builder never ran on a deserialized
instance (ADR-0002, ADR-0007), keeps the check.
Its reason is a forged or hand-edited serialized form, and the check is what turns such a form into
a named failure rather than a hang or an arithmetic exception.
A version skew is no longer one of the reasons.

## Consequences

[#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071) removes the fallbacks from
`FileLoadsOptions`, `BufferedStreamOptions`, `DefaultStreamOptions`, the BigQuery
`TableCreateOptions` and `CdcTableOptions`, `BigQuerySinkConfig`, `BigtableWriterOptions`,
`BigtableRequestOptions`, the Bigtable `TableCreateOptions`, `CellValueCodec`'s typed decoder,
`PubSubPublisherOptions` and `SpannerWriterOptions`, and deletes the tests that forged an older form.
`BigQuerySinkConfig` no longer stores whether it manages the CDC table contract; it manages it
exactly when CDC options are configured, which was the only value a constructed config could hold.
The Pub/Sub publisher's progress-budget re-check now goes through the shared
`OptionChecks.checkPositive`, so a forged null budget fails with a `NullPointerException` naming
the option instead of an `IllegalArgumentException` saying it must be positive.

A JobManager in the unsupported path above may fail to start the job, with a deserialization error
or a `NullPointerException` from a field the older form did not carry.
Before this change the same JobManager read most added fields as defaults, but not all: ADR-0024
and ADR-0125 already recorded serialized-form changes that it could not read.

ADR-0125's rule, that no connector-minted serializable lambda crosses the job graph, is not
revisited here.
Its reachability paragraph is corrected to point to this record.
