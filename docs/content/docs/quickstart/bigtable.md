---
title: Bigtable
type: docs
weight: 40
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

# Write a stream into a Bigtable table

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

**Create the table and its column family first.** By default the sink creates neither: a table's
schema is its column families and their garbage-collection policies, which is the part a sink
cannot guess. (Declaring that schema on the builder instead is
[table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation);
the instance always has to exist.)

```sh
gcloud bigtable instances create my-instance \
    --display-name="my-instance" --cluster-config=id=my-cluster,zone=asia-northeast1-a,nodes=1
gcloud bigtable instances tables create orders --instance=my-instance \
    --column-families=cf
```

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
// Not optional: the sink is at-least-once only with checkpointing, which is what makes Flink wait
// for every outstanding mutation before the barrier passes.
env.enableCheckpointing(60_000);

env.fromData("a-1", "a-2")
        .sinkTo(
                BigtableSink.<String>builder()
                        .table(TableDestination.of("my-project", "my-instance", "orders"))
                        .serializer(
                                (element, context) ->
                                        RowMutationEntry.create("order#" + element)
                                                // An explicit cell timestamp, so a replayed record
                                                // overwrites this cell instead of adding a version.
                                                .setCell(
                                                        "cf",
                                                        "payload",
                                                        context.timestamp() == null
                                                                ? 0L
                                                                : context.timestamp() * 1_000,
                                                        element))
                        .build());

env.execute("bigtable-quickstart");
```

Read the rows back with the `cbt` CLI:

```sh
cbt -project my-project -instance my-instance read orders
```

Two things decided in that job rather than by the sink. The **row key** is the whole access pattern
in Bigtable — reads are by key or by key range — so `order#<id>` is a choice about how the data will
be read, not a formality. And the **cell timestamp** is what makes the replay of a record after a
failure an overwrite rather than a second version of the cell; leaving it out lets the server's
clock decide, and both versions then live until garbage collection removes one.

## Next

[Bigtable examples]({{< relref "docs/examples/bigtable" >}}) — several mutations per record,
deletes, a table named per record, dropping bad rows instead of failing, and running against the
emulator.
