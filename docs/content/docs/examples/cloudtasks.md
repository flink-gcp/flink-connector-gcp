---
title: Cloud Tasks
type: docs
weight: 30
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

# Cloud Tasks examples

Starting from the [Cloud Tasks quickstart]({{< relref "docs/quickstart/cloudtasks" >}}) job.

## Sharding across queues

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#cloud-tasks-queues) places this sharding pattern in the shared resolver contract.

```java
CloudTasksSink.<OrderEvent>builder()
        .destinationResolver(
                (element, context) ->
                        QueueDestination.of(
                                "my-project",
                                "asia-northeast1",
                                "webhooks-" + Math.floorMod(element.customerId().hashCode(), 4)))
        .serializer(
                CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                        .withBody(new OrderEventSchema()))
        .build();
```

A single Cloud Tasks client serves every queue, and the sink creates no per-queue client, stream, publisher or batcher to cache or evict.
When `CloudTasksWriterOptions.builder().perDestinationMetrics(true).build()` is supplied through `writerOptions(...)`, each queue with a recorded send or failure registers counters that remain for the task lifetime because Flink cannot unregister metrics.
That optional metric registry state is separate from service-client state.

Sharding this way is how a pipeline exceeds the per-queue throughput ceiling.
The aggregate limits, and why they rarely matter for the workload this connector exists for, are on the [Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}}) page.
All the queues must exist; the sink creates none of them.

## Running against the emulator

Google publishes no Cloud Tasks emulator; the one the integration tests use is
[`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator) (MIT). Queues are
declared at startup, since neither the emulator nor the sink creates one on demand:

```sh
docker run --rm -p 8123:8123 --add-host=host.docker.internal:host-gateway \
    ghcr.io/aertje/cloud-tasks-emulator:1.2.0 \
    -host 0.0.0.0 -port 8123 \
    -queue projects/my-project/locations/asia-northeast1/queues/webhooks
```

```java
CloudTasksSink.<String>builder()
        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
        .serializer(
                // Not localhost: the emulator dispatches from inside the container, where that
                // would be the container itself. --add-host above is what makes this name resolve
                // to the host on Linux; Docker Desktop provides it already.
                CloudTasksSerializationSchema.httpTarget("http://host.docker.internal:9000/orders")
                        .withBody(new SimpleStringSchema()))
        .emulatorEndpoint("localhost:8123")
        .build();
```

Unlike the Pub/Sub emulator this one dispatches over **real HTTP**, so a server on your machine
sees exactly what the tasks carry — which is the whole reason it is worth running, and also why the
target URL has to be reachable from the container's network rather than yours. (The module's own
tests solve the same problem with testcontainers' `exposeHostPorts(...)`.)

What it cannot show, per the
[rule about emulators]({{< relref "docs/examples" >}}#an-emulator-is-a-convenience-not-an-authority):
task-name garbage collection, so the deduplication *window* is untestable and only the
`ALREADY_EXISTS` response is; queue-level `uriOverride` routing; the OAuth token path, since it
implements OIDC only; failure injection; and any size limit.
