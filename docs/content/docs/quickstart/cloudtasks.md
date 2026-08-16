---
title: Cloud Tasks
type: docs
weight: 30
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

# Dispatch a stream as Cloud Tasks

Assumes the artifacts and credentials from the
[Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the
[Java API reference]({{< param ApiDocsURL >}}).

**Create the queue first.** The sink will not create one, and that is deliberate: the queue's rate
limits are the entire reason to use the service, and a queue created with defaults would carry
Cloud Tasks' own (500 dispatches/second, 1000 concurrent) rather than the pacing the target
endpoint can absorb.

```sh
gcloud tasks queues create webhooks --location=asia-northeast1 \
    --max-dispatches-per-second=10 --max-concurrent-dispatches=5
```

{{< java-snippet file="CloudTasksQuickstartDispatch.java" tag="cloud-tasks-quickstart-dispatch" >}}

The endpoint must be reachable from Cloud Tasks, which for an HTTP target generally means a public
IP — the exception, and how to authorize against a Cloud Run service with `withOidcToken(...)`, is
on the [Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}}) page.

Tasks are unnamed by default, so a record Flink replays after a failure creates a second task and
calls the endpoint twice. `taskIdExtractor(...)` opts into deduplication, at a latency cost Google
documents as significant.

## Next

[Cloud Tasks examples]({{< relref "docs/examples/cloudtasks" >}}) — sharding across queues, and
running against the emulator so the dispatches land on a server you can inspect.
