---
title: Firestore
type: docs
weight: 60
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

# Write a stream into Firestore

Assumes the artifacts and credentials from the [Quickstart]({{< relref "docs/quickstart" >}}) index, and the imports an IDE resolves from the [Java API reference]({{< api-docs-url >}}).

**Create the database first.** The sink writes to an existing Firestore database in Native mode and creates no database; collections need no creating.
A project's `(default)` database is created once, for example with:

```sh
gcloud firestore databases create --location=asia-northeast1
```

{{< java-snippet file="FirestoreQuickstartWrite.java" tag="firestore-quickstart-write" >}}

The documents appear under the `orders` collection in the Firestore console, or read them back with the client library.

Two things are decided in that job rather than by the sink.
The **document** comes from the write, not from the builder: one sink writes to as many collections as the serializer names.
And the **operation** is what decides whether a replay is harmless; the [connector page]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee) has the table of which operations are idempotent.

The identity running the job needs permission to create, update and delete documents, which [`roles/datastore.user`](https://cloud.google.com/firestore/docs/security/iam) carries.

## Next

[Firestore examples]({{< relref "docs/examples/firestore" >}}) covers merges, deletes, conditional updates, dropping refused writes, and local development against the emulator.
