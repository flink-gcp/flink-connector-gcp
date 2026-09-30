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

# Firestore examples

Worked cases for the Firestore sink.
The basic job is the [Quickstart]({{< relref "docs/quickstart/firestore" >}}); what each option does is on the [connector page]({{< relref "docs/connectors/datastream/firestore" >}}).

## DataStream sink

### Merging fields into existing documents

`setMerge` writes the fields it names and leaves the others in place, creating the document if it is missing.
A nested map is merged key by key, so the example updates one key of `audit` without replacing the map.

{{< java-snippet file="FirestoreExamplesMergingFields.java" tag="firestore-examples-merging-fields" >}}

`update` also leaves unnamed fields alone, but it replaces a named map as a whole and fails the job when the document is missing; prefer `setMerge` when a document may not exist yet.

### Deleting documents

A serializer can choose the operation per record.
Deleting a missing document succeeds, so a replayed delete is harmless.

{{< java-snippet file="FirestoreExamplesDeletingDocuments.java" tag="firestore-examples-deleting-documents" >}}

Writes to the same document are not ordered: a `set` and a later `delete` of one document may be applied in either order.
A stream that deletes and rewrites the same documents in quick succession needs an order the sink does not keep yet ([#1556]({{< param BookRepo >}}/issues/1556)).

### Updating a document only if it has not changed

An `update` or `delete` given a `lastUpdateTime` applies only if the document was last updated at exactly that time.
Here a price change carries the `getUpdateTime()` of the document snapshot it was computed from, so it applies only if nothing changed the document since that snapshot; a snapshot's read time would not do, because the precondition compares the document's last update time exactly.
A write that lost the race is refused with `FAILED_PRECONDITION`, and the routing policy hands it to the failure handler instead of failing the job.

{{< java-snippet file="FirestoreExamplesConditionalUpdates.java" tag="firestore-examples-conditional-updates" >}}

A replay of a conditional write that had already been applied is refused the same way, because its first application changed the update time.
Under the default policy that fails the job again on every restart, which is why a stream of conditional writes usually wants this policy.

### Dropping refused writes instead of failing the job

A dropping handler keeps the job running through writes Firestore refuses, such as a document over 1 MiB.
`maxConsecutiveRejections` fails the job anyway once that many refusals arrive with nothing applied between them, which is what a stream whose data is broken wholesale looks like.

{{< java-snippet file="FirestoreExamplesDroppingRefusedWrites.java" tag="firestore-examples-dropping-refused-writes" >}}

## Local development

### Running against the emulator

Start the emulator from the Google Cloud CLI:

```sh
gcloud emulators firestore start --host-port=localhost:8080
```

Then point the sink at it.
The emulator needs no credentials, so the sink sends none.

{{< java-snippet file="FirestoreExamplesEmulatorSink.java" tag="firestore-examples-emulator-sink" >}}

Set the endpoint on the builder rather than exporting `FIRESTORE_EMULATOR_HOST`.
The client library would honor the variable too, and would then send a job meant for the real service to the emulator; the sink warns when the variable is set without an endpoint.
The emulator is not the service: it does not enforce the request-size limit or IAM, and the [deviation table]({{< relref "docs/connectors/datastream/firestore" >}}#emulator-deviations) lists what is known to differ.
