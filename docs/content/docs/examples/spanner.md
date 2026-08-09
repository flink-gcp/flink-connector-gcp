---
title: Spanner
type: docs
weight: 50
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

# Spanner examples

Worked cases beyond the [quickstart]({{< relref "docs/quickstart/spanner" >}}). What each option
means is on the [Spanner options]({{< relref "docs/reference/spanner" >}}) page; why the sink
behaves as it does is on the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page.

## Writing to several tables from one sink

The mutation names its table, so routing by record needs no option — just a serializer that decides.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(
                (event, context) ->
                        Mutation.newInsertOrUpdateBuilder(
                                        event.isAudit() ? "AuditEvents" : "Events")
                                .set("EventId").to(event.getId())
                                .set("Body").to(event.getBody())
                                .build())
        .build();
```

The cell weights the sink reads at start-up cover every table of the database, so both tables are
weighed correctly against the batch limit.

## Deleting rows

A delete is a mutation like any other, so a stream of keys is a stream of deletes.

```java
SpannerSink.<String>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "orders-db"))
        .serializer((orderId, context) -> Mutation.delete("Orders", Key.of(orderId)))
        .build();
```

Deletes are idempotent, so a replayed record is harmless. A delete over a key *range* is also
possible — note that the sink counts it as a single row against `maxBatchCells`, because nothing on
the client side can know how many rows a range matches.

## Skipping records

Returning `null` skips the record: it is written nowhere, is not a failure, and is counted by
`recordsSkipped`.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(
                (event, context) ->
                        event.isHeartbeat()
                                ? null
                                : Mutation.newInsertOrUpdateBuilder("Events")
                                        .set("EventId").to(event.getId())
                                        .build())
        .build();
```

## Dropping refused mutations instead of failing the job

By default the first refused mutation fails the job. `failedMutationHandler` changes that.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .failedMutationHandler(FailureHandler.logAndDrop())
        .build();
```

**Read what actually reaches this handler before relying on it.** By default only `ALREADY_EXISTS`
and `INVALID_ARGUMENT` do — a `NOT NULL` violation, an over-long value, a foreign key or a `CHECK`
constraint all fail the job instead. That default is deliberate, and it is also an option:

```java
.constraintViolationPolicy(ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER)
```

which sends constraint violations to the same handler, so the handler above then decides. Choose it
when your stream genuinely carries occasional records the schema will not accept; leave it alone
when a refusal would mean your column mapping is wrong. The
[table on the connector page]({{< relref "docs/connectors/datastream/spanner" >}}#what-does-not-reach-the-failure-handler-and-why)
lists every case, and says why the default is what it is.

A handler taking the connector's own type sees the mutation itself:

```java
.failedMutationHandler(
        (FailureHandler<FailedMutation>)
                failure -> {
                    Mutation refused = failure.getMutation();
                    LOG.warn("Dropping a mutation on {}: {}",
                            failure.getTable(), failure.getErrorMessage());
                })
```

## Tuning the batch

The defaults are Apache Beam's, and they are chosen to stay well under Spanner's per-request limits.
Lower them for latency; raise them only knowing what `maxBatchCells` counts.

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .writerOptions(
                SpannerWriterOptions.builder()
                        .maxBatchMutations(100)
                        // A commit delay trades latency for throughput by letting Spanner group
                        // this commit with others. Zero to 500 ms.
                        .maxCommitDelay(Duration.ofMillis(50))
                        // A backfill that must not disturb serving traffic on the same instance.
                        .rpcPriority(SpannerRpcPriority.LOW)
                        .build())
        .build();
```

`maxBatchCells` counts index entries, not columns: a row of five columns in a table with three
indexes covering them costs far more than five. Watch `bufferedCells` beside `bufferedBytes` to see
which limit is the one actually firing.

## Running against the emulator

```sh
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator:1.5.56
```

```java
SpannerSink.<Event>builder()
        .database(SpannerDatabase.of("my-project", "my-instance", "events-db"))
        .serializer(...)
        .emulatorEndpoint("localhost:9010")
        .build();
```

Setting the endpoint also stops the client looking for credentials. Pin an image at **v1.5.31 or
newer**: the emulator implements the `BatchWrite` RPC this sink writes with only from that release,
and an older one answers `UNIMPLEMENTED` to everything. The emulator has no IAM and serializes
concurrent transactions, so it is a convenience for fast feedback rather than evidence about the
service.
