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

# Bigtable SQL Connector

The `bigtable` connector writes a table to Cloud Bigtable through the module
`flink-connector-gcp-bigtable`. It is a mapping onto the DataStream sink documented in
[Bigtable]({{< relref "docs/connectors/datastream/bigtable" >}}) — that page carries the design, the
delivery guarantees and the error handling; this one carries the DDL surface. Per-feature status is
in the module README.

**The table is write-only for now.** A `SELECT` over one fails with "Connector 'bigtable' can only
be used as a sink"; reading is [#459]({{< param BookRepo >}}/issues/459) and a lookup join is
[#460]({{< param BookRepo >}}/issues/460).

`sink.parallelism` comes from Flink's own `FactoryUtil` rather than from this connector. There is no
`format` option: a Bigtable row is a schema this DDL describes, cell by cell, and the cell encoding
is the HBase ecosystem's rather than a choice, so there is nothing for a format factory to decide.

A column family is a *column name*, so it has to be a legal SQL identifier — a reserved word such as
`identity` needs backticks, or a different name.

```sql
CREATE TABLE profiles (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  usage ROW<requests BIGINT, last_seen TIMESTAMP_LTZ(3)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles'
);

INSERT INTO profiles
SELECT user_id, ROW(name, email), ROW(requests, last_seen) FROM staged_profiles;
```

## Getting the connector onto the classpath

Use `flink-connector-gcp-bigtable` and let the deployment resolve its transitive dependencies —
Maven or Gradle for a job jar, or a `lib/` directory assembled with those dependencies present. An
uber-jar for the SQL client, the sibling of `flink-sql-connector-gcp-bigquery` and
`flink-sql-connector-gcp-pubsub`, is [#461]({{< param BookRepo >}}/issues/461); until it exists,
adding this connector to a bare SQL client means assembling the Bigtable client's runtime tree by
hand.

## The schema

The DDL model is Flink's HBase connector's, so a table definition moves between the two with its
schema intact and a table written by either is readable by the other:

- **Exactly one column is not a `ROW`, and that column is the row key.** It may be declared
  anywhere among the columns. Its type decides how the key bytes are formed, so a `BIGINT` key is
  eight big-endian bytes rather than its decimal text.
- **Every `ROW` column is one column family**, and the column's name is the family's. Its nested
  fields are the qualifiers, one cell each. A nested `ROW` is rejected: a cell holds bytes, not a
  structure.
- A family name containing `:` is rejected. Bigtable's family filter is a regular expression that
  refuses a colon even escaped, so such a family could be written but never selectively read.

A `PRIMARY KEY` is optional, exactly as it is in the HBase connector — a Bigtable write is keyed on
the row key whether or not the DDL says so. If one is declared it must be the row-key column and
nothing else.

**Declaring it makes an updating query cheaper.** A delete has to reach the sink carrying the row
key, and which of two ways that is arranged depends on the primary key. With one declared, the sink
tells the planner a delete may carry the upsert key alone — that key *is* the row key, so nothing
else is needed. With none declared, the planner keys its upserts on whatever the query happens to be
unique by, which need not be the row-key column at all, so the sink asks for whole rows and the
planner completes each one before the delete reaches it. That completion is a `ChangelogNormalize`,
which keeps state proportional to the keyspace. A query that carries no deletes gets neither, so an
insert-only job pays nothing either way.

**A completed delete is completed from what the job has seen.** `ChangelogNormalize` holds the last
row per key in Flink state, so a `-D` for a key this job never inserted has nothing to complete from
and is dropped rather than applied — a row written by an earlier job, or before the state was
cleared, is not deleted by it. This is the planner's behaviour rather than the connector's, and it
already applied to every table that declares a primary key; from
[#470]({{< param BookRepo >}}/issues/470) it applies to those that do not. If deleting rows a
different job wrote is the goal, emit the row key in the delete — a retract source carrying whole
rows reaches the sink without a normalize.

**Upgrading past [#470]({{< param BookRepo >}}/issues/470) changes the plan** of an updating query
over a table with no `PRIMARY KEY`, because `ChangelogNormalize` is a new stateful operator. Flink
assigns a SQL pipeline's operator UIDs explicitly only when it came from a persisted `COMPILED PLAN`
— `table.exec.uid.generation` defaults to `PLAN_ONLY` — and otherwise lets the lower layers generate
them "taking the complete topology into account". A savepoint taken before the upgrade therefore
does not map onto the new topology, so restoring one is a plan migration rather than a version bump.
A table that declares its primary key is unaffected: such a job already had the operator.

**A persisted `COMPILED PLAN` pins the old shape**, which is what it is for. The plan *is* the
execution graph — `stream-exec-changelog-normalize` is one of its own node types — and its sink node
stores the changelog mode it was compiled with rather than asking the connector again. A plan
written before this change therefore keeps the pre-upgrade topology, and the job running it keeps
the pre-upgrade behaviour, until the plan is recompiled.

**Every rejection on this page happens when a statement is planned, not at `CREATE TABLE`.** Flink
does not consult a connector while registering a table, so a `CREATE TABLE` naming a column this
connector cannot encode is accepted and the first `INSERT INTO` over it fails. The message arrives
wrapped in Flink's own "Unable to create a sink for writing table ..." — the actionable sentence is
in the cause.

## Options

Every option maps onto one builder setter of the DataStream API, which stays the source of truth.
An option left out of the DDL leaves that setter uncalled, so its default is whatever the connector
or the SDK already uses — the default is never restated here. The full list of defaults is in the
[configuration reference]({{< relref "docs/reference/bigtable" >}}).

The one exception is `null-string-literal`, which configures this layer's own cell codec rather
than a builder, and so carries its default here.

### Destination

| Option | Type | Maps to |
|---|---|---|
| `project` | String | The project part of `table(...)`; a bare project id |
| `instance` | String | The instance part of `table(...)` |
| `table` | String | The table part of `table(...)`. One SQL table writes to one Bigtable table: per-record routing has no SQL surface and stays on the DataStream API |
| `emulator-endpoint` | String | `emulatorEndpoint(...)` as `host:port` — parsed when the planner builds the sink, so a malformed value fails there |
| `null-string-literal` | String | The cell value that stands for a null in a character-string column; defaults to `null`. Not a builder setter: it configures the cell codec this layer supplies. Every other type writes a null as an empty cell |

### Sink

| Option | Type | Maps to |
|---|---|---|
| `sink.app-profile-id` | String | `appProfileId(...)`. Named for the sink rather than shared, because a Data Boost profile reads and cannot write, so once reading arrives ([#459]({{< param BookRepo >}}/issues/459)) one table can legitimately scan and write under different profiles |
| `sink.create-disposition` | Enum | `createDisposition(...)` — `create-if-needed` or `create-never` |
| `sink.batching.element-count` | Long | `BigtableWriterOptions.batchElementCount(...)`. Counts **entries** — one row's mutations — not mutations |
| `sink.batching.byte-size` | MemorySize | `BigtableWriterOptions.batchByteSize(...)` |
| `sink.in-flight.max-entries` | Integer | `BigtableWriterOptions.maxInFlightEntries(...)` |
| `sink.in-flight.max-bytes` | MemorySize | `BigtableWriterOptions.maxInFlightBytes(...)` |
| `sink.max-consecutive-rejections` | Integer | `BigtableWriterOptions.maxConsecutiveRejections(...)`. **Inert from SQL**: a DDL has no failure-policy option, so the sink fails the job on the first confirmed rejection and never reaches a bound. It exists so the DDL surface stays one key per writer knob |
| `sink.recovery.initial-backoff` | Duration | `BigtableWriterOptions.recoveryInitialBackoff(...)`, the budget for repairing a missing table or family |
| `sink.recovery.max-backoff` | Duration | `BigtableWriterOptions.recoveryMaxBackoff(...)` |
| `sink.recovery.max-attempts` | Integer | `BigtableWriterOptions.recoveryMaxAttempts(...)` |
| `sink.destination-idle-timeout` | Duration | `BigtableWriterOptions.destinationIdleTimeout(...)` |
| `sink.metrics.per-destination` | Boolean | `BigtableWriterOptions.perDestinationMetrics(...)` |
| `sink.parallelism` | Integer | The sink's parallelism (Flink's own option) |

### Table creation

| Option | Type | Maps to |
|---|---|---|
| `sink.table-create.gc-rule.max-versions` | Integer | `GcRule.maxVersions(...)` for every family the sink creates |
| `sink.table-create.gc-rule.max-age` | Duration | `GcRule.maxAge(...)` for every family the sink creates. Set beside the version limit, the two are combined as a **union** — a cell goes when it is either too old or too far down the version list |

**The families come from the DDL, not from a key.** A `ROW<...>` column already says a family
exists, so naming the same families again in the `WITH` clause would only create a way for the two
lists to disagree.

**The garbage-collection rule does not.** A `GcRule` is a tree of unions and intersections to any
depth, and a flat `WITH` namespace cannot carry one; the two keys above are the contraction, and
they apply the same rule to every family. A family needing anything else is created out of band,
which is what `create-never` is for.

At least one of the two keys is **required** under `create-if-needed`, which the DataStream API
does not require. A family created with no rule keeps every version of every cell forever, and this
sink is at-least-once and upsert-shaped: each replay writes another version of the same cells, so a
rule-less family created from a DDL would grow without bound with nothing reporting it.

Setting either key without `sink.create-disposition` = `create-if-needed` is rejected rather than
ignored. A table that declares no column family is rejected outright, whatever the disposition: a
mutation with no cell in it is not a write.

## Type mapping

A Bigtable cell is an uninterpreted byte string, so a convention has to be picked. This connector
uses the HBase ecosystem's — `org.apache.hadoop.hbase.util.Bytes` as Flink's HBase connector applies
it — reproduced here rather than depended on, since `hbase-common` drags in Hadoop. The row key
takes the same encodings.

| Flink type | Cell bytes |
|---|---|
| `CHAR`, `VARCHAR`, `STRING` | UTF-8, with no length prefix |
| `BOOLEAN` | One byte: `0xFF` for true, `0x00` for false |
| `BINARY`, `VARBINARY`, `BYTES` | The bytes themselves |
| `DECIMAL(p, s)` | A four-byte big-endian scale, then the unscaled value as a two's-complement big-endian `BigInteger` |
| `TINYINT` | One byte |
| `SMALLINT` | Two bytes, big-endian |
| `INT`, `DATE`, `INTERVAL YEAR TO MONTH` | Four bytes, big-endian. A `DATE` is a day count, not an epoch-millisecond value |
| `TIME(p)` | Four bytes, big-endian, the millisecond of the day |
| `BIGINT`, `INTERVAL DAY TO SECOND` | Eight bytes, big-endian |
| `FLOAT` | Four bytes: the IEEE 754 bits, big-endian |
| `DOUBLE` | Eight bytes: the IEEE 754 bits, big-endian |
| `TIMESTAMP(p)`, `TIMESTAMP_LTZ(p)` | Eight bytes, big-endian, milliseconds since the epoch |

`ARRAY`, `MAP`, `MULTISET`, a nested `ROW`, `RAW` and `TIMESTAMP WITH TIME ZONE` have no encoding
and are rejected — but see below for *when*.

### Precision stops at milliseconds

A `TIME` or `TIMESTAMP` cell holds milliseconds, so a precision above 3 is rejected rather than
silently truncated — the same bound the HBase connector draws. `TIMESTAMP` and `TIMESTAMP_LTZ`
encode identically, to the same epoch-millisecond value.

### Nulls

A null is an **empty cell** for every type except a character string, where an empty cell is a
legitimate value; a null there writes `null-string-literal` instead. Nulls are written rather than
skipped: a qualifier left unwritten keeps whatever an earlier version of the row put there, which is
not what "this column is null now" means. A whole column family that is null writes no cells at all,
since there is no value to encode.

Two collisions the convention does not resolve, both inherited from the HBase connector. A null and
a zero-length value are the **same bytes** in a `BINARY`, `VARBINARY` or `BYTES` cell — only a
character string gets a marker — so a column that must tell them apart needs the distinction encoded
in the value. And a character string whose value happens to equal `null-string-literal` reads back
as a null; pick a literal the data cannot contain.

## Delivery guarantees

The sink is **at-least-once** and its changelog mode is **upsert**. A Bigtable write is an upsert on
the row key by construction — `setCell` overwrites — and there is no append-only path to offer
instead, so an updating query is accepted rather than rejected. On Flink 2.x the mode says a delete
may carry the upsert key alone only when the DDL declares the primary key, which is what makes that
key the row key; [The schema](#the-schema) above has the consequence for a job. Flink 1.20 has no
such distinction and always completes the row first.

A `-D` deletes the **whole row**, not the declared qualifiers one by one. The row key is the primary
key, so "this key is gone" is what a delete means here; removing only the declared cells would leave
a row behind made of whatever else was in it.

### Two rows for one key in one batch have no defined winner

The writer hands entries to the client's bulk-mutation batcher, and Bigtable's own contract for
`MutateRows` is that its entries "may be applied in arbitrary order (even between entries for the
same row)". So when a job produces two changelog rows for the same key close enough together to
share a request, which one lands last is not defined — and if they share a millisecond they also
share a cell timestamp, so they collapse to one version rather than two.

**Separate requests are not the fix**, and setting `sink.batching.element-count` to `1` to force one
entry per request is the shape that looks like it. The batcher sends each request without waiting
for the previous one's response, so requests a single job has in flight are concurrent rather than
ordered — measured, forcing one entry per request made a delete stop taking effect. What has a
defined order is separate *writes*: a request whose response has been awaited before the next is
issued, which is what successive jobs give you.

A job that needs last-write-wins per key within a batch has to keep at most one row per key in
flight — by aggregating upstream, or by putting the version in the row key. Google's own guidance
lists "multiple mutations to the same row" under when *not* to use batch writes. Whether the sink
should enforce it is [#471]({{< param BookRepo >}}/issues/471).

### The cell timestamp is the writer's clock

A cell is written with the TaskManager's wall clock at the moment the mutation is built, not with a
server-side timestamp. That is the client library's default and the right one here: a retried
mutation rewrites the same cell version rather than adding a new one, which is what keeps an
at-least-once retry idempotent. The consequence is that cell version ordering follows the writer's
clock, so a clock that steps backwards can leave a newer value behind an older one.

### Three record-level rejections

Each of these fails the record through the sink's failure handler rather than skipping it:

- An `UPDATE_BEFORE` row. The declared changelog mode means the planner never sends one, so its
  arrival is a bug — and treating it as a delete, which is what a two-branch converter does, would
  erase the row the following `UPDATE_AFTER` is about to rewrite.
- A row key that is null or encodes to zero bytes. Bigtable has no such row; Flink's HBase connector
  drops the record instead, which leaves an incomplete table under a green job.
- A row whose every column family is null, which would produce a mutation with no cell in it. The
  service refuses that with an `INVALID_ARGUMENT` naming neither the row nor the reason, so the
  connector refuses it where both can be said. A partial column list — `INSERT INTO t (rowkey)
  VALUES (...)` — is the ordinary way to reach it.

Retries, error classification and the failure handler are the DataStream sink's and are described on
its [page]({{< relref "docs/connectors/datastream/bigtable" >}}). A SQL table has no failure-policy
option, so its sink always fails the job on a routed failure.

## Design decisions

**The DDL model is the HBase connector's, and that is the whole point.** Upstream
google/flink-connector-gcp models a family with a `value.format`, which cannot give a single
qualifier its own type and ties a family to a format; it was weighed and declined on
[#34]({{< param BookRepo >}}/issues/34). `apache/flink-connector-hbase` has no Flink 2.x release, so
the population this model serves has nowhere else to go.

**The encoding is normative.** It exists to be byte-compatible with the HBase ecosystem, so it is
pinned to exact byte arrays by a golden-vector test rather than round-tripped through this
connector's own code, which would pass while the interop was broken.

**No metadata columns.** The one piece of envelope a mutation has is the cell timestamp; a writable
one is [#473]({{< param BookRepo >}}/issues/473). A cell written by this sink takes the writer's clock, as above.

**Per-record table routing has no SQL surface.** A DDL names one table. Writing to several is a
`STATEMENT SET` of `INSERT`s, one per table, which is what SQL already offers.

## Testing

The emulator suite drives `CREATE TABLE` and `INSERT INTO` through the production factory, with the
emulator endpoint interpolated into the DDL rather than injected through a test-only factory, and
reads the rows back with its own client. What the emulator cannot show is covered by the gated
real-GCP suite: the client-construction path that authenticates with application-default
credentials, and `sink.app-profile-id`, which the emulator ignores entirely.
