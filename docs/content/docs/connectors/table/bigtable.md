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

The `bigtable` connector reads and writes a table in Cloud Bigtable through the module
`flink-connector-gcp-bigtable`. It is a mapping onto the DataStream sink and scan source documented
in [Bigtable]({{< relref "docs/connectors/datastream/bigtable" >}}) — that page carries the design,
the delivery guarantees and the error handling; this one carries the DDL surface. Per-feature
status is in the module README.

`sink.parallelism` and `scan.parallelism` come from Flink's own `FactoryUtil` rather than from this
connector.
Bounded scans and sinks have no format option: a Bigtable row is a schema this DDL describes, cell
by cell, and the cell encoding is the HBase ecosystem's rather than a choice.
The selected-cell Change Streams mode is the exception because one cell holds a serialized logical
row and `value.format` decodes it.

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
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profiles
SELECT user_id, ROW(name, email), ROW(requests, last_seen) FROM staged_profiles;

-- A bounded scan of the same table; only the families the query reads leave the server.
SELECT rowkey, profile FROM profiles;
```

## Getting the connector onto the classpath

Use `flink-sql-connector-gcp-bigtable`, an uber-jar built for exactly this: put it in Flink's
`lib/` directory, or add it with `ADD JAR` in the SQL client. It bundles
`flink-connector-gcp-bigtable` together with its whole runtime tree — the Bigtable client, gRPC,
protobuf, Guava, the Google auth and HTTP clients — which is 67 artifacts, not a dependency list
anyone wants to assemble by hand.

The plain `flink-connector-gcp-bigtable` jar works too, where the deployment already resolves
transitive dependencies. That is the right choice for a DataStream job built with Maven or Gradle.
For SQL it usually is not.

The uber-jar does not bundle Flink format implementations.
A selected-cell Change Streams job must also put the chosen format jar, such as `flink-json`, on
the SQL client and cluster classpaths.

### Everything bundled is relocated

Every bundled package moves under `io.github.flink.gcp.connector.bigtable.shaded.`, so the
versions of gRPC, protobuf and Guava this connector needs cannot collide with the ones a job,
another connector, or Flink itself brings. Six packages are deliberately *not* relocated, and none
of them can collide in a way that matters: `org.conscrypt`, which gRPC picks up reflectively as an
optional TLS provider and does without when it is unusable; and the annotation-only
`javax.annotation` (jsr305's classes only — `javax.annotation-api`, the other artifact publishing
into that package, is not bundled, [#352]({{< param BookRepo >}}/issues/352)), `org.jspecify`,
`org.codehaus.mojo.animal_sniffer`, `android.annotation` and `org.checkerframework`, where a
duplicate class is inert because nothing ever invokes it.

`io.grpc:grpc-netty-shaded` *is* relocated, including the rename of its `META-INF/native/`
libraries that relocating an already-relocated gRPC requires. The full reasoning — why exempting
it instead would trade a real collision for a hypothetical one — is on the
[Pub/Sub SQL page]({{< relref "docs/connectors/table/pubsub" >}}), and this jar inherits it. One
consequence worth repeating: relocation rewrites netty's system-property names along with its
packages, so a `-D` spelled with the upstream `io.grpc.netty.shaded.io.netty.` prefix has no
effect inside this jar.

Sharing a `lib/` with the sibling GCP SQL uber-jars works, and it is measured for this jar rather
than inherited: of the 560 file entries it shares with the Pub/Sub jar and the 1,034 it shares
with the BigQuery jar, all but four are byte-identical in each pair, and the four are per-jar
metadata Flink reads through `ServiceLoader` or enumeration — the manifest, the `NOTICE`, and two
service files (measured 2026-08-10, one build of each jar; the
[BigQuery SQL page]({{< relref "docs/connectors/table/bigquery" >}}) carries the same measurement
for the pair it names).
**Merging jars into one fat jar is the case that does not work**: without maven-shade's
`ServicesResourceTransformer`, one jar's
`META-INF/services/org.apache.flink.table.factories.Factory` entry silently shadows the other's,
as does one jar's `NOTICE`. Put the jars in `lib/`, or add each with its own `ADD JAR`.

That warning has a second reader here that the sibling pages do not have:
google/flink-connector-gcp publishes a Bigtable table connector that registers the same
`bigtable` identifier ([#472]({{< param BookRepo >}}/issues/472)). With both jars in `lib/`,
`FactoryUtil` discovery fails loudly, naming the ambiguous identifier — the acceptable outcome.
Merged into one fat jar, the failure is silent: whichever factory registration survives owns
`bigtable`, and the DDL then means whatever that connector says it means. The two option
vocabularies overlap in only `project`, `instance` and `table`, so the first symptom is the other
connector rejecting options it never declared.

### Licensing

`META-INF/NOTICE` inside the jar lists every bundled artifact grouped by licence, and
`META-INF/licenses/` carries the full text of each non-Apache-2.0 one — protobuf, gax and
api-common, the Google auth library, the ThreeTen backport, RE2/J, animal-sniffer and the Checker
Framework qualifiers.

The prose of the NOTICE is human-written, in the module's `NOTICE.template`; the artifact lists
are generated into it from what Maven actually resolves, so a wrong licence grouping or a stale
version cannot be written at all. Each licence text has a pinned source — the artifact's own jar
where one ships a text, otherwise a curated URL matched to the bundled version — recorded with its
sha256, so a text that changes upstream fails the build instead of being shipped unreviewed. `just
update-notice <module>` regenerates both after a dependency change; `just check-notice <module>`
verifies, offline, that what is checked in still matches the bundle and the pins.

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
which keeps state proportional to the keyspace. For this delete-completion decision, a query that
carries no deletes needs no `ChangelogNormalize` either way. Flink 2.3's separate conflict-strategy
state for insert-only input is covered under [delivery guarantees](#delivery-guarantees).

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

## Reading

With the default `scan.mode = bounded`, a `SELECT` is a **bounded scan** over the DataStream source
— the same split planning, resumption and metrics that
[page]({{< relref "docs/connectors/datastream/bigtable" >}}) describes — and it works in both batch
and streaming jobs. The design record is
[ADR-0092](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0092-the-bigtable-table-source-serves-projection-as-a-family-filter.md).

**Projection is pushed to the server as a family filter.** The query's retained column families
become a filter the scan carries, so an unread family never leaves the server. A query that reads
no family at all — `SELECT rowkey`, `SELECT COUNT(*)` — scans keys only: one cell per row, its
value stripped, since Bigtable has no row without a cell. The projection is by whole columns; a
retained family always arrives as its full declared `ROW`.

The filter is applied whether or not the query projects, naming exactly the declared families.
Two consequences:

- A family the physical table has but the DDL does not declare is never read.
- **A family the DDL declares but the table lacks fails the scan** with the service's `NOT_FOUND`
  ("Requested column family not found"). The source does not pre-validate the DDL against the
  table — that would cost every scan a metadata read to soften an error the service already
  reports precisely. A row-key-only query still answers, its keys-only filter naming no family.

**Which rows a query sees follows from the storage model.** A Bigtable row exists while it has a
cell, so a query that reads families returns the rows with at least one cell in a family it reads
— a row whose every *read* family is empty has nothing for the server-side filter to return and
does not appear. `SELECT *` includes a row holding data in any declared family, with the empty
ones `NULL`; a narrower projection can exclude that same row; and a query reading no family —
`SELECT rowkey`, `COUNT(*)` — sees every physical row, including one whose cells all live in
families the DDL never declared. Which columns a query selects therefore also decides which rows
it sees. This is the wide-column model's row existence, not an artifact of the pushdown. Flink's
HBase connector also makes row membership projection-dependent, but selects each declared
qualifier; this connector filters at the family boundary, so a row holding only an undeclared
qualifier in a read family appears here with that family `NULL` where HBase omits it. Projecting
the row key alongside a family does not change membership: once a query reads a family, only rows
with a cell in a read family appear. A keys-only query is the SQL shape that sees every physical
row.

**The latest version of each cell is read.** Bigtable stores timestamped versions; the scan takes
the newest per qualifier. A qualifier the declared family holds but the DDL does not name is
ignored.

### Change Streams

Set `scan.mode = change-stream` to read the table's mutation log through the DataStream Change
Streams source.
The default remains `bounded`, so existing DDL keeps its current row-scan behavior.

A Change Streams table is source-only.
Its physical envelope has exactly the `row_key` and `entries` columns; this example also selects
all optional metadata as virtual columns:

```sql
CREATE TABLE profile_mutations (
  row_key BYTES,
  entries ARRAY<ROW<
    entry_index INT,
    kind STRING,
    family STRING,
    qualifier ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `value` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    delete_range ROW<
      start_bound STRING,
      start_micros BIGINT,
      end_bound STRING,
      end_micros BIGINT
    >
  >>,
  mutation_type STRING NOT NULL
    METADATA FROM 'mutation-type' VIRTUAL,
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
  tie_breaker INT NOT NULL
    METADATA FROM 'tie-breaker' VIRTUAL,
  estimated_low_watermark TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'estimated-low-watermark' VIRTUAL
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'envelope',
  'scan.app-profile-id' = 'single-cluster-profile'
);
```

The source emits one `INSERT` row per Bigtable mutation.
`entries` retains the service order, and `entry_index` is its zero-based position in that list.
The source rejects a primary key because two mutations for the same `row_key` are distinct log
records rather than updates to one Flink row.

The metadata columns expose scalar fields attached to the mutation:

| Metadata key | Type | Meaning |
|---|---|---|
| `mutation-type` | `STRING NOT NULL` | `USER` for a user mutation or `GARBAGE_COLLECTION` for a garbage-collection mutation |
| `source-cluster-id` | `STRING` | The originating cluster for a user mutation; null for garbage collection |
| `commit-timestamp` | `TIMESTAMP_LTZ(9) NOT NULL` | The service commit time, retaining nanoseconds |
| `tie-breaker` | `INT NOT NULL` | The service tie breaker for mutations committed at the same time |
| `estimated-low-watermark` | `TIMESTAMP_LTZ(9) NOT NULL` | The producing partition's estimated low watermark at this mutation |

The declared column names are local to the DDL; `METADATA FROM` selects the stable connector key.
Flink permits an explicitly castable declared type and applies the cast after the source, while the
connector always emits the type in the table above.
Marking the columns `VIRTUAL` keeps them out of a sink schema if the catalog table is reused.

| `kind` | `qualifier` | `timestamp` | `value` | `delete_range` |
|---|---|---|---|---|
| `SET_CELL` | `RAW_VALUE` | `RAW_TIMESTAMP` | `RAW_VALUE` | null |
| `DELETE_CELLS` | `RAW_VALUE` | null | null | timestamp bounds |
| `DELETE_FAMILY` | null | null | null | null |
| `ADD_TO_CELL` | generic value | generic value | generic value | null |
| `MERGE_TO_CELL` | generic value | generic value | generic value | null |

A generic value sets `value_type` to `RAW_VALUE`, `RAW_TIMESTAMP`, or `INT64`.
`RAW_VALUE` populates `bytes_value`; the other two populate `long_value`, with raw timestamps in
microseconds.
Delete bounds use `OPEN`, `CLOSED`, or `UNBOUNDED`, and an unbounded endpoint has a null micros
field.
Fields that do not apply to an entry kind are null.
If a later client library introduces an entry or value subtype the SDK converter does not know, the
job fails with that subtype's class name before emitting an incomplete mutation.

`UNNEST` expands the ordered entry array for relational processing:

```sql
SELECT
  row_key,
  mutation_type,
  commit_timestamp,
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
FROM profile_mutations
CROSS JOIN UNNEST(entries) AS entry_table(
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
);
```

SQL result rows have no implicit arrival order.
`entry_index` carries each entry's original zero-based service position through the expansion, so a
downstream keyed computation can reconstruct the order without relying on `UNNEST` output order.

The envelope is a mutation record, not a reconstructed Bigtable row.
Bigtable does not supply before or complete after images, so a cell or family deletion remains an
inserted envelope row rather than a Flink `DELETE` or `UPDATE`.

#### Selected-cell upserts

Set `scan.change-stream.changelog-mode = selected-cell` only when one Bigtable cell contains the
complete serialized non-key part of a logical row.
The mutation row key supplies exactly one declared physical primary key, which may appear anywhere
in the DDL, and `value.format` decodes every other physical column.
At least one non-key column is required.

```sql
CREATE TABLE current_profiles (
  name STRING,
  profile_id STRING NOT NULL,
  score INT,
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'selected-cell',
  'scan.app-profile-id' = 'single-cluster-profile',
  'scan.change-stream.selected-cell.family' = 'state',
  -- Base64 for the qualifier "current"; an empty qualifier is ''.
  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',
  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',
  'value.format' = 'json'
);
```

The source recognizes only this atomic producer protocol:

- An upsert is one full delete of the selected column across all timestamps, or one delete of the
  selected family, followed by exactly one selected `SetCell` in the same mutation.
- A delete is that full selected-column or selected-family delete without a later selected
  `SetCell` and produces a key-only `DELETE`.
- Entries for other cells and families produce no row.

The upsert is emitted as `UPDATE_AFTER`, not an invented `INSERT`.
A downstream keyed table can materialize the first update it observes as the current value.
The source performs no point lookup, stores no previous row image, and does not scan a snapshot
before its configured Change Streams start position.

The configured source cluster must match every mutation that affects the selected cell.
This excludes cross-cluster conflict ordering from the changelog contract.
The job fails on standalone, repeated, or out-of-order selected `SetCell` entries; partial
timestamp deletes; garbage-collection or aggregate mutations affecting the selected cell; or a
mutation from another cluster.
The chosen format must be insert-only and emit exactly one non-null row for every upsert.
Format metadata is not exposed because a key-only delete has no payload to decode.

These are producer requirements, not inferences the connector can make from arbitrary Bigtable
traffic.
A writer that does not atomically replace the complete selected value with the sequence above must
use the lossless `envelope` mode instead.

The application profile is required and must route to one cluster, as on the DataStream source.
The emulator is rejected because it does not implement Change Streams.
Row ranges, the HBase-compatible cell codec, lookup options, projection pushdown, and filter
pushdown belong to the bounded source and are not Change Streams settings.
The factory rejects those options in Change Streams mode and rejects Change Streams options in
bounded mode.

An absent `scan.startup.mode` retains the DataStream builder's latest-position default.
Choose `earliest` or `timestamp`; the timestamp mode also requires
`scan.startup.timestamp-millis`.
Restored checkpoint state wins over that fresh-start setting.
If a restored continuation has expired, the job fails unless
`scan.resume-fallback.mode` explicitly opts into `earliest`, `latest`, or a timestamp
with `scan.resume-fallback.timestamp-millis`.
This restore contract is [ADR-0094](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0094-change-stream-start-positions-resolve-once-and-restored-state-wins-until-it-expires.md).

Set `scan.end-timestamp-millis` to make the source bounded; without it the source is continuous.
`scan.max-concurrent-streams-per-subtask` bounds open partition reads in each source subtask and
keeps the DataStream builder's default of two when absent.
Source parallelism multiplied by that value is configured job capacity, not a Bigtable quota.

Continuation tokens and partition ranges remain internal checkpoint protocol state rather than
queryable metadata.
The `estimated-low-watermark` value belongs to the partition that produced the mutation; it is not
a safe stream-wide event-time frontier, and the connector does not provide native
`SOURCE_WATERMARK()` support.
Bigtable permits a future record below a previously observed estimate, so including queued and
enumerator-held partitions would still not satisfy Flink's non-early contract.

Do not declare `SOURCE_WATERMARK()` for this connector.
Flink can retain that expression in a separate watermark operator when the source does not support
pushdown, but the Bigtable source does not provide the source-generated watermark it requests.

A DDL can instead declare an ordinary, application-owned watermark expression over commit-time
metadata:

```sql
commit_timestamp TIMESTAMP_LTZ(3) NOT NULL
  METADATA FROM 'commit-timestamp' VIRTUAL,
WATERMARK FOR commit_timestamp AS commit_timestamp - INTERVAL '5' MINUTE
```

The five-minute delay is an example policy, not a recommended or service-backed bound.
Bigtable publishes no finite maximum lateness, and source concurrency can leave partitions queued
or unassigned, so the job must choose how it handles records behind that watermark.
See [ADR-0109]({{< param BookRepo >}}/blob/main/docs/adr/0109-bigtable-change-stream-estimates-do-not-become-native-source-watermarks.md)
for the decision and the different Spanner heartbeat contract.

### Filter pushdown

The source consumes row-key predicates exactly when the SQL comparison has the same ordering as
the HBase-compatible byte encoding.
Exact equality, inequality and `IN` predicates become one or more Bigtable row ranges.
`AND` intersects ranges, and `OR` unions them.
The ranges configured by `scan.row-prefix`, `scan.row-range.*` and `scan.row-ranges` are first
treated as one union, then intersected with the SQL ranges.
The same final ranges and filters serve the bounded scan and a FULL-cache loader created from that
filtered source plan.

| Row-key type | `=`, `<>`, `IN` | `<`, `<=`, `>`, `>=` | `IS NULL`, `IS NOT NULL` |
|---|---|---|---|
| `VARCHAR`, `VARBINARY` | Exact pushdown | Exact pushdown | Exact pushdown |
| Integer, date, time, timestamp and interval types | Exact pushdown | Evaluated by Flink | Exact pushdown |
| `CHAR`, `BINARY`, `BOOLEAN`, `DECIMAL`, `FLOAT`, `DOUBLE` | Evaluated by Flink | Evaluated by Flink | Exact pushdown |

Fixed-width integer and temporal decoders ignore suffix bytes, so equality uses a key-prefix range
rather than only the canonical encoded key.
Their encodings do not preserve signed SQL order.
An empty `VARCHAR` or `VARBINARY` literal remains with Flink: the SDK cannot express an empty-key
range, and although the service rejects empty row keys, the emulator accepts them.
Fixed-width character and binary values may require SQL padding, a nonzero byte decodes as boolean
true, decimal encodings carry their own scale, and floating point has signed-zero and `NaN`
semantics.
Those many-to-one or noncanonical cases remain with Flink.
An expression outside the table, including a cast or computed expression around the row key,
stays with Flink unless the planner has already reduced it to a supported field-literal form.

Positive predicates on a family or qualifier use a **best-effort cell-existence prefilter**.
For example, `cf1 IS NOT NULL`, `cf1.name IS NOT NULL`, `cf1.name = 'alice'` and
`cf1.name IN ('alice', 'bob')` can avoid returning rows that have no relevant cell.
The source reports the same SQL expression as a residual filter, so Flink still compares the
decoded value and applies its null semantics.
`IS NULL`, `NOT`, an `OR` with an unsupported branch and other predicates that cannot yield a
necessary positive existence test stay entirely with Flink.

The connector deliberately does not push raw cell-value comparisons.
Bigtable compares encoded bytes rather than decoded SQL values, an empty or sentinel cell may
decode as `NULL`, and a row may hold several timestamped versions while SQL sees only the latest.
Those differences make a raw value filter unsafe as the final SQL answer.
The existence prefilter is composed with projection through a Bigtable conditional row filter;
Google documents conditional filters as non-atomic and warns that they can perform poorly.
Treat this pushdown as an opportunity to reduce returned rows, not as a guarantee that every
cell predicate makes a read faster.

### What a read produces

The cell bytes decode by the same [type mapping](#type-mapping) the write side uses, and nulls
reverse the write-side convention: an empty cell is `NULL` — except in a character-string column,
where the `null-string-literal` is `NULL` and an empty cell is an empty string. A column family
none of whose declared qualifiers has a cell is a `NULL` field, mirroring the sink, whose null
family writes no cells; a family with some cells is a `ROW` whose absent qualifiers are null.
(Flink's HBase connector differs here: it always builds the nested row.)

Two more read-side facts worth knowing:

- **A decimal wider than its column reads as `NULL`.** A cell whose value does not fit the
  declared `DECIMAL(p, s)` decodes as a SQL `NULL` rather than failing — the HBase connector's
  behaviour, silently aliased onto the null convention. A fixed-width cell shorter than its
  declared layout, by contrast, fails the scan with a message naming the cell and its row. As in
  HBase's `Bytes` decoder, trailing bytes after a complete fixed-width value are ignored.
- **Declare a qualifier `NOT NULL` only when every row carries the cell.** The read path cannot
  manufacture a value for an absent cell, so sparse data under a `NOT NULL` column hands the
  planner a null it was told cannot exist; an *empty* cell there fails the scan outright, the
  plain decoder having no null to offer.

### Bounding the scan

`scan.row-prefix`, the legacy `scan.row-range.*` pair and `scan.row-ranges` bound the scan by row
key, server-side, and are additive — overlapping or adjacent selections are merged, so no row is
read twice.
`scan.row-ranges` uses semicolon-separated closed-start, open-end entries such as
`[account-a,account-m);[account-q,)`.
Either endpoint may be omitted, but not both.
Use a backslash before `\`, `;`, `,`, `[`, `]`, `(` or `)` when that character belongs to a UTF-8
endpoint rather than to the range grammar.
The `\\` sequence is the complete input for one literal backslash; it needs no additional prefix.
Malformed, empty, equal or inverted entries fail table validation with their one-based entry
number.
`scan.row-key-encoding = UTF8`, the default, preserves the original text behavior.
Set it to `BASE64` to express exact binary keys using canonical padded RFC 4648 standard Base64.
The Base64 mode rejects the URL-safe alphabet, whitespace, missing or non-canonical padding, and
malformed input.
The standard alphabet does not contain `;`, so the existing separator for multiple prefixes stays
unambiguous: the connector splits the list before decoding each prefix.
Every mode rejects a value that decodes to an empty key because the client would silently widen it,
and "scan everything" is spelled by leaving the option unset.
Supported SQL row-key predicates further intersect this configured union.
An empty intersection returns no rows rather than widening back to the configured range or the
whole table.

### Lookup joins

The table supports processing-time temporal joins by equality on its row-key column. The lookup
key is interpreted after projection, so the row key may appear anywhere in the DDL and the query
may reorder the lookup table's output. Composite keys, nested family fields and predicates that do
not include row-key equality are rejected when the join is planned.

```sql
SELECT e.event_id, p.profile.name
FROM events AS e
LEFT JOIN profiles FOR SYSTEM_TIME AS OF e.proc_time AS p
  ON e.user_id = p.rowkey;
```

By default each input row performs a synchronous Bigtable point read. Set `lookup.async = true`
for asynchronous point reads. A missing Bigtable row produces no lookup result, so a left join
keeps the input row with null lookup columns and an inner join drops it. Both forms apply the same
family projection filter as a scan and use `scan.app-profile-id`; a Data Boost application profile
cannot serve the point-read modes.
Flink does not currently pass an additional right-side temporal-join predicate through
`SupportsFilterPushDown`.
It keeps that expression in the lookup operator, where NONE, PARTIAL and FULL modes evaluate the
same residual predicate.
Configured `scan.row-prefix`, `scan.row-range.*` and `scan.row-ranges` bounds still apply to point
reads and FULL-cache contents.

`lookup.cache = PARTIAL` uses Flink's standard on-demand lookup cache around either the synchronous
or asynchronous function. Configure at least one of `lookup.partial-cache.max-rows`,
`lookup.partial-cache.expire-after-access` or `lookup.partial-cache.expire-after-write` with it.
`lookup.cache = FULL` loads the projected table through a bounded scan into each lookup task; choose
`PERIODIC` with `lookup.full-cache.periodic-reload.interval`, or `TIMED` with
`lookup.full-cache.timed-reload.iso-time`. FULL is synchronous by definition, so combining it with
`lookup.async = true` is rejected. Its scan may use a Data Boost profile. Scan prefix and range
bounds apply consistently to point reads and FULL-cache contents.

Point reads retry only `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `ABORTED`. `lookup.max-retries`
counts retries after the first attempt and defaults to 3. Other failures surface immediately. The
connector adds no lookup-specific metrics; Flink owns cache metrics and the Bigtable client owns
RPC metrics.

Scan, connection and write options map onto builder setters of the DataStream API, which stays their
source of truth. Lookup options are table-layer or Flink-owned instead. An option left out of the
DDL leaves the corresponding setter or lookup setting untouched; the full list of defaults is in
the [configuration reference]({{< relref "docs/reference/bigtable" >}}).

`scan.mode`, `null-string-literal`, `scan.row-key-encoding`, `lookup.async`,
`sink.cell-timestamp.truncate-to-millis` and `sink.insert-only-input-mode` belong to the table layer
because they configure its codec, runtime shape or planner contract rather than a DataStream
builder.

### Destination

| Option | Type | Maps to |
|---|---|---|
| `project` | String | The project part of `table(...)`; a bare project id |
| `instance` | String | The instance part of `table(...)` |
| `table` | String | The table part of `table(...)`. One SQL table writes to one Bigtable table: per-record routing has no SQL surface and stays on the DataStream API |
| `service-account-key-file` | String | Shared credential path mapped to `serviceAccountKeyFile(...)` for the sink, scan and every lookup cache mode. Unset keeps ADC. Every eligible TaskManager must see the path; a scan also needs it on the JobManager. The option is rejected beside `emulator-endpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/bigtable" >}}#credential-file-deployment) |
| `emulator-endpoint` | String | `emulatorEndpoint(...)` as `host:port` — parsed when the planner builds the sink, so a malformed value fails there |
| `null-string-literal` | String | The cell value that stands for a null in a character-string column; defaults to `null`. Not a builder setter: it configures the cell codec this layer supplies, in both directions. Every other type writes a null as an empty cell |

### Scan

| Option | Type | Maps to |
|---|---|---|
| `scan.mode` | Enum | Selects `bounded` (default) or `change-stream`. This table-layer option chooses the source builder rather than calling one setter |
| `scan.app-profile-id` | String | `appProfileId(...)` on the selected source builder. Required for Change Streams. Separate from `sink.app-profile-id`, because a Data Boost profile reads and cannot write, so one table legitimately scans and writes under different profiles |
| `scan.row-key-encoding` | Enum | How row-key prefixes and range endpoints are decoded: `UTF8` (default) or canonical padded RFC 4648 standard `BASE64` |
| `scan.row-prefix` | List of String | `prefix(...)`, once per decoded element. `;`-separated and additive with every range |
| `scan.row-range.start-closed` | String | The inclusive decoded start key of the legacy single `rowRange(...)`. Either bound may be given alone |
| `scan.row-range.end-open` | String | The exclusive decoded end key of that range |
| `scan.row-ranges` | String | Additional `[start,end)` ranges separated by unescaped `;`. Either endpoint may be omitted. Backslash escapes grammar characters inside UTF-8 endpoints |
| `scan.change-stream.changelog-mode` | Enum | Required in Change Streams mode. `envelope` selects the fixed insert-only generic mutation envelope; `selected-cell` emits keyed upserts and deletes under the documented atomic producer protocol |
| `scan.change-stream.selected-cell.family` | String | Required only in selected-cell mode; family that holds the complete serialized logical value |
| `scan.change-stream.selected-cell.qualifier-base64` | String | Required only in selected-cell mode; exact qualifier in canonical padded RFC 4648 standard Base64. An empty decoded qualifier is valid |
| `scan.change-stream.selected-cell.source-cluster-id` | String | Required only in selected-cell mode; the one source cluster accepted for mutations affecting the selected cell |
| `value.format` | String | Required only in selected-cell mode; insert-only Flink format that decodes the selected cell into all non-key physical columns. Its `value.<format>.*` options configure that format |
| `scan.startup.mode` | Enum | `startPosition(...)`: `earliest`, `latest`, or `timestamp`. Unset retains the builder's latest default |
| `scan.startup.timestamp-millis` | Long | Epoch-millisecond instant paired with startup mode `timestamp` |
| `scan.resume-fallback.mode` | Enum | `resumeFallback(...)`: explicit fallback for an expired restored continuation; uses the same three modes |
| `scan.resume-fallback.timestamp-millis` | Long | Epoch-millisecond instant paired with resume-fallback mode `timestamp` |
| `scan.end-timestamp-millis` | Long | `endTime(...)`; makes Change Streams bounded at the epoch-millisecond instant |
| `scan.max-concurrent-streams-per-subtask` | Integer | `maxConcurrentStreamsPerSubtask(...)`; unset keeps the builder default of two |
| `scan.parallelism` | Integer | The scan's parallelism (Flink's own option) |

### Lookup

| Option | Type | Meaning |
|---|---|---|
| `lookup.async` | Boolean | Use asynchronous point reads; defaults to `false`. Cannot be combined with FULL caching |
| `lookup.cache` | Enum | Flink's cache mode: `NONE`, `PARTIAL` or `FULL` |
| `lookup.max-retries` | Integer | Retries after the initial point read for transient failures; defaults to `3` |
| `lookup.partial-cache.expire-after-access` | Duration | Standard PARTIAL-cache access expiry |
| `lookup.partial-cache.expire-after-write` | Duration | Standard PARTIAL-cache write expiry |
| `lookup.partial-cache.cache-missing-key` | Boolean | Whether PARTIAL caches misses |
| `lookup.partial-cache.max-rows` | Long | Maximum PARTIAL-cache rows |
| `lookup.full-cache.reload-strategy` | Enum | FULL reload strategy: `PERIODIC` or `TIMED` |
| `lookup.full-cache.periodic-reload.interval` | Duration | Interval for periodic FULL reloads |
| `lookup.full-cache.periodic-reload.schedule-mode` | Enum | Periodic schedule mode: `FIXED_DELAY` or `FIXED_RATE` |
| `lookup.full-cache.timed-reload.iso-time` | String | Local ISO time for a timed FULL reload |
| `lookup.full-cache.timed-reload.interval-in-days` | Integer | Days between timed FULL reloads |

### Sink

| Option | Type | Maps to |
|---|---|---|
| `sink.app-profile-id` | String | `appProfileId(...)`. Named for the sink rather than shared, because a Data Boost profile reads and cannot write, so one table legitimately scans and writes under different profiles — the scan's profile is `scan.app-profile-id` |
| `sink.create-disposition` | Enum | `createDisposition(...)` — `create-if-needed` or `create-never` |
| `sink.insert-only-input-mode` | Enum | Planner mode for an input containing inserts alone: `upsert` (default) exposes Flink conflict strategies; `insert-only` keeps a plain insert portable but makes `ON CONFLICT` unavailable to that statement |
| `sink.cell-timestamp.truncate-to-millis` | Boolean | Whether the connector drops the sub-millisecond part of writable `timestamp` metadata before sending it; defaults to `false`. Disabled, the connector preserves the value and Bigtable validates its millisecond granularity |
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

A read reverses the convention through the same option —
[What a read produces](#what-a-read-produces) above.

## Delivery guarantees

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics)
for the cross-connector distinction between an insert-only changelog and destination-side
insert-if-absent behavior.

The sink is **at-least-once** and advertises **upsert** by default, including when the requested
input contains inserts alone. A Bigtable write is an upsert on the row key by construction —
`setCell` overwrites — and there is no retract path to offer instead. On Flink 2.x the upsert mode
says a delete may carry the upsert key alone only when the DDL declares the primary key, which is
what makes that key the row key; [The schema](#the-schema) above has the consequence for a job.
Flink 1.20 has no such distinction and always completes the row first.

An append-only row design must generate a unique row key for each logical event, because reusing a
row key updates that row.
An application that keeps cell history within one row must instead choose distinct qualifiers or
stable event timestamps deliberately.
A stable timestamp makes a replay target the same cell version, while omitting the timestamp uses
the writer's wall clock and can create a new version after Flink recovery.

A `-D` deletes the **whole row**, not the declared qualifiers one by one. The row key is the primary
key, so "this key is gone" is what a delete means here; removing only the declared cells would leave
a row behind made of whatever else was in it.

### Flink 2.3 may demand ON CONFLICT

Flink 2.3
([FLIP-558](https://cwiki.apache.org/confluence/display/FLINK/FLIP-558%3A+Improvements+to+SinkUpsertMaterializer+and+changelog+disorder))
refuses to plan into an upsert sink table declaring a `PRIMARY KEY` when the query's upsert key
differs from that key — or when it cannot infer one at all, as for `INSERT INTO .. VALUES` — unless
the statement carries one of the `ON CONFLICT DO DEDUPLICATE` / `DO ERROR` / `DO NOTHING` clauses
Flink 2.3 introduces. The refusal is the planner's, not this connector's: rows with different
upsert keys mapping to one primary key reach the sink in no defined order, and Flink 2.3 asks the
query to say what that should mean instead of materializing silently. An updating query whose
upsert key *is* the primary key plans with no clause, exactly as on 2.2.

The default `sink.insert-only-input-mode = upsert` applies that rule to an insert-only input too.
It exposes all three conflict strategies and reflects the physical write, which overwrites a row
with the same key. On an insert-only input, `DO DEDUPLICATE` adds no materializer and leaves those
writes on the ordinary overwrite path. `DO NOTHING` keeps the first row the *job* observes per key
and `DO ERROR` fails when the job observes a conflict; both require watermarks. Neither inspects
Bigtable, so `DO NOTHING` is not an atomic insert-if-absent — a fresh job, expired state or cleared
state can overwrite an existing row.

For a plain insert that must use the same DDL on Flink 1.20, 2.2 and 2.3, set
`sink.insert-only-input-mode = insert-only`. This table-local compatibility mode restores the
append answer introduced by [#488]({{< param BookRepo >}}/issues/488), so the statement plans
without the 2.3-only clause. In exchange, Flink rejects an `ON CONFLICT` clause on that insert-only
statement because the sink has advertised only INSERT changes. Updating inputs remain upsert and
are unaffected by the option.

`table.exec.sink.require-on-conflict = false` is the supported planner-wide alternative. It lets
2.3 plan a statement without the clause while leaving this sink in upsert mode; measured on 2.3.0,
the plain insert plan then carries `upsertMaterialize=[true]`, while the connector's `insert-only`
mode carries no materializer. Flink 1.20 and 2.2 do not define the setting and ignore it. Prefer
the connector option when the compatibility decision belongs to one Bigtable table and the extra
keyed state is not wanted; use the Flink setting when relaxing the check for every sink planned by
the session is intentional.

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

A job that needs last-write-wins per key cannot obtain it from this sink's submission order. It can
put the version in the row key, or separate dependent mutations into writes whose completion is
awaited before the next begins. Upstream aggregation helps only when it emits at most one mutation
per key for the lifetime of the write; windowed aggregation can still leave requests concurrent.
Google's own guidance lists "multiple mutations to the same row" under when *not* to use batch
writes.

The sink does not serialize same-key entries. A real-service campaign submitted 86,196 same-row
pairs in mirrored arms, across request sizes from 2 through 19,998 entries, and observed no
submission-order reversals on 2026-08-11. That is evidence about the tested service behaviour, not
a contract: the documented arbitrary-order allowance still applies, and a future service or client
change may exercise it. Enforcing one in-flight entry per key would add key-indexed pending state
that grows with active keys and head-of-line blocking to protect a behaviour the campaign did not
observe; ADR-0093 records why [#471]({{< param BookRepo >}}/issues/471) therefore keeps the
existing bulk path and this explicit caveat.

### Cell timestamps

The sink exposes writable metadata named `timestamp` with type `TIMESTAMP_LTZ(6)`.
One value is applied to every cell written by that row; a delete ignores it because `deleteRow`
has no cell timestamp.

```sql
CREATE TABLE profiles_with_event_time (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  cell_timestamp TIMESTAMP_LTZ(6) METADATA FROM 'timestamp',
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profiles_with_event_time
SELECT user_id, ROW(name, email), event_time FROM staged_profiles;
```

Flink casts the metadata column to the advertised `TIMESTAMP_LTZ(6)` type before the sink runtime
receives it.
A declaration below precision 6, such as `TIMESTAMP_LTZ(3)`, is widened without adding fractional
digits.
A declaration above precision 6, such as `TIMESTAMP_LTZ(9)`, is truncated to microseconds by that
cast, independently of `sink.cell-timestamp.truncate-to-millis`.

An absent metadata column or a `NULL` value keeps the existing path: the Bigtable client stamps the
mutation from the TaskManager's wall clock when the mutation is built.
The same `RowMutationEntry` is reused by the client when it retries an RPC, so that retry rewrites
the same cell version.
A Flink recovery serializes the record again, however, and therefore takes a new writer-clock value.
Use a stable event timestamp from the record when the same version must be addressed across job
replay.

Bigtable stores timestamps as epoch microseconds but accepts values only at millisecond granularity.
By default, `sink.cell-timestamp.truncate-to-millis` is `false`: the connector sends the explicit
microsecond value unchanged and lets Bigtable reject a value whose last three digits are nonzero.
Set the option to `true` to opt into dropping those three digits before the mutation is sent.
This is truncation, not rounding; every cell written by the row receives the same truncated value.

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

**The writable metadata surface contains only the cell timestamp.** It applies one value to every
cell of the row and deliberately does not timestamp deletes.
The opt-in truncation option exists because the SQL type can carry microseconds while Bigtable
accepts only millisecond-aligned values; preserving the user's explicit value and letting the
service validate it remains the default.

**Per-record table routing has no SQL surface.** A DDL names one table. Writing to several is a
`STATEMENT SET` of `INSERT`s, one per table, which is what SQL already offers.

**Projection pushdown is family pruning, served by one filter**
([ADR-0092](https://github.com/laughingman7743/flink-connector-gcp/blob/main/docs/adr/0092-the-bigtable-table-source-serves-projection-as-a-family-filter.md)).
Bigtable's read API takes a filter per scan, so a projection is a filter to build rather than an
index list to apply client-side; the edge the ADR pins is the projection retaining no family,
which must become a keys-only chain and not an empty filter. Qualifier-level pruning and a
latest-version filter are compatible follow-ups the ADR names.

## Testing

The emulator suite drives `CREATE TABLE`, `INSERT INTO` and `SELECT` through the production
factory, with the emulator endpoint interpolated into the DDL rather than injected through a
test-only factory, and seeds or reads rows with its own client. The gated real-GCP suite covers the
production-endpoint path that authenticates with application-default credentials;
`sink.app-profile-id` and `scan.app-profile-id`, which the emulator ignores entirely; split
planning, which needs a pre-split real table because the emulator models no tablets; and the family
filter's server-side `NOT_FOUND` for a declared family the table lacks, which the emulator answers
with an empty result instead. The explicit-key path cannot accompany the emulator and does not need
a service RPC to prove credential injection: unit and runtime-boundary tests parse a key file and
inspect every affected client settings family.

Change Streams metadata and `UNNEST(entries)` are covered without a service: converter tests use
the connector-owned mutation model directly, planner tests select and cast metadata, and the existing
`flink-sql-connector-gcp-bigtable` uber-jar plans the same DDL through its discovered `bigtable`
factory.
Selected-cell tests drive the strict mutation classifier and row assembly directly, while a
MiniCluster job executes the canonical upsert, unrelated-mutation, and key-only-delete paths.
The emulator implements no Change Streams RPC, so real-GCP Table API acceptance remains in
[#602]({{< param BookRepo >}}/issues/602).
