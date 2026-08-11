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

# ADR-0086: The Bigtable table layer maps onto the builders over an HBase-compatible DDL

- Status: Accepted
- Date: 2026-08-10
- Issues: [#458](https://github.com/laughingman7743/flink-connector-gcp/issues/458) (under
  [#217](https://github.com/laughingman7743/flink-connector-gcp/issues/217); ADR-0014 holds the
  shared mapping rules)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Context

`flink-connector-gcp-bigtable` had a DataStream sink (ADR-0041) and a bounded scan source
(ADR-0080) and nothing reachable from SQL. A Bigtable cell is an uninterpreted byte string, so a
table layer cannot avoid choosing both a schema model — how a DDL says which family and qualifier a
column addresses — and a byte encoding for the cells themselves. Neither choice is free: this
connector inherits users from Bigtable-via-HBase, and `apache/flink-connector-hbase` has no Flink
2.x release, so whatever is chosen here is what that population moves onto.

The `table` layer is a *mapping* onto the DataStream builders, never a second implementation: the
Pub/Sub rules (ADR-0014) apply unchanged. What follows is this module's own.

## Decision

**The DDL model and the cell encoding are Flink's HBase connector's, and the encoding is
normative.** Exactly one column is not a `ROW` and is the row key; every `ROW` column is a column
family whose nested fields are its qualifiers; cell bytes are `org.apache.hadoop.hbase.util.Bytes`
as `HBaseSerde` applies it, plus `null-string-literal`. The encoding is reproduced rather than
depended on — `hbase-common` drags in Hadoop — and it has the status ADR-0026 gives the protobuf
mapping: a golden-vector test pins each type to an exact byte array, because a round trip through
this connector's own code would pass while the interop the choice was made for was broken.

**`BigtableTableSchema` and `CellValueCodec` live at the `table` package root**, beside the options
class and the factory, rather than in a `table.codec` subpackage. Both are shared by `table.sink`
and `table.source`, and neither direction package may import the other. This is ADR-0055's
module-root rule applied one level down; a `table.codec` layer would fail [#119](https://github.com/laughingman7743/flink-connector-gcp/issues/119)'s test, since the
only sibling schema model in prospect was declined on [#34](https://github.com/laughingman7743/flink-connector-gcp/issues/34).

**The changelog is an upsert for an updating query, and a `-D` deletes the whole row.** A Bigtable
write is an upsert on the row key by construction, so there is no retract mode to offer instead.
The row key is the primary key, so a delete means the key is gone; removing only the declared cells
would leave a row behind made of whatever else was in it.

**An insert-only query is answered with insert-only, and the answer is load-bearing on Flink 2.3**
([#488](https://github.com/laughingman7743/flink-connector-gcp/issues/488), refining the
unconditional upsert answer this ADR first recorded). FLIP-558 changed the 2.3 planner's
upsert-materialize analysis — measured against the 2.2.1 and 2.3.0 `flink-table-planner` sources:
2.2 returned early when the *input* was insert-only, 2.3 dropped that early return and, with the
new `table.exec.sink.require-on-conflict` defaulting to `true`, refuses to plan a query whose
upsert key differs from the sink's `PRIMARY KEY` — a set that includes every query the planner
cannot infer an upsert key for, `INSERT INTO .. VALUES` first among them — unless the statement
carries one of the new `ON CONFLICT DO ..` clauses. The analysis still returns early when the
*sink* accepts only inserts, which is the seam this layer uses: an append answer keeps every
insert-only statement planning on 2.3 exactly as it did on 2.2, while an updating query keeps the
upsert answer and, where its upsert key genuinely differs, meets Flink's demand as designed — the
docs page carries what to write then. The demand is narrower than the failure made it look:
upsert-key inference is unique-key metadata, so a retract source *with a declared key* satisfies
it — measured on 2.3.0, `aChangelogDeleteRemovesTheWholeRow`'s retract statement plans with no
clause, no escape option and no materializer, because its view's `primaryKey("k")` maps onto the
sink's `PRIMARY KEY` — which is why the ITCase suite needs no escape anywhere. This is also why
the BigQuery and Pub/Sub table layers never met the change: both answer `insertOnly()`
throughout, and neither DDL takes a `PRIMARY KEY`. **The append answer is Flink's own HBase connector's**, which this layer's DDL model already
follows: `HBaseDynamicTableSink.getChangelogMode` echoes the requested kinds back minus
`UPDATE_BEFORE`, so an insert-only query gets an insert-only answer there too. The shape here is
kept rather than replaced by that echo, because the echo cannot carry #470's key-only-deletes
flag; the two agree on every input either can express.

The append answer has one measured cost: 2.3 validates an `ON CONFLICT` clause against the sink's
mode before the strategy analysis, so an insert-only statement carrying one is rejected with "ON
CONFLICT clause is only allowed for upsert sinks". What that costs, measured behaviour by
behaviour on 2.3.0 against the unconditional-upsert answer: `DO DEDUPLICATE` costs **nothing** —
the planner returns early for it on append input (`inputIsAppend && isDeduplicateConflictStrategy`),
so the plan carried no materializer and was the plain-`INSERT` plan already; `DO NOTHING` and
`DO ERROR` did materialize (`upsertMaterialize=[true], conflictStrategy=[NOTHING]`/`[ERROR]`) and
are genuinely lost, both being further gated on every source table declaring a watermark.
Accepted: the alternative broke every plain insert into a primary-keyed table, the docs page's
first example among them, and an append query wanting first-wins can express it in the query.
An *updating* query keeps the clause — measured, `DO DEDUPLICATE` plans — so the gap is confined
to insert-only statements, and it is
[#496](https://github.com/laughingman7743/flink-connector-gcp/issues/496), which prices the one
local mechanism (a DDL option forcing the upsert answer, inert on two of the three supported
versions) against what `DO NOTHING` measurably is: a job-local, TTL-able materializer state, not
a probe of the table, so it does not give the insert-if-absent semantics its name suggests. Nothing else moves — the pre-sink keyed shuffle reads the declared primary key and the
parallelism, not the sink's answer (measured in 2.3.0's `CommonExecSink.applyKeyBy`), and
compiled-plan restore reads the serialized changelog mode rather than asking the sink again.
`BigtableDynamicSinkTest.anInsertOnlyQueryIsConsumedAsInsertsAlone` pins the answer on the floor
version, where the two answers otherwise plan identically and the ITCase cannot show the
difference.

**Whether a delete may carry the upsert key alone is answered by the DDL's primary key**
([#470](https://github.com/laughingman7743/flink-connector-gcp/issues/470)). Declaring one makes
that key the row key, which the factory enforces, so the key alone is everything `deleteRow` reads.
Declaring none is allowed — an HBase DDL has to move across unchanged — and the planner then keys
its upserts on whatever the query is unique by, which need not be the row-key column, so the sink
asks for whole rows instead. Measured on Flink 2.2.1 against an upsert source keyed on a non-row-key
column: with a key declared the plan already carries `ChangelogNormalize` and `upsertMaterialize`;
with none, answering `false` is what puts `ChangelogNormalize` there and fills the row key in; and
an insert-only query into the same table got neither operator — the observation
[#488](https://github.com/laughingman7743/flink-connector-gcp/issues/488) later hardened into an
explicit insert-only answer. Answering `true` unconditionally, as this layer did until #470,
sends that delete to `RowDataSerializationSchema` with a null row key — measured end to end against
the emulator, the job fails with "The row-key column 'rowkey' is null", loud rather than silent but
dying on every delete.

**What the completion costs beyond state**: `ChangelogNormalize` completes a delete from what *this
job* has seen, so a `-D` for a key the job never inserted is dropped rather than applied. That is
the planner's behaviour, not this connector's, and it already applied to every table declaring a
primary key; #470 extends it to those that do not. It is the reason
`BigtableTableSinkITCase.aChangelogDeleteRemovesTheWholeRow` uses a retract source, having been
written before #470 when a PK-less table had no normalize, while the #470 test rides the insert
and the delete on one upsert stream.

`ChangelogMode.upsert(boolean)` and `keyOnlyDeletes()` arrive in Flink **2.1** — verified absent
from the 2.0.0 sources and present in 2.1.0 — and do not exist on 1.20, so the answer
goes through `CrossVersionChangelogMode` in the per-major source roots — package-private beside
its only caller, as `CrossVersionCheckpointId` is — and nothing in the code or the tests may name either method
directly. 1.20 has no key-only concept at all and was never exposed: its planner completes the row
before every delete regardless. The *sink* behaves identically on both — it reads only the row key
on a delete — but the *planner* does not, which a test discovered: an upsert source feeding this
sink gets a stateful `ChangelogNormalize` on 1.20 and, before #470, none on 2.x, so an integration
test proving `deleteRow` has to use a retract source to run on both.

**Four record-level conditions fail the record rather than skipping it**: an `UPDATE_BEFORE` row,
a null row key, a row key encoding to zero bytes, and a row whose every column family is null — the
last of which would otherwise reach the service as a mutation-less entry and come back as an
`INVALID_ARGUMENT` naming neither the row nor the reason.

**Table creation takes the families from the DDL and the garbage-collection rule from two keys**,
`sink.table-create.gc-rule.max-versions` and `.max-age`, unioned when both are set and applied
uniformly to every family. At least one is **required** under `create-if-needed`, which the
DataStream API does not require.

**`BigtableOptionParityTest` covers three surfaces rather than the options builders alone**:
`BigtableWriterOptions.Builder`, whose every knob has a key and which carries no exemption;
`BigtableSinkBuilder`, carrying an exemption set whose entries state why a setter has no DDL form;
and `TableCreateOptions.Builder`, whose one setter is wholly exempt, the families being the DDL's
`ROW<...>` columns rather than a key. `BigtableSourceBuilder` waited for the `scan.*` surface —
exempting every one of its setters first would have meant writing reasons scheduled to stop being
true — and joined as the fourth surface when
[#459](https://github.com/laughingman7743/flink-connector-gcp/issues/459) landed (ADR-0092). Two further
assertions ride along: no option feeds two setters, and the declared options equal the mapped ones
in **both** directions, so an option with no home fails and so does a table entry naming a key that
no longer exists.

## Evidence

Measured 2026-08-10 against the pom-pinned `flink.version` 2.2.1 sources jars,
`google-cloud-bigtable` 2.80.0, `hbase-common` 2.6.6 and `flink-connector-hbase-base`
4.0.0-1.19:

- **The whole Table API surface this layer needs is in `flink-table-common`**, up to and including
  `DefaultLookupCache`, which reaches shaded Guava through `flink-core` and exposes nothing from
  `flink-table-runtime` in its public signature. So the module's only new compile-scope dependency
  is `flink-table-common` at `provided`, and `flink-table-runtime` stays test scope. Nothing this
  layer imports needs a `scripts/flink-api-tiers.toml` entry.
- **`Context.createTypeInformation(DataType)` returns exactly the `InternalTypeInfo` the planner
  would have built**, through a `@PublicEvolving` interface — which is how the source side of [#459](https://github.com/laughingman7743/flink-connector-gcp/issues/459)
  satisfies `ResultTypeQueryable` without that artifact.
- **Two upstream references disagree about `DATE` and `TIME`, and only one is the interop target.**
  `HBaseSerde` — the `RowData` path, what a Flink SQL HBase job writes — encodes a `DATE` as a
  four-byte day count and a `TIME` as a four-byte millisecond-of-day, while `HBaseTypeUtils`, the
  legacy `Row`/`java.sql` path, uses eight-byte epoch millis for both. The golden vectors follow
  `HBaseSerde`.
- **`Bytes.toBytes(boolean)` writes `0xFF` for true, not `0x01`**, and there is no
  `Bytes.toBytes(byte)`: a `TINYINT` routed through an overload would widen to `short` and silently
  produce two bytes. Both are pinned.
- **A colon in a column-family name survives escaping.** `family().exactMatch(v)` quotes through
  RE2's `QuoteMeta`, but `familyNameRegexFilter` refuses a colon even backslash-escaped, so such a
  family could be written and never selectively read. It is rejected when the sink is built.

## Alternatives declined

- **Upstream google/flink-connector-gcp's `value.format` per family** — declined on [#34](https://github.com/laughingman7743/flink-connector-gcp/issues/34): it cannot
  give a single qualifier its own type, and it ties a family to a format.
- **Depending on `hbase-common` for `Bytes`** — nine one-line encoders against a Hadoop dependency
  tree in a module headed for a SQL uber-jar.
- **Deleting the declared qualifiers one by one on a `-D`**, which is what Flink's HBase connector
  does. It leaves a row behind made of whatever the DDL does not declare, which contradicts the
  primary key the changelog is keyed on.
- **Dropping a record whose row key is empty**, which is also what that connector does. A silent
  drop leaves an incomplete table under a green job, and the emulator accepts an empty key while the
  service does not — so failing is what keeps the two agreeing.
- **Allowing `create-if-needed` with no garbage-collection rule**, which the DataStream API allows.
  A rule-less family keeps every version forever, and an at-least-once upsert sink writes another
  version of the same cells on every replay.
- **Defaulting the rule to `maxVersions(1)` instead of requiring one.** That would be the table
  layer inventing a default rather than mapping one, which is exactly what ADR-0014's
  no-default-restated rule exists to prevent.
- **Metadata columns for the cell timestamp** — deferred rather than decided here; [#473](https://github.com/laughingman7743/flink-connector-gcp/issues/473).
- **`SupportsPartitioning`** — a Bigtable table is partitioned by row-key range, which the service
  chooses and moves; there is nothing a `PARTITIONED BY` clause could name.

## Consequences

**Two rows for one key inside one `MutateRows` request have no defined winner.** The proto's own
contract is that entries "may be applied in arbitrary order (even between entries for the same
row)", and two writes sharing a millisecond also share a cell timestamp and so collapse to one
version. That is a property of batching over this API rather than of this layer, and it is stated on
the docs page instead of being papered over ([#471](https://github.com/laughingman7743/flink-connector-gcp/issues/471) asks whether the sink should enforce an
order); a test asserting an in-batch winner would be asserting
the emulator's submission order, which is why the integration tests that need an order use separate
**jobs**.

Separate *requests* are not enough, and `sink.batching.element-count` = `1` is not the escape hatch
it looks like: measured on #470's follow-up, forcing one entry per request made a delete stop taking
effect on the 1.20 build, because the requests one job has in flight are concurrent rather than
ordered. Where two jobs are impossible — the table layer's delete test, since `ChangelogNormalize`
knows only what its own job has seen — the assertion has to be order-independent instead.
`aKeyOnlyDeleteRemovesTheRowWhenNoPrimaryKeyIsDeclared` asserts that the job *completes*, which is
exactly what #470 bought, and leaves the row's final state unasserted.

**The cell timestamp is the writer's clock**, not the server's: the client library's three-argument
`setCell` stamps `System.currentTimeMillis()`, which is what makes a retried mutation rewrite the
same version rather than add one. Idempotent retry is worth more here than monotonicity, and the
docs page says which the user gets.

`sink.max-consecutive-rejections` is mapped but inert from SQL: a DDL has no failure-policy option,
so the sink always fails the job on the first confirmed rejection. Mapping it anyway keeps the
`BigtableWriterOptions.Builder` parity surface exemption-free, and the docs page says it is inert —
an exemption reading "currently unreachable" is the kind that goes stale silently.

The DDL surface is deliberately narrower than the DataStream API in one place — the
garbage-collection rule — and stricter in two: the rule is required, and an empty row key fails.
Each is a decision to engage rather than a gap to close.
