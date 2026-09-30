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

# ADR-0172: Bigtable column families may be declared as maps

- Status: Accepted
- Date: 2026-09-27; refined 2026-09-28 (option name, keep-latest mutation counts, real-Bigtable measurement)
- Issues: [#1215](https://github.com/flink-gcp/flink-connector-gcp/issues/1215) (under
  [#1212](https://github.com/flink-gcp/flink-connector-gcp/issues/1212); prerequisite of
  [#1216](https://github.com/flink-gcp/flink-connector-gcp/issues/1216))
- Modules: bigtable, docs-validation
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Context

The Bigtable Table DDL is Flink's HBase connector's (ADR-0086, incorporated by ADR-0102): one atomic column is the row key and every `ROW<qualifier type, ...>` column is a column family.
That form needs every qualifier and its type in the DDL.
Bigtable stores neither; a table's metadata names its column families and, for an aggregate family, the value type.
No `ROW` schema can therefore be derived from metadata alone, which is what a catalog over Bigtable tables (#1216) has to do. HBase has the same gap: FLIP-117 proposed a catalog for Flink's HBase connector, and `apache/flink-connector-hbase` ships none (its source tree, read 2026-09-28, has no catalog class).
The `ROW` form also cannot read a family whose qualifiers are data (a date, an identifier), because it names every qualifier in advance.

GoogleSQL for Bigtable answers the same problem by presenting each family as a `MAP` from qualifier to the latest value, with keys and values as `BYTES`, and a `with_history` variant `MAP<key, ARRAY<STRUCT<timestamp, value>>>`.
Flink's HBase connector has no `MAP` form: `HBaseTypeUtils.isSupportedType` rejects `MAP`, and `HBaseTableSchema` accepts only `ROW` families and one atomic row key.

## Decision

**A top-level `MAP<K, V>` column is the column family of that name.**
The row key becomes the one column that is neither a `ROW` nor a `MAP`, both family forms may appear in one DDL, and the `ROW` form and its byte encoding are unchanged.
The map form is this connector's addition; a DDL using it does not move to the HBase connector.

**The key is `STRING`/`VARCHAR` or `BYTES`/`VARBINARY`.**
`BYTES` is GoogleSQL's shape and keeps qualifier bytes as they are; `STRING` reads them as UTF-8 through `StringData.fromBytes`, which keeps the bytes and validates nothing.
`CHAR` and `BINARY` are rejected because their fixed length would pad or truncate a qualifier.
A null key fails the record, since a cell has no null qualifier.

**The value is any type the cell codec encodes, one type per family, under the `ROW` form's null convention.**
`MAP<BYTES, BYTES>` is GoogleSQL's shape and reads any family.
A typed value is how a Flink query reads a number out of a cell: Flink has no `CAST` from `BYTES` to a numeric type, so GoogleSQL's "cast the value in the query" works in Flink for strings alone.
The codec's `BIGINT` layout (eight big-endian bytes) is the layout Bigtable's Int64 aggregate encoding uses, so `MAP<BYTES, BIGINT>` reads SUM, MIN and MAX aggregate families and `MAP<BYTES, BYTES>` reads HLL state; this is the mapping #1216 derives from `ColumnFamily` value types.
A map value that is an `ARRAY`, a `MAP` or a `ROW` is rejected, with the `with_history` shape named separately.

**A read holds the latest version of each qualifier, and a family with no cell reads as `NULL`.**
The latest version is the first cell per qualifier, as for a `ROW` family (ADR-0092).
`NULL` rather than an empty map mirrors ADR-0092's rule for a `ROW` family, and keeps the family-existence prefilter `BigtableFilterPushDown` already pushes for `m IS NOT NULL` exact.
Projection needs no change, because the family filter retains a map family whole.
An entry access (`m['q']`) is an `ITEM` call rather than a nested field reference, so it stays residual without any change to the pushdown.
`BYTES` keys live in a tree map ordered by unsigned comparison, the order Bigtable sorts qualifiers in, because a `byte[]` hashes by identity: a hash map misses the latest-wins check and keeps one entry per version, and `GenericMapData.get` is a `Map.get`, which is how Flink's generated code reads `m[key]` from one.

**A write turns each entry into one cell, and `sink.map-family.update-mode` decides what happens to the family's other qualifiers.**
Settled with the owner on 2026-09-27, after both behaviours were weighed:

- `merge` (the default) writes `SetCell` per entry and nothing else. It is Bigtable's own write, matches the `ROW` form's rule that undeclared qualifiers are preserved, and supports a family that accumulates qualifiers across writes. Its cost is that no SQL write removes an entry.
- `replace` puts `DeleteFromFamily` before the entries' `SetCell`s in one row entry. A row entry's mutations apply in order and atomically, so the family reads back as the written map, and an empty map clears it.

A null map leaves the family untouched under either choice, as a null `ROW` family does.
Under `merge` an empty map adds no mutation and counts toward the existing rejection of a row that writes nothing.
The explicit `timestamp` metadata applies to every entry and its absence takes ADR-0149's per-cell writer clock, as for `ROW` qualifiers.
Keep-latest deletes each written column under `merge`; behind a family delete the column delete removes nothing more, so `replace` omits it.
The staged runtime (ADR-0163) accepts `DeleteFromFamily` on a data family, and its `-D` already deletes every declared family, a map family included.

**Map families are written in `upsert`, `keep-latest` and `insert-if-absent` and under checkpoint-owned delivery.**
`append`, `increment` and `aggregate` reject a map family: their per-qualifier rules would need a per-entry form, and a family with no declared qualifiers would otherwise pass their type checks vacuously.
The update-mode option is rejected under every mode but `upsert` and `keep-latest` (under `insert-if-absent` the row is absent, so there is nothing to replace), and on a table that declares no map family, where it could change nothing.

## Evidence

Measured 2026-09-27 against the pom-pinned Flink 2.2.1 and the Bigtable emulator, and on
2026-09-28 against real Bigtable:

- **Aggregate state reads through a map family** (real Bigtable, gated
  `BigtableAggregateTableRealGcpITCase.aggregateStateReadsBackAlikeThroughMapRowAndGoogleSql`, an
  ephemeral one-node instance, 81 s): contributions of 3 and 5 written by the aggregate sink read
  as SUM 8, MIN 3 and MAX 5 through `MAP<BYTES, BIGINT>`, through a `ROW<q BIGINT>` read DDL and
  through GoogleSQL's `totals['q']` alike, and HLL state reads through `MAP<BYTES, BYTES>` and
  `ROW<q BYTES>` as the stored cell bytes. The emulator has no aggregate families, so this is the
  only measurement of the claim that the codec's `BIGINT` layout is the service's Int64 encoding.

- flink-table-planner 2.2.1's `ExprCodeGenerator` routes `ITEM` on a `MAP` to `ScalarOperatorGens.generateMapGet`, whose non-binary branch casts to `GenericMapData` and calls `get`.
- With the converter's tree map replaced by a hash map, the emulator ITCase read a two-version `BYTES` qualifier as `CARDINALITY` 2, and four converter unit tests failed.
  An `m[x'71']` lookup in the same ITCase still answered correctly, even with `pipeline.object-reuse` enabled, so the lookup is not what pins the tree map; the version count is.
- The emulator ITCase reads cells written through a `ROW` DDL through a `MAP` DDL and gets what the `ROW` DDL gets, `CAST(cf['name'] AS STRING)` and `counts[x'61']` included; a key the next map omits survives a `merge` and disappears under `replace` across two jobs.
- The array-element encoder reads an element through Flink's `ArrayData.createElementGetter` and encodes it with the field encoder of the same type, so there is one layout switch; `CellValueCodecTest` holds both paths to the same bytes for every supported type root, from a generic and from a binary array, including a decimal too wide for the compact layout.
- A map family makes the mutations per entry data-dependent, and google-cloud-bigtable 2.82.0 bounds them twice: `Mutation` refuses the 100,001st as it is added ("Too many mutations per row"), and `RowMutationEntry.toProto()` checks the same bound again. `Mutation` also refuses a mutation that would take the entry past 200 MiB ("Byte size of mutations is too large") at the same point, so a map of few but large values fails the same way. The first fires inside `serialize()`, so the writer routes an oversized map as a serialization failure, never reaching the `toProto()` it calls outside that path; the serializer adds no check of its own. The staged writer (ADR-0163) has no routing path and bounds a staged entry at 99,999 mutations plus its marker, so there an oversized map fails the job. The test pins that an entry at the bound builds its proto and one past it fails inside `serialize()`.

## Alternatives declined

- **`BYTES` values only.** It is enough for a catalog, but a Flink query could then read no number out of a map family.
- **`merge` only, or `replace` only.** Each loses a real use: `merge` cannot remove an entry from SQL, and `replace` cannot accumulate qualifiers. The owner chose the option with the conservative default.
- **An empty map for a family with no cell.** It would make the pushed `m IS NOT NULL` prefilter drop rows the residual predicate keeps.
- **Map families in `append`, `increment` and `aggregate`.** Deferred until asked for; each needs a per-entry rule and its own tests.
- **Naming the option `sink.map-family.write-semantics`.** No other option in the repository uses `-semantics`, and the Bigtable sink options nearest it in shape, `sink.write-mode` and `sink.insert-only-input-mode`, end in `-mode`, so the owner chose `update-mode` (2026-09-28); `write-mode` was avoided because `sink.write-mode` already selects the operation. Enum options are not uniformly suffixed (`sink.delivery-guarantee`, `sink.create-disposition` and `decode.trailing-bytes` are among the exceptions), so this follows the nearest siblings rather than a repository-wide rule.
- **An update-mode setting per family.** `sink.aggregate.column-family-types` shows the connector can key an option by family, but no case yet needs one table to merge one map family and replace another; a per-family option can be added later beside the table-wide one.

## Consequences

- Two writes of one row in one `MutateRows` request have no defined winner (ADR-0086). Under `replace` the family reads back as one of the two maps, atomically, but not necessarily the later one.
- The `with_history` form is a compatible follow-up: it changes what a write means (every entry carries versions) and needs a version filter on the read.
- Pushing `m['q'] IS NOT NULL` as a qualifier-existence prefilter is a compatible follow-up; today it is residual.
- Typed qualifiers from a catalog remain out of scope for #1216; its schema is one `MAP` per family.
