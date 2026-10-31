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

# ADR-0194: Change Streams sources fill physical commit-coordinate columns

- Status: Accepted
- Date: 2026-10-31 (the planner measured 2026-10-31 on Flink 2.2.1, 2.3.0 and 1.20.4)
- Issues: [#1960], [#1618], [#1945]
- Modules: bigtable (`table`, `table.source`), spanner (`table`, `table.source`)
- Current behavior: `docs/content/docs/connectors/table/bigtable.md` § A delete's own commit time;
  `docs/content/docs/connectors/table/spanner.md` § A delete's own coordinates;
  `docs/content/docs/examples/bigtable.md` § Replicating a selected cell into BigQuery;
  `docs/content/docs/examples/bigquery.md` § Native Change Streams SQL

## Context

[ADR-0193] made a BigQuery CDC write carry a change sequence, and the two examples that feed one
from a Change Streams source build it from the source's commit coordinates: the Bigtable
selected-cell example from the `commit-timestamp` and `tie-breaker` metadata columns, the Spanner
example from `commit-timestamp`, `sequence` and `mod-number`.
Both sources emit an upsert changelog whose delete carries the key alone, and reading a metadata
column of such a source makes the planner put a `ChangelogNormalize` operator after it.
That operator completes a key-only delete from the last row it holds for the key
(`DeduplicateFunctionHelper#processLastRowOnChangelog`), metadata included, so the delete carries
the sequence of the change it deletes; and it drops the delete of a key it holds no row for.

[#1945] kept `OrderingMode.PER_TABLE` for the resulting tie, and the Bigtable example documented
three cases in which a delete is still lost and the row stays in BigQuery: a change read across a
Change Streams partition split or merge reaching the operator in the opposite order, an append that
timed out on the client and was committed after the delete, and a key the operator holds no row
for. [#1960] asked for a delete that carries its own coordinates.

## Decision

- **Each source fills named physical columns with the commit coordinates of every row it emits, a
  key-only delete included.** Bigtable's selected-cell mode takes
  `scan.change-stream.commit-timestamp-column` (`TIMESTAMP_LTZ(9)`) and
  `scan.change-stream.tie-breaker-column` (`INT`); Spanner's change-stream mode takes
  `scan.change-stream.commit-timestamp-column` (`TIMESTAMP_LTZ(9)`),
  `scan.change-stream.sequence-column` (`STRING`) and `scan.change-stream.mod-number-column`
  (`INT`). Each key is the existing metadata key with `-column` appended, and the value is the
  same one that metadata column reads, through the metadata column's own reader. The Bigtable keys
  carry no `selected-cell.` segment although only selected-cell mode accepts them, as
  `value.format` carries none: the spelling is shared with Spanner's, and the mode that accepts
  them is enforced rather than spelled.
- **The columns are taken out of what the source decodes.** Bigtable's `value.format` does not
  decode them; Spanner neither validates a record against them nor expects one to carry them, so
  the table's schema converter is built over the remaining columns and each converted row is
  widened back to the declared one.
- **A named column must be a declared, non-key physical column of exactly that type**, checked when
  the source is created and named under the option key ([ADR-0127]'s parsed-value shape). The
  commit timestamp is held at nanosecond precision: a millisecond column would let two changes of
  one key inside a millisecond compare equal. Nullability is not checked, since the column is
  filled on every row. The types exclude each other, so two options cannot name one column.
- **On Spanner, a coordinate column is neither marked nor watched.** A type marker
  (`json-field-paths`, `uuid-field-paths`, `generated-columns`, `proto-type-names`,
  `enum-type-names`) naming one is refused under both option keys, since the schema the marker is
  validated against no longer holds the column. A record that carries a watched column of a
  coordinate column's name fails deserialization: the coordinate would otherwise replace the
  watched value on every row without a word, [ADR-0127]'s silent per-record shape. Bigtable has no
  such check, because a format payload is not inspected field by field; a payload field of a
  coordinate column's name is simply not decoded, and the reference page says so.
- **The options are refused wherever the source does not run**: on a bounded table, on a sink, and
  in Bigtable's envelope mode, with the other options of those modes. **They are refused in
  Spanner's `full` mode too**: a retraction must repeat the row it withdraws, and an
  `UPDATE_BEFORE` or `DELETE` would carry the retracting change's coordinates instead, so a
  downstream `GROUP BY` on a coordinate would retract from the wrong group. `full` mode plans no
  `ChangelogNormalize` unless `table.exec.source.cdc-events-duplicate` is set
  (`DynamicSourceUtils#isSourceChangeEventsDuplicate`, Flink 2.2.1), so there the metadata columns
  already serve it.
- **The 1.20 build accepts them too.** The owner decided on 2026-10-31 that the options behave
  the same on every build and the documentation says that the three cases remain on 1.20, rather
  than refusing the options there.

## Evidence

Flink 2.2.1's `ChangelogNormalizeRequirementResolver#isRequired` keeps the operator when it has a
filter condition, when update-before rows are required, when
`table.exec.source.cdc-events-duplicate` is set, or when a column the query needs is a metadata
column of a `SupportsReadingMetadata` source; otherwise
`FlinkChangelogModeInferenceProgram` removes it when the source's delete kind satisfies the sink's.
Flink 1.20 has no such removal.

A scratch probe declared an upsert source with key-only deletes whose two coordinate columns were
physical and filled on every row, fed it an upsert of key `k1` at t1, a delete of `k1` at t2 and a
delete of `k2` at t3, and planned the documentation's sequence expression into the BigQuery CDC
sink, then ran the same projection.

| Flink | Plan | `k1` delete's sequence | `k2` delete |
|---|---|---|---|
| 2.2.1 | scan, calc, sink | its own (t2) | reaches the sink |
| 2.3.0 | scan, calc, sink | its own (t2) | reaches the sink |
| 1.20.4 | `ChangelogNormalize` | `k1`'s upsert (t1) | dropped |

Declaring the coordinate columns `NOT NULL` changed nothing on 2.2.1 or 2.3.0: the
[FLINK-40477](https://issues.apache.org/jira/browse/FLINK-40477) and
[FLINK-40528](https://issues.apache.org/jira/browse/FLINK-40528) failures need a `NULL` in a
`NOT NULL` column, and these columns carry a value.
`SelectedCellCommitColumnsPlanTest` and `SpannerChangeStreamCoordinatePlanTest` pin both plan
shapes per build, with the metadata-column query as the control that keeps the operator everywhere.
`SpannerTableSourceITCase` runs an insert, an update and a delete of one key through the emulator in
`upsert` mode and finds the delete carrying its own commit timestamp on 2.x and the update's on
1.20; a credential-gated Bigtable test finds four selected-cell changes of one key, the delete
included, carrying strictly increasing commit timestamps on the service.

## Alternatives declined

- **Keep the metadata columns and document the cases.** That was [#1945]'s state; the three cases
  are silent losses, and on Flink 2.x a source change removes them.
- **Declare full deletes (`ChangelogMode.upsert(false)`).** The delete's non-key columns are `NULL`,
  so the source would claim a complete row it does not have, and reading a metadata column keeps the
  operator whatever the delete kind is.
- **One option mapping any metadata key to a physical column.** Only the ordering coordinates are
  needed on a delete; the other metadata either has no ordering role or, like Bigtable's estimated
  low watermark, would invite reading partition state as a row value.
- **Have the source format the BigQuery sequence.** It would couple the Bigtable and Spanner
  connectors to one sink's encoding; the coordinates are what each source owns, and the sink-side
  encoding already exists for Spanner (`spanner-change-sequence`).
- **Refuse the options on the 1.20 build.** Declined by the owner: the columns are not wrong there,
  only insufficient, and the documentation states what remains.

## Consequences

- On Flink 2.x a query that reads the coordinate columns and no metadata column, filters nothing
  and needs no update-before rows hands each delete to the sink with its own coordinates, and the
  delete of a key the job never saw reaches the sink.
- Reading any metadata column, filtering on a non-key column or aggregating brings the operator
  back; the documentation says so beside the options.
- A key-only delete is no longer `NULL` in every non-key column: the coordinate columns are
  filled.
- A catalog table cannot declare the columns; the hand-written table that Flink 2.2 and 2.3's bugs
  already call for is where they go.
- The examples keep `'sink.ordering-mode' = 'per-table'` for the 1.20 build.

[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1945]: https://github.com/flink-gcp/flink-connector-gcp/issues/1945
[#1960]: https://github.com/flink-gcp/flink-connector-gcp/issues/1960
[ADR-0127]: 0127-a-configured-name-is-checked-for-what-this-project-will-do-with-it.md
[ADR-0193]: 0193-bigquery-cdc-writes-require-a-change-sequence-number.md
