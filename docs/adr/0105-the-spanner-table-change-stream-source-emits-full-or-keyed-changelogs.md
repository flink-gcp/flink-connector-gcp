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

# ADR-0105: The Spanner table Change Stream source emits full or keyed changelogs

- Status: Accepted
- Date: 2026-08-13
- Issues: [#582](https://github.com/flink-gcp/flink-connector-gcp/issues/582), [#583](https://github.com/flink-gcp/flink-connector-gcp/issues/583) (under [#225](https://github.com/flink-gcp/flink-connector-gcp/issues/225)), [#635](https://github.com/flink-gcp/flink-connector-gcp/issues/635)
- Modules: spanner
- Current behavior: `docs/content/docs/connectors/table/spanner.md`

## Context

The DataStream Change Streams source exposes self-describing `DataChangeRecord` objects and checkpoints Spanner's partition lineage.
The Table API needs relational changelog rows instead, but the safe row shapes depend on the stream's value-capture mode and on whether the DDL supplies a primary key.
A stream can watch several tables, and the schema descriptors carried by an older record can differ from the current table DDL.

## Decision

**`scan.mode` selects one of two independent scan sources and defaults to `bounded`.**
`change-stream` creates an unbounded `ScanTableSource` over `SpannerChangeStreamSource`; it does not advertise bounded projection or filter pushdown and cannot serve lookup joins.
The factory rejects options owned by the other mode instead of accepting settings that would have no effect.
The same rule makes the table source-only on the sink path, which [ADR-0096](0096-the-spanner-table-sink-maps-ddl-rows-to-native-mutations.md) records.

**One DDL maps to one exact dialect-aware native table name.**
GoogleSQL catalog names compare case-insensitively, while PostgreSQL preserves the distinction established by quoted identifiers.
A valid record for another watched table emits no row and still advances source progress.
Extra watched columns are ignored, but every physical DDL column must have a compatible descriptor in the record.
Named PROTO and ENUM descriptors must also carry the declared fully qualified type name.

**The DDL chooses `full` or `upsert` explicitly.**
`full` accepts only `NEW_ROW_AND_OLD_VALUES` and declares insert, update-before, update-after, and delete kinds.
An update-after row comes from the complete new row; its adjacent update-before row copies that row and replaces every reported modified value with its old value.
Deletes contain the complete old row.
`upsert` accepts `NEW_ROW` and `NEW_ROW_AND_OLD_VALUES`, requires a declared primary key matching the record's key metadata, and declares insert, update-after, and key-only delete kinds.
The Flink 1.20 compatibility seam uses that version's sole upsert declaration, while Flink 2.x passes `true` to the newer API's key-only-delete flag.

**The record's recursive type descriptors and JSON value encodings are authoritative.**
The converter validates them against the existing Table API physical mapping, creates typed Spanner values, and reuses the bounded source's `Struct` to `RowData` converter.
This keeps exact decimal, UUID, JSON, PROTO, ENUM, timestamp, array, and null behavior shared across bounded scans, lookups, and CDC.
An absent JSON member remains distinct from explicit JSON null and fails when the selected row shape requires a complete value.

**One data-change record is an atomic conversion batch.**
All mods are converted into staged rows before the collector receives any of them.
A later malformed mod therefore cannot leave an earlier mod from the same record partially emitted before source progress fails.
The cause-free error identifies the table, commit timestamp, transaction, record sequence, and mod index without including row JSON, credential paths, or a nested exception.

**Stable scalar record and transaction fields are readable metadata.**
For the Spanner fields Debezium also exposes as source metadata, the SQL keys translate its snake-case vocabulary to this connector's hyphenated spelling: commit timestamp, record sequence, server transaction identifier, final-record flag, table, value-capture type, transaction record and partition counts, transaction tag, system-transaction flag, and mod number.
This connector additionally exposes the original Spanner `mod-type`; Debezium represents the resulting row-level operation outside its source metadata.
`mod-number` is the zero-based position in the original `DataChangeRecord.mods` list, and both rows of a full-mode update share it.
Raw key and value JSON, partition tokens, runtime processing timestamps, low watermarks, and configured resource names remain outside the metadata surface because they are either already relational rows, internal progress state, runtime observations, or static DDL.

**Source watermarks reuse the DataStream source's progress contract.**
The dynamic source implements Flink's source-watermark ability without adding a generator because the FLIP-27 reader already timestamps data records at commit time and emits the coordinator's complete-ledger heartbeat frontier.
The native commit metadata is `TIMESTAMP_LTZ(9)` so converting a record does not discard Spanner precision.
Flink watermark columns accept at most precision 3, so a DDL using `SOURCE_WATERMARK()` declares the same key as `TIMESTAMP_LTZ(3)` and lets the planner apply the compatible cast.

**The Table options map onto the DataStream builder's recovery and capacity contract.**
Fresh start, expired-restore fallback, absent-retention fallback, heartbeat interval, RPC priority, per-subtask query concurrency, emulator endpoint, service-account key path, and source parallelism retain their DataStream meanings.
The builder serializes only a configured credential path; the JobManager coordinator and each TaskManager reader load it when opening their own clients, while absence retains ADC.

## Evidence

Measured 2026-08-13 against the pom-pinned Flink 2.2.1 and Spanner emulator 1.5.56:

- Unit tests cover insert, update, delete, multiple mods, full and upsert modes, key-only deletes, explicit null, absent required values, extra and missing columns, type and primary-key mismatches, and value-capture changes.
- JSON conversion tests cover every GoogleSQL physical type supported by the Table schema mapping and PostgreSQL numeric and JSONB annotations.
- Factory tests cover mode-specific options, timestamp pairs, primary-key requirements, source boundedness, changelog declarations, source parallelism, copy, equality, and builder-option parity.
- Metadata conversion tests cover every key, planner-selected order, nanosecond commit precision, zero-based multi-mod numbering, and shared update-before/update-after identity.
- Planner tests cover metadata casts and `SOURCE_WATERMARK()`, and shaded-jar tests assert the implementation classes are present in the exact SQL artifact.
- Production Table planner jobs emit the four full changelog kinds for default-schema, named-schema, and quoted tables in both GoogleSQL and PostgreSQL emulator databases.
- Emulator Table jobs read all metadata fields with source watermarks in both dialects and both changelog modes.
- A fresh emulator Change Stream rejects an `earliest` position derived from its seven-day retention because the read begins before stream creation, so functional tests start at the first committed mutation rather than treating retention as creation history.

## Alternatives declined

- **Infer the changelog shape from a startup metadata read**: the value-capture type is carried per record and can change while a job runs, so startup metadata cannot make an incompatible historical record safe.
- **Emit partial update rows**: Flink update-before and update-after rows represent complete physical rows, and substituting null for absent values would turn unchanged data into SQL null.
- **Treat every stream table as the DDL table**: a Change Stream may watch several tables, and a regex filter cannot reproduce dialect-aware named-schema identity without also weakening quoted-name behavior.
- **Collect each mod as soon as it converts**: a later mod failure would make checkpoint replay repeat an already emitted prefix of the same record for a reason the converter could avoid.
- **Expose the DataStream regex column filters in SQL**: Table projection and filter semantics belong to the planner, while those regexes are connector-side record projections with different correctness rules.
- **Expose raw keys, old values, new values, or column descriptors as metadata**: the Table source has already converted those payloads into physical changelog columns, so a second JSON representation creates two schema contracts for one value.
- **Expose partition tokens or low watermarks**: those identify internal query partitions and progress rather than the logical change, and checkpoint redistribution can change their operational meaning.
- **Generate a second watermark from the metadata column**: the underlying reader already owns the heartbeat-driven complete-ledger frontier, so another generator would create conflicting progress policies.

## Consequences

Existing DDL remains bounded unless it opts into `scan.mode = 'change-stream'`.
Full mode requires the Change Stream to use `NEW_ROW_AND_OLD_VALUES`; upsert mode permits `NEW_ROW` but requires the DDL and every record to agree on the primary key.
A DDL added after a watched column existed can ignore that extra column, while a DDL that expects a column absent from an older record fails rather than inventing its value.
Metadata columns are virtual by convention and do not become physical Spanner columns.
Consumers can retain nanoseconds with `TIMESTAMP_LTZ(9)` or opt into Flink's millisecond rowtime precision with `TIMESTAMP_LTZ(3)` and `SOURCE_WATERMARK()`.
