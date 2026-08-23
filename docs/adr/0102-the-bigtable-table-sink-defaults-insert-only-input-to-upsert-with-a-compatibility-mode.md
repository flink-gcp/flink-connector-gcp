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

# ADR-0102: The Bigtable table sink defaults insert-only input to upsert with a compatibility mode

- Status: Accepted
- Date: 2026-08-12
- Issues: [#496](https://github.com/flink-gcp/flink-connector-gcp/issues/496)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Context

ADR-0086 answered an insert-only requested changelog with `ChangelogMode.insertOnly()`. That took
Flink 2.3's surviving sink-is-append early return and kept every plain insert planning as it had on
2.2 and 1.20. It also made every `ON CONFLICT` clause invalid for an insert-only statement, hiding
the conflict strategies Flink 2.3 added even though a Bigtable write is physically an upsert.

The evidence behind that answer still holds. Flink 2.3 validates the clause against the sink mode
before it analyzes the strategy; the connector receives no clause or strategy in
`getChangelogMode`; and 1.20 and 2.2 cannot parse the clause. The answer changes because portability
and capability need not share one implicit default: a table option can make the trade explicit and
local to the DDL.

This ADR supersedes ADR-0086. Every decision in ADR-0086 other than the insert-only changelog
default remains accepted and is incorporated here by reference.

## Decision

The Bigtable table sink advertises upsert by default for every requested changelog, including one
containing inserts alone. It continues to construct that answer through
`CrossVersionChangelogMode.upsert(keyOnlyDeletesAreSafe)`, preserving the source-level Flink 1.20
compatibility seam and the one-artifact Flink 2.2/2.3 contract.

The table-owned option `sink.insert-only-input-mode` has two values:

- `upsert`, the default, exposes the physical capability and Flink 2.3 conflict strategies;
- `insert-only` returns `ChangelogMode.insertOnly()` only when the requested changelog contains
  INSERT alone, keeping a clause-less statement portable across the supported Flink lines.

The option never narrows an updating requested changelog. In compatibility mode an insert-only
statement cannot carry `ON CONFLICT`, because the planner correctly rejects a conflict strategy for
a sink advertising only INSERT changes.

`table.exec.sink.require-on-conflict = false` remains a supported planner-wide alternative. Older
Flink lines ignore it. The connector option is the recommended compatibility control when the
decision belongs to one Bigtable table rather than every sink the session plans.

## Evidence

- Measured on 2026-08-12 against Flink 2.3.0 and this change: the default upsert answer makes a
  plain `INSERT INTO .. VALUES` over a declared primary key fail planning; adding `ON CONFLICT DO
  DEDUPLICATE` plans with no materializer; selecting `sink.insert-only-input-mode = insert-only`
  plans the clause-less statement with no materializer; and setting
  `table.exec.sink.require-on-conflict = false` plans it with `upsertMaterialize=[true]`.
- On Flink 2.3.0, `DO DEDUPLICATE` over append input added no materializer; `DO NOTHING` and
  `DO ERROR` added job-local, watermark-gated state. None of them probes Bigtable, so `DO NOTHING`
  is not insert-if-absent.
- Flink 1.20.4 and 2.2.1 reject the `ON CONFLICT` syntax and ignore
  `table.exec.sink.require-on-conflict`, while the connector option is ordinary DDL both parsers
  can retain.

## Alternatives declined

**Keep insert-only as the implicit default.** This preserves every plain insert but permanently
hides valid Flink 2.3 behavior from an upsert-capable sink. The compatibility need is real but does
not have to define the default.

**Use only `table.exec.sink.require-on-conflict = false`.** This reaches the planner before the
connector and remains useful for a whole session. It is too broad as the sole answer when one table
needs portable SQL and other sinks should retain Flink's conflict check.

**Select a different implementation by Flink minor.** The project deliberately ships one binary
artifact for 2.2 and 2.3. The existing compatibility roots address cross-major Java API differences;
they cannot make the older parsers accept new SQL syntax and must not split that artifact by minor.

## Consequences

Upgrading to this behavior can make a Flink 2.3 plain insert fail planning under the new default.
The diagnostic and documentation direct a portable statement to the table-local `insert-only`
mode, or to the planner-wide configuration when that scope is intended. Flink 1.20 and 2.2 behavior
is unchanged unless a user inspects the connector's declared changelog mode directly.
