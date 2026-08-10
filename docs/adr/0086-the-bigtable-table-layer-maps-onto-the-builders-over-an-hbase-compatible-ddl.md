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

**The changelog is `ChangelogMode.upsert()`, and a `-D` deletes the whole row.** A Bigtable write
is an upsert on the row key by construction, so there is no append-only mode to offer instead. The
row key is the primary key, so a delete means the key is gone; removing only the declared cells
would leave a row behind made of whatever else was in it. On 2.x `upsert()` is the key-only-deletes
form, which is honest because `deleteRow` reads nothing else; 1.20 has no such concept, so nothing
in the code or the tests may name `keyOnlyDeletes()`. The *sink* behaves identically on both — it
reads only the row key on a delete — but the *planner* does not, which a test discovered: an upsert
source feeding this sink gets a stateful `ChangelogNormalize` on 1.20 and none on 2.x, so an
integration test proving `deleteRow` has to use a retract source to run on both.

The changelog mode also carries an exposure the planner does not guard, recorded in
[#470](https://github.com/laughingman7743/flink-connector-gcp/issues/470): a key-only delete over a table with no declared primary key can arrive with a null
row key, which fails loudly rather than deleting the wrong row.

**Four record-level conditions fail the record rather than skipping it**: an `UPDATE_BEFORE` row,
a null row key, a row key encoding to zero bytes, and a row whose every column family is null — the
last of which would otherwise reach the service as a mutation-less entry and come back as an
`INVALID_ARGUMENT` naming neither the row nor the reason.

**Table creation takes the families from the DDL and the garbage-collection rule from two keys**,
`sink.table-create.gc-rule.max-versions` and `.max-age`, unioned when both are set and applied
uniformly to every family. At least one is **required** under `create-if-needed`, which the
DataStream API does not require.

**The reflective parity test covers four surfaces rather than the options builders alone**:
`BigtableWriterOptions.Builder` (no exemptions), `TableCreateOptions.Builder`,
`BigtableSinkBuilder`, and — from [#459](https://github.com/laughingman7743/flink-connector-gcp/issues/459) — `BigtableSourceBuilder`, each connector builder carrying an
exemption set whose entries state why a setter has no DDL form. A fifth assertion accounts for every
declared option that feeds something other than one setter, so an option with no home fails too.

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
requests.

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
