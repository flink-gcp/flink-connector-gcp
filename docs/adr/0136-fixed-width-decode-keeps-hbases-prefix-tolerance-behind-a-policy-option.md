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

# ADR-0136: Fixed-width decode keeps HBase's prefix tolerance behind a policy option

- Status: Accepted
- Date: 2026-08-23
- Issues: [#1037]
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/table/bigtable.md` § What a read produces,
  § Filter pushdown and § Selected-cell upserts

## Context

`CellValueCodec`'s fixed-width decoders read the first N bytes of a cell or row key and ignore an
overlong value's tail, so a 12-byte externally written key on a `BIGINT` column silently decodes
as its 8-byte prefix. [#1037], raised from the independent review of the row-key message work
([#1012]), named the consequences: distinct keys sharing a decoded prefix collapse to one Flink
key, and on the selected-cell change-stream path a wrong-keyed `DELETE` or `UPDATE_AFTER` is
emitted with no error. The [ADR-0086] guard messages fire only on a *short* value, so "holds N
byte(s), which the declared column type cannot decode" held on one side of the length mismatch.

The issue left one question to decide the fix's shape: does the mirrored HBase contract itself
accept an overlong array? Verified against `hbase-common` rel/2.6.6 `Bytes.java` on 2026-08-23:

- `toLong(byte[])`, `toInt(byte[])` and `toShort(byte[])` delegate to their offset-taking
  overloads, whose whole check is `length != N || offset + length > bytes.length` (lines 753–758,
  915–920, 1051–1056). An overlong array passes and the leading N bytes decode; a short one throws
  `IllegalArgumentException`. Flink's `HBaseSerde` 4.0.0-1.19 delegates to these directly, so the
  interop target reads an overlong value's prefix too.
- `toBoolean(byte[])` is the one fixed-width decoder with a length rule of its own: any array that
  is not exactly one byte throws `IllegalArgumentException("Array has wrong size: N")` (lines
  702–707). This connector's `BOOLEAN` decoder was `value[0] != 0` — *more* tolerant than the
  method its comment cited.

The prefix tolerance was also already load-bearing inside this repository: [ADR-0092]'s
filter-pushdown refinement turns fixed-width row-key equality into a `ByteStringRange.prefix`
range *because* "the HBase-compatible decoder ignores bytes after the declared width", which is
what reads the leading component of an HBase-style composite key (`long` + suffix); the docs page
stated the rule; and `CellValueCodecTest`'s golden vectors pinned it.

So both readings of an overlong value are defensible — the mirrored contract's own behavior and a
legitimate composite-key use on one side, a silent-corruption hazard with per-row blast radius on
the other. That is the [ADR-0076] shape: a configurable policy rather than the connector picking
one.

## Decision

**A new table option, `decode.trailing-bytes`, decides what a fixed-width decode does with bytes
past the declared layout; its default, `ignore`, is the mirrored HBase behavior.** `ignore`
decodes the declared width and discards the tail; `reject` fails the read on any length other than
the exact layout — including a short value, which now fails with a deliberate message naming both
lengths rather than an incidental `ArrayIndexOutOfBoundsException`. The policy is resolved beside
the declared type on both sides of the job graph ([ADR-0125]'s `TypedFieldDecoder`), and it
governs every path that decodes through the codec: the bounded scan, both lookup shapes, the full
lookup cache, and the selected-cell primary key. The envelope change-stream mode decodes no cell,
so a DDL that sets the option there is refused rather than ignored — the factory's standing rule
for an option of a mode the DDL did not select.

**`BOOLEAN` rejects any length but one under both settings.** That is `Bytes.toBoolean`'s own
rule, so the strictness is fidelity to the mirrored contract, not part of the policy; the encoding
paragraph of `CellValueCodec`'s javadoc now records the verified evidence beside the layouts.

**Filter pushdown keeps its prefix-range equality under `ignore`, and under `reject` fixed-width
row-key equality, `IN` and `<>` stay with Flink.** Under `ignore` the suffix-bearing keys the
prefix range admits are matches, which is [ADR-0092]'s rationale. Under `reject` no range is exact
for a fixed-width key at all — found by the independent review round, which produced two concrete
counterexamples to this ADR's first draft (which kept the prefix under both settings): a prefix
`=` range admits a suffix-bearing key as a match, and when the projection also drops the key
column nothing decodes it, so the wrong-keyed row reaches the result with no error; and the
prefix's complement excludes that key from a `<>` scan server-side, so a query that must fail on
it completes silently. A singleton range has the mirrored hole (`=` skips the malformed key).
Leaving the predicate residual keys the evaluation on the decoded value, where the policy throws,
at the cost of the pushdown in a mode whose point is validation. To close the projection hole the
converter validates the row key under `reject` even when the projection dropped it — a full scan
with no key predicate meets the same guarantee. That forced validation covers exactly the types
the policy governs: validating a dropped `DECIMAL` key would let this option change [ADR-0135]'s
overflow behavior for a column outside its scope.

The documentation recommends `reject` for selected-cell change streams specifically, where the
prefix rule's failure mode is a wrong-keyed `DELETE`, and names `BYTES`/`STRING` as the right
declaration for a key column not written under this encoding.

**The asymmetry with [ADR-0135] — decimal overflow is an unconditional failure while overlong
tolerance is an option — is deliberate.** Prefix-reading has a legitimate use, the leading
component of a composite key, and [ADR-0092]'s prefix-range pushdown is built on it, so both
readings of an overlong fixed-width value are defensible and the user chooses. A decimal decoded
to null has no such use: it is indistinguishable from an empty cell and hands the planner a null
on a `NOT NULL` column, which is why [ADR-0135] declines the equivalent opt-in. `DECIMAL` is also
outside this option's scope for a mechanical reason: its layout is self-delimiting — a four-byte
scale header and a whole-array unscaled value — so "trailing bytes" does not exist for it.

## Alternatives declined

- **Unconditional strict rejection ("reject any length ≠ N").** A deviation from the verified
  mirrored contract, and a reversal of [ADR-0092]'s pushdown refinement: composite-key tables that
  Flink's HBase connector reads today would fail here with no recourse, and the module's stated
  point ([ADR-0086]) is that a table written by either connector is readable by the other.
- **Documenting the prefix rule and changing nothing.** The docs already stated the rule, and the
  wrong-keyed-`DELETE` scenario survives documentation; the [#430] precedent ([ADR-0076]) is that
  when both readings of a failure are defensible the user chooses, with the conservative reading
  one option away rather than one fork away.
- **`reject` as the default.** It cannot reject anything a correct writer produced — encoders emit
  exactly N bytes — but it breaks the composite-key reading by default and makes a job that
  Flink's HBase connector runs green fail here, the exact anti-interop surprise the module exists
  to avoid. The default stays the mirrored behavior; the docs put the strong recommendation where
  the hazard lives.
- **Keeping the prefix-range pushdown under `reject`** (this ADR's first draft) and **flipping it
  to a singleton key** are both defeated by the counterexamples above: each makes the same corrupt
  row visible or invisible depending on the query's `WHERE` clause and projection — the prefix via
  `<>`'s complement and the unvalidated projected-away key, the singleton via `=` skipping the
  malformed key server-side. Only the residual evaluation fails uniformly.

## Consequences

- An overlong fixed-width value is no longer silently truncated *by accident* anywhere: under the
  default it is a documented compatibility behavior with a documented opt-out, and under `reject`
  it fails through the existing [#1012]-shape guards, which catch `RuntimeException` and name the
  cell or key.
- A `BOOLEAN` cell holding a value of any size but one byte now fails the read where it
  previously read its first byte; a nullable column's empty cell remains a SQL `NULL`, which the
  null convention answers before any decoder runs. No HBase-written table holds such a cell — `Bytes.toBytes(boolean)` writes exactly one —
  so only foreign-written data is affected, and Flink's HBase connector already fails on the same
  cell.
- Under `reject`, fixed-width row-key equality predicates lose their pushdown and the converter
  decodes the row key even when the projection dropped it — a validation mode paying a validation
  cost; `ignore`, the default, keeps ADR-0092's pushdown unchanged.
- The golden-vector suite pins both sides: every fixed-width family's overlong prefix decode under
  `ignore`, its rejection under `reject`, the `BOOLEAN` length rule under both, and that a decoder
  restored from the job graph keeps its policy. The emulator ITCase pins the pushdown interaction:
  `=`, `<>` and a key-dropping projection all fail on the malformed key under `reject`, and `=`
  matches / `<>` excludes it under `ignore`.

[#430]: https://github.com/flink-gcp/flink-connector-gcp/issues/430
[#1012]: https://github.com/flink-gcp/flink-connector-gcp/issues/1012
[#1037]: https://github.com/flink-gcp/flink-connector-gcp/issues/1037
[ADR-0076]: 0076-two-spanner-statuses-are-routed-and-a-request-failure-never-is.md
[ADR-0086]: 0086-the-bigtable-table-layer-maps-onto-the-builders-over-an-hbase-compatible-ddl.md
[ADR-0092]: 0092-the-bigtable-table-source-serves-projection-as-a-family-filter.md
[ADR-0135]: 0135-a-decimal-overflowing-its-declared-type-is-a-decode-failure-not-a-null.md
[ADR-0125]: 0125-no-connector-minted-serializable-lambda-crosses-the-job-graph.md
