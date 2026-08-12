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

# ADR-0092: The Bigtable table source serves projection as a family filter

- Status: Accepted
- Date: 2026-08-11
- Issues: [#459](https://github.com/laughingman7743/flink-connector-gcp/issues/459),
  [#518](https://github.com/laughingman7743/flink-connector-gcp/issues/518) (under
  [#217](https://github.com/laughingman7743/flink-connector-gcp/issues/217); ADR-0086 holds the
  shared table layer, ADR-0080 the DataStream scan source it maps onto)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Context

ADR-0086 gave the `bigtable` table its DDL model and its sink; reading stayed on the DataStream
API. A SQL scan over the same DDL has one genuinely open mapping question: what a *projection*
becomes. The HBase model makes every top-level column either the row key or a whole column family,
and Bigtable's read API takes one filter per scan — `BigtableSourceBuilder.filter(...)` already
exists because per-cell shaping is all expressible through one (ADR-0080). So a projection is not
an index list to apply client-side; it is a filter to build, and the corner cases are where a naive
build is silently wrong.

## Decision

**`BigtableDynamicSource` implements `ScanTableSource` and `SupportsProjectionPushDown` over the
DataStream builder, with `supportsNestedProjection()` false.** A retained family always reads as
its full declared `ROW`; each projected index is one top-level column. The source reports the
`producedDataType` it was handed, never `toPhysicalRowDataType()`, and its type information is
`ScanContext.createTypeInformation(producedDataType)` — the seam ADR-0086 measured, which keeps
`flink-table-runtime` off the module. Nested projection was declined for now: Bigtable could serve
qualifier pruning through the same filter, but a nested projection also reshapes the produced
`ROW` type, which the converter would then have to mirror — deferred until asked for.

**The retained families become `FILTERS.interleave()` of `family().exactMatch(name)`, and a
projection retaining no family becomes the keys-only chain** —
`chain().filter(limit().cellsPerRow(1)).filter(value().strip())`. The chain is the load-bearing
edge: Bigtable has no row without a cell, so an empty interleave would not strip rows to their
keys, it would drop every row, and `SELECT rowkey FROM t` and `SELECT COUNT(*)` would silently
answer zero. `exactMatch` rather than `regex` because a family name is a literal; the one character
escaping cannot save, `:`, is already rejected at DDL validation (ADR-0086).

**The filter is applied whether or not a projection was pushed**: an unprojected scan retains
every declared family. Three things become uniform. A row-key-only *DDL* table — legal to read,
refused for writing — is served by the same keys-only chain. Families the physical table has but
the DDL does not declare never leave the server, projected or not. And the documented failure mode
— a filter naming a column family the table lacks fails the read with `NOT_FOUND`, which the
source deliberately does not pre-validate ([#481](https://github.com/laughingman7743/flink-connector-gcp/issues/481), ADR-0080) — arrives identically in both
cases, instead of only appearing once a query happens to project.

**The family filter decides row membership, not only row width, and that is accepted rather than
compensated.** A Bigtable row exists while it has a cell, so a row whose every retained family is
empty has nothing for the filter to return and is absent from the result: `SELECT *` returns a row
holding data in any declared family, a narrower projection can exclude that same row, and a
keys-only query sees every physical row, including one whose cells all live in undeclared
families. Which columns a query selects therefore also decides which rows it sees. This is the
wide-column model's row existence, and the emulator ITCase pins all three arms
(`aRowAppearsWhereAReadFamilyHasACell`). Flink's HBase connector also makes row membership depend
on projection, but adds each declared qualifier with `Scan.addColumn`; this source filters at the
family boundary promised by #459, so a row holding only an undeclared qualifier in a retained
family appears here with that family `NULL` where HBase omits it. The compensating mapping was
considered and declined: an
extra interleave branch carrying the keys-only chain would make every row appear, but its stripped
cells are indistinguishable from genuine empty cells of a retained family — a labelled branch
(`apply_label_transformer`) could mark them, at the cost of a per-cell label on every row and a
converter that reads labels — machinery whose only customer is a semantic the HBase population
this DDL serves has never had.

**The converter resolves the original schema plus a projected-index array; it never re-derives a
narrowed `BigtableTableSchema`.** `BigtableTableSchema.of` requires exactly one atomic column, and
a projection legitimately drops the row key (`SELECT cf1`), or every column (`COUNT(*)`), so a
re-derived schema is a dead end, not merely inelegant. Output position `i` is physical column
`projectedFields[i]`; reordering falls out. The converter, like the sink's serializer, resolves
everything into `Serializable` state at construction because the schema itself is not.

**What a row decodes into on the way out:**

- **The latest version of a cell wins, chosen by the converter, not pushed as a filter.** The
  client documents a row's cells ordered by family, qualifier, then timestamp descending, so the
  first cell seen per qualifier is the latest. Pushing `cellsPerColumn(1)` instead would spare the
  server sending older versions and was deferred: the projection filter is about *which columns*, a
  version filter is about *depth of history*, and conflating them in one change hides which one a
  wire regression came from. It remains a compatible follow-up (an added chain link).
- **A family none of whose declared qualifiers has a cell reads as a null field** — the mirror of
  the sink, whose null family writes no cells, so a sink round trip restores what was written.
  This diverges from `HBaseSerde`, which always builds the nested row; a row of nulls was declined
  because it erases the one distinction the sink's contract keeps.
- A declared qualifier's empty cell is `NULL` — for a character string the `null-string-literal`
  is, and the empty cell is an empty string — read by the decode half `CellValueCodec` gained,
  whose switch joins the golden vectors and the every-type-root agreement test.
- An undeclared qualifier of a declared family arrives and is dropped; nothing pre-validates it,
  for the same reason nothing pre-validates families.

**The `scan.*` surface is a union of row ranges and repeatable prefixes under one encoding.**
`scan.row-range.start-closed` / `scan.row-range.end-open` build a single range from
`ByteStringRange.unbounded()`, so a one-sided bound is expressible.
`scan.row-ranges` adds semicolon-separated `[start,end)` entries; either endpoint may be omitted,
and backslash escapes the grammar characters inside an endpoint.
The factory rejects an empty entry, a range with both endpoints omitted, malformed delimiters or
escapes, and equal or inverted decoded bounds while naming the one-based entry.
`scan.row-prefix` is a list additive with both range forms; `RowRanges.coalesce` merges every
overlapping or adjacent selection before the DataStream source plans splits (ADR-0080).
`scan.app-profile-id` is separate from `sink.app-profile-id` because a Data Boost profile reads and
cannot write.
`scan.row-key-encoding` applies to every prefix and endpoint: `UTF8` is the backward-compatible
default, while `BASE64` accepts canonical padded RFC 4648 standard Base64 and produces the exact
`ByteString` the DataStream builder receives.
The standard alphabet contains no semicolon, so `scan.row-prefix` remains a semicolon-separated
list that the factory splits before decoding each element.
URL-safe characters, whitespace, missing or non-canonical padding and malformed Base64 are rejected
when the planner builds the source.
The factory also rejects any value that decodes to an **empty row key**: an empty key means "before
every row", so the client would silently normalize an empty prefix or either range endpoint to an
unbounded side and broaden the configured selection.
An inverted legacy single range stays with the builder's own rejection; every `scan.row-ranges`
entry is rejected by the factory before job submission so its diagnostic can identify the entry.
The decoded bytes are retained by `BigtableDynamicSource`, so bounded scans, point-lookup range
membership and FULL-cache loading cannot reinterpret the option text independently.

### Filter-pushdown refinement (Issue #518)

**Safe row-key predicates become exact ranges and are removed from the residual plan.**
Equality, inequality, `IN`, null tests and conjunctions or disjunctions made entirely from those
forms translate against a direct row-key field and a non-null literal.
Equality is safe for variable-width `VARCHAR` and `VARBINARY` keys and for fixed-width integer and
temporal keys.
The latter use a prefix range because the HBase-compatible decoder ignores bytes after the declared
width; a singleton would miss suffix-bearing keys that decode to the same SQL value.
An empty `VARCHAR` or `VARBINARY` literal remains residual because the SDK normalises an empty
range bound to unbounded while the emulator, unlike the service, accepts an empty row key.
`CHAR`, `BINARY`, `BOOLEAN`, `DECIMAL`, `FLOAT` and `DOUBLE` remain residual because padding,
noncanonical true bytes, decimal scales, signed zero or `NaN` can make more than one byte sequence
represent the same SQL value.
Ordering is safe only for `VARCHAR` and `VARBINARY`; signed numeric and temporal byte encodings do
not sort in SQL order.
The configured prefixes and range remain a union, and the runtime reads its intersection with all
exact SQL ranges.
An empty intersection is represented by a blocking filter because omitting every range from the
DataStream builder means the whole table, not no table.

**Positive family and qualifier predicates become necessary cell-existence prefilters, never the
final SQL answer.**
The source accepts such an expression on a best-effort basis and also returns it as remaining, as
`SupportsFilterPushDown` permits.
The Bigtable condition can reject a row with no matching family or qualifier, while Flink retains
authority over decoded values, nulls and the latest visible version.
Raw value filters are not pushed: lexicographic encoded bytes do not implement every SQL
comparison, the codec maps some bytes to null, and Bigtable may test a version SQL would not expose.
The existence predicate becomes the condition and the existing projection filter its true branch,
so predicate evaluation never replaces the columns the residual operation needs.
The service documents conditional row filters as non-atomic and potentially poor-performing; the
user documentation presents the optimization as best effort rather than a latency guarantee.

## Evidence

Measured 2026-08-11 against `google-cloud-bigtable` 2.80.0 and the pom-pinned `flink.version`
2.2.1 (the 1.20 leg needs nothing new: `SupportsProjectionPushDown`, `SOURCE_PARALLELISM` and
`SourceProvider.of(source, parallelism)` all exist there, and the module gains no `CrossVersion*`
file):

- **The client collapses a one-branch interleave**: `FILTERS.interleave().filter(f).toProto()`
  equals `f.toProto()`. Pinned by `FamilyProjectionFilterTest`, so an SDK that stopped collapsing
  would change the wire visibly rather than silently.
- **`exactMatch` quotes RE2 metacharacters**: a family named `a.b` produces
  `family_name_regex_filter: "a\\.b"`, not a pattern also matching `axb`. Pinned.
- **The pinned client's `ConditionFilter` warning sets the performance claim**: its javadoc says
  predicate and branches are not atomic and that the filter may perform poorly.
  The connector uses only a true branch, but does not turn that warning into a speed guarantee.
- **The keys-only chain works against both the emulator and the service**: the emulator ITCase
  reads a row-key-only DDL and a `COUNT(*)`; the gated suite's absent-family pair shows the
  pruning is the server's — `SELECT *` over a DDL declaring a family the table lacks fails with
  `NOT_FOUND` while `SELECT rowkey` over the same table answers, which no client-side projection
  could produce.
- **Split planning and `scan.app-profile-id` are gated-suite-only** (ADR-0080's rule applied): the
  emulator models no tablets and ignores profiles, so the gated class reads a pre-split table
  through SQL and carries the missing-profile control.
- **Filter translation is pinned at three layers**: source-unit tests inspect ranges and filter
  protos, planner tests distinguish exact consumption from a residual cell predicate, and the
  emulator suite checks the returned rows and configured-range intersection.
  The gated suite verifies that real Bigtable accepts the condition/projection composition with SQL
  row-key bounds.
- **Binary bounds are pinned without the service**: decoder golden vectors cover zero and non-UTF-8
  bytes and every rejected Base64 form; factory tests inspect exact `ByteStringRange` values; and
  the emulator suite reads binary keys through a bounded scan and every lookup cache mode.

## Alternatives declined

- **Re-deriving a projected `BigtableTableSchema`** — `of()` rejects the rowkey-less and empty
  shapes a projection produces, and widening it to accept them would weaken the sink's validation
  to serve a converter that only needs an index map.
- **No filter on an unprojected scan** — reads undeclared families off the server for `SELECT *`,
  and makes the absent-family `NOT_FOUND` appear only under projection, a behavior split nothing
  would document.
- **An empty interleave for a rowkey-only projection** — matches no cell, so it drops every row;
  the naive mapping the keys-only chain exists to replace.
- **`HBaseSerde`'s row-of-nulls for a cell-less family** — erases the sink's own distinction
  between "family written as null" and "family written with null cells".
- **Pushing `cellsPerColumn(1)`** — deferred, not rejected; see the decision.
- **Pre-validating the DDL's families against the table** — a metadata read on every scan to
  soften an error the service reports precisely (ADR-0080's reasoning, unchanged by SQL).
- **A `ScanOptionsMapper`** — ADR-0080 records the scan source has no options object, so there is
  nothing for a mapper to build; the factory reads the four keys inline.
- **Raw cell-value filters** — unsafe across codec nulls, byte ordering and multiple cell versions.
- **Claiming a conditional existence filter is always faster** — the service explicitly warns that
  conditional filters can perform poorly.

## Consequences

- A DDL declaring a family the physical table lacks fails every `SELECT` that reads any family,
  with the service's `NOT_FOUND`; `SELECT rowkey` still answers. The docs page states it.
- `BigtableOptionParityTest` covers four surfaces: `BigtableSourceBuilder` joined with the
  `scan.*` options, closing the "joins when" note ADR-0086 recorded. The destination and the
  emulator endpoint feed both directions' builders, so the no-two-setters rule holds per
  direction and the cross-direction overlap is pinned to exactly `table` and `emulator-endpoint`.
- The scan is bounded and insert-only; `scan.parallelism` is Flink's own `SOURCE_PARALLELISM`,
  borrowed like `sink.parallelism`.
- The lookup half of the Table API ([#460](https://github.com/laughingman7743/flink-connector-gcp/issues/460)) builds on this source: the projection's family
  filter is what its point reads will carry.
- A filtered bounded scan and the FULL-cache loader created from that source share the same exact
  range intersection and best-effort cell predicate, refined in ADR-0095.
