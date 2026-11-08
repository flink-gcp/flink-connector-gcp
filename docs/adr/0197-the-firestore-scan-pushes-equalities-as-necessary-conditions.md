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

# ADR-0197: The Firestore scan pushes equalities as necessary conditions

- Status: Accepted
- Date: 2026-11-05 (client library facts read in google-cloud-firestore 3.50.0; service behavior measured 2026-11-05 against ephemeral Standard- and Enterprise-edition Native-mode databases, one run each, and against the pinned emulator image); revised 2026-11-07 by [#2066] (the document id, fields of `ROW` columns, `CHAR` and `BINARY`, and the matcher shared with Datastore mode) and by [#2076] (ranges, `<>`, `IN`, `IS NOT NULL`, coarse timestamps and `scan.filter-push-down.enabled`, measured the same day on the emulator and on ephemeral Standard- and Enterprise-edition databases); revised 2026-11-08 by [#2088] (the source orders every query, so the cursor reason for not sending `!=` lapsed and the ranges stay for the reason left)
- Issues: [#1830], [#1829] (the pushdown umbrella), [#1838] (LIMIT, which depends on this record), [#2066], [#2076], [#2088]
- Modules: firestore (`io.github.flink.gcp.connector.firestore.table.source`, `firestore.common.table`)
- Current behavior: `docs/content/docs/connectors/table/firestore.md` § Filter pushdown

## Context

The `firestore` scan ([ADR-0179]) pushed projections but no predicates, so a selective query read, and was billed for, every document of its collection.
The DataStream source already runs any user query and resumes it through the client library's `startAfter(DocumentSnapshot)` ([ADR-0173]), so pushing a predicate is a table-layer translation.
The question [#1830] put was which predicates the service evaluates as Flink would, and, where it does not, whether it matches a superset of what Flink keeps.
A filter that matched a subset would drop rows without an error, and nothing after the read could recover them.

## Evidence

A probe wrote the same documents to the emulator, a Standard-edition database and an Enterprise-edition database, and ran each filter through the client library.
The client library 3.50.0 sends a `Blob` of subtype 0, `Blob.createBsonBinary(0, ...)` included, as native bytes (`UserDataConverter`), so the subtype-0 BSON form was written by hand as a raw `__binary__` value; the independent review found that the first probe had not done so.
Every equality answered identically in the three:

| Filter | Matched | Flink's `=` on the read row |
|---|---|---|
| `f == 5` (integer) or `f == 5.0` | stored `5` and `5.0`; on Enterprise also a 32-bit BSON `5` | `5.0` under `BIGINT` is a type mismatch, so the service matches more |
| `f == 0` or `f == -0.0` | `0`, `0.0`, `-0.0` | a superset or the same |
| `f == NaN` | `NaN` | a superset or the same |
| `f == 2^53` (integer) | the double `2^53`, not the integer `2^53 + 1` | exact |
| `f == '5'` | the string only, not the integer `5` | a value of another type never matches |
| `f == null` | a stored null, not a missing field | Flink reads a missing field as NULL |
| a timestamp one nanosecond past a stored microsecond | nothing | exact |
| a string in NFC | the NFC string, not its NFD form | exact |
| `Blob` bytes | the bytes | exact; on Enterprise, writing a BSON binary of subtype 0 in its reserved `__binary__` form is refused with `INVALID_ARGUMENT: ... subtype 0 is not permitted. Use the standard bytes type instead.`, so no stored value the converter reads as `BYTES` is other than native bytes |

The inequalities differ by edition: on Enterprise, `f != 5` and `f != null` also matched a missing field and a stored null, which they did not on Standard or the emulator.

On one collection of a Standard-edition database, equalities on two and on three fields ran without a composite index.
On a collection group, one equality failed with `FAILED_PRECONDITION: The query requires a COLLECTION_GROUP_ASC index`, and two with `The query requires an index`; the Enterprise-edition database answered both.
`PartitionQuery` takes no filter, so a filtered collection-group scan would partition the unfiltered group.

The planner probe of the source (`ClientQueryPlanner`) runs the scan's query with `limit(1)` on the JobManager before any split exists, so a query the service refuses for an index fails when the job starts.

## Decision

- **A pushed equality is a necessary condition, and every predicate stays a Flink residual** (owner's choice, 2026-11-05), the shape the BigQuery scan takes. The numeric rows of the table above show the service matching more than Flink keeps; an exact contract would have to prove each column type exact, and for `BIGINT` and `DOUBLE` it is not.
- **Pushed**: `column = literal` either way round, a bare `BOOLEAN` column (`= TRUE`) and its `NOT` (`= FALSE`), on a top-level physical column that is `STRING`/`VARCHAR`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `BYTES`/`VARBINARY`, or `TIMESTAMP_LTZ` of precision 6 or more; under `AND`, every conjunct that translates. Each is a `whereEqualTo` on `FieldPath.of(name)`, so a name containing a dot filters that field, as the projection selects it.
- **Declined**, each for a document the service would drop while Flink keeps it, or for a shape outside the measurements:
  - the PRIMARY KEY column, which is the document id and not a field (pushed as a document-id filter since [#2066]'s revision below);
  - a column `reference-field-paths` names, because a reference never equals a string;
  - `CHAR` and `BINARY`: the planner presented a `CHAR` comparison as a cast (`=(CAST(c AS VARCHAR(3)), 'ab')`), not a column reference, and the declined branch keeps it that way should a later planner present the column itself, because no measurement covers fixed-length values (pushed beside a literal of the column's length since [#2066]'s revision below);
  - `TIMESTAMP_LTZ` below precision 6, because the converter truncates the stored microseconds to the column's precision, so Flink keeps documents an equality on the literal would not match;
  - `IS NULL`, because a missing field matches no filter;
  - a string or bytes literal over 1,500 bytes: a Standard-edition database considers only that prefix of a value in a query, while an Enterprise-edition one considers the whole value (Google's data-types page), and no longer literal was measured; a timestamp outside the range the service represents;
  - inequalities, `OR`, `IN`, and `ARRAY`, `MAP` and `ROW` columns, which [#1830] leaves for later and whose matching differs by edition where measured (a field of a `ROW` column is pushed since [#2066]'s revision below).
- **Both `type-mismatch-policy` values push** (owner's choice, 2026-11-05). A document whose value has another type matches no equality, so under `fail` a mismatch in a document the filter excludes no longer fails the scan; nor, under either policy, does a `NOT NULL` column missing from a document the filter excludes, because a missing field matches no equality either. The docs state that type checking covers the documents read, which projection pushdown already made true of the columns read.
- **A collection-group scan pushes nothing** (owner's choice, 2026-11-05): a Standard-edition database refuses the filtered query without a collection-group index it does not create, and `PartitionQuery` would still partition the unfiltered group.

`FirestoreTableFilterPushDownPlanTest` pins the pushed and declined shapes and that the residual stays; `FirestoreTableFilterPushDownITCase` compares each pushed predicate's rows with the same predicate over a collection-group table of the same documents, which pushes nothing, including missing fields, nulls, mismatched types, integers under `DOUBLE`, doubles under `BIGINT` and `-0.0`, checks that the unpushed side keeps the expected rows for every predicate, and holds the `fail`-policy consequence; `FirestoreTableFilterPushDownRealGcpITCase` holds two equalities on a Standard-edition collection, the unfiltered collection-group read there, and, after checking that each is pushed, the Enterprise 32-bit integer match and the refusal of a subtype-0 BSON binary.

### Wider equalities ([#2066], 2026-11-07)

[#2066] asked which of the shapes declined above keep the necessary-condition contract, and moved the search for equalities into `EqualityMatcher` in `firestore.common.table`, which the Datastore-mode scan shares (ADR-0170's refinement, ADR-0184's revision); each mode keeps its own column checks and value mapping.
The planner's shapes were read on Flink 1.20.5 and 2.2.1 alike, the plan tests passing on 1.20.5, 2.2.1 and 2.3.0, and the service's on the pinned emulator once on 2026-11-07 and in `FirestoreTableFilterPushDownRealGcpITCase` against an ephemeral Standard-edition database the same day.

| Shape | What was measured | Decision |
|---|---|---|
| `id = 'k'` on the PRIMARY KEY | `whereEqualTo(FieldPath.documentId(), "k")` matched that document alone on the emulator and the Standard edition; the client library refused an empty id and a path (`a/sub/a`) before sending, and the emulator refused `__x__` and `.` with `INVALID_ARGUMENT` | pushed, unless the value cannot be an id in the collection (`FirestoreDocumentLookups.isStorableId`, the lookup's own test) |
| `e.x = 1` on a field of a `ROW` column | the planner hands it to `applyFilters` as a `NestedFieldReferenceExpression`; `FieldPath.of("e", "x")` matched the map's field, `1.0` included, and neither a top-level field named `e.x` nor a list of maps | pushed, unless a geographical point lies on the path or `reference-field-paths` names the field |
| a `NOT NULL` field under a nullable `ROW` | Flink takes such a field to be never NULL ([#2050]), so its comparison may evaluate where the document has no value | declined, as BigQuery's `NestedFieldPaths.restrictableLeaf` declines it |
| `c = 'abc'` on `CHAR(3)`, `bin = x'0102'` on `BINARY(2)` | the planner presents the column bare only beside a literal of its length, and a cast otherwise; the converter reads a stored value unpadded and untruncated; the emulator ITs found Flink's `c = 'ab '` keeping `"ab "` and not `"ab"` | pushed as `VARCHAR` and `VARBINARY` are |
| `ARRAY_CONTAINS(arr, 'x')` | never reaches `applyFilters` on Flink 1.20.5 or 2.2.1 | nothing to push; the service's `array-contains` (matching `[5]` and `[5.0]`, not a scalar `5`) stays unused |

A name holding a dot or a backquote was already one literal segment of a `FieldPath`, so [#2066]'s quoting item asks nothing of Native mode.
The id equality adds no ordering, so a cursor still resumes the query by document name.

`FirestoreTableFilterPushDownPlanTest` pins the new pushed and declined shapes, `EqualityMatcherTest` the shared search; `FirestoreTableFilterPushDownITCase` compares each new shape with its oracle, fixed-length near misses and a map holding a double, an array, a non-map value, a list of maps and a dotted top-level field included, and the id with a filter on the document path; the gated class holds an id and a map field's equality on a Standard-edition collection.

### Ranges, `IN` and `IS NOT NULL` ([#2076], 2026-11-07)

[#2076] took up the shapes [#2066] left undecided, and `EqualityMatcher` became `PredicateMatcher`, which finds `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NOT NULL`, and an `OR` of equalities on one reference, the planner's form of `IN`.
The planner's forms were read on Flink 2.2.1: a range either way round, `BETWEEN` as two conjuncts, `NOT (n > 5)` folded to `n <= 5`, `IN` of 22 values as a left-deep `OR` tree, `NOT IN` as `<>` conjuncts, `IS NOT NULL` as a call, and `d > 5` with an `INT` literal under `DOUBLE`.
The service's answers were read through the client library on the pinned emulator and on ephemeral Standard- and Enterprise-edition databases, one run each, and the plan tests and both modes' emulator ITs pass on Flink 2.2.1 and 1.20.5.

| Measured | Standard and Enterprise | Consequence |
|---|---|---|
| a range on a field | matches only values of the literal's type, integers and doubles as numbers; never null, a missing field, an array or NaN | a range never drops a value the column reads |
| strings and bytes | UTF-8 byte order (an emoji above U+FFFD) and unsigned bytes, as Flink orders bytes (`byteArrayCompare`) | see the string row below |
| Flink's own string order | `BinaryStringData.compareTo` compares two strings still held as Java strings with `String.compareTo`, by UTF-16 code units, and materialized ones by UTF-8 bytes; in the emulator IT, Flink's own filter left an emoji out of `s > U+FFFD` on a column it read, and kept it under `SUBSTRING(path) >= U+FFFD`, a string it computed | a string or id range is pushed only under a literal below U+D800, where both orders agree (`OrderedStrings`, a property test over code points straddling every boundary) |
| a timestamp literal between microseconds | compared exactly | the bounds are rounded to the column's unit anyway (`TimestampBound`, shared with Datastore mode, which floors the literal) |
| `!=` and `!= null` | Standard leaves out a missing field and a stored null; Enterprise returns them, and `startAfter` cannot take a cursor from a document lacking the field (the independent review's finding: the planner's one-document probe and every page after the first take one) | neither is sent: `<>` goes as `< v OR > v` (and `= NaN` on `DOUBLE`, which Flink's `<>` keeps and no range matches), `IS NOT NULL` as `>=` the type's lowest value (and `= NaN` on `DOUBLE`), which no missing field matches on either edition |
| an `!= null` read in pages | `UnaryFilterInternal.isInequalityFilter` is `false` in client library 3.50.0, so `startAfter` orders the cursor by the name alone, and the emulator refused the resumed query with `order by clause cannot contain more fields after the key` | moot once `!= null` is not sent |
| an inequality read in pages | `startAfter` needs the ordered field's value from the last document | the projection adds the field |
| composite indexes (Standard) | refused with `FAILED_PRECONDITION`: an equality beside a range, `!=` or `!= null` on another field, ranges on two fields, an `OR` holding a range; answered: an equality beside `IN`, two ranges on one field, an id range beside a field range | the job fails when it starts; Enterprise answers all |

- **Pushed** (owner's choice, 2026-11-07): `<>` (the first `<>` or `IS NOT NULL` in a query, so a query never carries two not-equal filters; whether the service takes two was not measured), `<`, `<=`, `>`, `>=` and `IS NOT NULL` (as the range above) on the scalar types, and `IN` or an `OR` of equalities on one reference, up to 30 disjuncts across the query; `CHAR` and `BINARY` push `=` and `IS NOT NULL` only; the id pushes `=`, `IN` and ranges; `TIMESTAMP_LTZ` below precision 6 pushes its comparisons as covering bounds (`ts3 = L` as `L <= ts3 < L + 1 ms`), `<>` excepted. A string or bytes literal of 1,500 bytes is pushed for `=` alone: a Standard-edition database compares that prefix of a longer value, which a range on a literal that long could misplace.
- **Index requirements are the service's** (owner, 2026-11-07): the connector does not predict which combination needs a composite index, because a refusal is loud, at job start with the index named, and the editions differ and may change. `scan.filter-push-down.enabled`, `true` by default, is the way out for a user who will not create the index: off, the scan pushes only an equality or `IN` on the document id.
- **Declined**: `IS NOT NULL` on `ARRAY`, `MAP` and `ROW` (whether an empty one matches is unmeasured), a range on a string or id literal from U+D800 up, ranges on `CHAR` and `BINARY` (no ordering of fixed-length values was measured), an `OR` across references or holding anything but equalities, `<>` and `IS NOT NULL` on the id, and `IS NULL`, still for the missing field.

`PredicateMatcherTest`, `TimestampBoundTest` (a property test of every operator against truncated reads), `OrderedStringsTest` and `FirestoreCollectionQueryFactoryTest` pin the search, the bounds, the string rule and the query; `FirestoreTableFilterPushDownPlanTest` the pushed and declined shapes and the option; `FirestoreTableFilterPushDownITCase` compares each new shape with its collection-group oracle, read in pages of two, with integers under `DOUBLE`, U+FFFD and an emoji, `0x7f` and `0x80`, and millisecond-column timestamps either side of a boundary, and the id ranges with filters on the document path; `FirestoreTableFilterPushDownRealGcpITCase` compares them on both editions with the same scan with the option off, a 32-bit integer on Enterprise included, and holds the Standard edition's refusal of a range beside an equality on another field.

### The cursor reason lapsed ([#2088], 2026-11-08)

[#2088] made the source order every query explicitly, by its inequality fields and then the document name, in the planner and in the split reader ([ADR-0173]).
A `!=` or `!= null` query is therefore ordered by its field, and an Enterprise-edition database then leaves out a document lacking the field, as Standard does (measured for `!= null` on 2026-11-07 and for `!=` on 2026-11-08, when a stored null still matched the ordered `!=` on Enterprise, a value the cursor can take), so the cursor failure the `!=` row above records no longer arises.
The ranges stay: a range matches only values of the literal's type, while `!=` also matches a value of every other type (a boolean and a string beside integers, on both editions on 2026-11-08), which the scan would then read and, under `type-mismatch-policy = 'fail'`, fail on.
The gated class now reads both editions in pages of two.

## Consequences

- A selective scan reads and is billed for fewer documents; the residual costs one comparison per document read.
- [#1838] can push a `LIMIT` only above exactly pushed filters, so a Firestore scan with a pushed filter keeps its limit in Flink.
- Since [#2076], a scan whose predicates combine an equality on one field with a range on another, which read the collection before, needs a composite index on a Standard-edition database, or `scan.filter-push-down.enabled = 'false'`; without either, the job fails when it starts.
- A query the database cannot serve, for example an equality on a field exempted from indexing, fails at job start instead of reading the collection.
- A change in the service's matching that narrows an equality would drop rows silently; the gated class pins the Enterprise 32-bit integer match and the refusal of a subtype-0 BSON binary, the two Enterprise-only premises the declined list does not already avoid.

## Alternatives declined

- **An exact contract for the string, boolean and bytes columns**, which would leave #1838 room to push a limit. The owner kept one contract for every type; exactness per type would need its own measurement against both editions. [ADR-0201] pushes a limit only onto a scan with no predicate, which the residual every pushed filter leaves already guarantees.
- **Pushing only under `type-mismatch-policy = 'null'`**, which keeps `fail` detecting mismatches in documents the filter excludes, at the cost of reading and billing them.
- **An opt-in equality pushdown for collection-group scans**, adding the filter to each partition's query, which needs a collection-group index the user creates.
- **`IS NOT NULL` as `!= null`**: a necessary condition on every edition measured, but an inequality orders the query by its field and needs that field in the field mask; left with the other inequalities.

[ADR-0173]: 0173-the-firestore-source-reads-partition-cursor-ranges-at-one-read-time.md
[ADR-0179]: 0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md
[ADR-0201]: 0201-the-firestore-and-datastore-scans-push-a-limit-as-one-split.md
[#1829]: https://github.com/flink-gcp/flink-connector-gcp/issues/1829
[#1830]: https://github.com/flink-gcp/flink-connector-gcp/issues/1830
[#1838]: https://github.com/flink-gcp/flink-connector-gcp/issues/1838
[#2050]: https://github.com/flink-gcp/flink-connector-gcp/issues/2050
[#2066]: https://github.com/flink-gcp/flink-connector-gcp/issues/2066
[#2076]: https://github.com/flink-gcp/flink-connector-gcp/issues/2076
[#2088]: https://github.com/flink-gcp/flink-connector-gcp/issues/2088
