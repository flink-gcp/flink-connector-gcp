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

# ADR-0077: Batch limits are counted in index-aware cells, read once when the writer opens

- Status: Accepted
- Date: 2026-08-09 (client library facts read in google-cloud-spanner 6.119.0; schema read verified
  2026-08-09 against `gcr.io/cloud-spanner-emulator/emulator:1.5.56`, both dialects)
- Issues: [#220]
- Modules: spanner (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Batching, and
  `docs/content/docs/reference/spanner.md`

## Context / Evidence

Spanner's limits apply to a batch write **request** as a whole — "the maximum size for a batch
write request is the same as the limit for a commit request" — which is **80,000 mutations
including index entries** and **100 MiB**. A request over either is refused outright, taking every
mutation in it with it. So a sink that accumulates mutations has to know how much of the budget it
has spent before it sends.

How Spanner counts is not how a naive reader would: "insert and update operations count with the
multiplicity of the number of columns they affect, and primary key columns are always affected";
a delete counts as one "regardless of the number of columns affected"; and in both cases every
secondary-index entry the write changes is counted individually. The index part is not derivable
from a `Mutation` — it is a property of the schema.

Two client-library facts constrain what can be measured at all (checked in 6.119.0):

- **There is no public route from a `Mutation` to its wire form.**
  `Mutation.toProtoAndReturnRandomMutation`, `Value.toProto()`, `Key.toProto()` and
  `KeySet.appendToProto` are all package-private. So the byte size of a mutation cannot be
  measured, only estimated — which is what Apache Beam's `MutationSizeEstimator` has done for
  years, and why this module has its own.
- **`Mutation.toString()` truncates every string value at 36 characters**
  (`Value.MAX_DEBUG_STRING_LENGTH`), so the debug rendering cannot stand in for the wire form
  anywhere — not for sizing, and not for the dead-letter payload.

## Decision

**Three limits, counted three ways, checked before a mutation joins the batch.**

- `maxBatchCells` (default 5,000) is counted the way Spanner counts: a written column costs one
  cell for the table plus one for each secondary index that contains it — as a key column or as a
  `STORING` one, since both rewrite an index entry — and a delete costs one plus the table's index
  entries.
- `maxBatchMutations` (default 500) and `maxBatchBytes` (default 1 MiB) are the other two. The byte
  figure is an estimate over the public accessors, documented as such on the setter; a type the
  client library adds later is counted at a fixed fallback rather than rejected, because a new
  Spanner type must not stop a running job.
- All three are Beam's long-proven values, read from its code rather than its prose: `SpannerIO`'s
  `DEFAULT_BATCH_SIZE_BYTES` (1 MiB), `DEFAULT_MAX_NUM_MUTATIONS` (5,000) and `DEFAULT_MAX_NUM_ROWS`
  (500), verified 2026-08-09 on Beam master and on the v2.68.0 tag. Beam's own javadoc for
  `withMaxNumRows` says 1,000, contradicting its own constant — so a reviewer checking Beam's
  documentation rather than its source will read this as wrong. The cell default sits **16 times**
  under Spanner's 80,000, and that headroom is load-bearing rather than decorative — it is what
  absorbs a table whose indexes the writer could not read.
- **Deletes are counted differently from Beam, deliberately.** Beam charges a point delete the sum
  of *every* column's weight and a range delete zero (`MutationCellCounter`: "There is no clear way
  to estimate range deletes, so they are ignored"). This sink charges both one plus the table's
  index entries, which is what Spanner's own documentation describes — "delete and delete range
  operations count as one mutation regardless of the number of columns affected", plus the index
  entries individually. Beam's point-delete figure over-counts and its range-delete figure
  under-counts without bound; one plus the indexes is the single-row truth, and the docs page says
  a range delete costs that per row it matches, which nothing client-side can know.
- The check runs **before** a mutation is added, so a batch only ever exceeds a limit when a single
  mutation does so on its own. Refusing that one here would be this connector inventing a limit;
  Spanner's own refusal names the real one better.

**The index coverage is read once, when the writer opens**, with a dialect-branched query over
`INFORMATION_SCHEMA.INDEX_COLUMNS` — GoogleSQL scopes the default schema at the empty catalog and
empty schema, PostgreSQL at schema `public`. The primary-key index is excluded by name
(`PRIMARY_KEY` in both dialects, the filter Beam has shipped against both the service and the
emulator), since its cells are already counted by the columns themselves.

- Reading at writer creation rather than lazily makes a database whose schema the sink cannot read
  a job that never starts, instead of one that dies at its first record.
- Names are matched case-insensitively. Spanner will not let two tables, or two columns of one
  table, differ only in case, so folding costs nothing — and it stops a serializer that spells a
  table `orders` where the schema says `Orders` from silently losing its index weights.
- **A table the weights do not know is counted without its index entries**: one created after the
  writer opened, or living in a named schema rather than the default one. That undercounts, and
  the headroom above is the answer. It is not an error, because the alternative — failing a job
  over a table that exists and works — is worse.
- A dialect the client library adds later throws rather than reading nothing, since reading
  nothing would silently undercount every mutation of every table.

## Consequences

- The sink needs read access to the database's `INFORMATION_SCHEMA` as well as write access —
  `roles/spanner.databaseUser` covers both. The quickstart's permission table says so.
- Raising `maxBatchCells` toward 80,000 removes the headroom that makes an unknown table safe.
  The setter's javadoc and the reference page both say what the number counts.
- The dead-letter payload cannot be a protobuf either, for the same missing-conversion reason. It
  is the Java-serialized `Mutation` — `Mutation`, `Value`, `Key` and `KeySet` each declare a
  `serialVersionUID`, so it is an affordance the library maintains — and `FailedMutation` exposes
  `getMutation()` for a handler that wants the object. `FailedMutationTest` round-trips a
  500-character value through the payload, and pins the truncation that rules out the alternative.

## Alternatives declined

- **Counting columns only, without index entries.** Cheaper by the whole schema read, and with a
  5,000 default it takes roughly fifteen covering indexes per written column to breach 80,000 — so
  it would rarely bite. Declined because it breaks exactly where a user raises the limit, which is
  the moment they most need the count to be true, and because it would make `maxBatchCells` mean
  something different from what Spanner's documentation means by the same word.
- **Building the wire proto ourselves to size mutations exactly.** It needs a `Value` → protobuf
  conversion for every Spanner type, arrays and structs included — some 250 lines duplicating SDK
  internals, silently wrong the first time Spanner adds a type. The estimate plus a 100-fold gap to
  the real limit is the better trade; Beam reached the same one.
- **Refreshing the weights periodically.** A schema change during a job is real, but the failure it
  causes is an undercount inside a 16-fold headroom, not a wrong answer. Reopen if a measurement
  shows the undercount reaching the limit.

[#220]: https://github.com/laughingman7743/flink-connector-gcp/issues/220
