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

# ADR-0135: A decimal overflowing its declared type is a decode failure, not a null

- Status: Accepted
- Date: 2026-08-23
- Issues: [#1038](https://github.com/flink-gcp/flink-connector-gcp/issues/1038)
- Modules: bigtable
- Current behavior: `CellValueCodec`'s `DECIMAL` decoder and the docs page's "What a read
  produces" section

## Context

`CellValueCodec` reproduces the HBase ecosystem's byte layouts so tables written by Flink's HBase
connector and by this one stay mutually readable. Until #1038 it also reproduced one piece of that
connector's *error policy*: `DecimalData.fromBigDecimal` answers a value whose integer digits do
not fit the declared `DECIMAL(p, s)` with **null** rather than throwing, and the decoder passed
that null through. The behavior was deliberate — pinned by
`CellValueCodecTest.aDecimalTooWideForTheDeclaredTypeDecodesAsNull` and stated on the docs page —
on the argument that it is exactly how the HBase connector reads such a cell.

The independent review round on the pull request for #1012 measured what that choice costs, and
issue #1038 recorded it. The decode-failure guards added by #923 (and completed for the
selected-cell path by #1012) catch `RuntimeException` only, so a null is not a failure they can
see:

- On a nullable cell column, the overflow is aliased onto the empty-cell null convention — real
  data reads as "no cell here", indistinguishably.
- On a `NOT NULL` column the nullable wrapper is skipped and the null lands in a field the
  planner was told cannot hold one.
- On a `DECIMAL` row key — legal DDL — the emitted row carries a null primary key, and nothing in
  the source path reports it; the failure surfaces, if at all, in whatever downstream operator
  first dereferences the key.

The sibling converters already disagreed with the HBase answer: BigQuery's
`GenericRecordToRowDataConverter` null-checks `fromBigDecimal` and throws
`IllegalArgumentException`, and Spanner's `StructToRowDataConverter` pre-checks precision and
scale and throws before `fromBigDecimal` can return null. Bigtable was the outlier.

## Decision

A null from `fromBigDecimal` is a decode failure. The `DECIMAL` decoder throws
`IllegalArgumentException`, which the existing guards catch and wrap with the failing cell's
address and escaped row key — the machinery #923 and #1012 built, unwidened.

- **The check lives in the decoder lambda**, the single funnel every read path resolves through:
  the scan source, both lookup functions, the FULL-cache load and the selected-cell change-stream
  primary key. One check, five paths, no per-caller duplication.
- **`IllegalArgumentException`**, matching the BigQuery precedent and the module's idiom for a
  value the code refuses; `IllegalStateException` stays what the guards *wrap with* and what the
  codec's unreachable-switch backstops throw.
- **The message carries the stored value's precision and scale, its rounded precision and the
  declared type, never the value itself** — a decimal cell is user data, and the guards already
  name the row. The rounded precision is there because the overflow is judged *after* rescaling:
  rounding fractional digits is not an error by itself, but a rounding carry can overflow a value
  whose stored digits look representable (`999.995` at `DECIMAL(5, 2)` rounds to `1000.00`), so
  the stored dimensions alone would let an operator conclude the value should have fit.
- **The guards are untouched.** Their "holds N byte(s) … written under a different encoding?"
  framing is imprecise for a well-formed cell that merely overflows, but the wrapped cause states
  the exact reason, and teaching the guard to distinguish failure kinds would couple it to the
  codec's internals. Weighed and accepted.
- **HBase interop is unchanged where it was ever claimed**: the byte layouts. The docs page now
  states the error-policy divergence explicitly where it used to state the parity.

## Declined alternatives

- **Keep the HBase-parity null.** What the parity preserved was silence, not data: no consumer
  can tell the aliased null from an absent cell, and the `NOT NULL` and row-key cases violate the
  contract the planner was given. Byte-layout compatibility — the reason the HBase conventions
  are normative here — does not require reproducing a lossy error policy.
- **Widen the guards to check for null.** The issue's own analysis: null is `fromBigDecimal`'s
  documented return value, not an exception, and at the guard a null is ambiguous — the nullable
  wrapper legitimately returns null for an empty cell, so only the decoder itself knows which
  null it is holding.
- **Reject at DDL time.** Nothing is wrong with the declared type; only a stored cell can be too
  wide, and only at read time.
- **An opt-in overflow-reads-as-null mode.** No user has asked for the old behavior; an option
  would preserve, behind a flag, exactly the silent aliasing this record removes. If migration
  from HBase-written tables ever needs it, that request can reopen this with a concrete case.

## Consequences

- Every read path fails the job (or the lookup/cache operation) on an overflowing decimal, with
  the cell address and escaped row key in the outer message and the dimensions in the cause. The
  lookup error classifier finds no gRPC status on the chain, so the failure is non-transient and
  burns no retry attempts.
- `CellValueCodecTest` now pins the rejection — including that the message omits the value — and
  that a value exactly filling the declared precision still decodes;
  `RowToRowDataConverterTest` and `SelectedCellRowDataDeserializationSchemaTest` pin the guard
  routing on the cell, row-key and selected-cell paths.
- A table readable end to end by the HBase connector can now fail a scan here where that
  connector would emit nulls. That asymmetry is the point, and the docs page says so.
