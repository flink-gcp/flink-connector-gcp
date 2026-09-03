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

# ADR-0149: The Table sink stamps its own writer-clock cell timestamp

- Status: Accepted
- Date: 2026-09-03
- Issues: [#1199], [#1205]
- Modules: bigtable (`table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigtable.md` § Cell timestamps

## Decision

When a row selects no writable `timestamp` metadata — or supplies a null one — the Table sink's
serializer stamps the cell itself with `System.currentTimeMillis() * 1000`, per cell, instead of
calling the client library's timestamp-less `setCell` overload and inheriting whatever clock it
uses.

The value is identical to what this connector wrote before, so nothing a user reads back changes.
What changes is where it comes from.

This applies where **the connector builds the mutation**. A DataStream serializer builds its own
`RowMutationEntry` and owns its timestamps: `BigtableWriter` sees a finished entry, and rewriting
cells there would mean parsing and mutating a proto the user handed over. So a DataStream user who
calls the timestamp-less `setCell` still gets the client library's clock, and will get microseconds
from it once the BOM reaches 2.82.0. That is the line, not an oversight.

## Context

`google-cloud-bigtable` 2.82.0, reached through `libraries-bom` 26.87.0, changed the writer-clock
overload:

```java
// 2.81.0
long timestamp = System.currentTimeMillis() * 1_000;          // always a multiple of 1000

// 2.82.0
Instant now = Instant.now();
long timestamp = now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000;
return setCell(familyName, qualifier, timestamp, value, TimestampOrigin.CLIENT_AUTO_GENERATED);
```

`Mutation.timestamp_origin` is a wire field `data.proto` defines precisely: the server **truncates**
a `CLIENT_AUTO_GENERATED` timestamp to the table's granularity, and **rejects** a `USER_SPECIFIED`
one whose precision does not match.

**Measured 2026-09-03 against a real instance**, writing through the Table sink's writer-clock path
on a `MILLIS`-granularity table: the write is accepted and the cell is stored at
`1788435696071000` — truncated. So the client change is benign against the service, and doing
nothing would have been correct for production.

**Measured the same day against the pinned emulator** (`google-cloud-cli:583.0.0-emulators`): the
same write is rejected with `invalid timestamp 1788436014893729`. `bttest` does not implement the
origin field at all, so it treats a client-generated timestamp as user-specified. That cost 63
errors across four emulator ITCases — including `BigtableTableSinkITCase`, where it is the sink's
own writes failing rather than the harness's seeding.

## Why stamp it here rather than leave it to the client

The emulator is why the question got asked, and it is not the answer. Settling what this connector
*sends* on what an emulator *accepts* would breach the rule this repository holds elsewhere. What
emulator coverage legitimately is, and what the declined alternatives below weigh, is an
engineering cost: which configurations can be tested without a billed run. That is a different
thing from evidence about the service, and the three reasons below stand without it:

- **Bigtable cell granularity is milliseconds** on every table this connector creates: its
  table-create path does not set granularity, so the server default applies. The sub-millisecond
  part the client now generates is then precision the wire carries and the table discards. A
  pre-existing `MICROS` table is the exception, and it is the cost this decision pays — recorded
  under Consequences rather than argued away here.
- **The value stops moving under a dependency upgrade.** It moved once, silently, in a patch-level
  BOM bump, and was caught only because a unit test happened to assert on it. A timestamp this
  connector writes is part of what a user reads back; inheriting it from a transitive clock means a
  future client release can change it again.
- **It is the value this connector already wrote.** Before 2.82.0 the client produced exactly
  `currentTimeMillis() * 1000`. Stamping it here reproduces 1.0.0's wire behaviour rather than
  changing it — the conservative option, not the novel one.

The clock is read **per cell**, not once per record, because that is what the client library did.
Reading it once would newly collapse a row's cells onto a single timestamp, which is a different
change wearing the same clothes.

## Consequences

- Against the pinned client the mutation carries no `timestamp_origin` at all: the field arrives
  with 2.82.0. Once it does, an explicitly stamped timestamp is `USER_SPECIFIED`, which the service
  **rejects** on a granularity mismatch rather than truncating — so the millisecond alignment
  becomes load-bearing at that moment, and is asserted rather than assumed.
- **The stamping is pinned by a clock seam, not by the value.** At 2.81.0 this connector and the
  client produce identical timestamps, so a bracket-and-alignment assertion passes whoever stamped
  the cell; it cannot tell the two apart, which is this change's blind spot. A package-private
  constructor takes a `CellClock` and the tests feed one no wall clock produces. It is a named
  `Serializable` type rather than a lambda, because this schema crosses the job graph and
  `crossesTheJobGraphWithoutItsCodecLambdas` pins that it carries no `SerializedLambda`.
- The same seam is what holds the per-cell reading. Without it the commitment would be
  unenforceable: three cells serialized in the same millisecond are indistinguishable whether the
  clock is read once or three times, so a refactor hoisting the read out of the loop would stay
  green.
- **A `MICROS`-granularity table loses precision it could have stored.** `Table.TimestampGranularity`
  admits `MICROS`, and `data.proto`'s `TimestampOrigin` comments say the server truncates a
  client-auto-generated timestamp *to the table's granularity* — so on such a table it would keep
  the microseconds. Read from the proto, not measured: the measurement above was taken on a
  `MILLIS` table, and this connector cannot create a `MICROS` one to test against.
  This connector neither creates one — its table-create path does not set granularity, so the
  server default applies — nor offers a DDL option asking for one, so reaching this needs a user to
  point the sink at a pre-existing `MICROS` table. Recorded rather than guarded: the guard would be
  an admin lookup on the write path, which costs more than the case is worth. A user who wants
  microsecond cell timestamps has the writable `timestamp` metadata column, which is passed through
  untouched.
- **No option, because nothing a user can express is lost.** The writable `timestamp` metadata
  column already is the escape hatch, and a per-row one rather than a job-level switch: a row that
  needs a specific timestamp supplies it and it is passed through untouched. A knob here would only
  choose between two spellings of "now".
- `sink.cell-timestamp.truncate-to-millis` keeps its meaning, and the asymmetry it now sits beside
  is deliberate: its default of `false` **refuses** to truncate an explicit value and lets the
  service reject it, while the writer-clock value is truncated unconditionally. One is a user's
  assertion about when a cell happened, which the connector does not quietly alter; the other is
  the connector's own clock, which it may round to what the table stores.
- **This does not fix the emulator**, and is not the reason it was done. Once the BOM moves to
  2.82.0 the emulator will still reject a client-auto-generated timestamp, so the harness needs an
  explicit one of its own; that workaround travels with the bump and is tracked, with its removal
  condition, in [#1205]. The deviation is reported upstream as [googleapis/google-cloud-go#20468]
  with a fix in [googleapis/google-cloud-go#20469].

## Alternatives declined

- **Leave the client to stamp it.** Correct against the service and wrong nowhere in production,
  but it leaves the emulator unable to exercise the sink's default configuration — the loss is the
  most common DDL, the one with no `timestamp` metadata column — and leaves the written value
  hostage to a transitive clock.
- **Hold the BOM bump.** Considered first and rejected on measurement: `bttest` on `main` still
  rejects this case, so there was no *released* fix and no release date — a fix is open upstream as
  [googleapis/google-cloud-go#20469], but waiting on it would have held the bump indefinitely.
- **Work around it only in the tests**, setting an explicit timestamp in every emulator test that
  writes through the sink. Keeps the client's behaviour but permanently stops the emulator from
  covering the default path, and buys nothing production wants.

[#1199]: https://github.com/flink-gcp/flink-connector-gcp/issues/1199
[#1205]: https://github.com/flink-gcp/flink-connector-gcp/issues/1205
[googleapis/google-cloud-go#20468]: https://github.com/googleapis/google-cloud-go/issues/20468
[googleapis/google-cloud-go#20469]: https://github.com/googleapis/google-cloud-go/pull/20469
