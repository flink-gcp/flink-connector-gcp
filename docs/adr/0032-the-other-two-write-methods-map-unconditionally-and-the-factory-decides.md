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

# ADR-0032: The other two write methods' mappers build unconditionally; the factory decides

- Status: Accepted
- Date: 2026-08-06
- Issues: [#288] (under [#57])
- Modules: bigquery (`table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Decision

`sink.buffered-stream.*` (9 keys) and `sink.file-loads.*` (10) onto `BufferedStreamOptions` /
`FileLoadsOptions`, under ADR-0031's mapping rules. `WriteDisposition` gained the `toString()`
its sibling enums carry, which made `BigQueryFileLoadsSink`'s streaming message mix spellings —
that message names `WriteDisposition.WRITE_APPEND` &c. in prose, so the value beside them takes
`.name()`. `LoadJobOrchestrator` is unaffected: it bridges through a `switch`, not
`valueOf(name())`.

- **The two new mappers build unconditionally, and the factory decides whether to call them from
  the write method** — the one place these diverge from `DefaultStreamOptionsMapper`, whose
  presence scan decides. It is not a missing symmetry: `defaultStreamOptions(...)` is
  *optional* on the builder while the other two are *required* for their write methods, so a DDL
  selecting exactly-once and tuning nothing would otherwise be told
  `bufferedStreamOptions(...) is required` — a method it never called and cannot call. Every
  buffered knob is defaulted, so `builder().build()` is exactly what that DDL means; FILE_LOADS
  needs its staging path, which is why that one rejection lives in `FileLoadsOptionsMapper`.
  `presentKeys` survives on all three for the wrong-family check alone.
- **Two of the four factory rejections are not about families**, and both became reachable from
  SQL for the first time here: `sink.schema-update.*` under exactly-once, and `emulator-*` under
  FILE_LOADS. The schema-update one fires on the *enabled* options object, the same condition
  the builder uses, so `allow-new-fields = false` passes here exactly as it passes there —
  pinned by a success-side test, the [#289] lesson.
- **The FILE_LOADS keys are spelled after the setters** (`sink.file-loads.schema-reconcile.*`),
  not after the `getSchemaUpdate*` getters, which the reflective tests key off too — and which
  keeps them clear of the unrelated `sink.schema-update.*` family. Both new mappers carry
  **both** reflective halves; the `everyOptionOfTheFamilyFeedsAKnob` prefix scan turned out to
  be the half `DefaultStreamOptionsMapperTest` had never had, so the scan moved to
  `OptionFamilies.declaredKeysUnder` and that test gained the guard.
- `BigQueryDynamicSink` took a `Builder` here rather than a fourteenth positional argument
  (decided with the user). **The identity test gained two guards because the builder weakened
  `copy()`**: a dropped positional argument does not compile, a dropped builder call does, and
  the old `aCopyEqualsTheOriginal` copied the *default* sink whose eleven optional fields are
  all null — so a `copy()` that lost one reproduced the default and compared equal (measured:
  that mutant survived). It now copies a **fully specified** sink built from the same
  `variations()` map, and a reflective check proves each entry varies the field it is keyed by.

## Evidence

**Neither new write method can be exercised against the emulator**, measured rather than assumed
(2026-08-06, goccy 0.8.1). FILE_LOADS stages to Cloud Storage that nothing stands in for — the
factory's own refusal is what the emulator suite asserts instead. Exactly-once was attempted and
dropped: `CreateWriteStream` answers `UNKNOWN` for a missing table, so `create-if-needed` cannot
auto-create ([#326] — the default-stream path carries that rewrite, the buffered one does not),
and with the table pre-created the emulator assigns its own append offsets, so
`BigQueryBufferedStreamWriter`'s consistency check fails on the first append. Both round trips
are therefore gated: `BigQueryTableExactlyOnceITCase` (a datagen sequence spanning several
checkpoints, so the second commit is exercised — a bounded `VALUES` insert commits once and
proves nothing about it) and `BigQueryTableFileLoadsITCase` (streaming plus batch, the latter
being the only place `write-disposition` has an effect). One measurement worth keeping
(2026-08-06, one run): at the planner's default parallelism every subtask races to create the
same table and BigQuery answers *"Exceeded rate limits: too many table update operations for
this table"* — the recovery schedule absorbs it and the job succeeds, so it is a cost rather
than a defect, and the test pins `sink.parallelism` to 2 rather than paying it.

## Consequences

`FileLoadsOptions.toString()` renders `writeDisposition=write-append`, the visible cost of the
enum's DDL spelling — log-only, nothing parses it (the counterpart of ADR-0014's note about
`StartPosition.toString()`).

[#57]: https://github.com/laughingman7743/flink-connector-gcp/issues/57
[#288]: https://github.com/laughingman7743/flink-connector-gcp/issues/288
[#289]: https://github.com/laughingman7743/flink-connector-gcp/issues/289
[#326]: https://github.com/laughingman7743/flink-connector-gcp/issues/326
