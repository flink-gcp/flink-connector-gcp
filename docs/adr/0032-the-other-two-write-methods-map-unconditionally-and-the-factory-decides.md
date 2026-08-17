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

# ADR-0032: The other two write methods' mappers build unconditionally; the factory decides

- Status: Accepted
- Date: 2026-08-06, revised by [#332] (2026-08-09) and [#77] (2026-08-13)
- Issues: [#288] (under [#57]), [#332], [#77]
- Modules: bigquery (`table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Decision

`sink.buffered-stream.*` (9 keys) and `sink.file-loads.*` (10) onto `BufferedStreamOptions` /
`FileLoadsOptions`, under ADR-0031's mapping rules. `WriteDisposition` gained the `toString()`
its sibling enums carry, which made `BigQueryFileLoadsSink`'s streaming message mix spellings —
that message names `WriteDisposition.WRITE_APPEND` &c. in prose, so the value beside them takes
`.name()`. The load path is unaffected: it bridges through a `switch`, not `valueOf(name())`
(`CommitPlanner.toWriteDisposition`, which moved off `LoadJobOrchestrator` with the rest of
planning).

- **The two new mappers build unconditionally, and the factory decides whether to call them from
  the write method** — the one place these diverge from `DefaultStreamOptionsMapper`, whose
  presence scan decides. It is not a missing symmetry: `defaultStreamOptions(...)` is
  *optional* on the builder while the other two are *required* for their write methods, so a DDL
  selecting exactly-once and tuning nothing would otherwise be told
  `bufferedStreamOptions(...) is required` — a method it never called and cannot call. Every
  buffered knob is defaulted, so `builder().build()` is exactly what that DDL means; FILE_LOADS
  needs its staging path, which is why that one rejection lives in `FileLoadsOptionsMapper`.
  `presentKeys` survives on all three for the wrong-family check alone.
- **Two of the four family-era factory rejections were not about families**, and both became
  reachable from SQL for the first time here: `sink.schema-update.*` under exactly-once, and
  `emulator-*` under FILE_LOADS.
  [#77] superseded the schema-update rejection when ADR-0022 added exactly-once schema evolution;
  `emulator-*` under FILE_LOADS remains rejected.
  Before that supersession, the schema-update check fired on the *enabled* options object, the same
  condition the builder used, so `allow-new-fields = false` passed here exactly as it passed there
  — pinned by a success-side test, the [#289] lesson.
- **The factory may read the *session* configuration, and the two FILE_LOADS streaming rules are
  where it does** ([#332]). A non-append `sink.file-loads.write-disposition` and a checkpoint
  interval below `sink.file-loads.min-checkpoint-interval` are `BigQueryFileLoadsSink`'s rules,
  and their messages name `WriteDisposition.WRITE_APPEND` and
  `FileLoadsOptions.minCheckpointInterval(...)` — the two a FILE_LOADS SQL user is likeliest to
  meet, in vocabulary they cannot act on. They are restated here in keys, gated on
  `execution.runtime-mode` being `STREAMING`, which is the first thing this class decides from
  anything but the `WITH` clause. It is safe to decide from, by two mechanisms rather than one —
  see the Evidence below — so at plan time the factory and the sink cannot disagree about either
  value, measured for both. Three consequences of the shape:
  - **The restated message stays in DDL vocabulary throughout**, so the runtime mode appears as
    the literal `streaming`/`batch` a user writes rather than as `RuntimeExecutionMode`'s own
    spelling — the same rule that made the sink's message take `.name()` above, applied from the
    other side.
  - The check runs **after** `FileLoadsOptionsMapper`, unlike the four, because both rules
    compare against a `FileLoadsOptions` knob whose default lives on that builder; reading the
    options directly would put a second copy of two defaults in the table layer. The ordering the
    four rely on survives — a missing staging path is still reported first.
  - **`FactoryMocks` builds its context over an empty `Configuration`, so every `FactoryMocks`
    test is implicitly streaming.** That is what makes the disposition rule reachable from
    `BigQueryDynamicTableFactoryTest` at all, and it is also why
    `fileLoadsKeysReachTheBuiltSink` no longer round-trips `write-truncate` — the value mapping
    is `FileLoadsOptionsMapperTest`'s, and the two remaining knobs carry what that test is for.
    The interval rule has no such reachability: `FactoryMocks` has no sink overload carrying a
    session `Configuration`, so both rules and the batch acceptance are pinned against a real
    `TableEnvironment` in `BigQueryTableWriteMethodsPlanTest`.
- **The FILE_LOADS keys are spelled `schema-reconcile`** (`sink.file-loads.schema-reconcile.*`),
  as are the setters and the getters, which keeps them clear of the unrelated
  `sink.schema-update.*` family. The reflective guard keys off the builder's setters. Both new
  mappers carry
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
auto-create ([#326], closed 2026-08-09 as an upstream report — the default-stream path carries
that rewrite, the buffered one deliberately does not;
[goccy/bigquery-emulator#504](https://github.com/goccy/bigquery-emulator/issues/504) tracks the
status code and `BigQueryEmulatorMissingTableDeviationITCase` pins the deviation), and with the
table pre-created the emulator assigns its own append offsets
([goccy/bigquery-emulator#505](https://github.com/goccy/bigquery-emulator/issues/505)), so
`BigQueryBufferedStreamWriter`'s consistency check fails on the first append. Both round trips
are therefore gated: `BigQueryTableExactlyOnceITCase` (a datagen sequence spanning several
checkpoints, so the second commit is exercised — a bounded `VALUES` insert commits once and
proves nothing about it) and `BigQueryTableFileLoadsITCase` (streaming plus batch, the latter
being the only place `write-disposition` has an effect). One measurement worth keeping
(2026-08-06, one run): at the planner's default parallelism every subtask races to create the
same table and BigQuery answers *"Exceeded rate limits: too many table update operations for
this table"* — the recovery schedule absorbs it and the job succeeds, so it is a cost rather
than a defect, and the test pins `sink.parallelism` to 2 rather than paying it.

**The factory and the sink converge on both values by two mechanisms, and the second is the one
that matters** (measured 2026-08-09 on Flink 2.2.1, one run each). The first is the fallback:
`TableConfig`'s root configuration is the `StreamExecutionEnvironment`'s own `Configuration`, the
object its `CheckpointConfig` is a view over. The first alone would leave a gap, since a value set
on `TableConfig`'s *own* layer shadows that root — and
`AbstractStreamTableEnvironmentImpl.attachAsDataStream` does not call `configure(...)` the way
`toStreamInternal` does. The hypothesis that this lets the two disagree was **tested and
disproved**: `PlannerBase.translate` itself calls
`StreamExecutionEnvironment.configure(tableConfig.getConfiguration(), classLoader)`, so every
translate — `attachAsDataStream` included — pushes that layer down. With the environment at five
minutes and `TableConfig` at thirty seconds, a `StatementSet.attachAsDataStream()` over a
FILE_LOADS table is refused by the factory; **with the factory check removed as a control, the sink
refuses the same plan with the same verdict**, and the environment reads 30 000 ms by then. The
runtime mode was measured on the same path and in both directions: a batch `TableConfig` over a
streaming environment leaves that environment reading `BATCH` after the attach, the factory skips
and the sink agrees; a streaming `TableConfig` over a batch environment leaves it reading
`STREAMING`, and the factory's disposition rule fires as the sink's would. The one genuine gap is a
value changed *after* the plan is built — measured too, and the sink catches it in its own
vocabulary, which is exactly the pre-[#332] behaviour rather than a regression.

**`RuntimeExecutionMode.AUTOMATIC` is refused before any factory runs** (measured 2026-08-09 on
Flink 2.2.1, one run): the Table API rejects it in `DefaultPlannerFactory` when the
`TableEnvironment` is created — *"Unsupported mode 'AUTOMATIC' for 'execution.runtime-mode'. Only
an explicit BATCH or STREAMING mode is supported in Table API"* — before any DDL, let alone a
connector factory. So the mode gate needs no `AUTOMATIC` branch, and does not restate
`BigQueryFileLoadsSink`'s refusal of it, which stays for the DataStream path where the mode does
arrive. It survives the one hole in that argument — a session set to `AUTOMATIC` *after* the
environment is built — because the gate is written against `STREAMING` rather than against
`BATCH`: a mode this connector has never seen takes the silent path instead of falling through
into rules it was never checked against.
`BigQueryTableWriteMethodsPlanTest.anAutomaticExecutionModeIsRefusedByFlinkBeforeAnyFactory` fails
should a later Flink accept it at construction — at which point the silent branch becomes ordinary
and has to be argued. The asserted string is byte-identical in flink-table-planner 1.20.4, 2.2.1
and 2.3.0, the whole range `verify-flink` covers, so it pins a premise rather than a version.

## Consequences

`FileLoadsOptions.toString()` renders `writeDisposition=write-append`, the visible cost of the
enum's DDL spelling — log-only, nothing parses it (the counterpart of ADR-0014's note about
`StartPosition.toString()`).

**From SQL, `BigQueryFileLoadsSink`'s two streaming rules are reached only by a value changed
after the plan is built** — every path where the factory sees the final value, it decides first,
because both read the same one. Their ordinary coverage is therefore
`BigQueryFileLoadsSinkTopologyTest`, on the DataStream path. What that costs is the plumbing claim
the planner test used to carry as a side effect: that a `TableEnvironment`'s
`execution.checkpointing.interval` arrives intact. It is re-pinned one layer up, by asserting the
interval *value* inside the factory's message (`'execution.checkpointing.interval' (30000 ms)`),
which is the layer that reads it.

## Alternatives declined

- **Naming both vocabularies in the sink's own messages** ([#332], option 2) — cheaper, no session
  read, but it puts DDL keys into a DataStream-facing message and so reverses this ADR's own
  decision that the two spellings must not meet inside one sentence.
- **Leaving the rules unrestated** ([#332], option 3). The failure is loud and at plan time, so
  nothing breaks silently; what a SQL user cannot do is act on it without translating a builder
  method name into an option key, which is exactly the boundary this class exists to hold.
- **`TimeUtils.formatWithHighestUnit` for the durations in the message.** `org.apache.flink.util
  .TimeUtils` is unannotated, so it would owe an entry in `scripts/config/flink-api-tiers.toml`; the
  message reports milliseconds instead, as `BigQueryFileLoadsSink`'s does.

[#57]: https://github.com/laughingman7743/flink-connector-gcp/issues/57
[#288]: https://github.com/laughingman7743/flink-connector-gcp/issues/288
[#289]: https://github.com/laughingman7743/flink-connector-gcp/issues/289
[#326]: https://github.com/laughingman7743/flink-connector-gcp/issues/326
[#332]: https://github.com/laughingman7743/flink-connector-gcp/issues/332
[#77]: https://github.com/laughingman7743/flink-connector-gcp/issues/77
