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

# ADR-0044: The E2E suite creates an ephemeral Bigtable instance per gated class

- Status: Accepted
- Date: 2026-08-02 ([#218]; the per-*class* deviation from that issue's settled design is
  recorded here)
- Issues: [#218], [#245], [#246], [#533], [#1196]
- Modules: bigtable (tests, `opentofu/`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Testing; the root
  CLAUDE.md `just e2e`/`sweep-e2e` entries

## Decision

Per gated *class*, not per run — the one deviation from [#218]'s settled design. When this
landed it was forced (`reuseForks=false` meant a fresh JVM per class); [#243]'s root-pom
override changed the calculus, and per-class was kept anyway: a shared holder would still be
raced by the two forks, a single class must stay runnable by hand, and best-effort deletion
tracks per class.

- Nothing persistent exists to run against because a one-node instance stands at roughly
  $470/month, so `opentofu/flink-gcp` carries only the two API enablements and
  `roles/bigtable.admin` — admin because *instance* lifecycle is administrator-level, not
  because the data path needs it.
- Leak control is a name-encoded creation time (`flink-it-<epochSeconds>-<runId>`, 28 characters
  inside Bigtable's 33) plus a sweep of anything older than two hours at the start of each
  class; [#959]'s default 90-minute integration-test fork ceiling sits below that threshold, so
  the age-gated sweep cannot reach an instance its owning fork is still using. CI has a separate
  whole-job ceiling. The cluster id is built from the run id rather than the instance id, which at
  28 characters leaves no room under a cluster id's own 30-character limit. Measured 2026-08-02:
  the two classes together, provisioning included, take about 7½ minutes. ([#246]'s scheduled
  `sweep-e2e` bounds what a run whose teardown never executed can cost.)
- **Every deletion disables Change Streams on the instance's tables first.** Bigtable rejects an
  instance deletion with `FAILED_PRECONDITION` while any table retains change-stream data. The
  per-class teardown, its startup sweep and the scheduled sweep all apply that prerequisite, so a
  failed Change Streams test does not turn the cleanup path itself into a permanent leak.
- **`BIGTABLE_IT_PROJECT` in a shell used to make every `just verify` create instances**,
  because the gate is on the classes and `verify` runs the same `integration-tests` execution
  `just e2e` does. Being the first gate billed per run is what forced [#245], which closed it:
  every gated class also carries `@Tag("gated")`, which surefire excludes by default, so the
  suite is opt-in per *command* rather than per shell. The variable is still required — the
  environment gate is unchanged — but setting it no longer costs anything by itself.

## Evidence

**What real Bigtable answers, measured 2026-08-02** (client 2.80.0) — what the connector page's
error-handling table states rather than infers. Routed (`INVALID_ARGUMENT`): a cell timestamp
that is not a multiple of 1000 ("Timestamp granularity mismatch"), and an empty row key ("Row
keys must be non-empty"). Fatal (`NOT_FOUND`): a mutation naming a column family the table lacks
— and the service reports it for **every** entry of the batch, the good ones included.

**Two conditions [#218]'s text expected to measure are unmeasurable through this connector**:
`Mutation` enforces its own limits in the private `addMutation` every mutation-adding method
funnels through — so "more mutations than a row accepts" and an oversized entry are thrown
client-side and arrive as *serialization* failures with no entry and no row key; the service
never sees them. The mutation-count half was run (110,000 mutations, never reached the wire);
the byte half is `MAX_BYTE_SIZE` = 200 MiB read from the client's class file, not exercised. A
single-cell size violation is unreachable for a second reason: the client's bulk flow controller
caps accumulated size at 100 MB, below Bigtable's 256 MB per row.

**`BigtableEmulatorDeviationITCase` asserts what the *emulator* does** — not a breach of the
emulator-is-not-an-authority rule but its enforcement: it records the traps so an image bump has
to declare them. The one that matters is the **status** — the emulator answers `INTERNAL` where
the service answers `INVALID_ARGUMENT` or `NOT_FOUND`, and `INTERNAL` is fatal to this sink, so
an emulator-only test would conclude "fails the job" for a condition the service makes
droppable. Every row of the documentation page's deviation table is asserted from both sides.

**The 2026-09-03 image bump to `583.0.0-emulators` ([#1196]) moved three measured rows**, and the
deviation suites are what surfaced them — the bump could not go in quietly, because the assertions
failed. Re-measured against the new pin on that date:

- **An empty row key is refused on the mutate paths.** Under `441.0.0-emulators` the emulator
  accepted it and stored a row that broke the client's own read state machine, a state the service
  cannot reach. `MutateRows` now answers `INTERNAL` wrapping the service's own "Row keys must be
  non-empty", per offending entry; single-row `MutateRow` answers `INVALID_ARGUMENT` unwrapped. So
  the status deviation is unchanged on the path the sink uses, which is the one that matters.
- **`ReadModifyWriteRow` still accepts one**, so the deviation narrowed rather than closed and the
  unreachable-state measurement is still live — it moved to
  `BigtableEmulatorReadDeviationITCase`, the only suite that can still produce it.
- **`SampleRowKeys` now always trails an end-of-table marker.** A three-row table answers
  `['c'@2, ''@3]` where it answered `['c'@2]`, and an empty table `['']` where it answered nothing.
  The planner drops empty-key samples, so no plan changed.

**`StubWriterInitContext` cannot drive this writer** (its metric group is a null-returning proxy
and the writer dereferences the group in its constructor), so the emulator tests build writers
through the sink's injecting `createWriter(batcher, mailbox, metricGroup)` overload with a
batcher the production factory created, and the MiniCluster job tests cover the
`WriterInitContext` path end to end. The module's own `RecordingSinkWriterMetricGroup` is gone
since [#237], superseded by test-utils' `TestSinkWriterMetricGroup`, which asserts by
*registered name*.

**The first live Change Streams acceptance run found the cleanup prerequisite above** ([#533],
2026-08-12). The test failed before bounded completion, then the teardown's direct instance delete
also failed because `change-stream-source` still had Change Streams enabled. Disabling the table
made deletion succeed; that measured recovery sequence is now the order every cleanup path uses.

[#218]: https://github.com/flink-gcp/flink-connector-gcp/issues/218
[#237]: https://github.com/flink-gcp/flink-connector-gcp/issues/237
[#243]: https://github.com/flink-gcp/flink-connector-gcp/issues/243
[#245]: https://github.com/flink-gcp/flink-connector-gcp/issues/245
[#246]: https://github.com/flink-gcp/flink-connector-gcp/issues/246
[#959]: https://github.com/flink-gcp/flink-connector-gcp/issues/959
[#533]: https://github.com/flink-gcp/flink-connector-gcp/issues/533
[#1196]: https://github.com/flink-gcp/flink-connector-gcp/issues/1196
