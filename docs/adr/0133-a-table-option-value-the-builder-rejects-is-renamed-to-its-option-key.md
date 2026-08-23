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

# ADR-0133: A Table option value the builder rejects is renamed to its option key

- Status: Accepted
- Date: 2026-08-22, revised by [#1027] (2026-08-22) and [#1047] (2026-08-23)
- Issues: [#1030], [#1019], [#895], [#235], [#1027], [#1047]
- Modules: bigquery, pubsub, cloudtasks, bigtable, spanner (table layers)
- Current behavior: each module's `table.OptionSetters` and the mapper javadocs that cite it

## Context

[ADR-0007] set the rule this record replaces for single-value checks: a check that fires inside
`createDynamicTable{Source,Sink}` is wrapped by `FactoryUtil`, and "a check whose message names
Java setters needs restating in option keys; one whose message needs no translation does not."
[ADR-0068] applied it twice and declined restating, on measured legibility: `shutdownTimeout`
reads as `sink.shutdown-timeout`, `recoveryInitialBackoff` as `…recovery.initial-backoff`, unlike
ADR-0007's own `retryTotalTimeout`, which appears nowhere in a `WITH` clause. The Bigtable
mapper's javadoc carried the same test for its cross-field check.

[#1030] measured what that per-case judgment had produced across the whole surface: ~130
Table-reachable option→setter mappings, of which ~110 rejected a bad value with a message naming
the setter — including genuine renames the legibility test fails outright
(`sink.buffer-flush.max-cells` → `maxBatchCells`), and four messages naming a method *parameter*
(`interval`, `fallback`, `maximum`, `expiration`), which no reader maps to anything. The rule
had been applied to cross-checks — ADR-0007's restatement and the create-options mappers' handful — but to
no single-value bound anywhere, and [#1019] had just fixed the same naming defect for
`emulator-endpoint` alone. A rule that requires a legibility judgment per knob was, for
single-value checks, a rule nobody was applying.

## Decision

Every Table mapper applies option values to builder setters through a per-module
`table.OptionSetters` helper: the setter's `IllegalArgumentException` is rethrown as a
`ValidationException` naming the option key first, with the builder's own sentence kept intact as
the detail and the original exception as the cause —
`Option 'sink.buffer-flush.max-cells' is invalid: maxBatchCells must be positive`. The same
rename covers the plan-to-runtime translation points that hold raw values
(`BigQueryDynamicSource`, `SpannerDynamicSource`, `SpannerChangeStreamDynamicSource`), keyed from
the `ConfigOption` constants. A two-value call is split so each value's rejection is attributable
to its own key.

The mirror case — one value fed by either of two options, the BigQuery source's `parentProject`
from `scan.parent-project` or `project` — was first left alone, on the grounds that attributing
it at the seam would add a plan-state field whose only job is an error message's noun, and that
`parentProject` is legible against both keys. [#1027] falsified the second ground for the arm that
matters: a caller who wrote only `project` was answered `parentProject must not contain '/'` —
under the name of an option their DDL does not contain, which is precisely why the fallback ran.
The first ground stands, so the fix moves the rename rather than adding the field: the factory's
`parentProject(ReadableConfig)` is the point that still knows which key supplied the value, and it
applies the builder's own component check there through `OptionSetters.accept` under that key. The
rule this refines to: **a value with more than one supplying option is renamed where the supplier
is resolved, because the seam cannot attribute what resolution has already erased.**

What this replaces is the *translation judgment*, not ADR-0068's decision: no bound moved, no
bound is restated, and the builders remain the single home of every check — the failure is
renamed at the seam, not re-implemented. The `parentProject` rename above is the one place a check
*runs* twice rather than once, and it is a second call to the same shared
`ResourceNames.checkComponent`, not a second statement of the bound — the shape ADR-0127 already
blesses for the emulator endpoints, where the factory makes the same call the runtime makes so the
sentence is identical wherever it lands. The legibility test survives where per-case judgment is
still the right tool: **cross-field `build()` checks are not renamed**, because a message naming
two knobs has no single key, and the Bigtable mapper's spelling test (with the Pub/Sub mapper's
one restatement as the counter-example) remains their rule.

The helper is copied per module rather than hoisted into `flink-connector-gcp-base`, although it
now has five consumers, because base deliberately carries no Table API dependency and
`ValidationException` lives in `flink-table-common`; adding that dependency for one 80-line class
is a bigger change to base's surface than five identical copies whose drift a cross-module grep
catches.

Alongside the rename, the [#895] treatment is applied to the five builder messages that named a
method parameter rather than their setter — `absentRetentionFallback`, `heartbeatInterval`,
`maxConcurrentQueriesPerSubtask` (Spanner), `maxConcurrentStreamsPerSubtask` (Bigtable),
`timePartitioningExpiration` (BigQuery) — which were wrong for DataStream callers too.

## Alternatives declined

- **Keeping ADR-0007's per-knob judgment** — the [#1030] inventory is the measurement against it:
  no single-value restatement in ~110 candidates, and the judgment silently lapsed for every new
  option.
- **Restating bounds in factories with key-named messages** (the shape
  `BigtableDynamicTableFactory` uses for `scan.max-concurrent-streams-per-subtask`) — ~110
  hand-copied bounds that drift when a builder's bound moves. That one existing restatement is
  kept, because it fires at factory validation, earlier than the wrapper can; the dynamic-source
  site behind it is wrapped as well.
- **Name-parameterizing every check** (the `EmulatorEndpoint.parse` shape [#1026] used) — right
  for a parser that already takes a display name, wrong as a sweep: it threads a naming parameter
  through every `@Public` setter for the Table layer's benefit.

## Consequences

- A SQL caller's out-of-bounds value now fails with the key they wrote, at the same time and
  place it failed before; the cause chain still carries the original `IllegalArgumentException`,
  so root-cause assertions are unaffected. The `parentProject` rename is the one exception on
  timing: its check moves from `getScanRuntimeProvider` to factory creation — both on the client,
  and earlier is the direction ADR-0127 asks for. The measurable consequence is the same
  degenerate shape [#1026] recorded for the emulator endpoints: a streaming statement whose scan
  the planner eliminates never called `getScanRuntimeProvider`, so a malformed billing project on
  it planned before and is refused at planning now (measured 2026-08-22, `EXPLAIN SELECT * FROM t
  WHERE FALSE` over a query source with `'project' = 'a/b'`: planned on `bef08373`, refused with
  this change). Nothing connected either way.
- [ADR-0068]'s "What a SQL user is shown" blocks predate this record: the same probes now show
  the `Option '…' is invalid` line first. Its decision — the bound lives at the setter, nowhere
  else — stands unchanged, and both records now say so.
- The helper's contract is `IllegalArgumentException` only. `IllegalStateException` from
  cross-field `build()` checks and `NullPointerException` from `checkNotNull` guards pass through
  unrenamed, the former by the cross-field rule above, the latter because a present option value
  is never null.
- A new mapper line goes through `OptionSetters`; a mapper-level rejection test per mapper is the
  guard that it does.

[#235]: https://github.com/laughingman7743/flink-connector-gcp/issues/235
[#895]: https://github.com/flink-gcp/flink-connector-gcp/issues/895
[#1019]: https://github.com/flink-gcp/flink-connector-gcp/issues/1019
[#1026]: https://github.com/flink-gcp/flink-connector-gcp/pull/1026
[#1027]: https://github.com/flink-gcp/flink-connector-gcp/issues/1027
[#1030]: https://github.com/flink-gcp/flink-connector-gcp/issues/1030
[#1047]: https://github.com/flink-gcp/flink-connector-gcp/issues/1047
[ADR-0007]: 0007-the-publisher-teardown-is-two-phase-and-its-bound-is-real.md
[ADR-0068]: 0068-duration-budgets-are-bounded-at-the-setter-by-what-a-nanosecond-clock-can-express.md
