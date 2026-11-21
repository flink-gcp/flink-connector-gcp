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

# ADR-0207: The Pub/Sub table source generates watermarks only when a table opts in

- Status: Accepted
- Date: 2026-11-20
- Issues: [#1837](https://github.com/flink-gcp/flink-connector-gcp/issues/1837),
  [#2223](https://github.com/flink-gcp/flink-connector-gcp/issues/2223),
  [#2232](https://github.com/flink-gcp/flink-connector-gcp/issues/2232)
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/table/pubsub.md` § Watermarks generated in the
  source

## Context

The `pubsub` table source returned a `SourceProvider` and implemented no watermark ability.
The planner therefore generated a DDL `WATERMARK` in its own operator after the source, which sees
neither splits nor the FLIP-296 table options.
`scan.watermark.alignment.*`, `scan.watermark.emit.strategy` and `scan.watermark.idle-timeout` are
read only when the planner pushes a watermark into a source implementing
`SupportsWatermarkPushDown`, so a Pub/Sub SQL table accepted them and they did nothing, although
the DataStream source supports watermark alignment.

The owner decided on 2026-11-07 to support the ability, as Flink's Kafka table source does.
Building it showed that a pushed watermark has two costs on top of the benefit, and neither is a
property of this connector.

- **Lineage.** The planner hands a `SourceProvider` `WatermarkStrategy.noWatermarks()`, so a pushed
  strategy reaches the source only through a `DataStreamScanProvider`, which calls `fromSource`
  itself ([FLINK-19722](https://issues.apache.org/jira/browse/FLINK-19722)).
  On Flink 2.x the planner extracts a table's connector lineage dataset only from a
  `SourceProvider` ([FLINK-39219](https://issues.apache.org/jira/browse/FLINK-39219)), so such a
  table reports its catalog name with an empty namespace and without the `gcp` facet of
  ADR-0160.
- **Idleness.** The planner adds source idleness to a pushed strategy from
  `scan.watermark.idle-timeout`, or from `table.exec.source.idle-timeout` when the table sets none.
  Without push-down the session option drives the planner's own watermark operator instead.
  Source idleness can mark a split idle while it still has records
  ([FLINK-40234](https://issues.apache.org/jira/browse/FLINK-40234)), and an event-time operator
  then drops them as late; the project does not recommend it (#2017, #2043).
  A push-down given to every table would move an existing session setting onto that path without
  a word.

## Decision

`scan.watermark-push-down.enabled` selects the push-down source; a table that does not set it to
`true` plans exactly as before.
The planner pushes a watermark into any source implementing the ability, so the option selects a
class, `PubSubWatermarkPushDownSource`, rather than a flag inside one.
That source reads through the `SourceProvider` until the planner calls `applyWatermark`, which a
table without a `WATERMARK`, a batch query and a plan compiled before the option was set never do.
Once a strategy is pushed it returns a `DataStreamScanProvider` that returns the `fromSource` stream
itself, so the planner's parallelism lands on the source operator, and it sets the uid from
`ProviderContext.generateUid("source")`, the name the planner gives a `SourceProvider`'s scan.
Flink 1.20 and 2.2 then overwrite the stream's name and uid with their own; Flink 2.3 does so only
for plans compiled before 2.3, so there the operator carries the connector's name.

`applyWatermark` refuses a strategy the planner would give a positive idle timeout, naming the
option that supplied it and the per-table `'scan.watermark.idle-timeout' = '0'` override.
The strategy is opaque, so `applyWatermark` resolves the timeout from the same two options with the
planner's precedence: a table value of zero or more wins, and only a positive result adds
idleness.
The factory keeps the table value and a reference to the live session configuration, and the
session value is read when the strategy arrives, which is when the planner reads it: a `Table`
defined with `sqlQuery` is optimized later, and a session change made in between reaches the
planner.
The check sits in `applyWatermark` rather than the factory so that a table with no `WATERMARK`
is not refused for a session option that cannot reach it.
Restoring a compiled plan rebuilds the source through the factory in the restoring session but
replays the idle timeout the plan recorded, so a plan compiled without one is refused in a session
whose `table.exec.source.idle-timeout` is positive.
The check errs on that side because the strategy cannot be asked: Flink's idleness wrapper,
`WatermarkStrategyWithIdleness`, is package-private on both supported lines.
The message therefore names the job-level `'table.exec.source.idle-timeout' = '0'` as well as the
table-level override, since a restored plan may carry its table options with it.

The lineage loss is documented as a limitation of opting in and pinned by a Flink 2.x test.
FLINK-39219's open pull request, apache/flink#27727, reuses the lineage vertex the returned source
transformation already carries when the provider exposes none; `SourceTransformation` takes that
vertex from a source implementing `LineageVertexProvider`, as this one does, so that revision would
restore the dataset with no change here, and the test would fail on the first Flink line that ships
it.
The pull request is still under review, and a design that read only the provider would not, so
the open pull request and the report this case owes the ticket are tracked by #2232.
FLINK-19722 would remove the cause instead, with the source back on a `SourceProvider`;
contributing it is tracked by #2223.

## Evidence

All planner facts were read from the bytecode of `flink-table-planner` 1.20.5 and 2.2.1, with the
provider branches also checked on 2.0.1, 2.1.1, 2.3.0 and 2.5-SNAPSHOT, on 2026-11-20.

- `CommonExecTableSourceScan.translateToPlanInternal` tests `SourceProvider` before
  `DataStreamScanProvider` and translates it with `noWatermarks()` (2.2.1 offsets 289 to 328).
  It calls `TableLineageUtils.extractLineageDataset` only in the `SourceFunctionProvider`,
  `InputFormatProvider` and `SourceProvider` branches, and overwrites the returned transformation's
  lineage vertex. `TableLineageDatasetImpl` then has namespace `""` and copies no facets.
  `PubSubLineageGraphTest` measures both outcomes through the planner.
- `WatermarkPushDownSpec.apply` adds `withIdleness` only for a positive
  `calculateIdleTimeoutMillis()`, which takes a table value of zero or more and otherwise the
  session option. With the refusal removed, the planner gave a pushed scan with only the session
  option set `idletimeout=[30000]` in its digest; a table value of `0` gave none.
- A `Table` defined with `sqlQuery` while the session idle timeout was 0, with the timeout then set
  to 30 s before `explain()`, was planned with `idletimeout=[30000]` while the check read the
  timeout in the factory; reading the live session configuration in `applyWatermark` refuses it.
  The independent review found this ordering.
- A plan compiled with no session idle timeout recorded `"idleTimeoutMillis" : -1`; loading it in
  a session with `table.exec.source.idle-timeout` = 30 s failed with this source's refusal, and in
  a session with 0 it loaded (Flink 2.2.1).
- `FactoryUtil.TableFactoryHelper` consumes the FLIP-296 keys, which is why they were accepted and
  inert, and the planner merges `OPTIONS` hints into the table options before the factory runs.
- Both provider branches give the scan the planner's `<id>_source` uid in 2.2.1.
  Flink 2.3 assigns it to a `DataStreamScanProvider`'s stream only for plans compiled before 2.3,
  hence the provider's own `generateUid` call.
  With uid generation off, a job graph with and without the option kept the same source vertex id;
  only the stateless `WatermarkAssigner` left the chain.
- Two subscriptions read by one subtask, one carrying 100 rows a second apart and the other none
  or one, fired no ten-second tumbling window in 20 seconds with the option and one to four
  without, in each of two runs. Without the option and at parallelism two, where each
  subscription has its own subtask, the same input fired none with no idle timeout and nine with
  `table.exec.source.idle-timeout` at 2 s.
- `PubSubTableWatermarkAlignmentITCase` reads two subscriptions from one subtask, one of them an
  hour ahead of the other, publishing the lagging one only after the other has been read, and
  counts the lagging subscription's rows that arrive behind `CURRENT_WATERMARK`: all 200 without
  the option, none with it, where lagging rows did observe a watermark.
- `PubSubTableWatermarkAlignmentITCase` holds an ahead subscription's split paused under
  alignment, and consumes it whole with the option off; making the factory ignore the option fails
  the first test.
- One subscription under per-key ordering read at parallelism 2, where one subtask has no split,
  fired the same windows as parallelism 1 on Flink 2.2.1, so a split-less subtask does not hold
  the watermark and no parallelism limit is documented.

## Alternatives declined

- **The ability on every table, as Kafka does.** Every table with a `WATERMARK` would lose its
  connector lineage on Flink 2.x and move a positive `table.exec.source.idle-timeout` onto source
  idleness, or fail planning if that were refused. The owner chose an explicit opt-in because the
  trade-off is real.
- **Opting in by setting any `scan.watermark.*` option.** It needs no new key, but a table that
  already carries one of those inert options would change on upgrade, and the switch would not
  name what the user is choosing.
- **Returning `stream.union()` or `stream.forward()` to keep the source's own lineage vertex.**
  The planner overwrites only a `TransformationWithLineage`, so a virtual transformation would keep
  the connector dataset, but it depends on that planner detail and loses the catalog-context
  dataset the planner builds. The owner judged lineage not important enough to carry it.
- **Only documenting the idleness prohibition.** A session-wide setting chosen for other sources
  would still reach this table silently.
- **Refusing the idle timeout in the factory.** A table with no `WATERMARK` would be refused for an
  option that cannot reach it.

## Consequences

- An opted-in table supports watermark alignment, with the pausing, parking and quiet-subscription
  behavior the DataStream page documents.
- An opted-in table cannot use source idleness at all. A subscription of an opted-in table that
  receives nothing therefore stops the table's watermark, whether alignment is configured or not,
  and nothing releases it. Without the option a quiet subscription stops it only when every
  subscription its subtask reads is quiet, and `table.exec.source.idle-timeout` releases it
  there. The remedies are those of
  the DataStream page.
- A compiled plan of an opted-in table restores only in a session whose resolved idle timeout is
  not positive, even when the plan itself carries none.
- When FLINK-19722 or FLINK-39219 reaches every supported Flink line, the lineage limitation can be
  removed: the first by returning to a `SourceProvider`, the second as merged, which in the current
  revision of its pull request needs no change here.

## When to revisit the default

The option stays off by default while either cost remains, because turning it on for every table
changes tables that did not ask for it: a table with a `WATERMARK` would lose its lineage dataset
on Flink 2.x, and one in a session with a positive `table.exec.source.idle-timeout` would fail to
plan after an upgrade.

Revisit the default, and with it whether the option is needed at all, when both of the following
hold on every supported Flink line:

1. The lineage cost is gone: FLINK-19722 lets a `SourceProvider` receive the pushed strategy, or
   FLINK-39219 keeps the connector dataset of a `DataStreamScanProvider`, measured by
   `PubSubLineageGraphTest`
   ([#2223](https://github.com/flink-gcp/flink-connector-gcp/issues/2223),
   [#2232](https://github.com/flink-gcp/flink-connector-gcp/issues/2232)).
2. The idleness cost is gone: FLINK-40234 is fixed, so the planner's source idleness no longer risks
   marking a split idle while it still has records, and the refusal in `applyWatermark` can be
   removed.

Until then the choice stays with each table.
Both together are still not enough to turn it on for every table.
A table whose quiet subscription shares a subtask with one that carries rows would stop firing
event-time windows on an upgrade that pushed its watermark, until it set an idle timeout, which
the planner's own operator never needed for it.
The revisit therefore also decides whether such tables are told to set an idle timeout, or are
kept on the planner's operator by the option's default.
