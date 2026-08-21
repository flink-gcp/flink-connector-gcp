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

# ADR-0028: Default-stream tuning — `recovery*` vs bare `retry*` naming, eviction, flush interval

- Status: Accepted
- Date: 2026-07-28 ([#54], with the naming revised on user feedback 2026-07-29); buffered-path
  knobs 2026-08-01 ([#198]); buffered-path eviction 2026-08-13 ([#76])
- Issues: [#54], [#76], [#198]
- Modules: bigquery (`sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Tuning;
  `docs/content/docs/reference/bigquery.md` for the values

## Decision

`DefaultStreamOptions` in `sink.storage` beside `BufferedStreamOptions`, same shape, but
**optional on the builder** — the one deliberate deviation from the two-adjacent-checks
convention (decided with the user, 2026-07-28): the "required" half exists so that *explicitly
choosing* a write method forces its options into view, and the default write method is chosen by
not choosing, so only the "rejected for other methods" half carries safety and only it stays.

- **Retry naming was revised on user feedback** (2026-07-29) after a first cut shipped `retry*`
  (connector) beside `sdkRetry*` (SDK): two knobs both stemmed "retry…MaxAttempts" were judged a
  confusion source. The **connector budget is `recovery*`**
  (`recoveryInitialBackoff`/`recoveryMaxBackoff`/`recoveryMaxAttempts`, matching the writer's
  internal `recoverySchedule` vocabulary, renamed in `BufferedStreamOptions` too so the two
  classes agree) and the **SDK knobs are bare `retry*`/`maxRetryDuration`** — the `sdk` prefix
  became redundant once "retry" uniquely meant the SDK layer, the bare names are the vendor's
  own words per the [#121]/[#147] rule, and the Pub/Sub builder already exposes SDK
  `RetrySettings` bare. The standing cross-module asymmetry: Cloud Tasks' `retry*` names a
  *connector-driven* schedule — its module has no second retry layer, so bare `retry*` is
  unambiguous there and renaming would churn a published-in-docs surface for no local gain.
- **[#198] gave the buffered path the same five SDK knobs**, deleting
  `StreamWriterRowAppenderFactory.RETRY_SETTINGS`; the SDK mapping stays in that factory as an
  overloaded `toRetrySettings`, **not** on the options class — the mapping-on-the-options rule
  is about `RetrySchedule`, and putting a gax type on a `@PublicEvolving` class would be worse
  than the `@Internal` project type. Reaching the service meant widening the
  `BufferedStreamServiceFactory` SPI to `create(location, options)` — both `@Internal` and
  unpublished, so the signature changed rather than being routed around. Defaults reproduce the
  old constant exactly; `maxRetryDuration` defaults to the SDK's own 5 minutes.
- `maxInflightRequests` **defaults to 100, deviating from the SDK's 1000 on purpose** (official
  multiplexing guidance, sample value 100): a pooled connection is a scale-up candidate above
  20% of its in-flight limits, so at the SDK default scale-up needs >200 queued requests per
  connection and rarely triggers — measured against SDK 3.30.0 sources, where the first writer's
  limits are baked into the JVM-static pool and later writers' are silently dropped (only a
  `limitExceededBehavior` mismatch throws). That first-writer-wins fact is also why the
  `ConnectionWorkerPool.setOptions` guard in `StreamWriterRowAppenderFactory` **warns and does
  not throw** on a second sink requesting different pool bounds in one JVM: a throw could not
  deliver the second value set either, and failing a session-cluster job over a hygiene knob is
  disproportionate. The pool floor is latched at pool construction, the ceiling is read live —
  hence the guard runs before this factory's first `StreamWriter.build()`, and its javadoc
  concedes another client may have created the pool first.
- The schema-wait schedule (flat 30 s × 30) is **deliberately not exposed**: it paces BigQuery
  metadata propagation, a service property, not a workload property. The writer keeps its
  package-private `(maxAppendRequestBytes, recoverySchedule, schemaWaitSchedule)` constructor
  for tests; the public options constructor takes the schedule from
  `DefaultStreamOptions.toRecoverySchedule()`, as the buffered writer and committer take theirs
  from `BufferedStreamOptions.toRecoverySchedule()`. Both mappings were jitter-free before
  [#197], with no reason ever recorded; they now carry `RetrySchedule.DEFAULT_JITTER_RATIO`.
- **Cold-destination eviction** (`destinationIdleTimeout`, default 1 h, enabled — decided with
  the user 2026-07-28; disable = set a large duration, no separate flag) sweeps at the **end of
  a successful `flush(boolean)`, skipped on `endOfInput`**: that is the point where every
  pending batch is empty and every in-flight append awaited, so closing an appender there
  cannot cancel a live append — placement is the design. The `pendingCount() == 0` guard is
  defensive (a dropping `FailureHandler` can leave re-appended rows pending past the await
  loop); a failed appender close is WARN-logged and never fails the flush (hygiene must not
  fail a checkpoint). `lastAccessNanos` lives on `DestinationState`, is refreshed in `write()`
  only, and is initialized at creation so a state rebuilt by a repair is not instantly idle;
  the boundary is strict (`> timeout` evicts), pinned by test.
- **[#76] applies that eviction contract to `BufferedStreamOptions` too.**
  A buffered destination is eligible only when its stream name and next offset still equal the
  latest snapshot, so a destination with rows added since that snapshot cannot disappear.
  Eviction closes only the local appender, deliberately never finalizes the remote stream, removes
  that destination from the next writer-state snapshot, and creates a new stream if the table
  receives another record.
  The default remains one hour and the same nanosecond upper bound applies.
- **`flushInterval`** (default disabled) registers a recurring processing-time timer from the
  writer constructor via `WriterInitContext.getProcessingTimeService()` — the first
  `ProcessingTimeService` use in the repository; safe because timer callbacks run on the
  mailbox/task thread, the same invariant `states` already relies on. The callback checks a
  task-thread `closed` flag rather than cancelling a future, calls the real `flush(false)` (so
  eviction rides it), re-arms itself, and lets exceptions propagate. It is a **mitigation only**
  for checkpoint-less streaming jobs; the documented guarantee still requires checkpointing.
- The issue's "connection injection seam" item was **not built**: [#15] resolved it with the
  test-only `EmulatorAppenderFactory` through the `@VisibleForTesting createWriter` overload
  (recorded on the issue; the seam story then changed again in ADR-0029). The `maybe*` methods
  were renamed with it: `createTableIfMissing`, `reconcileSchemaIfMismatched` (kept returning
  "ran just now" — its caller switches to the schema-wait schedule on exactly that),
  `warnIfCommitsAreTooFrequent`. **Refined by [#827]** (2026-08-22): the repair's state moved into
  a private `RepairState` value object, so `reconcileSchemaIfMismatched` makes that schedule
  switch itself and its return is only the caller's branch selector. Which schedule a fresh
  reconciliation installs is unchanged. What the two helpers return no longer differs by what they
  accumulate but by what the drain branches on: `createTableIfMissing` returns nothing, because no
  branch turns on a creation, and its sibling still returns, because the drain has to know whether
  to skip row-level routing.

[#15]: https://github.com/laughingman7743/flink-connector-gcp/issues/15
[#54]: https://github.com/laughingman7743/flink-connector-gcp/issues/54
[#76]: https://github.com/laughingman7743/flink-connector-gcp/issues/76
[#121]: https://github.com/laughingman7743/flink-connector-gcp/issues/121
[#147]: https://github.com/laughingman7743/flink-connector-gcp/issues/147
[#197]: https://github.com/laughingman7743/flink-connector-gcp/issues/197
[#198]: https://github.com/laughingman7743/flink-connector-gcp/issues/198
[#827]: https://github.com/flink-gcp/flink-connector-gcp/issues/827
