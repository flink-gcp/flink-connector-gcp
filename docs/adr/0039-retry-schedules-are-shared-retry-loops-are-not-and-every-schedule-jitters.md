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

# ADR-0039: Retry schedules and asynchronous lookup loops are shared; service-specific loops stay local, and every schedule jitters at one ratio

- Status: Accepted
- Date: 2026-08-01 ([#61], [#197]), revised 2026-10-04 ([#1642])
- Issues: [#61], [#197], [#235], [#1642]
- Modules: base (`base.retry`, `base.rpc`, `base.table`)
- Current behavior: the reference pages' retry/recovery option tables

## Decision

- **Service-specific retry loops stay in the connectors.** The schedule, the backoff sleep and
  status-code extraction are shared. [#61]'s plan sketched a `Retries.run(schedule, isRetryable, action)`
  executor, and it was evaluated against every loop and adopted nowhere (recorded on [#61]): all
  seven measured loops are not plain predicate-retry — success-via-exception in
  `BufferedStreamCommitter.flush`, repair side effects in `createStream`, a mid-loop schedule
  swap in `retryBatches`, condition-driven retry in `FileLoadsSchemaReconciler`'s schema loop,
  unbounded completion polling in `BigQueryLoadJobRunner.awaitJob`, drain-based success in
  `TopicRepairer.repair`, and no loop at all in Cloud Tasks' park-and-redispatch
  writer — and each carries site-specific messages and logging that tests pin. Do not add an
  unused executor; a future consumer with a genuinely plain loop is what would justify one.
- **The asynchronous Table API lookup loop is shared in `base.table.AsyncLookupRetries`** ([#1642]).
  Spanner, Bigtable and Firestore had the same work-counter trampoline and retry-or-complete decision.
  Each now supplies the read as a `Supplier<ApiFuture<T>>`, a row converter that can throw a checked exception, its own transient-failure classifier, and the retry budget.
  Keys, conversion rules, error classification and client lifecycle stay per connector.
  The helper makes one initial read plus at most the configured number of retries, uses direct callbacks, and turns immediate failures into a loop without an executor or backoff.
  Read failures, including a synchronous throw, may be retried; conversion failures are terminal.
  Blocking lookup loops remain per connector.

  The shared helper does not cancel an active client future when its result is cancelled.
  Flink 1.20.4 and 2.2.1 chain `AsyncLookupFunction.eval` and `CachingAsyncLookupFunction` through `whenComplete`, without propagating cancellation back to the lookup result.
  With libraries-bom 26.90.0, Bigtable 2.85.0 classic point reads support upstream cancellation in `BigtableUnaryOperationCallable.UnaryFuture`; Spanner 6.124.0 returns a detached `SettableApiFuture` from `AbstractReadContext.consumeSingleRowAsync`; Firestore 3.49.0 returns a transform of `FirestoreImpl.getAll`'s `SettableApiFuture`, which is not wired to its stream.
  Those are client-source observations, not a common RPC cancellation guarantee.
  Cancelling the helper's result prevents later callbacks from scheduling retries or converting a value; an in-flight read retains the client's own lifetime.
  `AsyncLookupRetriesTest` holds the small-stack immediate-failure case, retries after a delayed failure, checked conversion failures, independent lookup state and the cancellation boundary through Flink's `eval`.

- **Every schedule jitters, at one shared ratio, and the ratio is never a knob** ([#197]).
  `RetrySchedule.DEFAULT_JITTER_RATIO` is the only ratio in the repository — a connector passing
  a literal is a review finding, and passing `0` needs a recorded reason (nothing in main
  sources does; the constructor accepts it because tests want deterministic backoffs). One
  number because the value is not load-bearing: the jitter is **mean-preserving** (factor in
  `[1 - r, 1 + r]`), so it costs the budget nothing in expectation and only has to be non-zero —
  which also disposes of the pre-[#197] argument that a short budget cannot afford jitter (true
  of full jitter, false of this shape). Not a builder knob: it fails the workload-property test
  the `recovery*`/`retry*` knobs pass. The AWS-taxonomy variants (full, equal, decorrelated) are
  unadopted **in `RetrySchedule`**; one arrives only with the call site whose measurement
  justifies it. Two full-jitter waits exist outside the type and are not counter-examples:
  `BigQueryDefaultStreamWriter.sleepJitter()` spreads subtasks across a metadata-update quota
  rather than backing off a retry, and gax jitters the SDK's own in-stream retries beneath these
  schedules.
- **A connector's knobs are mapped onto a `RetrySchedule` by the options class that owns them**
  (`toRetrySchedule()`), never in the consumer: one method serves every consumer of the same
  knobs, and the mapping becomes directly unit-testable — a ratio silently regressing to zero
  inside a writer constructor is otherwise unobservable, exactly the mutant [#197]'s tests had
  to kill.
- **`Retries.sleep` takes the interruption message as a parameter** because the five call sites
  it replaced each named what was being waited for; new call sites follow suit.
- **`StatusCodes.codeOf` inspects one throwable and never walks the cause chain.** Which element
  of a chain classifies a failure is per-connector policy (Pub/Sub matches any element, Cloud
  Tasks the first classifiable one), so the traversal stays at the call site with
  `ExceptionUtils.findThrowable`. Classification itself also stays per-connector; BigQuery's
  `AppendErrorClassifier` deliberately does not use this helper, since it targets
  `io.grpc.Status.Code` with gRPC-first precedence, and converting it would churn the classifier
  for no dedup gain.
- **`EmulatorEndpoint` is the parsed form of every connector's `emulatorEndpoint(String)`, and
  the only form that travels past the setter** ([#235]). It shares `base.rpc` with `StatusCodes`
  — that package is the client seam in both directions, and a one-class package would fail the
  [#119] layer test. Thirteen setters parse in the setter itself, so a client can never be handed
  an endpoint nothing has checked; the Table API's lookup and full-cache runtimes are the
  exception, holding the option's value and parsing when they open. Public signatures stay
  `String`: the type is `@Internal` and must not leak into a `@PublicEvolving` one. **`parse`
  takes the name of the setting to blame and has no one-argument form** ([#895]): a fixed
  `emulatorEndpoint` told BigQuery's two `emulatorRestEndpoint` callers that the other setter was
  malformed, which is the misdirection [#235] moved the parse into the setter to remove, so a
  builder passes its setter's name and a runtime passes the `WITH` key its user typed — so one
  SQL table can be told two different names, depending on whether the value reached a builder or
  a lookup. Two parse decisions not to re-litigate: **whitespace is rejected, never trimmed**
  (a trimmed value is silently a different endpoint, and the stray space is one of the typos
  [#235] exists to catch), and **the host is split at the last colon and kept verbatim**, so a
  bracketed IPv6 literal reaches the client unchanged
  and `getTarget()` reconstructs the input. One message covers every malformed value, which is
  why the old "must not be blank" is gone: a blank endpoint is not a separate kind of mistake.

[#61]: https://github.com/flink-gcp/flink-connector-gcp/issues/61
[#119]: https://github.com/flink-gcp/flink-connector-gcp/issues/119
[#197]: https://github.com/flink-gcp/flink-connector-gcp/issues/197
[#235]: https://github.com/flink-gcp/flink-connector-gcp/issues/235
[#895]: https://github.com/flink-gcp/flink-connector-gcp/issues/895
[#1642]: https://github.com/flink-gcp/flink-connector-gcp/issues/1642
