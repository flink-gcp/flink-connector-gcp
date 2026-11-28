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

# ADR-0040: `base.lifecycle` closes through its own written-out loop, keeping the throwable's type

- Status: Accepted
- Date: 2026-08-03 ([#229]); the written-out loop 2026-08-05 ([#276], reversing [#229]'s
  "delegate rather than reimplement"); revised by [#2369] (2026-11-28: a log call on a close path
  is not guarded against a throwing appender)
- Issues: [#229], [#276], [#297], [#2369]
- Modules: base (`Closers`), every connector's `close()` paths
- Current behavior: the `Closers` javadoc carries the contract (published by the API reference)

## Decision

Every `close()`-shaped call site in this repository goes through `Closers.closeAll` (closing is
the operation) or `Closers.closeAllSuppressing` (something else already failed) — nothing calls
`IOUtils.closeAll` any more, so its `scripts/config/flink-api-tiers.toml` entry is gone.

**The loop is written out rather than delegated to `IOUtils.closeAll`, and that is [#276]'s
whole decision** — reversing [#229]'s "delegate rather than reimplement", worth stating because
the reversal was argued, not drifted into. `IOUtils.closeAll` rethrows *from inside its loop*
anything its `Class` argument does not cover, so `Exception.class` (what the one-argument
varargs form passes) abandoned every later resource on an `Error`: the live bug at nine sites,
with the failure handler last in six of the lists and, since [#211], owning an SDK `Publisher`
and a gRPC `ManagedChannel`. `Throwable.class` closes everything but collects a non-`Exception`
as `new Exception(e)` — and **a wrapped `Error` is a different thing to Flink**:
`Task.preProcessException` tests the throwable itself (it unwraps only
`WrappingRuntimeException`) and halts the JVM on
`isJvmFatalError(t) || t instanceof OutOfMemoryError`, so a wrapped `OutOfMemoryError` from a
teardown fails the task into a restart loop instead. Ten lines over
`ExceptionUtils.firstOrSuppressed` and `rethrowException` keep the type, which is what both
`closeAll` tests and every site's `Error` test assert.

- `closeAllSuppressing` catches **`Throwable`**, not `Exception`, since an `Error` is no longer
  wrapped — and its suppressed entry is the `Error` itself.
- **Its one exception to "the caller's failure is never replaced" is
  `ExceptionUtils.isJvmFatalOrOutOfMemoryError`**: such a close failure takes the top slot with
  the caller's suppressed onto it — `Task.preProcessException` inspects only the throwable it is
  handed, so a fatal one in a suppressed slot is one nothing halts on, and for an
  `OutOfMemoryError` that means silently overriding `taskmanager.jvm-exit-on-oom`. The set is
  narrow by construction (`NoClassDefFoundError` is deliberately *not* escalated, which keeps
  this from degenerating into "any `Error` wins"); the escalation runs after the loop, so it
  costs no resource; not extended to `closeAll`, which has nothing to escalate over.
- For the same reason the creation-guard call sites catch **`Throwable`, not `Exception`** — a
  client's first classload failing with `NoClassDefFoundError` repeats on every restart attempt
  — which precise rethrow makes compile without widening any `throws` clause.
- The module's multiple-consumers bar was cleared on arrival with six call sites — the five
  sinks [#229] fixed plus `BigtableMutateRowsSink`, whose private `closeSuppressing` it replaced
  so the tree keeps one idiom — and [#276] added nine more. [#297] then made "one list, never a
  loop then a call" the rule at the reader teardown too (ADR-0012).

## A log call on a close path is not guarded against a throwing appender ([#2369], 2026-11-28)

A log call can throw.
In log4j2 (read at 2.24.3), an appender configured to pass its failures on, for most appenders by
`ignoreExceptions="false"` against a default of `true`, can raise a `LoggingException`, such as the
`AppenderLoggingException` that log4j's stream appenders raise on a failed write, and log4j-api's
`AbstractLogger.handleLogMessageException` lets it through to the caller.
Whether a given configuration does so depends on its appenders and their error handlers.
Under one that does, a release that follows the log call in the same straight-line code of a
`close()` is skipped.
For example, the three split enumerators of ADR-0208's [#2369] refinement close their
`AsyncStartupFailureReporter`, which may log at WARN, before they close the planner, client or
admin their start-up work calls.

**These calls are not guarded, and a new one is not either.**

- **Flink's own close does not survive an appender that throws at INFO.**
  `SourceCoordinator.close()` logs at INFO before it closes the enumerator and the coordinator
  context, in Flink 1.20.5 and 2.2.1, so under an appender that throws on that line the close
  never reaches the enumerator, and no guard inside the enumerator changes that.
- **A guard would bind every close path that logs before it releases.**
  Each would have to move its release ahead of the log call, or carry the appender's failure past
  the release as `Closers` carries a close failure, and no checker holds that rule, for a
  configuration that is not log4j2's default.
- **The limit is an appender that throws on the connector's line but not on Flink's**, by level
  or by logger.
  Flink's INFO line then passes, and the connector's line throws before the release after it.
  That configuration is not guarded either: the guard would cost the same, and an appender set to
  throw asks for a logging failure to stop its caller.

[#211]: https://github.com/flink-gcp/flink-connector-gcp/issues/211
[#229]: https://github.com/flink-gcp/flink-connector-gcp/issues/229
[#276]: https://github.com/flink-gcp/flink-connector-gcp/issues/276
[#297]: https://github.com/flink-gcp/flink-connector-gcp/issues/297
[#2369]: https://github.com/flink-gcp/flink-connector-gcp/issues/2369
