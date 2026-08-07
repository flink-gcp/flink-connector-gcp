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

# ADR-0040: `base.lifecycle` closes through its own written-out loop, keeping the throwable's type

- Status: Accepted
- Date: 2026-08-03 ([#229]); the written-out loop 2026-08-05 ([#276], reversing [#229]'s
  "delegate rather than reimplement")
- Issues: [#229], [#276], [#297]
- Modules: base (`Closers`), every connector's `close()` paths
- Current behavior: the `Closers` javadoc carries the contract (published by the API reference)

## Decision

Every `close()`-shaped call site in this repository goes through `Closers.closeAll` (closing is
the operation) or `Closers.closeAllSuppressing` (something else already failed) — nothing calls
`IOUtils.closeAll` any more, so its `scripts/flink-api-tiers.toml` entry is gone.

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

[#211]: https://github.com/laughingman7743/flink-connector-gcp/issues/211
[#229]: https://github.com/laughingman7743/flink-connector-gcp/issues/229
[#276]: https://github.com/laughingman7743/flink-connector-gcp/issues/276
[#297]: https://github.com/laughingman7743/flink-connector-gcp/issues/297
