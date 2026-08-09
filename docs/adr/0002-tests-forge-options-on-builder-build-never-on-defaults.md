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

# ADR-0002: A test forges an options object on `builder().build()`, never on `defaults()`

- Status: Accepted
- Date: 2026-08-06
- Issues: [#316](https://github.com/laughingman7743/flink-connector-gcp/issues/316)
- Modules: all connector modules (test sources)

## Context

Every options class whose `defaults()` returns a
`private static final DEFAULTS = builder().build()` hands out a **JVM-wide singleton** — ten of
them as of 2026-08-06, in all four connector modules. The writer-creation-guard tests each carry
a private `forged(T options, String name, int value)` that reflectively writes a value the
builder would reject, and `setAccessible(true)` **does** permit writing a non-static final field
of a normal class — so forging on `defaults()` writes into that singleton for the rest of the
surefire JVM, and nothing restores it.

That is what [#316](https://github.com/laughingman7743/flink-connector-gcp/issues/316) was.
`BigtableMutateRowsSinkTest` forged on `BigtableWriterOptions.defaults()`, so every later
`defaults()` in the same fork carried `maxInFlightEntries = 0`, and
`BigtableWriterMetricsTest`'s 13 tests all died in the writer's precondition — on about one run
in three, because `default-test` runs `forkCount=4` with no configured `runOrder` and
`reuseForks` left at surefire's default of `true` (do not go looking for it in a pom; only the
`integration-tests` execution states it), so class-to-fork assignment decides whether the two
classes share a JVM.

## Decision

**Forge on `builder().build()`**, and the forging test asserts the singleton survived it —
placed in the class that would do the writing, so a regression fails deterministically there
instead of intermittently in whichever class the fork ran next. The pin holds whatever the
surefire fork settings become, which is why nothing here proposes changing them.

The fix was in the test, and the writers' preconditions stay exactly where they are: a forged
`0` is **not** reachable in production — the builders reject a non-positive value on every
setter, and Java serialization of the job graph restores the written field values. It becomes
reachable under serial-form evolution (a field added while `serialVersionUID` stays `1L`, read
from an older stream), which is precisely what those preconditions and their comments exist for.

## Evidence

Measured rather than inferred: under
`-Dflink.forkCountUnitTest=1 -Dsurefire.runOrder=alphabetical` the failure reproduces every
time, and under `reversealphabetical` it passes every time — which is also how a fix here is
measured against a failing case rather than against a green run that would have been green
anyway.

## Consequences

- BigQuery carries no singleton-survival assertion because its three forged types
  (`DefaultStreamOptions`, `BufferedStreamOptions`, `FileLoadsOptions`) have no `defaults()` at
  all, so there is no singleton to poison — the absence is checked, not an oversight. Read that
  as a property of those three types and **not** of the module: six of the ten singletons are
  BigQuery's, so a new forging test there owes the pin like any other.
- The reproduction recipe above (`forkCount` + `runOrder`) is the general tool for making any
  surefire order-dependent failure deterministic.
