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

# ADR-0180: Shared millisecond checks name conversion overflow

- Status: Accepted
- Date: 2026-10-04
- Issues: [#1593](https://github.com/flink-gcp/flink-connector-gcp/issues/1593)
- Modules: base (`base.options`), all connector consumers
- Current behavior: [Configuration reference](../content/docs/reference/_index.md#duration-validation)

## Context

[ADR-0068](0068-duration-budgets-are-bounded-at-the-setter-by-what-a-nanosecond-clock-can-express.md) deliberately preserved the raw `ArithmeticException` from `Duration.toMillis()` for an overflowing duration because it still failed at the setter.
That exception names no option, which makes a mistaken unit harder to locate in a builder chain.
It also bypasses the Table API mapper's handling of `IllegalArgumentException`, so the SQL option key is not attached there.

## Decision

`OptionChecks.checkAtLeastOneMilli` catches only the `ArithmeticException` thrown by `Duration.toMillis()` and rejects the value with an `IllegalArgumentException` containing the option name and duration.
The arithmetic exception is retained as the cause.
`checkAtLeastOneMilliOrZero` continues to delegate nonzero values to that helper.
This replaces only ADR-0068's choice to expose millisecond conversion overflow directly.

This decision applies to the two shared one-millisecond checks.
The Pub/Sub publisher retains its separate SDK-minimum validator and ADR-0068's raw overflow behavior for `retryTotalTimeout`, `retryInitialRpcTimeout` and `retryMaxRpcTimeout`.
Those setters enforce the publisher's own minima rather than the shared one-millisecond floor; changing that deliberately pinned validator would extend the repair beyond the two shared helpers.

The conversion remains the acceptance boundary: `Duration.ofMillis(Long.MAX_VALUE)` plus 999,999 nanoseconds is accepted, because the fractional millisecond is truncated; one millisecond above that whole-millisecond boundary is rejected.
Null checks, non-overflowing lower-bound failures, zero exemptions and the separate nanosecond ceiling retain their existing behavior.
Callers with a tighter nanosecond ceiling may keep that check first to report their specific limit, which then rejects the input before millisecond conversion and has its own message and cause behavior.

## Evidence

A JDK 17 JShell probe on 2026-10-04, one run per input, returned `Long.MAX_VALUE` for `Duration.ofMillis(Long.MAX_VALUE)` and for that duration plus 999,999 nanoseconds.
Adding one millisecond instead threw `ArithmeticException`, as did `Duration.ofSeconds(Long.MAX_VALUE)` and `Duration.ofSeconds(Long.MIN_VALUE)`.
`OptionChecksTest` covers both helpers at those boundaries and pins the option name, value and arithmetic cause on overflow.
`OptionSettersTest` covers the mapper's translation to an SQL-key-named `ValidationException`.

## Alternatives declined

- Compare only against `Duration.ofMillis(1)`: this would accept values whose later millisecond conversion overflows, moving a configuration failure past the setter.
- Add separate ceiling checks at every caller: the common helper already performs the conversion and can report the option supplied to it without duplicating validation.
- Reject everything above `Duration.ofMillis(Long.MAX_VALUE)` before converting: this would also reject fractional milliseconds whose truncated value still fits, changing the accepted range.
