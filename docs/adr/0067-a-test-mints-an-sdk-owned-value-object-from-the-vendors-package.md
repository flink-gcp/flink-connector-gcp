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

# ADR-0067: A test mints an SDK-owned value object from a helper in the vendor's package

- Status: Accepted
- Date: 2026-08-08
- Issues: [#337]
- Modules: bigquery (tests); the rule is cross-connector
- Current behavior: root `AGENTS.md` § Cross-connector contracts

## Decision

**When a class under test reads a value object the vendor's SDK will not let anyone construct, the
test mints it from a helper declared in the vendor's package** — not by abstracting the value away
in production code, and not by adding a mocking framework. The helper is a test source, carries the
project's licence header, states in its javadoc *which* members it reaches and *why* no other reach
exists, and names the SDK version the reach was verified against.

The bar is narrow, and both halves must hold:

1. The value object cannot be built through public API — no public constructor, no public factory,
   and (for a non-final class) no constructor a subclass outside the package could call.
2. The behaviour under test genuinely reads that value. A test that only needs *a* value of some
   type does not qualify; the seam it wants is an interface it can already implement.

The helper also reaches **as few** package-private members as it can: `TestJobs.status(State)`
delegates to the three-argument `JobStatus` constructor rather than reaching the one-argument one
as well, because a redundant overload is one more member an SDK release can move.

`flink-connector-gcp-bigquery/src/test/java/com/google/cloud/bigquery/TestJobs.java` is the first
and, today, only instance. It is the **only Java source in this repository whose package is
outside `io.github.flink.gcp.*`**, and a second one is a decision, not a precedent to follow
silently.

## Context

`BigQueryLoadJobRunner` had no unit test at all ([#337]), so its deterministic job-id probing —
which decides whether a restarted Flink job loads the same staged files twice, once, or not at all
— was exercised only by the gated real-GCP FILE_LOADS suites, and only along the happy path.
FILE_LOADS rejects an emulator endpoint (ADR-0029) and `gated` is excluded from every surefire
execution, so in an ordinary `verify` the class executed zero lines.

The seam the issue named — the `@VisibleForTesting BigQueryLoadJobRunner(BigQuery, String,
RetrySchedule)` constructor — turned out not to be sufficient on its own. Stubbing `BigQuery` is
easy; what the stub has to *return* is not.

## Evidence

Measured 2026-08-08 against `google-cloud-bigquery` 2.68.0, the version `libraries-bom` 26.85.1
resolves:

- `Job(BigQuery, JobInfo.BuilderImpl)`, both `Job.Builder` constructors, `Job.Builder#setStatus`,
  `Job.fromPb(BigQuery, model.Job)` and both `JobStatus` constructors are package-private. `Job` is
  not `final`, but with no reachable constructor it cannot be subclassed from elsewhere either.
  `JobInfo`'s public factories produce a `JobInfo`, never a `Job`. `BigQueryError`, by contrast,
  has two public constructors.
- The jar carries no `module-info` and no `Sealed: true`, so a test class declaring the package
  links against it on surefire's flat classpath.
- `BigQuery` has 55 methods (54 declared plus `getOptions()` from `com.google.cloud.Service`), of
  which the runner reads three: `create(JobInfo, JobOption...)`, `getJob(JobId, JobOption...)` and
  `delete(TableId)`. `getOptions()` is a fourth the runner never calls itself — `Job`'s constructor
  does, so a stub that throws there fails on the first submitted job.

## Consequences

- The coupling is to package-private members of a pinned dependency, so a release that moves one
  **the helper reaches** breaks a **test** at **compile** time. Which members are reached is the
  helper's javadoc's to enumerate, not this record's: the list grows with the values tests need
  ([#477] added a statistics reach, [#485] a `Table` one), and the copy that used to sit here
  went stale the first time it did. The rest of the Evidence list is why no other reach exists,
  not what is reached. A loud failure with an obvious fix, but it lands on whoever bumps
  `libraries-bom`, not on whoever is thinking about this class.
- The runner is driven through the real SDK types, which is how [#337]'s change measured — rather
  than modelled — the `Job#reload()` behaviour ADR-0018 records. It is not a behaviour a
  hand-written fake would have reproduced: a fake would have been written to match the assumption
  the change was about to disprove.
- The reach assumes one runtime package, i.e. a flat classpath. Running these tests on the module
  path would break it; nothing here does.

## Alternatives declined

- **A mocking framework.** This project has none, deliberately, and [#337] restated it. Mockito
  would also answer the *shape* of the problem — an unconstructible value — with a proxy whose
  behaviour is whatever the test scripts, losing the real `reload()` semantics above.
- **A narrow `@Internal` seam in production code**, in the shape ADR-0047 gave the Bigtable batcher
  adapter: replace `BigQuery` with an interface of our own, and `Job` with a handle type. Declined
  because ADR-0047's precedent does not reach this case — it exists because `BigtableDataClient`'s
  batcher could not be *driven* at all, whereas `BigQuery` is a plain public interface any test may
  implement. Here the obstacle is value *construction*, which deserves a factory, not a new SPI.
  The seam would also have to reimplement `reload()`'s semantics in code we own (a behaviour change
  wearing a refactor's clothes), and the delegating implementation would itself be untested — the
  "a seam whose wiring no test covered" failure ADR-0007 records from [#321].
- **`MockHttpTransport` under the real client.** Yields genuine `Job` objects with no
  package-private reach, but scripts the REST wire format by hand and pulls the SDK's HTTP layer
  into every assertion, for a new test dependency.

[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#337]: https://github.com/laughingman7743/flink-connector-gcp/issues/337
[#477]: https://github.com/laughingman7743/flink-connector-gcp/issues/477
[#485]: https://github.com/laughingman7743/flink-connector-gcp/issues/485
