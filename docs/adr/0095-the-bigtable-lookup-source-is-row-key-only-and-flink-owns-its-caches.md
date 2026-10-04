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

# ADR-0095: The Bigtable lookup source is row-key-only, and Flink owns its caches

- Status: Accepted
- Date: 2026-08-11, revised 2026-10-04
- Issues: [#460](https://github.com/flink-gcp/flink-connector-gcp/issues/460),
  [#518](https://github.com/flink-gcp/flink-connector-gcp/issues/518) (under
  [#217](https://github.com/flink-gcp/flink-connector-gcp/issues/217)),
  [#1643](https://github.com/flink-gcp/flink-connector-gcp/issues/1643)
- Modules: bigtable, base (`table`)
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Context

ADR-0092 added bounded scans and projection to the Bigtable table source. A temporal lookup join
needs a different read shape: one row key from the input should become one Bigtable point read, or
one cache lookup whose contents came from the same projected table. Flink already defines the
lookup cache options and implementations, so inventing connector-specific cache policy would make
the same SQL mean different things between connectors.

## Decision

`BigtableDynamicSource` implements `LookupTableSource`. The lookup keys must include the DDL's
physical row-key column, interpreted after projection. Sync and async functions encode
the key with the same HBase-compatible `CellValueCodec` as scans and writes, apply the projected
family filter, convert with the scan's `RowToRowDataConverter`, and return an empty collection for
a missing row or null key.

Issue #1643 adds whole-family `ROW` equality keys with scalar children, supplied by constants or
input fields, while retaining the row key as the only addressing key.
The shared `base.table.LookupKeyFilter` projects the addressing tuple before the point read and
compares additional values against the converted row before Flink caches the result.
Binary children compare by content; nested NULL children compare equal, while a NULL whole-family
key or result produces no match.
`MAP` families, nested key paths and metadata keys remain unsupported.
The helper snapshots asynchronous comparison values and normalizes the entire PARTIAL cache key
into generic rows, so generated binary rows and converted generic rows compare consistently.
Flink still owns expiry, size limits, missing-key policy, metrics and lifecycle.
FULL accepts only the row-key equality key: its standard index uses the nested row objects'
`equals` and `hashCode`, which differ between `BinaryRowData` and `GenericRowData`.
Supporting whole-family equality there would require a normalized full-cache index and its scan
and reload contract, beyond this point-read refinement.

`lookup.async`, default false, selects the provider shape. The async function bridges the client's
`ApiFuture` with `ApiFutures.addCallback(..., Runnable::run)`: the callback completes the Flink
future directly and does not create an executor the task cannot manage.

Every standard `LookupOptions` cache key is imported unchanged. PARTIAL uses
`DefaultLookupCache` and Flink's sync or async partial-caching provider. FULL uses
`FullCachingLookupProvider` and the standard reload triggers. Flink 1.20's full-cache operator only
accepts an `InputFormatProvider` or `SourceFunctionProvider`, not the connector's modern
`SourceProvider`, so FULL has a single-split bounded `InputFormat` loader over the same projected
Bigtable query. `lookup.async = true` with FULL is rejected because Flink exposes no asynchronous
full-cache provider.

Point lookups make one exception to the connector-wide rule that client libraries own retries.
`lookup.max-retries`, a standard Flink option defaulting to 3, retries only
`DEADLINE_EXCEEDED`, `UNAVAILABLE` and `ABORTED`, and counts attempts after the initial read. The
lookup contract exposes this option and users expect it to control the lookup call; delegating only
to opaque SDK retry settings would accept the key while not giving it the promised meaning.
Those three are the set the client itself retries `ReadRow` on
(`EnhancedBigtableStubSettings.readRowSettings()`, google-cloud-bigtable 2.81.0), so the option
adds budget on top of the vendor's rather than a retry policy of the connector's own. Permanent
failures and an exhausted retry budget surface unchanged. Cache loads remain scans and retain the
scan client's retry behavior.

No connector metrics are added. Flink's cache implementations and the Bigtable client retain
ownership of their metrics.

Issue #518 makes a pushed source plan available to the FULL-cache loader without rebuilding or
dropping its range intersection, projection or best-effort cell-existence predicate.
Flink 2.2 keeps a scalar qualifier temporal-join predicate in `LookupJoin.where`; it does not
invoke `SupportsFilterPushDown` for that expression.
NONE, PARTIAL and FULL evaluate that same lookup residual and therefore expose the same rows, while
configured scan ranges continue to constrain both point reads and FULL-cache contents.
Point-range membership is distinct from split planning: a key equal to a closed start belongs to
the range even though it cannot split a non-empty left side from that range.
The lookup path now uses the shared membership operation instead of the stricter split-cut
operation, correcting the prior rejection of a closed-start key.

## Consequences

- Row-key equality is required; whole-family ROW equality adds a post-read condition in NONE and
  PARTIAL. It is refused with FULL. Nested qualifier conditions remain Flink residuals.
- A Data Boost application profile can load FULL through a scan, but cannot serve NONE or PARTIAL
  point reads. All lookup forms reuse `scan.app-profile-id` rather than adding a second profile key.
- The emulator suite executes hits and misses through sync, async, PARTIAL and FULL providers; the
  planner suite pins lookup selection after a reordered projection.
- One parameterized emulator case carries the same lookup residual and configured closed-start
  range through sync, async, PARTIAL and FULL providers.
- A second emulator case exercises whole-family ROW keys with NONE and PARTIAL in both provider
  modes, FULL's planning rejection, and scalar qualifier constants in ON and WHERE across all modes.
- A source-unit test inspects the FULL loader directly and pins that an already accepted scan
  filter keeps its range intersection and condition/projection composition.

The asynchronous retry loop is shared through `base.table.AsyncLookupRetries` ([ADR-0039](0039-retry-schedules-are-shared-retry-loops-are-not-and-every-schedule-jitters.md)); the read, conversion and failure classifier stay in this connector.

## Alternatives declined

- Connector-specific cache keys: Flink already owns compatible cache policy and validation.
- Treating async plus FULL as sync: accepting an option while ignoring its requested provider shape
  is worse than rejecting the unsupported combination.
- Retrying every failure: authentication, permission and invalid-argument failures do not become
  valid with another point read.
- Retrying `RESOURCE_EXHAUSTED`, which the sink's classifier does treat as transient: the client
  excludes it from `readRowSettings()`'s retryable set, Bigtable raises it for an exhausted admin
  API quota or a node or storage limit, and this loop carries no backoff of its own, so the whole
  budget would be spent in quick succession on a refusal no point read clears. The sink's set gates
  a dead letter rather than another call, which is why the same status belongs in it.
- Lookup-specific metrics: they would duplicate the cache and client layers' ownership.
