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

# ADR-0142: Lazy Bigtable data clients rely on owner teardown instead of operation leases

- Status: Accepted
- Date: 2026-08-23
- Issues: [#1086](https://github.com/flink-gcp/flink-connector-gcp/issues/1086)
- Modules: bigtable
- Current behavior: each owner closes its lazy data client under its own teardown protocol, and the
  holder retains neither the client nor its credentials after close

## Context

The scan reader's row-stream opener, the scan enumerator's row-key sampler, and the Change Streams
reader's opener each own a `LazyBigtableDataClient` that builds one data client on first use.
They share the holder implementation and its lifecycle question, not one holder or client instance.
Its monitor prevents construction from escaping a concurrent close, but its cached-client fast
path can return immediately before another thread clears and closes that client.

Synchronizing the return does not close the interval.
The caller could still release the monitor before starting its operation, and an operation lease
would have to span a synchronous sampling RPC, a row stream, or an asynchronous Change Streams
read to cover the three call shapes.
The question is therefore whether an owner permits that interval outside teardown.

The investigation also found that `close()` cleared the cached client but retained the pushed
`CredentialsProvider`.
For a service-account key file, that provider retains the loaded credentials after the seam has
finished with them.

## Decision

The three owners keep their existing teardown protocols, and the shared holder does not introduce
an operation lease.

The scan reader closes its opener after `SourceReaderBase.close()` has asked every fetcher to stop
and waited for their termination.
If Flink's configured source-reader close timeout expires, the opener may close while a fetcher is
still returning from its operation, but the task is already in teardown and the SDK client close
releases the shared transport instead of leaving the fetcher to own it.

The Change Streams reader opens on its task thread, marks itself closed on that same thread, asks
every active controller to cancel, and only then closes the opener.
A controller installed after teardown observes the reader's closed flag and cancels itself, while
a late response is discarded.

The scan enumerator deliberately permits close to overlap asynchronous sampling.
`PullAssignmentSplitEnumerator.close()` publishes its one-way closed flag before closing the
sampler, and its completion handler ignores either a plan or a failure that arrives afterwards.
The overlap can therefore finish or fail only work from an enumerator already being torn down.

The holder keeps its construction/close monitor and cached-client fast path.
Credential injection now takes that same monitor and refuses an injection after close, while
`close()` clears the client and credential references together before closing the client outside
the monitor.
Injection therefore waits for an in-progress construction, but every production owner injects
before its first use.
This preserves the non-blocking close around the SDK call while preventing a concurrent credential
injection from recreating the retention.
The retention guarantee is scoped to `LazyBigtableDataClient` itself.
For example, a Change Streams reader shares one provider with its separate restore resolver, and
this decision neither owns nor clears that resolver's reference.

## Evidence

Measured 2026-08-23 against one repository checkout at `a319281f`, Flink 2.2.1,
`google-cloud-bigtable` 2.81.0, and gax 2.83.0.

- `SourceReaderBase.close()` delegates to `SplitFetcherManager.close()`, which requests fetcher
  shutdown and waits up to the configured close timeout.
  `BigtableSourceReader.close()` invokes that path before it closes the row-stream opener.
- `BigtableChangeStreamReader.close()` sets `closed`, cancels every `ActiveRead`, clears its task
  state, and then closes the opener.
  Its existing tests exercise a response racing with close and a controller arriving after close.
- `PullAssignmentSplitEnumerator.close()` sets a volatile `closed` flag before closing its planner.
  `onPlanCompleted()` checks that flag before reporting a failure, installing a plan, incrementing
  metrics, or serving a waiting reader.
- `BigtableDataClient.close()` closes its enhanced stub.
  The pinned gax `BackgroundResource.close()` initiates orderly shutdown without awaiting
  termination, and the pinned gRPC channel refuses new calls while already-started calls may
  finish.

## Alternatives declined

**Synchronize the cached-client return.**
This narrows the interval without guarding an operation, because close can acquire the monitor
after the return and before the caller starts its RPC.
It adds contention without establishing a lifecycle boundary.

**Hold the holder monitor through each operation.**
This covers synchronous sampling only by making enumerator close wait for the sampling RPC's total
timeout.
For scan and Change Streams reads, it must instead hold the monitor for a stream's or callback's
whole lifetime, duplicating the cancellation and handover state their readers already own.

**Add reference-counted client leases.**
A lease still needs each owner to define when a stream or asynchronous read releases it, so it
duplicates all three owner protocols.
Waiting for the last lease delays teardown, while closing without waiting reduces to the current
owner ordering.

## Consequences

Normal operation cannot race an owner close: reader-side opens stop under their task teardown, and
enumerator completion after close is inert.
An operation already in flight may finish or fail after teardown begins, but it cannot publish a
new plan, rebuild a client, fall back to ADC, or retain the holder's client and credentials.

The owner-order tests are the executable boundary of this decision.
A future change that introduces another thread, moves opener close ahead of cancellation, or acts
on a late sampling completion must update or supersede this record.
