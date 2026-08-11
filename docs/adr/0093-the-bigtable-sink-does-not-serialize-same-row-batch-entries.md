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

# ADR-0093: The Bigtable sink does not serialize same-row batch entries

- Status: Accepted
- Date: 2026-08-11
- Issues: [#471](https://github.com/laughingman7743/flink-connector-gcp/issues/471)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` and
  `docs/content/docs/connectors/table/bigtable.md`

## Context

The sink hands each record to the Bigtable client's bulk mutation batcher. `MutateRows` explicitly
permits entries to be applied in arbitrary order, including entries for the same row. Two entries
that update the same cell can also receive the same millisecond timestamp from the writer and
collapse to one version. The connector therefore cannot derive a last-write-wins guarantee from
submission order.

The failure had not been observed against the service. Adding per-key serialization without first
measuring it would put a key-indexed queue, memory retention, and head-of-line blocking on every
writer. Reducing a request to one entry would not establish order either: the batcher may keep
multiple requests in flight.

## Decision

**Keep the existing bulk path and document that same-key last-write-wins is not guaranteed.** The
connector does not add per-key serialization or synthetic timestamps. Jobs that require a defined
winner must encode the version in the row key or separate dependent mutations into writes whose
completion is awaited before the next begins. Upstream aggregation is sufficient only if it emits
at most one mutation per key for the entire write, not merely per window.

The decision follows a pre-registered real-service campaign. For each request size, one arm
submitted every first value and then every last value; a mirrored arm swapped those values to
detect a value-dependent result. Each request completed before the next one began. Both entries of
each pair used the same row, cell, and timestamp.

| Entries per request | Requests per arm | Pairs per arm | Forward reversals | Mirrored reversals |
|---:|---:|---:|---:|---:|
| 2 | 100 | 100 | 0 | 0 |
| 10 | 100 | 500 | 0 | 0 |
| 100 | 50 | 2,500 | 0 | 0 |
| 1,000 | 10 | 5,000 | 0 | 0 |
| 10,000 | 3 | 15,000 | 0 | 0 |
| 19,998 | 2 | 19,998 | 0 | 0 |

The campaign observed zero reversals across 86,196 pairs on 2026-08-11, using
`google-cloud-bigtable` 2.80.0 against a one-node SSD instance in `us-central1-b`. This negative
result does not override the API contract. It only means the proposed failure was not reproduced
with the tested client, service, region, request sizes, and timing.

## Alternatives declined

- **Serialize entries per row key in the sink** — would require a key-indexed pending structure,
  retain keys until asynchronous completion, and let one slow row block its later entries. The
  measured campaign found no service reversal to justify imposing that cost on every job, while
  the documented caveat would remain necessary for other writers and historical versions.
- **Assign monotonically increasing timestamps** — changes the connector's timestamp semantics,
  requires durable ordering state across recovery, and does not make `MutateRows` apply entries in
  order. A generated timestamp can choose which version a latest-cell read returns, but that is a
  different guarantee from ordered mutation application.
- **Set the batch element count to one** — produces concurrent single-entry requests, not a
  request-response sequence, so it cannot establish a winner.
- **Retain the campaign as a gated regression test** — would assert behaviour the Bigtable contract
  permits the service to change. The measurement belongs in evidence, not in a green-build gate.

## Consequences

- The connector keeps the throughput and bounded-memory properties of the client's bulk batcher.
- Same-key records close enough to share concurrent writes still have no connector-level winner.
- Documentation must distinguish the zero-reversal observation from a service guarantee.
- A future report containing a real reversal reopens the serialization decision with positive
  evidence and a reproducible workload.
