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

# ADR-0084: The BigQuery read path is bounded and explained, and the client's own resumption is the loop

- Status: Accepted
- Date: 2026-08-10 (client behaviour read out of the pinned sources; stream counts measured against
  BigQuery the same day)
- Issues: [#391], [#64]
- Modules: bigquery (`source`, `source.reader`, `source.split`, `source.enumerator`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Source

## Context / Evidence

[#390] left three gaps in the read path, and [#391] named the first as a measurement rather than a
design: does the pinned client's built-in `ReadRows` resumption hold, and if it does, how much of a
retry loop does the connector still owe? Read out of `google-cloud-bigquerystorage` 3.30.0 (the
version `libraries-bom` 26.85.1 resolves) and its gax 2.82.0:

- `BigQueryReadClient.readRowsCallable()` is built by `EnhancedBigQueryReadStub`, which installs
  `ReadRowsResumptionStrategy`. Its resume request is `originalRequest.getOffset() + rowsProcessed`,
  where `rowsProcessed` counts the rows of every response handed to the caller. **A broken
  `ReadRows` is therefore resumed at exactly the right row, transparently — no duplicate, no gap.**
- What earns a resume is `ApiResultRetryAlgorithm` over `Errors.isRetryableStatus`, plus the
  configured retryable codes: **`UNAVAILABLE`**, an `INTERNAL` whose description names one of a
  fixed list of transport faults (`RST_STREAM`, `Received unexpected EOS`, `Connection closed with
  unknown cause`, …), and a `RESOURCE_EXHAUSTED` **only when its trailers carry `RetryInfo`**, whose
  `retryDelay` is then honoured. A bare `INTERNAL` and a bare `RESOURCE_EXHAUSTED` are excluded
  deliberately — the vendor's own comments say so.
- The backoff is 100 ms growing by 1.3 to a 60 s cap, and **"jitter" understates what gax does with
  it**: `ExponentialRetryAlgorithm` draws each wait uniformly from `[0, nominal)`, so the expected
  wait is half the sequence rather than centred on it. The `INTERNAL` and `RESOURCE_EXHAUSTED`
  families do not reach that code at all — `ApiResultRetryAlgorithm` returns settings of its own
  first, with the randomized delay set to a flat **1 ms** (`DEADLINE_SLEEP_DURATION`) for the
  `INTERNAL` family and to the server's `retryDelay` for the other, and returns `null` for
  `UNAVAILABLE`, which is what lets the exponential algorithm answer that one. **`maxAttempts` is
  unset and the total budget is twenty-four hours.**
- `StreamingRetryAlgorithm` resets the attempt count and the delay whenever an attempt produced a
  response, while carrying `firstAttemptStartTimeNanos` forward. So **`maxAttempts` counts
  consecutive failures without progress, and `totalTimeout` runs from the moment the stream was
  opened** — they are not two spellings of one bound.
- gax's `ServerStream` buffers one response ahead of the consumer (`QueuingResponseObserver`, a
  two-slot queue). A response already counted into `rowsProcessed` is still delivered to the caller
  when the attempt behind it fails, so the client's accounting and the reader's `deliveredOffset`
  cannot drift apart.

Measured against BigQuery on 2026-08-10, choosing a fixture for the real-GCP case:

| Table (`bigquery-public-data`) | Size | Streams, every column | Streams, one column |
|---|---|---|---|
| `samples.shakespeare` | 6 MB | 1 | 1 |
| `usa_names.usa_1910_current` | 195 MB | 1 | 1 |
| `austin_bikeshare.bikeshare_trips` | 264 MB | **4** | 1 |
| `world_bank_intl_education.international_education` | 625 MB | 6 | 2 |
| `google_trends.top_terms` | 3.3 GB | 64 | 11 |
| `new_york_citibike.citibike_trips` | 8.0 GB | 59 | 12 |

Two things ADR-0079 did not have. The threshold at which BigQuery splits a table at all sits
**between 195 MB and 264 MB**. And **a projection lowers the stream count**: the count follows the
bytes actually selected, not the table's size, which is why a single-column read of an 8 GB table
gets twelve streams where the whole table gets fifty-nine.

## Decision

**The connector runs no retry loop of its own over `ReadRows`, and that is a decision rather than an
omission.** A connector-level reopen at the reader's own offset is what the client already does,
more precisely — the only thing it could add is retrying the codes the client declines, and the
vendor declines those with a stated reason this project has no evidence against. Widening
`setRetryableCodes` is refused on the same ground.

**What the client lacks is a stop, so the connector supplies one: `retryMaxAttempts`, default 25.**
The knob is spelled bare `retry*` because it is forwarded to the SDK rather than spent here, and it
sets `maxAttempts` and nothing else — the schedule stays the vendor's. `maxAttempts` and not
`totalTimeout`, because of the reset asymmetry above: shortening the total budget would cut off the
retry of a stream that had been healthy for hours, while the attempt count bounds exactly the thing
worth bounding. What 25 consecutive failures *costs* depends on the failure, and the two ends are
far apart: an `UNAVAILABLE` waits a uniform draw from `[0, nominal)` over a nominal 100 ms × 1.3
sequence, so twenty-five attempts is about three minutes at worst and half that on average, while
the retryable `INTERNAL` family is retried a flat millisecond apart with no growth at all and
reaches the bound almost at once. Without a bound, a stream that will never come back holds a split
fetcher for twenty-four hours while reporting nothing.

**Nothing reports a retry where anyone would see it, so `readRetries` does.** gax does log one,
from `BasicRetryingFuture` — through `java.util.logging` at `FINEST`, which is neither Flink's
logging framework nor a level any deployment runs at. The bound only catches a stream that is
making no progress; one that keeps failing and resuming resets the attempt count every time, never
trips the bound, never fails the job, and simply reads slowly. The counter is wired through
`BigQueryReadSettings.setReadRowsRetryAttemptListener`, reached by a `setRetryListener(Runnable)`
seam on `RowStreamOpener` that the source calls once per subtask before any fetcher starts — the
client captures the listener when it is built, so a later registration would be ignored. It is a
`ThreadSafeSimpleCounter`: the callback runs on the client's retry scheduler, a thread this
connector neither owns nor can name. The `Runnable` deliberately does not carry the SDK's status and
metadata, so nothing outside the client wrapper names types it would not read.

**A read that fails after its session expired says so, and the split is what carries the expiry.**
The split already carries the session's Avro schema because a reader is handed splits and nothing
else; the expiry travels the same way, for one purpose. The failure message names the gRPC status
when the failure carries one and, when the session's expiry has passed, explains that a restart
resumes against the same expired session and cannot help.

**Nothing pre-empts the expiry, and no claim is made about what BigQuery answers.** The expiry is
BigQuery's to apply and the connector's clock is not the one that decides, so neither the reader nor
the enumerator refuses to proceed because a local clock says the session is old — that would fail a
read the service might still have served. Confining the connector to explaining a failure it
actually met is also what makes this testable without a six-hour wait: the sentence is true of the
session either way, so no measurement of the service's expiry status code is owed. The enumerator's
restore-time check stays a warning for the same reason.

**Both split serializers move to version 2 and read version 1.** A version-1 split comes back with
no expiry and loses only the annotation on a failure it may never meet. The enumerator state
serializer derives the split layout from its own version rather than writing a second version
number beside the splits: independent formats, one line of mapping. Migrating rather than rejecting
is cheap here and keeps a savepoint written by today's `main` restorable.

**The real-GCP case reads a public dataset**, `austin_bikeshare.bikeshare_trips`, all columns, at
parallelism 2 with one deliberate failure. ADR-0079 recorded that a table large enough to be split
"costs more than the assignment logic is worth"; that priced *creating and storing* one. A public
dataset removes the storage entirely and leaves the Storage Read API's per-byte charge — 264 MB at
$1.10/TiB, a fraction of a cent per run, and 14 s of wall clock (measured 2026-08-10). The table is
not ours to hold still, so the read and the row count it is checked against are pinned to one
instant with `snapshotTime` and `FOR SYSTEM_TIME AS OF`. Reading every column is load-bearing: the
projection measurement above shows a single-column read collapses the fixture to one stream.

## Consequences

- The job that used to hang for a day now fails in minutes, and Flink's restart strategy is the
  outer loop — restoring from the last checkpoint, where each stream resumes at the offset that
  checkpoint holds rather than being read from the top.
- `readRetries` is the only reader metric this connector cannot increment itself, and its thread is
  the reason `BigQuerySourceReaderMetrics` now documents three of them.
- `RowStreamOpener` gained a default no-op method, so a fake opener that never retries stays
  unchanged. Its contract says registration happens before any fetcher starts; an implementation
  that captured the listener later would silently report nothing.
- A savepoint written by a version of `main` between [#390] and this change restores, with no
  session expiry on its splits.
- The weekly E2E suite gains a case that reads a third-party public table. If Google retires or
  reshapes `austin_bikeshare.bikeshare_trips`, that case fails — and the assertion that more than
  one subtask read is what turns a table that stopped splitting into a failure rather than a quiet
  loss of coverage. **The response is meant to be a one-line change**, so the case names no column
  at all: its reader schema declares no fields (Avro skips every writer field a reader does not
  declare), its row-count query derives its path from the same constant, and it asserts how many
  rows came back rather than what was in them. The requirements on a replacement are only that
  BigQuery splits it and that it is small enough to read in a test.
- Not covered, and deliberately: how much a retrying-but-progressing stream costs in wall clock.
  `readRetries` reports that it is happening; deciding what to do about it is an operator's, and a
  connector-side response would be the retry loop this record declines.

[#64]: https://github.com/laughingman7743/flink-connector-gcp/issues/64
[#390]: https://github.com/laughingman7743/flink-connector-gcp/issues/390
[#391]: https://github.com/laughingman7743/flink-connector-gcp/issues/391
