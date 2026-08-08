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

# ADR-0071: A lost table-creation race is retried by a wrapped `TableAdmin`

- Status: Accepted
- Date: 2026-08-08 (measured)
- Issues: [#383]
- Modules: bigquery (`sink.tables`, `sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Table auto-creation

## Context / Evidence

Both storage writers created a missing table from inside a retry loop but with the call **outside
the loop's `try`** — `BigQueryBufferedStreamWriter.createStream` and, on the at-least-once side,
every path reaching `createTable`. `BigQueryTableAdmin.create` swallowed HTTP 409 and wrapped
everything else in an `IOException`, so 409 was the only creation failure the loops tolerated and
anything else failed the write outright.

The trigger is the creation race itself. Every subtask races to create the same missing table; the
losers are normally answered 409, which is why the race looked free. **Measured 2026-08-08**, by
racing sixteen concurrent `BigQueryTableAdmin.create` calls at one absent table:

```text
code=403  reason=rateLimitExceeded  isRetryable()=false
Exceeded rate limits: too many table update operations for this table.
```

Five of the sixteen were answered that way; the rest created the table or got the 409. The message
alone had been recorded before (on `BigQueryBufferedStreamMissingTableITCase`, at parallelism ten),
but not the code or the reason — and this ADR exists partly because ADR-0030 records what guessing
at an unmeasured code costs.

Two facts came out of the same run that the documentation would not have settled:

- **`isRetryable()` is `false`.** `BigQueryImpl.create` *does* run under `runWithRetries`, but the
  handler consults `BigQueryException.isRetryable()`, whose `RETRYABLE_ERRORS` set is
  `500/502/503/504` alone (read in google-cloud-bigquery 2.68.0). So the rate limit reaches the
  caller on the first attempt: there is no SDK retry to sit behind, and a connector rule is the only
  place this can be fixed.
- **The reason is the one already in this file.** `BigQueryTableAdmin.isLostRace` has matched
  `rateLimitExceeded` since schema updates were written, for the *same* per-table metadata-update
  quota. Creating and updating a table spend one budget; the constant now has two consumers.

## Decision

**The retryability verdict lives beside the REST client, and travels as a type.**
`BigQueryTableAdmin.isRetriable` answers it — `isRetryable()` borrowed from the client, plus HTTP
429, plus the `rateLimitExceeded` reason — and `toFailure` turns a failed creation into either a
`RetriableTableAdminException` or a plain `IOException`. Not a predicate the writers call over the
cause chain, and emphatically not a branch in `AppendErrorClassifier`: that classifier reads gRPC
and gax status codes off the Storage Write API, this reads an HTTP code and a reason string off the
REST API, and the one thing `TableAdmin` exists for is to keep `BigQueryException` away from the
writers. A caller that had to reach through the SPI for a vendor exception would be undoing the
abstraction it is written against.

**The retry is a decorator, `RetryingTableAdmin`, not a loop at the creation sites.** The first
version of this change was a shared static helper the writers called instead of the SPI, and it was
wrong in a way worth recording, because it looked right: it took the `TableAdmin` as its first
argument and forwarded three more, so the writers stopped calling the SPI the whole abstraction
exists for, and correctness became "did every site get changed". It had not: the FILE_LOADS
committer's creation, in `LoadJobOrchestrator`, was missed exactly because a use-site rewrite has
no natural stopping point. Review caught it; nothing else would have.

Wrapping moves the decision to where a `TableAdmin` is **constructed**, and there are three of
those — `BigQueryDefaultStreamSink.createWriter`, `BigQueryBufferedStreamSink.restoreWriter`, and
`FileLoadsCommitter`'s default factory — which is a list one can finish. Every caller keeps the
interface it already held. And the budget becomes structural rather than disciplinary: no site
passes a schedule, so no site can pass the wrong one. The mutant that had to police that choice
(recovery → schema-wait at `createTable`) has no code left to mutate.

**None of the four creation sites retried, including the two that look as if they did.** The issue
names the call sitting outside the loop's `try`, which is the visible half; the other half is that
even inside the `try` they would not have repeated. `createTableIfMissing` and `createStream` are
guarded by a `tableCreated` flag, so the next pass skips creation entirely, and `recoverDestination`
and `reconcileSchema` return to a different loop — or none — before their caller comes round again.
So the fix could not have been "move the call inside the `try`" either.

**FILE_LOADS is wrapped too, on `schemaReconcile*` rather than a knob of its own.** That path races
less — loads commit on one subtask, so nothing there competes with itself — but a second job or a
restart still can, and leaving it out would make "a creation is retried" depend on which write
method was picked. The reconcile schedule is already this write method's budget for contention on
exactly that quota, since the etag race `updateSchema` loses is the same per-table budget a creation
spends; a third `recovery*` family here would be a knob nothing distinguishes.

**Only `create` retries.** `getSchema` and `updateSchema` pass through: the latter reports its own
lost race as `false` precisely so the caller re-reads and re-derives, and a blind repeat inside the
wrap would re-submit a proposal built against a snapshot known to be stale.

**The budget is always the recovery schedule**, whichever budget the repair around it is running
on. This is `scheduleFor`'s reasoning from ADR-0030 applied one level down: a rate limit on table
updates clears in seconds, while the schema-wait schedule is fifteen minutes, and letting a creation
inherit that would turn a permission failure that is immediate and well named into a checkpoint
timeout with no cause attached. No new knob: `recovery*` already means "how long may the connector
spend repairing", and there is no workload for which a *different* number is right here.

**`quotaExceeded` is deliberately not retriable**, though the issue named it — on the
widen-only-what-was-observed rule ADR-0030 exists to record. What the measurement answered was
`rateLimitExceeded`, and no creation here has been seen to answer the other reason. BigQuery
attaches `quotaExceeded` to quotas that refill on boundaries longer than any connector budget as
well as to rates, so accepting it unmeasured would risk converting a failure that already names its
own reason into a budget exhaustion that does not — the same direction of harm `scheduleFor` guards
against. That last sentence is the reason to be cautious, not a measured claim about which reason
string a particular BigQuery cap uses.

Three things not to re-derive:

- **429 is in the rule although the measurement was 403.** Not padding: the rule is about a rate
  limit, and the measurement pins one shape of it. Carrying both means a service that starts
  answering the standard code is already handled, and the cost of the extra clause is a code no
  BigQuery path has been seen to answer at all.
- **`isRetryable()` is borrowed rather than restated.** A client release that widens its own
  retryable set widens this one, which is the direction that stays correct without an edit. A test
  pins the *current* answer for the rate limit specifically, so a release that starts retrying it —
  and would then have two layers retrying at once — fails here rather than in production.
- **The nested budgets multiply, and are bounded.** Each writer guards creation with a
  `tableCreated`-style flag, so at most one creation budget is spent per repair: worst case is one
  recovery budget for the creation plus one for the enclosing loop, about two minutes at the
  defaults, not the product of the two attempt counts.

## Consequences

- A job at high sink parallelism whose destination table does not exist starts without the restart
  it used to need. That was never data loss — the restart recovered it — so what this buys is
  startup latency and one less red-looking failure in the logs, not correctness.
- `tablesCreated` still counts one creation per table this subtask asked for, because the retries
  happen below the counter. A test pins that, since counting attempts there would silently redefine
  what the metric answers.
- A genuinely terminal creation failure is unchanged: not retried, reported with its own message.
  The `bigquery.tables.create` denial ADR-0030 talks about still surfaces immediately — and now
  carries, as a **suppressed** exception, the verdict that made this writer try to create a table
  at all. `ensureState` discarded it: the `catch` that recognised the missing-table verdict threw
  the recovery's failure without chaining or suppressing the one it had caught, so a reader was
  told "cannot create the table" with no way to see whether the masked `PERMISSION_DENIED` meant a
  missing table or an existing one these credentials cannot write to — the ambiguity ADR-0030
  exists to name. Suppressed rather than chained, because the recovery failure is the one to act
  on; the same shape as `BigQueryLoadJobRunner.create`'s conflict lookup. Two `LOG.info` lines
  gained the cause for the same reason, one of which was **passing it already** and having it
  dropped: `logRepair`'s `CREATE_TABLE` branch had one placeholder and two arguments, and slf4j
  discards an extra argument that is not a `Throwable`. No log assertion for either — a repair the
  `tablesCreated` counter and the behavioural tests already cover is exactly the second report
  `LogCapture`'s javadoc declines to pin.
- `RetriableTableAdminException` is `public` because the writers are in a sibling package — the same
  reason `AppendErrorClassifier.isExistenceMasked` is — and `@Internal` either way. A second
  `TableAdmin` implementation inherits the contract from the SPI's javadoc, which also says what to
  do when it cannot tell the two cases apart: report the terminal one, since a repeat that can never
  succeed spends a budget before failing.
- **The wrap is invisible to its callers, so three tests assert it was applied** — one per
  construction site, each checking the schedule's attempt count and not merely the type, since the
  count is what names *which* schedule was taken. Without them a `createWriter` that stopped
  wrapping would ship green: every writer and committer test injects its own admin. That is
  `BufferedStreamCommitter.getCreateDisposition`'s argument, and `FileLoadsCommitter.tableAdmin()`
  widened to package-private for it.
- **`BigQueryBufferedStreamSink`'s test seam now takes a `Supplier<TableAdmin>`.** Building the
  admin reads the options for its budget and can therefore throw, and the buffered sink opens the
  failure handler in a *different* method from the one that used to build it — so evaluating the
  factory outside the guard left the handler unopened on that one path and open on every other.
  `BigQuerySinkFailureHandlerOpenTest` caught it. The supplier puts the construction back inside the
  guard, which is the shape `FileLoadsCommitter` already had.
- `NoopTableAdmin.create` in the writer tests keeps the SPI's `throws IOException` although it never
  throws. Without it no subclass can script a failing creation at all — an override may not widen
  the checked exceptions of what it overrides — so it is load-bearing rather than leftover.
- **`StubBigQuery` moved up to `io.github.flink.gcp.connector.bigquery`, beside `RealBigQuery`, and
  became `public`.** Not tidying: `isRetriable` and `toFailure` can be tested directly and the
  writers can be tested against a scripted `TableAdmin`, but *nothing* drove
  `BigQueryTableAdmin.create` itself — so replacing `throw toFailure(...)` with a plain `IOException`
  compiled, left every other test green, and silently restored this defect, since the writers route
  on the type alone. It survived the first mutation batch and is what the move exists to kill. The
  stub gained a scripted `create(TableInfo)` whose success path deliberately throws rather than
  returning: a `Table` is another SDK-owned value object nobody can construct (ADR-0067), and no
  caller reads the return. The move also gave the 409-is-success rule its first unit test.

[#383]: https://github.com/laughingman7743/flink-connector-gcp/issues/383
