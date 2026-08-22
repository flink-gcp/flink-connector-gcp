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

# ADR-0127: A configured name is checked for what this project will do with it

- Status: Accepted
- Date: 2026-08-22 (measured 2026-08-22), revised by [#1009] and [#1013] (2026-08-22), then by
  [#1019] (2026-08-22)
- Issues: [#984], [#920], [#976], [#1009], [#1013], [#1019]
- Modules: base (`base.options`), bigquery, bigtable, cloudtasks, pubsub, spanner
- Current behavior: `docs/content/docs/connectors/_index.md`, "What a builder checks"

## Context

[#920] and [#976] finished moving every blankness check in `src/main` onto `Character.isWhitespace`
semantics, and left one question behind: having settled what counts as blank, does a connector also
say which *characters* a configured project, instance, table, topic, subscription, queue, dataset,
app profile or index name may contain? [#984] asked it, and proposed that answering it needed a
credential-gated run against the real services.

It did not. The measurement that decides the question is a read of the pinned client sources.

**The premise.** `String.trim()` strips every character at or below `U+0020`, so the pre-[#920]
idiom rejected a value made only of C0 control characters as a side effect of its implementation.
`Character.isWhitespace` does not call those characters whitespace, so `isBlank()` accepts them. On
that reading the unification lost a check, and the loss would matter most for `appProfileId`, which
[#984] describes as reaching the client as gRPC request metadata — where a control character might
surface as a transport error naming no option at all, rather than as a service error naming the
profile.

**What the client actually does with it.** gax does not put the value in a header as written. Every
Bigtable call routes its request parameters through `RequestUrlParamsEncoder`, which percent-escapes
each value with `PercentEscaper("._-~")`, so a NUL arrives as `%00` inside `x-goog-request-params`:
a legal HTTP/2 header value. The same value also travels as the request message's own
`app_profile_id` field, where a NUL is valid UTF-8 and the encoding raises nothing. Neither path can
produce the opaque transport failure the question was built on. Spanner, which [#984] names beside
Bigtable, has no app-profile concept at all.

**What the tree was actually doing.** [#984] reports that no site validates the character set of any
of the values it names, and the first draft of this ADR answered that the repository had a
consistent unwritten position. Reviewing that draft falsified both. Six byte-identical copies of a
private `checkComponent` already rejected `/` and edge whitespace at `project`, `dataset`, `table`,
`instance`, `database`, `topic`, `subscription`, `location` and `queue` — and because that check
compares against `trim()` rather than `isBlank()`, an *edge* control character was still rejected at
all nine, so nothing was lost there by the unification either. But the same rule was not applied to
every value of the same shape. `parentProject` was blank-only while `ReadSessionRequests` composed
`"projects/" + parentProject`; `queryResultDataset` was blank-only while it became a segment of a
`TableId`; the FILE_LOADS `tempDataset` was checked only when `CommitPlanner` reached it mid-job;
and `BigQuerySinkBuilder.location` carried no check past `checkNotNull` at all.

So there was no settled position to write down. There was a rule applied in six places, absent in
four, and stated nowhere.

**What the rule has to account for.** Three Cloud Tasks checks look at first like counter-examples,
and reading them is what produced the rule below. `HttpTargetSerializationSchema.checkUrl` requires
an `http://` or `https://` prefix and then forwards the string untouched; `HttpTargetSpec` applies
the same rule to `http.url` and *discards* the `URI` it parses; `AppEngineTargetChecks`
`checkHeaderName` refuses `Host`, `Content-Length` and the `X-Google-`/`X-AppEngine-` prefixes. None
of them parses the value for its own use. Neither reason is "the service would reject it", and both
are about where the failure lands and what it names. A reserved header is one this connector's own
documentation already calls "owned by Cloud Tasks and rejected before task creation": setting it
cannot take effect, so a user who set it deliberately is told at the setter rather than left to
notice its absence later. What Cloud Tasks itself does with such a header, refuse the request or
replace the value, is not measured here, and the check does not rest on which. A target URL is
checked twice — at the builder for a static value, and again in `serialize` for one an extractor
produced — and the second call is what turns a service rejection naming the whole request into one
naming `extracted url`.

## Decision

**A connector checks a configured name when the failure of not checking it would be worse than the
service's own rejection.** Three shapes qualify, and each is a property of what the code does with
the value:

1. **A component of a resource path the connector composes** is rejected for `/` and edge
   whitespace. A `/` inside a component does not fail — it silently addresses a *different*
   resource, so the service answers accurately about a name the user never typed. This is
   `ResourceNames.checkComponent`, and it now reaches every such component rather than nine of them.
2. **A value the connector parses, splits, quotes or emits** is checked against the grammar that
   will read it, because otherwise the failure names the grammar rather than the option:
   `SpannerIdentifier` (which decodes canonical quoting), `SpannerTableName`, `RowRangeParser`,
   `RowKeyDecoder`, `EmulatorEndpoint`, `FileLoadsOptions`' staging path, `AppEngineTargetChecks`'
   relative URI, and the protobuf and Avro identifier patterns in `AdditionalField` and
   `TableSchemaToAvroConverter`.
3. **A value whose service-side failure would be silent, or per-record, or would not name the
   option** is checked even though the connector only forwards it. That is what the three Cloud
   Tasks checks are, and stating it is what keeps them from reading as exceptions.

**Everything else about a name is the service's answer.** Whether a project, dataset, table,
instance, database, topic, subscription, queue, app profile, change stream or index exists, and
whether its name is one that service accepts, is answered where the resource lives. Those values get
presence and blankness — `ResourceNames.checkNotBlank` — and no more. A copy of a service's naming
rules kept here would go stale in the direction that hurts, refusing a name the service would have
accepted; `docs/content/docs/connectors/table/bigquery.md` already declines that trade for clustering
types, on the grounds that "that list has grown before, and a stale copy here would refuse a table
BigQuery would have created".

A proposal to add a character check names which of the three shapes it is. "The service might reject
it" is not one of them, and shape 3 asks for the failure it prevents, not the possibility of one.

**A check runs where the value is configured.** The first version of this ADR said which values are
checked and left where open, and that silence is what [#1009] and [#1013] found: `EmulatorEndpoint`
is named under shape 2, yet the Bigtable and Spanner lookup runtimes held the option's value as a
string and parsed it on a TaskManager, so a malformed endpoint on a table joined as a lookup
dimension survived planning and job submission. For a DataStream caller the configuring point is the
builder setter, which is where [#235] put it. For a SQL caller it is the table factory reading the
option, and the two connectors whose lookup runtimes held the value now parse there.

The other three were left alone at that point, because their factory hands the string to a dynamic
source or sink that reaches the builder setter during plan-to-runtime translation — later than a
factory check, but still on the client, so no job was ever submitted over an endpoint nothing had
looked at. [#1019] is what that reasoning missed: where a check runs decides what it can name. A
parse reached through `emulatorEndpoint(String)` answers a caller who wrote `emulator-endpoint` by
naming a setter they never called, which is the failure [#235] set out to remove — it names *a*
setting, and it is the wrong one — and the one [#895] had already fixed once, for BigQuery's two
`emulatorRestEndpoint` setters against DataStream callers. Being early enough is therefore not the
whole rule: the value is configured in the `WITH` clause, so the factory is where it is checked, and
all five now parse there.

A later re-check is not a contradiction, and two shapes of it appear here. One is a value that does
not exist until later: a Cloud Tasks target URL an extractor produced cannot be checked at the
builder, which is why `serialize` checks again. The other is an `@Internal` constructor a check
would otherwise sit in front of rather than behind — the lookup runtimes keep their parse for that
reason, passing the same option key, so the sentence is identical wherever it lands. Every builder
setter keeps its own parse on the same footing: it is the check behind the DataStream API, where the
setter's name is the name the caller wrote. What is ruled out is a value that *is* known at
configuration time and is checked only at runtime, because that buys nothing and costs a submitted
job.

**A shape check goes behind every check that refuses an option outright, never in front of one.** A
DDL being told to remove an option is not helped by an answer about that option's shape, and the
option pre-empted need not be the one being parsed: Bigtable's `emulator-endpoint` is bounded-only,
so the parse follows the scan-mode check, but Spanner accepts one in every mode and the parse still
follows `validateSourceMode`, because that call refuses *other* options. Both connectors' sink paths
sit behind their change-stream-option refusal for the same reason. Getting this wrong is invisible
in a passing build, so each ordering carries a test that asserts the removal message and asserts the
shape message is absent.

"Every check" means every check the factory method makes before it starts assembling a source or
sink. Where an option mapper evaluated during assembly refuses an option too — `TopicCreateOptionsMapper`,
`PublisherOptionsMapper`, `SubscriptionCreateOptionsMapper` and `TableCreateOptionsMapper` all carry
"remove the options" refusals — a DDL that trips one of those *and* carries a malformed endpoint
reads the shape message first. Moving behind them would mean hoisting five mapper calls out of
Pub/Sub's constructor arguments and three out of BigQuery's builder chain, the latter inverting the
ordering `BigQueryDynamicTableFactory` deliberately documents, that the checks the class owns run
ahead of the first mapper. The endpoint parse sits with those checks, which is where the sibling
shape checks each factory already had sit too.

It goes behind the checks that report a *required* option missing as well, for a different reason:
a table that has not said where it points should hear that first. Three connectors get this from
`helper.validate()`, which reports a missing `requiredOptions()` entry before any connector check
runs. Pub/Sub and BigQuery declare theirs conditionally instead — one factory serves directions
that need different options — so their `orElseThrow` and `destination(...)` checks are ordinary
statements, and the parse follows them so that all five answer alike.

## Evidence

Measured on `origin/main` at `fb600036`, 2026-08-22.

**The client's handling of `appProfileId`.** `EnhancedBigtableStub.composeRequestParams` (line 1195
of `google-cloud-bigtable-2.81.0-sources.jar`) builds the routing map, and
`GrpcUnaryRequestParamCallable` hands it to `RequestUrlParamsEncoder`, whose `percentEncodeString`
escapes with `PercentEscaper("._-~")` (`RequestUrlParamsEncoder.java:50`, `gax-2.83.0-sources.jar`).
Both versions are what the BOM resolves, checked with `dependency:tree` rather than assumed.

**The divergence [#984] describes is real, and is 23 characters**: comparing `s.trim().isEmpty()`
against `s.isBlank()` for every code point up to `U+0020` gives `U+0000`-`U+0008` and
`U+000E`-`U+001B`. That does not make the unification a regression. `trim()` rejected those
characters because of how it is implemented, not because this project decided a NUL is blank, and a
string of NULs is neither empty nor whitespace.

**The consistency gap, and the cost of closing it.** The six `checkComponent` copies were
byte-identical, 11 lines each, same order and wording. Four values of the same shape lacked the
check. Extracting every literal passed to those four setters across the repository, the test tree
and `docs/` found nothing that a uniform check would newly reject: the values in use are
`my-project`, `payer`, `billing-project`, `scratch`, `temp_dataset`, `US`, `asia-northeast1`. Two
values were deliberately left on blankness alone, and both are outside shape 1 rather than
exceptions to it — Pub/Sub's `kmsKeyName` *is* a `projects/…/cryptoKeys/…` path, and
`BigtableSourceBuilder.prefix("")` is a documented "scan the whole table" contract with a test
naming it.

**The size of the alternative [#984] proposed.** 54 blankness checks across the five connector
modules (bigquery 16, bigtable 9, cloudtasks 9, pubsub 9, spanner 11) in 37 files, sharing no helper
across modules before this change: 30 `String.isBlank()` and 24 `StringUtils.isNullOrWhitespaceOnly`.
A predicate encoding *which characters* a service accepts would have had to reach all of them.

**What a SQL caller read before [#1019].** Measured 2026-08-22 on `7e1a13c4` with the new factory
parses removed, by driving each dynamic source or sink to its runtime provider with
`'…' = 'localhost'` and reporting the root cause. Every arm is the setter name, not the DDL key:

| Path | Root cause |
|---|---|
| Pub/Sub sink and source, `emulator-endpoint` | `IllegalArgumentException: emulatorEndpoint must be host:port, was 'localhost'` |
| Cloud Tasks sink, `emulator-endpoint` | the same sentence |
| BigQuery sink, direct-table source and query source, `emulator-endpoint` | the same sentence |
| BigQuery sink and query source, `emulator-rest-endpoint` | `IllegalArgumentException: emulatorRestEndpoint must be host:port, was 'localhost'` |
| BigQuery **direct-table source**, `emulator-rest-endpoint` | *no throwable* — the value is dropped before any parse |

The last row is the one that could not be read off the code with confidence, and it is the only
configuration this change refuses that nothing refused before.

## Alternatives declined

**Validate the character set of forwarded names at the setter.** This was [#984]'s second answer,
conditional on the measurement showing a bad failure quality. The measurement shows the opposite for
the one value it named, and the general cost stands on its own: any character set encoded here is a
copy of the service's rules with no way to learn that they changed.

**Restore `trim().isEmpty()` at the affected sites.** Declined as a misreading of what [#920] did,
and unnecessary at the nine path-component names, where `value.equals(value.trim())` already rejects
an edge control character.

**Leave the four unchecked values alone and record the rule as aspirational.** Declined because an
ADR describing a rule the code does not follow is worth less than no ADR: the next reader cannot
tell which sites are the rule and which are the omission. The gap was four setters and no test
changes.

**Extend `OptionChecks`.** Declined because that class is about `Duration` and says so, and because
its javadoc refuses to become a general-purpose precondition library. `ResourceNames` is a sibling
with its own argument, which is what that refusal asks for.

## Consequences

Four values that previously accepted a `/`, edge whitespace or a blank string now reject it at the
setter: `parentProject`, `queryResultDataset`, the FILE_LOADS `tempDataset` (which moves from a
commit-time failure to a build-time one) and `BigQuerySinkBuilder.location`, plus the four Proto and
Avro field-path setters that previously took a blank path. Nothing in the repository, its tests or
its documentation used such a value. A configuration that did was already reaching the service with
a name it could not resolve.

A name carrying a control character away from its edges still reaches the service. What the service
then answers is not measured here and this ADR does not claim it: the question that would have
needed a billed run was whether the *client* could fail opaquely first, and it cannot.

Tightening an input check after `1.0.0` is the expensive direction, since it rejects configuration
that previously worked. That is why the question carried the release milestone, and why the four
setters were brought into line now rather than recorded as known gaps.

A malformed `emulator-endpoint` on a Bigtable or Spanner table now fails at planning on every SQL
path rather than at TaskManager `open()` on the lookup ones. The sink and the scans already failed
on the client, but during plan-to-runtime translation and with a message naming the Java setter
`emulatorEndpoint`; they now fail in the factory, naming the option key the DDL carried.

Almost every endpoint this newly refuses is one some path rejected a moment later anyway, and one
case is not. A statement whose scan the optimizer eliminates — `SELECT ... FROM t WHERE FALSE` —
never reached `getScanRuntimeProvider`, so a malformed endpoint on it planned and ran; the factory
runs before that rule fires, so it is now refused. Nothing connected either way, and the stricter
answer is the intended one, but the claim that no working configuration is refused would be false.

On Pub/Sub, BigQuery and Cloud Tasks the same move is a rename rather than a reprieve. Every one of
those paths already failed on the client, so what changes is the sentence: `emulator-endpoint must
be host:port` and `emulator-rest-endpoint must be host:port` where a SQL caller previously read
`emulatorEndpoint` and `emulatorRestEndpoint`. The pruned-scan case above transfers to one of the
three rather than to all, which reading the code would not have told either way. Measured 2026-08-22
with `EXPLAIN SELECT * FROM t WHERE FALSE`: on a Pub/Sub source in streaming execution it planned
before and is refused now, exactly as on Bigtable; on a BigQuery source in batch execution the
planner asked for the scan runtime provider anyway, so it was already refused and only the sentence
changes. Cloud Tasks is sink-only, and a sink is not eliminated.

BigQuery adds a newly-refused case of its own. Its source drops `emulator-rest-endpoint` unless the
statement runs a query, deliberately — one `WITH` clause serves both directions, and a table read as
a source must tolerate the endpoint its sink half needs — so nothing parsed the value on a
direct-table read at all, which the table above measured rather than inferred. A well-formed one is
still accepted and still unused; a malformed one is now refused where it was ignored. A value that
cannot be an endpoint at all is a typo wherever it sits, and the same DDL used as a sink was already
refused.

What that leaves untouched is the asymmetry underneath: `BigQuerySourceBuilder.build()` *rejects*
`emulatorRestEndpoint(...)` without `query(...)` or `materializeViews()`, while the table factory
silently ignores the key. That is a separate defect about which options a direction accepts, not
about what a rejection names, and it is not decided here.

[#235]: https://github.com/flink-gcp/flink-connector-gcp/issues/235
[#895]: https://github.com/flink-gcp/flink-connector-gcp/issues/895
[#920]: https://github.com/flink-gcp/flink-connector-gcp/issues/920
[#976]: https://github.com/flink-gcp/flink-connector-gcp/issues/976
[#984]: https://github.com/flink-gcp/flink-connector-gcp/issues/984
[#1009]: https://github.com/flink-gcp/flink-connector-gcp/issues/1009
[#1013]: https://github.com/flink-gcp/flink-connector-gcp/issues/1013
[#1019]: https://github.com/flink-gcp/flink-connector-gcp/issues/1019
