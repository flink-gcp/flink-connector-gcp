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
- Date: 2026-08-22 (measured 2026-08-22)
- Issues: [#984], [#920], [#976]
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

[#920]: https://github.com/flink-gcp/flink-connector-gcp/issues/920
[#976]: https://github.com/flink-gcp/flink-connector-gcp/issues/976
[#984]: https://github.com/flink-gcp/flink-connector-gcp/issues/984
