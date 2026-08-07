# Architecture Decision Records

This directory is the repository's decision archive: one file per settled decision, holding what
was decided, when, on what evidence, which alternatives were declined and why, and — when a later
decision replaces one — what superseded it. It is deliberately **not** rendered on the
documentation site: the site's pages describe current behavior for users, while these records
keep the decision *process*, withdrawn conclusions included.

How the three documentation homes divide (the boundary is recorded in
[ADR-0000](0000-adopt-architecture-decision-records.md)):

| Content | Home |
|---|---|
| Current behavior, and the rationale a user needs to operate it | `docs/content/` pages |
| The decision event: evidence, declined alternatives, supersession history | `docs/adr/` |
| Behavioral rules a Claude session must follow | `CLAUDE.md` (root and per module), as imperative rules pointing here |

## Writing an ADR

- A newly settled decision gets its ADR **in the pull request that lands it**; a migrated decision
  keeps the date it was settled, not the date it was migrated.
- The file is `NNNN-title-slug.md`, numbered from one global sequence — take the next free number
  in the index below.
- The title states the decision as a claim (`A serializer returning null skips the record`), so
  the index alone answers "what was decided".
- Issue references are explicit links (these files render on GitHub, where bare `#N` is dead
  text); each file carries the Apache-2.0 header as an HTML comment (apache-rat checks it).
- Sections: `Context`, `Decision` and `Consequences` are required; `Evidence` (measured facts,
  each with its date and sample size) and `Alternatives declined` (each with the reason) appear
  whenever they exist. A superseded ADR keeps its file, gets
  `Status: Superseded by ADR-NNNN`, and the superseding ADR's Context says what it replaces —
  corrections never accrete inline.
- A decision *cluster* (one design with several dependent sub-decisions) is one ADR with
  subsections, not one file per sub-decision.

Template:

```markdown
<!-- (Apache-2.0 header) -->

# ADR-NNNN: <the decision, stated as a claim>

- Status: Accepted
- Date: YYYY-MM-DD
- Issues: [#N](https://github.com/laughingman7743/flink-connector-gcp/issues/N)
- Modules: <which modules the decision governs>
- Current behavior: <docs page section, when one exists>

## Context
## Decision
## Evidence
## Alternatives declined
## Consequences
```

## Index

| ADR | Decision | Status | Date | Issues | Modules |
|---|---|---|---|---|---|
| [0000](0000-adopt-architecture-decision-records.md) | Decisions are recorded as ADRs in `docs/adr/`, unrendered | Accepted | 2026-08-07 | — | all |
| [0001](0001-a-serializer-returning-null-skips-the-record.md) | A serializer returning `null` skips the record | Accepted | 2026-08-04 | [#230](https://github.com/laughingman7743/flink-connector-gcp/issues/230) | all connectors |
| [0002](0002-tests-forge-options-on-builder-build-never-on-defaults.md) | A test forges an options object on `builder().build()`, never on `defaults()` | Accepted | 2026-08-06 | [#316](https://github.com/laughingman7743/flink-connector-gcp/issues/316) | all connectors (tests) |
| [0003](0003-a-vendor-clients-teardown-may-re-report-a-consumed-failure.md) | A vendor client's teardown may re-report a failure the connector already consumed | Accepted | 2026-08-07 | [#325](https://github.com/laughingman7743/flink-connector-gcp/issues/325) | bigtable, pubsub (contract: all client-wrapping SPIs) |
| [0004](0004-the-pubsub-sink-is-a-mailbox-writer-over-sdk-publishers.md) | The Pub/Sub sink is a mailbox writer over SDK publishers, with writer-owned caps | Accepted | 2026-07-20 | [#18](https://github.com/laughingman7743/flink-connector-gcp/issues/18), [#78](https://github.com/laughingman7743/flink-connector-gcp/issues/78), [#85](https://github.com/laughingman7743/flink-connector-gcp/issues/85) | pubsub |
| [0005](0005-exactly-two-pubsub-sink-failures-are-routed.md) | Exactly two Pub/Sub sink failures are routed to the failure handler | Accepted | 2026-08-02 | [#206](https://github.com/laughingman7743/flink-connector-gcp/issues/206) | pubsub |
| [0006](0006-ordering-is-allowed-beside-a-dropping-failure-policy.md) | Message ordering is allowed beside a dropping failure policy | Accepted | 2026-08-03 | [#215](https://github.com/laughingman7743/flink-connector-gcp/issues/215) | pubsub |
| [0007](0007-the-publisher-teardown-is-two-phase-and-its-bound-is-real.md) | The publisher teardown is two-phase, and its bound is real | Accepted | 2026-08-06 | [#265](https://github.com/laughingman7743/flink-connector-gcp/issues/265), [#310](https://github.com/laughingman7743/flink-connector-gcp/issues/310), [#311](https://github.com/laughingman7743/flink-connector-gcp/issues/311), [#312](https://github.com/laughingman7743/flink-connector-gcp/issues/312) | pubsub, base |
| [0008](0008-a-message-level-verdict-is-confirmed-solo-before-it-is-routed.md) | A `MESSAGE_LEVEL` verdict is confirmed solo before it is routed | Accepted | 2026-08-05 | [#264](https://github.com/laughingman7743/flink-connector-gcp/issues/264), [#303](https://github.com/laughingman7743/flink-connector-gcp/issues/303) | pubsub |
| [0009](0009-the-dead-letter-queue-is-a-standalone-bounded-publisher.md) | `PubSubDeadLetterQueue` is a standalone publisher with bounded flush and close | Accepted | 2026-08-02 | [#211](https://github.com/laughingman7743/flink-connector-gcp/issues/211), [#321](https://github.com/laughingman7743/flink-connector-gcp/issues/321) | pubsub |
| [0010](0010-pubsub-sink-metric-placements.md) | Pub/Sub sink metric placements | Accepted | 2026-08-03 | [#208](https://github.com/laughingman7743/flink-connector-gcp/issues/208) | pubsub |
| [0011](0011-the-pubsub-source-is-flip27-streaming-pull-and-readers-checkpoint-no-splits.md) | The Pub/Sub source is FLIP-27 streaming pull, and readers checkpoint no splits | Accepted | 2026-07-25 | [#79](https://github.com/laughingman7743/flink-connector-gcp/issues/79), [#80](https://github.com/laughingman7743/flink-connector-gcp/issues/80), [#81](https://github.com/laughingman7743/flink-connector-gcp/issues/81) | pubsub |
| [0012](0012-the-subscriber-teardown-absorbs-discriminates-and-releases-in-order.md) | The subscriber teardown absorbs, discriminates, and releases in order | Accepted | 2026-08-07 | [#297](https://github.com/laughingman7743/flink-connector-gcp/issues/297), [#348](https://github.com/laughingman7743/flink-connector-gcp/issues/348)–[#351](https://github.com/laughingman7743/flink-connector-gcp/issues/351) | pubsub |
| [0013](0013-the-pubsub-emulator-never-answers-the-keepalive-ping.md) | The Pub/Sub emulator never answers the keepalive ping, so idle streams cycle | Accepted | 2026-08-03 | [#244](https://github.com/laughingman7743/flink-connector-gcp/issues/244) | pubsub (harness) |
| [0014](0014-the-pubsub-table-layer-maps-onto-the-datastream-builders.md) | The Pub/Sub table layer is a mapping onto the DataStream builders | Accepted | 2026-07-27 | [#47](https://github.com/laughingman7743/flink-connector-gcp/issues/47), [#135](https://github.com/laughingman7743/flink-connector-gcp/issues/135)–[#137](https://github.com/laughingman7743/flink-connector-gcp/issues/137), [#153](https://github.com/laughingman7743/flink-connector-gcp/issues/153) | pubsub |
| [0015](0015-everything-bundled-in-a-sql-uber-jar-is-relocated-and-its-notice-is-generated.md) | Everything bundled in a SQL uber-jar is relocated, and its NOTICE is generated and pinned | Accepted | 2026-07-27 | [#138](https://github.com/laughingman7743/flink-connector-gcp/issues/138) | all flink-sql-connector-gcp-* |
| [0016](0016-the-bigquery-sink-is-a-bigqueryio-style-facade.md) | The BigQuery sink is a `BigQueryIO`-style facade with per-write-method options | Accepted | 2026-07-19 | [#13](https://github.com/laughingman7743/flink-connector-gcp/issues/13), [#14](https://github.com/laughingman7743/flink-connector-gcp/issues/14) | bigquery |
| [0017](0017-bigquery-error-handling-routes-row-verdicts-and-repairs-dead-writers.md) | BigQuery error handling routes row verdicts only, and repairs dead writers in place | Accepted | 2026-07-19 | [#13](https://github.com/laughingman7743/flink-connector-gcp/issues/13), [#205](https://github.com/laughingman7743/flink-connector-gcp/issues/205), [#163](https://github.com/laughingman7743/flink-connector-gcp/issues/163) | bigquery |
| [0018](0018-file-loads-commits-deterministic-load-jobs-in-the-committer.md) | FILE_LOADS commits deterministic load jobs in the committer, on the checkpoint | Accepted | 2026-07-19 | [#14](https://github.com/laughingman7743/flink-connector-gcp/issues/14), [#69](https://github.com/laughingman7743/flink-connector-gcp/issues/69), [#198](https://github.com/laughingman7743/flink-connector-gcp/issues/198) | bigquery |
| [0019](0019-the-staging-format-is-a-real-constraint-not-an-interchangeable-detail.md) | The staging format is a real constraint, not an interchangeable detail | Accepted | 2026-08-04 | [#281](https://github.com/laughingman7743/flink-connector-gcp/issues/281) | bigquery |
| [0020](0020-file-loads-stages-datetime-as-local-timestamp-micros-never-as-text.md) | FILE_LOADS stages `DATETIME` as `local-timestamp-micros`, never as text | Accepted | 2026-08-04 | [#282](https://github.com/laughingman7743/flink-connector-gcp/issues/282) | bigquery |
| [0021](0021-every-file-loads-load-reconciles-against-the-live-table-first.md) | Every FILE_LOADS load reconciles against the live table first | Accepted | 2026-07-27 | [#142](https://github.com/laughingman7743/flink-connector-gcp/issues/142) | bigquery |
| [0022](0022-exactly-once-uses-buffered-streams-reused-across-checkpoints-never-finalized.md) | Exactly-once uses buffered streams reused across checkpoints, never finalized | Accepted | 2026-07-25 | [#30](https://github.com/laughingman7743/flink-connector-gcp/issues/30) | bigquery |
| [0023](0023-json-and-geography-columns-are-schema-derivation-markers-only.md) | `JSON` and `GEOGRAPHY` columns are schema-derivation markers, decided at one point | Accepted | 2026-07-27 | [#49](https://github.com/laughingman7743/flink-connector-gcp/issues/49), [#50](https://github.com/laughingman7743/flink-connector-gcp/issues/50), [#126](https://github.com/laughingman7743/flink-connector-gcp/issues/126) | bigquery |
| [0024](0024-the-avro-serializer-mirrors-the-proto-path-and-derives-eagerly.md) | The Avro serializer accepts `IndexedRecord`, derives eagerly, and rejects unstorable logical types | Accepted | 2026-07-26 | [#66](https://github.com/laughingman7743/flink-connector-gcp/issues/66) | bigquery |
| [0025](0025-the-json-serializer-delegates-conversion-to-the-client-library.md) | The JSON serializer delegates conversion to the client library; its `BYTES` gap is upstream | Accepted | 2026-07-26 | [#66](https://github.com/laughingman7743/flink-connector-gcp/issues/66), [#131](https://github.com/laughingman7743/flink-connector-gcp/issues/131) | bigquery |
| [0026](0026-the-protobuf-mapping-is-normative-and-nullable-is-the-default-mode.md) | The protobuf mapping is normative for every serializer, and `NULLABLE` is the default mode | Accepted | 2026-07-26 | [#124](https://github.com/laughingman7743/flink-connector-gcp/issues/124), [#145](https://github.com/laughingman7743/flink-connector-gcp/issues/145) | bigquery |
| [0027](0027-well-known-types-are-recognised-by-name-and-shape-with-six-constants.md) | Protobuf well-known types are recognised by name *and* shape, with six constants | Accepted | 2026-07-27 | [#147](https://github.com/laughingman7743/flink-connector-gcp/issues/147) | bigquery |
| [0028](0028-default-stream-tuning-recovery-vs-retry-naming-eviction-and-flush-interval.md) | Default-stream tuning: `recovery*` vs bare `retry*`, eviction, flush interval | Accepted | 2026-07-28 | [#54](https://github.com/laughingman7743/flink-connector-gcp/issues/54), [#198](https://github.com/laughingman7743/flink-connector-gcp/issues/198) | bigquery |
| [0029](0029-the-builder-takes-two-emulator-endpoints-one-per-transport.md) | The BigQuery builder takes two emulator endpoints, one per transport | Accepted | 2026-08-06 | [#57](https://github.com/laughingman7743/flink-connector-gcp/issues/57), [#287](https://github.com/laughingman7743/flink-connector-gcp/issues/287) | bigquery |
| [0030](0030-a-missing-bigquery-table-does-not-answer-not-found.md) | A missing BigQuery table does not answer `NOT_FOUND` | Accepted | 2026-08-06 | [#289](https://github.com/laughingman7743/flink-connector-gcp/issues/289), [#318](https://github.com/laughingman7743/flink-connector-gcp/issues/318) | bigquery |
| [0031](0031-the-bigquery-table-layer-maps-onto-the-builder-with-no-format-option.md) | The BigQuery table layer maps onto the builder, with no `format` option | Accepted | 2026-08-06 | [#287](https://github.com/laughingman7743/flink-connector-gcp/issues/287) | bigquery |
| [0032](0032-the-other-two-write-methods-map-unconditionally-and-the-factory-decides.md) | The other two write methods' mappers build unconditionally; the factory decides | Accepted | 2026-08-06 | [#288](https://github.com/laughingman7743/flink-connector-gcp/issues/288) | bigquery |
| [0033](0033-table-create-options-check-shape-in-the-mapper-never-the-type-list.md) | `sink.table-create.*` checks shape in the mapper, never a clonable type list | Accepted | 2026-08-06 | [#289](https://github.com/laughingman7743/flink-connector-gcp/issues/289) | bigquery |
| [0034](0034-bigquery-sink-metrics-are-three-writer-classes-and-one-committer-counter.md) | BigQuery sink metrics are three writer classes and one committer counter | Accepted | 2026-08-03 | [#210](https://github.com/laughingman7743/flink-connector-gcp/issues/210) | bigquery |
| [0035](0035-what-the-bigquery-uber-jar-does-differently-from-the-shared-shading-record.md) | What the BigQuery uber-jar does differently from the shared shading record | Accepted | 2026-08-07 | [#290](https://github.com/laughingman7743/flink-connector-gcp/issues/290) | flink-sql-connector-gcp-bigquery |
| [0036](0036-base-failure-is-the-shared-user-implemented-failure-spi.md) | `base.failure` is the shared, user-implemented failure SPI — the module's one public package | Accepted | 2026-08-01 | [#37](https://github.com/laughingman7743/flink-connector-gcp/issues/37), [#205](https://github.com/laughingman7743/flink-connector-gcp/issues/205) | base |
| [0037](0037-base-metrics-helpers-and-numrecordssend-counts-each-record-once.md) | `base.metrics` helpers, and `numRecordsSend` counts each record once at first hand-off | Accepted | 2026-08-03 | [#208](https://github.com/laughingman7743/flink-connector-gcp/issues/208) | base |
| [0038](0038-metric-names-are-per-connector-inventories-in-one-shape-without-flinks-num-prefix.md) | Metric names are per-connector inventories, in one shape, without Flink's `num` prefix | Accepted | 2026-08-05 | [#280](https://github.com/laughingman7743/flink-connector-gcp/issues/280) | base, all connectors |
| [0039](0039-retry-schedules-are-shared-retry-loops-are-not-and-every-schedule-jitters.md) | Retry schedules are shared, retry loops are not, and every schedule jitters at one ratio | Accepted | 2026-08-01 | [#61](https://github.com/laughingman7743/flink-connector-gcp/issues/61), [#197](https://github.com/laughingman7743/flink-connector-gcp/issues/197), [#235](https://github.com/laughingman7743/flink-connector-gcp/issues/235) | base |
| [0040](0040-base-lifecycle-closes-through-its-own-written-out-loop.md) | `base.lifecycle` closes through its own written-out loop, keeping the throwable's type | Accepted | 2026-08-05 | [#229](https://github.com/laughingman7743/flink-connector-gcp/issues/229), [#276](https://github.com/laughingman7743/flink-connector-gcp/issues/276) | base |
| [0041](0041-the-bigtable-sink-is-implemented-on-four-checked-sdk-facts.md) | The Bigtable sink is implemented — not adopted or vendored — on four checked SDK facts | Accepted | 2026-08-02 | [#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33) | bigtable |
| [0042](0042-invalid-argument-alone-is-routed-and-both-halves-read-the-chain.md) | `INVALID_ARGUMENT` alone is routed, and both halves of the condition read the chain | Accepted | 2026-08-02 | [#33](https://github.com/laughingman7743/flink-connector-gcp/issues/33), [#207](https://github.com/laughingman7743/flink-connector-gcp/issues/207) | bigtable |
| [0043](0043-bigtable-metrics-are-the-series-standard-reached-late.md) | Bigtable metrics are the failure-series standard, reached late | Accepted | 2026-08-03 | [#237](https://github.com/laughingman7743/flink-connector-gcp/issues/237), [#234](https://github.com/laughingman7743/flink-connector-gcp/issues/234) | bigtable |
| [0044](0044-the-e2e-suite-creates-an-ephemeral-bigtable-instance-per-gated-class.md) | The E2E suite creates an ephemeral Bigtable instance per gated class | Accepted | 2026-08-02 | [#218](https://github.com/laughingman7743/flink-connector-gcp/issues/218) | bigtable |
| [0045](0045-a-row-level-verdict-is-confirmed-solo-before-it-is-routed.md) | A `ROW_LEVEL` verdict is confirmed solo before it is routed (Bigtable) | Accepted | 2026-08-07 | [#239](https://github.com/laughingman7743/flink-connector-gcp/issues/239) | bigtable |
| [0046](0046-the-batchers-shutdown-report-is-absorbed-not-thrown.md) | The batcher's shutdown report is absorbed, not thrown | Accepted | 2026-08-06 | [#238](https://github.com/laughingman7743/flink-connector-gcp/issues/238) | bigtable |
| [0047](0047-the-batcher-adapter-holds-functional-values-and-closes-through-closers.md) | The batcher adapter holds functional values, and its teardown closes through `Closers` | Accepted | 2026-08-06 | [#324](https://github.com/laughingman7743/flink-connector-gcp/issues/324) | bigtable |
