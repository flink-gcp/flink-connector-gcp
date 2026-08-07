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
