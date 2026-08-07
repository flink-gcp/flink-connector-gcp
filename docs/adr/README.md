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
