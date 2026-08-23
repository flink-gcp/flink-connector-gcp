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

# ADR-0117: Metric tables are held bidirectionally to connector inventories

- Status: Accepted
- Date: 2026-08-05
- Issues: [#280], [#296]
- Modules: all connectors (documentation tooling)
- Current behavior: [`check-metric-docs.py`](../../scripts/check-metric-docs.py),
  [`metric-docs.toml`](../../scripts/config/metric-docs.toml),
  [metric-documentation curation](../../.agents/skills/curate-metric-docs/SKILL.md)

## Context

[PR #293](https://github.com/flink-gcp/flink-connector-gcp/pull/293) renamed three metrics and edited 16 documentation lines by hand across five pages.
A repository-wide grep for the old names was the only evidence that every table and prose mention had moved together.
A new metric with no row or a rename that missed one row would still have passed CI.

That change also established one `*MetricNames` inventory per connector and kept subgroup leaves in the shared registrar that creates them.
The resulting source shape made names and registration kinds mechanically discoverable without trying to infer the English meaning of a metric.

## Decision

`check-metric-docs.py` holds the DataStream metric tables to those inventories in both directions.
A table opts in when its first column header is exactly `Metric`, and its second column is `Type`.
Fenced examples earn no coverage.

The check enforces these contracts per mapped connector module:

- Every inventory name appears in at least one opted-in row, because the same metric may legitimately appear in several write-method tables on one page.
- Each row's Type cell starts with `counter` or `gauge`, and each registered backticked name uses the kind found at registration.
- Every backticked name in a row resolves to a connector registration, a subgroup template the connector actually wires, or a guarded Flink-standard name.
- Every `.counter(` and `.gauge(` registration goes through a `*MetricNames` constant, and every inventory constant is registered somewhere.
- A name cannot be registered with two kinds, and repository-owned names do not take Flink's `num` prefix.

Flink standard names come from metric-group accessors rather than a connector inventory.
Their table rows use `(Flink standard)` in the Type cell, and the checker rejects that marker when the repository registers the same plain or templated name itself.
The marker is therefore a guarded attribution rather than a general exemption.

Shared subgroup rows use an all-capital placeholder such as `errorClass.CODE.errors` or `destination.TABLE.recordsSend`.
The group and leaf names are parsed from the `base.metrics` registrar sources named under `[[subgroups]]`, and a connector owes the template only when its sources use that registrar.
The configuration never repeats the group or leaf literals.

As in the option checker, `[exempt]` is source-side and `[extra]` is page-side.
Both require reasons, point in opposite directions, and fail when an entry no longer changes the verdict.

The audit checks names and kinds but not Meaning cells or surrounding prose.
It also checks only the mechanical half of the naming convention.
Whether a counter names an event and a gauge names state requires language judgment and remains a review responsibility.

## Evidence

Issue [#296] reproduced every wrinkle found in the manual [PR #293](https://github.com/flink-gcp/flink-connector-gcp/pull/293) sweep before implementation:
Flink-owned names, templated subgroups, one class spanning several tables, and a metric named only in prose.
[PR #302](https://github.com/flink-gcp/flink-connector-gcp/pull/302) turned the prose-only `inFlightAppends` mention into a one-row table rather than adding a per-class mapping that would still leave prose unchecked.

The pull request applied 19 mutants to the metric checker and four to the option-checker hardening.
They covered the exact-header opt-in, registration inventory, kind agreement, standard marker, subgroup use, both failure directions, dead allowlist entries, fenced examples and malformed configuration.
All were killed by synthetic fixtures after the two self-review rounds.

Adversarial probes also established loud failure directions for unsupported source shapes.
A static-imported registration, a Java text block containing `.counter(`, a decorated table header and a constant defined by reference cannot silently reduce the discovered metric set.
They either fail policy or fail as an infrastructure/configuration error.

## Alternatives declined

- **Keep using grep during metric changes**: a one-time search cannot fail a later change that adds a name without a row.
- **Require exactly one row per metric**: the BigQuery page legitimately repeats shared writer metrics in the separate write-method tables that report them.
- **Map each registering class to its own table**: it would not make prose machine-readable and would let a new class remain invisible until the mapping changed.
- **Check the counter/event and gauge/state morphology**: English morphology would create false positives on the names that need review rather than mechanical repair.
- **Repeat subgroup literals in configuration**: the source and configuration could then drift together while each looked internally valid.

## Consequences

Adding, renaming or deleting a metric updates its inventory, registration and table rows together.
Any prose mention still needs a repository-wide sweep because the checker intentionally does not parse prose.

The curation skill owns every judgment-bearing failure, including whether a real exception belongs on the source side or page side and what reason makes it reviewable later.

[#280]: https://github.com/flink-gcp/flink-connector-gcp/issues/280
[#296]: https://github.com/flink-gcp/flink-connector-gcp/issues/296
