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

# ADR-0059: `ci.yaml` orchestrates pull-request CI behind one required check, "CI passed"

- Status: Accepted
- Date: 2026-08-02 ([#250], live since the plan upgrade; shape decided with the user on PR
  [#305](https://github.com/laughingman7743/flink-connector-gcp/pull/305))
- Issues: [#250]
- Modules: all (CI)
- Current behavior: `scripts/ci-gate.py` (the hand-runnable truth table), `ci.yaml`

## Context

A private free-plan repository cannot set required checks at all, so [#250] waited on the plan
upgrade. When it landed, the shape chosen is suzuki-shunsuke's required-status-check pattern —
decided with the user on PR
[#305](https://github.com/laughingman7743/flink-connector-gcp/pull/305), after that PR
briefly enumerated the checkers as their own required contexts.

## Decision

- `verify.yaml`, `lint.yaml`, `docs.yaml` and `tofu-plan.yaml` run as reusable workflows
  called from `ci.yaml`, whose gate job `needs` them all — the only way a verdict can span
  workflows — and derives its verdict from the whole `needs` context via `scripts/ci-gate.py`.
- The children with a legitimately skippable job (`verify`'s Maven build, `tofu-plan`'s plan)
  carry an internal verdict job running the same script, telling it which skip is legitimate
  via `SKIPPED_OK` — a workflow result alone cannot expose an illegitimate skip.
- The wiring tests in `scripts/tests/test_ci_gate.py` hold every gate's `needs` to its file's
  full job list.
- Branch protection requires exactly one context, the gate's job name **`CI passed`**.

## Alternatives declined

- **A settings-side list of required contexts** (what PR [#305](https://github.com/laughingman7743/flink-connector-gcp/pull/305) briefly had): it has to be
  edited every time a job is added or retired, falls silently out of step with a renamed job,
  and cannot follow path-conditional jobs. A stale `needs` entry or `uses` path, by contrast,
  is a workflow-parse error no run survives unnoticed.

## Consequences

- A job or workflow enrolls in `ci.yaml` or its child alone and touches no repository
  setting. The one exception is the gate's own name: a required context is matched **by job
  name**, so renaming `CI passed` must update branch protection in the same change.
- `docs.yaml` and `lint.yaml` are required through the gate like everything else; their
  `paths` filters survive on the push trigger only, as cost control (ADR-0058).
- **A fresh pull request showing "no checks reported" is diagnosed at the merge state
  first, not at the workflows.** A pull request whose `mergeable` is `CONFLICTING` triggers
  **zero** `pull_request` workflow runs — the event runs on the test merge commit, which a
  conflicting PR does not have, so no run is queued, no check suite appears, and
  close/reopen changes nothing. With the gate required, that presents as a PR blocked forever
  with every workflow showing active. Run `gh pr view <n> --json
  mergeable,mergeStateStatus` before debugging CI; a rebase starts the runs the moment the PR
  is mergeable. Measured on PR
  [#301](https://github.com/laughingman7743/flink-connector-gcp/pull/301): ~30 minutes of
  status-page/billing/trigger diagnosis before the merge state was checked.

[#250]: https://github.com/laughingman7743/flink-connector-gcp/issues/250
