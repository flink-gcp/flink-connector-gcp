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

# ADR-0058: `verify.yaml` selects what a pull request builds instead of filtering whether it runs

- Status: Accepted
- Date: 2026-08-02 ([#243]; the root-only class added the same day, [#253])
- Issues: [#243], [#253]
- Modules: all (CI)
- Current behavior: `scripts/ci-maven-args.py` (its docstring is the specification)

## Context

CI took 8 minutes for every change, whatever the change touched ([#243]). The usual fix —
`paths` filters deciding whether workflows run — was impossible here, because **a required
check that never reports blocks a pull request forever** (ADR-0059), so pull requests reach
`verify.yaml` through `ci.yaml` with no paths filter anywhere.

## Decision

**A `changes` job derives the Maven `-pl` subset a pull request builds**, instead of any
workflow-level filter. The pieces:

- **The changed-file list comes from git alone.** The pull_request checkout is the
  base-into-head merge commit, fetched at depth 2, so `HEAD^1` is the current base tip and
  `--diff HEAD^1` is the pull request's net change. A third-party changed-files action was
  tried and removed on PR
  [#247](https://github.com/laughingman7743/flink-connector-gcp/pull/247) as avoidable
  supply-chain surface.
- **The decision is `scripts/ci-maven-args.py`, and its module mapping is derived from the
  poms, never configured** — `<modules>` for the set and reactor order, `io.github.flink-gcp`
  dependencies for the edges: dependents of a changed module build transitively, its
  dependencies ride along for reactor resolution. A new module is covered the moment the root
  pom names it. `just ci-maven-args --diff origin/main` reproduces the decision by hand.
- **The ignore list lives twice on purpose**: as the script's first classification rule, and
  as a real `paths-ignore` on the **push** trigger only, where no required check can be
  blocked and a tofu-only merge stays free. The list: `opentofu/**`, the tofu workflows,
  `**/README.md` / `**/CLAUDE.md` — the last two only because apache-rat's exclude list
  already carries exactly those patterns, so no licence-header check is lost — plus
  everything under `.github/` that is **not** a workflow or a composite action (templates,
  CODEOWNERS, `dependabot.yml`; a rule, not a list, so a new template needs no edit). The two
  copies are no longer identical: the push list keeps naming the inert `.github/` files one
  by one, because GitHub's `!` negation in `paths-ignore` is order-sensitive and a mistake
  there silently stops CI on a real workflow change — while the cost of not mirroring the
  rule is one full build per merge of a template.
- **A root-only change builds `-pl .` alone** ([#253]): `docs/**`, `scripts/**` and the root
  uv project (`pyproject.toml`, `uv.lock`) are the paths whose only Maven-relevant consumer
  is the root module's rat run, which scans the whole working tree and is their only
  pre-merge licence check — a `scripts/tests/`-only pull request had been paying 7m41s of
  full reactor for it. **Two files are deliberately outside that class**,
  `scripts/licence-sources.toml` and `scripts/check-notice.py`: the NOTICE check is a step
  *inside* the `build` job gated on `check_notice`, which is false when no shaded module is
  built, so routing them root-only would skip the licence check on exactly the change that
  edits the licence pins. That the other checkers' scripts *are* in the class is the same
  fact from the other side — the checker jobs are unconditional, so nothing about them
  depends on what the deriver picks. The `justfile` stays full-reactor too: it carries the
  Maven invocations themselves.
- Pushes to `main` and `workflow_dispatch` always build the full reactor.

**Where a check runs follows from what its inputs are:**

- **The checker scripts (`api_tiers`, `option_docs`, `metric_docs`, `gated_tags`) run as
  their own unconditional `verify.yaml` jobs, not in `lint.yaml` and not inside `just
  lint`.** Their inputs are the main and test Java sources (plus `docs/content/` for the two
  docs checkers), which every pull-request run covers — where `lint.yaml`'s push-side paths
  filter would have had to grow to every Java source. `check-flink-api-tiers` also downloads
  the Flink sources jars, while `just lint` stays offline.
- **`check-notice` runs over the shaded modules *in the built set*** — derived from
  `NOTICE.template` presence by `ci-maven-args.py`, which is also what selects them — so a
  new shaded module is checked from the commit that adds it, and one nothing touched is not
  rebuilt to re-check it.
- **`lint.yaml` is where linters Maven does not run live** (spotless and checkstyle cover the
  Java sources inside `verify`). A workflow of its own rather than jobs in `verify.yaml` so
  results arrive in seconds rather than behind the integration tests — that is the whole
  reason, the mise-versus-`setup-java` one having turned out to be a disarmable default
  rather than a conflict (ADR-0057). On pull requests it runs unfiltered under `ci.yaml`'s
  gate; its `paths` filter exists on the push trigger only, where it is cost-saving, and it
  still must list **every input to a lint, not just the linted files** — `mise.toml` is in it
  because that is where the shellcheck version is pinned, and skipping the lint on a version
  bump would skip it in the one change that most needs it. `docs.yaml`'s push filter carries
  `mise.toml` for the same reason since [#111], and the main sources and poms since [#88] —
  and on pull requests `docs.yaml` runs unfiltered too, accepted over splitting the site's
  definition across two workflows back when its filter decided PR runs.

## Consequences

- A template-or-CODEOWNERS-only merge costs one full push build — accepted over mirroring the
  ignore *rule* into order-sensitive `paths-ignore` negations.
- The deriver and the gate carry pytest suites (`just test-scripts`), whose synthetic-tree
  rule and real-repo exception are recorded with the Build rules in the root `CLAUDE.md`.

[#88]: https://github.com/laughingman7743/flink-connector-gcp/issues/88
[#111]: https://github.com/laughingman7743/flink-connector-gcp/issues/111
[#243]: https://github.com/laughingman7743/flink-connector-gcp/issues/243
[#253]: https://github.com/laughingman7743/flink-connector-gcp/issues/253
