# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project overview

GCP connectors for Apache Flink: BigQuery, Cloud Pub/Sub, Cloud Tasks and Bigtable (Spanner
planned). Independent OSS project — not affiliated with the Apache Software Foundation or Google.
Maven multi-module build based on `org.apache.flink:flink-connector-parent`, with Google Cloud
dependencies managed through `com.google.cloud:libraries-bom`.

## Build

Commands live in the `justfile` and **CI calls the same recipes**, so what runs locally is what
runs in the workflows (#111). `just --list` is the index; run `mise x -- just <recipe>` in a shell
without mise activated. Add a command here rather than to a workflow `run:` block.

- `just verify` — full build (`./mvnw -ntp verify`): spotless/checkstyle (validate), unit tests,
  integration tests, apache-rat license-header check. Requires JDK 17; `mise.toml` pins java/maven
- `just format` — run before committing; CI fails on unformatted code
- `just verify-flink 2.3.0` / `just verify-module flink-connector-gcp-bigquery` — one Flink
  version, one module. A 1.x version also selects the `flink1` compat source root (see the
  version policy), which is why the recipe exists rather than passing `-Dflink.version` by hand. `just verify <maven args>` is the passthrough the weekly matrix uses, and
  passing nothing means the version pinned in the pom. **`verify-module` carries `-am`**, and it is
  load-bearing rather than tidiness: `-pl` alone resolves the `io.github.flink-gcp` siblings from
  `~/.m2`, so the recipe reports on whichever jar is installed there rather than on the working
  tree — which fails in *both* directions, a stale jar failing a green tree (`NoClassDefFoundError`
  on a test-utils class added by #323, met on #324) and a newer one passing a broken reactor
  change. Nothing in CI is affected either way, since CI builds the reactor. That is a different
  problem from the one `binary-compat` and `e2e` solve by installing first: theirs is that a
  goal-only or repeated-`-pl` invocation cannot span one reactor at all
- `just binary-compat 2.3.0` — the floor-build/install/fingerprint/ceiling-rerun/diff
  sequence, whose order is load-bearing. Reproducing a red weekly `binary_compat` is what it is
  for. The install step (root pom + each connector a SQL uber-jar bundles + the base module every
  connector compiles against + the test-utils module every module's
  tests depend on) exists because the goal-only rerun
  cannot resolve inter-module dependencies from the reactor — same mechanism as the licence-goal
  rule below, bitten via the SQL uber-jar in #181 — and it primes `~/.m2` with
  `io.github.flink-gcp` SNAPSHOTs when run by hand (the recipe comment has the cleanup line)
- `just e2e` — the ITCases gated on the `BQ_IT_*` / `PUBSUB_IT_PROJECT` / `BIGTABLE_IT_PROJECT`
  variables, **and the only thing that runs them** (#245): each gated class also carries
  `@Tag("gated")`, which the root pom's `test.excluded.groups` excludes from every surefire
  execution, and this recipe is what clears it with `-Dtest.excluded.groups=`. The environment
  gate alone was all-or-nothing for a *shell* — `just verify` runs the same `integration-tests`
  execution, so a shell holding `BIGTABLE_IT_PROJECT` created two one-node instances on every
  full build — and the tag is what makes the choice per command instead, for `./mvnw verify` as
  much as for `just verify`. The `@EnabledIfEnvironmentVariable` stays exactly where it is (the
  discovery greps it), so the two markers must be kept together, which
  `just check-gated-tags` enforces. Around that: a pre-flight that makes a missing variable an
  error — as strict as before, because this recipe *is* now the opt-in — and a post-run assertion
  (`scripts/e2e-gated-its.sh`, which derives the class list from the gating annotation) that the
  gated classes actually executed. Its `-pl`-scoped builds install the base and test-utils modules
  first, for the same reactor-resolution reason `binary-compat` installs (#27, #61). The weekly E2E workflow (`e2e.yaml`) runs this same recipe
  via WIF; locally the variables come from the uncommitted `.env`, which a fresh worktree does
  not have — run `just worktree-env` once there to symlink the main checkout's copy (#156; the same
  link also carries `just tofu`'s credentials)
- `just sweep-e2e [--dry-run]` — deletes Bigtable instances an E2E run abandoned (#246).
  `AbstractBigtableRealGcpITCase` already sweeps at the start of a gated class, but only the
  weekly E2E workflow schedules one, so a run whose teardown never executed leaves a one-node
  instance standing until the next Saturday — about **$109**, and two gated classes run per
  execution. `sweep-e2e.yaml` runs this daily, which is what bounds that number; detection of
  what a sweep cannot foresee (a billing budget) is the other half of #246 and deliberately
  separate, since it needs a billing-account-level grant this project has never made. The
  instance prefix and the two-hour threshold are **read out of the Java source**, in the
  `e2e-gated-its.sh` tradition: a second copy of `flink-it-` would go stale silently, and a
  sweep that matches nothing looks exactly like a sweep with nothing to do — so both greps are
  hard errors. The listing is captured into a variable rather than piped into the loop for the
  same reason: `set -e` does not see a failing process substitution, so the pipe form reports
  "0 stale instances swept" and exits 0 on an unauthenticated gcloud
- `just check-notice <module>` / `just update-notice <module>` — a shaded module's
  `META-INF/NOTICE` is generated (prose from the module's `NOTICE.template`, artifact lists from
  what Maven resolves) and its `META-INF/licenses/` texts come from sha256-pinned sources in
  `scripts/licence-sources.toml`. `update-notice` regenerates after a dependency change;
  `check-notice` verifies offline in CI. Both take the module as an argument, which is what lets
  the two SQL uber-jars share them and what a third would reuse unchanged; verify.yaml runs
  `check-notice` over the shaded modules *in the built set* — derived from `NOTICE.template`
  presence by `scripts/ci-maven-args.py`, which is also what selects them — so a new shaded module
  is checked from the commit that adds it, and one nothing touched is not rebuilt to re-check it. **Invoke the licence goal
  through a phase, never as a bare `license:add-third-party`**: a CLI goal invocation selects
  reactor modules without building them, so the module cannot resolve the connector it bundles —
  `-am` does not change that, and it only appears to work against a local repository some earlier
  `install` primed
- `just check-flink-api-tiers` — classifies every `org.apache.flink` type the main sources import
  by its class-level stability annotation, read from the `-sources.jar`s at the pom-pinned
  `flink.version` (never class files: their constant pool lists method-level annotations too,
  the #103 miscount). `@Internal`, `@Experimental` and unannotated types each need a reasoned
  allowlist entry in `scripts/flink-api-tiers.toml`; a new one — or a stale entry — fails.
  Runs as its own `verify.yaml` job, not in `lint.yaml` and not inside `just lint`: its inputs (the
  main sources, `pom.xml`) are what every pull-request run covers, where lint.yaml's push-side
  paths filter would have had to grow to every Java source — and it downloads the sources jars (into
  `target/flink-api-tiers/`) while `just lint` stays offline
- `just check-option-docs` — holds `docs/content/docs/reference/` to the options the connectors
  actually take (#89), both directions: every public builder setter and every Table API
  `ConfigOption` key must be named in a table whose **first column header is exactly `Option`** —
  that header *is* the opt-in, which is what keeps the check off the metadata, type-mapping and
  policy tables the same pages carry — and every option those tables name must exist. Modules are
  mapped to pages in `scripts/option-docs.toml`, not classes, so a **new** `*Options` class is
  required to appear from the moment it exists. Two allowlists, pointing opposite ways — `[exempt]`
  is a setter with no row, `[extra]` a row with no setter — and **an entry that never fires fails**,
  as a stale one does in `check-flink-api-tiers.toml`: the four `[exempt]` entries this shipped
  with were dead on arrival, since the pages name each bulk overload in the same row as its
  singular. `[exempt]` is empty today; `[extra]` holds Flink's own `FactoryUtil` keys (`format`,
  `scan.parallelism`, `sink.parallelism`), which the SQL page documents because a reader writing
  DDL needs every key the connector accepts. The pages are
  **hand-written, not generated** — their tables group knobs (one Pub/Sub row covers eight
  `retry*` setters) and carry defaults the sources do not hold, since an unset knob's default
  belongs to the client library; this check buys back the one property generation would have given
  free. Its own `verify.yaml` job for the same reason as `check-flink-api-tiers`, plus one of its own:
  its inputs are the main sources *and* `docs/content/` — `lint.yaml`'s push-side paths carry
  docs/content only for markdownlint, and would have had to grow to every Java source.
  **How to respond to each failure — where a row goes, what its Default column may say,
  and which of `[exempt]` (a setter with no row) and `[extra]` (a row with no setter) a case
  belongs in — is `.claude/skills/curate-option-docs/`**, one of the checker skills. Note
  what the check does *not* do: it compares the set of options, not their values, so a changed
  default has to be edited in the same commit
- `just check-metric-docs` — the same shape of check for the **metrics tables on the DataStream
  pages** (#296), which #293 measured the need for: 16 documentation lines swept by hand across
  five pages, verified only by grep. Both directions, per module in `scripts/metric-docs.toml`:
  every name in a connector's `*MetricNames` inventory must appear in a table whose **first column
  header is exactly `Metric`** — the `check-option-docs` opt-in trick — with the Type column
  leading `counter` or `gauge` as the source registers it, and every name those tables carry must
  be registered, a `base.metrics` subgroup template the module wires (`errorClass.CODE.errors`,
  all-caps placeholder middle segment; group and leaf names are read from the sources named under
  `[[subgroups]]`, never from config), or **marked `(Flink standard)` in the Type cell** — the
  marker is load-bearing, and guarded: a marked row whose name the module does register fails.
  Three inventory-integrity rules ride along, plus the mechanical half of the #280 naming rule
  (no `num`-prefixed name; the event/state morphology half stays with review): every registration
  goes through a `*MetricNames` constant, every constant is registered, no name has two kinds.
  Both allowlists (`[exempt]` keyed `Class.name`, `[extra]` keyed as the table writes it) are
  empty today, and an entry that never fires fails. Its own `verify.yaml` job for exactly
  `check-option-docs`'s reason. **How to respond to each failure is
  `.claude/skills/curate-metric-docs/`**. What it does *not* check: Meaning cells and the prose
  around the tables, so a rename still sweeps those by hand — in the same commit
- `just check-gated-tags` — the two markers a gated real-GCP ITCase carries have to stay together
  (#245): the `@EnabledIfEnvironmentVariable` the E2E suite is *discovered* by, and the
  `@Tag("gated")` that keeps the class out of every ordinary build. `scripts/e2e-gated-its.sh
  --check-tags` fails in both directions — a gate with no tag runs the suite during any
  `just verify` in a shell holding the variable (billed Bigtable instances), a tag with no gate
  runs nowhere at all, since `just e2e` selects by the gate. Deliberately **gate-agnostic**,
  matching the annotation rather than the three variables the E2E workflow sets, so
  `BigQueryDefaultStreamSchemaEvolutionITCase` — outside that suite on purpose, ~2 h against the
  real service — is covered too. Its own `verify.yaml` job for `check-flink-api-tiers`'s reason (its
  inputs are the Java *test* sources, which every pull-request run covers), and it needs no JDK, no
  Python and no network. **The one checker with no `curate-*` skill**, and the exemption is
  argued rather than an oversight: those skills exist for allowlist judgment — which entry, with
  what reason — and this check has no allowlist and exactly two mechanical fixes, both named in
  the failure message
- `just ci-maven-args` — CI's module-selection decision (#243): which Maven modules does a
  change build? `verify.yaml`'s `changes` job calls it with `--diff HEAD^1` (the pull_request
  checkout is the base-into-head merge commit, fetched at depth 2, so that diff is the pull
  request's net change) or `--full`; `just ci-maven-args --diff origin/main` reproduces by hand
  what a pull request with the current branch's committed diff would build. The mapping is
  derived from the poms, never configured — the script's docstring is the specification, and
  the CI-architecture bullet under Version policy carries the design
- `just test-scripts` — pytest over `scripts/`: the CI deriver and the CI gate (#243), and the
  three checkers (#249). Through
  the uv project at the repository root (decided with the user on PR #247): `pyproject.toml`
  holds a loose pytest constraint plus the one layout customisation (`testpaths`, since the
  code under test is executables in `scripts/`, not a package — `package = false`), `uv.lock`
  pins what actually installs (committed, rat-excluded as machine-written), uv itself is pinned
  in `mise.toml` like the linters. Runs as lint.yaml's `script_tests` job, whose paths list the
  root `pyproject.toml`/`uv.lock` for exactly the mise.toml reason. A new `scripts/*.py`
  checker owes its tests here, alongside the curate-* skill the checker rule already demands —
  a skill being owed for *judgment*, which is why `check-gated-tags` (#245) has tests here and
  no skill.
  **A checker's tests are synthetic — a tree built in `tmp_path` with `ROOT`/`CONFIG`/`SOURCES`
  monkeypatched onto it — never assertions against the real repository**, which is what keeps
  lint.yaml's paths filter from having to grow to every input those checkers read (every Java
  source, for two of the three). `test_ci_maven_args.py`'s real-repo CLI layer is the exception
  that names its own inputs in that filter, and it is why the poms and `NOTICE.template`s are
  listed there. The direction the tests are aimed at is a checker quietly finding *less* than
  it should: that reads exactly like a clean tree, so each rule is pinned by a case that fails
  when the rule is removed (measured on #249 with ten mutants, two of which found tests that
  did not in fact discriminate)
- `just lint` — shellcheck over `scripts/*.sh`, ruff over `scripts/` (check *and* format), actionlint
  over `.github/workflows/`, markdownlint (markdownlint-cli2, pinned via mise's npm backend) over
  the **rendered** markdown — `docs/content/` and the READMEs, never the `CLAUDE.md`s — at strict
  defaults except MD013 (line length: issue-link syntax legitimately outruns any source-line cap)
  and MD060 (table style), both declined with reasons in `.markdownlint-cli2.jsonc`; MD051's
  in-page anchor check is the half Hugo's build does not cover (`relref` validates cross-page
  links only). Those two do **not** add up to the whole, though: a *cross-page* link carrying a
  `#fragment` is checked by neither — `relref` resolves the page and stops, MD051 reads same-page
  anchors only — so it can point at nothing while both stay green. Splitting a page into a section
  turns every same-page anchor into one of those, which is why #90 resolved them by hand against
  the `id=`s in the built `docs/public` (all 19 pages, ~12 lines of throwaway Python). A `scripts/`
  checker for it was weighed and deferred there: build it when the pages actually rot, not before.
  Also `tofu fmt -check` over `opentofu/` (`tofu validate` is deliberately
  absent: it needs a provider-downloading init, and every PR touching `opentofu/` gets a full plan
  from the tofu-plan workflow, which subsumes it). Deliberately
  does **not** run `just --fmt --check`: that is an unstable feature, excluded from just's
  compatibility guarantee, so with `just` installed unpinned it could fail an unchanged pull
  request. actionlint is handed `-shellcheck "$(mise which shellcheck)"` rather than letting it
  find one on `PATH` — the runner image ships its own, and it is not the pinned one
- `just docs` / `just docs-serve` / `just docs-chroma` — build the site as CI does (a deprecation,
  a broken `relref` or a missing shortcode fails the build), preview it, regenerate the chroma
  palettes. `mise.toml` pins hugo-extended and Go; hugo-book is a Hugo module pinned in
  `docs/go.mod`. These build the hand-written pages only; the generated half is
  `just docs-javadoc` (below), which the docs workflow runs first
- `just docs-javadoc` — the aggregated JavaDoc that ships as the site's API reference (#88), into
  `docs/static/api/java`, which Hugo copies verbatim (gitignored, rat-excluded). The one correct
  bare goal in this repository; the exemption from the licence-goal rule above is argued, and
  measured, in the justfile
- `just pin-actions` — pin GitHub Actions to commit SHAs; when to run it is a Workflow rule
  (a workflow added, or an action version changed)
- `just tofu <args>` — OpenTofu in `opentofu/flink-gcp`, the root module holding the project's
  persistent GCP resources (#5). Local escape hatch only: plan/apply normally run in CI (see
  "Infrastructure (OpenTofu)" below). Credentials come from
  `GOOGLE_APPLICATION_CREDENTIALS` in the uncommitted `.env` — the google provider does not read
  `CLOUDSDK_CONFIG` (only the gcloud CLI does; see `opentofu/README.md`)
- Recipe bodies stay one command per line — no embedded `#!/usr/bin/env bash` blocks. A single
  compound command is fine (`binary-compat`'s final `diff … || { …; exit 1; }`); a multi-line
  script block is not. The boundary is shellcheck coverage: it reads `scripts/`, actionlint reads
  inline `run:` blocks, and nothing reads inside a recipe
- **Inside a recipe, always name the tool: `mise x <tool> -- …`, never bare `mise x -- …`.** The
  bare form activates every tool in `mise.toml` and installs what is missing, silently undoing the
  `install_args` meant to limit a CI job. Caught in CI on #113: `mise x -- shellcheck` in the lint
  job pulled a JDK, Maven, Hugo, Go and a second copy of `just`, shadowing the one
  `install-action` had already put on `PATH`. The bare form stays right for the *entrypoint*
  (`mise x -- just <recipe>`), which does want everything — `just verify` needs java and maven and
  names neither
- A top-level justfile variable assigned from a shell command runs on **every** `just` invocation,
  whichever recipe was asked for; a default *parameter* value runs only when its own recipe does.
  That is why `check-flink-release`'s ceiling is a parameter default rather than a variable

## Documentation (`docs/` vs module READMEs)

- `docs/content/docs/connectors/datastream/<connector>.md` is **the design record**: API notes,
  design decisions, delivery guarantees, error handling and the testing strategy. Behavior or
  public API changed → update the docs page, not the README
- `docs/content/docs/reference/<connector>.md` is **the option tables**, and the only place they
  live (#89): the design record states *why* a default is what it is and links here for *what* it
  is, so a knob's name and value appear exactly once. Adding or renaming an option means editing
  this page in the same change — `just check-option-docs` fails otherwise, in both directions.
  `docs/content/docs/{quickstart,examples}/<connector>.md` (#90) are the other two per-connector
  pages: one runnable job each, and the worked cases the design record describes but does not show
- The module `README.md` is an **overview only**: title, one-paragraph description, the
  feature-status table (`Implemented (#N)` / `Planned (#N)`), a minimal code sample, a link to the
  docs page, and the **provenance/attribution section** — provenance pairs with `NOTICE` and is a
  licensing obligation, so it stays in the repository
- Implementation status lives in the README table only; the docs page links to it instead of
  repeating it. Keep the two from drifting by adding status nowhere else
- **Issue references in module READMEs and docs pages are explicit links**, never bare `#N`.
  GitHub autolinks `#N` only in issue/PR *comments*, not in repository markdown files, and Hugo
  never does — so a bare `#N` is dead text in both places a reader actually sees. READMEs use the
  full URL; docs pages use `[#N]({{< param BookRepo >}}/issues/N)`. The `#N` link text must match
  the `/issues/N` in the URL it wraps — a copy-paste where they disagree sends the reader to the
  wrong record, and nothing renders differently to reveal it. Cross-repository references
  keep their `owner/repo#N` text and point at *that* repository — `goccy/bigquery-emulator#342`
  is the one in the tree, and a blind `#N` rewrite would have pointed it here. `CLAUDE.md` is
  deliberately exempt: it is read by Claude, not rendered for users
- **A status claim in a rendered file carries the issue link that *is* the status.** Wording
  like "under investigation", "planned" or "not yet supported" rots silently when the tracker
  moves on, so it may appear only beside the issue link whose open/closed state lets a reader
  check it — and prefer recording the decision plus the protocol over the status word ("closed
  wait-and-see; a reproduction gets a new issue referencing #174" outlives "under
  investigation"). Closing or re-scoping an issue includes rewording its rendered mentions in
  the same change — #186, sweeping the mentions after #174 closed, is the precedent
- **Comments and javadoc state what the code cannot show** — a measured fact, a decision with
  its issue number, or the vendor behavior being worked around — never narration of the change
  that introduced them ("now", "new", "previously", "fixed in this PR"), which is stale the
  moment it merges and meaningless to a reader who wasn't in the session that wrote it, and
  never implementation status, whose single home is the README table. Wall-clock or measured
  numbers carry their date and sample size ("measured 2026-07-31, one run") so a later reader
  can weigh them, and a superseding measurement edits the original spot rather than appending a
  correction beside it
- Pages are plain markdown with front matter (`title`, `type: docs`, `weight` — spaced by 10 so a
  new connector slots in without renumbering) and the plain Apache-2.0 header as an HTML comment.
  **No Flink shortcodes and no vendored Flink layout code** — `artifact`/`tabs`/`hint` do not
  exist here, and staying clear of them is why `NOTICE` needs no entry. Hugo's own built-ins
  (`relref`, `param`) are fine; prefer `{{< param BookRepo >}}` over hardcoding the repository URL
- Syntax highlighting is class-based (`markup.highlight.noClasses = false`) with the palettes
  selected by `prefers-color-scheme` in `docs/assets/_custom.scss`, which hugo-book bundles into
  its own stylesheet. Regenerate the palettes with `just docs-chroma`, which is where the two
  `hugo gen chromastyles` style names live (verbatim output; apache-rat excludes them)
- The site is built as a CI check only; GitHub Pages publishing waits until the repository is
  public (#6). Each module README links to its docs page by in-repo relative path — those links
  become site URLs when Pages goes live, which is a checklist item on #6
- **The API reference is the site's generated half** (#88): `just docs-javadoc` aggregates JavaDoc
  across every module into `docs/static/api/java`, which Hugo copies verbatim, so it is part of the
  Pages artifact the moment #93 adds a deploy job that runs `just docs-javadoc` before uploading. It is never
  committed (gitignored, rat-excluded), and pages link to it with `{{< param ApiDocsURL >}}` —
  a param rather than a `relref` because the output is not Hugo *content*, and not `Book*`-prefixed
  because that namespace is hugo-book's. Three decisions behind it, the first two measured rather
  than assumed:
  - **Nothing is filtered by API tier.** `@Internal` is `@Documented`, so the tier is a badge on
    every class page, and using an `@Internal` type is the caller's risk — a consumer can audit
    tiers mechanically exactly as `just check-flink-api-tiers` does against Flink. Filtering was
    priced and declined: package-level exclusion would still leave 32 `@Internal` files documented,
    because 12 packages mix tiers, `sourceFileExcludes` drops files from the *source path* and so breaks
    resolution of public signatures that name them, and a doclet buys zero-maintenance filtering
    at a cost this project has no reason to pay. Apache Flink publishes unfiltered too
  - **Doclint stays off, `failOnWarnings` is on instead.** The parent supplies `-Xdoclint:none`
    through `<additionalJOptions>` — and it turns out not to be the check worth having. JavaDoc resolves `{@link}` itself rather than through doclint, so an unresolvable
    reference is reported regardless (two existed when this landed, both in
    `JsonDocumentSerializerOptions`, left behind by #125's fully-qualified-link rule); a
    reference the reader cannot follow is what a published reference must be free of, a missing
    `@param` is not. Nothing links out through a fetched index: no `<links>`, which is the only
    setting here that would probe a remote site, and `detectJavaApiLink` off. That last one costs
    no links — the JDK cross-links come from the doclet's own automatic platform links, so the
    count is identical either way; what the default adds is a second mechanism for them, against
    an element-list the plugin unpacks from its own jar, bundled for Java 10–15 only
  - **One unversioned path, tracking `main`.** Per-release references wait for artifact
    publishing, which is #39's scope; javadoc.io serves released versions from Central for free
    once that happens
- A module `CLAUDE.md` (`flink-connector-gcp-<product>/CLAUDE.md`) is the third document in this
  split and the only **Claude-facing** one — never rendered, never linked from the site, so
  nothing user-facing belongs in it. It carries that module's design decisions and nothing else; behavior and public
  API still go to the docs page, status still goes to the README table. Being unrendered, it keeps
  bare `#N` references under the same exemption the root `CLAUDE.md` already has
- **`docs/adr/` is the decision archive** (ADR-0000, which records the whole scheme): one file per
  settled decision — context, dated evidence, alternatives declined with reasons, supersession.
  Deliberately unrendered: the site's pages describe current behavior for users, the archive keeps
  the decision *process*, withdrawn conclusions included. The division of homes: a docs page holds
  current behavior and the rationale a user needs, an ADR holds the decision event, a `CLAUDE.md`
  holds the imperative rules a session must follow with a pointer to the record — so where a docs
  page already carries a decision's full operative record, the `CLAUDE.md` entry points there and
  no ADR is written. **When the archive changes**: design discussion stays on the issue (the
  `Design (settled YYYY-MM-DD)` comment, which may just say "Settled — see ADR-NNNN"); the PR
  that implements a settled decision carries the ADR in its diff, with the settled date; a later
  PR that *refines* the decision edits the same ADR in place (`revised by #N` on its Date line);
  a *reversal* adds a new ADR and flips the old one's status to `Superseded by ADR-NNNN` — a
  decision is revisable by design, and "do not silently revisit" means engage the record, not
  keep it forever. Granularity is the decision cluster, not the PR, and most PRs touch no ADR —
  the trigger is the residue: **an ADR is owed exactly where a decision record would otherwise
  be owed to a `CLAUDE.md`, README or docs page** (something weighed and declined, measured, or
  chosen in a way a later reader must not re-argue without the reasoning), which is the
  self-review question to ask of every diff. Statuses are
  `Accepted` and `Superseded by ADR-NNNN` only (an unmerged PR *is* the proposed state; a
  rejected proposal is a declined alternative inside the winning ADR). ADR files carry the
  Apache-2.0 header and full issue URLs (they render
  on GitHub, where bare `#N` is dead text — the READMEs' rule, not the `CLAUDE.md` exemption), and
  `docs/adr/README.md` holds the hand-maintained index that allocates the next number.
  **A repository-relevant decision or measured fact is recorded in the repository — an ADR, a
  docs page, or a rule here — never only in Claude's private session memory**: memory keeps user
  preferences, session workflow and pointers, and anything a future maintainer or session would
  need belongs where the repository's readers can reach it

## Workflow rules

- **One git worktree per PR** under `/tmp/worktrees/flink-connector-gcp/`; never switch branches
  in the main checkout. Remove the worktree and local branch after merge. If the branch needs the
  real-GCP ITs or `just tofu`, run `just worktree-env` once in the fresh worktree — it symlinks
  the main checkout's uncommitted `.env` (#156)
- All changes go through **draft PRs**; nothing is pushed directly to `main` after the initial
  skeleton
- **A description follows `.github/PULL_REQUEST_TEMPLATE.md`: `## WHAT` and `## WHY`, both
  filled** (#257). WHY is the half that does not survive in git — a diff shows what changed and
  never why it was worth changing — so a description that only restates the diff is incomplete
  even when it is long. The template is a floor, not a cap: everything this project already
  expects of a description goes **under** those headings — the verification list, the mutant
  table a change with new tests carries, the decisions weighed and declined. Squash before merge
  (the review fix-ups are session bookkeeping), so the merged commit message carries the same
  WHAT/WHY and the per-round detail stays in the PR comments
- **After creating a draft PR, always self-review it** — applying simplification and efficiency
  findings, not only correctness ones — and push the fixes before asking for review. Record the
  findings *and the deferrals, with their reasons* as a PR comment; recording is not routing, which
  the bullet below governs. Which command to use:
  - `/review <pr>` reviews a pull request and **Claude can start it itself**, so this is the one
    to reach for once the draft PR exists
  - `/code-review` reviews the working diff and is **user-invocable only** — Claude gets
    `disable-model-invocation` if it tries, so ask the user to run it rather than assuming it will
    happen
  - With neither, fall back to review subagents given *distinct* lenses (correctness and
    concurrency, public API and simplification, test quality and flakiness). One agent asked for
    "a review" returns much less than three asked for different things — and verify each finding
    against the code before acting on it
- **Then run a second round with different lenses, and do it before saying the PR is ready.** This
  is not "review twice"; the two rounds answer different questions. Round one asks whether the code
  does what the description says, and takes the description as the specification. **Round two asks
  whether the description is true** — which is why its lenses point outward rather than at the diff:
  the user meeting the error, the operator reading the dashboard, the blast radius of a move, an
  adversary trying to defeat the invariant. Measured on 2026-08-06 across PRs #314 and #317, both of
  which had passed round one and were CI-green: round two found, in each, **claims written in the
  PR's own javadoc, docs or description that were false** — `"it reaches SQL unchanged as an
  IllegalStateException"` (`FactoryUtil` wraps everything a factory throws), `"the only shape that
  works"` (it forced the storage, not the instrument), `"the repository's first main-code static"`
  (it is the second), `"scoped to the job's class loader"` (true for a job jar, false for the
  `lib/` deployment the docs recommend). None of those is reachable from a lens aimed at the diff.
  Three rules the rounds share. **Converging agents are one source** — two lenses reporting the same
  thing is one finding to verify, not two, and verify it against the *pinned* dependency version
  rather than whichever one an agent happened to read. **A deferred measurement is a claim**: if a
  premise was flagged as reasoned-but-unmeasured, measure it in round two, because the flag is not a
  substitute. And **re-run the mutation batch after acting on a review**, not only before — #317's
  rework left alive a mutant that had been alive all along, because no test pinned that the call
  sites feed the counter the metric reports. The cost is real (three agents plus verification, and on
  #317 it changed the design), so it is for changes whose description makes claims about framework
  behaviour, deployment, or "this is the only way" — not for a typo fix.
- **A finding outside the issue being worked is routed by the user, not by the note written about
  it — and `gh issue create` is never how that routing happens.** Three outcomes, not two: folded
  into the current change, filed as an issue, or **dropped**. Dropping is a real answer and has to
  stay on the list, because a rule offering only "fold or file" makes filing the safe default, and
  a tracker keeps what is filed forever. Ask in the session that found it, and **file only what the
  user has said is not being folded in** — an issue for work about to happen in the open pull
  request is noise. Four constraints, each of them a mistake PR #339 made in one sitting:
  - **Verify the finding before routing it.** A review subagent's example is not evidence. #339
    filed a hole in the licensing gate on a synthesised `THIRD-PARTY.txt` line that
    license-maven-plugin does not in fact produce for this tree's one dual-licensed artifact — so
    the issue described a defect that does not exist. An unverified finding wastes the user's
    decision, not just the tracker.
  - **Route every finding, not the ones that look like they need a decision.** #339 asked about
    three, got three answers, and filed four more unasked. Having asked is not a licence for the
    rest.
  - **A decision the user has already given is not reopenable by the note.** Told to fold the pom
    consolidation into #339, it filed an issue instead. That is the same error pointed the other
    way, and it is worse: it overrides an answer rather than skipping a question.
  - **Never batch-file at the end of a review.** Findings arrive together; the decisions are one
    per finding, and a batch is how the ones that should have been dropped ride along with the ones
    that should not.
  A deferral left in a PR comment or a `CLAUDE.md` "known gap" line is
  the silent deferral wearing a disguise: the reason is recorded, the work is not tracked, and the
  next reader meets a claim with no anchor. A new issue states the **grounded** reason the work is
  not being done now — a measured cost, a blocker in the code, scope the user has not approved —
  never "not planned" or "out of scope", which describe an intention rather than the item, and it
  carries the better approach when one can be named plus a **measure-first** step when the cost or
  benefit is asserted rather than measured. Filing with *no* known answer is still right when the
  first task is finding out whether the problem is real; say so in the issue. #323, #324 and #325
  came out of PR #322 this way, after the same mistake had been made there first — three findings
  written up in a self-review comment and a module `CLAUDE.md`, and left there.
- Pin GitHub Actions to commit SHAs with `just pin-actions` whenever a workflow is added or an
  action version changes
- Commit messages, PR titles/descriptions, code comments, javadoc and issues are written in
  English
- **An issue carries three things: a milestone, a `priority:` label, and a `module:` or `area:`
  label.** The `v0.3.0+` catch-all was retired on 2026-08-07 — it had become the receptacle for
  every unfinished thing (50 open), which expresses neither order nor a release. In its place,
  one milestone per release through the public one: `v0.3.0` correctness on shipped paths,
  `v0.4.0` completeness and performance of the shipped connectors, `v0.5.0` sources, `v0.6.0`
  Spanner, `v0.7.0` Table API and the deferred features, `v1.0.0` **going public** — publishing,
  the release checklist and everything that waits on a public repository. Each milestone's
  description carries its theme; read it rather than guessing from the number. The priority
  labels are ordered by *what breaks*, not by urgency: `priority:P0` is a shipped path where data
  breaks silently or the job stays green while broken, `P1` is correctness with a narrower blast
  radius or a blocker for going public, `P2` is a feature, performance or guardrail that breaks
  nothing by waiting, `P3` is a future feature or one blocked outside the repository. Milestone
  and priority are orthogonal on purpose — the milestone says which release, the label says the
  order inside it, which a milestone alone cannot express. GitHub sub-issues are used where a
  parent genuinely decomposes (#36 → #220–#225); a *cluster* of issues sharing one root cause is
  not that, and is recorded as a comment on the one to work instead, so each keeps its own
  closure record (#348, holding #349/#350/#351)
- PRs close their
  issue with `Closes #N`, written **unformatted** — a closing keyword inside a code span is not
  parsed, so the pull request links nothing and the issue survives the merge. Nothing reports it:
  the body renders, CI is green, and the omission shows up only when the issue is still open
  afterwards. Check `closingIssuesReferences` rather than by eye (`gh api graphql -f
  query='{repository(owner:"laughingman7743",name:"flink-connector-gcp"){pullRequest(number:N)
  {closingIssuesReferences(first:5){nodes{number}}}}}'`), which is how it was caught on PR #322

## Infrastructure (OpenTofu, `opentofu/`)

- `opentofu/flink-gcp` is the single root module for the project's **persistent** GCP resources
  (#5): enabled APIs, the state bucket, the WIF pool/provider, three service accounts and the
  shared IT bucket/dataset. Fine-grained test resources (tables, topics, subscriptions, queues)
  are created by the tests themselves and never belong here. A new connector's API and E2E grants
  are added in the PR that first needs them (Bigtable and Spanner are the known candidates), not
  in advance
- CI is **tfaction v2** (`tfaction-root.yaml` at the root): pull requests touching `opentofu/**`
  get a plan comment (`tofu-plan.yaml`), the merge applies that reviewed plan file from GitHub
  Artifacts and comments the result (`tofu-apply.yaml`); both resolve the changed root modules
  through the shared `tofu-list.yaml`. State locking is the GCS backend's
  native locking. These two workflows are the standing exception to the #111 just-recipe rule:
  tfaction is itself the named, rerunnable sequence, and `just tofu <args>` is the local
  equivalent. They run on plain `GITHUB_TOKEN` — no GitHub App, so tfaction's push-back features
  (auto-fix commits, follow-up PRs) are unused. **The apply workflow must set
  `TFACTION_IS_APPLY: "true"`** — tfaction's job_type is "terraform" for plan and apply alike,
  and setup falls back to `terraform_plan_config` (the read-only account) without it; that
  misconfiguration shipped with #168 and hid behind no-change applies until #170's first real
  write, whose 403s were then misdiagnosed twice (a missing service agent was blamed on evidence
  that never included the authenticated principal — **read the auth step's log first**).
  **A failed apply is recovered by a follow-up pull request**, never by re-running the apply
  job: the failure bumps the state serial, making the saved plan stale, so a re-run can only
  fail again ("Saved plan is stale") — and tofu cancels unstarted operations on the first
  error, so assume nothing from the failed apply exists until measured. tfaction's native
  automation of that follow-up PR needs the GitHub App, deliberately deferred to the dedicated
  org at go-public time (#177; decided with the user on #176, where a dispatch-triggered
  fresh-apply workflow was built as an alternative and withdrawn in the App's favour). One
  service-agent fact worth keeping: enabling an API does **not** create its service agent (they
  provision lazily; `gcloud beta services identity create` is the per-service one-off, in
  `opentofu/README.md`)
- **No service account keys, ever.** All CI credentials are short-lived WIF tokens; the provider
  condition pins the immutable repository/owner IDs, and per-account bindings restrict the apply
  account to `push` on `main` and the E2E account to `push`/`schedule`/`workflow_dispatch` on
  `main`. Plan runs read-only (`roles/viewer` + `roles/iam.securityReviewer`, plus state-bucket
  writes for the lock). Local runs authenticate via `GOOGLE_APPLICATION_CREDENTIALS` from
  `.env`; the bootstrap that created the backend's own bucket is recorded
  in `opentofu/README.md`
- The tofu version is pinned twice on purpose: `mise.toml` (what installs) and
  `versions.tf` `required_version` (what refuses to run on a skew) — a bump edits both

## Version policy

- Releases follow full semver (`v0.1.0`, `v0.2.0`, ...). Early milestones are **tags only** — no
  artifact publishing. Publishing to Maven Central happens once all connectors are implemented,
  as `v1.0.0` (Central namespace registration, signing and the Flink 1.x/2.x publishing strategy
  are decided then; see issues #29 and #39)
- `main` supports **the current and previous Flink minor**, mirroring Flink's own support policy
  (decided in #102). Today that is **2.2 and 2.3**, with `flink.version` pinned to the floor
  (`2.2.1`) because compiling against the oldest and running on newer is the direction that
  works. A new Flink minor moves both ends: that is a deliberate edit to `flink.version` plus
  `.github/workflows/weekly.yaml`, never a dependabot minor bump — which is now enforced by an
  `ignore` rule (patch bumps still arrive). Closed PRs #42 and #97 are the precedent for
  rejecting minor bumps.
- **Flink 1.20 (1.x LTS) is supported from this same source, not from a branch or per-version
  modules** (decided in #32, reversing the branch plan first recorded here — measured, not
  assumed): the whole API delta between 1.20 and 2.x for the surface these connectors touch is
  (a) 1.20 still declaring the deprecated `createWriter(Sink.InitContext)` abstract while 2.x
  removed the type — absorbed by the one-interface `CrossVersionSink` seam under
  `src/main/java-flink1`/`java-flink2`, selected by the `flink.compat` Maven property (default
  `flink2`; `just verify-flink 1.20.x` adds `-Dflink.compat=flink1` itself) — and (b)
  `CommittableMessage.getCheckpointId()` changing its return type across the majors, dodged by
  calling `getCheckpointIdOrEOI()` (present in both, deprecated on 2.x; if a 2.x minor removes
  it, that call is the line to revisit). The 1.20 bridge default is compile-only: 1.20's runtime
  always creates writers through `createWriter(WriterInitContext)`, measured by the whole suite
  running green on 1.20.4 with the bridge throwing. **This is source-level support** — the
  weekly `lts` row compiles and tests everything at `FLINK_LTS`, a jar is compiled per major,
  and no cross-major binary claim is made (the one-artifact claim below spans the 2.x range
  only). A red `lts` row reproduces locally with `just verify-flink <FLINK_LTS>` — the same
  first-move rule `binary_compat` has. A Dataproc-style per-version module split was considered and declined: it buys
  isolation the two ~15-line interface variants already provide. Publishing (the kafka-style
  `X.Y.Z-1.20` suffix) is decided in #29/#39
- **One artifact covers the supported range**, so there is no per-minor artifact suffix (the
  `-2.1` suffix assumption from before #102 is dropped; #29/#39 decide publishing). Only about
  half the Flink API surface these connectors touch is `@Public` — and `@Public` guarantees
  source, not binary, compatibility across minors — so the claim rests on the `binary_compat`
  job in `weekly.yaml`: build against the floor, then re-run the whole suite with the newest
  supported Flink swapped onto the classpath and nothing recompiled. If it ever goes red, the
  fallback is per-minor artifacts as `apache/flink-connector-kafka` publishes them
  (`5.0.0-2.1` / `5.0.0-2.2` from one branch), which is also what Paimon and Iceberg do
- The version matrix lives in `weekly.yaml`, not `verify.yaml`: per-PR CI stays single-version for
  latency, matching Flink's own `push_pr.yml` / `weekly.yml` split. Every matrix job checks out
  `github.sha` rather than a branch — a merge landing mid-run once made one version look like it
  had silently skipped 60 tests. Matrix rows carry a **role** (`floor` / `ceiling` / `next` /
  `lts`), not a version, because GitHub does not expose the `env` context to `strategy` and a
  version repeated across rows is how one of them gets missed; the version is resolved in a step
  from `FLINK_CEILING` / `FLINK_NEXT_SNAPSHOT` / `FLINK_LTS` at the top of the file. A 1.20
  patch bump is an edit to `FLINK_LTS` there — dependabot does not see workflow env, the same
  accepted staleness `FLINK_CEILING` has. The `floor` row passes no
  `-Dflink.version` at all, so the pom stays the single source of truth for it, and it runs on
  JDK 21 because floor-on-17 is already covered by `verify.yaml` and by `binary_compat`. The `next`
  row is upstream early-warning and is deliberately **not** `continue-on-error`
- **Moving the supported range** (when Flink releases a new minor): `verify.yaml` needs no edit — it
  names no Flink version and no ceiling, so bumping the pom moves it. The order is
  (1) `pom.xml` `flink.version` → the old ceiling, (2) `weekly.yaml` `FLINK_CEILING` and
  `FLINK_NEXT_SNAPSHOT`, (3) `docs/content/_index.md` table, (4) `README.md` under Build,
  (5) this section. Then **re-run the binary-compatibility measurement against the new ceiling
  before claiming the range** (`just binary-compat <new ceiling>`) — the old measurement says
  nothing about the new pair. Do not hand-maintain this list: `scripts/check-flink-release.sh`
  prints it in its failure output, which is the copy that gets read
- `scripts/check-flink-release.sh` (the `new_minor_check` job) exists because suppressing the
  dependabot minor PR removed the only thing that announced a Flink release. It compares the
  ceiling passed to it against Maven Central weekly and fails until the range is moved. It is
  deliberately **not** a dependency of the other jobs: a new upstream release must not stop the
  current range from being verified
- CI helpers live in `scripts/` as files, not inline in workflow `run:` blocks, so they can be
  run by hand — reproducing a red `binary_compat` locally is the first thing to do when it goes
  red. `tools/` is not the place: it holds build tool *configuration*
  (`tools/maven/checkstyle.xml`), following Flink's layout. Two consequences: `scripts/` is
  outside the `.github/**` rat exclude, so each file carries the plain Apache-2.0 header, and
  `just lint` shellchecks them — and also runs `actionlint`, which shellchecks inline `run:`
  blocks, so a script stays linted whether it lives in `scripts/` or in a `run:` block.
  **A `justfile` recipe is neither** — nothing
  lints inside one — so a recipe body holds commands, and anything that grows into a script goes
  to `scripts/`
- **A multi-step sequence is named once, in the `justfile`, and CI calls that recipe** (#111) —
  `binary_compat` is one step invoking `just binary-compat` rather than four `run:` blocks, so
  the order the sequence depends on has a single definition and is rerunnable by hand. The cost
  was weighed and accepted: a failure names the `==>` phase inside the recipe rather than a step
  in the GitHub UI
- **`lint.yaml` is where linters Maven does not run live** (spotless and checkstyle cover the
  Java sources inside `verify`). Today that is shellcheck, ruff, actionlint, markdownlint and
  `tofu fmt -check` — the `just lint` bullet under Build carries the details
  (`tofu validate` is subsumed by the tofu-plan workflow's plan). A workflow of its
  own rather than jobs in `verify.yaml` so results arrive in seconds rather than behind the
  integration tests — that is the whole reason, the mise-versus-`setup-java` one having turned
  out to be a disarmable default rather than a conflict (see below). On pull requests it runs
  unfiltered, as a reusable workflow under `ci.yaml`'s gate; its `paths` filter exists on the
  push trigger only, where it is cost-saving, and it still must list **every input to a lint,
  not just the linted files** — `mise.toml` is in it because that is where the shellcheck
  version is pinned, and skipping the lint on a version bump would skip it in the one change
  that most needs it. `docs.yaml`'s push filter carries `mise.toml` for the same reason since
  #111, and the main sources and poms since #88 made the API reference part of the site — and
  on pull requests `docs.yaml` runs unfiltered too, accepted over splitting the site's
  definition across two workflows back when its filter decided PR runs, and cheaper to accept
  now that it already matched nearly every one
- **Where a tool's version lives decides how CI installs it.** Pin in `mise.toml` and install
  with `jdx/mise-action` + `install_args` when a version skew can fail a pull request that
  changed nothing — shellcheck (0.9.0 on ubuntu-24.04, 0.11.0 on 26.04, so an `ubuntu-latest`
  migration would fail an unrelated PR) and hugo/go (`docs.yaml` moved onto this shape in #111,
  retiring its `HUGO_VERSION`-plus-"keep in sync" duplication). `install_args` matters: it names
  the subset of `mise.toml` the job needs. Otherwise install with `taiki-e/install-action` and no
  version — that is `just`, whose 1.x compatibility guarantee ("there will never be a 2.0") means
  a newer release cannot break an unchanged justfile, so `mise.toml` says `just = "1"` and CI
  says `tool: just`. **That guarantee covers stable features only**, so nothing CI runs may
  depend on a `--unstable` one; `just --fmt --check` is kept out of `just lint` for exactly this
  reason. Reach for an unstable feature and the tool needs a pin, which means an inline version
  in every install-action step — six of them today
- **`jdx/mise-action` beside `setup-java` needs `add_shims_to_path: false`.** `mise.toml` pins java
  and maven, and the action defaults **both** `add_shims_to_path` and `export_path` to `true`, so
  out of the box its shims and env paths land in front of the JDK the job just installed. That is a
  default to disarm, not a combination to forbid — this bullet said "must not run" until it was
  checked, extrapolated from a real incident that was a *different* mechanism (`mise x --` without
  a tool name, which has its own rule under Build); the two have never actually run together in any
  workflow here. When a `setup-java` job does need a mise-pinned tool: set
  `add_shims_to_path: false`, scope `install_args` to that tool, and invoke it by explicit path —
  `just lint` already does exactly that, handing actionlint `-shellcheck "$(mise which shellcheck)"`.
  **Better still, arrange not to need it.** A tool the Java build depends on should come from Maven
  where it can, because its version then lives beside the dependency it has to track and no workflow
  changes at all — protoc resolves as `com.google.protobuf:protoc:<version>:exe:<platform>`, which
  is why #132 added code generation without touching CI. None of this changes why `just` comes from
  `taiki-e/install-action` in every workflow: that rests on its own reason, one binary on `PATH` and
  no shims at all. `docs.yaml` takes java from `mise.toml` for the same reason since #88, rather
  than adding a second JDK installer for the shim rule above to have to disarm
- **`verify.yaml` selects what a pull request builds instead of filtering whether it runs**
  (#243): pull requests reach it through `ci.yaml` with no paths filter anywhere — required
  checks made filtering impossible, because **a required check that never reports blocks a pull
  request forever** — and a `changes` job derives the Maven `-pl` subset instead. The changed-file list comes from git
  alone — the pull_request checkout is the base-into-head merge commit, fetched at depth 2 so
  `HEAD^1` is the current base tip; a third-party changed-files action was tried and removed on
  PR #247 as avoidable supply-chain surface. The decision is `scripts/ci-maven-args.py`, whose
  module mapping is
  derived from the poms (`<modules>` for the set and reactor order, `io.github.flink-gcp`
  dependencies for the edges — dependents of a changed module build transitively, its
  dependencies ride along for reactor resolution), so a new module is covered the moment the
  root pom names it. `just ci-maven-args --diff origin/main` reproduces the decision by hand.
  The old ignore list (`opentofu/**`, the tofu workflows, `**/README.md` / `**/CLAUDE.md` —
  the last two only because apache-rat's exclude list already carries exactly those patterns,
  so no licence-header check is lost — plus everything under `.github/` that is **not** a
  workflow or a composite action: templates, CODEOWNERS, `dependabot.yml`. That last one is a
  rule, not a list, so a new template needs no edit here; the two directories that decide what
  CI itself does are the named exception) lives twice on purpose: as the script's first
  classification rule, and as a real `paths-ignore` on the **push** trigger only, where no
  required check can be blocked and a tofu-only merge stays free. The two are no longer
  identical: the push list keeps naming the inert `.github/` files one by one, because
  GitHub's `!` negation in `paths-ignore` is order-sensitive and a mistake there silently
  stops CI on a real workflow change — while the cost of not mirroring the rule is one full
  build per merge of a template. A **root-only** change builds
  `-pl .` alone: `docs/**`, `scripts/**` and the root uv project (`pyproject.toml`, `uv.lock`)
  are the paths whose only Maven-relevant consumer is the root module's rat run, which scans the
  whole working tree and is their only pre-merge licence check (#253 — a `scripts/tests/`-only
  pull request had been paying 7m41s of full reactor for it). **Two files are deliberately
  outside that class**, `scripts/licence-sources.toml` and `scripts/check-notice.py`: the NOTICE
  check is a step *inside* the `build` job gated on `check_notice`, which is false when no
  shaded module is built, so routing them there would skip the licence check on exactly the
  change that edits the licence pins. That the other checkers' scripts *are* in the class is the
  same fact from the other side — `api_tiers`, `option_docs` and `metric_docs` are unconditional jobs, so
  nothing about them depends on what the deriver picks. The `justfile` stays full-reactor too:
  it carries the Maven invocations themselves.
  Pushes to main and `workflow_dispatch` always build the full reactor.
  **`ci.yaml` is the pull-request orchestrator, and branch protection requires exactly its one
  gate, `CI passed`** (#250, live since the plan upgrade — a private free-plan repository cannot
  set required checks at all; the shape is suzuki-shunsuke's required-status-check pattern,
  decided with the user on PR #305 after it briefly enumerated checkers as their own required
  contexts): `verify.yaml`, `lint.yaml`, `docs.yaml` and `tofu-plan.yaml` run as reusable
  workflows called from `ci.yaml`, whose gate `needs` them all — the only way a verdict can span
  workflows — and derives its verdict from the whole `needs` context (`scripts/ci-gate.py`, the
  hand-runnable truth table). A settings-side list has to be edited every time a job is added or
  retired, falls silently out of step with a renamed job, and cannot follow path-conditional
  jobs; a stale `needs` entry or `uses` path is a workflow-parse error no run survives
  unnoticed, and the wiring tests in `scripts/tests/test_ci_gate.py` hold every gate's `needs`
  to its file's full job list. The children with a legitimately skippable job (`verify`'s Maven
  build, `tofu-plan`'s plan) carry an internal verdict job with the same script, telling it
  which skip is legitimate via `SKIPPED_OK` — a workflow result alone cannot expose an
  illegitimate skip. So a job or workflow enrolls in ci.yaml/its child alone and touches no
  repository setting; a required context is matched **by job name**, so `CI passed` is the one
  name whose rename must update branch protection in the same change. `docs.yaml` and
  `lint.yaml` are required through the gate like everything else; their `paths` filters survive
  on the push trigger only, as cost control
- JUnit stays on 5.x and testcontainers on 1.x for now; their major-version dependabot PRs are
  intentionally left open/deferred
- Google Cloud library versions come only from `libraries-bom`; never pin individual
  google-cloud artifact versions

## Licensing and provenance

- Files written for this project carry the plain Apache-2.0 header
  (`Copyright 2026 laughingman7743`). Files copied from Apache projects keep their ASF header.
  apache-rat enforces this (configuration overridden in the root POM; new unheaderable file
  types need a rat exclude there) — and enforces *those two headers*, not "an Apache licence is
  mentioned somewhere", since #255 turned rat's built-in matchers off. They had been approving a
  file for carrying the bare licence URL, so a header could lose its first line and still pass.
  Still **not** checked, deliberately: the contents of the copyright line, and which of the two
  headers a given file carries. A third header would need its own configured pattern — which is
  the point at which someone decides whether it belongs here at all
- When adapting Apache-2.0 code from other projects (Beam, Dataproc connector,
  google/flink-connector-gcp, java-bigquerystorage, apache/flink-connector-gcp-pubsub):
  record the provenance in the module README and the repository `NOTICE`, and keep original
  headers where applicable. Keep each module README's "no code copied" claim accurate
- **This project is Apache-2.0 with no usage restrictions, and its dependencies must be too.**
  A library under a restrictive licence — the GPL family, or the newer source-available and
  non-commercial ones (SSPL, BUSL, Commons Clause, …) — is normally rejected outright rather than
  recorded in a NOTICE; adoption of one would be a project discussion, not a licensing entry.
  `scripts/check-notice.py` enforces this for the shaded modules (decided with the user on #138;
  the one standing exemption is `javax.annotation-api`, dual-licensed and taken under CDDL with
  the classpath exception)
- Never open or reference the private in-house implementation this project supersedes; design
  references must be public OSS or official documentation only

## Package layout convention (all connector modules)

Under `io.github.flink.gcp.connector.<product>` (decided in #63, applied to BigQuery first;
Pub/Sub, Cloud Tasks and later modules follow the same skeleton):

- `sink` — public sink API only: the facade + builder, write-method enum, shared options/enums,
  destination types, and the `@Internal` types shared by every write method (the sink config,
  the fixed-destination resolver; retry machinery lives in `base.retry` since #61 extracted it)
- `sink.<writepath>` — one subpackage per write-path family, which may host several write
  methods (BigQuery: `sink.storage` holds the Storage Write API family — the default-stream
  at-least-once method today, and the #30 buffered-stream exactly-once method beside it,
  sharing the appender machinery; `sink.fileloads` holds FILE_LOADS). The package root holds
  the Sink classes, the family's public options objects and committable contracts; internal
  stages follow the Flink FileSink precedent with `.writer`, `.committer` and
  post-commit-topology subpackages (`.loadjob` here, FileSink's `.compactor`) as the topology
  requires — a family without 2PC simply has no `.committer` package.
  **One family, with no second one in prospect, means no layer**: the module goes straight to
  `sink` + `sink.writer` (`sink.committer`, … as the topology requires). Decided in #119, where
  Cloud Tasks' `sink.createtask` was named after the `CreateTask` RPC rather than after a design
  and no sibling can arrive at all — `BatchCreateTasks` and `BufferTask` are REST-only and absent
  from the Java client. Pub/Sub's `sink.publisher` went with it so the two single-family modules
  stay alike. Adding the layer back is what a second family costs, and it is a mechanical move —
  the two layers #119 removed held nothing public, though a family layer generally may (BigQuery's
  `BufferedStreamOptions` and `FileLoadsOptions` are `@PublicEvolving` in theirs).
  **The layer is spelled the way Google spells it in code, with no `api` suffix** (#121):
  `sink.storage` mirrors `com.google.cloud.bigquery.storage.v1` and the
  `google-cloud-bigquerystorage` artifact, as `sink.fileloads` already drops the Jobs-API word.
  The public `WriteMethod.STORAGE_API_*` constants keep the product's documented name on
  purpose — the package names an implementation family to maintainers, the enum names a feature
  to users. Inside it, an SPI's real implementation is named after **the SDK resource it owns**
  (the one its `close()` releases) — `WriteClientBufferedStreamService` over `BigQueryWriteClient`,
  as `StreamWriterRowAppenderFactory` is over `StreamWriter`. Neither `Storage*` nor the
  repository's usual `Default*` works here: the SPI is equally a Storage Write API type, so that
  prefix distinguishes nothing, and *default stream* is BigQuery's implicit always-on stream,
  named throughout this package and the opposite of a buffered one
- `sink.tables` — shared table-metadata layer consumed by every write method: the `TableAdmin`
  SPI and its REST implementation, schema snapshot/unifier, REST↔Storage schema converters
- `sink.serializer` — the record-conversion SPI (`BigQueryProtoSerializer`) alone, with
  `sink.serializer.<format>` beneath it for each input format: `.proto`, `.avro` and `.json`
  (#66). Each format package holds its facade, its `@PublicEvolving` options
  object and the `@Internal` types behind them, mirroring how `sink.<writepath>` keeps
  `FileLoadsOptions` and `BufferedStreamOptions` inside their family packages — so this is a
  public-API layer, not merely an internals split. Decided in #125, after #123 took the package
  to ten classes with the names already doing the package's job (`Proto*` ×4, `Avro*` ×4). It
  **passes** the #119 rule rather than contradicting it: that rule is a test, not a count — *one
  family, with no second one in prospect, means no layer* — and here two formats exist today while
  the issue that introduced the second one already plans a third, the exact opposite of Cloud
  Tasks' `sink.createtask`. `BigQueryProtoSerializer` keeps its name and its place at the root:
  the split makes it *read* as the proto family's SPI when it means "the wire form is protobuf,
  whatever the input was", but its javadoc says so, and renaming would touch the sink core, the
  writers and ~20 tests for no behavioural gain — considered and declined, unlike the #121 rename.
  Every package-private coupling stays inside one format, so nothing had to widen to `public`;
  that holds only because the tests move with their format. The format packages must not import
  each other: the three Avro→proto javadoc references are **fully-qualified `{@link}`s rather than
  imports**, so the independence is a property of the import graph and not just of the call graph.
  Spotless does keep a javadoc-only import (measured, not assumed), so the short form was
  available and was declined for that reason
- `sink.failure` — holds the connector-specific failure type only (`FailedRow`); the handler/DLQ
  SPI itself is the shared `base.failure` package since #37 extracted it (#205), whose contract
  the base module's CLAUDE.md records. The package's original purpose ("keep the extraction
  cheap") is discharged; it stays in place because moving `FailedRow` would churn ~a dozen files
  (10 importers plus the class and its test, measured on #213) for no behavioural gain. Later connectors put their failure type at the `sink` root instead (a
  one-class `sink.failure` fails the #119 layer test)
- `source` / `table` — sources (#31, #34, #64) and Table API (#47, #57), with the
  same philosophy: public API at the package root, implementation subpackages beneath. The
  family rule above applies here too, and `source.streamingpull` **keeps** its layer under it:
  the sibling Cloud Tasks cannot have is real here, since a unary-`Pull` source is a live
  alternative — weighed, and rejected on trade-offs the connector documentation records as
  cutting both ways

A new top-level class in a module's `sink` root needs a reason to be public API; implementation
types belong in the subpackages. The one standing exception is a single-family module's
`@Internal` `Sink` class (`CloudTasksCreateTaskSink`, `PubSubPublisherSink`), which sits beside
its facade because there is no family package left to hold it. Every module's `sink` root also
carries the `@Internal` `CrossVersionSink` seam in the per-major source roots
(`src/main/java-flink1`/`java-flink2` — see the version policy): it must be importable by every
sink in the module, and its two variants share one FQCN on purpose. Test sources mirror the
main-tree packages.

The **module root** package holds what belongs to the connector as a whole rather than to one
direction. Today that is the `@Internal` `<Product>MetricNames` every connector carries (#280 — the
base module's CLAUDE.md records why the names live per connector) and, in Bigtable, the
`@PublicEvolving` `TableDestination`. `PubSubMetricNames` is why the placement is a rule rather than
a preference: its names span `sink.writer`, `source.streamingpull.reader` and `.enumerator`, so the
module root is the only package that can hold one inventory. Note what the two have in common is
the *scope*, not the visibility — a names class is `public` because Java has no module-internal
access and its sub-packages must import it, which is what the `@Internal` annotation is there to
say.

## Emulators are conveniences, not authorities

An emulator is a convenience for fast feedback, never evidence about the service's behaviour.
Where the two disagree, the real service decides, and the emulator gets a documented workaround
naming the deviation. A mapping or behaviour decision may not be settled on emulator evidence
alone (#156). The record behind the rule: the goccy/bigquery-emulator divergence table — including
the one that nearly cost a correct mapping — is on #156, and the Pub/Sub emulator's blind spots
(ordered dispatch, ordered seek, dead-letter forwarding, IAM, and every create-option knob it
stores but ignores) are why the #82 real-GCP gated suite is the only coverage of those behaviours.

## Design decisions (do not silently revisit)

Per-connector design decisions live in module-scoped `CLAUDE.md` files, which load when Claude
touches a file in that module. **Read the module file before changing a module's behavior or
public API — and before answering a design question about it**, which is the case the on-demand
load does not cover: a session that never opens a file in the module never sees its decisions, and
reasoning about a module without them is how a settled decision gets re-argued. The topics below
are the trigger; they are not a summary, and none of them is safe to answer from:

- `flink-connector-gcp-bigquery/CLAUDE.md` — facade and serializer SPI, error handling (#13),
  FILE_LOADS (#14), its streaming form (#69) and its live-table reconciliation (#142),
  STORAGE_API_EXACTLY_ONCE (#30), per-write-method
  option scoping, JSON columns (#49/#50), geography columns (#126), Avro and JSON serializers (#66),
  column modes (#124/#145), protobuf well-known types (#147), default-stream tuning knobs
  and the connection-pool guard (#54), Table API/SQL (#57, split into #287–#290) and its shaded
  uber-jar (#290), deferred `location()` (#10). Migrated to ADRs (`docs/adr/0016`–`0035`)
- `flink-connector-gcp-pubsub/CLAUDE.md` — vendoring provenance (#17/#31), sink (#18), topic
  auto-creation (#19), tuning (#20) and in-flight bounds (#85), ordering×repair (#78), emulator
  (#21), source (#79/#80), Table API/SQL (#47, split into #135–#138) and the shaded uber-jar
  (#138). Migrated to ADRs (`docs/adr/0004`–`0015`); the **general** shading decisions every
  later `flink-sql-connector-gcp-*` inherits are `docs/adr/0015` — read it before adding a
  third; what is specific to a tree (an artifact kept out of the bundle, a relocation only it
  needs) belongs beside that connector, as #290's does
- `flink-connector-gcp-cloudtasks/CLAUDE.md` — sink design (#23) and implementation (#24)
- `flink-connector-gcp-bigtable/CLAUDE.md` — sink design and implementation (#33): implement rather
  than adopt or vendor, the four SDK facts the writer rests on (including the client's own blocking
  flow controller and two Google-internal annotations accepted deliberately), the row-level vs fatal
  boundary — `INVALID_ARGUMENT` alone, settled on gRPC's definition plus AIP-194 rather than left
  unmeasured — one fixed table per sink, and why the send metrics landed here rather than with #37.
  Also the E2E enablement (#218): the ephemeral per-class instance and its sweep, what real Bigtable
  answers each rejection with, and the emulator deviation table those measurements produced.
  Migrated to ADRs (`docs/adr/0041`–`0047`)
- `flink-connector-gcp-test-utils/CLAUDE.md` — the shared test-utils module (#27): test-support
  code only (main-code sharing belongs in `flink-connector-gcp-base`), all-provided dependencies,
  no forced unification of emulator container fixtures, and the justfile install-list coupling its
  reactor-sibling consumers create
- `flink-connector-gcp-base/CLAUDE.md` — the shared main-code module (#61, joined by #37's
  DLQ/metrics as `base.failure`/`base.metrics` and by `base.lifecycle`/`base.rpc`): the failure
  SPI and metric-name conventions (#280), retry loops and
  retryability classification stay per-connector (the evaluated-and-declined `Retries.run`
  executor is recorded there), compile-scope consumers, and the shading/install-list consequences
  that scope carries. Migrated to ADRs (`docs/adr/0036`–`0040`)

Decisions that span connectors stay here: the package layout convention above, the version policy
and the licensing rules. A new connector gets its own module file rather than a section here.

## Cross-connector contracts (rules here; full records in `docs/adr/`)

- **A serializer returning `null` skips the record** (#230; `docs/adr/0001`): every connector
  serialization SPI is `@Nullable` on `serialize`, and a `null` means filter, only filter — the
  record is written nowhere, is not a failure, never reaches the `FailureHandler`, and
  `recordsSkipped` is the only thing that reports it. The writer's check sits immediately after
  the serializer's `catch`, ahead of any per-destination state; a combinator over one of our own
  SPIs propagates the `null` unchanged; a `null` from a wrapped Flink `SerializationSchema` is a
  serialization *failure*, never a skip. Precedents, the rejected alternative and the combinator
  bug are in the ADR.
- **A test forges an options object on `builder().build()`, never on `defaults()`** (#316;
  `docs/adr/0002`): `defaults()` hands out a JVM-wide singleton, and a reflective forge on it
  poisons every later test in the surefire fork. The forging test also asserts the singleton
  survived, placed in the class that would do the writing. The incident, the fork mechanics and
  the deterministic reproduction recipe are in the ADR.
- **A vendor client's teardown may re-report a failure the connector already consumed** (#325;
  `docs/adr/0003`): the Bigtable batcher and the Pub/Sub subscriber do, by unrelated mechanisms.
  Their wrappers absorb the re-report, each SPI's `close()` javadoc binds a third implementation
  to the same contract, and the absorb stays per-connector because the mechanisms share no type
  to catch. A client that cannot be subclassed holds its operations as functional values, or the
  absorb has no test. The nine-SPI survey, the asymmetric consequences and the #351 refinement
  (what else a wide absorb catches) are in the ADR.
