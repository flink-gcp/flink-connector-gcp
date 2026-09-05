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

# ADR-0150: Jackson is managed as one set by a jackson-bom import paired with Avro's pin

- Status: Accepted
- Date: 2026-09-05
- Issues: [#1207], [#1191]
- Modules: all (build); bigquery, bigtable, spanner (shaded bundles)
- Current behavior: the root `pom.xml` — the `jackson-bom.version` property and the first
  `dependencyManagement` import — and the matching `ignore` rule in `.github/dependabot.yml`

## Decision

The root POM imports `com.fasterxml.jackson:jackson-bom` in `dependencyManagement`, first among
the imported BOMs, at the version `avro-parent ${avro.version}` pins — 2.22.1 today. Every module
and every shaded bundle therefore resolves one Jackson set — core, databind, annotations,
dataformat, datatype — by construction, whatever order its dependencies are declared in and
whatever the libraries BOM's clients declare.

Three things follow:

- The per-module `jackson-annotations` test pins that [#1191] added to
  `flink-connector-gcp-cloudtasks` and `flink-connector-gcp-docs-validation`, and the
  `jackson-annotations.version` property behind them, are removed. The import covers what they
  covered.
- `jackson-bom` carries a dependabot `ignore` rule for every update type, the rule every version
  pinned to another project's pin carries in this repository. It moves when `avro.version` moves
  and at no other time.
- The three bundles that ship Jackson — `flink-sql-connector-gcp-bigquery` (five artifacts),
  `flink-sql-connector-gcp-bigtable` and `flink-sql-connector-gcp-spanner` (`jackson-core`) —
  have their NOTICEs regenerated.

## Context

`flink-connector-gcp-bigquery` bundled a coherent Jackson 2.18.3 set, and [#1191]'s self-review
found that nothing held it there. Measured 2026-09-05 on `main` (`libraries-bom` 26.87.0, avro
1.12.2):

- The effective POM manages no `jackson-core`, `jackson-databind` or `jackson-annotations`. The
  only Jackson-named entries the libraries BOM contributes are `google-http-client-jackson2` and
  `google-api-client-jackson2`. So Maven's nearest-then-first-declared rule decided the set.
- `google-cloud-bigquerystorage` 3.32.0 declares core, annotations, databind and jsr310 at 2.18.3
  directly. `avro` 1.12.2 declares core and databind at the 2.22.1 its parent's own `jackson-bom`
  import pins, with annotations 2.22 one level below. Both sit at depth two in the BigQuery
  module, and bigquerystorage is declared thirteen dependencies earlier, so 2.18.3 won.
- Swapping the two declarations flips it: core and databind 2.22.1 from avro; annotations and
  jsr310 2.18.3 from bigquerystorage; `jackson-dataformat-xml` 2.18.3 from `google-cloud-storage`.
  On that classpath none of the 17 cases in `AvroSchemaRoundTripTest` and
  `AvroRecordSerializationSchemaTest` passes: 14 end in
  `NoClassDefFoundError: com/fasterxml/jackson/annotation/JsonSerializeAs` out of
  `ObjectMapper.<clinit>` — databind 2.22 reads an annotation added in 2.21 — and the other
  three in `assertThatThrownBy` assertions that caught that error while expecting a different
  one.
- On the two test classpaths [#1191] patched, the mismatch had the same shape with a different
  first-declared client. In `flink-connector-gcp-cloudtasks`, testcontainers' `docker-java-api`
  puts annotations 2.10.3 one edge nearer the root than the 2.22 avro's databind brings. In
  `flink-connector-gcp-docs-validation`, whose graph contains no docker-java at all, the BigQuery
  connector's bigquerystorage puts annotations 2.18.3 at the same depth as avro's and is declared
  first — the BigQuery shape again, one module over.
- No source file in this repository imports `com.fasterxml.jackson`. Only classpath composition
  and the bundles change.

**One premise of [#1207] measured false.** The issue said a flip would land "in a published jar
rather than a test classpath". It would land in both, and the BigQuery module's own unit tests
reach it first: the 17 cases above, in whichever pull request swapped the declarations or
brought a BOM whose bigquerystorage declares a different subset of the four. What was true is
that nothing pinned the set and nothing named it. Making the set structural is what this decision
buys, not a test the bundle lacked.

With the import in place the same swap changes nothing but where each artifact enters the tree:
2.22.1 for core, databind, jsr310 and dataformat-xml and 2.22 for annotations, on every scope of
all thirteen modules, the docs-validation profile module included.

## Why first among the imports, and why Avro's pin

- **First among the imports.** When two imported BOMs manage the same artifact the first import
  wins. Placing `jackson-bom` before `libraries-bom` keeps the Avro-paired version authoritative
  even if a future libraries BOM starts managing Jackson. Today it manages none, so the order
  costs nothing.
- **Avro's pin, not the Google clients' declaration.** Avro is the consumer whose compiled
  expectations bite: its databind reads annotations it was built against. The Google clients
  declare an older Jackson and are satisfied by any newer 2.x, which is the direction Jackson
  keeps compatible. Following `avro-parent` also makes this the rule `zstd-jni` already follows
  rather than a fifth rule.

## Consequences

- **Two bundles move for a reason unrelated to them.** `flink-sql-connector-gcp-bigtable` and
  `flink-sql-connector-gcp-spanner` carry `jackson-core` because their clients declare it at
  runtime scope; they use no Avro. They now ship 2.22.1 where their client declared 2.18.3 — the
  compatible direction, and the price of one mechanism rather than three.
- **An `avro.version` bump owes two property moves**, `jackson-bom.version` and
  `zstd-jni.version`, plus the three NOTICEs. [#1191] missed the zstd-jni half once and
  self-review caught it; the pom comment now names both. A bump that forgets `jackson-bom` leaves
  the set coherent but older than Avro was built against, which fails only where Avro reaches an
  API the older release lacks — and the Avro tests in three modules are where that would show.
- **A libraries BOM bump whose clients declare a Jackson newer than `avro-parent`'s is the case
  the pin does not follow.** It leaves the Google clients running on an older Jackson than they
  declare, the direction Jackson does not promise. It is not silent, because the bump PR's tests
  run on the managed set; but a plain `dependency:tree` prints only the managed result, so the
  check at bump time is `google-cloud-bigquerystorage`'s own pom, as the `org-json.version`
  comment already asks. If it arrives, the decision to re-take is whether
  Avro's pin or the clients' should lead, not whether to manage the set.
- `jackson-bom` manages only `com.fasterxml.jackson.*` group ids, so `jackson-dataformat-xml`
  2.22.1 now runs on the `woodstox-core` 7.0.0 and `stax2-api` 4.2.2 that `google-cloud-storage`
  declares — it excludes the 7.2.0 and 4.3.0 that dataformat-xml itself declares — where 2.18.3
  did before. This connector builds its `Storage` client on the HTTP transport and calls only
  `writer` and `delete`; the XML stack is bundled because the client declares it, not because
  any path here reaches it, so the pairing is one no test here exercises and none needs to.
- Test classpaths that reached annotations 2.10.3 through docker-java-api now get 2.22 — newer
  than docker-java declares, which [#1191] already ran on two modules.
- A consumer of the unshaded `flink-connector-gcp-bigquery` artifact resolves Jackson in its own
  build: Maven 3 does not apply a dependency's `dependencyManagement` to the consumer's graph.
  Maven 4's resolver does so by default, which would hand such a consumer this managed set under
  the connector's subtree — the favourable direction. Neither is this decision's subject.

## Alternatives declined

- **Pin `jackson-annotations` alone in the BigQuery module**, the way [#1191] pinned the test
  classpaths. About eight lines, and it closes only the annotations gap: with avro declared first,
  the swap above still pairs databind 2.22.1 with jsr310 and dataformat-xml 2.18.3 — the same
  class one artifact over.
- **Import the BOM in the BigQuery module only.** One bundle changes instead of three, but the two
  test pins stay as a second mechanism, which [#1207] asked to avoid.
- **Import it in each module that needs it** — three copies today, and a module added later has
  to remember. The root import is the only form that holds for a module that does not exist yet.
- **Let dependabot float `jackson-bom` to the newest 2.x** instead of pairing it with Avro. That is
  always at or above whatever any consumer declares, with no paired step to forget. But Jackson
  releases roughly monthly, so every bump would move three published bundles and regenerate three
  NOTICEs, and it gives up "the pair Avro tests", the rule the other pins keep.
- **A checker asserting that every module resolves a coherent set**, [#1207]'s third option.
  Priced before declining, as this repository asks of every checker: the import is a dozen lines
  of POM and a comment; the checker would be a Maven invocation, a parser, synthetic tests and,
  once it grew an allowlist, a curation skill. Once the set is managed there is nothing left for
  it to check except that the import is still there.

[#1207]: https://github.com/flink-gcp/flink-connector-gcp/issues/1207
[#1191]: https://github.com/flink-gcp/flink-connector-gcp/pull/1191
