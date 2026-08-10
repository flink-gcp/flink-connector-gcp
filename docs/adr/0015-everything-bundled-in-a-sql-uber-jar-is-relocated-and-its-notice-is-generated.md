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

# ADR-0015: Everything bundled in a SQL uber-jar is relocated, and its NOTICE is generated and pinned

- Status: Accepted
- Date: 2026-07-27, revised by [#352] (2026-08-08), [#346] (2026-08-09) and [#412] (2026-08-09)
- Issues: [#138] (the first shaded module; what is decided here is inherited, not re-argued, by
  every later `flink-sql-connector-gcp-*` — [#290] paid the sibling cost and records what is
  specific to its own tree), [#352], [#346], [#412]
- Modules: flink-sql-connector-gcp-pubsub (and, by inheritance, every flink-sql-connector-gcp-*)
- Current behavior: the SQL connector pages and each SQL module's README

## Decision

**Everything bundled is relocated under `io.github.flink.gcp.connector.<product>.shaded.`, with
no exemption for `grpc-netty-shaded`.** The exemption was the tempting answer and was built
first: that artifact carries native libraries whose names netty derives from its own package,
and maven-shade does not rename native resources. It was rejected on a measurement — with
`io.grpc.netty.shaded` left in place the jar cannot share a classpath with anything else
carrying that package, failing with `ServiceConfigurationError: NettyChannelProvider not a
subtype`, and the *first* thing to trigger that would be a second GCP SQL connector built the
same way. The price is two path relocations renaming
`META-INF/native/(lib)?io_grpc_netty_shaded_netty*` to the relocated prefix with dots as
underscores; both forms are needed because Windows DLLs carry no `lib`.

**That is the established form and it was checked, not assumed**: identical pairs are in
googleapis/java-bigtable-hbase (citing netty#6995 and grpc-java#2485), Dataproc's gcs-connector,
spark-bigquery, Beam's `GrpcVendoring`, and the uber-jars of both Google Flink connectors —
while GoogleCloudDataproc/flink-bigquery-connector relocates without renaming and ships a jar
whose tcnative and epoll can never load. `rawString` is **not** needed (maven-shade matches
resource paths directly) and no surveyed project uses it. The replacement is constrained by
netty's `calculateMangledPackagePrefix()`: the relocated name must remain a pure *prefix* of
`io.netty.util.internal.NativeLibraryLoader` — Beam gets away with collapsing `io.grpc.netty
.shaded` to its vendor root only because what remains still satisfies that — and an underscore
in the prefix would have to be spelled `_1`, which is why the shaded prefix must not grow one.
`AbstractSqlConnectorPackagingITCase` derives the expected string from the shaded prefix rather
than repeating it, so config and assertion cannot drift.

Untested residue, deliberately: whether the renamed libraries load through JNI is only exercised
on Linux with epoll or tcnative, and a wrong rename degrades to NIO and JDK SSL *silently*.
Still unrelocated are `org.conscrypt` (native libraries too, but a reflectively-loaded optional
TLS provider gRPC does without) and four annotation-only packages, where a duplicate class is
inert because nothing invokes it.

**An artifact whose licence costs more than its classes are worth is excluded from the bundle,
and a dual licence is such a cost.** A dual licence is an *offer*: shipping the artifact means
**electing an arm on this project's behalf**, saying which in the NOTICE, and meeting that arm's
obligations — CDDL-1.0 §3.1's source-availability clause, for the one case there has been. So the
first question is not how to word the election but whether anything uses the artifact.
`javax.annotation:javax.annotation-api` ([#352]) is that case, and nothing did; the Evidence
section carries the measurement. Two things follow. `javax/annotation/` is a **single-licence**
package in both jars — jsr305 (Apache-2.0) publishes the rest of it — which the packaging
allow-list cannot report either way, being prefix-based and satisfied by *some* class under the
entry. And `check-notice.py`'s restricted-licence gate keeps **no exemption**, so an artifact
resolving to that licence name **fails the build** rather than riding a list: the exclusion's own
regression test. **An excluded artifact leaves by an `<exclusion>` on the connector dependency,
never a shade filter** — the tree the NOTICE, the licence report and
`BundledDependenciesNoticeTest` read is then the tree that is bundled, one fact rather than a fact
plus an exception list — and the connector jars still declare it, so a DataStream user resolving
transitively is unaffected. ADR-0035 records the exclusion only one tree takes.

**Three build traps worth not rediscovering:**

- **Declaring a Google artifact at `test` scope in the SQL module demotes it out of the bundle**
  — Maven's nearest-definition rule beats the transitive `compile` scope — which silently cut
  the jar down to guava plus a few annotation jars. `maven-dependency-plugin:analyze` is absent
  for the same reason: the scoping it would demand is the scoping that breaks the bundle, so the
  test harness uses classes that arrive transitively and declares none of them.
- **`artifactSet` is `*:*`, and the enumerated include list it replaced should not come back.**
  The list's justifications each died when measured: an unlisted new transitive does *not* fail
  the build, contrary to what [#138] assumed — it is silently dropped from the jar, the worst
  available outcome — and "readable beside the NOTICE" ended when the NOTICE became generated.
  With the wildcard a new dependency is bundled automatically; what remains human is relocating
  a genuinely new package root (a real decision — conscrypt must *not* be, and commons-lang3
  under `org.apache.commons` would arrive unrelocated because only `commons.codec` is mapped,
  measured), and the packaging tests fail with the artifact's name until it is made.
  `BundledDependenciesNoticeTest` diffs the NOTICE against the recorded runtime tree both ways.
- **`ApacheNoticeResourceTransformer` needs `organizationName` and `inceptionYear`, not just
  `projectName`**, or the aggregated NOTICE still reads "Copyright 2006-2026 The Apache Software
  Foundation". Relatedly, the root pom sets `<organization>`: without it the ASF parent's
  remote-resources bundle stamped that same claim into *every* module jar this project builds.

**The half of a SQL module's pom that is not its relocation list lives in the root pom's
`pluginManagement`, and a module switches it on by declaring the plugin.** The two poms were very
nearly one: of the Pub/Sub module's 232 non-comment, non-blank lines, **230 also appear** in the
BigQuery module's 269 once the connector name is normalised (measured 2026-08-09; the count on
[#339](https://github.com/laughingman7743/flink-connector-gcp/pull/339), 215 of 224 against 253,
strips comments differently and reaches the same conclusion). The duplication had already been paid
for: that pull request changed the shade filter list three times — `META-INF/versions/*/`
`module-info.class` widened to `META-INF/versions/**`, then `META-INF/native-image/**` and
`META-INF/proguard/**` added — and mirrored each by hand into the second pom it was writing
alongside (the issue counts a fourth, the `META-INF/LICENSE` exclusion tried and reverted inside
the same pull request, which its merged diff therefore does not show). Nothing but a comment and a
reviewer kept the two in step. So `maven-shade-plugin`
(the `shade-flink` execution's `artifactSet`, `filters` and `transformers`),
`maven-dependency-plugin` (both recording executions) and `license-maven-plugin` (its execution;
its configuration was already there) are configured once. A module then declares all three, two of
them empty and the shade plugin carrying nothing but its own relocations.

**Three things must not follow them, and the first is why this was a change of its own:**

- **`<relocations>`.** A relocation rewrites *references* as well as bundled classes, so a pattern
  wider than the module's own tree renames names nothing can then supply — the `org.apache.commons`
  case is ADR-0035's. A union of the two modules' lists would be that same defect, and it is
  measured rather than argued: the Pub/Sub bundle carries **five references to
  `com.github.luben.zstd` from four classes** of grpc-netty-shaded's optional Zstd codec, an
  artifact absent from that tree — and BigQuery relocates `com.github.luben.zstd`, because
  FILE_LOADS puts it in *its* tree. Under a shared list those become a name private to the Pub/Sub
  jar, so supplying zstd-jni in `lib/` would stop working, exactly as it does for commons-logging.
  Five string constants naming `io.netty` are in the same position, one of them inside
  `NativeLibraryLoader`, whose mangled prefix the netty rename dance above already depends on.
  Each list stays derived from its own module's `runtime-classpath.txt`.
- **The surefire `integration-tests` override**, whose `classpathDependencyExcludes` names the
  module's own connector artifact.
- **`japicmp.skip`**, which is a property rather than plugin configuration.

**Why the exclude list may be shared when the relocation list may not, since the two look alike
and the answer is mechanical rather than a judgment.** Three kinds of per-bundle list appear here,
and what each does when applied to a tree that does not contain the thing it names decides where
it lives:

| Kind | Applied to a tree without the named thing | Where it lives |
|---|---|---|
| **Rewrite** — `<relocations>` | still rewrites *references* to it, into a name private to the jar that nobody can supply | per module |
| **Permit** — the packaging suite's unrelocated allow list | silently exempts whatever arrives under that root later, weakening the *other* module's check | per module |
| **Filter** — `<excludes>` | removes nothing; a filter cannot drop an entry that is not there | shared |

The rewrite row is measured above; the permit row is not hypothetical either — the allow list was
first written as the union over both trees, which took `org/checkerframework/`, a BigQuery-only
artifact, out of the Pub/Sub module's check, where a `libraries-bom` bump could then have shipped
it unrelocated. `everyExemptionOnTheAllowListIsInTheJar` exists because of that, and fails an
entry that never fires. The filter row is measured too: stating the two JDK-interface SPI excludes
for both bundles changed the Pub/Sub jar by zero entries.

**The boundary that would move an exclude back to a module** is an exclude that is a claim about a
*tree* rather than about the shape of the artifact: "this bundle must not ship X, and that one
must". All ten are the second kind — duplicate licence files at the jar root, build-tool
configuration no Flink deployment reads, a multi-release tree the JVM never looks at, SPI files
whose interface is a JDK type. The first kind would be a silent removal, so it belongs in the pom
of the module that owns the claim, next to the relocations.

**The shade plugin is thereby configured at three levels, and the merge between them is
load-bearing.** `flink-connector-parent` declares `<filters>` and `<transformers>` at *plugin*
level, both `combine.children="append"`; this repository's root pom declares them at *execution*
level, with `combine.children="append"` on the filters (MSHADE-305: merging rather than appending
misbehaves) and `combine.self="override"` on the transformers, without which the ASF notice
transformer comes back alongside ours; the module adds its relocations to that same execution. A
mistake there changes what ships, silently, in the licensing path of two artifacts meant for
publication — so the check is not reading the merge but **comparing entry names and CRCs of both
built uber-jars against the previous build, and accepting only a zero delta**.

**What already fails when that merge breaks — measured 2026-08-09, three deliberate breaks
([#412]).** Deleting the shared filter list outright, the failure that takes all ten excludes at
once, cannot ship silently: the packaging suite goes red in both modules. The BigQuery bundle is
caught twice — `everyBundledPackageIsRelocatedExceptTheDocumentedExemptions` on the restored
`META-INF/versions/**` tree (jackson-core's real versioned classes among it) and
`noServiceFileRegistersARelocatedImplementationUnderAnUnrelocatedInterface` on the four restored
JDK-interface SPI files. The Pub/Sub bundle is caught once, and that catch rests entirely on two
restored `META-INF/versions/*/module-info.class` entries: its tree carries no other versioned
class and neither SPI file, so if its dependencies ever stop shipping multi-release jars the net
is gone there and this decision is due for re-judging. A merge-level failure takes the list as a
whole, so the six excludes with no assertion of their own — `META-INF/native-image/**`,
`META-INF/proguard/**`, the three licence-file paths and `META-INF/DEPENDENCIES` — cannot be
lost by the merge without the guarded four going too, which is why [#412] settled on recording
this measurement instead of adding a derived-from-the-pom assertion. What that leaves uncovered
is a hand edit to a single exclude line — deleting the `META-INF/native-image/**` line alone
shipped its tree in both jars with every test green — and the only guard for that is the
entry-name/CRC comparison the paragraph above prescribes for changes to this block. Three of
the ten currently remove nothing at all: no bundled artifact ships a root-level `LICENSE` or
`LICENSE.txt`, and every `module-info.class` in either tree lives under `META-INF/versions/`,
so that exclude's work is done by the tree-wide one — which the removes-nothing rule above
already prices in. And dropping `combine.children="append"` from the root pom's `<filters>`
alone is masked today: the parent declares the same attribute on its own filter elements, Maven
keeps the recessive side's combine attribute when the dominant carries none, and the effective
model came out identical but for its generation timestamp, both jars identical in every entry
name. That copy of the attribute protects against `flink-connector-parent` dropping theirs, not
against an edit here.

**One filter list, so the two bundles cannot answer one question two ways.** What it excludes, and
the one thing it deliberately does not:

- `LICENSE`, `LICENSE.txt`, `META-INF/LICENSE.txt`, `META-INF/DEPENDENCIES` and
  `module-info.class`: bundled jars' own top-level licence files would land on top of each other at
  the jar root, and the licences are carried in `META-INF/licenses/` instead.
- `META-INF/versions/**`, the whole multi-release tree rather than just the `module-info` under it.
  maven-shade relocates a versioned class's *contents* and leaves it at its original path, so
  jackson-core's Java 11/17/21/22 variants shipped spelled `com/fasterxml/...` in a jar whose base
  copies had moved — measured on the BigQuery bundle, caught by the packaging test. They are dead
  weight either way, since an uber-jar's manifest carries no `Multi-Release: true`: the JVM never
  looks there, and the relocated Java 8 copies are what runs.
- `META-INF/native-image/**` and `META-INF/proguard/**`, build-tool inputs naming unrelocated
  classes — 35 entries in the Pub/Sub bundle, 54 in the BigQuery one, and one of them injects
  image-global build arguments. GraalVM and R8 read them, a Flink deployment reads neither, and no
  bundled class reads them at runtime (grepped, not assumed). Reinstating them is what native-image
  support would cost, and it would have to relocate them too.
- `META-INF/services/javax.xml.stream.*` and `META-INF/services/java.time.chrono.Chronology`,
  service files whose *interface* is a JDK type, so only the implementation relocates. Left in, a
  bundle registers its relocated Woodstox as the **JVM's** StAX provider and adds nine chronologies
  to `Chronology.getAvailableChronologies()` for everything sharing it — and the deployment these
  artifacts are for is Flink's `lib/`, so that is Flink and every job on the TaskManager, silently
  changing how unrelated XML is parsed. Only the BigQuery tree carries either (Woodstox arrives
  because google-cloud-storage brings jackson-dataformat-xml, which falls back to the JDK factory
  and wraps it), and the Pub/Sub jar contains neither entry — measured 2026-08-09, which is what
  made stating the exclusion for both a zero-delta change. An SPI whose interface relocates with it
  (gRPC's providers, Jackson's modules) is unaffected, and the Flink factory SPI is these jars'
  whole point. The need was found by review rather than by a test, and the test written in response
  — `noServiceFileRegistersARelocatedImplementationUnderAnUnrelocatedInterface` — is still the only
  assertion in the packaging suite that looks at resources at all.
- **`META-INF/LICENSE` is deliberately not excluded.** Shade takes the project jar first, so the
  surviving copy is this project's own, measured byte-identical to the repository root `LICENSE` in
  both uber-jars. Excluding it alongside its `.txt` sibling was tried in [#290] and reverted: it
  left the two jars a user downloads directly as the only artifacts here carrying no licence, with
  two NOTICE lines pointing at an "accompanying LICENSE file" that was no longer there. Apache-2.0
  section 4(a) asks for a copy of the licence; `theProjectsOwnLicenceIsInTheJar` holds it.

**The NOTICE's prose is hand-written; everything mechanical is generated and pinned.** The split
is `NOTICE.template` (module root): human paragraphs plus one `{{Licence}}` placeholder per
group, which `scripts/check-notice.py --update` fills from what license-maven-plugin resolved —
so a wrong group, a duplicate bullet or a stale version is not a checkable mistake but an
*inexpressible* one. `just update-notice <module>` regenerates; `just check-notice <module>`
(CI) re-renders in memory and fails on any drift, offline. Licence *texts* come only from
`scripts/licence-sources.toml`, each entry pinned by **sha256** with its provenance recorded:
the artifact's own jar where one ships a text (best provenance, version-exact), otherwise a
curated URL whose ref matches the bundled version and whose note says why (POM-declared URLs are
often HTML pages or bare templates, and the script rejects HTML outright). A fetch that stops
hashing to its pin fails: upstream changed, a human reviews. This replaced an earlier state
where five texts had been curl'd from repository heads chosen by hand — wrong provenance, and
the reason the pin exists. **Curating a new entry follows a fixed fallback ladder** (printed by
the failure message, and the judgment calls are `.claude/skills/curate-licence-source/`): (1) a
licence file inside the artifact's own jar; (2) the publisher's repository at the tag matching
the bundled version; (3) the publisher's repository head only when it is frozen (archived) or no
version tag exists, with the reason in the note; and there is no rung 4 — a generic template is
not the project's text, since the copyright line is part of a BSD or MIT licence, so an artifact
with no pinnable publisher text is a reason to question the dependency, not to substitute one.

**`download-licenses` must not be used for the licence texts**: it names files after the
*licence*, so protobuf, gax, google-auth and threetenbp collapse into one BSD-3-Clause file and
the last download wins. Measured — it left ThreeTen's copyright line standing for Google's code,
and the copyright holder is part of a BSD or MIT text.

**Invoke the licence goal through a phase, never as a bare `license:add-third-party`** — a CLI
goal invocation selects reactor modules but does not build them, so `-pl` cannot resolve the
connector the module bundles, not even with `-am`, unless an earlier `install` happened to leave
it in the local repository. That failed in CI and passed locally twice for exactly that reason.
It costs nothing to bind: the goal reads POMs Maven has already resolved and fetches nothing.

## Evidence

Measured before the NOTICE machinery was built: license-maven-plugin's classification matched
the hand grouping on **all 52 artifacts**, including the two that inherit `<licenses>` from a
parent pom (guava, animal-sniffer), the dual-licensed `javax.annotation-api`, and re2j's
non-SPDX "Go License". `licenseMerges` in the root pom's `pluginManagement` is what makes the
vocabulary stable — this tree alone spells Apache-2.0 six ways — and it lives there so a sibling
SQL module inherits one vocabulary rather than inventing a second.

**What a sibling actually costs, and [#290] paid it**: the plugin block and one execution in its
pom, its own `NOTICE.template`, a CI step, and `licence-sources.toml` entries for its non-Apache
artifacts (the file and its pins are shared, so overlapping dependencies cost nothing twice).
The estimate held except where it was measured beforehand: the BigQuery bundle resolves **110**
third-party artifacts against the **114** the connector's own runtime tree shows, and needed
**four** new pinned texts. No new `licenseMerges`, as predicted. Paying it twice is what moved the
shared configuration to the root pom, so a third module's pom is now its dependency block, its
surefire override, `japicmp.skip` and three plugin declarations of which only the shade one carries
configuration — its relocations. Everything else it inherits.

**`javax.annotation-api` is referenced by nothing** (measured 2026-08-08, offline): across every
artifact of both SQL trees as they then stood — 52 and 111, all resolvable from the local
repository — and every class of both built uber-jars (16444 and 26947), **not one** references
any of its 15 types, searched both as constant-pool entries (`javax/annotation/Generated`, …) and
as dotted names. Every `javax.annotation.*` string that *is* in the jars belongs to jsr305. Three
things explain it: `javax.annotation.Generated`, the annotation gRPC's generated stubs carry, is
`@Retention(SOURCE)` and so cannot appear in a class file at all (`javap -v`); upstream
`api-common` lists the artifact under `ignoredUnusedDeclaredDependencies`, "declared to fix upper
bound failures"; and it reaches the trees only through the few artifacts that declare it at
`compile` where most of their siblings declare it `provided`.

## Consequences

- **The test trio is extracted, not copied** ([#290], discharging the [#26] trigger):
  `ShadedJar` and the two test bases live in `flink-connector-gcp-test-utils` under
  `testutils.sql`; this module contributes four values through a local `UberJar` holder. What
  the second consumer asked for — a per-module artifact floor, the `ManagedChannelProvider` SPI
  name derived from the shaded prefix, the unrelocated-packages list split into shared and
  per-module parts — is why extracting on one consumer would have guessed wrong.
- `BundledDependenciesNoticeTest` overlaps the script's first check deliberately: the comparison
  is a Python script rather than a Maven plugin, so it is a CI step of its own, and the test is
  what makes the same drift fail inside `just verify`.
- What is specific to a tree (an artifact kept out of the bundle, a relocation only it needs)
  belongs beside that connector — the BigQuery jar's record is its module's, ADR-0035, and
  `slf4j-api`'s reasoning lives there; the Bigtable tree, which meets the same artifact through
  its own client, takes that exclusion by reference in its pom. An exclusion every tree takes is
  this record's, which is where `javax.annotation-api` sits.

[#26]: https://github.com/laughingman7743/flink-connector-gcp/issues/26
[#138]: https://github.com/laughingman7743/flink-connector-gcp/issues/138
[#290]: https://github.com/laughingman7743/flink-connector-gcp/issues/290
[#346]: https://github.com/laughingman7743/flink-connector-gcp/issues/346
[#352]: https://github.com/laughingman7743/flink-connector-gcp/issues/352
[#412]: https://github.com/laughingman7743/flink-connector-gcp/issues/412
