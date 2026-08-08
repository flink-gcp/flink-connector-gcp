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

# ADR-0035: What the BigQuery uber-jar does differently from the shared shading record

- Status: Accepted
- Date: 2026-08-07
- Issues: [#290] (under [#57]); the inherited general record is ADR-0015
- Modules: flink-sql-connector-gcp-bigquery
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`; the module README

## Decision

The shading and licensing decisions are ADR-0015's, inherited wholesale. What is this tree's
own:

- **`org.slf4j:slf4j-api` is the artifact kept out of *this* bundle and not the other's** (the
  Pub/Sub tree carries no slf4j at all; this one gets it through Avro). The exclusion both trees
  take, `javax.annotation-api`, is ADR-0015's. Bundling slf4j is wrong either way
  round: relocated, the connector's own `LoggerFactory` calls are rewritten with it, so they
  bind to a copy no Flink log configuration reaches and the connector goes silent under a green
  job; unrelocated, the jar puts a second `slf4j-api` on a classpath that already has
  flink-dist's. It leaves by an `<exclusion>` on the connector dependency rather than a shade
  filter, which is ADR-0015's rule for every exclusion and carries its reason.
- **The SQL module's runtime tree is not the connector module's** — 110 third-party artifacts
  against 114 — worth knowing before reading a relocation list against the wrong
  `dependency:tree`: `commons-lang3` and `commons-io` appear only in the connector's, and
  `commons-compress` resolves 1.24.0 here against 1.26.0 there, because their only path is
  `flink-core`, which is `provided` on the connector and contributes nothing transitively. The
  other two are the exclusions this pom declares — `slf4j-api`, this tree's, and
  `javax.annotation-api`, both trees' (ADR-0015). The relocation list is derived from the SQL
  module's own `runtime-classpath.txt`.
- **A relocation pattern rewrites *references*, so it must not be wider than the tree.**
  `org.apache.commons` was the tempting one line for a tree carrying only commons-codec and
  commons-compress, and it silently renamed httpclient's 113 references to
  `org.apache.commons.logging` — an artifact this tree does not have — into a name private to
  the jar, which no user can then satisfy by putting commons-logging in `lib/`. Two named
  patterns instead. `com.google` is the standing exception, wholesale in both SQL modules, and
  it pays the same cost for `com.google.appengine` — an optional dependency already absent.
- **Three SPI files are filtered out because their interface is a JDK type**
  (`javax.xml.stream.XML{Input,Output,Event}Factory`, `java.time.chrono.Chronology`). Only the
  implementation relocates, so the jar would otherwise register relocated Woodstox as the
  **JVM's** StAX provider for everything sharing it — and the deployment this artifact is for is
  Flink's `lib/`, so that is Flink and every job on the TaskManager. Nothing here needs them:
  Woodstox arrives only because google-cloud-storage brings jackson-dataformat-xml, which falls
  back to the JDK factory. An SPI whose interface relocates with it (gRPC's providers, Jackson's
  modules) is unaffected, and the Flink factory SPI is the jar's whole point. Found by review —
  no assertion in the packaging suite looks at resources.
- **`org.apache.avro` is relocated, which makes the uber-jar's `AvroRecordSerializer` unusable
  from a DataStream job** — its signature there takes a relocated `IndexedRecord`. Accepted
  rather than exempted: the same trade the Pub/Sub jar makes with `PubsubMessage`, and leaving
  Avro alone would put a second copy beside whatever `flink-avro` a SQL deployment carries. Both
  READMEs and the docs page point a DataStream user at the plain connector jar.
- **Four `META-INF` paths are excluded from both SQL modules, and `META-INF/LICENSE`
  deliberately is not.** `META-INF/native-image/**` and `META-INF/proguard/**` are build-tool
  inputs naming unrelocated classes — GraalVM and R8 read them, a Flink deployment reads
  neither, and no bundled class reads them at runtime (grepped, not assumed). The two
  JDK-interface SPI files are above. `META-INF/LICENSE` was excluded with them and **reverted**:
  shade takes the project jar first, so the surviving copy is this project's own — measured
  byte-identical to the repository root `LICENSE` — and dropping it left the two jars a user
  downloads directly as the only artifacts here carrying no licence.
  `theProjectsOwnLicenceIsInTheJar` holds it now.
- **`META-INF/versions/**` is excluded from both SQL modules.** maven-shade relocates a
  versioned class's *contents* and leaves it at its original path, so jackson-core's Java
  11/17/21/22 variants shipped spelled `com/fasterxml/...` in a jar whose base copies had moved
  — caught by the packaging test. They are dead weight either way, since an uber-jar's manifest
  carries no `Multi-Release: true`; the Pub/Sub module takes the same exclusion so the two poms
  cannot answer one question two ways.
- **Arrow, netty and flatbuffers are bundled for a code path this connector never runs** — the
  Storage *Read* API in `google-cloud-bigquerystorage`. Excluding them was weighed and declined:
  it reintroduces the enumerated include list ADR-0015 removed after measuring that an unlisted
  transitive is silently *dropped*, and [#64] would need them back. Priced before declining:
  3.2 MB of the 58 MB of compressed entries, about 5% (measured 2026-08-06) — a third of what
  the first estimate assumed.
- The smoke ITCase lets the sink **create its own table**, so one job drives both relocated
  transports — REST for the metadata half, gRPC for the rows — the shape only this connector
  needs, since only it has two `emulator-*` options.
- This module discharged the [#26] trigger: the packaging/NOTICE/`ShadedJar` trio moved to
  `flink-connector-gcp-test-utils` instead of being copied.

[#26]: https://github.com/laughingman7743/flink-connector-gcp/issues/26
[#57]: https://github.com/laughingman7743/flink-connector-gcp/issues/57
[#64]: https://github.com/laughingman7743/flink-connector-gcp/issues/64
[#290]: https://github.com/laughingman7743/flink-connector-gcp/issues/290
