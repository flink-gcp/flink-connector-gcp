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

# ADR-0025: The JSON serializer delegates conversion to the client library, and its `BYTES` gap is pursued upstream

- Status: Accepted
- Date: 2026-07-26
- Issues: [#66] (JSON half, closing the issue), [#131]
- Modules: bigquery (`sink.serializer.json`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § JSON documents

## Decision

`JsonDocumentSerializer` takes **`String`** records and a **supplied** schema, since JSON has
none of its own — either the Storage `TableSchema` or the REST `Schema` (converted with the
existing `BigQuerySchemaConverter`, which is why `google-cloud-bigquery` is on the module's
public API). That is also why it needs no JSON-column marker: the schema already says `JSON`.
Conversion is the client library's own `JsonToProtoMessage` — the one `JsonStreamWriter` uses —
so there is deliberately **no row converter class here**. Named for its input like its siblings,
and *not* `JsonSerializer`: that simple name collides with Jackson's and Gson's, in exactly the
pipelines that produce JSON text.

Traps and decisions not to re-derive:

- `new JSONObject(String)` **stops at the end of the first value and ignores the rest**, so a
  mis-split newline-delimited record would silently become one row and drop the remainder —
  `serialize` parses through a `JSONTokener` and rejects trailing content instead.
- The library reports every per-row problem as an **unchecked** exception
  (`RowIndexToErrorException`, package-private, so it cannot be named in a catch clause), and a
  bare `IllegalStateException("JSONObject is empty.")` for `{}` — which is pre-empted, not
  caught, so the message can say which record it was.
- A **`BYTES` column takes a JSON array of byte values, never base64** ([#131]), and a **`JSON`
  column takes the JSON *text* as a string, never a nested object** — both contradict what a
  JSON document usually carries, and both are pinned by tests so they read as known limitations.
- **The `BYTES` half is pursued upstream, not here**: a local base64 pre-pass was designed and
  declined, because walking the schema for `BYTES` paths would make this connector own a piece
  of the JSON→proto mapping it otherwise delegates whole — and would shadow the library once it
  decodes base64 itself. Reported as
  [googleapis/google-cloud-java#13980](https://github.com/googleapis/google-cloud-java/issues/13980),
  fix proposed in
  [googleapis/google-cloud-java#13981](https://github.com/googleapis/google-cloud-java/pull/13981)
  covering
  [googleapis/google-cloud-java#13979](https://github.com/googleapis/google-cloud-java/issues/13979)
  too. Two things measured while writing that patch: the fix belongs in
  `fillField`/`fillRepeatedField`, where the library's own recursion already handles the nested
  and repeated paths (including case-insensitive key matching); and it must be guarded on the
  `TableSchema` saying `BYTES`, since proto `BYTES` also carries `NUMERIC`/`BIGNUMERIC` and an
  unguarded decode turns a `NUMERIC` error into a silently wrong value. Whatever upstream
  decides, `bytesColumnsTakeAJsonArrayOfByteValuesAndNotBase64` is what fails when a
  `libraries-bom` bump changes the behaviour, and that is the signal to revisit.
- A **bare number in a `TIMESTAMP` column is epoch microseconds**, so epoch-seconds and
  epoch-millis documents are accepted and stored as some other instant; pinned, since nothing
  can detect it. Keys match columns **case-insensitively**, so a differently-spelled key is not
  an "unknown field". `ignoreUnknownFields` is the one option (default strict).
- `org.json:json` is declared explicitly with a version property because it is used directly;
  that entry *overrides* bigquerystorage's own transitive version rather than following it, and
  `dependency:tree` cannot reveal the drift.
- The descriptor is derived **in the constructor** (the ADR-0024 reason), and `descriptor()` is
  called *outside* `serialize`'s `catch (RuntimeException)` so a schema problem is not reported
  as a bad record — on a task manager the constructor never runs, and every writer calls
  `serialize` before `getDescriptor`, so that is where the first build happens there. An empty
  schema is rejected outright.

## Consequences

Pursuing a fix upstream means proving the patch both ways — fails without it, passes with it —
and neither proof needs the 2.1 GB `googleapis/google-cloud-java` monorepo, whose SNAPSHOT-only
parent chain does not close from a sparse checkout. Two routes, measured 2026-08-06 while
verifying an upstream `java-pubsub` patch under [#265] (the test cycle is ~13 s):

- **A standalone pom over the upstream sources**: no parent, `libraries-bom` for versions,
  `sourceDirectory`/`testSourceDirectory` pointed at the checkout, compiler `<includes>` naming
  only the patched file (every sibling resolves from the released jar) and `<testIncludes>`
  naming the submitted test and its helpers. Faithful whenever the patched file is
  byte-identical between the released version and upstream `main` — check that first. The
  checkout itself can be `--depth 1 --filter=blob:none --sparse` (~60 MB instead of 2.1 GB).
- **The patched class compiled ahead of the jar**: `javac` the single patched source against an
  existing project's classpath, then run a reproducer with the output directory first on
  `-cp` — package-private access to the jar's siblings works because the source declares the
  same package.

Verify the *fix* with the second route (minutes), the *submitted test* with the first before
submitting — an unrun upstream test wastes a reviewer's cycle on a repository that requires two
approvals.

[#66]: https://github.com/laughingman7743/flink-connector-gcp/issues/66
[#131]: https://github.com/laughingman7743/flink-connector-gcp/issues/131
[#265]: https://github.com/laughingman7743/flink-connector-gcp/issues/265
