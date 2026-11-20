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

# ADR-0025: The JSON serializer delegates conversion to the client library, and its `BYTES` gap is pursued upstream

- Status: Accepted
- Date: 2026-07-26; revised by [#2188] (2026-11-18: keys are renamed where the default locale lower-cases them differently; 2026-11-19: cached, and only on request)
- Issues: [#66] (JSON half, closing the issue), [#131], [#2188]
- Modules: bigquery (`sink.serializer.json`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § JSON records

## Decision

`JsonDocumentSerializationSchema` takes **`String`** records and a **supplied** schema, since JSON has
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
- The descriptor is derived **in the constructor** (the ADR-0024 reason), and the derived state
  (`state()`) is read *outside* `serialize`'s `catch (RuntimeException)` so a schema problem is not reported
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

## Revision for #2188: keys can be renamed before the library lower-cases them

`JsonToProtoMessage` lower-cases each key with the JVM's default locale and looks the result up in
the row descriptor. It also checks each column of the schema it is handed against the descriptor,
lower-casing the column name the same way. Under a Turkish default locale the key `userId` becomes
`userıd`. ADR-0024's revision for #2188 gives the descriptor the field `userid` there, so with
nothing more the key would name no field. The record would then fail as carrying an unknown field,
or, when `JsonDocumentOptions.Builder#ignoreUnknownFields()` is set, lose the value without a
trace, which is worse than the rejected append it would replace. A mutant without the renaming
measured exactly that: the value read back as `0` instead of `5`.

The serializer therefore makes two changes before calling the library, the second only when asked
to (see below):

- **It hands the library the schema with `Locale.ROOT` lower-case names** (`RowDescriptors.lowerCased`),
  so the library's own check passes whichever form the descriptor was derived from. The library
  reads the schema's column names only for that check and its failure message.
- **It renames each key that reaches a field under either lower-casing and whose default-locale and
  `Locale.ROOT` lower-casings differ** to a spelling the default locale lower-cases to the key's
  `Locale.ROOT` form (`JsonKeyNormalizer`): that form itself, or, where the document already holds
  that key, the form with as few letters upper-cased as give a free spelling (`ID` beside `id`
  becomes `iD`). A key reaches a field the way the library resolves it: by its lower-cased form or,
  where that is not protobuf-compatible, by its `col_` placeholder (`RowDescriptors.fieldName`).
  Under Turkish `İD` reaches no field under `Locale.ROOT` but lower-cases to `id`, and `col_5a6İ`
  lower-cases to `col_5a6i`, the placeholder of a column `客`; renamed, neither reaches one. A key
  that reaches no field under either lower-casing is unknown to the library under the default
  locale as under `Locale.ROOT` and is left as it is, so `ignoreUnknownFields()` keeps its meaning
  and no record's key, however long, is searched. It does this at the top level and inside
  every `STRUCT` column the schema declares, repeated ones included. It never changes a value, so
  the text of a `JSON` column keeps its keys. The check is made per key rather than against a list of languages:
  Unicode's `SpecialCasing.txt` gives Lithuanian, Turkish and Azerbaijani language-sensitive
  mappings, and the JDK's word boundaries for Greek final sigma make Thai differ as well. Under
  most locales no key's lower-casings differ, so nothing is renamed.

Because no key is merged into another, the library meets every key and treats two keys that name
one column as under a locale that lower-cases them alike: it skips a null, validates each value,
appends both to a repeated column and sets a singular one twice. Which value a singular column
keeps, and the order a repeated one takes the elements in, follow the order org.json holds the keys
in. No locale defines that order, and renaming a key can change it. A document holding more keys for
one column than spellings can be found for fails the record, never leaving a key that reaches a
column it does not name or a value unmatched.

The bounded independent reviews of the repair found seven gaps in earlier respellings.

- It failed a record over two unknown keys that `ignoreUnknownFields()` should accept.
- It upper-cased one letter at a time, so it reported no spelling while a two-letter one was free.
- Leaving keys naming no column alone, the first fix for the first gap, let `İD` reach `id`.
- Testing each letter's upper case on its own missed spellings whose lower case depends on
  context, such as a final capital sigma.
- Dropping that test let letters that never lower-case back, such as a Turkish `i`, use up the
  4,096 tries before a free spelling was reached. Each letter is now tested in its place in the
  whole form, a context that upper-casing another letter does not change.
- That test costs a pass over the key per letter, which made a long unknown key's search
  quadratic. Only a key reaching a field under either lower-casing is now searched, so the cost is
  bounded by the schema's field names rather than by a record's keys.
- Comparing keys with column names, the first form of that bound, missed a key reaching a column
  through its placeholder, which the library resolves too.

A first draft merged a renamed key into the one already there, joining arrays and otherwise keeping
one value. The independent review found that this broke all three of the library's rules. Under
Turkish, `{"ID":null,"id":7}` lost the `7`, `{"ID":7,"id":"bad"}` was accepted, and two arrays for
a singular `BYTES` column were joined into one value. An English locale kept `7`, refused `"bad"` and
set one array.

The renaming as first written lower-cased every key of every record twice. Measured on the
serializer alone (2026-11-19, one thread, JDK 21, a document of 28 keys), that took 14% of the
per-record time under an English locale, where it renames nothing. What a key needs depends only on
the key, the locale and the schema level, so each level now caches its decision per key, up to
twice its field count plus 1,024 keys (callers adding a key at once can each add one more), and
starts afresh when the locale changes. A key longer than
twice the level's longest field name is decided as met rather than cached, so that a stream of long
unknown keys cannot hold memory; a key reaching a field is about as long as the field's name.
Two runs on the cached code put the cost at 1.4% and 2.5%, with byte-identical rows. Under
Turkish, where the document's keys are renamed, a record took 9.4% and 15.6% longer than without
the renaming under English; the spread between runs is the machine's.

The owner then asked that the renaming be optional and, since few task managers run under a locale
that needs it, off by default: `JsonDocumentOptions.Builder#normalizeKeysForDefaultLocale()` asks
for it. Without it, under a locale that needs it, a key such as `userId` names no column, so every
record carrying one fails, or, with `ignoreUnknownFields()`, loses the value, and a key such as
`İD` reaches a column `id` it does not name under `Locale.ROOT`. By default no record pays for the
renaming under the locales most hosts run, and a host running such a locale without asking for it
meets those failures. A JVM's default locale usually comes from the host rather
than from the job, so the option's Javadoc and the JSON records section name the locales that need
it, and a record that fails without it while holding such a key names the key and the option in its
message. Only a record that already failed is searched for such a key, so no other record pays for
it. The option is the user's statement about their task managers, not a list of languages the
serializer checks, so the Thai finding below does not apply to it. Once a
release of the client library lower-cases keys under `Locale.ROOT`, the renaming has nothing left to
do, and the option goes with it at a minor release, with a release-notes entry (ADR-0199), rather
than staying as a method that does nothing; its Javadoc says so, and [#2191] lists it.

This does not reopen the `BYTES` decision above, which declined a schema-walking pre-pass. That
pre-pass would have converted values, owning part of the JSON-to-proto mapping and shadowing the
library once it decoded base64 itself. Renaming keys converts nothing. Once the library lower-cases
under `Locale.ROOT`, a renamed key still lower-cases to the field its original would have named, so
the renaming changes nothing the library does and cannot shadow such a fix; the alternative is
values lost without a trace.

Declined alternatives:

- **Failing fast under such a locale** was the smaller change. The owner chose full support
  instead.
- **Renaming by default** covered a host under such a locale without its asking, at the renaming's
  cost on every record under every other locale. The owner chose to make it opt-in, since few task
  managers run such a locale.
- **Naming the option `renameKeysForDefaultLocale()`**, after "renaming", the word this record and
  the docs use for the operation, was proposed by a review. The owner kept
  `normalizeKeysForDefaultLocale()`, and its Javadoc's first sentence says that the normalizing is
  this renaming.
- **Checking a list of languages before the per-key check** saved two lower-casings per key, but it
  missed Thai, which the correctness review of the first draft found.

The library's default-locale lower-casing, in `BQTableSchemaToProtoDescriptor` and
`JsonToProtoMessage` alike, is reported upstream as
[googleapis/google-cloud-java#15056](https://github.com/googleapis/google-cloud-java/issues/15056),
and [#2191] tracks when a release lets these workarounds go.

The rest of this decision stands: conversion is still the client library's, and unless the renaming
is asked for and a key's lower-casings differ, the library sees the document as it was parsed.

[#66]: https://github.com/flink-gcp/flink-connector-gcp/issues/66
[#131]: https://github.com/flink-gcp/flink-connector-gcp/issues/131
[#265]: https://github.com/flink-gcp/flink-connector-gcp/issues/265
[#2188]: https://github.com/flink-gcp/flink-connector-gcp/issues/2188
[#2191]: https://github.com/flink-gcp/flink-connector-gcp/issues/2191
