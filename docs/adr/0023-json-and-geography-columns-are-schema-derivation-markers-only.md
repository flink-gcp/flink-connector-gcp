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

# ADR-0023: `JSON` and `GEOGRAPHY` columns are schema-derivation markers, decided at one point

- Status: Accepted
- Date: 2026-07-26 (JSON, [#50] widening [#49]'s paths); 2026-07-27 (geography, [#126], which
  also extracted the single decision point)
- Issues: [#49], [#50], [#126]
- Modules: bigquery (`sink.serializer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § JSON columns,
  § Geography columns

## Decision

A `JSON` column is carried as a string by the Storage Write API, so `ProtoSchemaOptions` is
purely a **schema-derivation marker** — it decides whether the derived schema says `JSON`
instead of `STRUCT`/`STRING` for table auto-creation, the write stream and load jobs. It covers
**message and string** fields (a message is printed as canonical proto JSON; a string is passed
through verbatim and *not* validated — malformed JSON is a BigQuery row-level error). [#50]'s
issue text said message-only; that was widened in the implementing PR because the corpus the
feature exists to migrate annotates **string** fields, so option selection alone would have
delivered nothing.

- An unset plain proto3 string is **left unset rather than written as `""`** (the row
  descriptor's JSON field has presence, and `""` is not valid JSON, so writing it would fail
  every record that omits the field) — limited to fields without presence, since elsewhere `""`
  is the user's own statement.
- An option **number matching no field is deliberately not an error**, unlike a path, because
  one configuration serves every message type a job writes.
- **No `ExtensionRegistry`**: protobuf-java never resolves custom options against the descriptor
  pool (not even for a declared dependency), so the unknown-fields read is the *normal* path,
  and `getAllFields()` reaches a generated extension by number without the generated class.
  Because protobuf's private extension range has no registry, the option's **full name is its
  identity** — `jsonFieldOption(GeneratedExtension)` captures it (the extension itself is not
  `Serializable` and must not be retained); `jsonFieldOptionNumber(int)` remains for descriptors
  that arrive without the annotations artifact. Both **accumulate** like `jsonFieldPath`, keyed
  by number so a named entry always wins over a bare one at the same number, and the last name
  wins when two extensions claim one number. The name **rules out a foreign declaration**; it
  cannot arbitrate between two rivals both present in the pool, since an unresolved option
  records only its number.

**Geography mirrors JSON exactly** ([#126]) and is *nothing* on the value path — a marked string
is `Kind.IDENTITY` with the empty-string rule, since `""` is no more a valid geometry than valid
JSON. Paths-only was the first decision and it was **reversed on measurement**: the estimate
behind it ("~200 lines of near-duplicate API") was wrong because `checkExtensionNumber` and
`BoolFieldOptionReader` are already static and shared — the real cost was ~80 lines, mostly
javadoc. Quantify before pricing a decline. **Strings only**, the one place this marker is
*narrower* than the JSON one: no protobuf message means a geography to BigQuery, so there would
be nothing to write; the rejection is stated about the *field's type*, so it fires however the
field was selected.

**The single decision point is `ProtoToTableSchemaConverter.markedType(field, path, options)`**
(returning `JSON`, `GEOGRAPHY` or `null`, folding the automatic JSON of
`Struct`/`Value`/`ListValue` on top of `ProtoSchemaOptions.markedType`): the JSON decision
expression used to be duplicated in `convertField` and `ProtoRowConverter.buildFieldPlan` with a
comment in each saying it must stay identical to the other, and a second marker would have
doubled that hazard. `AvroSchemaOptions.markedType(path)` is the same shape on that side.
A field claimed by **both** markers is rejected in `markedType` — the single decision point —
rather than in `Builder.build()`: a JSON *field option* cannot be intersected with a geography
*path* without a descriptor, so build() could never own the whole rule, and every sibling rule
already lives at derivation. **One extension number registered as both markers** is the
exception and *is* rejected in `build()`, being broken for every message rather than for some.
A configured marking **wins over well-known-type recognition and is then rejected** for not
being a string, rather than silently falling back to the automatic `JSON`.

**Derivation is the right place because `ProtoMessageSerializer` derives eagerly in its
constructor**, which [#126] fixed as part of the change: it did not, so every proto schema
misconfiguration was reported from `serialize()`, inside the writers' `FailureHandler` catch,
where log-and-drop swallows it once per record for the life of the job and leaves the table
empty with the job green — the failure went through the *row-failure* path, and the fix was the
one line `AvroRecordSerializer` had carried all along.

## Evidence

- The goccy emulator *does* create and round-trip a `GEOGRAPHY` column (unlike the
  `ARRAY<JSON>` it rejects outright), so the marker is covered by the ordinary emulator IT,
  asserting the created column's **type**.
- FILE_LOADS carries geography end to end: `BigQueryFileLoadsITCase` stages a `GEOGRAPHY` column
  and reads it back with `ST_ASTEXT` against real BigQuery — worth running rather than trusting,
  since BigQuery's documentation describes WKT loading for CSV and JSON and *not* for Avro.
  `AvroRowConverter.toKind` was the one exhaustive switch without a `GEOGRAPHY` case, and its
  absence would not have failed a schema test: the column derives correctly and then throws on
  the first record, inside the `FailureHandler` catch.

## Consequences

- `INTERVAL` and `RANGE` stay underivable, considered and declined (the docs say so): Avro's
  `duration` is a lossy re-encode in either direction, and `TableSchemaToAvroConverter` rejects
  both outright — deriving either would break the FILE_LOADS round trip
  `AvroSchemaRoundTripTest` pins. `RANGE` has no Avro or protobuf equivalent at all.
- Adding the geography marker to a running pipeline is a **breaking schema change**
  (`STRING` → `GEOGRAPHY`): `SchemaUnifier` only relaxes, so it rejects the union rather than
  corrupting rows, as it does for ADR-0027's `STRUCT` → scalar.

[#49]: https://github.com/laughingman7743/flink-connector-gcp/issues/49
[#50]: https://github.com/laughingman7743/flink-connector-gcp/issues/50
[#126]: https://github.com/laughingman7743/flink-connector-gcp/issues/126
