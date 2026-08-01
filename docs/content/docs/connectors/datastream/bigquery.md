---
title: BigQuery
type: docs
weight: 10
---

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

# BigQuery Connector

BigQuery sink for Apache Flink with a unified, `BigQueryIO`-style write API, provided by the
`flink-connector-gcp-bigquery` module.

One builder dispatches to a write-method implementation at job-graph construction time:

| Write method | Semantics |
|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Storage Write API default stream; dynamic per-record table destinations; connection multiplexing delegated to the client's connection pool |
| `STORAGE_API_EXACTLY_ONCE` | Storage Write API buffered streams + two-phase commit on checkpoints; single fixed destination |
| `FILE_LOADS` | GCS-staged Avro files + BigQuery load jobs; batch and streaming (checkpoint-triggered), exactly-once |

Per-feature implementation status is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-bigquery/README.md).

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .build();
```

API notes:

- `BigQueryProtoSerializer` is an abstract class exposing `getDescriptor(TableDestination)` in
  addition to `serialize`, so the sink can derive table/stream schemas *before* the first record
  of a destination (table auto-creation, write-stream and load-job schemas). Protobuf
  `Descriptor`s are not Java-serializable — obtain them statically or lazily, don't store them in
  instance fields.
- `DestinationResolver.resolve(element, context)` receives the writer context (event timestamp,
  watermark) so time-based routing such as daily tables is expressible. Resolvers run per record:
  cache and reuse `TableDestination` instances.
- `ProtoMessageSerializer.of(MyMessage.class)` is the built-in serializer for records that
  already are protobuf messages. The BigQuery schema is derived from the message descriptor; see
  [Protobuf messages](#protobuf-messages) for the type mapping and for `ProtoSchemaOptions`.
- `AvroRecordSerializer.of(schema)` is the built-in serializer for Avro records — both
  `GenericRecord` and generated `SpecificRecord` streams, since it accepts `IndexedRecord`. The
  BigQuery schema is derived from the Avro writer schema; see [Avro records](#avro-records) for
  the type mapping and for `AvroSchemaOptions`.
- `JsonDocumentSerializer.of(schema)` is the built-in serializer for records that are JSON documents, as
  text. JSON carries no schema, so the destination schema is supplied rather than derived; see
  [JSON records](#json-records).
- `TableDestination` is pure table identity (`equals`/`hashCode` over project/dataset/table);
  per-destination creation metadata (partitioning, clustering) is supplied through
  `TableCreateOptionsProvider` so destination identity stays stable as a cache/connection key.

## Column modes

**A derived column is `NULLABLE`. A constraint is something you ask for.** `REPEATED` is the one mode
derived without being asked for, because a repeated field has no nullable form — a BigQuery
`REPEATED` column is empty, never NULL.

| Serializer | Default | To constrain |
|---|---|---|
| [Protobuf messages](#protobuf-messages) | every non-repeated column `NULLABLE` | `ProtoSchemaOptions.builder().deriveRequiredColumns()` — `REQUIRED` where the field has no presence |
| [Avro records](#avro-records) | every non-repeated column `NULLABLE` | `AvroSchemaOptions.builder().deriveRequiredColumns()` — `REQUIRED` where the field is not a `["null", T]` union |
| [JSON records](#json-records) | whatever the schema you supply says; an omitted mode is `NULLABLE` | write the mode you want in that schema. **No option, deliberately** — nothing is derived, so there is nothing to overrule, and the schema is often the destination table's own |

The two derived serializers take **the same option under the same name**; only the signal differs,
which is the point — the same records should reach the same table shape whichever front end carried
them.

Why the policy runs this way:

- **`REQUIRED` is the mode BigQuery cannot walk back.** It cannot be added to an existing table, so a
  `REQUIRED` column only ever appears at creation time, and relaxing one afterwards is a schema update
  rather than an edit — needing `allowFieldRelaxation`, which is off by default. Defaulting to the
  irreversible choice is the wrong way round.
- **The protobuf mapping is the normative one**, which is why Avro follows it rather than the other
  way round: every write path ends in a protobuf row — `STORAGE_API_*` writes protobuf directly, the
  Avro and JSON serializers convert into one, and File loads stages Avro only incidentally, a staging
  format rather than a contract. Where a front end disagrees, the front end moves.
- **A source schema's "mandatory" is not always a statement about mandatoriness.** On the protobuf
  side especially: a plain proto3 scalar has no way to say "unset", which is a property of the syntax
  rather than a decision its author made, so deriving `REQUIRED` from it would constrain nearly every
  scalar column of an auto-created table on the strength of a default nobody chose. An Avro
  `["null", T]` union *is* deliberate, so this reason does not carry on that side — Avro shares the
  default because of the two above, not this one.

There is deliberately no inverse switch anywhere: with `NULLABLE` as the default, "all columns
nullable" is just not asking for the constraint.

## Protobuf messages

`ProtoMessageSerializer` derives the BigQuery schema from the message descriptor and rewrites each
message into the protobuf row the Storage Write API accepts.

| Protobuf | BigQuery |
|---|---|
| `int32`, `sint32`, `sfixed32`, `int64`, `sint64`, `sfixed64` | `INT64` |
| `uint32`, `fixed32` | `INT64`, widened unsigned |
| `uint64`, `fixed64` | `INT64`; a value above `Long.MAX_VALUE` is a row-level failure |
| `float`, `double` | `DOUBLE` |
| `bool` | `BOOL` |
| `string` | `STRING` |
| `bytes` | `BYTES` |
| enum | `STRING`, the value name |
| `google.protobuf.Timestamp` | `TIMESTAMP`, microsecond precision; anything finer is truncated |
| `google.protobuf.Duration` | `INT64` microseconds, likewise truncated |
| `google.protobuf.FieldMask` | `STRING`, the paths joined by commas |
| `Int32Value`, `UInt32Value`, `Int64Value`, `UInt64Value` | `INT64`, `NULLABLE` |
| `FloatValue`, `DoubleValue` | `DOUBLE`, `NULLABLE` |
| `BoolValue`, `StringValue`, `BytesValue` | `BOOL` / `STRING` / `BYTES`, `NULLABLE` |
| `google.protobuf.Struct`, `Value`, `ListValue` | `JSON`, with no configuration |
| `google.protobuf.Any` | `STRUCT<type_url, value>`, not unpacked |
| message | `STRUCT`, recursively |
| `map<K, V>` | `REPEATED STRUCT<key, value>` |
| message or string marked by `ProtoSchemaOptions` | `JSON`, see [JSON columns](#json-columns) |
| string marked by `ProtoSchemaOptions` | `GEOGRAPHY`, see [Geography columns](#geography-columns) |

A recursive message is rejected — BigQuery schemas cannot represent one — as are sibling fields
whose names differ only by case, which the Storage API cannot tell apart because it lowercases
descriptor field names, and a message with no fields at all, `google.protobuf.Empty` among them,
since a BigQuery `STRUCT` must have at least one column.

### Well-known types

*Well-known types* is protobuf's own term for the messages shipped in `google/protobuf/*.proto` —
see [Protocol Buffers Well-Known Types](https://protobuf.dev/reference/protobuf/google.protobuf/).
The connector recognises them by their fully-qualified names and uses protobuf's own grouping;
nothing here is a name this project invented.

**Wrapper types map to the scalar they wrap**, and stay `NULLABLE` even under
[`deriveRequiredColumns()`](#nullability) — a wrapper is a message field, so it has presence. That
is the point of the type: "unset" stays distinguishable from `0` or `""` all the way to the column,
so an unset `Int64Value` is NULL while one explicitly set to `Int64Value.of(0)` is `0`. Otherwise a
query would have to say `n.value` against a `STRUCT<value>`. The one exception is a proto2
`required` wrapper, which derives `REQUIRED` — it is mandatory, so that is faithful.

**`Struct`, `Value` and `ListValue` become `JSON` columns automatically.** They exist to carry
arbitrary JSON and they are mutually recursive (`Value` → `Struct` → `map<string, Value>`), so there
is no other shape a BigQuery schema can hold them in — before this they failed the whole job at
schema derivation, with a message pointing at the message tree rather than at the mapping. The value
is the type's canonical protobuf JSON, so a `Value` holding a string comes out as `"abc"` and one
holding `null_value` as the JSON literal `null` — distinct from the field being unset, which leaves
the column NULL. `repeated Struct` gives a `REPEATED JSON` column.

**`Any` is deliberately left as `STRUCT<type_url, value>`.** Expanding the payload needs the
descriptor its type URL names, which the connector has no way to obtain; the packed bytes are
preserved as they are. Marking an `Any` field as a JSON column is not a way around this — the
printer then fails on every record with `Cannot find type for url`.

**Explicit configuration wins over all of the above.** A `jsonFieldPath` or field option on a
wrapper or a `Timestamp` field gives a `JSON` column carrying that type's canonical protobuf JSON —
so `Int64Value.of(5)` becomes the quoted string `"5"` — rather than the flattened value. Marking
one [as geography](#geography-columns), by path or by field option, wins in the same way and is then
rejected, none of them being a string: the configured marking is never quietly ignored.

A `Duration` outside protobuf's valid range is a row-level failure routed to the configured
[`FailedRowHandler`](#error-handling), like a `uint64` too large for `INT64`. `FieldMask` paths are
joined exactly as declared, *not* lowerCamelCased the way protobuf's canonical JSON form renders
them, so they come back as they were written.

### Nullability

By default every non-repeated column is `NULLABLE`.
`ProtoSchemaOptions.builder().deriveRequiredColumns()` reads each field's presence instead:

```java
ProtoMessageSerializer.of(
        MyMessage.class,
        ProtoSchemaOptions.builder().deriveRequiredColumns().build());
```

| Field | Mode |
|---|---|
| `repeated`, including maps | `REPEATED` |
| plain proto3 singular scalar or enum | `REQUIRED` |
| proto3 `optional` | `NULLABLE` |
| `oneof` member | `NULLABLE` |
| singular message field | `NULLABLE` |
| proto2 `required` | `REQUIRED` |
| proto2 `optional` | `NULLABLE` |
| singular `JSON` or `GEOGRAPHY` column | `NULLABLE`, always |
| singular well-known type | `NULLABLE` — a message field, so it has presence; proto2 `required` still gives `REQUIRED` |

A plain proto3 scalar cannot say "unset" — an unset value is indistinguishable from the type
default — so `REQUIRED` is the faithful mode for it, and one the value path already satisfies: such
a field always reaches the column as `0`, `""` or the first enum value, never as NULL. proto2
`required` is listed separately because it *has* presence and is mandatory all the same, so a
presence test alone would map the one unambiguous case to `NULLABLE`.

A **proto3** map entry's `key` and `value` have implicit presence too, so a `map<string, int64>`
becomes `REPEATED STRUCT<key REQUIRED, value REQUIRED>` — which is what the Avro path derives for a
map key under its own [`deriveRequiredColumns()`](#avro-records), the two converging by design. The scope matters: a **message-valued** map follows the message rule instead, so
`map<string, Foo>` keeps a `NULLABLE` value, and in proto2 both entry fields have explicit presence
and stay `NULLABLE`.

Why `NULLABLE` is the default, and why there is no inverse switch, is in
[Column modes](#column-modes).

**A marked column is never `REQUIRED`.** A marked string without presence is left unset when empty
rather than written as `""` (see [JSON columns](#json-columns) and
[Geography columns](#geography-columns)), and "no presence" is precisely the condition that would
otherwise make the column `REQUIRED` — the two together would fail every record that legitimately
omits the field. Note this is the one place a presence-less field is *not* written: elsewhere it
always reaches the column as its type default.

Three things to weigh before enabling it:

- Only the derived schema changes. Values are converted identically either way, and toggling the
  option changes protobuf field labels rather than the encoding of any value, so rows already
  serialized stay valid.
- A record that leaves a `REQUIRED`-derived field unset is a row-level failure routed to the
  configured `FailedRowHandler` (see [Error handling](#error-handling)). Reaching that needs a
  proto2 `required` field missing from a partially built message; every other `REQUIRED` column is
  one the value path always writes.
- **Turning it back off later is not symmetrical.** Simply removing the option leaves existing rows
  writable, since a presence-less field still carries its default. What bites is the *field* gaining
  presence — adding `optional` to it, or moving it into a `oneof`. The derived mode becomes `NULLABLE`
  and the row then legitimately omits the column, while the table still has it `REQUIRED`, so every
  such record is a row-level failure until the column is relaxed — which needs
  `allowFieldRelaxation`, off by default.
- **BigQuery cannot add a `REQUIRED` column to an existing table**, so a column derived this way is
  only ever created together with the table. See
  [Table auto-creation](#table-auto-creation), [Schema evolution](#schema-evolution) and
  [File loads](#file-loads) for what that means afterwards.

## JSON columns

The Storage Write API carries a `JSON` column as a string, so nothing in the *value* path
distinguishes it from a `STRING` column. What a JSON column needs is a **marker at schema-derivation
time**, so that the schema the connector derives — used for table auto-creation, the write stream
and load jobs — says `JSON` rather than `STRING` or `STRUCT`. `ProtoSchemaOptions` carries that
marker. Two field types can be marked:

| Source field | Written as | Note |
|---|---|---|
| message (not a map) | canonical protobuf JSON | the message is *not* expanded into a `STRUCT` |
| string | the string itself, verbatim | the value is taken to be JSON text already |

There are two ways to designate the fields, and they are unioned — a field marked either way is a
JSON column.

**By dotted field path**, when the mapping is a property of the pipeline:

```java
ProtoSchemaOptions.builder()
        .jsonFieldPath("payload")
        .jsonFieldPath("event.details")
        .build();
```

Paths are the proto declared field names (snake_case, not the JSON names), joined from the root
message. A path matching no field is rejected when the schema is derived, so a typo fails the job
rather than silently producing the wrong column type.

**By protobuf field option**, when the mapping is a property of the schema — the better fit for a
large proto corpus, since it is one line of configuration regardless of how many messages and
fields are involved, it survives fields being renamed or moved deeper, and it stays correct when one
job writes several message types to different destinations:

```proto
// your existing annotations proto — nothing here has to change
extend google.protobuf.FieldOptions {
  optional bool json = 50000;
}

message Event {
  string payload = 1 [(json) = true];
}
```

When the generated extension class is on your classpath, pass it directly:

```java
ProtoSchemaOptions.builder().jsonFieldOption(MyAnnotations.json).build();
```

Otherwise — a schema registry hands you descriptors but not the annotations artifact — the extension
number alone works:

```java
ProtoSchemaOptions.builder().jsonFieldOptionNumber(50000).build();
```

Both are additive, like `jsonFieldPath`, so a job whose messages come from several sources can name
each annotation vocabulary it has to understand. Only one entry is kept per extension number:
registering the same number both ways keeps the one that carries a name — an unnamed entry would
match anything at that number and defeat the check the named one is there for — and if two
extensions claim one number, the last one registered wins.

Either way the option is found whether the descriptor knows it as a registered extension
(descriptors from generated code) or carries it as an unknown field (descriptors built from a
serialized `FileDescriptorSet` — protobuf-java does not resolve custom options against the
descriptor pool, not even for a declared dependency). An existing private extension number can
therefore be adopted as-is: no change to the protobuf sources, and no annotations proto to publish
or register.

**Prefer the extension over the bare number.** Protobuf's private extension range has no registry,
so an unrelated annotation can occupy the same number — and a job that writes several message types
is exactly where protos from different sources meet. The extension supplies the option's full name
as well, so a declaration found under a different name is treated as an unrelated option and the
field is left alone. It also makes the compiler check that the option really is a `bool`.

How much the connector can verify depends on what reaches it, in three steps:

| What is available | Name checked | Type checked |
|---|---|---|
| The generated extension class (option is a resolved extension) | yes | exactly, from the descriptor |
| The annotations proto among the descriptor's transitive dependencies | yes | exactly, from the declaration |
| Neither — the number is all there is | no | from the wire encoding only |

The name rules out a declaration that is *not* yours. It cannot arbitrate between two rival
declarations that are both in the pool: an unresolved option records only its number, so nothing
says which of them it was written against. Passing the generated extension is the only form where
the value itself carries that identity.

The third row is a real case: a `FileDescriptorSet` assembled without the annotations import leaves
nothing to identify the option but its bytes. There the connector requires the encoding of a
singular `bool` — one varint of `0` or `1` — so a string, a repeated, or an integer option outside
that range is rejected with *"is not encoded as a singular bool"* rather than silently marking a
column. An integer option that happens to hold `0` or `1` is indistinguishable from a `bool` and is
accepted; that is why passing the extension, or shipping the annotations proto with the schema, is
worth doing.

Three consequences worth knowing:

- **A field option number that matches nothing is not an error**, unlike a path — a message
  legitimately need not have JSON columns, and the same configuration is meant to serve many message
  types. A mistyped number therefore yields `STRING`/`STRUCT` columns silently, and under
  `CreateDisposition.CREATE_IF_NEEDED` that mistake becomes durable in the auto-created table.
  Check the derived schema with `serializer.getTableSchema(destination)` when adopting a number.
- **JSON-mapped strings are not validated by the connector.** Parsing every record to pre-empt a
  malformed value would defeat the point of a passthrough, so an invalid JSON string is rejected by
  BigQuery as a row-level error and routed through the configured `FailedRowHandler`
  (see [Error handling](#error-handling)).
- **An unset plain proto3 string leaves the column NULL**, rather than writing `""`. A plain proto3
  scalar has no presence, so an unset value reaches the sink as the empty string — which is not
  valid JSON, and would fail every record that legitimately omits the field. This applies only to
  fields *without* presence: where the proto can say "unset" (`optional string`, or proto2), an
  explicit `""` is your own statement and is passed through as-is. Repeated elements are likewise
  explicit and passed through. This is also why a JSON column is never `REQUIRED` under
  [`deriveRequiredColumns()`](#nullability) — the condition that leaves the value unset is the
  same one that would make the column mandatory.

Marking a field that is neither a message nor a string — including a proto map, whose BigQuery shape
is `REPEATED STRUCT<key, value>` — is rejected when the schema is derived, through either mechanism.

## Geography columns

The Storage Write API carries a `GEOGRAPHY` column as a string too, so it needs the same
**schema-derivation marker** a [JSON column](#json-columns) does, and for the same reason: nothing in
a protobuf descriptor or an Avro schema says "this string is a geometry", and BigQuery's own
documentation is explicit that schema auto-detection loads WKT as `STRING`.

**By dotted field path**, on both derived serializers, under the same name:

```java
ProtoSchemaOptions.builder().geographyFieldPath("site.boundary").build();
AvroSchemaOptions.builder().geographyFieldPath("site.boundary").build();
```

**By protobuf field option**, when the mapping is a property of the schema rather than of the
pipeline — the same trade-off as for [JSON columns](#json-columns), and the same mechanism, so a
`bool` extension of `google.protobuf.FieldOptions` marks the fields wherever they appear:

```proto
// your existing annotations proto — nothing here has to change
extend google.protobuf.FieldOptions {
  optional bool geography = 50006;
}

message Site {
  string boundary = 1 [(geography) = true];
}
```

```java
ProtoSchemaOptions.builder().geographyFieldOption(MyAnnotations.geography).build();
// or, when only the number is available:
ProtoSchemaOptions.builder().geographyFieldOptionNumber(50006).build();
```

Everything configured is unioned, so a field selected any of those ways is a `GEOGRAPHY` column. As
with JSON, a field option **number matching no field is deliberately not an error** — one
configuration is meant to serve every message type a job writes — so a mistyped number yields
`STRING` columns silently, and under `CreateDisposition.CREATE_IF_NEEDED` that mistake becomes
durable in the auto-created table. Check the derived schema with
`serializer.getTableSchema(destination)` when adopting a number. `AvroSchemaOptions` has no
annotation-driven form, because Avro has no field-option mechanism to key off.

The value must already be one of the text forms BigQuery accepts for a geography — WKT
(`POINT(1 2)`), hex-encoded WKB, or GeoJSON — and reaches the column verbatim. Everything the JSON
marker says about that passthrough holds unchanged here: the connector does **not** validate the
value, so malformed geometry is a BigQuery row-level error routed to the configured
`FailedRowHandler` (see [Error handling](#error-handling)); an unset presence-less proto string is
left `NULL` rather than written as `""`, which is not a valid geometry either; and a marked column is
therefore never `REQUIRED` under [`deriveRequiredColumns()`](#nullability). A repeated marked field
becomes `REPEATED GEOGRAPHY`.

Two differences from the JSON marker, both deliberate:

- **Strings only.** `jsonFieldPath` also accepts a message and writes its canonical protobuf JSON;
  no protobuf message means a geography to BigQuery, so there would be nothing to write. Marking a
  message, a map, or any non-string field is rejected when the schema is derived — including by
  annotation, where you do not choose which fields are selected, so one annotation landing on a
  message field fails the job rather than skipping that field.
- **A field marked both ways is an error**, not a precedence question — a column has one type. One
  extension number registered as *both* a JSON and a geography option is rejected by `build()`, since
  it is broken for every message rather than for some. Every other collision — an option against a
  path, or two different numbers meeting on one field — needs a descriptor and so is rejected when the
  schema is derived. It also covers marking a `Struct`, `Value` or `ListValue` field, which is
  [automatically a `JSON` column](#well-known-types): the configured marking wins, and is then
  rejected for not being a
  string, rather than silently falling back.

Changing an existing `STRING` column to `GEOGRAPHY` by adding the marker to a running pipeline is a
**breaking schema change**. Schema evolution only relaxes modes and adds columns, so the union is
rejected rather than rows being corrupted — see [Schema evolution](#schema-evolution).

`FILE_LOADS` carries a `GEOGRAPHY` column as well: staged Avro files hold the text in a `string`
field and the load job is given an explicit destination schema that types it. That pairing is
verified end to end against real BigQuery by `BigQueryFileLoadsITCase`, BigQuery's documentation
describing WKT loading for CSV and JSON but not for Avro.

`INTERVAL` and `RANGE` stay outside what the two *derived* serializers can produce, **considered and
declined** rather than overlooked. (The [JSON serializer](#json-records) derives nothing, so what its
supplied schema may contain is a separate question — `RANGE` it rejects outright.)

- **`INTERVAL`.** Avro's `duration` logical type is a `fixed(12)` of months, days and milliseconds,
  while BigQuery's `INTERVAL` is a year-month part plus a day-time part at microsecond precision.
  They are not the same value space, so either direction is a lossy re-encode. `TableSchemaToAvroConverter`
  rejects `INTERVAL` outright, so deriving one would also break the FILE_LOADS round trip
  `AvroSchemaRoundTripTest` pins — which is why `google.protobuf.Duration` maps to `INT64`
  microseconds rather than to `INTERVAL`.
- **`RANGE`.** Neither Avro nor protobuf has an equivalent, so supporting it would mean reading a
  two-field record as a range by convention. `TableSchemaToAvroConverter` rejects it too.

## Avro records

`AvroRecordSerializer` writes Avro records without a protobuf definition in sight. It takes
one Avro writer schema for the whole job — as a `Schema` or as its JSON text, for jobs that read it
from a schema registry or a configuration option — derives the BigQuery schema from it, and
rewrites each record into the protobuf row the Storage Write API accepts.

```java
Sink<GenericRecord> sink =
        BigQuerySink.<GenericRecord>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(AvroRecordSerializer.of(schema))
                .build();
```

Records are accepted as `IndexedRecord`, so generated `SpecificRecord` classes work too. Values are
read in whichever representation the record carries: a `GenericRecord` decoded without conversions
holds the raw base value (`long`, `int`, `ByteBuffer`), while a `SpecificRecord` generated with
Avro's logical-type conversions holds `Instant`, `LocalDate`, `LocalTime`, `LocalDateTime`,
`BigDecimal` or `UUID`. Both are accepted for every logical type.

**Type mapping.**

| Avro | BigQuery |
|---|---|
| `string`, `string` + `uuid` | `STRING` |
| `enum` | `STRING` (the symbol name) |
| `bytes`, `fixed` | `BYTES` |
| `int`, `long` | `INT64` |
| `float`, `double` | `DOUBLE` |
| `boolean` | `BOOL` |
| `int` + `date` | `DATE` |
| `int` + `time-millis`, `long` + `time-micros` | `TIME` |
| `long` + `timestamp-millis`, `long` + `timestamp-micros` | `TIMESTAMP` (microseconds) |
| `long` + `local-timestamp-millis`, `long` + `local-timestamp-micros` | `DATETIME` |
| `bytes`/`fixed` + `decimal(p, s)` | `NUMERIC` when `s ≤ 9` and `p - s ≤ 29`, else `BIGNUMERIC` (`s ≤ 38`, `p - s ≤ 38`); the precision and scale are carried onto the column |
| `record` | `STRUCT`, recursively |
| `map<string, V>` | `REPEATED STRUCT<key, value>` — the shape a proto map already gets |
| `array<T>` | mode `REPEATED`, whether or not a union around it admitted null — as does a map |
| anything else | mode `NULLABLE`; `REQUIRED` under `deriveRequiredColumns()` when not a `["null", T]` union |

**Nullability.** Every non-repeated column is `NULLABLE` by default, as on every other serializer
(see [Column modes](#column-modes) for why).
`AvroSchemaOptions.builder().deriveRequiredColumns()` reads the Avro schema instead, deriving
`REQUIRED` for any field that is not a `["null", T]` union:

```java
AvroRecordSerializer.of(
        schema, AvroSchemaOptions.builder().deriveRequiredColumns().build());
```

This changes the derived schema — the one used for table auto-creation, for the write stream and for
load jobs. `REPEATED` fields are unaffected, since a BigQuery `REPEATED` column cannot be `NULLABLE`;
nested record fields and map entry columns are covered along with the rest, so a map key becomes
`REQUIRED` too — the same shape the protobuf path derives for a proto3 map key under the same option.

The one thing it changes in the value path is what happens to a record that omits a field the Avro
schema declares mandatory: by default the column is left unset, and under `deriveRequiredColumns()`
that record is a row-level failure routed to the configured `FailedRowHandler`. Records that do carry
the value convert identically either way.

It also changes what staged FILE_LOADS files look like, since `NULLABLE` becomes `["null", T]` on the
way back out: a value costs a union branch index and an unset field is written as an explicit Avro
null. Self-consistent — both staging converters read the same derived schema — but worth knowing when
comparing file sizes across the change.

**Writing into a table that already has `REQUIRED` columns.** Tables an Avro pipeline auto-created
before this default changed have `REQUIRED` scalars, and the derived schema no longer agrees with
them. The disagreement is tolerated silently: the schema union only ever *relaxes*, and relaxing needs
`allowFieldRelaxation`, which is off by default. Rows that carry every value are unaffected. A row
that omits one is not, and where it surfaces depends on the write method:

| Write method | A row omitting a column the table has as `REQUIRED` |
|---|---|
| `STORAGE_API_*` | BigQuery rejects that row; it is routed to the `FailedRowHandler` per policy |
| `FILE_LOADS` | the **load job** fails, taking every other row in the same commit with it — there is no row-level policy at load time |

So on a pre-existing table, either keep `deriveRequiredColumns()` on — which reproduces the old
schema and moves the rejection back to the client, where the message names the field — or relax the
table's columns once with `schemaUpdateOptions(SchemaUpdateOptions.builder().allowFieldRelaxation()
.build())`.

**JSON columns.** `AvroSchemaOptions.builder().jsonFieldPath("event.payload")` derives a `string`
field at that dotted path as a [`JSON` column](#json-columns) instead of `STRING`. As on the
protobuf path the value is passed through verbatim and is *not* validated — malformed JSON is a
BigQuery row-level error, routed to the configured `FailedRowHandler`. A path matching no field, or
matching a field that is not a `string`, is rejected when the schema is derived. A marker is needed
at all because Avro has no standard JSON logical type to infer the column from; there is no
annotation-driven equivalent of `ProtoSchemaOptions`' field options for a different reason, that Avro
has no field-option mechanism to key off.

**Geography columns.** `AvroSchemaOptions.builder().geographyFieldPath("site.boundary")` does the
same for a [`GEOGRAPHY` column](#geography-columns), on the same terms — string fields only, the
value passed through unvalidated, never `REQUIRED`. As with JSON columns there is no
annotation-driven equivalent of `ProtoSchemaOptions`' field options: Avro has no field-option
mechanism to key off, which is the reason for both. A path claimed by both markers is rejected.

**Rejected at job start**, because writing something plausible instead would be worse than failing
early: unions with more than one non-null branch (BigQuery has no union type), a bare `null` field,
arrays of nullable elements and arrays of arrays or maps (a `REPEATED` column holds no NULLs and
does not nest), recursive record types, sibling fields whose names differ only by case (the Storage
API lowercases descriptor field names), a decimal wider than `BIGNUMERIC`, and the logical types
BigQuery cannot store without losing information — `timestamp-nanos`, `local-timestamp-nanos`,
`duration`, `big-decimal`, and `uuid` on a `fixed`. A logical type Avro itself rejects as invalid is
dropped by its parser, so the field lands on its base type.

"Job start" is literal: the schema is derived when `AvroRecordSerializer.of(...)` is called, so a
mapping problem is thrown where the pipeline is built. Deferring it to the first record would put it
inside the sink's per-record failure handling, where a log-and-drop or DLQ policy would swallow one
misconfiguration once per record instead of failing the job.

**Row-level failures**, routed to the `FailedRowHandler` (see
[Error handling](#error-handling)): a missing value for a `REQUIRED` column — which for a
derived schema means only under `deriveRequiredColumns()`, since otherwise no derived column is
mandatory (see [Column modes](#column-modes)) — a null element in a
repeated field, a decimal too wide or too precise for its column, and a value whose Java type does
not match the field. A `BigDecimal` carrying more fractional digits than the column declares is one
of these rather than being rounded silently — the byte form of the same field cannot express it
either.

A null array and an empty one are indistinguishable once written: `["null", array<T>]` derives a
`REPEATED` column, and BigQuery has no NULL array to map the difference onto.

**Cost.** Conversion is one pass over each record, reading Avro values and writing protobuf ones.
Note that a protobuf stream is not free either — `ProtoMessageSerializer` also rebuilds every record
into the row descriptor's shape, since the Storage Write API wants BigQuery's column layout rather
than your message's — but it starts from protobuf accessors rather than Avro ones and has no logical
types to convert.

## JSON records

`JsonDocumentSerializer` writes records that are JSON documents, as `String`s. JSON carries no schema of its
own, so unlike the protobuf and Avro serializers this one cannot derive the destination schema — it
is supplied, in whichever form the surrounding code already holds:

```java
Sink<String> sink =
        BigQuerySink.<String>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(JsonDocumentSerializer.of(schema))
                .build();
```

`of(...)` takes either the Storage API `TableSchema` the sink uses internally or the REST client's
`Schema` — the type a table read back through `BigQuery.getTable(...)` gives you. That schema is the
source of truth for table auto-creation, the write stream and load jobs, and it decides every column
type, [`JSON` columns](#json-columns) included: there is no marker option here, because the schema
already says so. A column type the Storage descriptor conversion cannot express — `RANGE` today — is
rejected when the serializer is created, so it fails where the pipeline is built rather than on the
first record.

**Column modes work the same way, which is why there is no nullability option here either.** The
other two serializers need one because they derive modes from a source schema that may not mean what
it appears to (see [Column modes](#column-modes)); here you wrote the schema, so a `REQUIRED` column
in it is your own statement and is passed through as-is — including when you fetched it from the
destination table, which is the point of the `Schema` overload. A column with no mode set is
`NULLABLE`, so the unconstrained default still holds for anything you did not decide. The
consequence to know: a document omitting a `REQUIRED` column is a row-level failure, reported by the
conversion library and routed through the configured `FailedRowHandler`.

Conversion is the Storage Write API client's own `JsonToProtoMessage`, the same one
`JsonStreamWriter` uses. What each column type accepts:

| Column | JSON value |
|---|---|
| `STRING` | a string; a number or boolean is stringified |
| `INT64`, `DOUBLE`, `BOOL` | the matching JSON type, or its string form |
| `NUMERIC`, `BIGNUMERIC` | a string (exact) or a number |
| `TIMESTAMP` | an ISO-8601 string, **or a number read as epoch microseconds** |
| `DATE` | a `yyyy-MM-dd` string, or a number read as days since the epoch |
| `DATETIME`, `TIME` | a string |
| `JSON` | the JSON **text**, as a string — not a nested object |
| `GEOGRAPHY` | a string in WKT, hex-encoded WKB or GeoJSON |
| `BYTES` | a JSON array of byte values — not base64 |
| `STRUCT` | an object |
| `REPEATED` | an array |

Three of those rows are traps worth stating plainly, because each is accepted rather than rejected:

- **A bare number in a `TIMESTAMP` column is epoch microseconds.** Epoch seconds and epoch
  milliseconds — the two encodings a JSON document usually carries — are therefore stored as some
  other instant, with no error anywhere. Send an ISO-8601 string, or convert before the sink.
- **A `JSON` column takes the JSON text as a string**, so `{"payload":{"k":1}}` fails and
  `{"payload":"{\"k\":1}"}` is what to write.
- **A `BYTES` column takes a JSON array of byte values**, such as `[104,105]` — not the base64 string
  that protobuf's own canonical JSON mapping uses. A base64 document fails per record until it is
  pre-decoded.

Keys are matched to columns **case-insensitively**, so a key whose spelling differs from the column's
is not an unknown field — and two keys differing only by case are not two fields either: one value
wins, and which one is undefined.

**Unknown fields.** A document carrying a field the table has no column for fails the record, on the
grounds that discarding data should be asked for. Ask for it when the source is a document stream
nobody controls — a topic whose producers add fields ahead of the table being the usual case:

```java
JsonDocumentSerializer.of(schema, JsonDocumentSerializerOptions.builder().ignoreUnknownFields().build());
```

A record whose fields are *all* dropped as unknown produces a row with every column NULL rather than
a failure — worth knowing if the destination has no `REQUIRED` column to catch it.

**Row-level failures**, routed to the `FailedRowHandler` (see
[Error handling](#error-handling)): text that is not a JSON object, a record carrying more than one
JSON value, an empty object, a value that will not convert to its column type, a missing `REQUIRED`
column, and an unknown field unless the option above is set. The client library reports the
conversion failures as unchecked exceptions; the serializer converts them so the sink can route the
row rather than fail the job. The `FailedRow` carries the diagnostic but not the document — the
writer is stateless, as on the other two paths.

The multi-value check is there because parsing stops at the end of the first JSON value: without it
a mis-split newline-delimited record would silently become one row and drop the rest.

**The schema is fixed for the life of the job.** This serializer reports no schema fingerprint, so
the sink never refreshes the stream from it. A table that has to follow its producers means
rebuilding the serializer and restarting.

**Cost.** Conversion is a JSON parse on top of the per-record pass the Avro serializer already
costs. Where the input format is yours to choose and throughput matters, a native protobuf record
avoids both.

## Table auto-creation

Under the default `CreateDisposition.CREATE_IF_NEEDED`, an append failing with `NOT_FOUND` is
recovered on the task thread: the destination table is created through the BigQuery REST API
(schema from the serializer's `getTableSchema`; partitioning/clustering from
`tableCreateOptions(...)` or a per-destination `tableCreateOptionsProvider(...)`), the
destination's stream writer is rebuilt, and the failed batch is re-appended with backoff while
table metadata propagates to the Storage Write API backend. Creation is idempotent across
parallel subtasks (HTTP 409 is treated as success); the credentials need
`bigquery.tables.create` on the destination dataset. Options apply only at creation time —
existing tables are never modified.

Creation is also the **only** moment a `REQUIRED` column can appear: BigQuery cannot add one to an
existing table. So the serializer's [column modes](#column-modes) are decided here, durably, and
relaxing a column afterwards is a schema update rather than an edit.

With `CreateDisposition.CREATE_NEVER`, writing to a missing table fails the job immediately.

## Schema evolution

Schema changes are handled without a job restart. Reactive handling is always on:

- **Server-pushed schema updates** — when an append response reports `updated_schema` (the
  table's schema changed, e.g. through DDL), the destination's stream writer is rebuilt with a
  fresh serializer descriptor. A raw Storage Write API `StreamWriter` never refreshes its schema
  by itself, also not under connection-pool multiplexing.
- **Serializer schema changes** — a serializer with an evolving schema overrides
  `getSchemaFingerprint(destination)` to return a cheap token that changes with its schema. The
  writer compares it per record and refreshes the destination's stream *before* appending rows
  serialized under the changed schema, so the first append after an evolution does not have to
  fail.
- **Stale-stream-writer failures** (`STREAM_FINALIZED`, `STREAM_NOT_FOUND`,
  `INVALID_STREAM_STATE`, writer-closed, the SDK's callback-wait watchdog timeout) are repaired
  by rebuilding the writer and re-appending within the transient retry budget instead of failing
  the job.

**Connector-driven table schema updates** are opt-in via `schemaUpdateOptions(...)`:

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .schemaUpdateOptions(
                        SchemaUpdateOptions.builder()
                                .allowNewFields()
                                .allowFieldRelaxation()
                                .build())
                .build();
```

When enabled and the serializer's schema evolves past the destination table's (detected through
the fingerprint pre-check or a `SCHEMA_MISMATCH_EXTRA_FIELDS` append failure), the sink updates
the table itself: fresh read of the live schema, union with the serializer schema, and an
etag-conditioned `tables.update`. The union is strictly widening — existing fields are never
dropped, reordered or re-typed (a type change fails the job); new fields are appended at the end
— including inside `STRUCT` columns: updates go through the REST API, which unlike SQL
`ALTER TABLE` supports adding nested fields — and forced `NULLABLE` (BigQuery cannot add
`REQUIRED` columns); `REQUIRED`→`NULLABLE` relaxation happens only under `allowFieldRelaxation`
(any mode not explicitly `REQUIRED` counts as nullable); `REPEATED` is never changed. Concurrent updates from
parallel subtasks need no coordination: updates are additive and idempotent, lost races (etag
mismatch, HTTP 409/412, `rateLimitExceeded` — the per-table quota is about five metadata updates
per ten seconds) re-read and re-union with jitter, and unions of concurrent unions converge.
The credentials need `bigquery.tables.get` and `bigquery.tables.update`.

Caveats:

- Rows already handed to the sink are retained as serialized bytes and are never re-encoded, so
  serializer schema evolution must be wire-compatible: append new fields at the end (including
  inside nested types) and relax `REQUIRED`→`NULLABLE`; never remove, reorder or re-type fields.
  Turning a nullability option on or off is wire-compatible in both directions — it changes
  protobuf field labels, not the encoding of any value.
- A column the serializer newly derives as `REQUIRED` is added to an existing table as `NULLABLE`,
  after which the derived schema and the table disagree about that column forever. That is
  harmless: the union only ever relaxes, so it reports no change and never tries to tighten. The
  reverse does bite — a table created with `REQUIRED` columns whose schema later relaxes needs
  `allowFieldRelaxation`, which is off by default.
- A schema update typically propagates to the Storage Write API backend in well under a minute —
  measured against the real service, six instrumented probe runs each had the widened rows
  accepted ~35 s after the instant REST update. The writer keeps re-appending affected batches
  for up to ~15 minutes (flat 30 s waits, ±25 % jitter, 30 attempts) — a schema repair can
  therefore block a checkpoint longer than Flink's default checkpoint timeout of 10 minutes,
  which may need raising on jobs that enable schema updates.
- One measured run sat far outside that envelope (a rare tail — one of seven runs to date):
  appends carrying the new column hung ~35 and ~79 minutes before resolving, ~2 h end to end,
  and the hung append that was finally reported as failed had been applied server-side anyway,
  landing its row twice (permitted by at-least-once; queries asserting exact multisets after a
  schema change should de-duplicate). In a checkpointed streaming job the checkpoint timeout is
  what bounds this tail: the hung repair blocks the checkpoint, the timeout fails the task, and
  failover rebuilds fresh stream writers — so its practical cost is a job restart, not an
  indefinite hang. The connector deliberately adds no second per-append timeout below that (it
  would race the SDK's own 5-minute callback watchdog and could tear down slow-but-progressing
  appends into duplicates); the tail's record and open hypotheses are in
  [#174]({{< param BookRepo >}}/issues/174), closed as wait-and-see until it reproduces.
- Schema unionization stays opt-in because BigQuery columns can never be dropped again: one
  malformed record shipping an unexpected field could otherwise poison a table permanently. With
  updates disabled, schema-mismatch appends fail the job (with a hint), and externally driven
  schema changes are still picked up reactively.

## Delivery guarantees and state

The `STORAGE_API_AT_LEAST_ONCE` writer is **stateless by design**: rows are appended
asynchronously as batches fill, and on **every checkpoint** Flink invokes the writer's `flush()`
(before the barrier is emitted), which appends all pending batches and awaits every in-flight
append with direct response inspection. A successful checkpoint therefore means *all* records up
to the barrier are acknowledged by BigQuery, and the writer stores nothing in Flink state —
**discarding operator state (savepoint-less redeploys, state resets) can never lose
sink-buffered data**. This is a deliberate decision: the alternative `AsyncSinkWriter`-style
model persists unflushed buffers into writer state instead of flushing at the barrier, which
silently loses those buffers whenever state is dropped.

Checkpointing must be enabled for the at-least-once guarantee in streaming jobs: without it,
Flink never calls `flush()` mid-stream, so sub-threshold buffers are lost on failure. For jobs
that must run without checkpointing, `DefaultStreamOptions`' `flushInterval` (see
[Tuning](#tuning)) registers a periodic processing-time flush that bounds this window — a
mitigation only, not a replacement for the guarantee. Batch execution is covered
by the end-of-input flush. End-to-end loss behavior additionally depends on the source's own
state handling.

**Discarded operator state.** The two Storage Write API methods differ in *when* rows become
durable relative to when the source advances its position, and that difference decides what a
state-less restart costs:

| | Rows become visible in BigQuery | Source commits offsets / acks |
|---|---|---|
| `STORAGE_API_AT_LEAST_ONCE` | Before the checkpoint barrier (in `flush()`) | After the checkpoint completes |
| `STORAGE_API_EXACTLY_ONCE` | After the checkpoint completes (`FlushRows` in the committer) | After the checkpoint completes |

At-least-once keeps the sink strictly ahead of the source: whatever the source has acked is
already visible in BigQuery, so discarding state can duplicate rows but cannot lose them.
Exactly-once puts both side effects in the same phase with no atomicity between them, so
discarding state opens a loss window of at most one checkpoint — rows appended but not yet
flushed, and committables checkpointed but not yet committed, stay invisible forever while the
source may already have acked past them. This is inherent to two-phase commit (a Kafka
exactly-once producer behaves the same way), not specific to this connector.

**The sink cannot detect this situation** — a writer restored with no state is indistinguishable
from a brand-new job — so the guard belongs in deployment tooling. Redeploy through savepoints
(`stop-with-savepoint`, then `flink run -s`); with the Flink Kubernetes Operator use
`upgradeMode: savepoint` (or `last-state`) and never `stateless`. When state has to be dropped,
rewind the source behind the last completed checkpoint so a potential loss becomes a duplicate,
and make duplicates harmless downstream (an idempotent key plus `MERGE` or
`QUALIFY ROW_NUMBER()`).

Neither method is uniformly safer — their loss paths are disjoint:

| Loss path | `STORAGE_API_AT_LEAST_ONCE` | `STORAGE_API_EXACTLY_ONCE` |
|---|---|---|
| Discarded operator state | none (duplicates only) | up to one checkpoint |
| Checkpointing disabled | buffered rows lost; window bounded by `flushInterval` when set | impossible — rejected at graph construction |
| Committable outliving its write stream | none (holds no committer state) | possible — see [Exactly-once](#exactly-once-buffered-streams) |
| `FailedRowHandler` drop policies | by configuration | by configuration |

## Exactly-once (buffered streams)

`WriteMethod.STORAGE_API_EXACTLY_ONCE` writes through application-created Storage Write API
**BUFFERED** streams committed with a two-phase commit protocol on Flink checkpoints: writers
append rows at explicit offsets (invisible while buffered), and when a checkpoint completes the
committer makes exactly that checkpoint's rows visible with `FlushRows`.

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                .build();

env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
env.enableCheckpointing(60_000); // EXACTLY_ONCE mode (the default)
```

Method-specific settings live in `BufferedStreamOptions` (required for this write method,
rejected for the others; all knobs are defaulted): `maxAppendRequestBytes` (512 KiB default) and
the connector-driven recovery schedule (`recoveryInitialBackoff` 500 ms, `recoveryMaxBackoff`
10 s, `recoveryMaxAttempts` 10) governing stream creation, transient re-appends and the restore
probe.

**Stream lifecycle.** Each writer subtask owns **one buffered stream, created lazily on its first
append and reused across checkpoints** — per GCP guidance, frequent `CreateWriteStream` churn
(e.g. a new stream per checkpoint × parallelism) is not intended usage of the API; a clean run
creates exactly one stream per subtask for its whole lifetime. The stream name and next append
offset are Flink writer state. The SDK connection pool is default-stream-only, so each stream
gets a dedicated `StreamWriter` connection; backpressure comes from the SDK's bounded in-flight
window. `prepareCommit()` emits one committable per subtask naming the offset the completed
checkpoint may flush up to; `FlushRows` is naturally idempotent (re-flushing an already-flushed
offset answers `ALREADY_EXISTS` = success), so re-commits after restarts need no deterministic-id
machinery, no checkpoint stamping, and no global committer routing — the committer runs at the
sink's parallelism.

**Restore.** A restored writer probes its stream with the first replayed batch at the restored
offset, synchronously. Success reuses the stream; `OFFSET_ALREADY_EXISTS` (the pre-crash attempt
appended past the restored offset), `OFFSET_OUT_OF_RANGE`, a finalized/unknown stream, or a
failure to reopen it abandon the stream and a fresh one starts at offset zero. This cannot lose
or duplicate data: rows appended past the restored offset were never named by any committable,
so nothing ever flushes them. Abandoned streams (and streams of closing writers) are
deliberately **never finalized** — BigQuery rejects `FlushRows` on a finalized stream (verified
against the real service, and the reason batch commits happen after writer close), so finalizing
could permanently break a restored-but-uncommitted committable; an open stream's unflushed tail
stays invisible either way. Commit failures follow the FILE_LOADS model: the committer throws,
the job restarts, and the framework re-commits the restored committables idempotently.

**Stream lifetime.** BigQuery gives a buffered write stream a default TTL of
[seven days with no traffic on the stream](https://docs.cloud.google.com/bigquery/docs/write-api-streaming),
and streams cannot be deleted explicitly — they age out on that TTL, so the streams this write
method abandons need no cleanup. A running writer's own appends keep its stream alive, so the
TTL matters across downtime: a job stopped for longer than the TTL and then restored with
committables still pending references a stream that may no longer exist, and those flushes may
fail permanently. The only escape from a permanently failing commit is to start without state,
which drops those rows — the same class of hazard as an expired FILE_LOADS staging object. A
subtask that received rows and then went idle past the TTL while the job is still running hits
the same expiry; a missing stream is terminal mid-run, so the job restarts and the restore probe
starts a fresh stream. **What exactly happens at expiry — whether the flush fails, with which
error, whether the seven days is configurable, and whether unflushed buffered rows are billed as
storage — is not stated in the documentation and has not been verified here.**

**Execution modes.** The mode must be explicit (`AUTOMATIC` is rejected at graph construction —
were it to resolve to streaming without checkpointing, buffered rows would never become visible).
Streaming requires checkpointing with `CheckpointingMode.EXACTLY_ONCE` and
checkpoints-after-tasks-finish enabled (the final batch of a bounded job rides the post-finish
checkpoint); a slow flush delays the next checkpoint — that is the backpressure, and `commit()`
returning means the rows are visible. `BATCH` execution is supported: the single end-of-input
committable is committed when the job completes. There is no checkpoint-cadence quota guard:
`FlushRows` once per subtask per checkpoint is far below its quota (unlike FILE_LOADS' per-table
daily load-job limit).

**Scope (v1).** One fixed `destination(...)` per sink — the builder rejects
`destinationResolver(...)` for this write method (dynamic destinations are a planned follow-up).
The table schema is pinned when the stream is created: **mid-stream schema evolution is not
supported** — no fingerprint refresh and no connector-driven schema updates, so the builder
rejects an enabled `schemaUpdateOptions(...)` rather than accepting a setting this write method
would silently ignore, and a schema mismatch fails the job with a hint (update the table out of
band and restart). Table auto-creation under `CREATE_IF_NEEDED` *is* supported: it runs at
stream-creation time — schema from the serializer, partitioning and clustering from
`tableCreateOptions(...)` — with retries while table metadata propagates, and `CREATE_NEVER`
fails immediately.

**Error handling.** Serialization failures and oversized rows go to the `FailedRowHandler` before
any stream exists, as in the at-least-once method. Server-side **row-level rejections are also
routed to the handler** — with more machinery than the at-least-once path needs: an append
request is rejected atomically (the offset never advances), so the writer routes the failing rows
to the handler and replays the surviving rows plus every batch appended behind the rejected one
at recomputed offsets. Transient failures are re-appended at their original offset
(`OFFSET_ALREADY_EXISTS` then means the original landed); a client-side dead `StreamWriter` (the
SDK's closed-writer error, or its callback-wait watchdog timing out a sent append after 5
minutes without a response) is reopened on the same stream before the resend. Stream-state
errors mid-run
(`STREAM_FINALIZED`, `STREAM_NOT_FOUND`, `INVALID_STREAM_STATE`) are terminal — the restart +
restore protocol is the repair. Consistency guards (an acknowledged append behind a rejected one,
an offset-echo mismatch, `OFFSET_ALREADY_EXISTS` during an offset-shifting replay) fail the job
rather than risk silent divergence.

## File loads

`WriteMethod.FILE_LOADS` writes each destination table's rows to Avro files on Cloud Storage and
loads them with BigQuery load jobs — free of streaming-insert cost, always exactly-once. Batch
execution loads everything at end of input; streaming execution loads each checkpoint's files
(the checkpoint is the trigger, like Beam's streaming FILE_LOADS `triggeringFrequency` model):

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .destinationResolver(
                        (e, ctx) -> TableDestination.of("my-project", "my_dataset", e.tableName()))
                .serializer(new MyEventProtoSerializer())
                .fileLoadsOptions(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://my-staging-bucket/flink-loads")
                                .build())
                .build();

env.setRuntimeMode(RuntimeExecutionMode.BATCH);
// or: env.setRuntimeMode(RuntimeExecutionMode.STREAMING) with checkpointing enabled.
```

FILE_LOADS-only settings live in `FileLoadsOptions` (required for this write method, rejected for
the others): `stagingPath` (required), `writeDisposition` (`WRITE_APPEND` default,
`WRITE_TRUNCATE` for atomic batch reloads, `WRITE_EMPTY`), `tempDataset`, and the streaming guard
`minCheckpointInterval` (all described below).

**Topology.** Parallel writers encode records (serializer proto bytes → Avro `GenericRecord`) and
stream them straight to per-destination GCS objects — rows never accumulate on the heap, so memory
use is ~5 MiB per open destination regardless of data volume; in streaming the inter-checkpoint
buffer *is* GCS. Files roll at 1.5 GiB. The pre-commit topology routes every subtask's
committables to a single committer subtask (in streaming through a stage that stamps each
committable with its checkpoint id), and that committer — the actual commit — groups the staged
files by destination table and runs **one load job per table** (all jobs submitted first, then
awaited — BigQuery runs them concurrently server-side): once at end of input in batch, once per
completed checkpoint in streaming. Before its first load of a run, each destination is
**reconciled against the live table** through the REST API — a missing table is created (schema
from the serializer, partitioning/clustering from `tableCreateOptions(...)`; `CREATE_NEVER` fails
with a client-side error instead), and the schema the load jobs then carry explicitly
(`useAvroLogicalTypes`) is the live table's, unioned with the serializer's when
schema updates are enabled (under `WRITE_TRUNCATE` it is the serializer's as-is — the load
replaces the table schema wholesale; see below). One reconciliation per destination per run,
whatever the partition count; the credentials therefore need `bigquery.tables.get` (plus
`bigquery.tables.create` / `bigquery.tables.update` for what the configuration enables). Because
the table is created before the load rather than by it, a load failure can leave an empty table
behind — as a schema union applied before a failed load also persists, columns being permanent
either way. Loading in the committer (rather than a post-commit
topology, where the [#14]({{< param BookRepo >}}/issues/14) batch implementation originally ran it) is deliberate: committables ride
in Flink's committer state until their loads succeed, and the final batch of a streaming job is
committed during task shutdown's final-checkpoint wait — records emitted to a post-commit
topology at that point are not guaranteed to be processed before the job terminates.

**Execution modes.** The mode must be explicit: `AUTOMATIC` is rejected when the job graph is
built, because were it to resolve to streaming with checkpointing disabled, no trigger would ever
come and files would stage forever. Streaming additionally requires, also checked at graph
construction: checkpointing enabled (the checkpoint is the load trigger),
`WriteDisposition.WRITE_APPEND` (truncating/rejecting per checkpoint is meaningless), and a
checkpoint interval compatible with BigQuery's **1,500 load jobs per table per day** quota — each
checkpoint issues at least one load job per destination table:

| Checkpoint interval | Load jobs per table per day |
|---|---|
| 1 min | 1,440 — too close to the ceiling, not viable |
| 2 min | 720 |
| 5 min | 288 |

Intervals below `minCheckpointInterval` (default 2 minutes) are rejected; intervals below 5
minutes log a warning. Lowering `minCheckpointInterval(...)` is the explicit opt-in for
short-lived jobs whose daily load count stays safe (the integration tests do this). A runtime
warning also fires when observed checkpoint cadence stays under 2 minutes, catching interval
configuration the client-side guard cannot see. Streaming pipelines that need second-level
latency belong on the Storage Write API methods; checkpoint-triggered file loads trade minutes of
latency for free ingestion.

**Streaming operation.** Each completed checkpoint's committables are committed — loaded — by the
framework at that checkpoint's completion, in checkpoint order. Loads are synchronous: a slow
load delays the next checkpoint's completion, which is the backpressure mechanism (loads of a few
minutes of data typically finish in seconds to tens of seconds, well within the quota-mandated
2-5 minute intervals). Everything not yet loaded rides in Flink's committer state: on recovery
the committables are re-committed and the deterministic job ids re-attach to jobs a previous
attempt already created. A load-job failure fails the ongoing checkpoint (and the job), which
restarts from the last checkpoint with the staged files still in place. On stop-with-savepoint
without `--drain`, the final checkpoint's rows land when the savepoint is resumed.

**Exactly-once.** Load jobs reference exactly the file URIs emitted by the writers — never a
bucket prefix — so files from failed/restarted attempts (which use unique names: Flink job id,
subtask, attempt, random component) can never leak into a load. Job ids are deterministic hashes
of the destination and its sorted file list (streaming ids additionally carry a visible
`-c<checkpointId>` segment for attribution): a retry after a failure re-attaches to the
already-running/completed BigQuery job instead of loading twice. Committables carry the Flink job
id of the run that staged them, so even a restore under a *new* Flink job id (`flink run -s` on a
savepoint or retained checkpoint) reproduces the original job ids and re-attaches. Known residual
risk (shared with the Beam and Dataproc designs): if a failure destroys the persisted
committables *and* re-runs the writer stage after load jobs were already submitted, the retried
run produces new file names — and thus new job ids — while the first run's jobs keep running
server-side, which can duplicate rows under `WRITE_APPEND`.

**Per-load-job limits.** In batch, a table whose staged files exceed one load job's limits
(10,000 source URIs / 11 TiB) is loaded partition-wise into temporary tables (`WRITE_TRUNCATE`,
so retries are idempotent) and appended to the final table with one atomic copy job. Temporary
tables go to the destination's dataset by default, or to `tempDataset(...)` — a dedicated dataset
with a default table expiration is recommended so tables orphaned by hard failures are
garbage-collected. Copy jobs support no schema update options and require matching schemas, so
the temporary tables are loaded with the reconciled schema — the same one every load of the run
carries. In streaming there is no temporary-table path: an
oversized checkpoint × table submits multiple direct append jobs (deterministic per-partition
ids keep exactly-once; only that checkpoint's atomic visibility is lost).

**Schema evolution.** The `schemaUpdateOptions(...)` flags drive the pre-load reconciliation:
when they allow it, the live schema is unioned with the serializer's and the table updated via
the REST API before any load — the same union rules as [Schema evolution](#schema-evolution) on
the Storage Write API path (new columns arrive `NULLABLE`, relaxation needs
`allowFieldRelaxation()`, retried etag-conditioned updates). The load jobs then carry the already
reconciled schema; on `WRITE_APPEND` jobs the native
`ALLOW_FIELD_ADDITION`/`ALLOW_FIELD_RELAXATION` options are still set as belt-and-braces against
schema changes made externally mid-run. With `WRITE_TRUNCATE` there is nothing to reconcile — the
loaded schema replaces the table schema wholesale. With updates **disabled**, the live table's
schema wins outright: the serializer's differences are not applied, and — measured against real
BigQuery — a staged Avro field the table lacks is then **silently ignored by the load**, the
remaining columns loading normally (the committer logs a warning naming the field, once per
destination per run; before [#142]({{< param BookRepo >}}/issues/142) the same configuration
failed the whole job at submission with *"Cannot add fields"* whenever the run fit one load job).

**`REQUIRED` columns and load jobs.** A load job carries a schema of its own, so what BigQuery does
when that schema disagrees with the destination table matters here in a way it does not for the
Storage Write API. Measured against real BigQuery:

| Provided schema vs. the table | Outcome |
|---|---|
| an existing column declared `REQUIRED` where the table has it `NULLABLE` | accepted; the tightening is silently ignored and the column stays `NULLABLE` |
| a **new** column declared `REQUIRED`, with `allowNewFields()` | the job is **rejected at submission** — *"Cannot add required fields to an existing schema"* |

The second row is why no load is submitted with an unreconciled schema. It used to be reachable —
a direct load once built its schema from the serializer alone, so a job asking for `REQUIRED`
columns (either serializer under `deriveRequiredColumns()`) failed outright when its schema grew a
new column against a pre-existing table, but only when the run fit a single load job; fixed in
[#142]({{< param BookRepo >}}/issues/142) by giving direct loads the reconciliation the temp-table
path always had. Now a new `REQUIRED` column reaches a pre-existing table as `NULLABLE` under
`allowNewFields()` — the union's demotion, applied to the table before the job is submitted — and
whether a run fits one partition no longer decides whether its records load
(`BigQueryFileLoadsSchemaEvolutionITCase` pins both this and the updates-disabled row above
against real BigQuery). The first row never comes up anymore for the same reason: with updates
disabled the provided schema *is* the table's, so no tightening is ever sent.

**Staging cleanup.** Staged files are deleted after a successful load — best-effort; on failure
they are deliberately kept so a Flink restart retries deterministically. Point `stagingPath` at a
**dedicated bucket (separate from checkpoint/savepoint storage) with a lifecycle rule** (for
example: delete objects after 1–7 days) so orphans from hard failures expire on their own. Size
the rule's age above the longest outage you intend to recover from: staged files referenced by a
checkpoint *are* the data, and restoring a streaming job after the rule already expired them
leaves the pending loads permanently failing (the poisoned committables can then only be dropped
by starting without state).

**Errors.** `FailedRowHandler` covers serialization/Avro-conversion failures (row-level, before
staging). A load job itself is all-or-nothing: there is no per-row policy at load time, and a
failed load fails the Flink job.

**Type mapping.** `TIMESTAMP`/`DATE`/`TIME` use Avro logical types, `DATETIME` travels as a
canonical civil-time string, `NUMERIC`/`BIGNUMERIC` as Avro decimals (parameterized
precision/scale respected), `JSON`/`GEOGRAPHY` as strings, `STRUCT`/`REPEATED` nest naturally.
`INTERVAL` and `RANGE` columns are not supported by this write method.

The integration tests (`BigQueryFileLoadsITCase` for batch, `BigQueryFileLoadsStreamingITCase`
for checkpoint-triggered streaming loads, `BigQueryFileLoadsSchemaEvolutionITCase` for loads
against a pre-existing table whose schema the serializer's extends) run real jobs against
BigQuery and GCS and are gated on
`BQ_IT_PROJECT`, `BQ_IT_DATASET` and `BQ_IT_GCS_BUCKET` (application-default credentials); they
are skipped when the variables are unset, keeping `./mvnw verify` credential-free. For local
runs, put the variables (plus `GOOGLE_APPLICATION_CREDENTIALS` if not using the default ADC
location) in an uncommitted `.env` at the repository root — mise loads it automatically. In a git
worktree, run `just worktree-env` once to make the repository root's `.env` reachable there. The
FILE_LOADS clients are built with `getDefaultInstance()`, so the environment must also resolve
a default project (`GOOGLE_CLOUD_PROJECT`, or a gcloud config the client library can see) —
with only the `BQ_IT_*` variables set, the load-job committer fails with "A project ID is
required for this service". `just e2e` runs every gated ITCase and fails loudly if the
variables are missing or a gated class did not actually execute. In CI the same recipe runs
weekly in the E2E workflow, authenticating via Workload Identity Federation
([#28]({{< param BookRepo >}}/issues/28)).

## Error handling

Append failures are classified on the task thread and routed by class:

| Class | Examples | Behavior |
|---|---|---|
| Transient | `UNAVAILABLE`, `ABORTED`, `INTERNAL`, `CANCELLED`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `UNKNOWN` | Retried by the SDK's in-stream retries first (by default 500 ms initial delay, ×2 up to 30 s, 5 attempts); failures that still surface are re-appended by the writer on a rebuilt stream writer with backoff (by default 500 ms initial, doubled up to 10 s, 10 attempts). They do not fail the job unless the retry budget is exhausted |
| Stale stream writer | `STREAM_FINALIZED`, `STREAM_NOT_FOUND`, `INVALID_STREAM_STATE`, writer closed, the SDK's callback-wait watchdog timeout (a sent append got no response within the SDK's hardcoded 5 minutes; the raw exception carries no status code) | Repaired like transient failures: the destination's stream writer is rebuilt and the batch re-appended within the retry budget |
| Schema mismatch | `SCHEMA_MISMATCH_EXTRA_FIELDS` (rows carry fields the table does not have) | With `schemaUpdateOptions(...)` enabled: the table schema is reconciled and the batch re-appended while the update propagates (see [Schema evolution](#schema-evolution)). Otherwise terminal |
| Terminal | `INVALID_ARGUMENT`, `PERMISSION_DENIED`, `NOT_FOUND` under `CREATE_NEVER`, retry-budget exhaustion, failures without a status code (other than the callback-wait timeout above) | Fail the ongoing write or checkpoint immediately |
| Row-level | Rows rejected with per-row error details (`AppendSerializationError`, response row errors), serialization failures, rows over the per-row size limit | Routed row by row to the configured `FailedRowHandler`; surviving rows of the batch are re-appended |

The failed-row policy is pluggable via `failedRowHandler(...)`:

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .failedRowHandler(FailedRowHandler.logAndDrop())
                .build();
```

- `FailedRowHandler.failJob()` (default) — every row-level failure fails the checkpoint
- `FailedRowHandler.logAndDrop()` — logs each failed row at WARN and drops it
- `FailedRowHandler.sendToDeadLetterQueue(...)` — forwards each failed row to a
  `DeadLetterQueue`, an experimental stub interface for the cross-connector DLQ
  standardization ([#37]({{< param BookRepo >}}/issues/37)). The stub has no flush/checkpoint lifecycle yet: implementations
  should write through synchronously (throwing on failure), and restarts can produce
  duplicate dead-letter entries
- Custom handlers implement `FailedRowHandler`; throwing from `handle` fails the checkpoint,
  returning drops the row. `FailedRow` carries the serialized protobuf bytes (the writer is
  stateless, so the original record object is gone by the time server-side row errors arrive),
  or `null` bytes when serialization itself failed

Retries preserve the at-least-once contract: a batch whose append outcome was lost may be
re-appended in full, so duplicates are possible (as with any retry in this write method). Worst
case, a single repair can take about a minute of SDK retries plus a minute of writer re-appends
before surfacing as terminal (with the default schedules). On the default-stream path both
schedules are configurable via `DefaultStreamOptions` (see [Tuning](#tuning)); on the
buffered-stream path the SDK schedule stays fixed and only the writer's own re-append budget is
configurable, via `BufferedStreamOptions`.

## Tuning

`STORAGE_API_AT_LEAST_ONCE` exposes its tuning knobs on `DefaultStreamOptions`, optional on the
builder — an unconfigured sink uses the defaults below:

```java
Sink<MyEvent> sink =
        BigQuerySink.<MyEvent>builder()
                .destination(TableDestination.of("my-project", "my_dataset", "events"))
                .serializer(new MyEventProtoSerializer())
                .defaultStreamOptions(
                        DefaultStreamOptions.builder()
                                .maxInflightRequests(200)
                                .maxConnectionsPerRegion(40)
                                .build())
                .build();
```

The knobs configure three distinct layers.

**Connector batching and recovery budget** (`recovery*`) — the writer's own batching cap and
the bounded re-append schedule that sits above the SDK's retries (the same knobs
`BufferedStreamOptions` exposes for the exactly-once path):

| Knob | Default | Meaning |
|---|---|---|
| `maxAppendRequestBytes` | 512 KiB | Serialized-row bytes buffered per destination before an append request is issued |
| `recoveryInitialBackoff` | 500 ms | First backoff of the connector-driven recovery schedule |
| `recoveryMaxBackoff` | 10 s | Backoff cap of that schedule (doubling) |
| `recoveryMaxAttempts` | 10 | Attempt cap of that schedule |

The 512 KiB default favors bounded memory and per-record latency; throughput-oriented jobs have
headroom to raise `maxAppendRequestBytes` to a few megabytes — the Storage Write API caps a
request at 10 MB — amortizing per-request overhead over larger batches at the cost of more
buffered bytes per destination and coarser retry units (a failed request re-appends more rows).

The schedule pacing schema-update propagation waits (flat 30 s, 30 attempts) is deliberately not
configurable: it tracks how long BigQuery metadata takes to propagate — a service property — not
a workload property.

**SDK in-stream retries** (`retry*`, spelled the SDK's way) — the schedule the SDK applies to
retriable append failures before they ever reach the writer; failures that exhaust it surface to
the connector's recovery budget above:

| Knob | Default | Meaning |
|---|---|---|
| `retryInitialDelay` | 500 ms | First retry delay |
| `retryDelayMultiplier` | 2.0 | Delay multiplier |
| `retryMaxDelay` | 30 s | Delay cap |
| `retryMaxAttempts` | 5 | Attempt cap |
| `maxRetryDuration` | 5 min | Overall ceiling on retrying one failure, across attempts (the SDK's default) |

**Connection pool (multiplexing)** — the default stream multiplexes appends over a shared
connection pool ([official guidance](https://cloud.google.com/bigquery/docs/write-api-best-practices)
recommends multiplexing beyond ~20 concurrent connections). The pool scales by load: a
connection counts as busy above **20 % of its in-flight limits** (or 3 s without a response),
and a busy pool adds connections up to the ceiling.

| Knob | Default | Meaning |
|---|---|---|
| `maxInflightRequests` | 100 | In-flight append requests per pooled connection before the writer blocks |
| `maxInflightBytes` | 100 MiB | In-flight append bytes per pooled connection (the SDK's default) |
| `minConnectionsPerRegion` | 2 | Starting connection count per pool (the SDK's default) |
| `maxConnectionsPerRegion` | 20 | Connection ceiling per pool (the SDK's default) |

`maxInflightRequests` deliberately deviates from the SDK's own default of 1000, following the
[official multiplexing guidance](https://cloud.google.com/bigquery/docs/write-api-streaming#use_multiplexing)
("for automatic scaling up to be more effective, you should consider lowering the
`maxInflightRequests` limit", with 100 in the sample): at 1000, a connection only counts as busy
above 200 queued requests, so load-based scale-up rarely triggers and throughput plateaus on the
starting connections. Set it back to 1000 to restore the SDK's behavior.

Caveats — the pool is JVM-global:

- The pool is **static per (location, credentials)** and adopts the settings of whichever stream
  writer is built first in the JVM: the in-flight limits, SDK retry schedule and
  `maxRetryDuration` of later writers are silently ignored by the SDK. All writers of one
  sink carry the same options, so a job is self-consistent — but on a session cluster, or with
  another Storage Write API client in the same JVM, whichever builds first wins.
- `minConnectionsPerRegion`/`maxConnectionsPerRegion` are applied once per JVM
  (`ConnectionWorkerPool.setOptions` is process-wide), before this connector builds its first
  writer. A second sink configuring different pool bounds in the same JVM is ignored with a
  warning. The floor is latched when a pool is constructed; the ceiling is read live.

**Writer housekeeping** — per-subtask behavior of the writer itself:

| Knob | Default | Meaning |
|---|---|---|
| `destinationIdleTimeout` | 1 h | How long a destination may go without records before its stream writer is closed and dropped |
| `flushInterval` | disabled | Periodic processing-time flush for streaming jobs running without checkpointing |

Cold-destination eviction is memory hygiene for long-lived jobs with dynamic destinations (for
example date-suffixed daily tables), whose per-destination state otherwise grows without bound.
The sweep runs at the end of each successful flush — the point where nothing is pending or in
flight, so closing a stream writer cannot cancel a live append. Correctness is unaffected: an
evicted destination that receives a record again rebuilds its stream writer transparently, at
the cost of that one rebuild. To never evict, set a very large duration.

`flushInterval` bounds the loss window of streaming jobs running *without* checkpointing, where
Flink only flushes at end of input: every interval, the writer appends all pending batches and
awaits every in-flight append, exactly as the checkpoint flush does (idle eviction runs from it
too). It is a mitigation only — the documented at-least-once guarantee still requires
checkpointing, because only a checkpoint coordinates the sink's flush with the source's
position. With checkpointing enabled the option is redundant; a flush of nothing is cheap, but
each flush blocks the task thread until in-flight appends are acknowledged.

## Testing

The module is tested at three levels; `./mvnw verify` runs the first two and needs no GCP
credentials.

**Unit tests** cover the builder/facade dispatch, serializers, schema converters, error
classification and the writer/committer state machines against in-memory fakes. The Avro
serializer additionally carries a round-trip test (`AvroSchemaRoundTripTest`) that pins
`AvroToTableSchemaConverter` against the `TableSchemaToAvroConverter` FILE_LOADS stages files with.
Without it the two could drift apart and corrupt staged files with nothing going red. The protobuf
mode mapping is pinned against real `.proto` fixtures compiled at build time — every proto3
presence shape and the proto2 `required`/`optional` pair, both by default and under
`deriveRequiredColumns()` — and `ProtoRowConverterTest` pins the value side of the same
question: an unselected `oneof` branch is left unset, while a presence-less field is written as its
type default.

**Emulator integration tests** run [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator)
in a testcontainer and exercise the Storage Write API gRPC endpoint plus the REST
table-metadata path end to end: plain at-least-once appends across checkpoint-style flushes
through the `BigQuerySink` facade (`BigQueryDefaultStreamWriterITCase`), dynamic multi-table
destinations (`BigQueryDynamicDestinationsITCase`), table auto-creation with create dispositions
(`BigQueryTableAutoCreationITCase`), schema evolution
(`BigQuerySchemaEvolutionITCase`), Avro records written through the facade into a table created
from the serializer's own derived schema (`BigQueryAvroSerializerITCase` — run under
`deriveRequiredColumns()` and asserting the created table's modes, so the option is verified rather
than merely exercised: `REQUIRED`/`NULLABLE`
scalars, `TIMESTAMP`, `DATE`, `BYTES`, an enum, a `REPEATED` field, a nested `STRUCT`, a map as
`REPEATED STRUCT<key, value>` and both marked column types, `JSON` and `GEOGRAPHY`; `TIME`,
`DATETIME` and `NUMERIC` are excluded
because the emulator implements neither the packed civil-time encoding nor the decimal byte
encoding and reads those columns back as unrelated values whatever is written), the same for JSON
documents including the `ignoreUnknownFields` option (`BigQueryJsonDocumentSerializerITCase`),
protobuf messages under `deriveRequiredColumns()` (`BigQueryProtoPresenceITCase` — the table is
created with the derived `REQUIRED` columns and the values read back as presence says they should:
presence-less columns carry `""`/`0`, `optional` and the unselected `oneof` branch come back NULL;
the query works around two emulator deviations around an *empty* repeated column, where
`ARRAY_TO_STRING` panics the emulator and `ARRAY_LENGTH` returns NULL instead of 0), and a
buffered-stream smoke test of the production
exactly-once client wiring (`BigQueryBufferedStreamSmokeITCase` — single flush only: the
emulator keeps no flush cursor, every `FlushRows` re-inserts all rows up to the offset, and
buffered appends neither honor the request offset nor raise `OFFSET_ALREADY_EXISTS`, so the
exactly-once semantics are verified against real BigQuery instead). The at-least-once tests
connect through a test-only plaintext appender
factory (`EmulatorAppenderFactory`) that also papers over two emulator deviations tracked by
[goccy/bigquery-emulator#342](https://github.com/goccy/bigquery-emulator/issues/342) (default-stream naming, `UNKNOWN` instead of `NOT_FOUND` for missing
tables); routing the *production* factory at the emulator via an injection seam is tracked in
[#54]({{< param BookRepo >}}/issues/54). One further deviation (same family): on a connection opened after an earlier connection to
the emulator has closed, only the first `AppendRows` request is durably applied — follow-ups are
acknowledged but never become queryable. The multi-flush scenario therefore runs in its own test
class, whose connection is guaranteed to be its container's first (one forked JVM and fresh
container per `*ITCase` class). Real BigQuery applies every acknowledged default-stream append.

**Real-GCP tests** cover what the emulator cannot faithfully reproduce, and stay out of
credential-less CI:

- the SDK connection pool under real flow control: MiniCluster streaming jobs running the
  production `StreamWriterRowAppenderFactory` — multiplexed fan-out to eight tables over one
  connection pool, and an induced mid-run restart with dynamic destinations showing the
  at-least-once contract, no gaps with duplicates permitted
  (`BigQueryDefaultStreamAtLeastOnceITCase`). Quota and retry behavior is covered implicitly by
  that production path; `RESOURCE_EXHAUSTED` is deliberately not synthesized, because reliably
  tripping a quota means sustained abusive load against the shared free-tier project, and the
  connector's handling of quota responses stays pinned by unit tests against fakes
- default-stream schema evolution against the real service is a **manual probe**
  (`BigQueryDefaultStreamSchemaEvolutionITCase`), deliberately outside the weekly suite: the
  connector widens the table itself and the evolved column's values are queried back — the half
  the emulator cannot show, since it applies `tables.update` to table metadata only. Propagation
  typically completes in well under a minute, but one measured run took ~2 hours end to end (the
  Storage Write API kept rejecting, then hanging, appends carrying the new column for ~1 h 56 m
  after the instant REST update) — a tail that would consume the whole weekly runner budget if
  the probe joined the suite. The probe is gated on `BQ_IT_SCHEMA_EVOLUTION` and instrumented to
  capture the next tail occurrence end to end (SDK-level connection logs, both schema views
  polled over time, a non-pooled canary writer); the hang's record and open hypotheses are in
  [#174]({{< param BookRepo >}}/issues/174), closed as wait-and-see — a captured reproduction
  gets a new issue referencing it
- serializer column-type fidelity (`BigQuerySerializerFidelityITCase`): the encodings an
  emulator divergence would silently corrupt — `NUMERIC`/`BIGNUMERIC` (decimal byte encoding)
  and `TIME`/`DATETIME` (packed civil-time encoding), which the emulator reads back as unrelated
  values and the emulator ITs therefore exclude, plus `TIMESTAMP` microsecond precision, `BYTES`,
  `JSON` including the `REPEATED JSON` the emulator rejects outright, and `GEOGRAPHY` — written
  per serializer (the full protobuf well-known-type fixture, an Avro schema, JSON documents) and
  read back with typed accessors
- load jobs: goccy/bigquery-emulator supports neither `gs://` load jobs nor a Cloud Storage
  endpoint, so the whole `FILE_LOADS` path runs against real services
  (`BigQueryFileLoadsITCase` and `BigQueryFileLoadsStreamingITCase`, env-gated as described
  [above](#file-loads))
- buffered-stream exactly-once semantics: idempotent re-flush, the restore probe, and the
  [issue #30]({{< param BookRepo >}}/issues/30) acceptance criterion — a MiniCluster streaming job
  with an induced mid-run restart showing no duplicates and no gaps — plus a clean streaming run
  and batch execution
  (`BigQueryBufferedStreamExactlyOnceITCase`, gated on `BQ_IT_PROJECT`/`BQ_IT_DATASET` only;
  no bucket needed)

These gated ITCases run weekly in the E2E workflow via Workload Identity Federation
([#28]({{< param BookRepo >}}/issues/28)); `just e2e` is the local equivalent.
