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

# ADR-0020: FILE_LOADS stages `DATETIME` as `local-timestamp-micros`, never as text

- Status: Accepted
- Date: 2026-08-04
- Issues: [#282]
- Modules: bigquery (`sink.fileloads`, `sink.serializer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` (type mapping)

## Context / Evidence

It used to stage a `string` formatted `yyyy-MM-dd'T'HH:mm:ss.SSSSSS`, on the stated grounds that
*"Avro has no timezone-less datetime logical type universally accepted by BigQuery loads"*. That
is measurably false, and the text form did not merely lose fidelity — **every** load job
carrying a `DATETIME` column failed, with `useAvroLogicalTypes` on or off: `400 Field v has
incompatible types. Configured schema: datetime; Avro file: string.` The neighbouring `JSON` and
`GEOGRAPHY` columns survive the same shape only because BigQuery coerces text into *those* — so
do not reason from them to a third type, and do not reintroduce the string form for `INTERVAL`
or any later type: the question is always what the load job accepts, and only a load job can
answer it.

Two things this cost, worth keeping: `TableSchemaToAvroConverterTest` and
`ProtoToAvroConverterTest` were green throughout, because they assert the two converters agree
with **each other** and the disagreement was with the service (the
*emulators-are-not-authorities* rule in a shape that has no emulator in it at all); and
`BigQueryFileLoadsITCase` carried `STRING`/`INT64`/`GEOGRAPHY` only, so no load job had ever
carried a `DATETIME`. That gap is closed by `everySupportedColumnTypeSurvivesTheLoad`, which
loads **every** type this write method supports and asserts each value back — all measured good
on 2026-08-04. A new supported type owes a column there.

## Decision

- Stage `DATETIME` as Avro `local-timestamp-micros`.
- **The string wire form is parsed rather than rejected or read strictly** (decided with the
  user, 2026-08-04): a hand-written serializer's `DATETIME` field declared as a proto `string`
  (the Storage Write API takes either) goes through
  `ProtoToAvroConverter.DATETIME_LITERAL`, which expresses BigQuery's documented literal grammar
  (`YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]`), so one serializer works unchanged under
  `STORAGE_API_*` and `FILE_LOADS`; rejecting it as `TIME`'s string form is rejected would have
  broken that parity, and strict ISO would have failed strings the service accepts. It is
  written from the public grammar rather than copied from the client library's private
  `JsonToProtoMessage` formatter, which keeps the module README's "no source code has been
  copied" claim true.
- **`.withResolverStyle(ResolverStyle.STRICT)` on that formatter is load-bearing**:
  `DateTimeFormatterBuilder.append(DateTimeFormatter)` copies the appended formatter's
  printer-parser but **not** its resolver style, so an assembled formatter gets the `SMART`
  default even though `ISO_LOCAL_DATE` is itself `STRICT`. Under `SMART`, `2026-02-30` resolves
  to the 28th and `24:00:00` rolls into the next day — the staged file then carries a date
  nobody wrote, under a green job, where BigQuery answers the same literal with an error. Found
  in self-review, measured on a JDK-17 probe; `STRICT` costs nothing (it governs resolution, not
  the `parseLenient` field widths, and `ISO_LOCAL_DATE` appends `YEAR`, not `YEAR_OF_ERA`, so no
  era is demanded).
- The formatter is deliberately a **superset** of the grammar — omitted seconds, a lowercase
  `t`, either separator, a year of other than four digits, a signed year — and accepting more is
  safe **only** because each of those has exactly one reading, which is precisely what the
  calendar cases do not. A year outside BigQuery's documented `0001`–`9999` is rejected in
  `toCivilMicros` rather than staged (a load job is all-or-nothing: a value no column can hold
  must fail its own row here, not the whole job later). That check fires on the **literal path
  only** — `CivilTimeEncoder.decodePacked64DatetimeMicrosLocalDateTime` applies the identical
  bound itself (measured against SDK 3.30.0), so the packed path cannot reach it; it stays
  because that bound is the SDK's invariant rather than ours.

## Consequences

One claim **not** to repeat, made here first and wrong: deleting the old `DATETIME_FORMAT` did
*not* fix a locale hazard. `DateTimeFormatterBuilder.toFormatter()` hardcodes
`DecimalStyle.STANDARD`, and `ofPattern`'s default locale sets only the *text* locale, so an
all-numeric pattern stays ASCII under `th-TH-u-nu-thai` — non-ASCII digits need an explicit
`withDecimalStyle`, which that code never called (measured). What the old formatter really
carried was the `STRICT` trap from the other side: `yyyy` is `YEAR_OF_ERA`, so it staged
proleptic year 0 as `0001` and year -1 as `0002` — a silently wrong year, the same shape of
defect the resolver style now rules out.

[#282]: https://github.com/laughingman7743/flink-connector-gcp/issues/282
