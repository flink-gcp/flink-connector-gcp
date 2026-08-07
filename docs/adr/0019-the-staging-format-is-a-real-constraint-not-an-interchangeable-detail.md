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

# ADR-0019: The staging format is a real constraint, not an interchangeable detail

- Status: Accepted
- Date: 2026-08-04
- Issues: [#281] (measurements there; [#283] zstd and [#285] the roll threshold came out of the
  same runs)
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads

## Context

Three places said FILE_LOADS stages Avro *only incidentally*, added by [#145] as part of the
argument that the *protobuf* mapping is the normative one, and two of them drew the
substitutability conclusion outright — the `AvroSchemaOptions` javadoc with "a staging format
rather than a contract, and Parquet is equally possible", the module's decision record with
"could stage Parquet"; the docs page named Parquet nowhere.

## Decision

**That conclusion is withdrawn as false.** Measured against real BigQuery, a Parquet load cannot
reach a `JSON` column by any route: with a provided schema it is refused at *job-configuration*
level — `Unsupported field type: JSON` whenever the schema names one, whatever the file holds —
and the schema-less routes fail the table's type check instead, except Parquet's own JSON
annotation under autodetect, which lands **silently as `BYTES`**. `INTERVAL`/`RANGE` are refused
by target type too. So the formats are not substitutable and a Parquet path cannot be a straight
swap — the constraint [#284]'s design has to work within.

What [#145] actually needed is the narrower claim, and it never depended on the staging format
at all: every write path goes through a protobuf row, and FILE_LOADS converts *that row* into
the file it stages — so the staging format sits downstream of the mapping. Say it that way and
it stays true whatever FILE_LOADS stages, which is the point: a formulation that has to be
revisited per format is how the withdrawn claim got written in the first place.

## Consequences

The sibling decision is ADR-0020, reached the same way — what the load job accepts is a question
only a load job answers.

[#145]: https://github.com/laughingman7743/flink-connector-gcp/issues/145
[#281]: https://github.com/laughingman7743/flink-connector-gcp/issues/281
[#283]: https://github.com/laughingman7743/flink-connector-gcp/issues/283
[#284]: https://github.com/laughingman7743/flink-connector-gcp/issues/284
[#285]: https://github.com/laughingman7743/flink-connector-gcp/issues/285
