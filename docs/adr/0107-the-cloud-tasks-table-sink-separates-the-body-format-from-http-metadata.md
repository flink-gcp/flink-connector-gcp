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

# ADR-0107: The Cloud Tasks table sink separates the body format from target metadata

- Status: Accepted
- Date: 2026-08-13
- Issues: [#99](https://github.com/laughingman7743/flink-connector-gcp/issues/99),
  [#605](https://github.com/laughingman7743/flink-connector-gcp/issues/605),
  [#606](https://github.com/laughingman7743/flink-connector-gcp/issues/606),
  [#608](https://github.com/laughingman7743/flink-connector-gcp/issues/608),
  [#634](https://github.com/laughingman7743/flink-connector-gcp/issues/634)
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/table/cloudtasks.md`

## Context

Cloud Tasks schedules a request rather than writing one fixed service resource.
The request is either an external `HttpRequest` or a same-project, same-region
`AppEngineHttpRequest` with optional service, version and instance routing.
The called API decides whether that request carries JSON, CSV, another byte representation, a
query string, or no body.
The existing DataStream sink already separates task serialization from queue resolution and owns
task naming outside the serializer.
The Table API needs to expose that model without inventing one SQL schema per target API or allowing
request metadata to leak into a generic body format.

## Decision

**The `cloud-tasks` table connector is an insert-only sink onto one fixed queue.**
`project`, `location`, `queue` and `format` are required.
The table layer maps onto `CloudTasksSinkBuilder<RowData>` and
`CloudTasksWriterOptions.Builder`; it neither creates queues nor exposes queue rate settings.
Dynamic queues remain a DataStream API feature because a SQL table represents one destination.

**A generic Flink serialization format encodes only physical columns into the request body.**
Writable metadata is appended to the runtime row but projected away before the format runs.
External HTTP `POST`, `PUT` and `PATCH` invoke the format and carry its bytes.
App Engine `POST` and `PUT` do the same; every other method creates a bodyless request without
invoking the format.
The built-in `form-urlencoded` format accepts physical `STRING` and `ARRAY<STRING>` columns and
encodes their names and values as a UTF-8 HTTP form in physical schema and array order.
It omits null fields, preserves empty strings, repeats array fields and rejects null array elements.
Other SQL types must be cast to `STRING`, while multipart remains outside the Table API contract.

**A format may own the Content-Type of the body it creates.**
`form-urlencoded` owns `application/x-www-form-urlencoded` and adds it only when the selected target
and method create a body.
A fixed or metadata header with the same trimmed, case-insensitive value is canonicalized, while a
different value or a parameterized value is rejected.
Generic formats retain the existing caller-owned header behavior.

**The target family selects disjoint fixed options and address metadata.**
`target.type` defaults to `http`, which preserves every existing table that omits it.
HTTP uses `http.*` options and writable `url`; App Engine uses `app-engine.*`, writable
`relative-uri`, and separate `app-engine-service`, `app-engine-version` and
`app-engine-instance` routing metadata.
Options and metadata from the other family are rejected rather than translated between protobuf
arms.

**Request properties are fixed options with non-null metadata overrides.**
Both families share writable `http-method`, `headers`, `schedule-time` and `task-id`.
Headers merge by case-insensitive name with the row value winning; duplicate case-insensitive
names within one row are rejected because their winner would otherwise depend on map iteration.
When `http.url` is absent, the catalog must declare writable `url` metadata as `STRING NOT NULL`;
when `app-engine.relative-uri` is absent, the equivalent requirement applies to writable
`relative-uri`.
The factory reads those declarations before Flink normalizes the consumed metadata type to the
connector's nullable advertised type.
App Engine routing fields override independently, and an empty metadata value clears its fixed
selector; routing is omitted when all three selectors are empty.
A dynamic invalid address, method, header or missing task id fails the row through the existing
sink contracts rather than being silently omitted.

**Task ids remain outside the task serializer.**
Selecting `task-id` installs a `TaskIdExtractor<RowData>` on the sink builder.
The existing writer therefore keeps sole ownership of SHA-256 hashing, queue-qualified task-name
composition, missing-key failure and `ALREADY_EXISTS` success.
The table serializer always leaves `Task.name` empty.

**Cloud Tasks API credentials and external dispatched-request credentials stay separate.**
`service-account-key-file` selects the former and preserves application-default credentials when
absent.
Fixed OIDC options serve Cloud Run, Cloud Run functions and handlers that validate
Google OIDC tokens; fixed OAuth options serve Google APIs on `*.googleapis.com`.
The two token modes are mutually exclusive and no per-row authentication metadata is exposed.
App Engine rejects both token families because Cloud Tasks uses its internal integration instead;
queue-level `appEngineRoutingOverride` remains authoritative over task-level routing.

## Evidence

- Factory and option-inventory tests cover target spelling and HTTP defaulting, both fixed target
  models, family rejection, generic JSON and CSV discovery, credentials, token exclusivity, writer
  tuning, sink parallelism, form discovery, physical-type validation and Content-Type conflicts.
- Planner tests prove that each target's `STRING NOT NULL` address metadata declaration is accepted,
  a nullable one is rejected when the insert is planned, wrong-family metadata is rejected, and the
  documented request DDL parses.
- Serializer tests cover both protobuf arms, physical-column projection, fixed and row request
  precedence, independent App Engine routing overrides, case-insensitive header replacement,
  target-specific body methods, schedule precision, OIDC, OAuth, reserved headers and malformed
  metadata. Form tests cover UTF-8 escaping, scalar and repeated fields, null and empty values,
  schema order and null array elements.
- Emulator integration tests execute SQL jobs through the production factory and writer.
  They verify HTTP JSON POST dispatch, bodyless GET creation, metadata overrides and named-task
  deduplication across separate completed jobs, plus the exact form bytes received by an HTTP
  handler and a FULL-view App Engine task retained in a paused queue.

## Alternatives declined

- **One column for a complete serialized `Task`** would make SQL users construct protobuf bytes,
  bypass task-id hashing and lose typed validation.
- **Selecting body encodings inside the connector factory** would duplicate Flink's format
  discovery and still fail whenever an external API chose another media type. The built-in form
  encoder remains a `SerializationFormatFactory`, so it composes with the existing format contract.
- **Put request metadata into the body format** would make ordinary JSON contain URL, method and
  scheduling fields that belong to Cloud Tasks rather than the called API.
- **Require a fixed URL for every table** would prevent a table from expressing record-specific
  REST resources and GET query strings.
- **Treat a null dynamic task id as unnamed** would silently disable deduplication for one row in a
  stream that opted into named tasks.
- **Expose App Engine routing as more HTTP metadata** would conflate two protobuf target types and
  hide their different regional and overload semantics. A target selector and disjoint address and
  routing metadata retain those differences while the method, header, schedule and task-id fields
  remain genuinely shared.

## Consequences

SQL can express external HTTP requests and same-project App Engine routing while the target API
remains responsible for its media type and query semantics.
Form targets need no duplicate Content-Type option, and conflicting fixed or row headers cannot
silently describe the bytes as another representation.
A table using a bodyless method still declares a format because the factory has one stable contract
and a per-row method can switch back to a body-carrying method.
SQL Client users should deploy the relocated `flink-sql-connector-gcp-cloudtasks` uber-jar, while
DataStream jobs should depend on the plain connector and resolve its dependencies normally.
OIDC does not make an internal-only handler reachable and does not authenticate the Flink writer;
deployments must configure both identities and network reachability independently.
