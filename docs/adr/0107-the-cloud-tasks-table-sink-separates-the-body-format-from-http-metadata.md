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

# ADR-0107: The Cloud Tasks table sink separates the body format from HTTP metadata

- Status: Accepted
- Date: 2026-08-13
- Issues: [#99](https://github.com/laughingman7743/flink-connector-gcp/issues/99),
  [#605](https://github.com/laughingman7743/flink-connector-gcp/issues/605)
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/table/cloudtasks.md`

## Context

Cloud Tasks schedules an HTTP request rather than writing one fixed service resource.
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

**A generic Flink serialization format encodes only physical columns into the HTTP body.**
Writable metadata is appended to the runtime row but projected away before the format runs.
`POST`, `PUT` and `PATCH` invoke the format and carry its bytes.
`GET`, `HEAD`, `DELETE` and `OPTIONS` do not invoke it and carry no body.
Form encoding is a separate format decision in #606, while multipart remains outside the Table API
contract.

**Request properties are fixed options with non-null metadata overrides.**
The writable keys are `url`, `http-method`, `headers`, `schedule-time` and `task-id`.
Headers merge by case-insensitive name with the row value winning; duplicate case-insensitive
names within one row are rejected because their winner would otherwise depend on map iteration.
When `http.url` is absent, the catalog must declare writable `url` metadata as `STRING NOT NULL`;
the factory reads that declaration before Flink normalizes the consumed metadata type to the
connector's nullable advertised type.
A dynamic invalid URL, method, header or missing task id fails the row through the existing sink
contracts rather than being silently omitted.

**Task ids remain outside the task serializer.**
Selecting `task-id` installs a `TaskIdExtractor<RowData>` on the sink builder.
The existing writer therefore keeps sole ownership of SHA-256 hashing, queue-qualified task-name
composition, missing-key failure and `ALREADY_EXISTS` success.
The table serializer always leaves `Task.name` empty.

**Cloud Tasks API credentials and dispatched-request credentials stay separate.**
`service-account-key-file` selects the former and preserves application-default credentials when
absent.
Fixed OIDC options serve Cloud Run, Cloud Run functions and handlers that validate
Google OIDC tokens; fixed OAuth options serve Google APIs on `*.googleapis.com`.
The two token modes are mutually exclusive and no per-row authentication metadata is exposed.
App Engine uses a different protobuf target with different routing and overload behavior and is
deferred to #608.

## Evidence

- Factory and option-inventory tests cover generic JSON and CSV discovery, required and unknown
  options, credentials, token exclusivity, writer tuning and sink parallelism.
- Planner tests prove that a `STRING NOT NULL` URL metadata declaration is accepted, a nullable one
  is rejected when the insert is planned, and the documented authenticated request DDL parses.
- Serializer tests cover physical-column projection, fixed and row request precedence,
  case-insensitive header replacement, bodyless methods, schedule precision, OIDC, OAuth and
  malformed metadata.
- Emulator integration tests execute SQL jobs through the production factory and writer.
  They verify JSON POST dispatch, bodyless GET creation, metadata overrides and named-task
  deduplication across separate completed jobs.

## Alternatives declined

- **One column for a complete serialized `Task`** would make SQL users construct protobuf bytes,
  bypass task-id hashing and lose typed validation.
- **A fixed set of body encodings inside the connector** would duplicate Flink's format discovery
  and still fail whenever an external API chose another media type.
- **Put request metadata into the body format** would make ordinary JSON contain URL, method and
  scheduling fields that belong to Cloud Tasks rather than the called API.
- **Require a fixed URL for every table** would prevent a table from expressing record-specific
  REST resources and GET query strings.
- **Treat a null dynamic task id as unnamed** would silently disable deduplication for one row in a
  stream that opted into named tasks.
- **Expose App Engine routing as more HTTP metadata** would conflate two protobuf target types and
  hide their different regional and overload semantics.

## Consequences

SQL can express the request shapes shared across external HTTP APIs while the target API remains
responsible for its media type and query semantics.
A table using a bodyless method still declares a format because the factory has one stable contract
and a per-row method can switch back to a body-carrying method.
SQL Client users need the plain connector and its dependency tree until #607 supplies the shaded
artifact.
OIDC does not make an internal-only handler reachable and does not authenticate the Flink writer;
deployments must configure both identities and network reachability independently.
