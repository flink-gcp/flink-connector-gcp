---
title: Cloud Tasks
type: docs
weight: 30
---

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

# Cloud Tasks SQL Connector

Start with the [Quickstart]({{< relref "docs/quickstart/cloudtasks" >}}) for the basic DataStream
job, or use the [Cloud Tasks examples]({{< relref "docs/examples/cloudtasks" >}}) for complete
Table sink requests and cross-connector pipelines.

## Overview and setup

The `cloud-tasks` table connector is a sink provided by the
`flink-connector-gcp-cloudtasks` module.
It maps onto the [DataStream sink]({{< relref "docs/connectors/datastream/cloudtasks" >}}), which
documents checkpoint behavior, retries, task naming and queue pacing.
This page defines how SQL rows become external HTTP or App Engine requests.

Cloud Tasks is a request dispatch queue rather than an API-specific client.
The target API therefore decides whether a request uses JSON, another body format, a query string,
or no body at all.
SQL represents that split with a Flink format for the body and writable metadata for the rest of
the request.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="overview" >}}

The JSON format sees only `order_id` and `amount`.
The three metadata columns configure the request outside that body.
Because `target.type` defaults to `http`, this DDL remains compatible with tables created before
App Engine target support.

### Getting the connector onto the classpath

Use `flink-sql-connector-gcp-cloudtasks`, the relocated SQL uber-jar, for SQL Client deployments.
Place `flink-sql-connector-gcp-cloudtasks-<version>.jar` in Flink's `lib/` before starting the
cluster, or load it for one SQL Client session:

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="add-jar" >}}

The jar bundles `flink-connector-gcp-cloudtasks` and the runtime dependency tree it needs. Java
dependency packages linked by the connector move under
`io.github.flink.gcp.connector.cloudtasks.shaded`, including the matching native-resource rename
required by the already-shaded gRPC Netty transport. Conscrypt remains unrelocated because it owns
native libraries and is optional; the other unrelocated packages are annotations only. The
generated `META-INF/NOTICE` enumerates every bundled artifact, with pinned permissive licence texts
under `META-INF/licenses/`.

Keep sibling SQL connector jars as separate files in `lib/` or add each with its own `ADD JAR`.
Merging them into another fat jar without merging service descriptors can silently discard one of
the factory registrations. A Maven or Gradle DataStream job should instead depend on the plain
`flink-connector-gcp-cloudtasks` module and resolve its transitive dependencies normally.

## Table sink

### Body format and request metadata

This overview covers generic body serialization and the request metadata projected before encoding.
Later sibling sections keep the form-specific examples, bodyless methods and writable metadata
independently visible in the page outline.

`format` is any `SerializationFormatFactory` available on the job classpath, such as `json`, `csv`,
Avro or `raw` where its schema requirements are met.
The connector does not interpret bytes from those generic formats, so set the matching
`Content-Type` header for the target API.
The [worked request-body examples]({{< relref "docs/examples/cloudtasks" >}}#table-api-request-bodies)
show their SQL input and exact JSON, CSV, raw and Avro bytes.

The module also provides the `form-urlencoded` format for
`application/x-www-form-urlencoded` bodies.
External HTTP requests carry those bodies under POST, PUT and PATCH, while App Engine requests
carry them under POST and PUT.
It accepts only these physical SQL column types:

| Physical SQL type | Form representation |
|-------------------|---------------------|
| `STRING` | One field using the column name |
| `ARRAY<STRING>` | One field per element, each using the column name |

All other physical types are rejected when Flink creates the sink.
This includes `CHAR`, numeric, Boolean, binary and temporal types, nested `ROW` and `MAP` types,
and every array type other than one-dimensional `ARRAY<STRING>`.
Cast scalar values to `STRING` so the SQL states their wire representation explicitly.
For structured values, flatten the required members into `STRING` columns or serialize the value
to a chosen string representation before inserting it into the sink.
The format does not choose a bracket, dotted-name or JSON convention for nested values because
`application/x-www-form-urlencoded` does not define one.

Fields follow physical schema order, repeated values follow array order, and both names and values
use UTF-8 form encoding.
A null field or array is omitted, an empty string is preserved as `name=`, an empty array adds no
field, and a null array element fails the row because a form cannot represent it.
Writable metadata columns are projected out before encoding and never become form fields.

The form format adds `Content-Type: application/x-www-form-urlencoded` automatically.
You do not need to set that header in the selected target's fixed headers or in metadata.
An equivalent value is canonicalized, while a different value or a value with media-type parameters
is rejected as a conflict.

### Form request examples

The examples below show the HTTP request definition created from one `INSERT` row.
Cloud Tasks may dispatch that request more than once under the queue retry policy.

#### Repeated and joined array values

An `ARRAY<STRING>` column repeats its column name, while `ARRAY_JOIN` converts an array to one
scalar form value when the receiving API expects a delimiter.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="repeated-form-values" >}}

The inserted row produces this request body.

```http
POST /orders HTTP/1.1
Content-Type: application/x-www-form-urlencoded

order_id=42&note=%E6%9D%B1%E4%BA%AC+%2B+pickup&tags=urgent&tags=gift&categories=books%2Csale
```

The comma is part of the `categories` value and is therefore percent-encoded as `%2C`.
The receiving form parser recovers the value `books,sale`.

#### Null and empty values

The same table can distinguish an empty string from an omitted value.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="null-and-empty-form-values" >}}

The empty `note` remains present, while the null `tags` array and null `categories` value add no
fields.
An empty array supplied by an upstream table also adds no fields.

```http
POST /orders HTTP/1.1
Content-Type: application/x-www-form-urlencoded

order_id=43&note=
```

#### Bracket and dotted-name conventions

Some servers interpret brackets or dots in field names as a nested structure.
The connector does not assign those meanings, but quoted SQL column names can produce the names a
specific server expects.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="nested-form-names" >}}

For an input containing items `['book', 'pen']`, customer `('Alice', '100-0001')` and priority
`high`, the request body is:

```http
items%5B%5D=book&items%5B%5D=pen&customer.name=Alice&customer%5BpostalCode%5D=100-0001&attributes%5Bpriority%5D=high
```

This projection works when the nested members and map keys are known in the sink schema.
It cannot turn arbitrary map keys or an arbitrary number of array indexes into field names because
physical column names are fixed when Flink plans the job.

#### JSON in one form field

`JSON_OBJECT` can convert selected structured values to one `STRING` column before the form format
encodes it.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="json-form-field" >}}

For the preceding customer and items, the JSON value can produce the following URL-encoded
`payload` field.

```http
payload=%7B%22items%22%3A%5B%22book%22%2C%22pen%22%5D%2C%22name%22%3A%22Alice%22%2C%22postalCode%22%3A%22100-0001%22%7D
```

JSON object member order is not part of the form format contract and may differ across Flink
versions.
The receiving API must interpret the decoded value as JSON rather than depend on its member order.

#### Fully custom form bodies

Dynamic field names such as `items[0]`, `items[1]` and every key from an arbitrary map require a
serializer that owns the complete form body.
One SQL approach is an application-provided scalar function combined with Flink's `raw` format.

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="custom-form-body" >}}

The function must return a complete UTF-8 form body with every name and value form-encoded.
For example, it could return:

```text
items%5B0%5D=book&items%5B1%5D=pen&attributes=priority%3Ahigh%2Ccolor%3Ablue
```

This example uses indexed array names and joins the map as `priority:high,color:blue` inside one
form value.
The function owns the map entry order, delimiters, escaping and null behavior because those rules
come from the receiving API rather than from the media type.

Do not send a pre-encoded body through `form-urlencoded`.
That format treats `&`, `=` and percent signs as data inside one field and encodes them again.
The DataStream API is another option when the custom transformation is easier to express as a
`CloudTasksSerializationSchema` than as a SQL function.

### Methods without a body

External HTTP requests carry the encoded body only under `POST`, `PUT` and `PATCH`.
App Engine requests carry it only under `POST` and `PUT`.
Every other method allowed by the selected protobuf request type leaves the body empty and does not
invoke the format for that row.

For a GET API, construct the query string as part of `http.url` or the `url` metadata column.
For example:

{{< sql-snippet file="flink/CloudTasksTableReference.sql" tag="get-request" >}}

`URL_ENCODE` is a built-in Flink SQL function on the supported Flink 2.x line.
Flink 1.20 jobs must register an equivalent UTF-8 URL-encoding scalar UDF or construct the complete
URL before the row reaches SQL.
Concatenating an unescaped value changes the query syntax and is not made safe by Cloud Tasks.

Multipart bodies are not part of this connector contract.
A job that needs multipart can implement `CloudTasksSerializationSchema` through the DataStream API,
where the boundary and part encodings can be controlled explicitly.

### Writable metadata

| Metadata key | Target | Type | Null behavior and precedence |
|---|---|---|---|
| `url` | HTTP | `STRING` | A non-null value overrides `http.url`. If `http.url` is absent, this metadata column is required and must be declared `STRING NOT NULL` so every row has a target. The value must be an absolute `http://` or `https://` URL |
| `relative-uri` | App Engine | `STRING` | A non-null value overrides `app-engine.relative-uri`. If that option is absent, this metadata column is required and must be declared `STRING NOT NULL`. The value is empty for the root path, or begins with `/` and contains a path and optional query only |
| `http-method` | Both | `STRING` | A non-null value overrides `http.method` or `app-engine.method`, case-insensitively. Accepted values are `POST`, `GET`, `HEAD`, `PUT`, `DELETE`, `PATCH` and `OPTIONS` |
| `headers` | Both | `MAP<STRING, STRING>` | A null map adds no row headers. Row entries override the selected target's fixed headers by case-insensitive name. A null or blank name, a null value, or duplicate case-insensitive names fail the record. App Engine also rejects headers owned by Cloud Tasks or App Engine |
| `app-engine-service` | App Engine | `STRING` | A non-null value overrides `app-engine.service`. An empty string clears the fixed selector |
| `app-engine-version` | App Engine | `STRING` | A non-null value overrides `app-engine.version`. An empty string clears the fixed selector |
| `app-engine-instance` | App Engine | `STRING` | A non-null value overrides `app-engine.instance`. An empty string clears the fixed selector; a non-empty instance requires a manually scaled service |
| `schedule-time` | Both | `TIMESTAMP_LTZ(6)` | A non-null value becomes the task schedule time with microsecond precision. Null leaves the service default |
| `task-id` | Both | `STRING` | Selecting this column enables named tasks for the entire sink. Every row must then supply a non-null, non-empty value. The sink hashes it with SHA-256 before composing the task name; a remembered duplicate is success |

Metadata is appended after the physical columns before the runtime serializer receives a row.
The connector projects the physical prefix before invoking the format, so request metadata never
appears in JSON, CSV or another body by accident.
The sink advertises only metadata belonging to the selected target family, so `url` cannot be used
with App Engine and the App Engine address and routing keys cannot be used with HTTP.

### App Engine targets

Set `target.type` to `app-engine` for an App Engine handler in the queue's project and region.
The connector creates the task's `AppEngineHttpRequest` arm rather than translating routing into an
external URL.
Task-level service, version and instance selectors are independent, and a queue-level
`appEngineRoutingOverride` remains authoritative when the queue defines one.

The [worked App Engine example]({{< relref "docs/examples/cloudtasks" >}}#an-app-engine-handler)
shows the target and routing metadata together in one planned statement.

An empty or absent routing value lets App Engine choose its default service, version and an
available instance.
Instance routing is valid only for a manually scaled service.
Reserved headers including `Host`, `Content-Length`, `X-Google-*` and `X-AppEngine-*` are set by
Cloud Tasks or App Engine and cannot be overridden.

### Authentication has two independent identities

`service-account-key-file` authenticates the Flink writer to the Cloud Tasks API.
When it is absent the writer uses application-default credentials.
The file path, not the credential contents, travels in the job graph, and each TaskManager reads the
file when its writer starts.

The `http.oidc.*` and `http.oauth.*` options configure a token that Cloud Tasks attaches later when
it dispatches the HTTP request.
They do not authenticate the Flink process and cannot replace permission to call `CreateTask`.

Use OIDC for Cloud Run, Cloud Run functions, and another endpoint that validates a Google-issued
OIDC token.
The OIDC service account must belong to the same project as the queue.
The principal that creates tasks needs permission to enqueue tasks and
`iam.serviceAccounts.actAs` on that service account; the service account in turn needs Cloud Run
Invoker on the target service or function.
The connector does not create or modify those IAM bindings.

When the task URL contains a path, set `http.oidc.audience` to the stable root URL of the Cloud Run
service or function, normally its default `run.app` URL.
If the option is absent, Cloud Tasks uses the complete target URL, including its path, as the
audience.
The Flink writer's identity, selected independently through `service-account-key-file` or
application-default credentials, remains the principal that calls `CreateTask`.

A public Compute Engine, GKE or on-premises handler can also use OIDC, but the application must
validate the signature, issuer, audience and intended service-account identity itself.
Cloud Tasks headers such as `X-CloudTasks-TaskName` are request metadata, not proof of identity.
The endpoint must be reachable by Cloud Tasks; an internal-only address is not made reachable by
setting a token.

Use OAuth for Google API endpoints on `*.googleapis.com` that require an access token and scope.
OIDC and OAuth are mutually exclusive because the Cloud Tasks request stores them in one protobuf
`oneof`.

App Engine targets do not accept the `http.oidc.*` or `http.oauth.*` options.
Cloud Tasks dispatches them through the same-project, same-region App Engine integration instead
of attaching an external HTTP authorization token.

## Options

The queue is fixed for a table.
SQL exposes no dynamic queue metadata and never creates or configures a queue.
Queue rate limits, dispatch concurrency, delivery retries, and paused or disabled queue behavior
are service-side settings and behaviors that apply equally to Table jobs; they are documented under
[Queues, rate limits and sink concurrency]({{< relref "docs/connectors/datastream/cloudtasks" >}}#queues-rate-limits-and-sink-concurrency)
rather than repeated as Table options.
[Writer concurrency]({{< relref "docs/connectors/datastream/cloudtasks" >}}#queues-rate-limits-and-sink-concurrency)
and [`CreateTask` RPC recovery]({{< relref "docs/connectors/datastream/cloudtasks" >}}#delivery-guarantees-and-state)
use the options below and follow the same runtime behavior as the DataStream sink.

An option left out leaves the corresponding DataStream setting at its existing default, except
`target.type`, `http.method` and `app-engine.method`, whose SQL defaults are `http`, `POST` and
`POST` respectively.

### Queue, body and writer identity

| Option | Type | Default | Maps to |
|---|---|---|---|
| `project` | String | **required** | the project component of `QueueDestination.of(...)` |
| `location` | String | **required** | the location component of `QueueDestination.of(...)` |
| `queue` | String | **required** | the queue component of `QueueDestination.of(...)` |
| `format` | String | **required** | serialization format discovery for physical columns; `form-urlencoded` provides the built-in form encoding described above |
| `target.type` | `http` \| `app-engine` | `http` | selects the external HTTP or App Engine protobuf request arm |
| `service-account-key-file` | String | application-default credentials | `CloudTasksSinkBuilder.serviceAccountKeyFile(...)` |
| `emulator-endpoint` | String | production Cloud Tasks | `CloudTasksSinkBuilder.emulatorEndpoint(...)` as `host:port`; plaintext and no credentials. Parsed when the statement is planned, so a malformed value fails on the client, and the rejection names `emulator-endpoint` — the key written in the DDL |

`service-account-key-file` and `emulator-endpoint` are mutually exclusive.
Service-account keys are long-lived secrets, so prefer an attached service account or Workload
Identity where the deployment supports one.

### HTTP request defaults

| Option | Type | Default | Maps to |
|---|---|---|---|
| `http.url` | String | the non-null `url` metadata column | default target URL |
| `http.method` | `POST` \| `GET` \| `HEAD` \| `PUT` \| `DELETE` \| `PATCH` \| `OPTIONS` | `POST` | default request method |
| `http.headers` | String map | empty | default request headers; use prefixed entries such as `http.headers.Content-Type` or one packed map, not both |
| `http.oidc.service-account-email` | String | no OIDC token | OIDC token service account |
| `http.oidc.audience` | String | target URL | OIDC audience; requires the OIDC service account |
| `http.oauth.service-account-email` | String | no OAuth token | OAuth token service account |
| `http.oauth.scope` | String | Cloud Tasks default | OAuth scope; requires the OAuth service account |

Every explicitly configured `http.*` option is rejected when `target.type` is `app-engine`.

### App Engine request defaults

| Option | Type | Default | Maps to |
|---|---|---|---|
| `app-engine.relative-uri` | String | the non-null `relative-uri` metadata column | default path and optional query |
| `app-engine.method` | `POST` \| `GET` \| `HEAD` \| `PUT` \| `DELETE` \| `PATCH` \| `OPTIONS` | `POST` | default request method; only POST and PUT carry a body |
| `app-engine.headers` | String map | empty | default request headers; use prefixed entries such as `app-engine.headers.Content-Type` or one packed map, not both |
| `app-engine.service` | String | App Engine default | default task-level service selector |
| `app-engine.version` | String | App Engine default | default task-level version selector |
| `app-engine.instance` | String | an available instance | default task-level instance selector; requires manual scaling |

Every explicitly configured `app-engine.*` option is rejected when `target.type` is `http`.

### Writer tuning

These map one-for-one onto `CloudTasksWriterOptions.Builder`.
The [DataStream tuning section]({{< relref "docs/connectors/datastream/cloudtasks" >}}#tuning)
explains why `NOT_FOUND` has a separate short budget and why no setting controls dispatch rate.

| Option | Type | Default | Maps to |
|---|---|---|---|
| `sink.in-flight.max-tasks` | Integer | 1000 | `maxInFlightTasks` |
| `sink.channel-pool-size` | Integer | the client's single channel | `channelPoolSize`; rejected beside `emulator-endpoint` |
| `sink.recovery.initial-backoff` | Duration | 100 ms | `recoveryInitialBackoff` |
| `sink.recovery.max-backoff` | Duration | 10 s | `recoveryMaxBackoff` |
| `sink.recovery.max-attempts` | Integer | 8 | `recoveryMaxAttempts` |
| `sink.recovery.not-found.initial-backoff` | Duration | 500 ms | `notFoundRecoveryInitialBackoff` |
| `sink.recovery.not-found.max-backoff` | Duration | 2 s | `notFoundRecoveryMaxBackoff` |
| `sink.recovery.not-found.max-attempts` | Integer | 3 | `notFoundRecoveryMaxAttempts` |
| `sink.metrics.per-destination` | Boolean | `false` | `perDestinationMetrics` |
| `sink.parallelism` | Integer | job parallelism | the sink operator parallelism |

## Delivery guarantees and task identity

See [Write and key-collision semantics]({{< relref "docs/connectors/delivery-guarantees" >}}#write-and-key-collision-semantics)
for the Table and DataStream API comparison.

The connector accepts an insert-only Flink changelog.
That planner contract prevents update and delete rows from becoming new HTTP requests, but it does
not deduplicate task creation by itself.
Without writable `task-id` metadata, a replay creates another unnamed task.

Selecting `task-id` installs the DataStream sink's existing task-id extractor for every row.
A remembered duplicate returns `ALREADY_EXISTS`, which the sink treats as successful creation
without comparing the existing task's payload or schedule.
Cloud Tasks cannot update a task after creation, so reusing an ID does not replace the originally
created task definition, even when only the executed or deleted task's retained name remains.
The metadata value must identify an immutable logical task, or include a content or schedule
version when a changed row must create another task.

This is bounded effectively-once task creation, not exactly-once handler execution.
Cloud Tasks may dispatch the handler more than once, so the handler still needs an idempotent
operation or its own durable event ledger.

## Testing

Planner tests translate every source-backed statement through connector discovery and sink
validation without submitting a job or calling GCP.
Serializer and factory tests cover physical-column projection, target-family metadata, header
precedence, body methods, OIDC and OAuth selection, and task-ID extraction.
The emulator integration tests add HTTP dispatch, metadata overrides, named-task deduplication,
form bytes, and an inspectable App Engine task; the App Engine real-service suite covers behavior
the emulator cannot implement.
