---
title: Cloud Tasks
type: docs
weight: 30
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

# Cloud Tasks SQL Connector

The `cloud-tasks` table connector is a sink provided by the
`flink-connector-gcp-cloudtasks` module.
It maps onto the [DataStream sink]({{< relref "docs/connectors/datastream/cloudtasks" >}}), which
documents checkpoint behavior, retries, task naming and queue pacing.
This page defines how SQL rows become HTTP requests.

Cloud Tasks is an HTTP dispatch queue rather than an API-specific client.
The target API therefore decides whether a request uses JSON, another body format, a query string,
or no body at all.
SQL represents that split with a Flink format for the body and writable metadata for the rest of
the request.

```sql
CREATE TABLE order_tasks (
  order_id   STRING,
  amount     DECIMAL(12, 2),
  trace      MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at TIMESTAMP_LTZ(6)   METADATA FROM 'schedule-time',
  dedupe_key STRING              METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project'   = 'my-project',
  'location'  = 'asia-northeast1',
  'queue'     = 'orders',
  'http.url'  = 'https://orders-abc-an.a.run.app/tasks',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'http.oidc.service-account-email' =
    'dispatcher@my-project.iam.gserviceaccount.com',
  'http.oidc.audience' = 'https://orders-abc-an.a.run.app',
  'format' = 'json'
);

INSERT INTO order_tasks
SELECT order_id,
       amount,
       MAP['X-Trace-Id', trace_id],
       dispatch_at,
       order_id
FROM staged_orders;
```

The JSON format sees only `order_id` and `amount`.
The three metadata columns configure the request outside that body.

## Getting the connector onto the classpath

Use `flink-sql-connector-gcp-cloudtasks`, the relocated SQL uber-jar, for SQL Client deployments.
Place `flink-sql-connector-gcp-cloudtasks-<version>.jar` in Flink's `lib/` before starting the
cluster, or load it for one SQL Client session:

```sql
ADD JAR '/path/to/flink-sql-connector-gcp-cloudtasks-0.1.0-SNAPSHOT.jar';
```

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

## Body format and request metadata

`format` is any `SerializationFormatFactory` available on the job classpath, such as `json`, `csv`,
Avro or `raw` where its schema requirements are met.
The connector does not interpret bytes from those generic formats, so set the matching
`Content-Type` header for the target API.

The module also provides the `form-urlencoded` format for
`application/x-www-form-urlencoded` POST, PUT and PATCH bodies.
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
You do not need to set that header in `http.headers` or metadata.
An equivalent value is canonicalized, while a different value or a value with media-type parameters
is rejected as a conflict.

### Form request examples

The examples below show the HTTP request definition created from one `INSERT` row.
Cloud Tasks may dispatch that request more than once under the queue retry policy.

#### Repeated and joined array values

An `ARRAY<STRING>` column repeats its column name, while `ARRAY_JOIN` converts an array to one
scalar form value when the receiving API expects a delimiter.

```sql
CREATE TABLE form_tasks (
  order_id   STRING,
  note       STRING,
  tags       ARRAY<STRING>,
  categories STRING
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'forms',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'format' = 'form-urlencoded'
);

INSERT INTO form_tasks
VALUES (
  '42',
  '東京 + pickup',
  ARRAY['urgent', 'gift'],
  ARRAY_JOIN(ARRAY['books', 'sale'], ',')
);
```

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

```sql
INSERT INTO form_tasks
VALUES (
  '43',
  '',
  CAST(NULL AS ARRAY<STRING>),
  CAST(NULL AS STRING)
);
```

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

```sql
CREATE TABLE nested_form_tasks (
  `items[]`              ARRAY<STRING>,
  `customer.name`        STRING,
  `customer[postalCode]` STRING,
  `attributes[priority]` STRING
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'forms',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'format' = 'form-urlencoded'
);

INSERT INTO nested_form_tasks
SELECT items,
       customer.name,
       customer.postal_code,
       attributes['priority']
FROM incoming_orders;
```

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

```sql
CREATE TABLE json_parameter_tasks (
  payload STRING
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'forms',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'format' = 'form-urlencoded'
);

INSERT INTO json_parameter_tasks
SELECT JSON_OBJECT(
         KEY 'name' VALUE customer.name,
         KEY 'postalCode' VALUE customer.postal_code,
         KEY 'items' VALUE items
       )
FROM incoming_orders;
```

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

```sql
CREATE TABLE custom_form_tasks (
  body STRING
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'forms',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/x-www-form-urlencoded',
  'format' = 'raw'
);

-- TO_API_FORM is a scalar function supplied and registered by the job.
INSERT INTO custom_form_tasks
SELECT TO_API_FORM(items, attributes)
FROM incoming_orders;
```

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

Only `POST`, `PUT` and `PATCH` carry the encoded body because those are the only methods for which
Cloud Tasks accepts an `HttpRequest.body`.
`GET`, `HEAD`, `DELETE` and `OPTIONS` do not invoke the format for that row and create a task with no
body.

For a GET API, construct the query string as part of `http.url` or the `url` metadata column.
For example:

```sql
CREATE TABLE search_tasks (
  unused_body STRING,
  target_url STRING NOT NULL METADATA FROM 'url'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'search',
  'http.method' = 'GET',
  'format' = 'raw'
);

INSERT INTO search_tasks
SELECT '',
       'https://api.example.com/search?q=' || URL_ENCODE(query_text)
FROM pending_searches;
```

`URL_ENCODE` is a built-in Flink SQL function on the supported Flink 2.x line.
Flink 1.20 jobs must register an equivalent UTF-8 URL-encoding scalar UDF or construct the complete
URL before the row reaches SQL.
Concatenating an unescaped value changes the query syntax and is not made safe by Cloud Tasks.

Multipart bodies are not part of this connector contract.
A job that needs multipart can implement `CloudTasksSerializationSchema` through the DataStream API,
where the boundary and part encodings can be controlled explicitly.

### Writable metadata

| Metadata key | Type | Null behavior and precedence |
|---|---|---|
| `url` | `STRING` | A non-null value overrides `http.url`. If `http.url` is absent, this metadata column is required and must be declared `STRING NOT NULL` so every row has a target. The value must be an absolute `http://` or `https://` URL |
| `http-method` | `STRING` | A non-null value overrides `http.method`, case-insensitively. Accepted values are `POST`, `GET`, `HEAD`, `PUT`, `DELETE`, `PATCH` and `OPTIONS` |
| `headers` | `MAP<STRING, STRING>` | A null map adds no row headers. Row entries override fixed headers by case-insensitive name. A null or blank name, a null value, or duplicate case-insensitive names fail the record |
| `schedule-time` | `TIMESTAMP_LTZ(6)` | A non-null value becomes the task schedule time with microsecond precision. Null leaves the service default |
| `task-id` | `STRING` | Selecting this column enables named tasks for the entire sink. Every row must then supply a non-null, non-empty value. The sink hashes it with SHA-256 before composing the task name; a remembered duplicate is success |

Metadata is appended after the physical columns before the runtime serializer receives a row.
The connector projects the physical prefix before invoking the format, so request metadata never
appears in JSON, CSV or another body by accident.

## Authentication has two independent identities

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

The separate App Engine target type, `AppEngineHttpRequest`, has routing and regional behavior that
does not fit this HTTP metadata contract and is tracked in
[#608]({{< param BookRepo >}}/issues/608).

## Options

The queue is fixed for a table.
SQL exposes no dynamic queue metadata and never creates or configures a queue.
Rate limits, dispatch concurrency and delivery retries remain queue configuration.

An option left out leaves the corresponding DataStream builder setting at its existing default,
except `http.method`, whose SQL default is explicitly `POST`.

### Queue, body and writer identity

| Option | Type | Default | Maps to |
|---|---|---|---|
| `project` | String | **required** | the project component of `QueueDestination.of(...)` |
| `location` | String | **required** | the location component of `QueueDestination.of(...)` |
| `queue` | String | **required** | the queue component of `QueueDestination.of(...)` |
| `format` | String | **required** | serialization format discovery for physical columns; `form-urlencoded` provides the built-in form encoding described above |
| `service-account-key-file` | String | application-default credentials | `CloudTasksSinkBuilder.serviceAccountKeyFile(...)` |
| `emulator-endpoint` | String | production Cloud Tasks | `CloudTasksSinkBuilder.emulatorEndpoint(...)`; plaintext and no credentials |

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

### Writer tuning

These map one-for-one onto `CloudTasksWriterOptions.Builder`.
The [DataStream tuning section]({{< relref "docs/connectors/datastream/cloudtasks" >}}#tuning)
explains why `NOT_FOUND` has a separate short budget and why no setting controls dispatch rate.

| Option | Type | Default | Maps to |
|---|---|---|---|
| `sink.max-in-flight-tasks` | Integer | 1000 | `maxInFlightTasks` |
| `sink.retry.initial-backoff` | Duration | 100 ms | `retryInitialBackoff` |
| `sink.retry.max-backoff` | Duration | 10 s | `retryMaxBackoff` |
| `sink.retry.max-attempts` | Integer | 8 | `retryMaxAttempts` |
| `sink.not-found-retry.initial-backoff` | Duration | 500 ms | `notFoundInitialBackoff` |
| `sink.not-found-retry.max-backoff` | Duration | 2 s | `notFoundMaxBackoff` |
| `sink.not-found-retry.max-attempts` | Integer | 3 | `notFoundMaxAttempts` |
| `sink.metrics.per-destination` | Boolean | `false` | `perDestinationMetrics` |
| `sink.parallelism` | Integer | job parallelism | the sink operator parallelism |

The connector is insert-only.
It rejects an updating changelog rather than serializing `UPDATE_BEFORE` or `DELETE` rows as new
HTTP requests.
