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

The table factory is in `flink-connector-gcp-cloudtasks`.
A Maven or Gradle job can use that module and its transitive runtime dependencies directly.

The standalone shaded `flink-sql-connector-gcp-cloudtasks` jar is tracked separately in
[#607]({{< param BookRepo >}}/issues/607).
Until it exists, SQL Client deployments must provide the plain connector and its runtime dependency
tree rather than expecting one self-contained jar.

## Body format and request metadata

`format` is any `SerializationFormatFactory` available on the job classpath, such as `json`, `csv`,
Avro or `raw` where its schema requirements are met.
The connector does not interpret the encoded bytes.
Set the matching `Content-Type` header for the target API.

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

`URL_ENCODE` above represents whichever escaping function or UDF the job standardizes on.
Concatenating an unescaped value changes the query syntax and is not made safe by Cloud Tasks.

`application/x-www-form-urlencoded` needs field-name, ordering and escaping semantics beyond a
generic byte format and is tracked in [#606]({{< param BookRepo >}}/issues/606).
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
| `format` | String | **required** | generic serialization format discovery for the physical columns |
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
