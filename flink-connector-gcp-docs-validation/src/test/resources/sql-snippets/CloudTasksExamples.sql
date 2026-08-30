-- Copyright 2026 The flink-gcp authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- tag::app-engine-target[]
CREATE TABLE app_engine_tasks (
  payload        STRING,
  target_path    STRING NOT NULL METADATA FROM 'relative-uri',
  service_name   STRING          METADATA FROM 'app-engine-service',
  version_name   STRING          METADATA FROM 'app-engine-version',
  instance_name  STRING          METADATA FROM 'app-engine-instance',
  dedupe_key     STRING          METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'app-engine-orders',
  'target.type' = 'app-engine',
  'app-engine.method' = 'POST',
  'app-engine.headers.Content-Type' = 'application/json',
  'format' = 'json'
);

INSERT INTO app_engine_tasks
VALUES ('ready', '/tasks/42?source=sql', 'worker', 'v2', CAST(NULL AS STRING), 'order-42');
-- end::app-engine-target[]

-- tag::cloud-run-function[]
CREATE TABLE function_tasks (
  order_id     STRING,
  amount       DECIMAL(12, 2),
  trace        MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at  TIMESTAMP_LTZ(6)    METADATA FROM 'schedule-time',
  dedupe_key   STRING              METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'functions',
  'http.url' = 'https://process-order-abc-an.a.run.app/tasks',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'http.oidc.service-account-email' =
    'dispatcher@my-project.iam.gserviceaccount.com',
  'http.oidc.audience' = 'https://process-order-abc-an.a.run.app',
  'format' = 'json'
);

INSERT INTO function_tasks
VALUES (
  'o-42',
  CAST(19.95 AS DECIMAL(12, 2)),
  MAP['X-Trace-Id', 'trace-42'],
  CAST(CURRENT_TIMESTAMP + INTERVAL '5' MINUTE AS TIMESTAMP_LTZ(6)),
  'order-o-42'
);
-- end::cloud-run-function[]

-- tag::external-api[]
CREATE TABLE external_api_tasks (
  order_id       STRING,
  status         STRING,
  amount         DECIMAL(12, 2),
  target_url     STRING NOT NULL    METADATA FROM 'url',
  request_method STRING             METADATA FROM 'http-method',
  request_headers MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at    TIMESTAMP_LTZ(6)   METADATA FROM 'schedule-time',
  dedupe_key     STRING             METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'partner-api',
  'format' = 'json'
);

INSERT INTO external_api_tasks
VALUES (
  'o-42',
  'ready',
  CAST(19.95 AS DECIMAL(12, 2)),
  'https://partner.example.com/orders/o-42',
  'PATCH',
  MAP['Content-Type', 'application/json', 'X-Tenant', 'north'],
  CAST(CURRENT_TIMESTAMP + INTERVAL '5' MINUTE AS TIMESTAMP_LTZ(6)),
  'partner-order-o-42-v1'
);
-- end::external-api[]

-- tag::pubsub-bigtable-cloud-tasks[]
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_orders (
  event_id    STRING,
  customer_id STRING,
  order_id    STRING,
  amount      DECIMAL(12, 2),
  dispatch_at TIMESTAMP_LTZ(6),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json'
);

CREATE TABLE customer_routes (
  rowkey  STRING,
  routing ROW<endpoint STRING, tenant STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'customer-routes',
  'lookup.async' = 'true'
);

CREATE TABLE enriched_order_tasks (
  event_id       STRING,
  order_id       STRING,
  amount         DECIMAL(12, 2),
  customer_id    STRING,
  target_url     STRING NOT NULL    METADATA FROM 'url',
  request_method STRING             METADATA FROM 'http-method',
  request_headers MAP<STRING, STRING> METADATA FROM 'headers',
  schedule_at    TIMESTAMP_LTZ(6)   METADATA FROM 'schedule-time',
  dedupe_key     STRING             METADATA FROM 'task-id'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'partner-api',
  'format' = 'json'
);

INSERT INTO enriched_order_tasks
SELECT e.event_id,
       e.order_id,
       e.amount,
       e.customer_id,
       r.routing.endpoint || '/orders/' || e.order_id,
       'POST',
       MAP['Content-Type', 'application/json', 'X-Tenant', r.routing.tenant],
       e.dispatch_at,
       e.event_id
FROM incoming_orders AS e
JOIN customer_routes FOR SYSTEM_TIME AS OF e.proc_time AS r
  ON e.customer_id = r.rowkey
WHERE e.event_id IS NOT NULL
  AND e.order_id IS NOT NULL
  AND r.routing.endpoint IS NOT NULL
  AND r.routing.tenant IS NOT NULL;
-- end::pubsub-bigtable-cloud-tasks[]

-- tag::nested-json[]
CREATE TABLE json_tasks (
  order_id STRING,
  customer ROW<name STRING, city STRING>,
  items ARRAY<ROW<sku STRING, quantity INT>>,
  attributes MAP<STRING, STRING>,
  note STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/orders',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'format' = 'json',
  'json.encode.ignore-null-fields' = 'false'
);

INSERT INTO json_tasks
VALUES (
  'o-42',
  CAST(ROW('Alice "A"', '東京') AS ROW<name STRING, city STRING>),
  ARRAY[
    CAST(ROW('book', 2) AS ROW<sku STRING, quantity INT>),
    CAST(ROW('pen', 1) AS ROW<sku STRING, quantity INT>)
  ],
  MAP['priority', 'high'],
  CAST(NULL AS STRING),
  MAP['X-Trace-Id', 'trace-42']
);
-- end::nested-json[]

-- tag::csv[]
CREATE TABLE csv_tasks (
  order_id STRING,
  note STRING,
  missing_value STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/import',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'text/csv; charset=UTF-8',
  'format' = 'csv',
  'csv.field-delimiter' = '|',
  'csv.quote-character' = '"',
  'csv.null-literal' = 'NULL'
);

INSERT INTO csv_tasks
SELECT '42',
       U&'line 1 | "quoted"\000Aline 2',
       CAST(NULL AS STRING),
       MAP['X-Trace-Id', 'trace-42'];
-- end::csv[]

-- tag::raw[]
CREATE TABLE raw_tasks (
  body STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/text',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'text/plain; charset=UTF-16BE',
  'format' = 'raw',
  'raw.charset' = 'UTF-16BE'
);

INSERT INTO raw_tasks
VALUES ('東京', MAP['X-Trace-Id', 'trace-42']);
-- end::raw[]

-- tag::avro[]
CREATE TABLE avro_tasks (
  order_id STRING NOT NULL,
  quantity INT NOT NULL,
  gift BOOLEAN NOT NULL,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'orders',
  'http.url' = 'https://api.example.com/avro-orders',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/octet-stream',
  'format' = 'avro',
  'avro.encoding' = 'binary'
);

INSERT INTO avro_tasks
VALUES ('o-7', 3, TRUE, MAP['X-Trace-Id', 'trace-7']);
-- end::avro[]
