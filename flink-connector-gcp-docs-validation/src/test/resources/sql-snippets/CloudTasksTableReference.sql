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

-- tag::overview[]
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
-- end::overview[]

-- tag::add-jar[]
ADD JAR '/path/to/flink-sql-connector-gcp-cloudtasks-0.1.0-SNAPSHOT.jar';
-- end::add-jar[]

-- tag::repeated-form-values[]
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
-- end::repeated-form-values[]

-- tag::null-and-empty-form-values[]
INSERT INTO form_tasks
VALUES (
  '43',
  '',
  CAST(NULL AS ARRAY<STRING>),
  CAST(NULL AS STRING)
);
-- end::null-and-empty-form-values[]

-- tag::nested-form-names[]
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
-- end::nested-form-names[]

-- tag::json-form-field[]
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
-- end::json-form-field[]

-- tag::custom-form-body[]
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
-- end::custom-form-body[]

-- tag::get-request[]
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
-- end::get-request[]
