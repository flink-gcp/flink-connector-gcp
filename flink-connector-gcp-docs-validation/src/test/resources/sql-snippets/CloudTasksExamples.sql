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
