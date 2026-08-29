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
CREATE TABLE events (
  id STRING,
  amount BIGINT,
  event_ts TIMESTAMP_LTZ(6),
  attributes ROW<source STRING, version INT>
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'events'
);

INSERT INTO events
SELECT id, amount, event_ts, ROW(source, version) FROM staged_events;

SELECT id, amount FROM events WHERE amount > 0;
-- end::overview[]

-- tag::query-source[]
CREATE TABLE recent_events (
  id STRING,
  amount BIGINT
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'scan.query' = 'SELECT id, amount FROM `analytics.events` WHERE event_date = CURRENT_DATE()',
  'scan.query-location' = 'US'
);
-- end::query-source[]

-- tag::formatted-cdc-sequence[]
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  sequence STRING METADATA FROM 'change-sequence-number',
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min',
  'sink.cdc.table-reconciliation' = 'reconcile'
);

INSERT INTO current_orders
SELECT id, amount, formatted_sequence FROM ordered_changes;
-- end::formatted-cdc-sequence[]
