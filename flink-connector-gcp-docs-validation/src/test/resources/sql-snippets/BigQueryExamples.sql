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

-- tag::mysql-sink-table[]
CREATE TABLE current_mysql_orders (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING>
    METADATA FROM 'debezium-source-properties',
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_mysql_orders',
  'sink.cdc.enabled' = 'true',
  'sink.cdc.debezium-mysql.source-uuids' = '24bc7850-2c16-11e6-a073-0242ac110002',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min',
  'sink.cdc.table-reconciliation' = 'reconcile'
);
-- end::mysql-sink-table[]

-- tag::mysql-sink-insert[]
INSERT INTO current_mysql_orders
SELECT id, amount, source_properties FROM mysql_source_changes;
-- end::mysql-sink-insert[]

-- tag::debezium-sink-table[]
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING>
    METADATA FROM 'debezium-source-properties',
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
-- end::debezium-sink-table[]

-- tag::debezium-sink-insert[]
INSERT INTO current_orders
SELECT id, amount, source_properties FROM source_changes;
-- end::debezium-sink-insert[]

-- tag::ticdc-source-table[]
CREATE TABLE source_changes (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING>
    METADATA FROM 'value.source.properties' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'tidb_test.test.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'bigquery-cdc-orders',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'true'
);
-- end::ticdc-source-table[]

-- tag::ticdc-sink-and-insert[]
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING>
    METADATA FROM 'debezium-source-properties',
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.cdc.ticdc.cluster-id' = 'tidb-prod',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min',
  'sink.cdc.table-reconciliation' = 'reconcile'
);

INSERT INTO current_orders
SELECT id, amount, source_properties FROM source_changes;
-- end::ticdc-sink-and-insert[]

-- tag::debezium-json-sink-and-insert[]
CREATE TABLE current_orders (
  id STRING NOT NULL,
  amount BIGINT,
  source_properties MAP<STRING, STRING> METADATA FROM 'debezium-source-properties',
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);

INSERT INTO current_orders
SELECT id, amount, source_properties FROM source_changes;
-- end::debezium-json-sink-and-insert[]

-- tag::spanner-change-stream-source[]
SET 'execution.checkpointing.interval' = '1 min';

CREATE TABLE order_changes (
  OrderId BIGINT,
  Customer STRING,
  Status STRING,
  commit_timestamp TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
  PRIMARY KEY (OrderId) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'Orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert',
  'scan.startup.mode' = 'latest'
);
-- end::spanner-change-stream-source[]

-- tag::spanner-change-stream-sink[]
CREATE TABLE current_orders (
  OrderId BIGINT NOT NULL,
  Customer STRING,
  Status STRING,
  change_sequence ROW<commit_timestamp TIMESTAMP_LTZ(9), record_sequence STRING, mod_number INT>
    METADATA FROM 'spanner-change-sequence',
  PRIMARY KEY (OrderId) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_orders',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);
-- end::spanner-change-stream-sink[]

-- tag::spanner-change-stream-insert[]
INSERT INTO current_orders
SELECT OrderId, Customer, Status,
       ROW(commit_timestamp, record_sequence, mod_number)
FROM order_changes;
-- end::spanner-change-stream-insert[]

-- tag::bounded-source[]
CREATE TABLE people (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'my_dataset',
  'table' = 'people'
);

SELECT name
FROM people;
-- end::bounded-source[]

-- tag::table-sink[]
SET 'execution.checkpointing.interval' = '5 min';

CREATE TABLE analytics_events (
  event_id STRING,
  amount BIGINT
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'events'
);

INSERT INTO analytics_events
SELECT event_id, amount
FROM (VALUES ('event-1', CAST(42 AS BIGINT))) AS staged_events(event_id, amount);
-- end::table-sink[]

-- tag::table-sink-exactly-once-options[]
'sink.write-method' = 'storage-api-exactly-once'
-- end::table-sink-exactly-once-options[]

-- tag::table-sink-file-loads-options[]
'sink.write-method' = 'file-loads',
'sink.file-loads.staging-path' = 'gs://my-staging-bucket/flink'
-- end::table-sink-file-loads-options[]
