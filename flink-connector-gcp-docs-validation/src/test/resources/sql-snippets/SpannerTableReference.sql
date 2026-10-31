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
CREATE TABLE orders (
  order_id BIGINT,
  customer STRING,
  total DECIMAL(38, 9),
  updated_at TIMESTAMP_LTZ(9),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders'
);

INSERT INTO orders SELECT order_id, customer, total, updated_at FROM staged_orders;

SELECT customer, total FROM orders;
-- end::overview[]

-- tag::lookup-equality[]
SELECT e.event_id, o.customer
FROM order_events AS e
LEFT JOIN orders FOR SYSTEM_TIME AS OF e.proc_time AS o
  ON e.order_id = o.order_id AND o.customer = 'Alice';
-- end::lookup-equality[]

-- tag::add-jar[]
ADD JAR '/path/to/flink-sql-connector-gcp-spanner-1.3.0.jar';
-- end::add-jar[]

-- tag::named-schema[]
CREATE TABLE sales_orders (
  order_id BIGINT,
  total DECIMAL(38, 9),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'named-schema' = 'sales',
  'table' = 'orders',
  'scan.index' = 'orders_by_total'
);
-- end::named-schema[]

-- tag::catalog[]
CREATE CATALOG sp WITH (
  'type' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'default-database' = 'orders-db'
);

USE CATALOG sp;
-- end::catalog[]

-- tag::change-stream[]
CREATE TABLE order_changes (
  order_id BIGINT,
  customer STRING,
  status STRING,
  commit_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  server_transaction_id STRING METADATA FROM 'server-transaction-id' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
  WATERMARK FOR commit_timestamp AS SOURCE_WATERMARK(),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert',
  'scan.startup.mode' = 'latest'
);
-- end::change-stream[]

-- tag::change-stream-coordinate-columns[]
CREATE TABLE order_coordinates (
  order_id BIGINT,
  status STRING,
  -- Ordinary physical columns: the source writes the record's values into them.
  commit_ts TIMESTAMP_LTZ(9) NOT NULL,
  record_sequence STRING NOT NULL,
  mod_number INT NOT NULL,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert',
  -- Name the columns above; any non-key column names of these types will do.
  'scan.change-stream.commit-timestamp-column' = 'commit_ts',
  'scan.change-stream.sequence-column' = 'record_sequence',
  'scan.change-stream.mod-number-column' = 'mod_number'
);
-- end::change-stream-coordinate-columns[]

-- tag::schema-markers[]
WITH (
  'uuid-field-paths' = 'id;related_ids',
  'json-field-paths' = 'metadata;payloads',
  'proto-type-names' = 'event:example.events.Event',
  'enum-type-names' = 'status:example.events.Status'
)
-- end::schema-markers[]
