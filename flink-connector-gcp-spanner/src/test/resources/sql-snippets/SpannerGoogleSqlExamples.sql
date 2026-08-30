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

-- tag::accounts-table-and-row[]
CREATE TABLE accounts (
  region STRING(16) NOT NULL,
  account INT64 NOT NULL,
  name STRING(128)
) PRIMARY KEY (region, account);

INSERT INTO accounts (region, account, name)
VALUES ('region-1', 1, 'Ada');
-- end::accounts-table-and-row[]

-- tag::account-status-table[]
CREATE TABLE account_status (
  region STRING(16) NOT NULL,
  account INT64 NOT NULL,
  status STRING(64)
) PRIMARY KEY (region, account);
-- end::account-status-table[]

-- tag::inventory-table-and-row[]
CREATE TABLE inventory (
  sku STRING(32) NOT NULL,
  quantity INT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL
) PRIMARY KEY (sku);

INSERT INTO inventory (sku, quantity, updated_at)
VALUES ('widget-1', 12, TIMESTAMP '2026-08-30T00:00:00Z');
-- end::inventory-table-and-row[]

-- tag::orders-change-stream-and-replica[]
CREATE TABLE source_orders (
  order_id INT64 NOT NULL,
  customer STRING(128),
  status STRING(32)
) PRIMARY KEY (order_id);

CREATE CHANGE STREAM source_order_changes FOR source_orders
OPTIONS (value_capture_type = 'NEW_ROW_AND_OLD_VALUES');

CREATE TABLE order_replica (
  order_id INT64 NOT NULL,
  customer STRING(128),
  status STRING(32),
  source_commit_timestamp TIMESTAMP NOT NULL,
  server_transaction_id STRING(MAX) NOT NULL,
  record_sequence STRING(MAX) NOT NULL,
  mod_number INT64 NOT NULL,
  source_table STRING(MAX) NOT NULL,
  mod_type STRING(16) NOT NULL
) PRIMARY KEY (order_id);
-- end::orders-change-stream-and-replica[]

-- tag::order-change-after-source-starts[]
INSERT INTO source_orders (order_id, customer, status)
VALUES (1, 'Ada', 'PENDING');
-- end::order-change-after-source-starts[]
