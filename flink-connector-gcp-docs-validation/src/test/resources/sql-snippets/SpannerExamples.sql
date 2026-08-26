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

-- tag::lookup-join[]
CREATE TABLE account_events (
  account BIGINT,
  region AS CONCAT('region-', CAST(account AS STRING)),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '1',
  'fields.account.kind' = 'sequence',
  'fields.account.start' = '1',
  'fields.account.end' = '1'
);

CREATE TABLE accounts (
  region STRING,
  account BIGINT,
  name STRING,
  PRIMARY KEY (region, account) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'accounts'
);

SELECT e.region, e.account, a.name
FROM account_events AS e
LEFT JOIN accounts FOR SYSTEM_TIME AS OF e.proc_time AS a
  ON e.account = a.account AND e.region = a.region;
-- end::lookup-join[]

-- tag::batch-upsert[]
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE status_events (
  region STRING,
  account BIGINT,
  status STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '1000',
  'fields.region.length' = '16',
  'fields.status.length' = '64'
);

CREATE TABLE account_status (
  region STRING,
  account BIGINT,
  status STRING,
  PRIMARY KEY (region, account) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'account_status'
);

INSERT INTO account_status
SELECT region, account, MAX(status)
FROM status_events
GROUP BY region, account;
-- end::batch-upsert[]

-- tag::change-stream-source[]
CREATE TABLE order_changes (
  order_id BIGINT,
  status STRING,
  commit_timestamp TIMESTAMP_LTZ(9) METADATA FROM 'commit-timestamp' VIRTUAL,
  record_sequence STRING METADATA FROM 'sequence' VIRTUAL,
  server_transaction_id STRING METADATA FROM 'server-transaction-id' VIRTUAL,
  mod_number INT METADATA FROM 'mod-number' VIRTUAL,
  records_in_transaction BIGINT
    METADATA FROM 'number-of-records-in-transaction' VIRTUAL,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'spanner',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'database' = 'orders-db',
  'table' = 'orders',
  'scan.mode' = 'change-stream',
  'scan.change-stream.name' = 'order_changes',
  'scan.change-stream.changelog-mode' = 'upsert'
);

SELECT server_transaction_id, record_sequence, mod_number,
       records_in_transaction, order_id, status
FROM order_changes;
-- end::change-stream-source[]
