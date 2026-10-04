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
  order_id STRING NOT NULL,
  customer STRING,
  total DOUBLE,
  updated_at TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'orders'
);

INSERT INTO orders SELECT order_id, customer, total, updated_at FROM staged_orders;
-- end::overview[]

-- tag::markers[]
CREATE TABLE stores (
  store_id STRING NOT NULL,
  name STRING,
  location ROW<latitude DOUBLE, longitude DOUBLE>,
  manager STRING,
  departments ARRAY<ROW<name STRING, head STRING>>,
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'database' = 'retail',
  'collection' = 'stores',
  'geo-point-field-paths' = 'location',
  'reference-field-paths' = 'manager;departments.head'
);
-- end::markers[]

-- tag::merge[]
CREATE TABLE order_status (
  order_id STRING NOT NULL,
  status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'orders',
  'sink.write-mode' = 'merge'
);
-- end::merge[]

-- tag::append[]
CREATE TABLE audit_events (
  actor STRING,
  action STRING,
  occurred_at TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'users/alice/audit'
);

INSERT INTO audit_events SELECT actor, action, occurred_at FROM staged_events;
-- end::append[]

-- tag::scan[]
CREATE TABLE order_snapshot (
  order_id STRING NOT NULL,
  customer STRING,
  total DOUBLE,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'orders',
  'scan.read-time' = '2026-10-04T00:00:00Z'
);

SELECT customer, SUM(total) FROM order_snapshot GROUP BY customer;
-- end::scan[]

-- tag::collection-group[]
CREATE TABLE all_audit_events (
  action STRING,
  path STRING METADATA FROM 'document-path' VIRTUAL,
  updated TIMESTAMP_LTZ(6) METADATA FROM 'update-time' VIRTUAL
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'audit',
  'scan.collection-group' = 'true',
  'type-mismatch-policy' = 'null'
);

SELECT path, action, updated FROM all_audit_events;
-- end::collection-group[]

-- tag::lookup[]
CREATE TABLE customers (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'firestore',
  'project' = 'my-project',
  'collection' = 'customers',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

SELECT e.order_id, c.name, c.tier
FROM order_events AS e
LEFT JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.customer_id = c.customer_id;
-- end::lookup[]
