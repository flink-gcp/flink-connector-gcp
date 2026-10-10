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
CREATE TABLE customers (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  updated_at TIMESTAMP_LTZ(6),
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'Customer'
);

INSERT INTO customers SELECT customer_id, name, tier, updated_at FROM staged_customers;
-- end::overview[]

-- tag::id-key[]
CREATE TABLE invoices (
  invoice_id BIGINT NOT NULL,
  customer STRING,
  lines ARRAY<ROW<sku STRING, quantity BIGINT>>,
  PRIMARY KEY (invoice_id) NOT ENFORCED
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'database' = 'billing',
  'namespace' = 'tenant-a',
  'kind' = 'Invoice'
);
-- end::id-key[]

-- tag::unindexed[]
CREATE TABLE articles (
  article_id STRING NOT NULL,
  title STRING,
  body STRING,
  thumbnail BYTES,
  PRIMARY KEY (article_id) NOT ENFORCED
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'Article',
  'sink.unindexed-columns' = 'body;thumbnail'
);
-- end::unindexed[]

-- tag::append[]
CREATE TABLE page_views (
  path STRING,
  visitor STRING,
  viewed_at TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'PageView'
);

INSERT INTO page_views SELECT path, visitor, viewed_at FROM staged_views;
-- end::append[]

-- tag::scan[]
CREATE TABLE customer_snapshot (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  updated_at TIMESTAMP_LTZ(6),
  version BIGINT NOT NULL METADATA VIRTUAL,
  read_at TIMESTAMP_LTZ(6) NOT NULL METADATA FROM 'read-time' VIRTUAL,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'Customer',
  'scan.read-time' = '2026-10-04T00:00:00Z',
  'type-mismatch-policy' = 'null'
);

SELECT tier, COUNT(*) AS customers FROM customer_snapshot GROUP BY tier;
-- end::scan[]

-- tag::key-less-read[]
CREATE TABLE page_view_log (
  path STRING,
  visitor STRING,
  viewed_at TIMESTAMP_LTZ(3),
  view_id BIGINT METADATA FROM 'key-id' VIRTUAL
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'PageView'
);

SELECT view_id, path, viewed_at FROM page_view_log;
-- end::key-less-read[]

-- tag::lookup[]
CREATE TABLE customer_dim (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'datastore',
  'project' = 'my-project',
  'kind' = 'Customer',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

SELECT e.order_id, c.name, c.tier
FROM order_events AS e
LEFT JOIN customer_dim FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.customer_id = c.customer_id;
-- end::lookup[]
