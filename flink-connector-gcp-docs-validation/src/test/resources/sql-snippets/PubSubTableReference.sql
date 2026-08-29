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

-- tag::sink-overview[]
CREATE TABLE orders (
  order_id STRING,
  amount   INT,
  attrs    MAP<STRING, STRING> METADATA FROM 'attributes',
  okey     STRING             METADATA FROM 'ordering-key'
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'orders',
  'format'    = 'json',
  'sink.message-ordering.enabled' = 'true'
);

INSERT INTO orders
SELECT order_id, amount, MAP['source', 'sql'], customer_id FROM staged_orders;
-- end::sink-overview[]

-- tag::source-overview[]
CREATE TABLE incoming_orders (
  order_id     STRING,
  amount       INT,
  message_id   STRING          METADATA FROM 'message-id'   VIRTUAL,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  attrs        MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
);

SELECT window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE incoming_orders, DESCRIPTOR(publish_time), INTERVAL '1' MINUTE))
GROUP BY window_start;
-- end::source-overview[]

-- tag::subscription-resource-spellings[]
'//pubsub.googleapis.com/'        || subscription  -- full resource name (IAM, Asset Inventory)
'https://pubsub.googleapis.com/v1/' || subscription  -- resource URI
-- end::subscription-resource-spellings[]

-- tag::timestamp-start-position[]
CREATE TABLE orders (
  id STRING,
  amount INT
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1735689600000'
);
-- end::timestamp-start-position[]

-- tag::single-subscription-auto-creation[]
CREATE TABLE orders (
  id STRING
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.auto-create.topics.orders-sub' = 'orders',
  'scan.auto-create.ack-deadline' = '60 s',
  'scan.auto-create.retain-acked-messages' = 'true'
);
-- end::single-subscription-auto-creation[]

-- tag::multiple-subscription-auto-creation[]
CREATE TABLE events (
  id STRING
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub;refunds-sub',
  'format' = 'json',
  'scan.auto-create.topics.orders-sub' = 'orders',
  'scan.auto-create.topics.refunds-sub' = 'refunds',
  'scan.auto-create.ack-deadline' = '60 s'
);
-- end::multiple-subscription-auto-creation[]

-- tag::packed-subscription-map[]
'subscription' = 'orders-sub;refunds-sub',
'scan.auto-create.topics' = 'orders-sub:orders,refunds-sub:refunds'
-- end::packed-subscription-map[]

-- tag::updating-query-rejected[]
INSERT INTO orders
SELECT id, CAST(COUNT(*) AS INT), MAP['source', 'sql'], CAST(NULL AS STRING)
FROM staged
GROUP BY id
-- Table sink ... doesn't support consuming update changes
-- end::updating-query-rejected[]
