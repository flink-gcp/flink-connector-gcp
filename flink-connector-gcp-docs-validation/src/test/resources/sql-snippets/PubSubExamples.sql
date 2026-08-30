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

-- tag::sink[]
CREATE TABLE outgoing_orders (
  order_id STRING,
  amount INT,
  attrs MAP<STRING, STRING> METADATA FROM 'attributes',
  ordering_key STRING METADATA FROM 'ordering-key'
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'topic' = 'orders',
  'format' = 'json',
  'sink.message-ordering.enabled' = 'true'
);

INSERT INTO outgoing_orders
VALUES
  ('a-1', 10, MAP['source', 'sql', 'profile-key', 'customer-1'], 'customer-1'),
  ('a-2', 20, MAP['source', 'sql', 'profile-key', 'customer-1'], 'customer-1');
-- end::sink[]

-- tag::source[]
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_orders (
  order_id STRING,
  amount INT,
  message_id STRING METADATA FROM 'message-id' VIRTUAL,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  attrs MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL,
  ordering_key STRING METADATA FROM 'ordering-key' VIRTUAL,
  subscription_name STRING METADATA FROM 'subscription' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.startup.mode' = 'continue-from-subscription',
  'scan.ordering-mode' = 'per-key'
);

SELECT order_id,
       amount,
       attrs['profile-key'] AS profile_key,
       ordering_key,
       message_id,
       publish_time,
       subscription_name
FROM incoming_orders;
-- end::source[]
