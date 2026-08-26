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
ADD JAR '/path/to/flink-sql-connector-gcp-pubsub-0.1.0-SNAPSHOT.jar';

CREATE TABLE orders (
  order_id STRING,
  amount   INT
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'orders',
  'format'    = 'json'
);

INSERT INTO orders VALUES ('a-1', 10), ('a-2', 20);
-- end::sink[]

-- tag::source[]
CREATE TABLE incoming_orders (
  order_id     STRING,
  amount       INT,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
);

SELECT * FROM incoming_orders;
-- end::source[]
