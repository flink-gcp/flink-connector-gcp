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

-- tag::batch-source[]
CREATE TABLE profiles (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  usage ROW<requests BIGINT, last_seen TIMESTAMP_LTZ(3)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles'
);

SELECT rowkey, profile.name, usage.last_seen
FROM profiles
WHERE rowkey >= 'customer#1000' AND rowkey < 'customer#2000';
-- end::batch-source[]

-- tag::lookup-join[]
CREATE TABLE events (
  event_id STRING,
  user_id STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '100'
);

SELECT e.event_id, e.user_id, p.profile.name
FROM events AS e
LEFT JOIN profiles FOR SYSTEM_TIME AS OF e.proc_time AS p
  ON e.user_id = p.rowkey;
-- end::lookup-join[]

-- tag::change-stream-envelope[]
CREATE TABLE profile_mutations (
  row_key BYTES,
  entries ARRAY<ROW<
    entry_index INT,
    kind STRING,
    family STRING,
    qualifier ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `timestamp` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    `value` ROW<value_type STRING, bytes_value BYTES, long_value BIGINT>,
    delete_range ROW<
      start_bound STRING,
      start_micros BIGINT,
      end_bound STRING,
      end_micros BIGINT
    >
  >>,
  mutation_type STRING NOT NULL
    METADATA FROM 'mutation-type' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'envelope',
  'scan.app-profile-id' = 'single-cluster-profile'
);

SELECT
  row_key,
  mutation_type,
  commit_timestamp,
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
FROM profile_mutations
CROSS JOIN UNNEST(entries) AS entry_table(
  entry_index,
  kind,
  family,
  qualifier,
  entry_timestamp,
  entry_value,
  delete_range
);
-- end::change-stream-envelope[]

-- tag::batch-upsert[]
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE order_events (
  user_id STRING,
  amount BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '1000'
);

CREATE TABLE profile_totals (
  rowkey STRING,
  stats ROW<order_count BIGINT, total_amount BIGINT>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profile-totals'
);

INSERT INTO profile_totals
SELECT user_id, ROW(COUNT(*), SUM(amount))
FROM order_events
GROUP BY user_id;
-- end::batch-upsert[]

-- tag::insert-only-sink[]
CREATE TABLE profile_events (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profile_events
VALUES ('user#1001', ROW('Alice', 'alice@example.com'));
-- end::insert-only-sink[]

-- tag::cell-timestamp-sink[]
CREATE TABLE profile_versions (
  rowkey STRING,
  profile ROW<name STRING>,
  cell_timestamp TIMESTAMP_LTZ(6) METADATA FROM 'timestamp',
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profile_versions
VALUES (
  'user#1001',
  ROW('Alice'),
  CAST('2026-08-30 10:15:30.123000' AS TIMESTAMP_LTZ(6))
);
-- end::cell-timestamp-sink[]

-- tag::attribute-enrichment-pipeline[]
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE incoming_events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  message_id STRING METADATA FROM 'message-id' VIRTUAL,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'events-sub',
  'format' = 'json'
);

CREATE TABLE user_attributes (
  rowkey STRING,
  profile ROW<tier STRING, api_path STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'user-attributes',
  'scan.row-prefix' = 'user#',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

CREATE TABLE api_tasks (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  user_tier STRING,
  api_path STRING,
  source_message_id STRING,
  request_headers MAP<STRING, STRING> METADATA FROM 'headers'
) WITH (
  'connector' = 'cloud-tasks',
  'project' = 'my-project',
  'location' = 'asia-northeast1',
  'queue' = 'events',
  'http.url' = 'https://api.example.com/events',
  'http.method' = 'POST',
  'http.headers.Content-Type' = 'application/json',
  'format' = 'json'
);

INSERT INTO api_tasks
SELECT
  e.event_id,
  e.user_id,
  e.event_type,
  a.profile.tier,
  a.profile.api_path,
  e.message_id,
  MAP['X-Source-Message-Id', e.message_id]
FROM incoming_events AS e
JOIN user_attributes FOR SYSTEM_TIME AS OF e.proc_time AS a
  ON e.user_id = a.rowkey;
-- end::attribute-enrichment-pipeline[]

-- tag::selected-cell-bigquery-cdc[]
CREATE TABLE current_profiles (
  profile_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'selected-cell',
  'scan.app-profile-id' = 'single-cluster-profile',
  'scan.change-stream.selected-cell.family' = 'state',
  -- Base64 for the qualifier "current".
  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',
  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',
  'value.format' = 'json'
);

CREATE TABLE analytics_profiles (
  profile_id STRING NOT NULL,
  name STRING,
  tier STRING,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigquery',
  'project' = 'my-project',
  'dataset' = 'analytics',
  'table' = 'current_profiles',
  'sink.cdc.enabled' = 'true',
  'sink.create-disposition' = 'create-if-needed',
  'sink.cdc.max-staleness' = '10 min'
);

INSERT INTO analytics_profiles
SELECT profile_id, name, tier FROM current_profiles;
-- end::selected-cell-bigquery-cdc[]
