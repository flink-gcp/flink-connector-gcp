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
