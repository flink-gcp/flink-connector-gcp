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
CREATE TABLE profiles (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  usage ROW<requests BIGINT, last_seen TIMESTAMP_LTZ(3)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profiles
SELECT user_id, ROW(name, email), ROW(requests, last_seen) FROM staged_profiles;

-- A bounded scan of the same table; only the families the query reads leave the server.
SELECT rowkey, profile FROM profiles;
-- end::overview[]

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
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
  tie_breaker INT NOT NULL
    METADATA FROM 'tie-breaker' VIRTUAL,
  estimated_low_watermark TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'estimated-low-watermark' VIRTUAL
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'scan.mode' = 'change-stream',
  'scan.change-stream.changelog-mode' = 'envelope',
  'scan.app-profile-id' = 'single-cluster-profile'
);
-- end::change-stream-envelope[]

-- tag::unnest-change-stream-entries[]
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
-- end::unnest-change-stream-entries[]

-- tag::selected-cell-upserts[]
CREATE TABLE current_profiles (
  name STRING,
  profile_id STRING NOT NULL,
  score INT,
  source_cluster_id STRING
    METADATA FROM 'source-cluster-id' VIRTUAL,
  commit_timestamp TIMESTAMP_LTZ(9) NOT NULL
    METADATA FROM 'commit-timestamp' VIRTUAL,
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
  -- Base64 for the qualifier "current"; an empty qualifier is ''.
  'scan.change-stream.selected-cell.qualifier-base64' = 'Y3VycmVudA==',
  'scan.change-stream.selected-cell.source-cluster-id' = 'cluster-a',
  'value.format' = 'json'
);
-- end::selected-cell-upserts[]

-- tag::application-watermark[]
commit_timestamp TIMESTAMP_LTZ(3) NOT NULL
  METADATA FROM 'commit-timestamp' VIRTUAL,
WATERMARK FOR commit_timestamp AS commit_timestamp - INTERVAL '5' MINUTE
-- end::application-watermark[]

-- tag::lookup-join[]
SELECT e.event_id, p.profile.name
FROM events AS e
LEFT JOIN profiles FOR SYSTEM_TIME AS OF e.proc_time AS p
  ON e.user_id = p.rowkey;
-- end::lookup-join[]

-- tag::cell-timestamps[]
CREATE TABLE profiles_with_event_time (
  rowkey STRING,
  profile ROW<name STRING, email STRING>,
  cell_timestamp TIMESTAMP_LTZ(6) METADATA FROM 'timestamp',
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profiles_with_event_time
SELECT user_id, ROW(name, email), event_time FROM staged_profiles;
-- end::cell-timestamps[]

-- tag::insert-if-absent[]
CREATE TABLE new_users (
  row_key STRING,
  profile ROW<name STRING, email STRING>
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'users',
  'sink.app-profile-id' = 'single-cluster',
  'sink.write-mode' = 'insert-if-absent'
);

INSERT INTO new_users VALUES ('u1', ROW('Alice', 'alice@example.com'));
-- end::insert-if-absent[]
