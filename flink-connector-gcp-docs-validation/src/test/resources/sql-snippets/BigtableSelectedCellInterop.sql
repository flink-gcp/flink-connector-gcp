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

-- tag::producer[]
CREATE TABLE profile_writes (
  profile_id STRING,
  state ROW<`current` STRING>,
  PRIMARY KEY (profile_id) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'profiles',
  'sink.app-profile-id' = 'single-cluster-profile',
  'sink.write-mode' = 'keep-latest',
  'sink.insert-only-input-mode' = 'insert-only'
);

INSERT INTO profile_writes
VALUES (
  'profile#1',
  ROW(JSON_OBJECT('name' VALUE 'Alice', 'tier' VALUE 'gold' NULL ON NULL))
);
-- end::producer[]

-- tag::replacement[]
INSERT INTO profile_writes
VALUES (
  'profile#1',
  ROW(JSON_OBJECT(
    'name' VALUE CAST(NULL AS STRING),
    'tier' VALUE 'silver' NULL ON NULL
  ))
);
-- end::replacement[]

-- tag::consumer[]
CREATE TABLE profile_changes (
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
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1788652800000',
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'true',
  'value.json.ignore-parse-errors' = 'false'
);

SELECT profile_id, name, tier FROM profile_changes;
-- end::consumer[]
