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

-- tag::conditional-outcome[]
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.async-scalar.max-attempts' = '1';
SET 'table.exec.async-scalar.max-concurrent-operations' = '10';
SET 'table.exec.async-scalar.timeout' = '1 min';
CREATE TEMPORARY SYSTEM FUNCTION BT_CHECK_AND_MUTATE AS
  'io.github.flink.gcp.connector.bigtable.table.function.BigtableCheckAndMutateFunction';

SET 'bigtable.functions.status.project' = 'my-project';
SET 'bigtable.functions.status.instance' = 'my-instance';
SET 'bigtable.functions.status.table' = 'orders';
SET 'bigtable.functions.status.app-profile-id' = 'single-row-transactions';
SET 'bigtable.functions.status.predicate.type' = 'latest-cell-value-equals';
SET 'bigtable.functions.status.predicate.family' = 'cf';
SET 'bigtable.functions.status.predicate.qualifier' = 'status';
SET 'bigtable.functions.status.predicate.value-argument' = '0';
SET 'bigtable.functions.status.then.0.operation' = 'set-cell';
SET 'bigtable.functions.status.then.0.family' = 'cf';
SET 'bigtable.functions.status.then.0.qualifier' = 'status';
SET 'bigtable.functions.status.then.0.value-argument' = '1';

SELECT order_id,
       BT_CHECK_AND_MUTATE('status', order_id, expected_status, next_status) AS matched
FROM (VALUES ('order-42', 'pending', 'paid'))
     AS changes(order_id, expected_status, next_status);
-- end::conditional-outcome[]

-- tag::changed-cells[]
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.async-scalar.max-attempts' = '1';
SET 'table.exec.async-scalar.max-concurrent-operations' = '10';
SET 'table.exec.async-scalar.timeout' = '1 min';
CREATE TEMPORARY SYSTEM FUNCTION BT_READ_MODIFY_WRITE AS
  'io.github.flink.gcp.connector.bigtable.table.function.BigtableReadModifyWriteFunction';

SET 'bigtable.functions.activity.project' = 'my-project';
SET 'bigtable.functions.activity.instance' = 'my-instance';
SET 'bigtable.functions.activity.table' = 'activity';
SET 'bigtable.functions.activity.app-profile-id' = 'single-row-transactions';
SET 'bigtable.functions.activity.rules.0.operation' = 'increment';
SET 'bigtable.functions.activity.rules.0.family' = 'cf';
SET 'bigtable.functions.activity.rules.0.qualifier' = 'visits';
SET 'bigtable.functions.activity.rules.0.value-argument' = '0';
SET 'bigtable.functions.activity.rules.1.operation' = 'append';
SET 'bigtable.functions.activity.rules.1.family' = 'cf';
SET 'bigtable.functions.activity.rules.1.qualifier' = 'history';
SET 'bigtable.functions.activity.rules.1.value-argument' = '1';

SELECT BT_READ_MODIFY_WRITE('activity', user_id, delta, event_text) AS changed
FROM (VALUES ('user-7', CAST(1 AS BIGINT), '|login'))
     AS events(user_id, delta, event_text);
-- end::changed-cells[]

-- tag::non-null-operands[]
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.async-scalar.max-attempts' = '1';
CREATE TEMPORARY SYSTEM FUNCTION BT_READ_MODIFY_WRITE AS
  'io.github.flink.gcp.connector.bigtable.table.function.BigtableReadModifyWriteFunction';

SET 'bigtable.functions.totals.project' = 'my-project';
SET 'bigtable.functions.totals.instance' = 'my-instance';
SET 'bigtable.functions.totals.table' = 'totals';
SET 'bigtable.functions.totals.app-profile-id' = 'single-row-transactions';
SET 'bigtable.functions.totals.rules.0.operation' = 'increment';
SET 'bigtable.functions.totals.rules.0.family' = 'cf';
SET 'bigtable.functions.totals.rules.0.qualifier' = 'count';
SET 'bigtable.functions.totals.rules.0.value-argument' = '0';

SELECT BT_READ_MODIFY_WRITE('totals', user_id, COALESCE(delta, CAST(0 AS BIGINT))) AS changed
FROM (VALUES ('user-7', CAST(2 AS BIGINT)), ('user-8', CAST(NULL AS BIGINT)))
     AS events(user_id, delta);
-- end::non-null-operands[]
