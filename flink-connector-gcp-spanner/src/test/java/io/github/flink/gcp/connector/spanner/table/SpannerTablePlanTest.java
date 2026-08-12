/*
 * Copyright 2026 laughingman7743
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Planner-level coverage for abilities applied to the production factory. */
class SpannerTablePlanTest {

    @Test
    void plannerPushesTopLevelProjectionIntoTheSource() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE people (id BIGINT, name STRING) WITH ("
                        + "'connector'='spanner', 'project'='p', 'instance'='i', "
                        + "'database'='d', 'table'='people')");

        assertThat(table.explainSql("SELECT name FROM people"))
                .contains(
                        "TableSourceScan(table=[[default_catalog, default_database, people, project=[name]]], fields=[name])");
    }

    @Test
    void temporalJoinUsesTheCompleteCompositePrimaryKeyLookup() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE facts (region STRING, account BIGINT, event_time AS PROCTIME()) "
                        + "WITH ('connector'='datagen', 'number-of-rows'='1')");
        table.executeSql(
                "CREATE TABLE accounts (region STRING, account BIGINT, name STRING, "
                        + "PRIMARY KEY (region, account) NOT ENFORCED) WITH ("
                        + "'connector'='spanner', 'project'='p', 'instance'='i', "
                        + "'database'='d', 'table'='accounts')");

        assertThat(
                        table.explainSql(
                                "SELECT a.name FROM facts AS f LEFT JOIN accounts "
                                        + "FOR SYSTEM_TIME AS OF f.event_time AS a "
                                        + "ON f.account = a.account AND f.region = a.region"))
                .contains("LookupJoin")
                .contains("region=region")
                .contains("account=account");
    }

    @Test
    void exactCompositePrimaryKeyFiltersAreConsumedByTheSource() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE records (tenant STRING, id BIGINT, name STRING, "
                        + "PRIMARY KEY (tenant, id) NOT ENFORCED) WITH ("
                        + "'connector'='spanner', 'project'='p', 'instance'='i', "
                        + "'database'='d', 'table'='records')");

        String plan =
                table.explainSql(
                        "SELECT name FROM records WHERE tenant = 'eu' AND id >= 7 AND id < 9");

        assertThat(plan)
                .contains("filter=[and(=(tenant")
                .contains(">=(id")
                .contains("<(id")
                .doesNotContain("Calc(select=[name]");
    }

    @Test
    void secondaryIndexPrefiltersRemainAsFlinkResiduals() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE records (tenant STRING, id BIGINT, score BIGINT, name STRING, "
                        + "PRIMARY KEY (tenant, id) NOT ENFORCED) WITH ("
                        + "'connector'='spanner', 'project'='p', 'instance'='i', "
                        + "'database'='d', 'table'='records', "
                        + "'scan.index'='records_by_score')");

        String plan = table.explainSql("SELECT name FROM records WHERE score = 5");

        assertThat(plan).contains("filter=[]").contains("Calc(select=[name], where=[=(score");
    }
}
