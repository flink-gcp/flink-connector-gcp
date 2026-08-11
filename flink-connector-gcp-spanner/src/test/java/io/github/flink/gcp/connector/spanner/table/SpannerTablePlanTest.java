/*
 * Copyright 2026 laughingman7743
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
}
