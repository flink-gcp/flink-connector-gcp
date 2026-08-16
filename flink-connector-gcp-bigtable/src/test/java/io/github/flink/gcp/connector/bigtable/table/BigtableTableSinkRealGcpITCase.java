/*
 * Copyright 2026 The flink-gcp authors
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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import io.github.flink.gcp.connector.bigtable.AbstractBigtableRealGcpITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The table sink against real Cloud Bigtable, driven through SQL with <b>no</b> {@code
 * emulator-endpoint} option.
 *
 * <p>Two things run nowhere else. The production client-construction path reached from a {@code
 * CREATE TABLE} — every other table test interpolates an emulator endpoint into the DDL, so the
 * branch that authenticates with application-default credentials is never taken there. And {@code
 * sink.app-profile-id}, which the emulator ignores entirely (ADR-0080), so only the service can say
 * whether the option reached the wire.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableTableSinkRealGcpITCase extends AbstractBigtableRealGcpITCase {

    private static final String APP_PROFILE = "flink-table-sink-it";

    private static String ddl(String tableId, String... keysAndValues) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", BigtableDynamicTableFactory.IDENTIFIER);
        options.put("project", PROJECT);
        options.put("instance", tableDestination(tableId).getInstance());
        options.put("table", tableId);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return "CREATE TABLE bt (\n"
                + "  rowkey STRING,\n"
                + "  "
                + FAMILY
                + " ROW<name STRING, amount BIGINT>,\n"
                + "  PRIMARY KEY (rowkey) NOT ENFORCED\n"
                + ") "
                + options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n  ", "WITH (\n  ", "\n)"));
    }

    private static TableEnvironment tableEnvironment() {
        return TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void writesThroughTheProductionClientPath() throws Exception {
        TableDestination table = createTable("table-sink");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(ddl("table-sink"));

        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('alice', CAST(7 AS BIGINT)))").await();

        assertThat(readRows(table))
                .extracting(row -> row.getKey().toStringUtf8())
                .containsExactly("r1");
        assertThat(readRows(table).get(0).getCells(FAMILY, "name").get(0).getValue().toStringUtf8())
                .isEqualTo("alice");
    }

    @Test
    void writesThroughTheConfiguredApplicationProfile() throws Exception {
        TableDestination table = createTable("table-sink-app-profile");
        createSingleClusterAppProfile(APP_PROFILE);
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(ddl("table-sink-app-profile", "sink.app-profile-id", APP_PROFILE));

        tEnv.executeSql("INSERT INTO bt VALUES ('r1', ROW('alice', CAST(7 AS BIGINT)))").await();

        assertThat(readRows(table)).hasSize(1);
    }

    @Test
    void failsWhenTheApplicationProfileDoesNotExist() {
        // The load-bearing half: a factory that dropped the option would pass the test above by
        // writing through the instance's default profile, and fail only here.
        createTable("table-sink-app-profile-missing");
        TableEnvironment tEnv = tableEnvironment();
        tEnv.executeSql(
                ddl("table-sink-app-profile-missing", "sink.app-profile-id", "no-such-profile"));

        // On the profile's name, not merely on "something threw": assertThatThrownBy already
        // fails when nothing does, so isNotNull() would accept a DDL parse error or an auth
        // failure and read as proof of something it never checked.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO bt VALUES ('r1', ROW('alice',"
                                                        + " CAST(7 AS BIGINT)))")
                                        .await())
                .hasStackTraceContaining("no-such-profile");
    }
}
