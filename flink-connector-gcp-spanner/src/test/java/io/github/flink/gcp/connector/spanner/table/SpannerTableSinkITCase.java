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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end SQL sink coverage against both dialects of the Spanner emulator. */
class SpannerTableSinkITCase extends AbstractSpannerEmulatorITCase {

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void insertsAndUpsertsThroughTheProductionFactory(Dialect dialect) throws Exception {
        SpannerDatabase database = database(dialect);
        TableEnvironment table =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        table.executeSql(tableDdl(database));

        table.executeSql("INSERT INTO target VALUES (1, 'first'), (2, 'keep')").await();
        // Separate jobs give writes for the same key a defined order even though BatchWrite may
        // apply mutations within one request in any order.
        table.executeSql("INSERT INTO target VALUES (1, 'second')").await();

        List<Struct> rows = query(database, "SELECT id, name FROM records ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(0).getString("name")).isEqualTo("second");
        assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
        assertThat(rows.get(1).getString("name")).isEqualTo("keep");
    }

    private static SpannerDatabase database(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE records (id bigint NOT NULL PRIMARY KEY, name varchar(64))");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE records (id INT64 NOT NULL, name STRING(64)) PRIMARY KEY (id)");
    }

    private static String tableDdl(SpannerDatabase database) {
        return "CREATE TABLE target (\n"
                + "  id BIGINT,\n"
                + "  name STRING,\n"
                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'spanner',\n"
                + "  'project' = '"
                + database.getProject()
                + "',\n"
                + "  'instance' = '"
                + database.getInstance()
                + "',\n"
                + "  'database' = '"
                + database.getDatabase()
                + "',\n"
                + "  'table' = 'records',\n"
                + "  'emulator-endpoint' = '"
                + emulatorEndpoint()
                + "'\n"
                + ")";
    }
}
