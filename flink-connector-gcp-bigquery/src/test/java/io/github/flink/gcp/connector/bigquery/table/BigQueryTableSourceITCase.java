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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.github.flink.gcp.connector.bigquery.source.AbstractBigQuerySourceEmulatorITCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The bounded {@code bigquery} table source, planned and executed against the emulator. */
class BigQueryTableSourceITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String TABLE = "table_source_people";

    @BeforeAll
    static void seed() throws Exception {
        createTable(
                TABLE,
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED)
                        .build(),
                Field.newBuilder("name", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        insert(TABLE, "id, name", "(1, 'Ada'), (2, 'Grace'), (3, 'Linus')");
    }

    @Test
    void scansProjectedRowsThroughThePlanner() throws Exception {
        TableEnvironment table = tableEnvironment();

        assertThat(rows(table, "SELECT name FROM people WHERE id >= 2 ORDER BY name"))
                .extracting(row -> row.getFieldAs(0).toString())
                .containsExactly("Grace", "Linus");
    }

    @Test
    void countsRowsThroughAnEmptyProjection() throws Exception {
        TableEnvironment table = tableEnvironment();

        assertThat(rows(table, "SELECT COUNT(*) FROM people"))
                .singleElement()
                .satisfies(row -> assertThat((Object) row.getField(0)).isEqualTo(3L));
    }

    private static TableEnvironment tableEnvironment() {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.getConfig().set("parallelism.default", "1");
        table.executeSql(
                "CREATE TABLE people (id BIGINT, name STRING) "
                        + TableDdl.withOptions(
                                PROJECT,
                                DATASET,
                                TABLE,
                                new String[] {"emulator-endpoint", grpcEndpoint()}));
        return table;
    }

    private static List<Row> rows(TableEnvironment table, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }
}
