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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.datastore.Entity;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.table.source.DatastoreDynamicSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A scan given a pushed limit reads at most that many entities, and a limited statement returns
 * rows of the unlimited scan ({@code docs/adr/0201}), against the emulator.
 *
 * <p>The kind holds more entities than any limit here, and a fetch holds fewer than a limit, so a
 * bounded read spans pages. The table asks for four key ranges, which the builder refuses for a
 * query it reads as one split: a limited scan succeeds only because it reads one split without the
 * count.
 */
@Testcontainers
class DatastoreTableLimitPushDownITCase extends AbstractDatastoreEmulatorITCase {

    private static final int ENTITIES = 10;

    private static String kind;

    @BeforeAll
    static void writeEntities() {
        kind = uniqueKind();
        for (int i = 0; i < ENTITIES; i++) {
            client().put(Entity.newBuilder(key(kind, "e" + i)).set("n", (long) i).build());
        }
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "datastore");
        options.put("project", PROJECT);
        options.put("emulator-endpoint", emulatorEndpoint());
        options.put("kind", kind);
        options.put("scan.max-rows-per-fetch", "2");
        options.put("scan.partition.max-partitions", "4");
        return options;
    }

    private static String with() {
        StringBuilder with = new StringBuilder();
        for (Map.Entry<String, String> option : options().entrySet()) {
            if (with.length() > 0) {
                with.append(", ");
            }
            with.append('\'')
                    .append(option.getKey())
                    .append("' = '")
                    .append(option.getValue())
                    .append('\'');
        }
        return with.toString();
    }

    private static List<Row> collect(String query) throws Exception {
        TableEnvironment table =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        table.executeSql(
                "CREATE TABLE entities (id STRING NOT NULL, n BIGINT, PRIMARY KEY (id) NOT ENFORCED)"
                        + " WITH ("
                        + with()
                        + ")");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void aLimitedStatementReturnsRowsOfTheUnlimitedScan() throws Exception {
        String table = "entities";
        List<Row> all = collect("SELECT id, n FROM " + table);
        assertThat(all).hasSize(ENTITIES);

        assertThat(collect("SELECT id, n FROM " + table + " LIMIT 3"))
                .hasSize(3)
                .doesNotHaveDuplicates()
                .isSubsetOf(all);
        // The planner pushes the offset and the fetch together.
        assertThat(collect("SELECT id, n FROM " + table + " LIMIT 3 OFFSET 4"))
                .hasSize(3)
                .doesNotHaveDuplicates()
                .isSubsetOf(all);
        // A limit past the kind reads all of it.
        assertThat(collect("SELECT id, n FROM " + table + " LIMIT 100"))
                .containsExactlyInAnyOrderElementsOf(all);
    }

    @Test
    void theRuntimeSourceOfALimitedScanEmitsAtMostTheLimit() throws Exception {
        DatastoreDynamicSource dynamic =
                (DatastoreDynamicSource)
                        FactoryMocks.createTableSource(
                                ResolvedSchema.of(Column.physical("n", DataTypes.BIGINT())),
                                options());
        dynamic.applyLimit(3);
        @SuppressWarnings("unchecked")
        Source<RowData, ?, ?> source =
                (Source<RowData, ?, ?>)
                        ((SourceProvider)
                                        ((ScanTableSource) dynamic)
                                                .getScanRuntimeProvider(
                                                        ScanRuntimeProviderContext.INSTANCE))
                                .createSource();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        int emitted = 0;
        // Flink's own limit is not here: everything the source emits is counted.
        try (CloseableIterator<RowData> rows =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "limited")
                        .executeAndCollect()) {
            while (rows.hasNext()) {
                rows.next();
                emitted++;
            }
        }

        assertThat(emitted).isEqualTo(3);
    }
}
