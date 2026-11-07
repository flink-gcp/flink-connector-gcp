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

package io.github.flink.gcp.connector.firestore.table;

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

import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreDynamicSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A scan given a pushed limit reads at most that many documents, and a limited statement returns
 * rows of the unlimited scan ({@code docs/adr/0201}), against the emulator.
 *
 * <p>The collection holds more documents than any limit here, and a fetch holds fewer than a limit,
 * so a bounded read spans pages. A collection-group table asks for four partitions, which the
 * emulator cannot plan ({@code PartitionQuery} is unimplemented there): its limited scan succeeds
 * only because a limit reads the group as one query.
 */
@Testcontainers
class FirestoreTableLimitPushDownITCase extends AbstractFirestoreEmulatorITCase {

    private static final int DOCUMENTS = 10;

    private static String collection;

    @BeforeAll
    static void writeDocuments() throws Exception {
        collection = uniqueCollection();
        for (int i = 0; i < DOCUMENTS; i++) {
            client().document(collection + "/d" + i).set(Map.of("n", (long) i)).get();
        }
    }

    private static Map<String, String> options(boolean collectionGroup) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "firestore");
        options.put("project", PROJECT);
        options.put("emulator-endpoint", emulatorEndpoint());
        options.put("collection", collection);
        options.put("scan.max-rows-per-fetch", "2");
        if (collectionGroup) {
            options.put("scan.collection-group", "true");
            options.put("scan.partition.max-partitions", "4");
        }
        return options;
    }

    private static String with(boolean collectionGroup) {
        StringBuilder with = new StringBuilder();
        for (Map.Entry<String, String> option : options(collectionGroup).entrySet()) {
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
                "CREATE TABLE single (path STRING METADATA FROM 'document-path' VIRTUAL, n BIGINT)"
                        + " WITH ("
                        + with(false)
                        + ")");
        table.executeSql(
                "CREATE TABLE grouped (path STRING METADATA FROM 'document-path' VIRTUAL, n BIGINT)"
                        + " WITH ("
                        + with(true)
                        + ")");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.executeSql(query).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }

    @ParameterizedTest
    @ValueSource(strings = {"single", "grouped"})
    void aLimitedStatementReturnsRowsOfTheUnlimitedScan(String table) throws Exception {
        // The group's id is unique to the class, so it holds exactly the collection's documents.
        List<Row> all = collect("SELECT path, n FROM single");
        assertThat(all).hasSize(DOCUMENTS);

        assertThat(collect("SELECT path, n FROM " + table + " LIMIT 3"))
                .hasSize(3)
                .doesNotHaveDuplicates()
                .isSubsetOf(all);
        // The planner pushes the offset and the fetch together.
        assertThat(collect("SELECT path, n FROM " + table + " LIMIT 3 OFFSET 4"))
                .hasSize(3)
                .doesNotHaveDuplicates()
                .isSubsetOf(all);
        // A limit past the collection reads all of it.
        assertThat(collect("SELECT path, n FROM " + table + " LIMIT 100"))
                .containsExactlyInAnyOrderElementsOf(all);
    }

    @Test
    void theUnlimitedCollectionGroupScanAsksForPartitions() {
        // The firing control of the grouped cases: without a limit the scan partitions, which
        // the emulator cannot plan.
        assertThatThrownBy(() -> collect("SELECT path, n FROM grouped"))
                .hasStackTraceContaining("Failed to plan the Firestore read of collection group")
                .hasStackTraceContaining("UNIMPLEMENTED");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void theRuntimeSourceOfALimitedScanEmitsAtMostTheLimit(boolean collectionGroup)
            throws Exception {
        FirestoreDynamicSource dynamic =
                (FirestoreDynamicSource)
                        FactoryMocks.createTableSource(
                                ResolvedSchema.of(Column.physical("n", DataTypes.BIGINT())),
                                options(collectionGroup));
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
