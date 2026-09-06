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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.lineage.PhysicalResourceFacet;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.TABLE;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.assertPhysical;
import static io.github.flink.gcp.connector.bigquery.source.BigQueryLineageTestFixtures.vertex;
import static org.assertj.core.api.Assertions.assertThat;

class BigQueryTableLineageTest {
    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.physical(
                    new String[] {"id"},
                    new org.apache.flink.table.types.DataType[] {DataTypes.BIGINT()});
    @TempDir Path directory;

    private Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigquery");
        options.put("project", TABLE.getProject());
        options.put("dataset", TABLE.getDataset());
        options.put("table", TABLE.getTable());
        options.put("service-account-key-file", directory.resolve("missing.json").toString());
        return options;
    }

    @ParameterizedTest
    @EnumSource(WriteMethod.class)
    void factoryAndCopyPassTheCatalogIdentityToEveryConcreteSink(WriteMethod method)
            throws Exception {
        Map<String, String> options = options();
        options.put("sink.write-method", method.toString());
        if (method == WriteMethod.FILE_LOADS) {
            options.put("sink.file-loads.staging-path", "gs://lineage-never-opened/staging");
        }
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, options);
        for (DynamicTableSink configured : List.of(sink, sink.copy())) {
            Sink<?> runtime =
                    ((SinkV2Provider)
                                    configured.getSinkRuntimeProvider(
                                            new SinkRuntimeProviderContext(false)))
                            .createSink();
            assertThat(vertex(InstantiationUtil.clone(runtime)).datasets())
                    .singleElement()
                    .satisfies(
                            dataset ->
                                    assertPhysical(
                                            dataset,
                                            FactoryMocks.IDENTIFIER.asSummaryString(),
                                            TABLE));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"table", "view", "query"})
    void factoryAndCopyPassTheCatalogIdentityToTheSource(String mode) throws Exception {
        Map<String, String> options = options();
        if (mode.equals("query")) {
            options.remove("dataset");
            options.remove("table");
            options.put("scan.query", "SELECT 1 AS id");
            options.put("scan.query-result-dataset", "scratch");
        } else if (mode.equals("view")) {
            options.put("scan.materialize-views", "true");
        }
        DynamicTableSource source = FactoryMocks.createTableSource(SCHEMA, options);
        for (DynamicTableSource configured : List.of(source, source.copy())) {
            Source<?, ?, ?> runtime =
                    ((SourceProvider)
                                    ((ScanTableSource) configured)
                                            .getScanRuntimeProvider(
                                                    ScanRuntimeProviderContext.INSTANCE))
                            .createSource();
            assertThat(vertex(InstantiationUtil.clone(runtime)).datasets())
                    .singleElement()
                    .satisfies(
                            dataset -> {
                                if (mode.equals("query")) {
                                    assertThat(dataset.name())
                                            .isEqualTo(FactoryMocks.IDENTIFIER.asSummaryString());
                                    assertThat(dataset.namespace()).isEqualTo("bigquery");
                                    assertThat(
                                                    ((PhysicalResourceFacet)
                                                                    dataset.facets().get("gcp"))
                                                            .resources())
                                            .isEmpty();
                                } else {
                                    assertPhysical(
                                            dataset,
                                            FactoryMocks.IDENTIFIER.asSummaryString(),
                                            TABLE);
                                }
                            });
        }
    }
}
