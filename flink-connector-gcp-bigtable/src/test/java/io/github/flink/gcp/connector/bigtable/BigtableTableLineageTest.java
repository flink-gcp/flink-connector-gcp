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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.conditional.BigtableConditionalSink;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.BigtableReadModifyWriteSink;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;
import io.github.flink.gcp.connector.bigtable.table.source.BigtableChangeStreamEnvelopeSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.assertLogical;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.lineage;
import static io.github.flink.gcp.connector.bigtable.BigtableLineageTest.roundTrip;
import static org.assertj.core.api.Assertions.assertThat;

class BigtableTableLineageTest {
    @TempDir Path directory;

    @ParameterizedTest
    @EnumSource(WriteMode.class)
    void factoryCopiesAndSerializedRuntimesRetainEverySinkMode(WriteMode mode) throws Exception {
        var options = options();
        options.put("sink.write-mode", mode.toString());
        options.put("sink.parallelism", "4");
        if (mode == WriteMode.AGGREGATE) {
            options.put("sink.aggregate.column-family-types", "cf:int64-sum");
        }
        var dynamic =
                FactoryMocks.createTableSink(
                        schema(mode == WriteMode.APPEND ? DataTypes.STRING() : DataTypes.BIGINT()),
                        options);
        var copy = dynamic.copy();
        assertThat(copy).isEqualTo(dynamic).hasSameHashCodeAs(dynamic);
        for (var candidate : List.of(dynamic, copy)) {
            var provider =
                    (SinkV2Provider)
                            candidate.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
            assertThat(provider.getParallelism()).contains(4);
            Object sink = provider.createSink();
            Class<?> expected;
            switch (mode) {
                case INSERT_IF_ABSENT:
                    expected = BigtableConditionalSink.class;
                    break;
                case APPEND:
                case INCREMENT:
                    expected = BigtableReadModifyWriteSink.class;
                    break;
                default:
                    expected = BigtableMutateRowsSink.class;
            }
            assertThat(sink).isExactlyInstanceOf(expected);
            assertLogical(lineage(sink), FactoryMocks.IDENTIFIER.asSummaryString());
            assertLogical(lineage(roundTrip(sink)), FactoryMocks.IDENTIFIER.asSummaryString());
        }
    }

    @Test
    void factoryCopiesAndSerializedRuntimesRetainAllScanModes() throws Exception {
        for (String mode : List.of("scan", "envelope", "selected-cell")) {
            for (boolean bounded : List.of(false, true)) {
                var options = options();
                ResolvedSchema schema = schema(DataTypes.BIGINT());
                options.put("scan.parallelism", "2");
                if (!mode.equals("scan")) {
                    options.put("scan.mode", "change-stream");
                    options.put("scan.change-stream.changelog-mode", mode);
                    options.put("scan.app-profile-id", "single-cluster");
                    if (bounded) {
                        options.put("scan.bounded.timestamp-millis", "1767312000000");
                    }
                    if (mode.equals("envelope")) {
                        schema =
                                ResolvedSchema.physical(
                                        DataType.getFieldNames(
                                                BigtableChangeStreamEnvelopeSchema.DATA_TYPE),
                                        DataType.getFieldDataTypes(
                                                BigtableChangeStreamEnvelopeSchema.DATA_TYPE));
                    } else {
                        schema =
                                new ResolvedSchema(
                                        List.of(
                                                Column.physical(
                                                        "rowkey", DataTypes.STRING().notNull()),
                                                Column.physical("score", DataTypes.BIGINT())),
                                        List.of(),
                                        UniqueConstraint.primaryKey("pk", List.of("rowkey")));
                        options.put("scan.change-stream.selected-cell.family", "cf");
                        options.put("scan.change-stream.selected-cell.qualifier-base64", "cQ==");
                        options.put(
                                "scan.change-stream.selected-cell.source-cluster-id", "cluster");
                        options.put("value.format", "json");
                    }
                } else {
                    options.put("scan.row-prefix", "r");
                }
                var dynamic = FactoryMocks.createTableSource(schema, options);
                var copy = dynamic.copy();
                assertThat(copy).isEqualTo(dynamic).hasSameHashCodeAs(dynamic);
                for (var candidate : List.of(dynamic, copy)) {
                    var provider =
                            (SourceProvider)
                                    ((ScanTableSource) candidate)
                                            .getScanRuntimeProvider(
                                                    ScanRuntimeProviderContext.INSTANCE);
                    assertThat(provider.getParallelism()).contains(2);
                    Source<?, ?, ?> source = provider.createSource();
                    for (Source<?, ?, ?> runtime : List.of(source, roundTrip(source))) {
                        assertLogical(lineage(runtime), FactoryMocks.IDENTIFIER.asSummaryString());
                        assertThat(((SourceLineageVertex) lineage(runtime)).boundedness())
                                .isEqualTo(runtime.getBoundedness());
                    }
                }
            }
        }
    }

    private Map<String, String> options() {
        return new HashMap<>(
                Map.of(
                        "connector",
                        "bigtable",
                        "project",
                        "lineage-project",
                        "instance",
                        "lineage-instance",
                        "table",
                        "events",
                        "service-account-key-file",
                        directory.resolve("missing.json").toString()));
    }

    private static ResolvedSchema schema(DataType cell) {
        return ResolvedSchema.of(
                Column.physical("rowkey", DataTypes.STRING()),
                Column.physical("cf", DataTypes.ROW(DataTypes.FIELD("q", cell))));
    }
}
