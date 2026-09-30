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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import com.google.bigtable.v2.Mutation;
import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the factory's handling of a {@code MAP} column family and of {@code
 * sink.map-family.update-mode}.
 *
 * <p>Rejections are asserted with {@code hasStackTraceContaining} on a phrase only this connector's
 * message carries, for the reason {@code BigtableDynamicTableFactoryTest} gives.
 */
class MapFamilyTableFactoryTest {

    private static final String UPDATE_MODE = "sink.map-family.update-mode";

    private static final ResolvedSchema MAP_ONLY =
            ResolvedSchema.of(
                    Column.physical("rowkey", DataTypes.STRING()),
                    Column.physical("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));

    private static final ResolvedSchema ROW_ONLY =
            ResolvedSchema.of(
                    Column.physical("rowkey", DataTypes.STRING()),
                    Column.physical("cf", DataTypes.ROW(DataTypes.FIELD("q", DataTypes.BIGINT()))));

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", "p");
        options.put("instance", "i");
        options.put("table", "t");
        return options;
    }

    /** The mutations the planned sink's serializer builds for one row of {@link #MAP_ONLY}. */
    @SuppressWarnings("unchecked")
    private static List<Mutation.MutationCase> mutationCases(DynamicTableSink tableSink)
            throws Exception {
        BigtableMutateRowsSink<RowData> sink =
                (BigtableMutateRowsSink<RowData>)
                        ((SinkV2Provider)
                                        tableSink.getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                                .createSink();
        Map<Object, Object> map = new HashMap<>();
        map.put(StringData.fromString("a"), StringData.fromString("x"));
        RowData row = GenericRowData.of(StringData.fromString("r1"), new GenericMapData(map));
        return sink
                .getConfig()
                .getSerializer()
                .serialize(row, null)
                .toProto()
                .getMutationsList()
                .stream()
                .map(Mutation::getMutationCase)
                .collect(Collectors.toList());
    }

    @Test
    void aMapFamilyAloneIsSomewhereToWrite() throws Exception {
        DynamicTableSink sink = FactoryMocks.createTableSink(MAP_ONLY, options());

        assertThat(sink).isInstanceOf(BigtableDynamicSink.class);
        assertThat(mutationCases(sink)).containsExactly(Mutation.MutationCase.SET_CELL);
    }

    @Test
    void replaceReachesTheSerializerAndSurvivesACopy() throws Exception {
        Map<String, String> options = options();
        options.put(UPDATE_MODE, "replace");

        DynamicTableSink replacing = FactoryMocks.createTableSink(MAP_ONLY, options);

        assertThat(mutationCases(replacing))
                .containsExactly(
                        Mutation.MutationCase.DELETE_FROM_FAMILY, Mutation.MutationCase.SET_CELL);
        assertThat(replacing.copy()).isEqualTo(replacing).hasSameHashCodeAs(replacing);
        assertThat(replacing).isNotEqualTo(FactoryMocks.createTableSink(MAP_ONLY, options()));
    }

    @Test
    void anExplicitMergeIsTheDefault() throws Exception {
        Map<String, String> options = options();
        options.put(UPDATE_MODE, "merge");

        DynamicTableSink merging = FactoryMocks.createTableSink(MAP_ONLY, options);

        assertThat(merging).isEqualTo(FactoryMocks.createTableSink(MAP_ONLY, options()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"merge", "replace"})
    void theOptionIsRejectedOnATableWithNoMapFamily(String value) {
        Map<String, String> options = options();
        options.put(UPDATE_MODE, value);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(ROW_ONLY, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("applies to MAP column families, and this table declares");
    }

    @ParameterizedTest
    @ValueSource(strings = {"insert-if-absent", "append", "increment", "conditional"})
    void theOptionIsRejectedUnderAWriteModeThatCannotReplace(String mode) {
        Map<String, String> options = options();
        options.put("sink.write-mode", mode);
        options.put(UPDATE_MODE, "replace");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(MAP_ONLY, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option '" + UPDATE_MODE + "' cannot be used with 'sink.write-mode' = '");
    }

    @Test
    void keepLatestAcceptsTheOption() {
        Map<String, String> options = options();
        options.put("sink.write-mode", "keep-latest");
        options.put(UPDATE_MODE, "replace");

        assertThat(FactoryMocks.createTableSink(MAP_ONLY, options))
                .isInstanceOf(BigtableDynamicSink.class);
    }

    @Test
    void insertIfAbsentWritesAMapFamily() {
        Map<String, String> options = options();
        options.put("sink.write-mode", "insert-if-absent");

        assertThat(FactoryMocks.createTableSink(MAP_ONLY, options))
                .isInstanceOf(BigtableDynamicSink.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "increment"})
    void aReadModifyWriteModeRejectsAMapFamily(String mode) {
        Map<String, String> options = options();
        options.put("sink.write-mode", mode);

        assertThatThrownBy(() -> FactoryMocks.createTableSink(MAP_ONLY, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("does not support MAP column family 'm'");
    }

    @Test
    void theAggregateModeRejectsAMapFamily() {
        Map<String, String> options = options();
        options.put("sink.write-mode", "aggregate");
        options.put("sink.aggregate.column-family-types", "m:int64-sum");

        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(
                                        ResolvedSchema.of(
                                                Column.physical("rowkey", DataTypes.STRING()),
                                                Column.physical(
                                                        "m",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BIGINT()))),
                                        options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("does not support MAP column family 'm'");
    }

    @Test
    void theAggregateModeRejectsAMapFamilyBeforeAskingForItsTypes() {
        Map<String, String> options = options();
        options.put("sink.write-mode", "aggregate");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(MAP_ONLY, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("does not support MAP column family 'm'");
    }

    @Test
    void theAggregateModeRejectsTheOption() {
        Map<String, String> options = options();
        options.put("sink.write-mode", "aggregate");
        options.put("sink.aggregate.column-family-types", "cf:int64-sum");
        options.put(UPDATE_MODE, "merge");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(ROW_ONLY, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option '" + UPDATE_MODE + "' cannot be used with 'sink.write-mode' = '");
    }

    @Test
    void aSourceReadsAMapFamily() {
        assertThat(FactoryMocks.createTableSource(MAP_ONLY, options())).isNotNull();
    }
}
