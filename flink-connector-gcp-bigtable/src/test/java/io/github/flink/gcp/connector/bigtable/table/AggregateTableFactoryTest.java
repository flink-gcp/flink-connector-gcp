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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.table.sink.BigtableDynamicSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AggregateTableFactoryTest {
    private static final String TYPES = "sink.aggregate.column-family-types";

    private static ResolvedSchema schema(DataType type) {
        return ResolvedSchema.of(
                Column.physical("key", DataTypes.STRING()),
                Column.physical("cf", DataTypes.ROW(DataTypes.FIELD("q", type))));
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", "p");
        options.put("instance", "i");
        options.put("table", "t");
        options.put("emulator-endpoint", "localhost:8086");
        options.put("sink.write-mode", "aggregate");
        options.put(TYPES, "cf:int64-sum");
        return options;
    }

    @ParameterizedTest
    @ValueSource(strings = {"int64-sum", "int64-min", "int64-max", "int64-hll"})
    void mapsAllAggregateTypesAndRetainsThemThroughCopy(String type) {
        var options = options();
        options.put(TYPES, "cf:" + type);
        options.put("sink.create-disposition", "create-if-needed");
        options.put("sink.table-create.gc-rule.max-versions", "2");
        options.put("sink.batching.element-count-threshold", "7");
        BigtableDynamicSink sink =
                (BigtableDynamicSink)
                        FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options);
        assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink.copy()).isEqualTo(sink).hasSameHashCodeAs(sink);
        SinkV2Provider provider =
                (SinkV2Provider) sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        var runtime = (BigtableMutateRowsSink<?>) provider.createSink();
        assertThat(runtime.getConfig().getWriterOptions().getBatchElementCountThreshold())
                .isEqualTo(7);
        assertThat(
                        runtime.getConfig()
                                .getTableCreateOptions()
                                .getColumnFamilyTypes()
                                .get("cf")
                                .toString())
                .isEqualTo(type);
        assertThatThrownBy(
                        () ->
                                sink.getChangelogMode(
                                        ChangelogMode.newBuilder()
                                                .addContainedKind(RowKind.INSERT)
                                                .addContainedKind(RowKind.UPDATE_AFTER)
                                                .build()))
                .hasMessageContaining("requires INSERT-only input");
    }

    @Test
    void theSerializedFactoryRuntimeAddressesDistinctFamiliesAndANonLeadingKey() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(
                                "first", DataTypes.ROW(DataTypes.FIELD("q", DataTypes.SMALLINT()))),
                        Column.physical("key", DataTypes.STRING()),
                        Column.physical(
                                "empty", DataTypes.ROW(DataTypes.FIELD("q", DataTypes.INT()))),
                        Column.physical(
                                "last", DataTypes.ROW(DataTypes.FIELD("q", DataTypes.BIGINT()))));
        var options = options();
        options.put(TYPES, "first:int64-sum,empty:int64-min,last:int64-hll");
        BigtableDynamicSink sink =
                (BigtableDynamicSink) FactoryMocks.createTableSink(schema, options);
        SinkV2Provider provider =
                (SinkV2Provider) sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        byte[] bytes =
                org.apache.flink.util.InstantiationUtil.serializeObject(provider.createSink());
        BigtableMutateRowsSink<org.apache.flink.table.data.RowData> restored =
                org.apache.flink.util.InstantiationUtil.deserializeObject(
                        bytes, getClass().getClassLoader());
        var input =
                org.apache.flink.table.data.GenericRowData.of(
                        org.apache.flink.table.data.GenericRowData.of((short) -3),
                        org.apache.flink.table.data.StringData.fromString("r"),
                        null,
                        org.apache.flink.table.data.GenericRowData.of(17L));
        var entry = restored.getConfig().getSerializer().serialize(input, null).toProto();
        assertThat(entry.getRowKey().toStringUtf8()).isEqualTo("r");
        assertThat(entry.getMutationsList())
                .hasSize(2)
                .allSatisfy(mutation -> assertThat(mutation.hasAddToCell()).isTrue());
        assertThat(entry.getMutations(0).getAddToCell().getFamilyName()).isEqualTo("first");
        assertThat(entry.getMutations(0).getAddToCell().getInput().getIntValue()).isEqualTo(-3);
        assertThat(entry.getMutations(1).getAddToCell().getFamilyName()).isEqualTo("last");
        assertThat(entry.getMutations(1).getAddToCell().getInput().getIntValue()).isEqualTo(17);
    }

    @Test
    void rejectsNonIntegerQualifiersThroughTheFactory() {
        for (DataType type :
                new DataType[] {
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.DECIMAL(10, 0),
                    DataTypes.DOUBLE()
                }) {
            assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(type), options()))
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(
                            "Aggregate column 'cf.q' requires TINYINT, SMALLINT, INT or BIGINT");
        }
    }

    @Test
    void namesMissingExtraAndInvalidFamilyDeclarations() {
        var options = options();
        options.remove(TYPES);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("requires '" + TYPES + "'");
        for (String value : new String[] {"cf:raw", "cf:unknown", "other:int64-sum"}) {
            options.put(TYPES, value);
            assertThatThrownBy(
                            () -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                    .hasStackTraceContaining("must declare column family 'cf'");
        }
        options.put(TYPES, "cf:int64-sum,other:int64-max");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("names undeclared column family 'other'");
    }

    @Test
    void refusesUnusedModeOptionsAndSourceReuse() {
        var options = options();
        options.put("sink.write-mode", "upsert");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("requires 'sink.write-mode' = 'aggregate'");
        options.put("sink.write-mode", "aggregate");
        options.put("sink.insert-only-input-mode", "insert-only");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("Option 'sink.insert-only-input-mode' cannot be used");
        options.remove("sink.insert-only-input-mode");
        options.put("null-string-literal", "NULL");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("Option 'null-string-literal' cannot be used");
        options.remove("null-string-literal");
        assertThatThrownBy(
                        () -> FactoryMocks.createTableSource(schema(DataTypes.BIGINT()), options))
                .hasStackTraceContaining("Aggregate input tables are sink-only");
    }

    @Test
    void sourceModeParsingUsesTheFactoryValidationDiagnostic() {
        var options = options();
        options.remove(TYPES);
        options.put("sink.write-mode", "malformed");
        assertThatThrownBy(
                        () -> FactoryMocks.createTableSource(schema(DataTypes.BIGINT()), options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Invalid value for option 'sink.write-mode'.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"upsert", "aggregate"})
    void sourceMapParsingPrecedesTheAggregateGuard(String mode) {
        var options = options();
        options.put("sink.write-mode", mode);
        options.put(TYPES, "malformed");
        assertThatThrownBy(
                        () -> FactoryMocks.createTableSource(schema(DataTypes.BIGINT()), options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Invalid value for option '" + TYPES + "'.");
    }
}
