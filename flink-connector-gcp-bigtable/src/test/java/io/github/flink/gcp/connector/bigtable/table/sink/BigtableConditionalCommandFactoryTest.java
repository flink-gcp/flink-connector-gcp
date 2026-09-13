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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.SCHEMA;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.cell;
import static io.github.flink.gcp.connector.bigtable.table.sink.ConditionalCommandTestSupport.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableConditionalCommandFactoryTest {
    @Test
    void usesADedicatedSinkWithUnusedComplexColumnsAndStableCopies() {
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, options());
        assertThat(sink).isInstanceOf(BigtableConditionalDynamicSink.class);
        assertThat(sink.copy()).isEqualTo(sink).hasSameHashCodeAs(sink);
        assertThat(FactoryMocks.createTableSink(SCHEMA, options())).isEqualTo(sink);
        assertThat(sink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
        Map<String, String> changed = options();
        changed.put("sink.conditional.then.0.operation", "delete-family");
        changed.put("sink.conditional.then.0.family", "cf");
        assertThat(FactoryMocks.createTableSink(SCHEMA, changed)).isNotEqualTo(sink);
        assertThatThrownBy(
                        () ->
                                sink.getChangelogMode(
                                        ChangelogMode.newBuilder()
                                                .addContainedKind(RowKind.INSERT)
                                                .addContainedKind(RowKind.UPDATE_AFTER)
                                                .build()))
                .hasMessageContaining("requires INSERT-only input");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "sink.conditional.row-key-column",
                "sink.conditional.predicate",
                "sink.conditional.then.0.operation"
            })
    void rejectsMissingRequiredOptions(String key) {
        Map<String, String> options = options();
        options.remove(key);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(
                        key.equals("sink.conditional.then.0.operation")
                                ? "must contain at least one mutation"
                                : "Option '" + key + "' is required");
    }

    @ParameterizedTest
    @MethodSource("invalidDefinitions")
    void rejectsInvalidDefinitionsUnderTheirOptionKeys(
            Map<String, String> changes, String diagnostic) {
        Map<String, String> options = options();
        options.putAll(changes);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(diagnostic);
    }

    static Stream<Arguments> invalidDefinitions() {
        return Stream.of(
                invalid("row-key-column", "absent", "references absent physical column 'absent'"),
                invalid("row-key-column", "unused", "has an unsupported column type"),
                invalid(
                        "predicate",
                        "chain",
                        "must be row-exists, cell-exists or latest-cell-value-equals"),
                invalid(
                        "predicate.family",
                        "cf",
                        "'sink.conditional.predicate.family' is unknown or inapplicable"),
                invalid(
                        "then.00.operation",
                        "delete-row",
                        "requires a canonical nonnegative index"),
                invalid(
                        "then.-1.operation",
                        "delete-row",
                        "requires a canonical nonnegative index"),
                invalid("then.2.operation", "delete-row", "indexes must be consecutive from zero"),
                invalid("then.2147483648.operation", "delete-row", "index is too large"),
                invalid("then.0", "delete-row", "must have the form <index>.<attribute>"),
                invalid(
                        "then.0.unknown",
                        "x",
                        "'sink.conditional.then.0.unknown' is unknown or inapplicable"),
                invalid(
                        "then.0.family",
                        "cf",
                        "'sink.conditional.then.0.family' is unknown or inapplicable"),
                invalid("then.0.operation", "increment", "has an unsupported conditional mutation"),
                Arguments.of(
                        Map.of("sink.conditional.then", "0.operation:delete-row"),
                        "'sink.conditional.then' must use either the packed map syntax or prefixed map entries, not both."),
                Arguments.of(
                        Map.of("sink.conditional.predicate", "cell-exists"),
                        "'sink.conditional.predicate.family' is required"),
                Arguments.of(
                        Map.of(
                                "sink.conditional.predicate",
                                "cell-exists",
                                "sink.conditional.predicate.family",
                                "cf"),
                        "requires exactly one qualifier representation"),
                Arguments.of(
                        Map.of(
                                "sink.conditional.predicate",
                                "cell-exists",
                                "sink.conditional.predicate.family",
                                "cf",
                                "sink.conditional.predicate.qualifier",
                                "",
                                "sink.conditional.predicate.value-utf8",
                                ""),
                        "'sink.conditional.predicate.value-utf8' is unknown or inapplicable"));
    }

    private static Arguments invalid(String suffix, String value, String diagnostic) {
        return Arguments.of(Map.of("sink.conditional." + suffix, value), diagnostic);
    }

    @ParameterizedTest
    @MethodSource("invalidCellBindings")
    void validatesOperationBindingsAndLiteralsAtPlanning(
            String operation, Map<String, String> attributes, String diagnostic) {
        Map<String, String> options = options();
        cell(options, "then", 0, operation);
        attributes.forEach((key, value) -> options.put("sink.conditional.then.0." + key, value));
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(diagnostic);
    }

    static Stream<Arguments> invalidCellBindings() {
        return Stream.of(
                Arguments.of("set-cell", Map.of(), "requires exactly one value binding"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-column", "missing"),
                        "references absent physical column 'missing'"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-column", "unused"),
                        "has an unsupported column type"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-utf8", "", "value-base64", ""),
                        "conflicts with another value binding"),
                Arguments.of(
                        "set-cell",
                        Map.of("qualifier-base64", ""),
                        "requires exactly one qualifier representation"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-base64", "YQ"),
                        "must be canonical padded Base64"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-int64", "9223372036854775808"),
                        "must be a signed BIGINT literal"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-int64", "1.0"),
                        "must be a signed BIGINT literal"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-int64", "1", "timestamp-micros", "-2"),
                        "must be at least -1"),
                Arguments.of(
                        "set-cell",
                        Map.of("value-int64", "1", "timestamp-column", "value"),
                        "'sink.conditional.then.0.timestamp-column' requires BIGINT"),
                Arguments.of(
                        "set-cell",
                        Map.of(
                                "value-int64",
                                "1",
                                "timestamp-column",
                                "ts",
                                "timestamp-micros",
                                "0"),
                        "conflicts with timestamp-micros"),
                Arguments.of(
                        "add-to-cell",
                        Map.of("value-int64", "1"),
                        "requires an explicit literal or column binding"),
                Arguments.of(
                        "merge-to-cell",
                        Map.of("value-base64", "", "timestamp-micros", "-1"),
                        "must be at least 0"),
                Arguments.of(
                        "add-to-cell",
                        Map.of("value-column", "k", "timestamp-micros", "0"),
                        "requires BIGINT or BYTES aggregate input"),
                Arguments.of(
                        "delete-cells",
                        Map.of("start-timestamp-micros", "2", "end-timestamp-micros", "2"),
                        "must exceed"),
                Arguments.of(
                        "delete-cells", Map.of("end-timestamp-micros", "0"), "must be at least 1"),
                Arguments.of(
                        "delete-cells",
                        Map.of("start-timestamp-micros", "-1"),
                        "must be at least 0"),
                Arguments.of(
                        "delete-cells", Map.of("value-utf8", ""), "is unknown or inapplicable"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"predicate", "then.0", "otherwise.0"})
    void namesTheActualFamilyOptionAndItsUnderlyingBound(String section) {
        Map<String, String> options = options();
        if (section.equals("predicate")) {
            options.put("sink.conditional.predicate", "cell-exists");
            options.put("sink.conditional.predicate.qualifier", "q");
        } else {
            options.put("sink.conditional." + section + ".operation", "delete-family");
        }
        String key = "sink.conditional." + section + ".family";
        options.put(key, " ");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("Option '" + key + "' is invalid")
                .hasStackTraceContaining("family must not be blank");
    }

    @Test
    void aggregateColumnsRejectIntAndFixedWidthBinaryWithoutWidening() {
        for (var type : List.of(DataTypes.INT(), DataTypes.BINARY(8))) {
            Map<String, String> options = options();
            cell(options, "then", 0, "add-to-cell");
            options.put("sink.conditional.then.0.value-column", "delta");
            options.put("sink.conditional.then.0.timestamp-micros", "0");
            ResolvedSchema schema =
                    ResolvedSchema.of(
                            Column.physical("k", DataTypes.STRING()),
                            Column.physical("delta", type));
            assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, options))
                    .hasStackTraceContaining(
                            "Option 'sink.conditional.then.0.value-column' requires BIGINT or BYTES aggregate input");
        }
    }

    @Test
    void bindsExactPhysicalNamesIncludingDotsAndCase() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("Key.Part", DataTypes.BIGINT()),
                        Column.physical("key.part", DataTypes.ARRAY(DataTypes.STRING())));
        Map<String, String> options = options();
        options.put("sink.conditional.row-key-column", "Key.Part");
        assertThat(FactoryMocks.createTableSink(schema, options))
                .isInstanceOf(BigtableConditionalDynamicSink.class);
        options.put("sink.conditional.row-key-column", "KEY.PART");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, options))
                .hasStackTraceContaining("references absent physical column 'KEY.PART'");
    }

    @ParameterizedTest
    @MethodSource("inertOptions")
    void rejectsExplicitInertOptionsIncludingDefaultValues(String key, String value) {
        Map<String, String> options = options();
        options.put(key, value);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("Option '" + key + "'")
                .hasStackTraceContaining("cannot be used");
    }

    static Stream<Arguments> inertOptions() {
        return Stream.of(
                Arguments.of("sink.cell-timestamp.truncate-to-millis", "false"),
                Arguments.of("null-string-literal", "null"),
                Arguments.of("decode.trailing-bytes", "ignore"),
                Arguments.of("sink.batching.element-count-threshold", "100"),
                Arguments.of("sink.batching.request-byte-threshold", "1mb"),
                Arguments.of("sink.in-flight.max-entries", "10"),
                Arguments.of("sink.in-flight.max-bytes", "1mb"),
                Arguments.of("sink.create-disposition", "create-never"),
                Arguments.of("sink.insert-only-input-mode", "upsert"),
                Arguments.of("sink.recovery.max-attempts", "1"),
                Arguments.of("sink.aggregate.column-family-types", "cf:int64-sum"),
                Arguments.of("scan.mode", "bounded"),
                Arguments.of("scan.parallelism", "1"),
                Arguments.of("lookup.async", "true"));
    }

    @Test
    void rejectsPrimaryKeyAllMetadataAndStagedDelivery() {
        ResolvedSchema primaryKey =
                new ResolvedSchema(
                        SCHEMA.getColumns(),
                        List.of(),
                        UniqueConstraint.primaryKey("pk", List.of("k")));
        assertThatThrownBy(() -> FactoryMocks.createTableSink(primaryKey, options()))
                .hasStackTraceContaining("must not declare a PRIMARY KEY");
        for (boolean virtual : List.of(false, true)) {
            List<Column> columns = new ArrayList<>(SCHEMA.getColumns());
            columns.add(Column.metadata("meta", DataTypes.BIGINT(), "unrecognized", virtual));
            assertThatThrownBy(
                            () ->
                                    FactoryMocks.createTableSink(
                                            new ResolvedSchema(columns, List.of(), null),
                                            options()))
                    .hasStackTraceContaining("must not declare metadata columns");
        }
        Map<String, String> options = options();
        options.put("sink.delivery-guarantee", "exactly-once");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(
                        "'exactly-once' does not support 'sink.write-mode' = 'conditional'");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "upsert",
                "keep-latest",
                "insert-if-absent",
                "append",
                "increment",
                "aggregate"
            })
    void rejectsCommandOptionsInEveryOtherMode(String mode) {
        Map<String, String> options = options();
        options.put("sink.write-mode", mode);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("requires 'sink.write-mode' = 'conditional'");
    }

    @Test
    void rejectsReadingCommandDdlBeforeOrdinarySchemaValidation() {
        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options()))
                .hasStackTraceContaining("command tables are write-only");
    }

    @Test
    void acceptsBothMapSpellingsIndividuallyAndAnInactiveFailurePolicy() {
        Map<String, String> options = options();
        options.remove("sink.conditional.then.0.operation");
        options.put("sink.conditional.then", "0.operation:delete-row");
        options.put("sink.conditional.otherwise.0.operation", "delete-row");
        options.put("sink.conditional.empty-branch-policy", "fail");
        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(BigtableConditionalDynamicSink.class);
    }
}
