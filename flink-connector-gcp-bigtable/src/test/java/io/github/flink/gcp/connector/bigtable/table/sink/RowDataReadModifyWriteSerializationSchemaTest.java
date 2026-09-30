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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteWireRules;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.WriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataReadModifyWriteSerializationSchemaTest {
    @Test
    void skipsNullCellsAndFamiliesAndRetainsRowKeyEncodingAcrossSerialization() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.APPEND);
        schema = InstantiationUtil.clone(schema, getClass().getClassLoader());
        ReadModifyWriteRequest request =
                schema.serialize(
                        GenericRowData.of(
                                GenericRowData.of(null, StringData.fromString("value")), 7L, null),
                        null);
        assertThat(request.getRules()).hasSize(1);
        assertThat(request.getRowKey())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 7}));
    }

    @Test
    void nullKeysAllNullInputsEmptyAppendsAndNonInsertRowsFail() {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.APPEND);
        assertThatThrownBy(() -> schema.serialize(GenericRowData.of(null, null, null), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("row-key column 'k' is null");
        assertThatThrownBy(() -> schema.serialize(GenericRowData.of(null, 1L, null), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("at least one nonnull cell");
        assertThatThrownBy(
                        () ->
                                schema.serialize(
                                        GenericRowData.of(GenericRowData.of(null, null), 1L, null),
                                        null))
                .hasMessageContaining("at least one nonnull cell");
        assertThatThrownBy(
                        () ->
                                schema.serialize(
                                        GenericRowData.of(
                                                GenericRowData.of(StringData.fromString(""), null),
                                                1L,
                                                null),
                                        null))
                .hasMessageContaining("cf.a")
                .hasMessageContaining("empty");
        GenericRowData update =
                GenericRowData.of(GenericRowData.of(StringData.fromString("v"), null), 1L, null);
        update.setRowKind(RowKind.UPDATE_AFTER);
        assertThatThrownBy(() -> schema.serialize(update, null))
                .hasMessageContaining("INSERT-only");
    }

    @Test
    void zeroAndNegativeIncrementsAreNotSkipped() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema = schema(WriteMode.INCREMENT);
        ReadModifyWriteRequest request =
                schema.serialize(GenericRowData.of(GenericRowData.of(0L, -1L), 1L, null), null);
        assertThat(request.getRules()).hasSize(2);
    }

    @Test
    void aMapFamilyIncrementsOnePerNonNullEntryInDeclarationOrder() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema =
                InstantiationUtil.clone(
                        mapSchema(WriteMode.INCREMENT), getClass().getClassLoader());
        ReadModifyWriteRequest request =
                schema.serialize(
                        GenericRowData.of(
                                StringData.fromString("r"),
                                GenericRowData.of(7L),
                                map(key("day-1"), 3L, key("skipped"), null, key("day-2"), -1L)),
                        null);
        assertThat(ReadModifyWriteWireRules.sentRules(request))
                .containsExactly(
                        increment("cf", "q", 7L),
                        increment("m", "day-1", 3L),
                        increment("m", "day-2", -1L));

        // Declared first, the map's rules come first: the order is the DDL's, not ROW-then-MAP.
        RowType mapFirst =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "m",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BIGINT())),
                                        DataTypes.FIELD(
                                                "cf",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("q", DataTypes.BIGINT()))))
                                .getLogicalType();
        assertThat(
                        ReadModifyWriteWireRules.sentRules(
                                new RowDataReadModifyWriteSerializationSchema(
                                                BigtableTableSchema.of(mapFirst),
                                                WriteMode.INCREMENT)
                                        .serialize(
                                                GenericRowData.of(
                                                        StringData.fromString("r"),
                                                        map(key("day-1"), 3L),
                                                        GenericRowData.of(7L)),
                                                null)))
                .containsExactly(increment("m", "day-1", 3L), increment("cf", "q", 7L));
    }

    @Test
    void aMapFamilyAppendsOnePerNonNullEntry() throws Exception {
        RowDataReadModifyWriteSerializationSchema schema =
                InstantiationUtil.clone(mapSchema(WriteMode.APPEND), getClass().getClassLoader());
        ReadModifyWriteRequest request =
                schema.serialize(
                        GenericRowData.of(
                                StringData.fromString("r"),
                                null,
                                map(key("a"), StringData.fromString("x"), key("b"), null)),
                        null);
        assertThat(ReadModifyWriteWireRules.sentRules(request))
                .containsExactly(
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("m")
                                .setColumnQualifier(ByteString.copyFromUtf8("a"))
                                .setAppendValue(ByteString.copyFromUtf8("x"))
                                .build());
    }

    @Test
    void aBytesValuedMapFamilyAppendsItsBytesUnchanged() throws Exception {
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "m",
                                                DataTypes.MAP(
                                                        DataTypes.BYTES(), DataTypes.BYTES())))
                                .getLogicalType();
        ReadModifyWriteRequest request =
                new RowDataReadModifyWriteSerializationSchema(
                                BigtableTableSchema.of(row), WriteMode.APPEND)
                        .serialize(
                                GenericRowData.of(
                                        StringData.fromString("r"),
                                        map(new byte[] {(byte) 0xff}, new byte[] {0, (byte) 0xfe})),
                                null);
        assertThat(ReadModifyWriteWireRules.sentRules(request))
                .containsExactly(
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("m")
                                .setColumnQualifier(ByteString.copyFrom(new byte[] {(byte) 0xff}))
                                .setAppendValue(ByteString.copyFrom(new byte[] {0, (byte) 0xfe}))
                                .build());
    }

    @Test
    void anEmptyOrAllNullMapIsRejectedWithTheExistingMessage() throws Exception {
        for (WriteMode mode : new WriteMode[] {WriteMode.APPEND, WriteMode.INCREMENT}) {
            RowDataReadModifyWriteSerializationSchema schema = mapSchema(mode);
            for (GenericMapData map : new GenericMapData[] {map(), map(key("a"), null)}) {
                assertThatThrownBy(
                                () ->
                                        schema.serialize(
                                                GenericRowData.of(
                                                        StringData.fromString("r"), null, map),
                                                null))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("at least one nonnull cell");
                // Beside a family that has a rule, the same map is simply nothing to add.
                Object cell = mode == WriteMode.APPEND ? StringData.fromString("v") : 1L;
                assertThat(
                                schema.serialize(
                                                GenericRowData.of(
                                                        StringData.fromString("r"),
                                                        GenericRowData.of(cell),
                                                        map),
                                                null)
                                        .getRules())
                        .hasSize(1);
            }
        }
    }

    @Test
    void anEmptyAppendValueNamesTheEscapedMapKey() {
        assertThatThrownBy(
                        () ->
                                mapSchema(WriteMode.APPEND)
                                        .serialize(
                                                GenericRowData.of(
                                                        StringData.fromString("r"),
                                                        null,
                                                        map(key("\n"), StringData.fromString(""))),
                                                null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Append value for key '\\x0a' of MAP column family 'm'");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = 1L)
    void aNullMapKeyIsRejectedNamingTheFamilyAndTheRow(Long value) {
        // A null value does not excuse a null key, and neither does another family's rule.
        Map<Object, Object> entries = new HashMap<>();
        entries.put(null, value);
        assertThatThrownBy(
                        () ->
                                mapSchema(WriteMode.INCREMENT)
                                        .serialize(
                                                GenericRowData.of(
                                                        StringData.fromString("r"),
                                                        GenericRowData.of(7L),
                                                        new GenericMapData(entries)),
                                                null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("MAP column family 'm' of the row with key 'r'")
                .hasMessageContaining("null key");
    }

    @Test
    void aMapPastTheRequestsRuleLimitFailsInsideSerialize() throws Exception {
        // ReadModifyWriteRequest allows 100,000 rules; the 100,001st fails inside serialize(),
        // which the single-row writer routes as a serialization failure.
        RowDataReadModifyWriteSerializationSchema schema = mapSchema(WriteMode.INCREMENT);
        int limit = 100_000;
        assertThat(schema.serialize(numbered(limit), null).getRules()).hasSize(limit);
        assertThatThrownBy(() -> schema.serialize(numbered(limit + 1), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 100000 rules");
    }

    /** A row of the map schema holding a map of {@code entries} increments of one. */
    private static GenericRowData numbered(int entries) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < entries; i++) {
            map.put(key("q" + i), 1L);
        }
        return GenericRowData.of(StringData.fromString("r"), null, new GenericMapData(map));
    }

    /**
     * A row key, a ROW family and a STRING-keyed map family, whose operands are BIGINT for
     * increment and STRING for append.
     */
    private static RowDataReadModifyWriteSerializationSchema mapSchema(WriteMode mode) {
        org.apache.flink.table.types.DataType type =
                mode == WriteMode.APPEND ? DataTypes.STRING() : DataTypes.BIGINT();
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "cf", DataTypes.ROW(DataTypes.FIELD("q", type))),
                                        DataTypes.FIELD(
                                                "m", DataTypes.MAP(DataTypes.STRING(), type)))
                                .getLogicalType();
        return new RowDataReadModifyWriteSerializationSchema(BigtableTableSchema.of(row), mode);
    }

    private static StringData key(String key) {
        return StringData.fromString(key);
    }

    /** A map in the given entry order; a null value stays a null value. */
    private static GenericMapData map(Object... keysAndValues) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return new GenericMapData(map);
    }

    private static com.google.bigtable.v2.ReadModifyWriteRule increment(
            String family, String qualifier, long amount) {
        return com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                .setFamilyName(family)
                .setColumnQualifier(ByteString.copyFromUtf8(qualifier))
                .setIncrementAmount(amount)
                .build();
    }

    private static RowDataReadModifyWriteSerializationSchema schema(WriteMode mode) {
        org.apache.flink.table.types.DataType type =
                mode == WriteMode.APPEND ? DataTypes.STRING() : DataTypes.BIGINT();
        RowType row =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "cf",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a", type),
                                                        DataTypes.FIELD("b", type))),
                                        DataTypes.FIELD("k", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "other", DataTypes.ROW(DataTypes.FIELD("v", type))))
                                .getLogicalType();
        return new RowDataReadModifyWriteSerializationSchema(BigtableTableSchema.of(row), mode);
    }
}
