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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Value;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowDataAggregateSerializationSchemaTest {
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("key", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "tiny", DataTypes.TINYINT()),
                                                            DataTypes.FIELD(
                                                                    "small", DataTypes.SMALLINT()),
                                                            DataTypes.FIELD("int", DataTypes.INT()),
                                                            DataTypes.FIELD(
                                                                    "long", DataTypes.BIGINT()))))
                                    .getLogicalType());

    private static RowDataAggregateSerializationSchema schema(
            boolean metadata, boolean truncate, RowDataSerializationSchema.CellClock clock) {
        return new RowDataAggregateSerializationSchema(
                SCHEMA,
                metadata
                        ? new WritableMetadata[] {WritableMetadata.TIMESTAMP}
                        : new WritableMetadata[0],
                truncate,
                clock);
    }

    private static GenericRowData row(Object cells, Object timestamp) {
        return GenericRowData.of(StringData.fromString("r"), cells, timestamp);
    }

    @Test
    void widensEveryIntegerWithoutTheRawCellCodec() throws Exception {
        var serializer =
                schema(
                        true,
                        false,
                        () -> {
                            throw new AssertionError("explicit timestamp must win");
                        });
        var mutations =
                serializer
                        .serialize(
                                row(
                                        GenericRowData.of(
                                                Byte.MIN_VALUE,
                                                Short.MIN_VALUE,
                                                Integer.MIN_VALUE,
                                                Long.MIN_VALUE),
                                        TimestampData.fromEpochMillis(123, 456000)),
                                null)
                        .toProto()
                        .getMutationsList();
        assertThat(mutations)
                .containsExactly(
                        add("tiny", 123456, Byte.MIN_VALUE), add("small", 123456, Short.MIN_VALUE),
                        add("int", 123456, Integer.MIN_VALUE), add("long", 123456, Long.MIN_VALUE));
    }

    @Test
    void skipsNullCellsAndReadsTheClockOnlyForWrittenCells() throws Exception {
        long[] now = {1000};
        var serializer =
                schema(
                        false,
                        false,
                        () -> {
                            long value = now[0];
                            now[0] += 1000;
                            return value;
                        });
        assertThat(
                        serializer
                                .serialize(
                                        row(
                                                GenericRowData.of(
                                                        (byte) 1, null, null, Long.MAX_VALUE),
                                                null),
                                        null)
                                .toProto()
                                .getMutationsList())
                .containsExactly(add("tiny", 1000, 1), add("long", 2000, Long.MAX_VALUE));
        assertThat(now[0]).isEqualTo(3000);
    }

    @Test
    void nullTimestampUsesTheClockAndExplicitEpochZeroIsNotMissing() throws Exception {
        var serializer = schema(true, false, () -> 9000);
        assertThat(
                        serializer
                                .serialize(row(GenericRowData.of(null, null, null, 7L), null), null)
                                .toProto()
                                .getMutationsList())
                .containsExactly(add("long", 9000, 7));
        assertThat(
                        serializer
                                .serialize(
                                        row(
                                                GenericRowData.of(null, null, null, 7L),
                                                TimestampData.fromEpochMillis(0)),
                                        null)
                                .toProto()
                                .getMutationsList())
                .containsExactly(add("long", 0, 7));
    }

    @Test
    void truncatesOnlyWhenRequestedAndRejectsNegativeOrOverflowingTimestamps() throws Exception {
        var serializer = schema(true, true, () -> 9000);
        var cells = GenericRowData.of(null, null, null, 7L);
        assertThat(
                        serializer
                                .serialize(
                                        row(cells, TimestampData.fromEpochMillis(123, 456000)),
                                        null)
                                .toProto()
                                .getMutationsList())
                .containsExactly(add("long", 123000, 7));
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        row(
                                                cells,
                                                TimestampData.fromInstant(
                                                        Instant.ofEpochSecond(-1, 999999000))),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("must not be before the Unix epoch");
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        row(cells, TimestampData.fromEpochMillis(Long.MAX_VALUE)),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("outside the range");
    }

    @Test
    void rejectsEmptyContributionsAndInvalidKeys() {
        var serializer = schema(false, false, () -> 1000);
        for (Object cells : new Object[] {null, GenericRowData.of(null, null, null, null)}) {
            assertThatThrownBy(() -> serializer.serialize(row(cells, null), null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Every aggregate cell");
        }
        for (Object key : new Object[] {null, StringData.fromString("")}) {
            var input = row(GenericRowData.of(null, null, null, 1L), null);
            input.setField(0, key);
            assertThatThrownBy(() -> serializer.serialize(input, null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("row-key column");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"UPDATE_BEFORE", "UPDATE_AFTER", "DELETE"})
    void refusesNonInsertRowsEvenWhenCalledWithoutThePlanner(RowKind kind) {
        RowData input = row(GenericRowData.of(null, null, null, 1L), null);
        input.setRowKind(kind);
        assertThatThrownBy(() -> schema(false, false, () -> 1000).serialize(input, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("requires INSERT-only input");
    }

    @Test
    void restoresTheJobGraphWithTheProductionClock() throws Exception {
        var serializer = schema(false, false, new RowDataSerializationSchema.WallClock());
        byte[] bytes = InstantiationUtil.serializeObject(serializer);
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataAggregateSerializationSchema restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        long before = System.currentTimeMillis() * 1000;
        Mutation mutation =
                restored.serialize(row(GenericRowData.of(null, null, null, 1L), null), null)
                        .toProto()
                        .getMutations(0);
        assertThat(mutation.getAddToCell().getTimestamp().getRawTimestampMicros())
                .isBetween(before, System.currentTimeMillis() * 1000)
                .isEqualTo(
                        mutation.getAddToCell().getTimestamp().getRawTimestampMicros()
                                / 1000
                                * 1000);
    }

    /** A ROW family beside a STRING-keyed BIGINT map and a BYTES-keyed TINYINT map. */
    private static final BigtableTableSchema MAP_SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("key", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.BIGINT()))),
                                            DataTypes.FIELD(
                                                    "m",
                                                    DataTypes.MAP(
                                                            DataTypes.STRING(),
                                                            DataTypes.BIGINT())),
                                            DataTypes.FIELD(
                                                    "t",
                                                    DataTypes.MAP(
                                                            DataTypes.BYTES(),
                                                            DataTypes.TINYINT())))
                                    .getLogicalType());

    private static RowDataAggregateSerializationSchema mapSchema(
            boolean metadata, RowDataSerializationSchema.CellClock clock) {
        return new RowDataAggregateSerializationSchema(
                MAP_SCHEMA,
                metadata
                        ? new WritableMetadata[] {WritableMetadata.TIMESTAMP}
                        : new WritableMetadata[0],
                false,
                clock);
    }

    private static GenericRowData mapRow(Object cf, Object m, Object t, Object timestamp) {
        return GenericRowData.of(StringData.fromString("r"), cf, m, t, timestamp);
    }

    /** A STRING-keyed map in the given entry order; a null value stays a null value. */
    private static GenericMapData longs(Object... keysAndValues) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(StringData.fromString((String) keysAndValues[i]), keysAndValues[i + 1]);
        }
        return new GenericMapData(map);
    }

    @Test
    void aMapFamilyAddsOneContributionPerNonNullEntry() throws Exception {
        var serializer =
                mapSchema(
                        true,
                        () -> {
                            throw new AssertionError("explicit timestamp must win");
                        });
        Map<Object, Object> tiny = new LinkedHashMap<>();
        tiny.put(new byte[] {(byte) 0xff, 0}, Byte.MIN_VALUE);
        var mutations =
                serializer
                        .serialize(
                                mapRow(
                                        GenericRowData.of(7L),
                                        longs("2026-09-30", 3L, "skipped", null, "total", -5L),
                                        new GenericMapData(tiny),
                                        TimestampData.fromEpochMillis(123, 456000)),
                                null)
                        .toProto()
                        .getMutationsList();
        assertThat(mutations)
                .containsExactly(
                        add("cf", ByteString.copyFromUtf8("q"), 123456, 7),
                        add("m", ByteString.copyFromUtf8("2026-09-30"), 123456, 3),
                        add("m", ByteString.copyFromUtf8("total"), 123456, -5),
                        add("t", ByteString.copyFrom(new byte[] {(byte) 0xff, 0}), 123456, -128));
    }

    @Test
    void theWriterClockIsReadOncePerContributingMapEntry() throws Exception {
        long[] now = {1000};
        var serializer =
                mapSchema(
                        false,
                        () -> {
                            long value = now[0];
                            now[0] += 1000;
                            return value;
                        });
        assertThat(
                        serializer
                                .serialize(
                                        mapRow(
                                                null,
                                                longs("a", 1L, "b", null, "c", 2L),
                                                null,
                                                null),
                                        null)
                                .toProto()
                                .getMutationsList())
                .containsExactly(
                        add("m", ByteString.copyFromUtf8("a"), 1000, 1),
                        add("m", ByteString.copyFromUtf8("c"), 2000, 2));
        assertThat(now[0]).isEqualTo(3000);
    }

    @Test
    void anEmptyOrAllNullMapContributesNothingAndIsRejectedAlone() throws Exception {
        var serializer = mapSchema(false, () -> 1000);
        for (Object m : new Object[] {longs(), longs("a", null, "b", null)}) {
            assertThatThrownBy(() -> serializer.serialize(mapRow(null, m, null, null), null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                            "Every aggregate cell of row 'r' is null, and every map is empty or"
                                    + " holds only null values");
            // Beside a family that contributes, the same map is simply nothing to add.
            assertThat(
                            serializer
                                    .serialize(mapRow(GenericRowData.of(7L), m, null, null), null)
                                    .toProto()
                                    .getMutationsList())
                    .containsExactly(add("cf", ByteString.copyFromUtf8("q"), 1000, 7));
        }
    }

    @Test
    void widensEveryIntegerMapValue() throws Exception {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "t",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.TINYINT())),
                                                DataTypes.FIELD(
                                                        "s",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.SMALLINT())),
                                                DataTypes.FIELD(
                                                        "i",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.INT())),
                                                DataTypes.FIELD(
                                                        "l",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.BIGINT())))
                                        .getLogicalType());
        var serializer =
                new RowDataAggregateSerializationSchema(
                        schema, new WritableMetadata[0], false, () -> 1000);
        // Boxed values of the declared width: a getter of another width fails on the cast.
        RowData input =
                GenericRowData.of(
                        StringData.fromString("r"),
                        new GenericMapData(Map.of(StringData.fromString("k"), Byte.MIN_VALUE)),
                        new GenericMapData(Map.of(StringData.fromString("k"), Short.MIN_VALUE)),
                        new GenericMapData(Map.of(StringData.fromString("k"), Integer.MIN_VALUE)),
                        new GenericMapData(Map.of(StringData.fromString("k"), Long.MIN_VALUE)));
        ByteString k = ByteString.copyFromUtf8("k");
        assertThat(serializer.serialize(input, null).toProto().getMutationsList())
                .containsExactly(
                        add("t", k, 1000, Byte.MIN_VALUE),
                        add("s", k, 1000, Short.MIN_VALUE),
                        add("i", k, 1000, Integer.MIN_VALUE),
                        add("l", k, 1000, Long.MIN_VALUE));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = 1L)
    void aNullMapKeyIsRejectedNamingTheFamilyAndTheRow(Long value) {
        // A null value does not excuse a null key, and neither does another contributing family.
        Map<Object, Object> map = new HashMap<>();
        map.put(null, value);
        assertThatThrownBy(
                        () ->
                                mapSchema(false, () -> 1000)
                                        .serialize(
                                                mapRow(
                                                        GenericRowData.of(7L),
                                                        new GenericMapData(map),
                                                        null,
                                                        null),
                                                null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("MAP column family 'm' of the row with key 'r'")
                .hasMessageContaining("null key");
    }

    @Test
    void aMapPastTheClientsMutationLimitFailsInsideSerialize() throws Exception {
        // AddToCell counts toward the client's per-entry bound like SetCell (ADR-0172): an entry
        // at the bound builds its proto, and the 100,001st contribution fails inside serialize(),
        // which the writer routes as a serialization failure.
        int limit = 100_000;
        var serializer = mapSchema(false, () -> 1000);
        assertThat(
                        serializer
                                .serialize(mapRow(null, numbered(limit), null, null), null)
                                .toProto()
                                .getMutationsCount())
                .isEqualTo(limit);
        assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        mapRow(null, numbered(limit + 1), null, null), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Too many mutations");
    }

    @Test
    void aMapFamilyCrossesTheJobGraph() throws Exception {
        var serializer = mapSchema(true, new RowDataSerializationSchema.WallClock());
        byte[] bytes = InstantiationUtil.serializeObject(serializer);
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataAggregateSerializationSchema restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        RowData input =
                mapRow(
                        null,
                        longs("a", 1L),
                        new GenericMapData(Map.of(new byte[] {1}, (byte) 2)),
                        TimestampData.fromEpochMillis(5));
        assertThat(restored.serialize(input, null).toProto())
                .isEqualTo(serializer.serialize(input, null).toProto());
    }

    private static GenericMapData numbered(int entries) {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < entries; i++) {
            map.put(StringData.fromString("q" + i), 1L);
        }
        return new GenericMapData(map);
    }

    private static Mutation add(String family, ByteString qualifier, long timestamp, long input) {
        return Mutation.newBuilder()
                .setAddToCell(
                        Mutation.AddToCell.newBuilder()
                                .setFamilyName(family)
                                .setColumnQualifier(Value.newBuilder().setRawValue(qualifier))
                                .setTimestamp(Value.newBuilder().setRawTimestampMicros(timestamp))
                                .setInput(Value.newBuilder().setIntValue(input)))
                .build();
    }

    private static Mutation add(String qualifier, long timestamp, long input) {
        return Mutation.newBuilder()
                .setAddToCell(
                        Mutation.AddToCell.newBuilder()
                                .setFamilyName("cf")
                                .setColumnQualifier(
                                        Value.newBuilder()
                                                .setRawValue(ByteString.copyFromUtf8(qualifier)))
                                .setTimestamp(Value.newBuilder().setRawTimestampMicros(timestamp))
                                .setInput(Value.newBuilder().setIntValue(input)))
                .build();
    }
}
