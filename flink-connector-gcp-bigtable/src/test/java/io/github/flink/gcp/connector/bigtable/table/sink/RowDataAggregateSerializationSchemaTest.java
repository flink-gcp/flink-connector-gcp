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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

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
