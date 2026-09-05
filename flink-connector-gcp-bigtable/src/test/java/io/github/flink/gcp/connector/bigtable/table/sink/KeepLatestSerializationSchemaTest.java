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

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeepLatestSerializationSchemaTest {
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("key", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "name", DataTypes.STRING()),
                                                            DataTypes.FIELD(
                                                                    "amount", DataTypes.BIGINT()))),
                                            DataTypes.FIELD(
                                                    "other",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "flag", DataTypes.BOOLEAN()))))
                                    .getLogicalType());
    private static final WritableMetadata[] METADATA = {WritableMetadata.TIMESTAMP};

    private static RowDataSerializationSchema serializer(boolean truncate) {
        return new RowDataSerializationSchema(SCHEMA, "NULL", METADATA, truncate, true);
    }

    private static GenericRowData row() {
        return GenericRowData.of(
                StringData.fromString("r"),
                GenericRowData.of(StringData.fromString("new"), 7L),
                GenericRowData.of(true),
                TimestampData.fromEpochMillis(10));
    }

    private static List<Mutation.SetCell> replacements(MutateRowsRequest.Entry entry) {
        List<Mutation> mutations = entry.getMutationsList();
        assertThat(mutations.size() % 2).isZero();
        for (int i = 0; i < mutations.size(); i += 2) {
            Mutation delete = mutations.get(i);
            Mutation set = mutations.get(i + 1);
            assertThat(delete.getMutationCase())
                    .isEqualTo(Mutation.MutationCase.DELETE_FROM_COLUMN);
            assertThat(set.getMutationCase()).isEqualTo(Mutation.MutationCase.SET_CELL);
            assertThat(delete.getDeleteFromColumn().hasTimeRange()).isFalse();
            assertThat(delete.getDeleteFromColumn().getFamilyName())
                    .isEqualTo(set.getSetCell().getFamilyName());
            assertThat(delete.getDeleteFromColumn().getColumnQualifier())
                    .isEqualTo(set.getSetCell().getColumnQualifier());
        }
        return IntStream.range(0, mutations.size() / 2)
                .mapToObj(i -> mutations.get(2 * i + 1).getSetCell())
                .collect(java.util.stream.Collectors.toList());
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void replacesEveryQualifierInOneOrderedEntry(RowKind kind) throws Exception {
        GenericRowData input = row();
        input.setRowKind(kind);
        MutateRowsRequest.Entry entry = serializer(false).serialize(input, null).toProto();
        assertThat(entry.getRowKey().toStringUtf8()).isEqualTo("r");
        assertThat(replacements(entry))
                .extracting(
                        cell ->
                                cell.getFamilyName()
                                        + ":"
                                        + cell.getColumnQualifier().toStringUtf8())
                .containsExactly("cf:name", "cf:amount", "other:flag");
        assertThat(replacements(entry))
                .extracting(Mutation.SetCell::getTimestampMicros)
                .containsOnly(10_000L);
    }

    @Test
    void writesScalarNullsAndSkipsNullFamilies() throws Exception {
        GenericRowData input = row();
        input.setField(1, GenericRowData.of(null, null));
        input.setField(2, null);
        List<Mutation.SetCell> cells =
                replacements(serializer(false).serialize(input, null).toProto());
        assertThat(cells).hasSize(2);
        assertThat(cells.get(0).getValue().toStringUtf8()).isEqualTo("NULL");
        assertThat(cells.get(1).getValue()).isEmpty();
        assertThat(cells).extracting(Mutation.SetCell::getFamilyName).containsOnly("cf");
    }

    @Test
    void rejectsAnEmptyWriteAndUpdateBefore() {
        GenericRowData input = row();
        input.setField(1, null);
        input.setField(2, null);
        assertThatThrownBy(() -> serializer(false).serialize(input, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Every column family");
        input.setRowKind(RowKind.UPDATE_BEFORE);
        assertThatThrownBy(() -> serializer(false).serialize(input, null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("UPDATE_BEFORE");
    }

    @Test
    void deletesTheWholeRowEvenWithNullFamiliesAndInvalidTimestamp() throws Exception {
        GenericRowData input = row();
        input.setRowKind(RowKind.DELETE);
        input.setField(1, null);
        input.setField(2, null);
        input.setField(3, TimestampData.fromEpochMillis(-1));
        assertThat(serializer(false).serialize(input, null).toProto().getMutationsList())
                .extracting(Mutation::getMutationCase)
                .containsExactly(Mutation.MutationCase.DELETE_FROM_ROW);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainsExplicitTimestampValidationAndTruncation(boolean truncate) throws Exception {
        GenericRowData input = row();
        input.setField(3, TimestampData.fromEpochMillis(10, 123_000));
        assertThat(replacements(serializer(truncate).serialize(input, null).toProto()))
                .extracting(Mutation.SetCell::getTimestampMicros)
                .containsOnly(truncate ? 10_000L : 10_123L);
        input.setField(3, TimestampData.fromEpochMillis(-1));
        assertThatThrownBy(() -> serializer(truncate).serialize(input, null))
                .hasMessageContaining("before the Unix epoch");
        input.setField(3, TimestampData.fromEpochMillis(Long.MAX_VALUE));
        assertThatThrownBy(() -> serializer(truncate).serialize(input, null))
                .hasMessageContaining("outside the range");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void usesThePerCellWriterClockWithAbsentOrNullMetadata(boolean declared) throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(
                        SCHEMA,
                        "NULL",
                        declared ? METADATA : new WritableMetadata[0],
                        false,
                        true,
                        new CountingClock());
        GenericRowData input = row();
        input.setField(3, null);
        assertThat(replacements(schema.serialize(input, null).toProto()))
                .extracting(Mutation.SetCell::getTimestampMicros)
                .containsExactly(1_000L, 2_000L, 3_000L);
        assertThat(replacements(schema.serialize(input, null).toProto()))
                .extracting(Mutation.SetCell::getTimestampMicros)
                .containsExactly(4_000L, 5_000L, 6_000L);
    }

    @Test
    void retainsReplacementAcrossTheJobGraph() throws Exception {
        byte[] bytes = InstantiationUtil.serializeObject(serializer(false));
        assertThat(new String(bytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        RowDataSerializationSchema restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.serialize(row(), null).toProto())
                .isEqualTo(serializer(false).serialize(row(), null).toProto());
        assertThat(replacements(restored.serialize(row(), null).toProto())).hasSize(3);
    }

    @Test
    void countsBothMutationsAgainstTheSdkRowLimit() throws Exception {
        DataTypes.Field[] fields =
                IntStream.range(0, 50_001)
                        .mapToObj(i -> DataTypes.FIELD("q" + i, DataTypes.STRING()))
                        .toArray(DataTypes.Field[]::new);
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("key", DataTypes.STRING()),
                                                DataTypes.FIELD("cf", DataTypes.ROW(fields)))
                                        .getLogicalType());
        RowData input =
                GenericRowData.of(StringData.fromString("r"), new GenericRowData(fields.length));
        assertThat(
                        new RowDataSerializationSchema(
                                        schema, "NULL", new WritableMetadata[0], false)
                                .serialize(input, null)
                                .toProto()
                                .getMutationsCount())
                .isEqualTo(50_001);
        assertThatThrownBy(
                        () ->
                                new RowDataSerializationSchema(
                                                schema,
                                                "NULL",
                                                new WritableMetadata[0],
                                                false,
                                                true)
                                        .serialize(input, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Too many mutations per row");
    }

    private static final class CountingClock implements RowDataSerializationSchema.CellClock {
        private static final long serialVersionUID = 1L;
        private long value;

        @Override
        public long micros() {
            value += 1_000;
            return value;
        }
    }
}
