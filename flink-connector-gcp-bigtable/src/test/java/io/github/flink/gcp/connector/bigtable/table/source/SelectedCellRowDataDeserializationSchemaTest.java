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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.TestBigtableChangeStreamMutations;
import io.github.flink.gcp.connector.bigtable.table.SelectedCellTableSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectedCellRowDataDeserializationSchemaTest {

    private static final String FAMILY = "state";
    private static final ByteString QUALIFIER = ByteString.copyFromUtf8("current");
    private static final DataType PHYSICAL_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("row_id", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("score", DataTypes.INT()));
    private static final SelectedCellTableSchema TABLE_SCHEMA =
            SelectedCellTableSchema.of(PHYSICAL_TYPE, new int[] {1});

    @Test
    void decodesAnUpsertAndInsertsTheNonLeadingKeyBeforeMetadata() throws Exception {
        SelectedCellRowDataDeserializationSchema schema =
                schema(
                        oneRow(StringData.fromString("Alice"), 7),
                        new ChangeStreamReadableMetadata[] {
                            ChangeStreamReadableMetadata.SOURCE_CLUSTER_ID
                        });
        List<RowData> output = new ArrayList<>();

        schema.deserialize(upsert(), collectingInto(output));

        assertThat(output).hasSize(1);
        RowData row = output.get(0);
        assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
        assertThat(row.getString(0).toString()).isEqualTo("Alice");
        assertThat(row.getString(1).toString()).isEqualTo("row-1");
        assertThat(row.getInt(2)).isEqualTo(7);
        assertThat(row.getString(3).toString()).isEqualTo("cluster-1");
    }

    @Test
    void resetsTheFormatCollectorBetweenUpserts() throws Exception {
        SelectedCellRowDataDeserializationSchema schema =
                schema(
                        oneRow(StringData.fromString("Alice"), 7),
                        new ChangeStreamReadableMetadata[0]);
        List<RowData> output = new ArrayList<>();

        schema.deserialize(upsert(), collectingInto(output));
        schema.deserialize(upsert(), collectingInto(output));

        assertThat(output).hasSize(2);
        assertThat(output)
                .allSatisfy(row -> assertThat(row.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER));
    }

    @Test
    void emitsAKeyOnlyDeleteWithoutInvokingTheFormat() throws Exception {
        SelectedCellRowDataDeserializationSchema schema =
                schema(
                        (bytes, out) -> {
                            throw new AssertionError("delete must not invoke the value format");
                        },
                        new ChangeStreamReadableMetadata[0]);
        List<RowData> output = new ArrayList<>();

        schema.deserialize(delete(), collectingInto(output));

        assertThat(output).hasSize(1);
        RowData row = output.get(0);
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getString(1).toString()).isEqualTo("row-1");
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void emitsNothingForAnUnrelatedMutation() throws Exception {
        SelectedCellRowDataDeserializationSchema schema =
                schema(
                        oneRow(StringData.fromString("unused"), 0),
                        new ChangeStreamReadableMetadata[0]);
        List<RowData> output = new ArrayList<>();

        schema.deserialize(unrelated(), collectingInto(output));

        assertThat(output).isEmpty();
    }

    @Test
    void rejectsZeroMultipleAndNullFormatRows() {
        assertFormatCardinality((bytes, out) -> {}, "emitted 0 rows");
        assertFormatCardinality(
                (bytes, out) -> {
                    out.collect(GenericRowData.of(StringData.fromString("one"), 1));
                    out.collect(GenericRowData.of(StringData.fromString("two"), 2));
                },
                "emitted 2 rows");
        SelectedCellRowDataDeserializationSchema schema =
                schema((bytes, out) -> out.collect(null), new ChangeStreamReadableMetadata[0]);
        assertThatThrownBy(() -> schema.deserialize(upsert(), collectingInto(new ArrayList<>())))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("must not collect null");
    }

    @Test
    void rejectsAFormatThatEmitsANonInsertRowDespiteItsDeclaredMode() {
        GenericRowData update = GenericRowData.of(StringData.fromString("Alice"), 7);
        update.setRowKind(RowKind.UPDATE_AFTER);
        SelectedCellRowDataDeserializationSchema schema =
                schema((bytes, out) -> out.collect(update), new ChangeStreamReadableMetadata[0]);

        assertThatThrownBy(() -> schema.deserialize(upsert(), collectingInto(new ArrayList<>())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("declared an insert-only changelog")
                .hasMessageContaining("UPDATE_AFTER");
    }

    private static void assertFormatCardinality(
            CollectorDeserializer deserializer, String expected) {
        SelectedCellRowDataDeserializationSchema schema =
                schema(deserializer, new ChangeStreamReadableMetadata[0]);
        assertThatThrownBy(() -> schema.deserialize(upsert(), collectingInto(new ArrayList<>())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exactly one non-null row")
                .hasMessageContaining(expected);
    }

    private static SelectedCellRowDataDeserializationSchema schema(
            DeserializationSchema<RowData> payload, ChangeStreamReadableMetadata[] metadata) {
        TypeInformation<RowData> producedType =
                InternalTypeInfo.of((RowType) PHYSICAL_TYPE.getLogicalType());
        return new SelectedCellRowDataDeserializationSchema(
                payload,
                new SelectedCellMutationClassifier(FAMILY, QUALIFIER, "cluster-1"),
                TABLE_SCHEMA,
                metadata,
                producedType);
    }

    private static SelectedCellRowDataDeserializationSchema schema(
            CollectorDeserializer payload, ChangeStreamReadableMetadata[] metadata) {
        return schema(
                new DeserializationSchema<RowData>() {
                    @Override
                    public RowData deserialize(byte[] message) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void deserialize(byte[] message, Collector<RowData> out)
                            throws IOException {
                        payload.deserialize(message, out);
                    }

                    @Override
                    public boolean isEndOfStream(RowData nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<RowData> getProducedType() {
                        return InternalTypeInfo.of(
                                (RowType) TABLE_SCHEMA.getPayloadDataType().getLogicalType());
                    }
                },
                metadata);
    }

    private static DeserializationSchema<RowData> oneRow(Object... fields) {
        return new DeserializationSchema<RowData>() {
            @Override
            public RowData deserialize(byte[] message) {
                return GenericRowData.of(fields);
            }

            @Override
            public boolean isEndOfStream(RowData nextElement) {
                return false;
            }

            @Override
            public TypeInformation<RowData> getProducedType() {
                return InternalTypeInfo.of(
                        (RowType) TABLE_SCHEMA.getPayloadDataType().getLogicalType());
            }
        };
    }

    private static BigtableChangeStreamMutation upsert() {
        return mutation(
                builder -> {
                    builder.deleteCells(FAMILY, QUALIFIER, Range.TimestampRange.unbounded());
                    builder.startCell(FAMILY, QUALIFIER, 1L);
                    builder.cellValue(ByteString.copyFromUtf8("payload"));
                    builder.finishCell();
                });
    }

    private static BigtableChangeStreamMutation delete() {
        return mutation(
                builder ->
                        builder.deleteCells(FAMILY, QUALIFIER, Range.TimestampRange.unbounded()));
    }

    private static BigtableChangeStreamMutation unrelated() {
        return mutation(builder -> builder.deleteFamily("other"));
    }

    private static BigtableChangeStreamMutation mutation(
            java.util.function.Consumer<ChangeStreamRecordBuilder<ChangeStreamRecord>> entries) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(
                ByteString.copyFromUtf8("row-1"),
                "cluster-1",
                Instant.parse("2026-08-13T00:00:00Z"),
                0);
        entries.accept(builder);
        return TestBigtableChangeStreamMutations.convert(
                (ChangeStreamMutation)
                        builder.finishChangeStreamMutation(
                                "token", Instant.parse("2026-08-12T23:59:00Z")));
    }

    private static Collector<RowData> collectingInto(List<RowData> output) {
        return new Collector<RowData>() {
            @Override
            public void collect(RowData record) {
                output.add(record);
            }

            @Override
            public void close() {}
        };
    }

    @FunctionalInterface
    private interface CollectorDeserializer {
        void deserialize(byte[] bytes, Collector<RowData> out) throws IOException;
    }
}
