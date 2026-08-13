/*
 * Copyright 2026 laughingman7743
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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.SelectedCellTableSchema;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the production selected-cell decoder inside a Flink job, including the zero-output path. */
class SelectedCellChangeStreamJobITCase {

    @Test
    void aFlinkJobEmitsTheCanonicalUpsertAndDeleteOnly() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> output = new ArrayList<>();
        try (CloseableIterator<String> records =
                env.fromElements(0, 1, 2)
                        .flatMap(new DecodeMutation())
                        .returns(Types.STRING)
                        .executeAndCollect()) {
            records.forEachRemaining(output::add);
        }

        assertThat(output).containsExactly("+U:Alice:row-1:7", "-D:null:row-1:null");
    }

    private static final class DecodeMutation implements FlatMapFunction<Integer, String> {
        private static final long serialVersionUID = 1L;
        private static final String FAMILY = "state";
        private static final ByteString QUALIFIER = ByteString.copyFromUtf8("current");

        private final SelectedCellRowDataDeserializationSchema schema;

        private DecodeMutation() {
            DataType physicalType =
                    DataTypes.ROW(
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("row_id", DataTypes.STRING().notNull()),
                            DataTypes.FIELD("score", DataTypes.INT()));
            SelectedCellTableSchema tableSchema =
                    SelectedCellTableSchema.of(physicalType, new int[] {1});
            TypeInformation<RowData> producedType =
                    InternalTypeInfo.of((RowType) physicalType.getLogicalType());
            schema =
                    new SelectedCellRowDataDeserializationSchema(
                            new PayloadDeserializer(tableSchema.getPayloadDataType()),
                            new SelectedCellMutationClassifier(FAMILY, QUALIFIER, "cluster-1"),
                            tableSchema,
                            new ChangeStreamReadableMetadata[0],
                            producedType);
        }

        @Override
        public void flatMap(Integer specification, Collector<String> out) throws Exception {
            schema.deserialize(
                    mutation(specification),
                    new Collector<RowData>() {
                        @Override
                        public void collect(RowData row) {
                            out.collect(
                                    row.getRowKind().shortString()
                                            + ":"
                                            + (row.isNullAt(0)
                                                    ? "null"
                                                    : row.getString(0).toString())
                                            + ":"
                                            + row.getString(1)
                                            + ":"
                                            + (row.isNullAt(2) ? "null" : row.getInt(2)));
                        }

                        @Override
                        public void close() {}
                    });
        }

        private static ChangeStreamMutation mutation(int specification) {
            ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                    new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
            builder.startUserMutation(
                    ByteString.copyFromUtf8("row-1"),
                    "cluster-1",
                    Instant.parse("2026-08-13T00:00:00Z"),
                    specification);
            if (specification == 0) {
                builder.deleteCells(FAMILY, QUALIFIER, Range.TimestampRange.unbounded());
                builder.startCell(FAMILY, QUALIFIER, 1L);
                builder.cellValue(ByteString.copyFromUtf8("Alice,7"));
                builder.finishCell();
            } else if (specification == 1) {
                builder.deleteFamily("unrelated");
            } else {
                builder.deleteCells(FAMILY, QUALIFIER, Range.TimestampRange.unbounded());
            }
            return (ChangeStreamMutation)
                    builder.finishChangeStreamMutation(
                            "token", Instant.parse("2026-08-12T23:59:00Z"));
        }
    }

    private static final class PayloadDeserializer implements DeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final TypeInformation<RowData> producedType;

        private PayloadDeserializer(DataType payloadType) {
            this.producedType = InternalTypeInfo.of((RowType) payloadType.getLogicalType());
        }

        @Override
        public RowData deserialize(byte[] message) {
            String[] fields =
                    new String(message, java.nio.charset.StandardCharsets.UTF_8).split(",");
            return GenericRowData.of(StringData.fromString(fields[0]), Integer.parseInt(fields[1]));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return producedType;
        }
    }
}
