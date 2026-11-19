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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowMessageProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCase;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCaseChild;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tables, sinks and checks the gated ITCases share for the {@code MixedCase} message: one row,
 * its columns spelled {@code id}, {@code hasNext}, {@code beforeUpdate}, {@code Items}, {@code
 * countsByName} and {@code userId}, written into a table created beforehand.
 */
final class MixedCaseTables {

    /** The JSON form of the record {@link MixedCaseSerializer} writes, keys spelled as columns. */
    static final String DOCUMENT =
            "{\"id\":\"a\",\"hasNext\":true,\"beforeUpdate\":{\"innerValue\":\"b\",\"Count\":2},"
                    + "\"Items\":[{\"innerValue\":\"c\",\"Count\":3}],"
                    + "\"countsByName\":[{\"key\":\"k\",\"value\":4}],\"userId\":5}";

    private MixedCaseTables() {}

    /**
     * Rows travel as their {@code id}, so the job's records need no protobuf type information; the
     * delegate is the serializer under test, and its message is handed over as the delegate hands
     * it over, so a load job converts the message rather than parsing its bytes.
     */
    static final class MixedCaseSerializer extends BigQueryProtoSerializationSchema<String>
            implements ProtoRowMessageProvider<String> {
        private static final long serialVersionUID = 1L;

        private final ProtoMessageSerializationSchema<MixedCase> delegate =
                ProtoMessageSerializationSchema.of(MixedCase.class);

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return delegate.getTableSchema(destination);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return delegate.getDescriptor(destination);
        }

        @Override
        public Object getSchemaFingerprint(TableDestination destination) {
            return delegate.getSchemaFingerprint(destination);
        }

        @Override
        public ByteString serialize(String element) throws IOException {
            return delegate.serialize(record(element));
        }

        @Override
        public Message serializeToMessage(String element) throws IOException {
            return delegate.serializeToMessage(record(element));
        }

        private static MixedCase record(String id) {
            return MixedCase.newBuilder()
                    .setId(id)
                    .setHasNext(true)
                    .setBeforeUpdate(MixedCaseChild.newBuilder().setInnerValue("b").setCount(2L))
                    .addItems(MixedCaseChild.newBuilder().setInnerValue("c").setCount(3L))
                    .putCountsByName("k", 4L)
                    .setUserId(5L)
                    .build();
        }
    }

    /** Creates the table and returns its fields as BigQuery reports them. */
    static FieldList createTable(String table, TableSchema schema) {
        RealTables.createTable(table, schema);
        return RealBigQuery.tableFields(table);
    }

    static Sink<String> loadSink(
            String table,
            BigQueryProtoSerializationSchema<String> serializer,
            String stagingPrefix) {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .table(RealTables.destination(table))
                .serializer(serializer)
                .fileLoadsOptions(
                        FileLoadsOptions.builder().stagingPath(RealGcs.uri(stagingPrefix)).build())
                .build();
    }

    static Sink<String> appendSink(
            String table, BigQueryProtoSerializationSchema<String> serializer) {
        return BigQuerySink.<String>builder()
                .table(RealTables.destination(table))
                .serializer(serializer)
                .build();
    }

    /** Writes the one record in a bounded job. */
    static void run(Sink<String> sink, String record) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.fromData(record).sinkTo(sink);
        env.execute("mixed-case-it");
    }

    /** The premise of a table created from the message's own schema: the names keep their case. */
    static void assertMixedCaseSpelling(FieldList fields) {
        assertThat(fields)
                .extracting(Field::getName)
                .containsExactly(
                        "id", "hasNext", "beforeUpdate", "Items", "countsByName", "userId");
        assertThat(fields.get("beforeUpdate").getSubFields())
                .extracting(Field::getName)
                .containsExactly("innerValue", "Count");
    }

    /** The one row carries each value in its column, and the table is as it was created. */
    static void assertRow(String table, FieldList created) throws Exception {
        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT id, hasNext, beforeUpdate.innerValue, beforeUpdate.Count,"
                                + " ARRAY_LENGTH(Items), Items[OFFSET(0)].innerValue,"
                                + " Items[OFFSET(0)].Count, ARRAY_LENGTH(countsByName),"
                                + " countsByName[OFFSET(0)].key, countsByName[OFFSET(0)].value,"
                                + " userId FROM "
                                + RealBigQuery.tablePath(table));
        assertThat(rows).hasSize(1);
        FieldValueList row = rows.get(0);
        assertThat(row.get(0).getStringValue()).as("id").isEqualTo("a");
        assertThat(row.get(1).getBooleanValue()).as("hasNext").isTrue();
        assertThat(row.get(2).getStringValue()).as("beforeUpdate.innerValue").isEqualTo("b");
        assertThat(row.get(3).getLongValue()).as("beforeUpdate.Count").isEqualTo(2L);
        assertThat(row.get(4).getLongValue()).as("ARRAY_LENGTH(Items)").isEqualTo(1L);
        assertThat(row.get(5).getStringValue()).as("Items[0].innerValue").isEqualTo("c");
        assertThat(row.get(6).getLongValue()).as("Items[0].Count").isEqualTo(3L);
        assertThat(row.get(7).getLongValue()).as("ARRAY_LENGTH(countsByName)").isEqualTo(1L);
        assertThat(row.get(8).getStringValue()).as("countsByName[0].key").isEqualTo("k");
        assertThat(row.get(9).getLongValue()).as("countsByName[0].value").isEqualTo(4L);
        assertThat(row.get(10).getLongValue()).as("userId").isEqualTo(5L);

        // The sink changed nothing about the table it was pointed at, nested columns, types,
        // modes and spelling included.
        assertThat(RealBigQuery.tableFields(table)).isEqualTo(created);
    }
}
