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
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowMessageProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCase;
import io.github.flink.gcp.connector.bigquery.testproto.MixedCaseChild;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Protobuf field names with upper-case letters against <b>real</b> BigQuery (#2179): a table
 * created beforehand with the columns {@code hasNext}, {@code beforeUpdate} and so on takes rows
 * from both write methods with no schema change. The columns keep the field names as written, while
 * the client library lower-cases the row descriptor's field names, so the Storage Write API half
 * rests on the service matching a column name case-insensitively. The load job half stages Avro
 * under the column names themselves. A table whose columns are spelled in another case, here all
 * lower-case as another writer may have created them, takes the same rows with no schema change
 * too. {@code ProtoRowConverterTest} and {@code ProtoToAvroConverterTest} pin the client-side
 * pairing offline.
 *
 * <p>Real GCP because load jobs have no emulator; skipped unless {@code BQ_IT_PROJECT}, {@code
 * BQ_IT_DATASET} and {@code BQ_IT_GCS_BUCKET} are set.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryProtoMixedCaseFieldNamesRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String LOAD_TABLE = "mixed_case_load_" + RUN_ID;
    private static final String APPEND_TABLE = "mixed_case_append_" + RUN_ID;
    private static final String LOWER_LOAD_TABLE = "mixed_case_lower_load_" + RUN_ID;
    private static final String LOWER_APPEND_TABLE = "mixed_case_lower_append_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-mixed-case-it/" + RUN_ID;

    /**
     * Rows travel as their {@code id}, so the job's records need no protobuf type information; the
     * delegate is the serializer under test, and its message is handed over as the delegate hands
     * it over, so a load job converts the message rather than parsing its bytes.
     */
    private static final class MixedCaseSerializer extends BigQueryProtoSerializationSchema<String>
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

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                LOAD_TABLE, APPEND_TABLE, LOWER_LOAD_TABLE, LOWER_APPEND_TABLE),
                () -> RealGcs.deletePrefix(STAGING_PREFIX));
    }

    @Test
    void aLoadJobWritesToTheMixedCaseColumns() throws Exception {
        FieldList created = createTable(LOAD_TABLE, new MixedCaseSerializer().getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(loadSink(LOAD_TABLE));

        assertRow(LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsToTheMixedCaseColumns() throws Exception {
        FieldList created =
                createTable(APPEND_TABLE, new MixedCaseSerializer().getTableSchema(null));
        assertMixedCaseSpelling(created);

        run(appendSink(APPEND_TABLE));

        assertRow(APPEND_TABLE, created);
    }

    @Test
    void aLoadJobWritesToColumnsSpelledInAnotherCase() throws Exception {
        FieldList created =
                createTable(
                        LOWER_LOAD_TABLE,
                        lowerCased(new MixedCaseSerializer().getTableSchema(null)));
        assertLowerCaseSpelling(created);

        run(loadSink(LOWER_LOAD_TABLE));

        assertRow(LOWER_LOAD_TABLE, created);
    }

    @Test
    void theStorageWriteApiAppendsToColumnsSpelledInAnotherCase() throws Exception {
        FieldList created =
                createTable(
                        LOWER_APPEND_TABLE,
                        lowerCased(new MixedCaseSerializer().getTableSchema(null)));
        assertLowerCaseSpelling(created);

        run(appendSink(LOWER_APPEND_TABLE));

        assertRow(LOWER_APPEND_TABLE, created);
    }

    /** Creates the table and returns its fields as BigQuery reports them. */
    private static FieldList createTable(String table, TableSchema schema) {
        RealTables.createTable(table, schema);
        return RealBigQuery.tableFields(table);
    }

    private static Sink<String> loadSink(String table) {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .table(RealTables.destination(table))
                .serializer(new MixedCaseSerializer())
                .fileLoadsOptions(
                        FileLoadsOptions.builder().stagingPath(RealGcs.uri(STAGING_PREFIX)).build())
                .build();
    }

    private static Sink<String> appendSink(String table) {
        return BigQuerySink.<String>builder()
                .table(RealTables.destination(table))
                .serializer(new MixedCaseSerializer())
                .build();
    }

    /** The serializer's schema with every column name lower-cased, at every depth. */
    private static TableSchema lowerCased(TableSchema schema) {
        TableSchema.Builder builder = TableSchema.newBuilder();
        for (TableFieldSchema field : schema.getFieldsList()) {
            builder.addFields(lowerCased(field));
        }
        return builder.build();
    }

    private static TableFieldSchema lowerCased(TableFieldSchema field) {
        TableFieldSchema.Builder builder =
                field.toBuilder().setName(field.getName().toLowerCase(Locale.ROOT)).clearFields();
        for (TableFieldSchema subField : field.getFieldsList()) {
            builder.addFields(lowerCased(subField));
        }
        return builder.build();
    }

    private static void assertMixedCaseSpelling(FieldList fields) {
        assertThat(fields)
                .extracting(Field::getName)
                .containsExactly(
                        "id", "hasNext", "beforeUpdate", "Items", "countsByName", "userId");
        assertThat(fields.get("beforeUpdate").getSubFields())
                .extracting(Field::getName)
                .containsExactly("innerValue", "Count");
    }

    /** The premise of the two other-case tests: the table's spelling differs from the fields'. */
    private static void assertLowerCaseSpelling(FieldList fields) {
        assertThat(fields)
                .extracting(Field::getName)
                .containsExactly(
                        "id", "hasnext", "beforeupdate", "items", "countsbyname", "userid");
        assertThat(fields.get("beforeupdate").getSubFields())
                .extracting(Field::getName)
                .containsExactly("innervalue", "count");
    }

    private static void run(Sink<String> sink) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.fromData("a").sinkTo(sink);
        env.execute("mixed-case-field-names-it");
    }

    private static void assertRow(String table, FieldList created) throws Exception {
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
