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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.StringValue;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoMessageSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions;
import io.github.flink.gcp.connector.bigquery.testproto.WellKnownTypesChild;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ProtoSchemaOptions.Builder#wellKnownTypesAsMessages} against <b>real</b> BigQuery (#2107):
 * a table created beforehand with the {@code STRUCT} columns another writer gives a {@code
 * StringValue} and a {@code Duration} — {@code STRUCT<value STRING>} and {@code STRUCT<seconds
 * INT64, nanos INT64>} — takes rows from both write methods with no schema change. The sub-fields
 * are {@code REQUIRED}, the stricter of the two shapes such a table can have: the derived schema
 * says {@code NULLABLE}, and the union keeps the table's mode. Without the option the sink refuses
 * such a table before writing ({@code WellKnownTypesAsMessagesTest} pins that refusal and the
 * schema union for both shapes, offline).
 *
 * <p>Real GCP because load jobs have no emulator; skipped unless {@code BQ_IT_PROJECT}, {@code
 * BQ_IT_DATASET} and {@code BQ_IT_GCS_BUCKET} are set.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryProtoWellKnownTypesAsMessagesRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String LOAD_TABLE = "wkt_as_messages_load_" + RUN_ID;
    private static final String APPEND_TABLE = "wkt_as_messages_append_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-wkt-as-messages-it/" + RUN_ID;

    private static final ProtoSchemaOptions AS_MESSAGES =
            ProtoSchemaOptions.builder()
                    .wellKnownTypesAsMessages(
                            ProtoSchemaOptions.WellKnownType.DURATION,
                            ProtoSchemaOptions.WellKnownType.WRAPPERS)
                    .build();

    /**
     * Rows travel as {@code "set"} or {@code "unset"}, so the job's records need no protobuf type
     * information; the delegate is the serializer under test.
     */
    private static final class ChildSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private final ProtoMessageSerializationSchema<WellKnownTypesChild> delegate =
                ProtoMessageSerializationSchema.of(WellKnownTypesChild.class, AS_MESSAGES);

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
            WellKnownTypesChild.Builder child = WellKnownTypesChild.newBuilder();
            if (element.equals("set")) {
                // An empty StringValue: set, so {value: ""} rather than NULL.
                child.setCString(StringValue.of(""))
                        .setCDuration(Duration.newBuilder().setSeconds(90).setNanos(5));
            }
            return delegate.serialize(child.build());
        }
    }

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () -> RealBigQuery.deleteTables(LOAD_TABLE, APPEND_TABLE),
                () -> RealGcs.deletePrefix(STAGING_PREFIX));
    }

    @Test
    void aLoadJobWritesToTheExistingStructColumns() throws Exception {
        RealBigQuery.createTable(LOAD_TABLE, existingSchema());

        run(
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .table(RealTables.destination(LOAD_TABLE))
                        .serializer(new ChildSerializer())
                        .fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath(RealGcs.uri(STAGING_PREFIX))
                                        .build())
                        .build());

        assertRows(LOAD_TABLE);
    }

    @Test
    void theStorageWriteApiAppendsToTheExistingStructColumns() throws Exception {
        RealBigQuery.createTable(APPEND_TABLE, existingSchema());

        run(
                BigQuerySink.<String>builder()
                        .table(RealTables.destination(APPEND_TABLE))
                        .serializer(new ChildSerializer())
                        .build());

        assertRows(APPEND_TABLE);
    }

    private static void run(org.apache.flink.api.connector.sink2.Sink<String> sink)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.fromData("set", "unset").sinkTo(sink);
        env.execute("wkt-as-messages-it");
    }

    private static void assertRows(String table) throws Exception {
        String path = RealBigQuery.tablePath(table);
        List<FieldValueList> rows =
                RealBigQuery.queryRows(
                        "SELECT c_string IS NULL, c_string.value, c_duration IS NULL,"
                                + " c_duration.seconds, c_duration.nanos FROM "
                                + path
                                + " ORDER BY c_string IS NULL");
        assertThat(rows).hasSize(2);
        FieldValueList set = rows.get(0);
        assertThat(set.get(0).getBooleanValue()).as("c_string IS NULL, set row").isFalse();
        assertThat(set.get(1).isNull()).as("c_string.value IS NULL, set row").isFalse();
        assertThat(set.get(1).getStringValue()).as("c_string.value").isEmpty();
        assertThat(set.get(2).getBooleanValue()).as("c_duration IS NULL, set row").isFalse();
        assertThat(set.get(3).getLongValue()).as("c_duration.seconds").isEqualTo(90L);
        assertThat(set.get(4).getLongValue()).as("c_duration.nanos").isEqualTo(5L);
        FieldValueList unset = rows.get(1);
        assertThat(unset.get(0).getBooleanValue()).as("c_string IS NULL, unset row").isTrue();
        assertThat(unset.get(2).getBooleanValue()).as("c_duration IS NULL, unset row").isTrue();

        // The sink changed nothing about the table it was pointed at.
        Field string = RealBigQuery.tableFields(table).get("c_string");
        assertThat(string.getType()).isEqualTo(LegacySQLTypeName.RECORD);
        assertThat(string.getSubFields()).extracting(Field::getName).containsExactly("value");
        assertThat(RealBigQuery.tableFields(table).get("c_duration").getSubFields())
                .extracting(Field::getName)
                .containsExactly("seconds", "nanos");
    }

    /** Each type a NULLABLE record whose sub-fields are REQUIRED. */
    private static Schema existingSchema() {
        return Schema.of(
                Field.newBuilder(
                                "c_string",
                                StandardSQLTypeName.STRUCT,
                                required("value", StandardSQLTypeName.STRING))
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                Field.newBuilder(
                                "c_duration",
                                StandardSQLTypeName.STRUCT,
                                required("seconds", StandardSQLTypeName.INT64),
                                required("nanos", StandardSQLTypeName.INT64))
                        .setMode(Field.Mode.NULLABLE)
                        .build());
    }

    private static Field required(String name, StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REQUIRED).build();
    }
}
