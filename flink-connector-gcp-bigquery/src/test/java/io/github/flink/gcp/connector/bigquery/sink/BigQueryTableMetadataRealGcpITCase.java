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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Table descriptions and labels against <b>real</b> BigQuery (#1668): what a table the sink creates
 * carries on each write method.
 *
 * <p>Real GCP because the emulator is not evidence for it: whether a {@code FILE_LOADS} load keeps
 * the description and labels of the table it writes into is the service's behavior.
 *
 * <p>Requires application-default credentials plus {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET} and
 * {@code BQ_IT_GCS_BUCKET}; skipped when they are absent.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryTableMetadataRealGcpITCase {

    private static final String RUN_ID = TestNames.runId();
    private static final String CREATED_DEFAULT_STREAM = "table_metadata_it_ds_" + RUN_ID;
    private static final String CREATED_APPEND = "table_metadata_it_append_" + RUN_ID;
    private static final String CREATED_TRUNCATE = "table_metadata_it_truncate_" + RUN_ID;
    private static final String CREATED_EXACTLY_ONCE = "table_metadata_it_eo_" + RUN_ID;
    private static final String STAGING_ROOT = "flink-table-metadata-it/" + RUN_ID;

    private static final String COLUMN_DESCRIPTION = "What the row is called";

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .setDescription(COLUMN_DESCRIPTION))
                    .build();

    private static final TableCreateOptions METADATA =
            TableCreateOptions.builder()
                    .description("Written by the table metadata IT")
                    .labels(labels("team", "sink", "pipeline", "table_metadata_it"))
                    .build();

    /** Rows travel as {@code "table|name"}; the schema carries a column description. */
    private static final class RowSerializer extends BigQueryProtoSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = super.getDescriptor(destination);
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(String element) {
            Descriptors.Descriptor row = getDescriptor(null);
            return DynamicMessage.newBuilder(row)
                    .setField(
                            row.findFieldByName("name"),
                            element.substring(element.indexOf('|') + 1))
                    .build()
                    .toByteString();
        }
    }

    private static Map<String, String> labels(String... keysAndValues) {
        Map<String, String> labels = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            labels.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return labels;
    }

    private static DestinationResolver<String> byPrefix() {
        return (element, context) ->
                RealTables.destination(element.substring(0, element.indexOf('|')));
    }

    private static StreamExecutionEnvironment batch() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }

    private static FileLoadsOptions fileLoads(String staging, WriteDisposition disposition) {
        return FileLoadsOptions.builder()
                .stagingPath(RealGcs.uri(STAGING_ROOT + "/" + staging))
                .writeDisposition(disposition)
                .build();
    }

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () ->
                        RealBigQuery.deleteTables(
                                CREATED_DEFAULT_STREAM,
                                CREATED_APPEND,
                                CREATED_TRUNCATE,
                                CREATED_EXACTLY_ONCE),
                () -> RealGcs.deletePrefix(STAGING_ROOT));
    }

    private static void assertCreatedWithMetadata(String table) {
        assertThat(RealBigQuery.tableDescription(table)).isEqualTo(METADATA.getDescription());
        assertThat(RealBigQuery.tableLabels(table)).isEqualTo(METADATA.getLabels());
        assertThat(RealBigQuery.tableFields(table).get("name").getDescription())
                .isEqualTo(COLUMN_DESCRIPTION);
    }

    @Test
    void aTableTheDefaultStreamCreatesCarriesTheMetadata() throws Exception {
        StreamExecutionEnvironment env = batch();
        env.fromData(CREATED_DEFAULT_STREAM + "|a", CREATED_DEFAULT_STREAM + "|b")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .destinationResolver(byPrefix())
                                .serializer(new RowSerializer())
                                .tableCreateOptions(METADATA)
                                .build());

        env.execute("table-metadata-default-stream-it");

        assertCreatedWithMetadata(CREATED_DEFAULT_STREAM);
    }

    /**
     * The direct load into a table the commit created, under both dispositions. Whether a load that
     * replaces the table's data and schema also keeps its description and labels is the service's
     * answer, and the one this case records (ADR-0182). The copy that finishes an oversized
     * truncating commit is not covered.
     */
    @Test
    void aTableFileLoadsCreatesCarriesTheMetadataUnderEitherDisposition() throws Exception {
        for (WriteDisposition disposition :
                new WriteDisposition[] {
                    WriteDisposition.WRITE_APPEND, WriteDisposition.WRITE_TRUNCATE
                }) {
            String table =
                    disposition == WriteDisposition.WRITE_APPEND
                            ? CREATED_APPEND
                            : CREATED_TRUNCATE;
            StreamExecutionEnvironment env = batch();
            env.fromData(table + "|a", table + "|b")
                    .sinkTo(
                            BigQuerySink.<String>builder()
                                    .writeMethod(WriteMethod.FILE_LOADS)
                                    .destinationResolver(byPrefix())
                                    .serializer(new RowSerializer())
                                    .tableCreateOptions(METADATA)
                                    .fileLoadsOptions(fileLoads(table, disposition))
                                    .build());

            env.execute("table-metadata-file-loads-" + disposition + "-it");

            assertThat(
                            RealBigQuery.queryLongs(
                                    "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(table)))
                    .as(disposition.name())
                    .containsExactly(2L);
            assertCreatedWithMetadata(table);
        }
    }

    /**
     * The exactly-once write method creates its table on a path of its own ({@code createStream}),
     * separate from the default stream's repair.
     */
    @Test
    void aTableTheExactlyOnceMethodCreatesCarriesTheMetadata() throws Exception {
        StreamExecutionEnvironment env = batch();
        env.fromData(CREATED_EXACTLY_ONCE + "|a", CREATED_EXACTLY_ONCE + "|b")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .destinationResolver(byPrefix())
                                .serializer(new RowSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .tableCreateOptions(METADATA)
                                .build());

        env.execute("table-metadata-exactly-once-it");

        assertCreatedWithMetadata(CREATED_EXACTLY_ONCE);
    }
}
