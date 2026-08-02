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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test against <b>real</b> BigQuery and Cloud Storage (the sink's post-load
 * cleanup and BigQuery load jobs have no emulator: goccy/bigquery-emulator supports neither {@code
 * gs://} load jobs nor a storage endpoint).
 *
 * <p>Runs a MiniCluster DataStream job in batch mode with dynamic destinations across two tables —
 * the acceptance scenario of issue #14. Load jobs are free; the test only costs cents of storage
 * for minutes.
 *
 * <p>Also the only place a {@code GEOGRAPHY} column is loaded end to end (#126): a staged Avro
 * {@code string} against an explicit destination schema saying {@code GEOGRAPHY}, a pairing
 * BigQuery's documentation describes for CSV and JSON but not for Avro.
 *
 * <p>Requires application-default credentials plus:
 *
 * <ul>
 *   <li>{@code BQ_IT_PROJECT} — project to write to (and run jobs in)
 *   <li>{@code BQ_IT_DATASET} — existing dataset for the destination tables
 *   <li>{@code BQ_IT_GCS_BUCKET} — existing bucket for staging (a lifecycle rule is recommended)
 * </ul>
 *
 * <p>Skipped automatically when the variables are absent, keeping {@code ./mvnw verify} and CI
 * credential-free.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryFileLoadsITCase {

    private static final String PROJECT = System.getenv("BQ_IT_PROJECT");
    private static final String DATASET = System.getenv("BQ_IT_DATASET");
    private static final String BUCKET = System.getenv("BQ_IT_GCS_BUCKET");

    private static final String RUN_ID = TestNames.runId();
    private static final String TABLE_A = "file_loads_it_a_" + RUN_ID;
    private static final String TABLE_B = "file_loads_it_b_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-file-loads-it/" + RUN_ID;

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("value")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    // Here to prove the claim #126 rests on: that a GEOGRAPHY column survives the
                    // whole FILE_LOADS path. The staging converters have folded GEOGRAPHY in with
                    // STRING and JSON since FILE_LOADS was written, but nothing could derive such a
                    // column until the marker options existed, so no load job had ever carried one
                    // — and BigQuery's own documentation spells out WKT loading for CSV and JSON
                    // only, never for Avro. What is under test is the pairing: an Avro `string`
                    // field against an explicit destination schema that says GEOGRAPHY.
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("boundary")
                                    .setType(TableFieldSchema.Type.GEOGRAPHY)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /**
     * Rows travel as {@code "table|name|value|boundary"} strings (an empty value or boundary means
     * NULL).
     */
    private static final class RowSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return descriptor();
        }

        private Descriptors.Descriptor descriptor() {
            if (descriptor == null) {
                try {
                    descriptor =
                            BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                                    SCHEMA);
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split("\\|", -1);
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(descriptor().findFieldByName("name"), parts[1]);
            if (!parts[2].isEmpty()) {
                row.setField(descriptor().findFieldByName("value"), Long.parseLong(parts[2]));
            }
            if (!parts[3].isEmpty()) {
                row.setField(descriptor().findFieldByName("boundary"), parts[3]);
            }
            return row.build().toByteString();
        }
    }

    @AfterAll
    static void cleanUp() {
        BigQuery bigQuery = bigQuery();
        bigQuery.delete(TableId.of(PROJECT, DATASET, TABLE_A));
        bigQuery.delete(TableId.of(PROJECT, DATASET, TABLE_B));
        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();
        for (Blob blob :
                storage.list(BUCKET, Storage.BlobListOption.prefix(STAGING_PREFIX)).iterateAll()) {
            blob.delete();
        }
    }

    @Test
    void multiTableBatchLoad() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        env.fromData(
                        TABLE_A + "|alpha|1|POINT(1 2)",
                        TABLE_A + "|beta|2|LINESTRING(0 0, 1 1)",
                        // A NULL geography as well as a populated one: the column is NULLABLE, and
                        // an unset one has to load too.
                        TABLE_A + "|gamma||",
                        TABLE_A + "|delta|4|",
                        TABLE_B + "|epsilon|5|POINT(3 4)",
                        TABLE_B + "|zeta|6|")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        PROJECT,
                                                        DATASET,
                                                        element.substring(0, element.indexOf('|'))))
                                .serializer(new RowSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(
                                                        "gs://" + BUCKET + "/" + STAGING_PREFIX)
                                                .build())
                                .build());

        env.execute("file-loads-it");

        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", TABLE_A)).containsExactly(4L);
        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", TABLE_B)).containsExactly(2L);
        assertThat(
                        queryLongs(
                                "SELECT value FROM `%s` WHERE name = 'beta' AND value IS NOT NULL",
                                TABLE_A))
                .containsExactly(2L);
        assertThat(
                        queryLongs(
                                "SELECT COUNT(*) FROM `%s` WHERE name = 'gamma' AND value IS"
                                        + " NULL",
                                TABLE_A))
                .containsExactly(1L);

        // The GEOGRAPHY column: the value BigQuery parsed out of the staged Avro string, read back
        // as WKT. A load that had stored the text verbatim in a STRING column would fail ST_AsText.
        assertThat(
                        queryStrings(
                                "SELECT ST_ASTEXT(boundary) FROM `%s` WHERE name = 'alpha'",
                                TABLE_A))
                .containsExactly("POINT(1 2)");
        assertThat(
                        queryLongs(
                                "SELECT COUNT(*) FROM `%s` WHERE name = 'gamma' AND boundary IS"
                                        + " NULL",
                                TABLE_A))
                .containsExactly(1L);

        // Staged objects are deleted after a successful load.
        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();
        assertThat(storage.list(BUCKET, Storage.BlobListOption.prefix(STAGING_PREFIX)).iterateAll())
                .isEmpty();
    }

    private static List<Long> queryLongs(String queryTemplate, String table) throws Exception {
        String query = String.format(queryTemplate, PROJECT + "." + DATASET + "." + table);
        TableResult result = bigQuery().query(QueryJobConfiguration.newBuilder(query).build());
        List<Long> values = new ArrayList<>();
        result.iterateAll()
                .forEach(row -> values.add(row.get(0).isNull() ? null : row.get(0).getLongValue()));
        return values;
    }

    private static List<String> queryStrings(String queryTemplate, String table) throws Exception {
        String query = String.format(queryTemplate, PROJECT + "." + DATASET + "." + table);
        TableResult result = bigQuery().query(QueryJobConfiguration.newBuilder(query).build());
        List<String> values = new ArrayList<>();
        result.iterateAll()
                .forEach(
                        row ->
                                values.add(
                                        row.get(0).isNull() ? null : row.get(0).getStringValue()));
        return values;
    }

    private static BigQuery bigQuery() {
        return BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
    }
}
