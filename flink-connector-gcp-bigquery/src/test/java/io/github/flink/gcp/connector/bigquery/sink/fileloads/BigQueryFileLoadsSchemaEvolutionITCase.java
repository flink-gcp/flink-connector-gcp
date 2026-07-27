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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
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
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The #142 regression against <b>real</b> BigQuery: a pre-existing table whose schema the
 * serializer's derived schema extends with a {@code REQUIRED} column. Before direct loads
 * reconciled against the live table, this failed the whole job at submission — {@code "Cannot add
 * required fields to an existing schema"} — because BigQuery rejects adding a {@code REQUIRED}
 * column even under {@code ALLOW_FIELD_ADDITION}, while the multi-partition temp-table path demoted
 * the addition to {@code NULLABLE} and succeeded. The single-partition load must now take the same
 * path: the new column arrives {@code NULLABLE} and the rows load.
 *
 * <p>Real GCP because load jobs have no emulator (see {@link BigQueryFileLoadsITCase}); same
 * environment variables, skipped automatically when they are absent.
 */
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryFileLoadsSchemaEvolutionITCase {

    private static final String PROJECT = System.getenv("BQ_IT_PROJECT");
    private static final String DATASET = System.getenv("BQ_IT_DATASET");
    private static final String BUCKET = System.getenv("BQ_IT_GCS_BUCKET");

    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);
    private static final String EVOLVING_TABLE = "file_loads_evo_it_" + RUN_ID;
    private static final String STRICT_TABLE = "file_loads_strict_it_" + RUN_ID;
    private static final String STAGING_PREFIX = "flink-file-loads-evo-it/" + RUN_ID;

    /** What the serializer derives: the pre-created table's column plus a REQUIRED addition. */
    private static final TableSchema EXTENDED_SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("extra")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .build();

    /** Rows travel as {@code "name|extra"} strings. */
    private static final class RowSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return EXTENDED_SCHEMA;
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
                                    EXTENDED_SCHEMA);
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
            row.setField(descriptor().findFieldByName("name"), parts[0]);
            row.setField(descriptor().findFieldByName("extra"), Long.parseLong(parts[1]));
            return row.build().toByteString();
        }
    }

    @AfterAll
    static void cleanUp() {
        BigQuery bigQuery = bigQuery();
        bigQuery.delete(TableId.of(PROJECT, DATASET, EVOLVING_TABLE));
        bigQuery.delete(TableId.of(PROJECT, DATASET, STRICT_TABLE));
        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();
        for (Blob blob :
                storage.list(BUCKET, Storage.BlobListOption.prefix(STAGING_PREFIX)).iterateAll()) {
            blob.delete();
        }
    }

    @Test
    void newRequiredColumnLandsAsNullableOnAPreExistingTable() throws Exception {
        createNameOnlyTable(EVOLVING_TABLE);

        runJob(EVOLVING_TABLE, SchemaUpdateOptions.builder().allowNewFields().build());

        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", EVOLVING_TABLE)).containsExactly(2L);
        assertThat(
                        queryLongs(
                                "SELECT extra FROM `%s` WHERE name = 'alpha' AND extra IS NOT"
                                        + " NULL",
                                EVOLVING_TABLE))
                .containsExactly(1L);

        // The live table gained the column, and as NULLABLE — the union demoted the serializer's
        // REQUIRED, which BigQuery cannot add to an existing table.
        Field extra = liveSchema(EVOLVING_TABLE).getFields().get("extra");
        assertThat(extra.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(extra.getMode()).isEqualTo(Field.Mode.NULLABLE);
    }

    @Test
    void newColumnWithoutAllowNewFieldsIsDroppedByTheLoad() throws Exception {
        createNameOnlyTable(STRICT_TABLE);

        // With updates disabled the live schema wins and the load job carries it. Measured, not
        // designed: BigQuery then ignores a staged Avro field absent from that schema, so the
        // rows load and the extra column's data is dropped (the orchestrator warns once per
        // destination). Before the fix this failed the whole job at submission instead ("Cannot
        // add fields"); the temp-table path has always behaved as pinned here.
        runJob(STRICT_TABLE, SchemaUpdateOptions.builder().build());

        assertThat(queryLongs("SELECT COUNT(*) FROM `%s`", STRICT_TABLE)).containsExactly(2L);
        assertThat(liveSchema(STRICT_TABLE).getFields())
                .extracting(Field::getName)
                .containsExactly("name");
    }

    private static void runJob(String table, SchemaUpdateOptions updateOptions) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env.fromData("alpha|1", "beta|2")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destination(TableDestination.of(PROJECT, DATASET, table))
                                .serializer(new RowSerializer())
                                .schemaUpdateOptions(updateOptions)
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(
                                                        "gs://"
                                                                + BUCKET
                                                                + "/"
                                                                + STAGING_PREFIX
                                                                + "/"
                                                                + table)
                                                .build())
                                .build());

        env.execute("file-loads-schema-evolution-it");
    }

    private static void createNameOnlyTable(String table) {
        bigQuery()
                .create(
                        TableInfo.of(
                                TableId.of(PROJECT, DATASET, table),
                                StandardTableDefinition.of(
                                        Schema.of(
                                                Field.newBuilder("name", StandardSQLTypeName.STRING)
                                                        .setMode(Field.Mode.NULLABLE)
                                                        .build()))));
    }

    private static Schema liveSchema(String table) {
        TableDefinition definition =
                bigQuery().getTable(TableId.of(PROJECT, DATASET, table)).getDefinition();
        return definition.getSchema();
    }

    private static List<Long> queryLongs(String queryTemplate, String table) throws Exception {
        String query = String.format(queryTemplate, PROJECT + "." + DATASET + "." + table);
        TableResult result = bigQuery().query(QueryJobConfiguration.newBuilder(query).build());
        List<Long> values = new ArrayList<>();
        result.iterateAll()
                .forEach(row -> values.add(row.get(0).isNull() ? null : row.get(0).getLongValue()));
        return values;
    }

    private static BigQuery bigQuery() {
        return BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
    }
}
