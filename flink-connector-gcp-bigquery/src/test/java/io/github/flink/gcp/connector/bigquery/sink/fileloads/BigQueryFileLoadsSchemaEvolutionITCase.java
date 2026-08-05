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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.RealGcs;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

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
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryFileLoadsSchemaEvolutionITCase {

    private static final String RUN_ID = TestNames.runId();
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
    private static final class RowSerializer extends FixedSchemaProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return EXTENDED_SCHEMA;
        }

        @Override
        public ByteString serialize(String element) {
            String[] parts = element.split("\\|", -1);
            DynamicMessage.Builder row = DynamicMessage.newBuilder(descriptor());
            row.setField(field("name"), parts[0]);
            row.setField(field("extra"), Long.parseLong(parts[1]));
            return row.build().toByteString();
        }
    }

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(EVOLVING_TABLE, STRICT_TABLE);
        RealGcs.deletePrefix(STAGING_PREFIX);
    }

    @Test
    void newRequiredColumnLandsAsNullableOnAPreExistingTable() throws Exception {
        RealBigQuery.createTable(EVOLVING_TABLE, Schema.of(nameField()));

        runJob(EVOLVING_TABLE, SchemaUpdateOptions.builder().allowNewFields().build());

        String evolvingPath = RealBigQuery.tablePath(EVOLVING_TABLE);
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + evolvingPath))
                .containsExactly(2L);
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT extra FROM "
                                        + evolvingPath
                                        + " WHERE name = 'alpha' AND extra IS NOT NULL"))
                .containsExactly(1L);

        // The live table gained the column, and as NULLABLE — the union demoted the serializer's
        // REQUIRED, which BigQuery cannot add to an existing table.
        Field extra = RealBigQuery.tableFields(EVOLVING_TABLE).get("extra");
        assertThat(extra.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(extra.getMode()).isEqualTo(Field.Mode.NULLABLE);
    }

    @Test
    void newColumnWithoutAllowNewFieldsIsDroppedByTheLoad() throws Exception {
        // The pre-created table also carries an INTERVAL column — a type the serializers cannot
        // derive — because with updates disabled the load's provided schema is the live one
        // verbatim, so it must remain loadable even when the table has columns this write method
        // could never create. Measured here: BigQuery accepts a provided schema naming INTERVAL.
        RealBigQuery.createTable(
                STRICT_TABLE,
                Schema.of(
                        nameField(),
                        Field.newBuilder("span", StandardSQLTypeName.INTERVAL)
                                .setMode(Field.Mode.NULLABLE)
                                .build()));

        // With updates disabled the live schema wins and the load job carries it. Measured, not
        // designed: BigQuery then ignores a staged Avro field absent from that schema, so the
        // rows load and the extra column's data is dropped (the orchestrator warns once per
        // destination). Before the fix this failed the whole job at submission instead ("Cannot
        // add fields"); the temp-table path has always behaved as pinned here.
        runJob(STRICT_TABLE, SchemaUpdateOptions.builder().build());

        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(STRICT_TABLE)))
                .containsExactly(2L);
        assertThat(RealBigQuery.tableFields(STRICT_TABLE))
                .extracting(Field::getName)
                .containsExactly("name", "span");
    }

    private static void runJob(String table, SchemaUpdateOptions updateOptions) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        env.fromData("alpha|1", "beta|2")
                .sinkTo(
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destination(RealBigQuery.destination(table))
                                .serializer(new RowSerializer())
                                .schemaUpdateOptions(updateOptions)
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(
                                                        RealGcs.uri(STAGING_PREFIX + "/" + table))
                                                .build())
                                .build());

        env.execute("file-loads-schema-evolution-it");
    }

    private static Field nameField() {
        return Field.newBuilder("name", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build();
    }
}
