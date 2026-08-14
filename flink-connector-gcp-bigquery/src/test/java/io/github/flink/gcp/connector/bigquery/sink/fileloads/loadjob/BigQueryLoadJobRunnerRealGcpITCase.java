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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableConstraints;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.RealGcs;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The re-attach path against BigQuery itself, on the suite's regional dataset, with {@code
 * location} unset — the configuration #491 measured as permanently stuck before the runner derived
 * each job's location from its destination dataset.
 *
 * <p>No other coverage reaches this: the FILE_LOADS suites run happy paths whose every id is fresh,
 * so their green against this same regional dataset says nothing about recovery, and the unit tests
 * script the metadata answer the derivation exists to fetch. What only the service can answer is
 * that the derived location is the one BigQuery inferred the first attempt's job into — that a
 * second attempt's {@code jobs.get} under it finds the finished job and attaches instead of
 * colliding.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(600)
class BigQueryLoadJobRunnerRealGcpITCase {

    private static final String TABLE = "load_reattach_" + TestNames.runId();
    private static final String QUERY_SOURCE = "truncate_data_source_" + TestNames.runId();
    private static final String QUERY_DESTINATION =
            "truncate_data_destination_" + TestNames.runId();

    /** Real polling backs off gently: a small load job usually finishes within a few seconds. */
    private static final RetrySchedule POLL = new RetrySchedule(500, 5_000, Integer.MAX_VALUE, 0);

    @AfterAll
    static void cleanUp() {
        RealBigQuery.deleteTables(TABLE, QUERY_SOURCE, QUERY_DESTINATION);
        RealGcs.deletePrefix(TABLE + "/");
    }

    @Test
    void reAttachesToAPreviousAttemptsLoadJobOnTheRegionalDataset() throws Exception {
        String path = TABLE + "/rows.avro";
        RealGcs.upload(path, oneRowAvroFile());
        LoadJobSpec spec =
                new LoadJobSpec(
                        RealBigQuery.destination(TABLE),
                        List.of(RealGcs.uri(path)),
                        Schema.of(Field.of("f1", StandardSQLTypeName.STRING)),
                        JobInfo.CreateDisposition.CREATE_IF_NEEDED,
                        JobInfo.WriteDisposition.WRITE_APPEND,
                        List.of(),
                        StagingFormat.AVRO);
        // Deterministic across the two runners below — the property re-attach rests on — and
        // unique across test runs, so BigQuery's six-month id retention cannot collide.
        String jobId = "flink-bq-load-reattach-it-" + TABLE;

        // The location is deliberately not passed: this suite's dataset is regional, and a runner
        // that builds location-less ids is exactly the stuck configuration under test — the first
        // submission would succeed (the server infers the location) and the second would miss the
        // probe and collide. Do not "fix" these constructors by supplying one.
        BigQueryLoadJobRunner first = new BigQueryLoadJobRunner(null, POLL);
        first.submitLoad(jobId, spec);
        first.awaitJob(jobId);

        // A fresh runner, as a failed-over or retrying committer would build one.
        BigQueryLoadJobRunner second = new BigQueryLoadJobRunner(null, POLL);
        second.submitLoad(jobId, spec);
        second.awaitJob(jobId);

        // Re-attached, not re-run: a second WRITE_APPEND load of the same file would have
        // doubled the rows.
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + RealBigQuery.tablePath(TABLE)))
                .containsExactly(1L);
    }

    @Test
    void terminalQueryReplacesRowsAndPreservesDestinationMetadata() throws Exception {
        Field id =
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setDescription("stable id description")
                        .build();
        Schema schema = Schema.of(id);
        RealBigQuery.createTable(QUERY_SOURCE, schema);
        RealBigQuery.queryRows(
                "INSERT INTO " + RealBigQuery.tablePath(QUERY_SOURCE) + " VALUES (2), (3)");
        RealBigQuery.createTableWithMetadata(
                QUERY_DESTINATION,
                schema,
                "stable destination description",
                Map.of("owner", "terminal-query-it"),
                TableConstraints.newBuilder()
                        .setPrimaryKey(PrimaryKey.newBuilder().setColumns(List.of("id")).build())
                        .build());
        RealBigQuery.queryRows(
                "INSERT INTO " + RealBigQuery.tablePath(QUERY_DESTINATION) + " VALUES (1)");
        String jobId = "flink-bq-query-truncate-data-it-" + QUERY_DESTINATION;
        QueryJobSpec spec =
                new QueryJobSpec(
                        RealBigQuery.destination(QUERY_SOURCE),
                        RealBigQuery.destination(QUERY_DESTINATION),
                        List.of());

        BigQueryLoadJobRunner runner = new BigQueryLoadJobRunner(null, POLL);
        runner.submitQuery(jobId, spec);
        runner.awaitJob(jobId);

        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT id FROM "
                                        + RealBigQuery.tablePath(QUERY_DESTINATION)
                                        + " ORDER BY id"))
                .containsExactly(2L, 3L);
        assertThat(RealBigQuery.tableDescription(QUERY_DESTINATION))
                .isEqualTo("stable destination description");
        assertThat(RealBigQuery.tableLabels(QUERY_DESTINATION))
                .containsEntry("owner", "terminal-query-it");
        assertThat(RealBigQuery.tableFields(QUERY_DESTINATION).get("id").getDescription())
                .isEqualTo("stable id description");
        assertThat(RealBigQuery.tableConstraints(QUERY_DESTINATION).getPrimaryKey().getColumns())
                .containsExactly("id");
        assertThat(RealBigQuery.queryBytesProcessed(jobId)).isPositive();
    }

    private static byte[] oneRowAvroFile() throws IOException {
        org.apache.avro.Schema schema =
                SchemaBuilder.record("Row").fields().requiredString("f1").endRecord();
        GenericRecord row = new GenericData.Record(schema);
        row.put("f1", "one");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer =
                new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, out);
            writer.append(row);
        }
        return out.toByteArray();
    }
}
