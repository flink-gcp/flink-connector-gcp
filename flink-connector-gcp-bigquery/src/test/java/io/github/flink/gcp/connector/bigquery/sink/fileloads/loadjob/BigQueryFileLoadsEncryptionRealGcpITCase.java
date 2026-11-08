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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.metrics.SimpleCounter;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQuerySchemaConverter;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code FILE_LOADS} commit against BigQuery itself, into destinations encrypted with a Cloud KMS
 * key (#1995, ADR-0203).
 *
 * <p>BigQuery refuses an appending job that states a key other than its existing destination's, and
 * a {@code WRITE_TRUNCATE} job that states none resets the destination to the default encryption.
 * Only the service can say whether the keys the commit states satisfy both, so every commit here
 * runs through temporary tables and an intermediate copy level, and reads each temporary table's
 * key back as the commit deletes it.
 *
 * <p>Requires application-default credentials plus {@code BQ_IT_PROJECT}, {@code BQ_IT_DATASET},
 * {@code BQ_IT_GCS_BUCKET} and {@code BQ_IT_KMS_KEY}, a key in the dataset's location that
 * BigQuery's encryption service account may use.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_KMS_KEY", matches = ".+")
@Timeout(value = 1_800, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryFileLoadsEncryptionRealGcpITCase {

    private static final String RUN = TestNames.runId();
    private static final String PREFIX = "cmek_" + RUN + "/";
    private static final String KEY = System.getenv("BQ_IT_KMS_KEY");

    private static final RetrySchedule POLL = new RetrySchedule(500, 5_000, Integer.MAX_VALUE, 0);

    /** Three one-file loads per destination over a fan-out of two: one intermediate copy level. */
    private static final Limits THROUGH_TEMP_TABLES =
            new Limits(2, 100, 100, 100).withMaxFilesPerJob(1);

    private static final Schema SCHEMA =
            Schema.of(
                    Field.of("n", StandardSQLTypeName.INT64),
                    Field.of("region", StandardSQLTypeName.STRING));

    private static final String FLINK_JOB_ID = UUID.randomUUID().toString().replace("-", "");

    private static final List<String> TABLES = new ArrayList<>();

    @AfterAll
    static void cleanUp() throws Exception {
        Closers.closeAll(
                () -> RealBigQuery.deleteTables(TABLES.toArray(new String[0])),
                () -> RealGcs.deletePrefix(PREFIX));
    }

    @ParameterizedTest
    @EnumSource(WriteDisposition.class)
    void anExistingDestinationKeepsItsKeyAndLendsItToTheTemporaryTables(
            WriteDisposition disposition) throws Exception {
        String table = table("existing_" + disposition.name().toLowerCase(Locale.ROOT));
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(table)
                        + " (n INT64, region STRING) OPTIONS (kms_key_name = '"
                        + KEY
                        + "')");
        if (disposition != WriteDisposition.WRITE_EMPTY) {
            RealBigQuery.queryRows(
                    "INSERT INTO " + RealBigQuery.tablePath(table) + " VALUES (99, 'old')");
        }
        RecordingRunner runner = new RecordingRunner();

        orchestrator(disposition, TableCreateOptions.defaults(), runner, THROUGH_TEMP_TABLES)
                .run(stage(table, 3));

        assertRows(table, disposition == WriteDisposition.WRITE_APPEND ? 4 : 3);
        assertThat(RealBigQuery.tableKmsKeyName(table)).isEqualTo(KEY);
        assertThat(runner.deletedTableKeys)
                .isNotEmpty()
                .allSatisfy((t, k) -> assertThat(k).as(t).isEqualTo(KEY));
    }

    /**
     * The destination is clustered, so its temporary tables are laid out: created by the commit
     * ahead of their loads rather than by the loads themselves.
     */
    @Test
    void aCreatedDestinationAndItsTemporaryTablesTakeTheConfiguredKey() throws Exception {
        String table = table("created");
        RecordingRunner runner = new RecordingRunner();

        orchestrator(
                        WriteDisposition.WRITE_APPEND,
                        TableCreateOptions.builder()
                                .clusteredFields(List.of("region"))
                                .kmsKeyName(KEY)
                                .build(),
                        runner,
                        THROUGH_TEMP_TABLES)
                .run(stage(table, 3));

        assertRows(table, 3);
        assertThat(RealBigQuery.tableKmsKeyName(table)).isEqualTo(KEY);
        assertThat(runner.deletedTableKeys)
                .isNotEmpty()
                .allSatisfy((t, k) -> assertThat(k).as(t).isEqualTo(KEY));
    }

    /** The case that reset a destination's key to the default before #1995. */
    @Test
    void aDirectTruncatingLoadKeepsTheDestinationsKey() throws Exception {
        String table = table("direct_truncate");
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(table)
                        + " (n INT64, region STRING) OPTIONS (kms_key_name = '"
                        + KEY
                        + "')");

        orchestrator(
                        WriteDisposition.WRITE_TRUNCATE,
                        TableCreateOptions.defaults(),
                        new RecordingRunner(),
                        Limits.BIGQUERY)
                .run(stage(table, 1));

        assertRows(table, 1);
        assertThat(RealBigQuery.tableKmsKeyName(table)).isEqualTo(KEY);
    }

    private static String table(String name) {
        String table = "cmek_" + name + "_" + RUN;
        TABLES.add(table);
        return table;
    }

    private static void assertRows(String table, long rows) throws InterruptedException {
        assertThat(RealBigQuery.queryLongs("SELECT COUNT(*) FROM " + RealBigQuery.tablePath(table)))
                .as(table)
                .containsExactly(rows);
    }

    private static LoadJobOrchestrator orchestrator(
            WriteDisposition disposition,
            TableCreateOptions createOptions,
            LoadJobRunner runner,
            Limits limits) {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath(RealGcs.uri(PREFIX + "staging"))
                        .writeDisposition(disposition)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        // Unused: every committable names its own destination.
                                        .table(RealTables.destination("cmek_unused_" + RUN))
                                        .serializer(new SchemaOnlySerializer())
                                        .tableCreateOptions(createOptions)
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        return new LoadJobOrchestrator(
                config,
                options,
                runner,
                new BigQueryTableAdmin(),
                new InMemoryStagingStorage(),
                FLINK_JOB_ID,
                null,
                new SimpleCounter(),
                limits);
    }

    /** {@code count} one-row Avro files for {@code table}. */
    private static List<FileLoadsCommittable> stage(String table, int count) throws IOException {
        org.apache.avro.Schema schema =
                SchemaBuilder.record("Row")
                        .fields()
                        .requiredLong("n")
                        .requiredString("region")
                        .endRecord();
        List<FileLoadsCommittable> committables = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            GenericRecord row = new GenericData.Record(schema);
            row.put("n", (long) i);
            row.put("region", "region-" + i);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DataFileWriter<GenericRecord> writer =
                    new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.create(schema, out);
                writer.append(row);
            }
            byte[] file = out.toByteArray();
            String path = PREFIX + table + "/" + i + ".avro";
            RealGcs.upload(path, file);
            committables.add(
                    new FileLoadsCommittable(
                            FLINK_JOB_ID,
                            RealTables.destination(table),
                            RealGcs.uri(path),
                            file.length,
                            1,
                            StagingFormat.AVRO));
        }
        return committables;
    }

    /**
     * The real runner, reading the key of each table the commit's cleanup deletes — its temporary
     * tables — just before it goes.
     */
    private static final class RecordingRunner implements LoadJobRunner {

        private final BigQueryLoadJobRunner delegate = new BigQueryLoadJobRunner(null, POLL);
        private final BigQueryTableAdmin admin = new BigQueryTableAdmin();
        final Map<String, String> deletedTableKeys = new LinkedHashMap<>();

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            delegate.submitLoad(jobId, spec);
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) throws IOException {
            delegate.submitCopy(jobId, spec);
        }

        @Override
        public boolean copySucceeded(String jobId, CopyJobSpec spec) throws IOException {
            return delegate.copySucceeded(jobId, spec);
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) throws IOException {
            delegate.submitQuery(jobId, spec);
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            delegate.awaitJob(jobId);
        }

        @Override
        public void deleteTable(TableDestination table) {
            try {
                deletedTableKeys.put(table.getTable(), admin.getSchema(table).getKmsKeyName());
            } catch (IOException | RuntimeException e) {
                throw new AssertionError("Could not read the key of " + table, e);
            }
            delegate.deleteTable(table);
        }
    }

    /** A serializer used only for the schema reconciliation reads. */
    private static final class SchemaOnlySerializer
            extends BigQueryProtoSerializationSchema<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return BigQuerySchemaConverter.toStorageSchema(SCHEMA);
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(Object element) {
            return ByteString.EMPTY;
        }
    }
}
