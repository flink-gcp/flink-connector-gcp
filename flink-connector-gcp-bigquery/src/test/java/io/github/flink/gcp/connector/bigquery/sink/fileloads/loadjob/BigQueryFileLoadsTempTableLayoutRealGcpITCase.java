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

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
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
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;
import org.apache.avro.LogicalTypes;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code FILE_LOADS} commit routed through temporary tables into partitioned and clustered
 * destinations, against BigQuery itself (#1671).
 *
 * <p>Only the service can answer what a copy job accepts: measured on 2026-10-09, it refuses to
 * copy the unpartitioned, unclustered temporary tables the commit used to create into a column-,
 * range- partitioned or clustered table. Every commit here is forced onto the temporary-table path
 * by one-file load jobs, and through one intermediate copy level by a fan-out of two, into one
 * destination of each layout BigQuery offers plus one the commit creates from {@link
 * TableCreateOptions}. Each destination must keep its layout, description and labels.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_DATASET", matches = ".+")
@EnabledIfEnvironmentVariable(named = "BQ_IT_GCS_BUCKET", matches = ".+")
@Timeout(value = 1_800, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryFileLoadsTempTableLayoutRealGcpITCase {

    private static final String RUN = TestNames.runId();
    private static final String PREFIX = "temp_layout_" + RUN + "/";

    /** Real polling backs off gently: a small job usually finishes within a few seconds. */
    private static final RetrySchedule POLL = new RetrySchedule(500, 5_000, Integer.MAX_VALUE, 0);

    /** Three one-file loads per destination over a fan-out of two: one intermediate copy level. */
    private static final Limits LIMITS = new Limits(2, 100, 100, 100).withMaxFilesPerJob(1);

    private static final Schema SCHEMA =
            Schema.of(
                    Field.of("n", StandardSQLTypeName.INT64),
                    Field.of("ts", StandardSQLTypeName.TIMESTAMP),
                    Field.of("region", StandardSQLTypeName.STRING));

    private static final String DESCRIPTION = "temp table layout it";
    private static final Map<String, String> LABELS = Map.of("owner", "temp-table-layout-it");

    /**
     * The destinations each commit writes, by name suffix, with the DDL clause that lays it out.
     */
    private static final Map<String, String> LAYOUTS = new LinkedHashMap<>();

    static {
        LAYOUTS.put("column", "PARTITION BY DATE(ts)");
        LAYOUTS.put("ingestion", "PARTITION BY _PARTITIONDATE");
        LAYOUTS.put("range", "PARTITION BY RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))");
        LAYOUTS.put("clustered", "CLUSTER BY region");
        LAYOUTS.put("both", "PARTITION BY TIMESTAMP_TRUNC(ts, HOUR) CLUSTER BY region, n");
        LAYOUTS.put("expiring", "PARTITION BY DATE(ts)");
    }

    /**
     * Options of the {@code expiring} destination, which a temporary table does not share and the
     * destination must keep: a partition expiration, and a partition filter requirement.
     */
    private static final String EXPIRING_OPTIONS =
            "partition_expiration_days = 36500, require_partition_filter = TRUE, ";

    private static final long EXPIRING_PARTITION_MS = 36_500L * 24 * 60 * 60 * 1000;

    /** The destination the commit itself creates, from {@link #CREATE_OPTIONS}. */
    private static final String CREATED = "created";

    private static final TableCreateOptions CREATE_OPTIONS =
            TableCreateOptions.builder()
                    .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "ts")
                    .clusteredFields(List.of("region"))
                    .description(DESCRIPTION)
                    .labels(LABELS)
                    .build();

    private static final TableLayout CREATED_LAYOUT =
            TableLayout.of(
                    TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("ts").build(),
                    null,
                    Clustering.newBuilder().setFields(List.of("region")).build());

    private static final String FLINK_JOB_ID = UUID.randomUUID().toString().replace("-", "");

    private static final List<String> TABLES = new ArrayList<>();

    @AfterAll
    static void cleanUp() throws Exception {
        // The commit deletes its temporary tables once it succeeds. After a failure, a laid-out one
        // expires with its own table expiration, and an unpartitioned one with the dataset's
        // 24-hour default table expiration.
        Closers.closeAll(
                () -> RealBigQuery.deleteTables(TABLES.toArray(new String[0])),
                () -> RealGcs.deletePrefix(PREFIX));
    }

    @ParameterizedTest
    @EnumSource(WriteDisposition.class)
    void anOversizedCommitKeepsEachDestinationsLayout(WriteDisposition disposition)
            throws Exception {
        String suffix = disposition.name().toLowerCase(Locale.ROOT);
        Map<String, TableLayout> before = new LinkedHashMap<>();
        List<FileLoadsCommittable> committables = new ArrayList<>();
        for (Map.Entry<String, String> layout : LAYOUTS.entrySet()) {
            String table = table(layout.getKey() + "_" + suffix);
            createDestination(
                    table,
                    layout.getValue(),
                    layout.getKey().equals("expiring") ? EXPIRING_OPTIONS : "");
            if (disposition != WriteDisposition.WRITE_EMPTY) {
                // A row the commit appends to, or replaces.
                RealBigQuery.queryRows(
                        "INSERT INTO "
                                + RealBigQuery.tablePath(table)
                                + " (n, ts, region)"
                                + " VALUES (99, TIMESTAMP '2026-10-01 00:00:00', 'old')");
            }
            before.put(table, TableLayout.of(RealBigQuery.tableDefinition(table)));
            committables.addAll(stage(table));
        }
        String created = table(CREATED + "_" + suffix);
        committables.addAll(stage(created));

        orchestrator(disposition).run(committables);

        long existingRows = disposition == WriteDisposition.WRITE_APPEND ? 1 : 0;
        for (Map.Entry<String, TableLayout> table : before.entrySet()) {
            assertThat(table.getValue().isEmpty()).as(table.getKey()).isFalse();
            assertDestination(table.getKey(), table.getValue(), 3 + existingRows);
        }
        String expiring = "temp_layout_expiring_" + suffix + "_" + RUN;
        assertThat(RealBigQuery.tableDefinition(expiring).getTimePartitioning().getExpirationMs())
                .isEqualTo(EXPIRING_PARTITION_MS);
        // A destination without a partition expiration did not take the leaves' 10,000 years.
        assertThat(
                        RealBigQuery.tableDefinition("temp_layout_column_" + suffix + "_" + RUN)
                                .getTimePartitioning()
                                .getExpirationMs())
                .isNull();
        // The partition filter requirement survived too: a query without one is refused.
        assertThatThrownBy(
                        () ->
                                RealBigQuery.queryLongs(
                                        "SELECT COUNT(*) FROM " + RealBigQuery.tablePath(expiring)))
                .hasMessageContaining("without a filter");
        assertDestination(created, CREATED_LAYOUT, 3);
    }

    /**
     * A commit retried after an upgrade, from the checkpoint a job restarting in a loop was stuck
     * on: an earlier version ran the same planned leaf loads and intermediate copy without the
     * destination's layout, under the same ids, and was refused the final copy on every restart
     * until no retry id was left for it.
     */
    @Test
    void aRetryAfterAnUpgradeRunsOnItsOwnTablesAndIds() throws Exception {
        String table = table("upgraded");
        createDestination(table, LAYOUTS.get("both"), "");
        TableLayout layout = TableLayout.of(RealBigQuery.tableDefinition(table));
        List<FileLoadsCommittable> committables = stage(table);
        DestinationCommitPlan planned = plan(committables);
        assertThat(planned.copy.intermediateLevels).isNotEmpty();
        BigQueryLoadJobRunner earlier = new BigQueryLoadJobRunner(null, POLL);
        for (PlannedLoad load : planned.loads) {
            earlier.submitLoad(load.jobId, specOf(load));
            earlier.awaitJob(load.jobId);
        }
        for (List<PlannedCopy> level : planned.copy.intermediateLevels) {
            for (PlannedCopy copy : level) {
                earlier.submitCopy(copy.jobId, copy.spec);
                earlier.awaitJob(copy.jobId);
            }
        }
        // Refused on every restart until no retry id is left. A refusal can arrive with the
        // submission, in which case one submission walks through every retry id at once.
        PlannedCopy finalCopy = planned.copy.finalCopy;
        IOException exhausted = null;
        for (int attempt = 0; attempt <= 6 && exhausted == null; attempt++) {
            try {
                earlier.submitCopy(finalCopy.jobId, finalCopy.spec);
            } catch (IOException e) {
                exhausted = e;
                break;
            }
            // Refused for the layout, whichever of the two refusals the service reports.
            assertThatThrownBy(() -> earlier.awaitJob(finalCopy.jobId))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(finalCopy.jobId)
                    .hasMessageMatching("(?s).*(partition|clustering).*");
        }
        assertThat(exhausted)
                .hasMessageContaining(finalCopy.jobId)
                .hasMessageContaining("all its retry ids failed")
                .hasMessageMatching("(?s).*(partition|clustering).*");

        orchestrator(WriteDisposition.WRITE_APPEND).run(committables);

        assertDestination(table, layout, 3);
    }

    /**
     * A commit retried after this version already filled its temporary tables re-attaches to the
     * loads that filled them instead of loading again.
     */
    @Test
    void aRetryReattachesToTheTempTablesThisVersionFilled() throws Exception {
        String table = table("refilled");
        createDestination(table, LAYOUTS.get("both"), "");
        TableLayout layout = TableLayout.of(RealBigQuery.tableDefinition(table));
        List<FileLoadsCommittable> committables = stage(table);
        DestinationCommitPlan planned = plan(committables);
        fillLeavesAsThisVersion(planned, layout);
        // The table carries the expiration the preparation gave it, and the explicit partition
        // expiration rather than any dataset default.
        Table leaf =
                new BigQueryTableAdmin().getSchema(planned.loads.get(0).jobDestination).getTable();
        assertThat(leaf.getExpirationTime())
                .isBetween(
                        System.currentTimeMillis() + Duration.ofHours(23).toMillis(),
                        System.currentTimeMillis() + Duration.ofHours(25).toMillis());
        assertThat(
                        ((StandardTableDefinition) leaf.getDefinition())
                                .getTimePartitioning()
                                .getExpirationMs())
                .isEqualTo(TableLayout.TEMPORARY_PARTITION_EXPIRATION_MS);
        // Without the staged files a load run again fails, so only a re-attach can succeed.
        RealGcs.deletePrefix(PREFIX + table + "/");

        orchestrator(WriteDisposition.WRITE_APPEND).run(committables);

        assertDestination(table, layout, 3);
    }

    /**
     * A commit retried after its final copy succeeded, as when another destination of the same
     * commit failed: it must neither load again, which would need the staged files, nor copy again,
     * which would append the rows twice.
     */
    @Test
    void aRetryAfterTheFinalCopySucceededReattachesToItAlone() throws Exception {
        String table = table("committed");
        createDestination(table, LAYOUTS.get("both"), "");
        TableLayout layout = TableLayout.of(RealBigQuery.tableDefinition(table));
        List<FileLoadsCommittable> committables = stage(table);
        orchestrator(WriteDisposition.WRITE_APPEND).run(committables);
        assertDestination(table, layout, 3);
        RealGcs.deletePrefix(PREFIX + table + "/");

        orchestrator(WriteDisposition.WRITE_APPEND).run(committables);

        assertDestination(table, layout, 3);
    }

    /**
     * A commit retried after one of the temporary tables an earlier attempt filled expired. The
     * retry creates it anew, and must fill it again rather than re-attach to the load that filled
     * its predecessor, or the copy would read an empty table and drop that load's rows.
     */
    @Test
    void aRetryRefillsATempTableThatExpired() throws Exception {
        String table = table("expired");
        createDestination(table, LAYOUTS.get("both"), "");
        TableLayout layout = TableLayout.of(RealBigQuery.tableDefinition(table));
        List<FileLoadsCommittable> committables = stage(table);
        DestinationCommitPlan planned = plan(committables);
        fillLeavesAsThisVersion(planned, layout);
        // As the table expiration would.
        RealBigQuery.deleteTables(planned.loads.get(0).jobDestination.getTable());

        orchestrator(WriteDisposition.WRITE_APPEND).run(committables);

        assertDestination(table, layout, 3);
    }

    /**
     * Runs what this version runs before its copies: lays the plan out, prepares its temporary
     * tables, and fills the leaves under the ids their incarnations give them.
     */
    private static void fillLeavesAsThisVersion(DestinationCommitPlan planned, TableLayout layout)
            throws IOException {
        BigQueryTableAdmin admin = new BigQueryTableAdmin();
        planned.layOutTempTables(
                layout,
                new DestinationCommitPlan.TempTables() {
                    @Override
                    public boolean copySucceeded(PlannedCopy copy) {
                        return false;
                    }

                    @Override
                    public long prepare(TableDestination temp) throws IOException {
                        TABLES.add(temp.getTable());
                        return admin.prepareTemporaryTable(
                                temp, SCHEMA, layout, Duration.ofDays(1));
                    }
                });
        BigQueryLoadJobRunner earlier = new BigQueryLoadJobRunner(null, POLL);
        for (PlannedLoad load : planned.loads) {
            earlier.submitLoad(load.jobId, specOf(load));
            earlier.awaitJob(load.jobId);
        }
    }

    /** The plan of a commit over one destination's committables, before reconciliation. */
    private static DestinationCommitPlan plan(List<FileLoadsCommittable> committables)
            throws IOException {
        DestinationCommitPlan planned =
                new CommitPlanner(
                                config(),
                                options(WriteDisposition.WRITE_APPEND),
                                FLINK_JOB_ID,
                                null,
                                LIMITS)
                        .plan(committables)
                        .destinations
                        .get(0);
        for (TableDestination temp : planned.copy.cleanupTables) {
            // Tables an earlier attempt fills and no later commit deletes.
            TABLES.add(temp.getTable());
        }
        return planned;
    }

    private static LoadJobSpec specOf(PlannedLoad load) {
        return new LoadJobSpec(
                load.jobDestination,
                load.uris,
                SCHEMA,
                load.createDisposition,
                load.writeDisposition,
                load.schemaUpdateOptions,
                load.format,
                load.tempTableLayout);
    }

    private static String table(String name) {
        String table = "temp_layout_" + name + "_" + RUN;
        TABLES.add(table);
        return table;
    }

    private static void createDestination(String table, String layoutClause, String options)
            throws InterruptedException {
        RealBigQuery.queryRows(
                "CREATE TABLE "
                        + RealBigQuery.tablePath(table)
                        + " (n INT64, ts TIMESTAMP, region STRING) "
                        + layoutClause
                        + " OPTIONS ("
                        + options
                        + "description = '"
                        + DESCRIPTION
                        + "', labels = [('owner', 'temp-table-layout-it')])");
    }

    private static void assertDestination(String table, TableLayout layout, long rows)
            throws InterruptedException {
        // The filter satisfies a partition filter requirement; every row has a later timestamp.
        assertThat(
                        RealBigQuery.queryLongs(
                                "SELECT COUNT(*) FROM "
                                        + RealBigQuery.tablePath(table)
                                        + " WHERE ts >= TIMESTAMP '2000-01-01'"))
                .as(table)
                .containsExactly(rows);
        assertThat(TableLayout.of(RealBigQuery.tableDefinition(table))).as(table).isEqualTo(layout);
        assertThat(RealBigQuery.tableDescription(table)).as(table).isEqualTo(DESCRIPTION);
        assertThat(RealBigQuery.tableLabels(table)).as(table).containsAllEntriesOf(LABELS);
    }

    private static LoadJobOrchestrator orchestrator(WriteDisposition disposition) {
        return new LoadJobOrchestrator(
                config(),
                options(disposition),
                new BigQueryLoadJobRunner(null, POLL),
                new BigQueryTableAdmin(),
                new InMemoryStagingStorage(),
                FLINK_JOB_ID,
                null,
                new SimpleCounter(),
                LIMITS);
    }

    private static FileLoadsOptions options(WriteDisposition disposition) {
        return FileLoadsOptions.builder()
                .stagingPath(RealGcs.uri(PREFIX + "staging"))
                .writeDisposition(disposition)
                .build();
    }

    private static BigQuerySinkConfig<Object> config() {
        return ((BigQueryFileLoadsSink<Object>)
                        BigQuerySink.builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                // Unused: every committable names its own destination.
                                .table(RealTables.destination("temp_layout_unused_" + RUN))
                                .serializer(new SchemaOnlySerializer())
                                .tableCreateOptions(CREATE_OPTIONS)
                                .fileLoadsOptions(options(WriteDisposition.WRITE_APPEND))
                                .build())
                .getConfig();
    }

    /** Three one-row Avro files for {@code table}, one per load job under {@link #LIMITS}. */
    private static List<FileLoadsCommittable> stage(String table) throws IOException {
        List<FileLoadsCommittable> committables = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            String path = PREFIX + table + "/" + i + ".avro";
            byte[] file = oneRowAvroFile(i, "region-" + i);
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

    private static byte[] oneRowAvroFile(int n, String region) throws IOException {
        org.apache.avro.Schema timestamp =
                LogicalTypes.timestampMicros()
                        .addToSchema(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        org.apache.avro.Schema schema =
                SchemaBuilder.record("Row")
                        .fields()
                        .requiredLong("n")
                        .name("ts")
                        .type(timestamp)
                        .noDefault()
                        .requiredString("region")
                        .endRecord();
        GenericRecord row = new GenericData.Record(schema);
        row.put("n", (long) n);
        // One row per day, so the partitioned destinations receive several partitions.
        row.put("ts", (1_791_072_000L + n * 86_400L) * 1_000_000L);
        row.put("region", region);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer =
                new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, out);
            writer.append(row);
        }
        return out.toByteArray();
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
