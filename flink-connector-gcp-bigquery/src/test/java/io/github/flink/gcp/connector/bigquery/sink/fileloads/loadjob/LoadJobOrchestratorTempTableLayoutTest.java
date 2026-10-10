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
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
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
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LoadJobOrchestrator} giving temporary tables the layout of a partitioned or
 * clustered destination (ADR-0183), against recording fakes.
 */
class LoadJobOrchestratorTempTableLayoutTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f2")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    /** A serializer only used for its schema. */
    private static final class SchemaOnlySerializer
            extends BigQueryProtoSerializationSchema<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
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

    private static FileLoadsCommittable file(
            TableDestination destination, String name, long bytes) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID,
                destination,
                "gs://bucket/prefix/" + name + StagingFormat.AVRO.getExtension(),
                bytes,
                10,
                StagingFormat.AVRO);
    }

    /** Everything one orchestration run touches. */
    private static final class Harness {
        private final FakeLoadJobRunner runner = new FakeLoadJobRunner();
        private final FakeTableAdmin tableAdmin = new FakeTableAdmin();
        private final LoadJobOrchestrator orchestrator;

        Harness(FileLoadsOptions options, Consumer<BigQuerySinkBuilder<Object>> customizer) {
            this(options, customizer, null, Limits.BIGQUERY);
        }

        Harness(
                FileLoadsOptions options,
                Consumer<BigQuerySinkBuilder<Object>> customizer,
                Long checkpointId,
                Limits limits) {
            BigQuerySinkBuilder<Object> builder =
                    BigQuerySink.builder()
                            .writeMethod(WriteMethod.FILE_LOADS)
                            .table(T1)
                            .serializer(new SchemaOnlySerializer())
                            .fileLoadsOptions(options);
            customizer.accept(builder);
            this.orchestrator =
                    new LoadJobOrchestrator(
                            ((BigQueryFileLoadsSink<Object>) builder.build()).getConfig(),
                            options,
                            runner,
                            tableAdmin,
                            new InMemoryStagingStorage(),
                            FLINK_JOB_ID,
                            checkpointId,
                            new SimpleCounter(),
                            limits);
        }

        static Harness plain() {
            return new Harness(
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                    builder -> {});
        }
    }

    private static final TableLayout PARTITIONED_AND_CLUSTERED =
            TableLayout.of(
                    TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("f1").build(),
                    null,
                    Clustering.newBuilder().setFields(List.of("f2")).build());

    private static final TableLayout CLUSTERED_ONLY =
            TableLayout.of(null, null, Clustering.newBuilder().setFields(List.of("f1")).build());

    private static final TableLayout INGESTION_TIME =
            TableLayout.of(TimePartitioning.of(TimePartitioning.Type.DAY), null, null);

    private static final TableLayout RANGE_PARTITIONED =
            TableLayout.of(
                    null,
                    RangePartitioning.newBuilder()
                            .setField("f2")
                            .setRange(
                                    RangePartitioning.Range.newBuilder()
                                            .setStart(0L)
                                            .setEnd(100L)
                                            .setInterval(10L)
                                            .build())
                            .build(),
                    null);

    /** A live table with a column the serializer lacks, so the union differs from both. */
    private static final TableSchema LIVE_F1_F3 =
            TableSchema.newBuilder()
                    .addFields(SCHEMA.getFields(0))
                    .addFields(SCHEMA.getFields(0).toBuilder().setName("f3"))
                    .build();

    private static final FileLoadsOptions OPTIONS =
            FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();

    private static List<FileLoadsCommittable> threeOversizedFiles() {
        long sixTiB = 6L << 40;
        return List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", sixTiB));
    }

    private static Harness withExistingTable(TableLayout layout) {
        return withExistingTable(OPTIONS, builder -> {}, layout);
    }

    private static Harness withExistingTable(
            FileLoadsOptions options,
            Consumer<BigQuerySinkBuilder<Object>> customizer,
            TableLayout layout) {
        Harness harness = new Harness(options, customizer);
        harness.tableAdmin.tables.put(T1, SCHEMA);
        harness.tableAdmin.layouts.put(T1, layout);
        return harness;
    }

    private static List<TableDestination> loadTables(Harness harness) {
        return harness.runner.loads.values().stream().map(LoadJobSpec::getDestination).toList();
    }

    private static CopyJobSpec onlyCopy(Harness harness) {
        assertThat(harness.runner.copies).hasSize(1);
        return harness.runner.copies.values().iterator().next();
    }

    /**
     * Asserts that {@code laidOut} ran the plan {@code unpartitioned} ran, with every temporary
     * table renamed and every job filling one salted with {@code layout}, and the copy into the
     * destination under the fixed marker.
     */
    private static void assertLaidOutAs(
            Harness laidOut, Harness unpartitioned, TableLayout layout) {
        String salt = CommitPlanner.layoutSalt(layout);
        assertThat(laidOut.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getLayout()).isEqualTo(layout));
        assertThat(unpartitioned.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getLayout()).isNull());
        assertThat(loadTables(laidOut))
                .containsExactlyElementsOf(
                        loadTables(unpartitioned).stream()
                                .map(table -> CommitPlanner.laidOutTable(table, salt))
                                .toList());
        // Each job filling a temporary table also names that table's incarnation.
        List<String> unpartitionedIds = new ArrayList<>(unpartitioned.runner.loads.keySet());
        List<TableDestination> tables = loadTables(laidOut);
        List<String> expectedIds = new ArrayList<>();
        for (int i = 0; i < unpartitionedIds.size(); i++) {
            expectedIds.add(
                    CommitPlanner.incarnationJobId(
                            CommitPlanner.laidOutJobId(unpartitionedIds.get(i), salt),
                            laidOut.tableAdmin.temporaryTables.get(tables.get(i))));
        }
        assertThat(laidOut.runner.loads.keySet()).containsExactlyElementsOf(expectedIds);
        // Every temporary table was prepared, with the layout and the default expiration.
        assertThat(laidOut.tableAdmin.preparedTemporaryTables)
                .containsExactlyInAnyOrderElementsOf(onlyCopy(laidOut).getSourceTables());
        assertThat(laidOut.tableAdmin.preparedLayouts).containsOnly(layout);
        assertThat(laidOut.tableAdmin.preparedExpirations)
                .containsOnly(FileLoadsOptions.DEFAULT_TEMP_TABLE_EXPIRATION);
        // Prepared before any job runs, so a laid-out load or copy never creates a table.
        assertThat(laidOut.runner.loads.values())
                .allSatisfy(
                        spec ->
                                assertThat(spec.getCreateDisposition())
                                        .isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER));
        assertThat(unpartitioned.tableAdmin.preparedTemporaryTables).isEmpty();
        CopyJobSpec copy = onlyCopy(laidOut);
        assertThat(laidOut.runner.copies.keySet())
                .containsExactly(
                        CommitPlanner.laidOutFinalCopyId(
                                unpartitioned.runner.copies.keySet().iterator().next()));
        assertThat(copy.getDestination()).isEqualTo(T1);
        assertThat(copy.getSourceTables()).containsExactlyElementsOf(loadTables(laidOut));
        // Each final copy names the other plan's as equivalent, so adding or removing the
        // destination's only clustering between attempts cannot repeat a copy that succeeded.
        String unpartitionedCopyId = unpartitioned.runner.copies.keySet().iterator().next();
        assertThat(copy.getEquivalentJobId()).isEqualTo(unpartitionedCopyId);
        assertThat(onlyCopy(unpartitioned).getEquivalentJobId())
                .isEqualTo(CommitPlanner.laidOutFinalCopyId(unpartitionedCopyId));
        // The cleanup deletes the renamed tables, and nothing else is ever deleted.
        assertThat(laidOut.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(loadTables(laidOut));
    }

    @Test
    void tempTablesTakeTheLayoutTheDestinationWasCreatedWith() throws IOException {
        Harness harness =
                new Harness(
                        OPTIONS,
                        builder ->
                                builder.tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .timePartitioning(
                                                        TableCreateOptions.TimePartitioningType.DAY,
                                                        "f1")
                                                .clusteredFields(List.of("f2"))
                                                .build()));
        Harness unpartitioned = Harness.plain();

        harness.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.created).containsExactly(T1);
        assertLaidOutAs(harness, unpartitioned, PARTITIONED_AND_CLUSTERED);
    }

    @Test
    void laidOutNamesAndIdsArePinned() throws IOException {
        Harness harness = withExistingTable(PARTITIONED_AND_CLUSTERED);

        harness.orchestrator.run(threeOversizedFiles());

        // Pinned literally: a retried commit restored across an upgrade finds its tables and
        // re-attaches to its jobs by these names and ids, so the salt must not drift.
        assertThat(CommitPlanner.layoutSalt(PARTITIONED_AND_CLUSTERED)).isEqualTo("b40ff274");
        assertThat(harness.runner.loads.keySet())
                .first()
                .isEqualTo(
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-4ecae23c5143f639-p0-layout-b40ff274-t1001");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_57b4d06d46c9_p0_layout_b40ff274");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-6a44639ead366984-laidout");
    }

    @Test
    void unpartitionedDestinationsKeepTheirJobIds() throws IOException {
        Harness harness = Harness.plain();

        harness.orchestrator.run(threeOversizedFiles());

        // Pinned literally: a retried commit restored across an upgrade re-attaches by these ids.
        assertThat(harness.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-4ecae23c5143f639-p0",
                        "flink-bq-load-" + FLINK_JOB_ID + "-17a6853089163595-p1",
                        "flink-bq-load-" + FLINK_JOB_ID + "-75e4dec0e13b0f39-p2");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-6a44639ead366984");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_57b4d06d46c9_p0");
    }

    @Test
    void ingestionTimePartitioningAloneKeepsThePlan() throws IOException {
        Harness ingestion = withExistingTable(INGESTION_TIME);
        Harness unpartitioned = withExistingTable(TableLayout.NONE);

        ingestion.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        // A copy into an ingestion-time-partitioned table accepts unpartitioned sources, and an
        // earlier version's final copy may have succeeded into it, so nothing may change.
        assertThat(ingestion.runner.loads.keySet())
                .containsExactlyElementsOf(unpartitioned.runner.loads.keySet());
        assertThat(loadTables(ingestion)).containsExactlyElementsOf(loadTables(unpartitioned));
        assertThat(ingestion.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getLayout()).isNull());
        assertThat(ingestion.runner.copies.keySet())
                .containsExactlyElementsOf(unpartitioned.runner.copies.keySet());
        assertThat(onlyCopy(ingestion).getEquivalentJobId())
                .isEqualTo(CommitPlanner.laidOutFinalCopyId(onlyCopyId(ingestion)));
        assertThat(ingestion.tableAdmin.preparedTemporaryTables).isEmpty();
    }

    @Test
    void columnPartitioningAloneIsCarried() throws IOException {
        TableLayout layout =
                TableLayout.of(
                        TimePartitioning.newBuilder(TimePartitioning.Type.MONTH)
                                .setField("f1")
                                .build(),
                        null,
                        null);
        Harness harness = withExistingTable(layout);
        Harness unpartitioned = withExistingTable(TableLayout.NONE);

        harness.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        assertLaidOutAs(harness, unpartitioned, layout);
    }

    @Test
    void ingestionTimePartitioningIsCarriedWithClustering() throws IOException {
        TableLayout layout =
                TableLayout.of(
                        TimePartitioning.of(TimePartitioning.Type.DAY),
                        null,
                        Clustering.newBuilder().setFields(List.of("f1")).build());
        Harness harness = withExistingTable(layout);
        Harness unpartitioned = withExistingTable(TableLayout.NONE);

        harness.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        assertLaidOutAs(harness, unpartitioned, layout);
    }

    @Test
    void tempTablesTakeAnExistingDestinationsRangePartitioning() throws IOException {
        Harness harness = withExistingTable(RANGE_PARTITIONED);
        Harness unpartitioned = withExistingTable(TableLayout.NONE);

        harness.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.created).isEmpty();
        assertLaidOutAs(harness, unpartitioned, RANGE_PARTITIONED);
    }

    @Test
    void theLayoutIsReadUnderEveryReconciliationOutcome() throws IOException {
        Harness truncating =
                withExistingTable(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .build(),
                        builder -> {},
                        CLUSTERED_ONLY);
        Harness unioning =
                new Harness(
                        OPTIONS,
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        unioning.tableAdmin.tables.put(T1, LIVE_F1_F3);
        unioning.tableAdmin.layouts.put(T1, CLUSTERED_ONLY);

        truncating.orchestrator.run(threeOversizedFiles());
        unioning.orchestrator.run(threeOversizedFiles());

        assertThat(truncating.runner.loads.values())
                .hasSize(3)
                .allSatisfy(spec -> assertThat(spec.getLayout()).isEqualTo(CLUSTERED_ONLY));
        assertThat(unioning.tableAdmin.schemaUpdates).containsExactly(T1);
        // The temporary tables take the reconciled schema: the live columns, then the
        // serializer's new one. Neither the serializer's schema (f1, f2) nor the live one (f1, f3)
        // alone would match.
        assertThat(unioning.tableAdmin.preparedSchemas)
                .hasSize(3)
                .allSatisfy(
                        schema ->
                                assertThat(schema.getFields())
                                        .extracting(com.google.cloud.bigquery.Field::getName)
                                        .containsExactly("f1", "f3", "f2"));
        // And the loads carry that same schema, or the copy would refuse mismatched sources.
        assertThat(unioning.runner.loads.values())
                .allSatisfy(
                        spec ->
                                assertThat(spec.getSchema().getFields())
                                        .extracting(com.google.cloud.bigquery.Field::getName)
                                        .containsExactly("f1", "f3", "f2"));
        assertThat(unioning.runner.loads.values())
                .hasSize(3)
                .allSatisfy(spec -> assertThat(spec.getLayout()).isEqualTo(CLUSTERED_ONLY));
    }

    @Test
    void aDirectLoadCarriesNoLayout() throws IOException {
        Harness harness = withExistingTable(PARTITIONED_AND_CLUSTERED);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads).hasSize(1);
        Map.Entry<String, LoadJobSpec> load = harness.runner.loads.entrySet().iterator().next();
        assertThat(load.getValue().getDestination()).isEqualTo(T1);
        assertThat(load.getValue().getLayout()).isNull();
        assertThat(load.getKey()).doesNotContain("-layout-");
    }

    @Test
    void writeTruncateDataTempTablesKeepNoLayout() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness harness = withExistingTable(options, builder -> {}, PARTITIONED_AND_CLUSTERED);
        Harness unpartitioned = withExistingTable(options, builder -> {}, TableLayout.NONE);

        harness.orchestrator.run(threeOversizedFiles());
        unpartitioned.orchestrator.run(threeOversizedFiles());

        assertThat(harness.runner.loads.keySet())
                .containsExactlyElementsOf(unpartitioned.runner.loads.keySet());
        assertThat(loadTables(harness)).containsExactlyElementsOf(loadTables(unpartitioned));
        assertThat(harness.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getLayout()).isNull());
        assertThat(harness.runner.copies.keySet())
                .containsExactlyElementsOf(unpartitioned.runner.copies.keySet());
        assertThat(harness.runner.queries.keySet())
                .containsExactlyElementsOf(unpartitioned.runner.queries.keySet());
        // The copy fills the aggregate temporary table, not the destination.
        assertThat(onlyCopy(harness).getEquivalentJobId()).isNull();
        assertThat(harness.tableAdmin.preparedTemporaryTables).isEmpty();
    }

    @Test
    void intermediateCopiesFillRenamedTablesUnderSaltedIds() throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        Harness laidOut = new Harness(OPTIONS, builder -> {}, 7L, limits);
        laidOut.tableAdmin.tables.put(T1, SCHEMA);
        laidOut.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        Harness unpartitioned = new Harness(OPTIONS, builder -> {}, 7L, limits);
        unpartitioned.tableAdmin.tables.put(T1, SCHEMA);
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10));

        laidOut.orchestrator.run(files);
        unpartitioned.orchestrator.run(files);

        // Three leaves over a fan-out of two: one intermediate copy, then the final copy.
        String salt = CommitPlanner.layoutSalt(PARTITIONED_AND_CLUSTERED);
        List<String> copyIds = new ArrayList<>(unpartitioned.runner.copies.keySet());
        List<CopyJobSpec> copies = new ArrayList<>(unpartitioned.runner.copies.values());
        assertThat(copyIds).hasSize(2);
        List<CopyJobSpec> laidOutCopies = new ArrayList<>(laidOut.runner.copies.values());
        assertThat(laidOutCopies)
                .extracting(CopyJobSpec::getCreateDisposition)
                .containsOnly(JobInfo.CreateDisposition.CREATE_NEVER);
        assertThat(laidOut.runner.copies.keySet())
                .containsExactly(
                        CommitPlanner.incarnationJobId(
                                CommitPlanner.laidOutJobId(copyIds.get(0), salt),
                                laidOut.tableAdmin.temporaryTables.get(
                                        laidOutCopies.get(0).getDestination())),
                        CommitPlanner.laidOutFinalCopyId(copyIds.get(1)));
        // Leaves and the intermediate table are all prepared before any job runs.
        assertThat(laidOut.tableAdmin.preparedTemporaryTables)
                .hasSize(4)
                .contains(laidOutCopies.get(0).getDestination());
        assertThat(laidOutCopies.get(0).getEquivalentJobId()).isNull();
        assertThat(laidOut.runner.deletedTables)
                .containsAll(
                        loadTables(unpartitioned).stream()
                                .map(table -> CommitPlanner.laidOutTable(table, salt))
                                .toList());
        assertThat(laidOutCopies.get(0).getDestination())
                .isEqualTo(CommitPlanner.laidOutTable(copies.get(0).getDestination(), salt));
        assertThat(laidOutCopies.get(0).getSourceTables())
                .containsExactlyElementsOf(
                        copies.get(0).getSourceTables().stream()
                                .map(table -> CommitPlanner.laidOutTable(table, salt))
                                .toList());
        assertThat(laidOutCopies.get(1).getDestination()).isEqualTo(T1);
        assertThat(laidOutCopies.get(1).getEquivalentJobId()).isEqualTo(copyIds.get(1));
        assertThat(copies.get(1).getEquivalentJobId())
                .isEqualTo(CommitPlanner.laidOutFinalCopyId(copyIds.get(1)));
        assertThat(laidOutCopies.get(1).getSourceTables())
                .containsExactlyElementsOf(
                        copies.get(1).getSourceTables().stream()
                                .map(table -> CommitPlanner.laidOutTable(table, salt))
                                .toList());
        assertThat(laidOut.runner.deletedTables)
                .contains(laidOutCopies.get(0).getDestination())
                .hasSize(unpartitioned.runner.deletedTables.size());
    }

    @Test
    void aTemporaryTableThatExpiredBeforeARetryIsFilledAgain() throws IOException {
        Harness first = withExistingTable(PARTITIONED_AND_CLUSTERED);
        first.orchestrator.run(threeOversizedFiles());
        List<TableDestination> tables = loadTables(first);

        // The retry runs against the same service: two tables survived, the first expired and
        // the next preparation creates it anew.
        Harness retry = withExistingTable(PARTITIONED_AND_CLUSTERED);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        retry.tableAdmin.temporaryTables.remove(tables.get(0));
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.orchestrator.run(threeOversizedFiles());

        List<String> before = new ArrayList<>(first.runner.loads.keySet());
        List<String> after = new ArrayList<>(retry.runner.loads.keySet());
        // A re-attach to the load that filled the expired table would copy an empty table.
        assertThat(after.get(0)).isNotEqualTo(before.get(0));
        assertThat(after.subList(1, 3)).isEqualTo(before.subList(1, 3));
        assertThat(retry.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
    }

    @Test
    void aDestinationWhoseFinalCopyRanIsNeitherPreparedNorLoadedAgain() throws IOException {
        Harness earlier = withExistingTable(PARTITIONED_AND_CLUSTERED);
        earlier.orchestrator.run(threeOversizedFiles());
        String finalCopyId = onlyCopyId(earlier);

        // Another destination failed after this one's final copy succeeded, and the restored
        // commit runs once the temporary tables expired: reloading them would need staged files a
        // lifecycle rule may have removed, for rows already copied.
        Harness retry = withExistingTable(PARTITIONED_AND_CLUSTERED);
        retry.runner.copiesThatSucceeded.add(finalCopyId);
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(finalCopyId);
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(earlier.runner.deletedTables);
    }

    @Test
    void aDestinationWithIntermediateLevelsWhoseFinalCopySucceededCopiesNothingElse()
            throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10));
        Harness earlier = new Harness(OPTIONS, builder -> {}, 7L, limits);
        earlier.tableAdmin.tables.put(T1, SCHEMA);
        earlier.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        earlier.orchestrator.run(files);
        assertThat(earlier.runner.copies).hasSize(2);
        String finalCopyId = new ArrayList<>(earlier.runner.copies.keySet()).get(1);

        Harness retry = new Harness(OPTIONS, builder -> {}, 7L, limits);
        retry.tableAdmin.tables.put(T1, SCHEMA);
        retry.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        retry.runner.copiesThatSucceeded.add(finalCopyId);
        retry.orchestrator.run(files);

        // No load and no intermediate copy: only the final copy, re-attached, and the same laid-out
        // tables, intermediates included, cleaned up.
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(finalCopyId);
        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(earlier.runner.deletedTables);
    }

    @Test
    void aFinalCopyThatRanWithoutALayoutAlsoStandsForTheLaidOutOne() throws IOException {
        Harness unpartitioned = withExistingTable(TableLayout.NONE);
        unpartitioned.orchestrator.run(threeOversizedFiles());

        Harness retry = withExistingTable(CLUSTERED_ONLY);
        retry.runner.copiesThatSucceeded.add(onlyCopyId(unpartitioned));
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
    }

    @Test
    void aLaidOutFinalCopyThatSucceededStandsForTheCopyAfterTheClusteringWasRemoved()
            throws IOException {
        Harness clustered = withExistingTable(CLUSTERED_ONLY);
        clustered.orchestrator.run(threeOversizedFiles());
        String laidOutCopyId = onlyCopyId(clustered);

        // The clustering was removed before the retry: the destination needs no layout now, and
        // its files may be gone, so it must not load them again before re-attaching.
        Harness retry = withExistingTable(TableLayout.NONE);
        retry.runner.copiesThatSucceeded.add(laidOutCopyId);
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads).isEmpty();
        // The unchanged plan's own copy, which names the laid-out one as equivalent.
        assertThat(retry.runner.copies.values())
                .singleElement()
                .satisfies(copy -> assertThat(copy.getEquivalentJobId()).isEqualTo(laidOutCopyId));
        assertThat(laidOutCopyId).endsWith("-laidout");
        assertThat(retry.runner.copies.keySet()).doesNotContain(laidOutCopyId);
    }

    @Test
    void anUnchangedPlanWhoseFinalCopySucceededLoadsNothingAgain() throws IOException {
        Harness earlier = withExistingTable(TableLayout.NONE);
        earlier.orchestrator.run(threeOversizedFiles());

        Harness retry = withExistingTable(TableLayout.NONE);
        retry.runner.copiesThatSucceeded.add(onlyCopyId(earlier));
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(onlyCopyId(earlier));
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(earlier.runner.deletedTables);
    }

    @Test
    void writeTruncateDataNeverSkipsItsLoadsOrQuery() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness earlier = withExistingTable(options, builder -> {}, TableLayout.NONE);
        earlier.orchestrator.run(threeOversizedFiles());

        // Its last copy fills the aggregate table, not the destination: a copy that succeeded
        // there stands for nothing the terminal query has written.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.runner.copiesThatSucceeded.addAll(earlier.runner.copies.keySet());
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads).hasSize(3);
        assertThat(retry.runner.queries).hasSize(1);
        assertThat(retry.runner.events).noneMatch(event -> event.startsWith("copy-succeeded?:"));
    }

    @Test
    void theConfiguredExpirationReachesEveryPreparation() throws IOException {
        Harness harness =
                withExistingTable(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .tempTableExpiration(Duration.ofHours(6))
                                .build(),
                        builder -> {},
                        CLUSTERED_ONLY);

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.preparedExpirations)
                .hasSize(3)
                .containsOnly(Duration.ofHours(6));
    }

    @Test
    void anotherLayoutFillsOtherTablesButCopiesUnderTheSameId() throws IOException {
        Harness before = withExistingTable(PARTITIONED_AND_CLUSTERED);
        Harness after = withExistingTable(CLUSTERED_ONLY);

        before.orchestrator.run(threeOversizedFiles());
        after.orchestrator.run(threeOversizedFiles());

        // A commit retried after the destination's clustering changed neither re-attaches to the
        // jobs that filled tables with the old layout nor loads into those tables. A final copy
        // that already succeeded is re-attached to rather than repeated.
        assertThat(CommitPlanner.layoutSalt(CLUSTERED_ONLY))
                .isNotEqualTo(CommitPlanner.layoutSalt(PARTITIONED_AND_CLUSTERED));
        assertThat(after.runner.loads.keySet())
                .doesNotContainAnyElementsOf(before.runner.loads.keySet());
        assertThat(loadTables(after)).doesNotContainAnyElementsOf(loadTables(before));
        assertThat(after.runner.copies.keySet())
                .containsExactlyElementsOf(before.runner.copies.keySet());
    }

    @Test
    void noTemporaryTableIsReadAndNoneIsDeletedBeforeTheFinalCopy() throws IOException {
        Harness harness = withExistingTable(PARTITIONED_AND_CLUSTERED);

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.schemaReads).isEqualTo(1);
        assertThat(harness.runner.events.indexOf("submit-copy:" + onlyCopyId(harness)))
                .isLessThan(firstDeletion(harness));
    }

    private static String onlyCopyId(Harness harness) {
        return harness.runner.copies.keySet().iterator().next();
    }

    private static int firstDeletion(Harness harness) {
        for (int i = 0; i < harness.runner.events.size(); i++) {
            if (harness.runner.events.get(i).startsWith("delete:")) {
                return i;
            }
        }
        throw new AssertionError("No table was deleted: " + harness.runner.events);
    }
}
