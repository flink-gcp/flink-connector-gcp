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
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittableSerializer;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.OlderVersionCommittables;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link LoadJobOrchestrator} giving temporary tables the layout of a partitioned or
 * clustered destination (ADR-0183), for the names and ids they and their jobs take for files this
 * version and an older one staged their jobs (#2289), and for what the jobs that fill a destination
 * state, against recording fakes.
 */
class LoadJobOrchestratorTempTableLayoutTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    /**
     * The salt of the schema reconciliation gives {@link #SCHEMA}, which every job filling a
     * temporary table takes; pinned so that a change to how it is derived is seen here.
     */
    private static final String SCHEMA_SALT = "7062e446";

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
                StagingFormat.AVRO,
                destination.getDataset());
    }

    /** {@code file} as a run whose temporary dataset resolved to {@code tempDataset} staged it. */
    private static FileLoadsCommittable inTempDataset(
            FileLoadsCommittable file, String tempDataset) {
        return new FileLoadsCommittable(
                file.getFlinkJobId(),
                file.getDestination(),
                file.getUri(),
                file.getByteCount(),
                file.getRowCount(),
                file.getFormat(),
                file.getCheckpointId(),
                file.getJobProject(),
                tempDataset);
    }

    /** The committables as they are restored from state a version before #2289 wrote. */
    private static List<FileLoadsCommittable> stagedByOlderVersion(
            List<FileLoadsCommittable> files) {
        return OlderVersionCommittables.of(files);
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
        return oversizedFiles("a", "b", "c");
    }

    /** Files too large to share a load job, so each fills a temporary table of its own. */
    private static List<FileLoadsCommittable> oversizedFiles(String... names) {
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = new ArrayList<>(names.length);
        for (String name : names) {
            files.add(file(T1, name, sixTiB));
        }
        return files;
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
     * Returns the ids of the loads {@code harness} ran, each without the schema salt and the
     * incarnation of the table it filled, after asserting that every id names both.
     */
    private static List<String> withoutIncarnations(Harness harness, List<String> loadIds) {
        List<TableDestination> tables = loadTables(harness);
        List<String> baseIds = new ArrayList<>(loadIds.size());
        for (int i = 0; i < loadIds.size(); i++) {
            baseIds.add(withoutPreparation(harness, loadIds.get(i), tables.get(i)));
        }
        return baseIds;
    }

    /**
     * Returns {@code jobId}, of a job that filled {@code table}, without the schema salt and the
     * incarnation that preparation appended, after asserting both.
     */
    private static String withoutPreparation(
            Harness harness, String jobId, TableDestination table) {
        String suffix = "-s" + SCHEMA_SALT + "-t" + harness.tableAdmin.temporaryTables.get(table);
        assertThat(jobId).endsWith(suffix);
        return jobId.substring(0, jobId.length() - suffix.length());
    }

    /** The id preparation gives a job that fills the laid-out {@code table}. */
    private static String preparedLaidOutJobId(
            Harness harness, String baseJobId, String salt, TableDestination table) {
        return CommitPlanner.incarnationJobId(
                CommitPlanner.schemaJobId(CommitPlanner.laidOutJobId(baseJobId, salt), SCHEMA_SALT),
                harness.tableAdmin.temporaryTables.get(table));
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
        // Each job filling a temporary table also names that table's incarnation, laid out or not.
        List<String> unpartitionedBaseIds =
                withoutIncarnations(
                        unpartitioned, new ArrayList<>(unpartitioned.runner.loads.keySet()));
        List<TableDestination> tables = loadTables(laidOut);
        List<String> expectedIds = new ArrayList<>();
        for (int i = 0; i < unpartitionedBaseIds.size(); i++) {
            expectedIds.add(
                    preparedLaidOutJobId(
                            laidOut, unpartitionedBaseIds.get(i), salt, tables.get(i)));
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
        // The unpartitioned plan prepared its tables too, under their own names and without one.
        assertThat(unpartitioned.tableAdmin.preparedTemporaryTables)
                .containsExactlyInAnyOrderElementsOf(loadTables(unpartitioned));
        assertThat(unpartitioned.tableAdmin.preparedLayouts).containsOnly(TableLayout.NONE);
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
    void olderVersionLaidOutNamesAndIdsArePinned() throws IOException {
        Harness harness = withExistingTable(PARTITIONED_AND_CLUSTERED);

        harness.orchestrator.run(stagedByOlderVersion(threeOversizedFiles()));

        // Pinned literally: a commit restored from state an older version wrote finds its tables
        // and re-attaches to its jobs by these names and ids, so neither they nor the salt may
        // drift.
        assertThat(CommitPlanner.layoutSalt(PARTITIONED_AND_CLUSTERED)).isEqualTo("b40ff274");
        assertThat(harness.runner.loads.keySet())
                .first()
                .isEqualTo(
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-4ecae23c5143f639-p0-layout-b40ff274-s7062e446-t1001");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_57b4d06d46c9_p0_layout_b40ff274");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-6a44639ead366984-laidout");
    }

    @Test
    void olderVersionUnpartitionedNamesAndIdsArePinned() throws IOException {
        Harness harness = Harness.plain();

        harness.orchestrator.run(stagedByOlderVersion(threeOversizedFiles()));

        // Pinned literally: a commit restored from state an older version wrote finds its tables
        // by these names and re-attaches to its final copy by this id. A successful attempt
        // deleted its staged files, so under any other names it could neither re-attach nor load
        // them again. Each load also names its table's incarnation, as this version's do (#2290):
        // a commit whose final copy succeeded loads nothing, and one that did not refills its
        // tables from the staged files it still has.
        assertThat(harness.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-4ecae23c5143f639-p0-s7062e446-t1001",
                        "flink-bq-load-" + FLINK_JOB_ID + "-17a6853089163595-p1-s7062e446-t1002",
                        "flink-bq-load-" + FLINK_JOB_ID + "-75e4dec0e13b0f39-p2-s7062e446-t1003");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-6a44639ead366984");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_57b4d06d46c9_p0");
    }

    @Test
    void olderVersionStreamingNamesAndIdsArePinned() throws IOException {
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, Limits.BIGQUERY);

        harness.orchestrator.run(stagedByOlderVersion(threeOversizedFiles()));

        // Pinned literally, and recomputed from 1.4.0's derivation outside this code: a streaming
        // commit restored from that version's checkpoint or savepoint re-attaches by these names
        // and ids, which take the checkpoint segment and fold the staging format into the hash.
        assertThat(harness.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-4ecae23c5143f639-p0-s7062e446-t1001",
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-17a6853089163595-p1-s7062e446-t1002",
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-c7-75e4dec0e13b0f39-p2-s7062e446-t1003");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_54a2badd475a_c7_p0");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-c7-aa511602215ce9c4");
    }

    @Test
    void olderVersionFilesWrittenBackByThisVersionKeepTheirNames() throws IOException {
        // A restored committable that has not committed is written again by this version, in the
        // version-5 layout, before it commits; it must still plan under the 1.4.0 names pinned in
        // olderVersionStreamingNamesAndIdsArePinned.
        FileLoadsCommittableSerializer serializer = new FileLoadsCommittableSerializer();
        List<FileLoadsCommittable> writtenBack = new ArrayList<>();
        for (FileLoadsCommittable file : stagedByOlderVersion(threeOversizedFiles())) {
            writtenBack.add(
                    serializer.deserialize(
                            serializer.getVersion(),
                            serializer.serialize(file.withCheckpointId(7L))));
        }
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, Limits.BIGQUERY);

        harness.orchestrator.run(writtenBack);

        assertThat(harness.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-4ecae23c5143f639-p0-s7062e446-t1001",
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-17a6853089163595-p1-s7062e446-t1002",
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-c7-75e4dec0e13b0f39-p2-s7062e446-t1003");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-c7-aa511602215ce9c4");
    }

    @Test
    void uriDerivedStreamingNamesAndIdsArePinned() throws IOException {
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, Limits.BIGQUERY);

        harness.orchestrator.run(threeOversizedFiles());

        // Pinned literally for the reason uriDerivedNamesAndIdsArePinned gives, and recomputed
        // outside this code: the streaming checkpoint segment comes on top of the URI hash.
        assertThat(harness.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-9bb39e0971d977a3-p0-s7062e446-t1001",
                        "flink-bq-load-" + FLINK_JOB_ID + "-c7-4cb2656901da57fd-p1-s7062e446-t1002",
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-c7-f514a856731087a5-p2-s7062e446-t1003");
        assertThat(loadTables(harness).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_e88106585483_c7_p0");
        assertThat(harness.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-c7-af346af83af6b828");
    }

    @Test
    void uriDerivedNamesAndIdsArePinned() throws IOException {
        Harness plain = Harness.plain();
        Harness laidOut = withExistingTable(PARTITIONED_AND_CLUSTERED);

        plain.orchestrator.run(threeOversizedFiles());
        laidOut.orchestrator.run(threeOversizedFiles());

        // Pinned literally: a retried commit re-attaches by these names and ids, so changing them
        // takes a new record in the committable, never an edit here. The suffix on each load is
        // the incarnation of the table it fills, its creation time in the fake.
        assertThat(plain.runner.loads.keySet())
                .containsExactly(
                        "flink-bq-load-" + FLINK_JOB_ID + "-a46d2635e4333ab8-p0-s7062e446-t1001",
                        "flink-bq-load-" + FLINK_JOB_ID + "-cf81386069c16b3e-p1-s7062e446-t1002",
                        "flink-bq-load-" + FLINK_JOB_ID + "-c39c2d39f5581ae8-p2-s7062e446-t1003");
        assertThat(loadTables(plain).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_e88106585483_p0");
        assertThat(plain.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-40644794e0f1d10f");
        assertThat(laidOut.runner.loads.keySet())
                .first()
                .isEqualTo(
                        "flink-bq-load-"
                                + FLINK_JOB_ID
                                + "-a46d2635e4333ab8-p0-layout-b40ff274-s7062e446-t1001");
        assertThat(loadTables(laidOut).get(0).getTable())
                .isEqualTo("tmp_" + FLINK_JOB_ID + "_e88106585483_p0_layout_b40ff274");
        assertThat(laidOut.runner.copies.keySet())
                .containsExactly("flink-bq-copy-" + FLINK_JOB_ID + "-40644794e0f1d10f-laidout");
    }

    @Test
    void aStreamingCommitRepeatingAnEarlierCommitsIdsLoadsItsOwnFiles() throws IOException {
        // The measured case of #2289: a redeploy without state under a fixed job id, or a restore
        // of an older checkpoint, repeats the Flink job id and the checkpoint id with new files.
        assertARepeatedCommitLoadsItsOwnFiles(
                () -> new Harness(OPTIONS, builder -> {}, 1L, Limits.BIGQUERY));
    }

    @Test
    void aBatchCommitRepeatingAnEarlierCommitsJobIdLoadsItsOwnFiles() throws IOException {
        assertARepeatedCommitLoadsItsOwnFiles(Harness::plain);
    }

    @Test
    void aLaidOutCommitRepeatingAnEarlierCommitsIdsLoadsItsOwnFiles() throws IOException {
        assertARepeatedCommitLoadsItsOwnFiles(
                () -> {
                    Harness harness = new Harness(OPTIONS, builder -> {}, 1L, Limits.BIGQUERY);
                    harness.tableAdmin.tables.put(T1, SCHEMA);
                    harness.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
                    return harness;
                });
    }

    @Test
    void aWriteTruncateDataCommitRepeatingAnEarlierCommitsIdsLoadsItsOwnFiles() throws IOException {
        // This disposition never asks whether its final copy succeeded; its copy and terminal
        // query would re-attach by id alone, so their ids must differ too. It is batch-only, so
        // the commits carry no checkpoint id.
        assertARepeatedCommitLoadsItsOwnFiles(
                () -> new Harness(options(WriteDisposition.WRITE_TRUNCATE_DATA), builder -> {}));
    }

    @Test
    void aCommitOfOlderVersionFilesStillReattachesToTheCopyItsIdsName() throws IOException {
        // What every version before #2289 did, kept only for committables restored from its
        // state: the commit cannot tell an earlier commit's copy from its own and loads nothing.
        Harness first = new Harness(OPTIONS, builder -> {}, 1L, Limits.BIGQUERY);
        first.orchestrator.run(stagedByOlderVersion(oversizedFiles("a", "b", "c")));
        Harness second = new Harness(OPTIONS, builder -> {}, 1L, Limits.BIGQUERY);
        second.runner.copiesThatSucceeded.addAll(first.runner.copies.keySet());

        second.orchestrator.run(stagedByOlderVersion(oversizedFiles("d", "e", "f")));

        assertThat(second.runner.loads).isEmpty();
        assertThat(second.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
    }

    @Test
    void aLeafLoadNamesTheTemporaryTableItFills() throws IOException {
        Harness destinationDataset = Harness.plain();
        Harness scratchDataset = Harness.plain();

        destinationDataset.orchestrator.run(threeOversizedFiles());
        scratchDataset.orchestrator.run(
                threeOversizedFiles().stream()
                        .map(file -> inTempDataset(file, "scratch"))
                        .toList());

        // The same files with their tables in another dataset fill other tables, so their load ids
        // differ too: a re-attach compares only ids, never the table a job filled.
        assertThat(loadTables(scratchDataset))
                .allSatisfy(table -> assertThat(table.getDataset()).isEqualTo("scratch"));
        assertThat(scratchDataset.runner.loads.keySet())
                .doesNotContainAnyElementsOf(destinationDataset.runner.loads.keySet());
    }

    @Test
    void aRestoredCommitNamesItsTablesInTheTemporaryDatasetItsFilesRecorded() throws IOException {
        // An attempt copied this destination and the commit then failed elsewhere, so the restore
        // commits the same files again, now under another configured tempDataset (#2289).
        List<FileLoadsCommittable> files = new ArrayList<>();
        for (FileLoadsCommittable file : threeOversizedFiles()) {
            files.add(inTempDataset(file.withCheckpointId(1L), "scratch_old"));
        }
        // Three different datasets: the recorded one, the newly configured one, and the
        // destination's own, so substituting either of the others for the recorded one fails.
        Harness first = new Harness(OPTIONS, builder -> {}, 1L, Limits.BIGQUERY);
        first.orchestrator.run(files);
        Harness restored =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .tempDataset("scratch")
                                .build(),
                        builder -> {},
                        1L,
                        Limits.BIGQUERY);
        restored.runner.copiesThatSucceeded.addAll(first.runner.copies.keySet());

        restored.orchestrator.run(files);

        // The recorded dataset names the tables the attempt filled, so the completed copy is
        // found and nothing is loaded or copied again.
        assertThat(restored.runner.loads).isEmpty();
        assertThat(restored.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
        assertThat(onlyCopy(restored).getSourceTables())
                .allSatisfy(table -> assertThat(table.getDataset()).isEqualTo("scratch_old"));

        // Files recording none, as an older version staged them, take the configured dataset under
        // that version's naming. Under scratch the copy an older run completed in scratch_old is
        // missed, while the old leaf load ids, which do not name their table, stay the same: the
        // runner re-attaches to the loads that filled scratch_old and the copy then reads scratch's
        // unfilled tables and fails on every restart (a laid-out destination, whose load ids name
        // the table's incarnation, would load again instead). The same files under
        // scratch_old find the copy. The dataset is the only change, which is why a new run
        // records it.
        List<FileLoadsCommittable> legacy =
                stagedByOlderVersion(
                        threeOversizedFiles().stream()
                                .map(file -> file.withCheckpointId(1L))
                                .toList());
        Harness legacyFirst =
                new Harness(tempDatasetOptions("scratch_old"), builder -> {}, 1L, Limits.BIGQUERY);
        legacyFirst.orchestrator.run(legacy);
        Harness unrecorded =
                new Harness(tempDatasetOptions("scratch"), builder -> {}, 1L, Limits.BIGQUERY);
        unrecorded.runner.copiesThatSucceeded.addAll(legacyFirst.runner.copies.keySet());
        unrecorded.orchestrator.run(legacy);
        Harness sameDataset =
                new Harness(tempDatasetOptions("scratch_old"), builder -> {}, 1L, Limits.BIGQUERY);
        sameDataset.runner.copiesThatSucceeded.addAll(legacyFirst.runner.copies.keySet());
        sameDataset.orchestrator.run(legacy);

        assertThat(loadTables(legacyFirst))
                .hasSize(3)
                .allSatisfy(table -> assertThat(table.getDataset()).isEqualTo("scratch_old"));
        assertThat(unrecorded.runner.loads.keySet())
                .containsExactlyElementsOf(legacyFirst.runner.loads.keySet());
        assertThat(unrecorded.runner.copies.keySet())
                .doesNotContainAnyElementsOf(legacyFirst.runner.copies.keySet());
        assertThat(onlyCopy(unrecorded).getSourceTables())
                .allSatisfy(table -> assertThat(table.getDataset()).isEqualTo("scratch"));
        assertThat(sameDataset.runner.loads).isEmpty();
    }

    private static FileLoadsOptions tempDatasetOptions(String tempDataset) {
        return FileLoadsOptions.builder()
                .stagingPath("gs://bucket/prefix")
                .tempDataset(tempDataset)
                .build();
    }

    @Test
    void aRestoredCommitKeepsEveryTemporaryTableAndIdInItsRecordedDataset() throws IOException {
        // Intermediate levels (a fan-out of two) and WRITE_TRUNCATE_DATA's aggregate table, copy
        // and terminal query: every one of their ids hashes a temporary table's path, and this
        // disposition asks only whether its terminal query succeeded, so each must be the
        // attempt's.
        Limits fanOutOfTwo = new Limits(2, 100, 100, 100);
        List<FileLoadsCommittable> files = new ArrayList<>();
        for (FileLoadsCommittable file : threeOversizedFiles()) {
            files.add(inTempDataset(file, "scratch_old"));
        }
        Harness first =
                new Harness(
                        options(WriteDisposition.WRITE_TRUNCATE_DATA),
                        builder -> {},
                        null,
                        fanOutOfTwo);
        first.orchestrator.run(files);
        Harness restored =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                                .tempDataset("scratch")
                                .build(),
                        builder -> {},
                        null,
                        fanOutOfTwo);

        restored.orchestrator.run(files);

        assertThat(first.runner.copies).hasSizeGreaterThan(1);
        assertThat(first.runner.queries).hasSize(1);
        assertThat(restored.runner.loads.keySet())
                .containsExactlyElementsOf(first.runner.loads.keySet());
        assertThat(restored.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
        assertThat(restored.runner.queries.keySet())
                .containsExactlyElementsOf(first.runner.queries.keySet());
        assertThat(restored.runner.copies.values())
                .allSatisfy(
                        copy -> {
                            assertThat(copy.getDestination().getDataset()).isEqualTo("scratch_old");
                            assertThat(copy.getSourceTables())
                                    .allSatisfy(
                                            table ->
                                                    assertThat(table.getDataset())
                                                            .isEqualTo("scratch_old"));
                        });
    }

    @Test
    void eachDestinationKeepsTheTemporaryDatasetItsFilesRecorded() throws IOException {
        // Without tempDataset each destination records its own dataset, so one commit routinely
        // carries several; each destination's tables go to its own.
        TableDestination inE = TableDestination.of("p", "e", "t2");
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = new ArrayList<>();
        for (String name : List.of("a", "b")) {
            files.add(file(T1, name, sixTiB));
            files.add(file(inE, name, sixTiB));
        }
        Harness harness = Harness.plain();

        harness.orchestrator.run(files);

        assertThat(harness.runner.copies.values())
                .hasSize(2)
                .allSatisfy(
                        copy ->
                                assertThat(copy.getSourceTables())
                                        .allSatisfy(
                                                table ->
                                                        assertThat(table.getDataset())
                                                                .isEqualTo(
                                                                        copy.getDestination()
                                                                                .getDataset())));
    }

    @Test
    void aDestinationWhoseFilesRecordTwoTemporaryDatasetsFailsBeforeAnyJob() {
        // One run resolves one dataset per destination, so two for one destination is corrupt
        // state; a mix of recorded and unrecorded files is the committer's to refuse.
        List<FileLoadsCommittable> files = threeOversizedFiles();
        Harness harness = Harness.plain();

        assertThatThrownBy(
                        () ->
                                harness.orchestrator.run(
                                        List.of(
                                                files.get(0),
                                                inTempDataset(files.get(1), "scratch"),
                                                files.get(2))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("two temporary datasets");
        assertThat(harness.runner.loads).isEmpty();
    }

    /**
     * Runs a commit, then a second commit of other files under the same Flink job id and checkpoint
     * id against a service where every copy of the first succeeded, and asserts that the second
     * loads and copies its own files rather than re-attaching to the first's jobs.
     */
    private static void assertARepeatedCommitLoadsItsOwnFiles(Supplier<Harness> newHarness)
            throws IOException {
        Harness first = newHarness.get();
        first.orchestrator.run(oversizedFiles("a", "b", "c"));
        Harness second = newHarness.get();
        second.runner.copiesThatSucceeded.addAll(first.runner.copies.keySet());

        second.orchestrator.run(oversizedFiles("d", "e", "f"));

        assertThat(second.runner.loads.values())
                .flatExtracting(LoadJobSpec::getSourceUris)
                .containsExactlyInAnyOrder(
                        "gs://bucket/prefix/d.avro",
                        "gs://bucket/prefix/e.avro",
                        "gs://bucket/prefix/f.avro");
        assertThat(second.runner.loads.keySet())
                .doesNotContainAnyElementsOf(first.runner.loads.keySet());
        assertThat(loadTables(second)).doesNotContainAnyElementsOf(loadTables(first));
        assertThat(second.runner.copies.keySet())
                .isNotEmpty()
                .doesNotContainAnyElementsOf(first.runner.copies.keySet());
        assertThat(second.runner.copies.values())
                .extracting(CopyJobSpec::getEquivalentJobId)
                .doesNotContainAnyElementsOf(first.runner.copies.keySet());
        assertThat(second.runner.queries).hasSameSizeAs(first.runner.queries);
        assertThat(second.runner.queries.keySet())
                .allSatisfy(id -> assertThat(first.runner.queries).doesNotContainKey(id));
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
        // Its temporary tables are prepared as an unpartitioned destination's are: without a
        // layout, so the ingestion-time partitioning never reaches them.
        assertThat(ingestion.tableAdmin.preparedTemporaryTables)
                .containsExactlyElementsOf(unpartitioned.tableAdmin.preparedTemporaryTables);
        assertThat(ingestion.tableAdmin.preparedLayouts).containsOnly(TableLayout.NONE);
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

    /**
     * A direct load never creates its destination, whatever the write disposition (#2181): only the
     * reconciliation creates one, with its creation options.
     */
    @ParameterizedTest
    @EnumSource(WriteDisposition.class)
    void aDirectLoadNeverCreatesItsDestination(WriteDisposition disposition) throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(disposition)
                                .build(),
                        builder -> {});

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10)));

        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.runner.loads.values())
                .singleElement()
                .extracting(LoadJobSpec::getCreateDisposition)
                .isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER);
    }

    @Test
    void aStreamingDirectLoadNeverCreatesItsDestination() throws IOException {
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, Limits.BIGQUERY);

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .extracting(LoadJobSpec::getCreateDisposition)
                .isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER);
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
        // Every temporary table, the aggregate included, is prepared without a layout.
        assertThat(harness.tableAdmin.preparedTemporaryTables)
                .containsExactlyElementsOf(unpartitioned.tableAdmin.preparedTemporaryTables)
                .containsAll(loadTables(harness))
                .contains(onlyCopy(harness).getDestination());
        assertThat(harness.tableAdmin.preparedLayouts).containsOnly(TableLayout.NONE);
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
        // The unpartitioned intermediate copy names its prepared table's incarnation too.
        String intermediateBaseId =
                withoutPreparation(unpartitioned, copyIds.get(0), copies.get(0).getDestination());
        List<CopyJobSpec> laidOutCopies = new ArrayList<>(laidOut.runner.copies.values());
        assertThat(laidOutCopies)
                .extracting(CopyJobSpec::getCreateDisposition)
                .containsOnly(JobInfo.CreateDisposition.CREATE_NEVER);
        assertThat(laidOut.runner.copies.keySet())
                .containsExactly(
                        preparedLaidOutJobId(
                                laidOut,
                                intermediateBaseId,
                                salt,
                                laidOutCopies.get(0).getDestination()),
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
    void anUnlaidTemporaryTableThatExpiredBeforeARetryIsFilledAgain() throws IOException {
        Harness first = withExistingTable(TableLayout.NONE);
        first.orchestrator.run(threeOversizedFiles());
        List<TableDestination> tables = loadTables(first);

        // As above, for a destination whose copy accepts tables without a layout (#2290). The
        // first run stands for an earlier attempt that stopped before its final copy: the fakes
        // carry its tables over, but not its success. Re-attaching to the load that filled the
        // expired table would leave the copy without its source on every attempt.
        Harness retry = withExistingTable(TableLayout.NONE);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        retry.tableAdmin.temporaryTables.remove(tables.get(0));
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.orchestrator.run(threeOversizedFiles());

        List<String> before = new ArrayList<>(first.runner.loads.keySet());
        List<String> after = new ArrayList<>(retry.runner.loads.keySet());
        assertThat(after.get(0)).isNotEqualTo(before.get(0));
        assertThat(after.subList(1, 3)).isEqualTo(before.subList(1, 3));
        assertThat(loadTables(retry)).isEqualTo(tables);
        assertThat(retry.tableAdmin.preparedTemporaryTables).containsExactlyElementsOf(tables);
        assertThat(retry.tableAdmin.preparedLayouts).containsOnly(TableLayout.NONE);
        assertThat(retry.tableAdmin.preparedExpirations)
                .containsOnly(FileLoadsOptions.DEFAULT_TEMP_TABLE_EXPIRATION);
        assertThat(retry.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
    }

    @ParameterizedTest
    @EnumSource(
            value = WriteDisposition.class,
            names = {"WRITE_APPEND", "WRITE_TRUNCATE_DATA"})
    void aSchemaThatChangedBetweenAttemptsRefillsEveryTemporaryTable(WriteDisposition disposition)
            throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(disposition)
                        .build();
        Harness first = withExistingTable(options, builder -> {}, TableLayout.NONE);
        first.orchestrator.run(threeOversizedFiles());

        // No table expired, but the destination gained a column, so the retry reconciles another
        // schema. Re-attaching to the jobs that filled the surviving tables would leave the copy
        // sources of two schemas once any one of them expired and was filled again.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.tableAdmin.tables.put(
                T1,
                SCHEMA.toBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("f3")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build());
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(loadTables(retry)).isEqualTo(loadTables(first));
        assertThat(retry.runner.loads.keySet())
                .hasSize(3)
                .doesNotContainAnyElementsOf(first.runner.loads.keySet());
        if (disposition == WriteDisposition.WRITE_APPEND) {
            // The final copy writes the destination and keeps its id.
            assertThat(retry.runner.copies.keySet())
                    .containsExactlyElementsOf(first.runner.copies.keySet());
        } else {
            // The copy into the surviving aggregate fills it again under the new schema; the
            // terminal query that reads it keeps its id.
            assertThat(onlyCopy(retry).getDestination())
                    .isEqualTo(onlyCopy(first).getDestination());
            assertThat(onlyCopyId(retry)).isNotEqualTo(onlyCopyId(first));
            assertThat(retry.runner.queries.keySet())
                    .containsExactlyElementsOf(first.runner.queries.keySet());
        }
    }

    @Test
    void anIntermediateCopyLeftUnfinishedIsWaitedForBeforeItsSourcesArePrepared()
            throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, limits);
        harness.tableAdmin.tables.put(T1, SCHEMA);
        harness.tableAdmin.events = harness.runner.events;

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10)));

        // Three leaves over a fan-out of two: one intermediate copy, from the first two leaves.
        // An earlier attempt's copy into the intermediate table may not have read those leaves
        // yet, so neither is prepared, and possibly created anew empty, before it has finished.
        assertWaitedForBeforeItsSourcesWerePrepared(
                harness,
                harness.runner.copies.keySet().iterator().next(),
                harness.runner.copies.values().iterator().next());
    }

    @Test
    void anAggregateCopyLeftUnfinishedIsWaitedForBeforeItsSourcesArePrepared() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness harness = withExistingTable(options, builder -> {}, TableLayout.NONE);
        harness.tableAdmin.events = harness.runner.events;

        harness.orchestrator.run(threeOversizedFiles());

        assertWaitedForBeforeItsSourcesWerePrepared(
                harness, onlyCopyId(harness), onlyCopy(harness));
    }

    @Test
    void everyLevelOfCopiesLeftUnfinishedIsWaitedForBeforeItsSourcesArePrepared()
            throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        Harness harness = new Harness(options, builder -> {}, null, limits);
        harness.tableAdmin.tables.put(T1, SCHEMA);
        harness.tableAdmin.events = harness.runner.events;

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10)));

        // An intermediate copy from the first two leaves, and the copy into the aggregate table
        // from that intermediate table and the third leaf: the aggregate's copy is waited for
        // before the intermediate table it reads is prepared, and so on down.
        assertThat(harness.runner.copies).hasSize(2);
        for (Map.Entry<String, CopyJobSpec> copy : harness.runner.copies.entrySet()) {
            assertWaitedForBeforeItsSourcesWerePrepared(harness, copy.getKey(), copy.getValue());
        }
    }

    @Test
    void aLaidOutIntermediateCopyIsWaitedForUnderTheIdItIsSubmittedUnder() throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        Harness harness = new Harness(OPTIONS, builder -> {}, 7L, limits);
        harness.tableAdmin.tables.put(T1, SCHEMA);
        harness.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        harness.tableAdmin.events = harness.runner.events;

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10)));

        assertWaitedForBeforeItsSourcesWerePrepared(
                harness,
                harness.runner.copies.keySet().iterator().next(),
                harness.runner.copies.values().iterator().next());
    }

    /**
     * Asserts that {@code harness} prepared the table {@code copy} fills, then asked about the copy
     * under {@code copyId}, whose answer waits for one left unfinished, and only then prepared any
     * of its sources.
     */
    private static void assertWaitedForBeforeItsSourcesWerePrepared(
            Harness harness, String copyId, CopyJobSpec copy) {
        List<String> events = harness.runner.events;
        int prepared = events.indexOf("prepare:" + copy.getDestination().toTablePath());
        int waited = events.indexOf("copy-succeeded?:" + copyId);
        assertThat(prepared).as("the filled table is prepared").isNotNegative();
        assertThat(waited).as("the filling copy is waited for after that").isGreaterThan(prepared);
        for (TableDestination source : copy.getSourceTables()) {
            assertThat(events.indexOf("prepare:" + source.toTablePath()))
                    .as("source %s is prepared after the wait", source)
                    .isGreaterThan(waited);
        }
    }

    @Test
    void theLeavesUnderAnAggregateCopyThatSucceededAreNotFilledAgain() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness first = withExistingTable(options, builder -> {}, TableLayout.NONE);
        first.orchestrator.run(threeOversizedFiles());
        TableDestination aggregate = onlyCopy(first).getDestination();

        // The copy into the aggregate table succeeded, the terminal query did not run, and every
        // leaf expired meanwhile: the aggregate already holds every row, so the retry does not
        // load the leaves, which would need staged files a lifecycle rule may have removed, and
        // runs the terminal query over the aggregate.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.tableAdmin.temporaryTables.put(
                aggregate, first.tableAdmin.temporaryTables.get(aggregate));
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.runner.copiesThatSucceeded.add(onlyCopyId(first));
        retry.orchestrator.run(threeOversizedFiles());

        // The expired leaves are not created anew: an empty one could only be read by an earlier
        // attempt's copy still running under another id.
        assertThat(retry.tableAdmin.preparedTemporaryTables).containsExactly(aggregate);
        assertThat(retry.tableAdmin.extendedTemporaryTables).isEmpty();
        assertThat(retry.tableAdmin.temporaryTables.keySet()).containsExactly(aggregate);
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(onlyCopyId(first));
        assertThat(retry.runner.queries.keySet())
                .containsExactlyElementsOf(first.runner.queries.keySet());
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(first.runner.deletedTables);
    }

    @Test
    void theLeavesUnderAnIntermediateCopyThatSucceededAreNotFilledAgain() throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10));
        Harness first = new Harness(OPTIONS, builder -> {}, 7L, limits);
        first.tableAdmin.tables.put(T1, SCHEMA);
        first.orchestrator.run(files);
        String intermediateCopyId = first.runner.copies.keySet().iterator().next();
        CopyJobSpec intermediate = first.runner.copies.values().iterator().next();

        // The intermediate copy from the first two leaves succeeded; those leaves expired, while
        // the intermediate table and the third leaf survived. Only the third leaf is loaded again,
        // and its load re-attaches; the expired leaves are not created anew.
        Harness retry = new Harness(OPTIONS, builder -> {}, 7L, limits);
        retry.tableAdmin.tables.put(T1, SCHEMA);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        for (TableDestination source : intermediate.getSourceTables()) {
            retry.tableAdmin.temporaryTables.remove(source);
        }
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.runner.copiesThatSucceeded.add(intermediateCopyId);
        retry.orchestrator.run(files);

        TableDestination thirdLeaf = loadTables(first).get(2);
        assertThat(retry.tableAdmin.preparedTemporaryTables)
                .containsExactlyInAnyOrder(intermediate.getDestination(), thirdLeaf);
        assertThat(retry.tableAdmin.extendedTemporaryTables).isEmpty();
        assertThat(loadTables(retry)).containsExactly(thirdLeaf);
        assertThat(retry.runner.loads.keySet())
                .containsExactly(new ArrayList<>(first.runner.loads.keySet()).get(2));
        assertThat(retry.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
    }

    @Test
    void everyLevelBelowACopyThatSucceededIsNotFilledAgain() throws IOException {
        // Five leaves over a fan-out of two: level one copies a+b and c+d, carrying e; level two
        // copies those two intermediate tables, carrying e again; the final copy reads the level
        // two table and e.
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        List<FileLoadsCommittable> files =
                List.of(
                        file(T1, "a", 10),
                        file(T1, "b", 10),
                        file(T1, "c", 10),
                        file(T1, "d", 10),
                        file(T1, "e", 10));
        Harness first = new Harness(OPTIONS, builder -> {}, 7L, limits);
        first.tableAdmin.tables.put(T1, SCHEMA);
        first.orchestrator.run(files);
        List<String> copyIds = new ArrayList<>(first.runner.copies.keySet());
        List<CopyJobSpec> copies = new ArrayList<>(first.runner.copies.values());
        assertThat(copies).hasSize(4);
        String topCopyId = copyIds.get(2);
        TableDestination top = copies.get(2).getDestination();

        // The level-two copy succeeded and every leaf under it expired: the level-one tables and
        // their leaves are neither loaded nor copied again, and only the carried leaf is loaded
        // for the final copy.
        Harness retry = new Harness(OPTIONS, builder -> {}, 7L, limits);
        retry.tableAdmin.tables.put(T1, SCHEMA);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        TableDestination carried = loadTables(first).get(4);
        for (TableDestination leaf : loadTables(first).subList(0, 4)) {
            retry.tableAdmin.temporaryTables.remove(leaf);
        }
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.runner.copiesThatSucceeded.add(topCopyId);
        retry.orchestrator.run(files);

        // The level-one tables survived: they are extended, so they live as long as the level-two
        // table for a later attempt that finds it expired, but neither created nor filled.
        assertThat(retry.tableAdmin.preparedTemporaryTables)
                .containsExactlyInAnyOrder(top, carried);
        assertThat(retry.tableAdmin.extendedTemporaryTables)
                .containsExactlyInAnyOrderElementsOf(copies.get(2).getSourceTables());
        assertThat(retry.tableAdmin.extendedExpirations)
                .containsOnly(FileLoadsOptions.DEFAULT_TEMP_TABLE_EXPIRATION);
        assertThat(loadTables(retry)).containsExactly(carried);
        assertThat(retry.runner.copies.keySet()).containsExactly(topCopyId, copyIds.get(3));
    }

    @Test
    void theTablesBelowALaidOutCopyThatSucceededAreNotFilledAgain() throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10));
        Harness first = new Harness(OPTIONS, builder -> {}, 7L, limits);
        first.tableAdmin.tables.put(T1, SCHEMA);
        first.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        first.orchestrator.run(files);
        String intermediateCopyId = first.runner.copies.keySet().iterator().next();
        CopyJobSpec intermediate = first.runner.copies.values().iterator().next();

        Harness retry = new Harness(OPTIONS, builder -> {}, 7L, limits);
        retry.tableAdmin.tables.put(T1, SCHEMA);
        retry.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.runner.copiesThatSucceeded.add(intermediateCopyId);
        retry.orchestrator.run(files);

        // The renamed leaves under the laid-out intermediate copy survived: extended, not loaded.
        assertThat(retry.tableAdmin.preparedTemporaryTables)
                .doesNotContainAnyElementsOf(intermediate.getSourceTables())
                .contains(intermediate.getDestination());
        assertThat(retry.tableAdmin.extendedTemporaryTables)
                .containsExactlyInAnyOrderElementsOf(intermediate.getSourceTables());
        assertThat(loadTables(retry)).doesNotContainAnyElementsOf(intermediate.getSourceTables());
        assertThat(retry.runner.loads).hasSize(1);
        assertThat(retry.runner.copies.keySet())
                .containsExactlyElementsOf(first.runner.copies.keySet());
    }

    @Test
    void anExpiredAggregateTableIsCopiedAgainUnderWriteTruncateData() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness first = withExistingTable(options, builder -> {}, TableLayout.NONE);
        first.orchestrator.run(threeOversizedFiles());
        TableDestination aggregate = onlyCopy(first).getDestination();

        // The first run stands for an earlier attempt that stopped before its terminal query
        // succeeded. The leaves survived and the aggregate expired: the copy into it runs again,
        // and the terminal query reads the refilled table.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.tableAdmin.temporaryTables.putAll(first.tableAdmin.temporaryTables);
        retry.tableAdmin.temporaryTables.remove(aggregate);
        retry.tableAdmin.clock = first.tableAdmin.clock;
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads.keySet())
                .containsExactlyElementsOf(first.runner.loads.keySet());
        assertThat(onlyCopy(retry).getDestination()).isEqualTo(aggregate);
        assertThat(onlyCopyId(retry)).isNotEqualTo(onlyCopyId(first));
        assertThat(onlyCopy(retry).getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER);
        assertThat(retry.runner.queries.keySet())
                .containsExactlyElementsOf(first.runner.queries.keySet());
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

        // Preparing its tables would recreate the ones the successful commit deleted, and the
        // loads they then need would read staged files the commit deleted too.
        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(onlyCopyId(earlier));
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(earlier.runner.deletedTables);
    }

    @Test
    void aCompletedCommitOfOlderVersionFilesIsNeitherPreparedNorLoadedAgain() throws IOException {
        Harness earlier = Harness.plain();
        earlier.orchestrator.run(stagedByOlderVersion(threeOversizedFiles()));

        // A restore of state an older version wrote commits its last checkpoint's files again,
        // which the commit that succeeded deleted: only its final copy, whose id is unchanged,
        // may be re-attached to.
        Harness retry = Harness.plain();
        retry.runner.copiesThatSucceeded.add(onlyCopyId(earlier));
        retry.orchestrator.run(stagedByOlderVersion(threeOversizedFiles()));

        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies.keySet()).containsExactly(onlyCopyId(earlier));
    }

    @ParameterizedTest(name = "staged by an older version: {0}")
    @ValueSource(booleans = {false, true})
    void writeTruncateDataWhoseTerminalQuerySucceededIsNeitherPreparedNorLoadedAgain(
            boolean olderVersion) throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        List<FileLoadsCommittable> files =
                olderVersion ? stagedByOlderVersion(threeOversizedFiles()) : threeOversizedFiles();
        Harness earlier = withExistingTable(options, builder -> {}, TableLayout.NONE);
        earlier.orchestrator.run(files);
        String queryId = earlier.runner.queries.keySet().iterator().next();

        // The commit succeeded and deleted its temporary tables and staged files; the restore
        // commits the same files again. Preparing the tables would create them anew, and the
        // loads under their new incarnations would read the deleted files on every attempt.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.runner.queriesThatSucceeded.add(queryId);
        retry.orchestrator.run(files);

        assertThat(retry.tableAdmin.preparedTemporaryTables).isEmpty();
        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.copies).isEmpty();
        assertThat(retry.runner.queries.keySet()).containsExactly(queryId);
        assertThat(retry.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(earlier.runner.deletedTables);
        assertThat(retry.runner.events).noneMatch(event -> event.startsWith("copy-succeeded?:"));
    }

    @Test
    void writeTruncateDataStillRunsItsQueryAfterItsAggregateCopySucceeded() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        Harness earlier = withExistingTable(options, builder -> {}, TableLayout.NONE);
        earlier.orchestrator.run(threeOversizedFiles());

        // Its last copy fills the aggregate table, not the destination: a copy that succeeded
        // there stands for nothing the terminal query has written, so the query still runs; it
        // only leaves the leaves unfilled. The tables survived, so the aggregate keeps the
        // incarnation that copy filled.
        Harness retry = withExistingTable(options, builder -> {}, TableLayout.NONE);
        retry.tableAdmin.temporaryTables.putAll(earlier.tableAdmin.temporaryTables);
        retry.tableAdmin.clock = earlier.tableAdmin.clock;
        retry.runner.copiesThatSucceeded.addAll(earlier.runner.copies.keySet());
        retry.orchestrator.run(threeOversizedFiles());

        assertThat(retry.runner.loads).isEmpty();
        assertThat(retry.runner.queries.keySet())
                .containsExactlyElementsOf(earlier.runner.queries.keySet());
        assertThat(retry.runner.events)
                .filteredOn(event -> event.startsWith("copy-succeeded?:"))
                .containsExactly("copy-succeeded?:" + onlyCopyId(retry));
        assertThat(retry.runner.events)
                .contains("query-succeeded?:" + retry.runner.queries.keySet().iterator().next());
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

    // --- Encryption: the destination's own key, stated only by jobs that replace a table ---

    private static final String KEY = "projects/p/locations/l/keyRings/r/cryptoKeys/k";
    private static final String OTHER_KEY = "projects/p/locations/l/keyRings/r/cryptoKeys/other";

    private static Harness withExistingKeyedTable(
            FileLoadsOptions options, TableLayout layout, Limits limits) {
        Harness harness = new Harness(options, builder -> {}, null, limits);
        harness.tableAdmin.tables.put(T1, SCHEMA);
        harness.tableAdmin.layouts.put(T1, layout);
        harness.tableAdmin.kmsKeyNames.put(T1, KEY);
        return harness;
    }

    private static FileLoadsOptions options(WriteDisposition disposition) {
        return FileLoadsOptions.builder()
                .stagingPath("gs://bucket/prefix")
                .writeDisposition(disposition)
                .build();
    }

    @Test
    void aCreatedDestinationsKeyEncryptsItsLaidOutTempTablesButNotTheAppendingCopy()
            throws IOException {
        Harness harness =
                new Harness(
                        OPTIONS,
                        builder ->
                                builder.tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .clusteredFields(List.of("f2"))
                                                .kmsKeyName(KEY)
                                                .build()));

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.createOptions.get(T1).getKmsKeyName()).isEqualTo(KEY);
        assertThat(harness.tableAdmin.preparedKmsKeyNames).hasSize(3).containsOnly(KEY);
        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getKmsKeyName)
                .containsOnly(KEY);
        // Appending keeps the destination's encryption without a key, and would fail with one
        // other than the table's.
        assertThat(onlyCopy(harness).getWriteDisposition())
                .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
        assertThat(onlyCopy(harness).getKmsKeyName()).isNull();
    }

    @Test
    void unlaidTempTablesAndIntermediateCopiesTakeAnExistingDestinationsKey() throws IOException {
        Limits limits = new Limits(2, 100, 100, 2).withMaxFilesPerJob(1);
        Harness harness = withExistingKeyedTable(OPTIONS, TableLayout.NONE, limits);

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10), file(T1, "c", 10)));

        // Unlaid temporary tables are created with the key, and the jobs that fill them state it.
        assertThat(harness.tableAdmin.preparedTemporaryTables).hasSize(4);
        assertThat(harness.tableAdmin.preparedKmsKeyNames).containsOnly(KEY);
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec -> {
                            assertThat(spec.getDestination()).isNotEqualTo(T1);
                            assertThat(spec.getKmsKeyName()).isEqualTo(KEY);
                        });
        List<CopyJobSpec> copies = new ArrayList<>(harness.runner.copies.values());
        assertThat(copies).hasSize(2);
        assertThat(copies.get(0).getDestination()).isNotEqualTo(T1);
        assertThat(copies.get(0).getKmsKeyName()).isEqualTo(KEY);
        assertThat(copies.get(1).getDestination()).isEqualTo(T1);
        assertThat(copies.get(1).getKmsKeyName()).isNull();
    }

    @Test
    void writeTruncateStatesTheDestinationsKeySoItIsNotReplacedWithTheDefault() throws IOException {
        Harness direct =
                withExistingKeyedTable(
                        options(WriteDisposition.WRITE_TRUNCATE),
                        TableLayout.NONE,
                        Limits.BIGQUERY);
        Harness throughTempTables =
                withExistingKeyedTable(
                        options(WriteDisposition.WRITE_TRUNCATE),
                        TableLayout.NONE,
                        Limits.BIGQUERY);

        direct.orchestrator.run(List.of(file(T1, "a", 10)));
        throughTempTables.orchestrator.run(threeOversizedFiles());

        LoadJobSpec load = direct.runner.loads.values().iterator().next();
        assertThat(load.getDestination()).isEqualTo(T1);
        assertThat(load.getKmsKeyName()).isEqualTo(KEY);
        assertThat(throughTempTables.runner.loads.values())
                .extracting(LoadJobSpec::getKmsKeyName)
                .containsOnly(KEY);
        assertThat(onlyCopy(throughTempTables).getDestination()).isEqualTo(T1);
        assertThat(onlyCopy(throughTempTables).getKmsKeyName()).isEqualTo(KEY);
    }

    @Test
    void writeTruncateDataKeysTheAggregateTableAndLeavesTheTerminalQueryToTheDestination()
            throws IOException {
        Harness harness =
                withExistingKeyedTable(
                        options(WriteDisposition.WRITE_TRUNCATE_DATA),
                        TableLayout.NONE,
                        Limits.BIGQUERY);

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getKmsKeyName)
                .containsOnly(KEY);
        assertThat(onlyCopy(harness).getDestination()).isNotEqualTo(T1);
        assertThat(onlyCopy(harness).getKmsKeyName()).isEqualTo(KEY);
        // The terminal query replaces only data, which keeps the destination's encryption.
        assertThat(harness.runner.queries.values())
                .singleElement()
                .satisfies(query -> assertThat(query.getDestination()).isEqualTo(T1));
    }

    @Test
    void aDirectAppendStatesNoKey() throws IOException {
        Harness harness = withExistingKeyedTable(OPTIONS, TableLayout.NONE, Limits.BIGQUERY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        load -> {
                            assertThat(load.getDestination()).isEqualTo(T1);
                            assertThat(load.getKmsKeyName()).isNull();
                        });
    }

    @Test
    void anExistingDestinationsKeyWinsOverTheConfiguredOne() throws IOException {
        Harness harness =
                new Harness(
                        options(WriteDisposition.WRITE_TRUNCATE),
                        builder ->
                                builder.tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .kmsKeyName(OTHER_KEY)
                                                .build()));
        harness.tableAdmin.tables.put(T1, SCHEMA);
        // Laid out, so the commit prepares its temporary tables and the key they take is visible.
        harness.tableAdmin.layouts.put(T1, PARTITIONED_AND_CLUSTERED);
        harness.tableAdmin.kmsKeyNames.put(T1, KEY);

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.created).isEmpty();
        assertThat(harness.tableAdmin.preparedKmsKeyNames).hasSize(3).containsOnly(KEY);
        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getKmsKeyName)
                .containsOnly(KEY);
        assertThat(onlyCopy(harness).getKmsKeyName()).isEqualTo(KEY);
    }

    @Test
    void aDestinationWithoutAKeyOfItsOwnStatesNone() throws IOException {
        Harness harness =
                withExistingTable(
                        options(WriteDisposition.WRITE_TRUNCATE),
                        builder -> {},
                        PARTITIONED_AND_CLUSTERED);

        harness.orchestrator.run(threeOversizedFiles());

        assertThat(harness.tableAdmin.preparedKmsKeyNames).hasSize(3).containsOnlyNulls();
        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getKmsKeyName)
                .containsOnlyNulls();
        assertThat(onlyCopy(harness).getKmsKeyName()).isNull();
    }

    @Test
    void onlyAReplacingJobStatesTheDestinationsKey() {
        for (JobInfo.WriteDisposition disposition : JobInfo.WriteDisposition.values()) {
            assertThat(LoadJobOrchestrator.jobKmsKeyName(disposition, KEY))
                    .as("%s", disposition)
                    .isEqualTo(disposition == JobInfo.WriteDisposition.WRITE_TRUNCATE ? KEY : null);
            assertThat(LoadJobOrchestrator.jobKmsKeyName(disposition, null)).isNull();
        }
    }
}
