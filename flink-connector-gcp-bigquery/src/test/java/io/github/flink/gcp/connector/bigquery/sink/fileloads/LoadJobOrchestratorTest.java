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

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LoadJobOrchestrator} against recording fakes. */
class LoadJobOrchestratorTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");
    private static final TableDestination T2 = TableDestination.of("p", "d", "t2");

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
    private static final class SchemaOnlySerializer extends BigQueryProtoSerializer<Object> {
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
                destination, "gs://bucket/prefix/" + name + ".avro", bytes, 10);
    }

    /** Everything one orchestration run touches. */
    private static final class Harness {
        private final FakeLoadJobRunner runner = new FakeLoadJobRunner();
        private final FakeTableAdmin tableAdmin = new FakeTableAdmin();
        private final InMemoryStagingStorage storage = new InMemoryStagingStorage();
        private final LoadJobOrchestrator orchestrator;

        Harness(FileLoadsOptions options, Consumer<BigQuerySinkBuilder<Object>> customizer) {
            BigQuerySinkBuilder<Object> builder =
                    BigQuerySink.builder()
                            .writeMethod(WriteMethod.FILE_LOADS)
                            .destination(T1)
                            .serializer(new SchemaOnlySerializer())
                            .fileLoadsOptions(options);
            customizer.accept(builder);
            BigQuerySinkConfig<Object> config =
                    ((BigQueryFileLoadsSink<Object>) builder.build()).getConfig();
            this.orchestrator =
                    new LoadJobOrchestrator(
                            config, options, runner, tableAdmin, storage, FLINK_JOB_ID);
        }

        static Harness plain() {
            return new Harness(
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                    builder -> {});
        }
    }

    @Test
    void singlePartitionSubmitsOneDirectLoadPerTable() throws IOException {
        Harness harness = Harness.plain();
        List<FileLoadsCommittable> files =
                List.of(file(T2, "z", 10), file(T1, "b", 10), file(T1, "a", 10));

        harness.orchestrator.run(files);

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.runner.deletedTables).isEmpty();

        LoadJobSpec t1Spec =
                harness.runner.loads.values().stream()
                        .filter(spec -> spec.getDestination().equals(T1))
                        .findFirst()
                        .orElseThrow();
        // Files are sorted by URI for deterministic partitions.
        assertThat(t1Spec.getSourceUris())
                .containsExactly("gs://bucket/prefix/a.avro", "gs://bucket/prefix/b.avro");
        assertThat(t1Spec.getSchema().getFields()).hasSize(2);
        assertThat(t1Spec.getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        assertThat(t1Spec.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
        assertThat(t1Spec.getSchemaUpdateOptions()).isEmpty(); // Updates disabled by default.
        assertThat(t1Spec.getTimePartitioning()).isNull();
        assertThat(t1Spec.getClustering()).isNull();

        // Every load was awaited and all staged files were cleaned up afterwards.
        assertThat(harness.runner.awaited)
                .containsExactlyInAnyOrderElementsOf(harness.runner.loads.keySet());
        assertThat(harness.storage.getDeleted())
                .containsExactlyInAnyOrder(
                        "gs://bucket/prefix/a.avro",
                        "gs://bucket/prefix/b.avro",
                        "gs://bucket/prefix/z.avro");
    }

    @Test
    void jobIdsAreDeterministicAcrossRetries() throws IOException {
        List<FileLoadsCommittable> files = List.of(file(T1, "a", 10), file(T1, "b", 10));

        Harness first = Harness.plain();
        first.orchestrator.run(files);
        Harness second = Harness.plain();
        second.orchestrator.run(new ArrayList<>(files));

        assertThat(second.runner.loads.keySet()).isEqualTo(first.runner.loads.keySet());
        assertThat(first.runner.loads.keySet())
                .allSatisfy(id -> assertThat(id).startsWith("flink-bq-load-" + FLINK_JOB_ID));

        Harness different = Harness.plain();
        different.orchestrator.run(List.of(file(T1, "other", 10)));
        assertThat(different.runner.loads.keySet())
                .doesNotContainAnyElementsOf(first.runner.loads.keySet());
    }

    @Test
    void schemaUpdateOptionsAreWiredWhenEnabled() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder()
                                                .allowNewFields()
                                                .allowFieldRelaxation()
                                                .build()));

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec ->
                                assertThat(spec.getSchemaUpdateOptions())
                                        .containsExactlyInAnyOrder(
                                                JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                                                JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
    }

    @Test
    void schemaUpdateOptionsAreOmittedForNonAppendDispositions() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getSchemaUpdateOptions()).isEmpty();
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                        });
    }

    @Test
    void partitioningAndClusteringArePassedToDirectLoads() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .timePartitioning(
                                                        TableCreateOptions.TimePartitioningType.DAY,
                                                        "f1")
                                                .clusteredFields(List.of("f2"))
                                                .build()));

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getTimePartitioning())
                                    .isEqualTo(
                                            TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                                    .setField("f1")
                                                    .build());
                            assertThat(spec.getClustering().getFields()).containsExactly("f2");
                        });
    }

    @Test
    void partitioningIsOmittedWhenTheTableAlreadyExists() throws IOException {
        // Creation options apply at creation time only; a partitioning spec against an existing,
        // differently-laid-out table would fail the load job.
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .timePartitioning(
                                                        TableCreateOptions.TimePartitioningType.DAY,
                                                        "f1")
                                                .clusteredFields(List.of("f2"))
                                                .build()));
        harness.tableAdmin.tables.put(T1, SCHEMA);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getTimePartitioning()).isNull();
                            assertThat(spec.getClustering()).isNull();
                        });
    }

    @Test
    void oversizedTablesGoThroughTempTablesAndOneCopyJob() throws IOException {
        Harness harness = Harness.plain();
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", sixTiB));

        harness.orchestrator.run(files);

        // 6 TiB + 6 TiB exceeds the 11 TiB budget: one partition (and temp table) per file.
        assertThat(harness.runner.loads).hasSize(3);
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec -> {
                            assertThat(spec.getDestination().getDataset()).isEqualTo("d");
                            assertThat(spec.getDestination().getTable())
                                    .startsWith("tmp_" + FLINK_JOB_ID + "_");
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                            assertThat(spec.getCreateDisposition())
                                    .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
                            assertThat(spec.getSchemaUpdateOptions()).isEmpty();
                        });

        assertThat(harness.runner.copies).hasSize(1);
        CopyJobSpec copy = harness.runner.copies.values().iterator().next();
        assertThat(copy.getDestination()).isEqualTo(T1);
        assertThat(copy.getSourceTables()).hasSize(3);
        assertThat(copy.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);

        // The final table was created before the copy (CREATE_IF_NEEDED, missing table).
        assertThat(harness.tableAdmin.created).containsExactly(T1);
        // Temp tables were deleted and staged files cleaned up.
        assertThat(harness.runner.deletedTables)
                .containsExactlyInAnyOrderElementsOf(copy.getSourceTables());
        assertThat(harness.storage.getDeleted()).hasSize(3);
    }

    @Test
    void tempTablesUseConfiguredTempDataset() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .tempDataset("temp_ds")
                                .build(),
                        builder -> {});
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec ->
                                assertThat(spec.getDestination().getDataset())
                                        .isEqualTo("temp_ds"));
    }

    @Test
    void copyIntoMissingTableFailsUnderCreateNever() {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder -> builder.createDisposition(CreateDisposition.CREATE_NEVER));
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB));

        assertThatThrownBy(() -> harness.orchestrator.run(files))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        // Nothing is cleaned up on failure, so a Flink restart can retry deterministically.
        assertThat(harness.storage.getDeleted()).isEmpty();
        assertThat(harness.runner.deletedTables).isEmpty();
    }

    @Test
    void unionsFinalTableSchemaBeforeCopyWhenUpdatesEnabled() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        // The live table only has f1; the serializer schema adds f2.
        harness.tableAdmin.tables.put(
                T1,
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("f1")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build());
        harness.tableAdmin.updateRacesToLose = 1; // First update loses a race and is retried.
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.tableAdmin.tables.get(T1).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f1", "f2");
        // Temp tables are loaded with the reconciled final-table schema so the copy matches.
        assertThat(harness.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getSchema().getFields()).hasSize(2));
        assertThat(harness.runner.copies).hasSize(1);
    }

    @Test
    void truncatingCopySkipsSchemaReconciliation() throws IOException {
        // WRITE_TRUNCATE replaces the final table's schema wholesale; unioning first would only
        // waste update races (or fail on combinations the truncate would simply replace).
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        harness.tableAdmin.tables.put(
                T1,
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("f1")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build());
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.tableAdmin.schemaUpdates).isEmpty();
        assertThat(harness.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getSchema().getFields()).hasSize(2));
        assertThat(harness.runner.copies).hasSize(1);
    }

    @Test
    void loadFailureLeavesStagingFilesInPlace() {
        Harness harness = Harness.plain();
        harness.runner.failAllAwaits = true;

        assertThatThrownBy(() -> harness.orchestrator.run(List.of(file(T1, "a", 10))))
                .isInstanceOf(IOException.class);

        assertThat(harness.storage.getDeleted()).isEmpty();
        assertThat(harness.runner.deletedTables).isEmpty();
    }

    @Test
    void partitionRespectsFileCountAndByteLimits() {
        List<FileLoadsCommittable> manyFiles = new ArrayList<>();
        for (int i = 0; i < LoadJobOrchestrator.MAX_FILES_PER_JOB + 1; i++) {
            manyFiles.add(file(T1, String.format("f%05d", i), 1));
        }
        List<List<FileLoadsCommittable>> byCount = LoadJobOrchestrator.partition(manyFiles);
        assertThat(byCount).hasSize(2);
        assertThat(byCount.get(0)).hasSize(LoadJobOrchestrator.MAX_FILES_PER_JOB);
        assertThat(byCount.get(1)).hasSize(1);

        long sixTiB = 6L << 40;
        List<List<FileLoadsCommittable>> byBytes =
                LoadJobOrchestrator.partition(
                        List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", 1)));
        assertThat(byBytes).hasSize(2);
        assertThat(byBytes.get(0)).hasSize(1);
        assertThat(byBytes.get(1)).hasSize(2);
    }

    @Test
    void emptyCommittablesDoNothing() throws IOException {
        Harness harness = Harness.plain();

        harness.orchestrator.run(List.of());

        assertThat(harness.runner.loads).isEmpty();
        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.storage.getDeleted()).isEmpty();
    }
}
