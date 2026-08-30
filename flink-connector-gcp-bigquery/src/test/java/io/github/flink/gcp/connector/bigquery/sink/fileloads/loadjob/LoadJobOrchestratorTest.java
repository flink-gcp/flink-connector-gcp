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
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

/** Tests for {@link LoadJobOrchestrator} against recording fakes. */
class LoadJobOrchestratorTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");
    private static final TableDestination T2 = TableDestination.of("p", "d", "t2");
    private static final TableDestination T3 = TableDestination.of("p", "d", "t3");
    private static final TableDestination T4 = TableDestination.of("p", "d", "t4");

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

    private static final TableSchema LIVE_F1_ONLY =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE))
                    .build();

    private static final TableSchema SCHEMA_WITH_F2_REQUIRED =
            SCHEMA.toBuilder()
                    .setFields(
                            1,
                            SCHEMA.getFields(1).toBuilder().setMode(TableFieldSchema.Mode.REQUIRED))
                    .build();

    /** A serializer only used for its schema. */
    private static final class SchemaOnlySerializer
            extends BigQueryProtoSerializationSchema<Object> {
        private static final long serialVersionUID = 1L;

        private final TableSchema schema;

        SchemaOnlySerializer(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return schema;
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
        return file(destination, name, bytes, StagingFormat.AVRO);
    }

    private static FileLoadsCommittable file(
            TableDestination destination, String name, long bytes, StagingFormat format) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID,
                destination,
                "gs://bucket/prefix/" + name + format.getExtension(),
                bytes,
                10,
                format);
    }

    /** Everything one orchestration run touches. */
    private static final class Harness {
        private final FakeLoadJobRunner runner = new FakeLoadJobRunner();
        private final FakeTableAdmin tableAdmin = new FakeTableAdmin();
        private final InMemoryStagingStorage storage = new InMemoryStagingStorage();
        private final SimpleCounter loadJobsSubmitted = new SimpleCounter();
        private final LoadJobOrchestrator orchestrator;

        Harness(FileLoadsOptions options, Consumer<BigQuerySinkBuilder<Object>> customizer) {
            this(options, customizer, null);
        }

        Harness(
                FileLoadsOptions options,
                Consumer<BigQuerySinkBuilder<Object>> customizer,
                Long checkpointId) {
            this(options, customizer, checkpointId, SCHEMA);
        }

        Harness(
                FileLoadsOptions options,
                Consumer<BigQuerySinkBuilder<Object>> customizer,
                Long checkpointId,
                TableSchema serializerSchema) {
            this(options, customizer, checkpointId, serializerSchema, null);
        }

        Harness(
                FileLoadsOptions options,
                Consumer<BigQuerySinkBuilder<Object>> customizer,
                Long checkpointId,
                TableSchema serializerSchema,
                Limits limits) {
            BigQuerySinkBuilder<Object> builder =
                    BigQuerySink.builder()
                            .writeMethod(WriteMethod.FILE_LOADS)
                            .table(T1)
                            .serializer(new SchemaOnlySerializer(serializerSchema))
                            .fileLoadsOptions(options);
            customizer.accept(builder);
            BigQuerySinkConfig<Object> config =
                    ((BigQueryFileLoadsSink<Object>) builder.build()).getConfig();
            this.orchestrator =
                    limits == null
                            ? new LoadJobOrchestrator(
                                    config,
                                    options,
                                    runner,
                                    tableAdmin,
                                    storage,
                                    FLINK_JOB_ID,
                                    checkpointId,
                                    loadJobsSubmitted)
                            : new LoadJobOrchestrator(
                                    config,
                                    options,
                                    runner,
                                    tableAdmin,
                                    storage,
                                    FLINK_JOB_ID,
                                    checkpointId,
                                    loadJobsSubmitted,
                                    limits);
        }

        static Harness plain() {
            return new Harness(
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                    builder -> {});
        }

        static Harness streaming(long checkpointId) {
            return new Harness(
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                    builder -> {},
                    checkpointId);
        }

        static Harness withLimits(Limits limits) {
            return new Harness(
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                    builder -> {},
                    7L,
                    SCHEMA,
                    limits);
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

        // The missing destination tables were created before the loads were submitted.
        assertThat(harness.tableAdmin.created).containsExactlyInAnyOrder(T1, T2);

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
    void writeAppendKeepsTwoFormatsAsTwoDirectLoadJobs() throws IOException {
        // The transitional commit: committables written before the staging format changed are
        // still in committer state alongside new ones. A load job carries exactly one format, so
        // append cannot mix them in a single job here — and the alternative, refusing the mix,
        // would wedge the restart that produced it.
        Harness harness = Harness.plain();

        harness.orchestrator.run(
                List.of(
                        file(T1, "a", 10, StagingFormat.AVRO),
                        file(T1, "b", 10, StagingFormat.AVRO),
                        file(T1, "c", 10, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getFormat)
                .containsExactlyInAnyOrder(StagingFormat.AVRO, StagingFormat.PARQUET);
        // Each job carries only its own format's files, which is the property that makes the
        // split correct rather than merely two jobs.
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec ->
                                assertThat(spec.getSourceUris())
                                        .allSatisfy(
                                                uri ->
                                                        assertThat(uri)
                                                                .endsWith(
                                                                        spec.getFormat()
                                                                                .getExtension())));
        // Deterministic ids still discriminate without the format being in them: the id hashes
        // the source URI list, and the two sets are disjoint.
        assertThat(harness.runner.loads.keySet()).doesNotHaveDuplicates();
    }

    @Test
    void writeTruncateCombinesTwoFormatsBeforeReplacingTheDestination() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .build(),
                        builder -> {});

        harness.orchestrator.run(
                List.of(
                        file(T1, "avro", 10, StagingFormat.AVRO),
                        file(T1, "parquet", 10, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads.values())
                .hasSize(2)
                .allSatisfy(
                        load -> {
                            assertThat(load.getDestination()).isNotEqualTo(T1);
                            assertThat(load.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                        });
        assertThat(harness.runner.copies.values())
                .singleElement()
                .satisfies(
                        copy -> {
                            assertThat(copy.getDestination()).isEqualTo(T1);
                            assertThat(copy.getSourceTables()).hasSize(2);
                            assertThat(copy.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                        });
        assertThat(harness.runner.queries).isEmpty();
    }

    @Test
    void oneDestinationInOneFormatIsStillOneLoadJob() throws IOException {
        // The control the test above needs: grouping on the format must not split what used to be
        // a single job, which is every commit that is not the transitional one.
        Harness harness = Harness.plain();

        harness.orchestrator.run(
                List.of(
                        file(T1, "a", 10, StagingFormat.AVRO),
                        file(T1, "b", 10, StagingFormat.AVRO)));

        assertThat(harness.runner.loads).hasSize(1);
    }

    @Test
    void jvmFatalAwaitFailureTakesPriorityOverAnEarlierOrdinaryFailure() throws IOException {
        List<FileLoadsCommittable> files =
                List.of(
                        file(T1, "avro", 10, StagingFormat.AVRO),
                        file(T1, "parquet", 10, StagingFormat.PARQUET));
        Harness successful = Harness.plain();
        successful.orchestrator.run(files);
        List<String> jobIds = new ArrayList<>(successful.runner.loads.keySet());
        Harness failing = Harness.plain();
        failing.runner.failOnAwaitWith.put(jobIds.get(0), new IOException("ordinary-failure"));
        failing.runner.failOnAwaitWith.put(jobIds.get(1), new OutOfMemoryError("fatal-failure"));

        assertThatThrownBy(() -> failing.orchestrator.run(files))
                .isInstanceOf(OutOfMemoryError.class)
                .hasMessage("fatal-failure")
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed())
                                        .extracting(Throwable::getMessage)
                                        .containsExactly("ordinary-failure"));
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
    void writeTruncateDataDirectLoadPreservesTheLiveSchemaWhenUpdatesAreDisabled()
            throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                                .build(),
                        builder -> {});
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        load -> {
                            assertThat(load.getDestination()).isEqualTo(T1);
                            assertThat(load.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE_DATA);
                            assertThat(load.getSchema().getFields())
                                    .extracting(Field::getName)
                                    .containsExactly("f1");
                            assertThat(load.getSchemaUpdateOptions()).isEmpty();
                        });
        assertThat(harness.tableAdmin.schemaUpdates).isEmpty();
        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.runner.queries).isEmpty();
    }

    @Test
    void writeTruncateDataDirectLoadCarriesExplicitSchemaUpdates() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                                .build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder()
                                                .allowNewFields()
                                                .allowFieldRelaxation()
                                                .build()));
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        load ->
                                assertThat(load.getSchemaUpdateOptions())
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
    void missingTableIsCreatedWithConfiguredCreateOptionsBeforeADirectLoad() throws IOException {
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

        assertThat(harness.tableAdmin.created).containsExactly(T1);
        TableCreateOptions options = harness.tableAdmin.createOptions.get(T1);
        assertThat(options.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(options.getTimePartitioningField()).isEqualTo("f1");
        assertThat(options.getClusteredFields()).containsExactly("f2");
    }

    @Test
    void missingFinalTableIsCreatedAndLoadedWithAdditionalFields() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.additionalFields(
                                        AdditionalFields.builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "source",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                element -> "connector"))
                                                .build()));

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.tables.get(T1).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f1", "f2", "source");
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec ->
                                assertThat(spec.getSchema().getFields())
                                        .extracting(Field::getName)
                                        .containsExactly("f1", "f2", "source"));
    }

    @Test
    void existingTableIsNotRecreatedByADirectLoad() throws IOException {
        // Creation options apply at creation time only; the reconciliation must not try to
        // re-create (or re-lay-out) a table that already exists.
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

        assertThat(harness.runner.loads).hasSize(1);
        assertThat(harness.tableAdmin.created).isEmpty();
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
                            assertThat(spec.getDestination().getTable()).doesNotContain("_c");
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                            assertThat(spec.getCreateDisposition())
                                    .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
                            assertThat(spec.getSchemaUpdateOptions()).isEmpty();
                        });

        assertThat(harness.runner.copies).hasSize(1);
        // The counter names load jobs, so the copy job that follows them is not counted.
        assertThat(harness.loadJobsSubmitted.getCount())
                .isEqualTo(harness.runner.loads.size())
                .isEqualTo(3);
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
    void writeTruncateDataOverflowUsesAnAggregateCopyAndOneTerminalQuery() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 6L << 40), file(T1, "b", 6L << 40));
        Harness harness =
                new Harness(
                        options,
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(files);

        assertThat(harness.runner.loads.values())
                .hasSize(2)
                .allSatisfy(
                        load ->
                                assertThat(load.getWriteDisposition())
                                        .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE));
        CopyJobSpec aggregateCopy =
                harness.runner.copies.values().stream().findFirst().orElseThrow();
        assertThat(aggregateCopy.getDestination()).isNotEqualTo(T1);
        assertThat(aggregateCopy.getDestination().getTable()).endsWith("_aggregate");
        assertThat(aggregateCopy.getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        assertThat(aggregateCopy.getWriteDisposition())
                .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
        assertThat(harness.runner.queries.values())
                .singleElement()
                .satisfies(
                        query -> {
                            assertThat(query.getSourceTable())
                                    .isEqualTo(aggregateCopy.getDestination());
                            assertThat(query.getDestination()).isEqualTo(T1);
                            assertThat(query.getSchemaUpdateOptions())
                                    .containsExactly(
                                            JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
                        });
        String copyJobId = harness.runner.copies.keySet().iterator().next();
        String queryJobId = harness.runner.queries.keySet().iterator().next();
        assertThat(queryJobId).startsWith("flink-bq-query-" + FLINK_JOB_ID);
        assertThat(harness.runner.events.indexOf("await:" + copyJobId))
                .isLessThan(harness.runner.events.indexOf("submit-query:" + queryJobId));
        assertThat(harness.runner.deletedTables)
                .contains(aggregateCopy.getDestination())
                .hasSize(3);

        Harness retry =
                new Harness(
                        options,
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        retry.tableAdmin.tables.put(T1, LIVE_F1_ONLY);
        retry.orchestrator.run(files);
        assertThat(retry.runner.loads.keySet()).isEqualTo(harness.runner.loads.keySet());
        assertThat(retry.runner.copies.keySet()).isEqualTo(harness.runner.copies.keySet());
        assertThat(retry.runner.queries.keySet()).isEqualTo(harness.runner.queries.keySet());
    }

    @Test
    void writeTruncateDataCombinesTwoSmallFormatsThroughTheTerminalQuery() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                                .build(),
                        builder -> {});

        harness.orchestrator.run(
                List.of(
                        file(T1, "avro", 10, StagingFormat.AVRO),
                        file(T1, "parquet", 10, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.runner.copies).hasSize(1);
        assertThat(harness.runner.queries).hasSize(1);
        assertThat(harness.runner.loads.values())
                .allSatisfy(load -> assertThat(load.getDestination()).isNotEqualTo(T1));
    }

    @Test
    void writeTruncateDataTerminalQueryFailureKeepsEveryRetryInput() throws IOException {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .writeDisposition(WriteDisposition.WRITE_TRUNCATE_DATA)
                        .build();
        List<FileLoadsCommittable> files =
                List.of(file(T1, "a", 6L << 40), file(T1, "b", 6L << 40));
        Harness successful = new Harness(options, builder -> {});
        successful.orchestrator.run(files);
        String queryJobId = successful.runner.queries.keySet().iterator().next();
        Harness retry = new Harness(options, builder -> {});
        retry.runner.failOnAwait.add(queryJobId);

        assertThatThrownBy(() -> retry.orchestrator.run(files)).isInstanceOf(IOException.class);

        assertThat(retry.runner.queries).containsOnlyKeys(queryJobId);
        assertThat(retry.runner.deletedTables).isEmpty();
        assertThat(retry.storage.getDeleted()).isEmpty();
    }

    @Test
    void streamingTempTablesUseConfiguredTempDataset() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .tempDataset("temp_ds")
                                .build(),
                        builder -> {},
                        7L);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.runner.loads.values())
                .hasSize(2)
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
                                        SchemaUpdateOptions.builder().allowNewFields().build()),
                        null,
                        SCHEMA_WITH_F2_REQUIRED);
        // The live table only has f1; the serializer schema adds f2 as REQUIRED.
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);
        harness.tableAdmin.updateRacesToLose = 1; // First update loses a race and is retried.
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.tableAdmin.tables.get(T1).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f1", "f2");
        // Temp tables are loaded with the reconciled final-table schema so the copy matches —
        // reconciled, not the serializer's: the REQUIRED addition arrives demoted to NULLABLE.
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec -> {
                            assertThat(spec.getSchema().getFields())
                                    .extracting(Field::getName)
                                    .containsExactly("f1", "f2");
                            assertThat(spec.getSchema().getFields().get("f2").getMode())
                                    .isEqualTo(Field.Mode.NULLABLE);
                        });
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
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB)));

        assertThat(harness.tableAdmin.schemaUpdates).isEmpty();
        assertThat(harness.runner.loads.values())
                .allSatisfy(spec -> assertThat(spec.getSchema().getFields()).hasSize(2));
        assertThat(harness.runner.copies).hasSize(1);
    }

    @Test
    void writeEmptyDirectLoadStillReconcilesWhenUpdatesEnabled() throws IOException {
        // WRITE_EMPTY is not WRITE_TRUNCATE: the live table's schema survives the load, so the
        // union runs on this path too — while the native schema update options stay append-only.
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_EMPTY)
                                .build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getSchema().getFields())
                                    .extracting(Field::getName)
                                    .containsExactly("f1", "f2");
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_EMPTY);
                            assertThat(spec.getSchemaUpdateOptions()).isEmpty();
                        });
    }

    @Test
    void updatesDisabledWarnsOncePerDestinationAboutUnappliedDifferences() throws IOException {
        // The warn is the loud part of the silent drop: with updates disabled the live schema
        // wins, and staged data for the serializer-only column f2 is ignored by BigQuery.
        Harness harness = Harness.streaming(7);
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);
        long sixTiB = 6L << 40;

        try (LogCapture capture = LogCapture.of(FileLoadsSchemaReconciler.class)) {
            harness.orchestrator.run(
                    List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", sixTiB)));

            // Three temp-table loads, one warning (the reconciliation is memoized), naming the
            // field.
            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains("live schema")
                    .contains("f2")
                    .contains(T1.toString());
        }
    }

    @Test
    void loadFailureLeavesStagingFilesInPlace() {
        Harness harness = Harness.plain();
        harness.runner.failAllAwaits = true;

        assertThatThrownBy(() -> harness.orchestrator.run(List.of(file(T1, "a", 10))))
                .isInstanceOf(IOException.class);

        assertThat(harness.storage.getDeleted()).isEmpty();
        assertThat(harness.runner.deletedTables).isEmpty();
        // The pre-load creation is not rolled back: a failed load can leave an empty table.
        assertThat(harness.tableAdmin.created).containsExactly(T1);
    }

    @Test
    void partitionRespectsFileCountAndByteLimits() {
        List<FileLoadsCommittable> manyFiles = new ArrayList<>();
        for (int i = 0; i < CommitPlanner.MAX_FILES_PER_JOB + 1; i++) {
            manyFiles.add(file(T1, String.format("f%05d", i), 1));
        }
        List<List<FileLoadsCommittable>> byCount = CommitPlanner.partition(manyFiles);
        assertThat(byCount).hasSize(2);
        assertThat(byCount.get(0)).hasSize(CommitPlanner.MAX_FILES_PER_JOB);
        assertThat(byCount.get(1)).hasSize(1);

        long sixTiB = 6L << 40;
        List<List<FileLoadsCommittable>> byBytes =
                CommitPlanner.partition(
                        List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", 1)));
        assertThat(byBytes).hasSize(2);
        assertThat(byBytes.get(0)).hasSize(1);
        assertThat(byBytes.get(1)).hasSize(2);
    }

    @Test
    void streamingJobIdsCarryTheCheckpointSegment() throws IOException {
        Harness harness = Harness.streaming(42);

        harness.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10)));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id -> assertThat(id).startsWith("flink-bq-load-" + FLINK_JOB_ID + "-c42-"));

        // The hash is the idempotency key and does not include the checkpoint id: the same files
        // under the same checkpoint reproduce the same job id across retries.
        Harness retry = Harness.streaming(42);
        retry.orchestrator.run(List.of(file(T1, "a", 10), file(T1, "b", 10)));
        assertThat(retry.runner.loads.keySet()).isEqualTo(harness.runner.loads.keySet());
    }

    @Test
    void batchJobIdsCarryNoCheckpointSegment() throws IOException {
        Harness harness = Harness.plain();

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id ->
                                assertThat(id)
                                        .matches(
                                                "flink-bq-load-" + FLINK_JOB_ID + "-[0-9a-f]{16}"));
    }

    @Test
    void streamingOverflowGoesThroughCheckpointScopedTempTablesAndOneCopyJob() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()),
                        7L);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(
                List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", sixTiB)));

        assertThat(harness.runner.loads).hasSize(3);
        assertThat(harness.runner.loads.keySet())
                .allSatisfy(id -> assertThat(id).contains("-c7-").containsPattern("-p\\d$"));
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec -> {
                            assertThat(spec.getDestination().getDataset()).isEqualTo("d");
                            assertThat(spec.getDestination().getTable())
                                    .matches("tmp_" + FLINK_JOB_ID + "_[0-9a-f]{12}_c7_p[0-2]");
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
                            assertThat(spec.getCreateDisposition())
                                    .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
                            assertThat(spec.getSchemaUpdateOptions()).isEmpty();
                        });
        assertThat(harness.loadJobsSubmitted.getCount()).isEqualTo(3);
        assertThat(harness.runner.copies).hasSize(1);
        assertThat(harness.runner.copies.keySet())
                .singleElement()
                .satisfies(id -> assertThat(id).contains("-c7-"));
        CopyJobSpec copy = harness.runner.copies.values().iterator().next();
        assertThat(copy.getDestination()).isEqualTo(T1);
        assertThat(copy.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
        assertThat(copy.getSourceTables())
                .containsExactlyElementsOf(
                        harness.runner.loads.values().stream()
                                .map(LoadJobSpec::getDestination)
                                .toList());
        // The missing table is created once, not once per partition (memoized reconciliation).
        assertThat(harness.tableAdmin.created).containsExactly(T1);
        assertThat(harness.runner.deletedTables).containsExactlyElementsOf(copy.getSourceTables());
        assertThat(harness.storage.getDeleted()).hasSize(3);
    }

    @Test
    void streamingTempTableNamesAreDeterministicWithinAUniqueCheckpoint() throws IOException {
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB));
        Harness first = Harness.streaming(7);
        Harness retry = Harness.streaming(7);
        Harness nextCheckpoint = Harness.streaming(8);

        first.orchestrator.run(files);
        retry.orchestrator.run(files);
        nextCheckpoint.orchestrator.run(files);

        List<TableDestination> firstNames =
                first.runner.loads.values().stream().map(LoadJobSpec::getDestination).toList();
        assertThat(retry.runner.loads.values())
                .extracting(LoadJobSpec::getDestination)
                .containsExactlyElementsOf(firstNames);
        assertThat(nextCheckpoint.runner.loads.values())
                .extracting(LoadJobSpec::getDestination)
                .doesNotContainAnyElementsOf(firstNames)
                .allSatisfy(table -> assertThat(table.getTable()).contains("_c8_"));
    }

    @Test
    void streamingOverflowCombinesFormatsInOneCopyJob() throws IOException {
        Harness harness = Harness.streaming(7);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(
                List.of(
                        file(T1, "avro-a", sixTiB, StagingFormat.AVRO),
                        file(T1, "avro-b", sixTiB, StagingFormat.AVRO),
                        file(T1, "parquet-a", sixTiB, StagingFormat.PARQUET),
                        file(T1, "parquet-b", sixTiB, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getDestination)
                .hasSize(4)
                .doesNotHaveDuplicates();
        assertThat(harness.runner.copies).hasSize(1);
        assertThat(harness.runner.copies.values())
                .singleElement()
                .satisfies(
                        copy -> {
                            assertThat(copy.getDestination()).isEqualTo(T1);
                            assertThat(copy.getSourceTables()).hasSize(4);
                        });
    }

    @Test
    void oneOverflowingFormatMovesEveryFormatForTheDestinationToTheCopy() throws IOException {
        Harness harness = Harness.streaming(7);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(
                List.of(
                        file(T1, "avro", 10, StagingFormat.AVRO),
                        file(T1, "parquet-a", sixTiB, StagingFormat.PARQUET),
                        file(T1, "parquet-b", sixTiB, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads.values())
                .hasSize(3)
                .extracting(LoadJobSpec::getDestination)
                .doesNotContain(T1)
                .doesNotHaveDuplicates();
        assertThat(harness.runner.copies.values())
                .singleElement()
                .satisfies(copy -> assertThat(copy.getSourceTables()).hasSize(3));
    }

    @Test
    void batchOverflowCombinesFormatsWithoutChangingSingleFormatNames() throws IOException {
        Harness harness = Harness.plain();
        long sixTiB = 6L << 40;

        harness.orchestrator.run(
                List.of(
                        file(T1, "avro-a", sixTiB, StagingFormat.AVRO),
                        file(T1, "avro-b", sixTiB, StagingFormat.AVRO),
                        file(T1, "parquet-a", sixTiB, StagingFormat.PARQUET),
                        file(T1, "parquet-b", sixTiB, StagingFormat.PARQUET)));

        assertThat(harness.runner.loads.values())
                .extracting(LoadJobSpec::getDestination)
                .hasSize(4)
                .doesNotHaveDuplicates()
                .allSatisfy(table -> assertThat(table.getTable()).doesNotContain("_c"));
        assertThat(harness.runner.copies.values())
                .singleElement()
                .satisfies(copy -> assertThat(copy.getSourceTables()).hasSize(4));
    }

    @Test
    void firstHierarchyCaseCopiesTwelveHundredSourcesAndCarriesTheLast() throws IOException {
        Harness harness = Harness.streaming(7);
        List<FileLoadsCommittable> files = new ArrayList<>(Limits.MAX_SOURCE_TABLES_PER_COPY + 1);
        files.add(file(T1, "avro", 10, StagingFormat.AVRO));
        IntStream.rangeClosed(1, Limits.MAX_SOURCE_TABLES_PER_COPY)
                .mapToObj(i -> file(T1, "parquet-" + i, 6L << 40, StagingFormat.PARQUET))
                .forEach(files::add);

        harness.orchestrator.run(files);

        assertThat(harness.runner.loads).hasSize(Limits.MAX_SOURCE_TABLES_PER_COPY + 1);
        assertThat(harness.runner.copies).hasSize(2);
        List<CopyJobSpec> copies = new ArrayList<>(harness.runner.copies.values());
        CopyJobSpec intermediate = copies.get(0);
        assertThat(intermediate.getSourceTables()).hasSize(Limits.MAX_SOURCE_TABLES_PER_COPY);
        assertThat(intermediate.getDestination().getTable()).contains("_c7_l1_g0");
        assertThat(intermediate.getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        assertThat(intermediate.getWriteDisposition())
                .isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);

        CopyJobSpec finalCopy = copies.get(1);
        assertThat(finalCopy.getDestination()).isEqualTo(T1);
        assertThat(finalCopy.getSourceTables())
                .containsExactly(
                        intermediate.getDestination(),
                        harness.runner.loads.values().stream()
                                .map(LoadJobSpec::getDestination)
                                .reduce((first, second) -> second)
                                .orElseThrow());
        assertThat(finalCopy.getCreateDisposition())
                .isEqualTo(JobInfo.CreateDisposition.CREATE_NEVER);
        assertThat(finalCopy.getWriteDisposition())
                .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
        assertThat(harness.runner.deletedTables)
                .hasSize(Limits.MAX_SOURCE_TABLES_PER_COPY + 2)
                .contains(intermediate.getDestination());
        assertThat(harness.storage.getDeleted()).hasSize(files.size());

        Harness retry = Harness.streaming(7);
        retry.orchestrator.run(new ArrayList<>(files));
        assertThat(retry.runner.loads.keySet()).isEqualTo(harness.runner.loads.keySet());
        assertThat(retry.runner.copies.keySet()).isEqualTo(harness.runner.copies.keySet());
        assertThat(retry.runner.copies.values())
                .extracting(CopyJobSpec::getSourceTables, CopyJobSpec::getDestination)
                .containsExactlyElementsOf(
                        harness.runner.copies.values().stream()
                                .map(copy -> tuple(copy.getSourceTables(), copy.getDestination()))
                                .toList());
    }

    @Test
    void copySourceTableLimitIsAcceptedAcrossFormats() throws IOException {
        Harness harness = Harness.streaming(7);
        List<FileLoadsCommittable> files = new ArrayList<>(Limits.MAX_SOURCE_TABLES_PER_COPY);
        files.add(file(T1, "avro", 10, StagingFormat.AVRO));
        IntStream.range(1, Limits.MAX_SOURCE_TABLES_PER_COPY)
                .mapToObj(i -> file(T1, "parquet-" + i, 6L << 40, StagingFormat.PARQUET))
                .forEach(files::add);

        harness.orchestrator.run(files);

        assertThat(harness.runner.loads).hasSize(Limits.MAX_SOURCE_TABLES_PER_COPY);
        assertThat(harness.runner.copies.values())
                .singleElement()
                .satisfies(
                        copy ->
                                assertThat(copy.getSourceTables())
                                        .hasSize(Limits.MAX_SOURCE_TABLES_PER_COPY));
    }

    @Test
    void reducedLimitsExerciseMultipleLevelsAndSubmissionWaves() throws IOException {
        Harness harness = Harness.withLimits(new Limits(3, 100, 100, 2));
        List<FileLoadsCommittable> files =
                IntStream.rangeClosed(1, 10)
                        .mapToObj(i -> file(T1, "part-" + i, 6L << 40))
                        .toList();

        harness.orchestrator.run(files);

        List<Map.Entry<String, CopyJobSpec>> copies =
                new ArrayList<>(harness.runner.copies.entrySet());
        assertThat(copies).hasSize(5);
        assertThat(copies.subList(0, 3))
                .allSatisfy(
                        copy -> {
                            assertThat(copy.getValue().getSourceTables()).hasSize(3);
                            assertThat(copy.getValue().getDestination().getTable())
                                    .contains("_l1_");
                        });
        assertThat(copies.get(3).getValue().getSourceTables()).hasSize(3);
        assertThat(copies.get(3).getValue().getDestination().getTable()).contains("_l2_g0");
        assertThat(copies.get(4).getValue().getSourceTables()).hasSize(2);
        assertThat(copies.get(4).getValue().getDestination()).isEqualTo(T1);

        String lastLevelOne = copies.get(2).getKey();
        String levelTwo = copies.get(3).getKey();
        String finalCopy = copies.get(4).getKey();
        assertThat(harness.runner.events.indexOf("await:" + lastLevelOne))
                .isLessThan(harness.runner.events.indexOf("submit-copy:" + levelTwo));
        assertThat(harness.runner.events.indexOf("await:" + levelTwo))
                .isLessThan(harness.runner.events.indexOf("submit-copy:" + finalCopy));

        List<String> loadEvents =
                harness.runner.events.stream().filter(event -> event.contains("-load-")).toList();
        assertThat(loadEvents.subList(0, 4))
                .satisfiesExactly(
                        event -> assertThat(event).startsWith("submit-load:"),
                        event -> assertThat(event).startsWith("submit-load:"),
                        event -> assertThat(event).startsWith("await:"),
                        event -> assertThat(event).startsWith("await:"));
    }

    @Test
    void submissionWaveLimitIsSharedAcrossConcurrentDestinations() throws Exception {
        int maximumPendingJobs = 2;
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        AtomicInteger pending = new AtomicInteger();
        AtomicInteger maximumPending = new AtomicInteger();
        CountDownLatch releaseAwaits = new CountDownLatch(1);
        CyclicBarrier schemaReadsReady = new CyclicBarrier(2);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> {
                            FakeTableAdmin tableAdmin = new FakeTableAdmin();
                            tableAdmin.firstSchemaReadBarrier = schemaReadsReady;
                            return new DestinationCommitExecutor.Worker(
                                    new PendingJobRunner(pending, maximumPending, releaseAwaits),
                                    tableAdmin);
                        },
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        storage,
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, maximumPendingJobs),
                        executor);

        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file(T1, "t1-avro", 10, StagingFormat.AVRO),
                                                file(T1, "t1-parquet", 10, StagingFormat.PARQUET),
                                                file(T2, "t2-avro", 10, StagingFormat.AVRO),
                                                file(T2, "t2-parquet", 10, StagingFormat.PARQUET)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitPendingJobs(pending, maximumPendingJobs);

            assertThat(maximumPending).hasValue(maximumPendingJobs);
            releaseAwaits.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isNull();
        } finally {
            releaseAwaits.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }

        assertThat(pending).hasValue(0);
        assertThat(maximumPending).hasValue(maximumPendingJobs);
    }

    @Test
    void submitsTheWholeCrossDestinationWaveBeforeAwaiting() throws Exception {
        int jobsInWave = 4;
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        AtomicInteger pending = new AtomicInteger();
        AtomicInteger maximumPending = new AtomicInteger();
        CountDownLatch releaseAwaits = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new PendingJobRunner(
                                                pending, maximumPending, releaseAwaits),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, jobsInWave),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file(T1, "t1", 10),
                                                file(T2, "t2", 10),
                                                file(T3, "t3", 10),
                                                file(T4, "t4", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitPendingJobs(pending, jobsInWave);

            assertThat(maximumPending).hasValue(jobsInWave);
            assertThat(coordinator.isAlive()).isTrue();
            releaseAwaits.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get()).isNull();
        } finally {
            releaseAwaits.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }

        assertThat(pending).hasValue(0);
    }

    @Test
    void submissionFailureStillAwaitsEverySuccessfullySubmittedJob() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        AtomicInteger submissions = new AtomicInteger();
        CountDownLatch successfulSubmissionsReady = new CountDownLatch(2);
        CountDownLatch awaitStarted = new CountDownLatch(2);
        CountDownLatch releaseAwait = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new PartiallyFailingSubmissionRunner(
                                                submissions,
                                                successfulSubmissionsReady,
                                                awaitStarted,
                                                releaseAwait),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 4),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file(T1, "t1-avro", 10, StagingFormat.AVRO),
                                                file(T2, "t2-avro", 10, StagingFormat.AVRO),
                                                file(T2, "t2-parquet", 10, StagingFormat.PARQUET)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(awaitStarted);

            assertThat(submissions).hasValue(3);
            assertThat(coordinator.isAlive()).isTrue();
            releaseAwait.countDown();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("scripted submission failure");
        } finally {
            releaseAwait.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void reportsConcurrentFatalSubmissionFailuresOnceInPlanOrder() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        OutOfMemoryError first = new OutOfMemoryError("first submission fatal");
        OutOfMemoryError second = new OutOfMemoryError("second submission fatal");
        Map<TableDestination, OutOfMemoryError> failures = Map.of(T1, first, T2, second);
        CountDownLatch submissionsReady = new CountDownLatch(2);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new ConcurrentFatalSubmissionRunner(
                                                failures, submissionsReady),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);

        Throwable failure;
        try {
            orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
            throw new AssertionError("The scripted submission failures should fail the commit");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failure).isSameAs(first);
        assertThat(failure.getSuppressed()).containsExactly(second);
    }

    @Test
    void ordersARecordedSubmissionFailureBeforeALaterWorkerCreationFailure() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException submissionFailure = new IOException("first destination submission failure");
        IOException workerFailure = new IOException("second destination worker failure");
        CountDownLatch submissionStarted = new CountDownLatch(1);
        CountDownLatch workerFailureReady = new CountDownLatch(1);
        ExecutorService workers = new PerTaskThreadExecutor("ordered-wave-worker");
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> {
                            // Threads 1 and 2 reconcile the two destinations. The submission wave
                            // then assigns plan index 0 to thread 3 and plan index 1 to thread 4.
                            if (Thread.currentThread().getName().endsWith("-4")) {
                                awaitLatch(submissionStarted);
                                workerFailureReady.countDown();
                                throw workerFailure;
                            }
                            return new DestinationCommitExecutor.Worker(
                                    new SubmissionFailingRunner(
                                            submissionFailure,
                                            submissionStarted,
                                            workerFailureReady),
                                    new FakeTableAdmin());
                        },
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()),
                        workers);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);

        Throwable failure;
        try {
            orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
            throw new AssertionError("The scripted worker failures should fail the commit");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failure).isSameAs(submissionFailure);
        assertThat(failure.getSuppressed()).containsExactly(workerFailure);
    }

    @Test
    void drainsSubmittedJobsSeriallyAfterAnAwaitWorkerCannotBeCreated() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException workerFailure = new IOException("await worker creation failure");
        ConcurrentMap<String, TableDestination> submitted = new ConcurrentHashMap<>();
        ExecutorService workers = new PerTaskThreadExecutor("await-worker-failure");
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> {
                            // Threads 1-2 reconcile and 3-4 submit. Thread 5 receives the first
                            // await batch; the single-worker fallback drains it after this worker
                            // cannot be created.
                            if (Thread.currentThread().getName().endsWith("-5")) {
                                throw workerFailure;
                            }
                            return new DestinationCommitExecutor.Worker(
                                    new FailingAwaitRunner(submitted), new FakeTableAdmin());
                        },
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()),
                        workers);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);

        Throwable failure;
        try {
            orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
            throw new AssertionError("The scripted worker failure should fail the commit");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failureMessages(failure)).contains(workerFailure.getMessage());
        assertThat(submitted).isEmpty();
    }

    @Test
    void awaitInterruptionPrecedesAnEarlierWorkerCreationFailure() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        IOException workerFailure = new IOException("submission-phase worker failure");
        CountDownLatch submissionStarted = new CountDownLatch(1);
        CountDownLatch workerFailureReady = new CountDownLatch(1);
        CountDownLatch awaitStarted = new CountDownLatch(1);
        CountDownLatch releaseAwait = new CountDownLatch(1);
        ExecutorService workers = new PerTaskThreadExecutor("interrupt-wave-worker");
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () -> {
                            if (Thread.currentThread().getName().endsWith("-4")) {
                                awaitLatch(submissionStarted);
                                workerFailureReady.countDown();
                                throw workerFailure;
                            }
                            return new DestinationCommitExecutor.Worker(
                                    new WorkerFailureThenBlockingAwaitRunner(
                                            submissionStarted,
                                            workerFailureReady,
                                            awaitStarted,
                                            releaseAwait),
                                    new FakeTableAdmin());
                        },
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()),
                        workers);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(awaitStarted);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for FILE_LOADS destinations");
            assertThat(failureMessages(observed.get())).contains("submission-phase worker failure");
            assertThat(failureReferences(observed.get(), workerFailure)).isOne();
        } finally {
            releaseAwait.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void interruptionKeepsAQueuedBatchSubmissionFailure() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        CountDownLatch awaitsStarted = new CountDownLatch(2);
        CountDownLatch releaseAwaits = new CountDownLatch(1);
        AtomicInteger submissions = new AtomicInteger();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new QueuedSubmissionFailureRunner(
                                                submissions, awaitsStarted, releaseAwaits),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 3),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file(T1, "t1", 10),
                                                file(T2, "t2", 10),
                                                file(T3, "t3", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(awaitsStarted);
            assertThat(submissions).hasValue(3);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for FILE_LOADS destinations");
            assertThat(failureMessages(observed.get()))
                    .contains("scripted queued submission failure");
        } finally {
            releaseAwaits.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void awaitInterruptionStopsTheCurrentBatchImmediately() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(1)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        AtomicInteger awaits = new AtomicInteger();
        CountDownLatch awaitStarted = new CountDownLatch(1);
        CountDownLatch releaseAwait = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new InterruptCountingAwaitRunner(
                                                awaits, awaitStarted, releaseAwait),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 3),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(
                                        List.of(
                                                file(T1, "part-1", 8L << 40),
                                                file(T1, "part-2", 8L << 40),
                                                file(T1, "part-3", 8L << 40),
                                                file(T2, "part-4", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(awaitStarted);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Orchestrator test coordination was interrupted");
            assertThat(awaits).hasValue(1);
        } finally {
            releaseAwait.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void submissionInterruptionKeepsAnEarlierRecordedFailure() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        CountDownLatch submissionsStarted = new CountDownLatch(2);
        CountDownLatch releaseBlockedSubmission = new CountDownLatch(1);
        TestSinkCommitterMetricGroup metricGroup = TestSinkCommitterMetricGroup.create();
        FileLoadsCommitterMetrics metrics = new FileLoadsCommitterMetrics(metricGroup);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FailingAndBlockingSubmissionRunner(
                                                submissionsStarted, releaseBlockedSubmission),
                                        new FakeTableAdmin()),
                        metrics);
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        metrics.loadJobsSubmitted(),
                        new Limits(3, 100, 100, 2),
                        executor);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            awaitLatch(submissionsStarted);
            awaitActiveDestinations(metricGroup, 1);
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while waiting for FILE_LOADS destinations");
            assertThat(failureMessages(observed.get()))
                    .contains("scripted submission failure before coordinator interruption");
        } finally {
            releaseBlockedSubmission.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void reportsEachCrossDestinationAwaitFailureOnceInPlanOrder() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        ConcurrentMap<String, TableDestination> submitted = new ConcurrentHashMap<>();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FailingAwaitRunner(submitted), new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);

        Throwable failure;
        try {
            orchestrator.run(List.of(file(T1, "t1", 10), file(T2, "t2", 10)));
            throw new AssertionError("The scripted await failures should fail the commit");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failure).isInstanceOf(IOException.class).hasMessage(T1.toString());
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0])
                .isInstanceOf(IOException.class)
                .hasMessage(T2.toString());
    }

    @Test
    void aggregatesThousandsOfAwaitFailuresWithoutDroppingTheirPlanOrder() throws Exception {
        int jobCount = 4_096;
        FileLoadsOptions options =
                FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        ConcurrentMap<String, TableDestination> submitted = new ConcurrentHashMap<>();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FailingAwaitRunner(submitted), new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 10_000, 10_000, jobCount),
                        executor);
        List<FileLoadsCommittable> files =
                IntStream.range(0, jobCount)
                        .mapToObj(index -> file(T1, "large-" + index, 6L << 40))
                        .toList();

        Throwable failure;
        try {
            orchestrator.run(files);
            throw new AssertionError("Every scripted await should fail");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failure).isInstanceOf(IOException.class).hasMessageStartingWith("p.d.tmp_");
        assertThat(failure.getSuppressed()).hasSize(jobCount - 1);
        assertThat(submitted).isEmpty();
    }

    @Test
    void jvmFatalAwaitFailureStopsTheCurrentBatchImmediately() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        OutOfMemoryError fatal = new OutOfMemoryError("scripted await fatal");
        AtomicInteger awaits = new AtomicInteger();
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new FatalAwaitRunner(fatal, awaits), new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        new InMemoryStagingStorage(),
                        FLINK_JOB_ID,
                        null,
                        new SimpleCounter(),
                        new Limits(3, 100, 100, 2),
                        executor);

        Throwable failure;
        try {
            orchestrator.run(
                    List.of(
                            file(T1, "t1-avro", 10, StagingFormat.AVRO),
                            file(T1, "t1-parquet", 10, StagingFormat.PARQUET)));
            throw new AssertionError("The scripted JVM-fatal failure should fail the commit");
        } catch (Throwable observed) {
            failure = observed;
        } finally {
            executor.close();
        }

        assertThat(failure).isSameAs(fatal);
        assertThat(awaits).hasValue(1);
    }

    @Test
    void cleanupInterruptionLeavesStagedObjectsForRetry() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(2)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        CountDownLatch cleanupStarted = new CountDownLatch(2);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        2,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new CleanupBlockingRunner(cleanupStarted, releaseCleanup),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        storage,
                        FLINK_JOB_ID,
                        7L,
                        new SimpleCounter(),
                        Limits.BIGQUERY,
                        executor);
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files =
                List.of(
                        file(T1, "t1-a", sixTiB),
                        file(T1, "t1-b", sixTiB),
                        file(T2, "t2-a", sixTiB),
                        file(T2, "t2-b", sixTiB));
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(files);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while cleaning up");
            assertThat(storage.getDeleted()).isEmpty();
        } finally {
            releaseCleanup.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void inlineCleanupInterruptionLeavesStagedObjectsForRetry() throws Exception {
        FileLoadsOptions options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .maxConcurrentDestinations(1)
                        .build();
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer(SCHEMA))
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () ->
                                new DestinationCommitExecutor.Worker(
                                        new CleanupBlockingRunner(cleanupStarted, releaseCleanup),
                                        new FakeTableAdmin()),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()));
        InMemoryStagingStorage storage = new InMemoryStagingStorage();
        LoadJobOrchestrator orchestrator =
                new LoadJobOrchestrator(
                        config,
                        options,
                        storage,
                        FLINK_JOB_ID,
                        7L,
                        new SimpleCounter(),
                        Limits.BIGQUERY,
                        executor);
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB));
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread coordinator =
                new Thread(
                        () -> {
                            try {
                                orchestrator.run(files);
                            } catch (Throwable failure) {
                                observed.set(failure);
                            }
                        });

        try {
            coordinator.start();
            assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));

            assertThat(coordinator.isAlive()).isFalse();
            assertThat(coordinator.isInterrupted()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while cleaning up");
            assertThat(storage.getDeleted()).isEmpty();
        } finally {
            releaseCleanup.countDown();
            coordinator.interrupt();
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            executor.close();
        }
    }

    @Test
    void jobCapsAreValidatedBeforeTableOrJobSideEffects() {
        Harness tooManyLoads = Harness.withLimits(new Limits(3, 3, 100, 2));

        assertThatThrownBy(
                        () ->
                                tooManyLoads.orchestrator.run(
                                        List.of(
                                                file(T1, "a", 10),
                                                file(T2, "b", 10),
                                                file(T3, "c", 10),
                                                file(T4, "d", 10))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("requires 4 load jobs")
                .hasMessageContaining("at most 3");
        assertThat(tooManyLoads.runner.loads).isEmpty();
        assertThat(tooManyLoads.tableAdmin.created).isEmpty();

        Harness tooManyCopies = Harness.withLimits(new Limits(3, 10, 1, 2));
        long sixTiB = 6L << 40;

        assertThatThrownBy(
                        () ->
                                tooManyCopies.orchestrator.run(
                                        List.of(
                                                file(T1, "a", sixTiB),
                                                file(T1, "b", sixTiB),
                                                file(T2, "c", sixTiB),
                                                file(T2, "d", sixTiB))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("requires 2 copy jobs")
                .hasMessageContaining("at most 1");
        assertThat(tooManyCopies.runner.loads).isEmpty();
        assertThat(tooManyCopies.tableAdmin.created).isEmpty();
    }

    @Test
    void publishedDailyJobBoundariesAreAcceptedAndRejectedOnTheCorrectSide() throws IOException {
        CommitPlanner.validateJobCounts(Limits.MAX_JOBS_PER_COMMIT, Limits.MAX_JOBS_PER_COMMIT);

        assertThatThrownBy(
                        () ->
                                CommitPlanner.validateJobCounts(
                                        Limits.MAX_JOBS_PER_COMMIT + 1L,
                                        Limits.MAX_JOBS_PER_COMMIT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("100001 load jobs");
        assertThatThrownBy(
                        () ->
                                CommitPlanner.validateJobCounts(
                                        Limits.MAX_JOBS_PER_COMMIT,
                                        Limits.MAX_JOBS_PER_COMMIT + 1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("100001 copy jobs");
    }

    @Test
    void aFailureAtEveryCopyLevelStopsDependentWorkAndCleanup() throws IOException {
        Limits limits = new Limits(3, 100, 100, 2);
        List<FileLoadsCommittable> files =
                IntStream.rangeClosed(1, 10)
                        .mapToObj(i -> file(T1, "part-" + i, 6L << 40))
                        .toList();
        Harness successful = Harness.withLimits(limits);
        successful.orchestrator.run(files);
        List<String> copyJobIds = new ArrayList<>(successful.runner.copies.keySet());

        String finalCopyJobId = copyJobIds.get(4);
        for (String failingJobId : List.of(copyJobIds.get(0), copyJobIds.get(3), finalCopyJobId)) {
            Harness retry = Harness.withLimits(limits);
            retry.runner.failOnAwait.add(failingJobId);

            assertThatThrownBy(() -> retry.orchestrator.run(files)).isInstanceOf(IOException.class);

            assertThat(retry.runner.copies).containsKey(failingJobId);
            if (failingJobId.equals(finalCopyJobId)) {
                assertThat(retry.runner.copies.values())
                        .anySatisfy(copy -> assertThat(copy.getDestination()).isEqualTo(T1));
            } else {
                assertThat(retry.runner.copies.values())
                        .noneSatisfy(copy -> assertThat(copy.getDestination()).isEqualTo(T1));
            }
            assertThat(retry.runner.deletedTables).isEmpty();
            assertThat(retry.storage.getDeleted()).isEmpty();
        }
    }

    @Test
    void streamingOverflowLoadFailureDoesNotSubmitCopyOrCleanUp() {
        Harness harness = Harness.streaming(7);
        harness.runner.failAllAwaits = true;
        long sixTiB = 6L << 40;

        assertThatThrownBy(
                        () ->
                                harness.orchestrator.run(
                                        List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB))))
                .isInstanceOf(IOException.class);

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.runner.deletedTables).isEmpty();
        assertThat(harness.storage.getDeleted()).isEmpty();
    }

    @Test
    void streamingOverflowCopyFailureLeavesTempTablesAndStagingFiles() throws IOException {
        long sixTiB = 6L << 40;
        List<FileLoadsCommittable> files = List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB));
        Harness firstAttempt = Harness.streaming(7);
        firstAttempt.orchestrator.run(files);
        String copyJobId = firstAttempt.runner.copies.keySet().iterator().next();
        Harness retry = Harness.streaming(7);
        retry.runner.failOnAwait.add(copyJobId);

        assertThatThrownBy(() -> retry.orchestrator.run(files)).isInstanceOf(IOException.class);

        assertThat(retry.runner.copies).containsOnlyKeys(copyJobId);
        assertThat(retry.runner.deletedTables).isEmpty();
        assertThat(retry.storage.getDeleted()).isEmpty();
    }

    @Test
    void directLoadDemotesANewRequiredColumnToNullable() throws IOException {
        // The #142 regression: the serializer's derived schema gains a REQUIRED column against a
        // pre-existing table. An unreconciled load job would be rejected at submission ("Cannot
        // add required fields to an existing schema"); the union demotes the addition to NULLABLE
        // and patches the table before the load, exactly as the temp-table path always has.
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()),
                        null,
                        SCHEMA_WITH_F2_REQUIRED);
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.tableAdmin.tables.get(T1).getFieldsList())
                .extracting(TableFieldSchema::getName, TableFieldSchema::getMode)
                .containsExactly(
                        tuple("f1", TableFieldSchema.Mode.NULLABLE),
                        tuple("f2", TableFieldSchema.Mode.NULLABLE));
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getSchema().getFields())
                                    .extracting(Field::getName)
                                    .containsExactly("f1", "f2");
                            assertThat(spec.getSchema().getFields().get("f2").getMode())
                                    .isEqualTo(Field.Mode.NULLABLE);
                        });
    }

    @Test
    void directLoadUsesTheLiveSchemaWhenUpdatesAreDisabled() throws IOException {
        // Without schemaUpdateOptions the live table's schema wins over the serializer's wider
        // one; the table is never patched.
        Harness harness = Harness.plain();
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.schemaUpdates).isEmpty();
        assertThat(harness.tableAdmin.created).isEmpty();
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec ->
                                assertThat(spec.getSchema().getFields())
                                        .extracting(Field::getName)
                                        .containsExactly("f1"));
    }

    @Test
    void streamingOverflowReconcilesOncePerDestination() throws IOException {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()),
                        7L);
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);
        long sixTiB = 6L << 40;

        harness.orchestrator.run(
                List.of(file(T1, "a", sixTiB), file(T1, "b", sixTiB), file(T1, "c", sixTiB)));

        // Three temp-table loads, but the live table was read and patched exactly once.
        assertThat(harness.runner.loads).hasSize(3);
        assertThat(harness.tableAdmin.schemaReads).isEqualTo(1);
        assertThat(harness.tableAdmin.schemaUpdates).containsExactly(T1);
        assertThat(harness.runner.loads.values())
                .allSatisfy(
                        spec ->
                                assertThat(spec.getSchema().getFields())
                                        .extracting(Field::getName)
                                        .containsExactly("f1", "f2"));
    }

    @Test
    void directLoadIntoMissingTableFailsUnderCreateNever() {
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build(),
                        builder -> builder.createDisposition(CreateDisposition.CREATE_NEVER));

        assertThatThrownBy(() -> harness.orchestrator.run(List.of(file(T1, "a", 10))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(harness.runner.loads).isEmpty();
        assertThat(harness.storage.getDeleted()).isEmpty();
    }

    @Test
    void truncatingDirectLoadSkipsSchemaReconciliation() throws IOException {
        // WRITE_TRUNCATE replaces the table's schema wholesale, so the serializer's schema is
        // used as-is and the live table is never patched.
        Harness harness =
                new Harness(
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .writeDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .build(),
                        builder ->
                                builder.schemaUpdateOptions(
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        harness.tableAdmin.tables.put(T1, LIVE_F1_ONLY);

        harness.orchestrator.run(List.of(file(T1, "a", 10)));

        assertThat(harness.tableAdmin.schemaUpdates).isEmpty();
        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec ->
                                assertThat(spec.getSchema().getFields())
                                        .extracting(Field::getName)
                                        .containsExactly("f1", "f2"));
    }

    @Test
    void emptyCommittablesDoNothing() throws IOException {
        Harness harness = Harness.plain();

        harness.orchestrator.run(List.of());

        assertThat(harness.runner.loads).isEmpty();
        assertThat(harness.runner.copies).isEmpty();
        assertThat(harness.storage.getDeleted()).isEmpty();
    }

    private static void awaitLatch(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for orchestrator test coordination");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Orchestrator test coordination was interrupted", failure);
        }
    }

    private static void awaitLatchIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            while (latch.getCount() > 0) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new AssertionError(
                            "Timed out waiting for orchestrator test coordination");
                }
                try {
                    if (!latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        throw new AssertionError(
                                "Timed out waiting for orchestrator test coordination");
                    }
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void awaitActiveDestinations(TestSinkCommitterMetricGroup metrics, int expected)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (metrics.<Integer>gaugeValue(BigQueryMetricNames.ACTIVE_COMMIT_DESTINATIONS)
                != expected) {
            if (System.nanoTime() >= deadline) {
                throw new IOException(
                        "Timed out waiting for " + expected + " active commit destinations");
            }
            Thread.onSpinWait();
        }
    }

    private static List<String> failureMessages(Throwable failure) {
        List<Throwable> pending = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        pending.add(failure);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            messages.add(candidate.getMessage());
            pending.addAll(List.of(candidate.getSuppressed()));
        }
        return messages;
    }

    private static int failureReferences(Throwable failure, Throwable expected) {
        List<Throwable> pending = new ArrayList<>();
        int references = 0;
        pending.add(failure);
        for (int index = 0; index < pending.size(); index++) {
            Throwable candidate = pending.get(index);
            if (candidate == expected) {
                references++;
            }
            pending.addAll(List.of(candidate.getSuppressed()));
        }
        return references;
    }

    private static final class PendingJobRunner implements LoadJobRunner {
        private final AtomicInteger pending;
        private final AtomicInteger maximumPending;
        private final CountDownLatch releaseAwaits;

        private PendingJobRunner(
                AtomicInteger pending, AtomicInteger maximumPending, CountDownLatch releaseAwaits) {
            this.pending = pending;
            this.maximumPending = maximumPending;
            this.releaseAwaits = releaseAwaits;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {
            int now = pending.incrementAndGet();
            maximumPending.accumulateAndGet(now, Math::max);
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            try {
                if (!releaseAwaits.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting to release pending test jobs");
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException("Pending test job was interrupted", failure);
            }
            pending.decrementAndGet();
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class PerTaskThreadExecutor extends AbstractExecutorService {
        private final String threadPrefix;
        private final AtomicInteger threadNumber = new AtomicInteger();
        private final List<Thread> threads = new ArrayList<>();
        private boolean shutdown;

        private PerTaskThreadExecutor(String threadPrefix) {
            this.threadPrefix = threadPrefix;
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("The test executor is shut down");
            }
            Thread thread =
                    new Thread(command, threadPrefix + "-" + threadNumber.incrementAndGet());
            threads.add(thread);
            thread.start();
        }

        @Override
        public synchronized void shutdown() {
            shutdown = true;
        }

        @Override
        public synchronized List<Runnable> shutdownNow() {
            shutdown = true;
            for (Thread thread : threads) {
                thread.interrupt();
            }
            return List.of();
        }

        @Override
        public synchronized boolean isShutdown() {
            return shutdown;
        }

        @Override
        public synchronized boolean isTerminated() {
            return shutdown && threads.stream().noneMatch(Thread::isAlive);
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            for (Thread thread : threadSnapshot()) {
                while (thread.isAlive()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
            }
            return true;
        }

        private synchronized List<Thread> threadSnapshot() {
            return List.copyOf(threads);
        }
    }

    private static final class PartiallyFailingSubmissionRunner implements LoadJobRunner {
        private final AtomicInteger submissions;
        private final CountDownLatch successfulSubmissionsReady;
        private final CountDownLatch awaitStarted;
        private final CountDownLatch releaseAwait;

        private PartiallyFailingSubmissionRunner(
                AtomicInteger submissions,
                CountDownLatch successfulSubmissionsReady,
                CountDownLatch awaitStarted,
                CountDownLatch releaseAwait) {
            this.submissions = submissions;
            this.successfulSubmissionsReady = successfulSubmissionsReady;
            this.awaitStarted = awaitStarted;
            this.releaseAwait = releaseAwait;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissions.incrementAndGet();
            if (spec.getFormat() == StagingFormat.PARQUET) {
                awaitLatch(successfulSubmissionsReady);
                throw new IOException("scripted submission failure");
            }
            successfulSubmissionsReady.countDown();
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            awaitStarted.countDown();
            awaitLatch(releaseAwait);
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class ConcurrentFatalSubmissionRunner implements LoadJobRunner {
        private final Map<TableDestination, OutOfMemoryError> failures;
        private final CountDownLatch submissionsReady;

        private ConcurrentFatalSubmissionRunner(
                Map<TableDestination, OutOfMemoryError> failures, CountDownLatch submissionsReady) {
            this.failures = failures;
            this.submissionsReady = submissionsReady;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissionsReady.countDown();
            // The first observed fatal stops and interrupts its peer. The barrier must still let
            // that peer throw its scripted fatal so the test measures plan-order aggregation.
            awaitLatchIgnoringInterrupt(submissionsReady);
            throw failures.get(spec.getDestination());
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            throw new AssertionError("A fatally failed submission must not be awaited");
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class SubmissionFailingRunner implements LoadJobRunner {
        private final IOException failure;
        private final CountDownLatch submissionStarted;
        private final CountDownLatch workerFailureReady;

        private SubmissionFailingRunner(
                IOException failure,
                CountDownLatch submissionStarted,
                CountDownLatch workerFailureReady) {
            this.failure = failure;
            this.submissionStarted = submissionStarted;
            this.workerFailureReady = workerFailureReady;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissionStarted.countDown();
            awaitLatch(workerFailureReady);
            throw failure;
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            throw new AssertionError("A failed submission must not be awaited");
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class WorkerFailureThenBlockingAwaitRunner implements LoadJobRunner {
        private final CountDownLatch submissionStarted;
        private final CountDownLatch workerFailureReady;
        private final CountDownLatch awaitStarted;
        private final CountDownLatch releaseAwait;

        private WorkerFailureThenBlockingAwaitRunner(
                CountDownLatch submissionStarted,
                CountDownLatch workerFailureReady,
                CountDownLatch awaitStarted,
                CountDownLatch releaseAwait) {
            this.submissionStarted = submissionStarted;
            this.workerFailureReady = workerFailureReady;
            this.awaitStarted = awaitStarted;
            this.releaseAwait = releaseAwait;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissionStarted.countDown();
            awaitLatch(workerFailureReady);
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            awaitStarted.countDown();
            awaitLatch(releaseAwait);
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class InterruptCountingAwaitRunner implements LoadJobRunner {
        private final AtomicInteger awaits;
        private final CountDownLatch awaitStarted;
        private final CountDownLatch releaseAwait;

        private InterruptCountingAwaitRunner(
                AtomicInteger awaits, CountDownLatch awaitStarted, CountDownLatch releaseAwait) {
            this.awaits = awaits;
            this.awaitStarted = awaitStarted;
            this.releaseAwait = releaseAwait;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            awaits.incrementAndGet();
            awaitStarted.countDown();
            awaitLatch(releaseAwait);
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class FailingAndBlockingSubmissionRunner implements LoadJobRunner {
        private final CountDownLatch submissionsStarted;
        private final CountDownLatch releaseBlockedSubmission;

        private FailingAndBlockingSubmissionRunner(
                CountDownLatch submissionsStarted, CountDownLatch releaseBlockedSubmission) {
            this.submissionsStarted = submissionsStarted;
            this.releaseBlockedSubmission = releaseBlockedSubmission;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            submissionsStarted.countDown();
            awaitLatch(submissionsStarted);
            if (spec.getDestination().equals(T1)) {
                throw new IOException(
                        "scripted submission failure before coordinator interruption");
            }
            awaitLatch(releaseBlockedSubmission);
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            throw new AssertionError("A failed submission wave must not await jobs");
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class QueuedSubmissionFailureRunner implements LoadJobRunner {
        private final AtomicInteger submissions;
        private final CountDownLatch awaitsStarted;
        private final CountDownLatch releaseAwaits;

        private QueuedSubmissionFailureRunner(
                AtomicInteger submissions,
                CountDownLatch awaitsStarted,
                CountDownLatch releaseAwaits) {
            this.submissions = submissions;
            this.awaitsStarted = awaitsStarted;
            this.releaseAwaits = releaseAwaits;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            if (submissions.incrementAndGet() == 3) {
                throw new IOException("scripted queued submission failure");
            }
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            awaitsStarted.countDown();
            awaitLatch(releaseAwaits);
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class FailingAwaitRunner implements LoadJobRunner {
        private final ConcurrentMap<String, TableDestination> submitted;

        private FailingAwaitRunner(ConcurrentMap<String, TableDestination> submitted) {
            this.submitted = submitted;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {
            submitted.put(jobId, spec.getDestination());
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) throws IOException {
            TableDestination destination = submitted.remove(jobId);
            if (destination == null) {
                throw new AssertionError("Awaited an unsubmitted job " + jobId);
            }
            throw new IOException(destination.toString());
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class FatalAwaitRunner implements LoadJobRunner {
        private final OutOfMemoryError fatal;
        private final AtomicInteger awaits;

        private FatalAwaitRunner(OutOfMemoryError fatal, AtomicInteger awaits) {
            this.fatal = fatal;
            this.awaits = awaits;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) {}

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a copy job");
        }

        @Override
        public void submitQuery(String jobId, QueryJobSpec spec) {
            throw new AssertionError("The direct-load test must not submit a query job");
        }

        @Override
        public void awaitJob(String jobId) {
            awaits.incrementAndGet();
            throw fatal;
        }

        @Override
        public void deleteTable(TableDestination table) {}
    }

    private static final class CleanupBlockingRunner implements LoadJobRunner {
        private final FakeLoadJobRunner delegate = new FakeLoadJobRunner();
        private final CountDownLatch cleanupStarted;
        private final CountDownLatch releaseCleanup;

        private CleanupBlockingRunner(
                CountDownLatch cleanupStarted, CountDownLatch releaseCleanup) {
            this.cleanupStarted = cleanupStarted;
            this.releaseCleanup = releaseCleanup;
        }

        @Override
        public void submitLoad(String jobId, LoadJobSpec spec) throws IOException {
            delegate.submitLoad(jobId, spec);
        }

        @Override
        public void submitCopy(String jobId, CopyJobSpec spec) throws IOException {
            delegate.submitCopy(jobId, spec);
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
            cleanupStarted.countDown();
            try {
                releaseCleanup.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void awaitPendingJobs(AtomicInteger pending, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (pending.get() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(pending).hasValueGreaterThanOrEqualTo(expected);
    }
}
