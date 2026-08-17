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
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private static final class SchemaOnlySerializer extends BigQueryProtoSerializer<Object> {
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
                            .destination(T1)
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
}
