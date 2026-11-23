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
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitterMetrics;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializationSchema;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableLayout;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that every job of a {@link LoadJobOrchestrator} commit runs in, and is looked up in, the
 * commit's job project (#2178): the runner names it in every job id, so a job the orchestrator
 * handed over without it would be created or looked for in the client's own project.
 */
class LoadJobOrchestratorJobProjectTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final String JOB_PROJECT = "jobs-project";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");
    private static final long SIX_TIB = 6L << 40;

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
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

    @Test
    void everyJobOfACommitRunsInItsJobProject() throws IOException {
        // A WRITE_TRUNCATE_DATA overflow submits all three kinds of job: loads into temporary
        // tables, the aggregate copy and the terminal query.
        FakeLoadJobRunner runner = new FakeLoadJobRunner();
        FakeTableAdmin tableAdmin = new FakeTableAdmin();
        tableAdmin.tables.put(T1, SCHEMA);

        run(
                options(WriteDisposition.WRITE_TRUNCATE_DATA),
                JOB_PROJECT,
                runner,
                tableAdmin,
                List.of(file("a", SIX_TIB), file("b", SIX_TIB)));

        assertThat(runner.loads.values())
                .hasSize(2)
                .allSatisfy(load -> assertThat(load.getJobProject()).isEqualTo(JOB_PROJECT));
        assertThat(runner.copies.values())
                .singleElement()
                .returns(JOB_PROJECT, CopyJobSpec::getJobProject);
        assertThat(runner.queries.values())
                .singleElement()
                .returns(JOB_PROJECT, QueryJobSpec::getJobProject);
    }

    @Test
    void aLaidOutCommitAsksAboutItsFinalCopyInItsJobProject() throws IOException {
        // A destination with clustering lays its temporary tables out, and that path first asks
        // whether an earlier attempt's final copy succeeded; the answer is only right when it is
        // asked where that copy would have run.
        FakeLoadJobRunner runner = new FakeLoadJobRunner();
        FakeTableAdmin tableAdmin = new FakeTableAdmin();
        tableAdmin.tables.put(T1, SCHEMA);
        tableAdmin.layouts.put(
                T1,
                TableLayout.of(
                        null, null, Clustering.newBuilder().setFields(List.of("f1")).build()));

        run(
                options(WriteDisposition.WRITE_APPEND),
                JOB_PROJECT,
                runner,
                tableAdmin,
                List.of(file("a", SIX_TIB), file("b", SIX_TIB), file("c", SIX_TIB)));

        assertThat(runner.copySucceededQueries)
                .singleElement()
                .returns(JOB_PROJECT, CopyJobSpec::getJobProject);
        assertThat(runner.copies.values())
                .singleElement()
                .returns(JOB_PROJECT, CopyJobSpec::getJobProject);
    }

    @Test
    void withoutAJobProjectEveryJobIsLeftToTheClientsProject() throws IOException {
        FakeLoadJobRunner runner = new FakeLoadJobRunner();
        FakeTableAdmin tableAdmin = new FakeTableAdmin();
        tableAdmin.tables.put(T1, SCHEMA);

        run(
                options(WriteDisposition.WRITE_TRUNCATE_DATA),
                null,
                runner,
                tableAdmin,
                List.of(file("a", SIX_TIB), file("b", SIX_TIB)));

        assertThat(runner.loads.values())
                .hasSize(2)
                .allSatisfy(load -> assertThat(load.getJobProject()).isNull());
        assertThat(runner.copies.values())
                .singleElement()
                .returns(null, CopyJobSpec::getJobProject);
        assertThat(runner.queries.values())
                .singleElement()
                .returns(null, QueryJobSpec::getJobProject);
    }

    private static FileLoadsOptions options(WriteDisposition writeDisposition) {
        return FileLoadsOptions.builder()
                .stagingPath("gs://bucket/prefix")
                .writeDisposition(writeDisposition)
                .build();
    }

    private static FileLoadsCommittable file(String name, long bytes) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID,
                T1,
                "gs://bucket/prefix/" + name + StagingFormat.AVRO.getExtension(),
                bytes,
                10,
                StagingFormat.AVRO,
                T1.getDataset());
    }

    private static void run(
            FileLoadsOptions options,
            @Nullable String jobProject,
            FakeLoadJobRunner runner,
            FakeTableAdmin tableAdmin,
            List<FileLoadsCommittable> files)
            throws IOException {
        BigQuerySinkConfig<Object> config =
                ((BigQueryFileLoadsSink<Object>)
                                BigQuerySink.builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(T1)
                                        .serializer(new SchemaOnlySerializer())
                                        .fileLoadsOptions(options)
                                        .build())
                        .getConfig();
        try (DestinationCommitExecutor executor =
                new DestinationCommitExecutor(
                        1,
                        () -> new DestinationCommitExecutor.Worker(runner, tableAdmin),
                        FileLoadsCommitterMetrics.unregistered(new SimpleCounter()))) {
            new LoadJobOrchestrator(
                            config,
                            options,
                            new InMemoryStagingStorage(),
                            FLINK_JOB_ID,
                            null,
                            jobProject,
                            new SimpleCounter(),
                            executor)
                    .run(files);
        }
    }
}
