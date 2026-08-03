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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.FakeLoadJobRunner;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.FakeTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.InMemoryStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsCommitter} against recording fakes. */
class FileLoadsCommitterTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f1")
                                    .setType(TableFieldSchema.Type.STRING)
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

    /** A minimal {@link CommitRequest}; signals are irrelevant to these tests. */
    private static final class TestCommitRequest implements CommitRequest<FileLoadsCommittable> {

        private final FileLoadsCommittable committable;

        TestCommitRequest(FileLoadsCommittable committable) {
            this.committable = committable;
        }

        @Override
        public FileLoadsCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(FileLoadsCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {}
    }

    /** Everything one committer test touches. */
    private static final class Harness {
        final FakeLoadJobRunner runner = new FakeLoadJobRunner();
        final FakeTableAdmin tableAdmin = new FakeTableAdmin();
        final InMemoryStagingStorage storage = new InMemoryStagingStorage();
        final TestSinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();
        final FileLoadsCommitter committer;

        Harness() {
            FileLoadsOptions options =
                    FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build();
            BigQuerySinkConfig<Object> config =
                    ((BigQueryFileLoadsSink<Object>)
                                    BigQuerySink.builder()
                                            .writeMethod(WriteMethod.FILE_LOADS)
                                            .destination(T1)
                                            .serializer(new SchemaOnlySerializer())
                                            .fileLoadsOptions(options)
                                            .build())
                            .getConfig();
            this.committer =
                    new FileLoadsCommitter(
                            config, options, storage, metrics, () -> runner, () -> tableAdmin);
        }

        void commit(FileLoadsCommittable... committables) throws IOException {
            committer.commit(
                    Arrays.stream(committables)
                            .map(TestCommitRequest::new)
                            .collect(Collectors.toList()));
        }
    }

    private static FileLoadsCommittable file(String name) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID, T1, "gs://bucket/prefix/" + name + ".avro", 10, 5);
    }

    @Test
    void batchCommittablesLoadWithoutACheckpointSegment() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("b"), file("a"));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id ->
                                assertThat(id)
                                        .matches(
                                                "flink-bq-load-" + FLINK_JOB_ID + "-[0-9a-f]{16}"));
        assertThat(harness.runner.loads.values().iterator().next().getSourceUris())
                .containsExactly("gs://bucket/prefix/a.avro", "gs://bucket/prefix/b.avro");
        assertThat(harness.storage.getDeleted())
                .containsExactlyInAnyOrder(
                        "gs://bucket/prefix/a.avro", "gs://bucket/prefix/b.avro");
    }

    @Test
    void stampedCommittablesLoadWithTheirCheckpointSegment() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a").withCheckpointId(7), file("b").withCheckpointId(7));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(id -> assertThat(id).contains("-c7-"));
        assertThat(harness.storage.getDeleted()).hasSize(2);
    }

    @Test
    void mixedCommitBatchesAreRejected() {
        // The framework commits one checkpoint at a time; a mixed batch would break the
        // per-checkpoint job-id attribution, so the invariant fails loudly.
        Harness harness = new Harness();

        assertThatThrownBy(
                        () ->
                                harness.commit(
                                        file("a").withCheckpointId(1),
                                        file("b").withCheckpointId(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mixes");
        assertThat(harness.runner.loads).isEmpty();
    }

    @Test
    void jobIdsFollowTheCommittablesOriginatingFlinkJobId() throws IOException {
        // Committer state restored under a NEW Flink job id must reproduce the ORIGINAL job's
        // deterministic BigQuery job ids so the runner re-attaches instead of double-loading;
        // the id therefore comes from the committable, not from the runtime.
        Harness harness = new Harness();
        String originalJobId = "fedcba9876543210fedcba9876543210";

        harness.commit(
                new FileLoadsCommittable(originalJobId, T1, "gs://bucket/prefix/a.avro", 10, 5)
                        .withCheckpointId(3));

        assertThat(harness.runner.loads.keySet())
                .singleElement()
                .satisfies(
                        id -> assertThat(id).startsWith("flink-bq-load-" + originalJobId + "-c3-"));
    }

    @Test
    void loadFailurePropagatesAndLeavesFilesInPlace() {
        Harness harness = new Harness();
        harness.runner.failAllAwaits = true;

        assertThatThrownBy(() -> harness.commit(file("a").withCheckpointId(3)))
                .isInstanceOf(IOException.class);

        assertThat(harness.storage.getDeleted()).isEmpty();
    }

    @Test
    void countsEveryLoadJobSubmitted() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a"));
        harness.commit(file("b").withCheckpointId(1));

        assertThat(harness.runner.loads).hasSize(2);
        assertThat(harness.metrics.counterValue(FileLoadsCommitter.LOAD_JOBS_SUBMITTED))
                .isEqualTo(2);
    }

    @Test
    void countsNoLoadJobForAnEmptyCommit() throws IOException {
        Harness harness = new Harness();

        harness.commit();

        assertThat(harness.metrics.counterValue(FileLoadsCommitter.LOAD_JOBS_SUBMITTED)).isZero();
    }

    @Test
    void streamingCommitsAppendWithTheConfiguredDispositions() throws IOException {
        Harness harness = new Harness();

        harness.commit(file("a").withCheckpointId(1));

        assertThat(harness.runner.loads.values())
                .singleElement()
                .satisfies(
                        spec -> {
                            assertThat(spec.getDestination()).isEqualTo(T1);
                            assertThat(spec.getWriteDisposition())
                                    .isEqualTo(JobInfo.WriteDisposition.WRITE_APPEND);
                        });
    }
}
