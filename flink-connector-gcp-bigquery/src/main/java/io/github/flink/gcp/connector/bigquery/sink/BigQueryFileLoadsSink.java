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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittableSerializer;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommitter;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsPostCommitOperator;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsWriter;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.GcsStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingStorage;

/**
 * The {@link WriteMethod#FILE_LOADS} sink: writers stage per-destination Avro files on Cloud
 * Storage, a no-op committer forwards their committables, and a parallelism-1 post-commit operator
 * turns them into BigQuery load jobs at end of input (see {@link
 * io.github.flink.gcp.connector.bigquery.sink.fileloads.LoadJobOrchestrator}).
 *
 * <p>Batch execution only, enforced twice: at graph construction here (streaming mode is rejected
 * when the post-commit topology is added) and at runtime in the writer (a pre-end-of-input flush
 * means a checkpoint, catching {@link RuntimeExecutionMode#AUTOMATIC} resolving to streaming).
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryFileLoadsSink<T>
        implements Sink<T>,
                SupportsCommitter<FileLoadsCommittable>,
                SupportsPostCommitTopology<FileLoadsCommittable> {

    private static final long serialVersionUID = 1L;

    private final BigQuerySinkConfig<T> config;
    private final FileLoadsOptions options;
    private final StagingStorage storage;

    BigQueryFileLoadsSink(BigQuerySinkConfig<T> config, FileLoadsOptions options) {
        this(config, options, new GcsStagingStorage());
    }

    @VisibleForTesting
    BigQueryFileLoadsSink(
            BigQuerySinkConfig<T> config, FileLoadsOptions options, StagingStorage storage) {
        this.config = config;
        this.options = options;
        this.storage = storage;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        return new FileLoadsWriter<>(
                config,
                options,
                storage,
                context.getJobInfo().getJobId().toString(),
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber());
    }

    @Override
    public Committer<FileLoadsCommittable> createCommitter(CommitterInitContext context) {
        return new FileLoadsCommitter();
    }

    @Override
    public SimpleVersionedSerializer<FileLoadsCommittable> getCommittableSerializer() {
        return new FileLoadsCommittableSerializer();
    }

    @Override
    public void addPostCommitTopology(
            DataStream<CommittableMessage<FileLoadsCommittable>> committables) {
        RuntimeExecutionMode mode =
                committables
                        .getExecutionEnvironment()
                        .getConfiguration()
                        .get(ExecutionOptions.RUNTIME_MODE);
        if (mode == RuntimeExecutionMode.STREAMING) {
            throw new IllegalStateException(
                    WriteMethod.FILE_LOADS
                            + " supports batch execution only. Run the pipeline in"
                            + " RuntimeExecutionMode.BATCH.");
        }
        committables
                .global()
                .transform(
                        "BigQuery load jobs",
                        Types.VOID,
                        new FileLoadsPostCommitOperator(config, options, storage))
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    /** Returns the sink configuration. */
    public BigQuerySinkConfig<T> getConfig() {
        return config;
    }

    /** Returns the FILE_LOADS options. */
    public FileLoadsOptions getOptions() {
        return options;
    }
}
