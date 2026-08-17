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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCheckpointStamper;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.committer.FileLoadsCommitter;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.FileLoadsWriter;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.GcsStagingStorage;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.StagingStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * The {@link WriteMethod#FILE_LOADS} sink: writers stage per-destination files on Cloud Storage in
 * the configured {@link FileLoadsOptions#getStagingFormat() staging format}, the pre-commit
 * topology routes every subtask's committables to a single committer subtask (stamping their
 * checkpoint id in streaming, see {@link FileLoadsCheckpointStamper}), and the committer turns each
 * batch into BigQuery load jobs (see {@link FileLoadsCommitter} and {@link
 * io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob.LoadJobOrchestrator}) — the whole
 * run at end of input in batch execution, each checkpoint's files at its completion in streaming
 * execution.
 *
 * <p>Loading in the committer (not a post-commit topology) is deliberate: the framework's committer
 * state carries everything not yet loaded across restarts, and at job shutdown the final batch is
 * committed during the final-checkpoint wait — records emitted to a post-commit topology at that
 * point are not guaranteed to be processed before the tasks close.
 *
 * <p>The execution mode must be explicit: {@link RuntimeExecutionMode#AUTOMATIC} is rejected when
 * the pre-commit topology is added, because were it to resolve to streaming with checkpointing
 * disabled, files would stage forever without ever being loaded. Streaming additionally requires
 * checkpointing (the checkpoint is the load trigger), {@link WriteDisposition#WRITE_APPEND} (other
 * dispositions are meaningless per checkpoint), and a checkpoint interval that stays clear of
 * BigQuery's daily load-job and destination-table modification limits — below {@link
 * FileLoadsOptions#getMinCheckpointInterval()} is an error, below {@link
 * #QUOTA_WARN_CHECKPOINT_INTERVAL} a warning.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryFileLoadsSink<T>
        implements CrossVersionSink<T>,
                SupportsCommitter<FileLoadsCommittable>,
                SupportsPreCommitTopology<FileLoadsCommittable, FileLoadsCommittable> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryFileLoadsSink.class);

    /** Streaming checkpoint intervals below this (but above the error threshold) log a warning. */
    private static final Duration QUOTA_WARN_CHECKPOINT_INTERVAL = Duration.ofMinutes(5);

    private final BigQuerySinkConfig<T> config;
    private final FileLoadsOptions options;
    private final StagingStorage storage;

    /**
     * Creates the sink; called by {@link
     * io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder}.
     *
     * @param config the sink configuration
     * @param options the FILE_LOADS options
     */
    public BigQueryFileLoadsSink(BigQuerySinkConfig<T> config, FileLoadsOptions options) {
        this(config, options, new GcsStagingStorage(config.getServiceAccountKeyFile()));
    }

    @VisibleForTesting
    BigQueryFileLoadsSink(
            BigQuerySinkConfig<T> config, FileLoadsOptions options, StagingStorage storage) {
        this.config = config;
        this.options = options;
        this.storage = storage;
    }

    /** Returns the staging storage wired from this sink's configuration. */
    @VisibleForTesting
    StagingStorage stagingStorage() {
        return storage;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        config.getFailureHandler().open(DefaultFailureHandlerContext.of(context));
        try {
            return new FileLoadsWriter<>(
                    config,
                    options,
                    storage,
                    context.metricGroup(),
                    context.getJobInfo().getJobId().toString(),
                    context.getTaskInfo().getIndexOfThisSubtask(),
                    context.getTaskInfo().getAttemptNumber());
        } catch (Throwable e) {
            // The handler is the only thing to release: the staging storage is a field of the sink
            // and opens no client until the writer asks it to. Nothing downstream would close it —
            // no writer exists to do it — and Flink rebuilds the writer on every restart attempt,
            // so an opened handler would accumulate per attempt on a task manager that stays alive.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(e, config.getFailureHandler()::close);
            throw e;
        }
    }

    @Override
    public Committer<FileLoadsCommittable> createCommitter(CommitterInitContext context) {
        return new FileLoadsCommitter(config, options, storage, context.metricGroup());
    }

    @Override
    public SimpleVersionedSerializer<FileLoadsCommittable> getCommittableSerializer() {
        return new FileLoadsCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FileLoadsCommittable> getWriteResultSerializer() {
        return new FileLoadsCommittableSerializer();
    }

    @Override
    public DataStream<CommittableMessage<FileLoadsCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<FileLoadsCommittable>> committables) {
        RuntimeExecutionMode mode =
                committables
                        .getExecutionEnvironment()
                        .getConfiguration()
                        .get(ExecutionOptions.RUNTIME_MODE);
        boolean streaming;
        switch (mode) {
            case BATCH:
                streaming = false;
                break;
            case STREAMING:
                validateStreaming(
                        committables.getExecutionEnvironment().getCheckpointConfig(),
                        committables.getExecutionEnvironment().getConfiguration());
                streaming = true;
                break;
            default:
                // AUTOMATIC: were it to resolve to streaming with checkpointing disabled, no
                // trigger would ever come and files would stage forever — and that resolution
                // is invisible here, so an explicit mode is required.
                throw new IllegalStateException(
                        WriteMethod.FILE_LOADS
                                + " requires an explicit execution mode, but the runtime mode is "
                                + mode
                                + ". Set RuntimeExecutionMode.BATCH or RuntimeExecutionMode"
                                + ".STREAMING explicitly (streaming additionally requires"
                                + " checkpointing).");
        }
        DataStream<CommittableMessage<FileLoadsCommittable>> prepared = committables;
        if (streaming) {
            prepared =
                    prepared.map(
                                    new FileLoadsCheckpointStamper(),
                                    CommittableMessageTypeInfo.of(this::getCommittableSerializer))
                            .name("Stamp checkpoint ids");
        }
        // The committer inherits the sink's parallelism; the trailing global exchange routes
        // every subtask's committables to committer subtask 0, giving one committer instance
        // the global per-table view the load jobs need (the other subtasks stay idle).
        return prepared.global();
    }

    /**
     * Rejects streaming setups that cannot work (no checkpointing — no load trigger; a non-append
     * disposition) or that would exhaust BigQuery's daily destination-table modification limit at a
     * sustained cadence.
     */
    private void validateStreaming(
            CheckpointConfig checkpointConfig, ReadableConfig configuration) {
        if (!checkpointConfig.isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    WriteMethod.FILE_LOADS
                            + " in streaming execution requires checkpointing: load jobs are"
                            + " triggered by checkpoints, so without checkpointing staged files"
                            + " would never be loaded. Enable checkpointing"
                            + " (execution.checkpointing.interval) or run in"
                            + " RuntimeExecutionMode.BATCH.");
        }
        if (checkpointConfig.getCheckpointingConsistencyMode() != CheckpointingMode.EXACTLY_ONCE) {
            // Under AT_LEAST_ONCE alignment, records processed after a barrier land in the
            // barrier's files and are replayed after a failure — duplicate loads.
            throw new IllegalStateException(
                    WriteMethod.FILE_LOADS
                            + " in streaming execution requires CheckpointingMode.EXACTLY_ONCE"
                            + " (the write method is exactly-once), but the checkpointing"
                            + " consistency mode is "
                            + checkpointConfig.getCheckpointingConsistencyMode()
                            + ".");
        }
        if (!configuration.get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)) {
            // The final batch of a bounded streaming job is committed by the checkpoint taken
            // after the tasks finished; without it the tail would stage but never load.
            throw new IllegalStateException(
                    WriteMethod.FILE_LOADS
                            + " in streaming execution requires"
                            + " execution.checkpointing.checkpoints-after-tasks-finish.enabled:"
                            + " the final batch is loaded by the checkpoint taken after the tasks"
                            + " finish, so disabling it would silently drop the tail of a bounded"
                            + " job.");
        }
        if (options.getWriteDisposition() != WriteDisposition.WRITE_APPEND) {
            throw new IllegalStateException(
                    WriteMethod.FILE_LOADS
                            + " in streaming execution supports WriteDisposition.WRITE_APPEND"
                            + " only (each checkpoint appends its rows; non-append dispositions"
                            + " would make every checkpoint replace or reject the table), but the"
                            + " write disposition is "
                            // name(), not toString(): the sentence above names the Java constants
                            // and the DDL spelling must not mix with it inside one message.
                            + options.getWriteDisposition().name()
                            + ".");
        }
        long intervalMs = checkpointConfig.getCheckpointInterval();
        long minIntervalMs = options.getMinCheckpointInterval().toMillis();
        if (intervalMs < minIntervalMs) {
            throw new IllegalStateException(
                    "The checkpoint interval ("
                            + intervalMs
                            + " ms) is below the "
                            + WriteMethod.FILE_LOADS
                            + " minimum ("
                            + minIntervalMs
                            + " ms). BigQuery allows 1,500 modifications per standard destination"
                            + " table per day and each checkpoint issues a direct load or an"
                            + " overflow copy"
                            + " (1 min = 1,440/day, 2 min = 720/day, 5 min = 288/day). Increase"
                            + " the checkpoint interval, or lower"
                            + " FileLoadsOptions.minCheckpointInterval(...) explicitly for a"
                            + " short-lived job whose daily load count stays safe.");
        }
        if (intervalMs < QUOTA_WARN_CHECKPOINT_INTERVAL.toMillis()) {
            LOG.warn(
                    "The checkpoint interval ({} ms) is below {} minutes; each checkpoint issues"
                            + " a direct load or an overflow copy per destination table, and"
                            + " BigQuery allows 1,500 modifications per standard table per day"
                            + " (2 min = 720/day, 5 min ="
                            + " 288/day). Consider a larger interval for sustained streaming.",
                    intervalMs,
                    QUOTA_WARN_CHECKPOINT_INTERVAL.toMinutes());
        }
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
