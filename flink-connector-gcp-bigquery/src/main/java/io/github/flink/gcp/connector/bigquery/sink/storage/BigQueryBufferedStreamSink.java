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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.committer.BufferedStreamCommitter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BigQueryBufferedStreamWriter;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamServiceFactory;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamWriterState;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamWriterStateSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.WriteClientBufferedStreamServiceFactory;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.tables.TableAdmin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * The {@link WriteMethod#STORAGE_API_EXACTLY_ONCE} sink: each writer subtask appends rows to one
 * application-created BUFFERED Storage Write API stream at explicit offsets (see {@link
 * BigQueryBufferedStreamWriter}), and the committer makes each completed checkpoint's rows visible
 * by flushing every committable's stream up to its offset (see {@link BufferedStreamCommitter}) —
 * two-phase commit on Flink checkpoints.
 *
 * <p>The stream is reused across checkpoints and tracked in writer state; one {@code
 * CreateWriteStream} per writer lifetime keeps well within the API's intended usage, and {@code
 * FlushRows} once per subtask per checkpoint is far below its quota, so no checkpoint-cadence guard
 * is needed (unlike FILE_LOADS with its per-table daily load-job limit).
 *
 * <p>The execution mode must be explicit: {@link RuntimeExecutionMode#AUTOMATIC} is rejected when
 * the pre-commit topology is added, because were it to resolve to streaming with checkpointing
 * disabled, rows would buffer forever without ever being flushed. Streaming requires checkpointing
 * with {@link CheckpointingMode#EXACTLY_ONCE} and checkpoints-after-tasks-finish (the final batch
 * rides the post-finish checkpoint). {@link RuntimeExecutionMode#BATCH} is supported: the single
 * end-of-input committable is committed when the job completes.
 *
 * <p>The pre-commit topology is identity — it exists only as the graph-construction validation
 * hook. Committables need no checkpoint stamping (flush offsets are absolute) and no global routing
 * (per-stream flushes are independent), so the committer runs at the sink's parallelism.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class BigQueryBufferedStreamSink<T>
        implements CrossVersionSink<T>,
                SupportsCommitter<BufferedStreamCommittable>,
                SupportsWriterState<T, BufferedStreamWriterState>,
                SupportsPreCommitTopology<BufferedStreamCommittable, BufferedStreamCommittable> {

    private static final long serialVersionUID = 1L;

    private final BigQuerySinkConfig<T> config;
    private final BufferedStreamOptions options;
    private final BufferedStreamServiceFactory serviceFactory;

    /**
     * Creates the sink; called by {@link
     * io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkBuilder}.
     *
     * @param config the sink configuration
     * @param options the buffered-stream options
     */
    public BigQueryBufferedStreamSink(BigQuerySinkConfig<T> config, BufferedStreamOptions options) {
        this(
                config,
                options,
                new WriteClientBufferedStreamServiceFactory(config.getEmulatorEndpoint()));
    }

    @VisibleForTesting
    BigQueryBufferedStreamSink(
            BigQuerySinkConfig<T> config,
            BufferedStreamOptions options,
            BufferedStreamServiceFactory serviceFactory) {
        this.config = config;
        this.options = options;
        this.serviceFactory = serviceFactory;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<T, BufferedStreamWriterState> restoreWriter(
            WriterInitContext context, Collection<BufferedStreamWriterState> recoveredState)
            throws IOException {
        return restoreWriter(
                context, recoveredState, new BigQueryTableAdmin(config.getEmulatorRestEndpoint()));
    }

    @VisibleForTesting
    StatefulSinkWriter<T, BufferedStreamWriterState> restoreWriter(
            WriterInitContext context,
            Collection<BufferedStreamWriterState> recoveredState,
            TableAdmin tableAdmin)
            throws IOException {
        config.getFailedRowHandler().open(DefaultFailureHandlerContext.of(context));
        try {
            return new BigQueryBufferedStreamWriter<>(
                    config,
                    options,
                    serviceFactory,
                    tableAdmin,
                    context.metricGroup(),
                    context.getTaskInfo().getIndexOfThisSubtask(),
                    recoveredState);
        } catch (Throwable e) {
            // The handler is the only thing to release: the service factory opens no client until
            // the writer asks it to, and the table admin is built by the caller. Nothing downstream
            // would close it — no writer exists to do it — and Flink rebuilds the writer on every
            // restart attempt, so an opened handler would accumulate per attempt on a task manager
            // that stays alive. This is also createWriter's failure path, which delegates here.
            //
            // Throwable, not Exception: a client's first classload can fail with a
            // NoClassDefFoundError, which repeats on every attempt and would otherwise walk past
            // this guard. Precise rethrow keeps the declared throws clause honest, and it also
            // means a checked exception added to anything above stays covered.
            Closers.closeAllSuppressing(e, config.getFailedRowHandler()::close);
            throw e;
        }
    }

    @Override
    public Committer<BufferedStreamCommittable> createCommitter(CommitterInitContext context) {
        return new BufferedStreamCommitter(serviceFactory, config.getLocation(), options);
    }

    @Override
    public SimpleVersionedSerializer<BufferedStreamCommittable> getCommittableSerializer() {
        return new BufferedStreamCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BufferedStreamCommittable> getWriteResultSerializer() {
        return new BufferedStreamCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BufferedStreamWriterState> getWriterStateSerializer() {
        return new BufferedStreamWriterStateSerializer();
    }

    @Override
    public DataStream<CommittableMessage<BufferedStreamCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<BufferedStreamCommittable>> committables) {
        RuntimeExecutionMode mode =
                committables
                        .getExecutionEnvironment()
                        .getConfiguration()
                        .get(ExecutionOptions.RUNTIME_MODE);
        switch (mode) {
            case BATCH:
                break;
            case STREAMING:
                validateStreaming(
                        committables.getExecutionEnvironment().getCheckpointConfig(),
                        committables.getExecutionEnvironment().getConfiguration());
                break;
            default:
                // AUTOMATIC: were it to resolve to streaming with checkpointing disabled, no
                // flush trigger would ever come and buffered rows would stay invisible forever —
                // and that resolution is invisible here, so an explicit mode is required.
                throw new IllegalStateException(
                        WriteMethod.STORAGE_API_EXACTLY_ONCE
                                + " requires an explicit execution mode, but the runtime mode is "
                                + mode
                                + ". Set RuntimeExecutionMode.BATCH or RuntimeExecutionMode"
                                + ".STREAMING explicitly (streaming additionally requires"
                                + " checkpointing).");
        }
        // Identity: no stamping (flush offsets are absolute) and no global routing (per-stream
        // flushes are independent) — this hook exists for the validation above.
        return committables;
    }

    /** Rejects streaming setups that cannot uphold the exactly-once contract. */
    private static void validateStreaming(
            CheckpointConfig checkpointConfig, ReadableConfig configuration) {
        if (!checkpointConfig.isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    WriteMethod.STORAGE_API_EXACTLY_ONCE
                            + " in streaming execution requires checkpointing: flushes are"
                            + " triggered by checkpoint commits, so without checkpointing buffered"
                            + " rows would never become visible. Enable checkpointing"
                            + " (execution.checkpointing.interval) or run in"
                            + " RuntimeExecutionMode.BATCH.");
        }
        if (checkpointConfig.getCheckpointingConsistencyMode() != CheckpointingMode.EXACTLY_ONCE) {
            // Under AT_LEAST_ONCE alignment, records processed after a barrier are appended
            // below the barrier's flush offset and replayed after a failure — duplicates.
            throw new IllegalStateException(
                    WriteMethod.STORAGE_API_EXACTLY_ONCE
                            + " requires CheckpointingMode.EXACTLY_ONCE, but the checkpointing"
                            + " consistency mode is "
                            + checkpointConfig.getCheckpointingConsistencyMode()
                            + ".");
        }
        if (!configuration.get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)) {
            // The final batch of a bounded streaming job is committed by the checkpoint taken
            // after the tasks finished; without it the tail would buffer but never flush.
            throw new IllegalStateException(
                    WriteMethod.STORAGE_API_EXACTLY_ONCE
                            + " in streaming execution requires"
                            + " execution.checkpointing.checkpoints-after-tasks-finish.enabled:"
                            + " the final batch is flushed by the checkpoint taken after the tasks"
                            + " finish, so disabling it would silently drop the tail of a bounded"
                            + " job.");
        }
    }

    /** Returns the sink configuration. */
    public BigQuerySinkConfig<T> getConfig() {
        return config;
    }

    /** Returns the buffered-stream options. */
    public BufferedStreamOptions getOptions() {
        return options;
    }

    /**
     * Returns the factory the writer opens its streams through. Exposed so a test can see what the
     * production constructor built — the emulator endpoint reaches the write path through it, and
     * is otherwise invisible from outside a running writer.
     */
    @VisibleForTesting
    public BufferedStreamServiceFactory getServiceFactory() {
        return serviceFactory;
    }
}
