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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.lineage.internal.Lineage;
import io.github.flink.gcp.connector.base.lineage.internal.LineageIdentifiers;
import io.github.flink.gcp.connector.cloudtasks.sink.committer.CloudTasksStagedCommitter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksStagedWriter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Checkpointed named creation with a stateless staging writer and a bounded committer. The graph
 * check admits only checkpointed exactly-once streaming execution.
 *
 * @param <T> input record type
 */
@Internal
public class CloudTasksStagedCreateTaskSink<T>
        implements CrossVersionSink<T>,
                LineageVertexProvider,
                SupportsCommitter<CloudTasksCommittable>,
                SupportsPreCommitTopology<CloudTasksCommittable, CloudTasksCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudTasksStagedCreateTaskSink.class);
    private static final long serialVersionUID = 1L;
    private final CloudTasksSinkConfig<T> config;
    private final CloudTasksStagingConfig staging;
    private final CloudTasksStagedOptions options;
    @Nullable private final String logicalTableName;

    CloudTasksStagedCreateTaskSink(
            CloudTasksSinkConfig<T> config, CloudTasksStagingConfig staging) {
        this(
                config,
                CloudTasksStagedOptions.builder()
                        .maxStagedTasks(staging.getMaxStagedTasks())
                        .maxStagedBytes(staging.getMaxStagedBytes())
                        .nameRetention(staging.getNameRetention())
                        .clockSkewAllowance(staging.getClockSkewAllowance())
                        .requestTimeout(staging.getRequestTimeout())
                        .build());
    }

    CloudTasksStagedCreateTaskSink(
            CloudTasksSinkConfig<T> config, CloudTasksStagedOptions options) {
        this(config, options, null);
    }

    /** Creates the staged runtime with the planner's optional logical table identity. */
    public CloudTasksStagedCreateTaskSink(
            CloudTasksSinkConfig<T> config,
            CloudTasksStagedOptions options,
            @Nullable String logicalTableName) {
        this.logicalTableName = logicalTableName;
        this.config = Preconditions.checkNotNull(config, "config");
        this.options = Preconditions.checkNotNull(options, "stagedOptions");
        this.staging = options.toStagingConfig();
        validate();
    }

    @Override
    public LineageVertex getLineageVertex() {
        QueueDestination queue =
                ((FixedDestinationResolver) config.getDestinationResolver()).getDestination();
        var resource =
                LineageIdentifiers.cloudTasksQueue(
                        queue.getProject(), queue.getLocation(), queue.getQueue());
        return logicalTableName == null
                ? Lineage.sink(List.of(resource))
                : Lineage.tableSink(logicalTableName, resource.namespace(), List.of(resource));
    }

    /** Returns the validated sink configuration. */
    public CloudTasksSinkConfig<T> getConfig() {
        return config;
    }

    /** Returns the staging and recovery settings. */
    public CloudTasksStagedOptions getStagedOptions() {
        return options;
    }

    private void validate() {
        options.toStagingConfig();
        Preconditions.checkArgument(
                config.getDestinationResolver() instanceof FixedDestinationResolver,
                "EXACTLY_ONCE requires a fixed queue(...).");
        staging.validate();
        Preconditions.checkArgument(
                config.getFailedTaskHandler() == FailureHandler.failJob(),
                "Cloud Tasks staging requires FailureHandler.failJob()");
    }

    @Override
    public final CommittingSinkWriter<T, CloudTasksCommittable> createWriter(
            WriterInitContext context) throws IOException {
        validate();
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Cloud Tasks staging serializer.");
        } catch (Exception e) {
            throw new IOException("Failed to open the Cloud Tasks staging serializer.");
        }
        return createStagedWriter(config, staging, context);
    }

    /** Constructs only the writer; overrides can inject a clock and random source in tests. */
    protected CloudTasksStagedWriter<T> createStagedWriter(
            CloudTasksSinkConfig<T> sinkConfig,
            CloudTasksStagingConfig stagingConfig,
            WriterInitContext context) {
        return new CloudTasksStagedWriter<>(sinkConfig, stagingConfig, context.metricGroup());
    }

    @Override
    public final SimpleVersionedSerializer<CloudTasksCommittable> getCommittableSerializer() {
        return new CloudTasksCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CloudTasksCommittable> getWriteResultSerializer() {
        return new CloudTasksCommittableSerializer();
    }

    @Override
    public Committer<CloudTasksCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        validate();
        String queue =
                ((FixedDestinationResolver) config.getDestinationResolver())
                        .getDestination()
                        .toQueuePath();
        DefaultTaskCreatorFactory factory = taskCreatorFactory();
        if (options.isVerifyQueueRetention() && config.getEmulatorEndpoint() == null) {
            verifyQueueRetention(factory, queue);
        }
        TaskCreator creator = createTaskCreator(factory);
        try {
            return new CloudTasksStagedCommitter(
                    queue,
                    options,
                    config.getWriterOptions(),
                    creator,
                    committerClock(),
                    context.metricGroup());
        } catch (Throwable failure) {
            Closers.closeAllSuppressing(failure, creator::close);
            throw failure;
        }
    }

    /** Builds the creation and retention-readback factory from the same authentication settings. */
    protected DefaultTaskCreatorFactory taskCreatorFactory() {
        return new DefaultTaskCreatorFactory(
                config.getServiceAccountKeyFile(),
                config.getEmulatorEndpoint(),
                config.getWriterOptions().getChannelPoolSize());
    }

    /**
     * Constructs a fresh creator per committer incarnation; test adapters may inject a registry.
     */
    protected TaskCreator createTaskCreator(DefaultTaskCreatorFactory factory) throws IOException {
        return factory.create();
    }

    /** Supplies the wall clock used at each send authorization. */
    protected TimeSource committerClock() {
        return TimeSource.SYSTEM;
    }

    /** Reads the fixed queue's retention before creating any task. */
    protected void verifyQueueRetention(DefaultTaskCreatorFactory factory, String queue)
            throws IOException {
        if (factory.readQueueRetention(queue).compareTo(options.getNameRetention()) < 0) {
            throw new IOException(
                    "Cloud Tasks queue tombstoneTtl is shorter than nameRetention; configure the pre-provisioned queue or lower nameRetention. The connector never changes queue policy.");
        }
    }

    @Override
    public DataStream<CommittableMessage<CloudTasksCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<CloudTasksCommittable>> committables) {
        var environment = committables.getExecutionEnvironment();
        var configuration = environment.getConfiguration();
        var checkpoints = environment.getCheckpointConfig();
        Preconditions.checkState(
                configuration.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Cloud Tasks EXACTLY_ONCE requires execution.runtime-mode=STREAMING; BATCH and AUTOMATIC are unsupported.");
        Preconditions.checkState(
                checkpoints.isCheckpointingEnabled(),
                "Cloud Tasks EXACTLY_ONCE requires checkpointing; set execution.checkpointing.interval.");
        Preconditions.checkState(
                checkpoints.getCheckpointingConsistencyMode() == CheckpointingMode.EXACTLY_ONCE,
                "Cloud Tasks EXACTLY_ONCE requires execution.checkpointing.mode=EXACTLY_ONCE.");
        Preconditions.checkState(
                configuration.get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),
                "Cloud Tasks EXACTLY_ONCE requires execution.checkpointing.checkpoints-after-tasks-finish=true.");
        LOG.info(
                "Cloud Tasks staging: maxConcurrentCheckpoints={}, tolerableCheckpointFailures={}. These settings do not bound retained batches. Size committer heap from peak pendingCommittables; retain externalized checkpoints on cancellation and use a bounded restart strategy.",
                checkpoints.getMaxConcurrentCheckpoints(),
                checkpoints.getTolerableCheckpointFailureNumber());
        return committables;
    }
}
