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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

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

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.BigtableLineage;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigtable.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.committer.BigtableStagedCommitter;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.BigtableStagedWriter;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.tables.BigtableStagedTableValidator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/** Checkpoint-owned writes through a stateless writer and Flink's persisted committer collector. */
@Internal
public final class BigtableStagedSink<T>
        implements CrossVersionSink<T>,
                LineageVertexProvider,
                SupportsCommitter<BigtableCommittable>,
                SupportsPreCommitTopology<BigtableCommittable, BigtableCommittable> {
    private static final long serialVersionUID = 1L;
    private final BigtableSinkConfig<T> config;
    private final BigtableStagedOptions options;
    private final Map<String, ColumnFamilyType> expectedFamilies;
    @Nullable private final String logicalTableName;

    /** Creates the staged sink selected by the public builder. */
    public BigtableStagedSink(BigtableSinkConfig<T> config, BigtableStagedOptions options) {
        this(config, options, Map.of(), null);
    }

    private BigtableStagedSink(
            BigtableSinkConfig<T> config,
            BigtableStagedOptions options,
            Map<String, ColumnFamilyType> expectedFamilies,
            @Nullable String logicalTableName) {
        this.config = config;
        this.options = options;
        this.expectedFamilies = Map.copyOf(expectedFamilies);
        this.logicalTableName = logicalTableName;
        validate();
    }

    private void validate() {
        options.validate();
        ResourceNames.checkComponent(config.getAppProfileId(), "appProfileId");
        Preconditions.checkArgument(
                config.getCreateDisposition() == CreateDisposition.CREATE_NEVER,
                "Bigtable EXACTLY_ONCE requires CREATE_NEVER and a pre-provisioned marker family");
        Preconditions.checkArgument(
                config.getFailedMutationHandler() == FailureHandler.failJob(),
                "Bigtable EXACTLY_ONCE requires FailureHandler.failJob()");
        Preconditions.checkArgument(
                !expectedFamilies.containsKey(options.getMarkerFamily()),
                "Application families must not include markerFamily");
    }

    /** Returns a copy with Table schema validation and catalog lineage. */
    public BigtableStagedSink<T> withTableConfiguration(
            Map<String, ColumnFamilyType> families, @Nullable String tableName) {
        return new BigtableStagedSink<>(config, options, families, tableName);
    }

    /** Returns the unchanged destination and serializer configuration. */
    public BigtableSinkConfig<T> getConfig() {
        return config;
    }

    /** Returns the validated staged settings. */
    public BigtableStagedOptions getStagedOptions() {
        return options;
    }

    @Override
    public LineageVertex getLineageVertex() {
        return BigtableLineage.sink(config.getDestinationResolver(), logicalTableName);
    }

    @Override
    public CommittingSinkWriter<T, BigtableCommittable> createWriter(WriterInitContext context)
            throws IOException {
        validate();
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening Bigtable staging serializer", failure);
        } catch (Exception failure) {
            throw new IOException("Failed to open Bigtable staging serializer", failure);
        }
        if (!expectedFamilies.isEmpty()) {
            Preconditions.checkState(
                    config.getDestinationResolver() instanceof FixedDestinationResolver,
                    "Table family validation requires a fixed destination");
            new BigtableStagedTableValidator(
                            config.getEmulatorEndpoint(),
                            BigtableCredentials.loadAll(config.getServiceAccountKeyFile()))
                    .validate(
                            ((FixedDestinationResolver) config.getDestinationResolver())
                                    .getDestination(),
                            config.getAppProfileId(),
                            options.getMarkerFamily(),
                            expectedFamilies);
        }
        return new BigtableStagedWriter<>(config, options, context.metricGroup());
    }

    @Override
    public Committer<BigtableCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        validate();
        CredentialsProvider credentials =
                BigtableCredentials.loadAll(config.getServiceAccountKeyFile());
        return new BigtableStagedCommitter(
                options,
                new BigtableStagedTableValidator(config.getEmulatorEndpoint(), credentials),
                profile ->
                        new DefaultSingleRowClientFactory(
                                profile,
                                options.getRequestOptions(),
                                config.getEmulatorEndpoint(),
                                credentials),
                expectedFamilies,
                context.metricGroup());
    }

    @Override
    public SimpleVersionedSerializer<BigtableCommittable> getCommittableSerializer() {
        return new BigtableCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigtableCommittable> getWriteResultSerializer() {
        return getCommittableSerializer();
    }

    @Override
    public DataStream<CommittableMessage<BigtableCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<BigtableCommittable>> committables) {
        var environment = committables.getExecutionEnvironment();
        var configuration = environment.getConfiguration();
        var checkpoints = environment.getCheckpointConfig();
        Preconditions.checkState(
                configuration.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Bigtable EXACTLY_ONCE requires execution.runtime-mode=STREAMING");
        Preconditions.checkState(
                checkpoints.isCheckpointingEnabled(),
                "Bigtable EXACTLY_ONCE requires execution.checkpointing.interval");
        Preconditions.checkState(
                checkpoints.getCheckpointingConsistencyMode() == CheckpointingMode.EXACTLY_ONCE,
                "Bigtable EXACTLY_ONCE requires execution.checkpointing.mode=EXACTLY_ONCE");
        Preconditions.checkState(
                configuration.get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),
                "Bigtable EXACTLY_ONCE requires execution.checkpointing.checkpoints-after-tasks-finish=true");
        return committables;
    }
}
