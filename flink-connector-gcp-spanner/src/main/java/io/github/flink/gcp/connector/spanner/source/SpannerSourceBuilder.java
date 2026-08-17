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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.BatchClientPartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.reader.BatchClientStructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerSplitReader;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import javax.annotation.Nullable;

/**
 * Builds a {@link SpannerSource}.
 *
 * <p>Everything is validated at {@link #build()} or at the setter that took it, so a configuration
 * mistake fails where the job is assembled rather than on a JobManager once the read is planned.
 *
 * @param <T> the record type produced
 */
@Public
public class SpannerSourceBuilder<T> {

    private @Nullable SpannerDatabase database;
    private @Nullable SpannerReadOperation readOperation;
    private @Nullable SpannerStructDeserializationSchema<T> deserializer;
    private TimestampBound timestampBound = TimestampBound.strong();
    private @Nullable Long maxPartitions;
    private @Nullable Long partitionSizeBytes;
    private boolean dataBoostEnabled;
    private @Nullable SpannerRpcPriority rpcPriority;
    private @Nullable String serviceAccountKeyFile;
    private @Nullable EmulatorEndpoint emulatorEndpoint;
    private @Nullable PartitionPlanner planner;
    private @Nullable StructStreamOpener opener;
    private int maxRecordsPerFetch = SpannerSplitReader.DEFAULT_MAX_ROWS_PER_FETCH;

    SpannerSourceBuilder() {}

    /**
     * Sets the database to read. Required.
     *
     * @param database the database
     * @return this builder
     */
    public SpannerSourceBuilder<T> database(SpannerDatabase database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets what to read: a query, or a table with its columns and key set. Required.
     *
     * @param readOperation the read operation
     * @return this builder
     */
    public SpannerSourceBuilder<T> readOperation(SpannerReadOperation readOperation) {
        this.readOperation =
                Preconditions.checkNotNull(readOperation, "readOperation must not be null");
        return this;
    }

    /**
     * Sets the deserializer turning rows into records. Required.
     *
     * @param deserializer the deserializer
     * @return this builder
     */
    public SpannerSourceBuilder<T> deserializer(
            SpannerStructDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Sets the snapshot to read at. Optional; the default is {@link TimestampBound#strong()}, the
     * latest committed data.
     *
     * <p>Only {@code strong()}, {@code ofReadTimestamp} and {@code ofExactStaleness} can bound a
     * batch read. The other two modes are refused here rather than on a JobManager: Spanner
     * restricts them to single-use transactions, and a batch read is by construction a multi-use
     * one — every partition rejoins the same transaction.
     *
     * <p>A stale read is cheaper for the service to serve and can be answered by any replica; a
     * strong one may have to reach the leader. Nothing about the source changes either way — every
     * partition is read at the one timestamp the plan fixed.
     *
     * @param timestampBound the bound
     * @return this builder
     * @throws IllegalArgumentException if the bound is one a batch read cannot take
     */
    public SpannerSourceBuilder<T> timestampBound(TimestampBound timestampBound) {
        Preconditions.checkNotNull(timestampBound, "timestampBound must not be null");
        TimestampBound.Mode mode = timestampBound.getMode();
        Preconditions.checkArgument(
                mode != TimestampBound.Mode.MAX_STALENESS
                        && mode != TimestampBound.Mode.MIN_READ_TIMESTAMP,
                "The timestamp bound mode %s cannot bound a batch read: Spanner allows it only on"
                        + " a single-use transaction, and every partition of a batch read rejoins"
                        + " one shared transaction. Use strong(), ofReadTimestamp(...) or"
                        + " ofExactStaleness(...).",
                mode);
        this.timestampBound = timestampBound;
        return this;
    }

    /**
     * Hints how many partitions the read should be divided into. Optional; unset leaves the choice
     * entirely to the service.
     *
     * <p>A hint, and Spanner documents it as one: it may return more or fewer. Setting it to the
     * job's parallelism is the reasonable thing to ask for, not something to rely on having got.
     *
     * @param maxPartitions the desired maximum number of partitions
     * @return this builder
     * @throws IllegalArgumentException if the value is not positive
     */
    public SpannerSourceBuilder<T> maxPartitions(long maxPartitions) {
        Preconditions.checkArgument(
                maxPartitions > 0, "maxPartitions must be positive, but was %s", maxPartitions);
        this.maxPartitions = maxPartitions;
        return this;
    }

    /**
     * Hints how much data each partition should cover. Optional; unset leaves the choice entirely
     * to the service.
     *
     * <p>A hint, like {@link #maxPartitions(long)}, and the two are asked for together at the
     * discretion rather than combined by this connector.
     *
     * @param partitionSizeBytes the desired size of one partition, in bytes
     * @return this builder
     * @throws IllegalArgumentException if the value is not positive
     */
    public SpannerSourceBuilder<T> partitionSizeBytes(long partitionSizeBytes) {
        Preconditions.checkArgument(
                partitionSizeBytes > 0,
                "partitionSizeBytes must be positive, but was %s",
                partitionSizeBytes);
        this.partitionSizeBytes = partitionSizeBytes;
        return this;
    }

    /**
     * Runs the read on Data Boost's independent compute. Optional; off by default.
     *
     * <p>Data Boost serves a partitioned read from compute that is not the instance's, so a large
     * scan does not contend with the workload the instance is serving. Three things come with it:
     * the caller needs {@code spanner.databases.useDataBoost} on the database, the read is billed
     * separately, and its concurrency has a quota of its own — so {@code RESOURCE_EXHAUSTED} is a
     * shape a boosted read can meet that an ordinary one does not.
     *
     * @param dataBoostEnabled whether to enable Data Boost
     * @return this builder
     */
    public SpannerSourceBuilder<T> dataBoostEnabled(boolean dataBoostEnabled) {
        this.dataBoostEnabled = dataBoostEnabled;
        return this;
    }

    /**
     * Sets the priority Spanner schedules the reads at. Optional; unset leaves the service's own
     * handling in place, which is the same as {@code HIGH}.
     *
     * <p>{@code LOW} is what a backfill of a large table wants: Spanner sheds low-priority work
     * first when an instance is at capacity, so the read yields to the traffic the instance is
     * serving rather than competing with it. {@code MEDIUM} is a step down from the default rather
     * than a restatement of it, since Spanner treats an unspecified priority as {@code HIGH}.
     *
     * <p>The priority applies to the reads that move the rows, which is where a job's load on the
     * instance actually is. It does not apply to the one call that plans the partitions.
     *
     * <p>Not a substitute for {@link #dataBoostEnabled(boolean)}: a low-priority read still runs on
     * the instance's own compute and still competes for it, while Data Boost does not use it at
     * all. Lowering the priority costs nothing extra; Data Boost is billed separately.
     *
     * @param rpcPriority the priority
     * @return this builder
     */
    public SpannerSourceBuilder<T> rpcPriority(SpannerRpcPriority rpcPriority) {
        this.rpcPriority = Preconditions.checkNotNull(rpcPriority, "rpcPriority must not be null");
        return this;
    }

    /**
     * Authenticates the source with the service-account JSON key at the given path instead of
     * application-default credentials. The JobManager reads the file when it creates or restores
     * the enumerator, and each TaskManager reads it when it creates a reader. Every eligible
     * process must therefore see the same path. Optional; when unset the real-service path uses
     * application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public SpannerSourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the source at an emulator, over a plaintext channel with no credentials. Never
     * production.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public SpannerSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /** Replaces the planner the enumerator plans with. For tests that must not reach a service. */
    @VisibleForTesting
    SpannerSourceBuilder<T> planner(PartitionPlanner planner) {
        this.planner = planner;
        return this;
    }

    /** Replaces the opener the readers read through. For tests that must not reach a service. */
    @VisibleForTesting
    SpannerSourceBuilder<T> opener(StructStreamOpener opener) {
        this.opener = opener;
        return this;
    }

    /**
     * Lowers how many rows one fetch hands to the task thread.
     *
     * <p>Not a public option, and not one because nothing about it is workload-dependent: the
     * client hands rows over one at a time, so the cap bounds a batch rather than a buffer. Tests
     * lower it so that a wake-up can land in the middle of a partition that holds only a few rows.
     */
    @VisibleForTesting
    SpannerSourceBuilder<T> maxRecordsPerFetch(int maxRecordsPerFetch) {
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     * @throws IllegalStateException if a required option was not set
     */
    public Source<T, PartitionSplit, SpannerBatchEnumeratorState> build() {
        Preconditions.checkState(database != null, "A database is required: set database(...).");
        Preconditions.checkState(
                readOperation != null, "A read operation is required: set readOperation(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required: set deserializer(...).");
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new SpannerBatchReadSource<>(
                new SpannerSourceConfig<>(
                        database,
                        readOperation,
                        deserializer,
                        timestampBound,
                        partitionOptions(),
                        dataBoostEnabled,
                        rpcPriority,
                        serviceAccountKeyFile,
                        planner != null
                                ? planner
                                : new BatchClientPartitionPlanner(
                                        database, emulatorEndpoint, serviceAccountKeyFile),
                        opener != null
                                ? opener
                                : new BatchClientStructStreamOpener(
                                        database, emulatorEndpoint, serviceAccountKeyFile),
                        maxRecordsPerFetch));
    }

    /**
     * Assembles the partition hints.
     *
     * <p>A hint that was not set is not passed at all. The client's builder rejects a non-positive
     * value, and its default instance leaves both fields at zero, which is the wire form for "no
     * preference" — so passing a zero explicitly would say the same thing in a way the next reader
     * of this code would have to check.
     */
    private PartitionOptions partitionOptions() {
        if (maxPartitions == null && partitionSizeBytes == null) {
            return PartitionOptions.getDefaultInstance();
        }
        PartitionOptions.Builder options = PartitionOptions.newBuilder();
        if (maxPartitions != null) {
            options.setMaxPartitions(maxPartitions);
        }
        if (partitionSizeBytes != null) {
            options.setPartitionSizeBytes(partitionSizeBytes);
        }
        return options.build();
    }
}
