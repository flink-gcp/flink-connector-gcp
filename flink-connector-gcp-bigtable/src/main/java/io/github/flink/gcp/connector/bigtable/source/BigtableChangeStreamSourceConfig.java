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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamRestoreResolver;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;

/**
 * Everything the Change Streams source was built with, assembled by the builder and carried into
 * the job graph.
 *
 * <p>Every value is reached through a getter, including from inside this package. The class offers
 * one access mechanism rather than two, so what the configuration exposes can be read from its
 * getters alone.
 *
 * <p>An unset value is a {@code null} field carrying {@link Nullable}, never an {@code Optional}
 * one: {@code Optional} models a result that may be absent, while a nullable field is configuration
 * nobody set.
 *
 * <p>The seams — the opener, the restore resolver and the coordinator client — are held as the
 * objects the source hands to its reader and enumerator, which is why this configuration is only as
 * serializable as they are.
 *
 * @param <T> the record type the deserializer produces
 */
@Internal
public final class BigtableChangeStreamSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final BigtableChangeStreamDeserializationSchema<T> deserializer;
    private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    private final StartPosition startPosition;
    @Nullable private final StartPosition resumeFallback;
    @Nullable private final Instant endTime;
    private final int maxConcurrentStreamsPerSubtask;
    private final BigtableChangeStreamMutationFilter mutationFilter;
    private final ChangeStreamOpener opener;
    private final ChangeStreamRestoreResolver restoreResolver;
    @Nullable private final ChangeStreamCoordinatorClient coordinatorClient;

    BigtableChangeStreamSourceConfig(
            TableDestination table,
            BigtableChangeStreamDeserializationSchema<T> deserializer,
            String appProfileId,
            @Nullable String serviceAccountKeyFile,
            StartPosition startPosition,
            @Nullable StartPosition resumeFallback,
            @Nullable Instant endTime,
            int maxConcurrentStreamsPerSubtask,
            BigtableChangeStreamMutationFilter mutationFilter,
            ChangeStreamOpener opener,
            ChangeStreamRestoreResolver restoreResolver,
            @Nullable ChangeStreamCoordinatorClient coordinatorClient) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.appProfileId =
                Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.startPosition =
                Preconditions.checkNotNull(startPosition, "startPosition must not be null");
        this.resumeFallback = resumeFallback;
        this.endTime = endTime;
        Preconditions.checkArgument(
                maxConcurrentStreamsPerSubtask > 0,
                "maxConcurrentStreamsPerSubtask must be positive: %s",
                maxConcurrentStreamsPerSubtask);
        this.maxConcurrentStreamsPerSubtask = maxConcurrentStreamsPerSubtask;
        this.mutationFilter =
                Preconditions.checkNotNull(mutationFilter, "mutationFilter must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        this.restoreResolver =
                Preconditions.checkNotNull(restoreResolver, "restoreResolver must not be null");
        this.coordinatorClient = coordinatorClient;
    }

    /** Returns the table whose change stream is read. */
    public TableDestination getTable() {
        return table;
    }

    /** Returns the deserializer turning change-stream mutations into records. */
    public BigtableChangeStreamDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /** Returns the single-cluster application profile the change stream is read through. */
    public String getAppProfileId() {
        return appProfileId;
    }

    /** Returns the service-account key-file path, or {@code null} to use ADC. */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** Returns the position a fresh start reads from. */
    public StartPosition getStartPosition() {
        return startPosition;
    }

    /**
     * Returns the position a partition restarts from when its restored position has fallen out of
     * retention, or {@code null} when none was configured, in which case such a restore fails the
     * job rather than advancing over unavailable records.
     */
    @Nullable
    public StartPosition getResumeFallback() {
        return resumeFallback;
    }

    /** Returns the instant the read stops at, or {@code null} for an unbounded read. */
    @Nullable
    public Instant getEndTime() {
        return endTime;
    }

    /** Returns the most partition streams one source subtask keeps open at a time. */
    public int getMaxConcurrentStreamsPerSubtask() {
        return maxConcurrentStreamsPerSubtask;
    }

    /** Returns the entry filter applied to every mutation before it is deserialized. */
    public BigtableChangeStreamMutationFilter getMutationFilter() {
        return mutationFilter;
    }

    /** Returns the opener a reader reads partitions through; the reader owns and closes it. */
    public ChangeStreamOpener getOpener() {
        return opener;
    }

    /** Returns the resolver a reader checks a restored split against; the reader owns it. */
    public ChangeStreamRestoreResolver getRestoreResolver() {
        return restoreResolver;
    }

    /**
     * Returns the coordinator client the enumerator plans with, or {@code null} to build the
     * default one from the table, application profile and credentials configured above.
     */
    @Nullable
    public ChangeStreamCoordinatorClient getCoordinatorClient() {
        return coordinatorClient;
    }
}
