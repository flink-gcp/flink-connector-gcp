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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Everything the batch source was built with, assembled by the builder and carried into the job
 * graph.
 *
 * <p>The partition hints travel as the client's own {@link PartitionOptions}, which is serializable
 * and whose unset fields already mean "no hint" — so the builder's two knobs fold into one value
 * here rather than into two longs that would have to be folded back at the call site.
 *
 * @param <T> the record type the deserializer produces
 */
@Internal
public final class SpannerSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    private final SpannerReadOperation readOperation;
    private final SpannerStructDeserializationSchema<T> deserializer;
    private final TimestampBound timestampBound;
    private final PartitionOptions partitionOptions;
    private final boolean dataBoostEnabled;
    @Nullable private final SpannerRpcPriority rpcPriority;
    private final PartitionPlanner planner;
    private final StructStreamOpener opener;
    private final int maxRecordsPerFetch;

    SpannerSourceConfig(
            SpannerDatabase database,
            SpannerReadOperation readOperation,
            SpannerStructDeserializationSchema<T> deserializer,
            TimestampBound timestampBound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority,
            PartitionPlanner planner,
            StructStreamOpener opener,
            int maxRecordsPerFetch) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.readOperation =
                Preconditions.checkNotNull(readOperation, "readOperation must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.timestampBound =
                Preconditions.checkNotNull(timestampBound, "timestampBound must not be null");
        this.partitionOptions =
                Preconditions.checkNotNull(partitionOptions, "partitionOptions must not be null");
        this.dataBoostEnabled = dataBoostEnabled;
        this.rpcPriority = rpcPriority;
        this.planner = Preconditions.checkNotNull(planner, "planner must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        Preconditions.checkArgument(
                maxRecordsPerFetch > 0,
                "maxRecordsPerFetch must be positive, but was %s",
                maxRecordsPerFetch);
        this.maxRecordsPerFetch = maxRecordsPerFetch;
    }

    /**
     * Returns the database to read.
     *
     * @return the database
     */
    public SpannerDatabase getDatabase() {
        return database;
    }

    /**
     * Returns what to read.
     *
     * @return the read operation
     */
    public SpannerReadOperation getReadOperation() {
        return readOperation;
    }

    /**
     * Returns the deserializer turning rows into records.
     *
     * @return the deserializer
     */
    public SpannerStructDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /**
     * Returns the snapshot to read at.
     *
     * @return the timestamp bound
     */
    public TimestampBound getTimestampBound() {
        return timestampBound;
    }

    /**
     * Returns the partition-count and partition-size hints.
     *
     * @return the partition options
     */
    public PartitionOptions getPartitionOptions() {
        return partitionOptions;
    }

    /**
     * Returns whether the read runs on Data Boost's independent compute.
     *
     * @return whether Data Boost is enabled
     */
    public boolean isDataBoostEnabled() {
        return dataBoostEnabled;
    }

    /**
     * Returns the priority Spanner schedules the reads at.
     *
     * @return the priority, or {@code null} to leave it unset
     */
    @Nullable
    public SpannerRpcPriority getRpcPriority() {
        return rpcPriority;
    }

    /**
     * Returns the seam the enumerator plans through.
     *
     * @return the planner
     */
    public PartitionPlanner getPlanner() {
        return planner;
    }

    /**
     * Returns the seam the readers read through.
     *
     * @return the opener
     */
    public StructStreamOpener getOpener() {
        return opener;
    }

    /**
     * Returns how many rows one fetch hands to the task thread.
     *
     * @return the per-fetch row cap
     */
    public int getMaxRecordsPerFetch() {
        return maxRecordsPerFetch;
    }
}
