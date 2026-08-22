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

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySamplerFactory;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Everything the scan source was built with, assembled by the builder and carried into the job
 * graph.
 *
 * <p><b>No {@code Query} field, and none may be added.</b> A {@code Query} is the client's request
 * object, and holding one here would be wrong in three ways that a serialization round-trip does
 * not reveal: it cannot be read back, since its target-id accessor is internal, it exposes no row
 * set and its bound is only the minimal range enclosing what it holds — so nothing could log it,
 * truncate it at a resume point, or compare two of them; its payload is the {@code ReadRowsRequest}
 * wire form, which would pin a vendor format into the connector's own state; and it is mutable with
 * a {@code transient} builder, so identity across a restore is not a property anyone should have to
 * reason about. The ranges and the filter are held instead, and the query is built per read.
 *
 * <p>Ranges are copied in and copied out. The client's {@code ByteStringRange} is mutable and its
 * mutators return the receiver, so a shared reference would let a caller change a running job's
 * plan.
 *
 * @param <T> the record type the deserializer produces
 */
@Internal
public final class BigtableSourceConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableDestination table;
    private final BigtableRowDeserializationSchema<T> deserializer;
    private final List<ByteStringRange> ranges;
    @Nullable private final Filters.Filter filter;
    @Nullable private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    private final RowKeySamplerFactory samplerFactory;
    private final RowStreamOpener opener;
    private final int maxRowsPerFetch;

    BigtableSourceConfig(
            TableDestination table,
            BigtableRowDeserializationSchema<T> deserializer,
            List<ByteStringRange> ranges,
            @Nullable Filters.Filter filter,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            RowKeySamplerFactory samplerFactory,
            RowStreamOpener opener,
            int maxRowsPerFetch) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        Preconditions.checkNotNull(ranges, "ranges must not be null");
        Preconditions.checkArgument(
                !ranges.isEmpty(),
                "ranges must not be empty; a scan with no configured range covers the whole table"
                        + " and is represented by one unbounded range");
        this.ranges = Collections.unmodifiableList(RowRanges.copyAll(ranges));
        this.filter = filter;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.samplerFactory =
                Preconditions.checkNotNull(samplerFactory, "samplerFactory must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        Preconditions.checkArgument(
                maxRowsPerFetch > 0, "maxRowsPerFetch must be positive: %s", maxRowsPerFetch);
        this.maxRowsPerFetch = maxRowsPerFetch;
    }

    /** Returns the table being read. */
    public TableDestination getTable() {
        return table;
    }

    /** Returns the deserializer turning rows into records. */
    public BigtableRowDeserializationSchema<T> getDeserializer() {
        return deserializer;
    }

    /**
     * Returns the ranges to read, normalised and coalesced, in key order and never empty.
     *
     * <p>Fresh copies, because the ranges are mutable and this configuration is shared by every
     * component of the source.
     */
    public List<ByteStringRange> getRanges() {
        return RowRanges.copyAll(ranges);
    }

    /** Returns the server-side filter to apply, or {@code null} when none was configured. */
    @Nullable
    public Filters.Filter getFilter() {
        return filter;
    }

    /** Returns the application profile to route through, or {@code null} for the default. */
    @Nullable
    public String getAppProfileId() {
        return appProfileId;
    }

    /** Returns the service-account key-file path, or {@code null} to use ADC. */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /**
     * Returns the factory the source mints one sampler per enumerator from.
     *
     * <p>A factory rather than a sampler because the JobManager holds one source object for a job's
     * whole life, so a sampler here would be shared by every enumerator a coordinator reset builds
     * and the first teardown would refuse every later one ({@code docs/adr/0128}).
     */
    public RowKeySamplerFactory getSamplerFactory() {
        return samplerFactory;
    }

    /** Returns the opener the readers read through; a reader owns and closes it. */
    public RowStreamOpener getOpener() {
        return opener;
    }

    /** Returns the most rows one fetch hands to the task thread. */
    public int getMaxRowsPerFetch() {
        return maxRowsPerFetch;
    }
}
