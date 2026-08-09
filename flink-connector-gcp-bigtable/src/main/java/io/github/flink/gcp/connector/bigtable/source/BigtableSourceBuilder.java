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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableScanEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.DataClientRowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.BigtableSplitReader;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.DataClientRowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds a {@link BigtableSource}.
 *
 * <p>Every range this builder is given is copied on the way in and validated at {@link #build()},
 * so a configuration mistake fails where the job is assembled rather than on a TaskManager once
 * rows begin to flow.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public class BigtableSourceBuilder<T> {

    private @Nullable TableDestination table;
    private @Nullable BigtableRowDeserializationSchema<T> deserializer;
    private final List<ByteStringRange> ranges = new ArrayList<>();
    private @Nullable Filters.Filter filter;
    private @Nullable String appProfileId;
    private @Nullable EmulatorEndpoint emulatorEndpoint;
    private @Nullable RowKeySampler sampler;
    private @Nullable RowStreamOpener opener;
    private int maxRowsPerFetch = BigtableSplitReader.DEFAULT_MAX_ROWS_PER_FETCH;

    BigtableSourceBuilder() {}

    /**
     * Sets the table to read. Required.
     *
     * @param table the table
     * @return this builder
     */
    public BigtableSourceBuilder<T> table(TableDestination table) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        return this;
    }

    /**
     * Sets the deserializer turning rows into records. Required.
     *
     * @param deserializer the deserializer
     * @return this builder
     */
    public BigtableSourceBuilder<T> deserializer(BigtableRowDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Adds a row-key range to read. Repeatable; with no range set at all the whole table is read.
     *
     * <p>Overlapping ranges are merged at {@link #build()} rather than rejected — nested prefixes
     * are easy to configure by accident — but an <em>empty</em> range is rejected, because a range
     * that reads nothing under a successful job looks exactly like a job with nothing to read.
     *
     * @param range the range; copied, so later changes to it do not affect the source
     * @return this builder
     */
    public BigtableSourceBuilder<T> rowRange(ByteStringRange range) {
        Preconditions.checkNotNull(range, "range must not be null");
        ranges.add(RowRanges.copyOf(range));
        return this;
    }

    /**
     * Adds a row-key range to read, from an inclusive start to an exclusive end.
     *
     * @param startClosed the first row key to read
     * @param endOpen the first row key past the range
     * @return this builder
     */
    public BigtableSourceBuilder<T> rowRange(ByteString startClosed, ByteString endOpen) {
        checkBoundKey(startClosed, "startClosed");
        checkBoundKey(endOpen, "endOpen");
        return rowRange(ByteStringRange.unbounded().startClosed(startClosed).endOpen(endOpen));
    }

    /**
     * Adds a row-key range to read, from an inclusive start to an exclusive end, given as UTF-8
     * text.
     *
     * @param startClosed the first row key to read
     * @param endOpen the first row key past the range
     * @return this builder
     */
    public BigtableSourceBuilder<T> rowRange(String startClosed, String endOpen) {
        Preconditions.checkNotNull(startClosed, "startClosed must not be null");
        Preconditions.checkNotNull(endOpen, "endOpen must not be null");
        return rowRange(ByteString.copyFromUtf8(startClosed), ByteString.copyFromUtf8(endOpen));
    }

    /**
     * Rejects an empty bound key, which the client library would silently widen to unbounded.
     *
     * <p>Without this, {@code rowRange("", "")} — the shape a start/end pair takes when the
     * configuration it came from defaulted to the empty string — becomes a scan of the whole table,
     * and {@link RowRanges#isEmpty} cannot object because an unbounded side is never empty. The
     * whole table is what {@link #prefix} means by an empty argument; it is not what these
     * overloads mean.
     */
    private static void checkBoundKey(ByteString key, String name) {
        Preconditions.checkNotNull(key, name + " must not be null");
        Preconditions.checkArgument(
                !key.isEmpty(),
                "%s must not be empty: an empty row key is not a bound, and would widen the range"
                        + " to the whole table. Leave the range unset to read every row.",
                name);
    }

    /**
     * Adds every row whose key starts with a prefix. Repeatable, and sugar for the range that
     * prefix describes.
     *
     * <p>The conversion is the client library's, which handles the two cases a hand-rolled one gets
     * wrong: a prefix that is all {@code 0xFF} bytes has no successor and becomes a range running
     * to the end of the table, and a prefix ending in {@code 0xFF} bytes carries the increment into
     * an earlier byte. An empty prefix is the whole table.
     *
     * @param prefix the row-key prefix
     * @return this builder
     */
    public BigtableSourceBuilder<T> prefix(ByteString prefix) {
        Preconditions.checkNotNull(prefix, "prefix must not be null");
        return rowRange(ByteStringRange.prefix(prefix));
    }

    /**
     * Adds every row whose key starts with a prefix, given as UTF-8 text.
     *
     * @param prefix the row-key prefix
     * @return this builder
     */
    public BigtableSourceBuilder<T> prefix(String prefix) {
        Preconditions.checkNotNull(prefix, "prefix must not be null");
        return rowRange(ByteStringRange.prefix(prefix));
    }

    /**
     * Sets the server-side filter every read applies. Optional; last writer wins.
     *
     * <p>One filter, applied identically to every split, which is safe by construction: Bigtable's
     * filter language has no row-count limiter — its limit and offset filters count cells within a
     * row — so nothing expressible here can depend on how the key space was divided.
     *
     * <p>Per-cell shaping is all expressible through a filter: which families and qualifiers to
     * return, which timestamp window, how many versions of a cell. There are no separate knobs for
     * those, and a filter is also the cheapest thing a scan can carry, since what it excludes never
     * leaves the server.
     *
     * @param filter the filter, built through the client's {@code Filters} factory
     * @return this builder
     */
    public BigtableSourceBuilder<T> filter(Filters.Filter filter) {
        this.filter = Preconditions.checkNotNull(filter, "filter must not be null");
        return this;
    }

    /**
     * Sets the application profile every call routes through. Optional; the instance's default
     * profile is used when this is not set.
     *
     * <p>It is a source option rather than part of {@link TableDestination} because it chooses a
     * path to the data, not the data's address. A Data Boost profile is named here like any other.
     *
     * @param appProfileId the application profile id
     * @return this builder
     */
    public BigtableSourceBuilder<T> appProfileId(String appProfileId) {
        Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(
                !appProfileId.trim().isEmpty(), "appProfileId must not be blank");
        this.appProfileId = appProfileId;
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
    public BigtableSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
        return this;
    }

    /** Replaces the sampler the enumerator plans with. For tests that must not reach a service. */
    @VisibleForTesting
    BigtableSourceBuilder<T> sampler(RowKeySampler sampler) {
        this.sampler = sampler;
        return this;
    }

    /** Replaces the opener the readers read through. For tests that must not reach a service. */
    @VisibleForTesting
    BigtableSourceBuilder<T> opener(RowStreamOpener opener) {
        this.opener = opener;
        return this;
    }

    /**
     * Lowers how many rows one fetch hands to the task thread.
     *
     * <p>Not a public option, and not one because nothing about it is workload-dependent: the
     * client hands rows over one at a time, so the cap bounds a batch rather than a buffer. Tests
     * lower it so that a checkpoint can land in the middle of a range that holds only a few rows.
     */
    @VisibleForTesting
    BigtableSourceBuilder<T> maxRowsPerFetch(int maxRowsPerFetch) {
        this.maxRowsPerFetch = maxRowsPerFetch;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     * @throws IllegalStateException if a required option was not set
     * @throws IllegalArgumentException if a configured range is empty, or if the filter is larger
     *     than the service accepts
     */
    public Source<T, RowRangeSplit, BigtableScanEnumeratorState> build() {
        Preconditions.checkState(table != null, "A table is required: set table(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required: set deserializer(...).");
        checkFilterFits();
        return new BigtableReadRowsSource<>(
                new BigtableSourceConfig<>(
                        table,
                        deserializer,
                        rangesToRead(),
                        filter,
                        appProfileId,
                        sampler != null
                                ? sampler
                                : new DataClientRowKeySampler(appProfileId, emulatorEndpoint),
                        opener != null
                                ? opener
                                : new DataClientRowStreamOpener(appProfileId, emulatorEndpoint),
                        maxRowsPerFetch));
    }

    /**
     * Returns the ranges to read: what was configured, checked and merged, or the whole table.
     *
     * <p>Merging rather than rejecting overlaps matters more than it looks: two overlapping ranges
     * would otherwise be cut into splits held by two different subtasks, and the rows they share
     * would be emitted twice by a run that succeeded.
     */
    private List<ByteStringRange> rangesToRead() {
        if (ranges.isEmpty()) {
            return Collections.singletonList(ByteStringRange.unbounded());
        }
        for (ByteStringRange range : ranges) {
            Preconditions.checkArgument(
                    !RowRanges.isEmpty(range),
                    "The row range %s holds no row key, so it would read nothing.",
                    RowRanges.format(range));
        }
        return RowRanges.coalesce(ranges);
    }

    /**
     * Rejects a filter the service would refuse, where the job is assembled.
     *
     * <p>The check is made by building a throwaway query — <b>not dead code</b>, and not to be
     * simplified away: the client's own size precondition runs inside {@code Query#filter}, and
     * this is the only way to reach it through public API. A filter too large for the service is
     * otherwise discovered by a TaskManager, once per subtask, as a restart loop.
     */
    private void checkFilterFits() {
        if (filter == null) {
            return;
        }
        Query.create(TableId.of(table.getTable())).filter(filter);
    }
}
