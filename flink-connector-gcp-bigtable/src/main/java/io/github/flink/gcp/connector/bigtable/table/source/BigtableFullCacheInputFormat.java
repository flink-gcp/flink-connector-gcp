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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.LeasedBigtableClient;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The single-split bounded scan Flink's full lookup cache uses to load all projected rows.
 *
 * <p>Its client keeps or drops the built-in Cloud Monitoring export as configured, but exports
 * nothing to Flink: Flink's full cache configures and opens its copy of this format with no runtime
 * context, so no metric group reaches it (Flink 2.2.1, {@code InputFormatCacheLoader}).
 *
 * <p>Its client holds a lease on the connector's gRPC event-loop group from {@link
 * #open(GenericInputSplit)} to {@link #close()} (ADR-0208), and both run inside {@link
 * GrpcEventLoops#interruptibleReleases()}: Flink stops a load by interrupting it and fails the
 * lookup's close when the load has not ended 10 s later ({@code CacheLoader.close()}), so a release
 * of the connector's last lease on an interrupted load stops waiting for the group once the wait's
 * first second has passed (#2269).
 */
@Internal
final class BigtableFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private final TableDestination destination;
    private final Filters.Filter filter;
    private final List<ByteStringRange> ranges;
    @Nullable private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    private final RowToRowDataConverter converter;
    @Nullable private final RowStreamOpener rowStreamOpener;
    private final boolean exportClientMetricsToCloudMonitoring;

    /** The client and its event-loop lease (ADR-0208), from {@link #open} to {@link #close}. */
    @Nullable private transient LeasedBigtableClient<BigtableDataClient> client;

    private transient Iterator<ByteStringRange> remainingRanges = Collections.emptyIterator();
    private transient Iterator<Row> rows = Collections.emptyIterator();

    /**
     * The range's read, kept so that a load stopped part-way can cancel it: the data client's close
     * waits up to six minutes for its classic calls still in flight and cancels none of them, so an
     * abandoned stream would hold that close (#2264). A range whose endpoints are equal and both
     * closed is read as a point read, whose stream completes with its row; its call, still waiting
     * for its final status, then ignores the cancellation, and the load may already have moved on
     * to the next range.
     */
    @Nullable private transient ServerStream<Row> stream;

    BigtableFullCacheInputFormat(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            TrailingBytes trailingBytes,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable RowStreamOpener rowStreamOpener,
            boolean exportClientMetricsToCloudMonitoring) {
        this.destination = destination;
        this.filter = filter;
        this.ranges = ranges;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.converter =
                new RowToRowDataConverter(
                        schema, projectedFields, nullStringLiteral, trailingBytes);
        this.rowStreamOpener = rowStreamOpener;
        this.exportClientMetricsToCloudMonitoring = exportClientMetricsToCloudMonitoring;
    }

    Filters.Filter getFilter() {
        return filter;
    }

    List<ByteStringRange> getRanges() {
        return RowRanges.copyAll(ranges);
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        try (GrpcEventLoops.InterruptibleReleases releases =
                GrpcEventLoops.interruptibleReleases()) {
            openLoad(split);
        }
    }

    private void openLoad(GenericInputSplit split) throws IOException {
        super.open(split);
        if (split.getSplitNumber() != 0) {
            rows = Collections.emptyIterator();
            return;
        }
        try {
            if (rowStreamOpener == null) {
                client = LeasedBigtableClient.openData(settingsBuilder());
            }
            remainingRanges = ranges.iterator();
            if (remainingRanges.hasNext()) {
                advanceRange();
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Builds the client settings. Visible to the module's tests, which see the Cloud Monitoring
     * choice reach them.
     */
    @VisibleForTesting
    BigtableDataSettings settings() throws IOException {
        return settingsBuilder().build();
    }

    private BigtableDataSettings.Builder settingsBuilder() throws IOException {
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint, BigtableConnectorOptions.EMULATOR_ENDPOINT.key());
        BigtableDataSettings.Builder settings =
                BigtableDataClients.settings(
                        destination,
                        appProfileId,
                        endpoint,
                        BigtableCredentials.loadData(serviceAccountKeyFile));
        BigtableDataClients.applyClientMetrics(
                settings, null, exportClientMetricsToCloudMonitoring);
        return settings;
    }

    @Override
    public boolean reachedEnd() {
        while (!rows.hasNext() && remainingRanges.hasNext()) {
            advanceRange();
        }
        return !rows.hasNext();
    }

    @Override
    public RowData nextRecord(RowData reuse) {
        return converter.convert(rows.next());
    }

    private void advanceRange() {
        ByteStringRange range = remainingRanges.next();
        if (rowStreamOpener != null) {
            rows = rowStreamOpener.open(range);
            return;
        }
        Query query = Query.create(TableId.of(destination.getTable())).range(range).filter(filter);
        stream = client.client().readRows(query);
        rows = stream.iterator();
    }

    @Override
    public void close() {
        try (GrpcEventLoops.InterruptibleReleases releases =
                GrpcEventLoops.interruptibleReleases()) {
            closeLoad();
        }
    }

    private void closeLoad() {
        rows = Collections.emptyIterator();
        remainingRanges = Collections.emptyIterator();
        if (stream != null) {
            // A finished stream ignores the cancellation. A scan the load stopped part-way is
            // cancelled, so it can terminate without the loader consuming its remaining rows,
            // which the client's close below would otherwise wait for. An earlier range's point
            // read whose call outlived its row is beyond reach here (see the field).
            stream.cancel();
            stream = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @FunctionalInterface
    interface RowStreamOpener extends Serializable {

        Iterator<Row> open(ByteStringRange range);
    }
}
