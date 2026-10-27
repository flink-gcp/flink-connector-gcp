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
import org.apache.flink.metrics.MetricGroup;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableClientMetrics;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Point reads through a {@link BigtableDataClient}, owned by one lookup function instance.
 *
 * <p>The client calls {@code ReadRow}, the one RPC whose client metrics the lookup registers when
 * they are exported to Flink. They are not closed: the group ends with the function ({@code
 * docs/adr/0186}).
 */
@Internal
final class BigtableDataClientRowLookup implements BigtableRowLookup {

    private static final long serialVersionUID = 1L;

    /** The one RPC the lookup's client calls. */
    private static final String READ_ROW = "ReadRow";

    private final TableDestination destination;
    private final Filters.Filter filter;
    private final List<ByteStringRange> ranges;
    @Nullable private final String appProfileId;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    private final boolean exportClientMetricsToFlink;
    private final boolean exportClientMetricsToCloudMonitoring;

    @Nullable private transient BigtableDataClient client;

    BigtableDataClientRowLookup(
            TableDestination destination,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            boolean exportClientMetricsToFlink,
            boolean exportClientMetricsToCloudMonitoring) {
        this.destination = destination;
        this.filter = filter;
        this.ranges = ranges;
        this.appProfileId = appProfileId;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.exportClientMetricsToFlink = exportClientMetricsToFlink;
        this.exportClientMetricsToCloudMonitoring = exportClientMetricsToCloudMonitoring;
    }

    @Override
    public void open(MetricGroup metricGroup) throws Exception {
        ClientMetrics clientMetrics =
                exportClientMetricsToFlink
                        ? BigtableClientMetrics.register(metricGroup, READ_ROW)
                        : null;
        client = BigtableDataClient.create(settings(clientMetrics));
    }

    /**
     * Builds the client settings. Visible to the module's tests, which see each client-metrics
     * choice reach them.
     */
    @VisibleForTesting
    BigtableDataSettings settings(@Nullable ClientMetrics clientMetrics) throws IOException {
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
                settings, clientMetrics, exportClientMetricsToCloudMonitoring);
        return settings.build();
    }

    @Override
    @Nullable
    public Row read(ByteString rowKey) {
        if (!isInRange(rowKey)) {
            return null;
        }
        return client().readRow(TableId.of(destination.getTable()), rowKey, filter);
    }

    @Override
    public ApiFuture<Row> readAsync(ByteString rowKey) {
        if (!isInRange(rowKey)) {
            return ApiFutures.immediateFuture(null);
        }
        return client().readRowAsync(TableId.of(destination.getTable()), rowKey, filter);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private BigtableDataClient client() {
        if (client == null) {
            throw new IllegalStateException("The Bigtable row lookup has not been opened.");
        }
        return client;
    }

    private boolean isInRange(ByteString rowKey) {
        return ranges.stream().anyMatch(range -> RowRanges.contains(range, rowKey));
    }
}
