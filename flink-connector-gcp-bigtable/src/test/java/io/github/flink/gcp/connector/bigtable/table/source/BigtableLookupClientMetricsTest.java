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

import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.ClientMetricsSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins what the lookups do with the two client-metrics choices. */
class BigtableLookupClientMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");
    private static final String ENDPOINT = "localhost:1";
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))))
                                    .getLogicalType());
    private static final Filters.Filter FILTER = Filters.FILTERS.family().exactMatch("cf");

    @Test
    void aPointLookupRegistersReadRowOnTheFunctionsGroup() throws Exception {
        MetricListener listener = new MetricListener();
        BigtableDataClientRowLookup lookup = lookup(ENDPOINT, true, true);

        // Against an endpoint nothing listens on: opening builds the client and reads nothing.
        lookup.open(listener.getMetricGroup());
        lookup.close();

        assertThat(listener.getCounter("rpcMethod", "ReadRow", "operations")).isPresent();
    }

    @Test
    void aPointLookupCarriesBothChoicesToItsClient() throws Exception {
        ClientMetrics metrics = ClientMetrics.create("method");

        // Without the emulator, whose builder installs its own choices; only settings are built.
        ClientMetricsSettings.assertRecordsInto(
                lookup(null, true, true).settings(metrics), metrics);
        ClientMetricsSettings.assertExportsNothing(lookup(null, false, false).settings(null));
    }

    @Test
    void aPointLookupRegistersNothingWhenTheExportIsOff() throws Exception {
        MetricListener listener = new MetricListener();
        BigtableDataClientRowLookup lookup = lookup(ENDPOINT, false, true);

        lookup.open(listener.getMetricGroup());
        lookup.close();

        assertThat(listener.getCounter("rpcMethod", "ReadRow", "operations")).isEmpty();
    }

    @Test
    void theFullCacheKeepsOnlyTheCloudMonitoringChoice() throws Exception {
        // Flink's full cache gives its loader no metric group, so there is nothing to register on.
        ClientMetricsSettings.assertExportsNothing(fullCache(false).settings());
        ClientMetricsSettings.assertAsShipped(fullCache(true).settings());
    }

    private static BigtableDataClientRowLookup lookup(
            @Nullable String endpoint, boolean exportToFlink, boolean exportToCloudMonitoring) {
        return new BigtableDataClientRowLookup(
                TABLE,
                FILTER,
                Collections.singletonList(ByteStringRange.unbounded()),
                null,
                null,
                endpoint,
                exportToFlink,
                exportToCloudMonitoring);
    }

    private static BigtableFullCacheInputFormat fullCache(boolean exportToCloudMonitoring) {
        return new BigtableFullCacheInputFormat(
                TABLE,
                SCHEMA,
                null,
                "null",
                TrailingBytes.IGNORE,
                FILTER,
                Collections.singletonList(ByteStringRange.unbounded()),
                null,
                null,
                null,
                null,
                exportToCloudMonitoring);
    }
}
