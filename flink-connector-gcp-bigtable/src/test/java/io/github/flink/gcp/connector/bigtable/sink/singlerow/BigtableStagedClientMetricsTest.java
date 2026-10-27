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

import org.apache.flink.metrics.Counter;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.ClientMetricsSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableDeliveryGuarantee;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.DefaultSingleRowClientFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins that the staged committer's per-profile client factories share one set of client metrics and
 * carry the Cloud Monitoring choice: a set per factory would register the same names twice on the
 * committer's group, and Flink would drop the second.
 */
class BigtableStagedClientMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    @Test
    void everyProfilesFactoryRecordsIntoTheOneSharedSet() throws Exception {
        ClientMetrics metrics = ClientMetrics.create("method");
        Counter operations =
                metrics.histogramCount(
                        "bigtable.googleapis.com/internal/client/operation_latencies",
                        "Bigtable.CheckAndMutateRow");
        BigtableStagedSink<String> sink = sink(false);

        DefaultSingleRowClientFactory first =
                (DefaultSingleRowClientFactory)
                        sink.clientFactories(null, metrics).create("profile-a");
        DefaultSingleRowClientFactory second =
                (DefaultSingleRowClientFactory)
                        sink.clientFactories(null, metrics).create("profile-b");

        ClientMetricsSettings.assertRecordsOnlyInto(first.settings(TABLE), metrics);
        ClientMetricsSettings.assertRecordsOnlyInto(second.settings(TABLE), metrics);
        ClientMetricsSettings.recordOneOperation(first.settings(TABLE), "CheckAndMutateRow");
        ClientMetricsSettings.recordOneOperation(second.settings(TABLE), "CheckAndMutateRow");
        // Evicting one profile closes its factory, and must not freeze the shared set.
        first.close();
        ClientMetricsSettings.recordOneOperation(second.settings(TABLE), "CheckAndMutateRow");
        assertThat(operations.getCount()).isEqualTo(3);
        second.close();
    }

    @Test
    void theFactoriesKeepTheClientsMetricsAsShippedByDefault() throws Exception {
        DefaultSingleRowClientFactory factory =
                (DefaultSingleRowClientFactory)
                        sink(true).clientFactories(null, null).create("profile");

        ClientMetricsSettings.assertAsShipped(factory.settings(TABLE));
        factory.close();
    }

    /** Without the emulator, whose builder installs its own choices; only settings are built. */
    private static BigtableStagedSink<String> sink(boolean exportToCloudMonitoring) {
        return (BigtableStagedSink<String>)
                BigtableSink.<String>builder()
                        .table(TABLE)
                        .serializer(
                                (value, context) ->
                                        RowMutationEntry.create(value)
                                                .setCell("data", "q", 1000, value))
                        .appProfileId("profile")
                        .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                        .stagedOptions(
                                BigtableStagedOptions.builder().markerFamily("markers").build())
                        .exportClientMetricsToCloudMonitoring(exportToCloudMonitoring)
                        .build();
    }
}
