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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.metrics.testutils.MetricListener;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the RPC method the readmodifywrite sink and async helper register, and that the async
 * helper's two client-metrics choices are not swapped on the way to its runtime. Against an
 * endpoint nothing listens on: the client factory builds a client on the first request only.
 */
class ReadModifyWriteClientMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final EmulatorEndpoint ENDPOINT =
            EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint");

    @Test
    void theAsyncHelperRegistersReadModifyWriteRowOnlyWhenExportingToFlink() throws Exception {
        assertThat(registeredByFunction(true, false)).isTrue();
        assertThat(registeredByFunction(false, true)).isFalse();
    }

    @Test
    void theSinkRegistersReadModifyWriteRowOnItsWritersGroup() throws Exception {
        StubWriterInitContext context = new StubWriterInitContext(0);

        BigtableReadModifyWriteSink.<String>builder()
                .table(TABLE)
                .serializer(
                        (input, ignored) ->
                                ReadModifyWriteRequest.of(
                                        ByteString.copyFromUtf8(input),
                                        List.of(
                                                ReadModifyWriteRule.increment(
                                                        "cf", ByteString.EMPTY, 1))))
                .emulatorEndpoint(ENDPOINT)
                .exportClientMetricsToFlink(true)
                .build()
                .createWriter(context)
                .close();

        assertThat(
                        context.getSinkWriterMetricGroup()
                                .hasMetric("rpcMethod", "ReadModifyWriteRow", "operations"))
                .isTrue();
    }

    @Test
    void bothBuildersStartFromTheSharedDefaults() {
        var sink =
                BigtableReadModifyWriteSink.<String>builder()
                        .table(TABLE)
                        .serializer((input, ignored) -> null)
                        .build()
                        .getConfig();
        ReadModifyWriteConfig<String> async =
                BigtableReadModifyWriteAsync.<String>builder()
                        .table(TABLE)
                        .serializer((input, ignored) -> null)
                        .build()
                        .config();

        assertThat(sink.isExportClientMetricsToFlink())
                .isEqualTo(BigtableDataClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK);
        assertThat(sink.isExportClientMetricsToCloudMonitoring())
                .isEqualTo(BigtableDataClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING);
        assertThat(async.exportClientMetricsToFlink)
                .isEqualTo(BigtableDataClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_FLINK);
        assertThat(async.exportClientMetricsToCloudMonitoring)
                .isEqualTo(BigtableDataClients.DEFAULT_EXPORT_CLIENT_METRICS_TO_CLOUD_MONITORING);
    }

    private static boolean registeredByFunction(
            boolean exportToFlink, boolean exportToCloudMonitoring) throws Exception {
        MetricListener listener = new MetricListener();
        ReadModifyWriteFunction<String> function =
                new ReadModifyWriteFunction<>(
                        new ReadModifyWriteConfig<String>(
                                (input, context) -> TABLE,
                                (input, context) ->
                                        ReadModifyWriteRequest.of(
                                                ByteString.copyFromUtf8(input),
                                                List.of(
                                                        ReadModifyWriteRule.increment(
                                                                "cf", ByteString.EMPTY, 1))),
                                null,
                                BigtableRequestOptions.builder().build(),
                                null,
                                ENDPOINT,
                                exportToFlink,
                                exportToCloudMonitoring));
        function.openWithMetrics(listener.getMetricGroup());
        function.close();
        return listener.getCounter("rpcMethod", "ReadModifyWriteRow", "operations").isPresent();
    }
}
