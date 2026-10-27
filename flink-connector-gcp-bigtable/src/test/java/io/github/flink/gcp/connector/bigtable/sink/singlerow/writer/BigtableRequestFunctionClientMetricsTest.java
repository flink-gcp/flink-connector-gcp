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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.bigtable.ClientMetricsSettings;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins what an async request function registers when it builds its production client factory. */
class BigtableRequestFunctionClientMetricsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void registersItsRpcMethodOnTheGroupItOpensWith() throws Exception {
        MetricListener listener = new MetricListener();

        DefaultSingleRowClientFactory factory =
                new Function(true, true).clientFactory(listener.getMetricGroup());

        assertThat(listener.getCounter("rpcMethod", "ReadModifyWriteRow", "operations"))
                .isPresent();
        // The set the function registered is the one its client records into.
        ClientMetricsSettings.recordOneOperation(factory.settings(TABLE), "ReadModifyWriteRow");
        assertThat(
                        listener.getCounter("rpcMethod", "ReadModifyWriteRow", "operations")
                                .orElseThrow()
                                .getCount())
                .isOne();
        assertThat(factory.settings(TABLE).areInternalMetricsEnabled()).isTrue();
        factory.close();
    }

    @Test
    void keepsTheTwoChoicesApartOnTheWayToTheClient() throws Exception {
        MetricListener on = new MetricListener();
        DefaultSingleRowClientFactory flinkOnly =
                new Function(true, false).clientFactory(on.getMetricGroup());
        assertThat(on.getCounter("rpcMethod", "ReadModifyWriteRow", "operations")).isPresent();
        assertThat(flinkOnly.settings(TABLE).areInternalMetricsEnabled()).isFalse();
        flinkOnly.close();

        MetricListener off = new MetricListener();
        DefaultSingleRowClientFactory monitoringOnly =
                new Function(false, true).clientFactory(off.getMetricGroup());
        assertThat(off.getCounter("rpcMethod", "ReadModifyWriteRow", "operations")).isEmpty();
        ClientMetricsSettings.assertAsShipped(monitoringOnly.settings(TABLE));
        monitoringOnly.close();
    }

    /** The production constructor's subclass; it never handles an input here. */
    private static final class Function extends BigtableRequestFunction<String, Object, String> {

        private static final long serialVersionUID = 1L;

        private Function(boolean exportToFlink, boolean exportToCloudMonitoring) {
            super(
                    null,
                    BigtableRequestOptions.builder().build(),
                    null,
                    // Production settings, which these tests read; no client is built.
                    null,
                    exportToFlink,
                    exportToCloudMonitoring,
                    RowOperation.READ_MODIFY_WRITE_ROW);
        }

        @Override
        protected TableDestination destination(String input) {
            return TABLE;
        }

        @Override
        @Nullable
        protected RowRequest<Object> request(String input) {
            return null;
        }

        @Override
        protected String result(
                String input, Object answer, TableDestination destination, RowRequest<Object> r) {
            return input;
        }
    }
}
