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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EnumeratorClientMetricsTest {

    @Test
    void registersOnTheFirstGroupAndHandsEveryLaterCallerTheSameSet() {
        EnumeratorClientMetrics holder = new EnumeratorClientMetrics("SampleRowKeys");
        MetricListener first = new MetricListener();
        MetricListener second = new MetricListener();

        ClientMetrics registered = holder.get(first.getMetricGroup());
        ClientMetrics again = holder.get(second.getMetricGroup());

        assertThat(registered).isNotNull();
        assertThat(again).isSameAs(registered);
        assertThat(first.getCounter("rpcMethod", "SampleRowKeys", "operations")).isPresent();
        // Registering again is what Flink would drop on the shared group of a reset coordinator.
        assertThat(second.getCounter("rpcMethod", "SampleRowKeys", "operations")).isEmpty();
    }

    @Test
    void registersNothingWithoutAGroup() {
        EnumeratorClientMetrics holder = new EnumeratorClientMetrics("SampleRowKeys");

        assertThat(holder.get(null)).isNull();
        assertThat(holder.get(new MetricListener().getMetricGroup())).isNotNull();
    }
}
