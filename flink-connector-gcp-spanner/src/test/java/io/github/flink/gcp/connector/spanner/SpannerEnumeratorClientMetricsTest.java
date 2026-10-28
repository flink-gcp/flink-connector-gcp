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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.metrics.testutils.MetricListener;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerEnumeratorClientMetrics}. */
class SpannerEnumeratorClientMetricsTest {

    @Test
    void registersOnTheFirstGroupAndHandsEveryLaterCallerTheSameSet() {
        MetricListener listener = new MetricListener();
        SpannerEnumeratorClientMetrics holder =
                new SpannerEnumeratorClientMetrics(SpannerClientMetrics.PARTITION_QUERY);

        ClientMetrics first = holder.get(listener.getMetricGroup());
        ClientMetrics second = holder.get(listener.getMetricGroup());

        assertThat(first).isNotNull();
        assertThat(second).isSameAs(first);
        assertThat(listener.getCounter("rpcMethod", "PartitionQuery", "operations")).isPresent();
    }

    @Test
    void registersNothingWithoutAGroup() {
        SpannerEnumeratorClientMetrics holder =
                new SpannerEnumeratorClientMetrics(SpannerClientMetrics.PARTITION_QUERY);

        assertThat(holder.get(null)).isNull();
        assertThat(holder.get(new MetricListener().getMetricGroup())).isNotNull();
    }
}
