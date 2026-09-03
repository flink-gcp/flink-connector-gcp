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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableRequestOptions}. */
class BigtableRequestOptionsTest {

    @Test
    void defaultsMatchTheDocumentedOnes() {
        BigtableRequestOptions options = BigtableRequestOptions.builder().build();

        // 100 is Flink's AsyncDataStream default capacity, so the two surfaces share one number;
        // 20 seconds is the client's own total timeout for both RPCs, pinned by
        // DefaultSingleRowClientFactoryTest.
        assertThat(options.getMaxInFlightRequests()).isEqualTo(100);
        assertThat(options.getRequestTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofHours(1));
        assertThat(options.getMaxActiveInstances()).isEqualTo(16);
        assertThat(options.isPerDestinationMetrics()).isFalse();
    }

    @Test
    void carriesEveryConfiguredValue() {
        BigtableRequestOptions options =
                BigtableRequestOptions.builder()
                        .maxInFlightRequests(7)
                        .requestTimeout(Duration.ofSeconds(3))
                        .destinationIdleTimeout(Duration.ofMinutes(15))
                        .maxActiveInstances(3)
                        .perDestinationMetrics(true)
                        .build();

        assertThat(options.getMaxInFlightRequests()).isEqualTo(7);
        assertThat(options.getRequestTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(15));
        assertThat(options.getMaxActiveInstances()).isEqualTo(3);
        assertThat(options.isPerDestinationMetrics()).isTrue();
        assertThat(options.toString())
                .contains(
                        "maxInFlightRequests=7",
                        "requestTimeout=PT3S",
                        "destinationIdleTimeout=PT15M",
                        "maxActiveInstances=3",
                        "perDestinationMetrics=true");
    }

    @Test
    void isValueBasedAndSerializable() throws Exception {
        BigtableRequestOptions options =
                BigtableRequestOptions.builder().maxInFlightRequests(7).build();

        assertThat(options)
                .isEqualTo(BigtableRequestOptions.builder().maxInFlightRequests(7).build())
                .hasSameHashCodeAs(BigtableRequestOptions.builder().maxInFlightRequests(7).build())
                .isNotEqualTo(BigtableRequestOptions.builder().build())
                .isNotEqualTo(
                        BigtableRequestOptions.builder()
                                .maxInFlightRequests(7)
                                .perDestinationMetrics(true)
                                .build());
        assertThat(InstantiationUtil.clone(options)).isEqualTo(options);
    }

    @Test
    void rejectsNonPositiveValues() {
        BigtableRequestOptions.Builder builder = BigtableRequestOptions.builder();

        assertThatThrownBy(() -> builder.maxInFlightRequests(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightRequests must be positive");
        assertThatThrownBy(() -> builder.maxActiveInstances(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxActiveInstances must be positive");
        assertThatThrownBy(() -> builder.destinationIdleTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRequestTimeoutIsAtLeastAMillisecondAndExpressibleInNanos() {
        // The client takes the deadline as a java.time.Duration, and the runtime renders it in
        // messages, so the bounds are the connector's (ADR-0068), not the client's.
        BigtableRequestOptions.Builder builder = BigtableRequestOptions.builder();
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);

        assertThat(builder.requestTimeout(Duration.ofMillis(1)).build().getRequestTimeout())
                .isEqualTo(Duration.ofMillis(1));
        assertThat(builder.requestTimeout(expressible).build().getRequestTimeout())
                .isEqualTo(expressible);
        assertThatThrownBy(() -> builder.requestTimeout(Duration.ofNanos(999_999)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(() -> builder.requestTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(() -> builder.requestTimeout(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void theIdleTimeoutTakesTheLargestDurationANanosecondClockCanExpress() {
        Duration expressible = Duration.ofNanos(Long.MAX_VALUE);
        BigtableRequestOptions.Builder builder = BigtableRequestOptions.builder();

        assertThat(builder.destinationIdleTimeout(expressible).build().getDestinationIdleTimeout())
                .isEqualTo(expressible);
        assertThatThrownBy(() -> builder.destinationIdleTimeout(expressible.plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.destinationIdleTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
