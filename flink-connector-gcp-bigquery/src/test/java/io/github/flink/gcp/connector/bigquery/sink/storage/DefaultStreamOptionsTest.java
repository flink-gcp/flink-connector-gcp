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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultStreamOptions}. */
class DefaultStreamOptionsTest {

    @Test
    void defaultsAreValid() {
        DefaultStreamOptions options = DefaultStreamOptions.builder().build();

        assertThat(options.getMaxAppendRequestBytes())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_APPEND_REQUEST_BYTES);
        assertThat(options.getRetryInitialBackoff())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_INITIAL_BACKOFF);
        assertThat(options.getRetryMaxBackoff())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_MAX_BACKOFF);
        assertThat(options.getRetryMaxAttempts())
                .isEqualTo(DefaultStreamOptions.DEFAULT_RETRY_MAX_ATTEMPTS);
        assertThat(options.getSdkRetryInitialDelay())
                .isEqualTo(DefaultStreamOptions.DEFAULT_SDK_RETRY_INITIAL_DELAY);
        assertThat(options.getSdkRetryDelayMultiplier())
                .isEqualTo(DefaultStreamOptions.DEFAULT_SDK_RETRY_DELAY_MULTIPLIER);
        assertThat(options.getSdkRetryMaxDelay())
                .isEqualTo(DefaultStreamOptions.DEFAULT_SDK_RETRY_MAX_DELAY);
        assertThat(options.getSdkRetryMaxAttempts())
                .isEqualTo(DefaultStreamOptions.DEFAULT_SDK_RETRY_MAX_ATTEMPTS);
        assertThat(options.getSdkMaxRetryDuration())
                .isEqualTo(DefaultStreamOptions.DEFAULT_SDK_MAX_RETRY_DURATION);
        assertThat(options.getMaxInflightRequests())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_REQUESTS);
        assertThat(options.getMaxInflightBytes())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_BYTES);
        assertThat(options.getMinConnectionsPerRegion())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MIN_CONNECTIONS_PER_REGION);
        assertThat(options.getMaxConnectionsPerRegion())
                .isEqualTo(DefaultStreamOptions.DEFAULT_MAX_CONNECTIONS_PER_REGION);
    }

    /**
     * The multiplexing default deliberately deviates from the SDK's 1000: a pooled connection is a
     * scale-up candidate above 20% of this limit, and the official guidance is to lower it.
     */
    @Test
    void inflightRequestDefaultFollowsMultiplexingGuidanceNotTheSdk() {
        assertThat(DefaultStreamOptions.DEFAULT_MAX_INFLIGHT_REQUESTS).isEqualTo(100);
    }

    @Test
    void carriesConfiguredValues() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .maxAppendRequestBytes(1024)
                        .retryInitialBackoff(Duration.ofMillis(100))
                        .retryMaxBackoff(Duration.ofSeconds(5))
                        .retryMaxAttempts(3)
                        .sdkRetryInitialDelay(Duration.ofMillis(250))
                        .sdkRetryDelayMultiplier(1.5)
                        .sdkRetryMaxDelay(Duration.ofSeconds(15))
                        .sdkRetryMaxAttempts(7)
                        .sdkMaxRetryDuration(Duration.ofMinutes(2))
                        .maxInflightRequests(50)
                        .maxInflightBytes(1024 * 1024)
                        .minConnectionsPerRegion(1)
                        .maxConnectionsPerRegion(4)
                        .build();

        assertThat(options.getMaxAppendRequestBytes()).isEqualTo(1024);
        assertThat(options.getRetryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRetryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(3);
        assertThat(options.getSdkRetryInitialDelay()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getSdkRetryDelayMultiplier()).isEqualTo(1.5);
        assertThat(options.getSdkRetryMaxDelay()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.getSdkRetryMaxAttempts()).isEqualTo(7);
        assertThat(options.getSdkMaxRetryDuration()).isEqualTo(Duration.ofMinutes(2));
        assertThat(options.getMaxInflightRequests()).isEqualTo(50);
        assertThat(options.getMaxInflightBytes()).isEqualTo(1024 * 1024);
        assertThat(options.getMinConnectionsPerRegion()).isEqualTo(1);
        assertThat(options.getMaxConnectionsPerRegion()).isEqualTo(4);
    }

    @Test
    void rejectsNonPositiveValues() {
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxAppendRequestBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().sdkRetryInitialDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().sdkRetryDelayMultiplier(0.99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().sdkRetryMaxDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().sdkRetryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().sdkMaxRetryDuration(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxInflightRequests(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxInflightBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().minConnectionsPerRegion(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DefaultStreamOptions.builder().maxConnectionsPerRegion(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiplierOfExactlyOneIsAccepted() {
        assertThat(
                        DefaultStreamOptions.builder()
                                .sdkRetryDelayMultiplier(1.0)
                                .build()
                                .getSdkRetryDelayMultiplier())
                .isEqualTo(1.0);
    }

    @Test
    void rejectsMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .retryInitialBackoff(Duration.ofSeconds(5))
                                        .retryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retryMaxBackoff");
    }

    @Test
    void acceptsMaxBackoffEqualToInitialBackoff() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .retryInitialBackoff(Duration.ofSeconds(5))
                        .retryMaxBackoff(Duration.ofSeconds(5))
                        .build();

        assertThat(options.getRetryMaxBackoff()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsSdkMaxDelayBelowSdkInitialDelay() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .sdkRetryInitialDelay(Duration.ofSeconds(5))
                                        .sdkRetryMaxDelay(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sdkRetryMaxDelay");
    }

    @Test
    void acceptsSdkMaxDelayEqualToSdkInitialDelay() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .sdkRetryInitialDelay(Duration.ofSeconds(5))
                        .sdkRetryMaxDelay(Duration.ofSeconds(5))
                        .build();

        assertThat(options.getSdkRetryMaxDelay()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsMaxConnectionsBelowMinConnections() {
        assertThatThrownBy(
                        () ->
                                DefaultStreamOptions.builder()
                                        .minConnectionsPerRegion(5)
                                        .maxConnectionsPerRegion(4)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxConnectionsPerRegion");
    }

    @Test
    void acceptsMaxConnectionsEqualToMinConnections() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(5)
                        .maxConnectionsPerRegion(5)
                        .build();

        assertThat(options.getMaxConnectionsPerRegion()).isEqualTo(5);
    }

    @Test
    void equalsAndHashCodeCoverEveryKnob() {
        DefaultStreamOptions defaults = DefaultStreamOptions.builder().build();

        assertThat(DefaultStreamOptions.builder().build()).isEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().build()).hasSameHashCodeAs(defaults);
        assertThat(DefaultStreamOptions.builder().maxAppendRequestBytes(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryInitialBackoff(Duration.ofMillis(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryMaxBackoff(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().retryMaxAttempts(1).build())
                .isNotEqualTo(defaults);
        assertThat(
                        DefaultStreamOptions.builder()
                                .sdkRetryInitialDelay(Duration.ofMillis(1))
                                .build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().sdkRetryDelayMultiplier(3.0).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().sdkRetryMaxDelay(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().sdkRetryMaxAttempts(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().sdkMaxRetryDuration(Duration.ofDays(1)).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxInflightRequests(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxInflightBytes(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().minConnectionsPerRegion(1).build())
                .isNotEqualTo(defaults);
        assertThat(DefaultStreamOptions.builder().maxConnectionsPerRegion(21).build())
                .isNotEqualTo(defaults);
    }
}
