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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubPublisherOptions}. */
class PubSubPublisherOptionsTest {

    /**
     * An options instance with every knob set, shared by the override and round-trip tests (also
     * reused by the builder round trip in {@code PubSubSinkBuilderTest}). Ordering is enabled here:
     * with the flow-control limits gone, no knob is mutually exclusive with it any more, so "every
     * knob" can be literal.
     */
    static PubSubPublisherOptions fullyPopulated() {
        return PubSubPublisherOptions.builder()
                .batchElementCountThreshold(5)
                .batchRequestByteThreshold(1_000)
                .batchDelayThreshold(Duration.ofMillis(20))
                .enableMessageOrdering(true)
                .maxInFlightBytes(1_048_576)
                .retryTotalTimeout(Duration.ofSeconds(120))
                .retryInitialDelay(Duration.ofMillis(50))
                .retryDelayMultiplier(2.0)
                .retryMaxDelay(Duration.ofSeconds(5))
                .retryInitialRpcTimeout(Duration.ofSeconds(3))
                .retryRpcTimeoutMultiplier(1.5)
                .retryMaxRpcTimeout(Duration.ofSeconds(30))
                .retryMaxAttempts(7)
                .maxInFlightMessages(42)
                .recoveryInitialBackoff(Duration.ofMillis(100))
                .recoveryMaxBackoff(Duration.ofSeconds(1))
                .recoveryMaxAttempts(3)
                .build();
    }

    @Test
    void defaultsLeaveSdkKnobsUnsetAndKeepSinkDefaults() {
        PubSubPublisherOptions defaults = PubSubPublisherOptions.defaults();

        assertThat(defaults.getBatchElementCountThreshold()).isNull();
        assertThat(defaults.getBatchRequestByteThreshold()).isNull();
        assertThat(defaults.getBatchDelayThreshold()).isNull();
        assertThat(defaults.getRetryTotalTimeout()).isNull();
        assertThat(defaults.getRetryInitialDelay()).isNull();
        assertThat(defaults.getRetryDelayMultiplier()).isNull();
        assertThat(defaults.getRetryMaxDelay()).isNull();
        assertThat(defaults.getRetryInitialRpcTimeout()).isNull();
        assertThat(defaults.getRetryRpcTimeoutMultiplier()).isNull();
        assertThat(defaults.getRetryMaxRpcTimeout()).isNull();
        assertThat(defaults.getRetryMaxAttempts()).isNull();
        assertThat(defaults.isEnableMessageOrdering()).isFalse();
        assertThat(defaults.getMaxInFlightMessages()).isEqualTo(1000);
        assertThat(defaults.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(defaults.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(defaults.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(defaults.getRecoveryMaxAttempts()).isEqualTo(10);
        assertThat(defaults.hasBatchingOverrides()).isFalse();
        assertThat(defaults.hasRetryOverrides()).isFalse();
        assertThat(defaults).isEqualTo(PubSubPublisherOptions.builder().build());
    }

    @Test
    void overridesAreKept() {
        PubSubPublisherOptions options = fullyPopulated();

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(5);
        assertThat(options.getBatchRequestByteThreshold()).isEqualTo(1_000);
        assertThat(options.getBatchDelayThreshold()).isEqualTo(Duration.ofMillis(20));
        assertThat(options.getRetryTotalTimeout()).isEqualTo(Duration.ofSeconds(120));
        assertThat(options.getRetryInitialDelay()).isEqualTo(Duration.ofMillis(50));
        assertThat(options.getRetryDelayMultiplier()).isEqualTo(2.0);
        assertThat(options.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getRetryInitialRpcTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getRetryRpcTimeoutMultiplier()).isEqualTo(1.5);
        assertThat(options.getRetryMaxRpcTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(options.isEnableMessageOrdering()).isTrue();
        assertThat(options.getMaxInFlightMessages()).isEqualTo(42);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(1_048_576);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(3);
        assertThat(options.hasBatchingOverrides()).isTrue();
        assertThat(options.hasRetryOverrides()).isTrue();
    }

    @Test
    void rejectsNonPositiveValues() {
        PubSubPublisherOptions.Builder builder = PubSubPublisherOptions.builder();

        assertThatThrownBy(() -> builder.batchElementCountThreshold(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchElementCountThreshold");
        assertThatThrownBy(() -> builder.batchRequestByteThreshold(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchRequestByteThreshold");
        assertThatThrownBy(() -> builder.batchDelayThreshold(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchDelayThreshold");
        assertThatThrownBy(() -> builder.batchDelayThreshold(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("batchDelayThreshold");
        assertThatThrownBy(() -> builder.retryTotalTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout");
        assertThatThrownBy(() -> builder.retryInitialDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay");
        assertThatThrownBy(() -> builder.retryDelayMultiplier(0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryDelayMultiplier");
        assertThatThrownBy(() -> builder.retryMaxDelay(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay");
        assertThatThrownBy(() -> builder.retryInitialRpcTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialRpcTimeout");
        assertThatThrownBy(() -> builder.retryRpcTimeoutMultiplier(0.9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryRpcTimeoutMultiplier");
        assertThatThrownBy(() -> builder.retryMaxRpcTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout");
        assertThatThrownBy(() -> builder.retryMaxAttempts(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts");
        assertThatThrownBy(() -> builder.maxInFlightMessages(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightMessages");
        // Zero would make the write admission predicate hold with nothing in flight, and yield()
        // blocks until a mail arrives — a task hang rather than backpressure.
        assertThatThrownBy(() -> builder.maxInFlightBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightBytes");
        assertThatThrownBy(() -> builder.maxInFlightBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightBytes");
        assertThatThrownBy(() -> builder.recoveryInitialBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryInitialBackoff");
        assertThatThrownBy(() -> builder.recoveryMaxBackoff(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxBackoff");
        assertThatThrownBy(() -> builder.recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxAttempts");
    }

    @Test
    void aByteBoundIsAvailableWithMessageOrdering() {
        // The point of #85: the SDK flow-control byte limit that used to be the only byte bound
        // could not be combined with ordering (it leaks a permit per publish cancelled on a paused
        // key), leaving ordered sinks — where a paused key holds its whole cascade — with no byte
        // bound at all. The writer-owned cap has no such restriction.
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .enableMessageOrdering(true)
                        .maxInFlightBytes(1_000)
                        .build();

        assertThat(options.isEnableMessageOrdering()).isTrue();
        assertThat(options.getMaxInFlightBytes()).isEqualTo(1_000);
    }

    @Test
    void rejectsSubMillisecondRecoveryBackoffs() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryMaxBackoff(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    @Test
    void rejectsRecoveryMaxBackoffBelowInitialBackoff() {
        assertThatThrownBy(
                        () ->
                                PubSubPublisherOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(5))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
    }

    @Test
    void zeroRetryMaxAttemptsIsAccepted() {
        assertThat(
                        PubSubPublisherOptions.builder()
                                .retryMaxAttempts(0)
                                .build()
                                .getRetryMaxAttempts())
                .isEqualTo(0);
    }

    @Test
    void equalsAndHashCode() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated());
        assertThat(fullyPopulated()).isNotEqualTo(PubSubPublisherOptions.defaults());
    }

    @Test
    void roundTripsJavaSerialization() throws Exception {
        PubSubPublisherOptions options = fullyPopulated();

        byte[] bytes = InstantiationUtil.serializeObject(options);
        PubSubPublisherOptions copy =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        assertThat(copy).isEqualTo(options);
    }
}
