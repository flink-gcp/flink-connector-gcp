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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubSubscriberOptions}. */
class PubSubSubscriberOptionsTest {

    @Test
    void defaultsLeaveSdkKnobsUnsetAndKeepSourceDefaults() {
        PubSubSubscriberOptions options = PubSubSubscriberOptions.defaults();

        assertThat(options.getFlowControlMaxOutstandingElementCount()).isNull();
        assertThat(options.getFlowControlMaxOutstandingRequestBytes()).isNull();
        assertThat(options.getParallelPullCount()).isNull();
        assertThat(options.getMaxAckExtensionPeriod()).isNull();
        assertThat(options.getMinDurationPerAckExtension()).isNull();
        assertThat(options.getMaxDurationPerAckExtension()).isNull();
        assertThat(options.getAwaitAckConfirmation()).isNull();
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(options.getMaxRecordsPerFetch()).isEqualTo(1_000);
        assertThat(options.getFirstCheckpointTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(PubSubSubscriberOptions.builder().build()).isEqualTo(options);
    }

    @Test
    void overridesAreKept() {
        PubSubSubscriberOptions options = fullyPopulated();

        assertThat(options.getFlowControlMaxOutstandingElementCount()).isEqualTo(500L);
        assertThat(options.getFlowControlMaxOutstandingRequestBytes()).isEqualTo(1_048_576L);
        assertThat(options.getParallelPullCount()).isNull();
        assertThat(options.getMaxAckExtensionPeriod()).isEqualTo(Duration.ofMinutes(30));
        assertThat(options.getMinDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(15));
        assertThat(options.getMaxDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(60));
        assertThat(options.getAwaitAckConfirmation()).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(options.getMaxRecordsPerFetch()).isEqualTo(250);
        assertThat(options.getFirstCheckpointTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void rejectsNonPositiveValues() {
        PubSubSubscriberOptions.Builder builder = PubSubSubscriberOptions.builder();

        assertThatThrownBy(() -> builder.flowControlMaxOutstandingElementCount(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flowControlMaxOutstandingElementCount");
        assertThatThrownBy(() -> builder.flowControlMaxOutstandingRequestBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flowControlMaxOutstandingRequestBytes");
        assertThatThrownBy(() -> builder.parallelPullCount(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelPullCount");
        assertThatThrownBy(() -> builder.maxAckExtensionPeriod(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAckExtensionPeriod");
        assertThatThrownBy(() -> builder.minDurationPerAckExtension(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minDurationPerAckExtension");
        assertThatThrownBy(() -> builder.maxDurationPerAckExtension(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDurationPerAckExtension");
        assertThatThrownBy(() -> builder.awaitAckConfirmation(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("awaitAckConfirmation");
        assertThatThrownBy(() -> builder.shutdownTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownTimeout");
        assertThatThrownBy(() -> builder.maxRecordsPerFetch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRecordsPerFetch");
        assertThatThrownBy(() -> builder.firstCheckpointTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("firstCheckpointTimeout");

        // Zero is the boundary the others reject and this one accepts: it disables the detector.
        assertThat(
                        builder.firstCheckpointTimeout(Duration.ZERO)
                                .build()
                                .getFirstCheckpointTimeout())
                .isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsAMinimumAckExtensionAtOrAboveTheMaximum() {
        // The SDK enforces this itself, but with a message-less argument check.
        assertThatThrownBy(
                        () ->
                                PubSubSubscriberOptions.builder()
                                        .minDurationPerAckExtension(Duration.ofSeconds(60))
                                        .maxDurationPerAckExtension(Duration.ofSeconds(60))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("minDurationPerAckExtension")
                .hasMessageContaining("maxDurationPerAckExtension");
    }

    @Test
    void acceptsOneAckExtensionBoundWithoutTheOther() {
        assertThat(
                        PubSubSubscriberOptions.builder()
                                .minDurationPerAckExtension(Duration.ofSeconds(60))
                                .build()
                                .getMaxDurationPerAckExtension())
                .isNull();
    }

    @Test
    void equalsAndHashCode() {
        assertThat(fullyPopulated())
                .isEqualTo(fullyPopulated())
                .hasSameHashCodeAs(fullyPopulated())
                .isNotEqualTo(PubSubSubscriberOptions.defaults());
    }

    @Test
    void roundTripsJavaSerialization() throws Exception {
        PubSubSubscriberOptions options = fullyPopulated();

        assertThat(
                        InstantiationUtil.<PubSubSubscriberOptions>deserializeObject(
                                InstantiationUtil.serializeObject(options),
                                getClass().getClassLoader()))
                .isEqualTo(options);
    }

    /**
     * Every knob set to a non-default value except {@code parallelPullCount}, which is the one with
     * a cross-option constraint (the source builder rejects it under ordered consumption), so the
     * fixture stays combinable with any ordering mode. Reused by {@link PubSubSourceBuilderTest}.
     */
    static PubSubSubscriberOptions fullyPopulated() {
        return PubSubSubscriberOptions.builder()
                .flowControlMaxOutstandingElementCount(500)
                .flowControlMaxOutstandingRequestBytes(1_048_576)
                .maxAckExtensionPeriod(Duration.ofMinutes(30))
                .minDurationPerAckExtension(Duration.ofSeconds(15))
                .maxDurationPerAckExtension(Duration.ofSeconds(60))
                .awaitAckConfirmation(Duration.ofSeconds(20))
                .shutdownTimeout(Duration.ofSeconds(3))
                .maxRecordsPerFetch(250)
                .firstCheckpointTimeout(Duration.ofMinutes(2))
                .build();
    }
}
