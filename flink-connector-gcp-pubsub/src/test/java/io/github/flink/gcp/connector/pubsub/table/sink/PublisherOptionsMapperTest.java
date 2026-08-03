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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PublisherOptionsMapper}. */
class PublisherOptionsMapperTest {

    /**
     * Every {@code PubSubPublisherOptions.Builder} setter and the option that feeds it.
     *
     * <p>Written out rather than derived, because the option keys are grouped ({@code
     * sink.batching.*}, {@code sink.retry.*}) and no naming rule turns one into the other. The
     * reflection test below is what makes the table exhaustive.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put(
                "batchElementCountThreshold",
                PubSubConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD);
        SETTER_TO_OPTION.put(
                "batchRequestByteThreshold",
                PubSubConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD);
        SETTER_TO_OPTION.put(
                "batchDelayThreshold", PubSubConnectorOptions.SINK_BATCHING_DELAY_THRESHOLD);
        SETTER_TO_OPTION.put("retryTotalTimeout", PubSubConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT);
        SETTER_TO_OPTION.put("retryInitialDelay", PubSubConnectorOptions.SINK_RETRY_INITIAL_DELAY);
        SETTER_TO_OPTION.put(
                "retryDelayMultiplier", PubSubConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER);
        SETTER_TO_OPTION.put("retryMaxDelay", PubSubConnectorOptions.SINK_RETRY_MAX_DELAY);
        SETTER_TO_OPTION.put(
                "retryInitialRpcTimeout", PubSubConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT);
        SETTER_TO_OPTION.put(
                "retryRpcTimeoutMultiplier",
                PubSubConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER);
        SETTER_TO_OPTION.put(
                "retryMaxRpcTimeout", PubSubConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT);
        SETTER_TO_OPTION.put("retryMaxAttempts", PubSubConnectorOptions.SINK_RETRY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "enableMessageOrdering", PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED);
        SETTER_TO_OPTION.put(
                "maxInFlightMessages", PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_MESSAGES);
        SETTER_TO_OPTION.put("maxInFlightBytes", PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES);
        SETTER_TO_OPTION.put(
                "recoveryInitialBackoff", PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxBackoff", PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxAttempts", PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "perDestinationMetrics", PubSubConnectorOptions.SINK_METRICS_PER_DESTINATION);
    }

    @Test
    void everyPublisherKnobHasAnOption() {
        // Deliberately not filtered on arity: the sibling SubscriptionCreateOptions.Builder already
        // has a no-arg setter and a two-arg one, so an arity filter here would let a knob of either
        // shape slip in unmapped — which is precisely what this guard exists to prevent.
        Set<String> setters =
                Arrays.stream(PubSubPublisherOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == PubSubPublisherOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        // Both directions: a new knob without an option, and an option whose knob was removed.
        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    @Test
    void anEmptyConfigProducesExactlyTheConnectorDefaults() {
        assertThat(PublisherOptionsMapper.map(new Configuration()))
                .isEqualTo(PubSubPublisherOptions.defaults());
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.batching.element-count-threshold", "17");
        options.put("sink.batching.request-byte-threshold", "3 kb");
        options.put("sink.batching.delay-threshold", "40 ms");
        options.put("sink.retry.total-timeout", "5 min");
        options.put("sink.retry.initial-delay", "7 s");
        options.put("sink.retry.delay-multiplier", "1.5");
        options.put("sink.retry.max-delay", "9 s");
        options.put("sink.retry.initial-rpc-timeout", "11 s");
        options.put("sink.retry.rpc-timeout-multiplier", "2.5");
        options.put("sink.retry.max-rpc-timeout", "13 s");
        options.put("sink.retry.max-attempts", "4");
        options.put("sink.message-ordering.enabled", "true");
        options.put("sink.in-flight.max-messages", "23");
        options.put("sink.in-flight.max-bytes", "5 mb");
        options.put("sink.recovery.initial-backoff", "600 ms");
        options.put("sink.recovery.max-backoff", "20 s");
        options.put("sink.recovery.max-attempts", "6");
        options.put("sink.metrics.per-destination", "true");

        PubSubPublisherOptions mapped = PublisherOptionsMapper.map(Configuration.fromMap(options));

        assertThat(mapped.getBatchElementCountThreshold()).isEqualTo(17L);
        assertThat(mapped.getBatchRequestByteThreshold()).isEqualTo(3L * 1024);
        assertThat(mapped.getBatchDelayThreshold()).isEqualTo(Duration.ofMillis(40));
        assertThat(mapped.getRetryTotalTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(mapped.getRetryInitialDelay()).isEqualTo(Duration.ofSeconds(7));
        assertThat(mapped.getRetryDelayMultiplier()).isEqualTo(1.5);
        assertThat(mapped.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(9));
        assertThat(mapped.getRetryInitialRpcTimeout()).isEqualTo(Duration.ofSeconds(11));
        assertThat(mapped.getRetryRpcTimeoutMultiplier()).isEqualTo(2.5);
        assertThat(mapped.getRetryMaxRpcTimeout()).isEqualTo(Duration.ofSeconds(13));
        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(4);
        assertThat(mapped.isEnableMessageOrdering()).isTrue();
        assertThat(mapped.getMaxInFlightMessages()).isEqualTo(23);
        assertThat(mapped.getMaxInFlightBytes()).isEqualTo(5L * 1024 * 1024);
        assertThat(mapped.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(600));
        assertThat(mapped.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(20));
        assertThat(mapped.getRecoveryMaxAttempts()).isEqualTo(6);
        assertThat(mapped.isPerDestinationMetrics()).isTrue();
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        PubSubPublisherOptions mapped =
                PublisherOptionsMapper.map(
                        Configuration.fromMap(
                                java.util.Collections.singletonMap(
                                        "sink.retry.max-attempts", "3")));

        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(3);
        assertThat(mapped.hasRetryOverrides()).isTrue();
        assertThat(mapped.hasBatchingOverrides()).isFalse();
        assertThat(mapped.getRetryTotalTimeout()).isNull();
    }
}
