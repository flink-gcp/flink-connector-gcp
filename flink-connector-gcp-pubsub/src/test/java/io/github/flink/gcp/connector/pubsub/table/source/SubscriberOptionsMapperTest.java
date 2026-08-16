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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubscriberOptionsMapper}. */
class SubscriberOptionsMapperTest {

    /**
     * Every {@code PubSubSubscriberOptions.Builder} setter and the option that feeds it, written
     * out because the key names are grouped and no naming rule derives one from the other. The
     * reflection test below is what makes the table exhaustive.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put(
                "flowControlMaxOutstandingElementCount",
                PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_ELEMENT_COUNT);
        SETTER_TO_OPTION.put(
                "flowControlMaxOutstandingRequestBytes",
                PubSubConnectorOptions.SCAN_FLOW_CONTROL_MAX_OUTSTANDING_REQUEST_BYTES);
        SETTER_TO_OPTION.put(
                "pausedSplitBufferMaxMessages",
                PubSubConnectorOptions.SCAN_PAUSED_SPLIT_BUFFER_MAX_MESSAGES);
        SETTER_TO_OPTION.put(
                "pausedSplitBufferMaxBytes",
                PubSubConnectorOptions.SCAN_PAUSED_SPLIT_BUFFER_MAX_BYTES);
        SETTER_TO_OPTION.put("parallelPullCount", PubSubConnectorOptions.SCAN_PARALLEL_PULL_COUNT);
        SETTER_TO_OPTION.put(
                "maxAckExtensionPeriod", PubSubConnectorOptions.SCAN_ACK_MAX_EXTENSION_PERIOD);
        SETTER_TO_OPTION.put(
                "minDurationPerAckExtension",
                PubSubConnectorOptions.SCAN_ACK_MIN_DURATION_PER_EXTENSION);
        SETTER_TO_OPTION.put(
                "maxDurationPerAckExtension",
                PubSubConnectorOptions.SCAN_ACK_MAX_DURATION_PER_EXTENSION);
        SETTER_TO_OPTION.put(
                "awaitAckConfirmation", PubSubConnectorOptions.SCAN_ACK_AWAIT_CONFIRMATION);
        SETTER_TO_OPTION.put("shutdownTimeout", PubSubConnectorOptions.SCAN_SHUTDOWN_TIMEOUT);
        SETTER_TO_OPTION.put(
                "maxRecordsPerFetch", PubSubConnectorOptions.SCAN_MAX_RECORDS_PER_FETCH);
        SETTER_TO_OPTION.put(
                "firstCheckpointTimeout", PubSubConnectorOptions.SCAN_FIRST_CHECKPOINT_TIMEOUT);
    }

    @Test
    void everySubscriberKnobHasAnOption() {
        Set<String> setters =
                Arrays.stream(PubSubSubscriberOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == PubSubSubscriberOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    @Test
    void anEmptyConfigProducesExactlyTheConnectorDefaults() {
        assertThat(SubscriberOptionsMapper.map(new Configuration()))
                .isEqualTo(PubSubSubscriberOptions.defaults());
    }

    /** The key of the option the named setter is fed by, so the table above is load-bearing. */
    private static String key(String setter) {
        return SETTER_TO_OPTION.get(setter).key();
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put(key("flowControlMaxOutstandingElementCount"), "500");
        options.put(key("flowControlMaxOutstandingRequestBytes"), "7 mb");
        options.put(key("pausedSplitBufferMaxMessages"), "900");
        options.put(key("pausedSplitBufferMaxBytes"), "11 mb");
        options.put(key("parallelPullCount"), "3");
        options.put(key("maxAckExtensionPeriod"), "30 min");
        options.put(key("minDurationPerAckExtension"), "20 s");
        options.put(key("maxDurationPerAckExtension"), "90 s");
        options.put(key("awaitAckConfirmation"), "4 s");
        options.put(key("shutdownTimeout"), "8 s");
        options.put(key("maxRecordsPerFetch"), "250");
        options.put(key("firstCheckpointTimeout"), "3 min");
        assertThat(options).hasSize(SETTER_TO_OPTION.size());

        PubSubSubscriberOptions mapped =
                SubscriberOptionsMapper.map(Configuration.fromMap(options));

        assertThat(mapped.getFlowControlMaxOutstandingElementCount()).isEqualTo(500L);
        assertThat(mapped.getFlowControlMaxOutstandingRequestBytes()).isEqualTo(7L * 1024 * 1024);
        assertThat(mapped.getPausedSplitBufferMaxMessages()).isEqualTo(900L);
        assertThat(mapped.getPausedSplitBufferMaxBytes()).isEqualTo(11L * 1024 * 1024);
        assertThat(mapped.getParallelPullCount()).isEqualTo(3);
        assertThat(mapped.getMaxAckExtensionPeriod()).isEqualTo(Duration.ofMinutes(30));
        assertThat(mapped.getMinDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(20));
        assertThat(mapped.getMaxDurationPerAckExtension()).isEqualTo(Duration.ofSeconds(90));
        assertThat(mapped.getAwaitAckConfirmation()).isEqualTo(Duration.ofSeconds(4));
        assertThat(mapped.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(8));
        assertThat(mapped.getMaxRecordsPerFetch()).isEqualTo(250);
        assertThat(mapped.getFirstCheckpointTimeout()).isEqualTo(Duration.ofMinutes(3));
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        PubSubSubscriberOptions mapped =
                SubscriberOptionsMapper.map(
                        Configuration.fromMap(
                                Collections.singletonMap("scan.parallel-pull-count", "2")));

        assertThat(mapped.getParallelPullCount()).isEqualTo(2);
        assertThat(mapped.getFlowControlMaxOutstandingElementCount()).isNull();
        assertThat(mapped.getMaxAckExtensionPeriod()).isNull();
        // A defaulted knob keeps the options object's default rather than picking one up here.
        assertThat(mapped.getShutdownTimeout())
                .isEqualTo(PubSubSubscriberOptions.defaults().getShutdownTimeout());
    }
}
