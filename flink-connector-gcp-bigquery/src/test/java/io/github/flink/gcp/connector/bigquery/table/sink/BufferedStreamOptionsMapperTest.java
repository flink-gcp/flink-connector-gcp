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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
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

/** Tests for {@link BufferedStreamOptionsMapper}. */
class BufferedStreamOptionsMapperTest {

    /** The key prefix the whole family shares, for the reflective coverage test below. */
    private static final String PREFIX = "sink.buffered-stream.";

    /**
     * Every {@code BufferedStreamOptions.Builder} setter and the option that feeds it.
     *
     * <p>Written out rather than derived, because the option keys are grouped ({@code
     * sink.buffered-stream.recovery.*}, {@code .retry.*}) and no naming rule turns one into the
     * other — {@code maxRetryDuration} is {@code retry.max-duration}, not {@code
     * max-retry-duration}. The two reflection tests below are what make the table exhaustive.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put(
                "maxAppendRequestBytes",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_MAX_APPEND_REQUEST_BYTES);
        SETTER_TO_OPTION.put(
                "destinationIdleTimeout",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_DESTINATION_IDLE_TIMEOUT);
        SETTER_TO_OPTION.put(
                "recoveryInitialBackoff",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_INITIAL_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxBackoff",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxAttempts",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RECOVERY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "retryInitialDelay",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_INITIAL_DELAY);
        SETTER_TO_OPTION.put(
                "retryDelayMultiplier",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_DELAY_MULTIPLIER);
        SETTER_TO_OPTION.put(
                "retryMaxDelay", BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DELAY);
        SETTER_TO_OPTION.put(
                "retryMaxAttempts",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "maxRetryDuration",
                BigQueryConnectorOptions.SINK_BUFFERED_STREAM_RETRY_MAX_DURATION);
    }

    private static String key(String setter) {
        return SETTER_TO_OPTION.get(setter).key();
    }

    private static BufferedStreamOptions map(Map<String, String> options) {
        return BufferedStreamOptionsMapper.map(Configuration.fromMap(options));
    }

    @Test
    void everyBufferedStreamKnobHasAnOption() {
        // Not filtered on arity or name: every knob of any shape must appear.
        Set<String> setters =
                Arrays.stream(BufferedStreamOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == BufferedStreamOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());
        // Both directions: a new knob without an option, and an option whose knob was removed.
        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    @Test
    void everyOptionOfTheFamilyFeedsAKnob() {
        // The other half of the guard above, and the one a new key would otherwise slip past: an
        // option declared under the prefix that no setter consumes. The expected side is read out
        // of BigQueryConnectorOptions rather than written here — a literal list would only restate
        // SETTER_TO_OPTION and could never disagree with it.
        Set<String> declared = OptionFamilies.declaredKeysUnder(PREFIX);
        // Guards the reflection itself: an empty set would make the assertion vacuous.
        assertThat(declared).isNotEmpty();

        Set<String> mapped =
                SETTER_TO_OPTION.values().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());

        assertThat(mapped).isEqualTo(declared);
    }

    @Test
    void noKeyOfTheFamilyStillProducesTheDefaults() {
        // Unlike DefaultStreamOptionsMapper, which returns null here: the builder requires this
        // object for STORAGE_API_EXACTLY_ONCE, so a DDL that selects that method and tunes nothing
        // must still get one — and every knob of it defaulted is exactly what it asked for.
        assertThat(map(new HashMap<>())).isEqualTo(BufferedStreamOptions.builder().build());

        // An unrelated sink option changes nothing either.
        Map<String, String> unrelated = new HashMap<>();
        unrelated.put(BigQueryConnectorOptions.SINK_LOCATION.key(), "US");
        assertThat(map(unrelated)).isEqualTo(BufferedStreamOptions.builder().build());
    }

    @Test
    void oneKeyOfTheFamilyLeavesEveryOtherKnobAtItsDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(key("retryMaxAttempts"), "7");

        BufferedStreamOptions mapped = map(options);
        BufferedStreamOptions defaults = BufferedStreamOptions.builder().build();

        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(mapped.getMaxAppendRequestBytes())
                .isEqualTo(defaults.getMaxAppendRequestBytes());
        assertThat(mapped.getRecoveryMaxAttempts()).isEqualTo(defaults.getRecoveryMaxAttempts());
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put(key("maxAppendRequestBytes"), "1 mb");
        options.put(key("destinationIdleTimeout"), "2 h");
        options.put(key("recoveryInitialBackoff"), "1 s");
        options.put(key("recoveryMaxBackoff"), "20 s");
        options.put(key("recoveryMaxAttempts"), "11");
        options.put(key("retryInitialDelay"), "2 s");
        options.put(key("retryDelayMultiplier"), "3.5");
        options.put(key("retryMaxDelay"), "40 s");
        options.put(key("retryMaxAttempts"), "6");
        options.put(key("maxRetryDuration"), "9 min");

        BufferedStreamOptions mapped = map(options);

        assertThat(mapped.getMaxAppendRequestBytes()).isEqualTo(1024L * 1024L);
        assertThat(mapped.getDestinationIdleTimeout()).isEqualTo(Duration.ofHours(2));
        assertThat(mapped.getRecoveryInitialBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(mapped.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(20));
        assertThat(mapped.getRecoveryMaxAttempts()).isEqualTo(11);
        assertThat(mapped.getRetryInitialDelay()).isEqualTo(Duration.ofSeconds(2));
        assertThat(mapped.getRetryDelayMultiplier()).isEqualTo(3.5);
        assertThat(mapped.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(40));
        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(6);
        assertThat(mapped.getMaxRetryDuration()).isEqualTo(Duration.ofMinutes(9));
    }

    @Test
    void reportsWhichKeysOfTheFamilyAreSet() {
        Map<String, String> options = new HashMap<>();
        options.put(key("retryMaxAttempts"), "6");
        options.put(key("recoveryMaxAttempts"), "11");

        assertThat(BufferedStreamOptionsMapper.presentKeys(Configuration.fromMap(options)))
                .containsExactly(key("recoveryMaxAttempts"), key("retryMaxAttempts"));
    }
}
