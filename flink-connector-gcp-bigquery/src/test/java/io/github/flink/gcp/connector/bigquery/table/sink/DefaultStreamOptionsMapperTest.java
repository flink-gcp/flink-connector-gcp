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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultStreamOptionsMapper}. */
class DefaultStreamOptionsMapperTest {

    /**
     * Every {@code DefaultStreamOptions.Builder} setter and the option that feeds it.
     *
     * <p>Written out rather than derived, because the option keys are grouped ({@code
     * sink.default-stream.recovery.*}, {@code .retry.*}) and no naming rule turns one into the
     * other — {@code maxRetryDuration} is {@code retry.max-duration}, not {@code
     * max-retry-duration}. The reflection test below is what makes the table exhaustive.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put(
                "maxAppendRequestBytes",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_APPEND_REQUEST_BYTES);
        SETTER_TO_OPTION.put(
                "recoveryInitialBackoff",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_INITIAL_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxBackoff",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_BACKOFF);
        SETTER_TO_OPTION.put(
                "recoveryMaxAttempts",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RECOVERY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "retryInitialDelay",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_INITIAL_DELAY);
        SETTER_TO_OPTION.put(
                "retryDelayMultiplier",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_DELAY_MULTIPLIER);
        SETTER_TO_OPTION.put(
                "retryMaxDelay", BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DELAY);
        SETTER_TO_OPTION.put(
                "retryMaxAttempts",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "maxRetryDuration",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_RETRY_MAX_DURATION);
        SETTER_TO_OPTION.put(
                "maxInflightRequests",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_REQUESTS);
        SETTER_TO_OPTION.put(
                "maxInflightBytes",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_INFLIGHT_BYTES);
        SETTER_TO_OPTION.put(
                "minConnectionsPerRegion",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MIN_CONNECTIONS_PER_REGION);
        SETTER_TO_OPTION.put(
                "maxConnectionsPerRegion",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_MAX_CONNECTIONS_PER_REGION);
        SETTER_TO_OPTION.put(
                "destinationIdleTimeout",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_DESTINATION_IDLE_TIMEOUT);
        SETTER_TO_OPTION.put(
                "flushInterval", BigQueryConnectorOptions.SINK_DEFAULT_STREAM_FLUSH_INTERVAL);
        SETTER_TO_OPTION.put(
                "perDestinationMetrics",
                BigQueryConnectorOptions.SINK_DEFAULT_STREAM_PER_DESTINATION_METRICS);
    }

    private static String key(String setter) {
        return SETTER_TO_OPTION.get(setter).key();
    }

    private static DefaultStreamOptions map(Map<String, String> options) {
        return DefaultStreamOptionsMapper.map(Configuration.fromMap(options));
    }

    @Test
    void everyDefaultStreamKnobHasAnOption() {
        // Not filtered on arity or name: a knob of any shape must appear, which is the whole point
        // of this guard.
        Set<String> setters =
                Arrays.stream(DefaultStreamOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == DefaultStreamOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        // Both directions: a new knob without an option, and an option whose knob was removed.
        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    @Test
    void everyOptionOfTheFamilyFeedsAKnob() {
        // The other half of the guard above, and the one a new key would otherwise slip past: an
        // option declared under the sink.default-stream.* prefix that no setter consumes.
        Set<String> declared = OptionFamilies.declaredKeysUnder("sink.default-stream.");
        // Guards the reflection itself: an empty set would make the assertion vacuous.
        assertThat(declared).isNotEmpty();

        Set<String> mapped =
                SETTER_TO_OPTION.values().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());

        assertThat(mapped).isEqualTo(declared);
    }

    @Test
    void noKeyOfTheFamilyMeansNoOptionsObject() {
        assertThat(map(new HashMap<>())).isNull();

        // An unrelated sink option does not conjure one either.
        Map<String, String> unrelated = new HashMap<>();
        unrelated.put(BigQueryConnectorOptions.SINK_LOCATION.key(), "US");
        assertThat(map(unrelated)).isNull();
    }

    @Test
    void oneKeyOfTheFamilyLeavesEveryOtherKnobAtItsDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(key("maxInflightRequests"), "7");

        DefaultStreamOptions mapped = map(options);
        DefaultStreamOptions defaults = DefaultStreamOptions.builder().build();

        assertThat(mapped.getMaxInflightRequests()).isEqualTo(7);
        assertThat(mapped.getMaxAppendRequestBytes())
                .isEqualTo(defaults.getMaxAppendRequestBytes());
        assertThat(mapped.getRecoveryMaxAttempts()).isEqualTo(defaults.getRecoveryMaxAttempts());
        assertThat(mapped.getFlushInterval()).isNull();
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put(key("maxAppendRequestBytes"), "1 mb");
        options.put(key("recoveryInitialBackoff"), "1 s");
        options.put(key("recoveryMaxBackoff"), "20 s");
        options.put(key("recoveryMaxAttempts"), "11");
        options.put(key("retryInitialDelay"), "2 s");
        options.put(key("retryDelayMultiplier"), "3.5");
        options.put(key("retryMaxDelay"), "40 s");
        options.put(key("retryMaxAttempts"), "6");
        options.put(key("maxRetryDuration"), "9 min");
        options.put(key("maxInflightRequests"), "42");
        options.put(key("maxInflightBytes"), "8 mb");
        options.put(key("minConnectionsPerRegion"), "3");
        options.put(key("maxConnectionsPerRegion"), "30");
        options.put(key("destinationIdleTimeout"), "2 h");
        options.put(key("flushInterval"), "5 s");
        options.put(key("perDestinationMetrics"), "true");

        DefaultStreamOptions mapped = map(options);

        assertThat(mapped.getMaxAppendRequestBytes()).isEqualTo(1024L * 1024L);
        assertThat(mapped.getRecoveryInitialBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(mapped.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(20));
        assertThat(mapped.getRecoveryMaxAttempts()).isEqualTo(11);
        assertThat(mapped.getRetryInitialDelay()).isEqualTo(Duration.ofSeconds(2));
        assertThat(mapped.getRetryDelayMultiplier()).isEqualTo(3.5);
        assertThat(mapped.getRetryMaxDelay()).isEqualTo(Duration.ofSeconds(40));
        assertThat(mapped.getRetryMaxAttempts()).isEqualTo(6);
        assertThat(mapped.getMaxRetryDuration()).isEqualTo(Duration.ofMinutes(9));
        assertThat(mapped.getMaxInflightRequests()).isEqualTo(42);
        assertThat(mapped.getMaxInflightBytes()).isEqualTo(8L * 1024L * 1024L);
        assertThat(mapped.getMinConnectionsPerRegion()).isEqualTo(3);
        assertThat(mapped.getMaxConnectionsPerRegion()).isEqualTo(30);
        assertThat(mapped.getDestinationIdleTimeout()).isEqualTo(Duration.ofHours(2));
        assertThat(mapped.getFlushInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(mapped.isPerDestinationMetrics()).isTrue();
    }

    @Test
    void reportsWhichKeysOfTheFamilyAreSet() {
        Map<String, String> options = new HashMap<>();
        options.put(key("flushInterval"), "5 s");
        options.put(key("maxInflightRequests"), "42");

        assertThat(DefaultStreamOptionsMapper.presentKeys(Configuration.fromMap(options)))
                .containsExactly(key("maxInflightRequests"), key("flushInterval"));
    }

    @Test
    void namesTheOptionKeyWhenAValueIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.default-stream.max-inflight-requests", "0");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Option 'sink.default-stream.max-inflight-requests' is invalid")
                .hasMessageContaining("maxInflightRequests must be positive");
    }
}
