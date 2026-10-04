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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Holds the mapping to the builder and the cross-checks the mapper words in option keys. The
 * messages are asserted on phrases only the mapper's own sentences carry, never on a bare key:
 * {@code FactoryUtil} echoes every option into its own message, so a key alone would pass with the
 * check deleted.
 */
class WriterOptionsMapperTest {

    private static FirestoreWriterOptions map(Map<String, String> options) {
        return WriterOptionsMapper.map(Configuration.fromMap(options));
    }

    @Test
    void noOptionLeavesEveryKnobAtTheBuildersDefault() {
        assertThat(map(Map.of())).isEqualTo(FirestoreWriterOptions.builder().build());
    }

    @Test
    void everyOptionReachesItsKnob() {
        FirestoreWriterOptions options =
                map(
                        Map.ofEntries(
                                Map.entry("sink.throttling.initial-ops-per-second", "100"),
                                Map.entry("sink.throttling.max-ops-per-second", "200"),
                                Map.entry("sink.write.max-attempts", "3"),
                                Map.entry("sink.retry.total-timeout", "2 min"),
                                Map.entry("sink.retry.initial-delay", "200 ms"),
                                Map.entry("sink.retry.delay-multiplier", "1.5"),
                                Map.entry("sink.retry.max-delay", "30 s"),
                                Map.entry("sink.retry.initial-rpc-timeout", "20 s"),
                                Map.entry("sink.retry.rpc-timeout-multiplier", "1.2"),
                                Map.entry("sink.retry.max-rpc-timeout", "40 s"),
                                Map.entry("sink.retry.max-attempts", "4"),
                                Map.entry("sink.in-flight.max-writes", "120"),
                                Map.entry("sink.in-flight.max-bytes", "4 mb")));

        assertThat(options)
                .isEqualTo(
                        FirestoreWriterOptions.builder()
                                .initialOpsPerSecond(100)
                                .maxOpsPerSecond(200)
                                .writeMaxAttempts(3)
                                .retryTotalTimeout(Duration.ofMinutes(2))
                                .retryInitialDelay(Duration.ofMillis(200))
                                .retryDelayMultiplier(1.5)
                                .retryMaxDelay(Duration.ofSeconds(30))
                                .retryInitialRpcTimeout(Duration.ofSeconds(20))
                                .retryRpcTimeoutMultiplier(1.2)
                                .retryMaxRpcTimeout(Duration.ofSeconds(40))
                                .retryMaxAttempts(4)
                                .maxInFlightWrites(120)
                                .maxInFlightBytes(4L * 1024 * 1024)
                                .build());
        assertThat(map(Map.of("sink.throttling.enabled", "false")).isThrottlingEnabled()).isFalse();
    }

    @Test
    void aValueTheBuilderRefusesIsRenamedToItsKey() {
        assertThatThrownBy(() -> map(Map.of("sink.write.max-attempts", "0")))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith("Option 'sink.write.max-attempts' is invalid:");
    }

    @Test
    void theCrossChecksNameTheOptionKeys() {
        assertThatThrownBy(
                        () ->
                                map(
                                        Map.of(
                                                "sink.throttling.enabled", "false",
                                                "sink.throttling.initial-ops-per-second", "100")))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "'sink.throttling.initial-ops-per-second' sets the throttle's rate, so it"
                                + " cannot be combined with 'sink.throttling.enabled' = false.");
        assertThatThrownBy(
                        () ->
                                map(
                                        Map.of(
                                                "sink.throttling.enabled", "false",
                                                "sink.throttling.max-ops-per-second", "100")))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "'sink.throttling.max-ops-per-second' sets the throttle's rate, so it"
                                + " cannot be combined with 'sink.throttling.enabled' = false.");
        assertThatThrownBy(
                        () ->
                                map(
                                        Map.of(
                                                "sink.throttling.initial-ops-per-second", "300",
                                                "sink.throttling.max-ops-per-second", "100")))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "'sink.throttling.initial-ops-per-second' (300) must not exceed"
                                + " 'sink.throttling.max-ops-per-second' (100).");
        assertThatThrownBy(() -> map(Map.of("sink.retry.max-delay", "1 ms")))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith(
                        "'sink.retry.max-delay' (PT0.001S) must not be shorter than 'sink.retry.initial-delay'");
        assertThatThrownBy(() -> map(Map.of("sink.retry.max-rpc-timeout", "1 ms")))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith(
                        "'sink.retry.max-rpc-timeout' (PT0.001S) must not be shorter than"
                                + " 'sink.retry.initial-rpc-timeout'");
    }

    @Test
    void retryOverridesEqualToGaxsDefaultsAreRefusedInOptionKeys() {
        RetrySettings gax = ServiceOptions.getDefaultRetrySettings();
        Map<String, String> options =
                Map.of(
                        "sink.retry.total-timeout",
                        gax.getTotalTimeoutDuration().toMillis() + " ms",
                        "sink.retry.initial-delay",
                        gax.getInitialRetryDelayDuration().toMillis() + " ms",
                        "sink.retry.delay-multiplier",
                        String.valueOf(gax.getRetryDelayMultiplier()),
                        "sink.retry.max-delay",
                        gax.getMaxRetryDelayDuration().toMillis() + " ms",
                        "sink.retry.initial-rpc-timeout",
                        gax.getInitialRpcTimeoutDuration().toMillis() + " ms",
                        "sink.retry.rpc-timeout-multiplier",
                        String.valueOf(gax.getRpcTimeoutMultiplier()),
                        "sink.retry.max-rpc-timeout",
                        gax.getMaxRpcTimeoutDuration().toMillis() + " ms",
                        "sink.retry.max-attempts",
                        String.valueOf(gax.getMaxAttempts()));

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith(
                        "The 'sink.retry.*' options add up to gax's default retry settings");
    }
}
