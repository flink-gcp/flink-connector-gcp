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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for mapping the {@code sink.*} keys onto the writer's tuning. */
class WriterOptionsMapperTest {

    private static Configuration configuration(String... keysAndValues) {
        Configuration config = new Configuration();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            config.setString(keysAndValues[i], keysAndValues[i + 1]);
        }
        return config;
    }

    @Test
    void anEmptyConfigProducesExactlyTheConnectorDefaults() {
        // The whole contract of the mapper: it adds no default of its own, so a DDL that tunes
        // nothing leaves every knob where the DataStream API put it.
        assertThat(WriterOptionsMapper.map(new Configuration()))
                .isEqualTo(BigtableWriterOptions.defaults());
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        BigtableWriterOptions options =
                WriterOptionsMapper.map(
                        configuration(
                                "sink.batching.element-count-threshold", "500",
                                "sink.batching.request-byte-threshold", "4mb",
                                "sink.in-flight.max-entries", "2000",
                                "sink.in-flight.max-bytes", "32mb",
                                "sink.max-consecutive-rejections", "7",
                                "sink.recovery.initial-backoff", "250ms",
                                "sink.recovery.max-backoff", "20s",
                                "sink.recovery.max-attempts", "4",
                                "sink.destination-idle-timeout", "30min",
                                "sink.metrics.per-destination", "true"));

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(500L);
        assertThat(options.getBatchRequestByteThreshold()).isEqualTo(4L * 1024 * 1024);
        assertThat(options.getMaxInFlightEntries()).isEqualTo(2000);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(32L * 1024 * 1024);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(7);
        assertThat(options.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(250));
        assertThat(options.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.getRecoveryMaxAttempts()).isEqualTo(4);
        assertThat(options.getDestinationIdleTimeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(options.isPerDestinationMetrics()).isTrue();
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        // The two batch thresholds are the ones where "unset" is visible: the connector leaves
        // them null so the client's own thresholds apply, and a mapper that defaulted them would
        // silently move that decision here.
        BigtableWriterOptions options =
                WriterOptionsMapper.map(
                        configuration("sink.batching.element-count-threshold", "500"));

        assertThat(options.getBatchElementCountThreshold()).isEqualTo(500L);
        assertThat(options.getBatchRequestByteThreshold()).isNull();
    }

    @Test
    void namesTheOptionKeyWhenAValueIsRejected() {
        assertThatThrownBy(
                        () ->
                                WriterOptionsMapper.map(
                                        configuration(
                                                "sink.batching.element-count-threshold", "0")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.batching.element-count-threshold' is invalid")
                .hasMessageContaining("batchElementCountThreshold must be positive");

        assertThatThrownBy(
                        () ->
                                WriterOptionsMapper.map(
                                        configuration(
                                                "sink.batching.request-byte-threshold", "0b")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.batching.request-byte-threshold' is invalid")
                .hasMessageContaining("batchRequestByteThreshold must be positive");

        assertThatThrownBy(
                        () ->
                                WriterOptionsMapper.map(
                                        configuration("sink.in-flight.max-entries", "0")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.in-flight.max-entries' is invalid")
                .hasMessageContaining("maxInFlightEntries must be positive");
    }
}
