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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Holds the mapping to the builder and the cross-check the mapper words in option keys. The
 * messages are asserted on phrases only the mapper's own sentences carry, never on a bare key.
 */
class WriterOptionsMapperTest {

    private static DatastoreWriterOptions map(Map<String, String> options) {
        return WriterOptionsMapper.map(Configuration.fromMap(options));
    }

    @Test
    void noOptionLeavesEveryKnobAtTheBuildersDefault() {
        assertThat(map(Map.of())).isEqualTo(DatastoreWriterOptions.builder().build());
    }

    @Test
    void everyOptionReachesItsKnob() {
        DatastoreWriterOptions options =
                map(
                        Map.ofEntries(
                                Map.entry("sink.buffer-flush.max-mutations", "100"),
                                Map.entry("sink.buffer-flush.max-size", "2 mb"),
                                Map.entry("sink.request-timeout", "30 s"),
                                Map.entry("sink.recovery.initial-backoff", "1 s"),
                                Map.entry("sink.recovery.max-backoff", "20 s"),
                                Map.entry("sink.recovery.max-attempts", "4"),
                                Map.entry("sink.throttling.enabled", "false"),
                                Map.entry("sink.throttling.parallelism", "8"),
                                Map.entry("sink.id-allocation.batch-size", "250")));

        assertThat(options)
                .isEqualTo(
                        DatastoreWriterOptions.builder()
                                .maxBatchMutations(100)
                                .maxBatchBytes(2L * 1024 * 1024)
                                .requestTimeout(Duration.ofSeconds(30))
                                .recoveryInitialBackoff(Duration.ofSeconds(1))
                                .recoveryMaxBackoff(Duration.ofSeconds(20))
                                .recoveryMaxAttempts(4)
                                .throttlingEnabled(false)
                                .throttlingParallelism(8)
                                .idAllocationBatchSize(250)
                                .build());
    }

    @Test
    void aValueTheBuilderRefusesIsReportedUnderItsKey() {
        assertThatThrownBy(() -> map(Map.of("sink.buffer-flush.max-size", "11 mb")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.buffer-flush.max-size' is invalid")
                .hasMessageContaining("Datastore's maximum request size");
        assertThatThrownBy(() -> map(Map.of("sink.buffer-flush.max-mutations", "0")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.buffer-flush.max-mutations' is invalid");
        assertThatThrownBy(() -> map(Map.of("sink.id-allocation.batch-size", "0")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.id-allocation.batch-size' is invalid")
                .hasMessageContaining("idAllocationBatchSize must be positive");
    }

    @Test
    void aBackoffCapBelowTheInitialBackoffIsRefusedInOptionKeys() {
        assertThatThrownBy(
                        () ->
                                map(
                                        Map.of(
                                                "sink.recovery.initial-backoff", "2 s",
                                                "sink.recovery.max-backoff", "1 s")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'sink.recovery.max-backoff' (PT1S) must be at least"
                                + " 'sink.recovery.initial-backoff' (PT2S)");
    }

    @Test
    void theCrossCheckComparesAnOptionWithTheOtherKnobsDefault() {
        // The builder defaults are 500 ms and 10 s: each option alone is held to the other's.
        assertThatThrownBy(() -> map(Map.of("sink.recovery.max-backoff", "100 ms")))
                .hasMessageContaining(
                        "'sink.recovery.max-backoff' (PT0.1S) must be at least"
                                + " 'sink.recovery.initial-backoff' (PT0.5S)");
        assertThatThrownBy(() -> map(Map.of("sink.recovery.initial-backoff", "11 s")))
                .hasMessageContaining("(PT10S) must be at least");
        assertThat(map(Map.of("sink.recovery.initial-backoff", "10 s")).getRecoveryInitialBackoff())
                .isEqualTo(Duration.ofSeconds(10));
    }
}
