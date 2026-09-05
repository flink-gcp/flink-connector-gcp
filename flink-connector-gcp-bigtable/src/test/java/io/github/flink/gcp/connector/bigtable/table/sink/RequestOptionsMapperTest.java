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

import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestOptionsMapperTest {
    @Test
    void mapsAllFiveOptionsWithoutDuplicatingDefaults() {
        assertThat(RequestOptionsMapper.map(new Configuration()))
                .isEqualTo(BigtableRequestOptions.builder().build());
        BigtableRequestOptions mapped =
                RequestOptionsMapper.map(
                        Configuration.fromMap(
                                Map.of(
                                        "sink.request-timeout",
                                        "3s",
                                        "sink.in-flight.max-requests",
                                        "7",
                                        "sink.destination-idle-timeout",
                                        "5min",
                                        "sink.max-active-instances",
                                        "2",
                                        "sink.metrics.per-destination",
                                        "true")));
        assertThat(mapped)
                .isEqualTo(
                        BigtableRequestOptions.builder()
                                .requestTimeout(Duration.ofSeconds(3))
                                .maxInFlightRequests(7)
                                .destinationIdleTimeout(Duration.ofMinutes(5))
                                .maxActiveInstances(2)
                                .perDestinationMetrics(true)
                                .build());
    }

    @ParameterizedTest
    @CsvSource({
        "sink.request-timeout, 0s, requestTimeout must be at least 1 millisecond",
        "sink.in-flight.max-requests, 0, maxInFlightRequests must be positive",
        "sink.destination-idle-timeout, 0s, destinationIdleTimeout must be positive",
        "sink.max-active-instances, 0, maxActiveInstances must be positive"
    })
    void rejectsValuesNamingBothTheSqlKeyAndTheBuilderConstraint(
            String key, String value, String detail) {
        assertThatThrownBy(
                        () -> RequestOptionsMapper.map(Configuration.fromMap(Map.of(key, value))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option '" + key + "' is invalid")
                .hasMessageContaining(detail);
    }
}
