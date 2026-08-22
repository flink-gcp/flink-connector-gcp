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

package io.github.flink.gcp.connector.bigquery.sink;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCreateOptions}. */
class TableCreateOptionsTest {

    @Test
    void defaultsAreEmpty() {
        TableCreateOptions options = TableCreateOptions.defaults();

        assertThat(options.getTimePartitioningType()).isNull();
        assertThat(options.getTimePartitioningField()).isNull();
        assertThat(options.getTimePartitioningExpirationMs()).isNull();
        assertThat(options.getClusteredFields()).isEmpty();
    }

    @Test
    void builderRoundTrip() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "event_ts")
                        .timePartitioningExpiration(Duration.ofDays(30))
                        .clusteredFields(Arrays.asList("customer", "region"))
                        .build();

        assertThat(options.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(options.getTimePartitioningField()).isEqualTo("event_ts");
        assertThat(options.getTimePartitioningExpirationMs())
                .isEqualTo(Duration.ofDays(30).toMillis());
        assertThat(options.getClusteredFields()).containsExactly("customer", "region");
    }

    @Test
    void ingestionTimePartitioningHasNoField() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.HOUR)
                        .build();

        assertThat(options.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.HOUR);
        assertThat(options.getTimePartitioningField()).isNull();
    }

    @Test
    void expirationRequiresPartitioning() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioningExpiration(Duration.ofDays(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires timePartitioning");
    }

    /**
     * The setter converts to milliseconds on the spot, so a sub-millisecond expiration used to
     * reach the create request as a zero — an expiration the user never asked for (ADR-0068).
     */
    @Test
    void rejectsASubMillisecondExpiration() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioningExpiration(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timePartitioningExpiration must be at least 1 millisecond");
    }

    @Test
    void rejectsTooManyClusteredFields() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .clusteredFields(Arrays.asList("a", "b", "c", "d", "e")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 4");
    }

    @Test
    void rejectsBlankClusteredField() {
        assertThatThrownBy(
                        () -> TableCreateOptions.builder().clusteredFields(Arrays.asList("a", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void rejectsBlankPartitioningField() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioning(
                                                TableCreateOptions.TimePartitioningType.DAY, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void valueEquality() {
        TableCreateOptions a =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "ts")
                        .build();
        TableCreateOptions b =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "ts")
                        .build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(TableCreateOptions.defaults());
    }
}
