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

package io.github.flink.gcp.connector.bigtable.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableWriterOptions}. */
class BigtableWriterOptionsTest {

    @Test
    void defaultsLeaveTheBatchThresholdsToTheClient() {
        BigtableWriterOptions options = BigtableWriterOptions.defaults();

        // Null rather than a restatement of the client's 100 / 20 MB: an unset threshold has to
        // stay unset all the way to the settings builder, or a client retune would be overridden.
        assertThat(options.getBatchElementCount()).isNull();
        assertThat(options.getBatchByteSize()).isNull();
        assertThat(options.getMaxInFlightMutations()).isEqualTo(1000);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(100);
        assertThat(options).isEqualTo(BigtableWriterOptions.builder().build());
    }

    @Test
    void carriesEveryConfiguredValue() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder()
                        .batchElementCount(50)
                        .batchByteSize(1024)
                        .maxInFlightMutations(7)
                        .maxInFlightBytes(4096)
                        .maxConsecutiveRejections(5)
                        .build();

        assertThat(options.getBatchElementCount()).isEqualTo(50L);
        assertThat(options.getBatchByteSize()).isEqualTo(1024L);
        assertThat(options.getMaxInFlightMutations()).isEqualTo(7);
        assertThat(options.getMaxInFlightBytes()).isEqualTo(4096L);
        assertThat(options.getMaxConsecutiveRejections()).isEqualTo(5);
        assertThat(options.toString())
                .contains(
                        "batchElementCount=50",
                        "maxInFlightMutations=7",
                        "maxConsecutiveRejections=5");
    }

    @Test
    void isValueBased() {
        BigtableWriterOptions options =
                BigtableWriterOptions.builder().maxInFlightMutations(7).build();

        assertThat(options)
                .isEqualTo(BigtableWriterOptions.builder().maxInFlightMutations(7).build())
                .hasSameHashCodeAs(BigtableWriterOptions.builder().maxInFlightMutations(7).build())
                .isNotEqualTo(BigtableWriterOptions.defaults())
                .isNotEqualTo(
                        BigtableWriterOptions.builder()
                                .maxInFlightMutations(7)
                                .batchElementCount(50)
                                .build());
    }

    @Test
    void rejectsNonPositiveValues() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        assertThatThrownBy(() -> builder.batchElementCount(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchByteSize(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightMutations(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxInFlightBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRejectionBoundTakesOnlyPositiveValuesOrTheUnboundedSentinel() {
        BigtableWriterOptions.Builder builder = BigtableWriterOptions.builder();

        // Zero has no meaning here: "no rejection tolerated" is 1, and a bound of zero would
        // silently override the dropping handler the user configured.
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConsecutiveRejections");
        assertThatThrownBy(() -> builder.maxConsecutiveRejections(-2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(
                        builder.maxConsecutiveRejections(BigtableWriterOptions.UNBOUNDED)
                                .build()
                                .getMaxConsecutiveRejections())
                .isEqualTo(BigtableWriterOptions.UNBOUNDED);
    }
}
