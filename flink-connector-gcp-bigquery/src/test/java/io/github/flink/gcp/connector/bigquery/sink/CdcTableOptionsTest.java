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

package io.github.flink.gcp.connector.bigquery.sink;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CdcTableOptions}. */
class CdcTableOptionsTest {

    @Test
    void defaultsLeaveTheExistingContractUnmanaged() {
        CdcTableOptions options = CdcTableOptions.builder().build();

        assertThat(options.getPrimaryKeyColumns()).isEmpty();
        assertThat(options.getMaxStaleness()).isNull();
        assertThat(options.managesMaxStaleness()).isFalse();
        assertThat(options.clearsMaxStaleness()).isFalse();
    }

    @Test
    void builderRoundTrip() {
        CdcTableOptions options =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("customer", "event_ts"))
                        .maxStaleness(Duration.ofMinutes(10))
                        .build();

        assertThat(options.getPrimaryKeyColumns()).containsExactly("customer", "event_ts");
        assertThat(options.getMaxStaleness()).isEqualTo(Duration.ofMinutes(10));
        assertThat(options.managesMaxStaleness()).isTrue();
        assertThat(options.clearsMaxStaleness()).isFalse();
    }

    @Test
    void maximumStalenessAndClearOverrideEachOther() {
        CdcTableOptions cleared =
                CdcTableOptions.builder()
                        .maxStaleness(Duration.ofMinutes(10))
                        .clearMaxStaleness()
                        .build();
        CdcTableOptions set =
                CdcTableOptions.builder()
                        .clearMaxStaleness()
                        .maxStaleness(Duration.ofMinutes(10))
                        .build();

        assertThat(cleared.clearsMaxStaleness()).isTrue();
        assertThat(cleared.getMaxStaleness()).isNull();
        assertThat(set.clearsMaxStaleness()).isFalse();
        assertThat(set.getMaxStaleness()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void rejectsDuplicatePrimaryKeyColumnsIgnoringCase() {
        assertThatThrownBy(
                        () ->
                                CdcTableOptions.builder()
                                        .primaryKeyColumns(Arrays.asList("id", "ID")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinct ignoring case");
    }

    @Test
    void rejectsMoreThanSixteenPrimaryKeyColumns() {
        assertThatThrownBy(
                        () ->
                                CdcTableOptions.builder()
                                        .primaryKeyColumns(
                                                Arrays.asList(
                                                        "c1", "c2", "c3", "c4", "c5", "c6", "c7",
                                                        "c8", "c9", "c10", "c11", "c12", "c13",
                                                        "c14", "c15", "c16", "c17")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 16");
    }

    @Test
    void rejectsSubMicrosecondMaximumStaleness() {
        assertThatThrownBy(() -> CdcTableOptions.builder().maxStaleness(Duration.ofNanos(1_001)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact number of microseconds");
    }

    @Test
    void rejectsNonPositiveMaximumStaleness() {
        assertThatThrownBy(() -> CdcTableOptions.builder().maxStaleness(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void valueEquality() {
        CdcTableOptions a =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("id"))
                        .clearMaxStaleness()
                        .build();
        CdcTableOptions b =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("id"))
                        .clearMaxStaleness()
                        .build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(CdcTableOptions.builder().build());
    }
}
