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

package io.github.flink.gcp.connector.bigtable.sink;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The service worker's backend client settings must build against the pinned SDK before a lease is
 * created: the SDK requires equal retry codes on the single-row, multi-row and bulk read settings
 * and checks them only at {@code build()}, which is where attempt 1 of the recovery plan failed on
 * the worker after the instance existed (see the plan's Attempts section).
 */
class BigtableProductionRecoveryProbeTest {
    @Test
    void backendSettingsBuildWithoutRetriesOnAnyReadPath() {
        var settings =
                BigtableProductionRecoveryProbe.backendSettings(
                                TableDestination.of("p", "i", "recovery-datastream-flink2"))
                        .build();
        var stubs = settings.getStubSettings();
        assertThat(stubs.getAppProfileId()).isEqualTo(LocalStagedHarness.PROFILE);
        assertThat(stubs.readRowsSettings().getRetryableCodes()).isEmpty();
        assertThat(stubs.readRowSettings().getRetryableCodes()).isEmpty();
        assertThat(stubs.bulkReadRowsSettings().getRetryableCodes()).isEmpty();
        assertThat(stubs.checkAndMutateRowSettings().getRetryableCodes()).isEmpty();
        assertThat(stubs.checkAndMutateRowSettings().getRetrySettings().getMaxAttempts())
                .isEqualTo(1);
        assertThat(stubs.checkAndMutateRowSettings().getRetrySettings().getTotalTimeoutDuration())
                .isEqualTo(Duration.ofSeconds(20));
    }
}
