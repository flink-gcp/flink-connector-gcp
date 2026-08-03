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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableWriterOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the settings {@link DefaultMutationBatcherFactory} builds — the options-to-settings
 * mapping, which is otherwise invisible: a threshold that never reaches the client looks exactly
 * like one that does.
 */
class DefaultMutationBatcherFactoryTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void carriesTheDestinationAndTheApplicationProfile() {
        BigtableDataSettings settings =
                factory("batch-profile", BigtableWriterOptions.defaults(), "localhost:8086")
                        .settings();

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
        assertThat(settings.getAppProfileId()).isEqualTo("batch-profile");
    }

    @Test
    void appliesTheConfiguredBatchThresholds() {
        BatchingSettings batching =
                batchingSettings(
                        BigtableWriterOptions.builder()
                                .batchElementCount(7)
                                .batchByteSize(4096)
                                .build());

        assertThat(batching.getElementCountThreshold()).isEqualTo(7L);
        assertThat(batching.getRequestByteThreshold()).isEqualTo(4096L);
    }

    @Test
    void leavesEachUnsetThresholdAtTheClientsOwn() {
        BatchingSettings clientDefaults = batchingSettings(BigtableWriterOptions.defaults());
        // Only one of the two is set, so the other has to keep the client's value rather than be
        // reset alongside it.
        BatchingSettings batching =
                batchingSettings(BigtableWriterOptions.builder().batchElementCount(7).build());

        assertThat(batching.getElementCountThreshold()).isEqualTo(7L);
        assertThat(batching.getRequestByteThreshold())
                .isEqualTo(clientDefaults.getRequestByteThreshold());
        assertThat(batching.getDelayThresholdDuration())
                .isEqualTo(clientDefaults.getDelayThresholdDuration());
    }

    @Test
    void pointsTheClientAtTheEmulatorEndpoint() {
        BigtableDataSettings settings =
                factory(null, BigtableWriterOptions.defaults(), "bigtable.example:9035").settings();

        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
        // The emulator mode is the only one that must never present credentials.
        assertThat(settings.getStubSettings().getCredentialsProvider().getClass().getSimpleName())
                .isEqualTo("NoCredentialsProvider");
    }

    private static BatchingSettings batchingSettings(BigtableWriterOptions options) {
        return factory(null, options, "localhost:8086")
                .settings()
                .getStubSettings()
                .bulkMutateRowsSettings()
                .getBatchingSettings();
    }

    private static DefaultMutationBatcherFactory factory(
            String appProfileId, BigtableWriterOptions options, String emulatorEndpoint) {
        return new DefaultMutationBatcherFactory(
                TABLE, appProfileId, options, EmulatorEndpoint.parse(emulatorEndpoint));
    }
}
