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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import com.google.api.gax.retrying.RetrySettings;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamWriterRowAppenderFactory}'s options mapping and pool-bounds guard. */
class StreamWriterRowAppenderFactoryTest {

    @BeforeEach
    @AfterEach
    void resetPoolBounds() {
        StreamWriterRowAppenderFactory.resetAppliedPoolBoundsForTests();
    }

    @Test
    void mapsEverySdkRetryKnobIntoRetrySettings() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .sdkRetryInitialDelay(Duration.ofMillis(250))
                        .sdkRetryDelayMultiplier(1.5)
                        .sdkRetryMaxDelay(Duration.ofSeconds(15))
                        .sdkRetryMaxAttempts(7)
                        .build();

        RetrySettings settings = StreamWriterRowAppenderFactory.toRetrySettings(options);

        assertThat(settings.getInitialRetryDelayDuration()).isEqualTo(Duration.ofMillis(250));
        assertThat(settings.getRetryDelayMultiplier()).isEqualTo(1.5);
        assertThat(settings.getMaxRetryDelayDuration()).isEqualTo(Duration.ofSeconds(15));
        assertThat(settings.getMaxAttempts()).isEqualTo(7);
    }

    /** The defaulted knobs must reproduce the schedule that used to be hardcoded. */
    @Test
    void defaultSdkRetryKnobsEqualTheSharedConstant() {
        RetrySettings settings =
                StreamWriterRowAppenderFactory.toRetrySettings(
                        DefaultStreamOptions.builder().build());

        assertThat(settings).isEqualTo(StreamWriterRowAppenderFactory.RETRY_SETTINGS);
    }

    @Test
    void firstApplicationRecordsThePoolBounds() {
        assertThat(StreamWriterRowAppenderFactory.appliedPoolBounds()).isNull();

        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(3)
                        .maxConnectionsPerRegion(9)
                        .build());

        assertThat(StreamWriterRowAppenderFactory.appliedPoolBounds())
                .isEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(3, 9));
    }

    @Test
    void identicalSecondApplicationIsANoOp() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(3)
                        .maxConnectionsPerRegion(9)
                        .build();
        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(options);

        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(options);

        assertThat(StreamWriterRowAppenderFactory.appliedPoolBounds())
                .isEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(3, 9));
    }

    /** A later factory with different bounds must not overwrite the applied ones. */
    @Test
    void differentSecondApplicationKeepsTheFirstBounds() {
        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(3)
                        .maxConnectionsPerRegion(9)
                        .build());

        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(
                DefaultStreamOptions.builder()
                        .minConnectionsPerRegion(4)
                        .maxConnectionsPerRegion(16)
                        .build());

        assertThat(StreamWriterRowAppenderFactory.appliedPoolBounds())
                .isEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(3, 9));
    }

    @Test
    void resetClearsTheAppliedBounds() {
        StreamWriterRowAppenderFactory.applyPoolBoundsOnce(DefaultStreamOptions.builder().build());

        StreamWriterRowAppenderFactory.resetAppliedPoolBoundsForTests();

        assertThat(StreamWriterRowAppenderFactory.appliedPoolBounds()).isNull();
    }

    @Test
    void poolBoundsEqualityIsByValue() {
        assertThat(new StreamWriterRowAppenderFactory.PoolBounds(2, 20))
                .isEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(2, 20))
                .isNotEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(3, 20))
                .isNotEqualTo(new StreamWriterRowAppenderFactory.PoolBounds(2, 21));
    }
}
