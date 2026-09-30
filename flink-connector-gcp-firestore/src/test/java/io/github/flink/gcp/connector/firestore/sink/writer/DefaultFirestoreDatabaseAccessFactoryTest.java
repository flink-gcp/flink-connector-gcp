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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.firestore.BulkWriterOptions;
import com.google.cloud.firestore.v1.FirestoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultFirestoreDatabaseAccessFactoryTest {

    @Test
    void unsetRatesLeaveTheLibrarysDefaultsInPlace() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            BulkWriterOptions options =
                    DefaultFirestoreDatabaseAccessFactory.bulkWriterOptions(
                            FirestoreWriterOptions.defaults(), executor);

            assertThat(options.getThrottlingEnabled()).isTrue();
            assertThat(options.getInitialOpsPerSecond()).isNull();
            assertThat(options.getMaxOpsPerSecond()).isNull();
            assertThat(options.getExecutor()).isSameAs(executor);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void setRatesAndDisabledThrottlingReachTheLibrary() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            BulkWriterOptions throttled =
                    DefaultFirestoreDatabaseAccessFactory.bulkWriterOptions(
                            FirestoreWriterOptions.builder()
                                    .initialOpsPerSecond(20)
                                    .maxOpsPerSecond(40)
                                    .build(),
                            executor);
            BulkWriterOptions unthrottled =
                    DefaultFirestoreDatabaseAccessFactory.bulkWriterOptions(
                            FirestoreWriterOptions.builder().throttlingEnabled(false).build(),
                            executor);

            assertThat(throttled.getInitialOpsPerSecond()).isEqualTo(20d);
            assertThat(throttled.getMaxOpsPerSecond()).isEqualTo(40d);
            assertThat(unthrottled.getThrottlingEnabled()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void aDeserializedRateBelowTheLibrarysBatchSizeIsRefused() throws Exception {
        // Forged on a fresh builder result, never on defaults() (ADR-0002).
        FirestoreWriterOptions forged = FirestoreWriterOptions.builder().build();
        Field field = FirestoreWriterOptions.class.getDeclaredField("maxOpsPerSecond");
        field.setAccessible(true);
        field.set(forged, 10);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            assertThatThrownBy(
                            () ->
                                    DefaultFirestoreDatabaseAccessFactory.bulkWriterOptions(
                                            forged, executor))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxOpsPerSecond");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void noRetryOverrideLeavesTheLibrarysSettingsInPlace() {
        assertThat(
                        DefaultFirestoreDatabaseAccessFactory.retrySettings(
                                FirestoreWriterOptions.defaults()))
                .isNull();
        // The library's own settings are what it then applies; ServiceOptions' default is the
        // sentinel for "none given".
        assertThat(factory(FirestoreWriterOptions.defaults()).clientSettings().getRetrySettings())
                .isEqualTo(ServiceOptions.getDefaultRetrySettings());
    }

    @Test
    void anUnsetKnobKeepsTheLibrarysBatchWriteValue() {
        RetrySettings settings =
                DefaultFirestoreDatabaseAccessFactory.retrySettings(
                        FirestoreWriterOptions.builder()
                                .retryTotalTimeout(Duration.ofSeconds(30))
                                .build());

        // The values the reference page states as the defaults (google-cloud-firestore 3.46.0).
        assertThat(settings.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(settings.getInitialRetryDelayDuration()).isEqualTo(Duration.ofMillis(100));
        assertThat(settings.getRetryDelayMultiplier()).isEqualTo(1.3);
        assertThat(settings.getMaxRetryDelayDuration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(settings.getInitialRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(settings.getRpcTimeoutMultiplier()).isEqualTo(1.0);
        assertThat(settings.getMaxRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(settings.getMaxAttempts())
                .isEqualTo(DefaultFirestoreDatabaseAccessFactory.LIBRARY_DEFAULT_MAX_ATTEMPTS);
        // The count comes from GrpcFirestoreRpc, not from the generated settings, which bound
        // BatchWrite by time alone. This fails once they carry a count of their own, which is
        // when the mirrored five needs re-reading.
        assertThat(
                        FirestoreSettings.newBuilder()
                                .batchWriteSettings()
                                .getRetrySettings()
                                .getMaxAttempts())
                .isZero();
    }

    @Test
    void aMaximumShorterThanItsInitialValueIsRefusedByName() {
        // A zero cap alone falls under the library's 100 ms initial delay.
        assertThatThrownBy(
                        () ->
                                DefaultFirestoreDatabaseAccessFactory.retrySettings(
                                        FirestoreWriterOptions.builder()
                                                .retryMaxDelay(Duration.ZERO)
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay (PT0S)")
                .hasMessageContaining("retryInitialDelay (PT0.1S, the library's value");
        assertThatThrownBy(
                        () ->
                                DefaultFirestoreDatabaseAccessFactory.retrySettings(
                                        FirestoreWriterOptions.builder()
                                                .retryInitialRpcTimeout(Duration.ofSeconds(90))
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout (PT1M, the library's value")
                .hasMessageContaining("retryInitialRpcTimeout (PT1M30S)");
        assertThatThrownBy(
                        () ->
                                DefaultFirestoreDatabaseAccessFactory.retrySettings(
                                        FirestoreWriterOptions.builder()
                                                .retryInitialRpcTimeout(Duration.ofSeconds(30))
                                                .retryMaxRpcTimeout(Duration.ofSeconds(20))
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout (PT20S)")
                .hasMessageContaining("retryInitialRpcTimeout (PT30S)");

        // Both halves at zero are what gax means by no delay and no deadline.
        RetrySettings zeros =
                DefaultFirestoreDatabaseAccessFactory.retrySettings(
                        FirestoreWriterOptions.builder()
                                .retryInitialDelay(Duration.ZERO)
                                .retryMaxDelay(Duration.ZERO)
                                .retryInitialRpcTimeout(Duration.ZERO)
                                .retryMaxRpcTimeout(Duration.ZERO)
                                .build());
        assertThat(zeros.getMaxRetryDelayDuration()).isZero();
        assertThat(zeros.getMaxRpcTimeoutDuration()).isZero();
    }

    @Test
    void everyRetryKnobReachesTheClientSettings() {
        FirestoreWriterOptions options =
                FirestoreWriterOptions.builder()
                        .retryTotalTimeout(Duration.ofSeconds(30))
                        .retryInitialDelay(Duration.ofMillis(200))
                        .retryDelayMultiplier(2.0)
                        .retryMaxDelay(Duration.ofSeconds(10))
                        .retryInitialRpcTimeout(Duration.ofSeconds(20))
                        .retryRpcTimeoutMultiplier(1.5)
                        .retryMaxRpcTimeout(Duration.ofSeconds(25))
                        .retryMaxAttempts(3)
                        .build();

        RetrySettings settings = factory(options).clientSettings().getRetrySettings();

        assertThat(settings.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(settings.getInitialRetryDelayDuration()).isEqualTo(Duration.ofMillis(200));
        assertThat(settings.getRetryDelayMultiplier()).isEqualTo(2.0);
        assertThat(settings.getMaxRetryDelayDuration()).isEqualTo(Duration.ofSeconds(10));
        assertThat(settings.getInitialRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(20));
        assertThat(settings.getRpcTimeoutMultiplier()).isEqualTo(1.5);
        assertThat(settings.getMaxRpcTimeoutDuration()).isEqualTo(Duration.ofSeconds(25));
        assertThat(settings.getMaxAttempts()).isEqualTo(3);
    }

    @Test
    void overridesTheLibraryWouldTreatAsUnsetAreRefused() {
        RetrySettings gaxDefault = ServiceOptions.getDefaultRetrySettings();
        FirestoreWriterOptions options =
                FirestoreWriterOptions.builder()
                        .retryTotalTimeout(gaxDefault.getTotalTimeoutDuration())
                        .retryInitialDelay(gaxDefault.getInitialRetryDelayDuration())
                        .retryDelayMultiplier(gaxDefault.getRetryDelayMultiplier())
                        .retryMaxDelay(gaxDefault.getMaxRetryDelayDuration())
                        .retryInitialRpcTimeout(gaxDefault.getInitialRpcTimeoutDuration())
                        .retryRpcTimeoutMultiplier(gaxDefault.getRpcTimeoutMultiplier())
                        .retryMaxRpcTimeout(gaxDefault.getMaxRpcTimeoutDuration())
                        .retryMaxAttempts(gaxDefault.getMaxAttempts())
                        .build();

        assertThatThrownBy(() -> DefaultFirestoreDatabaseAccessFactory.retrySettings(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("treats as unset");
    }

    private static DefaultFirestoreDatabaseAccessFactory factory(FirestoreWriterOptions options) {
        // Settings only: nothing here opens a client, and the emulator endpoint keeps the
        // settings off application-default credentials (ADR-0064).
        return new DefaultFirestoreDatabaseAccessFactory(
                DatabaseDestination.of("p"),
                options,
                EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"),
                null);
    }
}
