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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.firestore.BulkWriter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreWriterOptionsTest {

    @Test
    void defaultsAreTheSameAsAnUntouchedBuilder() {
        FirestoreWriterOptions defaults = FirestoreWriterOptions.defaults();

        assertThat(defaults).isEqualTo(FirestoreWriterOptions.builder().build());
        assertThat(defaults.isThrottlingEnabled()).isTrue();
        assertThat(defaults.getInitialOpsPerSecond()).isNull();
        assertThat(defaults.getMaxOpsPerSecond()).isNull();
        assertThat(defaults.getWriteMaxAttempts()).isEqualTo(11);
        assertThat(defaults.getRetryTotalTimeout()).isNull();
        assertThat(defaults.getRetryInitialDelay()).isNull();
        assertThat(defaults.getRetryDelayMultiplier()).isNull();
        assertThat(defaults.getRetryMaxDelay()).isNull();
        assertThat(defaults.getRetryInitialRpcTimeout()).isNull();
        assertThat(defaults.getRetryRpcTimeoutMultiplier()).isNull();
        assertThat(defaults.getRetryMaxRpcTimeout()).isNull();
        assertThat(defaults.getRetryMaxAttempts()).isNull();
        assertThat(defaults.getMaxInFlightWrites()).isEqualTo(250);
        assertThat(defaults.getMaxInFlightBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(defaults.getMaxConsecutiveRejections()).isEqualTo(100);
    }

    @Test
    void everyKnobParticipatesInEqualityAndIsRendered() throws Exception {
        FirestoreWriterOptions changed =
                FirestoreWriterOptions.builder()
                        .initialOpsPerSecond(20)
                        .maxOpsPerSecond(40)
                        .writeMaxAttempts(3)
                        .retryTotalTimeout(Duration.ofSeconds(30))
                        .retryInitialDelay(Duration.ofMillis(200))
                        .retryDelayMultiplier(2.0)
                        .retryMaxDelay(Duration.ofSeconds(10))
                        .retryInitialRpcTimeout(Duration.ofSeconds(20))
                        .retryRpcTimeoutMultiplier(1.5)
                        .retryMaxRpcTimeout(Duration.ofSeconds(25))
                        .retryMaxAttempts(3)
                        .maxInFlightWrites(7)
                        .maxInFlightBytes(1024)
                        .maxConsecutiveRejections(FirestoreWriterOptions.UNBOUNDED)
                        .build();
        FirestoreWriterOptions unthrottled =
                FirestoreWriterOptions.builder().throttlingEnabled(false).build();

        for (Field field : FirestoreWriterOptions.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            field.setAccessible(true);
            FirestoreWriterOptions differs =
                    field.getName().equals("throttlingEnabled") ? unthrottled : changed;
            assertThat(field.get(differs))
                    .as(field.getName())
                    .isNotEqualTo(field.get(FirestoreWriterOptions.defaults()));
            assertThat(differs.toString()).contains(field.getName() + "=");
        }
        assertThat(changed).isNotEqualTo(FirestoreWriterOptions.defaults());
        assertThat(unthrottled).isNotEqualTo(FirestoreWriterOptions.defaults());
        assertThat(InstantiationUtil.clone(changed)).isEqualTo(changed);
        assertThat(InstantiationUtil.clone(changed).hashCode()).isEqualTo(changed.hashCode());
    }

    @Test
    void theInFlightCapStopsAtTheLibrarysPendingCeiling() {
        assertThat(
                        FirestoreWriterOptions.builder()
                                .maxInFlightWrites(500)
                                .build()
                                .getMaxInFlightWrites())
                .isEqualTo(500);
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxInFlightWrites(501))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 500");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxInFlightWrites(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ratesAreRefusedWhereTheLibraryWouldRefuseThem() {
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .throttlingEnabled(false)
                                        .initialOpsPerSecond(20)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("throttlingEnabled(false)");
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .initialOpsPerSecond(40)
                                        .maxOpsPerSecond(20)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not exceed");
    }

    @Test
    void aRateBelowTheLibrarysBatchSizeIsRefused() {
        // The library opens its first batch at 20 writes and never sends a batch larger than its
        // rate, so a lower rate would keep that batch unsent.
        assertThat(FirestoreWriterOptions.MIN_OPS_PER_SECOND).isEqualTo(BulkWriter.MAX_BATCH_SIZE);
        assertThat(
                        FirestoreWriterOptions.builder()
                                .initialOpsPerSecond(20)
                                .maxOpsPerSecond(20)
                                .build())
                .isNotNull();
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().initialOpsPerSecond(19))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 20");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxOpsPerSecond(19))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 20");
    }

    @Test
    void transportRetryKnobsAreCheckedAsGaxReadsThem() {
        FirestoreWriterOptions zeros =
                FirestoreWriterOptions.builder()
                        .retryTotalTimeout(Duration.ZERO)
                        .retryInitialDelay(Duration.ZERO)
                        .retryMaxDelay(Duration.ZERO)
                        .retryInitialRpcTimeout(Duration.ZERO)
                        .retryMaxRpcTimeout(Duration.ZERO)
                        .retryMaxAttempts(0)
                        .build();
        assertThat(zeros.getRetryTotalTimeout()).isEqualTo(Duration.ZERO);
        assertThat(zeros.getRetryMaxAttempts()).isZero();

        // gax reads these with toMillis(), so a sub-millisecond value would silently become zero.
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .retryTotalTimeout(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryTotalTimeout");
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .retryInitialDelay(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialDelay");
        assertThatThrownBy(
                        () -> FirestoreWriterOptions.builder().retryMaxDelay(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxDelay");
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .retryInitialRpcTimeout(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryInitialRpcTimeout");
        assertThatThrownBy(
                        () ->
                                FirestoreWriterOptions.builder()
                                        .retryMaxRpcTimeout(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxRpcTimeout");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().retryDelayMultiplier(0.9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryDelayMultiplier");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().retryRpcTimeoutMultiplier(0.9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryRpcTimeoutMultiplier");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().retryMaxAttempts(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts");
    }

    @Test
    void attemptAndRejectionBoundsAreChecked() {
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().writeMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writeMaxAttempts");
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxInFlightBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxConsecutiveRejections(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FirestoreWriterOptions.builder().maxConsecutiveRejections(-2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
