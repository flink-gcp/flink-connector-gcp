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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreWriterOptionsTest {

    @Test
    void defaultsAreTheSameAsAnUntouchedBuilder() {
        DatastoreWriterOptions defaults = DatastoreWriterOptions.defaults();

        assertThat(defaults).isEqualTo(DatastoreWriterOptions.builder().build());
        assertThat(defaults.getMaxBatchMutations()).isEqualTo(500);
        assertThat(defaults.getMaxBatchBytes()).isEqualTo(9_000_000L);
        assertThat(defaults.getRequestTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(defaults.getRecoveryInitialBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(defaults.getRecoveryMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(defaults.getRecoveryMaxAttempts()).isEqualTo(10);
        assertThat(defaults.isThrottlingEnabled()).isTrue();
        assertThat(defaults.getThrottlingParallelism()).isNull();
        assertThat(defaults.getMaxConsecutiveRejections()).isEqualTo(100);
    }

    @Test
    void everyKnobParticipatesInEqualityAndIsRendered() throws Exception {
        // One variant per field, each differing from the defaults in that field alone, so that a
        // field left out of equals() or hashCode() leaves its variant equal to the defaults.
        Map<String, DatastoreWriterOptions> variants = new LinkedHashMap<>();
        variants.put(
                "maxBatchMutations", DatastoreWriterOptions.builder().maxBatchMutations(7).build());
        variants.put("maxBatchBytes", DatastoreWriterOptions.builder().maxBatchBytes(1024).build());
        variants.put(
                "requestTimeout",
                DatastoreWriterOptions.builder().requestTimeout(Duration.ofSeconds(5)).build());
        variants.put(
                "recoveryInitialBackoff",
                DatastoreWriterOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(20))
                        .build());
        variants.put(
                "recoveryMaxBackoff",
                DatastoreWriterOptions.builder()
                        .recoveryMaxBackoff(Duration.ofSeconds(20))
                        .build());
        variants.put(
                "recoveryMaxAttempts",
                DatastoreWriterOptions.builder().recoveryMaxAttempts(3).build());
        variants.put(
                "throttlingEnabled",
                DatastoreWriterOptions.builder().throttlingEnabled(false).build());
        variants.put(
                "throttlingParallelism",
                DatastoreWriterOptions.builder().throttlingParallelism(4).build());
        variants.put(
                "maxConsecutiveRejections",
                DatastoreWriterOptions.builder()
                        .maxConsecutiveRejections(DatastoreWriterOptions.UNBOUNDED)
                        .build());

        List<String> fields = new ArrayList<>();
        for (Field field : DatastoreWriterOptions.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                fields.add(field.getName());
            }
        }
        assertThat(variants.keySet()).containsExactlyInAnyOrderElementsOf(fields);
        DatastoreWriterOptions defaults = DatastoreWriterOptions.defaults();
        for (Map.Entry<String, DatastoreWriterOptions> variant : variants.entrySet()) {
            DatastoreWriterOptions options = variant.getValue();
            assertThat(options).as(variant.getKey()).isNotEqualTo(defaults);
            assertThat(options.hashCode()).as(variant.getKey()).isNotEqualTo(defaults.hashCode());
            assertThat(options.toString()).contains(variant.getKey() + "=");
            assertThat(InstantiationUtil.clone(options)).isEqualTo(options);
        }
    }

    @Test
    void theRecoveryKnobsBecomeAJitteredSchedule() {
        RetrySchedule schedule =
                DatastoreWriterOptions.builder()
                        .recoveryInitialBackoff(Duration.ofMillis(100))
                        .recoveryMaxBackoff(Duration.ofMillis(400))
                        .recoveryMaxAttempts(4)
                        .build()
                        .toRecoverySchedule();

        assertThat(schedule.maxAttempts()).isEqualTo(4);
        assertThat(schedule.jitterRatio()).isEqualTo(RetrySchedule.DEFAULT_JITTER_RATIO);
        assertThatThrownBy(
                        () ->
                                DatastoreWriterOptions.builder()
                                        .recoveryInitialBackoff(Duration.ofSeconds(2))
                                        .recoveryMaxBackoff(Duration.ofSeconds(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryMaxBackoff");
    }

    @Test
    void theByteCapStopsAtTheDocumentedRequestLimit() {
        assertThat(
                        DatastoreWriterOptions.builder()
                                .maxBatchBytes(10L * 1024 * 1024)
                                .build()
                                .getMaxBatchBytes())
                .isEqualTo(DatastoreWriterOptions.MAX_BATCH_BYTES_LIMIT);
        assertThatThrownBy(
                        () -> DatastoreWriterOptions.builder().maxBatchBytes(10L * 1024 * 1024 + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum request size");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().maxBatchBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void boundsAreChecked() {
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().maxBatchMutations(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchMutations");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().recoveryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryMaxAttempts");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().throttlingParallelism(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("throttlingParallelism");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().maxConsecutiveRejections(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1 (unbounded)");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().maxConsecutiveRejections(-2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> DatastoreWriterOptions.builder().requestTimeout(Duration.ofNanos(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTimeout");
        assertThatThrownBy(
                        () ->
                                DatastoreWriterOptions.builder()
                                        .requestTimeout(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("292 years");
        assertThatThrownBy(() -> DatastoreWriterOptions.builder().recoveryInitialBackoff(null))
                .isInstanceOf(NullPointerException.class);
    }
}
