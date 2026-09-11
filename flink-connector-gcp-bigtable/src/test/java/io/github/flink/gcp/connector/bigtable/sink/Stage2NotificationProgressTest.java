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

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2NotificationProgressTest {
    @Test
    void durationIncludesWorkBetweenInvocationsAndRestoredWorkIsSeparate() throws Exception {
        Stage2NotificationProgress progress = new Stage2NotificationProgress();
        progress.invocation(7);
        progress.started(3, 100);
        progress.invocation(2);
        progress.invocation(3);
        progress.invocation(1);
        var active = json(progress.sample(250));
        assertThat(active.path("activeNotifications").get(0).path("ageNanos").asLong())
                .isEqualTo(150);
        assertThat(active.path("activeNotifications").get(0).path("entries").asLong()).isEqualTo(6);
        progress.finished(true, 300);
        var completed = json(progress.sample(400));
        assertThat(completed.path("activeNotifications")).isEmpty();
        assertThat(completed.path("maxFinishedNotificationNanos").asLong()).isEqualTo(200);
        assertThat(completed.path("maxNotificationInvocations").asLong()).isEqualTo(3);
        assertThat(completed.path("maxNotificationEntries").asLong()).isEqualTo(6);
        assertThat(completed.path("outsideNotificationInvocations").asLong()).isEqualTo(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void actualBoundaryRecordsFailureAndPropagatesOriginalException() throws Exception {
        Stage2NotificationProgress progress = new Stage2NotificationProgress();
        IOException original = new IOException("notification failed");
        OneInputStreamOperator<Long, Long> delegate =
                (OneInputStreamOperator<Long, Long>)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OneInputStreamOperator.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("notifyCheckpointComplete")) {
                                        progress.invocation(5);
                                        throw original;
                                    }
                                    return null;
                                });
        var observed = Stage2NotificationOperatorFactory.observe(delegate, progress);
        assertThatThrownBy(() -> observed.notifyCheckpointComplete(42)).isSameAs(original);
        var sample = json(progress.sample(System.nanoTime()));
        assertThat(sample.path("activeNotifications")).isEmpty();
        assertThat(sample.path("failedNotifications").asLong()).isEqualTo(1);
        assertThat(sample.path("maxNotificationEntries").asLong()).isEqualTo(5);
        observed.notifyCheckpointAborted(42);
        assertThat(json(progress.sample(System.nanoTime())).path("startedNotifications").asLong())
                .isEqualTo(1);
    }

    private static org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode json(
            String value) throws Exception {
        return new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(value);
    }
}
