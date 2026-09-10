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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.cloud.tasks.v2.CreateTaskRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.grpc.Deadline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordingTaskProxyTest {
    @TempDir Path temporary;

    @Test
    void closeWaitsForDetachedHandlerAndReportsItsLateFailure() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closeStarted = new CountDownLatch(1);
        var closer = Executors.newSingleThreadExecutor();
        try (var evidence = new AcceptanceEvidence(temporary.resolve("events.jsonl"))) {
            var proxy =
                    new RecordingTaskProxy(
                            (request, deadline) -> {
                                entered.countDown();
                                if (!release.await(10, TimeUnit.SECONDS)) {
                                    throw new AssertionError("Handler was not released");
                                }
                                throw new AssertionError("late oracle failure");
                            },
                            evidence);
            try (var creator =
                    new DefaultTaskCreatorFactory(
                                    null, EmulatorEndpoint.parse(proxy.endpoint(), "proxy"), null)
                            .create()) {
                var response =
                        creator.createTask(
                                CreateTaskRequest.getDefaultInstance(),
                                Deadline.after(20, TimeUnit.SECONDS));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                response.cancel(true);
                var closed =
                        closer.submit(
                                () -> {
                                    closeStarted.countDown();
                                    proxy.close();
                                    return null;
                                });
                assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
                try {
                    assertThatThrownBy(() -> closed.get(200, TimeUnit.MILLISECONDS))
                            .isInstanceOf(java.util.concurrent.TimeoutException.class);
                } finally {
                    release.countDown();
                }
                assertThatThrownBy(() -> closed.get(10, TimeUnit.SECONDS))
                        .hasStackTraceContaining("late oracle failure");
            } finally {
                release.countDown();
                try {
                    proxy.close();
                } catch (AssertionError expected) {
                    assertThat(expected).hasStackTraceContaining("late oracle failure");
                }
            }
        } finally {
            release.countDown();
            closer.shutdownNow();
        }
    }
}
