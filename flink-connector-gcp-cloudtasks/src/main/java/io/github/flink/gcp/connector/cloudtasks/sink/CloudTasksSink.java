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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Public;

/**
 * Entry point for building a Cloud Tasks sink.
 *
 * <p>The sink creates one HTTP task per record, at-least-once, and flushes every outstanding
 * creation at each checkpoint barrier. Dispatch pacing is <em>not</em> configured here: Cloud Tasks
 * paces execution on the queue, so the queue's rate limits and retry policy — applied by whoever
 * created it — decide how fast the tasks run. The sink only decides how fast tasks are handed over.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link
 * CloudTasksSinkBuilder#failedTaskHandler(FailureHandler)}, a completed checkpoint means every
 * record up to the barrier was either durably accepted, skipped by the serializer, or handed to
 * that handler.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocCloudTasksExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<OrderEvent> sink =
 *         CloudTasksSink.<OrderEvent>builder()
 *                 .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
 *                 .serializer(
 *                         CloudTasksSerializationSchema.httpTarget(
 *                                         "https://api.example.com/v1/orders")
 *                                 .withBody(new MyEventJsonSerializationSchema())
 *                                 .withHeaders(
 *                                         e -> Map.of("Content-Type", "application/json"))
 *                                 .withOidcToken(
 *                                         "dispatcher@my-project.iam.gserviceaccount.com"))
 *                 .build();
 * }</pre>
 */
@Public
public final class CloudTasksSink {

    private CloudTasksSink() {}

    /**
     * Creates a new {@link CloudTasksSinkBuilder}.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> CloudTasksSinkBuilder<T> builder() {
        return new CloudTasksSinkBuilder<>();
    }
}
