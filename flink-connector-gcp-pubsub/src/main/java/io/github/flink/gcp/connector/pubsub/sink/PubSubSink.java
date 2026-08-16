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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Entry point for building a Pub/Sub sink.
 *
 * <p>The sink publishes records at-least-once through {@code google-cloud-pubsub} {@code Publisher}
 * instances, resolving the destination topic per record (dynamic destinations) and flushing all
 * outstanding publishes at every checkpoint barrier.
 *
 * <p>That at-least-once statement assumes the default {@code FailureHandler.failJob()} policy.
 * Under a dropping policy configured through {@link PubSubSinkBuilder#failedMessageHandler}, a
 * completed checkpoint means every record up to the barrier was either published, skipped by the
 * serializer, or handed to that handler.
 *
 * <p>Example:
 * <!-- javadoc-example file="JavadocPubSubExamples.java" tag="sink" -->
 *
 * <pre>{@code
 * Sink<MyEvent> sink =
 *         PubSubSink.<MyEvent>builder()
 *                 .destinationResolver(
 *                         (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
 *                 .serializer(
 *                         PubSubSerializationSchema.dataOnly(
 *                                 new MyEventSerializationSchema()))
 *                 .build();
 * }</pre>
 */
@PublicEvolving
public final class PubSubSink {

    private PubSubSink() {}

    /**
     * Creates a new {@link PubSubSinkBuilder}.
     *
     * @param <T> type of the records written by the sink
     * @return a new builder
     */
    public static <T> PubSubSinkBuilder<T> builder() {
        return new PubSubSinkBuilder<>();
    }
}
