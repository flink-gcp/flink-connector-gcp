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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.pubsub.v1.PubsubMessage;

/**
 * Publishes messages to one Pub/Sub topic.
 *
 * <p>An interface (instead of the concrete {@code com.google.cloud.pubsub.v1.Publisher}) so the
 * writer can be unit-tested against fakes; {@code Publisher} is a concrete class and its {@code
 * PublisherInterface} exposes neither flushing nor shutdown.
 */
@Internal
public interface TopicPublisher extends AutoCloseable {

    /**
     * Publishes the given message asynchronously.
     *
     * @param message the message
     * @return a future completing with the server-assigned message id, or exceptionally when the
     *     publish terminally fails
     */
    ApiFuture<String> publish(PubsubMessage message);

    /**
     * Resumes publishing for an ordering key the publisher paused after a failed publish (the SDK
     * {@code Publisher} rejects further publishes to a failed key until resumed). A no-op when the
     * key is not paused or message ordering is disabled.
     *
     * @param orderingKey the ordering key to resume
     */
    void resumePublish(String orderingKey);

    /**
     * Sends all messages buffered by the publisher without waiting for the batching thresholds to
     * be met. Completion is observed through the futures returned by {@link
     * #publish(PubsubMessage)}.
     */
    void flushOutstanding();

    /**
     * Shuts the publisher down, waiting a bounded time for termination. A graceful shutdown may
     * still send messages buffered inside the publisher (the SDK {@code Publisher} does); callers
     * needing completion guarantees flush and await the publish futures first.
     */
    @Override
    void close() throws Exception;
}
