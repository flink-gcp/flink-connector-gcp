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
     * Asks the publisher to shut down without waiting for it, so a writer owning several publishers
     * can ask every one before it waits on any: the waits then overlap, and the whole release costs
     * one shutdown timeout however many publishers it covers. Idempotent, and implied by {@link
     * #close()} when it was not called — a publisher closed on its own needs no two calls.
     *
     * <p>Starting the shutdown is what starts the timeout {@link #close()} then waits out.
     */
    void shutdown();

    /**
     * Whether the completed close left publisher shutdown work or resources alive. Production
     * publishers report this so running-task eviction can fail before opening a replacement and
     * accumulating abandoned resources. Test publishers that complete teardown synchronously need
     * not override it.
     *
     * @return whether shutdown was abandoned
     */
    default boolean wasShutdownIncomplete() {
        return false;
    }

    /**
     * Completes the shutdown {@link #shutdown()} started, waiting a bounded time for termination
     * and releasing the transport whatever happened. The bound is measured from the {@code
     * shutdown()} call, not from here.
     *
     * <p>A graceful shutdown may still send messages buffered inside the publisher (the SDK {@code
     * Publisher} does); callers needing completion guarantees flush and await the publish futures
     * first.
     */
    @Override
    void close() throws Exception;
}
