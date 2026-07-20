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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * {@link PublisherFactory} building {@code google-cloud-pubsub} {@link Publisher} instances with
 * SDK-default batching, flow control and retries.
 *
 * <p>The SDK defaults are deliberate for now: exposing {@code BatchingSettings}, {@code
 * FlowControlSettings} and {@code RetrySettings} on the sink builder is tracked in issue #20, and
 * emulator endpoint support in issue #21.
 */
@Internal
public final class DefaultPublisherFactory implements PublisherFactory {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPublisherFactory.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    @Override
    public TopicPublisher create(TopicDestination destination) throws IOException {
        return new PublisherAdapter(
                Publisher.newBuilder(destination.toTopicPath()).build(), destination);
    }

    /** Adapts the SDK {@link Publisher} to the writer-facing {@link TopicPublisher} interface. */
    private static final class PublisherAdapter implements TopicPublisher {

        private final Publisher publisher;
        private final TopicDestination destination;

        private PublisherAdapter(Publisher publisher, TopicDestination destination) {
            this.publisher = publisher;
            this.destination = destination;
        }

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            return publisher.publish(message);
        }

        @Override
        public void flushOutstanding() {
            publisher.publishAllOutstanding();
        }

        @Override
        public void close() throws Exception {
            publisher.shutdown();
            if (!publisher.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn(
                        "The Pub/Sub publisher for topic {} did not terminate within {} seconds;"
                                + " its resources may leak until the JVM exits.",
                        destination,
                        SHUTDOWN_TIMEOUT_SECONDS);
            }
        }
    }
}
