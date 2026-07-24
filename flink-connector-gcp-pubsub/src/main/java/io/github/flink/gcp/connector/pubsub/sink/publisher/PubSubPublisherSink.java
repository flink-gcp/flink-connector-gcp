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

package io.github.flink.gcp.connector.pubsub.sink.publisher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkConfig;
import io.github.flink.gcp.connector.pubsub.sink.publisher.writer.DefaultPublisherFactory;
import io.github.flink.gcp.connector.pubsub.sink.publisher.writer.PubSubWriter;
import io.github.flink.gcp.connector.pubsub.sink.publisher.writer.PublisherFactory;
import io.github.flink.gcp.connector.pubsub.sink.topics.PubSubTopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;

import java.io.IOException;

/**
 * At-least-once sink publishing through {@code google-cloud-pubsub} {@code Publisher} instances
 * with dynamic per-record topic destinations.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public class PubSubPublisherSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final PubSubSinkConfig<T> config;

    /**
     * Creates the sink; called by {@link
     * io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder}.
     *
     * @param config the sink configuration
     */
    public PubSubPublisherSink(PubSubSinkConfig<T> config) {
        this.config = config;
    }

    /** Returns the sink configuration. */
    public PubSubSinkConfig<T> getConfig() {
        return config;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Pub/Sub serialization schema.", e);
        } catch (Exception e) {
            throw new IOException("Failed to open the Pub/Sub serialization schema.", e);
        }
        String emulatorEndpoint = config.getEmulatorEndpoint();
        return createWriter(
                new DefaultPublisherFactory(config.getPublisherOptions(), emulatorEndpoint),
                emulatorEndpoint == null
                        ? new PubSubTopicAdmin()
                        : new PubSubTopicAdmin(emulatorEndpoint),
                context.getMailboxExecutor());
    }

    @VisibleForTesting
    public SinkWriter<T> createWriter(
            PublisherFactory publisherFactory,
            TopicAdmin topicAdmin,
            MailboxExecutor mailboxExecutor) {
        return new PubSubWriter<>(config, publisherFactory, topicAdmin, mailboxExecutor);
    }
}
