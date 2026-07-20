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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.sink.publisher.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

/**
 * Builder for Pub/Sub sinks, obtained from {@link PubSubSink#builder()}.
 *
 * <p>Required settings: a serialization schema and a destination. The destination is set through
 * either {@link #topic(TopicDestination)} (fixed topic) or {@link
 * #destinationResolver(DestinationResolver)} (per-record dynamic destinations); the two override
 * each other and the last call wins.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class PubSubSinkBuilder<T> {

    private DestinationResolver<? super T> destinationResolver;
    private PubSubSerializationSchema<? super T> serializer;
    private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
    private PubSubPublisherOptions publisherOptions = PubSubPublisherOptions.defaults();

    PubSubSinkBuilder() {}

    /**
     * Publishes every record to the given fixed topic. Overrides any previously set topic or
     * resolver.
     *
     * @param topic the destination topic
     * @return this builder
     */
    public PubSubSinkBuilder<T> topic(TopicDestination topic) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(topic, "topic must not be null"));
        return this;
    }

    /**
     * Resolves the destination topic per record (dynamic destinations). Overrides any previously
     * set topic or resolver.
     *
     * @param destinationResolver the resolver
     * @return this builder
     */
    public PubSubSinkBuilder<T> destinationResolver(
            DestinationResolver<? super T> destinationResolver) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
        return this;
    }

    /**
     * Sets the record serialization schema.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public PubSubSinkBuilder<T> serializer(PubSubSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Sets whether the sink may create destination topics that do not exist. Defaults to {@link
     * CreateDisposition#CREATE_IF_NEEDED}.
     *
     * @param createDisposition the disposition
     * @return this builder
     */
    public PubSubSinkBuilder<T> createDisposition(CreateDisposition createDisposition) {
        this.createDisposition =
                Preconditions.checkNotNull(createDisposition, "createDisposition must not be null");
        return this;
    }

    /**
     * Sets the publisher and writer tuning options (batching, flow control, publish retries,
     * message ordering, the in-flight cap and the topic auto-creation recovery backoff). Optional;
     * defaults to {@link PubSubPublisherOptions#defaults()}.
     *
     * @param publisherOptions the options
     * @return this builder
     */
    public PubSubSinkBuilder<T> publisherOptions(PubSubPublisherOptions publisherOptions) {
        this.publisherOptions =
                Preconditions.checkNotNull(publisherOptions, "publisherOptions must not be null");
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     */
    public Sink<T> build() {
        Preconditions.checkState(serializer != null, "A serializer is required.");
        Preconditions.checkState(
                destinationResolver != null,
                "A destination is required: set topic(...) or destinationResolver(...).");
        return new PubSubPublisherSink<>(
                new PubSubSinkConfig<>(
                        destinationResolver, serializer, createDisposition, publisherOptions));
    }
}
