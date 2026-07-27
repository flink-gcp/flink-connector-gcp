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

import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;

import javax.annotation.Nullable;

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
    @Nullable private TopicCreateOptions topicCreateOptions;
    private PubSubPublisherOptions publisherOptions = PubSubPublisherOptions.defaults();
    @Nullable private String emulatorEndpoint;

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
     * Sets the settings applied to topics the sink creates under {@link
     * CreateDisposition#CREATE_IF_NEEDED} — message retention, a customer-managed encryption key
     * and the message storage policy. Optional: without it a created topic takes every field's
     * service default. One options object applies to every topic the sink creates, including each
     * one a {@link DestinationResolver} resolves. Rejected together with {@link
     * CreateDisposition#CREATE_NEVER}, which never creates a topic these settings could apply to.
     *
     * @param topicCreateOptions the creation settings
     * @return this builder
     */
    public PubSubSinkBuilder<T> topicCreateOptions(TopicCreateOptions topicCreateOptions) {
        this.topicCreateOptions =
                Preconditions.checkNotNull(
                        topicCreateOptions, "topicCreateOptions must not be null");
        return this;
    }

    /**
     * Sets the publisher and writer tuning options (batching, publish retries, message ordering,
     * the in-flight caps and the topic auto-creation recovery backoff). Optional; defaults to
     * {@link PubSubPublisherOptions#defaults()}.
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
     * Points the sink at a Pub/Sub emulator instead of the production service. Connections to the
     * given {@code host:port} — the per-topic publishers and, when topic auto-creation triggers,
     * the admin client — use a plaintext channel with no credentials, so this must only ever be
     * used against an emulator (for example a testcontainers {@code PubSubEmulatorContainer}).
     * Optional; when unset the sink connects to Pub/Sub with application-default credentials.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     */
    public PubSubSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        Preconditions.checkNotNull(emulatorEndpoint, "emulatorEndpoint must not be null");
        Preconditions.checkArgument(
                !emulatorEndpoint.trim().isEmpty(), "emulatorEndpoint must not be blank");
        this.emulatorEndpoint = emulatorEndpoint;
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
        Preconditions.checkState(
                topicCreateOptions == null || createDisposition != CreateDisposition.CREATE_NEVER,
                "topicCreateOptions(...) configures topics the sink creates, but"
                        + " createDisposition(CREATE_NEVER) never creates one. Remove the options"
                        + " or use CREATE_IF_NEEDED.");
        return new PubSubPublisherSink<>(
                new PubSubSinkConfig<>(
                        destinationResolver,
                        serializer,
                        createDisposition,
                        topicCreateOptions,
                        publisherOptions,
                        emulatorEndpoint));
    }
}
