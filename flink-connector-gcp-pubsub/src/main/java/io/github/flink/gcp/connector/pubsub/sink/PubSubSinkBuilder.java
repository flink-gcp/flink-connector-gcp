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

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
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
    private FailureHandler<? super FailedMessage> failedMessageHandler = FailureHandler.failJob();
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;

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
     * Sets what happens to a message that terminally fails to be published: the record could not be
     * serialized, or Pub/Sub rejected the message itself as invalid ({@code INVALID_ARGUMENT} —
     * over the size limit, malformed attributes, an unusable ordering key). Defaults to {@link
     * FailureHandler#failJob()}.
     *
     * <p>Only those data-shaped failures reach the handler. A missing topic is repaired by {@link
     * CreateDisposition#CREATE_IF_NEEDED} instead, and everything else — an outage the SDK's
     * retries gave up on, {@code PERMISSION_DENIED}, a destination resolver that fails — keeps
     * failing the job, so a dropping policy cannot bleed the stream during an incident.
     *
     * <p>Returning from {@link FailureHandler#handle} drops the message; throwing fails the ongoing
     * write or checkpoint. The parameter is contravariant, so a cross-connector {@code
     * FailureHandler<FailedElement>} is accepted as-is.
     *
     * <p>Under {@code PubSubPublisherOptions.builder().enableMessageOrdering(true)}, dropping a
     * message that carries an ordering key leaves a gap in that key's stream which a consumer
     * cannot tell apart from a lost message. The messages queued behind it keep their relative
     * order — the sink resumes the key and republishes them — and the dead-letter record carries
     * the whole serialized message, so the gap can be replayed from it.
     *
     * @param failedMessageHandler the handler
     * @return this builder
     */
    public PubSubSinkBuilder<T> failedMessageHandler(
            FailureHandler<? super FailedMessage> failedMessageHandler) {
        this.failedMessageHandler =
                Preconditions.checkNotNull(
                        failedMessageHandler, "failedMessageHandler must not be null");
        return this;
    }

    /**
     * Authenticates the sink with the service-account JSON key at the given path instead of
     * application-default credentials. The file is read on each TaskManager when its writer is
     * created, so the same path must be readable by every TaskManager that can run this sink.
     * Optional; when unset the sink uses application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel deliberately carries no
     * credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public PubSubSinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the sink at a Pub/Sub emulator instead of the production service. Connections to the
     * given {@code host:port} — the per-topic publishers and, when topic auto-creation triggers,
     * the admin client — use a plaintext channel with no credentials, so this must only ever be
     * used against an emulator (for example a testcontainers {@code PubSubEmulatorContainer}).
     * Optional; when unset the sink connects to Pub/Sub with application-default credentials.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public PubSubSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
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
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new PubSubPublisherSink<>(
                new PubSubSinkConfig<>(
                        destinationResolver,
                        serializer,
                        createDisposition,
                        topicCreateOptions,
                        publisherOptions,
                        failedMessageHandler,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }
}
