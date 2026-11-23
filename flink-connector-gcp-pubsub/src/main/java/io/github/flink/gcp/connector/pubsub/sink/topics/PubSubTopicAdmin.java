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

package io.github.flink.gcp.connector.pubsub.sink.topics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.MessageStoragePolicy;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * Default {@link TopicAdmin} backed by the Pub/Sub {@link TopicAdminClient}.
 *
 * <p>Jobs whose destination topics all exist never construct a client (and never open its gRPC
 * channel). When auto-creation does trigger, the client is short-lived: opened for the creation
 * call and closed with it — together with its channel — so its resources are not held for the
 * writer's remaining lifetime for what is typically a one-shot event ({@link #close()} therefore
 * has nothing to release). The client's channels run on the connector's gRPC event-loop group
 * ({@link GrpcEventLoops}, ADR-0208), and closing it waits for them before it releases its lease.
 * With an emulator endpoint the client is handed a plaintext channel opened for it, with no
 * credentials, and closed after it. Creation conflicts ({@code ALREADY_EXISTS}, the topic was
 * created concurrently — for example by a parallel subtask) are treated as success.
 */
@Internal
public class PubSubTopicAdmin implements TopicAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicAdmin.class);

    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final CredentialsProvider credentialsOverride;

    /** Creates an admin using application-default credentials. */
    public PubSubTopicAdmin() {
        this(null, null);
    }

    /**
     * Creates the admin.
     *
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Pub/Sub with application-default credentials
     */
    public PubSubTopicAdmin(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(emulatorEndpoint, null);
    }

    /**
     * Creates an admin with an optional production credentials override.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for production Pub/Sub
     * @param credentialsOverride credentials to use in production, or {@code null} for ADC
     */
    public PubSubTopicAdmin(
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentialsOverride = credentialsOverride;
    }

    @Override
    public void createTopic(TopicDestination destination, @Nullable TopicCreateOptions options)
            throws IOException {
        try (GrpcEventLoops.LeasedClient<TopicAdminClient> admin = newClient()) {
            admin.client().createTopic(toTopic(destination, options));
            LOG.info("Created Pub/Sub topic {}", destination);
        } catch (AlreadyExistsException e) {
            LOG.info("Pub/Sub topic {} already exists, not creating it", destination);
        } catch (RuntimeException e) {
            throw new IOException("Failed to create Pub/Sub topic " + destination, e);
        }
    }

    /**
     * Translates the create options into the topic to create. Unset knobs leave their protobuf
     * fields untouched, so Pub/Sub applies its own defaults.
     */
    @VisibleForTesting
    static Topic toTopic(TopicDestination destination, @Nullable TopicCreateOptions options) {
        Topic.Builder topic =
                Topic.newBuilder()
                        .setName(
                                TopicName.of(destination.getProject(), destination.getTopic())
                                        .toString());
        if (options == null) {
            return topic.build();
        }
        Duration messageRetention = options.getMessageRetention();
        if (messageRetention != null) {
            topic.setMessageRetentionDuration(toProtoDuration(messageRetention));
        }
        if (options.getKmsKeyName() != null) {
            topic.setKmsKeyName(options.getKmsKeyName());
        }
        if (options.getAllowedPersistenceRegions() != null) {
            topic.setMessageStoragePolicy(
                    MessageStoragePolicy.newBuilder()
                            .addAllAllowedPersistenceRegions(options.getAllowedPersistenceRegions())
                            .setEnforceInTransit(options.isEnforceInTransit())
                            .build());
        }
        return topic.build();
    }

    private static com.google.protobuf.Duration toProtoDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }

    @Override
    public void close() {
        // Clients are short-lived within createTopic; there is nothing to release here.
    }

    /** Opens a client for one call, on the connector's event-loop group; for tests too. */
    @VisibleForTesting
    GrpcEventLoops.LeasedClient<TopicAdminClient> newClient() throws IOException {
        try {
            if (emulatorEndpoint == null) {
                TopicAdminSettings.Builder settings = productionSettings();
                return GrpcEventLoops.open(
                        lease -> TopicAdminClient.create(lease.applyTo(settings).build()));
            }
            return EmulatorChannels.openLeasedClient(
                    TopicAdminSettings.defaultGrpcTransportProviderBuilder(),
                    emulatorEndpoint,
                    channel ->
                            TopicAdminClient.create(
                                    TopicAdminSettings.newBuilder()
                                            .setCredentialsProvider(NoCredentialsProvider.create())
                                            .setTransportChannelProvider(channel)
                                            .build()));
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Pub/Sub admin client", e);
        }
    }

    /** Returns the production client's settings, before the lease is applied; for tests too. */
    @VisibleForTesting
    TopicAdminSettings.Builder productionSettings() {
        TopicAdminSettings.Builder settings = TopicAdminSettings.newBuilder();
        if (credentialsOverride != null) {
            settings.setCredentialsProvider(credentialsOverride);
        }
        return settings;
    }
}
