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

package io.github.flink.gcp.connector.pubsub.sink.topics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.MessageStoragePolicy;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannelBuilder;
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
 * has nothing to release). With an emulator endpoint the short-lived clients connect to it over a
 * plaintext channel with no credentials. Creation conflicts ({@code ALREADY_EXISTS}, the topic was
 * created concurrently — for example by a parallel subtask) are treated as success.
 */
@Internal
public class PubSubTopicAdmin implements TopicAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicAdmin.class);

    @Nullable private final String emulatorEndpoint;

    /** Creates an admin using application-default credentials. */
    public PubSubTopicAdmin() {
        this(null);
    }

    /**
     * Creates the admin.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port} (plaintext, no
     *     credentials), or {@code null} for production Pub/Sub with application-default credentials
     */
    public PubSubTopicAdmin(@Nullable String emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public void createTopic(TopicDestination destination, @Nullable TopicCreateOptions options)
            throws IOException {
        try (TopicAdminClient client = newClient()) {
            client.createTopic(toTopic(destination, options));
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

    private TopicAdminClient newClient() throws IOException {
        try {
            if (emulatorEndpoint == null) {
                return TopicAdminClient.create();
            }
            // The instantiating provider is auto-closed by the client, so the try-with-resources
            // in createTopic closes the emulator channel together with the client.
            return TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setCredentialsProvider(NoCredentialsProvider.create())
                            .setTransportChannelProvider(
                                    TopicAdminSettings.defaultGrpcTransportProviderBuilder()
                                            .setEndpoint(emulatorEndpoint)
                                            .setChannelConfigurator(
                                                    ManagedChannelBuilder::usePlaintext)
                                            .build())
                            .build());
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Pub/Sub admin client", e);
        }
    }
}
