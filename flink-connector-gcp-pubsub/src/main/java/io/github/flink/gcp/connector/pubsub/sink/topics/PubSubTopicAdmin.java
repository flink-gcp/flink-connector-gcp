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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Default {@link TopicAdmin} backed by the Pub/Sub {@link TopicAdminClient}.
 *
 * <p>Jobs whose destination topics all exist never construct a client (and never open its gRPC
 * channel). When auto-creation does trigger, the client is short-lived: opened for the creation
 * call and closed with it, so its channel and threads are not held for the writer's remaining
 * lifetime for what is typically a one-shot event. An injected client (tests) is used as-is and
 * closed with the admin instead; with an emulator endpoint the short-lived clients connect to it
 * over a plaintext channel with no credentials. Creation conflicts ({@code ALREADY_EXISTS}, the
 * topic was created concurrently — for example by a parallel subtask) are treated as success.
 */
@Internal
public class PubSubTopicAdmin implements TopicAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicAdmin.class);

    @Nullable private final TopicAdminClient injectedClient;
    @Nullable private final String emulatorEndpoint;

    /** Creates an admin using application-default credentials. */
    public PubSubTopicAdmin() {
        this.injectedClient = null;
        this.emulatorEndpoint = null;
    }

    /**
     * Creates an admin using the given client, closed with the admin.
     *
     * @param client the Pub/Sub admin client
     */
    public PubSubTopicAdmin(TopicAdminClient client) {
        this.injectedClient = client;
        this.emulatorEndpoint = null;
    }

    /**
     * Creates an admin connecting to the Pub/Sub emulator at the given {@code host:port} over a
     * plaintext channel with no credentials. Like the default-credentials admin, the client is
     * short-lived: opened for a creation call and closed with it, together with its channel.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     */
    public PubSubTopicAdmin(String emulatorEndpoint) {
        this.injectedClient = null;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public void createTopic(TopicDestination destination) throws IOException {
        if (injectedClient != null) {
            createTopic(injectedClient, destination);
            return;
        }
        try (TopicAdminClient client = newClient()) {
            createTopic(client, destination);
        }
    }

    @Override
    public void close() {
        if (injectedClient != null) {
            injectedClient.close();
        }
    }

    private static void createTopic(TopicAdminClient client, TopicDestination destination)
            throws IOException {
        TopicName topicName = TopicName.of(destination.getProject(), destination.getTopic());
        try {
            client.createTopic(topicName);
            LOG.info("Created Pub/Sub topic {}", destination);
        } catch (AlreadyExistsException e) {
            LOG.info("Pub/Sub topic {} already exists, not creating it", destination);
        } catch (RuntimeException e) {
            throw new IOException("Failed to create Pub/Sub topic " + destination, e);
        }
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
