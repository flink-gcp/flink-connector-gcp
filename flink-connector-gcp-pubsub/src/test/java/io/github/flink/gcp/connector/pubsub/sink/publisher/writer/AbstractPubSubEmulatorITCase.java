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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.PubSubTopicAdmin;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Shared harness for integration tests against the Pub/Sub emulator: the container, plaintext
 * admin/subscriber clients pointed at it, and a no-op {@link SinkWriter.Context}. Use together with
 * {@link EmulatorPublisherFactory}.
 */
@Testcontainers
@Timeout(180)
abstract class AbstractPubSubEmulatorITCase {

    static final String PROJECT = "it-project";

    static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    @Container
    private static final PubSubEmulatorContainer EMULATOR =
            new PubSubEmulatorContainer(
                    DockerImageName.parse(
                            "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators"));

    private static ManagedChannel channel;
    private static TransportChannelProvider channelProvider;
    static TopicAdminClient topicAdminClient;
    static SubscriptionAdminClient subscriptionAdminClient;
    private static SubscriberStub subscriberStub;

    @BeforeAll
    static void createClients() throws IOException {
        channel =
                ManagedChannelBuilder.forTarget(EMULATOR.getEmulatorEndpoint())
                        .usePlaintext()
                        .build();
        // A fixed provider is not auto-closed by the clients, so all of them (including the
        // per-writer admins handed out by newTopicAdmin()) can share this one channel.
        channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        topicAdminClient = newTopicAdminClient();
        subscriptionAdminClient =
                SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
        subscriberStub =
                GrpcSubscriberStub.create(
                        SubscriberStubSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());
    }

    @AfterAll
    static void closeClients() {
        if (subscriberStub != null) {
            subscriberStub.close();
        }
        if (subscriptionAdminClient != null) {
            subscriptionAdminClient.close();
        }
        if (topicAdminClient != null) {
            topicAdminClient.close();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    static String emulatorEndpoint() {
        return EMULATOR.getEmulatorEndpoint();
    }

    /**
     * Returns a fresh emulator-backed {@link TopicAdmin} for a writer under test (the writer closes
     * its admin, so it must not receive the harness-owned client).
     */
    static TopicAdmin newTopicAdmin() throws IOException {
        return new PubSubTopicAdmin(newTopicAdminClient());
    }

    private static TopicAdminClient newTopicAdminClient() throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build());
    }

    static boolean topicExists(TopicDestination destination) {
        try {
            topicAdminClient.getTopic(
                    TopicName.of(destination.getProject(), destination.getTopic()));
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    static void createSubscription(TopicDestination topic, String subscriptionId) {
        subscriptionAdminClient.createSubscription(
                SubscriptionName.of(PROJECT, subscriptionId),
                TopicName.of(topic.getProject(), topic.getTopic()),
                PushConfig.getDefaultInstance(),
                10);
    }

    /** Creates a subscription that preserves ordering-key delivery order. */
    static void createOrderedSubscription(TopicDestination topic, String subscriptionId) {
        subscriptionAdminClient.createSubscription(
                Subscription.newBuilder()
                        .setName(SubscriptionName.format(PROJECT, subscriptionId))
                        .setTopic(TopicName.format(topic.getProject(), topic.getTopic()))
                        .setAckDeadlineSeconds(10)
                        .setEnableMessageOrdering(true)
                        .build());
    }

    /** Pulls up to {@code maxMessages} from the subscription and returns their UTF-8 payloads. */
    static List<String> pullPayloads(String subscriptionId, int maxMessages) {
        return pullMessages(subscriptionId, maxMessages).stream()
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    /** Pulls up to {@code maxMessages} from the subscription and returns the full messages. */
    static List<PubsubMessage> pullMessages(String subscriptionId, int maxMessages) {
        return subscriberStub
                .pullCallable()
                .call(
                        PullRequest.newBuilder()
                                .setSubscription(
                                        ProjectSubscriptionName.format(PROJECT, subscriptionId))
                                .setMaxMessages(maxMessages)
                                .build())
                .getReceivedMessagesList()
                .stream()
                .map(received -> received.getMessage())
                .collect(Collectors.toList());
    }
}
