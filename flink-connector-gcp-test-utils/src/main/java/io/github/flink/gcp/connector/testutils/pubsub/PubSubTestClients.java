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

package io.github.flink.gcp.connector.testutils.pubsub;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The Pub/Sub admin, publish and pull machinery every integration-test harness needs, parameterised
 * over the transport: a plaintext channel with no credentials against the emulator, or
 * application-default credentials against the real service. The harnesses in the connector modules
 * keep their connector-typed conveniences and delegate the client work here; everything on this
 * class is stock {@code com.google.*} types, which is what lets the SQL module's smoke test drive
 * the emulator with these clients while the connector under test uses its relocated copies.
 *
 * <p>Subscriptions are addressed by their full resource path ({@code
 * projects/<p>/subscriptions/<s>}) so no connector destination type crosses the module boundary.
 */
@Internal
public final class PubSubTestClients implements AutoCloseable {

    private final TopicAdminClient topicAdmin;
    private final SubscriptionAdminClient subscriptionAdmin;

    /** Applied to publishers so they share the emulator channel; null on the ADC transport. */
    private final TransportChannelProvider publisherChannelProvider;

    /** The emulator channel this instance owns and shuts down; null on the ADC transport. */
    private final ManagedChannel ownedChannel;

    /** How long a pull-until loop sleeps after an empty pull before pulling again. */
    private final Duration pollInterval;

    private PubSubTestClients(
            TopicAdminClient topicAdmin,
            SubscriptionAdminClient subscriptionAdmin,
            TransportChannelProvider publisherChannelProvider,
            ManagedChannel ownedChannel,
            Duration pollInterval) {
        this.topicAdmin = topicAdmin;
        this.subscriptionAdmin = subscriptionAdmin;
        this.publisherChannelProvider = publisherChannelProvider;
        this.ownedChannel = ownedChannel;
        this.pollInterval = pollInterval;
    }

    /**
     * Clients over one plaintext channel to the emulator, with no credentials. The returned
     * instance owns the channel and shuts it down on {@link #close()}.
     */
    public static PubSubTestClients forEmulator(String endpoint) throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        // A fixed provider is not auto-closed by the clients, so all of them can share this one
        // channel.
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        TopicAdminClient topicAdmin = null;
        try {
            topicAdmin =
                    TopicAdminClient.create(
                            TopicAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(NoCredentialsProvider.create())
                                    .build());
            SubscriptionAdminClient subscriptionAdmin =
                    SubscriptionAdminClient.create(
                            SubscriptionAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(NoCredentialsProvider.create())
                                    .build());
            return new PubSubTestClients(
                    topicAdmin,
                    subscriptionAdmin,
                    channelProvider,
                    channel,
                    Duration.ofMillis(100));
        } catch (IOException | RuntimeException e) {
            // A half-built instance is never returned, so close what exists here: the caller's
            // teardown only ever sees a fully-constructed one.
            if (topicAdmin != null) {
                topicAdmin.close();
            }
            channel.shutdownNow();
            throw e;
        }
    }

    /**
     * Clients on the SDK's defaults: application-default credentials, real service. The poll
     * interval is looser than the emulator transport's because every pull is a network round trip.
     */
    public static PubSubTestClients withApplicationDefaultCredentials() throws IOException {
        TopicAdminClient topicAdmin = TopicAdminClient.create();
        try {
            return new PubSubTestClients(
                    topicAdmin,
                    SubscriptionAdminClient.create(),
                    null,
                    null,
                    Duration.ofMillis(200));
        } catch (IOException | RuntimeException e) {
            topicAdmin.close();
            throw e;
        }
    }

    public TopicAdminClient topicAdmin() {
        return topicAdmin;
    }

    public SubscriptionAdminClient subscriptionAdmin() {
        return subscriptionAdmin;
    }

    /**
     * Publishes the payloads, optionally under one ordering key, and waits for the acknowledgements
     * so the messages are durable before a source starts.
     *
     * @param orderingKey the ordering key, or null for unordered publishing
     * @param publishEndpoint overrides the publisher's endpoint, or null for the transport's
     *     default — the real-service ordering tests pass their regional endpoint here, because
     *     ordered publishing is only guaranteed through one
     */
    public void publishOrdered(
            TopicName topic, String orderingKey, String publishEndpoint, String... payloads)
            throws IOException, InterruptedException, ExecutionException {
        Publisher.Builder builder = Publisher.newBuilder(topic);
        if (publisherChannelProvider != null) {
            builder.setChannelProvider(publisherChannelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create());
        }
        if (orderingKey != null) {
            builder.setEnableMessageOrdering(true);
        }
        if (publishEndpoint != null) {
            builder.setEndpoint(publishEndpoint);
        }
        Publisher publisher = builder.build();
        try {
            List<ApiFuture<String>> published = new ArrayList<>(payloads.length);
            for (String payload : payloads) {
                PubsubMessage.Builder message =
                        PubsubMessage.newBuilder()
                                .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8));
                if (orderingKey != null) {
                    message.setOrderingKey(orderingKey);
                }
                published.add(publisher.publish(message.build()));
            }
            publisher.publishAllOutstanding();
            for (ApiFuture<String> future : published) {
                future.get();
            }
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Pulls and acknowledges until {@code expected} distinct payloads have arrived or the deadline
     * passes, returning what did arrive.
     *
     * <p>A single pull is not guaranteed to return everything outstanding even when more is
     * available, so an exact-count assertion on one pull would be flaky.
     */
    public Set<String> pullAndAckUntil(String subscriptionPath, int expected, Duration timeout)
            throws InterruptedException {
        Set<String> payloads = new LinkedHashSet<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (payloads.size() < expected && System.nanoTime() < deadline) {
            List<PubsubMessage> pulled = pullMessagesAndAck(subscriptionPath, expected);
            if (pulled.isEmpty()) {
                Thread.sleep(pollInterval.toMillis());
            }
            for (PubsubMessage message : pulled) {
                payloads.add(message.getData().toStringUtf8());
            }
        }
        return payloads;
    }

    /**
     * Pulls and acknowledges until {@code expected} <em>distinct</em> messages have arrived or the
     * deadline passes, returning them whole — attributes, publish time and ordering key included,
     * which {@link #pullAndAckUntil} discards.
     *
     * <p>Distinct by message id, for the same reason {@link #pullAndAckUntil} collects into a set:
     * an acknowledgement is not necessarily applied before the next pull is served, and the sinks
     * are at-least-once, so the same message can come back. Counting redeliveries would make every
     * exact-count assertion a coin flip.
     */
    public List<PubsubMessage> pullMessagesUntil(
            String subscriptionPath, int expected, Duration timeout) throws InterruptedException {
        Map<String, PubsubMessage> messages = new LinkedHashMap<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (messages.size() < expected && System.nanoTime() < deadline) {
            List<PubsubMessage> pulled = pullMessagesAndAck(subscriptionPath, expected);
            if (pulled.isEmpty()) {
                Thread.sleep(pollInterval.toMillis());
            }
            for (PubsubMessage message : pulled) {
                messages.putIfAbsent(message.getMessageId(), message);
            }
        }
        return new ArrayList<>(messages.values());
    }

    /**
     * Pulls up to {@code maxMessages} and acknowledges them, returning the messages. One pull only
     * — use {@link #pullMessagesUntil} to assert on a known count.
     */
    public List<PubsubMessage> pullMessagesAndAck(String subscriptionPath, int maxMessages) {
        PullResponse response = pull(subscriptionPath, maxMessages);
        List<PubsubMessage> messages = new ArrayList<>(response.getReceivedMessagesCount());
        List<String> ackIds = new ArrayList<>(response.getReceivedMessagesCount());
        for (ReceivedMessage received : response.getReceivedMessagesList()) {
            messages.add(received.getMessage());
            ackIds.add(received.getAckId());
        }
        if (!ackIds.isEmpty()) {
            subscriptionAdmin.acknowledge(subscriptionPath, ackIds);
        }
        return messages;
    }

    /**
     * Pulls up to {@code maxMessages} <em>without</em> acknowledging, returning the messages — for
     * tests that must leave the subscription's state untouched, at the cost of the next pull
     * waiting out the acknowledgement deadline before a redelivery.
     */
    public List<PubsubMessage> pullMessages(String subscriptionPath, int maxMessages) {
        List<ReceivedMessage> received =
                pull(subscriptionPath, maxMessages).getReceivedMessagesList();
        List<PubsubMessage> messages = new ArrayList<>(received.size());
        for (ReceivedMessage message : received) {
            messages.add(message.getMessage());
        }
        return messages;
    }

    private PullResponse pull(String subscriptionPath, int maxMessages) {
        return subscriptionAdmin
                .getStub()
                .pullCallable()
                .call(
                        PullRequest.newBuilder()
                                .setSubscription(subscriptionPath)
                                .setMaxMessages(maxMessages)
                                .build());
    }

    @Override
    public void close() {
        subscriptionAdmin.close();
        topicAdmin.close();
        if (ownedChannel != null) {
            ownedChannel.shutdownNow();
        }
    }
}
