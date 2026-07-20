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

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Publisher factory opening plaintext connections to the Pub/Sub emulator, shared by the
 * integration tests. Each publisher owns its channel and shuts it down on close.
 */
final class EmulatorPublisherFactory implements PublisherFactory {

    private static final long serialVersionUID = 1L;

    private final String endpoint;

    EmulatorPublisherFactory(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public TopicPublisher create(TopicDestination destination) throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        Publisher publisher =
                Publisher.newBuilder(destination.toTopicPath())
                        .setChannelProvider(
                                FixedTransportChannelProvider.create(
                                        GrpcTransportChannel.create(channel)))
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();
        return new TopicPublisher() {
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
                try {
                    publisher.shutdown();
                    publisher.awaitTermination(30, TimeUnit.SECONDS);
                } finally {
                    channel.shutdownNow();
                }
            }
        };
    }
}
