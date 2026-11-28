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

package io.github.flink.gcp.connector.pubsub;

import com.google.api.core.ApiFunction;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks that a client's production transport provider builds its channels on the group. */
public final class EventLoopChannelChecks {

    private EventLoopChannelChecks() {}

    /**
     * Takes a baseline of the group's count of event loops started while it runs, before a client
     * opens its lease ({@link EventLoopObserver}).
     *
     * @return a baseline that later readings of the count are compared against
     */
    public static EventLoopObserver.LoopStarts loopStartsFromNow() {
        return EventLoopObserver.loopStartsFromNow(
                GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
    }

    /**
     * Builds a channel through the provider's configurator, connects it to a local socket and
     * checks that one of the connector's group's event loops started for it. The client's own
     * channels connect lazily, so no loop has started before this channel registers. A provider
     * without the lease's configurator builds the channel on gRPC's group, and this fails.
     *
     * @param provider the client's transport provider
     * @param starts the baseline {@link #loopStartsFromNow()} took before the client opened its
     *     lease
     * @throws Exception if the check cannot run
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void assertBuildsChannelsOnTheGroup(
            TransportChannelProvider provider, EventLoopObserver.LoopStarts starts)
            throws Exception {
        assertThat(provider).isInstanceOf(InstantiatingGrpcChannelProvider.class);
        assertThat(starts.anyStarted())
                .as("a group event loop started before the channel connects")
                .isFalse();
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                ((InstantiatingGrpcChannelProvider) provider).toBuilder().getChannelConfigurator();
        ManagedChannelBuilder builder;
        try (ServerSocket server = new ServerSocket(0)) {
            builder =
                    NettyChannelBuilder.forAddress("127.0.0.1", server.getLocalPort())
                            .usePlaintext();
            ManagedChannel channel =
                    (configurator == null ? builder : configurator.apply(builder)).build();
            try {
                channel.getState(true);
                assertThat(starts.awaitAnyStarted())
                        .as("a group event loop started for the channel")
                        .isTrue();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }
}
