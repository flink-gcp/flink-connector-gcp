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

package io.github.flink.gcp.connector.base.rpc;

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link GrpcEventLoops}.
 *
 * <p>The group is process-wide state, so every test closes every lease it opens in a {@code
 * finally}, and {@link #noLeaseIsLeftOpen()} checks that it did — a lease left open would keep the
 * group running into the next test and hide a release that never happened.
 */
class GrpcEventLoopsTest {

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void aChannelRunsOnTheGroupAndItsThreadsStopWithTheLastRelease() throws Exception {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (ServerSocket server = new ServerSocket(0)) {
            ManagedChannel channel =
                    configurator(lease)
                            .apply(
                                    NettyChannelBuilder.forAddress(
                                                    "127.0.0.1", server.getLocalPort())
                                            .usePlaintext())
                            .build();
            try {
                // Connecting registers the channel on an event loop, which starts its thread.
                channel.getState(true);
                assertThat(GrpcEventLoopThreads.awaitAlive())
                        .as("a thread named %s* while the channel connects", prefix())
                        .isTrue();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        } finally {
            lease.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no thread named %s* after the last release", prefix())
                .isTrue();
    }

    @Test
    void theTestUtilitiesLookForTheGroupsThreadNames() {
        // GrpcEventLoopThreads restates the prefix, since test-utils cannot depend on base.
        assertThat(GrpcEventLoopThreads.PREFIX).isEqualTo(GrpcEventLoops.THREAD_NAME_PREFIX);
    }

    @Test
    void aClosedLeaseRefusesToPlaceAChannel() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        lease.close();

        assertThatThrownBy(() -> lease.applyTo(NettyChannelBuilder.forTarget("localhost:1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lease is closed");
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void theLastReleaseWaitsForTheGroupToFinish() throws Exception {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Thread closer = new Thread(lease::close, "lease-closer");
        try {
            // A task still running on a group thread is what a release that did not wait would
            // leave behind; the release must not return before it has finished.
            GrpcEventLoops.executeOnGroup(
                    () -> {
                        running.countDown();
                        try {
                            finish.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();

            closer.start();
            closer.join(300);
            assertThat(closer.isAlive())
                    .as("the release returned while a group thread was still running a task")
                    .isTrue();
        } finally {
            finish.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(15));
            lease.close();
        }
        assertThat(closer.isAlive()).isFalse();
    }

    @Test
    void aConfiguratorUsedAfterItsLeaseLeavesTheChannelToGrpc() throws Exception {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        ApiFunction<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> configurator;
        try {
            configurator = configurator(lease);
        } finally {
            lease.close();
        }

        NettyChannelBuilder builder = NettyChannelBuilder.forTarget("localhost:1").usePlaintext();
        ManagedChannel channel = configurator.apply(builder).build();
        try {
            channel.getState(true);

            assertThat(GrpcEventLoops.isRunning()).isFalse();
            assertThat(GrpcEventLoopThreads.alive())
                    .as("no group was started for the channel")
                    .isFalse();
        } finally {
            channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void theGroupRunsUntilItsLastLeaseIsReleased() {
        GrpcEventLoops.Lease first = GrpcEventLoops.acquire();
        GrpcEventLoops.Lease second = GrpcEventLoops.acquire();
        try {
            first.close();
            assertThat(GrpcEventLoops.isRunning()).isTrue();
        } finally {
            first.close();
            second.close();
        }
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void closingALeaseTwiceReleasesItOnce() {
        GrpcEventLoops.Lease first = GrpcEventLoops.acquire();
        GrpcEventLoops.Lease second = GrpcEventLoops.acquire();
        try {
            first.close();
            first.close();
            // A second release of the first lease would have counted the second one out too.
            assertThat(GrpcEventLoops.isRunning()).isTrue();
        } finally {
            second.close();
        }
    }

    @Test
    void applyingToAProviderComposesAfterItsConfiguratorAndKeepsItsSettings() {
        List<String> calls = new ArrayList<>();
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            InstantiatingGrpcChannelProvider provider =
                    InstantiatingGrpcChannelProvider.newBuilder()
                            .setEndpoint("localhost:1")
                            .setKeepAliveWithoutCalls(true)
                            .setChannelConfigurator(
                                    builder -> {
                                        calls.add("existing");
                                        return builder.usePlaintext();
                                    })
                            .build();

            TransportChannelProvider applied = lease.applyTo(provider);

            assertThat(applied).isInstanceOf(InstantiatingGrpcChannelProvider.class);
            InstantiatingGrpcChannelProvider.Builder copy =
                    ((InstantiatingGrpcChannelProvider) applied).toBuilder();
            assertThat(copy.getEndpoint()).isEqualTo("localhost:1");
            assertThat(copy.getKeepAliveWithoutCalls()).isTrue();
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget("localhost:1");
            assertThat(copy.getChannelConfigurator().apply(builder)).isSameAs(builder);
            assertThat(calls).containsExactly("existing");
        } finally {
            lease.close();
        }
    }

    @Test
    void applyingToAProviderRefusesAChannelTheGroupCannotReach() throws Exception {
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            TransportChannelProvider fixed =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

            assertThatThrownBy(() -> lease.applyTo(fixed))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("builds its own channels");
        } finally {
            lease.close();
            channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void openReleasesTheLeaseWhenTheClientCannotBeCreated() {
        assertThatThrownBy(
                        () ->
                                GrpcEventLoops.open(
                                        lease -> {
                                            assertThat(GrpcEventLoops.isRunning()).isTrue();
                                            throw new IOException("no client");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("no client");
        // noLeaseIsLeftOpen() is the assertion that matters.
    }

    @Test
    void aLeasedClientReleasesItsLeaseOnlyOnceTheClientTerminated() throws Exception {
        FakeClient client = new FakeClient(Termination.TERMINATES);
        GrpcEventLoops.LeasedClient<FakeClient> leased = GrpcEventLoops.open(lease -> client);

        assertThat(leased.client()).isSameAs(client);
        leased.close();
        leased.close();

        assertThat(client.calls).containsExactly("shutdown", "awaitTermination");
        assertThat(client.groupRunningAtEachCall).containsOnly(true);
    }

    @Test
    void aSecondCloseWaitsForTheFirstToFinish() throws Exception {
        CountDownLatch terminating = new CountDownLatch(1);
        CountDownLatch terminate = new CountDownLatch(1);
        BlockingClient client = new BlockingClient(terminating, terminate);
        GrpcEventLoops.LeasedClient<BlockingClient> leased = GrpcEventLoops.open(lease -> client);
        Thread first = new Thread(leased::close, "first-close");
        Thread second = new Thread(leased::close, "second-close");
        try {
            first.start();
            assertThat(terminating.await(10, TimeUnit.SECONDS)).isTrue();

            second.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (second.getState() != Thread.State.WAITING
                    && second.getState() != Thread.State.TERMINATED
                    && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(second.getState())
                    .as("the second close returned while the first was still closing the client")
                    .isEqualTo(Thread.State.WAITING);
        } finally {
            terminate.countDown();
            first.join(TimeUnit.SECONDS.toMillis(15));
            second.join(TimeUnit.SECONDS.toMillis(15));
        }
        assertThat(second.isAlive()).isFalse();
        assertThat(client.shutdowns).hasValue(1);
    }

    @Test
    void closeAfterCancelsAClientThatDoesNotStopAndStillReleases() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        FakeClient client = new FakeClient(Termination.TIMES_OUT);

        lease.closeAfter(client);

        assertThat(client.calls)
                .containsExactly("shutdown", "awaitTermination", "shutdownNow", "awaitTermination");
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void closeAfterRestoresAnInterruptAndStillReleases() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        FakeClient client = new FakeClient(Termination.INTERRUPTED);
        try {
            lease.closeAfter(client);

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clears the flag, so the interrupt does not leak into the next test.
            Thread.interrupted();
        }
        assertThat(client.calls).containsExactly("shutdown", "awaitTermination", "shutdownNow");
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void closeAfterReleasesEvenWhenShutdownThrows() {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        FakeClient client = new FakeClient(Termination.SHUTDOWN_THROWS);

        assertThatThrownBy(() -> lease.closeAfter(client))
                .isInstanceOf(IllegalStateException.class);
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ApiFunction<ManagedChannelBuilder<?>, ManagedChannelBuilder<?>> configurator(
            GrpcEventLoops.Lease lease) {
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                lease.applyTo(InstantiatingGrpcChannelProvider.newBuilder())
                        .getChannelConfigurator();
        return builder -> configurator.apply(builder);
    }

    private static String prefix() {
        return GrpcEventLoops.THREAD_NAME_PREFIX;
    }

    private enum Termination {
        TERMINATES,
        TIMES_OUT,
        INTERRUPTED,
        SHUTDOWN_THROWS
    }

    /** A client whose termination wait answers as configured, recording what it was asked. */
    private static final class FakeClient implements BackgroundResource {

        private final Termination termination;
        private final List<String> calls = new ArrayList<>();
        private final List<Boolean> groupRunningAtEachCall = new ArrayList<>();

        private FakeClient(Termination termination) {
            this.termination = termination;
        }

        private void record(String call) {
            calls.add(call);
            groupRunningAtEachCall.add(GrpcEventLoops.isRunning());
        }

        @Override
        public void shutdown() {
            record("shutdown");
            if (termination == Termination.SHUTDOWN_THROWS) {
                throw new IllegalStateException("shutdown failed");
            }
        }

        @Override
        public boolean isShutdown() {
            return calls.contains("shutdown");
        }

        @Override
        public boolean isTerminated() {
            return termination == Termination.TERMINATES && isShutdown();
        }

        @Override
        public void shutdownNow() {
            record("shutdownNow");
        }

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
            record("awaitTermination");
            if (termination == Termination.INTERRUPTED) {
                throw new InterruptedException();
            }
            return termination == Termination.TERMINATES;
        }

        @Override
        public void close() {
            shutdown();
        }
    }

    /** A client whose termination wait blocks until the test releases it, counting shutdowns. */
    private static final class BlockingClient implements BackgroundResource {

        private final CountDownLatch terminating;
        private final CountDownLatch terminate;
        private final AtomicInteger shutdowns = new AtomicInteger();

        private BlockingClient(CountDownLatch terminating, CountDownLatch terminate) {
            this.terminating = terminating;
            this.terminate = terminate;
        }

        @Override
        public void shutdown() {
            shutdowns.incrementAndGet();
        }

        @Override
        public boolean isShutdown() {
            return shutdowns.get() > 0;
        }

        @Override
        public boolean isTerminated() {
            return terminate.getCount() == 0;
        }

        @Override
        public void shutdownNow() {}

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
            terminating.countDown();
            // Ignores the bound, so the first close stays inside it until the test lets go.
            terminate.await();
            return true;
        }

        @Override
        public void close() {
            shutdown();
        }
    }
}
