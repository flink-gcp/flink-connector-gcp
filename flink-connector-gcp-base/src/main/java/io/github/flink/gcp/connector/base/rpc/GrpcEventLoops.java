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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ClientSettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollIoHandler;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.netty.shaded.io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Netty event-loop group the connectors' gRPC channels run on, in place of gRPC's own.
 *
 * <p>gRPC's Netty transport shares one worker event-loop group per class loader and shuts it down
 * about a second after the last channel using it closes; the group's own threads then run their
 * teardown. When the connector ships in the job jar, those classes belong to the job's user-code
 * class loader, which Flink closes as soon as the job leaves the TaskManager. The delayed destroy
 * then runs on a closed loader, the first class it has not loaded yet fails with {@link
 * NoClassDefFoundError}, and under {@code cluster.uncaught-exception-handling: FAIL} the
 * TaskManager exits (#2237, measured on grpc-netty-shaded 1.83.0: 7 exits in 34 trials).
 *
 * <p>The group here is shut down by the thread that releases its last {@link Lease}, which waits
 * for the threads to finish. A connector releases its leases from its own {@code close()}. On a
 * TaskManager, Flink runs a writer's and a reader's {@code close()} on the task thread before it
 * releases the class loader, so the group is gone first. The wait ignores an interrupt, except
 * inside {@link #interruptibleReleases()}. On the JobManager, Flink closes a source's enumerator on
 * a thread of its own and does not wait for it, and in practice after the loader is released (Flink
 * 2.2.1, measured in #2237), so a JobManager-side client must not stay open until the enumerator
 * closes: it closes once the call it was opened for returns.
 *
 * <p>One group serves every lease in the class loader, the way gRPC's does: it is created by the
 * first lease and shut down by the release of the last one, and a later lease creates a new one. A
 * channel therefore has to close before the lease that covers it is released. A client that owns
 * its channels is opened with {@link #open(ClientFactory)}, whose {@link LeasedClient#close()} does
 * both in that order. A client whose settings take a channel configurator instead of a transport
 * provider composes {@link #channelConfigurator} and is opened under a lease its owner holds. A
 * channel built while no lease is open falls back to gRPC's own group, so a configurator that
 * outlives its lease never hands a channel a terminated group.
 *
 * <p>Like gRPC's group, it uses epoll where the shaded native transport loads and NIO otherwise,
 * with Netty's default thread count. The threads are daemons named {@code flink-gcp-grpc-elg-*}.
 */
@Internal
public final class GrpcEventLoops {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcEventLoops.class);

    /** The prefix of the group's thread names. */
    @VisibleForTesting static final String THREAD_NAME_PREFIX = "flink-gcp-grpc-elg";

    /**
     * How long a client's channels get to finish in-flight calls before they are cancelled, and
     * again after the cancellation. gax passes the same duration to each resource a client
     * aggregates, so the whole wait is a small multiple of this.
     */
    private static final long CLIENT_TERMINATION_SECONDS = 5;

    /** How long the group's shutdown may take; Netty closes any channel still open on it. */
    private static final long GROUP_SHUTDOWN_SECONDS = 10;

    /** How long a release waits for the group's shutdown at most. */
    private static final long GROUP_RELEASE_WAIT_SECONDS = GROUP_SHUTDOWN_SECONDS + 1;

    /**
     * How long a release inside {@link #interruptibleReleases()} waits for the group's shutdown
     * even if its thread is interrupted. Beyond this, an interrupt ends the wait.
     */
    @VisibleForTesting static final long GROUP_SHUTDOWN_UNINTERRUPTIBLE_SECONDS = 1;

    /** Set on a thread inside {@link #interruptibleReleases()}; absent otherwise. */
    private static final ThreadLocal<Boolean> INTERRUPTIBLE_RELEASES = new ThreadLocal<>();

    private static final Object LOCK = new Object();

    @GuardedBy("LOCK")
    @Nullable
    private static Group group;

    @GuardedBy("LOCK")
    private static int leases;

    private GrpcEventLoops() {}

    /**
     * Opens a lease on the group, creating the group if no lease is open.
     *
     * <p>For a client that owns its channels, {@link #open(ClientFactory)} is the safer form: it
     * releases the lease if the client cannot be created and orders the release after the client's
     * termination. A bare lease suits a caller whose channels belong to something else, such as the
     * BigQuery Storage Write API's connection pool, which closes and awaits them itself.
     *
     * @return the lease; the caller closes it after the channels it covers have terminated
     */
    public static Lease acquire() {
        synchronized (LOCK) {
            if (group == null) {
                group = Group.create();
            }
            leases++;
            return new Lease();
        }
    }

    /**
     * Opens a lease and creates a client under it, releasing the lease if the creation fails.
     *
     * @param factory creates the client, applying the lease to its settings
     * @param <C> the client type
     * @return the client paired with its lease
     * @throws IOException if the factory does
     */
    public static <C extends BackgroundResource> LeasedClient<C> open(ClientFactory<C> factory)
            throws IOException {
        Preconditions.checkNotNull(factory, "factory must not be null");
        return open(factory, null);
    }

    /**
     * Opens a lease and creates a client under it that also owns {@code channel}: closing the
     * returned holder closes the client, then the channel. For {@link
     * EmulatorChannels#openLeasedClient}, whose client runs on a channel the lease cannot reach.
     */
    static <C extends BackgroundResource> LeasedClient<C> open(
            ClientFactory<C> factory, @Nullable LeasedClient<?> channel) throws IOException {
        Lease lease = acquire();
        try {
            return new LeasedClient<>(factory.create(lease), lease, channel);
        } catch (IOException | RuntimeException | Error e) {
            lease.close();
            throw e;
        }
    }

    /**
     * Lets an interrupt end this thread's waits for the group once their first second has passed,
     * until the returned scope is closed.
     *
     * <p>Outside such a scope, the release of the last lease waits for the group's shutdown
     * whatever the thread's interrupt says, so that a task thread Flink interrupts to cancel the
     * task, which can release a lease outside its close, still sees the group gone before Flink
     * releases the class loader. Open the scope only on a thread Flink stops by interrupting it and
     * gives a deadline to end, where a wait for the whole bound could outlast that deadline. The
     * first second still ignores an interrupt, which covers a shutdown no busy event loop holds up
     * (measured in #2269 at milliseconds).
     *
     * @return the scope, to be closed on the same thread, usually by try-with-resources
     */
    public static InterruptibleReleases interruptibleReleases() {
        boolean outermost = !inInterruptibleReleases();
        INTERRUPTIBLE_RELEASES.set(Boolean.TRUE);
        return new InterruptibleReleases(outermost);
    }

    /**
     * Returns whether this thread is inside {@link #interruptibleReleases()}. For tests that check
     * a release runs in the scope.
     *
     * @return whether the scope is open on this thread
     */
    @VisibleForTesting
    public static boolean inInterruptibleReleases() {
        return Boolean.TRUE.equals(INTERRUPTIBLE_RELEASES.get());
    }

    /**
     * Returns a channel configurator that applies {@code existing} and then puts the channel on the
     * group, for a client whose settings take a configurator rather than a transport provider, such
     * as Spanner's {@code SpannerOptions}.
     *
     * <p>It is the configurator a lease's {@code applyTo} puts on a provider builder, offered as a
     * static method because such settings are usually assembled before the client is opened and its
     * lease exists. Like that one, it reaches the group only while a lease is open, so the caller
     * opens one before the client builds its first channel and releases it once the client's
     * channels have terminated; a channel it configures while no lease is open runs on gRPC's own
     * group.
     *
     * @param existing the configurator to apply first, or {@code null}
     * @return the composed configurator
     */
    @SuppressWarnings("rawtypes")
    public static ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> channelConfigurator(
            @Nullable ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> existing) {
        return builder -> configure(existing == null ? builder : existing.apply(builder));
    }

    /**
     * Returns whether the group is running, which it is exactly while a lease is open. For tests
     * that check a client released its lease when they expect it to.
     *
     * @return whether a lease is open
     */
    @VisibleForTesting
    public static boolean isRunning() {
        synchronized (LOCK) {
            return group != null;
        }
    }

    /** Runs a task on one of the group's threads; a lease must be open. */
    @VisibleForTesting
    static void executeOnGroup(Runnable task) {
        synchronized (LOCK) {
            Preconditions.checkState(group != null, "No gRPC event-loop lease is open");
            group.eventLoopGroup.execute(task);
        }
    }

    /**
     * Points a Netty channel builder at the group, or leaves it alone when it is not a Netty
     * builder or no lease is open.
     */
    private static ManagedChannelBuilder<?> configure(ManagedChannelBuilder<?> builder) {
        if (!(builder instanceof NettyChannelBuilder)) {
            return builder;
        }
        synchronized (LOCK) {
            if (group == null) {
                return builder;
            }
            return ((NettyChannelBuilder) builder)
                    .eventLoopGroup(group.eventLoopGroup)
                    .channelType(group.channelType);
        }
    }

    private static void release() {
        boolean interruptible = inInterruptibleReleases();
        Group toStop = null;
        synchronized (LOCK) {
            Preconditions.checkState(leases > 0, "No gRPC event-loop lease is open");
            if (--leases == 0) {
                toStop = group;
                group = null;
            }
        }
        if (toStop != null) {
            toStop.shutdown(interruptible);
        }
    }

    /**
     * The scope {@link #interruptibleReleases()} opens. Scopes nest: closing the outermost one ends
     * the scope on the thread, and closing a nested one leaves it open.
     */
    public static final class InterruptibleReleases implements AutoCloseable {

        private final boolean outermost;

        private InterruptibleReleases(boolean outermost) {
            this.outermost = outermost;
        }

        /** Closes the scope on the calling thread, which must be the one that opened it. */
        @Override
        public void close() {
            if (outermost) {
                INTERRUPTIBLE_RELEASES.remove();
            }
        }
    }

    /** Creates a client whose settings the given lease has been applied to. */
    @FunctionalInterface
    public interface ClientFactory<C extends BackgroundResource> {

        /**
         * Creates the client.
         *
         * @param lease the lease to apply to the client's settings
         * @return the client
         * @throws IOException if the client cannot be created
         */
        C create(Lease lease) throws IOException;
    }

    /**
     * A hold on the group. Closing the last open lease shuts the group down and waits for its
     * threads, which an interrupt cuts short only inside {@link #interruptibleReleases()}; closing
     * a lease twice does nothing.
     */
    public static final class Lease implements AutoCloseable {

        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease() {}

        /**
         * Puts the channels a client's settings build on the group, composed after any channel
         * configurator the settings' transport provider already carries, such as {@link
         * EmulatorChannels}'s plaintext one.
         *
         * @param settings a settings builder whose transport provider builds its own channels
         * @param <S> the settings type
         * @param <B> the settings builder type
         * @return the same builder
         * @throws IllegalArgumentException if the transport provider is not an instantiating gRPC
         *     provider, such as a fixed one wrapping a channel the caller built — the group cannot
         *     reach that channel, and leaving it on gRPC's group silently is what this class exists
         *     to prevent
         */
        public <S extends ClientSettings<S>, B extends ClientSettings.Builder<S, B>> B applyTo(
                B settings) {
            Preconditions.checkNotNull(settings, "settings must not be null");
            return settings.setTransportChannelProvider(
                    applyTo(settings.getTransportChannelProvider()));
        }

        /** Returns a copy of an instantiating provider with the group applied; refuses others. */
        @VisibleForTesting
        TransportChannelProvider applyTo(TransportChannelProvider provider) {
            Preconditions.checkArgument(
                    provider instanceof InstantiatingGrpcChannelProvider,
                    "The event-loop group applies only to a provider that builds its own channels,"
                            + " but the settings carry %s",
                    provider);
            return applyTo(((InstantiatingGrpcChannelProvider) provider).toBuilder()).build();
        }

        /**
         * Puts the channels a provider builds on the group, composed after any configurator the
         * builder already carries.
         *
         * @param provider the API's own transport provider builder
         * @return the same builder
         */
        public InstantiatingGrpcChannelProvider.Builder applyTo(
                InstantiatingGrpcChannelProvider.Builder provider) {
            Preconditions.checkNotNull(provider, "provider must not be null");
            return provider.setChannelConfigurator(
                    channelConfigurator(provider.getChannelConfigurator()));
        }

        /**
         * Puts a channel built outside any transport provider on the group, for {@link
         * EmulatorChannels#openLeasedPlaintextChannel(InstantiatingGrpcChannelProvider.Builder,
         * EmulatorEndpoint)}'s caller-owned channel.
         *
         * @param builder the channel builder
         * @return the same builder
         * @throws IllegalStateException if this lease is closed, since the channel would then run
         *     on a group no lease holds open
         */
        NettyChannelBuilder applyTo(NettyChannelBuilder builder) {
            Preconditions.checkNotNull(builder, "builder must not be null");
            Preconditions.checkState(!closed.get(), "The gRPC event-loop lease is closed");
            return (NettyChannelBuilder) configure(builder);
        }

        /**
         * Shuts a client down, waits for its channels to terminate, and then closes this lease.
         *
         * <p>In-flight calls get a bounded time to finish before they are cancelled. The lease is
         * closed whatever happens, so the group never outlives its leases because a client would
         * not stop; Netty then closes any channel still open on it. An interrupt cuts the wait for
         * the client short and is restored; the wait for the group is {@link #close()}'s.
         */
        void closeAfter(BackgroundResource client) {
            try {
                client.shutdown();
                if (!client.awaitTermination(CLIENT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    client.shutdownNow();
                    client.awaitTermination(CLIENT_TERMINATION_SECONDS, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                client.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release();
            }
        }
    }

    /**
     * A client and the lease covering its channels. Closing it shuts the client down, waits for its
     * channels to terminate, cancels their calls if they have not within a bounded time, and only
     * then releases the lease; a holder from {@link EmulatorChannels#openLeasedClient} then closes
     * the emulator channel the client ran on the same way. The first {@link #close()} does that; a
     * concurrent or later one waits until it has finished, unless it is interrupted while it waits.
     * A client that does not terminate within the bounds is given up on, so a finished close means
     * the lease is released, not that every channel has terminated.
     *
     * @param <C> the client type
     */
    public static final class LeasedClient<C extends BackgroundResource> implements AutoCloseable {

        private final C client;
        private final Lease lease;

        /** A channel the client runs on and this holder closes after it, or {@code null}. */
        @Nullable private final LeasedClient<?> channel;

        private final AtomicBoolean closing = new AtomicBoolean();
        private final CountDownLatch closed = new CountDownLatch(1);

        private LeasedClient(C client, Lease lease, @Nullable LeasedClient<?> channel) {
            this.client = client;
            this.lease = lease;
            this.channel = channel;
        }

        /** Returns the client; close this holder, not the client. */
        public C client() {
            return client;
        }

        /**
         * Closes the client and releases its lease, or waits for the close already under way. An
         * interrupt ends the wait early and is restored.
         */
        @Override
        public void close() {
            if (closing.compareAndSet(false, true)) {
                try {
                    // closeAll's contract: the channel closes whatever the client's close did,
                    // and the first failure is the one reported, with a later one suppressed.
                    Closers.closeAll(() -> lease.closeAfter(client), channel);
                } catch (Exception e) {
                    // Neither close declares a checked exception, so this is the unchecked one
                    // closeAll collected; an Error leaves closeAll as itself and never reaches
                    // here.
                    ExceptionUtils.rethrow(e);
                } finally {
                    closed.countDown();
                }
                return;
            }
            try {
                closed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** A running event-loop group and the channel type that matches its transport. */
    private static final class Group {

        private final EventLoopGroup eventLoopGroup;
        private final Class<? extends Channel> channelType;

        private Group(EventLoopGroup eventLoopGroup, Class<? extends Channel> channelType) {
            this.eventLoopGroup = eventLoopGroup;
            this.channelType = channelType;
        }

        static Group create() {
            DefaultThreadFactory threads = new DefaultThreadFactory(THREAD_NAME_PREFIX, true);
            if (Epoll.isAvailable()) {
                return new Group(
                        new MultiThreadIoEventLoopGroup(0, threads, EpollIoHandler.newFactory()),
                        EpollSocketChannel.class);
            }
            return new Group(
                    new MultiThreadIoEventLoopGroup(0, threads, NioIoHandler.newFactory()),
                    NioSocketChannel.class);
        }

        /**
         * Shuts the group down and waits for its threads, for up to {@code
         * GROUP_RELEASE_WAIT_SECONDS}. Unless the release is interruptible, the whole wait ignores
         * an interrupt. If it is, only the first {@code GROUP_SHUTDOWN_UNINTERRUPTIBLE_SECONDS} do,
         * so that a shutdown no busy event loop holds up still finishes before the release returns;
         * after that, an interrupt ends the wait and is restored, and the group finishes on its own
         * threads.
         */
        void shutdown(boolean interruptible) {
            Future<?> termination =
                    eventLoopGroup.shutdownGracefully(0, GROUP_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
            long uninterruptible =
                    interruptible
                            ? GROUP_SHUTDOWN_UNINTERRUPTIBLE_SECONDS
                            : GROUP_RELEASE_WAIT_SECONDS;
            try {
                if (termination.awaitUninterruptibly(uninterruptible, TimeUnit.SECONDS)
                        || (interruptible
                                && termination.await(
                                        GROUP_RELEASE_WAIT_SECONDS - uninterruptible,
                                        TimeUnit.SECONDS))) {
                    return;
                }
                LOG.warn(
                        "The gRPC event-loop group did not terminate within {} s; its threads may"
                                + " outlive the job's class loader",
                        GROUP_RELEASE_WAIT_SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while waiting for the gRPC event-loop group to terminate; its"
                                + " threads may outlive the job's class loader");
            }
        }
    }
}
