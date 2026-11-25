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
import org.apache.flink.util.UserCodeClassLoader;

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
import java.util.function.Function;

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
 * channel therefore has to close before the lease that covers it is released. Once {@link
 * #pinTo(UserCodeClassLoader)} has pinned the group to the job's class loader, the release of the
 * last lease leaves it running, and Flink's release of that loader shuts it down instead. A client
 * that owns its channels is opened with {@link #open(ClientFactory)}, or with {@link
 * #openAutoCloseable(ClientFactory)} when it is not a gax {@link BackgroundResource}, and the
 * returned {@link LeasedClient#close()} does both in that order. A client whose settings take a
 * channel configurator instead of a transport provider composes {@link #channelConfigurator} into
 * them before the lease exists. A channel built while the group is not running falls back to gRPC's
 * own group, so a configurator that outlives its lease never hands a channel a terminated group;
 * while a pinned group runs without a lease, such a channel goes on that group.
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

    /**
     * The name {@link #pinTo(UserCodeClassLoader)} registers its release hook under: this class's
     * name, which a shaded copy carries relocated. Flink keeps one hook per name, so if two copies
     * in one job loader each pinned, such as the connector's and a SQL connector JAR's relocated
     * one, the second copy's hook would not be ignored as a duplicate of the first. (Flink's
     * registry is an unsynchronized map, so registrations racing on two threads could still lose
     * one.)
     */
    @VisibleForTesting static final String RELEASE_HOOK_NAME = GrpcEventLoops.class.getName();

    /** Set on a thread inside {@link #interruptibleReleases()}; absent otherwise. */
    private static final ThreadLocal<Boolean> INTERRUPTIBLE_RELEASES = new ThreadLocal<>();

    private static final Object LOCK = new Object();

    @GuardedBy("LOCK")
    @Nullable
    private static Group group;

    @GuardedBy("LOCK")
    private static int leases;

    /**
     * Whether a release hook on the job's class loader, rather than the release of the last lease,
     * shuts the group down.
     */
    @GuardedBy("LOCK")
    private static boolean pinned;

    /**
     * Counts the release hooks that have run. A lease counts towards {@code leases} only while it
     * belongs to the current generation: the hook stops the group under any lease still open, and
     * such a lease's later release must not keep a group created after the hook from stopping.
     */
    @GuardedBy("LOCK")
    private static int generation;

    /**
     * The class loader whose release hook has run, which this copy does not pin to again: Flink has
     * already run that loader's hooks and would not run a new one. The loader itself rather than
     * the {@link UserCodeClassLoader}, because Flink hands each call to {@code
     * getUserCodeClassLoader()} a new wrapper around the same loader.
     */
    @GuardedBy("LOCK")
    @Nullable
    private static ClassLoader releasedLoader;

    private GrpcEventLoops() {}

    /**
     * Opens a lease on the group, creating the group if it is not running.
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
            return new Lease(generation);
        }
    }

    /**
     * Opens a lease and creates a gax client under it, releasing the lease if the creation fails.
     *
     * <p>The returned holder's {@link LeasedClient#close()} shuts the client down, waits a bounded
     * time for its channels to terminate, cancels their calls if they have not, and then releases
     * the lease.
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
        return open(factory, client -> () -> shutdownBounded(client), channel, false);
    }

    /**
     * Opens a lease and creates a client under it that is not a gax {@link BackgroundResource},
     * releasing the lease if the creation fails. For a client such as Bigtable's or Spanner's,
     * whose termination this class cannot wait for.
     *
     * <p>The returned holder's {@link LeasedClient#close()} calls the client's own {@code close()}
     * once and then releases the lease, whatever that close did. How long the close waits, and for
     * what, is the client library's; a gax client is opened with {@link #open(ClientFactory)}
     * instead, whose close this class bounds. A close that consumes the thread's interrupt, whether
     * set before it began or delivered while it ran and reported as the cause of its failure, has
     * the interrupt restored before the lease is released, so that a release inside {@link
     * #interruptibleReleases()} still sees it.
     *
     * @param factory creates the client, applying the lease to its settings if they take a
     *     transport provider
     * @param <C> the client type
     * @return the client paired with its lease
     * @throws IOException if the factory does
     * @throws IllegalArgumentException if the factory returns a gax {@link BackgroundResource},
     *     which is shut down within {@link #open(ClientFactory)}'s bounds before this is thrown,
     *     since its own {@code close()} would not wait for its channels
     */
    public static <C extends AutoCloseable> LeasedClient<C> openAutoCloseable(
            ClientFactory<C> factory) throws IOException {
        return openAutoCloseable(factory, AutoCloseable::close);
    }

    /**
     * Opens as {@link #openAutoCloseable(ClientFactory)} does, but closes the client with {@code
     * closer} rather than its own {@code close()}. For a client whose own close waits for calls
     * that cancelling their futures does not end, such as Firestore's, whose {@code close()} waits
     * for a query stream until the stream's deadline (google-cloud-firestore 3.53.0): the closer
     * can end them first. The interrupt is handled as {@link #openAutoCloseable(ClientFactory)}
     * handles it around the client's close.
     *
     * @param factory creates the client, applying the lease to its settings if they take a
     *     transport provider
     * @param closer closes the client
     * @param <C> the client type
     * @return the client paired with its lease
     * @throws IOException if the factory does
     * @throws IllegalArgumentException if the factory returns a gax {@link BackgroundResource}, as
     *     {@link #openAutoCloseable(ClientFactory)} does
     */
    public static <C extends AutoCloseable> LeasedClient<C> openAutoCloseable(
            ClientFactory<C> factory, ClientCloser<? super C> closer) throws IOException {
        Preconditions.checkNotNull(factory, "factory must not be null");
        Preconditions.checkNotNull(closer, "closer must not be null");
        return open(
                factory,
                client -> () -> closeRestoringInterrupt(() -> closer.close(client)),
                null,
                true);
    }

    private static <C> LeasedClient<C> open(
            ClientFactory<C> factory,
            Function<? super C, AutoCloseable> closer,
            @Nullable LeasedClient<?> channel,
            boolean ownClose)
            throws IOException {
        Lease lease = acquire();
        try {
            C client = factory.create(lease);
            Preconditions.checkState(client != null, "The client factory returned null");
            if (ownClose && client instanceof BackgroundResource) {
                shutdownBounded((BackgroundResource) client);
                throw new IllegalArgumentException(
                        "A gax BackgroundResource is opened with GrpcEventLoops.open, whose close"
                                + " waits for its channels; got "
                                + client.getClass().getName());
            }
            return new LeasedClient<>(client, closer.apply(client), ownClose, lease, channel);
        } catch (IOException | RuntimeException | Error e) {
            lease.close();
            throw e;
        }
    }

    /**
     * Shuts a gax client down and waits for its channels to terminate, cancelling their calls if
     * they have not within a bounded time.
     *
     * <p>{@code shutdown()} reaches every resource the client aggregates before anything waits (gax
     * 2.91.0 {@code BackgroundResourceAggregation}), so an interrupt cuts short only the waits: it
     * is answered with {@code shutdownNow()} and restored.
     */
    private static void shutdownBounded(BackgroundResource client) {
        boolean interrupted = false;
        try {
            client.shutdown();
            if (!client.awaitTermination(CLIENT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                client.shutdownNow();
                client.awaitTermination(CLIENT_TERMINATION_SECONDS, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            interrupted = true;
            client.shutdownNow();
        } finally {
            // Restored even if shutdownNow() threw, so that a release inside
            // interruptibleReleases() still sees it.
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Closes a client, with its own {@code close()} or with a {@link ClientCloser}, then restores
     * an interrupt that close consumed: one already set when the close began, or one that arrived
     * during it, which a client such as Bigtable's or Spanner's reports only as a close failure
     * caused by an {@link InterruptedException}.
     */
    private static void closeRestoringInterrupt(AutoCloseable client) throws Exception {
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
            client.close();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                interrupted = true;
            }
            throw e;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
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
     * Keeps the group running until Flink releases the job's user-code class loader, if that loader
     * is the one that defined this class.
     *
     * <p>From then on, the release of the last lease leaves the group running, and a release hook
     * on the loader shuts it down and waits for its threads, which Flink runs before it closes the
     * loader. For channels that outlive every lease but not the loader: the BigQuery Storage Write
     * API's connection pool is static per class loader and builds every connection from the first
     * writer's settings, so a job's own writer can run on connections the connector's settings put
     * on the group, and the release of the connector's last lease would stop the group under them.
     *
     * <p>Where this class comes from a loader the job shares, such as one Flink never closes when
     * the connector is in Flink's {@code lib/} directory or on the application class path, the
     * group is shared with other jobs in the process, so this does nothing and the last lease's
     * release keeps shutting the group down. Pinning again, from any writer of the same job, does
     * nothing either, nor does pinning to a loader whose hook has already run.
     *
     * <p>The hook must run for the group to stop. Flink runs a loader's hooks once and keeps no
     * record that it has, so a pin registered after that pass, which can happen only when Flink
     * releases the loader without waiting for a task still creating its writer (a TaskManager
     * shutting down or dropping the job), keeps the group's threads until the process exits.
     *
     * <p>Call it only where a client's channels can outlive the connector's last lease, from a
     * writer's or reader's creation on a TaskManager, whose init context offers the loader.
     * Everywhere else the lease rule already stops the group with its last channel, and a pin only
     * moves that shutdown to the release of the loader, which Flink runs on the TaskManager's main
     * thread while it holds its library cache's lock; the hook waits there for the group's shutdown
     * however that thread is interrupted (ADR-0208).
     *
     * @param userCodeClassLoader the job's user-code class loader
     */
    public static void pinTo(UserCodeClassLoader userCodeClassLoader) {
        Preconditions.checkNotNull(userCodeClassLoader, "userCodeClassLoader must not be null");
        ClassLoader loader;
        try {
            loader = userCodeClassLoader.asClassLoader();
            if (!isLoadedByJob(GrpcEventLoops.class, loader, ClassLoader.getSystemClassLoader())) {
                return;
            }
        } catch (RuntimeException | LinkageError e) {
            // The loaders could not be inspected, a security manager refusing getClassLoader, say;
            // the pin is optional, so the writer goes on without it.
            return;
        }
        pin(userCodeClassLoader, loader);
    }

    /**
     * Pins the group to {@code userCodeClassLoader} without checking which loader defined this
     * class, which in a test is never the job's.
     */
    @VisibleForTesting
    static void pin(UserCodeClassLoader userCodeClassLoader) {
        pin(userCodeClassLoader, userCodeClassLoader.asClassLoader());
    }

    /**
     * Pins the group to {@code userCodeClassLoader}, whose class loader {@code loader} the caller
     * has already read, so that it is read once.
     */
    private static void pin(UserCodeClassLoader userCodeClassLoader, ClassLoader loader) {
        synchronized (LOCK) {
            if (pinned || loader == releasedLoader) {
                return;
            }
            userCodeClassLoader.registerReleaseHookIfAbsent(
                    RELEASE_HOOK_NAME, () -> releasePin(loader));
            pinned = true;
        }
    }

    /**
     * Returns whether {@code type} was defined by the job's own class loader, which Flink closes
     * when the job leaves the process, rather than by one it shares with other jobs.
     *
     * <p>{@code userCode} can be Flink's wrapper around the loader that defines the job's classes,
     * whose parent is the same as that loader's. So the check is that the job's loader resolves
     * {@code type}'s name to {@code type} itself, that its parent does not, and that {@code type}'s
     * loader is neither the system class loader nor one of its ancestors. A parent that resolves
     * the same class shares it with every job whose loader has that parent: the connector in {@code
     * lib/}, on the application class path, resolved parent-first, or reached through a parent that
     * delegates to another loader. The system class loader is excluded because Flink's JobManager
     * hands it to a job with no JARs or class paths of its own and never closes it.
     *
     * @param type the class whose loader is in question
     * @param userCode the loader Flink gave the job
     * @param system the system class loader
     * @return whether the job's loader defined {@code type}
     */
    @VisibleForTesting
    static boolean isLoadedByJob(Class<?> type, ClassLoader userCode, ClassLoader system) {
        try {
            ClassLoader loader = type.getClassLoader();
            if (loader == null || isSelfOrAncestor(loader, system)) {
                return false;
            }
            return !resolves(userCode.getParent(), type) && resolves(userCode, type);
        } catch (RuntimeException | LinkageError e) {
            // Inconclusive, so the pin, which is optional, is skipped: Flink's leak-check wrapper
            // throws IllegalStateException once its loader has closed, and a parent's own copy
            // can fail to load (a sealing violation, say) while the job's loads.
            return false;
        }
    }

    /**
     * Returns whether {@code loader} resolves {@code type}'s name to {@code type} itself, or {@code
     * false} if it cannot find the name; any other failure propagates.
     */
    private static boolean resolves(@Nullable ClassLoader loader, Class<?> type) {
        if (loader == null) {
            return false;
        }
        try {
            return Class.forName(type.getName(), false, loader) == type;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean isSelfOrAncestor(ClassLoader candidate, @Nullable ClassLoader loader) {
        for (ClassLoader current = loader; current != null; current = current.getParent()) {
            if (current == candidate) {
                return true;
            }
        }
        return false;
    }

    /**
     * The release hook {@link #pin(UserCodeClassLoader, ClassLoader)} registers: shuts the group
     * down and waits for its threads, ignoring an interrupt, and returns the group to the lease
     * rule.
     *
     * <p>A lease can still be open when it runs: Flink releases the loader without waiting for the
     * job's tasks when the TaskManager shuts down or drops the job, and a client nothing closed
     * holds its lease for good. The group stops under such a lease anyway, since its threads must
     * not outlive the loader, and the lease leaves the count, so that its release, if it ever
     * comes, neither stops nor holds up a group a later lease creates.
     *
     * @param loader the class loader whose hook this is, which this copy then does not pin to again
     */
    private static void releasePin(ClassLoader loader) {
        Group toStop;
        int open;
        synchronized (LOCK) {
            pinned = false;
            releasedLoader = loader;
            generation++;
            open = leases;
            leases = 0;
            toStop = group;
            group = null;
        }
        if (open > 0) {
            LOG.warn(
                    "The job's class loader is being released while {} gRPC event-loop lease(s)"
                            + " are open, held by clients that have not closed; shutting the group"
                            + " down under them",
                    open);
        }
        if (toStop != null) {
            toStop.shutdown(false);
        }
    }

    /**
     * Returns a channel configurator that applies {@code existing} and then puts the channel on the
     * group, for a client whose settings take a configurator rather than a transport provider, such
     * as Spanner's {@code SpannerOptions}.
     *
     * <p>It is the configurator a lease's {@code applyTo} puts on a provider builder, offered as a
     * static method because such settings are usually assembled before the client is opened and its
     * lease exists. Like that one, it reaches the group only while the group runs, so the caller
     * opens a lease before the client builds its first channel and releases it once the client's
     * channels have terminated; a channel it configures while the group is not running runs on
     * gRPC's own group.
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
     * Returns whether the group is pinned to a class loader. For tests that check a pin was
     * released.
     *
     * @return whether a release hook, rather than the last lease, shuts the group down
     */
    @VisibleForTesting
    static boolean isPinned() {
        synchronized (LOCK) {
            return pinned;
        }
    }

    /**
     * Returns whether the group is running, which it is while a lease opened since the last release
     * hook is open and, once {@link #pinTo(UserCodeClassLoader)} has pinned it, until the job's
     * class loader is released. For tests that check a client released its lease when they expect
     * it to.
     *
     * @return whether the group is running
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
     * builder or the group is not running.
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

    private static void release(int leaseGeneration) {
        boolean interruptible = inInterruptibleReleases();
        Group toStop = null;
        synchronized (LOCK) {
            if (leaseGeneration != generation) {
                // Opened before a release hook ran, which stopped its group and dropped it from
                // the count.
                return;
            }
            Preconditions.checkState(leases > 0, "No gRPC event-loop lease is open");
            if (--leases == 0 && !pinned) {
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

    /**
     * Closes a client opened with {@link #openAutoCloseable(ClientFactory, ClientCloser)}.
     *
     * @param <C> the client type
     */
    @FunctionalInterface
    public interface ClientCloser<C> {

        /**
         * Closes the client.
         *
         * @param client the client
         * @throws Exception if the close fails
         */
        void close(C client) throws Exception;
    }

    /**
     * Creates a client under a lease that is already open. A client whose settings take a transport
     * provider applies the lease to them; one whose settings already carry {@link
     * #channelConfigurator}, such as Spanner's, has nothing to apply.
     */
    @FunctionalInterface
    public interface ClientFactory<C> {

        /**
         * Creates the client.
         *
         * @param lease the open lease, to apply to the client's settings if they take a transport
         *     provider
         * @return the client
         * @throws IOException if the client cannot be created
         */
        C create(Lease lease) throws IOException;
    }

    /**
     * A hold on the group. Unless the group is pinned to the job's class loader, closing the last
     * lease opened since the group started shuts it down and waits for its threads, which an
     * interrupt cuts short only inside {@link #interruptibleReleases()}. A lease opened before a
     * release hook ran no longer holds any group, and closing it does nothing; closing a lease
     * twice does nothing either.
     */
    public static final class Lease implements AutoCloseable {

        private final AtomicBoolean closed = new AtomicBoolean();

        /** The release-hook generation the lease was opened in. */
        private final int generation;

        private Lease(int generation) {
            this.generation = generation;
        }

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

        /**
         * Returns a copy of a transport provider that puts the channels it builds on the group,
         * composed after any channel configurator it already carries; every other setting it holds
         * is kept. For settings that take a transport provider but do not extend gax's {@code
         * ClientSettings}, such as a Bigtable client's stub settings.
         *
         * @param provider the settings' transport provider
         * @return the copy, to set on the settings in place of {@code provider}
         * @throws IllegalArgumentException if the provider is not an instantiating gRPC provider,
         *     as {@link #applyTo(ClientSettings.Builder)} does
         */
        public TransportChannelProvider applyTo(TransportChannelProvider provider) {
            Preconditions.checkNotNull(provider, "provider must not be null");
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
         * @throws IllegalStateException if this lease is closed, since the channel could then run
         *     on a group no lease holds open
         */
        NettyChannelBuilder applyTo(NettyChannelBuilder builder) {
            Preconditions.checkNotNull(builder, "builder must not be null");
            Preconditions.checkState(!closed.get(), "The gRPC event-loop lease is closed");
            return (NettyChannelBuilder) configure(builder);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(generation);
            }
        }
    }

    /**
     * A client and the lease covering its channels.
     *
     * <p>Closing it closes the client the way the method that opened it states ({@link
     * GrpcEventLoops#open(ClientFactory)} bounds a gax client's shutdown; {@link
     * GrpcEventLoops#openAutoCloseable(ClientFactory)} calls the client's own {@code close()}),
     * then closes the emulator channel the client ran on if it came from {@link
     * EmulatorChannels#openLeasedClient}, and only then releases the lease, whatever the earlier
     * steps did. The first {@link #close()} does that; a concurrent or later one waits until it has
     * finished, and an interrupt, whether set when it begins to wait or delivered while it waits,
     * ends the wait at once and is restored. A finished close means the lease is released, not that
     * every channel has terminated: a client that does not terminate within its bounds is given up
     * on, and Netty closes a channel still open on the group when the group shuts down.
     *
     * @param <C> the client type
     */
    public static final class LeasedClient<C> implements AutoCloseable {

        private final C client;

        /** Closes the client, without the lease. */
        private final AutoCloseable clientClose;

        /**
         * Whether the client closes with its own {@code close()}, which can skip cleanup on an
         * interrupted thread, rather than with the bounded gax shutdown, which skips none.
         */
        private final boolean ownClose;

        private final Lease lease;

        /** A channel the client runs on and this holder closes after it, or {@code null}. */
        @Nullable private final LeasedClient<?> channel;

        private final AtomicBoolean closing = new AtomicBoolean();
        private final CountDownLatch closed = new CountDownLatch(1);

        private LeasedClient(
                C client,
                AutoCloseable clientClose,
                boolean ownClose,
                Lease lease,
                @Nullable LeasedClient<?> channel) {
            this.client = client;
            this.clientClose = clientClose;
            this.ownClose = ownClose;
            this.lease = lease;
            this.channel = channel;
        }

        /**
         * Returns the client; close this holder, not the client.
         *
         * @return the client
         */
        public C client() {
            return client;
        }

        /**
         * Closes the client, then its emulator channel if it has one, and releases the lease, or
         * waits for the close already under way. The lease is released even if an earlier step
         * fails; the first failure is then thrown, a checked one wrapped in an unchecked exception.
         * An interrupt ends a waiting caller's wait at once and is restored.
         */
        @Override
        public void close() {
            close(false);
        }

        /**
         * Closes as {@link #close()} does, but for a client opened with {@link
         * GrpcEventLoops#openAutoCloseable(ClientFactory)} or its form with a {@link ClientCloser},
         * if this call is the one that closes it, the thread's interrupt is held back while the
         * client closes and is restored before the lease is released. A caller that waits for a
         * close already under way is not affected: an interrupt still ends its wait. A gax client
         * opened with {@link GrpcEventLoops#open(ClientFactory)} closes as {@link #close()} does:
         * its bounded shutdown answers an interrupt with {@code shutdownNow()}, which skips none of
         * its cleanup, and holding the interrupt back would only make the close wait out its
         * bounds.
         *
         * <p>For {@link PerCallClientRunner}: a client library can give up its own close at once on
         * an interrupted thread and skip the rest of its cleanup (Bigtable's data client does), and
         * the runner's clients are closed on threads Flink interrupts to stop them. An interrupt
         * delivered during the close still reaches it.
         */
        void closeWithInterruptHeldBack() {
            close(ownClose);
        }

        private void close(boolean holdInterrupt) {
            if (!closing.compareAndSet(false, true)) {
                try {
                    closed.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            boolean held = holdInterrupt && Thread.interrupted();
            try {
                // closeAll's contract: every step runs whatever an earlier one did, and the first
                // failure is the one reported, with a later one suppressed. The lease goes last,
                // after the held-back interrupt is restored, so that a release inside
                // interruptibleReleases() still sees it.
                Closers.closeAll(
                        clientClose,
                        channel,
                        () -> {
                            if (held) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        lease);
            } catch (Exception e) {
                // An Error leaves closeAll as itself and never reaches here.
                ExceptionUtils.rethrow(e);
            } finally {
                closed.countDown();
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
