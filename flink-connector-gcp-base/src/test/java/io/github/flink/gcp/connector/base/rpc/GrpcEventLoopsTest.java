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

import org.apache.flink.util.UserCodeClassLoader;

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.github.flink.gcp.connector.testutils.FakeUserCodeClassLoader;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link GrpcEventLoops}.
 *
 * <p>The group is process-wide state, so every test closes every lease it opens in a {@code
 * finally}, and {@link #noLeaseIsLeftOpen()} checks that it did — a lease left open would keep the
 * group running into the next test and hide a release that never happened. A test that pins the
 * group runs its loader's release hooks in a {@code finally} too, since a pin left behind would
 * keep every later group running past its last lease.
 */
class GrpcEventLoopsTest {

    private static final long UNINTERRUPTIBLE_MILLIS =
            TimeUnit.SECONDS.toMillis(GrpcEventLoops.GROUP_SHUTDOWN_UNINTERRUPTIBLE_SECONDS);

    @BeforeEach
    void noGroupIsRunningBefore() {
        assertThat(GrpcEventLoops.isRunning())
                .as("a group was running before the test: an earlier class in this fork left it")
                .isFalse();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isPinned()).as("a pin was left registered").isFalse();
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
        // A task still running on a group thread is what a release that did not wait would leave
        // behind; the release must not return before it has finished.
        Release release = Release.outsideTheScope(false);
        try (release) {
            release.thread.join(300);
            release.assertWaiting(
                    "the release returned while a group thread was still running a task");
        }
        release.assertReturned();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void outsideTheScopeAnInterruptedReleaseStillWaitsForTheGroup() throws Exception {
        // A task thread Flink interrupts to cancel the task, releasing outside its close.
        Release release = Release.outsideTheScope(true);
        try (release) {
            release.thread.join(UNINTERRUPTIBLE_MILLIS + 500);
            release.assertWaiting(
                    "an interrupted release outside the scope stopped waiting for a busy group");
        }
        release.assertReturned();
        assertThat(release.interruptedAfter).as("the interrupt was kept").isTrue();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void inTheScopeAnInterruptBeforeTheReleaseEndsTheWaitAfterTheFirstSecond() throws Exception {
        try (Release release = Release.inTheScope(true)) {
            // An absolute bound, well inside the 10 s Flink's full lookup cache gives an
            // interrupted load, so it also holds the length of the part that ignores an interrupt.
            release.thread.join(TimeUnit.SECONDS.toMillis(5));
            release.assertReturned();
            // Measured on the releasing thread, so a slow test thread cannot fake it, and against
            // the documented first second rather than the constant, which a regression could zero.
            assertThat(release.elapsedNanos)
                    .as("the release waited through the first second despite the interrupt")
                    .isGreaterThanOrEqualTo(TimeUnit.SECONDS.toNanos(1));
            assertThat(release.interruptedAfter).as("the interrupt was restored").isTrue();
        }
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("the group finished on its own threads once its task returned")
                .isTrue();
    }

    @Test
    void inTheScopeAnInterruptDuringTheWaitEndsIt() throws Exception {
        try (Release release = Release.inTheScope(false)) {
            release.thread.join(UNINTERRUPTIBLE_MILLIS + 500);
            release.assertWaiting("the release returned before it was interrupted");

            release.thread.interrupt();
            release.thread.join(3000);
            release.assertReturned();
            assertThat(release.interruptedAfter).as("the interrupt was restored").isTrue();
        }
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void theScopeEndsWithItsBlockAndNests() {
        assertThat(GrpcEventLoops.inInterruptibleReleases()).isFalse();
        try (GrpcEventLoops.InterruptibleReleases outer = GrpcEventLoops.interruptibleReleases()) {
            try (GrpcEventLoops.InterruptibleReleases inner =
                    GrpcEventLoops.interruptibleReleases()) {
                assertThat(GrpcEventLoops.inInterruptibleReleases()).isTrue();
            }
            assertThat(GrpcEventLoops.inInterruptibleReleases())
                    .as("closing a nested scope ended the outer one")
                    .isTrue();
        }
        assertThat(GrpcEventLoops.inInterruptibleReleases()).isFalse();
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
    void aPinnedGroupOutlivesItsLastLeaseUntilTheLoaderIsReleased() throws Exception {
        FakeUserCodeClassLoader loader = jobLoader();
        GrpcEventLoops.pin(loader);
        try {
            GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
            try {
                // Starts one of the group's threads, which Netty starts only on demand.
                GrpcEventLoops.executeOnGroup(() -> {});
            } finally {
                lease.close();
            }

            assertThat(GrpcEventLoops.isRunning())
                    .as("the release of the last lease stopped a pinned group")
                    .isTrue();
            assertThat(GrpcEventLoopThreads.alive()).isTrue();
            assertThat(loader.releaseHookNames()).containsExactly(GrpcEventLoops.RELEASE_HOOK_NAME);
        } finally {
            loader.release();
        }

        assertThat(GrpcEventLoops.isRunning()).isFalse();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void eachLoopThatStartsWhileItsGroupRunsIsCounted() throws Exception {
        long before = GrpcEventLoops.loopsStartedWhileRunning();
        CountDownLatch ran = new CountDownLatch(2);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try {
            // The group hands tasks to its loops in turn, so with Netty's default of two or more
            // loops the two tasks start two of them; each loop that ran a task started one thread.
            for (int i = 0; i < 2; i++) {
                GrpcEventLoops.executeOnGroup(
                        () -> {
                            threads.add(Thread.currentThread());
                            ran.countDown();
                        });
            }
            assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(threads).isNotEmpty();
            assertThat(GrpcEventLoops.loopsStartedWhileRunning() - before)
                    .as("threads the tasks ran on: %s", threads)
                    .isEqualTo(threads.size());
        } finally {
            lease.close();
        }
    }

    @Test
    void theLoopsTheLastReleaseStartsToStopThemAreNotCounted() throws Exception {
        long before = GrpcEventLoops.loopsStartedWhileRunning();

        // No task runs under the lease, so every loop the group has first starts in its shutdown.
        GrpcEventLoops.acquire().close();

        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
        assertThat(GrpcEventLoops.loopsStartedWhileRunning() - before).isZero();
    }

    @Test
    void theLoopsTheReleaseHookStartsToStopThemAreNotCounted() throws Exception {
        FakeUserCodeClassLoader loader = jobLoader();
        GrpcEventLoops.pin(loader);
        long before = GrpcEventLoops.loopsStartedWhileRunning();
        try {
            // The pin keeps the group running with none of its loops started.
            GrpcEventLoops.acquire().close();
            assertThat(GrpcEventLoops.isRunning()).isTrue();
        } finally {
            loader.release();
        }

        assertThat(GrpcEventLoops.isRunning()).isFalse();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
        assertThat(GrpcEventLoops.loopsStartedWhileRunning() - before).isZero();
    }

    @Test
    void aLoopAnEarlierGroupStartsWhileALaterOneRunsIsNotCounted() throws Exception {
        CountDownLatch detached = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicReference<Throwable> releaseFailure = new AtomicReference<>();
        GrpcEventLoops.Lease first = GrpcEventLoops.acquire();
        Thread releasing =
                new Thread(
                        () -> {
                            try {
                                first.close();
                            } catch (Throwable t) {
                                releaseFailure.set(t);
                            }
                        },
                        "release-first");
        GrpcEventLoops.beforeShutdown = () -> awaitQuietly(detached, proceed);
        try {
            releasing.start();
            assertThat(detached.await(10, TimeUnit.SECONDS)).isTrue();
            GrpcEventLoops.beforeShutdown = null;

            // The first group is detached but not yet shut down, and none of its loops has run.
            // Its shutdown starts them all while the second group runs.
            GrpcEventLoops.Lease second = GrpcEventLoops.acquire();
            try {
                long before = GrpcEventLoops.loopsStartedWhileRunning();
                proceed.countDown();
                releasing.join(TimeUnit.SECONDS.toMillis(15));
                assertThat(releasing.isAlive()).as("the first release is still waiting").isFalse();
                assertThat(releaseFailure.get()).as("the first release failed").isNull();

                assertThat(GrpcEventLoops.loopsStartedWhileRunning() - before)
                        .as("loops the first group's shutdown started, counted for the second")
                        .isZero();
            } finally {
                second.close();
            }
        } finally {
            GrpcEventLoops.beforeShutdown = null;
            proceed.countDown();
            releasing.join(TimeUnit.SECONDS.toMillis(15));
        }
    }

    @Test
    void theReleaseHookWaitsForTheGroupWhateverTheInterruptSays() throws Exception {
        FakeUserCodeClassLoader loader = jobLoader();
        GrpcEventLoops.pin(loader);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread releasing =
                new Thread(
                        () -> {
                            loader.release();
                            interruptedAfter.set(Thread.currentThread().isInterrupted());
                        },
                        "loader-release");
        try {
            CountDownLatch running = new CountDownLatch(1);
            GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
            try {
                GrpcEventLoops.executeOnGroup(() -> awaitQuietly(running, finish));
                assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                lease.close();
            }

            releasing.start();
            releasing.interrupt();
            releasing.join(UNINTERRUPTIBLE_MILLIS + 500);
            assertThat(loader.releaseHookNames()).as("the loader's release began").isEmpty();
            assertThat(releasing.isAlive())
                    .as("the hook returned while a group thread was still running a task")
                    .isTrue();
        } finally {
            finish.countDown();
            if (releasing.getState() == Thread.State.NEW) {
                loader.release();
            }
            releasing.join(TimeUnit.SECONDS.toMillis(15));
        }

        assertThat(releasing.isAlive()).as("the hook is still waiting").isFalse();
        assertThat(interruptedAfter).as("the interrupt was kept").isTrue();
        assertThat(GrpcEventLoops.isRunning()).isFalse();
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void pinningAgainRegistersNoSecondHook() {
        FakeUserCodeClassLoader first = jobLoader();
        FakeUserCodeClassLoader second = jobLoader();
        GrpcEventLoops.pin(first);
        try {
            GrpcEventLoops.pin(second);

            assertThat(first.releaseHookNames()).containsExactly(GrpcEventLoops.RELEASE_HOOK_NAME);
            assertThat(second.releaseHookNames()).isEmpty();
        } finally {
            // Ran with no group ever started: it only unpins.
            first.release();
        }
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void aHookRunUnderAnOpenLeaseStopsTheGroupAndReturnsItToTheLeases() {
        FakeUserCodeClassLoader loader = jobLoader();
        GrpcEventLoops.pin(loader);
        GrpcEventLoops.Lease stranded = GrpcEventLoops.acquire();
        try (LogCapture log = LogCapture.of(GrpcEventLoops.class)) {
            loader.release();

            assertThat(GrpcEventLoops.isRunning())
                    .as("the group outlived the release of the job's class loader")
                    .isFalse();
            assertThat(log.getMessages())
                    .singleElement(as(InstanceOfAssertFactories.STRING))
                    .contains("while 1 gRPC event-loop lease(s) are open");

            // A task Flink is still cancelling opens a client after the hook, while the stranded
            // lease is still open: the new group must stop with the new lease alone.
            GrpcEventLoops.Lease later = GrpcEventLoops.acquire();
            try {
                assertThat(GrpcEventLoops.isRunning()).isTrue();
            } finally {
                later.close();
            }
            assertThat(GrpcEventLoops.isRunning())
                    .as("a lease opened before the hook kept a later group running")
                    .isFalse();

            // A later writer's init context, which wraps the same loader in a new instance.
            FakeUserCodeClassLoader again = new FakeUserCodeClassLoader(loader.asClassLoader());
            GrpcEventLoops.pin(again);
            assertThat(GrpcEventLoops.isPinned())
                    .as("pinned again to a loader whose hook has run, which Flink never reruns")
                    .isFalse();
            assertThat(again.releaseHookNames()).isEmpty();
            again.release();
        } finally {
            stranded.close();
            loader.release();
        }
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void aLeaseStrandedByTheHookDoesNotStopALaterGroupWhenItClosesFirst() {
        FakeUserCodeClassLoader loader = jobLoader();
        GrpcEventLoops.pin(loader);
        GrpcEventLoops.Lease stranded = GrpcEventLoops.acquire();
        GrpcEventLoops.Lease later = null;
        try {
            loader.release();
            later = GrpcEventLoops.acquire();

            stranded.close();

            assertThat(GrpcEventLoops.isRunning())
                    .as("a lease opened before the hook stopped a group a later lease holds")
                    .isTrue();
        } finally {
            stranded.close();
            if (later != null) {
                later.close();
            }
            loader.release();
        }
        assertThat(GrpcEventLoops.isRunning()).isFalse();
    }

    @Test
    void pinningToALoaderThatDidNotLoadTheClassDoesNothing() {
        // Flink's loader for a job that ships no JAR: the connector comes from its parent.
        FakeUserCodeClassLoader loader = jobLoader();
        try {
            GrpcEventLoops.pinTo(loader);

            assertThat(loader.releaseHookNames()).isEmpty();
            GrpcEventLoops.acquire().close();
            assertThat(GrpcEventLoops.isRunning()).isFalse();
        } finally {
            loader.release();
        }
    }

    @Test
    void aLoaderThatCannotBeInspectedLeavesTheWriterUnpinned() {
        // As under a security manager that refuses getClassLoader, or a wrapper whose own class
        // cannot link: the pin is optional, so the writer's creation must not fail on it.
        for (Throwable failure :
                List.of(
                        new SecurityException("getClassLoader refused"),
                        new NoClassDefFoundError("a class the wrapper needs"))) {
            UserCodeClassLoader refusing =
                    new UserCodeClassLoader() {
                        @Override
                        public ClassLoader asClassLoader() {
                            if (failure instanceof Error) {
                                throw (Error) failure;
                            }
                            throw (RuntimeException) failure;
                        }

                        @Override
                        public void registerReleaseHookIfAbsent(String name, Runnable hook) {
                            throw new AssertionError("registered a hook on an uninspected loader");
                        }
                    };

            GrpcEventLoops.pinTo(refusing);

            assertThat(GrpcEventLoops.isPinned()).as("pinned after %s", failure).isFalse();
        }
    }

    @Test
    void pinningToTheLoaderThatDefinedTheClassRegistersItsHook() throws Exception {
        // A copy of the class defined by its own job loader, as when the connector ships in the
        // job's JAR; pinTo is called on the copy, which checks its own class.
        String name = GrpcEventLoops.class.getName();
        CopyingClassLoader job =
                new CopyingClassLoader(GrpcEventLoops.class.getClassLoader(), name);
        Class<?> copy = job.loadClass(name);
        FakeUserCodeClassLoader loader = new FakeUserCodeClassLoader(job);
        // Answers asClassLoader() once: the loader the ownership check read is the one pinned.
        AtomicInteger reads = new AtomicInteger();
        UserCodeClassLoader readOnce =
                new UserCodeClassLoader() {
                    @Override
                    public ClassLoader asClassLoader() {
                        if (reads.getAndIncrement() > 0) {
                            throw new IllegalStateException("read the loader twice");
                        }
                        return job;
                    }

                    @Override
                    public void registerReleaseHookIfAbsent(String hookName, Runnable hook) {
                        loader.registerReleaseHookIfAbsent(hookName, hook);
                    }
                };
        Method isPinned = copy.getDeclaredMethod("isPinned");
        isPinned.setAccessible(true);
        try {
            copy.getMethod("pinTo", UserCodeClassLoader.class).invoke(null, readOnce);

            assertThat(loader.releaseHookNames()).containsExactly(name);
            assertThat(isPinned.invoke(null)).isEqualTo(true);
        } finally {
            loader.release();
        }
        assertThat(isPinned.invoke(null)).as("the hook unpinned the copy").isEqualTo(false);
        assertThat(GrpcEventLoops.isPinned()).as("the pin reached the other copy").isFalse();
    }

    @Test
    void aClassIsTheJobsOnlyWhenTheJobsLoaderDefinedIt() throws Exception {
        ClassLoader shared = GrpcEventLoops.class.getClassLoader();
        ClassLoader system = ClassLoader.getSystemClassLoader();
        String name = GrpcEventLoops.class.getName();
        CopyingClassLoader job = new CopyingClassLoader(shared, name);
        Class<?> copy = job.loadClass(name);
        assertThat(copy).isNotSameAs(GrpcEventLoops.class);
        // Flink's leak-check wrapper: a sibling of the defining loader that delegates to it.
        ClassLoader wrapper =
                new ClassLoader(shared) {
                    @Override
                    protected Class<?> loadClass(String className, boolean resolve)
                            throws ClassNotFoundException {
                        return job.loadClass(className);
                    }
                };
        ClassLoader jobWithoutJar = new URLClassLoader(new URL[0], shared);

        assertThat(GrpcEventLoops.isLoadedByJob(copy, job, system))
                .as("the job's own loader")
                .isTrue();
        assertThat(GrpcEventLoops.isLoadedByJob(copy, wrapper, system))
                .as("the wrapper Flink hands out")
                .isTrue();
        assertThat(GrpcEventLoops.isLoadedByJob(GrpcEventLoops.class, jobWithoutJar, system))
                .as("loaded by the job loader's parent, as from lib/")
                .isFalse();
        // The same with a parent that is not the system class loader, as Flink's own can be.
        assertThat(GrpcEventLoops.isLoadedByJob(copy, new URLClassLoader(new URL[0], job), system))
                .as("loaded by the job loader's parent, which is not the system class loader")
                .isFalse();
        assertThat(GrpcEventLoops.isLoadedByJob(copy, job, job))
                .as("the system class loader, which Flink never closes")
                .isFalse();
        assertThat(GrpcEventLoops.isLoadedByJob(copy, jobWithoutJar, system))
                .as("a loader that resolves the name to another copy")
                .isFalse();
        // A parent outside the defining loader's chain that delegates to it, as an embedding
        // application can arrange: every job with that parent shares the copy.
        ClassLoader delegatingParent =
                new ClassLoader(shared) {
                    @Override
                    protected Class<?> loadClass(String className, boolean resolve)
                            throws ClassNotFoundException {
                        return className.equals(name)
                                ? job.loadClass(className)
                                : super.loadClass(className, resolve);
                    }
                };
        assertThat(
                        GrpcEventLoops.isLoadedByJob(
                                copy, new URLClassLoader(new URL[0], delegatingParent), system))
                .as("a parent that delegates to the defining loader")
                .isFalse();
        // Flink's leak-check wrapper after its loader has closed refuses every lookup.
        ClassLoader closedWrapper =
                new ClassLoader(shared) {
                    @Override
                    protected Class<?> loadClass(String className, boolean resolve) {
                        throw new IllegalStateException("Trying to access closed classloader.");
                    }
                };
        assertThat(GrpcEventLoops.isLoadedByJob(copy, closedWrapper, system))
                .as("a job loader that has closed")
                .isFalse();
        // A parent whose own copy fails to load, through a sealing violation or a linkage
        // failure, while the job's loader defines its copy: the answer is open, so no pin.
        for (RuntimeException failure :
                List.of(new SecurityException("sealing violation"), new RuntimeException())) {
            assertThat(isLoadedUnderAFailingParent(shared, name, failure, system))
                    .as("a parent whose copy fails with %s", failure)
                    .isFalse();
        }
        assertThat(isLoadedUnderAFailingParent(shared, name, null, system))
                .as("a parent whose copy fails with a LinkageError")
                .isFalse();
        assertThat(GrpcEventLoops.isLoadedByJob(String.class, job, system))
                .as("the bootstrap loader")
                .isFalse();
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
    @SuppressWarnings("rawtypes")
    void aStandaloneConfiguratorAppliesTheExistingOneFirstAndThenTheGroup() throws Exception {
        List<String> calls = new ArrayList<>();
        // An existing configurator that sets an event-loop group of its own: applied after the
        // group's, it would win, and the channel would run on its threads instead.
        EventLoopGroup foreign =
                new MultiThreadIoEventLoopGroup(
                        1,
                        new DefaultThreadFactory("foreign-elg", true),
                        NioIoHandler.newFactory());
        // Built before any lease is open, as a client's settings are.
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                GrpcEventLoops.channelConfigurator(
                        builder -> {
                            calls.add("existing");
                            return ((NettyChannelBuilder) builder.usePlaintext())
                                    .eventLoopGroup(foreign)
                                    .channelType(NioSocketChannel.class);
                        });
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (ServerSocket server = new ServerSocket(0)) {
            ManagedChannel channel =
                    configurator
                            .apply(
                                    NettyChannelBuilder.forAddress(
                                            "127.0.0.1", server.getLocalPort()))
                            .build();
            try {
                channel.getState(true);
                assertThat(calls).containsExactly("existing");
                assertThat(GrpcEventLoopThreads.awaitAlive())
                        .as("a thread named %s* while the channel connects", prefix())
                        .isTrue();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        } finally {
            lease.close();
            foreign.shutdownGracefully(0, 5, TimeUnit.SECONDS)
                    .awaitUninterruptibly(10, TimeUnit.SECONDS);
        }
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    @SuppressWarnings("rawtypes")
    void aStandaloneConfiguratorWithoutAnExistingOneAcceptsTheBuilderAsItIs() {
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                GrpcEventLoops.channelConfigurator(null);
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget("localhost:1");

        assertThat(configurator.apply(builder)).isSameAs(builder);
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
    void aLeasedClientCancelsAClientThatDoesNotStopAndStillReleases() throws Exception {
        FakeClient client = new FakeClient(Termination.TIMES_OUT);

        GrpcEventLoops.open(lease -> client).close();

        assertThat(client.calls)
                .containsExactly("shutdown", "awaitTermination", "shutdownNow", "awaitTermination");
    }

    @Test
    void aLeasedClientRestoresAnInterruptAndStillReleases() throws Exception {
        FakeClient client = new FakeClient(Termination.INTERRUPTED);
        GrpcEventLoops.LeasedClient<FakeClient> leased = GrpcEventLoops.open(lease -> client);
        try {
            leased.close();

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clears the flag, so the interrupt does not leak into the next test.
            Thread.interrupted();
        }
        assertThat(client.calls).containsExactly("shutdown", "awaitTermination", "shutdownNow");
    }

    /**
     * An interrupt that ended the wait comes back even when the forced shutdown that answers it
     * fails, and the lease is still released.
     */
    @Test
    void aLeasedClientRestoresAnInterruptEvenWhenShutdownNowThrows() throws Exception {
        FakeClient client = new FakeClient(Termination.INTERRUPTED_THEN_SHUTDOWN_NOW_THROWS);
        GrpcEventLoops.LeasedClient<FakeClient> leased = GrpcEventLoops.open(lease -> client);
        try {
            assertThatThrownBy(leased::close).hasMessage("shutdownNow failed");

            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt was restored")
                    .isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(client.calls).containsExactly("shutdown", "awaitTermination", "shutdownNow");
    }

    @Test
    void aLeasedClientReleasesEvenWhenShutdownThrows() throws Exception {
        GrpcEventLoops.LeasedClient<FakeClient> leased =
                GrpcEventLoops.open(lease -> new FakeClient(Termination.SHUTDOWN_THROWS));

        assertThatThrownBy(leased::close).isInstanceOf(IllegalStateException.class);
    }

    /**
     * The held-back close holds nothing back for a gax client: its bounded shutdown answers the
     * interrupt with {@code shutdownNow()}, which skips none of its cleanup, rather than waiting
     * out its bounds.
     */
    @Test
    void aHeldBackCloseLetsAGaxClientAnswerTheInterrupt() throws Exception {
        FakeClient client = new FakeClient(Termination.INTERRUPTIBLE);
        GrpcEventLoops.LeasedClient<FakeClient> leased = GrpcEventLoops.open(lease -> client);
        Thread.currentThread().interrupt();
        try {
            leased.closeWithInterruptHeldBack();

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        // The fake's wait throws on an interrupted thread only, so the interrupt reached it.
        assertThat(client.calls).containsExactly("shutdown", "awaitTermination", "shutdownNow");
    }

    /**
     * The release of the group's only lease on a thread of its own, started while a group task
     * holds its thread. Closing it lets the task finish and waits for the release.
     */
    private static final class Release implements AutoCloseable {

        private final CountDownLatch finish = new CountDownLatch(1);
        private final Thread thread;
        private volatile boolean interruptedAfter;
        private volatile long elapsedNanos;
        @Nullable private volatile Throwable thrown;

        /** Releases outside the scope, on a thread interrupted first if {@code interruptFirst}. */
        static Release outsideTheScope(boolean interruptFirst) throws InterruptedException {
            return start(interruptFirst, false);
        }

        /** Releases inside the scope, on a thread interrupted first if {@code interruptFirst}. */
        static Release inTheScope(boolean interruptFirst) throws InterruptedException {
            return start(interruptFirst, true);
        }

        private static Release start(boolean interruptFirst, boolean inScope)
                throws InterruptedException {
            GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
            Release release = new Release(lease, interruptFirst, inScope);
            CountDownLatch running = new CountDownLatch(1);
            try {
                GrpcEventLoops.executeOnGroup(() -> awaitQuietly(running, release.finish));
                assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (Throwable e) {
                release.finish.countDown();
                lease.close();
                throw e;
            }
            release.thread.start();
            return release;
        }

        private Release(GrpcEventLoops.Lease lease, boolean interruptFirst, boolean inScope) {
            this.thread =
                    new Thread(
                            () -> {
                                try (GrpcEventLoops.InterruptibleReleases scope =
                                        inScope ? GrpcEventLoops.interruptibleReleases() : null) {
                                    if (interruptFirst) {
                                        Thread.currentThread().interrupt();
                                    }
                                    long start = System.nanoTime();
                                    lease.close();
                                    elapsedNanos = System.nanoTime() - start;
                                    interruptedAfter = Thread.currentThread().isInterrupted();
                                } catch (Throwable t) {
                                    thrown = t;
                                }
                            },
                            "lease-release");
        }

        /** Asserts that the release is still waiting, naming what it threw if it is not. */
        private void assertWaiting(String failure) {
            assertThat(thrown).as("the release threw").isNull();
            assertThat(thread.isAlive()).as(failure).isTrue();
        }

        /** Asserts that the release returned rather than threw or kept waiting. */
        private void assertReturned() {
            assertThat(thrown).as("the release threw").isNull();
            assertThat(thread.isAlive()).as("the release is still waiting").isFalse();
        }

        @Override
        public void close() throws InterruptedException {
            finish.countDown();
            thread.join(TimeUnit.SECONDS.toMillis(15));
        }
    }

    /** A loader Flink gives a job that ships no JAR: an empty child of the connector's loader. */
    private static FakeUserCodeClassLoader jobLoader() {
        return new FakeUserCodeClassLoader(
                new URLClassLoader(new URL[0], GrpcEventLoops.class.getClassLoader()));
    }

    /**
     * Asks whether a copy of the class that a child loader defines is the job's, under a parent
     * whose own lookup of that class throws {@code failure}, or a {@link LinkageError} if it is
     * {@code null}. The child is both the defining loader and the job's, so only the parent's
     * failure can make the answer {@code false}.
     */
    private static boolean isLoadedUnderAFailingParent(
            ClassLoader shared, String name, @Nullable RuntimeException failure, ClassLoader system)
            throws ClassNotFoundException {
        ClassLoader failingParent =
                new ClassLoader(shared) {
                    @Override
                    protected Class<?> loadClass(String className, boolean resolve)
                            throws ClassNotFoundException {
                        if (className.equals(name)) {
                            if (failure == null) {
                                throw new LinkageError("cannot link the parent's copy");
                            }
                            throw failure;
                        }
                        return super.loadClass(className, resolve);
                    }
                };
        CopyingClassLoader job = new CopyingClassLoader(failingParent, name);
        return GrpcEventLoops.isLoadedByJob(job.loadClass(name), job, system);
    }

    /** A loader that defines its own copy of one class and delegates every other to its parent. */
    private static final class CopyingClassLoader extends ClassLoader {

        private final String copied;

        private CopyingClassLoader(ClassLoader parent, String copied) {
            super(parent);
            this.copied = copied;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(copied)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded != null) {
                    return loaded;
                }
                try (InputStream in =
                        getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                    if (in == null) {
                        throw new ClassNotFoundException(name);
                    }
                    byte[] bytes = in.readAllBytes();
                    return defineClass(name, bytes, 0, bytes.length);
                } catch (IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
        }
    }

    /** A group task: signals that it runs, then holds its thread until {@code finish} opens. */
    private static void awaitQuietly(CountDownLatch running, CountDownLatch finish) {
        running.countDown();
        try {
            finish.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        /** Terminates, unless the waiting thread is interrupted, which the wait then throws on. */
        INTERRUPTIBLE,
        /** Throws an interrupt from the wait, and then fails the forced shutdown. */
        INTERRUPTED_THEN_SHUTDOWN_NOW_THROWS,
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
            if (termination == Termination.INTERRUPTED_THEN_SHUTDOWN_NOW_THROWS) {
                throw new IllegalStateException("shutdownNow failed");
            }
        }

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
            record("awaitTermination");
            if (termination == Termination.INTERRUPTED
                    || termination == Termination.INTERRUPTED_THEN_SHUTDOWN_NOW_THROWS
                    || (termination == Termination.INTERRUPTIBLE && Thread.interrupted())) {
                throw new InterruptedException();
            }
            return termination == Termination.TERMINATES
                    || termination == Termination.INTERRUPTIBLE;
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
