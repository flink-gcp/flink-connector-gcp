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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings mapping and the bounded teardown of {@link DefaultPublisherFactory}.
 *
 * <p>{@code @Timeout} because half of these drive a shutdown that never returns: a teardown that
 * stopped bounding it would hang the build rather than fail it.
 */
@Timeout(30)
class DefaultPublisherFactoryTest {

    private static final TopicDestination TOPIC = TopicDestination.of("test-project", "t");

    private static final BatchingSettings SDK_BATCHING_DEFAULTS =
            Publisher.Builder.getDefaultBatchingSettings();

    @Test
    void batchingSettingsOverlayOnlySetThresholdsOverSdkDefaults() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder().batchElementCountThreshold(5).build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getElementCountThreshold()).isEqualTo(5);
        assertThat(batching.getRequestByteThreshold())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getRequestByteThreshold());
        assertThat(batching.getDelayThresholdDuration())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getDelayThresholdDuration());
        assertThat(batching.getFlowControlSettings())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getFlowControlSettings());
    }

    @Test
    void batchingSettingsApplyByteAndDelayThresholds() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchRequestByteThreshold(2_048)
                        .batchDelayThreshold(Duration.ofMillis(20))
                        .build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getRequestByteThreshold()).isEqualTo(2_048);
        assertThat(batching.getDelayThresholdDuration()).isEqualTo(Duration.ofMillis(20));
        assertThat(batching.getElementCountThreshold())
                .isEqualTo(SDK_BATCHING_DEFAULTS.getElementCountThreshold());
    }

    @Test
    void batchingSettingsNeverEnableTheSdkFlowController() {
        // In-flight publishes are bounded by the writer, not by the SDK: it blocks the task thread
        // instead of yielding to the mailbox, and it leaks a permit per publish cancelled on a
        // paused ordering key. Leaving the settings at the SDK default of Ignore is what keeps
        // Publisher from constructing a controller at all.
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchRequestByteThreshold(4_096)
                        .maxInFlightBytes(1_024)
                        .build();

        BatchingSettings batching = DefaultPublisherFactory.batchingSettings(options);

        assertThat(batching.getFlowControlSettings().getLimitExceededBehavior())
                .isEqualTo(FlowController.LimitExceededBehavior.Ignore);
        // The writer cap is not an SDK knob, so it does not shrink the batch thresholds.
        assertThat(batching.getRequestByteThreshold()).isEqualTo(4_096);
    }

    @Test
    void retrySettingsOverlayOnlySetKnobsOverMirroredSdkDefaults() {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .retryInitialDelay(Duration.ofSeconds(1))
                        .retryMaxAttempts(7)
                        .build();

        RetrySettings retry = DefaultPublisherFactory.retrySettings(options);

        assertThat(retry.getInitialRetryDelayDuration()).isEqualTo(Duration.ofSeconds(1));
        assertThat(retry.getMaxAttempts()).isEqualTo(7);
        assertThat(retry.getTotalTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getTotalTimeoutDuration());
        assertThat(retry.getRetryDelayMultiplier())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getRetryDelayMultiplier());
        assertThat(retry.getMaxRetryDelayDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getMaxRetryDelayDuration());
        assertThat(retry.getInitialRpcTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS
                                .getInitialRpcTimeoutDuration());
        assertThat(retry.getRpcTimeoutMultiplier())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getRpcTimeoutMultiplier());
        assertThat(retry.getMaxRpcTimeoutDuration())
                .isEqualTo(
                        DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS.getMaxRpcTimeoutDuration());
    }

    /**
     * Pins the mirrored retry defaults to the SDK's own (package-private) {@code
     * Publisher.Builder.DEFAULT_RETRY_SETTINGS}, so an SDK upgrade changing publisher retry
     * defaults fails loudly here instead of silently diverging.
     */
    @Test
    void mirroredRetryDefaultsMatchTheSdk() throws Exception {
        Field defaults = Publisher.Builder.class.getDeclaredField("DEFAULT_RETRY_SETTINGS");
        defaults.setAccessible(true);
        RetrySettings sdkDefaults = (RetrySettings) defaults.get(null);

        assertThat(DefaultPublisherFactory.DEFAULT_RETRY_SETTINGS).isEqualTo(sdkDefaults);
    }

    /**
     * Wires the mapping into a real SDK publisher (a lazy plaintext channel to an unused port —
     * gRPC connects on first use, so build() succeeds offline) and asserts the settings took effect
     * on the built instance. (Ordering wiring is covered behaviorally by the emulator ITs, which
     * reuse {@code configure}; the SDK rejects ordered publishes unless the flag took effect.)
     */
    @Test
    void configureAppliesSettingsToABuiltPublisher() throws Exception {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchElementCountThreshold(5)
                        .batchDelayThreshold(Duration.ofMillis(20))
                        .build();
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
        Publisher publisher = null;
        try {
            Publisher.Builder builder =
                    Publisher.newBuilder("projects/test-project/topics/test-topic")
                            .setChannelProvider(
                                    FixedTransportChannelProvider.create(
                                            GrpcTransportChannel.create(channel)))
                            .setCredentialsProvider(NoCredentialsProvider.create());
            DefaultPublisherFactory.configure(builder, options);
            publisher = builder.build();

            assertThat(publisher.getBatchingSettings().getElementCountThreshold()).isEqualTo(5);
            assertThat(publisher.getBatchingSettings().getDelayThresholdDuration())
                    .isEqualTo(Duration.ofMillis(20));
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
            channel.shutdownNow();
        }
    }

    @Test
    void emulatorEndpointBuildsAndClosesPublisherOffline() throws Exception {
        // gRPC channels connect lazily, so building against an unreachable endpoint is safe
        // offline; the behavioral emulator coverage lives in the *ITCase classes, which all
        // publish through this factory's emulator mode.
        DefaultPublisherFactory factory =
                new DefaultPublisherFactory(
                        PubSubPublisherOptions.defaults(), EmulatorEndpoint.parse("localhost:1"));
        TopicPublisher publisher = factory.create(TopicDestination.of("test-project", "t"));
        publisher.close();
    }

    @Test
    void theConfiguredShutdownTimeoutReachesTheTeardown() throws Exception {
        DefaultPublisherFactory factory =
                new DefaultPublisherFactory(
                        PubSubPublisherOptions.builder()
                                .shutdownTimeout(Duration.ofSeconds(7))
                                .build(),
                        EmulatorEndpoint.parse("localhost:1"));

        TopicPublisher publisher = factory.create(TOPIC);
        try {
            assertThat(((DefaultPublisherFactory.PublisherAdapter) publisher).teardown.timeout())
                    .isEqualTo(Duration.ofSeconds(7));
        } finally {
            publisher.close();
        }
    }

    @Test
    void closeGivesUpOnAShutdownThatNeverReturnsAndStillReleasesTheChannel() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger terminationWaits = new AtomicInteger();
        AtomicReference<Thread> shutdownThread = new AtomicReference<>();
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> {
                            shutdownThread.set(Thread.currentThread());
                            awaitUninterruptibly(blocked);
                        },
                        (timeout, unit) -> {
                            terminationWaits.incrementAndGet();
                            return true;
                        },
                        TOPIC,
                        channel,
                        Duration.ofMillis(50));

        try {
            teardown.close();

            assertThat(channel.isShutdown()).isTrue();
            // Not reached, because it runs behind the shutdown on the same thread.
            assertThat(terminationWaits).hasValue(0);
            // The thread outlives the close by design, so a non-daemon one would keep the JVM from
            // exiting; its name is what identifies it in an operator's thread dump. Captured from
            // inside the shutdown rather than scanned for by name, since sibling tests here run in
            // the same JVM and name their thread after the same topic.
            assertThat(shutdownThread.get())
                    .isNotNull()
                    .matches(Thread::isDaemon, "a daemon thread")
                    .extracting(Thread::getName)
                    .asString()
                    // The caller's name is carried so a thread dump can say which subtask leaked;
                    // on a task thread that reads "... for Sink: Writer (2/4)#1".
                    .startsWith("pubsub-publisher-shutdown-" + TOPIC + " for ")
                    .endsWith(Thread.currentThread().getName());
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void theTerminationWaitGetsWhatTheShutdownLeftOfTheBudget() throws Exception {
        Duration timeout = Duration.ofSeconds(2);
        Duration spentByShutdown = Duration.ofMillis(300);
        AtomicLong awaitedNanos = new AtomicLong(-1);
        AtomicReference<Thread> awaitedOn = new AtomicReference<>();
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> sleepUninterruptibly(spentByShutdown),
                        (t, unit) -> {
                            awaitedNanos.set(unit.toNanos(t));
                            awaitedOn.set(Thread.currentThread());
                            return true;
                        },
                        TOPIC,
                        channel,
                        timeout);

        teardown.close();

        // One budget spans both steps: the wait gets what the shutdown left, never a fresh one.
        // Asserted strictly below timeout - spent, so handing over the full budget fails here.
        assertThat(awaitedNanos).hasValueBetween(1L, timeout.minus(spentByShutdown).toNanos() - 1);
        // And it runs on the shutdown thread, not this one. gax hands its full timeout to each
        // background resource in turn rather than sharing one deadline across them, so awaiting
        // here would cost a multiple of the budget instead of the budget.
        assertThat(awaitedOn.get())
                .isNotNull()
                .isNotSameAs(Thread.currentThread())
                .extracting(Thread::getName)
                .asString()
                .startsWith("pubsub-publisher-shutdown-" + TOPIC);
        assertThat(channel.isShutdown()).isTrue();
    }

    @Test
    void aShutdownThatThrowsIsRethrownByCloseAsItself() {
        // On a separate thread its exception would otherwise reach only Flink's JVM-wide uncaught
        // handler, so the writer would report a clean close — and under FAIL mode the whole
        // TaskManager would exit instead of the task. An Error must arrive as an Error, which is
        // the same reason Closers.closeAll does not wrap.
        Error failure = new NoClassDefFoundError("shutdown blew up");
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> {
                            throw failure;
                        },
                        (t, unit) -> true,
                        TOPIC,
                        channel,
                        Duration.ofSeconds(30));

        assertThatThrownBy(teardown::close).isSameAs(failure);
        assertThat(channel.isShutdown()).isTrue();
    }

    @Test
    void closeReleasesTheChannelWhenTheTerminationWaitThrows() {
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> {},
                        (timeout, unit) -> {
                            throw new IllegalStateException("termination wait blew up");
                        },
                        TOPIC,
                        channel,
                        Duration.ofSeconds(30));

        assertThatThrownBy(teardown::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("termination wait blew up");
        assertThat(channel.isShutdown()).isTrue();
    }

    @Test
    void theBudgetRunsFromTheShutdownCallRatherThanFromTheClose() throws Exception {
        // The property the writer's overlapped teardown rests on: it asks every publisher to shut
        // down and only then closes them, so a budget restarted here would be one timeout per
        // topic again rather than one for the whole close.
        Duration timeout = Duration.ofSeconds(1);
        CountDownLatch blocked = new CountDownLatch(1);
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        TOPIC,
                        channel,
                        timeout);

        try {
            teardown.start();
            Thread.sleep(timeout.toMillis() + 200);

            long startedAt = System.nanoTime();
            teardown.close();
            Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

            // Comfortably below the timeout a restarted budget would have waited out again.
            assertThat(waited).isLessThan(timeout.dividedBy(2));
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void anInterruptedCloseLeavesTheFlagSetForTheRestOfTheTeardown() throws Exception {
        // Closers.closeAll collects a failure and carries on, and Thread.join clears the flag when
        // it throws — so without restoring it the writer's remaining publishers, its topic admin
        // and its failure handler would all stop honouring the cancellation that interrupted us.
        CountDownLatch blocked = new CountDownLatch(1);
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        () -> awaitUninterruptibly(blocked),
                        (t, unit) -> true,
                        TOPIC,
                        channel,
                        Duration.ofMinutes(1));

        try {
            teardown.start();
            Thread.currentThread().interrupt();

            assertThatThrownBy(teardown::close).isInstanceOf(InterruptedException.class);

            // Consumes the flag as it reads it, so the assertion cannot pass on a stale one and the
            // rest of this test class is not left interrupted.
            assertThat(Thread.interrupted()).isTrue();
            assertThat(channel.isShutdown()).isTrue();
        } finally {
            blocked.countDown();
        }
    }

    @Test
    void startingTheShutdownTwiceDoesNotStartASecondThread() throws Exception {
        AtomicInteger shutdowns = new AtomicInteger();
        ManagedChannel channel = lazyChannel();
        DefaultPublisherFactory.BoundedShutdown teardown =
                new DefaultPublisherFactory.BoundedShutdown(
                        shutdowns::incrementAndGet,
                        (timeout, unit) -> true,
                        TOPIC,
                        channel,
                        Duration.ofSeconds(30));

        teardown.start();
        teardown.start();
        // close() implies start(), so a publisher closed without a preceding shutdown still runs
        // one — and one already started is not run again.
        teardown.close();

        assertThat(shutdowns).hasValue(1);
    }

    /** Connects lazily, so no test here needs anything listening. */
    private static ManagedChannel lazyChannel() {
        return ManagedChannelBuilder.forTarget("localhost:1").usePlaintext().build();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepUninterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
