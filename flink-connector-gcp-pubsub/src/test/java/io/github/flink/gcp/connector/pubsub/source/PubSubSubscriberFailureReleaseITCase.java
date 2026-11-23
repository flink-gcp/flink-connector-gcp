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

package io.github.flink.gcp.connector.pubsub.source;

import com.google.api.core.ApiService;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.flink.gcp.connector.testutils.Awaits.await;

/**
 * What the SDK subscriber releases when its streaming pull fails permanently — the empirical half
 * of what {@link
 * io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.StreamingPullSubscriber}'s
 * {@code startOrRelease} javadoc argues (#349).
 *
 * <p><b>What it measures is narrower than that argument, and the difference is worth naming.</b>
 * The start here succeeds; the failure arrives afterwards, from the streaming pull on a missing
 * subscription. So this exercises the connection-listener release mechanism the failed-start
 * argument rests on, not a start that throws; {@code StreamingPullSubscriberFailedStartTest}
 * measures a start whose stub creation throws (#2291), and the other start failures stay source
 * reading. And the observable is the executor alone: whether the stub's channel is released is
 * inferred from {@code runShutdown()} being one sequence, not seen.
 *
 * <p><b>What is under test here is the vendor's behaviour, not ours</b>, and it is here because a
 * javadoc in this repository asserts it. #325 measured that {@code stopAsync()} is a no-op on a
 * {@code FAILED} service and concluded that our failed-start guard therefore strands the channel
 * and executors the client had already opened; #349 asked for that leak to be confirmed empirically
 * rather than read. It does not happen. {@code Subscriber.startStreamingConnections()} adds to
 * every connection a listener whose {@code failed(...)} runs {@code runShutdown()} — {@code
 * stopAllStreamingConnections}, {@code shutdownBackgroundResources}, {@code
 * subscriberStub.shutdownNow()} — <em>before</em> it calls {@code notifyFailed}, so the release
 * happens on the SDK's own path and our guard has nothing left to recover.
 *
 * <p>The executor is the observable. It is one of the three things {@code runShutdown()} releases
 * and the only one a caller can hold a handle to without also taking ownership of it away from the
 * SDK: a {@code FixedTransportChannelProvider} would make {@code shouldAutoClose()} false and so
 * measure our ownership rather than the client's. Its shutdown is evidence that the whole of {@code
 * runShutdown()} ran, {@code subscriberStub.shutdownNow()} being the next line.
 *
 * <p>Using the emulator here is not the usual compromise: what is being measured is client-library
 * code, which is the same code against either endpoint, and all the emulator has to supply is a
 * {@code NOT_FOUND} the connection treats as permanent. A BOM bump that changes the release
 * sequence fails this test, which is the point of writing it down.
 */
class PubSubSubscriberFailureReleaseITCase extends AbstractPubSubSourceEmulatorITCase {

    /**
     * Bound on each wait. Each attempt runs three (the start, the failure and the release), and the
     * cleanup adds at most one {@link #CLEANUP_TIMEOUT} per attempt, so the worst case is 9 × 15 s
     * + 3 × 5 s = 150 s. {@link AbstractPubSubSourceEmulatorITCase}'s class timeout is 180 s
     * <em>per method</em>, so the sum has to stay inside it or a real regression is reported as a
     * method timeout with no diagnosis. The wait for {@code RUNNING} is bounded too: an untimed
     * {@code awaitRunning()} waits uninterruptibly, which a method timeout cannot end. The stub
     * creation that {@code startAsync()} runs on this thread before that wait starts is not. A
     * green run spends about 5 s in total.
     */
    private static final Duration FAILURE_TIMEOUT = Duration.ofSeconds(15);

    /** Bound on waiting out each subscriber the cleanup stops after a wait has failed. */
    private static final Duration CLEANUP_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Repeated because the claim is about a crash-looping job, which is many failed starts rather
     * than one: each attempt has to release what it opened, and each is checked as it ends.
     */
    private static final int ATTEMPTS = 3;

    @Test
    void aPermanentlyFailedSubscriberReleasesTheResourcesItOpened() throws Exception {
        // Recorded as they are created, not after the waits pass, so that a wait that times out —
        // exactly the regression this class exists to catch — still leaves the cleanup a handle
        // on every subscriber and executor in the shared integration-test fork.
        List<Subscriber> everySubscriber = new CopyOnWriteArrayList<>();
        List<ScheduledExecutorService> everyExecutor = new CopyOnWriteArrayList<>();
        Throwable failed = null;
        try {
            for (int i = 0; i < ATTEMPTS; i++) {
                int attempt = i;
                List<ScheduledExecutorService> executors = new CopyOnWriteArrayList<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();

                Subscriber subscriber =
                        subscriberOnAMissingSubscription(
                                recordingExecutorProvider(executors, everyExecutor), failure);
                everySubscriber.add(subscriber);
                subscriber
                        .startAsync()
                        .awaitRunning(FAILURE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                await(
                        "the streaming pull to fail on the missing subscription",
                        FAILURE_TIMEOUT,
                        () -> failure.get() != null,
                        () -> "attempt " + attempt + " saw no failure");

                // The connection listener runs runShutdown() before it notifies this listener, so
                // the executors should already be shut down when the failure arrives. This stays
                // an await so the test does not rest on that order, which holds for a stream that
                // dies after a successful start but not for a connection that fails to start
                // (ADR-0012). It also carries the "at least one" half: nothing removes from the
                // list, so a separate isNotEmpty assertion afterwards could not fail.
                await(
                        "the client to release the executors it opened",
                        FAILURE_TIMEOUT,
                        () ->
                                !executors.isEmpty()
                                        && executors.stream().allMatch(e -> e.isShutdown()),
                        () ->
                                "attempt "
                                        + attempt
                                        + " held "
                                        + executors.size()
                                        + " executors, of which "
                                        + executors.stream().filter(e -> e.isShutdown()).count()
                                        + " were shut down");

                // Our own guard, which #325 showed is a no-op on a FAILED service. Called anyway,
                // to pin that it neither helps nor hurts: the executors are already released.
                subscriber.stopAsync();
            }
        } catch (Throwable t) {
            failed = t;
            throw t;
        } finally {
            // A subscriber already FAILED is the outcome under test and has nothing to stop. One
            // still running when a wait expired is asked to stop and given a bounded wait, and a
            // stop that does not finish is reported rather than swallowed: beside the test's own
            // failure when there is one, as the failure otherwise. The executors go regardless.
            List<Exception> unstopped = new ArrayList<>();
            for (Subscriber subscriber : everySubscriber) {
                if (subscriber.state() == ApiService.State.FAILED) {
                    continue;
                }
                subscriber.stopAsync();
                try {
                    subscriber.awaitTerminated(CLEANUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException | IllegalStateException e) {
                    // The state is read once, after the wait. TERMINATED means the subscriber
                    // had stopped by the time cleanup read its state. Anything else is reported
                    // with the state it
                    // ended in, and a timeout that raced a failure also carries why it failed;
                    // the IllegalStateException a FAILED subscriber throws already has that cause.
                    ApiService.State ended = subscriber.state();
                    if (ended == ApiService.State.TERMINATED) {
                        continue;
                    }
                    IllegalStateException report =
                            new IllegalStateException("A subscriber ended the cleanup " + ended, e);
                    if (ended == ApiService.State.FAILED && e instanceof TimeoutException) {
                        report.addSuppressed(subscriber.failureCause());
                    }
                    unstopped.add(report);
                }
            }
            everyExecutor.forEach(ScheduledExecutorService::shutdownNow);
            if (!unstopped.isEmpty()) {
                if (failed != null) {
                    unstopped.forEach(failed::addSuppressed);
                } else {
                    AssertionError cleanup =
                            new AssertionError("A subscriber did not stop during cleanup");
                    unstopped.forEach(cleanup::addSuppressed);
                    throw cleanup;
                }
            }
        }
    }

    private static Subscriber subscriberOnAMissingSubscription(
            ExecutorProvider executorProvider, AtomicReference<Throwable> failure) {
        MessageReceiver ackEverything = (message, consumer) -> consumer.ack();
        Subscriber subscriber =
                Subscriber.newBuilder(
                                ProjectSubscriptionName.of(PROJECT, "no-such-subscription"),
                                ackEverything)
                        .setChannelProvider(
                                InstantiatingGrpcChannelProvider.newBuilder()
                                        .setEndpoint(emulatorEndpoint())
                                        .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                        .build())
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setExecutorProvider(executorProvider)
                        .build();
        subscriber.addListener(
                new com.google.api.core.ApiService.Listener() {
                    @Override
                    public void failed(
                            com.google.api.core.ApiService.State from, Throwable thrown) {
                        failure.compareAndSet(null, thrown);
                    }
                },
                Runnable::run);
        return subscriber;
    }

    /**
     * Hands out executors this test keeps a handle to, and declares them auto-closing so the client
     * takes ownership — which is exactly the ownership whose release is being measured.
     */
    private static ExecutorProvider recordingExecutorProvider(
            List<ScheduledExecutorService> opened, List<ScheduledExecutorService> everyOpened) {
        return new ExecutorProvider() {

            @Override
            public boolean shouldAutoClose() {
                return true;
            }

            @Override
            public ScheduledExecutorService getExecutor() {
                ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
                opened.add(executor);
                everyOpened.add(executor);
                return executor;
            }
        };
    }
}
