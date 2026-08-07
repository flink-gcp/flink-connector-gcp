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

package io.github.flink.gcp.connector.pubsub.source;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the SDK subscriber releases when its streaming pull fails permanently — the empirical half
 * of what {@link
 * io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.PubSubNotifyingPullSubscriber}'s
 * {@code startOrRelease} javadoc argues (#349).
 *
 * <p><b>What it measures is narrower than that argument, and the difference is worth naming.</b>
 * The start here succeeds; the failure arrives afterwards, from the streaming pull on a missing
 * subscription. So this exercises the connection-listener release mechanism the failed-start
 * argument rests on, not a start that throws — that half stays source reading. And the observable
 * is the executor alone: whether the stub's channel is released is inferred from {@code
 * runShutdown()} being one sequence, not seen.
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
     * Bound on each wait. Six of them run in this one method, and {@link
     * AbstractPubSubSourceEmulatorITCase}'s class timeout is 180 s <em>per method</em>, so the sum
     * has to stay inside it or a real regression is reported as a method timeout with no diagnosis.
     * A green run spends about 5 s in total.
     */
    private static final Duration FAILURE_TIMEOUT = Duration.ofSeconds(20);

    /**
     * Repeated because the claim is about a crash-looping job, which is many failed starts rather
     * than one. Every executor from every attempt is kept and asserted together at the end, so this
     * measures accumulation and not just one release — a per-attempt assertion would have been
     * three copies of the same independent check.
     */
    private static final int ATTEMPTS = 3;

    @Test
    void aPermanentlyFailedSubscriberReleasesTheResourcesItOpened() throws Exception {
        List<ScheduledExecutorService> everyExecutor = new CopyOnWriteArrayList<>();
        try {
            for (int i = 0; i < ATTEMPTS; i++) {
                int attempt = i;
                List<ScheduledExecutorService> executors = new CopyOnWriteArrayList<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();

                Subscriber subscriber =
                        subscriberOnAMissingSubscription(
                                recordingExecutorProvider(executors), failure);
                subscriber.startAsync().awaitRunning();

                await(
                        "the streaming pull to fail on the missing subscription",
                        FAILURE_TIMEOUT,
                        () -> failure.get() != null,
                        () -> "attempt " + attempt + " saw no failure");

                // The release runs on the SDK's own thread, after the listener has been notified,
                // so this is an await rather than an assertion made the instant the failure
                // arrives. It also carries the "at least one" half: nothing removes from the list,
                // so a separate isNotEmpty assertion afterwards could not fail.
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

                everyExecutor.addAll(executors);
                // Our own guard, which #325 showed is a no-op on a FAILED service. Called anyway,
                // to pin that it neither helps nor hurts: the executors are already released.
                subscriber.stopAsync();
            }

            // The accumulation check, which is what makes the repetition worth its runtime: a
            // crash-looping job is many failed starts, and none of them may leave anything behind.
            assertThat(everyExecutor)
                    .as("every attempt opened an executor")
                    .hasSizeGreaterThanOrEqualTo(ATTEMPTS)
                    .allSatisfy(executor -> assertThat(executor.isShutdown()).isTrue());
        } finally {
            // Without this, an await that times out — which is exactly the regression this class
            // exists to catch — strands two threads per attempt in the shared integration-test
            // fork.
            everyExecutor.forEach(ScheduledExecutorService::shutdownNow);
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
            List<ScheduledExecutorService> opened) {
        return new ExecutorProvider() {

            @Override
            public boolean shouldAutoClose() {
                return true;
            }

            @Override
            public ScheduledExecutorService getExecutor() {
                ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
                opened.add(executor);
                return executor;
            }
        };
    }
}
