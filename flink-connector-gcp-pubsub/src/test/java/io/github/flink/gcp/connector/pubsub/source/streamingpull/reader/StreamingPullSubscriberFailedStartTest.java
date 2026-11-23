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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What a failed stub creation leaves behind, measured through the production factory (#2291).
 *
 * <p>{@code Subscriber.Builder.build()} creates the system executor, and the {@code Subscriber}
 * constructor registers it for shutdown before {@code doStart()} runs; only {@code runShutdown()}
 * shuts it down. When {@code GrpcSubscriberStub.create} throws, no connection exists, so no
 * connection listener runs {@code runShutdown()}, and {@code stopAsync()} enters nothing on the
 * {@code FAILED} service. So the executor is never shut down on this path. What this test pins, for
 * a throw from {@code ClientContext.create}, is the reason that costs nothing: the executor has run
 * no task and therefore started no thread, so it is an unreachable object rather than a stranded
 * thread pool. A throw later in the stub's construction would find the stream watchdog already
 * scheduled; ADR-0012 records that case as read, not measured.
 *
 * <p>Like {@code PubSubSubscriberFailureReleaseITCase}, this tests the vendor's behaviour on
 * purpose, because ADR-0012 and {@code StreamingPullSubscriber.startOrRelease}'s javadoc assert it.
 * A BOM bump that makes {@code ClientContext.create} schedule work before it can fail, or that
 * changes the field read here, fails this test.
 *
 * <p>The failure is a credentials provider that throws, which {@code ClientContext.create} calls
 * before it builds any channel, so no endpoint is contacted and no Application Default Credentials
 * are read. The transport provider and the stub settings are still built as in production, so on a
 * host configured for mTLS client certificates both run the ambient certificate discovery, which
 * may open the configured certificate and key or launch a configured certificate helper, as {@code
 * DefaultSubscriberFactoryTest}'s production-transport test already does. A lookup failure that
 * escapes those builds fails this test before the measurement; one they absorb does not. One failed
 * start is enough: each builds its own executor, and nothing carries over between starts for a
 * second one to accumulate.
 *
 * <p>The same start also pins {@code DefaultSubscriberFactory.SubscriberAdapter.start}'s wiring.
 * The SDK fails the service inside {@code startAsync()}, and Guava hands that failure, on the
 * calling thread, to the listeners registered at that moment; a listener added afterwards is never
 * told. So the adapter reporting the failure itself, once, and the reader's wake-up firing once on
 * this thread, show that the listener was registered before the start, passes the cause on, and
 * runs on a direct executor.
 */
@Timeout(30)
class StreamingPullSubscriberFailedStartTest {

    private static final SubscriptionDestination SUBSCRIPTION =
            SubscriptionDestination.of("project", "orders");

    @Test
    void aStubThatCannotBeCreatedLeavesItsSystemExecutorWithoutAThread() throws Exception {
        IOException noCredentials = new IOException("no credentials");
        CredentialsProvider failingCredentials =
                () -> {
                    throw noCredentials;
                };
        DefaultSubscriberFactory production =
                new DefaultSubscriberFactory(
                        PubSubSubscriberOptions.builder().build(),
                        OrderingMode.NONE,
                        null,
                        failingCredentials);
        // Recorded as soon as the factory returns, so cleanup never depends on the assertions.
        List<SubscriberClient> clients = new ArrayList<>();
        List<ScheduledThreadPoolExecutor> executors = new ArrayList<>();
        List<Throwable> reported = new ArrayList<>();
        SubscriberFactory recording =
                (subscription, consumer) -> {
                    SubscriberClient client = production.create(subscription, consumer);
                    clients.add(client);
                    executors.add(systemExecutorOf(client));
                    return reportingTo(client, reported);
                };
        List<Thread> wakeUps = new ArrayList<>();
        AtomicReference<StreamingPullSubscriber> started = new AtomicReference<>();
        try {
            Throwable thrown =
                    catchThrowable(
                            () ->
                                    started.set(
                                            new StreamingPullSubscriber(
                                                    "0",
                                                    SUBSCRIPTION,
                                                    recording,
                                                    new RecordingAckTracker(new ArrayList<>()),
                                                    () -> wakeUps.add(Thread.currentThread()),
                                                    Duration.ofSeconds(1),
                                                    SubscriberBufferBudget.unbounded())));
            assertThat(thrown)
                    .as("the failed stub creation fails the start, not the build")
                    .isInstanceOf(IOException.class)
                    .hasMessageStartingWith("Failed to start")
                    .rootCause()
                    .isSameAs(noCredentials);
            assertThat(reported).as("failures the adapter reported").hasSize(1);
            // The very object the SDK failed with, not a wrapper: the reader's re-report check
            // compares by identity.
            assertThat(reported.get(0))
                    .as("the failure the adapter reported")
                    .isSameAs(
                            ((DefaultSubscriberFactory.SubscriberAdapter) clients.get(0))
                                    .subscriber()
                                    .failureCause());
            assertThat(reported.get(0)).rootCause().isSameAs(noCredentials);
            assertThat(wakeUps)
                    .as("threads that woke the reader for the failure")
                    .containsExactly(Thread.currentThread());
            assertThat(executors).as("the factory built one subscriber").hasSize(1);
            ScheduledThreadPoolExecutor executor = executors.get(0);

            assertThat(executor.isShutdown())
                    .as("the failed start shut the executor down")
                    .isFalse();
            assertThat(executor.getTaskCount()).as("tasks the executor was given").isZero();
            assertThat(executor.getLargestPoolSize())
                    .as("threads the executor ever started")
                    .isZero();
        } finally {
            // A start that unexpectedly succeeded is stopped and waited out before its transport
            // goes; the failed start has already closed the client, and closing again is a no-op.
            if (started.get() != null) {
                started.get().close();
            }
            clients.forEach(SubscriberClient::close);
            executors.forEach(ScheduledThreadPoolExecutor::shutdownNow);
        }
    }

    /** Passes every call through, recording each failure the client reports to its listener. */
    private static SubscriberClient reportingTo(SubscriberClient client, List<Throwable> reported) {
        return new SubscriberClient() {
            @Override
            public void start(Consumer<Throwable> onPermanentFailure) {
                client.start(
                        failure -> {
                            reported.add(failure);
                            onPermanentFailure.accept(failure);
                        });
            }

            @Override
            public void stopAsync() {
                client.stopAsync();
            }

            @Override
            public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
                client.awaitTerminated(timeout, unit);
            }

            @Override
            public void close() {
                client.close();
            }
        };
    }

    /**
     * The executor {@code Subscriber.Builder.build()} creates when none is set, as the SDK holds.
     */
    private static ScheduledThreadPoolExecutor systemExecutorOf(SubscriberClient client) {
        Subscriber subscriber = ((DefaultSubscriberFactory.SubscriberAdapter) client).subscriber();
        Object executor;
        try {
            Field field = Subscriber.class.getDeclaredField("alarmsExecutor");
            field.setAccessible(true);
            executor = field.get(subscriber);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                    "Subscriber no longer holds its system executor as read here", e);
        }
        assertThat(executor)
                .as("Subscriber's system executor, read from the field alarmsExecutor")
                .isInstanceOf(ScheduledThreadPoolExecutor.class);
        return (ScheduledThreadPoolExecutor) executor;
    }
}
