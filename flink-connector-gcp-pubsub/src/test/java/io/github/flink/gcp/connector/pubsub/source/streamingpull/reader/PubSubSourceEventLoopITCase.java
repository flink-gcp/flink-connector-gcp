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

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.flink.gcp.connector.testutils.pubsub.PubSubSplitReaders.fetchUntil;
import static io.github.flink.gcp.connector.testutils.pubsub.PubSubSplitReaders.payloads;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source's gRPC clients run on the connector's own event-loop group and stop it when they close
 * (ADR-0208, #2263): the reader's subscribers on the TaskManager, and the enumerator's subscription
 * admin on the JobManager, whose client closes when each call returns because Flink releases the
 * job's class loader there without waiting for the enumerator to close.
 *
 * <p>The emulator arms are the ones a test can reach; the production arms apply the same lease to
 * the SDK's default provider, which {@code PubSubTransportsTest} covers.
 */
class PubSubSourceEventLoopITCase extends AbstractPubSubSourceEmulatorITCase {

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, each within its own
     * shutdown budget.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first. The harness's own clients run on
     * gRPC's group, not this one.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(EventLoopObserver.awaitNoGroup(GrpcEventLoops::isRunning, LEFTOVER_TEARDOWN))
                .as(
                        "no lease and no %s* thread before the test: an earlier class in this fork"
                                + " left the group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theReadersSubscriberRunsOnTheGroupAndStopsItOnClose() throws Exception {
        SubscriptionDestination subscription = createTopicAndSubscription("event-loop", 10);
        publish("event-loop", "m1");
        PubSubAckTracker ackTracker = new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
        PubSubSubscriberOptions options = PubSubSubscriberOptions.builder().build();

        try (PubSubSplitReader reader = reader(ackTracker, options)) {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new SubscriptionSplit(subscription, "0"))));

            assertThat(payloads(fetchUntil(reader, 1, Duration.ofSeconds(30))))
                    .containsExactly("m1");
            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread while the reader is open", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the reader closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    /**
     * The emulator channel carries the subscriber's own inbound limit, 20 MiB, which the SDK sets
     * on the provider it builds by default; a channel opened with gRPC's defaults would refuse this
     * message at 4 MiB and fail the stream.
     */
    @Test
    void anEmulatorSubscriberReceivesAMessageOverGrpcsDefaultInboundLimit() throws Exception {
        SubscriptionDestination subscription = createTopicAndSubscription("event-loop-large", 10);
        String large = "x".repeat(5 * 1024 * 1024);
        publish("event-loop-large", large);
        PubSubAckTracker ackTracker = new PubSubAckTracker(new TestReaderMetrics().metrics(), null);

        try (PubSubSplitReader reader =
                reader(ackTracker, PubSubSubscriberOptions.builder().build())) {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new SubscriptionSplit(subscription, "0"))));

            assertThat(payloads(fetchUntil(reader, 1, Duration.ofSeconds(30))))
                    .containsExactly(large);
        }
    }

    /**
     * The admin's client runs on the group and closes when each call returns. The call must start
     * one of the group's event loops while the group is running, since finding none afterwards
     * alone would pass for a client whose channels ran on gRPC's group too ({@link
     * EventLoopObserver}).
     */
    @Test
    void theSubscriptionAdminRunsOnTheGroupAndClosesItsClientWhenEachCallReturns()
            throws Exception {
        SubscriptionDestination subscription = createTopicAndSubscription("event-loop-admin", 10);

        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            AtomicBoolean heldAtReturn = new AtomicBoolean();
            assertThat(
                            EventLoopObserver.sawGroupDuring(
                                    GrpcEventLoops::isRunning,
                                    GrpcEventLoops::loopsStartedWhileRunning,
                                    () -> {
                                        SubscriptionInfo info = admin.describe(subscription);
                                        heldAtReturn.set(GrpcEventLoops.isRunning());
                                        assertThat(info).isNotNull();
                                    }))
                    .as("a group event loop started while the call ran")
                    .isTrue();

            // The admin is still open, as the enumerator's is until the job ends; nothing holds a
            // lease once the call has returned.
            assertThat(heldAtReturn.get())
                    .as("the admin's client was still holding the group when the call returned")
                    .isFalse();
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the call returned", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }
    }

    private static PubSubSplitReader reader(
            PubSubAckTracker ackTracker, PubSubSubscriberOptions options) {
        return new PubSubSplitReader(
                new DefaultSubscriberFactory(
                        options,
                        OrderingMode.NONE,
                        EmulatorEndpoint.parse(emulatorEndpoint(), "emulatorEndpoint")),
                ackTracker,
                options,
                new MissingCheckpointDetector(Duration.ZERO, ackTracker::outstandingAckCount),
                new TestReaderMetrics().metrics(),
                event -> {
                    throw new AssertionError("Unexpected buffer overflow: " + event);
                });
    }
}
