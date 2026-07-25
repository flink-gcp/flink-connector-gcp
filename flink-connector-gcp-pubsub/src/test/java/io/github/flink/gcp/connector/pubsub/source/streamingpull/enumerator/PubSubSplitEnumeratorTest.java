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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.enumerator;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.FlinkRuntimeException;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.StartPosition;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionInfo;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link PubSubSplitEnumerator}. */
class PubSubSplitEnumeratorTest {

    private static final String PROJECT = "test-project";
    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of(PROJECT, "subscription-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of(PROJECT, "subscription-b");
    private static final TopicDestination TOPIC = TopicDestination.of(PROJECT, "topic");

    @Test
    void everyRegisteredReaderReceivesItsSplits() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = started(context, OrderingMode.NONE, SUB_A, SUB_B);

        context.registerReader(0);
        enumerator.addReader(0);
        context.registerReader(1);
        enumerator.addReader(1);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(subscriptionsOf(context.assignedSplits(0)))
                .doesNotContainAnyElementsOf(subscriptionsOf(context.assignedSplits(1)));
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void oneSubscriptionIsSpreadOverEveryReaderWhenOrderingIsNotRequired() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator = started(context, OrderingMode.NONE, SUB_A);

        registerAll(context, enumerator, 3);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
        assertThat(context.assignedSplits(2)).hasSize(1);
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void orderedModeLeavesSurplusReadersWithNothingToDoAndFinishesThem() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator = startedOrdered(context, SUB_A);

        registerAll(context, enumerator, 3);

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.assignedSplits(2)).isEmpty();
        // Finishing idle subtasks keeps them from holding the watermark back forever.
        assertThat(context.readersToldNoMoreSplits()).containsExactly(1, 2);
    }

    @Test
    void aRestartedReaderIsHandedExactlyTheSplitsItReturned() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = started(context, OrderingMode.NONE, SUB_A, SUB_B);
        registerAll(context, enumerator, 2);
        List<SubscriptionSplit> beforeFailure = List.copyOf(context.assignedSplits(1));

        context.forgetAssignments();
        enumerator.addSplitsBack(beforeFailure, 1);
        enumerator.addReader(1);

        assertThat(context.assignedSplits(1)).containsExactlyElementsOf(beforeFailure);
    }

    @Test
    void restoringWithADifferentParallelismRecomputesTheAssignment() {
        PubSubEnumeratorState restored =
                new PubSubEnumeratorState(Arrays.asList(SUB_A, SUB_B), true);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(4);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        config(OrderingMode.NONE, SUB_A, SUB_B),
                        admin(SUB_A, SUB_B),
                        restored);

        registerAll(context, enumerator, 4);

        // Four readers, two subscriptions: the plan grows to four splits so nobody idles.
        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(3)).hasSize(1);
        assertThat(context.readersToldNoMoreSplits()).isEmpty();
    }

    @Test
    void restoringWithChangedSubscriptionsUsesTheConfiguredOnes() {
        PubSubEnumeratorState restored =
                new PubSubEnumeratorState(Collections.singletonList(SUB_A), true);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(
                        context,
                        config(OrderingMode.NONE, SUB_A, SUB_B),
                        admin(SUB_A, SUB_B),
                        restored);
        context.registerReader(0);
        enumerator.addReader(0);

        assertThat(subscriptionsOf(context.assignedSplits(0))).containsExactly(SUB_A, SUB_B);
    }

    @Test
    void snapshotRecordsTheConfiguredSubscriptions() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator = started(context, OrderingMode.NONE, SUB_A, SUB_B);

        assertThat(enumerator.snapshotState(1L).getSubscriptions()).containsExactly(SUB_A, SUB_B);
    }

    // -- The startup check -----------------------------------------------------------------

    @Test
    void readersRegisteringBeforeTheCheckCompletesAreAssignedWhenItDoes() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context,
                        config(OrderingMode.NONE, SUB_A, SUB_B),
                        admin(SUB_A, SUB_B),
                        null);
        enumerator.start();

        registerAll(context, enumerator, 2);

        // No subscriber may attach before the subscriptions are verified and any seek has landed.
        assertThat(context.assignedSplits(0)).isEmpty();
        assertThat(context.assignedSplits(1)).isEmpty();
        assertThat(context.pendingAsyncCalls()).isEqualTo(1);

        context.runAsyncCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).hasSize(1);
    }

    @Test
    void idleReadersAreFinishedOnlyAfterTheCheckCompletes() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context, config(OrderingMode.PER_KEY, SUB_A), ordered(SUB_A), null);
        enumerator.start();
        registerAll(context, enumerator, 3);

        assertThat(context.readersToldNoMoreSplits()).isEmpty();

        context.runAsyncCalls();

        assertThat(context.readersToldNoMoreSplits()).containsExactly(1, 2);
    }

    @Test
    void aReaderThatLeavesWhileWaitingIsSkipped() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context,
                        config(OrderingMode.NONE, SUB_A, SUB_B),
                        admin(SUB_A, SUB_B),
                        null);
        enumerator.start();
        registerAll(context, enumerator, 2);
        // It failed while waiting; the coordinator dropped it and will call addReader again.
        context.unregisterReader(1);

        context.runAsyncCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
        assertThat(context.assignedSplits(1)).isEmpty();
    }

    @Test
    void aReaderRegisteringTwiceWhileWaitingIsAssignedOnce() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context, config(OrderingMode.NONE, SUB_A), admin(SUB_A), null);
        enumerator.start();
        context.registerReader(0);
        // A subtask that fails and comes back while waiting is handed addReader a second time.
        enumerator.addReader(0);
        enumerator.addReader(0);

        context.runAsyncCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
    }

    @Test
    void aMissingSubscriptionWithNoCreateOptionsFailsTheJob() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context,
                        config(OrderingMode.NONE, SUB_A),
                        new FakeSubscriptionAdmin(),
                        null);
        enumerator.start();
        context.registerReader(0);
        enumerator.addReader(0);

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessageContaining("does not exist")
                .hasMessageContaining("subscription(destination, SubscriptionCreateOptions)");
        assertThat(context.assignedSplits(0)).isEmpty();
    }

    @Test
    void aMissingSubscriptionWithCreateOptionsIsCreated() {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscription(
                                        SUB_A,
                                        SubscriptionCreateOptions.builder().topic(TOPIC).build()));
        PubSubSplitEnumerator enumerator = new PubSubSplitEnumerator(context, config, admin, null);
        enumerator.start();
        context.registerReader(0);
        enumerator.addReader(0);

        context.runAsyncCalls();

        assertThat(admin.created).containsExactly(SUB_A);
        assertThat(context.assignedSplits(0)).hasSize(1);
    }

    @Test
    void eachSubscriptionIsCreatedWithItsOwnSettings() {
        // The trap this guards: one options object shared by both would bind them to the same
        // topic, and Pub/Sub gives each subscription a complete copy of that topic's stream.
        SubscriptionCreateOptions optionsA =
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of(PROJECT, "topic-a"))
                        .build();
        SubscriptionCreateOptions optionsB =
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of(PROJECT, "topic-b"))
                        .retainAckedMessages(true)
                        .build();
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(sourceBuilder().subscription(SUB_A, optionsA).subscription(SUB_B, optionsB));

        start(context, config, admin, null);

        assertThat(admin.createdWith)
                .containsExactly(entry(SUB_A, optionsA), entry(SUB_B, optionsB));
    }

    @Test
    void aSubscriptionCreatedConcurrentlyIsVerifiedByItsOwnSettings() {
        // The race the read-back exists for: someone else created it first, with ordering off.
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        admin.createReturns = SubscriptionInfo.builder().messageOrderingEnabled(false).build();
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscription(
                                        SUB_A,
                                        SubscriptionCreateOptions.builder()
                                                .topic(TOPIC)
                                                .enableMessageOrdering(true)
                                                .build())
                                .orderingMode(OrderingMode.PER_KEY));
        PubSubSplitEnumerator enumerator = new PubSubSplitEnumerator(context, config, admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessageContaining("orderingMode(PER_KEY)");
    }

    @Test
    void nothingIsSoughtWhenAnotherSubscriptionIsAboutToBeRejected() {
        // A seek rewrites state shared with every other consumer, so a deterministic rejection must
        // not leave the first subscription already rewound.
        FakeSubscriptionAdmin admin =
                new FakeSubscriptionAdmin()
                        .withSubscription(
                                SUB_A,
                                SubscriptionInfo.builder().messageOrderingEnabled(true).build())
                        .withSubscription(SUB_B);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscriptions(SUB_A, SUB_B)
                                .orderingMode(OrderingMode.PER_KEY)
                                .startPosition(StartPosition.earliestRetained()));
        PubSubSplitEnumerator enumerator = new PubSubSplitEnumerator(context, config, admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls).isInstanceOf(FlinkRuntimeException.class);

        assertThat(admin.seekedSubscriptions).isEmpty();
    }

    @Test
    void aFailingCreateFailsTheJob() {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        admin.createFailure = new IOException("no pubsub.subscriptions.create permission");
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscription(
                                        SUB_A,
                                        SubscriptionCreateOptions.builder().topic(TOPIC).build()));
        PubSubSplitEnumerator enumerator = new PubSubSplitEnumerator(context, config, admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessage("no pubsub.subscriptions.create permission");
    }

    @Test
    void aFailingSeekFailsTheJob() {
        FakeSubscriptionAdmin admin = admin(SUB_A);
        admin.seekFailure = new IOException("no pubsub.subscriptions.update permission");
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, earliestRetained(SUB_A), admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessage("no pubsub.subscriptions.update permission");
    }

    @Test
    void anUnorderedSubscriptionIsRejectedUnderOrderedConsumption() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(
                        context, config(OrderingMode.PER_KEY, SUB_A), admin(SUB_A), null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessageContaining("orderingMode(PER_KEY)")
                .hasMessageContaining("message ordering");
    }

    @Test
    void anExactlyOnceDeliverySubscriptionIsRejected() {
        FakeSubscriptionAdmin admin =
                new FakeSubscriptionAdmin()
                        .withSubscription(
                                SUB_A,
                                SubscriptionInfo.builder()
                                        .exactlyOnceDeliveryEnabled(true)
                                        .build());
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, config(OrderingMode.NONE, SUB_A), admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessageContaining("exactly-once delivery")
                .hasMessageContaining("at-least-once");
    }

    @Test
    void nackingWithoutADeadLetterPolicyIsRejected() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscription(SUB_A)
                                .deserializationFailurePolicy(DeserializationFailurePolicy.NACK));
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, config, admin(SUB_A), null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .rootCause()
                .hasMessageContaining("deserializationFailurePolicy(NACK)")
                .hasMessageContaining("dead-letter policy");
    }

    @Test
    void nackingIsAcceptedWithADeadLetterPolicy() {
        FakeSubscriptionAdmin admin =
                new FakeSubscriptionAdmin()
                        .withSubscription(
                                SUB_A,
                                SubscriptionInfo.builder()
                                        .deadLetterPolicyConfigured(true)
                                        .build());
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(
                        sourceBuilder()
                                .subscription(SUB_A)
                                .deserializationFailurePolicy(DeserializationFailurePolicy.NACK));
        PubSubSplitEnumerator enumerator = new PubSubSplitEnumerator(context, config, admin, null);
        enumerator.start();
        context.registerReader(0);
        enumerator.addReader(0);

        context.runAsyncCalls();

        assertThat(context.assignedSplits(0)).hasSize(1);
    }

    @Test
    void aFailingAdminFailsTheJobNamingTheSubscriptions() {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin().withSubscription(SUB_A);
        admin.describeFailure = new IOException("no pubsub.subscriptions.get permission");
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, config(OrderingMode.NONE, SUB_A), admin, null);
        enumerator.start();

        assertThatThrownBy(context::runAsyncCalls)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(SUB_A.toString())
                .rootCause()
                .hasMessage("no pubsub.subscriptions.get permission");
    }

    // -- The start position ----------------------------------------------------------------

    @Test
    void theDefaultStartPositionSeeksNothing() {
        FakeSubscriptionAdmin admin = admin(SUB_A, SUB_B);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        start(context, config(OrderingMode.NONE, SUB_A, SUB_B), admin, null);

        assertThat(admin.seekedSubscriptions).isEmpty();
    }

    @Test
    void everySubscriptionIsSoughtOnAFreshStart() {
        FakeSubscriptionAdmin admin = admin(SUB_A, SUB_B);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                start(context, earliestRetained(SUB_A, SUB_B), admin, null);

        assertThat(admin.seekedSubscriptions).containsExactly(SUB_A, SUB_B);
        assertThat(admin.seekTimes).containsExactly(Instant.EPOCH, Instant.EPOCH);
        assertThat(enumerator.snapshotState(1L).isStartPositionApplied()).isTrue();
    }

    @Test
    void theStartPositionIsNotAppliedAgainOnARestore() {
        FakeSubscriptionAdmin admin = admin(SUB_A);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubEnumeratorState restored =
                new PubSubEnumeratorState(Collections.singletonList(SUB_A), true);

        start(context, earliestRetained(SUB_A), admin, restored);

        // Without this guard every failover would rewind the subscription.
        assertThat(admin.seekedSubscriptions).isEmpty();
    }

    @Test
    void theStartPositionIsAppliedWhenTheRestoredStateSaysItWasNot() {
        FakeSubscriptionAdmin admin = admin(SUB_A);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        // Checkpointed while the check was still in flight, so no reader held a split.
        PubSubEnumeratorState restored =
                new PubSubEnumeratorState(Collections.singletonList(SUB_A), false);

        start(context, earliestRetained(SUB_A), admin, restored);

        assertThat(admin.seekedSubscriptions).containsExactly(SUB_A);
    }

    @Test
    void eachStartPositionResolvesToItsSeekTime() {
        Instant now = Instant.parse("2026-07-25T12:00:00Z");
        Instant earlier = Instant.parse("2026-07-20T09:30:00Z");

        // Pub/Sub treats a target older than the retention window as "everything still retained",
        // so the epoch reaches as far back as the subscription goes.
        assertThat(PubSubSplitEnumerator.seekTimeFor(StartPosition.earliestRetained(), now))
                .isEqualTo(Instant.EPOCH);
        assertThat(PubSubSplitEnumerator.seekTimeFor(StartPosition.latest(), now)).isEqualTo(now);
        assertThat(PubSubSplitEnumerator.seekTimeFor(StartPosition.fromTimestamp(earlier), now))
                .isEqualTo(earlier);
        assertThatThrownBy(
                        () ->
                                PubSubSplitEnumerator.seekTimeFor(
                                        StartPosition.continueFromSubscription(), now))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires no seek");
    }

    @Test
    void theLatestStartPositionIsPinnedWhenTheCheckStarts() {
        FakeSubscriptionAdmin admin = admin(SUB_A);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSourceConfig<?> config =
                config(sourceBuilder().subscription(SUB_A).startPosition(StartPosition.latest()));

        Instant before = Instant.now();
        start(context, config, admin, null);
        Instant after = Instant.now();

        // Resolved on the coordinator thread before the check runs, not while it is running, so it
        // cannot drift with however long the admin calls take.
        assertThat(admin.seekTimes).hasSize(1);
        assertThat(admin.seekTimes.get(0)).isBetween(before, after);
    }

    @Test
    void aSnapshotTakenBeforeTheCheckCompletesRecordsThatTheSeekHasNotHappened() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, earliestRetained(SUB_A), admin(SUB_A), null);
        enumerator.start();

        assertThat(enumerator.snapshotState(1L).isStartPositionApplied()).isFalse();

        context.runAsyncCalls();

        assertThat(enumerator.snapshotState(2L).isStartPositionApplied()).isTrue();
    }

    @Test
    void closingReleasesTheAdminAndSilencesAnInFlightCheck() throws Exception {
        FakeSubscriptionAdmin admin = admin(SUB_A);
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, earliestRetained(SUB_A), admin, null);
        enumerator.start();
        context.registerReader(0);
        enumerator.addReader(0);

        enumerator.close();
        // The job is going away; a check completing now must not assign or fail anything.
        context.runAsyncCalls();

        assertThat(admin.closeCalls).isEqualTo(1);
        assertThat(context.assignedSplits(0)).isEmpty();
        // close() does not abort a check already running, so the seek it was issuing still lands.
        assertThat(admin.seekedSubscriptions).containsExactly(SUB_A);
    }

    @Test
    void aCheckThatFailsAfterClosingDoesNotFailTheJob() {
        // Tearing the job down closes the admin under the check, so its failure is our own doing —
        // reporting it would turn a clean cancellation into a failure blaming Pub/Sub.
        FakeSubscriptionAdmin admin = admin(SUB_A);
        admin.describeFailure = new IOException("client is shut down");
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, config(OrderingMode.NONE, SUB_A), admin, null);
        enumerator.start();

        assertThatCode(
                        () -> {
                            enumerator.close();
                            context.runAsyncCalls();
                        })
                .doesNotThrowAnyException();
    }

    // -- Metrics ---------------------------------------------------------------------------

    @Test
    void reportsHowManySplitsAreAssignedAndHowManyReadersGotNothing() {
        // Two subscriptions under PER_KEY give two splits, so the third subtask gets nothing.
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(3);
        PubSubSplitEnumerator enumerator = startedOrdered(context, SUB_A, SUB_B);

        assertThat(context.<Integer>gauge("assignedSplits")).isZero();
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(2L);

        registerAll(context, enumerator, 3);

        assertThat(context.<Integer>gauge("assignedSplits")).isEqualTo(2);
        assertThat(context.<Integer>gauge("unassignedReaders")).isEqualTo(1);
        assertThat(context.<Long>gauge("unassignedSplits")).isZero();
    }

    @Test
    void returnedSplitsAreCountedBackAsUnassigned() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(1);
        PubSubSplitEnumerator enumerator = started(context, OrderingMode.NONE, SUB_A);
        context.registerReader(0);
        enumerator.addReader(0);

        enumerator.addSplitsBack(context.assignedSplits(0), 0);

        assertThat(context.<Integer>gauge("assignedSplits")).isZero();
        assertThat(context.<Long>gauge("unassignedSplits")).isEqualTo(1L);
    }

    @Test
    void reRegisteringASubtaskDoesNotInflateTheGauges() {
        // A failover removes the subtask from the coordinator's registered readers and calls
        // addReader again, while addSplitsBack returns only the part of the assignment no
        // completed checkpoint covers — often nothing. Counting deltas would drift upward here and
        // drive unassignedSplits negative.
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = startedOrdered(context, SUB_A, SUB_B);
        registerAll(context, enumerator, 2);

        enumerator.addSplitsBack(Collections.emptyList(), 0);
        enumerator.addReader(0);

        assertThat(context.<Integer>gauge("assignedSplits")).isEqualTo(2);
        assertThat(context.<Long>gauge("unassignedSplits")).isZero();
    }

    @Test
    void reRegisteringAnIdleSubtaskDoesNotInflateTheIdleGauge() {
        FakeSplitEnumeratorContext context = new FakeSplitEnumeratorContext(2);
        PubSubSplitEnumerator enumerator = startedOrdered(context, SUB_A);
        context.registerReader(1);
        enumerator.addReader(1);
        enumerator.addReader(1);
        enumerator.addReader(1);

        assertThat(context.<Integer>gauge("unassignedReaders")).isEqualTo(1);
    }

    // -- Helpers ---------------------------------------------------------------------------

    /**
     * Builds an enumerator whose startup check has already completed, over subscriptions that all
     * exist with default settings.
     */
    private static PubSubSplitEnumerator started(
            FakeSplitEnumeratorContext context,
            OrderingMode orderingMode,
            SubscriptionDestination... subscriptions) {
        return start(context, config(orderingMode, subscriptions), admin(subscriptions), null);
    }

    /**
     * Builds an enumerator under {@link OrderingMode#PER_KEY} whose startup check has already
     * completed, over subscriptions that all have message ordering enabled.
     */
    private static PubSubSplitEnumerator startedOrdered(
            FakeSplitEnumeratorContext context, SubscriptionDestination... subscriptions) {
        return start(
                context, config(OrderingMode.PER_KEY, subscriptions), ordered(subscriptions), null);
    }

    private static PubSubSplitEnumerator start(
            FakeSplitEnumeratorContext context,
            PubSubSourceConfig<?> config,
            FakeSubscriptionAdmin admin,
            @Nullable PubSubEnumeratorState restoredState) {
        PubSubSplitEnumerator enumerator =
                new PubSubSplitEnumerator(context, config, admin, restoredState);
        enumerator.start();
        context.runAsyncCalls();
        return enumerator;
    }

    private static FakeSubscriptionAdmin admin(SubscriptionDestination... subscriptions) {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        for (SubscriptionDestination subscription : subscriptions) {
            admin.withSubscription(subscription);
        }
        return admin;
    }

    /** An admin whose subscriptions all have message ordering enabled. */
    private static FakeSubscriptionAdmin ordered(SubscriptionDestination... subscriptions) {
        FakeSubscriptionAdmin admin = new FakeSubscriptionAdmin();
        SubscriptionInfo info = SubscriptionInfo.builder().messageOrderingEnabled(true).build();
        for (SubscriptionDestination subscription : subscriptions) {
            admin.withSubscription(subscription, info);
        }
        return admin;
    }

    private static PubSubSourceConfig<?> config(
            OrderingMode orderingMode, SubscriptionDestination... subscriptions) {
        return config(sourceBuilder().subscriptions(subscriptions).orderingMode(orderingMode));
    }

    private static PubSubSourceConfig<?> earliestRetained(
            SubscriptionDestination... subscriptions) {
        return config(
                sourceBuilder()
                        .subscriptions(subscriptions)
                        .startPosition(StartPosition.earliestRetained()));
    }

    private static PubSubSourceBuilder<String> sourceBuilder() {
        return PubSubSource.<String>builder()
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()));
    }

    private static PubSubSourceConfig<?> config(PubSubSourceBuilder<String> builder) {
        return ((PubSubStreamingPullSource<String>) builder.build()).getConfig();
    }

    private static void registerAll(
            FakeSplitEnumeratorContext context, PubSubSplitEnumerator enumerator, int parallelism) {
        for (int subtask = 0; subtask < parallelism; subtask++) {
            context.registerReader(subtask);
            enumerator.addReader(subtask);
        }
    }

    private static List<SubscriptionDestination> subscriptionsOf(List<SubscriptionSplit> splits) {
        return splits.stream().map(SubscriptionSplit::getSubscription).collect(Collectors.toList());
    }
}
