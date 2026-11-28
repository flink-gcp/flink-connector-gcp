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

import com.google.cloud.pubsub.v1.AckResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubAckTracker}, which defines the source's at-least-once guarantee. */
class PubSubAckTrackerTest {

    private static final String SPLIT_A = "split-a";
    private static final String SPLIT_B = "split-b";

    private final TestReaderMetrics metrics = new TestReaderMetrics();
    private final PubSubAckTracker tracker = new PubSubAckTracker(metrics.metrics(), null);

    @Test
    void messageIsAcknowledgedOnlyWhenTheCheckpointCoveringItsEmissionCompletes() throws Exception {
        RecordingAckHandle message = new RecordingAckHandle("m1");
        tracker.addPendingAck(SPLIT_A, "m1", message);

        // Received but not emitted: a checkpoint must not acknowledge it.
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);
        assertThat(message.isUnsettled()).isTrue();

        tracker.stagePendingAck(SPLIT_A, "m1");
        tracker.addCheckpoint(2L);
        assertThat(message.isUnsettled()).isTrue();

        tracker.notifyCheckpointComplete(2L);
        assertThat(message.isAcked()).isTrue();
    }

    @Test
    void emittedMessageIsNotAcknowledgedUntilItsCheckpointIsTaken() throws Exception {
        RecordingAckHandle message = new RecordingAckHandle("m1");
        tracker.addPendingAck(SPLIT_A, "m1", message);
        tracker.stagePendingAck(SPLIT_A, "m1");

        // Staged but not yet bound to a checkpoint: completing an older one changes nothing.
        tracker.notifyCheckpointComplete(7L);

        assertThat(message.isUnsettled()).isTrue();
    }

    @Test
    void completingACheckpointSweepsEveryEarlierOneSoAbortedCheckpointsHeal() throws Exception {
        RecordingAckHandle first = new RecordingAckHandle("m1");
        RecordingAckHandle second = new RecordingAckHandle("m2");
        stageOn(SPLIT_A, "m1", first);
        tracker.addCheckpoint(1L);
        stageOn(SPLIT_A, "m2", second);
        tracker.addCheckpoint(2L);

        // Checkpoint 1 was aborted, so its completion notification never arrives; checkpoint 2
        // must still acknowledge its messages.
        tracker.notifyCheckpointComplete(2L);

        assertThat(first.isAcked()).isTrue();
        assertThat(second.isAcked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();
    }

    @Test
    void repeatingACheckpointCompletionIsHarmless() throws Exception {
        RecordingAckHandle message = new RecordingAckHandle("m1");
        stageOn(SPLIT_A, "m1", message);
        tracker.addCheckpoint(1L);

        tracker.notifyCheckpointComplete(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(message.isAcked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();
    }

    @Test
    void nackingASplitReleasesItsMessagesInEveryState() throws Exception {
        RecordingAckHandle pending = new RecordingAckHandle("pending");
        RecordingAckHandle staged = new RecordingAckHandle("staged");
        RecordingAckHandle checkpointed = new RecordingAckHandle("checkpointed");
        stageOn(SPLIT_A, "checkpointed", checkpointed);
        tracker.addCheckpoint(1L);
        stageOn(SPLIT_A, "staged", staged);
        tracker.addPendingAck(SPLIT_A, "pending", pending);

        tracker.nackSplit(SPLIT_A);

        assertThat(pending.isNacked()).isTrue();
        assertThat(staged.isNacked()).isTrue();
        assertThat(checkpointed.isNacked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();
    }

    @Test
    void nackingOneSplitLeavesTheOtherSplitsUntouched() throws Exception {
        RecordingAckHandle onA = new RecordingAckHandle("a");
        RecordingAckHandle onB = new RecordingAckHandle("b");
        stageOn(SPLIT_A, "m", onA);
        stageOn(SPLIT_B, "m", onB);
        tracker.addCheckpoint(1L);

        tracker.nackSplit(SPLIT_A);

        assertThat(onA.isNacked()).isTrue();
        assertThat(onB.isUnsettled()).isTrue();

        tracker.notifyCheckpointComplete(1L);
        assertThat(onB.isAcked()).isTrue();
    }

    @Test
    void aThrowingNackStillNacksTheRestOfTheSplit() throws Exception {
        // #2326: the split's handles leave the maps before any is nacked, so a nack that threw used
        // to leave every handle after it un-nacked and tracked by nothing, and count none of them.
        IllegalStateException refused = new IllegalStateException("the consumer refused");
        RecordingAckHandle pending = new RecordingAckHandle("pending").failNackWith(refused);
        RecordingAckHandle staged = new RecordingAckHandle("staged");
        RecordingAckHandle checkpointed = new RecordingAckHandle("checkpointed");
        RecordingAckHandle onB = new RecordingAckHandle("b");
        stageOn(SPLIT_A, "checkpointed", checkpointed);
        tracker.addCheckpoint(1L);
        stageOn(SPLIT_A, "staged", staged);
        tracker.addPendingAck(SPLIT_A, "pending", pending);
        tracker.addPendingAck(SPLIT_B, "b", onB);

        assertThatThrownBy(() -> tracker.nackSplit(SPLIT_A)).isSameAs(refused);

        assertThat(pending.nackCount()).isEqualTo(1);
        assertThat(staged.nackCount()).isEqualTo(1);
        assertThat(checkpointed.nackCount()).isEqualTo(1);
        assertThat(onB.isUnsettled()).isTrue();
        // Only the nacks that returned count as messages returned for redelivery.
        assertThat(metrics.counter("messagesNacked")).isEqualTo(2);
        assertThat(tracker.outstandingAckCount()).isEqualTo(1);

        // Forgotten all the same, so a second release settles nothing again.
        tracker.nackSplit(SPLIT_A);
        assertThat(pending.nackCount()).isEqualTo(1);
        assertThat(staged.nackCount()).isEqualTo(1);
        assertThat(checkpointed.nackCount()).isEqualTo(1);
    }

    @Test
    void aLaterNackFailureIsSuppressedOntoTheFirst() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second");
        RecordingAckHandle between = new RecordingAckHandle("between");
        tracker.addPendingAck(SPLIT_A, "m1", new RecordingAckHandle("m1").failNackWith(first));
        tracker.addPendingAck(SPLIT_A, "m2", between);
        tracker.addPendingAck(SPLIT_A, "m3", new RecordingAckHandle("m3").failNackWith(second));

        assertThatThrownBy(() -> tracker.nackSplit(SPLIT_A))
                .isSameAs(first)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(second));
        assertThat(between.nackCount()).isEqualTo(1);
    }

    @Test
    void anErrorFromANackIsThrownAsItselfAfterTheRestAreNacked() {
        // The one way found for a production handle's nack to throw is an Error from a listener of
        // its reply future (#2326), so an Error must neither be wrapped nor end the loop.
        AssertionError listenerFailure = new AssertionError("a listener failed");
        RecordingAckHandle after = new RecordingAckHandle("after");
        tracker.addPendingAck(
                SPLIT_A, "m1", new RecordingAckHandle("m1").failNackWith(listenerFailure));
        tracker.addPendingAck(SPLIT_A, "m2", after);

        assertThatThrownBy(() -> tracker.nackSplit(SPLIT_A)).isSameAs(listenerFailure);
        assertThat(after.nackCount()).isEqualTo(1);
    }

    @Test
    void aThrowingAckStillAcknowledgesTheRestOfTheCompletedCheckpoints() throws Exception {
        // #2337: every completed checkpoint leaves the map before any handle is acknowledged, so an
        // ack that threw used to leave every handle after it unacknowledged and tracked by nothing,
        // and count none of them, not even those already sent.
        IllegalStateException refused = new IllegalStateException("the consumer refused");
        RecordingAckHandle before = new RecordingAckHandle("before");
        RecordingAckHandle throwing = new RecordingAckHandle("throwing").failAckWith(refused);
        RecordingAckHandle later = new RecordingAckHandle("later");
        stageOn(SPLIT_A, "before", before);
        stageOn(SPLIT_B, "throwing", throwing);
        tracker.addCheckpoint(1L);
        stageOn(SPLIT_A, "later", later);
        tracker.addCheckpoint(2L);

        assertThatThrownBy(() -> tracker.notifyCheckpointComplete(2L)).isSameAs(refused);

        assertThat(before.ackCount()).isEqualTo(1);
        assertThat(throwing.ackCount()).isEqualTo(1);
        assertThat(later.ackCount()).isEqualTo(1);
        // Only the acks that returned count as acknowledgements requested.
        assertThat(metrics.counter("messagesAcked")).isEqualTo(2);
        assertThat(tracker.outstandingAckCount()).isZero();
        assertThat(tracker.checkpointsPendingAckCount()).isZero();

        // Forgotten all the same, so a repeated notification settles nothing again.
        tracker.notifyCheckpointComplete(2L);
        assertThat(before.ackCount()).isEqualTo(1);
        assertThat(throwing.ackCount()).isEqualTo(1);
        assertThat(later.ackCount()).isEqualTo(1);
    }

    @Test
    void aLaterAckFailureIsSuppressedOntoTheFirst() {
        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second");
        RecordingAckHandle between = new RecordingAckHandle("between");
        stageOn(SPLIT_A, "m1", new RecordingAckHandle("m1").failAckWith(first));
        stageOn(SPLIT_A, "m2", between);
        stageOn(SPLIT_A, "m3", new RecordingAckHandle("m3").failAckWith(second));
        tracker.addCheckpoint(1L);

        assertThatThrownBy(() -> tracker.notifyCheckpointComplete(1L))
                .isSameAs(first)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(second));
        assertThat(between.ackCount()).isEqualTo(1);
        assertThat(metrics.counter("messagesAcked")).isEqualTo(1);
    }

    @Test
    void anErrorFromAnAckIsThrownAsItselfAfterTheRestAreAcknowledged() {
        // The one way found for a production handle's settlement to throw is an Error from a
        // listener of its reply future (#2326), so an Error must neither be wrapped nor end the
        // loop.
        AssertionError listenerFailure = new AssertionError("a listener failed");
        RecordingAckHandle after = new RecordingAckHandle("after");
        stageOn(SPLIT_A, "m1", new RecordingAckHandle("m1").failAckWith(listenerFailure));
        stageOn(SPLIT_A, "m2", after);
        tracker.addCheckpoint(1L);

        assertThatThrownBy(() -> tracker.notifyCheckpointComplete(1L)).isSameAs(listenerFailure);
        assertThat(after.ackCount()).isEqualTo(1);
    }

    @Test
    void sameMessageIdOnDifferentSplitsIsTrackedSeparately() throws Exception {
        // Two subscriptions of one topic deliver the same message id to the same reader.
        RecordingAckHandle onA = new RecordingAckHandle("a");
        RecordingAckHandle onB = new RecordingAckHandle("b");
        tracker.addPendingAck(SPLIT_A, "shared-id", onA);
        tracker.addPendingAck(SPLIT_B, "shared-id", onB);

        assertThat(onA.isUnsettled()).isTrue();
        assertThat(onB.isUnsettled()).isTrue();

        tracker.stagePendingAck(SPLIT_A, "shared-id");
        tracker.stagePendingAck(SPLIT_B, "shared-id");
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(onA.isAcked()).isTrue();
        assertThat(onB.isAcked()).isTrue();
    }

    @Test
    void redeliveryOfAnUnsettledMessageNacksTheSupersededHandle() throws Exception {
        RecordingAckHandle firstDelivery = new RecordingAckHandle("first");
        RecordingAckHandle redelivery = new RecordingAckHandle("redelivery");
        tracker.addPendingAck(SPLIT_A, "m1", firstDelivery);

        tracker.addPendingAck(SPLIT_A, "m1", redelivery);

        // The stale handle must be settled, or the client library never releases its flow-control
        // permit.
        assertThat(firstDelivery.isNacked()).isTrue();
        assertThat(redelivery.isUnsettled()).isTrue();
        assertThat(tracker.outstandingAckCount()).isEqualTo(1);
    }

    @Test
    void theLedgerHoldsWhatTheCheckpointCoversAndNothingElse() throws Exception {
        tracker.addPendingAck(SPLIT_A, "received-only", new RecordingAckHandle("received-only"));
        stageOn(SPLIT_A, "acked", new RecordingAckHandle("acked"));
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);
        stageOn(SPLIT_A, "older-unnotified", new RecordingAckHandle("older-unnotified"));
        tracker.addCheckpoint(2L);
        stageOn(SPLIT_B, "covered", new RecordingAckHandle("covered"));
        tracker.addCheckpoint(3L);
        stageOn(SPLIT_B, "after-barrier", new RecordingAckHandle("after-barrier"));

        // Checkpoint 3 covers everything emitted before its barrier, including checkpoint 2's
        // messages whose notification never came. It covers neither the message received but not
        // emitted — suppressing its redelivery after a restore would lose it — nor the one emitted
        // after the barrier, nor the one checkpoint 1 already acknowledged.
        assertThat(tracker.checkpointedMessageIds())
                .containsOnlyKeys(SPLIT_A, SPLIT_B)
                .containsEntry(SPLIT_A, List.of("older-unnotified"))
                .containsEntry(SPLIT_B, List.of("covered"));
    }

    @Test
    void aNackedSplitLeavesTheLedger() throws Exception {
        stageOn(SPLIT_A, "m1", new RecordingAckHandle("m1"));
        tracker.addCheckpoint(1L);

        tracker.nackSplit(SPLIT_A);

        assertThat(tracker.checkpointedMessageIds()).isEmpty();
    }

    @Test
    void stagingAnUnknownMessageIsIgnored() throws Exception {
        tracker.stagePendingAck(SPLIT_A, "never-received");
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(tracker.outstandingAckCount()).isZero();
    }

    private void stageOn(String splitId, String messageId, RecordingAckHandle consumer) {
        stageOn(tracker, splitId, messageId, consumer);
    }

    private static void stageOn(
            PubSubAckTracker target,
            String splitId,
            String messageId,
            RecordingAckHandle consumer) {
        target.addPendingAck(splitId, messageId, consumer);
        target.stagePendingAck(splitId, messageId);
    }

    @Test
    void awaitingConfirmationSucceedsWhenTheServerConfirms() throws Exception {
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker awaiting =
                new PubSubAckTracker(testMetrics.metrics(), Duration.ofSeconds(30));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        stageOn(awaiting, SPLIT_A, "m1", handle);
        awaiting.addCheckpoint(1L);
        handle.confirm(AckResponse.SUCCESSFUL);

        awaiting.notifyCheckpointComplete(1L);

        assertThat(handle.isAcked()).isTrue();
        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
        // A future already confirmed is observed through get(); the timeout test below covers the
        // other path, a pending future observed through addListener.
        assertThat(handle.confirmationObservations()).isPositive();
    }

    @Test
    void awaitingConfirmationDoesNotWaitAfterAnAckThrew() {
        // #2337: a throwing ack already fails the task, so the bounded wait could only delay
        // that by up to its timeout; it is skipped, and nothing observes the other futures.
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker awaiting =
                new PubSubAckTracker(testMetrics.metrics(), Duration.ofMillis(50));
        IllegalStateException refused = new IllegalStateException("the consumer refused");
        RecordingAckHandle before = RecordingAckHandle.withConfirmation("before");
        RecordingAckHandle throwing =
                RecordingAckHandle.withConfirmation("throwing").failAckWith(refused);
        RecordingAckHandle after = RecordingAckHandle.withConfirmation("after");
        stageOn(awaiting, SPLIT_A, "before", before);
        stageOn(awaiting, SPLIT_A, "throwing", throwing);
        stageOn(awaiting, SPLIT_A, "after", after);
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isSameAs(refused)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).isEmpty());

        assertThat(after.ackCount()).isEqualTo(1);
        assertThat(before.confirmationObservations()).isZero();
        assertThat(after.confirmationObservations()).isZero();
        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(2);
    }

    @Test
    void aMissingConfirmationFutureFailsOnceAfterEveryAckIsSent() {
        // A subscriber built without acknowledgement responses returns no future. The default
        // factory selects the flavor from the same option as the tracker, so this guards the
        // internal seam: every ack is still sent and counted, one failure names how many futures
        // were missing, and nothing waits on the futures that did come back.
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker awaiting =
                new PubSubAckTracker(testMetrics.metrics(), Duration.ofMillis(50));
        RecordingAckHandle confirmedFirst = RecordingAckHandle.withConfirmation("first");
        RecordingAckHandle missing = new RecordingAckHandle("missing");
        RecordingAckHandle missingToo = new RecordingAckHandle("missing-too");
        RecordingAckHandle confirmedLast = RecordingAckHandle.withConfirmation("last");
        stageOn(awaiting, SPLIT_A, "first", confirmedFirst);
        stageOn(awaiting, SPLIT_A, "missing", missing);
        stageOn(awaiting, SPLIT_A, "missing-too", missingToo);
        stageOn(awaiting, SPLIT_A, "last", confirmedLast);
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2 of the 4 acknowledgements of checkpoint 1 that returned")
                .hasMessageContaining("without acknowledgement responses")
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).isEmpty());

        assertThat(confirmedFirst.ackCount()).isEqualTo(1);
        assertThat(missing.ackCount()).isEqualTo(1);
        assertThat(missingToo.ackCount()).isEqualTo(1);
        assertThat(confirmedLast.ackCount()).isEqualTo(1);
        assertThat(confirmedFirst.confirmationObservations()).isZero();
        assertThat(confirmedLast.confirmationObservations()).isZero();
        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(4);
    }

    @Test
    void aThrownAckFailureRanksAboveAMissingConfirmationFuture() {
        // The tracker's own diagnostic never takes the top slot from a failure an ack threw, and it
        // counts only the acknowledgements that returned.
        PubSubAckTracker awaiting =
                new PubSubAckTracker(new TestReaderMetrics().metrics(), Duration.ofMillis(50));
        AssertionError listenerFailure = new AssertionError("a listener failed");
        stageOn(awaiting, SPLIT_A, "missing", new RecordingAckHandle("missing"));
        stageOn(
                awaiting,
                SPLIT_A,
                "throwing",
                RecordingAckHandle.withConfirmation("throwing").failAckWith(listenerFailure));
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isSameAs(listenerFailure)
                .satisfies(
                        thrown ->
                                assertThat(thrown.getSuppressed())
                                        .singleElement()
                                        .isInstanceOf(IllegalStateException.class)
                                        .extracting(Throwable::getMessage)
                                        .asString()
                                        .contains(
                                                "1 of the 1 acknowledgements of checkpoint 1"
                                                        + " that returned"));
    }

    @Test
    void everyMessageHandedOverIsCountedWhenItsAckIsRegistered() {
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker counting = new PubSubAckTracker(testMetrics.metrics(), null);

        counting.addPendingAck(SPLIT_A, "m1", new RecordingAckHandle("m1"));
        counting.addPendingAck(SPLIT_B, "m2", new RecordingAckHandle("m2"));

        // addPendingAck is the sole increment site for messagesReceived, and this is the only
        // assertion in the tree that reaches that counter by the name a reporter sees.
        assertThat(testMetrics.counter("messagesReceived")).isEqualTo(2);
    }

    @Test
    void awaitingConfirmationFailsTheCheckpointOnTimeout() {
        // On a subscription without exactly-once delivery the failure of an acknowledgement
        // request completes nothing, so the timeout is the only signal there is.
        PubSubAckTracker awaiting =
                new PubSubAckTracker(new TestReaderMetrics().metrics(), Duration.ofMillis(50));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        stageOn(awaiting, SPLIT_A, "m1", handle);
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not confirm the acknowledgements of checkpoint 1")
                .hasMessageContaining("failure of an acknowledgement request completes nothing");
        // A pending future is observed through addListener, the path any wait in the no-wait tests
        // would take on their never-completed futures, so their zero readings can fail.
        assertThat(handle.confirmationObservations()).isPositive();
    }

    @Test
    void awaitingConfirmationFailsOnARejectedAcknowledgement() {
        PubSubAckTracker awaiting =
                new PubSubAckTracker(new TestReaderMetrics().metrics(), Duration.ofSeconds(30));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        stageOn(awaiting, SPLIT_A, "m1", handle);
        awaiting.addCheckpoint(1L);
        handle.confirm(AckResponse.PERMISSION_DENIED);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("PERMISSION_DENIED");
    }

    @Test
    void anAcknowledgementIsCountedWhenItIsRequestedEvenIfItIsNeverConfirmed() {
        // messagesAcked counts acknowledgements requested, not confirmed — the class javadoc says
        // so, and the counter is what an operator reads next to
        // subscription/oldest_unacked_message_age. Counting after the wait instead would report
        // zero for exactly the checkpoints worth investigating.
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker awaiting =
                new PubSubAckTracker(testMetrics.metrics(), Duration.ofMillis(50));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        stageOn(awaiting, SPLIT_A, "m1", handle);
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IOException.class);

        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
    }

    @Test
    void immediateSettlementRemovesTheMessageFromTheLifecycle() throws Exception {
        RecordingAckHandle suppressed = new RecordingAckHandle("suppressed");
        RecordingAckHandle failed = new RecordingAckHandle("failed");
        tracker.addPendingAck(SPLIT_A, "suppressed", suppressed);
        tracker.addPendingAck(SPLIT_A, "failed", failed);

        tracker.ackPendingImmediately(SPLIT_A, "suppressed");
        tracker.nackPendingImmediately(SPLIT_A, "failed");

        assertThat(suppressed.isAcked()).isTrue();
        assertThat(failed.isNacked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();

        // Gone from the tracker, so nackSplit cannot settle them a second time.
        tracker.nackSplit(SPLIT_A);
        assertThat(suppressed.isNacked()).isFalse();
    }

    @Test
    void immediateSettlementIgnoresAnUnknownMessage() {
        assertThatCode(() -> tracker.ackPendingImmediately(SPLIT_A, "absent"))
                .doesNotThrowAnyException();
        assertThatCode(() -> tracker.nackPendingImmediately(SPLIT_A, "absent"))
                .doesNotThrowAnyException();
    }

    @Test
    void tracksHowManyCheckpointsAreWaitingToBeAcknowledged() throws Exception {
        tracker.addPendingAck(SPLIT_A, "m1", new RecordingAckHandle("m1"));
        tracker.stagePendingAck(SPLIT_A, "m1");
        tracker.addCheckpoint(1L);
        tracker.addPendingAck(SPLIT_A, "m2", new RecordingAckHandle("m2"));
        tracker.stagePendingAck(SPLIT_A, "m2");
        tracker.addCheckpoint(2L);

        assertThat(tracker.checkpointsPendingAckCount()).isEqualTo(2);

        tracker.notifyCheckpointComplete(2L);

        assertThat(tracker.checkpointsPendingAckCount()).isZero();
    }

    @Test
    void nackingASplitKeepsEveryCheckpointThatHasNotCompleted() throws Exception {
        // #2325: nackSplit used to drop every checkpoint entry it found empty, its own split's or
        // not, so the gauge read 1 here instead of 3. A checkpoint emptied by the release has still
        // not completed.
        tracker.addCheckpoint(1L);
        stageOn(SPLIT_A, "a1", new RecordingAckHandle("a1"));
        tracker.addCheckpoint(2L);
        RecordingAckHandle onB = new RecordingAckHandle("b");
        stageOn(SPLIT_A, "a2", new RecordingAckHandle("a2"));
        stageOn(SPLIT_B, "b", onB);
        tracker.addCheckpoint(3L);

        tracker.nackSplit(SPLIT_A);

        assertThat(tracker.checkpointsPendingAckCount()).isEqualTo(3);
        assertThat(tracker.checkpointedMessageIds()).containsOnlyKeys(SPLIT_B);

        tracker.notifyCheckpointComplete(3L);

        assertThat(onB.isAcked()).isTrue();
        assertThat(tracker.checkpointsPendingAckCount()).isZero();
        assertThat(tracker.outstandingAckCount()).isZero();
    }

    @Test
    void nackingAnUnknownSplitLeavesAnEmptyCheckpointPending() {
        tracker.addCheckpoint(7L);

        tracker.nackSplit("unrelated");

        assertThat(tracker.checkpointsPendingAckCount()).isEqualTo(1);
    }
}
