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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubAckTracker}, which defines the source's at-least-once guarantee. */
class PubSubAckTrackerTest {

    private static final String SPLIT_A = "split-a";
    private static final String SPLIT_B = "split-b";

    private final PubSubAckTracker tracker = newTracker();

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
    void stagingAnUnknownMessageIsIgnored() throws Exception {
        tracker.stagePendingAck(SPLIT_A, "never-received");
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(tracker.outstandingAckCount()).isZero();
    }

    private void stageOn(String splitId, String messageId, RecordingAckHandle consumer) {
        tracker.addPendingAck(splitId, messageId, consumer);
        tracker.stagePendingAck(splitId, messageId);
    }

    private static PubSubAckTracker newTracker() {
        return new PubSubAckTracker(new TestReaderMetrics().metrics(), null);
    }

    @Test
    void awaitingConfirmationSucceedsWhenTheServerConfirms() throws Exception {
        TestReaderMetrics testMetrics = new TestReaderMetrics();
        PubSubAckTracker awaiting =
                new PubSubAckTracker(testMetrics.metrics(), Duration.ofSeconds(30));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        awaiting.addPendingAck(SPLIT_A, "m1", handle);
        awaiting.stagePendingAck(SPLIT_A, "m1");
        awaiting.addCheckpoint(1L);
        handle.confirm(AckResponse.SUCCESSFUL);

        awaiting.notifyCheckpointComplete(1L);

        assertThat(handle.isAcked()).isTrue();
        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
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
        // On a subscription without exactly-once delivery a failed acknowledgement never completes
        // its future, so the timeout is the only signal there is.
        PubSubAckTracker awaiting =
                new PubSubAckTracker(new TestReaderMetrics().metrics(), Duration.ofMillis(50));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        awaiting.addPendingAck(SPLIT_A, "m1", handle);
        awaiting.stagePendingAck(SPLIT_A, "m1");
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not confirm the acknowledgements of checkpoint 1")
                .hasMessageContaining("never completes its future");
    }

    @Test
    void awaitingConfirmationFailsOnARejectedAcknowledgement() {
        PubSubAckTracker awaiting =
                new PubSubAckTracker(new TestReaderMetrics().metrics(), Duration.ofSeconds(30));
        RecordingAckHandle handle = RecordingAckHandle.withConfirmation("m1");
        awaiting.addPendingAck(SPLIT_A, "m1", handle);
        awaiting.stagePendingAck(SPLIT_A, "m1");
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
        awaiting.addPendingAck(SPLIT_A, "m1", handle);
        awaiting.stagePendingAck(SPLIT_A, "m1");
        awaiting.addCheckpoint(1L);

        assertThatThrownBy(() -> awaiting.notifyCheckpointComplete(1L))
                .isInstanceOf(IOException.class);

        assertThat(testMetrics.counter("messagesAcked")).isEqualTo(1);
    }

    @Test
    void immediateSettlementRemovesTheMessageFromTheLifecycle() throws Exception {
        RecordingAckHandle dropped = new RecordingAckHandle("dropped");
        RecordingAckHandle failed = new RecordingAckHandle("failed");
        tracker.addPendingAck(SPLIT_A, "dropped", dropped);
        tracker.addPendingAck(SPLIT_A, "failed", failed);

        tracker.ackPendingImmediately(SPLIT_A, "dropped");
        tracker.nackPendingImmediately(SPLIT_A, "failed");

        assertThat(dropped.isAcked()).isTrue();
        assertThat(failed.isNacked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();

        // Gone from the tracker, so nackSplit cannot settle them a second time.
        tracker.nackSplit(SPLIT_A);
        assertThat(dropped.isNacked()).isFalse();
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
}
