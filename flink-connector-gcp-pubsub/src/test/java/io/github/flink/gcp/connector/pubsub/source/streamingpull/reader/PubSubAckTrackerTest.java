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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PubSubAckTracker}, which defines the source's at-least-once guarantee. */
class PubSubAckTrackerTest {

    private static final String SPLIT_A = "split-a";
    private static final String SPLIT_B = "split-b";

    private final PubSubAckTracker tracker = new PubSubAckTracker();

    @Test
    void messageIsAcknowledgedOnlyWhenTheCheckpointCoveringItsEmissionCompletes() {
        RecordingAckReplyConsumer message = new RecordingAckReplyConsumer("m1");
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
    void emittedMessageIsNotAcknowledgedUntilItsCheckpointIsTaken() {
        RecordingAckReplyConsumer message = new RecordingAckReplyConsumer("m1");
        tracker.addPendingAck(SPLIT_A, "m1", message);
        tracker.stagePendingAck(SPLIT_A, "m1");

        // Staged but not yet bound to a checkpoint: completing an older one changes nothing.
        tracker.notifyCheckpointComplete(7L);

        assertThat(message.isUnsettled()).isTrue();
    }

    @Test
    void completingACheckpointSweepsEveryEarlierOneSoAbortedCheckpointsHeal() {
        RecordingAckReplyConsumer first = new RecordingAckReplyConsumer("m1");
        RecordingAckReplyConsumer second = new RecordingAckReplyConsumer("m2");
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
    void repeatingACheckpointCompletionIsHarmless() {
        RecordingAckReplyConsumer message = new RecordingAckReplyConsumer("m1");
        stageOn(SPLIT_A, "m1", message);
        tracker.addCheckpoint(1L);

        tracker.notifyCheckpointComplete(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(message.isAcked()).isTrue();
        assertThat(tracker.outstandingAckCount()).isZero();
    }

    @Test
    void nackingASplitReleasesItsMessagesInEveryState() {
        RecordingAckReplyConsumer pending = new RecordingAckReplyConsumer("pending");
        RecordingAckReplyConsumer staged = new RecordingAckReplyConsumer("staged");
        RecordingAckReplyConsumer checkpointed = new RecordingAckReplyConsumer("checkpointed");
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
    void nackingOneSplitLeavesTheOtherSplitsUntouched() {
        RecordingAckReplyConsumer onA = new RecordingAckReplyConsumer("a");
        RecordingAckReplyConsumer onB = new RecordingAckReplyConsumer("b");
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
    void sameMessageIdOnDifferentSplitsIsTrackedSeparately() {
        // Two subscriptions of one topic deliver the same message id to the same reader.
        RecordingAckReplyConsumer onA = new RecordingAckReplyConsumer("a");
        RecordingAckReplyConsumer onB = new RecordingAckReplyConsumer("b");
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
    void redeliveryOfAnUnsettledMessageNacksTheSupersededHandle() {
        RecordingAckReplyConsumer firstDelivery = new RecordingAckReplyConsumer("first");
        RecordingAckReplyConsumer redelivery = new RecordingAckReplyConsumer("redelivery");
        tracker.addPendingAck(SPLIT_A, "m1", firstDelivery);

        tracker.addPendingAck(SPLIT_A, "m1", redelivery);

        // The stale handle must be settled, or the client library never releases its flow-control
        // permit.
        assertThat(firstDelivery.isNacked()).isTrue();
        assertThat(redelivery.isUnsettled()).isTrue();
        assertThat(tracker.outstandingAckCount()).isEqualTo(1);
    }

    @Test
    void stagingAnUnknownMessageIsIgnored() {
        tracker.stagePendingAck(SPLIT_A, "never-received");
        tracker.addCheckpoint(1L);
        tracker.notifyCheckpointComplete(1L);

        assertThat(tracker.outstandingAckCount()).isZero();
    }

    private void stageOn(String splitId, String messageId, RecordingAckReplyConsumer consumer) {
        tracker.addPendingAck(splitId, messageId, consumer);
        tracker.stagePendingAck(splitId, messageId);
    }
}
