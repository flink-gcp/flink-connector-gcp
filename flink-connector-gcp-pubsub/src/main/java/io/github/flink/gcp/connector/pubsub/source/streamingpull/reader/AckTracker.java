/*
 * Copyright 2023 Google LLC
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

import org.apache.flink.annotation.Internal;

/**
 * Tracks the acknowledgement lifecycle of received Pub/Sub messages, which is what makes the source
 * at-least-once.
 *
 * <p>A message passes through four states:
 *
 * <ol>
 *   <li><b>pending</b> — received from Pub/Sub but not yet emitted downstream
 *   <li><b>staged</b> — emitted downstream, and therefore eligible for the next checkpoint
 *   <li><b>bound to a checkpoint</b> — included in the checkpoint being taken
 *   <li><b>acknowledged</b> — that checkpoint completed
 * </ol>
 *
 * <p>Only step 4 tells Pub/Sub the message is done, so a failure at any earlier point leaves the
 * message unacknowledged and Pub/Sub redelivers it. Messages still pending or staged when the
 * reader closes are nacked instead of left to expire, so redelivery is immediate.
 *
 * <p>Two paths settle a pending message at once instead: a message the deserialization failure
 * policy drops, and a message whose emission failed downstream. Neither produced records any
 * checkpoint could cover.
 *
 * <p>Every operation is keyed by split. Two subscriptions of the same topic deliver the same
 * message id to the same reader, so a reader-wide message-id map would silently drop one of the two
 * acknowledgements; and a split that goes away must only nack its own messages.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * whose tracker is reader-wide and whose {@code nackAll} therefore nacks every split's messages
 * when any one subscriber shuts down.
 */
@Internal
public interface AckTracker {

    /**
     * Records a message received on the given split but not yet emitted. If a message with the same
     * id is already pending on the split — a redelivery whose predecessor was never settled — the
     * superseded acknowledgement is nacked so the client library releases its flow-control permit.
     *
     * @param splitId the split the message was received on
     * @param messageId the Pub/Sub message id
     * @param ackHandle the handle settling this delivery
     */
    void addPendingAck(String splitId, String messageId, AckHandle ackHandle);

    /**
     * Nacks a delivery rejected before it enters the pending state. The delivery still counts as
     * received and nacked even though no later tracker operation can address it.
     *
     * @param ackHandle the handle settling the rejected delivery
     */
    void nackReceived(AckHandle ackHandle);

    /**
     * Marks a pending message as emitted downstream, making it eligible for the next checkpoint.
     * Does nothing if the message is not pending.
     *
     * @param splitId the split the message was received on
     * @param messageId the Pub/Sub message id
     */
    void stagePendingAck(String splitId, String messageId);

    /**
     * Acknowledges a pending message immediately, without waiting for a checkpoint. Used for
     * messages the deserialization failure policy drops: they produced no records, so no checkpoint
     * covers them and withholding the acknowledgement would only delay redelivery of a message that
     * is meant to be gone. Does nothing if the message is not pending.
     *
     * @param splitId the split the message was received on
     * @param messageId the Pub/Sub message id
     */
    void ackPendingImmediately(String splitId, String messageId);

    /**
     * Nacks a pending message immediately, so Pub/Sub redelivers it. Used when emitting the
     * message's records failed downstream. Does nothing if the message is not pending.
     *
     * @param splitId the split the message was received on
     * @param messageId the Pub/Sub message id
     */
    void nackPendingImmediately(String splitId, String messageId);

    /**
     * Binds everything staged so far to the given checkpoint. Called while the checkpoint is taken,
     * so exactly the messages emitted before the barrier are covered by it.
     *
     * @param checkpointId the checkpoint being taken
     */
    void addCheckpoint(long checkpointId);

    /**
     * Acknowledges every message bound to a checkpoint at or below the given id. Checkpoints are
     * swept rather than matched exactly, so a checkpoint that is aborted or whose completion
     * notification is lost is acknowledged by the next successful one.
     *
     * @param checkpointId the completed checkpoint
     * @throws Exception if acknowledgement confirmation was requested and did not arrive in time
     */
    void notifyCheckpointComplete(long checkpointId) throws Exception;

    /**
     * Nacks and forgets every message of the given split, in all states. Pub/Sub redelivers them
     * immediately rather than after the acknowledgement deadline expires.
     *
     * @param splitId the split to release
     */
    void nackSplit(String splitId);
}
