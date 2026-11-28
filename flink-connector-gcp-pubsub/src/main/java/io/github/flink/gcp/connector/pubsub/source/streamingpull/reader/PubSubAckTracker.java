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
import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckResponse;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link AckTracker} keeping the acknowledgement handles in memory until their checkpoint
 * completes.
 *
 * <p>State changes are synchronized: {@link #addPendingAck} runs on client-library callback threads
 * while the remaining methods run on the reader's task and fetcher threads. Settling itself happens
 * <em>outside</em> the monitor — with {@code awaitAckConfirmation} set it blocks for a round trip,
 * and holding the lock across that would stall the callback threads delivering new messages.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * extended with per-split scoping, settling of superseded redeliveries, immediate settlement paths
 * and acknowledgement confirmation.
 */
@Internal
public class PubSubAckTracker implements AckTracker {

    /** Pending (received, not yet processed) acknowledgements, per split and message id. */
    @GuardedBy("this")
    private final Map<String, Map<String, AckHandle>> pendingAcks = new HashMap<>();

    /** Processed (emitted or dropped) but not yet bound to a checkpoint. */
    @GuardedBy("this")
    private final List<TrackedAck> stagedAcks = new ArrayList<>();

    /** Bound to a checkpoint that has not completed yet, ordered by checkpoint id. */
    @GuardedBy("this")
    private final SortedMap<Long, List<TrackedAck>> checkpoints = new TreeMap<>();

    private final PubSubSourceReaderMetrics metrics;
    @Nullable private final AckConfirmationWait confirmationWait;

    /**
     * Creates the tracker.
     *
     * @param metrics counts the lifecycle transitions
     * @param awaitAckConfirmation how long a completed checkpoint waits for its acknowledgements to
     *     be confirmed, or {@code null} for fire-and-forget acknowledgement
     */
    public PubSubAckTracker(
            PubSubSourceReaderMetrics metrics, @Nullable Duration awaitAckConfirmation) {
        this.metrics = metrics;
        this.confirmationWait =
                awaitAckConfirmation != null ? new AckConfirmationWait(awaitAckConfirmation) : null;
    }

    @Override
    public void addPendingAck(String splitId, String messageId, AckHandle ackHandle) {
        AckHandle superseded;
        synchronized (this) {
            superseded =
                    pendingAcks
                            .computeIfAbsent(splitId, key -> new LinkedHashMap<>())
                            .put(messageId, ackHandle);
        }
        metrics.messageReceived();
        if (superseded != null) {
            // The message was redelivered before the previous delivery was settled — its
            // acknowledgement id is already stale, but the client library only releases that
            // delivery's flow-control permit once the handle is settled.
            superseded.nack();
            metrics.messagesNacked(1);
        }
    }

    @Override
    public void nackReceived(AckHandle ackHandle) {
        metrics.messageReceived();
        ackHandle.nack();
        metrics.messagesNacked(1);
    }

    @Override
    public synchronized void stagePendingAck(String splitId, String messageId) {
        AckHandle handle = removePendingAck(splitId, messageId);
        if (handle != null) {
            stagedAcks.add(new TrackedAck(splitId, messageId, handle));
        }
    }

    @Override
    public void ackPendingImmediately(String splitId, String messageId) {
        AckHandle handle;
        synchronized (this) {
            handle = removePendingAck(splitId, messageId);
        }
        if (handle != null) {
            // The response future is dropped rather than awaited: this acknowledgement is bound to
            // no checkpoint, so no confirmation wait reads it.
            handle.ack();
            metrics.messagesAcked(1);
        }
    }

    @Override
    public void nackPendingImmediately(String splitId, String messageId) {
        AckHandle handle;
        synchronized (this) {
            handle = removePendingAck(splitId, messageId);
        }
        if (handle != null) {
            handle.nack();
            metrics.messagesNacked(1);
        }
    }

    @GuardedBy("this")
    @Nullable
    private AckHandle removePendingAck(String splitId, String messageId) {
        Map<String, AckHandle> splitAcks = pendingAcks.get(splitId);
        if (splitAcks == null) {
            return null;
        }
        AckHandle handle = splitAcks.remove(messageId);
        if (splitAcks.isEmpty()) {
            pendingAcks.remove(splitId);
        }
        return handle;
    }

    @Override
    public synchronized void addCheckpoint(long checkpointId) {
        checkpoints.put(checkpointId, new ArrayList<>(stagedAcks));
        stagedAcks.clear();
    }

    /**
     * Returns the ids of every message bound to a checkpoint that has not been acknowledged yet,
     * per split. Called right after {@link #addCheckpoint(long)}, this is exactly the set the
     * checkpoint being taken covers but whose acknowledgement could still be lost: everything
     * processed before its barrier, minus what earlier completions already acknowledged.
     *
     * <p>Received-but-unprocessed messages are never included: their records are not in the
     * checkpoint, so suppressing their redelivery after a restore would lose them.
     *
     * @return the message ids per split id, in binding order
     */
    @Override
    public synchronized Map<String, List<String>> checkpointedMessageIds() {
        Map<String, List<String>> ids = new LinkedHashMap<>();
        for (List<TrackedAck> checkpointed : checkpoints.values()) {
            for (TrackedAck ack : checkpointed) {
                ids.computeIfAbsent(ack.splitId, key -> new ArrayList<>()).add(ack.messageId);
            }
        }
        return ids;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws IOException {
        List<TrackedAck> toAck = new ArrayList<>();
        synchronized (this) {
            while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
                toAck.addAll(checkpoints.remove(checkpoints.firstKey()));
            }
        }
        // Every handle is acknowledged before anything is thrown, for the reason nackSplit gives
        // (#2337).
        List<ApiFuture<AckResponse>> confirmations =
                confirmationWait != null ? new ArrayList<>(toAck.size()) : null;
        Throwable failure = null;
        int acked = 0;
        for (TrackedAck ack : toAck) {
            ApiFuture<AckResponse> confirmation;
            try {
                confirmation = ack.handle.ack();
            } catch (Throwable e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
                continue;
            }
            acked++;
            if (confirmations != null && confirmation != null) {
                confirmations.add(confirmation);
            }
        }
        metrics.messagesAcked(acked);
        int missingConfirmations = confirmations != null ? acked - confirmations.size() : 0;
        if (missingConfirmations > 0) {
            // Skipping the promised wait because a handle reports nothing would be exactly the
            // silent failure this option exists to remove, so make the broken wiring loud. It is
            // built after the loop so that it never takes the top slot from a failure an ack threw:
            // Flink's fatal-error check in Task.preProcessException does not look at suppressed
            // exceptions.
            failure =
                    ExceptionUtils.firstOrSuppressed(
                            new IllegalStateException(
                                    "awaitAckConfirmation is set but "
                                            + missingConfirmations
                                            + " of the "
                                            + acked
                                            + " acknowledgements of checkpoint "
                                            + checkpointId
                                            + " that returned gave no confirmation future: the"
                                            + " subscriber was built without acknowledgement"
                                            + " responses."),
                            failure);
        }
        if (failure != null) {
            // A throwing ack already fails the task, so the bounded wait below could only delay
            // that by up to its timeout. It runs only when every ack returned its future.
            ExceptionUtils.rethrow(failure);
        }
        if (confirmations != null && !confirmations.isEmpty()) {
            confirmationWait.await(confirmations, checkpointId);
        }
    }

    @Override
    public void nackSplit(String splitId) {
        List<AckHandle> toNack = new ArrayList<>();
        synchronized (this) {
            Map<String, AckHandle> splitAcks = pendingAcks.remove(splitId);
            if (splitAcks != null) {
                toNack.addAll(splitAcks.values());
            }
            drainSplit(stagedAcks, splitId, toNack);
            // Every checkpoint entry stays, including one this drain empties (#2325): the
            // checkpoint has still not completed, which is what checkpointsPendingAckCount()
            // counts, and notifyCheckpointComplete sweeps an empty entry like any other. An empty
            // entry adds no message id to the ledger. One this drain empties becomes the shared
            // empty list, so it stops holding the array its handles were copied into.
            checkpoints.replaceAll(
                    (checkpointId, checkpointed) -> {
                        drainSplit(checkpointed, splitId, toNack);
                        return checkpointed.isEmpty() ? Collections.emptyList() : checkpointed;
                    });
        }
        // The split's handles are already out of the maps, so one this loop skipped would be left
        // to the client library's own shutdown or lease expiry. Every handle is therefore nacked
        // before anything is thrown, with the first failure thrown as itself and later ones
        // suppressed onto it, as Closers.closeAll reports (#2326).
        Throwable failure = null;
        int nacked = 0;
        for (AckHandle handle : toNack) {
            try {
                handle.nack();
                nacked++;
            } catch (Throwable e) {
                failure = ExceptionUtils.firstOrSuppressed(e, failure);
            }
        }
        metrics.messagesNacked(nacked);
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }

    private static void drainSplit(List<TrackedAck> acks, String splitId, List<AckHandle> into) {
        Iterator<TrackedAck> iterator = acks.iterator();
        while (iterator.hasNext()) {
            TrackedAck ack = iterator.next();
            if (ack.splitId.equals(splitId)) {
                into.add(ack.handle);
                iterator.remove();
            }
        }
    }

    /**
     * Returns how many messages are received or emitted but not yet acknowledged, across every
     * split and state. {@link MissingCheckpointDetector} uses it to tell "nothing is being
     * acknowledged" apart from "there is nothing to acknowledge".
     *
     * @return the outstanding message count
     */
    public synchronized int outstandingAckCount() {
        int outstanding = stagedAcks.size();
        for (Map<String, AckHandle> splitAcks : pendingAcks.values()) {
            outstanding += splitAcks.size();
        }
        for (List<TrackedAck> checkpointed : checkpoints.values()) {
            outstanding += checkpointed.size();
        }
        return outstanding;
    }

    /**
     * Returns how many checkpoints have been taken but not yet completed, and therefore how many
     * generations of acknowledgements are being held.
     *
     * @return the pending checkpoint count
     */
    public synchronized int checkpointsPendingAckCount() {
        return checkpoints.size();
    }

    /** An acknowledgement handle together with the split and message it belongs to. */
    private static final class TrackedAck {

        private final String splitId;
        private final String messageId;
        private final AckHandle handle;

        private TrackedAck(String splitId, String messageId, AckHandle handle) {
            this.splitId = splitId;
            this.messageId = messageId;
            this.handle = handle;
        }
    }
}
