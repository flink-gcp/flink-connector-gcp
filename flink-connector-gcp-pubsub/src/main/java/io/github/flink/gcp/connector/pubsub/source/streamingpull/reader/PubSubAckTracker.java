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
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckResponse;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
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

    /** Pending (received, not yet emitted) acknowledgements, per split and message id. */
    @GuardedBy("this")
    private final Map<String, Map<String, AckHandle>> pendingAcks = new HashMap<>();

    /** Emitted but not yet bound to a checkpoint. */
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
    public synchronized void stagePendingAck(String splitId, String messageId) {
        AckHandle handle = removePendingAck(splitId, messageId);
        if (handle != null) {
            stagedAcks.add(new TrackedAck(splitId, handle));
        }
    }

    @Override
    public void ackPendingImmediately(String splitId, String messageId) {
        AckHandle handle;
        synchronized (this) {
            handle = removePendingAck(splitId, messageId);
        }
        if (handle != null) {
            // The response future is dropped rather than awaited: a dropped message produced no
            // records, so no checkpoint depends on its acknowledgement landing.
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

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws IOException {
        List<TrackedAck> toAck = new ArrayList<>();
        synchronized (this) {
            while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
                toAck.addAll(checkpoints.remove(checkpoints.firstKey()));
            }
        }
        List<ApiFuture<AckResponse>> confirmations =
                confirmationWait != null ? new ArrayList<>(toAck.size()) : null;
        for (TrackedAck ack : toAck) {
            ApiFuture<AckResponse> confirmation = ack.handle.ack();
            if (confirmations != null) {
                // Skipping the promised wait because a handle reports nothing would be exactly the
                // silent failure this option exists to remove, so make the broken wiring loud.
                Preconditions.checkState(
                        confirmation != null,
                        "awaitAckConfirmation is set but the subscriber was built without"
                                + " acknowledgement responses.");
                confirmations.add(confirmation);
            }
        }
        metrics.messagesAcked(toAck.size());
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
            Iterator<Map.Entry<Long, List<TrackedAck>>> checkpointed =
                    checkpoints.entrySet().iterator();
            while (checkpointed.hasNext()) {
                Map.Entry<Long, List<TrackedAck>> entry = checkpointed.next();
                drainSplit(entry.getValue(), splitId, toNack);
                if (entry.getValue().isEmpty()) {
                    checkpointed.remove();
                }
            }
        }
        toNack.forEach(AckHandle::nack);
        metrics.messagesNacked(toNack.size());
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

    /** An acknowledgement handle together with the split it belongs to. */
    private static final class TrackedAck {

        private final String splitId;
        private final AckHandle handle;

        private TrackedAck(String splitId, AckHandle handle) {
            this.splitId = splitId;
            this.handle = handle;
        }
    }
}
