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

import com.google.cloud.pubsub.v1.AckReplyConsumer;

import javax.annotation.concurrent.GuardedBy;

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
 * <p>Fully synchronized: {@link #addPendingAck} runs on client-library callback threads while the
 * remaining methods run on the reader's task and fetcher threads.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * extended with per-split scoping and with settling of superseded redeliveries.
 */
@Internal
public class PubSubAckTracker implements AckTracker {

    /** Pending (received, not yet emitted) acknowledgements, per split and message id. */
    @GuardedBy("this")
    private final Map<String, Map<String, AckReplyConsumer>> pendingAcks = new HashMap<>();

    /** Emitted but not yet bound to a checkpoint. */
    @GuardedBy("this")
    private final List<TrackedAck> stagedAcks = new ArrayList<>();

    /** Bound to a checkpoint that has not completed yet, ordered by checkpoint id. */
    @GuardedBy("this")
    private final SortedMap<Long, List<TrackedAck>> checkpoints = new TreeMap<>();

    @Override
    public synchronized void addPendingAck(
            String splitId, String messageId, AckReplyConsumer ackReplyConsumer) {
        AckReplyConsumer superseded =
                pendingAcks
                        .computeIfAbsent(splitId, key -> new LinkedHashMap<>())
                        .put(messageId, ackReplyConsumer);
        if (superseded != null) {
            // The message was redelivered before the previous delivery was settled — its
            // acknowledgement id is already stale, but the client library only releases that
            // delivery's flow-control permit once the handle is settled.
            superseded.nack();
        }
    }

    @Override
    public synchronized void stagePendingAck(String splitId, String messageId) {
        Map<String, AckReplyConsumer> splitAcks = pendingAcks.get(splitId);
        if (splitAcks == null) {
            return;
        }
        AckReplyConsumer consumer = splitAcks.remove(messageId);
        if (consumer != null) {
            stagedAcks.add(new TrackedAck(splitId, consumer));
        }
        if (splitAcks.isEmpty()) {
            pendingAcks.remove(splitId);
        }
    }

    @Override
    public synchronized void addCheckpoint(long checkpointId) {
        checkpoints.put(checkpointId, new ArrayList<>(stagedAcks));
        stagedAcks.clear();
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        List<TrackedAck> toAck = new ArrayList<>();
        while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
            toAck.addAll(checkpoints.remove(checkpoints.firstKey()));
        }
        for (TrackedAck ack : toAck) {
            ack.consumer.ack();
        }
    }

    @Override
    public synchronized void nackSplit(String splitId) {
        Map<String, AckReplyConsumer> splitAcks = pendingAcks.remove(splitId);
        if (splitAcks != null) {
            splitAcks.values().forEach(AckReplyConsumer::nack);
        }
        nackAndRemove(stagedAcks, splitId);
        Iterator<Map.Entry<Long, List<TrackedAck>>> checkpointed =
                checkpoints.entrySet().iterator();
        while (checkpointed.hasNext()) {
            Map.Entry<Long, List<TrackedAck>> entry = checkpointed.next();
            nackAndRemove(entry.getValue(), splitId);
            if (entry.getValue().isEmpty()) {
                checkpointed.remove();
            }
        }
    }

    private static void nackAndRemove(List<TrackedAck> acks, String splitId) {
        Iterator<TrackedAck> iterator = acks.iterator();
        while (iterator.hasNext()) {
            TrackedAck ack = iterator.next();
            if (ack.splitId.equals(splitId)) {
                ack.consumer.nack();
                iterator.remove();
            }
        }
    }

    @Override
    public synchronized int outstandingAckCount() {
        int outstanding = stagedAcks.size();
        for (Map<String, AckReplyConsumer> splitAcks : pendingAcks.values()) {
            outstanding += splitAcks.size();
        }
        for (List<TrackedAck> checkpointed : checkpoints.values()) {
            outstanding += checkpointed.size();
        }
        return outstanding;
    }

    /** An acknowledgement handle together with the split it belongs to. */
    private static final class TrackedAck {

        private final String splitId;
        private final AckReplyConsumer consumer;

        private TrackedAck(String splitId, AckReplyConsumer consumer) {
            this.splitId = splitId;
            this.consumer = consumer;
        }
    }
}
