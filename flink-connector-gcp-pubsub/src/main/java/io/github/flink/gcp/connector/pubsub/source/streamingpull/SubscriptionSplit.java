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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A unit of work for one reader subtask: a streaming-pull connection to one subscription.
 *
 * <p>The split carries <em>no progress state</em>. Pub/Sub has no partition offset or cursor a
 * reader could resume from — delivery state lives on the server, and a message that is not
 * acknowledged is redelivered. Consequently a split is fully described by its subscription and a
 * uid, restoring a split costs nothing, and returned splits can simply be re-derived.
 *
 * <p>The uid distinguishes several splits that target the same subscription, which the source
 * creates when parallelism exceeds the subscription count under {@link
 * io.github.flink.gcp.connector.pubsub.source.OrderingMode#NONE}. Under {@code PER_KEY} a
 * subscription appears in exactly one split, so that its ordering keys are never spread across
 * reader subtasks.
 *
 * <p>A split of the second kind, a <em>ledger chunk</em>, consumes nothing. It carries part of the
 * restore-deduplication ledger: the ids of messages a checkpoint covers but whose acknowledgement
 * may not have reached Pub/Sub. Readers checkpoint only ledger chunks, never consuming splits, so
 * the enumerator stays the sole owner of consumption (ADR-0011); a reader that is handed a restored
 * chunk forwards it to the enumerator instead of subscribing. The chunk's split id cannot collide
 * with a consuming split's, whose uid is a plan index.
 */
@Internal
public final class SubscriptionSplit implements SourceSplit, Serializable {

    /** Serializable so that a ledger chunk can travel inside a {@link LedgerChunkEvent}. */
    private static final long serialVersionUID = 1L;

    private final SubscriptionDestination subscription;
    private final String uid;
    private final String splitId;
    private final boolean ledgerChunkKind;

    /**
     * The encoded ids, or {@code null} for a consuming split or once released. Not final: Flink's
     * {@code SourceOperator} keeps every restored split for the lifetime of the execution, so a
     * restored chunk would otherwise pin its payload long after the ledger expired.
     */
    @Nullable private byte[] ledgerChunk;

    /**
     * Creates a consuming split.
     *
     * @param subscription the subscription to consume
     * @param uid distinguishes splits sharing a subscription; unique within one job
     */
    public SubscriptionSplit(SubscriptionDestination subscription, String uid) {
        this(subscription, uid, null);
    }

    private SubscriptionSplit(
            SubscriptionDestination subscription, String uid, @Nullable byte[] ledgerChunk) {
        this.subscription = Preconditions.checkNotNull(subscription, "subscription");
        this.uid = Preconditions.checkNotNull(uid, "uid");
        this.ledgerChunk = ledgerChunk;
        this.ledgerChunkKind = ledgerChunk != null;
        this.splitId =
                subscription.toSubscriptionPath() + (ledgerChunk == null ? "#" : "#ledger:") + uid;
    }

    /**
     * Creates a ledger chunk.
     *
     * @param subscription the subscription whose message ids the chunk holds
     * @param uid distinguishes the chunk from every other chunk of the same checkpoint
     * @param chunk the ids, encoded by {@link MessageIdLedgerCodec}
     * @return the chunk split
     */
    public static SubscriptionSplit ledgerChunk(
            SubscriptionDestination subscription, String uid, byte[] chunk) {
        return new SubscriptionSplit(subscription, uid, Preconditions.checkNotNull(chunk, "chunk"));
    }

    /** Returns whether this split is a ledger chunk rather than a subscription to consume. */
    public boolean isLedgerChunk() {
        return ledgerChunkKind;
    }

    /**
     * Returns the encoded ledger chunk.
     *
     * @throws IllegalStateException if this is a consuming split
     */
    public byte[] getLedgerChunk() {
        Preconditions.checkState(ledgerChunkKind, "%s is not a ledger chunk.", splitId);
        Preconditions.checkState(
                ledgerChunk != null, "The payload of %s has been released.", splitId);
        return ledgerChunk;
    }

    /**
     * Returns a copy of this ledger chunk and drops this instance's reference to the payload, so
     * that the instance Flink retains no longer holds it.
     *
     * @return a ledger chunk with the same id and payload
     * @throws IllegalStateException if this is a consuming split or was already released
     */
    public SubscriptionSplit takeLedgerChunk() {
        SubscriptionSplit copy = ledgerChunk(subscription, uid, getLedgerChunk());
        ledgerChunk = null;
        return copy;
    }

    /** Returns the subscription this split consumes. */
    public SubscriptionDestination getSubscription() {
        return subscription;
    }

    /** Returns the uid distinguishing this split from others on the same subscription. */
    public String getUid() {
        return uid;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionSplit that = (SubscriptionSplit) o;
        return splitId.equals(that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }

    @Override
    public String toString() {
        if (!ledgerChunkKind) {
            return "SubscriptionSplit{" + splitId + "}";
        }
        return ledgerChunk == null
                ? "SubscriptionSplit{" + splitId + ", released}"
                : "SubscriptionSplit{" + splitId + ", " + ledgerChunk.length + " ledger bytes}";
    }
}
