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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.DecimalMessageIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The message ids a restored checkpoint covers, held by a reader so that their redeliveries are
 * acknowledged instead of emitted (#2049).
 *
 * <p>A job restores only from a completed checkpoint, so every id in the ledger had its records
 * committed by that checkpoint, whether or not the acknowledgement reached Pub/Sub before the
 * previous execution stopped. Suppressing a redelivery of such a message is therefore never loss.
 * The reverse failure — an entry that is missing, late or already expired — leaves the duplicate
 * that the source produces without this ledger.
 *
 * <p>An entry is not removed when it first matches: one message can be redelivered more than once
 * (a redelivery to another stream, or a second lease expiry), and every copy is covered. The whole
 * ledger is dropped once its time-to-live has run out, at the next lookup or checkpoint. The clock
 * starts when the reader is first handed a subscription to consume, because nothing can be
 * redelivered to it before then.
 *
 * <p>Task-thread confined, except {@link #size()}, which the metric reporter reads.
 */
@Internal
public final class RestoredLedger {

    private static final Logger LOG = LoggerFactory.getLogger(RestoredLedger.class);

    private final Duration timeToLive;
    private final LongSupplier nanoClock;
    private final PubSubSourceReaderMetrics metrics;

    /** The entries of each subscription, by subscription path. */
    private final Map<String, SubscriptionIds> entries = new HashMap<>();

    private volatile int size;

    private long expiresAtNanos;
    private boolean clockStarted;
    private boolean expired;

    /**
     * Creates an empty ledger.
     *
     * @param timeToLive how long the ledger is kept once the reader starts consuming
     * @param nanoClock a monotonic clock in nanoseconds
     * @param metrics counts suppressions and expiries
     */
    public RestoredLedger(
            Duration timeToLive, LongSupplier nanoClock, PubSubSourceReaderMetrics metrics) {
        Preconditions.checkArgument(
                !timeToLive.isNegative() && !timeToLive.isZero(), "timeToLive must be positive");
        this.timeToLive = timeToLive;
        this.nanoClock = nanoClock;
        this.metrics = metrics;
    }

    /**
     * Adds message ids of one subscription. Ignored once the ledger has expired, since an entry
     * arriving then is already outside the window it was kept for.
     *
     * @param subscription the subscription the ids were received on
     * @param messageIds the ids
     */
    public void add(SubscriptionDestination subscription, Collection<String> messageIds) {
        if (expired) {
            return;
        }
        SubscriptionIds ids =
                entries.computeIfAbsent(
                        subscription.toSubscriptionPath(), key -> new SubscriptionIds());
        for (String id : messageIds) {
            ids.add(id);
        }
        recount();
    }

    /** Starts the time-to-live, once; later calls do nothing. */
    public void startClock() {
        if (!clockStarted) {
            clockStarted = true;
            expiresAtNanos = nanoClock.getAsLong() + timeToLive.toNanos();
        }
    }

    /**
     * Returns whether a delivery is covered by the restored checkpoint, and counts it if so.
     *
     * @param subscription the subscription the message was received on
     * @param messageId the message id
     * @return {@code true} to acknowledge the delivery instead of emitting it
     */
    public boolean suppresses(SubscriptionDestination subscription, String messageId) {
        if (size == 0) {
            return false;
        }
        if (expireIfDue()) {
            return false;
        }
        SubscriptionIds ids = entries.get(subscription.toSubscriptionPath());
        if (ids != null && ids.match(messageId)) {
            metrics.redeliverySuppressed();
            return true;
        }
        return false;
    }

    /**
     * Drops the ledger if its time-to-live has run out. Lookups check this themselves; the reader
     * also calls it at every checkpoint, so a subscription that goes quiet after a restore does not
     * keep the ledger in memory indefinitely.
     *
     * @return whether the ledger is expired
     */
    public boolean expireIfDue() {
        if (expired) {
            return true;
        }
        if (clockStarted && nanoClock.getAsLong() - expiresAtNanos >= 0) {
            expire();
            return true;
        }
        return false;
    }

    /**
     * Returns how many entries the ledger holds, matched or not.
     *
     * @return the entry count
     */
    public int size() {
        return size;
    }

    private void expire() {
        long neverMatched = 0;
        for (SubscriptionIds ids : entries.values()) {
            neverMatched += ids.unmatched();
        }
        LOG.info(
                "The restored deduplication ledger expired after {}: {} entries were never"
                        + " redelivered to this subtask.",
                timeToLive,
                neverMatched);
        metrics.ledgerEntriesExpired(neverMatched);
        entries.clear();
        expired = true;
        size = 0;
    }

    private void recount() {
        int total = 0;
        for (SubscriptionIds ids : entries.values()) {
            total += ids.size();
        }
        size = total;
    }

    /**
     * One subscription's entries, each with whether it has matched yet.
     *
     * <p>Ids that {@link DecimalMessageIds} packs, the shape of every id the service was measured
     * assigning, live in an open-addressing table of {@code long}s: about 40 bytes an entry as
     * measured, against about 110 for a {@code HashSet<String>}, which matters because every reader
     * of a subscription holds all of its entries. Any other id is kept as a string.
     */
    private static final class SubscriptionIds {

        private static final long EMPTY = -1L;

        private long[] packed = newTable(16);
        private long[] packedMatched = new long[1];
        private int packedSize;
        private int packedMatchedCount;

        private final Set<String> unmatchedStrings = new HashSet<>();
        private final Set<String> matchedStrings = new HashSet<>();

        void add(String id) {
            long value = DecimalMessageIds.pack(id);
            if (value == DecimalMessageIds.NOT_PACKED) {
                if (!matchedStrings.contains(id)) {
                    unmatchedStrings.add(id);
                }
                return;
            }
            int slot = slotOf(packed, value);
            if (packed[slot] != EMPTY) {
                return;
            }
            if ((packedSize + 1) * 2 > packed.length) {
                grow();
                slot = slotOf(packed, value);
            }
            packed[slot] = value;
            packedSize++;
        }

        /** Returns whether the id is an entry, marking it matched. */
        boolean match(String id) {
            long value = DecimalMessageIds.pack(id);
            if (value == DecimalMessageIds.NOT_PACKED) {
                if (unmatchedStrings.remove(id)) {
                    matchedStrings.add(id);
                    return true;
                }
                return matchedStrings.contains(id);
            }
            int slot = slotOf(packed, value);
            if (packed[slot] == EMPTY) {
                return false;
            }
            long bit = 1L << slot;
            if ((packedMatched[slot >>> 6] & bit) == 0) {
                packedMatched[slot >>> 6] |= bit;
                packedMatchedCount++;
            }
            return true;
        }

        int size() {
            return packedSize + unmatchedStrings.size() + matchedStrings.size();
        }

        int unmatched() {
            return packedSize - packedMatchedCount + unmatchedStrings.size();
        }

        private void grow() {
            long[] old = packed;
            long[] oldMatched = packedMatched;
            packed = newTable(old.length * 2);
            packedMatched = new long[Math.max(1, packed.length >>> 6)];
            for (int i = 0; i < old.length; i++) {
                if (old[i] != EMPTY) {
                    int slot = slotOf(packed, old[i]);
                    packed[slot] = old[i];
                    if ((oldMatched[i >>> 6] & (1L << i)) != 0) {
                        packedMatched[slot >>> 6] |= 1L << slot;
                    }
                }
            }
        }

        private static long[] newTable(int capacity) {
            long[] table = new long[capacity];
            Arrays.fill(table, EMPTY);
            return table;
        }

        /** The slot holding {@code value}, or the empty slot where it would go. */
        private static int slotOf(long[] table, long value) {
            int mask = table.length - 1;
            // Multiplicative hashing spreads ids that differ only in a few digits across the table.
            int slot = (int) ((value * 0x9E3779B97F4A7C15L) >>> 32) & mask;
            while (table[slot] != EMPTY && table[slot] != value) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }
    }
}
