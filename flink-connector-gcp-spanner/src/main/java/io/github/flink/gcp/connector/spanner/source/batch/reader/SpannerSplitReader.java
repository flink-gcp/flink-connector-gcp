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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reads the partitions this subtask was assigned, a bounded batch at a time.
 *
 * <p>Only one partition is open at a time. The enumerator hands out one split per request, so a
 * second only arrives after this reader reported the first finished; a queue is kept regardless,
 * because a {@code SplitReader} may be handed several at once.
 *
 * <p>The per-fetch cap is a correctness floor rather than a tuning knob: without it a fetch would
 * not return until a whole partition had been read, and no checkpoint could be taken in between.
 *
 * <p><b>A cancelled read and a finished read look the same, and telling them apart is this class's
 * job.</b> Measured against the pinned emulator on 2026-08-10: closing a {@code ResultSet} from
 * another thread while a reader sits in {@code next()} ends that call <em>either</em> by returning
 * {@code false}, exactly as at a clean end, <em>or</em> by throwing {@code CANCELLED: User
 * cancelled stream}. Both shapes occur, so the cancelled flag is what decides whether a partition
 * finished, never the read's behaviour.
 *
 * <p><b>A wake-up costs a partition its progress.</b> Spanner exposes no position inside a
 * partition, so a cancelled one is opened again at its start and the rows it had already handed on
 * are delivered a second time. That is within this source's at-least-once contract — the same
 * duplicate window a restart has — but it happens in a run that never failed, so it is logged and
 * counted rather than left to be discovered downstream. Reading on instead was the alternative, and
 * it was declined: a fetch blocked on a service that has gone quiet would then hold the subtask
 * until the client's own read deadline, and a job being cancelled would wait for it.
 */
@Internal
public class SpannerSplitReader implements SplitReader<Struct, PartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSplitReader.class);

    /**
     * The most rows one fetch hands to the task thread, unless a test lowers it.
     *
     * <p>A correctness floor rather than tuning: without a cap a fetch would not return until a
     * whole partition had been read, and no checkpoint could be taken in between. It is not a
     * builder option because nothing about it is workload-dependent — the client hands rows over
     * one at a time, so this bounds a batch rather than a buffer, and a measurement rather than a
     * preference is what would turn it into a knob.
     */
    public static final int DEFAULT_MAX_ROWS_PER_FETCH = 1000;

    private final SpannerDatabase database;
    private final StructStreamOpener opener;
    private final int maxRowsPerFetch;
    private final SpannerSourceReaderMetrics metrics;

    private final Deque<PartitionSplit> queued = new ArrayDeque<>();

    /**
     * Volatile because {@link #wakeUp()} runs on the task thread while {@code fetch()} is in flight
     * on the fetcher thread: the fetcher holds no lock the task thread shares, so a plain field
     * would let the wake-up read a stale {@code null} and never cancel the read it was meant to
     * interrupt.
     */
    @Nullable private volatile ActivePartition active;

    /**
     * A wake-up that arrived with no read to cancel.
     *
     * <p>Held on the reader rather than on the active partition, because the two windows that need
     * it have no partition to hold it: a wake-up landing before {@code fetch()} has taken a split
     * off the queue has nowhere to record itself, and one landing after that but before the reopen
     * clears the partition's own flag would be erased by the clear. Either way the fetch would then
     * block on a freshly opened read that nothing will interrupt, and a cancelling job would wait
     * for a first row that may be a long time coming.
     */
    private volatile boolean pendingWakeUp;

    /**
     * Creates the split reader.
     *
     * @param database the database being read, for the messages
     * @param opener opens the reads; shared with this subtask's other split readers and closed by
     *     the source reader, not here
     * @param maxRowsPerFetch the most rows one fetch hands to the task thread
     * @param metrics the reader's metrics
     */
    public SpannerSplitReader(
            SpannerDatabase database,
            StructStreamOpener opener,
            int maxRowsPerFetch,
            SpannerSourceReaderMetrics metrics) {
        Preconditions.checkArgument(
                maxRowsPerFetch > 0, "maxRowsPerFetch must be positive: %s", maxRowsPerFetch);
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        this.maxRowsPerFetch = maxRowsPerFetch;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public RecordsWithSplitIds<Struct> fetch() throws IOException {
        RecordsBySplits.Builder<Struct> batch = new RecordsBySplits.Builder<>();
        if (active == null) {
            PartitionSplit split = queued.poll();
            if (split == null) {
                return batch.build();
            }
            active = new ActivePartition(split);
        }
        ActivePartition partition = active;
        if (pendingWakeUp) {
            // A wake-up arrived with nothing open to cancel. Returning control is what it asked
            // for; the next fetch opens where this one would have.
            pendingWakeUp = false;
            return batch.build();
        }

        if (partition.needsReopen()) {
            boolean opened;
            try {
                opened = partition.reopen();
            } catch (RuntimeException e) {
                throw new IOException(openFailureMessage(partition), e);
            }
            if (!opened) {
                // A wake-up landed between this partition becoming active and the read being
                // opened. Hand over nothing and return control; the next fetch opens where this one
                // would have. Opening anyway would block on a read nobody has asked to interrupt.
                return batch.build();
            }
        }

        int emitted = 0;
        while (emitted < maxRowsPerFetch) {
            Struct row;
            try {
                row = partition.read();
            } catch (RuntimeException e) {
                if (partition.cancelled) {
                    // A wake-up landed while the fetcher was blocked, and the cancellation came
                    // back as an error rather than as an end of read. Hand over what was read; the
                    // next fetch opens the partition again from its start.
                    partition.wakeUpServed();
                    break;
                }
                throw new IOException(readFailureMessage(partition), e);
            }
            if (row == null) {
                if (partition.cancelled) {
                    partition.wakeUpServed();
                    break;
                }
                finish(batch, partition);
                break;
            }
            metrics.rowRead();
            batch.add(partition.split.splitId(), row);
            emitted++;
            partition.delivered += 1;
        }
        return batch.build();
    }

    private String openFailureMessage(ActivePartition partition) {
        return "Failed to open " + partition.split + " of " + database + ".";
    }

    private String readFailureMessage(ActivePartition partition) {
        return "Failed to read " + partition.split + " of " + database + ".";
    }

    /** Reports the active partition finished and releases its read. */
    private void finish(RecordsBySplits.Builder<Struct> batch, ActivePartition partition) {
        batch.addFinishedSplit(partition.split.splitId());
        partition.close();
        active = null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PartitionSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            queued.addAll(splitsChanges.splits());
        } else if (splitsChanges instanceof SplitsRemoval) {
            for (PartitionSplit split : splitsChanges.splits()) {
                removeSplit(split);
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChanges);
        }
    }

    private void removeSplit(PartitionSplit split) {
        queued.removeIf(queuedSplit -> queuedSplit.splitId().equals(split.splitId()));
        ActivePartition partition = active;
        if (partition != null && partition.split.splitId().equals(split.splitId())) {
            partition.close();
            active = null;
        }
    }

    @Override
    public void wakeUp() {
        ActivePartition partition = active;
        if (partition == null) {
            // Nothing open yet. Recording it is what keeps the fetch that is about to open a read
            // from blocking on one nobody asked to be interrupted.
            pendingWakeUp = true;
            return;
        }
        partition.cancel();
    }

    @Override
    public void close() throws Exception {
        ActivePartition partition = active;
        active = null;
        // The opener is shared with this subtask's other split readers and is closed once, by the
        // source reader.
        Closers.closeAll(partition);
    }

    /** One assigned partition, and the read open over it. */
    private final class ActivePartition implements AutoCloseable {

        private final PartitionSplit split;

        /**
         * How many rows this reader has handed to the task thread from this partition.
         *
         * <p>Not a resume point — there is none — but the number that says whether reopening the
         * partition will deliver rows twice, and how many.
         */
        private long delivered;

        /** Volatile for the same reason {@link #active} is: {@link #cancel()} reads it. */
        @Nullable private volatile StructStream stream;

        /** Set by {@link #wakeUp()} on the task thread, read by the fetcher thread. */
        private volatile boolean cancelled;

        private ActivePartition(PartitionSplit split) {
            this.split = split;
        }

        private boolean needsReopen() {
            return stream == null || cancelled;
        }

        /**
         * Records that the fetch loop has honoured a cancellation.
         *
         * <p>What distinguishes a wake-up that has been acted on from one that has not. Clearing
         * the flag here — with the read released, so the next fetch reopens — is what keeps {@link
         * #reopen()} from refusing to open a read for a wake-up that was already served by breaking
         * out of the loop.
         */
        private void wakeUpServed() {
            close();
            cancelled = false;
        }

        /**
         * Opens the partition, unless a wake-up has already asked for one not to be.
         *
         * <p>The flag is read <em>before</em> it is cleared, and that order is the whole point: a
         * wake-up that landed after this partition became active but before the read was opened has
         * nothing to cancel, so clearing the flag first would erase it and the fetch below would
         * then block on a read nothing will interrupt. Clearing it here rather than leaving it set
         * is what lets the next fetch open normally.
         *
         * @return false when a wake-up was pending, in which case no read was opened
         */
        private boolean reopen() throws IOException {
            close();
            if (cancelled) {
                cancelled = false;
                return false;
            }
            if (delivered > 0) {
                metrics.partitionReread();
                LOG.warn(
                        "Opening {} of {} again from its start after a wake-up cancelled it; the"
                                + " {} row(s) it had already handed on are delivered a second"
                                + " time. Spanner exposes no position inside a partition to resume"
                                + " at, so a partition is this source's unit of re-reading.",
                        split,
                        database,
                        delivered);
            } else {
                LOG.info("Opening {} of {}.", split, database);
            }
            StructStream opened = opener.open(split.getBatchTransactionId(), split.getPartition());
            stream = opened;
            if (cancelled) {
                // A wake-up that landed while the read was being opened. Cancelling here is what
                // keeps the read below from blocking; the loop sees the flag and hands over what it
                // has rather than reporting the split finished.
                opened.close();
            }
            return true;
        }

        @Nullable
        private Struct read() {
            StructStream open = stream;
            return open == null ? null : open.next();
        }

        private void cancel() {
            cancelled = true;
            StructStream open = stream;
            if (open != null) {
                open.close();
            }
        }

        @Override
        public void close() {
            StructStream open = stream;
            stream = null;
            if (open != null) {
                open.close();
            }
        }
    }
}
