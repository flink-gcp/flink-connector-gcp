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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reads the row-key ranges this subtask was assigned, a bounded batch at a time.
 *
 * <p>Only one range is open at a time. The enumerator hands out one split per request, so a second
 * only arrives after this reader reported the first finished; a queue is kept regardless, because a
 * {@code SplitReader} may be handed several at once.
 *
 * <p>The per-fetch cap is a correctness floor rather than a tuning knob: without it a fetch would
 * not return until a whole range had been read, and no checkpoint could be taken in between. It is
 * a constructor parameter so a test can make it small, and it is seeded from a constant rather than
 * a builder option — the client already hands rows over one at a time, so nothing here is
 * workload-dependent in the way a response-block size would be.
 *
 * <p><b>A cancelled stream and an ended stream look the same, and telling them apart is this
 * class's job.</b> Read out of gax 2.82.0 on 2026-08-09: {@code ServerStream.cancel()} sets a flag
 * after which the iterator's {@code hasNext()} returns false, exactly as at a clean end — and if
 * the fetcher was already blocked waiting for a response, the cancellation instead arrives as a
 * buffered error and {@code hasNext()} throws. So a wake-up can end a fetch either way, and in
 * neither case is the split finished. The cancelled flag is what decides, never the stream's
 * behaviour.
 */
@Internal
public class BigtableSplitReader implements SplitReader<Row, RowRangeSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSplitReader.class);

    /**
     * The most rows one fetch hands to the task thread, unless a test lowers it.
     *
     * <p>A correctness floor rather than tuning: without a cap a fetch would not return until a
     * whole range had been read, and no checkpoint could be taken in between. It is not a builder
     * option because nothing about it is workload-dependent — the client hands rows over one at a
     * time, so this bounds a batch rather than a buffer, and a measurement rather than a preference
     * is what would turn it into a knob.
     */
    public static final int DEFAULT_MAX_ROWS_PER_FETCH = 1000;

    private final TableDestination table;
    private final RowStreamOpener opener;
    @Nullable private final Filters.Filter filter;
    private final int maxRowsPerFetch;
    private final BigtableSourceReaderMetrics metrics;

    private final Deque<RowRangeSplit> queued = new ArrayDeque<>();

    /**
     * Volatile because {@link #wakeUp()} runs on the task thread while {@code fetch()} is in flight
     * on the fetcher thread: the fetcher holds no lock the task thread shares, so a plain field
     * would let the wake-up read a stale {@code null} and never cancel the call it was meant to
     * interrupt.
     */
    @Nullable private volatile ActiveRange active;

    /**
     * A wake-up that arrived with no stream to cancel.
     *
     * <p>Held on the reader rather than on the active range, because the two windows that need it
     * have no range to hold it: a wake-up landing before {@code fetch()} has taken a split off the
     * queue has nowhere to record itself, and one landing after that but before the reopen clears
     * the range's own flag would be erased by the clear. Either way the fetch would then block on a
     * freshly opened call that nothing will interrupt, and a cancelling job would wait for a first
     * row that may be a long time coming.
     */
    private volatile boolean pendingWakeUp;

    /**
     * Creates the split reader.
     *
     * @param table the table being read
     * @param opener opens the {@code ReadRows} calls; shared with this subtask's other split
     *     readers and closed by the source reader, not here
     * @param filter the server-side filter to apply, or {@code null} for none
     * @param maxRowsPerFetch the most rows one fetch hands to the task thread
     * @param metrics the reader's metrics
     */
    public BigtableSplitReader(
            TableDestination table,
            RowStreamOpener opener,
            @Nullable Filters.Filter filter,
            int maxRowsPerFetch,
            BigtableSourceReaderMetrics metrics) {
        Preconditions.checkArgument(
                maxRowsPerFetch > 0, "maxRowsPerFetch must be positive: %s", maxRowsPerFetch);
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        this.filter = filter;
        this.maxRowsPerFetch = maxRowsPerFetch;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        RecordsBySplits.Builder<Row> batch = new RecordsBySplits.Builder<>();
        if (active == null) {
            RowRangeSplit split = queued.poll();
            if (split == null) {
                return batch.build();
            }
            active = new ActiveRange(split);
        }
        ActiveRange range = active;
        if (pendingWakeUp) {
            // A wake-up arrived with nothing open to cancel. Returning control is what it asked
            // for; the next fetch opens where this one would have.
            pendingWakeUp = false;
            return batch.build();
        }

        if (range.isExhausted()) {
            // A range truncated to nothing — the normal state of a split whose last row was
            // emitted before the checkpoint. Reporting it finished without opening a stream is what
            // keeps an inverted range from ever reaching the service, which refuses one with
            // INVALID_ARGUMENT rather than answering it empty (measured 2026-08-10, #481).
            LOG.info(
                    "Split {} has nothing left to read; finishing it without opening a stream.",
                    range.split.splitId());
            finish(batch, range);
            return batch.build();
        }
        if (range.needsReopen()) {
            boolean opened;
            try {
                opened = range.reopen();
            } catch (RuntimeException e) {
                throw new IOException(
                        "Failed to open a read of "
                                + table
                                + " over "
                                + RowRanges.format(range.remaining())
                                + ".",
                        e);
            }
            if (!opened) {
                // A wake-up landed between this range becoming active and the call being opened.
                // Hand over nothing and return control; the next fetch opens where this one would
                // have. Opening anyway would block on a call nobody has asked to interrupt.
                return batch.build();
            }
        }

        int emitted = 0;
        while (emitted < maxRowsPerFetch) {
            Row row;
            try {
                row = range.read();
            } catch (RuntimeException e) {
                if (range.cancelled) {
                    // A wake-up landed while the fetcher was blocked, and the cancellation came
                    // back as an error rather than as an end of stream. Hand over what was read;
                    // the next fetch reopens past the rows already handed over.
                    range.wakeUpServed();
                    break;
                }
                throw new IOException(
                        "Failed to read "
                                + table
                                + " over "
                                + RowRanges.format(range.remaining())
                                + ".",
                        e);
            }
            if (row == null) {
                if (range.cancelled) {
                    range.wakeUpServed();
                    break;
                }
                finish(batch, range);
                break;
            }
            metrics.rowRead();
            batch.add(range.split.splitId(), row);
            emitted++;
            range.deliveredKey = row.getKey();
        }
        return batch.build();
    }

    /** Reports the active range finished and releases its stream. */
    private void finish(RecordsBySplits.Builder<Row> batch, ActiveRange range) {
        batch.addFinishedSplit(range.split.splitId());
        range.close();
        active = null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RowRangeSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            queued.addAll(splitsChanges.splits());
        } else if (splitsChanges instanceof SplitsRemoval) {
            for (RowRangeSplit split : splitsChanges.splits()) {
                removeSplit(split);
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChanges);
        }
    }

    private void removeSplit(RowRangeSplit split) {
        queued.removeIf(queuedSplit -> queuedSplit.splitId().equals(split.splitId()));
        ActiveRange range = active;
        if (range != null && range.split.splitId().equals(split.splitId())) {
            range.close();
            active = null;
        }
    }

    @Override
    public void wakeUp() {
        ActiveRange range = active;
        if (range == null) {
            // Nothing open yet. Recording it is what keeps the fetch that is about to open a call
            // from blocking on one nobody asked to be interrupted.
            pendingWakeUp = true;
            return;
        }
        range.cancel();
    }

    @Override
    public void close() throws Exception {
        ActiveRange range = active;
        active = null;
        // The opener is shared with this subtask's other split readers and is closed once, by the
        // source reader.
        Closers.closeAll(range);
    }

    /** One assigned range, the call open over it, and how far this reader has handed rows on. */
    private final class ActiveRange implements AutoCloseable {

        private final RowRangeSplit split;

        /**
         * The last row key handed to the task thread, including rows still in the element queue.
         *
         * <p>Not the same clock as the split state's last emitted key, which the record emitter
         * advances as the task thread drains that queue. A reopen must resume from this one, or
         * every row still in flight is handed over a second time inside a single successful run.
         */
        @Nullable private ByteString deliveredKey;

        /** Volatile for the same reason {@link #active} is: {@link #cancel()} reads it. */
        @Nullable private volatile RowStream stream;

        /** Set by {@link #wakeUp()} on the task thread, read by the fetcher thread. */
        private volatile boolean cancelled;

        private ActiveRange(RowRangeSplit split) {
            this.split = split;
        }

        /** Returns the range still to read: the split's, resumed past whatever was handed over. */
        private ByteStringRange remaining() {
            ByteStringRange range = split.getRange();
            return deliveredKey == null ? range : RowRanges.truncateStartOpen(range, deliveredKey);
        }

        private boolean isExhausted() {
            return RowRanges.isEmpty(remaining());
        }

        private boolean needsReopen() {
            return stream == null || cancelled;
        }

        /**
         * Records that the fetch loop has honoured a cancellation.
         *
         * <p>What distinguishes a wake-up that has been acted on from one that has not. Clearing
         * the flag here — with the stream released, so the next fetch reopens — is what keeps
         * {@link #reopen()} from refusing to open a call for a wake-up that was already served by
         * breaking out of the loop.
         */
        private void wakeUpServed() {
            close();
            cancelled = false;
        }

        /**
         * Opens the call for the range that is left, unless a wake-up has already asked for one not
         * to be.
         *
         * <p>The flag is read <em>before</em> it is cleared, and that order is the whole point: a
         * wake-up that landed after this range became active but before the call was opened has
         * nothing to cancel, so clearing the flag first would erase it and the fetch below would
         * then block on a call nothing will interrupt. Clearing it here rather than leaving it set
         * is what lets the next fetch open normally.
         *
         * @return false when a wake-up was pending, in which case no call was opened
         */
        private boolean reopen() throws IOException {
            close();
            if (cancelled) {
                cancelled = false;
                return false;
            }
            ByteStringRange range = remaining();
            LOG.info("Opening a read of {} over {}.", table, RowRanges.format(range));
            RowStream opened = opener.open(table, range, filter);
            stream = opened;
            if (cancelled) {
                // A wake-up that landed while the call was being opened. Cancelling here is what
                // keeps the read below from blocking; the loop sees the flag and hands over what it
                // has rather than reporting the split finished.
                opened.close();
            }
            return true;
        }

        @Nullable
        private Row read() {
            RowStream open = stream;
            return open == null ? null : open.next();
        }

        private void cancel() {
            cancelled = true;
            RowStream open = stream;
            if (open != null) {
                open.close();
            }
        }

        @Override
        public void close() {
            RowStream open = stream;
            stream = null;
            if (open != null) {
                open.close();
            }
        }
    }
}
