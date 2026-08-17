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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reads the rows of the streams this subtask was assigned, a bounded batch at a time.
 *
 * <p>Rows are decoded straight into the batch handed to the task thread: a response block holds up
 * to about 128 MiB, and materializing one into a list before copying it into a batch would double
 * both the memory and the work. The per-fetch cap is what bounds a batch, so a checkpoint can be
 * taken between two fetches of one large block.
 *
 * <p>Only one stream is open at a time. The enumerator hands out one split per request, so a second
 * one only arrives after this reader reported the first finished; a queue is kept regardless,
 * because a {@code SplitReader} may be handed several at once.
 */
@Internal
public class BigQuerySplitReader implements SplitReader<GenericRecord, BigQueryReadStreamSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySplitReader.class);

    private final RowStreamOpener opener;
    private final int maxRecordsPerFetch;
    private final BigQuerySourceReaderMetrics metrics;
    @Nullable private final Schema readerSchema;

    private final Deque<BigQueryReadStreamSplit> queued = new ArrayDeque<>();

    /**
     * Volatile because {@link #wakeUp()} runs on the task thread while {@code fetch()} is in flight
     * on the fetcher thread: the fetcher holds no lock the task thread shares, so a plain field
     * would let the wake-up read a stale {@code null} and never cancel the call it was meant to
     * interrupt.
     */
    @Nullable private volatile ActiveStream active;

    /**
     * Creates the split reader.
     *
     * @param opener opens the {@code ReadRows} calls; shared with this subtask's other split
     *     readers and closed by the source reader, not here
     * @param maxRecordsPerFetch the most rows one fetch hands to the task thread
     * @param readerSchema the schema rows are decoded into, or {@code null} for the session's own
     * @param metrics the reader's metrics
     */
    public BigQuerySplitReader(
            RowStreamOpener opener,
            int maxRecordsPerFetch,
            @Nullable Schema readerSchema,
            BigQuerySourceReaderMetrics metrics) {
        Preconditions.checkArgument(
                maxRecordsPerFetch > 0,
                "maxRecordsPerFetch must be positive: %s",
                maxRecordsPerFetch);
        this.opener = Preconditions.checkNotNull(opener, "opener must not be null");
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.readerSchema = readerSchema;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public RecordsWithSplitIds<GenericRecord> fetch() throws IOException {
        RecordsBySplits.Builder<GenericRecord> batch = new RecordsBySplits.Builder<>();
        if (active == null) {
            takeNextSplit();
        }
        ActiveStream stream = active;
        if (stream == null) {
            return batch.build();
        }
        if (stream.needsReopen()) {
            stream.reopen();
        }

        int emitted = 0;
        while (emitted < maxRecordsPerFetch) {
            if (!stream.cursor.hasNext()) {
                ReadRowsResponse response;
                try {
                    response = stream.stream.next();
                } catch (RuntimeException e) {
                    if (stream.cancelled) {
                        // wakeUp() cancelled the call. Hand over what was decoded; the next fetch
                        // reopens the stream past the rows already handed over.
                        break;
                    }
                    // Nothing decoded in this fetch is handed over: the batch is dropped with the
                    // exception, so the rows counted into deliveredOffset above go nowhere and the
                    // reader restarts from the offset its last checkpoint holds. That is what makes
                    // a failure here lose no row and duplicate none.
                    throw new IOException(readFailureMessage(stream, e), e);
                }
                if (response == null) {
                    finish(batch, stream);
                    break;
                }
                metrics.rowsRead(response.getRowCount());
                metrics.bytesRead(response.getAvroRows().getSerializedBinaryRows().size());
                stream.cursor.reset(response);
                continue;
            }
            batch.add(stream.split.getStreamName(), stream.cursor.next());
            emitted++;
            stream.deliveredOffset++;
        }
        return batch.build();
    }

    /**
     * Builds the message a failed read is reported with.
     *
     * <p>Two things are added to the stream and the offset, and neither claims anything about what
     * BigQuery answered. The status code is named when the failure carries one, so the failure can
     * be grepped for and looked up. And a session already past its expiry is called out, because
     * that failure is the one a restart cannot fix: the reader would resume against the same
     * expired session, so the job has to start over and create a new one. Whether the service
     * reports expiry as this particular failure is not asserted — the clock is read here, and the
     * sentence only says what is true of the session either way.
     */
    private static String readFailureMessage(ActiveStream stream, Throwable failure) {
        StringBuilder message =
                new StringBuilder("Failed to read from the BigQuery read stream ")
                        .append(stream.split.getStreamName())
                        .append(" at offset ")
                        .append(stream.deliveredOffset);
        ExceptionUtils.findThrowable(failure, cause -> StatusCodes.codeOf(cause) != null)
                .map(StatusCodes::codeOf)
                .ifPresent(code -> message.append(" (status ").append(code).append(')'));
        message.append('.');
        Instant expireTime = stream.split.getSessionExpireTime();
        if (expireTime != null && Instant.now().isAfter(expireTime)) {
            message.append(" The read session expired at ")
                    .append(expireTime)
                    .append(". A BigQuery read session lives six hours from its creation, and a")
                    .append(" read that outlives it cannot be resumed: restarting reads against")
                    .append(" the same expired session, so the job has to be started over for a")
                    .append(" new session to be created. Read the table with more parallelism, or")
                    .append(" fewer columns, so that it finishes inside that window.");
        }
        return message.toString();
    }

    /**
     * Makes the next queued split active.
     *
     * <p>A split restored at exactly its stream's row count is opened like any other: BigQuery
     * answers with an empty stream and no error (measured 2026-08-09), which the fetch loop reports
     * as a finished split.
     */
    private void takeNextSplit() {
        BigQueryReadStreamSplit split = queued.poll();
        active = split == null ? null : new ActiveStream(split);
    }

    /** Reports the active stream finished and releases it. */
    private void finish(RecordsBySplits.Builder<GenericRecord> batch, ActiveStream stream) {
        LOG.info(
                "Read stream {} ended after {} row(s) handed over by this reader.",
                stream.split.getStreamName(),
                stream.deliveredOffset);
        batch.addFinishedSplit(stream.split.getStreamName());
        stream.close();
        active = null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<BigQueryReadStreamSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            queued.addAll(splitsChanges.splits());
        } else if (splitsChanges instanceof SplitsRemoval) {
            for (BigQueryReadStreamSplit split : splitsChanges.splits()) {
                removeSplit(split);
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChanges);
        }
    }

    private void removeSplit(BigQueryReadStreamSplit split) {
        queued.removeIf(queuedSplit -> queuedSplit.splitId().equals(split.splitId()));
        ActiveStream stream = active;
        if (stream != null && stream.split.splitId().equals(split.splitId())) {
            stream.close();
            active = null;
        }
    }

    @Override
    public void wakeUp() {
        ActiveStream stream = active;
        if (stream != null) {
            stream.cancel();
        }
    }

    @Override
    public void close() throws Exception {
        ActiveStream stream = active;
        active = null;
        // The opener is shared with this subtask's other split readers and is closed once, by the
        // source reader.
        Closers.closeAll(stream);
    }

    /** One open {@code ReadRows} call, and how far into its stream this reader has handed rows. */
    private final class ActiveStream implements AutoCloseable {

        private final BigQueryReadStreamSplit split;
        private final AvroRowCursor cursor;

        /** Rows of this stream handed to the task thread, including those still in the queue. */
        private long deliveredOffset;

        /** Volatile for the same reason {@code active} is: {@link #cancel()} reads it. */
        @Nullable private volatile RowStream stream;

        /** Set by {@link #wakeUp()} on the task thread, read by the fetcher thread. */
        private volatile boolean cancelled;

        private ActiveStream(BigQueryReadStreamSplit split) {
            this.split = split;
            this.cursor =
                    new AvroRowCursor(
                            new Schema.Parser().parse(split.getAvroSchemaJson()), readerSchema);
            this.deliveredOffset = split.getOffset();
        }

        private boolean needsReopen() {
            return stream == null || cancelled;
        }

        private void reopen() throws IOException {
            close();
            cancelled = false;
            // Reopening at deliveredOffset, not at the split state's offset: the rows between the
            // two are in the element queue on their way to the emitter, and re-reading them here
            // would hand them over twice.
            cursor.discard();
            LOG.info(
                    "Opening read stream {} at offset {}.", split.getStreamName(), deliveredOffset);
            RowStream opened = opener.open(split.getStreamName(), deliveredOffset);
            stream = opened;
            if (cancelled) {
                // A wake-up that landed between clearing the flag and opening the call found no
                // stream to cancel. Cancelling here is what keeps the fetch below from blocking on
                // a call nothing will interrupt.
                opened.cancel();
            }
        }

        private void cancel() {
            cancelled = true;
            RowStream open = stream;
            if (open != null) {
                open.cancel();
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
