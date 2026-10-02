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

package io.github.flink.gcp.connector.firestore.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.batch.FetchedDocument;
import io.github.flink.gcp.connector.firestore.source.batch.QueryCursors;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Reads the queries this subtask was assigned, one page per fetch.
 *
 * <p>A page is one request: the split's query continued after the last document this reader handed
 * over, limited to the page size or to what is left of the query's own {@code limit}, whichever is
 * smaller. A page shorter than it was allowed to be is the end of the split. The page size bounds
 * what one fetch hands to Flink's element queue. A fetch is one request, which is why {@link
 * #wakeUp()} interrupts nothing: the fetch in flight returns when its request does, and the wait
 * for the request honours a thread interrupt.
 *
 * <p>Only one split is read at a time. The enumerator hands out one split per request, so a second
 * only arrives after this reader reported the first finished; a queue is kept regardless, because a
 * {@code SplitReader} may be handed several at once.
 */
@Internal
public class FirestoreSplitReader implements SplitReader<FetchedDocument, QuerySplit> {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSplitReader.class);

    private final DatabaseDestination database;
    private final QueryPageReader pageReader;
    private final int pageSize;
    private final FirestoreSourceReaderMetrics metrics;

    private final Deque<QuerySplit> queued = new ArrayDeque<>();

    @Nullable private ActiveSplit active;

    /**
     * Creates the split reader.
     *
     * @param database the database being read
     * @param pageReader reads the pages; shared with this subtask's other split readers and closed
     *     by the source reader, not here
     * @param pageSize the most documents one request asks for
     * @param metrics the reader's metrics
     */
    public FirestoreSplitReader(
            DatabaseDestination database,
            QueryPageReader pageReader,
            int pageSize,
            FirestoreSourceReaderMetrics metrics) {
        Preconditions.checkArgument(pageSize > 0, "pageSize must be positive: %s", pageSize);
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.pageReader = Preconditions.checkNotNull(pageReader, "pageReader must not be null");
        this.pageSize = pageSize;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public RecordsWithSplitIds<FetchedDocument> fetch() throws IOException {
        RecordsBySplits.Builder<FetchedDocument> batch = new RecordsBySplits.Builder<>();
        if (active == null) {
            QuerySplit split = queued.poll();
            if (split == null) {
                return batch.build();
            }
            if (split.isExhausted()) {
                // The normal state of a split whose limit was used up before the checkpoint.
                LOG.info("Split {} has nothing left to read; finishing it.", split.splitId());
                batch.addFinishedSplit(split.splitId());
                return batch.build();
            }
            active = new ActiveSplit(split, pageReader.query(database, split.getQuery()));
        }
        ActiveSplit split = active;

        int pageLimit = split.pageLimit();
        Query page;
        try {
            page =
                    split.lastDelivered == null
                            ? split.query
                            : QueryCursors.after(split.query, split.lastDelivered);
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        List<? extends DocumentSnapshot> documents;
        try {
            documents = pageReader.read(page.limit(pageLimit), split.split.getReadTime());
        } catch (IOException e) {
            throw new IOException(
                    "Failed to read split "
                            + split.split.splitId()
                            + " of "
                            + database
                            + " at read time "
                            + split.split.getReadTime()
                            + ". If the cause concerns the read time: the service keeps versions"
                            + " for an hour, or, with point-in-time recovery, for seven days at"
                            + " whole minutes.",
                    e);
        }
        if (documents.size() > pageLimit) {
            // The client library retries a page broken mid-stream from its last document with the
            // page's limit unchanged, so a retried page can hold more than it asked for. What
            // follows the limit is read again by the next page.
            documents = documents.subList(0, pageLimit);
        }
        for (DocumentSnapshot document : documents) {
            metrics.documentRead();
            batch.add(split.split.splitId(), new FetchedDocument(document, split.query));
        }
        if (!documents.isEmpty()) {
            split.lastDelivered = documents.get(documents.size() - 1);
            split.delivered += documents.size();
        }
        if (documents.size() < pageLimit || split.pageLimit() == 0) {
            batch.addFinishedSplit(split.split.splitId());
            active = null;
        }
        return batch.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<QuerySplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            queued.addAll(splitsChanges.splits());
        } else if (splitsChanges instanceof SplitsRemoval) {
            for (QuerySplit split : splitsChanges.splits()) {
                queued.removeIf(queuedSplit -> queuedSplit.splitId().equals(split.splitId()));
                if (active != null && active.split.splitId().equals(split.splitId())) {
                    active = null;
                }
            }
        } else {
            throw new IllegalArgumentException("Unsupported split change: " + splitsChanges);
        }
    }

    /**
     * Does nothing: a fetch is one request and returns when that request does, so a wake-up waits
     * for the request rather than cancelling it.
     */
    @Override
    public void wakeUp() {}

    @Override
    public void close() {
        // The page reader is shared with this subtask's other split readers and is closed once, by
        // the source reader.
        active = null;
    }

    /** One assigned split, and how far this reader has handed its documents on. */
    private final class ActiveSplit {

        private final QuerySplit split;
        private final Query query;

        /** The split's limit, or {@code null} when its query has none. */
        @Nullable private final Integer limit;

        /**
         * The last document handed to the task thread, including documents still in the element
         * queue — not the split state's last emitted document, which trails it.
         */
        @Nullable private DocumentSnapshot lastDelivered;

        private long delivered;

        private ActiveSplit(QuerySplit split, Query query) {
            this.split = split;
            this.query = query;
            this.limit = QueryCursors.limitOf(query);
        }

        /** Returns the most documents the next page may hold. */
        private int pageLimit() {
            return limit == null
                    ? pageSize
                    : (int) Math.min(pageSize, Math.max(0, limit - delivered));
        }
    }
}
