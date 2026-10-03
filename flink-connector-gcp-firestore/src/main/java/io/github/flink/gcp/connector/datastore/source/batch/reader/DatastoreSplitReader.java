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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reads the queries this subtask was assigned, one page per fetch.
 *
 * <p>A page is one {@code RunQuery} call: the split's query at the split's read time, started at
 * the cursor after what this reader has handed over, limited to the page size or to what is left of
 * the query's own {@code limit}, whichever is smaller, and carrying what is left of the query's own
 * {@code offset}. The service may answer with fewer entities than the limit, and says in the batch
 * whether more remain; the next page starts at the batch's end cursor. The page size bounds what
 * one fetch hands to Flink's element queue. A fetch is one call, which is why {@link #wakeUp()}
 * interrupts nothing: the fetch in flight returns when its call does.
 *
 * <p>The offset is the service's to apply, page by page: a batch reports how many entities it
 * skipped, and returns an entity only once the whole offset has been skipped. A call is one unary
 * request, so a retried call is the same request and skips the same entities.
 *
 * <p>Only one split is read at a time. The enumerator hands out one split per request, so a second
 * only arrives after this reader reported the first finished; a queue is kept regardless, because a
 * {@code SplitReader} may be handed several at once.
 */
@Internal
public class DatastoreSplitReader implements SplitReader<FetchedEntity, QuerySplit> {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSplitReader.class);

    private final DatabaseDestination database;
    private final QueryPageReader pageReader;
    private final int pageSize;
    private final DatastoreSourceReaderMetrics metrics;

    private final Deque<QuerySplit> queued = new ArrayDeque<>();

    @Nullable private ActiveSplit active;

    /**
     * Creates the split reader.
     *
     * @param database the database being read
     * @param pageReader reads the pages; shared with this subtask's other split readers and closed
     *     by the source reader, not here
     * @param pageSize the most entities one request asks for
     * @param metrics the reader's metrics
     */
    public DatastoreSplitReader(
            DatabaseDestination database,
            QueryPageReader pageReader,
            int pageSize,
            DatastoreSourceReaderMetrics metrics) {
        Preconditions.checkArgument(pageSize > 0, "pageSize must be positive: %s", pageSize);
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.pageReader = Preconditions.checkNotNull(pageReader, "pageReader must not be null");
        this.pageSize = pageSize;
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public RecordsWithSplitIds<FetchedEntity> fetch() throws IOException {
        RecordsBySplits.Builder<FetchedEntity> batch = new RecordsBySplits.Builder<>();
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
            active = new ActiveSplit(split);
        }
        ActiveSplit split = active;

        int pageLimit = split.pageLimit();
        QueryResultBatch page;
        try {
            page = pageReader.read(database, split.page(pageLimit));
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
        int count = page.getEntityResultsCount();
        if (count > pageLimit) {
            throw new IOException(
                    "Datastore answered a page of split "
                            + split.split.splitId()
                            + " limited to "
                            + pageLimit
                            + " entities with "
                            + count
                            + "; the split cannot be resumed past them.");
        }
        for (EntityResult result : page.getEntityResultsList()) {
            if (!result.getEntity().hasKey()) {
                // The API allows a projection result without a key; the deserializer takes an
                // Entity, which always carries one. Not observed against the emulator.
                throw new IOException(
                        "Datastore answered split "
                                + split.split.splitId()
                                + " with a projection result that has no key, which the"
                                + " deserializer contract, an Entity with its key, cannot carry."
                                + " Read the query without its projection.");
            }
            metrics.entityRead();
            batch.add(
                    split.split.splitId(),
                    new FetchedEntity(
                            split.projected
                                    ? ProjectedValues.toEntity(result.getEntity())
                                    : Entity.fromPb(result.getEntity()),
                            result.getCursor()));
        }
        boolean finished = split.advance(page);
        if (finished) {
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
     * Does nothing: a fetch is one call and returns when that call does, so a wake-up waits for the
     * call rather than cancelling it.
     */
    @Override
    public void wakeUp() {}

    @Override
    public void close() {
        // The page reader is shared with this subtask's other split readers and is closed once, by
        // the source reader.
        active = null;
    }

    /** One assigned split, and how far this reader has read it. */
    private final class ActiveSplit {

        private final QuerySplit split;

        /**
         * Whether the query projects properties, so that its index values are read back ({@link
         * ProjectedValues}); as the client library's own query API, only for such a query.
         */
        private final boolean projected;

        /** The split's limit, or {@code null} when its query has none. */
        @Nullable private final Integer limit;

        /**
         * Where the next page starts: the batch end cursor of the last page, which is past the
         * entities in the element queue too — not the split state's cursor, which trails it. Before
         * the first page, the split's own start cursor, or {@code null} when it has none.
         */
        @Nullable private ByteString cursor;

        /** What is left of the query's offset. */
        private int offset;

        private long delivered;

        private ActiveSplit(QuerySplit split) {
            this.split = split;
            Query query = split.getRequest().getQuery();
            this.limit = query.hasLimit() ? query.getLimit().getValue() : null;
            this.projected = query.getProjectionCount() > 0;
            this.offset = query.getOffset();
            this.cursor = query.getStartCursor().isEmpty() ? null : query.getStartCursor();
        }

        /** Returns the most entities the next page may hold. */
        private int pageLimit() {
            return limit == null
                    ? pageSize
                    : (int) Math.min(pageSize, Math.max(0, limit - delivered));
        }

        /** Builds the next page's request. */
        private RunQueryRequest page(int pageLimit) {
            RunQueryRequest.Builder request = split.getRequest().toBuilder();
            request.setReadOptions(
                    ReadOptions.newBuilder().setReadTime(split.getReadTime().toProto()));
            Query.Builder query = request.getQueryBuilder();
            if (cursor != null) {
                query.setStartCursor(cursor);
            }
            query.setOffset(offset).setLimit(Int32Value.of(pageLimit));
            return request.build();
        }

        /**
         * Moves past a page the service answered.
         *
         * @return whether the split is finished
         * @throws IOException if the service reported more results without moving
         */
        private boolean advance(QueryResultBatch page) throws IOException {
            int count = page.getEntityResultsCount();
            delivered += count;
            offset = Math.max(0, offset - page.getSkippedResults());
            if (pageLimit() == 0) {
                // The query's own limit is spent, whatever the service says may follow: the next
                // page would ask for no entity.
                return true;
            }
            switch (page.getMoreResults()) {
                case NO_MORE_RESULTS:
                case MORE_RESULTS_AFTER_CURSOR:
                    return true;
                case MORE_RESULTS_AFTER_LIMIT:
                case NOT_FINISHED:
                    break;
                default:
                    throw new IOException(
                            "Datastore answered a page of split "
                                    + split.splitId()
                                    + " with an unknown more-results state "
                                    + page.getMoreResults()
                                    + "; the split cannot tell whether it is finished.");
            }
            // A batch that only skipped part of the offset ends where the skipping stopped.
            ByteString next =
                    page.getEndCursor().isEmpty() ? page.getSkippedCursor() : page.getEndCursor();
            boolean moved =
                    count > 0
                            || page.getSkippedResults() > 0
                            || (!next.isEmpty() && !next.equals(cursor));
            if (!moved || next.isEmpty()) {
                throw new IOException(
                        "Datastore answered a page of split "
                                + split.splitId()
                                + " with more results to come but "
                                + (next.isEmpty() ? "no end cursor" : "no progress")
                                + "; the split cannot be continued.");
            }
            cursor = next;
            return false;
        }
    }
}
