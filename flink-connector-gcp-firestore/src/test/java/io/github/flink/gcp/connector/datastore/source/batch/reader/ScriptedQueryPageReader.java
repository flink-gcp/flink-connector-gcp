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

import com.google.auth.Credentials;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A page reader over a scripted kind, answering name-ordered pages as the service would: from the
 * page's start cursor, past what is left of its offset, up to its limit, and no further than its
 * end cursor. The cursor after an entity is the entity's name, so a test can read where a page
 * started and ended. It records every page it was asked for, and fails the page it was told to.
 *
 * <p>Four of the service's freedoms can be scripted: answering with fewer entities than the limit
 * while more remain ({@link #answeringAtMost}); skipping only part of an offset in one batch
 * ({@link #skippingAtMost}), and then setting only the skipped cursor ({@link #skippedCursorOnly});
 * and answering the first page with no entity but a cursor that moved ({@link #emptyFirstBatch}),
 * as a scan over index entries a filter rejects may.
 */
final class ScriptedQueryPageReader implements QueryPageReader {

    private static final long serialVersionUID = 1L;

    /** A cursor before every name the tests use, which all start with a letter. */
    static final ByteString BEFORE_EVERY_NAME = ByteString.copyFromUtf8(" ");

    private final List<String> names;
    private final transient List<RunQueryRequest> pages = new ArrayList<>();
    private int failPage = -1;
    private int batchCap = Integer.MAX_VALUE;
    private int skipCap = Integer.MAX_VALUE;
    private int overfill;
    private boolean skippedCursorOnly;
    private boolean emptyFirstBatch;
    @Nullable private QueryResultBatch fixedAnswer;
    private int closeCalls;

    ScriptedQueryPageReader(List<String> names) {
        this.names = new ArrayList<>(names);
        this.names.sort(String::compareTo);
    }

    /** Fails the page with this index, counting from zero. */
    ScriptedQueryPageReader failingPage(int index) {
        this.failPage = index;
        return this;
    }

    /** Answers with at most this many entities, saying more remain when they do. */
    ScriptedQueryPageReader answeringAtMost(int entities) {
        this.batchCap = entities;
        return this;
    }

    /** Skips at most this many entities of an offset in one batch. */
    ScriptedQueryPageReader skippingAtMost(int entities) {
        this.skipCap = entities;
        return this;
    }

    /** Leaves the end cursor of a batch that only skipped unset; the skipped cursor carries it. */
    ScriptedQueryPageReader skippedCursorOnly() {
        this.skippedCursorOnly = true;
        return this;
    }

    /**
     * Answers the first page with no entity and more to come, ending at a cursor before every
     * entity, which differs from the page's absent start cursor.
     */
    ScriptedQueryPageReader emptyFirstBatch() {
        this.emptyFirstBatch = true;
        return this;
    }

    /**
     * Answers every page with this many entities beyond its limit, which the service never does.
     */
    ScriptedQueryPageReader overfilling(int entities) {
        this.overfill = entities;
        return this;
    }

    /** Answers every page with this batch, whatever it asked for. */
    ScriptedQueryPageReader answeringWith(QueryResultBatch batch) {
        this.fixedAnswer = batch;
        return this;
    }

    List<RunQueryRequest> pages() {
        return pages;
    }

    int closeCalls() {
        return closeCalls;
    }

    /** Returns the cursor after the entity of this name. */
    static ByteString cursorAfter(String name) {
        return ByteString.copyFromUtf8(name);
    }

    @Override
    public QueryResultBatch read(DatabaseDestination database, RunQueryRequest page)
            throws IOException {
        pages.add(page);
        if (pages.size() - 1 == failPage) {
            throw new IOException("scripted failure");
        }
        if (fixedAnswer != null) {
            return fixedAnswer;
        }
        if (emptyFirstBatch && pages.size() == 1) {
            return QueryResultBatch.newBuilder()
                    .setEndCursor(BEFORE_EVERY_NAME)
                    .setMoreResults(QueryResultBatch.MoreResultsType.NOT_FINISHED)
                    .build();
        }
        Query query = page.getQuery();
        int from = after(query.getStartCursor());
        int last = query.getEndCursor().isEmpty() ? names.size() : after(query.getEndCursor());
        QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
        int skipped = Math.max(0, Math.min(Math.min(query.getOffset(), skipCap), last - from));
        batch.setSkippedResults(skipped);
        from += skipped;
        if (skipped > 0) {
            batch.setSkippedCursor(cursorAfter(names.get(from - 1)));
        }
        if (skipped < query.getOffset() && from < last) {
            // Stopped skipping part-way: nothing returned yet, more to come.
            if (!skippedCursorOnly) {
                batch.setEndCursor(cursorAfter(names.get(from - 1)));
            }
            return batch.setMoreResults(QueryResultBatch.MoreResultsType.NOT_FINISHED).build();
        }
        int limit = query.hasLimit() ? query.getLimit().getValue() : Integer.MAX_VALUE;
        int answered = Math.max(0, Math.min(Math.min(limit, batchCap), last - from));
        int to = Math.min(last, from + answered + overfill);
        for (int i = from; i < to; i++) {
            batch.addEntityResults(
                    EntityResult.newBuilder()
                            .setEntity(entity(database, names.get(i)))
                            .setCursor(cursorAfter(names.get(i))));
        }
        if (to > from) {
            batch.setEndCursor(cursorAfter(names.get(to - 1)));
        } else if (skipped > 0) {
            batch.setEndCursor(cursorAfter(names.get(from - 1)));
        }
        QueryResultBatch.MoreResultsType more;
        if (answered == limit) {
            more = QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_LIMIT;
        } else if (from + answered < last) {
            more = QueryResultBatch.MoreResultsType.NOT_FINISHED;
        } else if (last < names.size()) {
            more = QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_CURSOR;
        } else {
            more = QueryResultBatch.MoreResultsType.NO_MORE_RESULTS;
        }
        return batch.setMoreResults(more).build();
    }

    /** Returns the index of the first name after the cursor, or zero for no cursor. */
    private int after(ByteString cursor) {
        if (cursor.isEmpty()) {
            return 0;
        }
        String position = cursor.toStringUtf8();
        int index = 0;
        while (index < names.size() && names.get(index).compareTo(position) <= 0) {
            index++;
        }
        return index;
    }

    private static Entity entity(DatabaseDestination database, String name) {
        return Entity.newBuilder()
                .setKey(
                        Key.newBuilder()
                                .setPartitionId(
                                        PartitionId.newBuilder()
                                                .setProjectId(database.getProject())
                                                .setDatabaseId(database.getDatabaseId()))
                                .addPath(Key.PathElement.newBuilder().setKind("K").setName(name)))
                .build();
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {}

    @Override
    public void close() {
        closeCalls++;
    }
}
