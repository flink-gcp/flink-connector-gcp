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

package io.github.flink.gcp.connector.firestore.common.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The bounded read Flink's full lookup cache loads a Native-mode or Datastore-mode table through:
 * one input split per load, whose planned queries one split reader of the scan reads to the end.
 *
 * <p>Each load is planned as exactly one {@link LoadSplit}. Flink runs the splits of one load on
 * several threads and gives every one of them the same cache-entry serializer, whose copy of a map
 * that is not already binary goes through reuse buffers the serializer keeps (Flink 2.2.1 and
 * 1.20.5, {@code InputFormatCacheLoader} and {@code MapDataSerializer}), so a second split could
 * corrupt {@code MAP} values silently (#2217).
 *
 * <p>This class opens the split's reader in the copy Flink deserializes for each load, hands it the
 * split's queries, and reads until the reader has reported every one of them finished. It needs no
 * guard against a fetch that returns nothing: each split reader either finishes a query or moves
 * its cursor on every page, and fails a page that claims more results without moving, so a page
 * with no record is progress. The two readers differ in which of those an empty page is, which is
 * why this class asks for nothing more: a Native-mode page with no document finishes its query,
 * while a Datastore-mode one can carry a moved cursor and more to come.
 *
 * <p>A page that fails fails the load: Flink publishes no part of it, and the lookups that follow
 * fail until the job restarts, except a few right after a failed first load, which Flink answers
 * from the empty cache before it records the failure (#2225). An exception releasing the load once
 * it is read is reported as an {@link IOException}, which Flink's loader logs while keeping the
 * rows; an unchecked one escaping its {@code finally} block would discard them. An {@code Error}
 * that is the first failure is thrown as itself. No client metrics reach Flink, which configures
 * and opens its copy with no runtime context; each load logs what it read instead.
 *
 * @param <E> the record the split reader fetches
 * @param <S> the split reader's split
 * @param <L> the load's split
 */
@Internal
public abstract class FullCacheInputFormat<
                E, S extends SourceSplit, L extends FullCacheInputFormat.LoadSplit>
        extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FullCacheInputFormat.class);

    private final Class<L> loadSplitType;
    private final String recordNoun;

    @Nullable private transient SplitReader<E, S> reader;
    private transient List<AutoCloseable> resources = new ArrayList<>();
    @Nullable private transient L split;
    private transient Set<String> unfinished = new HashSet<>();
    private transient ArrayDeque<RowData> pending = new ArrayDeque<>();
    @Nullable private transient Collector<RowData> pendingRows;
    private transient long records;

    /**
     * Creates the format.
     *
     * @param loadSplitType the subclass's load split, which {@link #open(GenericInputSplit)} checks
     * @param recordNoun what the log calls a read record, in the plural, such as {@code documents}
     */
    protected FullCacheInputFormat(Class<L> loadSplitType, String recordNoun) {
        this.loadSplitType =
                Preconditions.checkNotNull(loadSplitType, "loadSplitType must not be null");
        this.recordNoun = Preconditions.checkNotNull(recordNoun, "recordNoun must not be null");
    }

    /**
     * Plans one load: takes the service's current time and the queries to read at it.
     *
     * @return the load's one split
     * @throws IOException if planning fails
     */
    protected abstract L planLoad() throws IOException;

    /**
     * Opens the split reader of one load. Whatever else the reader reads through is passed to
     * {@link #closeWithLoad(AutoCloseable)} as soon as it is opened.
     *
     * @return the split reader, which this class closes
     * @throws IOException if the reader cannot be opened
     */
    protected abstract SplitReader<E, S> openReader() throws IOException;

    /**
     * Returns the queries of the load as the split reader's splits, each with a distinct id.
     *
     * @param split the load's split
     * @return the splits the reader reads, in order, at least one
     */
    protected abstract List<S> querySplits(L split);

    /**
     * Turns one fetched record into the rows it produces.
     *
     * @param record the fetched record
     * @param out where the rows go
     * @throws IOException if the record cannot be read into the table
     */
    protected abstract void emit(E record, Collector<RowData> out) throws IOException;

    /**
     * Names the table a load reads, for the log, so that two tables' loads can be told apart.
     *
     * @param split the load's split
     * @return what the load reads, such as the collection and its database
     */
    protected abstract String describe(L split);

    /**
     * Holds a resource the split reader reads through until the load is closed, after the reader.
     *
     * @param resource the opened resource
     * @param <C> the resource's type
     * @return the resource
     */
    protected final <C extends AutoCloseable> C closeWithLoad(C resource) {
        resources.add(Preconditions.checkNotNull(resource, "resource must not be null"));
        return resource;
    }

    /**
     * Plans the load as its one split.
     *
     * @param minNumSplits ignored; Flink's loader asks for one, and a second split would run on
     *     another thread beside the first
     * @return the one split of the load
     * @throws IOException if planning fails
     */
    @Override
    @SuppressWarnings("unchecked")
    public final L[] createInputSplits(int minNumSplits) throws IOException {
        L load = planLoad();
        LOG.info(
                "Planned a full lookup cache load of {} at read time {}.",
                describe(load),
                load.getReadTime());
        L[] splits = (L[]) Array.newInstance(loadSplitType, 1);
        splits[0] = load;
        return splits;
    }

    /**
     * Opens the load's split for reading.
     *
     * @param split the split {@link #createInputSplits(int)} returned
     * @throws IOException if the split reader cannot be opened
     * @throws IllegalArgumentException if the split was not planned by this format
     */
    @Override
    public final void open(GenericInputSplit split) throws IOException {
        Preconditions.checkArgument(
                loadSplitType.isInstance(split), "not a split of a full-cache load: %s", split);
        super.open(split);
        L load = loadSplitType.cast(split);
        this.split = load;
        // Transient fields start null in the copy Flink deserializes for each load.
        resources = new ArrayList<>();
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = new PendingRows(pending);
        records = 0;
        try {
            List<S> queries = querySplits(load);
            for (S query : queries) {
                unfinished.add(query.splitId());
            }
            // Registered before the reader opens, so a reader that fails to open or to take them
            // leaves them unfinished.
            reader = openReader();
            reader.handleSplitsChanges(new SplitsAddition<>(queries));
        } catch (Throwable e) {
            // Flink's loader also closes the format after a failed open; closing here keeps this
            // method from relying on that, and close() is idempotent.
            Closers.closeAllSuppressing(e, this::close);
            throw e;
        }
    }

    @Override
    public final boolean reachedEnd() throws IOException {
        while (pending.isEmpty() && reader != null && !unfinished.isEmpty()) {
            RecordsWithSplitIds<E> batch = reader.fetch();
            try {
                while (batch.nextSplit() != null) {
                    E record;
                    while ((record = batch.nextRecordFromSplit()) != null) {
                        records++;
                        emit(record, pendingRows);
                    }
                }
                unfinished.removeAll(batch.finishedSplits());
            } finally {
                batch.recycle();
            }
        }
        return pending.isEmpty();
    }

    @Override
    @Nullable
    public final RowData nextRecord(RowData reuse) throws IOException {
        return reachedEnd() ? null : pending.poll();
    }

    @Override
    public final void close() throws IOException {
        List<AutoCloseable> open = new ArrayList<>();
        open.add(reader);
        if (resources != null) {
            open.addAll(resources);
        }
        L openSplit = split;
        // Complete only when a reader opened and reported every query finished: a load whose
        // queries could not be read registered none, which an empty set would read as finished.
        boolean complete = reader != null && unfinished != null && unfinished.isEmpty();
        reader = null;
        resources = new ArrayList<>();
        split = null;
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = null;
        try {
            Closers.closeAll(open);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to release a full lookup cache load.", e);
        } finally {
            if (openSplit != null) {
                LOG.info(
                        "{} a full lookup cache load of {} at read time {} after reading {} {}.",
                        complete ? "Finished" : "Stopped",
                        describe(openSplit),
                        openSplit.getReadTime(),
                        records,
                        recordNoun);
            }
        }
    }

    /** The one split of a load: the planned queries and the read time they share. */
    public abstract static class LoadSplit extends GenericInputSplit {

        private static final long serialVersionUID = 1L;

        private final Timestamp readTime;

        /**
         * Creates the split.
         *
         * @param readTime the read time every query of the load reads at
         */
        protected LoadSplit(Timestamp readTime) {
            super(0, 1);
            this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        }

        /**
         * Returns the read time every query of the load reads at.
         *
         * @return the read time
         */
        public Timestamp getReadTime() {
            return readTime;
        }
    }

    /** Collects what a subclass emits for one record into the pending queue. */
    private static final class PendingRows implements Collector<RowData> {

        private final ArrayDeque<RowData> pending;

        private PendingRows(ArrayDeque<RowData> pending) {
            this.pending = pending;
        }

        @Override
        public void collect(RowData record) {
            pending.add(record);
        }

        @Override
        public void close() {}
    }
}
