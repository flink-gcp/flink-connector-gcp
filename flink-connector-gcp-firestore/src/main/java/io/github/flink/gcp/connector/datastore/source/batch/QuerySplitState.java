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

package io.github.flink.gcp.connector.datastore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.Query;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;

import javax.annotation.Nullable;

/**
 * How far through one {@link QuerySplit} the source has emitted.
 *
 * <p>Separate from the split for the reason the Bigtable scan's state is: the split reader works on
 * the fetcher thread, while the record emitter advances this state on the task thread. It tracks
 * the cursor after the last entity <em>successfully deserialized</em>, including one that produced
 * no output, which is what a checkpoint must record; the split reader keeps its own cursor after
 * what it handed over, which is what the next page must continue from.
 *
 * <p>Not thread-safe, and does not need to be: every method here is called from the task thread.
 */
@Internal
public final class QuerySplitState {

    private final QuerySplit split;

    @Nullable private ByteString lastCursor;
    private long emitted;

    /**
     * Creates the state of a freshly assigned or restored split.
     *
     * <p>It starts with no progress, whatever the split's history: a restored split already carries
     * its previous progress in its query.
     *
     * @param split the split being read
     */
    public QuerySplitState(QuerySplit split) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
    }

    /**
     * Records that an entity has been successfully deserialized.
     *
     * <p>Called once per entity, whatever the deserializer made of it: the split resumes at a
     * cursor, so an entity that produced no record still has to move the resume point.
     *
     * @param entity the entity handed to the emitter, with its cursor
     */
    public void recordEmitted(FetchedEntity entity) {
        Preconditions.checkNotNull(entity, "entity must not be null");
        this.lastCursor = entity.getCursor();
        this.emitted++;
    }

    /** Returns how many entities have been successfully deserialized since assignment. */
    @VisibleForTesting
    public long getEmitted() {
        return emitted;
    }

    /**
     * Returns the split as it should be checkpointed: the work that is left.
     *
     * <p>The query starts at the cursor after the last entity passed. Its {@code offset} is
     * dropped, because the service returns an entity only once the whole offset has been skipped;
     * its {@code limit}, when it has one, is reduced by the entities passed; and its end cursor,
     * when it has one, stays.
     *
     * @return the assigned split unchanged when no entity has been successfully deserialized,
     *     otherwise a split whose query starts after the last such entity
     */
    public QuerySplit toSplit() {
        if (lastCursor == null) {
            return split;
        }
        RunQueryRequest request = split.getRequest();
        Query.Builder query =
                request.getQuery().toBuilder().setStartCursor(lastCursor).setOffset(0);
        if (query.hasLimit()) {
            query.setLimit(Int32Value.of((int) Math.max(0, query.getLimit().getValue() - emitted)));
        }
        return new QuerySplit(
                split.splitId(), request.toBuilder().setQuery(query).build(), split.getReadTime());
    }
}
