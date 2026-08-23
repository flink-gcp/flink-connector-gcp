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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.TestRows;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link RowStreamOpener} over an in-memory table, which honours the range it is asked for.
 *
 * <p>Honouring an <em>exclusive</em> start is the whole point: that is what a resumed split asks
 * for, and a double that ignored it would let a source that re-reads everything pass its resume
 * tests. Every range and filter it is opened with is recorded, so a test can assert on what the
 * connector actually asked the service for rather than only on what came back.
 *
 * <p>State lives in a static registry keyed by a name, because this double is {@link
 * java.io.Serializable} and travels into a job graph: a MiniCluster job runs in this JVM, so the
 * copy the reader deserializes finds the same recording the test holds.
 *
 * <p>Two cancellation behaviours are offered because the real client has both. Read out of gax
 * 2.82.0 on 2026-08-09: cancelling a {@code ServerStream} makes its iterator report the end of the
 * stream, exactly as a clean end does — unless the consumer was already blocked waiting, in which
 * case the cancellation arrives as a buffered error and the iterator throws instead.
 */
public final class ScriptedRowStreamOpener implements RowStreamOpener {

    private static final long serialVersionUID = 1L;

    private static final Map<String, State> STATES = new ConcurrentHashMap<>();

    /** How a cancelled stream ends, mirroring the two ways the real client's does. */
    public enum CancelBehaviour {
        /** {@code next()} answers with the end of the stream, as a clean end does. */
        ENDS_QUIETLY,
        /** {@code next()} throws, as it does when the consumer was already blocked. */
        THROWS
    }

    private final String id;

    private ScriptedRowStreamOpener(String id) {
        this.id = id;
    }

    /**
     * Returns an opener over a table holding the given row keys.
     *
     * @param id a name unique to the test, since the recording lives in a static registry
     * @param keys the row keys the table holds, in ascending order
     * @return the opener
     */
    public static ScriptedRowStreamOpener over(String id, String... keys) {
        STATES.put(id, new State(java.util.Arrays.asList(keys)));
        return new ScriptedRowStreamOpener(id);
    }

    /** Forgets every recording, so one test's registry entries cannot reach another's. */
    public static void reset() {
        STATES.clear();
    }

    @Override
    public RowStream open(
            TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter) {
        State state = state();
        // The interface says the caller guarantees a non-empty range, and the reader relies on it.
        // A double that accepted one would let a regression past every test but the single
        // assertion that counts opens.
        if (RowRanges.isEmpty(range)) {
            throw new IllegalArgumentException(
                    "The reader opened an empty range: " + RowRanges.format(range));
        }
        state.openedRanges.add(RowRanges.format(range));
        state.openedFilters.add(filter);
        state.opens.incrementAndGet();
        RuntimeException failure = state.failNextOpen;
        if (failure != null) {
            state.failNextOpen = null;
            throw failure;
        }
        return new ScriptedStream(state, rowsIn(state, range));
    }

    /** Answers from a script rather than a client, so there is nothing to authenticate. */
    @Override
    public void useCredentials(@Nullable CredentialsProvider credentials) {}

    @Override
    public void close() {
        State state = state();
        state.lifecycleEvents.add("opener");
        state.closes.incrementAndGet();
    }

    /** Returns the ranges this opener was asked for, rendered, in the order it was asked. */
    public List<String> openedRanges() {
        return new ArrayList<>(state().openedRanges);
    }

    /** Returns the filters this opener was asked for, in the order it was asked. */
    public List<Filters.Filter> openedFilters() {
        return new ArrayList<>(state().openedFilters);
    }

    /** Returns how many streams were opened. */
    public int openCalls() {
        return state().opens.get();
    }

    /** Returns how many times this opener itself was closed. */
    public int closeCalls() {
        return state().closes.get();
    }

    /** Returns how many streams were closed, which is how a cancelled read is observed. */
    public int streamCloseCalls() {
        return state().streamCloses.get();
    }

    /** Returns stream and opener close events in the order they happened. */
    public List<String> lifecycleEvents() {
        return new ArrayList<>(state().lifecycleEvents);
    }

    /** Makes the next open throw. */
    public void failNextOpenWith(RuntimeException failure) {
        state().failNextOpen = failure;
    }

    /** Makes every stream block after handing over this many rows, until it is closed. */
    public void blockAfter(int rows, CancelBehaviour behaviour) {
        State state = state();
        state.blockAfter = rows;
        state.cancelBehaviour = behaviour;
    }

    /**
     * Makes every stream fail after handing over this many rows, with nothing having cancelled it.
     *
     * <p>Distinct from {@link #blockAfter}: this is a read that genuinely went wrong, which the
     * reader must report rather than treat as a wake-up.
     */
    public void failReadAfter(int rows, RuntimeException failure) {
        State state = state();
        state.failReadAfter = rows;
        state.readFailure = failure;
    }

    /** Waits until a stream has blocked, so a test can cancel it deterministically. */
    public void awaitBlocked() throws InterruptedException {
        state().blocked.await();
    }

    /**
     * Holds every stream after this many rows until the gate opens, and paces the rows it does hand
     * over.
     *
     * <p>What a failover test needs and {@link #blockAfter} does not give it: a stream that pauses
     * long enough for a checkpoint to complete over rows already read, and then carries on. Without
     * it a small table is read to its end before the first qualifying checkpoint, and the failure
     * the test is about never fires.
     *
     * @param rows how many rows a stream hands over before it waits
     * @param open answers true once the stream may continue
     * @param rowDelayMillis how long to pause between rows
     */
    public void gateAfter(int rows, java.util.function.BooleanSupplier open, long rowDelayMillis) {
        State state = state();
        state.gateAfter = rows;
        state.gate = open;
        state.rowDelayMillis = rowDelayMillis;
    }

    private State state() {
        State state = STATES.get(id);
        if (state == null) {
            throw new IllegalStateException(
                    "No scripted table registered under " + id + "; was reset() called?");
        }
        return state;
    }

    private static List<Row> rowsIn(State state, ByteStringRange range) {
        List<Row> rows = new ArrayList<>();
        for (String key : state.keys) {
            if (contains(range, ByteString.copyFromUtf8(key))) {
                rows.add(TestRows.row(key));
            }
        }
        return rows;
    }

    private static boolean contains(ByteStringRange range, ByteString key) {
        if (range.getStartBound() != BoundType.UNBOUNDED) {
            int cmp = RowRanges.compareKeys(key, range.getStart());
            if (cmp < 0 || (cmp == 0 && range.getStartBound() == BoundType.OPEN)) {
                return false;
            }
        }
        if (range.getEndBound() != BoundType.UNBOUNDED) {
            int cmp = RowRanges.compareKeys(key, range.getEnd());
            return cmp < 0 || (cmp == 0 && range.getEndBound() == BoundType.CLOSED);
        }
        return true;
    }

    /** The recording and the script, shared by every deserialized copy of one opener. */
    private static final class State {

        private final List<String> keys;
        private final List<String> openedRanges = new CopyOnWriteArrayList<>();
        private final List<Filters.Filter> openedFilters =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger streamCloses = new AtomicInteger();
        private final List<String> lifecycleEvents = new CopyOnWriteArrayList<>();

        private volatile int blockAfter = Integer.MAX_VALUE;
        private volatile int failReadAfter = Integer.MAX_VALUE;
        private volatile int gateAfter = Integer.MAX_VALUE;
        private volatile java.util.function.BooleanSupplier gate = () -> true;
        private volatile long rowDelayMillis;
        private volatile CancelBehaviour cancelBehaviour = CancelBehaviour.ENDS_QUIETLY;
        private final CountDownLatch blocked = new CountDownLatch(1);
        @Nullable private volatile RuntimeException failNextOpen;
        @Nullable private volatile RuntimeException readFailure;

        private State(List<String> keys) {
            this.keys = new ArrayList<>(keys);
        }
    }

    /** One scripted read. */
    private static final class ScriptedStream implements RowStream {

        private final State state;
        private final List<Row> rows;

        private int position;
        private volatile boolean closed;

        private ScriptedStream(State state, List<Row> rows) {
            this.state = state;
            this.rows = rows;
        }

        @Override
        @Nullable
        public Row next() {
            if (closed) {
                return endOfStream();
            }
            if (position >= state.failReadAfter && state.readFailure != null) {
                throw state.readFailure;
            }
            if (position >= state.blockAfter) {
                state.blocked.countDown();
                while (!closed) {
                    sleep(1);
                }
                return endOfStream();
            }
            while (position >= state.gateAfter && !state.gate.getAsBoolean() && !closed) {
                sleep(1);
            }
            if (closed) {
                return endOfStream();
            }
            if (state.rowDelayMillis > 0) {
                sleep(state.rowDelayMillis);
            }
            return position < rows.size() ? rows.get(position++) : null;
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while pacing a scripted read", e);
            }
        }

        @Nullable
        private Row endOfStream() {
            if (state.cancelBehaviour == CancelBehaviour.THROWS) {
                throw new IllegalStateException("stream cancelled");
            }
            return null;
        }

        @Override
        public void close() {
            if (!closed) {
                state.lifecycleEvents.add("stream");
                state.streamCloses.incrementAndGet();
            }
            closed = true;
        }
    }
}
