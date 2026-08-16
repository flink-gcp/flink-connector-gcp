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

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.source.TestStructs;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StructStreamOpener} over in-memory partitions.
 *
 * <p>Every partition it is opened with is recorded, so a test can assert on what the connector
 * actually asked the service for rather than only on what came back — including how many times one
 * partition was opened, which is what a re-read after a wake-up looks like.
 *
 * <p>State lives in a static registry keyed by a name, because this double is {@link
 * java.io.Serializable} and travels into a job graph: a MiniCluster job runs in this JVM, so the
 * copy the reader deserializes finds the same recording the test holds.
 *
 * <p>Two cancellation behaviours are offered because the real client has both. Measured against the
 * pinned emulator on 2026-08-10: closing a {@code ResultSet} from another thread while a reader
 * sits in {@code next()} ends that call either by returning {@code false}, exactly as a clean end
 * does, or by throwing {@code CANCELLED: User cancelled stream}.
 */
public final class ScriptedStructStreamOpener implements StructStreamOpener {

    private static final long serialVersionUID = 1L;

    private static final Map<String, State> STATES = new ConcurrentHashMap<>();

    /** How a cancelled read ends, mirroring the two ways the real client's does. */
    public enum CancelBehaviour {
        /** {@code next()} answers with the end of the read, as a clean end does. */
        ENDS_QUIETLY,
        /** {@code next()} throws, as it does when the consumer was already blocked. */
        THROWS
    }

    private final String id;

    private ScriptedStructStreamOpener(String id) {
        this.id = id;
    }

    /**
     * Returns an opener whose partitions hold the given rows.
     *
     * @param id a name unique to the test, since the recording lives in a static registry
     * @param rowsByToken the rows each partition token holds, in the order they are returned
     * @return the opener
     */
    public static ScriptedStructStreamOpener over(
            String id, Map<String, List<Struct>> rowsByToken) {
        STATES.put(id, new State(rowsByToken));
        return new ScriptedStructStreamOpener(id);
    }

    /**
     * Returns an opener with one partition, named {@code p0}, holding rows with the given ids.
     *
     * @param id a name unique to the test
     * @param ids the {@code id} column values the single partition holds
     * @return the opener
     */
    public static ScriptedStructStreamOpener single(String id, long... ids) {
        Map<String, List<Struct>> rows = new HashMap<>();
        rows.put("p0", TestStructs.rows(ids));
        return over(id, rows);
    }

    /** Forgets every recording, so one test's registry entries cannot reach another's. */
    public static void reset() {
        STATES.clear();
    }

    @Override
    public StructStream open(BatchTransactionId batchTransactionId, Partition partition) {
        State state = state();
        String token = partition.getPartitionToken().toStringUtf8();
        state.openedTokens.add(token);
        RuntimeException failure = state.failNextOpen;
        if (failure != null) {
            state.failNextOpen = null;
            throw failure;
        }
        List<Struct> rows = state.rowsByToken.get(token);
        if (rows == null) {
            throw new IllegalArgumentException("The reader opened an unknown partition: " + token);
        }
        return new ScriptedStream(state, rows);
    }

    @Override
    public void close() {
        state().closes.incrementAndGet();
    }

    /** Returns the partition tokens this opener was asked for, in the order it was asked. */
    public List<String> openedTokens() {
        return new ArrayList<>(state().openedTokens);
    }

    /** Returns how many times {@link #close()} was called. */
    public int closes() {
        return state().closes.get();
    }

    /** Returns how many open reads were closed. */
    public int streamCloses() {
        return state().streamCloses.get();
    }

    /** Makes the next open throw. */
    public void failNextOpen(RuntimeException failure) {
        state().failNextOpen = failure;
    }

    /**
     * Makes a read throw once it has handed over the given number of rows.
     *
     * <p>The shape a transient service failure takes mid-partition, which the reader has to tell
     * apart from a cancellation.
     *
     * @param rows how many rows to hand over first
     * @param failure what to throw
     */
    public void failReadAfter(int rows, RuntimeException failure) {
        State state = state();
        state.failReadAfterRow = rows;
        state.readFailure = failure;
    }

    /** Chooses how a cancelled read ends. */
    public void cancelBehaviour(CancelBehaviour behaviour) {
        state().cancelBehaviour = behaviour;
    }

    /**
     * Makes a read block before handing over the row at the given index, until {@link #release()}.
     *
     * <p>What lets a test drive a wake-up into a reader that is genuinely blocked, rather than into
     * one that merely has not been asked for a row yet.
     *
     * @param rowIndex the index of the row the read blocks before
     */
    public void blockBefore(int rowIndex) {
        state().blockBeforeRow = rowIndex;
    }

    /** Lets a blocked read continue. */
    public void release() {
        state().released.countDown();
    }

    /** Waits until a read is actually blocked, so a test's wake-up cannot land too early. */
    public void awaitBlocked() throws InterruptedException {
        state().blocked.await();
    }

    private State state() {
        State state = STATES.get(id);
        if (state == null) {
            throw new IllegalStateException("No recording named " + id + "; was reset() called?");
        }
        return state;
    }

    /** One test's recording. */
    private static final class State {

        private final Map<String, List<Struct>> rowsByToken;
        private final List<String> openedTokens = new CopyOnWriteArrayList<>();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger streamCloses = new AtomicInteger();
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch blocked = new CountDownLatch(1);

        private volatile CancelBehaviour cancelBehaviour = CancelBehaviour.ENDS_QUIETLY;
        private volatile int blockBeforeRow = -1;
        private volatile int failReadAfterRow = -1;
        @Nullable private volatile RuntimeException failNextOpen;
        @Nullable private volatile RuntimeException readFailure;

        private State(Map<String, List<Struct>> rowsByToken) {
            this.rowsByToken = new HashMap<>(rowsByToken);
        }
    }

    /** One open read over a partition's rows. */
    private static final class ScriptedStream implements StructStream {

        private final State state;
        private final List<Struct> rows;

        private int next;
        private volatile boolean closed;

        private ScriptedStream(State state, List<Struct> rows) {
            this.state = state;
            this.rows = new ArrayList<>(rows);
        }

        @Override
        @Nullable
        public Struct next() {
            if (next == state.blockBeforeRow && !closed) {
                state.blocked.countDown();
                try {
                    state.released.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while blocked.", e);
                }
            }
            if (closed) {
                if (state.cancelBehaviour == CancelBehaviour.THROWS) {
                    throw new IllegalStateException("CANCELLED: User cancelled stream");
                }
                return null;
            }
            if (next == state.failReadAfterRow && state.readFailure != null) {
                throw state.readFailure;
            }
            return next < rows.size() ? rows.get(next++) : null;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                state.streamCloses.incrementAndGet();
            }
            // Releasing here is what lets a blocked read observe the cancellation: the real client
            // closes the underlying call, which is what unblocks its consumer.
            state.released.countDown();
        }
    }
}
