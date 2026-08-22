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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.TestPartitions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link PartitionPlanner} that answers with partitions a test named, and records what it was
 * asked for.
 *
 * <p>A test holds the {@link Factory}; the planners are minted by the source, one per enumerator,
 * and the test never sees them directly. The recording therefore lives in a static registry keyed
 * by a name, so that the factory a test holds and every planner it mints reach the same one even
 * across a copy of the factory. The sibling doubles in the other two connectors keep the same
 * recording in an instance field, which also works; this one keeps the registry it already had
 * rather than changing the eleven tests that read through it.
 *
 * <p>Every count is an aggregate over the planners of one recording, which is what makes an
 * assertion like {@code closes() == 1} say "one planner, closed once" rather than merely "something
 * was closed once". Refusing after {@link #close()} is per planner, mirroring {@link
 * BatchClientPartitionPlanner}: without it a shared planner and a fresh one behave identically, and
 * a test cannot tell the two apart.
 */
public final class ScriptedPartitionPlanner implements PartitionPlanner {

    private static final Map<String, State> STATES = new ConcurrentHashMap<>();

    private final String id;

    private volatile boolean closed;
    private final AtomicInteger ownCloses = new AtomicInteger();

    private ScriptedPartitionPlanner(String id) {
        this.id = id;
    }

    /**
     * Returns a factory minting planners that answer with one query partition per token.
     *
     * @param id a name unique to the test, since the recording lives in a static registry
     * @param tokens the partition tokens to answer with
     * @return the factory
     */
    public static Factory planning(String id, String... tokens) {
        List<Partition> partitions = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            partitions.add(TestPartitions.queryPartition(token, "SELECT id FROM t"));
        }
        STATES.put(id, new State(partitions));
        return new Factory(id);
    }

    /** Forgets every recording, so one test's registry entries cannot reach another's. */
    public static void reset() {
        STATES.clear();
    }

    /** Ignores them: nothing here reaches the service, so there is no client to scope. */
    @Override
    public void useCredentials(@Nullable GoogleCredentials credentials) {}

    @Override
    public PartitionPlan plan(
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException {
        if (closed) {
            throw new IOException(
                    "The Spanner partition planner for the scripted read was closed before it was"
                            + " used.");
        }
        State state = state(id);
        state.plans.incrementAndGet();
        state.bounds.add(bound);
        state.partitionOptions.add(partitionOptions);
        state.dataBoostFlags.add(dataBoostEnabled);
        state.priorities.add(String.valueOf(rpcPriority));
        RuntimeException failure = state.failNextPlan;
        if (failure != null) {
            state.failNextPlan = null;
            throw failure;
        }
        return new PartitionPlan(
                TestPartitions.batchTransactionId(),
                Timestamp.ofTimeMicroseconds(1_000L),
                state.partitions);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        ownCloses.incrementAndGet();
        State state = state(id);
        state.closes.incrementAndGet();
        RuntimeException failure = state.failClose;
        if (failure != null) {
            throw failure;
        }
    }

    /** Returns whether this planner refuses further planning. */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns how many times <em>this</em> planner was closed.
     *
     * <p>Separate from {@link Factory#closes()}, which aggregates: an aggregate of one cannot say
     * whether one planner was closed once or a second was closed twice while the first was not
     * closed at all.
     */
    public int closeCalls() {
        return ownCloses.get();
    }

    private static State state(String id) {
        State state = STATES.get(id);
        if (state == null) {
            throw new IllegalStateException("No recording named " + id + "; was reset() called?");
        }
        return state;
    }

    /** Mints the planners of one recording, and answers for all of them together. */
    public static final class Factory implements PartitionPlannerFactory {

        private static final long serialVersionUID = 1L;

        private final String id;

        private Factory(String id) {
            this.id = id;
        }

        @Override
        public PartitionPlanner create() {
            ScriptedPartitionPlanner planner = new ScriptedPartitionPlanner(id);
            state(id).minted.add(planner);
            return planner;
        }

        /** Returns the planners minted from this recording, in the order they were minted. */
        public List<ScriptedPartitionPlanner> minted() {
            return new ArrayList<>(state(id).minted);
        }

        /** Makes the next plan throw. */
        public void failNextPlan(RuntimeException failure) {
            state(id).failNextPlan = failure;
        }

        /** Makes {@link ScriptedPartitionPlanner#close()} throw. */
        public void failClose(RuntimeException failure) {
            state(id).failClose = failure;
        }

        /** Returns how many times a plan was asked for. */
        public int plans() {
            return state(id).plans.get();
        }

        /** Returns how many times {@link ScriptedPartitionPlanner#close()} was called. */
        public int closes() {
            return state(id).closes.get();
        }

        /** Returns the timestamp bounds this recording's planners were asked for, in order. */
        public List<TimestampBound> bounds() {
            return new ArrayList<>(state(id).bounds);
        }

        /** Returns the partition hints this recording's planners were asked for, in order. */
        public List<PartitionOptions> partitionOptions() {
            return new ArrayList<>(state(id).partitionOptions);
        }

        /** Returns the Data Boost flags this recording's planners were asked for, in order. */
        public List<Boolean> dataBoostFlags() {
            return new ArrayList<>(state(id).dataBoostFlags);
        }

        /**
         * Returns the RPC priorities this recording's planners were asked for, rendered, in order.
         *
         * <p>Rendered rather than held as the enum, so that "unset" is a value a test can assert on
         * rather than a null the assertion has to work around.
         */
        public List<String> priorities() {
            return new ArrayList<>(state(id).priorities);
        }
    }

    /** One test's recording. */
    private static final class State {

        private final List<Partition> partitions;
        private final List<ScriptedPartitionPlanner> minted = new CopyOnWriteArrayList<>();
        private final AtomicInteger plans = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final List<TimestampBound> bounds = new CopyOnWriteArrayList<>();
        private final List<PartitionOptions> partitionOptions = new CopyOnWriteArrayList<>();
        private final List<Boolean> dataBoostFlags = new CopyOnWriteArrayList<>();
        private final List<String> priorities = new CopyOnWriteArrayList<>();

        @Nullable private volatile RuntimeException failNextPlan;
        @Nullable private volatile RuntimeException failClose;

        private State(List<Partition> partitions) {
            this.partitions = new ArrayList<>(partitions);
        }
    }
}
