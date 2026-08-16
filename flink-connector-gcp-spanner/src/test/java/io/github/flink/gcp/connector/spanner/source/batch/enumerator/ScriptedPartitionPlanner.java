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
 * <p>State lives in a static registry keyed by a name, because this double is {@link
 * java.io.Serializable} and travels into a job graph: a MiniCluster job runs in this JVM, so the
 * copy the enumerator deserializes finds the same recording the test holds.
 */
public final class ScriptedPartitionPlanner implements PartitionPlanner {

    private static final long serialVersionUID = 1L;

    private static final Map<String, State> STATES = new ConcurrentHashMap<>();

    private final String id;

    private ScriptedPartitionPlanner(String id) {
        this.id = id;
    }

    /**
     * Returns a planner answering with one query partition per token.
     *
     * @param id a name unique to the test, since the recording lives in a static registry
     * @param tokens the partition tokens to answer with
     * @return the planner
     */
    public static ScriptedPartitionPlanner planning(String id, String... tokens) {
        List<Partition> partitions = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            partitions.add(TestPartitions.queryPartition(token, "SELECT id FROM t"));
        }
        STATES.put(id, new State(partitions));
        return new ScriptedPartitionPlanner(id);
    }

    /** Forgets every recording, so one test's registry entries cannot reach another's. */
    public static void reset() {
        STATES.clear();
    }

    /** Makes the next plan throw. */
    public void failNextPlan(RuntimeException failure) {
        state().failNextPlan = failure;
    }

    /** Makes {@link #close()} throw. */
    public void failClose(RuntimeException failure) {
        state().failClose = failure;
    }

    /** Returns how many times a plan was asked for. */
    public int plans() {
        return state().plans.get();
    }

    /** Returns how many times {@link #close()} was called. */
    public int closes() {
        return state().closes.get();
    }

    /** Returns the timestamp bounds this planner was asked for, in order. */
    public List<TimestampBound> bounds() {
        return new ArrayList<>(state().bounds);
    }

    /** Returns the partition hints this planner was asked for, in order. */
    public List<PartitionOptions> partitionOptions() {
        return new ArrayList<>(state().partitionOptions);
    }

    /** Returns the Data Boost flags this planner was asked for, in order. */
    public List<Boolean> dataBoostFlags() {
        return new ArrayList<>(state().dataBoostFlags);
    }

    /**
     * Returns the RPC priorities this planner was asked for, rendered, in order.
     *
     * <p>Rendered rather than held as the enum, so that "unset" is a value a test can assert on
     * rather than a null the assertion has to work around.
     */
    public List<String> priorities() {
        return new ArrayList<>(state().priorities);
    }

    @Override
    public PartitionPlan plan(
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException {
        State state = state();
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
        State state = state();
        state.closes.incrementAndGet();
        RuntimeException failure = state.failClose;
        if (failure != null) {
            throw failure;
        }
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

        private final List<Partition> partitions;
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
