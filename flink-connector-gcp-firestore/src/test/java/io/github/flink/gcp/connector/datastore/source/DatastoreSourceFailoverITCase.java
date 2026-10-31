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

package io.github.flink.gcp.connector.datastore.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real Flink job over the source against the emulator, failing once part-way through and
 * recovering: every entity is read, and a restored split resumes at the cursor after its last
 * entity rather than starting over.
 *
 * <p>The failure is armed by a checkpoint whose barrier passed entities, not by a count: what a
 * split resumes from is what the barrier saw, and a checkpoint that crossed the source before any
 * entity proves nothing.
 */
class DatastoreSourceFailoverITCase extends AbstractDatastoreEmulatorITCase {

    private static final int ENTITIES = 120;

    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    @BeforeEach
    void forgetTheLastRun() {
        FAILED_ONCE.set(false);
        FailAfterACheckpoint.SEEN_AT_BARRIER.clear();
        FailAfterACheckpoint.INJECTED_BY.set(null);
        GatedPageReader.reset();
    }

    @Test
    void readsEveryEntityAcrossAFailureAndResumesAfterTheLastOne() throws Exception {
        String kind = uniqueKind();
        List<String> names = new ArrayList<>();
        List<FullEntity<?>> batch = new ArrayList<>();
        for (int n = 0; n < ENTITIES; n++) {
            String name = "e" + String.format("%03d", n);
            names.add(name);
            batch.add(Entity.newBuilder(key(kind, name)).set("n", n).build());
        }
        client().put(batch.toArray(new FullEntity<?>[0]));

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(20);

        DatastoreSourceBuilder<String> builder =
                DatastoreSource.<String>builder()
                        .database(database())
                        .kind(kind)
                        .deserializer(new TestSources.KeyNameDeserializer())
                        .pageSize(5)
                        .emulatorEndpoint(emulatorEndpoint());
        TestSources.withPlannerFactory(
                builder,
                new FixedSplitsPlannerFactory(
                        emulatorEndpoint(), List.of(names.get(ENTITIES / 2))));
        TestSources.withPageReader(builder, new GatedPageReader(emulatorEndpoint(), 4));

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> records =
                env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "datastore")
                        .map(new FailAfterACheckpoint())
                        .executeAndCollect()) {
            records.forEachRemaining(collected::add);
        }

        assertThat(FAILED_ONCE)
                .as(
                        "the job failed once on purpose (entities at each checkpoint/subtask"
                                + " barrier: %s)",
                        FailAfterACheckpoint.SEEN_AT_BARRIER)
                .isTrue();
        assertThat(collected).containsExactlyInAnyOrderElementsOf(names);
        // Both planned ranges start without a cursor. A split whose first page in an attempt starts
        // at one can only be one a checkpoint rewrote: this names the resume mechanism, where a
        // source re-reading its restored splits from the beginning would show only as duplicates
        // above.
        assertThat(GatedPageReader.STARTED_SPLITS)
                .as(
                        "the restored reader resumed after its last entity"
                                + " (injected by %s;"
                                + " entities at each checkpoint/subtask barrier: %s)",
                        FailAfterACheckpoint.INJECTED_BY, FailAfterACheckpoint.SEEN_AT_BARRIER)
                .hasSizeGreaterThan(2)
                .anyMatch(page -> !page.getQuery().getStartCursor().isEmpty());
    }

    /**
     * Fails the job once, after a checkpoint whose barrier passed entities has completed, and only
     * in the subtask whose entities it saw.
     */
    private static final class FailAfterACheckpoint extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        /**
         * Each subtask's {@code seenHere} at each checkpoint's barrier, keyed {@code
         * checkpoint/subtask}, for the failure messages.
         */
        static final Map<String, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        /** Which subtask injected the failure, and its first covering checkpoint. */
        static final AtomicReference<String> INJECTED_BY = new AtomicReference<>();

        /**
         * What this subtask's map saw, not the job's: source and map are chained, so a count above
         * zero at this subtask's barrier proves its own split's position had moved. A counter
         * shared by both subtasks could be raised by the other one between the two barriers.
         */
        private transient int seenHere;

        /**
         * The first checkpoint whose barrier found {@code seenHere} above zero; zero before one.
         */
        private transient long firstCoveringCheckpoint;

        /**
         * Whether a checkpoint covering this subtask's entities has completed. Only such a subtask
         * fails the job: it fails on its next entity, so its split cannot finish first, and every
         * checkpoint the job can restore holds that split past its start. A subtask without
         * entities in a completed checkpoint could fail after the other subtask's split had
         * finished and left the checkpoint, and the restore would then reopen only a split at its
         * start.
         *
         * <p>That holds while the gate is still holding the split back when the subtask arms. Once
         * the gate's minute runs out, injection still needs another record to reach an armed
         * subtask; if none does, {@code FAILED_ONCE} stays false.
         */
        private transient boolean armedHere;

        @Override
        public String map(String name) {
            seenHere++;
            if (armedHere && FAILED_ONCE.compareAndSet(false, true)) {
                INJECTED_BY.set(
                        "subtask "
                                + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                                + " after checkpoint "
                                + firstCoveringCheckpoint);
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return name;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            SEEN_AT_BARRIER.put(
                    context.getCheckpointId()
                            + "/"
                            + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    seenHere);
            if (seenHere > 0 && firstCoveringCheckpoint == 0) {
                firstCoveringCheckpoint = context.getCheckpointId();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // Checkpoint ids start at one and grow, and seenHere only grows, so completing this
            // checkpoint or a later one means a completed checkpoint covered this subtask's
            // entities.
            if (firstCoveringCheckpoint > 0 && checkpointId >= firstCoveringCheckpoint) {
                armedHere = true;
                GatedPageReader.GATE_OPEN.set(true);
            }
        }
    }
}
