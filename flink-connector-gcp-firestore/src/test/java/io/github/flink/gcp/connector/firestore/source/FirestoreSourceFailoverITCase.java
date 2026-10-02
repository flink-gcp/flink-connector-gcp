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

package io.github.flink.gcp.connector.firestore.source;

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

import com.google.cloud.firestore.WriteBatch;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real Flink job over the source against the emulator, failing once part-way through and
 * recovering: every document is read, and a restored split resumes after its last document rather
 * than starting over.
 *
 * <p>The failure is armed by a checkpoint whose barrier passed documents, not by a count: what a
 * split resumes from is what the barrier saw, and a checkpoint that crossed the source before any
 * document proves nothing.
 */
class FirestoreSourceFailoverITCase extends AbstractFirestoreEmulatorITCase {

    private static final int DOCUMENTS = 120;

    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    /** Every document either subtask's map saw, for the failure message only. */
    private static final AtomicInteger SEEN = new AtomicInteger();

    @BeforeEach
    void forgetTheLastRun() {
        FAILED_ONCE.set(false);
        SEEN.set(0);
        FailAfterACheckpoint.SEEN_AT_BARRIER.clear();
        GatedPageReader.reset();
    }

    @Test
    void readsEveryDocumentAcrossAFailureAndResumesAfterTheLastOne() throws Exception {
        String group = uniqueCollection();
        List<String> paths = new ArrayList<>();
        WriteBatch batch = client().batch();
        for (int n = 0; n < DOCUMENTS; n++) {
            String path = group + "/d" + String.format("%03d", n);
            paths.add(path);
            batch.set(client().document(path), Map.of("n", n));
        }
        batch.commit().get(30, TimeUnit.SECONDS);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(20);

        FirestoreSourceBuilder<String> builder =
                FirestoreSource.<String>builder()
                        .database(database())
                        .collectionGroup(group)
                        .deserializer(new TestSources.DocumentPathDeserializer())
                        .pageSize(5)
                        .emulatorEndpoint(emulatorEndpoint());
        TestSources.withPlannerFactory(
                builder,
                new FixedPartitionsPlannerFactory(
                        emulatorEndpoint(), List.of(paths.get(DOCUMENTS / 2))));
        TestSources.withPageReader(builder, new GatedPageReader(emulatorEndpoint(), 4));

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> records =
                env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "firestore")
                        .map(new FailAfterACheckpoint())
                        .executeAndCollect()) {
            records.forEachRemaining(collected::add);
        }

        assertThat(FAILED_ONCE)
                .as(
                        "the job failed once on purpose (documents seen: %s, checkpoints: %s)",
                        SEEN.get(), FailAfterACheckpoint.SEEN_AT_BARRIER)
                .isTrue();
        assertThat(collected).containsExactlyInAnyOrderElementsOf(paths);
        // Two partitions start inclusively or unbounded. A split that starts *after* a document
        // can only be one a checkpoint rewrote: this names the resume mechanism, where a source
        // re-reading its restored splits from the beginning would show only as duplicates above.
        assertThat(GatedPageReader.STARTED_SPLITS)
                .as("the restored reader resumed after its last document")
                .hasSizeGreaterThan(2)
                .anyMatch(FirestoreSourceFailoverITCase::startsAfterADocument);
    }

    private static boolean startsAfterADocument(RunQueryRequest request) {
        StructuredQuery query = request.getStructuredQuery();
        return query.hasStartAt() && !query.getStartAt().getBefore();
    }

    /** Fails the job once, after a checkpoint whose barrier passed documents has completed. */
    private static final class FailAfterACheckpoint extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        /**
         * What this subtask's map saw, not the job's: source and map are chained, so a count above
         * zero at this subtask's barrier proves its own split state had moved. The shared {@code
         * SEEN} could be raised by the other subtask between the two barriers.
         */
        private transient int seenHere;

        @Override
        public String map(String path) {
            SEEN.incrementAndGet();
            seenHere++;
            if (GatedPageReader.GATE_OPEN.get() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return path;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            SEEN_AT_BARRIER.merge(context.getCheckpointId(), seenHere, Math::max);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (SEEN_AT_BARRIER.getOrDefault(checkpointId, 0) > 0) {
                GatedPageReader.GATE_OPEN.set(true);
            }
        }
    }
}
