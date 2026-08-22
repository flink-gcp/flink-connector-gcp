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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the failure issue #990 reported, end to end, against the emulator.
 *
 * <p>The connector-side half of that defect — two enumerators over one source object must not share
 * a seam — is pinned deterministically by {@code SpannerBatchReadSourcePlannerLifecycleTest}. The
 * Flink-side half — a global restore builds the next enumerator from {@code
 * Source#createEnumerator} on the <em>same</em> object — is measured by {@code
 * BigQueryQueryJobIdentityITCase} on every supported version. This class is the composition: a real
 * job, a real coordinator reset, and the real planner.
 *
 * <p><b>Which failure, and why not a simpler one.</b> A task failure does not rebuild the
 * enumerator, under the default {@code region} failover strategy or under {@code full} alike; only
 * the global-restore path resets coordinators, and in one JVM it is reached by a
 * coordinator-reported failure, since {@code SplitEnumeratorContext#failJob} escalates to {@code
 * handleGlobalFailure}. So the probe below throws from a coordinator action rather than from a map
 * function, which is what separates this class from {@code SpannerSourceFailoverITCase} beside it.
 *
 * <p><b>Checkpointing is off deliberately.</b> That is the window the defect lived in and the
 * ordinary configuration for a bounded read: with no {@code CheckpointCoordinator} the restore
 * carries no state, the enumerator is rebuilt from scratch, and it has to plan again. Before the
 * repair the second planning call met a planner the first enumerator's teardown had closed, and the
 * job failed with {@code The Spanner partition planner for … was closed before it was used.} and
 * did not recover — measured by reverting the repair and running this class, which then ends in a
 * terminal {@code JobExecutionException} rather than completing on the restart it was given.
 */
@Timeout(180)
class SpannerSourceGlobalFailoverITCase extends AbstractSpannerEmulatorITCase {

    private static final int ROWS = 200;

    /** The runs whose one deliberate coordinator failure has already fired. */
    private static final Set<String> FAILED = ConcurrentHashMap.newKeySet();

    /** How many enumerators each run built, which is the recreation half of the measurement. */
    private static final ConcurrentHashMap<String, AtomicInteger> ENUMERATORS =
            new ConcurrentHashMap<>();

    /** What the map function saw, since the collect protocol cannot be used here (see below). */
    private static final ConcurrentHashMap<String, Collection<Long>> RECORDED =
            new ConcurrentHashMap<>();

    @Test
    void aGlobalFailoverBeforeAnyCheckpointPlansAgainAndDelivers() throws Exception {
        String runId = "global-" + System.nanoTime();
        SpannerDatabase database = seededDatabase();

        Configuration configuration = new Configuration();
        // One restart: the injected global failure recovers, anything further fails the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        // No checkpointing, deliberately: see the class javadoc.

        // Recorded through a map function rather than executeAndCollect(): with checkpointing off
        // the collect protocol takes UncheckpointedCollectResultBuffer with failureTolerance=false,
        // whose sinkRestarted() throws "Job restarted" as soon as the client has seen the first
        // attempt's sink version. That is a race against the deliberate failure below, and it would
        // fail this test with a message naming nothing about planners. The precedent this class
        // cites sinks to a discarding sink for the same reason.
        env.fromSource(
                        new FailOnceCoordinatorSource(runId, source(database)),
                        WatermarkStrategy.noWatermarks(),
                        "spanner")
                .map(new RecordingMap(runId))
                .sinkTo(new DiscardingSink<>());
        env.execute();
        List<Long> ids = new ArrayList<>(RECORDED.getOrDefault(runId, Collections.emptyList()));

        assertThat(FAILED)
                .as("the deliberate coordinator failure fired, so a global restore happened")
                .contains(runId);
        assertThat(ENUMERATORS.get(runId).get())
                .as("one enumerator before the failure and one the global restore rebuilt")
                .isEqualTo(2);
        // Every row, and no duplicates — but the absence of duplicates is a property of the run
        // rather than of the connector. The failure lands before any reader has finished a
        // partition, so nothing is replayed. Where a partition *is* replayed, rows repeat, which is
        // what SpannerSourceFailoverITCase asserts beside this.
        assertThat(ids)
                .as("the read the restore planned afresh delivers every row")
                .containsExactlyInAnyOrderElementsOf(expectedIds());
    }

    private static Source<Long, PartitionSplit, SpannerBatchEnumeratorState> source(
            SpannerDatabase database) {
        return SpannerSource.<Long>builder()
                .database(database)
                .readOperation(SpannerReadOperation.query(Statement.of("SELECT id FROM singers")))
                .deserializer(new TestSources.IdDeserializer())
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    private static List<Long> expectedIds() {
        return LongStream.range(0, ROWS).boxed().collect(Collectors.toList());
    }

    private static SpannerDatabase seededDatabase() throws Exception {
        SpannerDatabase database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE singers (id INT64 NOT NULL, name STRING(64))"
                                + " PRIMARY KEY (id)");
        List<Mutation> rows = new ArrayList<>();
        for (long id = 0; id < ROWS; id++) {
            rows.add(
                    Mutation.newInsertOrUpdateBuilder("singers")
                            .set("id")
                            .to(id)
                            .set("name")
                            .to("singer-" + id)
                            .build());
        }
        client(database).write(rows);
        return database;
    }

    /** Records what reached the pipeline, keyed by run, since collect() cannot be used here. */
    private static final class RecordingMap extends RichMapFunction<Long, Long> {

        private static final long serialVersionUID = 1L;

        private final String runId;

        RecordingMap(String runId) {
            this.runId = runId;
        }

        @Override
        public Long map(Long value) {
            RECORDED.computeIfAbsent(runId, unused -> new ConcurrentLinkedQueue<>()).add(value);
            return value;
        }
    }

    /**
     * Wraps the real source, counting enumerators and failing the first one from the coordinator.
     *
     * <p>Every call delegates, so what the job runs is the connector's own source: the count and
     * the one throw are the only things this adds.
     */
    private static final class FailOnceCoordinatorSource
            implements Source<Long, PartitionSplit, SpannerBatchEnumeratorState>,
                    ResultTypeQueryable<Long> {

        private static final long serialVersionUID = 1L;

        private final String runId;
        private final Source<Long, PartitionSplit, SpannerBatchEnumeratorState> delegate;

        FailOnceCoordinatorSource(
                String runId, Source<Long, PartitionSplit, SpannerBatchEnumeratorState> delegate) {
            this.runId = runId;
            this.delegate = delegate;
        }

        @Override
        public Boundedness getBoundedness() {
            return delegate.getBoundedness();
        }

        @Override
        public SourceReader<Long, PartitionSplit> createReader(SourceReaderContext context)
                throws Exception {
            return delegate.createReader(context);
        }

        @Override
        public SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> createEnumerator(
                SplitEnumeratorContext<PartitionSplit> context) throws Exception {
            ENUMERATORS.computeIfAbsent(runId, unused -> new AtomicInteger()).incrementAndGet();
            return new FailOnceEnumerator(runId, delegate.createEnumerator(context));
        }

        @Override
        public SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> restoreEnumerator(
                SplitEnumeratorContext<PartitionSplit> context,
                SpannerBatchEnumeratorState checkpoint)
                throws Exception {
            ENUMERATORS.computeIfAbsent(runId, unused -> new AtomicInteger()).incrementAndGet();
            return new FailOnceEnumerator(runId, delegate.restoreEnumerator(context, checkpoint));
        }

        @Override
        public SimpleVersionedSerializer<PartitionSplit> getSplitSerializer() {
            return delegate.getSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<SpannerBatchEnumeratorState>
                getEnumeratorCheckpointSerializer() {
            return delegate.getEnumeratorCheckpointSerializer();
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return ((ResultTypeQueryable<Long>) delegate).getProducedType();
        }
    }

    /** Delegates to the real enumerator, throwing once per run from a coordinator action. */
    private static final class FailOnceEnumerator
            implements SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> {

        private final String runId;
        private final SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> delegate;

        FailOnceEnumerator(
                String runId,
                SplitEnumerator<PartitionSplit, SpannerBatchEnumeratorState> delegate) {
            this.runId = runId;
            this.delegate = delegate;
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (FAILED.add(runId)) {
                // Thrown from a coordinator action, which is what escalates to a global failure and
                // therefore rebuilds this enumerator. A task-side throw would not.
                throw new IllegalStateException("deliberate failure to force a global failover");
            }
            delegate.handleSplitRequest(subtaskId, requesterHostname);
        }

        @Override
        public void addSplitsBack(List<PartitionSplit> splits, int subtaskId) {
            delegate.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            delegate.addReader(subtaskId);
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
            delegate.handleSourceEvent(subtaskId, sourceEvent);
        }

        @Override
        public SpannerBatchEnumeratorState snapshotState(long checkpointId) throws Exception {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
