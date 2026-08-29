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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySample;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.ScriptedRowKeySampler;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.ScriptedRowStreamOpener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real Flink job over the source, failing once part-way through and recovering.
 *
 * <p>The seams are scripted rather than pointed at the emulator, and the reason is specific to this
 * connector: the emulator's {@code SampleRowKeys} answers with next to no boundaries, so an
 * emulator-driven job runs on <em>one</em> split — and one split cannot show a split being
 * reassigned, nor a truncated range being read from the wrong end. Scripting the sampler is what
 * makes a multi-split failover deterministic.
 *
 * <p>The failure is armed by a checkpoint whose <em>barrier</em> passed records, not by a record
 * count: what a split resumes from is what the barrier saw, and a checkpoint that merely completed
 * late may have crossed the source before a single row was read — restoring to which starts the
 * ranges over and proves nothing.
 */
@Timeout(180)
class BigtableSourceFailoverITCase {

    private static final String OPENER_ID = BigtableSourceFailoverITCase.class.getName();

    /** Enough rows that a checkpoint lands mid-range, few enough that the job stays quick. */
    private static final int ROWS = 200;

    /** Where the scripted sampler cuts, which is what makes this a two-split job. */
    private static final String BOUNDARY = key(ROWS / 2);

    /** Static because the map function is shipped into the job and runs in this same JVM. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    private static final AtomicBoolean CHECKPOINTED_AFTER_RECORDS = new AtomicBoolean();
    private static final AtomicInteger SEEN = new AtomicInteger();

    private static String key(int index) {
        return String.format("row-%04d", index);
    }

    @BeforeEach
    void forgetTheLastRun() {
        // In a hook rather than in the test body: a second test in this class would otherwise
        // inherit FAILED_ONCE from the first, never fail its job, and assert against the previous
        // run's state.
        FAILED_ONCE.set(false);
        CHECKPOINTED_AFTER_RECORDS.set(false);
        SEEN.set(0);
        FailAfterACheckpoint.SEEN_AT_BARRIER.clear();
        ScriptedRowStreamOpener.reset();
    }

    @Test
    void readsEveryRowAcrossAFailureAndResumesRatherThanRestarting() throws Exception {
        String[] keys =
                IntStream.range(0, ROWS)
                        .mapToObj(BigtableSourceFailoverITCase::key)
                        .toArray(String[]::new);
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over(OPENER_ID, keys);
        // Every range stops after a few rows until a checkpoint has covered rows already read, and
        // paces what it does hand over. Without both the table is read to its end before the first
        // qualifying checkpoint, and the failure never fires.
        opener.gateAfter(10, CHECKPOINTED_AFTER_RECORDS::get, 2);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(20);

        List<String> collected = new ArrayList<>();
        try (CloseableIterator<String> records =
                env.fromSource(source(opener), WatermarkStrategy.noWatermarks(), "bigtable")
                        .map(new FailAfterACheckpoint())
                        .executeAndCollect()) {
            records.forEachRemaining(collected::add);
        }

        assertThat(FAILED_ONCE)
                .as(
                        "the job failed once on purpose (rows seen: %s, checkpoints that covered"
                                + " rows: %s)",
                        SEEN.get(), FailAfterACheckpoint.SEEN_AT_BARRIER)
                .isTrue();
        assertThat(collected)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, ROWS)
                                .mapToObj(BigtableSourceFailoverITCase::key)
                                .collect(Collectors.toList()));
        // Two splits, so two opens is a job that never recovered; more than two means a range was
        // reopened, and an exclusive start among them means the reopen resumed rather than started
        // the range over. Without that last clause this test passes on a source that re-reads
        // everything.
        assertThat(opener.openedRanges())
                .as("the recovered reader resumed rather than starting its range over")
                .hasSizeGreaterThan(2)
                .anyMatch(range -> range.startsWith("(row-"));
    }

    private static Source<String, ?, ?> source(ScriptedRowStreamOpener opener) {
        BigtableSourceBuilder<String> builder =
                BigtableSource.<String>builder()
                        .table(TestSources.TABLE)
                        .deserializer(new TestSources.RowKeyDeserializer());
        TestSources.withSamplerFactory(
                builder,
                ScriptedRowKeySampler.Factory.answering(
                        RowKeySample.of(ByteString.copyFromUtf8(BOUNDARY), 1_000L)));
        TestSources.withOpener(builder, opener);
        // A small byte target makes the fetch return after five scripted one-byte rows, so a
        // checkpoint can land mid-range and the restore exercises the byte-boundary path.
        builder.maxRowsPerFetch(1000).maxBytesPerFetch(5);
        return builder.build();
    }

    /** Fails the job once, after a checkpoint whose barrier passed rows has completed. */
    private static final class FailAfterACheckpoint extends RichMapFunction<String, String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        @Override
        public String map(String rowKey) {
            SEEN.incrementAndGet();
            if (CHECKPOINTED_AFTER_RECORDS.get() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return rowKey;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            SEEN_AT_BARRIER.merge(context.getCheckpointId(), SEEN.get(), Math::max);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (SEEN_AT_BARRIER.getOrDefault(checkpointId, 0) > 0) {
                CHECKPOINTED_AFTER_RECORDS.set(true);
            }
        }
    }
}
