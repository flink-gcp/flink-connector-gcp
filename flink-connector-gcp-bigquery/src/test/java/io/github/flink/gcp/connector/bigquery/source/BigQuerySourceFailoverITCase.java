/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source;

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

import io.github.flink.gcp.connector.bigquery.source.enumerator.ScriptedReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ScriptedRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
 * <p>The seams are scripted rather than pointed at an emulator on purpose: the emulator ignores
 * {@code ReadRowsRequest.offset}, so a recovery against it would re-read from row zero and this
 * test would be asserting the emulator's behaviour rather than the connector's. The scripted server
 * honours the offset, as BigQuery does (measured 2026-08-09).
 *
 * <p>The failure is triggered by a checkpoint completing after records have flowed, not by a record
 * count alone: that is what makes the recovery resume from a non-zero offset rather than start
 * over, which the assertion on the reopened offsets then holds.
 */
@Timeout(180)
class BigQuerySourceFailoverITCase {

    private static final String OPENER_ID = BigQuerySourceFailoverITCase.class.getName();
    private static final int ROWS_PER_STREAM = 500;
    private static final int BLOCK_SIZE = 50;
    private static final int STREAMS = 2;

    /** Static because the map function is shipped into the job and runs in this same JVM. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    private static final AtomicBoolean CHECKPOINTED_AFTER_RECORDS = new AtomicBoolean();
    private static final AtomicInteger SEEN = new AtomicInteger();

    @Test
    void readsEveryRowExactlyOnceAcrossAFailure() throws Exception {
        FAILED_ONCE.set(false);
        CHECKPOINTED_AFTER_RECORDS.set(false);
        SEEN.set(0);
        FailAfterACheckpoint.SEEN_AT_BARRIER.clear();
        ScriptedRowStreamOpener.reset(OPENER_ID);
        // Every stream stops after its first block until a checkpoint has covered records already
        // read. Without it the table can be read to its end before that happens, and the failure
        // this test is about never fires — which is how its first form was flaky.
        ScriptedRowStreamOpener.gate(OPENER_ID, CHECKPOINTED_AFTER_RECORDS::get);

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
                env.fromSource(source(), WatermarkStrategy.noWatermarks(), "bigquery")
                        .map(new FailAfterACheckpoint())
                        .executeAndCollect()) {
            records.forEachRemaining(collected::add);
        }

        assertThat(FAILED_ONCE)
                .as(
                        "the job failed once on purpose (records seen: %s, checkpoints that covered"
                                + " records: %s)",
                        SEEN.get(), FailAfterACheckpoint.SEEN_AT_BARRIER)
                .isTrue();
        assertThat(collected)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, ROWS_PER_STREAM * STREAMS)
                                .mapToObj(id -> "row-" + id)
                                .collect(Collectors.toList()));
        // More opens than streams means a stream was reopened, and a non-zero offset among them
        // means the reopen resumed. Only a recovery reopens here: the readers' one wake-up is the
        // shutdown that follows the last row.
        assertThat(ScriptedRowStreamOpener.opens(OPENER_ID))
                .as("the recovered reader resumed rather than starting its stream over")
                .hasSizeGreaterThan(STREAMS)
                .anyMatch(open -> !open.endsWith("@0"));
    }

    private static Source<GenericRecord, ?, ?> source() {
        // Each stream holds its own rows, so a duplicate or a gap names the stream it came from.
        Map<String, int[]> rowsByStream = new HashMap<>();
        for (int stream = 0; stream < STREAMS; stream++) {
            rowsByStream.put(
                    ScriptedReadSessionCreator.streamName(stream),
                    new int[] {stream * ROWS_PER_STREAM, ROWS_PER_STREAM});
        }
        return BigQuerySource.<GenericRecord>builder()
                .table(TestSources.TABLE)
                .deserializer(BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON))
                .maxRecordsPerFetch(50)
                .sessionCreator(ScriptedReadSessionCreator.withStreams(STREAMS))
                // Ten milliseconds a block, so the job spends long enough reading for checkpoints
                // to
                // complete while rows flow; without that the whole table is read before the
                // first qualifying checkpoint and the failure never happens.
                .rowStreamOpener(new ScriptedRowStreamOpener(OPENER_ID, rowsByStream, 50, 10))
                .build();
    }

    /**
     * Fails the job once, after a checkpoint whose <em>barrier</em> passed records has completed.
     *
     * <p>Both halves matter. A checkpoint that merely completes late says nothing: its barrier may
     * have crossed the source before a single row was emitted, and restoring to it starts the
     * streams over — which is what an earlier version of this test measured. What the source's
     * offset follows is the barrier, so the record count is captured in {@code snapshotState} and
     * only a checkpoint that had already seen rows arms the failure.
     */
    private static final class FailAfterACheckpoint extends RichMapFunction<GenericRecord, String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        static final Map<Long, Integer> SEEN_AT_BARRIER = new ConcurrentHashMap<>();

        @Override
        public String map(GenericRecord row) {
            SEEN.incrementAndGet();
            if (CHECKPOINTED_AFTER_RECORDS.get() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return "row-" + row.get("id");
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
