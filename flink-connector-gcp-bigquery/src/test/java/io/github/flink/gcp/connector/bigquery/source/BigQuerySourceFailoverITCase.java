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
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
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
import java.util.concurrent.atomic.AtomicReference;
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

    @Test
    void readsEveryRowExactlyOnceAcrossAFailure() throws Exception {
        FAILED_ONCE.set(false);
        CHECKPOINTED_AFTER_RECORDS.set(false);
        FailAfterACheckpoint.SEEN_AT_BARRIER.clear();
        FailAfterACheckpoint.INJECTED_BY.set(null);
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
                        "the job failed once on purpose (records at each checkpoint/subtask"
                                + " barrier: %s)",
                        FailAfterACheckpoint.SEEN_AT_BARRIER)
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
                .as(
                        "the recovered reader resumed rather than starting its stream over"
                                + " (injected by %s;"
                                + " records at each checkpoint/subtask barrier: %s)",
                        FailAfterACheckpoint.INJECTED_BY, FailAfterACheckpoint.SEEN_AT_BARRIER)
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
                .deserializer(BigQueryRowDeserializationSchema.genericRecord(TestRows.SCHEMA_JSON))
                .maxRecordsPerFetch(50)
                .maxBytesPerFetch(32)
                .sessionCreatorFactory(ScriptedReadSessionCreator.Factory.withStreams(STREAMS))
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
     * only a checkpoint that had already seen rows arms the failure, and only in the subtask whose
     * records it saw.
     */
    private static final class FailAfterACheckpoint extends RichMapFunction<GenericRecord, String>
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
         * zero at this subtask's barrier proves its own split's offset had moved. A counter shared
         * by both subtasks could be raised by the other one between the two barriers, which armed
         * the failure on a checkpoint that covered no subtask's records (#1939).
         */
        private transient int seenHere;

        /**
         * The first checkpoint whose barrier found {@code seenHere} above zero; zero before one.
         */
        private transient long firstCoveringCheckpoint;

        /**
         * Whether a checkpoint covering this subtask's records has completed. Only such a subtask
         * fails the job: it fails on its next record, so its split cannot finish first, and every
         * checkpoint the job can restore holds that split past its start. A subtask without records
         * in a completed checkpoint could fail after the other subtask's split had finished and
         * left the checkpoint, and the restore would then reopen only a split at its start.
         */
        private transient boolean armedHere;

        @Override
        public String map(GenericRecord row) {
            seenHere++;
            if (armedHere && FAILED_ONCE.compareAndSet(false, true)) {
                INJECTED_BY.set(
                        "subtask "
                                + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                                + " after checkpoint "
                                + firstCoveringCheckpoint);
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return "row-" + row.get("id");
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
            // records.
            if (firstCoveringCheckpoint > 0 && checkpointId >= firstCoveringCheckpoint) {
                armedHere = true;
                CHECKPOINTED_AFTER_RECORDS.set(true);
            }
        }
    }
}
