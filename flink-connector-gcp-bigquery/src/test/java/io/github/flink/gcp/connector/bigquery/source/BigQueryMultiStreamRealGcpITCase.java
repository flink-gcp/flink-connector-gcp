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

import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real Flink job over a read session BigQuery splits into several streams, failing once part-way
 * through, against BigQuery itself.
 *
 * <p>What no other tier covers. The unit tests drive one stream against a fake, and {@link
 * BigQuerySourceFailoverITCase} drives a scripted two-stream session on a MiniCluster — both decide
 * for themselves how many streams there are. Here BigQuery decides, and the failure exercises the
 * path the connector cannot script: a subtask dying with a stream in hand, its splits coming back
 * through {@code addSplitsBack}, and another subtask finishing them.
 *
 * <p><b>The table is a public dataset, and that is the cost decision.</b> The read is billed to
 * {@code BQ_IT_PROJECT} at the Storage Read API's per-byte rate, so this run costs a fraction of a
 * cent for its 264 MB — where a table of our own large enough for BigQuery to split would have to
 * be created and then stored. {@code austin_bikeshare.bikeshare_trips} is the smallest candidate
 * measured that BigQuery splits at all: 195 MB answered with one stream and 264 MB with four
 * (measured 2026-08-10), so the fixture sits just above a threshold BigQuery does not document.
 * Reading every column is load-bearing rather than lazy: the stream count follows the bytes
 * actually selected, and each of this table's columns read on its own answered with a single
 * stream.
 *
 * <p>The table is public and therefore not ours to hold still, so both the read and the row count
 * it is checked against are pinned to the same instant with {@code snapshotTime} and {@code FOR
 * SYSTEM TIME AS OF}. Without that, a row inserted between the two would read as a lost or
 * duplicated one.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@Timeout(600)
class BigQueryMultiStreamRealGcpITCase {

    private static final TableDestination TABLE =
            TableDestination.of("bigquery-public-data", "austin_bikeshare", "bikeshare_trips");

    /**
     * Only {@code trip_id}, resolved out of the session's own schema by Avro.
     *
     * <p>The read session still selects every column, because that is what makes BigQuery split the
     * table; the reader schema only keeps the decoded record small.
     */
    private static final String READER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"trip_id\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    /** Static because the map function is shipped into a job running in this same JVM. */
    private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    private static final AtomicBoolean CHECKPOINTED_AFTER_RECORDS = new AtomicBoolean();
    private static final Set<Integer> SUBTASKS_THAT_READ = ConcurrentHashMap.newKeySet();

    @Test
    void readsEveryRowExactlyOnceAcrossAFailureOverSeveralStreams() throws Exception {
        FAILED_ONCE.set(false);
        CHECKPOINTED_AFTER_RECORDS.set(false);
        SUBTASKS_THAT_READ.clear();

        // A minute back, so the instant is inside the time-travel window and safely behind any
        // write in flight against the public table.
        Instant snapshot = Instant.now().minus(Duration.ofMinutes(1));
        long expected = rowCountAsOf(snapshot);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(1000);

        AtomicLong read = new AtomicLong();
        try (CloseableIterator<Byte> records =
                env.fromSource(source(snapshot), WatermarkStrategy.noWatermarks(), "bigquery")
                        .map(new FailAfterACheckpoint())
                        .executeAndCollect()) {
            // Counted rather than collected: two million trip ids would be held in this JVM for no
            // assertion that needs them. executeAndCollect is what makes the count exactly-once —
            // it deduplicates by checkpoint, where an ordinary sink would re-receive whatever the
            // restart replayed and this test would pass however the source behaved.
            records.forEachRemaining(record -> read.incrementAndGet());
        }

        assertThat(FAILED_ONCE).as("the job failed once on purpose").isTrue();
        assertThat(SUBTASKS_THAT_READ)
                .as(
                        "BigQuery still splits this table, so more than one subtask read from it —"
                                + " a single stream would make the failure a single-reader case and"
                                + " this class would be testing nothing the others do not")
                .hasSizeGreaterThan(1);
        assertThat(read).hasValue(expected);
    }

    /** The table's row count at the same instant the read is pinned to. */
    private static long rowCountAsOf(Instant snapshot) throws InterruptedException {
        return RealBigQuery.queryLongs(
                        "SELECT COUNT(*) FROM `bigquery-public-data.austin_bikeshare"
                                + ".bikeshare_trips` FOR SYSTEM_TIME AS OF TIMESTAMP('"
                                + snapshot
                                + "')")
                .get(0);
    }

    private static Source<GenericRecord, ?, ?> source(Instant snapshot) {
        return BigQuerySource.<GenericRecord>builder()
                .table(TABLE)
                // The public dataset cannot be billed for its own reads, so the session belongs to
                // the project the gated suite runs in.
                .parentProject(RealBigQuery.project())
                .deserializer(BigQueryRowDeserializer.genericRecord(READER_SCHEMA))
                .snapshotTime(snapshot)
                .build();
    }

    /**
     * Fails the job once, after a checkpoint whose barrier had already passed records.
     *
     * <p>The same shape as {@link BigQuerySourceFailoverITCase}'s, and for the same reason: a
     * checkpoint that merely completes says nothing, since its barrier may have crossed the source
     * before a row was emitted, and restoring to that one starts every stream over. Only a
     * checkpoint that had already seen rows arms the failure, so the recovery this measures is a
     * resume.
     */
    private static final class FailAfterACheckpoint extends RichMapFunction<GenericRecord, Byte>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private long seen;
        private long seenAtLastBarrier;

        @Override
        public Byte map(GenericRecord record) {
            seen++;
            SUBTASKS_THAT_READ.add(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            if (CHECKPOINTED_AFTER_RECORDS.get() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return (byte) 0;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            seenAtLastBarrier = seen;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (seenAtLastBarrier > 0) {
                CHECKPOINTED_AFTER_RECORDS.set(true);
            }
        }
    }
}
