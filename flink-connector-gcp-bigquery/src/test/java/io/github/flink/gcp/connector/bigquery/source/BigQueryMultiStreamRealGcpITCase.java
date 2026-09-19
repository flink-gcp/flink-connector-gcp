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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
 * <p><b>The table is a public dataset, and that is the cost decision.</b> Session planning reported
 * 264 MB for the selected fields, and this test then calls {@code ReadRows}, so the documented
 * bytes-read pricing applies to {@code BQ_IT_PROJECT}. Whether the bytes-read component produces a
 * monetary charge depends on the billing account's monthly free-tier use; applicable network
 * transfer is charged separately. A table of our own large enough for BigQuery to split would also
 * have to be created and then stored. {@code austin_bikeshare.bikeshare_trips} is the smallest
 * candidate measured that BigQuery splits at all: 195 MB answered with one stream and 264 MB with
 * four (measured 2026-08-10), so the fixture sits just above a threshold BigQuery does not
 * document. Reading every column is load-bearing rather than lazy: the stream count follows the
 * bytes actually selected, and each of this table's columns read on its own answered with a single
 * stream.
 *
 * <p>The table is public and therefore not ours to hold still, so both the read and the row count
 * it is checked against are pinned to the same instant with {@code snapshotTime} and {@code FOR
 * SYSTEM TIME AS OF}. Without that, a row inserted between the two would read as a lost or
 * duplicated one.
 *
 * <p><b>Swapping the table is meant to be a one-line change</b>, because a public dataset can be
 * withdrawn or reshaped and the replacement's only requirements are that BigQuery splits it and
 * that it is small enough to read in a test. So nothing here names a column: the reader schema
 * declares no fields at all, the query derives its path from {@link #TABLE}, and the assertions are
 * about how many rows came back rather than what was in them. Point {@code TABLE} at another table
 * and the rest follows.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "BQ_IT_PROJECT", matches = ".+")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
class BigQueryMultiStreamRealGcpITCase {

    private static final TableDestination TABLE =
            TableDestination.of("bigquery-public-data", "austin_bikeshare", "bikeshare_trips");

    /**
     * A record with no fields, which every table's schema resolves into.
     *
     * <p>Avro skips a writer field the reader does not declare, so this decodes any row BigQuery
     * sends into an empty record — which is all this case needs, since it counts rows rather than
     * reading them. That is what keeps the fixture swappable: naming even one column would tie the
     * test to a table's schema. The read session still selects every column, since that is what
     * makes BigQuery split the table in the first place.
     *
     * <p>It also rests on Avro not requiring the two <em>record names</em> to match — BigQuery
     * writes {@code __root__} and this says {@code Row} — which in Avro 1.12.1 is a check {@code
     * Resolver} carries commented out, with "current implementation doesn't do this check. To pass
     * regressions tests, we can't either." An Avro release that restores it would fail this case,
     * and every hand-written reader schema besides.
     */
    private static final String READER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":[]}";

    /** Static because the map function is shipped into a job running in this same JVM. */
    static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

    static CompletableFuture<Void> checkpointGate = new CompletableFuture<>();
    static final Set<Integer> SUBTASKS_THAT_READ = ConcurrentHashMap.newKeySet();

    @TempDir Path checkpointDirectory;

    @Test
    void readsEveryRowExactlyOnceAcrossAFailureOverSeveralStreams() throws Exception {
        resetProbe();

        // A minute back keeps the snapshot inside the time-travel window and safely behind writes
        // in flight against the public table. BigQuery's timestamp string conversion accepts at
        // most six fractional digits. Truncate once so the count query and Storage Read share
        // the same instant.
        Instant snapshot =
                Instant.now().minus(Duration.ofMinutes(1)).truncatedTo(ChronoUnit.MICROS);
        long expected = rowCountAsOf(snapshot);

        Configuration configuration = new Configuration();
        // The collect sink checkpoints its buffered results, which can exceed the default
        // memory-backed storage's 5 MiB limit even though the iterator only counts rows.
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory.toUri().toString());
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(1000);

        AtomicLong read = new AtomicLong();
        try (CloseableIterator<Byte> records =
                env.fromSource(
                                new CheckpointGatedSource(source(snapshot)),
                                WatermarkStrategy.noWatermarks(),
                                "bigquery")
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

    static void resetProbe() {
        FAILED_ONCE.set(false);
        checkpointGate = new CompletableFuture<>();
        SUBTASKS_THAT_READ.clear();
    }

    /** Keeps the production reader's task thread available for a non-empty checkpoint. */
    static final class CheckpointGatedSource extends BigQueryStorageReadSource<GenericRecord> {
        private static final long serialVersionUID = 1L;

        CheckpointGatedSource(
                Source<GenericRecord, ReadStreamSplit, BigQueryReadEnumeratorState> source) {
            super(((BigQueryStorageReadSource<GenericRecord>) source).getConfig());
        }

        @Override
        public SourceReader<GenericRecord, ReadStreamSplit> createReader(
                SourceReaderContext context) throws Exception {
            return new CheckpointGatedReader(
                    super.createReader(context),
                    context.metricGroup().getIOMetricGroup().getNumRecordsInCounter());
        }
    }

    /** Pauses only polling; checkpointing, assignment and shutdown still reach the real reader. */
    static final class CheckpointGatedReader
            implements SourceReader<GenericRecord, ReadStreamSplit> {
        private final SourceReader<GenericRecord, ReadStreamSplit> delegate;
        private final Counter consumed;

        CheckpointGatedReader(
                SourceReader<GenericRecord, ReadStreamSplit> delegate, Counter consumed) {
            this.delegate = delegate;
            this.consumed = consumed;
        }

        private boolean paused() {
            // SourceReaderBase counts consumed rows per reader attempt, before deserialization.
            // Aggregate subtask participation survives restart and cannot govern this gate.
            return !checkpointGate.isDone() && consumed.getCount() > 0;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<GenericRecord> output) throws Exception {
            return paused() ? InputStatus.NOTHING_AVAILABLE : delegate.pollNext(output);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return paused() ? checkpointGate : delegate.isAvailable();
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public List<ReadStreamSplit> snapshotState(long checkpointId) {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void addSplits(List<ReadStreamSplit> splits) {
            delegate.addSplits(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            delegate.notifyNoMoreSplits();
        }

        @Override
        public void handleSourceEvents(SourceEvent event) {
            delegate.handleSourceEvents(event);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            delegate.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            delegate.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void pauseOrResumeSplits(Collection<String> pause, Collection<String> resume) {
            delegate.pauseOrResumeSplits(pause, resume);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    /**
     * The table's row count at the same instant the read is pinned to.
     *
     * <p>Free, whatever the table's size: {@code COUNT(*)} is answered from metadata and bills no
     * bytes. The path is derived from {@link #TABLE} rather than written out, so the table is named
     * exactly once in this class.
     */
    private static long rowCountAsOf(Instant snapshot) throws InterruptedException {
        return RealBigQuery.queryLongs(
                        "SELECT COUNT(*) FROM `"
                                + TABLE.getProject()
                                + "."
                                + TABLE.getDataset()
                                + "."
                                + TABLE.getTable()
                                + "` FOR SYSTEM_TIME AS OF TIMESTAMP('"
                                + snapshot
                                + "')")
                .get(0);
    }

    private static Source<GenericRecord, ReadStreamSplit, BigQueryReadEnumeratorState> source(
            Instant snapshot) {
        return BigQuerySource.<GenericRecord>builder()
                .table(TABLE)
                // The public dataset cannot be billed for its own reads, so the session belongs to
                // the project the gated suite runs in.
                .parentProject(RealBigQuery.project())
                .deserializer(BigQueryRowDeserializationSchema.genericRecord(READER_SCHEMA))
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
     * resume. The reader pauses after its first emitted row until that checkpoint completes, so a
     * fast read cannot finish before failure injection is armed.
     */
    static final class FailAfterACheckpoint extends RichMapFunction<GenericRecord, Byte>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private long seen;
        private final Map<Long, Long> seenAtBarrier = new HashMap<>();

        @Override
        public Byte map(GenericRecord record) {
            seen++;
            SUBTASKS_THAT_READ.add(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            if (checkpointGate.isDone() && FAILED_ONCE.compareAndSet(false, true)) {
                throw new IllegalStateException("Failing the job once, on purpose.");
            }
            return (byte) 0;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            seenAtBarrier.put(context.getCheckpointId(), seen);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (seenAtBarrier.getOrDefault(checkpointId, 0L) > 0) {
                checkpointGate.complete(null);
            }
            seenAtBarrier.keySet().removeIf(id -> id <= checkpointId);
        }
    }
}
