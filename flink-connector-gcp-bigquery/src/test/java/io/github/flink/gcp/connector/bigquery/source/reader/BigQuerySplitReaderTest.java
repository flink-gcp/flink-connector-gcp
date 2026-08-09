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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQuerySplitReaderTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";

    private TestReaderMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new TestReaderMetrics();
        ScriptedRowStreamOpener.reset(testId());
    }

    @Test
    void opensAtTheSplitsOffset() throws Exception {
        BigQuerySplitReader reader = reader(opener(10, 10), 100);
        reader.handleSplitsChanges(addition(split(4)));

        List<GenericRecord> rows = drain(reader);

        assertThat(ScriptedRowStreamOpener.offsets(testId())).containsExactly(4L);
        assertThat(names(rows))
                .containsExactly("row-4", "row-5", "row-6", "row-7", "row-8", "row-9");
    }

    @Test
    void capsEveryBatchEvenWhenOneBlockCarriesEverything() throws Exception {
        // The emulator answers a whole table in one block, so this is its shape as well as the
        // shape of a 128 MiB BigQuery block.
        BigQuerySplitReader reader = reader(opener(10, 10), 3);
        reader.handleSplitsChanges(addition(split(0)));

        List<Integer> batchSizes = new ArrayList<>();
        List<GenericRecord> rows = new ArrayList<>();
        drain(reader, rows, batchSizes);

        assertThat(batchSizes).startsWith(3, 3, 3, 1);
        assertThat(rows).hasSize(10);
    }

    @Test
    void reportsTheSplitFinishedInTheFetchThatReachesTheEnd() throws Exception {
        BigQuerySplitReader reader = reader(opener(2, 10), 100);
        reader.handleSplitsChanges(addition(split(0)));

        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        assertThat(count(records)).isEqualTo(2);
        assertThat(records.finishedSplits()).containsExactly(STREAM);
    }

    @Test
    void reportsTheSplitFinishedOnTheNextFetchWhenTheCapEndedTheBatch() throws Exception {
        // The cap stops the batch exactly at the last row, so this fetch cannot know the stream
        // ended; the next one asks and finds out.
        BigQuerySplitReader reader = reader(opener(2, 10), 2);
        reader.handleSplitsChanges(addition(split(0)));

        RecordsWithSplitIds<GenericRecord> first = reader.fetch();
        assertThat(count(first)).isEqualTo(2);
        assertThat(first.finishedSplits()).isEmpty();

        assertThat(reader.fetch().finishedSplits()).containsExactly(STREAM);
    }

    @Test
    void reportsASplitRestoredAtTheStreamsRowCountFinished() throws Exception {
        // The window a checkpoint can land in: every row emitted, the split not yet recorded as
        // finished. BigQuery answers such a read with an empty stream and no error (measured
        // 2026-08-09), and that is the whole of the handling — there is no flag to mark.
        BigQuerySplitReader reader = reader(opener(10, 10), 100);
        reader.handleSplitsChanges(addition(split(10)));

        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        assertThat(count(records)).isZero();
        assertThat(records.finishedSplits()).containsExactly(STREAM);
        assertThat(ScriptedRowStreamOpener.offsets(testId())).containsExactly(10L);
    }

    @Test
    void reopensPastTheRowsAlreadyHandedOverAfterAWakeUp() throws Exception {
        // The cap and the block size are deliberately unaligned — three rows taken out of a block
        // of four — so the reopen has to discard the row left in the cursor. Aligned, the discard
        // would be a no-op and removing it would go unnoticed until a job read a row twice.
        BigQuerySplitReader reader = reader(opener(10, 4), 3);
        reader.handleSplitsChanges(addition(split(0)));

        RecordsWithSplitIds<GenericRecord> first = reader.fetch();
        assertThat(count(first)).isEqualTo(3);

        reader.wakeUp();
        List<GenericRecord> rest = drain(reader);

        // Reopened at three, not at zero: the rows already handed over must not be read again, and
        // the fourth row of the first block must not be handed over from the stale cursor.
        assertThat(ScriptedRowStreamOpener.offsets(testId())).containsExactly(0L, 3L);
        assertThat(names(rest))
                .containsExactly("row-3", "row-4", "row-5", "row-6", "row-7", "row-8", "row-9");
    }

    @Test
    void handsOverWhatItDecodedWhenAWakeUpCancelsACallInFlight() throws Exception {
        // The other half of wakeUp(): the call is already in flight when it lands, so next() throws
        // and the batch decoded so far still has to reach the task thread.
        WakingOpener opener = new WakingOpener();
        BigQuerySplitReader reader = new BigQuerySplitReader(opener, 100, null, metrics.metrics());
        opener.wakeOn(reader);
        reader.handleSplitsChanges(addition(split(0)));

        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        assertThat(names(collectInto(records))).containsExactly("row-0", "row-1");
        assertThat(records.finishedSplits()).isEmpty();
    }

    @Test
    void countsRowsAndBytesAsBlocksArrive() throws Exception {
        BigQuerySplitReader reader = reader(opener(6, 3), 100);
        reader.handleSplitsChanges(addition(split(0)));

        drain(reader);

        // Named by string literal, never by the constant: a constant is inlined into the test at
        // compile time, so comparing it against itself would pass for any value.
        assertThat(metrics.counter("rowsRead")).isEqualTo(6);
        assertThat(metrics.counter("bytesRead")).isPositive();
    }

    @Test
    void wrapsAReadFailureWithTheStreamAndOffset() {
        RowStreamOpener failing =
                new RowStreamOpener() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public RowStream open(String streamName, long offset) {
                        return new RowStream() {
                            @Override
                            public com.google.cloud.bigquery.storage.v1.ReadRowsResponse next() {
                                throw new IllegalStateException("boom");
                            }

                            @Override
                            public void cancel() {}

                            @Override
                            public void close() {}
                        };
                    }

                    @Override
                    public void close() {}
                };
        BigQuerySplitReader reader = new BigQuerySplitReader(failing, 10, null, metrics.metrics());
        reader.handleSplitsChanges(addition(split(7)));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining(STREAM)
                .hasMessageContaining("offset 7");
    }

    @Test
    void dropsARemovedSplitWithoutReadingIt() throws Exception {
        BigQuerySplitReader reader = reader(opener(10, 10), 100);
        reader.handleSplitsChanges(addition(split(0)));
        reader.handleSplitsChanges(new SplitsRemoval<>(Collections.singletonList(split(0))));

        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        assertThat(count(records)).isZero();
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(ScriptedRowStreamOpener.offsets(testId())).isEmpty();
    }

    @Test
    void cancelsAStreamOpenedIntoAWakeUpThatArrivedFirst() throws Exception {
        // The narrow window in reopen(): the wake-up lands after the cancelled flag is cleared and
        // before the call exists, so it finds no stream to cancel. Without the re-check that
        // follows the open, the fetch below would block on a call nothing will ever interrupt.
        WakeOnOpenOpener opener = new WakeOnOpenOpener();
        BigQuerySplitReader reader = new BigQuerySplitReader(opener, 100, null, metrics.metrics());
        opener.wakeOn(reader);
        reader.handleSplitsChanges(addition(split(0)));

        RecordsWithSplitIds<GenericRecord> records = reader.fetch();

        assertThat(count(records)).isZero();
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(opener.cancelled()).isTrue();
    }

    private static BigQueryReadStreamSplit split(long offset) {
        return new BigQueryReadStreamSplit(STREAM, offset, TestRows.SCHEMA_JSON);
    }

    private static List<GenericRecord> collectInto(RecordsWithSplitIds<GenericRecord> records) {
        List<GenericRecord> rows = new ArrayList<>();
        collect(records, rows);
        return rows;
    }

    /** An opener that wakes the reader up while it is opening the call. */
    private static final class WakeOnOpenOpener implements RowStreamOpener {

        private static final long serialVersionUID = 1L;

        private transient BigQuerySplitReader reader;
        private final AtomicBoolean cancelled = new AtomicBoolean();

        void wakeOn(BigQuerySplitReader reader) {
            this.reader = reader;
        }

        boolean cancelled() {
            return cancelled.get();
        }

        @Override
        public RowStream open(String streamName, long offset) {
            reader.wakeUp();
            return new RowStream() {
                @Override
                public com.google.cloud.bigquery.storage.v1.ReadRowsResponse next() {
                    if (cancelled.get()) {
                        throw new IllegalStateException("cancelled");
                    }
                    return TestRows.block(TestRows.rows(2));
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void close() {}
    }

    /** An opener whose second block arrives only after the reader has been woken. */
    private static final class WakingOpener implements RowStreamOpener {

        private static final long serialVersionUID = 1L;

        private transient BigQuerySplitReader reader;

        void wakeOn(BigQuerySplitReader reader) {
            this.reader = reader;
        }

        @Override
        public RowStream open(String streamName, long offset) {
            return new RowStream() {
                private int served;

                @Override
                public com.google.cloud.bigquery.storage.v1.ReadRowsResponse next() {
                    if (served++ == 0) {
                        return TestRows.block(TestRows.rows(2));
                    }
                    // What a cancelled gax stream does to a thread blocked in the iterator.
                    reader.wakeUp();
                    throw new IllegalStateException("cancelled");
                }

                @Override
                public void cancel() {}

                @Override
                public void close() {}
            };
        }

        @Override
        public void close() {}
    }

    private static SplitsAddition<BigQueryReadStreamSplit> addition(BigQueryReadStreamSplit split) {
        return new SplitsAddition<>(Collections.singletonList(split));
    }

    private BigQuerySplitReader reader(ScriptedRowStreamOpener opener, int maxRecordsPerFetch) {
        return new BigQuerySplitReader(opener, maxRecordsPerFetch, null, metrics.metrics());
    }

    private static ScriptedRowStreamOpener opener(int rowCount, int blockSize) {
        return ScriptedRowStreamOpener.singleStream(testId(), STREAM, rowCount, blockSize);
    }

    private static String testId() {
        return BigQuerySplitReaderTest.class.getName();
    }

    private static List<GenericRecord> drain(BigQuerySplitReader reader) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        drain(reader, rows, new ArrayList<>());
        return rows;
    }

    private static void drain(
            BigQuerySplitReader reader, List<GenericRecord> rows, List<Integer> batchSizes)
            throws IOException {
        for (int fetches = 0; fetches < 100; fetches++) {
            RecordsWithSplitIds<GenericRecord> records = reader.fetch();
            int size = collect(records, rows);
            if (size > 0) {
                batchSizes.add(size);
            }
            if (!records.finishedSplits().isEmpty()) {
                return;
            }
        }
        throw new AssertionError("The split never finished.");
    }

    private static int count(RecordsWithSplitIds<GenericRecord> records) {
        return collect(records, new ArrayList<>());
    }

    private static int collect(
            RecordsWithSplitIds<GenericRecord> records, List<GenericRecord> into) {
        int size = 0;
        while (records.nextSplit() != null) {
            GenericRecord row;
            while ((row = records.nextRecordFromSplit()) != null) {
                into.add(row);
                size++;
            }
        }
        return size;
    }

    private static List<String> names(List<GenericRecord> rows) {
        List<String> names = new ArrayList<>(rows.size());
        rows.forEach(row -> names.add(String.valueOf(row.get("name"))));
        return names;
    }
}
