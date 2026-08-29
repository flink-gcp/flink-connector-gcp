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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.TestStructs;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerSplitReader}. */
class SpannerSplitReaderTest {

    private static final io.github.flink.gcp.connector.spanner.DatabaseDestination DATABASE =
            io.github.flink.gcp.connector.spanner.DatabaseDestination.of("p", "i", "db");

    private TestReaderMetrics metrics;

    @AfterEach
    void forgetRecordings() {
        ScriptedStructStreamOpener.reset();
    }

    @Test
    void aPartitionIsReadToItsEndAndReportedFinished() throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> batch = reader.fetch();

        assertThat(idsOf(batch, "p0")).containsExactly(1L, 2L, 3L);
        assertThat(batch.finishedSplits()).containsExactly("p0");
        assertThat(metrics.counter(SpannerMetricNames.ROWS_READ)).isEqualTo(3);
        assertThat(opener.openedTokens()).containsExactly("p0");
    }

    @Test
    void anEmptyPartitionIsFinishedWithoutRows() throws Exception {
        // The emulator plans one on every run, so this is not a corner case: a reader that treated
        // an empty partition as a failure would fail every emulator job.
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t");
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> batch = reader.fetch();

        assertThat(batch.finishedSplits()).containsExactly("p0");
        assertThat(idsOf(batch, "p0")).isEmpty();
    }

    @Test
    void aFetchStopsAtTheRowCapAndTheNextOneContinuesOnTheSameRead() throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3, 4);
        SpannerSplitReader reader = reader(opener, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();
        RecordsWithSplitIds<Struct> third = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L, 2L);
        assertThat(first.finishedSplits()).isEmpty();
        assertThat(idsOf(second, "p0")).containsExactly(3L, 4L);
        assertThat(third.finishedSplits()).containsExactly("p0");
        // One open, not three: hitting the cap is not a cancellation, so the read stays open and
        // no row is delivered twice.
        assertThat(opener.openedTokens()).containsExactly("p0");
        assertThat(metrics.counter(SpannerMetricNames.PARTITIONS_REREAD)).isZero();
    }

    @Test
    void aFetchStopsBeforeTheNextRowWouldCrossTheByteTarget() throws Exception {
        ScriptedStructStreamOpener opener =
                ScriptedStructStreamOpener.over(
                        "t",
                        Collections.singletonMap(
                                "p0", Arrays.asList(sized(1, 10), sized(2, 10), sized(3, 12))));
        SpannerSplitReader reader = reader(opener, 10, 21);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L, 2L);
        assertThat(first.finishedSplits()).isEmpty();
        assertThat(idsOf(second, "p0")).containsExactly(3L);
        assertThat(second.finishedSplits()).containsExactly("p0");
        assertThat(opener.openedTokens()).containsExactly("p0");
    }

    @Test
    void aRowThatExactlyReachesTheByteTargetStaysInTheBatch() throws Exception {
        ScriptedStructStreamOpener opener =
                ScriptedStructStreamOpener.over(
                        "t",
                        Collections.singletonMap("p0", Arrays.asList(sized(1, 10), sized(2, 11))));
        SpannerSplitReader reader = reader(opener, 10, 21);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L, 2L);
        assertThat(second.finishedSplits()).containsExactly("p0");
    }

    @Test
    void anOversizedRowIsHandedOverAloneSoTheSourceMakesProgress() throws Exception {
        ScriptedStructStreamOpener opener =
                ScriptedStructStreamOpener.over(
                        "t",
                        Collections.singletonMap("p0", Arrays.asList(sized(1, 20), sized(2, 9))));
        SpannerSplitReader reader = reader(opener, 10, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L);
        assertThat(idsOf(second, "p0")).containsExactly(2L);
        assertThat(second.finishedSplits()).containsExactly("p0");
    }

    @Test
    void theDefaultCanBatchAServiceMaximumCellWithOrdinaryFields() throws Exception {
        int mebibyte = 1024 * 1024;
        Struct maximumCell =
                Struct.newBuilder()
                        .set("id")
                        .to(1L)
                        .set("payload")
                        .to("x".repeat(10 * mebibyte))
                        .build();
        Struct ordinaryFields = sized(2, 2 * mebibyte - Long.BYTES);
        assertThat(
                        StructSizeEstimator.estimate(maximumCell)
                                + StructSizeEstimator.estimate(ordinaryFields))
                .isEqualTo(SpannerSourceBuilder.DEFAULT_MAX_BYTES_PER_FETCH);

        ScriptedStructStreamOpener opener =
                ScriptedStructStreamOpener.over(
                        "t",
                        Collections.singletonMap("p0", Arrays.asList(maximumCell, ordinaryFields)));
        SpannerSplitReader reader =
                reader(
                        opener,
                        SpannerSourceBuilder.DEFAULT_MAX_ROWS_PER_FETCH,
                        SpannerSourceBuilder.DEFAULT_MAX_BYTES_PER_FETCH);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L, 2L);
        assertThat(second.finishedSplits()).containsExactly("p0");
    }

    @Test
    void aWakeUpDiscardsALookAheadRowAndRereadsTheWholePartition() throws Exception {
        ScriptedStructStreamOpener opener =
                ScriptedStructStreamOpener.over(
                        "t",
                        Collections.singletonMap("p0", Arrays.asList(sized(1, 12), sized(2, 12))));
        SpannerSplitReader reader = reader(opener, 10, 15);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        reader.wakeUp();
        RecordsWithSplitIds<Struct> woken = reader.fetch();
        RecordsWithSplitIds<Struct> replayedFirst = reader.fetch();
        RecordsWithSplitIds<Struct> replayedSecond = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L);
        assertThat(woken.nextSplit()).isNull();
        assertThat(idsOf(replayedFirst, "p0")).containsExactly(1L);
        assertThat(idsOf(replayedSecond, "p0")).containsExactly(2L);
        assertThat(replayedSecond.finishedSplits()).containsExactly("p0");
        assertThat(opener.openedTokens()).containsExactly("p0", "p0");
        // The discarded look-ahead row was pulled from the SDK stream but never accepted into a
        // fetch batch, so it does not become an input-row metric event.
        assertThat(metrics.counter(SpannerMetricNames.ROWS_READ)).isEqualTo(3);
        assertThat(metrics.counter(SpannerMetricNames.PARTITIONS_REREAD)).isEqualTo(1);
    }

    @Test
    void severalPartitionsAreReadOneAfterTheOther() throws Exception {
        Map<String, List<Struct>> rows = new HashMap<>();
        rows.put("p0", TestStructs.rows(1));
        rows.put("p1", TestStructs.rows(2, 3));
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.over("t", rows);
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(
                new SplitsAddition<>(java.util.Arrays.asList(split("p0"), split("p1"))));

        RecordsWithSplitIds<Struct> first = reader.fetch();
        RecordsWithSplitIds<Struct> second = reader.fetch();

        assertThat(idsOf(first, "p0")).containsExactly(1L);
        assertThat(idsOf(second, "p1")).containsExactly(2L, 3L);
        assertThat(opener.openedTokens()).containsExactly("p0", "p1");
    }

    @Test
    void aFetchWithNothingAssignedHandsOverNothing() throws Exception {
        SpannerSplitReader reader = reader(ScriptedStructStreamOpener.single("t", 1), 10);

        assertThat(reader.fetch().nextSplit()).isNull();
    }

    @Test
    void aWakeUpBeforeAnythingIsOpenReturnsControlWithoutOpening() throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2);
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        reader.wakeUp();
        RecordsWithSplitIds<Struct> woken = reader.fetch();
        RecordsWithSplitIds<Struct> next = reader.fetch();

        // Nothing was opened, so nothing was cancelled and nothing is re-read.
        assertThat(woken.nextSplit()).isNull();
        assertThat(idsOf(next, "p0")).containsExactly(1L, 2L);
        assertThat(opener.openedTokens()).containsExactly("p0");
        assertThat(metrics.counter(SpannerMetricNames.PARTITIONS_REREAD)).isZero();
    }

    @Test
    void aWakeUpBetweenTwoFetchesOpensNothingUntilTheNextOne() throws Exception {
        // The window the reader-level flag cannot cover: a partition is active but has no read
        // open, so the wake-up lands on the partition instead. Opening anyway would block the
        // fetch on a read nobody has asked to interrupt, and the job being cancelled would wait
        // for a row that may be a long time coming.
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        SpannerSplitReader reader = reader(opener, 1);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));
        reader.fetch();

        reader.wakeUp();
        RecordsWithSplitIds<Struct> woken = reader.fetch();

        assertThat(idsOf(woken, "p0")).isEmpty();
        assertThat(opener.openedTokens()).containsExactly("p0");

        // And the fetch after it opens normally rather than staying stuck on the cleared flag.
        RecordsWithSplitIds<Struct> resumed = reader.fetch();

        assertThat(opener.openedTokens()).containsExactly("p0", "p0");
        assertThat(idsOf(resumed, "p0")).containsExactly(1L);
    }

    @Test
    @Timeout(30)
    void aWakeUpThatCancelsAQuietReadRereadsThePartition() throws Exception {
        assertRereadAfterWakeUp(ScriptedStructStreamOpener.CancelBehaviour.ENDS_QUIETLY);
    }

    @Test
    @Timeout(30)
    void aWakeUpThatCancelsAThrowingReadRereadsThePartition() throws Exception {
        // The two shapes a cancelled Spanner read takes, measured on 2026-08-10. The cancelled flag
        // is what decides that the partition did not finish — a reader that trusted the read's
        // behaviour would report this one finished and drop every row after the wake-up.
        assertRereadAfterWakeUp(ScriptedStructStreamOpener.CancelBehaviour.THROWS);
    }

    private void assertRereadAfterWakeUp(ScriptedStructStreamOpener.CancelBehaviour behaviour)
            throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        opener.cancelBehaviour(behaviour);
        opener.blockBefore(2);
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        AtomicReference<RecordsWithSplitIds<Struct>> blockedFetch = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread fetcher =
                new Thread(
                        () -> {
                            try {
                                blockedFetch.set(reader.fetch());
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            } finally {
                                done.countDown();
                            }
                        });
        fetcher.start();
        opener.awaitBlocked();
        reader.wakeUp();
        done.await();

        // The split is not reported finished: the rows past the wake-up have not been read.
        assertThat(blockedFetch.get().finishedSplits()).isEmpty();
        assertThat(idsOf(blockedFetch.get(), "p0")).containsExactly(1L, 2L);

        RecordsWithSplitIds<Struct> resumed = reader.fetch();

        // Re-read from the start, because Spanner has no position inside a partition — and counted,
        // because that is the only thing that explains the duplicate rows downstream.
        assertThat(opener.openedTokens()).containsExactly("p0", "p0");
        assertThat(metrics.counter(SpannerMetricNames.PARTITIONS_REREAD)).isEqualTo(1);
        assertThat(idsOf(resumed, "p0")).containsExactly(1L, 2L, 3L);
        assertThat(resumed.finishedSplits()).containsExactly("p0");
    }

    @Test
    void aRemovedSplitIsDroppedFromTheQueueAndFromTheOpenRead() throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        SpannerSplitReader reader = reader(opener, 1);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));
        reader.fetch();

        reader.handleSplitsChanges(new SplitsRemoval<>(Collections.singletonList(split("p0"))));

        assertThat(opener.streamCloses()).isEqualTo(1);
        assertThat(reader.fetch().nextSplit()).isNull();
    }

    @Test
    void aFailureToOpenNamesTheSplitAndTheDatabase() {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1);
        opener.failNextOpen(new IllegalStateException("PERMISSION_DENIED"));
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition split p0")
                .hasMessageContaining("databases/db")
                .hasRootCauseMessage("PERMISSION_DENIED");
    }

    @Test
    void aFailureWhileReadingNamesTheSplitAndTheDatabase() throws Exception {
        // Not a cancellation: the flag is clear, so the same throw that a wake-up would absorb has
        // to fail the job instead.
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        opener.failReadAfter(1, new IllegalStateException("UNAVAILABLE"));
        SpannerSplitReader reader = reader(opener, 10);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read partition split p0")
                .hasMessageContaining("databases/db");
    }

    @Test
    void closingTheReaderClosesTheOpenReadButNotTheSharedOpener() throws Exception {
        ScriptedStructStreamOpener opener = ScriptedStructStreamOpener.single("t", 1, 2, 3);
        SpannerSplitReader reader = reader(opener, 1);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split("p0"))));
        reader.fetch();

        reader.close();

        assertThat(opener.streamCloses()).isEqualTo(1);
        // The opener is shared with this subtask's other split readers and is the source reader's
        // to close; closing it here would release a client another fetcher is still reading
        // through.
        assertThat(opener.closes()).isZero();
    }

    @Test
    void aNonPositiveRowCapIsRefused() {
        assertThatThrownBy(() -> reader(ScriptedStructStreamOpener.single("t", 1), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRowsPerFetch must be positive");
    }

    @Test
    void aNonPositiveByteTargetIsRefused() {
        metrics = new TestReaderMetrics();

        assertThatThrownBy(
                        () ->
                                new SpannerSplitReader(
                                        DATABASE,
                                        ScriptedStructStreamOpener.single("t", 1),
                                        1,
                                        0,
                                        metrics.metrics()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBytesPerFetch must be positive");
    }

    private SpannerSplitReader reader(ScriptedStructStreamOpener opener, int maxRowsPerFetch) {
        return reader(opener, maxRowsPerFetch, Long.MAX_VALUE);
    }

    private SpannerSplitReader reader(
            ScriptedStructStreamOpener opener, int maxRowsPerFetch, long maxBytesPerFetch) {
        metrics = new TestReaderMetrics();
        return new SpannerSplitReader(
                DATABASE, opener, maxRowsPerFetch, maxBytesPerFetch, metrics.metrics());
    }

    private static BatchReadSplit split(String token) {
        return new BatchReadSplit(
                token,
                TestPartitions.batchTransactionId(),
                TestPartitions.queryPartition(token, "SELECT id FROM singers"));
    }

    private static Struct sized(long id, int bytes) {
        return Struct.newBuilder()
                .set("id")
                .to(id)
                .set("payload")
                .to("x".repeat(bytes - Long.BYTES))
                .build();
    }

    private static List<Long> idsOf(RecordsWithSplitIds<Struct> batch, String splitId) {
        List<Long> ids = new ArrayList<>();
        String next = batch.nextSplit();
        while (next != null) {
            if (next.equals(splitId)) {
                Struct row = batch.nextRecordFromSplit();
                while (row != null) {
                    ids.add(TestStructs.idOf(row));
                    row = batch.nextRecordFromSplit();
                }
            }
            next = batch.nextSplit();
        }
        return ids;
    }
}
