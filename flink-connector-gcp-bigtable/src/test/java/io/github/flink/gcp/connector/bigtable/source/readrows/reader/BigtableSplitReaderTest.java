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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.TestRows;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableSplitReader}. */
@Timeout(30)
class BigtableSplitReaderTest {

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    @AfterEach
    void forgetScriptedTables() {
        ScriptedRowStreamOpener.reset();
    }

    private BigtableSplitReader reader(
            ScriptedRowStreamOpener opener, int maxRowsPerFetch, @Nullable Filters.Filter filter) {
        return new BigtableSplitReader(
                TestSources.TABLE, opener, filter, maxRowsPerFetch, metrics.metrics());
    }

    private static RowRangeSplit split(String id, ByteStringRange range) {
        return new RowRangeSplit(id, range);
    }

    /** Drains one fetch into the keys it produced, in order. */
    private static List<String> keysOf(RecordsWithSplitIds<Row> records) {
        List<String> keys = new ArrayList<>();
        String splitId;
        while ((splitId = records.nextSplit()) != null) {
            Row row;
            while ((row = records.nextRecordFromSplit()) != null) {
                keys.add(TestRows.keyOf(row));
            }
        }
        return keys;
    }

    @Test
    void readsARangeInOrder() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("read", "a", "b", "c", "d");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        List<String> keys = keysOf(reader.fetch());

        assertThat(keys).containsExactly("a", "b", "c", "d");
        assertThat(metrics.counter(BigtableMetricNames.ROWS_READ)).isEqualTo(4);
        reader.close();
    }

    @Test
    void honoursTheFetchCapSoACheckpointCanLandMidRange() throws Exception {
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.over("cap", "a", "b", "c", "d", "e");
        BigtableSplitReader reader = reader(opener, 2, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        assertThat(keysOf(reader.fetch())).containsExactly("a", "b");
        assertThat(keysOf(reader.fetch())).containsExactly("c", "d");
        assertThat(keysOf(reader.fetch())).containsExactly("e");
        reader.close();
    }

    @Test
    void opensOnlyTheRangeItWasAssigned() throws Exception {
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.over("range", "a", "b", "c", "d", "e");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                split(
                                        "0",
                                        ByteStringRange.unbounded()
                                                .startClosed("b")
                                                .endOpen("d")))));

        assertThat(keysOf(reader.fetch())).containsExactly("b", "c");
        assertThat(opener.openedRanges()).containsExactly("[b, d)");
        reader.close();
    }

    @Test
    void resumesARestoredSplitPastTheKeyItStoppedAt() throws Exception {
        // The split a checkpoint produced: an exclusive start at the last emitted row.
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("resume", "a", "b", "c", "d");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                split("0", ByteStringRange.unbounded().startOpen("b")))));

        assertThat(keysOf(reader.fetch())).containsExactly("c", "d");
        assertThat(opener.openedRanges()).containsExactly("(b, *)");
        reader.close();
    }

    @Test
    void carriesTheFilterIntoEveryRead() throws Exception {
        Filters.Filter filter = Filters.FILTERS.family().exactMatch(TestRows.FAMILY);
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("filter", "a");
        BigtableSplitReader reader = reader(opener, 10, filter);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        reader.fetch();

        assertThat(opener.openedFilters()).containsExactly(filter);
        reader.close();
    }

    @Test
    void reportsASplitFinishedWhenItsStreamEnds() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("finish", "a");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        RecordsWithSplitIds<Row> records = reader.fetch();
        keysOf(records);

        assertThat(records.finishedSplits()).containsExactly("0");
        reader.close();
    }

    @Test
    void finishesAnEmptyRangeWithoutOpeningAStream() throws Exception {
        // The normal state of a split whose last row was emitted before the checkpoint: opening it
        // would ask the service to serve an inverted range, which real Bigtable refuses with
        // INVALID_ARGUMENT rather than answering empty (measured 2026-08-10, #481) — so the zero
        // open calls asserted below are load-bearing, not tidiness.
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("empty", "a", "z");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                split(
                                        "0",
                                        ByteStringRange.unbounded()
                                                .startOpen("z")
                                                .endClosed("z")))));

        RecordsWithSplitIds<Row> records = reader.fetch();

        assertThat(keysOf(records)).isEmpty();
        assertThat(records.finishedSplits()).containsExactly("0");
        assertThat(opener.openCalls()).isZero();
        reader.close();
    }

    @Test
    void handsOverWhatItReadWhenAWakeUpEndsTheStreamQuietly() throws Exception {
        // gax cancels a stream by making its iterator report the end, exactly as a clean end does.
        // Treating that as "the split finished" would lose every row after the wake-up.
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.over("wake-quiet", "a", "b", "c", "d");
        opener.blockAfter(2, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        RecordsWithSplitIds<Row> records = fetchWhileWakingUp(reader, opener);

        assertThat(keysOf(records)).containsExactly("a", "b");
        assertThat(records.finishedSplits()).isEmpty();
        reader.close();
    }

    @Test
    void handsOverWhatItReadWhenAWakeUpMakesTheStreamThrow() throws Exception {
        // The other half of the same gax behaviour: a consumer already blocked gets the
        // cancellation as an error rather than as an end of stream.
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.over("wake-throw", "a", "b", "c", "d");
        opener.blockAfter(2, ScriptedRowStreamOpener.CancelBehaviour.THROWS);
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        RecordsWithSplitIds<Row> records = fetchWhileWakingUp(reader, opener);

        assertThat(keysOf(records)).containsExactly("a", "b");
        assertThat(records.finishedSplits()).isEmpty();
        reader.close();
    }

    @Test
    void reopensPastTheRowsItAlreadyHandedOver() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("reopen", "a", "b", "c", "d");
        opener.blockAfter(2, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));
        fetchWhileWakingUp(reader, opener);

        opener.blockAfter(Integer.MAX_VALUE, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);
        List<String> rest = keysOf(reader.fetch());

        assertThat(rest).containsExactly("c", "d");
        assertThat(opener.openedRanges()).containsExactly("(*, *)", "(b, *)");
        reader.close();
    }

    @Test
    void opensNothingForAWakeUpThatLandedBetweenStreams() throws Exception {
        // The window the reopen refusal exists for: a wake-up arrives while a split is active but
        // no call is open. Opening one anyway would block on a call nobody asked to interrupt, and
        // a cancelling job would wait for a first row that may be a long time coming.
        ScriptedRowStreamOpener opener =
                ScriptedRowStreamOpener.over("wake-between", "a", "b", "c", "d");
        opener.blockAfter(2, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));
        fetchWhileWakingUp(reader, opener);
        opener.blockAfter(Integer.MAX_VALUE, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);

        reader.wakeUp();
        RecordsWithSplitIds<Row> afterWakeUp = reader.fetch();

        assertThat(keysOf(afterWakeUp)).isEmpty();
        assertThat(afterWakeUp.finishedSplits()).isEmpty();
        assertThat(opener.openCalls()).as("no call opened for a pending wake-up").isEqualTo(1);
        // And the next fetch carries on from where the rows already handed over left off.
        assertThat(keysOf(reader.fetch())).containsExactly("c", "d");
        reader.close();
    }

    @Test
    void opensNothingForAWakeUpThatLandedBeforeAnySplitWasActive() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("wake-early", "a", "b");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(split("0", ByteStringRange.unbounded()))));

        reader.wakeUp();

        assertThat(keysOf(reader.fetch())).isEmpty();
        assertThat(opener.openCalls()).isZero();
        assertThat(keysOf(reader.fetch())).containsExactly("a", "b");
        reader.close();
    }

    @Test
    void dropsASplitThatWasRemovedBeforeItWasRead() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("removed", "a", "b");
        BigtableSplitReader reader = reader(opener, 10, null);
        RowRangeSplit removed = split("0", ByteStringRange.unbounded());
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(removed)));

        reader.handleSplitsChanges(new SplitsRemoval<>(Collections.singletonList(removed)));

        assertThat(keysOf(reader.fetch())).isEmpty();
        assertThat(opener.openCalls()).isZero();
        reader.close();
    }

    @Test
    void closesTheStreamOfASplitRemovedWhileItWasBeingRead() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("removed-active", "a", "b");
        BigtableSplitReader reader = reader(opener, 1, null);
        RowRangeSplit active = split("0", ByteStringRange.unbounded());
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(active)));
        reader.fetch();

        reader.handleSplitsChanges(new SplitsRemoval<>(Collections.singletonList(active)));

        // The stream has to be cancelled, not merely dropped: a forgotten ReadRows call keeps its
        // server-side resources until the client is closed.
        assertThat(opener.streamCloseCalls()).isEqualTo(1);
        assertThat(keysOf(reader.fetch())).isEmpty();
        assertThat(opener.openCalls()).isEqualTo(1);
        reader.close();
    }

    @Test
    void namesTheTableAndTheRangeWhenAReadCannotBeOpened() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("fail", "a");
        opener.failNextOpenWith(new IllegalStateException("boom"));
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                split(
                                        "0",
                                        ByteStringRange.unbounded()
                                                .startClosed("a")
                                                .endOpen("m")))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.i.orders")
                .hasMessageContaining("[a, m)")
                .hasRootCauseMessage("boom");
        reader.close();
    }

    @Test
    void wrapsAReadFailureWithTheTableAndTheRange() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("fail-mid", "a", "b");
        opener.failReadAfter(1, new IllegalStateException("stream broke"));
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                split(
                                        "0",
                                        ByteStringRange.unbounded()
                                                .startClosed("a")
                                                .endOpen("m")))));

        // Nothing cancelled this stream, so the throw is a real failure and must say where the read
        // had got to — the range still outstanding, not the one the split was assigned, so a reader
        // of the log can see how far the split progressed before it broke.
        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.i.orders")
                .hasMessageContaining("(a, m)");
        reader.close();
    }

    @Test
    void readsQueuedSplitsOneAfterTheOther() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("queued", "a", "b", "c");
        BigtableSplitReader reader = reader(opener, 10, null);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        java.util.Arrays.asList(
                                split("0", ByteStringRange.unbounded().endOpen("b")),
                                split("1", ByteStringRange.unbounded().startClosed("b")))));

        assertThat(keysOf(reader.fetch())).containsExactly("a");
        assertThat(keysOf(reader.fetch())).containsExactly("b", "c");
        reader.close();
    }

    @Test
    void answersWithNothingWhenItHoldsNoSplit() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("idle", "a");
        BigtableSplitReader reader = reader(opener, 10, null);

        RecordsWithSplitIds<Row> records = reader.fetch();

        assertThat(keysOf(records)).isEmpty();
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(opener.openCalls()).isZero();
        reader.close();
    }

    @Test
    void leavesTheSharedOpenerForTheSourceReaderToClose() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("shared", "a");
        BigtableSplitReader reader = reader(opener, 10, null);

        reader.close();

        assertThat(opener.closeCalls()).isZero();
    }

    /** Runs a fetch that blocks, wakes the reader up, and returns what the fetch handed over. */
    private RecordsWithSplitIds<Row> fetchWhileWakingUp(
            BigtableSplitReader reader, ScriptedRowStreamOpener opener) throws Exception {
        List<RecordsWithSplitIds<Row>> result = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        Thread fetcher =
                new Thread(
                        () -> {
                            try {
                                result.add(reader.fetch());
                            } catch (Throwable t) {
                                failures.add(t);
                            }
                        });
        fetcher.start();
        opener.awaitBlocked();
        reader.wakeUp();
        fetcher.join();
        if (!failures.isEmpty()) {
            throw new AssertionError("The fetch failed", failures.get(0));
        }
        return result.get(0);
    }
}
