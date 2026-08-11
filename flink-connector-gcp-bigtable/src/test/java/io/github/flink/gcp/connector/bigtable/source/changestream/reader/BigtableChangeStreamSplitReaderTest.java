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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamSplitReaderTest {

    @Test
    void readsThroughCloseAndReportsTheSplitFinished() throws Exception {
        ScriptedOpener opener =
                new ScriptedOpener(
                        TestChangeStreamRecords.mutation(
                                Instant.parse("2026-08-11T01:00:00Z"),
                                Instant.parse("2026-08-11T00:59:00Z"),
                                "next"),
                        TestChangeStreamRecords.close("child"));
        BigtableChangeStreamSplitReader reader = reader(opener);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(Collections.emptyList()))));

        RecordsWithSplitIds<ChangeStreamRecord> records = reader.fetch();

        assertThat(drain(records)).hasSize(2);
        assertThat(records.finishedSplits()).containsExactly("change-stream-0");
        assertThat(opener.openedStreams.get(0).closeCalls).isEqualTo(1);
        reader.close();
    }

    @Test
    void opensARestoredSplitWithItsExactTokens() throws Exception {
        ChangeStreamContinuationToken left =
                ChangeStreamContinuationToken.create(ByteStringRange.create("a", "m"), "left");
        ChangeStreamContinuationToken right =
                ChangeStreamContinuationToken.create(ByteStringRange.create("m", "z"), "right");
        ScriptedOpener opener = new ScriptedOpener(TestChangeStreamRecords.close("done"));
        BigtableChangeStreamSplitReader reader = reader(opener);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(Arrays.asList(left, right)))));

        reader.fetch();

        assertThat(opener.opened.get(0).getContinuationTokens()).containsExactly(left, right);
        reader.close();
    }

    @Test
    void wakeUpClosesTheBlockedStreamAndReopensFromTheLatestToken() throws Exception {
        Instant commit = Instant.parse("2026-08-11T01:00:00Z");
        Instant watermark = Instant.parse("2026-08-11T00:59:00Z");
        ScriptedOpener opener = new ScriptedOpener();
        opener.addScript(true, TestChangeStreamRecords.mutation(commit, watermark, "latest-token"));
        opener.addScript(false, TestChangeStreamRecords.close("child"));
        BigtableChangeStreamSplitReader reader = reader(opener);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(Collections.emptyList()))));
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<RecordsWithSplitIds<ChangeStreamRecord>> interruptedFetch =
                    executor.submit(reader::fetch);
            assertThat(opener.blockingNextEntered.await(5, TimeUnit.SECONDS)).isTrue();

            reader.wakeUp();

            assertThat(drain(interruptedFetch.get(5, TimeUnit.SECONDS))).hasSize(1);
            assertThat(opener.openedStreams.get(0).cancelCalls).isEqualTo(1);
            assertThat(opener.openedStreams.get(0).closeCalls).isEqualTo(1);
            assertThat(drain(reader.fetch())).singleElement().isInstanceOf(CloseStream.class);
            assertThat(opener.opened.get(1).getContinuationTokens())
                    .singleElement()
                    .satisfies(token -> assertThat(token.getToken()).isEqualTo("latest-token"));
        } finally {
            reader.close();
            executor.shutdownNow();
        }
    }

    private static BigtableChangeStreamSplitReader reader(ScriptedOpener opener) {
        return new BigtableChangeStreamSplitReader(
                TableDestination.of("p", "i", "t"), opener, null);
    }

    private static ChangeStreamPartitionSplit split(List<ChangeStreamContinuationToken> tokens) {
        return new ChangeStreamPartitionSplit(
                "change-stream-0",
                ByteStringRange.create("a", "z"),
                tokens,
                Instant.parse("2026-08-11T00:00:00Z"));
    }

    private static List<ChangeStreamRecord> drain(RecordsWithSplitIds<ChangeStreamRecord> records) {
        List<ChangeStreamRecord> drained = new ArrayList<>();
        while (records.nextSplit() != null) {
            ChangeStreamRecord record;
            while ((record = records.nextRecordFromSplit()) != null) {
                drained.add(record);
            }
        }
        return drained;
    }

    private static final class ScriptedOpener implements ChangeStreamOpener {
        private static final long serialVersionUID = 1L;
        private final Deque<List<ChangeStreamRecord>> scripts = new ArrayDeque<>();
        private final Deque<Boolean> blockWhenEmpty = new ArrayDeque<>();
        private final List<ChangeStreamPartitionSplit> opened = new ArrayList<>();
        private final List<ScriptedStream> openedStreams = new ArrayList<>();
        private final CountDownLatch blockingNextEntered = new CountDownLatch(1);

        private ScriptedOpener(ChangeStreamRecord... records) {
            if (records.length > 0) {
                addScript(false, records);
            }
        }

        private void addScript(boolean blocksWhenEmpty, ChangeStreamRecord... records) {
            scripts.add(Arrays.asList(records));
            blockWhenEmpty.add(blocksWhenEmpty);
        }

        @Override
        public ChangeStream open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                @Nullable Instant endTime) {
            opened.add(split);
            ScriptedStream stream =
                    new ScriptedStream(
                            scripts.remove(), blockWhenEmpty.remove(), blockingNextEntered);
            openedStreams.add(stream);
            return stream;
        }

        @Override
        public void close() {}
    }

    private static final class ScriptedStream implements ChangeStream {
        private final Deque<ChangeStreamRecord> records;
        private final boolean blockWhenEmpty;
        private final CountDownLatch blockingNextEntered;
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private int cancelCalls;
        private int closeCalls;

        private ScriptedStream(
                List<ChangeStreamRecord> records,
                boolean blockWhenEmpty,
                CountDownLatch blockingNextEntered) {
            this.records = new ArrayDeque<>(records);
            this.blockWhenEmpty = blockWhenEmpty;
            this.blockingNextEntered = blockingNextEntered;
        }

        @Override
        @Nullable
        public ChangeStreamRecord next() {
            ChangeStreamRecord record = records.poll();
            if (record != null || !blockWhenEmpty) {
                return record;
            }
            blockingNextEntered.countDown();
            try {
                cancelled.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("scripted stream was cancelled");
        }

        @Override
        public void cancel() {
            cancelCalls++;
            cancelled.countDown();
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
