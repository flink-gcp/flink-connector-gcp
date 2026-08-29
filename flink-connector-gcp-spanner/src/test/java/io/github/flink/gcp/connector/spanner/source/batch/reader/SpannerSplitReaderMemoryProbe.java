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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplit;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;

/** Child-process entry point for {@link SpannerSplitReaderMemoryBoundaryTest}. */
final class SpannerSplitReaderMemoryProbe {

    private static final int ROWS = 768;
    private static final int PAYLOAD_BYTES = 256 * 1024;
    private static final int HELD_BATCHES = 4;

    private SpannerSplitReaderMemoryProbe() {}

    public static void main(String[] args) throws Exception {
        DatabaseDestination database = DatabaseDestination.of("p", "i", "d");
        SpannerSplitReader reader =
                new SpannerSplitReader(
                        database,
                        new GeneratingOpener(),
                        SpannerSourceBuilder.DEFAULT_MAX_ROWS_PER_FETCH,
                        SpannerSourceBuilder.DEFAULT_MAX_BYTES_PER_FETCH,
                        new TestReaderMetrics().metrics());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new BatchReadSplit(
                                        "probe",
                                        TestPartitions.batchTransactionId(),
                                        TestPartitions.queryPartition(
                                                "probe", "SELECT id FROM rows")))));

        int rows = 0;
        int maxBatchRows = 0;
        Deque<RecordsWithSplitIds<Struct>> held = new ArrayDeque<>();
        while (true) {
            RecordsWithSplitIds<Struct> batch = reader.fetch();
            int batchRows = count(batch);
            rows += batchRows;
            maxBatchRows = Math.max(maxBatchRows, batchRows);
            if (batchRows > 0) {
                held.addLast(batch);
                if (held.size() > HELD_BATCHES) {
                    held.removeFirst();
                }
            }
            if (!batch.finishedSplits().isEmpty()) {
                break;
            }
        }
        reader.close();

        if (rows != ROWS || maxBatchRows != 47 || held.size() != HELD_BATCHES) {
            throw new AssertionError(
                    "rows=" + rows + ", maxBatchRows=" + maxBatchRows + ", held=" + held.size());
        }
        System.out.println(
                "PASS rows="
                        + rows
                        + " maxBatchRows="
                        + maxBatchRows
                        + " heldBatches="
                        + held.size());
    }

    private static int count(RecordsWithSplitIds<Struct> batch) {
        int count = 0;
        while (batch.nextSplit() != null) {
            while (batch.nextRecordFromSplit() != null) {
                count++;
            }
        }
        return count;
    }

    private static final class GeneratingOpener implements StructStreamOpener {

        private static final long serialVersionUID = 1L;

        @Override
        public StructStream open(BatchTransactionId transactionId, Partition partition) {
            return new StructStream() {
                private int position;

                @Override
                @Nullable
                public Struct next() {
                    if (position >= ROWS) {
                        return null;
                    }
                    int rowNumber = position++;
                    char[] characters = new char[PAYLOAD_BYTES];
                    Arrays.fill(characters, (char) ('a' + rowNumber % 26));
                    Struct row =
                            Struct.newBuilder()
                                    .set("id")
                                    .to((long) rowNumber)
                                    .set("payload")
                                    .to(new String(characters))
                                    .build();
                    return row;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void useCredentials(@Nullable GoogleCredentials credentials) {}

        @Override
        public void close() {}
    }
}
