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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.RowAdapter.RowBuilder;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceBuilder;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;

/** Child-process entry point for {@link BigtableSplitReaderMemoryBoundaryTest}. */
final class BigtableSplitReaderMemoryProbe {

    private static final int ROWS = 384;
    private static final int PAYLOAD_BYTES = 256 * 1024;
    private static final int HELD_BATCHES = 4;

    private BigtableSplitReaderMemoryProbe() {}

    public static void main(String[] args) throws Exception {
        BigtableSplitReader reader =
                new BigtableSplitReader(
                        TestSources.TABLE,
                        new GeneratingOpener(),
                        null,
                        BigtableSourceBuilder.DEFAULT_MAX_ROWS_PER_FETCH,
                        BigtableSourceBuilder.DEFAULT_MAX_BYTES_PER_FETCH,
                        new TestReaderMetrics().metrics());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new RowRangeSplit("probe", ByteStringRange.unbounded()))));

        int rows = 0;
        int maxBatchRows = 0;
        Deque<RecordsWithSplitIds<com.google.cloud.bigtable.data.v2.models.Row>> held =
                new ArrayDeque<>();
        while (true) {
            RecordsWithSplitIds<com.google.cloud.bigtable.data.v2.models.Row> batch =
                    reader.fetch();
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

        if (rows != ROWS || maxBatchRows != 31 || held.size() != HELD_BATCHES) {
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

    private static int count(
            RecordsWithSplitIds<com.google.cloud.bigtable.data.v2.models.Row> batch) {
        int count = 0;
        while (batch.nextSplit() != null) {
            while (batch.nextRecordFromSplit() != null) {
                count++;
            }
        }
        return count;
    }

    private static final class GeneratingOpener implements RowStreamOpener {

        private static final long serialVersionUID = 1L;

        @Override
        public RowStream open(
                TableDestination table, ByteStringRange range, @Nullable Filters.Filter filter) {
            return new RowStream() {
                private int position;

                @Override
                @Nullable
                public MeasuredRow next() {
                    if (position >= ROWS) {
                        return null;
                    }
                    int row = position++;
                    byte[] payload = new byte[PAYLOAD_BYTES];
                    payload[0] = (byte) row;
                    ByteString value = ByteString.copyFrom(payload);
                    RowBuilder<MeasuredRow> builder = new MeasuringRowAdapter().createRowBuilder();
                    builder.startRow(ByteString.copyFromUtf8(String.format("row-%04d", row)));
                    builder.startCell(
                            "cf",
                            ByteString.copyFromUtf8("q"),
                            row,
                            Collections.emptyList(),
                            value.size());
                    builder.cellValue(value);
                    builder.finishCell();
                    return builder.finishRow();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void close() {}
    }
}
