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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceBuilder;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;

/** Child-process entry point for {@link BigQuerySplitReaderMemoryBoundaryTest}. */
final class BigQuerySplitReaderMemoryProbe {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/probe";
    private static final int ROWS = 384;
    private static final int PAYLOAD_BYTES = 256 * 1024;
    private static final int HELD_BATCHES = 4;

    private BigQuerySplitReaderMemoryProbe() {}

    public static void main(String[] args) throws Exception {
        ReadRowsResponse response = response();
        RowStreamOpener opener = new OneResponseOpener(response);
        BigQuerySplitReader reader =
                new BigQuerySplitReader(
                        opener,
                        BigQuerySourceBuilder.DEFAULT_MAX_RECORDS_PER_FETCH,
                        BigQuerySourceBuilder.DEFAULT_MAX_BYTES_PER_FETCH,
                        null,
                        new TestReaderMetrics().metrics());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new ReadStreamSplit(STREAM, 0, TestRows.SCHEMA_JSON, null))));

        int rows = 0;
        int maxBatchRows = 0;
        Deque<RecordsWithSplitIds<GenericRecord>> held = new ArrayDeque<>();
        while (true) {
            RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
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

    private static int count(RecordsWithSplitIds<GenericRecord> batch) {
        int count = 0;
        while (batch.nextSplit() != null) {
            while (batch.nextRecordFromSplit() != null) {
                count++;
            }
        }
        return count;
    }

    private static ReadRowsResponse response() throws Exception {
        ByteString.Output bytes = ByteString.newOutput();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(TestRows.SCHEMA);
        GenericRecord row = new GenericData.Record(TestRows.SCHEMA);
        String payload = "x".repeat(PAYLOAD_BYTES);
        row.put("name", payload);
        for (int i = 0; i < ROWS; i++) {
            row.put("id", (long) i);
            writer.write(row, encoder);
        }
        encoder.flush();
        ByteString serializedRows = bytes.toByteString();
        return ReadRowsResponse.newBuilder()
                .setAvroRows(
                        AvroRows.newBuilder()
                                .setSerializedBinaryRows(serializedRows)
                                .setRowCount(ROWS)
                                .build())
                .setRowCount(ROWS)
                .build();
    }

    private static final class OneResponseOpener implements RowStreamOpener {

        private static final long serialVersionUID = 1L;

        private final ReadRowsResponse response;

        private OneResponseOpener(ReadRowsResponse response) {
            this.response = response;
        }

        @Override
        public RowStream open(String streamName, long offset) {
            if (!STREAM.equals(streamName) || offset != 0) {
                throw new IllegalArgumentException(streamName + "@" + offset);
            }
            return new RowStream() {
                private boolean delivered;

                @Override
                public ReadRowsResponse next() {
                    if (delivered) {
                        return null;
                    }
                    delivered = true;
                    return response;
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
}
