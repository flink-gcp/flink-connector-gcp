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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Decodes the rows of one response block, one row per call.
 *
 * <p>A response is not decoded into a list: a BigQuery response block holds up to about 128 MiB of
 * rows, and the split reader takes only as many as its per-fetch cap allows before handing a batch
 * to the task thread, so the cursor stays put between fetches and resumes mid-block. The emulator's
 * habit of answering a whole table in one response makes it the strictest test of that.
 *
 * <p>The datum reader and the binary decoder are created once per split and reused across blocks;
 * the {@link GenericRecord} is <em>not</em> reused, because it is handed to user code and then
 * travels downstream, where a reused instance would be overwritten before it is read.
 */
@Internal
final class AvroRowCursor {

    private final GenericDatumReader<GenericRecord> datumReader;

    @Nullable private BinaryDecoder decoder;
    private long remaining;

    /**
     * Creates the cursor.
     *
     * @param writerSchema the schema the rows were written with, from the read session
     * @param readerSchema the schema to decode into, or {@code null} to decode into the writer's
     */
    AvroRowCursor(Schema writerSchema, @Nullable Schema readerSchema) {
        this.datumReader =
                readerSchema == null
                        ? new GenericDatumReader<>(writerSchema)
                        : new GenericDatumReader<>(writerSchema, readerSchema);
    }

    /**
     * Binds the cursor to a new response block, discarding any unread rows of the previous one.
     *
     * @param response the block to decode
     */
    void reset(ReadRowsResponse response) {
        // newInput() reads the block's bytes in place; toByteArray() would copy up to 128 MiB.
        decoder =
                DecoderFactory.get()
                        .binaryDecoder(
                                response.getAvroRows().getSerializedBinaryRows().newInput(),
                                decoder);
        remaining = response.getRowCount();
    }

    /** Returns whether the current block still has rows. */
    boolean hasNext() {
        return remaining > 0;
    }

    /**
     * Drops the rows left in the current block.
     *
     * <p>Used when the stream behind the block is cancelled: what is left in the block is re-read
     * from the reopened stream, so decoding it would duplicate rows.
     */
    void discard() {
        remaining = 0;
    }

    /**
     * Decodes the next row of the current block.
     *
     * @return the row
     * @throws IOException if the row cannot be decoded
     * @throws IllegalStateException if the block has no rows left
     */
    GenericRecord next() throws IOException {
        if (remaining <= 0) {
            throw new IllegalStateException("No row left in the current response block.");
        }
        remaining--;
        return datumReader.read(null, decoder);
    }

    /** Returns the decoder in use, so a test can assert it is the same instance across blocks. */
    @VisibleForTesting
    @Nullable
    BinaryDecoder decoder() {
        return decoder;
    }
}
