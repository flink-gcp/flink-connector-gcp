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

import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Rows in the shape the Storage Read API delivers them: Avro-encoded blocks inside {@link
 * ReadRowsResponse}s.
 *
 * <p>Encoding real Avro here rather than handing fakes a list of records is what makes the reader
 * tests exercise the decoder, the block boundaries and the row counts the connector actually reads.
 */
public final class TestRows {

    /** A two-column table: {@code id} and {@code name}, both required. */
    public static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"name\",\"type\":\"string\"}]}";

    public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private TestRows() {}

    /** Returns {@code count} rows, numbered from zero: {@code (0, "row-0")}, and so on. */
    public static List<GenericRecord> rows(int count) {
        return rows(0, count);
    }

    /**
     * Returns {@code count} rows numbered from {@code from}, so two streams can hold distinct ones.
     */
    public static List<GenericRecord> rows(int from, int count) {
        List<GenericRecord> rows = new ArrayList<>(count);
        for (int i = from; i < from + count; i++) {
            GenericRecord row = new GenericData.Record(SCHEMA);
            row.put("id", (long) i);
            row.put("name", "row-" + i);
            rows.add(row);
        }
        return rows;
    }

    /** Encodes the given rows into one response block. */
    public static ReadRowsResponse block(List<GenericRecord> rows) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
        try {
            for (GenericRecord row : rows) {
                writer.write(row, encoder);
            }
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ReadRowsResponse.newBuilder()
                .setAvroRows(
                        AvroRows.newBuilder()
                                .setSerializedBinaryRows(ByteString.copyFrom(bytes.toByteArray()))
                                .setRowCount(rows.size())
                                .build())
                .setRowCount(rows.size())
                .build();
    }

    /** Encodes the given rows into blocks of at most {@code blockSize} rows each. */
    public static List<ReadRowsResponse> blocks(List<GenericRecord> rows, int blockSize) {
        List<ReadRowsResponse> blocks = new ArrayList<>();
        for (int from = 0; from < rows.size(); from += blockSize) {
            blocks.add(block(rows.subList(from, Math.min(from + blockSize, rows.size()))));
        }
        return blocks;
    }
}
