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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a Parquet staging file rolls (#1704). Parquet's row groups land just under the threshold
 * for one incompressible column and at a fraction of it for many compressible columns, so the two
 * shapes below bound a roll rule from both sides: rolling on the byte count alone fails the first,
 * rolling after one row group fails the second.
 */
class ParquetStagedFileWriterTest {

    private static final long THRESHOLD = 256 * 1024;

    @Test
    void anIncompressibleRowGroupClosesTheFileAlone() throws IOException {
        Schema schema = SchemaBuilder.record("Row").fields().requiredBytes("payload").endRecord();
        List<Long> sizes =
                rolledFileSizes(
                        schema,
                        id -> {
                            byte[] payload = new byte[1024];
                            new SplittableRandom(id).nextBytes(payload);
                            GenericRecord row = new GenericData.Record(schema);
                            row.put("payload", ByteBuffer.wrap(payload));
                            return row;
                        });

        // One group, which can overshoot the threshold slightly; two would be about twice it.
        assertThat(sizes)
                .allSatisfy(size -> assertThat(size).isBetween(THRESHOLD / 2, THRESHOLD * 3 / 2));
    }

    @Test
    void smallRowGroupsAccumulateTowardsTheThreshold() throws IOException {
        int columns = 64;
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Row").fields();
        for (int c = 0; c < columns; c++) {
            fields = fields.requiredString("c" + c);
        }
        Schema schema = fields.endRecord();
        List<Long> sizes =
                rolledFileSizes(
                        schema,
                        id -> {
                            SplittableRandom random = new SplittableRandom(id);
                            GenericRecord row = new GenericData.Record(schema);
                            for (int c = 0; c < columns; c++) {
                                row.put("c" + c, "category-" + random.nextInt(100));
                            }
                            return row;
                        });

        assertThat(sizes)
                .allSatisfy(size -> assertThat(size).isBetween(THRESHOLD / 2, THRESHOLD * 3 / 2));
    }

    /** Writes three files the way the writer rolls them and returns their finished sizes. */
    private static List<Long> rolledFileSizes(Schema schema, LongFunction<GenericRecord> rows)
            throws IOException {
        List<Long> sizes = new ArrayList<>();
        long id = 0;
        while (sizes.size() < 3) {
            ParquetStagedFileWriter writer =
                    new ParquetStagedFileWriter(
                            "job",
                            TableDestination.of("project", "dataset", "table"),
                            "gs://bucket/object",
                            schema,
                            OutputStream.nullOutputStream(),
                            ParquetCompression.ZSTD,
                            THRESHOLD);
            while (!writer.isFull(THRESHOLD)) {
                writer.append(rows.apply(id++));
            }
            sizes.add(writer.finish().getByteCount());
        }
        return sizes;
    }
}
