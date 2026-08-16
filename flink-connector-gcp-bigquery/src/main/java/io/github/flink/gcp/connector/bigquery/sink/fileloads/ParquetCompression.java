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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.PublicEvolving;

/**
 * How {@link StagingFormat#PARQUET} staging files are compressed. Rejected when the staging format
 * is {@link StagingFormat#AVRO}, whose codec is not configurable.
 *
 * <p>The choice exists for one reason, and it is a dependency reason rather than a tuning one: only
 * {@link #NONE} can be written without Hadoop classes on the runtime classpath. Everything else in
 * {@code parquet-hadoop} goes through {@code CodecFactory.getCodec}, which is Hadoop's {@code
 * CompressionCodec} SPI — measured, and true of gzip and snappy as well as zstd.
 */
@PublicEvolving
public enum ParquetCompression {

    /** Zstandard. The default, and what the size comparison against Avro is measured with. */
    ZSTD("zstd"),

    /**
     * No compression, which is the only Parquet configuration that needs no Hadoop runtime.
     *
     * <p><b>It costs more than the Avro it would replace.</b> Measured 2026-08-08 on the same rows:
     * uncompressed Parquet staged 1.21x the bytes of Avro/zstd, against 0.78x for Parquet/zstd. So
     * this is an escape hatch for a deployment that cannot place a Hadoop runtime, not a
     * recommendation — every staged byte is uploaded to Cloud Storage and read by the load job, and
     * inflating the input also moves a load relative to the 256 MiB threshold the docs page
     * describes.
     */
    NONE("none");

    private final String value;

    ParquetCompression(String value) {
        this.value = value;
    }

    /**
     * Returns the lower-case spelling this constant takes in a {@code
     * sink.file-loads.parquet-compression} DDL option, for example {@code zstd}.
     *
     * <p>Flink resolves an enum-valued {@code ConfigOption} by matching this string
     * case-insensitively, so the DDL vocabulary is defined here rather than by a table-local copy.
     * Use {@link #name()} where a message means the Java constant.
     */
    @Override
    public String toString() {
        return value;
    }
}
