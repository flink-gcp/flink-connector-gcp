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

import org.apache.flink.annotation.Public;

/**
 * How {@link StagingFormat#AVRO} staging files are compressed. Rejected when the staging format is
 * {@link StagingFormat#PARQUET}, whose codec {@link ParquetCompression} sets.
 *
 * <p>The choice trades the writer's CPU against the bytes uploaded to Cloud Storage and read by the
 * load job. The zstandard level, set separately, moves along the same line; snappy and deflate are
 * not offered because, on the row ADR-0196 measured, zstandard at some level matched or beat each
 * of them on both CPU and bytes.
 */
@Public
public enum AvroCompression {
    /** Zstandard, at the configured level. The default. */
    ZSTD("zstd"),
    /**
     * No compression: the least CPU on the writer's task thread and the most bytes, for a job short
     * of CPU and not of upload bandwidth.
     */
    NONE("none");

    private final String value;

    AvroCompression(String value) {
        this.value = value;
    }

    /**
     * Returns the lower-case spelling this constant takes in a {@code
     * sink.file-loads.avro-compression} DDL option, for example {@code zstd}.
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
