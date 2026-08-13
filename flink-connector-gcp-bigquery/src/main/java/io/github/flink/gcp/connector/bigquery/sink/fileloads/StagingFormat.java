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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The file format {@link io.github.flink.gcp.connector.bigquery.sink.WriteMethod#FILE_LOADS
 * FILE_LOADS} stages rows in before loading them, set by {@code
 * FileLoadsOptions.Builder#stagingFormat}.
 *
 * <p><b>{@link #AVRO} is the default and the recommended value.</b> {@link #PARQUET} stages fewer
 * bytes and loads a large batch faster, but it is not a straight improvement: it needs dependencies
 * this connector does not ship, it cannot carry a {@code JSON} column at all, and BigQuery loads it
 * several times more slowly below 256 MiB of total input per load job — the regime a streaming
 * checkpoint normally sits in. The docs page sets out all three.
 *
 * <p>The format travels in each committable, so a file is always loaded as the format it was
 * actually written in, and load jobs are grouped on it.
 */
@PublicEvolving
public enum StagingFormat {

    /** Avro container files. The default, and the only format with no unshipped dependencies. */
    AVRO("avro", ".avro"),

    /**
     * Parquet files. Opt-in, and worth it mainly for a batch job whose per-destination volume
     * clears 256 MiB per load job.
     *
     * <p>A destination whose schema names a {@code JSON} column stages {@link #AVRO} whatever this
     * is set to: a {@code PARQUET} load is refused at job-configuration level when the provided
     * schema names one, so it is a correctness override rather than a preference.
     */
    PARQUET("parquet", ".parquet");

    private final String value;
    private final String extension;

    StagingFormat(String value, String extension) {
        this.value = value;
        this.extension = extension;
    }

    /** Returns the staging object name suffix for this format, including the dot. */
    public String getExtension() {
        return extension;
    }

    /**
     * Returns the lower-case spelling this constant takes in a {@code
     * sink.file-loads.staging-format} DDL option, for example {@code parquet}.
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
