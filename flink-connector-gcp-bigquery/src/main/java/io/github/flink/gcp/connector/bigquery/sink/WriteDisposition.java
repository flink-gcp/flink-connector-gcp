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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * How data lands in a destination table that already contains data. Mirrors the BigQuery load-job
 * write dispositions; only {@link io.github.flink.gcp.connector.bigquery.sink.WriteMethod
 * WriteMethod.FILE_LOADS} consults it (the Storage Write API methods always append).
 */
@PublicEvolving
public enum WriteDisposition {

    /** Appends loaded rows to the existing table data. The default. */
    WRITE_APPEND("write-append"),

    /** Replaces the existing table data (and schema) with the loaded rows. */
    WRITE_TRUNCATE("write-truncate"),

    /** Replaces the existing table data while preserving the table schema and constraints. */
    WRITE_TRUNCATE_DATA("write-truncate-data"),

    /** Fails the load when the destination table is not empty. */
    WRITE_EMPTY("write-empty");

    private final String value;

    WriteDisposition(String value) {
        this.value = value;
    }

    /**
     * Returns the hyphenated lower-case spelling this constant takes in a {@code
     * sink.file-loads.write-disposition} DDL option, for example {@code write-truncate}.
     *
     * <p>Flink resolves an enum-valued {@code ConfigOption} by matching this string
     * case-insensitively and normalizing nothing else, so the DDL vocabulary is defined here rather
     * than by a table-local copy of the enum. Use {@link #name()} where a message means the Java
     * constant.
     */
    @Override
    public String toString() {
        return value;
    }
}
