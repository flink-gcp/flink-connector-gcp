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

import java.util.Locale;

/**
 * The file format {@link io.github.flink.gcp.connector.bigquery.sink.WriteMethod#FILE_LOADS
 * FILE_LOADS} stages rows in before loading them.
 *
 * <p>Only {@link #AVRO} is produced today; no option selects the other value yet (#284). The type
 * exists because the format has to travel in the committable and drive the load job before anything
 * can write the second format: a committable recovered from state must be loaded as the format its
 * file was <em>actually</em> written in, which configuration read at commit time cannot tell you.
 */
@PublicEvolving
public enum StagingFormat {

    /** Avro container files. The only format the sink writes today. */
    AVRO(".avro"),

    /**
     * Parquet files.
     *
     * <p>Nothing selects this yet — the load path handles it so that the writer can start producing
     * it without the committable layout changing again. A destination whose schema names a {@code
     * JSON} column will stage {@link #AVRO} whatever selects it, because a {@code PARQUET} load is
     * refused at job-configuration level when the provided schema names one.
     */
    PARQUET(".parquet");

    private final String extension;

    StagingFormat(String extension) {
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
        return name().toLowerCase(Locale.ROOT);
    }
}
