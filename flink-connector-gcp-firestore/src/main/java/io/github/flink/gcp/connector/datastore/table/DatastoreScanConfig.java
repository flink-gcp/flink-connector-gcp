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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * The {@code scan.*} options of a {@code datastore} table, read and checked when the statement is
 * planned and applied to the source's builder later.
 *
 * <p>Each value is applied once to a throwaway builder here, so a value the builder refuses fails
 * planning under its option key (ADR-0133).
 */
@Internal
public final class DatastoreScanConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Integer maxPartitions;
    @Nullable private final Instant readTime;
    @Nullable private final Integer maxRowsPerFetch;

    private DatastoreScanConfig(
            @Nullable Integer maxPartitions,
            @Nullable Instant readTime,
            @Nullable Integer maxRowsPerFetch) {
        this.maxPartitions = maxPartitions;
        this.readTime = readTime;
        this.maxRowsPerFetch = maxRowsPerFetch;
    }

    /**
     * Reads and checks the scan options.
     *
     * @param config the table options
     * @return the scan options
     * @throws ValidationException if a value is refused
     */
    public static DatastoreScanConfig from(ReadableConfig config) {
        DatastoreScanConfig options =
                new DatastoreScanConfig(
                        config.getOptional(DatastoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS)
                                .orElse(null),
                        config.getOptional(DatastoreConnectorOptions.SCAN_READ_TIME)
                                .map(DatastoreScanConfig::parseReadTime)
                                .orElse(null),
                        config.getOptional(DatastoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH)
                                .orElse(null));
        options.applyTo(DatastoreSource.builder());
        return options;
    }

    private static Instant parseReadTime(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new ValidationException(
                    "Option '"
                            + DatastoreConnectorOptions.SCAN_READ_TIME.key()
                            + "' is invalid: '"
                            + value
                            + "' is not an ISO-8601 instant such as '2026-10-04T00:00:00Z'.",
                    e);
        }
    }

    /** Applies the options to a source builder, renaming a refused value to its option key. */
    public void applyTo(DatastoreSourceBuilder<?> builder) {
        OptionSetters.accept(
                DatastoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS.key(),
                maxPartitions,
                builder::splitCount);
        OptionSetters.accept(
                DatastoreConnectorOptions.SCAN_READ_TIME.key(), readTime, builder::readTime);
        OptionSetters.accept(
                DatastoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH.key(),
                maxRowsPerFetch,
                builder::pageSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatastoreScanConfig that = (DatastoreScanConfig) o;
        return Objects.equals(maxPartitions, that.maxPartitions)
                && Objects.equals(readTime, that.readTime)
                && Objects.equals(maxRowsPerFetch, that.maxRowsPerFetch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPartitions, readTime, maxRowsPerFetch);
    }
}
