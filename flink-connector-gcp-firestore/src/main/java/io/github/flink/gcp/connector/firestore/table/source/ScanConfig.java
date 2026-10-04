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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.firestore.source.FirestoreSource;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceBuilder;
import io.github.flink.gcp.connector.firestore.table.FirestoreConnectorOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * The {@code scan.*} options of a {@code firestore} table, read and checked when the statement is
 * planned and applied to the source's builder later.
 *
 * <p>Each value is applied once to a throwaway builder here, so a value the builder refuses fails
 * planning under its option key (ADR-0133). The partition count is refused without {@code
 * scan.collection-group} here too: the builder's own refusal names its setter, which no {@code
 * WITH} clause contains.
 */
@Internal
public final class ScanConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean collectionGroup;
    @Nullable private final Integer maxPartitions;
    @Nullable private final Instant readTime;
    @Nullable private final Integer maxRowsPerFetch;

    private ScanConfig(
            boolean collectionGroup,
            @Nullable Integer maxPartitions,
            @Nullable Instant readTime,
            @Nullable Integer maxRowsPerFetch) {
        this.collectionGroup = collectionGroup;
        this.maxPartitions = maxPartitions;
        this.readTime = readTime;
        this.maxRowsPerFetch = maxRowsPerFetch;
    }

    /**
     * Reads and checks the scan options.
     *
     * @param config the table options
     * @return the scan options
     * @throws ValidationException if a value or a combination is refused
     */
    public static ScanConfig from(ReadableConfig config) {
        boolean collectionGroup = config.get(FirestoreConnectorOptions.SCAN_COLLECTION_GROUP);
        Integer maxPartitions =
                config.getOptional(FirestoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS)
                        .orElse(null);
        if (maxPartitions != null && !collectionGroup) {
            throw new ValidationException(
                    FirestoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS.key()
                            + " partitions a collection-group scan; set "
                            + FirestoreConnectorOptions.SCAN_COLLECTION_GROUP.key()
                            + " = 'true', or remove it. A single collection is read as one"
                            + " split.");
        }
        Instant readTime =
                config.getOptional(FirestoreConnectorOptions.SCAN_READ_TIME)
                        .map(ScanConfig::parseReadTime)
                        .orElse(null);
        ScanConfig options =
                new ScanConfig(
                        collectionGroup,
                        maxPartitions,
                        readTime,
                        config.getOptional(FirestoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH)
                                .orElse(null));
        options.applyTo(FirestoreSource.builder());
        return options;
    }

    private static Instant parseReadTime(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new ValidationException(
                    "Option '"
                            + FirestoreConnectorOptions.SCAN_READ_TIME.key()
                            + "' is invalid: '"
                            + value
                            + "' is not an ISO-8601 instant such as '2026-10-04T00:00:00Z'.",
                    e);
        }
    }

    /** Applies the options to a source builder, renaming a refused value to its option key. */
    void applyTo(FirestoreSourceBuilder<?> builder) {
        OptionSetters.accept(
                FirestoreConnectorOptions.SCAN_PARTITION_MAX_PARTITIONS.key(),
                maxPartitions,
                builder::partitionCount);
        OptionSetters.accept(
                FirestoreConnectorOptions.SCAN_READ_TIME.key(), readTime, builder::readTime);
        OptionSetters.accept(
                FirestoreConnectorOptions.SCAN_MAX_ROWS_PER_FETCH.key(),
                maxRowsPerFetch,
                builder::pageSize);
    }

    /** Returns whether the scan reads a collection group. */
    public boolean isCollectionGroup() {
        return collectionGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScanConfig that = (ScanConfig) o;
        return collectionGroup == that.collectionGroup
                && Objects.equals(maxPartitions, that.maxPartitions)
                && Objects.equals(readTime, that.readTime)
                && Objects.equals(maxRowsPerFetch, that.maxRowsPerFetch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectionGroup, maxPartitions, readTime, maxRowsPerFetch);
    }
}
