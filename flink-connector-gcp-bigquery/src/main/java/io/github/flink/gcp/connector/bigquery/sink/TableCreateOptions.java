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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Options applied when the sink creates a destination table under {@link
 * CreateDisposition#CREATE_IF_NEEDED}: time partitioning and clustering.
 *
 * <p>The table <em>schema</em> is not part of these options — it always comes from {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer#getTableSchema}.
 * Partitioning and clustering only affect table creation. CDC properties are configured through
 * {@link CdcTableOptions} because they also describe existing-table verification and
 * reconciliation.
 *
 * <p>Instances are immutable and serializable. Use {@link #defaults()} for plain, unpartitioned
 * tables.
 */
@PublicEvolving
public final class TableCreateOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** BigQuery has a hard limit of four clustering columns per table. */
    private static final int MAX_CLUSTERED_FIELDS = 4;

    /** Granularity of time-based partitioning. */
    public enum TimePartitioningType {
        /** One partition per hour. */
        HOUR("hour"),
        /** One partition per day. */
        DAY("day"),
        /** One partition per month. */
        MONTH("month"),
        /** One partition per year. */
        YEAR("year");

        private final String value;

        TimePartitioningType(String value) {
            this.value = value;
        }

        /**
         * Returns the lower-case spelling this constant takes in a {@code
         * sink.table-create.time-partitioning.type} DDL option, for example {@code day}.
         *
         * <p>Flink resolves an enum-valued {@code ConfigOption} by matching this string
         * case-insensitively and normalizing nothing else. The hyphenation is the rule the
         * connector's other DDL-facing enums follow rather than a fact about these four constants,
         * none of which carries an underscore. Use {@link #name()} where a message means the Java
         * constant — {@code BigQueryTableAdmin} bridges to the client library's own {@code
         * TimePartitioning.Type} by name, so the constant names are load-bearing.
         */
        @Override
        public String toString() {
            return value;
        }
    }

    private static final TableCreateOptions DEFAULTS = builder().build();

    private final TimePartitioningType timePartitioningType;
    private final String timePartitioningField;
    private final Long timePartitioningExpirationMs;
    private final List<String> clusteredFields;

    private TableCreateOptions(Builder builder) {
        this.timePartitioningType = builder.timePartitioningType;
        this.timePartitioningField = builder.timePartitioningField;
        this.timePartitioningExpirationMs = builder.timePartitioningExpirationMs;
        this.clusteredFields =
                Collections.unmodifiableList(new ArrayList<>(builder.clusteredFields));
    }

    /** Returns options creating a plain table: no partitioning, no clustering. */
    public static TableCreateOptions defaults() {
        return DEFAULTS;
    }

    /**
     * Creates a new {@link Builder}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the time-partitioning granularity, or {@code null} when unpartitioned. */
    public TimePartitioningType getTimePartitioningType() {
        return timePartitioningType;
    }

    /**
     * Returns the column the table is partitioned on, or {@code null} for ingestion-time
     * partitioning (and for unpartitioned tables).
     */
    public String getTimePartitioningField() {
        return timePartitioningField;
    }

    /**
     * Returns the partition expiration in milliseconds, or {@code null} when partitions never
     * expire.
     */
    public Long getTimePartitioningExpirationMs() {
        return timePartitioningExpirationMs;
    }

    /** Returns the clustering columns, in precedence order; empty when unclustered. */
    public List<String> getClusteredFields() {
        return clusteredFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableCreateOptions that = (TableCreateOptions) o;
        return timePartitioningType == that.timePartitioningType
                && Objects.equals(timePartitioningField, that.timePartitioningField)
                && Objects.equals(timePartitioningExpirationMs, that.timePartitioningExpirationMs)
                && clusteredFields.equals(that.clusteredFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                timePartitioningType,
                timePartitioningField,
                timePartitioningExpirationMs,
                clusteredFields);
    }

    @Override
    public String toString() {
        return "TableCreateOptions{timePartitioningType="
                + timePartitioningType
                + ", timePartitioningField="
                + timePartitioningField
                + ", timePartitioningExpirationMs="
                + timePartitioningExpirationMs
                + ", clusteredFields="
                + clusteredFields
                + "}";
    }

    /** Builder for {@link TableCreateOptions}. */
    @PublicEvolving
    public static final class Builder {

        private TimePartitioningType timePartitioningType;
        private String timePartitioningField;
        private Long timePartitioningExpirationMs;
        private List<String> clusteredFields = Collections.emptyList();

        private Builder() {}

        /**
         * Partitions the table on ingestion time with the given granularity.
         *
         * @param type the partitioning granularity
         * @return this builder
         */
        public Builder timePartitioning(TimePartitioningType type) {
            this.timePartitioningType = Preconditions.checkNotNull(type, "type must not be null");
            this.timePartitioningField = null;
            return this;
        }

        /**
         * Partitions the table on the given {@code TIMESTAMP}, {@code DATE} or {@code DATETIME}
         * column with the given granularity — the three column types BigQuery partitions on.
         *
         * @param type the partitioning granularity
         * @param field the column to partition on
         * @return this builder
         */
        public Builder timePartitioning(TimePartitioningType type, String field) {
            this.timePartitioningType = Preconditions.checkNotNull(type, "type must not be null");
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(field), "field must not be blank");
            this.timePartitioningField = field;
            return this;
        }

        /**
         * Sets the partition expiration; partitions older than this are deleted by BigQuery.
         * Requires time partitioning to be configured.
         *
         * @param expiration the partition expiration, at least 1 ms
         * @return this builder
         */
        public Builder timePartitioningExpiration(Duration expiration) {
            OptionChecks.checkAtLeastOneMilli(expiration, "expiration");
            this.timePartitioningExpirationMs = expiration.toMillis();
            return this;
        }

        /**
         * Clusters the table on the given columns, in precedence order (at most four).
         *
         * @param fields the clustering columns
         * @return this builder
         */
        public Builder clusteredFields(List<String> fields) {
            Preconditions.checkNotNull(fields, "fields must not be null");
            Preconditions.checkArgument(!fields.isEmpty(), "fields must not be empty");
            Preconditions.checkArgument(
                    fields.size() <= MAX_CLUSTERED_FIELDS,
                    "BigQuery supports at most %s clustering columns: %s",
                    MAX_CLUSTERED_FIELDS,
                    fields);
            for (String field : fields) {
                Preconditions.checkArgument(
                        !StringUtils.isNullOrWhitespaceOnly(field),
                        "clustering columns must not be blank: %s",
                        fields);
            }
            this.clusteredFields = new ArrayList<>(fields);
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public TableCreateOptions build() {
            Preconditions.checkState(
                    timePartitioningExpirationMs == null || timePartitioningType != null,
                    "timePartitioningExpiration requires timePartitioning to be set.");
            return new TableCreateOptions(this);
        }
    }
}
