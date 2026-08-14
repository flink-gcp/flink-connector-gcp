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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Desired BigQuery table contract for CDC writes.
 *
 * <p>Primary-key columns create a missing table and detect incompatible existing tables. When they
 * are absent, an existing table may supply them through its metadata, but a missing table cannot be
 * created. Maximum staleness is unmanaged by default; callers may set a duration or explicitly
 * clear the table option.
 */
@PublicEvolving
public final class CdcTableOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int MAX_PRIMARY_KEY_COLUMNS = 16;
    private static final CdcTableOptions DEFAULTS = builder().build();

    private final List<String> primaryKeyColumns;
    @Nullable private final Duration maxStaleness;
    private final boolean clearMaxStaleness;

    private CdcTableOptions(Builder builder) {
        this.primaryKeyColumns =
                Collections.unmodifiableList(new ArrayList<>(builder.primaryKeyColumns));
        this.maxStaleness = builder.maxStaleness;
        this.clearMaxStaleness = builder.clearMaxStaleness;
    }

    /** Returns an undeclared primary key and unmanaged maximum staleness. */
    public static CdcTableOptions defaults() {
        return DEFAULTS;
    }

    /** Creates a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the desired primary-key columns, or an empty list when none is declared. */
    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns == null ? Collections.emptyList() : primaryKeyColumns;
    }

    /** Returns the desired maximum staleness, or {@code null} when not setting a duration. */
    @Nullable
    public Duration getMaxStaleness() {
        return maxStaleness;
    }

    /** Returns whether maximum staleness is explicitly managed by this contract. */
    public boolean managesMaxStaleness() {
        return maxStaleness != null || clearMaxStaleness;
    }

    /** Returns whether the desired state has maximum staleness disabled. */
    public boolean clearsMaxStaleness() {
        return clearMaxStaleness;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdcTableOptions that = (CdcTableOptions) o;
        return clearMaxStaleness == that.clearMaxStaleness
                && getPrimaryKeyColumns().equals(that.getPrimaryKeyColumns())
                && Objects.equals(maxStaleness, that.maxStaleness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPrimaryKeyColumns(), maxStaleness, clearMaxStaleness);
    }

    @Override
    public String toString() {
        return "CdcTableOptions{primaryKeyColumns="
                + getPrimaryKeyColumns()
                + ", maxStaleness="
                + maxStaleness
                + ", clearMaxStaleness="
                + clearMaxStaleness
                + '}';
    }

    /** Builder for {@link CdcTableOptions}. */
    @PublicEvolving
    public static final class Builder {

        private List<String> primaryKeyColumns = Collections.emptyList();
        @Nullable private Duration maxStaleness;
        private boolean clearMaxStaleness;

        private Builder() {}

        /** Declares the columns of the unenforced primary key used by BigQuery CDC. */
        public Builder primaryKeyColumns(List<String> columns) {
            Preconditions.checkNotNull(columns, "columns must not be null");
            Preconditions.checkArgument(!columns.isEmpty(), "columns must not be empty");
            Preconditions.checkArgument(
                    columns.size() <= MAX_PRIMARY_KEY_COLUMNS,
                    "BigQuery supports at most %s primary-key columns: %s",
                    MAX_PRIMARY_KEY_COLUMNS,
                    columns);
            Set<String> normalized = new HashSet<>();
            for (String column : columns) {
                Preconditions.checkArgument(
                        !StringUtils.isNullOrWhitespaceOnly(column),
                        "primary-key columns must not be blank: %s",
                        columns);
                Preconditions.checkArgument(
                        normalized.add(column.toLowerCase(Locale.ROOT)),
                        "primary-key columns must be distinct ignoring case: %s",
                        columns);
            }
            this.primaryKeyColumns = new ArrayList<>(columns);
            return this;
        }

        /**
         * Sets the maximum staleness BigQuery applies to CDC queries.
         *
         * <p>This overrides an earlier {@link #clearMaxStaleness()} call.
         */
        public Builder maxStaleness(Duration maxStaleness) {
            OptionChecks.checkPositive(maxStaleness, "maxStaleness");
            OptionChecks.checkExpressibleInNanos(maxStaleness, "maxStaleness");
            Preconditions.checkArgument(
                    maxStaleness.toNanos() % 1_000 == 0,
                    "maxStaleness must be an exact number of microseconds: %s",
                    maxStaleness);
            this.maxStaleness = maxStaleness;
            this.clearMaxStaleness = false;
            return this;
        }

        /**
         * Disables maximum staleness on the destination table.
         *
         * <p>This overrides an earlier {@link #maxStaleness(Duration)} call.
         */
        public Builder clearMaxStaleness() {
            this.maxStaleness = null;
            this.clearMaxStaleness = true;
            return this;
        }

        /** Builds the immutable options. */
        public CdcTableOptions build() {
            return new CdcTableOptions(this);
        }
    }
}
