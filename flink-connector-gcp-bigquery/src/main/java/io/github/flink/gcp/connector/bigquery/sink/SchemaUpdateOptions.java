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

import java.io.Serializable;
import java.util.Objects;

/**
 * Options gating connector-driven destination table schema updates.
 *
 * <p>When the serializer's schema (from {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer#getTableSchema})
 * evolves past the destination table's schema, the sink can update the table itself via the
 * BigQuery API before continuing to write. The update is a <em>union</em>: existing table fields
 * are never dropped or reordered, new fields are appended, and field type changes are always
 * rejected. What the union may change is gated by these flags:
 *
 * <ul>
 *   <li>{@link Builder#allowNewFields()} — serializer fields missing from the table are appended.
 *       BigQuery cannot add {@code REQUIRED} columns to an existing table, so new fields are always
 *       added as {@code NULLABLE} even when the serializer declares them {@code REQUIRED}.
 *   <li>{@link Builder#allowFieldRelaxation()} — a table field declared {@code REQUIRED} is relaxed
 *       to {@code NULLABLE} when the serializer declares it nullable. {@code REPEATED} fields are
 *       never changed.
 * </ul>
 *
 * <p>Both flags default to off, in which case the sink never modifies table schemas (schema changes
 * made externally — for example via DDL — are still picked up reactively). Enabling updates
 * requires the {@code bigquery.tables.get} and {@code bigquery.tables.update} permissions.
 *
 * <p>Schema unionization is deliberately opt-in: BigQuery columns can never be dropped again, so a
 * single malformed record shipping an unexpected field could otherwise poison a table permanently.
 *
 * <p>Instances are immutable and serializable. Use {@link #defaults()} to keep updates disabled.
 */
@PublicEvolving
public final class SchemaUpdateOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final SchemaUpdateOptions DEFAULTS = builder().build();

    private final boolean allowNewFields;
    private final boolean allowFieldRelaxation;

    private SchemaUpdateOptions(Builder builder) {
        this.allowNewFields = builder.allowNewFields;
        this.allowFieldRelaxation = builder.allowFieldRelaxation;
    }

    /** Returns options with connector-driven schema updates disabled. */
    public static SchemaUpdateOptions defaults() {
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

    /** Returns whether missing serializer fields may be appended to the table. */
    public boolean isAllowNewFields() {
        return allowNewFields;
    }

    /** Returns whether {@code REQUIRED} table fields may be relaxed to {@code NULLABLE}. */
    public boolean isAllowFieldRelaxation() {
        return allowFieldRelaxation;
    }

    /** Returns whether any connector-driven schema update is enabled. */
    public boolean isEnabled() {
        return allowNewFields || allowFieldRelaxation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaUpdateOptions that = (SchemaUpdateOptions) o;
        return allowNewFields == that.allowNewFields
                && allowFieldRelaxation == that.allowFieldRelaxation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowNewFields, allowFieldRelaxation);
    }

    @Override
    public String toString() {
        return "SchemaUpdateOptions{allowNewFields="
                + allowNewFields
                + ", allowFieldRelaxation="
                + allowFieldRelaxation
                + "}";
    }

    /** Builder for {@link SchemaUpdateOptions}. */
    @PublicEvolving
    public static final class Builder {

        private boolean allowNewFields;
        private boolean allowFieldRelaxation;

        private Builder() {}

        /**
         * Allows appending serializer fields missing from the table (always as {@code NULLABLE}).
         *
         * @return this builder
         */
        public Builder allowNewFields() {
            this.allowNewFields = true;
            return this;
        }

        /**
         * Allows relaxing {@code REQUIRED} table fields to {@code NULLABLE} when the serializer
         * declares them nullable.
         *
         * @return this builder
         */
        public Builder allowFieldRelaxation() {
            this.allowFieldRelaxation = true;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public SchemaUpdateOptions build() {
            return new SchemaUpdateOptions(this);
        }
    }
}
