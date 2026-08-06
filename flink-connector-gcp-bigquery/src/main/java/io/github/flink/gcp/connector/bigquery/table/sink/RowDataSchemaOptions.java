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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * How {@link RowTypeToTableSchemaConverter} derives a BigQuery schema from a SQL table's columns —
 * the {@code RowData} counterpart of {@code AvroSchemaOptions}, carrying the same three knobs under
 * the same names.
 *
 * <p>Dotted paths rather than annotations, for the reason the Avro side has them: DDL has no
 * annotation mechanism, so a column's BigQuery type has to be named from outside the schema. A
 * nested column is {@code parent.child}; a map value is {@code theMap.value}, and a map key cannot
 * be marked at all.
 *
 * <p>{@code @Internal} rather than public, unlike its Avro sibling: this class exists to carry
 * three DDL options into the serializer, and nothing on the DataStream API takes a {@code RowData}
 * serializer yet.
 */
@Internal
public final class RowDataSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final RowDataSchemaOptions DEFAULTS = builder().build();

    private final Set<String> jsonFieldPaths;
    private final Set<String> geographyFieldPaths;
    private final boolean deriveRequiredColumns;

    private RowDataSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new LinkedHashSet<>(builder.json));
        this.geographyFieldPaths =
                Collections.unmodifiableSet(new LinkedHashSet<>(builder.geography));
        this.deriveRequiredColumns = builder.deriveRequiredColumns;
    }

    /** Returns the options every knob of which is left alone. */
    public static RowDataSchemaOptions defaults() {
        return DEFAULTS;
    }

    /** Returns a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the paths of columns derived as BigQuery {@code JSON} columns. */
    public Set<String> getJsonFieldPaths() {
        return jsonFieldPaths;
    }

    /** Returns the paths of columns derived as BigQuery {@code GEOGRAPHY} columns. */
    public Set<String> getGeographyFieldPaths() {
        return geographyFieldPaths;
    }

    /** Returns whether a {@code NOT NULL} column derives a {@code REQUIRED} BigQuery column. */
    public boolean isDeriveRequiredColumns() {
        return deriveRequiredColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowDataSchemaOptions that = (RowDataSchemaOptions) o;
        return deriveRequiredColumns == that.deriveRequiredColumns
                && jsonFieldPaths.equals(that.jsonFieldPaths)
                && geographyFieldPaths.equals(that.geographyFieldPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonFieldPaths, geographyFieldPaths, deriveRequiredColumns);
    }

    @Override
    public String toString() {
        return "RowDataSchemaOptions{jsonFieldPaths="
                + jsonFieldPaths
                + ", geographyFieldPaths="
                + geographyFieldPaths
                + ", deriveRequiredColumns="
                + deriveRequiredColumns
                + "}";
    }

    /** Builder for {@link RowDataSchemaOptions}. */
    @Internal
    public static final class Builder {

        private final Set<String> json = new LinkedHashSet<>();
        private final Set<String> geography = new LinkedHashSet<>();
        private boolean deriveRequiredColumns;

        private Builder() {}

        /**
         * Derives a {@code REQUIRED} BigQuery column from a column declared {@code NOT NULL}.
         *
         * @param deriveRequiredColumns whether to derive required columns
         * @return this builder
         */
        public Builder deriveRequiredColumns(boolean deriveRequiredColumns) {
            this.deriveRequiredColumns = deriveRequiredColumns;
            return this;
        }

        /**
         * Derives the named columns as BigQuery {@code JSON} columns.
         *
         * @param paths dotted column paths
         * @return this builder
         */
        public Builder jsonFieldPaths(Collection<String> paths) {
            json.addAll(paths);
            return this;
        }

        /**
         * Derives the named columns as BigQuery {@code GEOGRAPHY} columns.
         *
         * @param paths dotted column paths
         * @return this builder
         */
        public Builder geographyFieldPaths(Collection<String> paths) {
            geography.addAll(paths);
            return this;
        }

        /** Builds the options. */
        public RowDataSchemaOptions build() {
            return new RowDataSchemaOptions(this);
        }
    }
}
