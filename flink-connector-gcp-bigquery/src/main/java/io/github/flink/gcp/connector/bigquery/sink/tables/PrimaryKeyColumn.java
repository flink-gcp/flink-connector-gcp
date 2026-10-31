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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.StandardSQLTypeName;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One column of a table's primary key, as the table declares it: its name, its GoogleSQL type, its
 * collation, and the scale of a parameterized decimal type. These decide whether two values a
 * serializer wrote for the column can be compared as the bytes it wrote them as.
 */
@Internal
public final class PrimaryKeyColumn {

    private final String name;
    private final StandardSQLTypeName type;
    @Nullable private final String collation;
    @Nullable private final Long scale;

    /**
     * Creates a key column without a declared scale.
     *
     * @param name the column name, as the table spells it
     * @param type the column's GoogleSQL type
     * @param collation the column's collation, or {@code null} or empty when it has none
     */
    public PrimaryKeyColumn(String name, StandardSQLTypeName type, @Nullable String collation) {
        this(name, type, collation, null);
    }

    /**
     * Creates a key column.
     *
     * @param name the column name, as the table spells it
     * @param type the column's GoogleSQL type
     * @param collation the column's collation, or {@code null} or empty when it has none
     * @param scale the scale of a parameterized {@code NUMERIC} or {@code BIGNUMERIC} type, or
     *     {@code null} when the type declares none
     */
    public PrimaryKeyColumn(
            String name,
            StandardSQLTypeName type,
            @Nullable String collation,
            @Nullable Long scale) {
        this.name = Preconditions.checkNotNull(name, "name must not be null");
        this.type = Preconditions.checkNotNull(type, "type must not be null");
        this.collation = collation == null || collation.isEmpty() ? null : collation;
        this.scale = scale;
    }

    /**
     * Returns the column name, as the table spells it.
     *
     * @return the column name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the column's GoogleSQL type.
     *
     * @return the column type
     */
    public StandardSQLTypeName getType() {
        return type;
    }

    /**
     * Returns the column's collation, or {@code null} when it has none.
     *
     * @return the collation, or {@code null}
     */
    @Nullable
    public String getCollation() {
        return collation;
    }

    /**
     * Returns the scale of a parameterized decimal type, or {@code null} when it declares none.
     * BigQuery rounds a value with more fractional digits to this scale.
     *
     * @return the scale, or {@code null}
     */
    @Nullable
    public Long getScale() {
        return scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrimaryKeyColumn)) {
            return false;
        }
        PrimaryKeyColumn that = (PrimaryKeyColumn) o;
        return name.equals(that.name)
                && type == that.type
                && Objects.equals(collation, that.collation)
                && Objects.equals(scale, that.scale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, collation, scale);
    }

    @Override
    public String toString() {
        return name
                + " "
                + type
                + (scale == null ? "" : "(scale " + scale + ")")
                + (collation == null ? "" : " COLLATE '" + collation + "'");
    }
}
